import os,time,json,threading,asyncio,collections,urllib.request,urllib.error,queue
from contextlib import asynccontextmanager
from fastapi import FastAPI,HTTPException,Query
from fastapi.responses import HTMLResponse
import websocket
from engine import Token,assess,update_trade,read_cap

FEED = os.getenv("PUMP_FEED_URL") or "wss://pumpdev.io/ws"
API_KEY = os.getenv("PUMPDEV_API_KEY", "").strip()
MAX_SUBS=25 if API_KEY else 5
SCANNER_VERSION="solana-mint-stream-v1"
SOLANA_WS_URL=os.getenv("SOLANA_WS_URL","wss://api.mainnet-beta.solana.com")
SOLANA_RPC_URL=os.getenv("SOLANA_RPC_URL","https://api.mainnet-beta.solana.com")
PRIVATE_RPC_CONFIGURED=bool(os.getenv("SOLANA_RPC_URL"))
PRIVATE_WS_CONFIGURED=bool(os.getenv("SOLANA_WS_URL"))
transaction_queue=queue.Queue(maxsize=100)
seen_signatures=set()
RPC_MIN_INTERVAL=float(os.getenv("SOLANA_RPC_MIN_INTERVAL","1.0"))
PUMP_PROGRAM_ID="6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
WINDOW=35
MAX_TOKENS=1200
lock=threading.RLock()
tokens={}
order=collections.deque()
active=set()
status={"connected":False,"last_message_at":None,"last_error":None,"launches":0,"trades":0,"started_at":time.time(),"source":"wss://pumpdev.io/ws","auth_status":"pending","auth_tier":None,"reconnects":0,"coverage":"partial token-trade coverage; subscriptions limited by configured tier","onchain_connected":False,"onchain_transactions_seen":0,"onchain_buy_log_candidates":0,"onchain_sell_log_candidates":0,"onchain_decoded_trades":0,"onchain_trade_mints":0,"mint_stream_connected":False,"mint_stream_subscriptions":0,"mint_stream_notifications":0,"onchain_rpc_errors":0,"onchain_rpc_rate_limited":0,"onchain_queue_dropped":0,"onchain_last_error":None,"onchain_last_message_at":None}
stopping=threading.Event()
ws_holder=[None]

def rebalance(ws,now):
    with lock:
        valid={m for m in active if m in tokens and now-tokens[m].detected<WINDOW}
        wanted=set(valid)
        for mint in order:
            if len(wanted)>=MAX_SUBS:break
            if now-tokens[mint].detected<WINDOW:wanted.add(mint)
        if order and now-tokens[order[0]].detected<5 and order[0] not in wanted and wanted:
            oldest=max(wanted,key=lambda m:now-tokens[m].detected)
            wanted.remove(oldest);wanted.add(order[0])
        add=wanted-active
        remove=active-wanted
        if not ws:return
        try:
            if remove:ws.send(json.dumps({"method":"unsubscribeTokenTrade","keys":sorted(remove)}))
            if add:ws.send(json.dumps({"method":"subscribeTokenTrade","keys":sorted(add)}))
            active.clear();active.update(wanted)
        except Exception as ex:status["last_error"]=str(ex)[:180]

def ingest(e,ws):
    if not isinstance(e,dict):return
    now=time.time()
    with lock:
        status["last_message_at"]=now
        if e.get("type")=="auth":
            status["auth_status"]=str(e.get("status") or "unknown")
            status["auth_tier"]=str(e.get("tier") or "")[:40] or None
            if e.get("status")=="ok":
                status["last_error"]=None
                rebalance(ws,now)
            else:
                status["last_error"]="Feed authentication not accepted: "+str(e.get("reason") or e.get("status"))[:100]
                rebalance(ws,now)
            return
        if e.get("type")=="error":
            status["last_error"]=str(e.get("message") or e.get("code") or e)[:180]
            return
        if e.get("txType")=="create":
            mint=e.get("mint")
            if not isinstance(mint,str) or not 32<=len(mint)<=48 or mint in tokens:return
            t=Token(mint=mint,name=str(e.get("name") or "")[:90],
                symbol=str(e.get("symbol") or "")[:30],creator=str(e.get("traderPublicKey") or ""),detected=now)
            cap=read_cap(e)
            if cap and cap>0:t.first_cap=cap;t.last_cap=cap
            tokens[mint]=t;order.appendleft(mint);status["launches"]+=1
            while len(order)>MAX_TOKENS:
                old=order.pop();tokens.pop(old,None);active.discard(old)
            rebalance(ws,now)
        elif e.get("txType") in ("buy","sell"):
            mint=e.get("mint")
            t=tokens.get(mint)
            if t and update_trade(t,e,now):status["trades"]+=1

def feed_loop():
    delay=2
    while not stopping.is_set():
        try:
            def opened(ws):
                with lock:
                    ws_holder[0]=ws;status["connected"]=True;status["last_error"]=None
                    active.clear()
                    status["auth_status"]="pending" if API_KEY else "anonymous"
                ws.send(json.dumps({"method":"subscribeNewToken"}))
                if API_KEY:
                    ws.send(json.dumps({"method":"auth","key":API_KEY}))
                else:
                    rebalance(ws,time.time())
            def message(ws,raw):
                try:ingest(json.loads(raw),ws)
                except Exception as ex:
                    with lock:status["last_error"]=str(ex)[:180]
            def error(ws,err):
                with lock:status["last_error"]=str(err)[:180]
            def closed(ws,code,reason):
                with lock:status["connected"]=False;ws_holder[0]=None
            ws=websocket.WebSocketApp(FEED,on_open=opened,on_message=message,on_error=error,on_close=closed)
            def rotation():
                while not stopping.wait(1):
                    with lock:
                        if ws_holder[0] is not ws:break
                        if status["connected"] and status["auth_status"]!="pending":
                            rebalance(ws,time.time())
            threading.Thread(target=rotation,daemon=True).start()
            ws.run_forever(ping_interval=25,ping_timeout=10)
        except Exception as ex:
            with lock:status["last_error"]=str(ex)[:180]
        with lock:status["connected"]=False;ws_holder[0]=None;status["reconnects"]+=1
        if stopping.wait(delay):break
        delay=min(30,delay*1.5)

def transaction_decoder():
    """Decode observed Pump.fun token transfers and wallet SOL changes from RPC metadata."""
    last_request=0
    backoff=0
    while not stopping.is_set():
        try:
            sig=transaction_queue.get(timeout=1)
        except queue.Empty:
            continue
        try:
            delay=max(0,RPC_MIN_INTERVAL-(time.monotonic()-last_request),backoff)
            if stopping.wait(delay):break
            last_request=time.monotonic()
            payload=json.dumps({"jsonrpc":"2.0","id":1,"method":"getTransaction",
                "params":[sig,{"encoding":"jsonParsed","commitment":"confirmed",
                "maxSupportedTransactionVersion":1}]}).encode()
            req=urllib.request.Request(SOLANA_RPC_URL,data=payload,
                headers={"Content-Type":"application/json"})
            with urllib.request.urlopen(req,timeout=9) as response:
                data=json.load(response)
            if data.get("error"):
                raise RuntimeError("RPC returned error code "+str(data["error"].get("code","unknown")))
            backoff=0
            tx=data.get("result")
            if not tx or not tx.get("meta") or tx["meta"].get("err"):continue
            meta=tx["meta"]
            message=tx["transaction"]["message"]
            keys=[k.get("pubkey") if isinstance(k,dict) else k for k in message.get("accountKeys",[])]
            pre={(b.get("accountIndex"),b.get("mint")):b for b in meta.get("preTokenBalances",[])}
            post={(b.get("accountIndex"),b.get("mint")):b for b in meta.get("postTokenBalances",[])}
            with lock:
                watched=set(tokens)
            matched_mints=set()
            for idx,mint in set(pre)|set(post):
                if mint not in watched:continue
                matched_mints.add(mint)
                before=pre.get((idx,mint),{})
                after=post.get((idx,mint),{})
                owner=after.get("owner") or before.get("owner")
                if not owner or owner not in keys:continue
                def amount(b):
                    return int(b.get("uiTokenAmount",{}).get("amount") or 0)
                delta=amount(after)-amount(before)
                if not delta:continue
                owner_idx=keys.index(owner)
                sol_before=meta.get("preBalances",[])
                sol_after=meta.get("postBalances",[])
                if owner_idx>=len(sol_before) or owner_idx>=len(sol_after):continue
                sol_delta=(sol_after[owner_idx]-sol_before[owner_idx])/1e9
                if delta>0 and sol_delta>=0:continue
                if delta<0 and sol_delta<=0:continue
                event={"txType":"buy" if delta>0 else "sell","mint":mint,
                    "signature":sig+":"+str(idx),"traderPublicKey":owner,
                    "solAmount":abs(sol_delta)}
                with lock:
                    token=tokens.get(mint)
                    if token and update_trade(token,event,time.time()):
                        status["trades"]+=1
                        status["onchain_decoded_trades"]+=1
            if matched_mints:
                with lock:status["onchain_trade_mints"]+=len(matched_mints)
        except urllib.error.HTTPError as ex:
            with lock:
                status["onchain_rpc_errors"]+=1
                if ex.code==429:status["onchain_rpc_rate_limited"]+=1
                status["onchain_last_error"]="RPC HTTP "+str(ex.code)
            backoff=min(30,max(2,backoff*2)) if ex.code==429 else 1
        except Exception as ex:
            with lock:
                status["onchain_rpc_errors"]+=1
                status["onchain_last_error"]=str(ex)[:180]
        finally:
            transaction_queue.task_done()

def onchain_observer():
    """Observe real Pump.fun program transaction logs, without guessing trade details."""
    while not stopping.is_set():
        try:
            def opened(ws):
                with lock:
                    status["onchain_connected"]=True
                    status["onchain_last_error"]=None
                ws.send(json.dumps({"jsonrpc":"2.0","id":1,"method":"logsSubscribe",
                    "params":[{"mentions":[PUMP_PROGRAM_ID]},{"commitment":"confirmed"}]}))
            def message(ws,raw):
                data=json.loads(raw)
                if "error" in data:
                    with lock:status["onchain_last_error"]=str(data["error"])[:180]
                if data.get("method")=="logsNotification":
                    value=data.get("params",{}).get("result",{}).get("value",{})
                    if value.get("err") is None and value.get("signature"):
                        with lock:
                            status["onchain_transactions_seen"]+=1
                            logs=value.get("logs") or []
                            # Program-wide stream is for diagnostics only. Mint-specific
                            # subscriptions below drive the RPC transaction decoder.
                            if any("Instruction: Buy" in line for line in logs):
                                status["onchain_buy_log_candidates"]+=1
                            if any("Instruction: Sell" in line for line in logs):
                                status["onchain_sell_log_candidates"]+=1
                            status["onchain_last_message_at"]=time.time()
            def error(ws,err):
                with lock:status["onchain_last_error"]=str(err)[:180]
            def closed(ws,code,reason):
                with lock:status["onchain_connected"]=False
            client=websocket.WebSocketApp(SOLANA_WS_URL,on_open=opened,
                on_message=message,on_error=error,on_close=closed)
            client.run_forever(ping_interval=25,ping_timeout=10)
        except Exception as ex:
            with lock:status["onchain_last_error"]=str(ex)[:180]
        with lock:status["onchain_connected"]=False
        stopping.wait(5)

def mint_trade_observer():
    """Subscribe to logs mentioning newly detected mint accounts.
    This avoids fetching every transaction across the entire Pump.fun program.
    """
    while not stopping.is_set():
        pending={}
        subscriptions={}
        request_id=100
        try:
            def opened(ws):
                with lock:status["mint_stream_connected"]=True
            def message(ws,raw):
                data=json.loads(raw)
                if data.get("id") in pending:
                    mint=pending.pop(data["id"])
                    if "result" in data and isinstance(data["result"],int):
                        subscriptions[mint]=data["result"]
                    elif "error" in data:
                        with lock:status["onchain_last_error"]="Mint subscription: "+str(data["error"])[:130]
                elif data.get("method")=="logsNotification":
                    value=data.get("params",{}).get("result",{}).get("value",{})
                    if value.get("err") is not None or not value.get("signature"):return
                    logs=value.get("logs") or []
                    if not any("Instruction: Buy" in x or "Instruction: Sell" in x for x in logs):return
                    signature=value["signature"]
                    with lock:
                        status["mint_stream_notifications"]+=1
                        if signature in seen_signatures:return
                        seen_signatures.add(signature)
                        if len(seen_signatures)>10000:seen_signatures.clear()
                        try:transaction_queue.put_nowait(signature)
                        except queue.Full:status["onchain_queue_dropped"]+=1
            def error(ws,err):
                with lock:status["onchain_last_error"]="Mint stream: "+str(err)[:140]
            def closed(ws,code,reason):
                with lock:status["mint_stream_connected"]=False
            ws=websocket.WebSocketApp(SOLANA_WS_URL,on_open=opened,
                on_message=message,on_error=error,on_close=closed)
            def rotation():
                nonlocal request_id
                while not stopping.wait(2):
                    with lock:
                        if not status["mint_stream_connected"]:continue
                        now=time.time()
                        wanted=[]
                        for mint in order:
                            if mint in tokens and now-tokens[mint].detected<90:
                                wanted.append(mint)
                            if len(wanted)>=10:break
                    try:
                        for mint,subscription in list(subscriptions.items()):
                            if mint not in wanted:
                                request_id+=1
                                ws.send(json.dumps({"jsonrpc":"2.0","id":request_id,
                                    "method":"logsUnsubscribe","params":[subscription]}))
                                subscriptions.pop(mint,None)
                        for mint in wanted:
                            if mint in subscriptions or mint in pending.values():continue
                            request_id+=1
                            pending[request_id]=mint
                            ws.send(json.dumps({"jsonrpc":"2.0","id":request_id,
                                "method":"logsSubscribe","params":[{"mentions":[mint]},
                                {"commitment":"confirmed"}]}))
                        with lock:status["mint_stream_subscriptions"]=len(subscriptions)
                    except Exception as ex:
                        with lock:status["onchain_last_error"]="Mint rotation: "+str(ex)[:140]
            threading.Thread(target=rotation,daemon=True).start()
            ws.run_forever(ping_interval=25,ping_timeout=10)
        except Exception as ex:
            with lock:status["onchain_last_error"]="Mint observer: "+str(ex)[:140]
        with lock:
            status["mint_stream_connected"]=False
            status["mint_stream_subscriptions"]=0
        stopping.wait(5)

@asynccontextmanager
async def lifespan(app):
    stopping.clear()
    thread=threading.Thread(target=feed_loop,daemon=True)
    thread.start()
    observer=threading.Thread(target=onchain_observer,daemon=True)
    observer.start()
    decoder=threading.Thread(target=transaction_decoder,daemon=True)
    decoder.start()
    mint_observer=threading.Thread(target=mint_trade_observer,daemon=True)
    mint_observer.start()
    yield
    stopping.set()
    with lock:
        if ws_holder[0]:
            try:ws_holder[0].close()
            except Exception:pass

app=FastAPI(title="Pump.fun Live Research Monitor",version="1.0",lifespan=lifespan)

def summary(t,now):
    a=assess(t,now)
    return {"mint":t.mint,"symbol":t.symbol,"name":t.name,"creator":t.creator,
      "detected_at":t.detected,"age_seconds":round(a["age"],1),"classification":a["label"],
      "score":a["score"],"observed_buys_30s":a["buys"],"observed_sells_30s":a["sells"],
      "distinct_buying_wallets_30s":a["wallets"],"observed_sol_volume_30s":round(a["volume"],4),
      "volume_trade_samples":a["volume_samples"],
      "observed_market_cap_sol":t.last_cap,
      "observed_cap_change_pct":round(a["cap_change"],2) if a["cap_change"] is not None else None,
      "reason":a["reason"],"currently_subscribed":t.mint in active and status["connected"],
      "pump_url":"https://pump.fun/coin/"+t.mint}

@app.get("/api/status")
def get_status():
    with lock:return {**status,"scanner_version":SCANNER_VERSION,"private_rpc_configured":PRIVATE_RPC_CONFIGURED,"private_ws_configured":PRIVATE_WS_CONFIGURED,"transaction_queue_depth":transaction_queue.qsize(),"active_subscriptions":len(active),"subscription_limit":MAX_SUBS,"tracked_tokens":len(tokens),"server_time":time.time(),
       "limitations":["No complete trade coverage","No verified unique humans","No live X feed","No prediction guarantee"]}

@app.get("/api/live")
def live(limit:int=Query(30,ge=1,le=200),mode:str="signals"):
    if mode not in ("signals","all","building"):raise HTTPException(400,"mode must be signals, building or all")
    with lock:
        now=time.time()
        rows=[summary(tokens[m],now) for m in order if m in tokens and now-tokens[m].detected<180]
        if mode=="signals":rows=[r for r in rows if r["classification"]=="EARLY SIGNAL"]
        if mode=="building":rows=[r for r in rows if r["classification"] in ("EARLY SIGNAL","BUILDING")]
        rows.sort(key=lambda r:(r["classification"]=="EARLY SIGNAL",r["score"],-r["age_seconds"]),reverse=True)
        return {"as_of":now,"feed_connected":status["connected"],"mode":mode,"count":len(rows),
                "coins":rows[:limit],"disclaimer":"Observed partial feed; not financial advice or a trading signal."}

@app.get("/api/coin/{mint}")
def coin(mint:str):
    with lock:
        t=tokens.get(mint)
        if not t:raise HTTPException(404,"Not currently retained")
        data=summary(t,time.time())
        data["recent_observed_trades"]=[{"seen_at":x[0],"side":x[1],"sol":x[2],"wallet":x[3],"signature":x[4]} for x in t.trades[-60:]]
        return data

HTML=r"""<!DOCTYPE html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Pump.fun First Seconds Monitor</title>
<style>body{font:15px system-ui;background:#0d1420;color:#e7edf6;margin:0;padding:24px}h1{margin:0 0 8px}
small,p{color:#b5c3d5}.status{margin:14px 0;padding:12px;background:#1b2a3d;border-radius:9px}
select,button{padding:10px;background:#233650;color:white;border:1px solid #455b77;border-radius:7px}
table{border-collapse:collapse;width:100%;margin-top:16px}th,td{padding:10px;border-bottom:1px solid #314054;text-align:left}
th{color:#a9bdd2}a{color:#87c5ff} .wrap{overflow-x:auto}.tag{color:#7ff0c2;font-weight:700}
</style></head><body>
<h1>⚡ Pump.fun First-Seconds Monitor</h1>
<p>Live research only · No wallet · No trading · Data is incomplete due to free-feed subscription limits.</p>
<div id="status" class="status">Connecting to monitor…</div>
<label for="mode">Show </label><select id="mode"><option value="signals">Early signals only</option><option value="building" selected>Signals + building</option><option value="all">All detected launches</option></select>
<div class="wrap"><table><thead><tr><th>Age</th><th>Token</th><th>Status</th><th>Score</th><th>Buys</th><th>Sells</th><th>Buying wallets</th><th>SOL volume</th><th>Cap Δ</th><th>Coverage</th></tr></thead><tbody id="rows"></tbody></table></div>
<p id="stamp"></p>
<p><strong>Important:</strong> “EARLY SIGNAL” is an unvalidated heuristic, not a buy recommendation. The anonymous feed only tracks up to five tokens simultaneously, so missing trades can distort scores. No X post analysis or guaranteed first-second detection.</p>
<p>For ChatGPT analysis, share your public URL ending in <code>/api/live?mode=building</code>. ChatGPT can check it when you ask, but cannot stay permanently connected to it in an ordinary chat.</p>
<script>
const esc=s=>String(s??"").replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
async function refresh(){
 try{
 const [s,r]=await Promise.all([fetch("/api/status",{cache:"no-store"}),fetch("/api/live?limit=100&mode="+encodeURIComponent(document.getElementById("mode").value),{cache:"no-store"})]);
 if(!s.ok||!r.ok)throw Error("Server returned an error");
 const st=await s.json(),d=await r.json();
 document.getElementById("status").textContent=(st.connected?"🟢 Feed connected":"🔴 Feed offline")+
 " | Launches: "+st.launches+" | Observed trades: "+st.trades+" | Active subscriptions: "+st.active_subscriptions+"/"+st.subscription_limit+
 (st.last_error?" | Error: "+st.last_error:"");
 document.getElementById("rows").innerHTML=d.coins.map(x=>`<tr><td>${Math.round(x.age_seconds)}s</td>
 <td><a target="_blank" rel="noopener noreferrer" href="${esc(x.pump_url)}">${esc(x.symbol||x.mint.slice(0,8))}</a></td>
 <td class="${x.classification==="EARLY SIGNAL"?"tag":""}">${esc(x.classification)}</td>
 <td>${x.score}</td><td>${x.observed_buys_30s}</td><td>${x.observed_sells_30s}</td>
 <td>${x.distinct_buying_wallets_30s}</td><td>${x.volume_trade_samples?x.observed_sol_volume:"?"}</td>
 <td>${x.observed_cap_change_pct===null?"?":x.observed_cap_change_pct+"%"}</td>
 <td>${x.currently_subscribed?"ACTIVE":"NOT LIVE"}</td></tr>`).join("")||"<tr><td colspan='10'>No matching tokens currently observed. Try All detected launches.</td></tr>";
 document.getElementById("stamp").textContent="Last refresh: "+new Date().toLocaleTimeString();
 }catch(e){document.getElementById("status").textContent="⚠️ Monitor unavailable: "+e.message}
}
document.getElementById("mode").addEventListener("change",refresh);
refresh();setInterval(refresh,3000);
</script></body></html>"""
@app.get("/",response_class=HTMLResponse)
def home():return HTML
