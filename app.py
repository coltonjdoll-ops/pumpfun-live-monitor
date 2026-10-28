import os,time,json,threading,asyncio,collections
from contextlib import asynccontextmanager
from fastapi import FastAPI,HTTPException,Query
from fastapi.responses import HTMLResponse
import websocket
from engine import Token,assess,update_trade,read_cap

FEED = os.getenv("PUMP_FEED_URL") or (
    "wss://pumpdev.io/ws?api-key=" + os.getenv("PUMPDEV_API_KEY", "")
)
MAX_SUBS=5
WINDOW=35
MAX_TOKENS=1200
lock=threading.RLock()
tokens={}
order=collections.deque()
active=set()
status={"connected":False,"last_message_at":None,"last_error":None,"launches":0,"trades":0,"started_at":time.time(),"source":FEED,"reconnects":0,"coverage":"up to five simultaneously subscribed tokens; incomplete"}
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
                ws.send(json.dumps({"method":"subscribeNewToken"}))
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
                        rebalance(ws,time.time())
            threading.Thread(target=rotation,daemon=True).start()
            ws.run_forever(ping_interval=25,ping_timeout=10)
        except Exception as ex:
            with lock:status["last_error"]=str(ex)[:180]
        with lock:status["connected"]=False;ws_holder[0]=None;status["reconnects"]+=1
        if stopping.wait(delay):break
        delay=min(30,delay*1.5)

@asynccontextmanager
async def lifespan(app):
    stopping.clear()
    thread=threading.Thread(target=feed_loop,daemon=True)
    thread.start()
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
    with lock:return {**status,"active_subscriptions":len(active),"tracked_tokens":len(tokens),"server_time":time.time(),
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
 " | Launches: "+st.launches+" | Observed trades: "+st.trades+" | Active subscriptions: "+st.active_subscriptions+"/5"+
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
