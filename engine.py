import time, json, sqlite3, math
from collections import deque
from dataclasses import dataclass,field

SOL="So11111111111111111111111111111111111111112"
def number(x):
    try:
        v=float(x)
        return v if math.isfinite(v) else None
    except (ValueError,TypeError,OverflowError):return None

@dataclass
class Token:
    mint:str
    name:str=""
    symbol:str=""
    creator:str=""
    detected:float=field(default_factory=time.time)
    first_cap:float|None=None
    last_cap:float|None=None
    trades:list=field(default_factory=list)
    signatures:set=field(default_factory=set)
    subscribed:bool=False
    subscription_pending:bool=False
    last_event:float=field(default_factory=time.time)
    score:int=0
    label:str="NEW / NO TRADES"
    reason:str=""
    samples:dict=field(default_factory=dict)
    alerted:bool=False

def read_cap(event):
    if event.get("quoteContextResolved") is False:return None
    if event.get("quoteMint") not in (None,SOL):return None
    return number(event.get("marketCapSol") if event.get("marketCapSol") is not None else event.get("marketCapQuote"))

def update_trade(token,event,now):
    typ=event.get("txType")
    if typ not in ("buy","sell"):return False
    sig=event.get("signature")
    if not isinstance(sig,str) or not sig or sig in token.signatures:return False
    token.signatures.add(sig)
    if len(token.signatures)>1000:token.signatures=set(list(token.signatures)[-500:])
    amount=number(event.get("solAmount"))
    if amount is None and event.get("quoteMint") in (None,SOL) and event.get("quoteContextResolved") is not False:
        amount=number(event.get("quoteAmount"))
    if amount is not None and amount<0:amount=None
    wallet=event.get("traderPublicKey") or ""
    token.trades.append((now,typ,amount,wallet,sig))
    token.last_event=now
    cap=read_cap(event)
    if cap is not None and cap>0:
        if token.first_cap is None:token.first_cap=cap
        token.last_cap=cap
    return True

def assess(token,now=None):
    now=now or time.time()
    age=max(0,now-token.detected)
    recent=[x for x in token.trades if now-x[0]<=30]
    buys=[x for x in recent if x[1]=="buy"]
    sells=[x for x in recent if x[1]=="sell"]
    wallets={x[3] for x in buys if x[3] and x[3]!=token.creator}
    valid_amounts=[x for x in recent if x[2] is not None]
    volume=sum(x[2] for x in valid_amounts)
    buy_volume=sum(x[2] for x in buys if x[2] is not None)
    sell_volume=sum(x[2] for x in sells if x[2] is not None)
    cap_change=(token.last_cap/token.first_cap-1)*100 if token.first_cap and token.last_cap else None
    total=len(recent)
    ratio=len(buys)/total if total else None
    creator_sells=sum(x[2] or 0 for x in sells if x[3] and x[3]==token.creator)
    # Evidence-weighted research ranking; not a calibrated probability of profit.
    score=0;positives=[];risk=[]
    buyers=len(wallets)
    if buyers>=5:score+=20;positives.append("5+ distinct non-creator buyers")
    if buyers>=10:score+=15;positives.append("10+ distinct non-creator buyers")
    if total>=8 and ratio is not None and ratio>=.70:
        score+=15;positives.append("70%+ observed buys")
    if len(valid_amounts)>=6 and volume>=2:
        score+=15;positives.append("2+ SOL observed turnover")
    if cap_change is not None and cap_change>=8:
        score+=15;positives.append("8%+ observed cap increase")
    if buy_volume>=1 and buy_volume>sell_volume*2 and len(valid_amounts)>=6:
        score+=10;positives.append("buy volume exceeds sell volume 2:1")
    if total>=8 and buyers>=5 and buyers/max(1,len(buys))>=.6:
        score+=10;positives.append("buy activity distributed across wallets")
    if creator_sells>=.5:risk.append("Creator sold 0.5+ SOL in observed window")
    if total>=8 and ratio is not None and ratio<.35:risk.append("Under 35% observed buys")
    if cap_change is not None and cap_change<=-15:risk.append("Observed cap drop 15%+")
    if len(buys)>=8 and buyers<=2:risk.append("Buying concentrated in 1-2 wallets")
    if len(valid_amounts)>=6 and volume>0:
        largest=max((x[2] or 0) for x in recent)
        if largest/volume>.75:risk.append("Single trade dominates observed volume")
    if risk:score=min(score,20)
    score=max(0,min(100,score))
    # Confidence measures sample sufficiency, not market direction or feed completeness.
    if total>=20 and buyers>=10 and len(valid_amounts)>=12:confidence="MODERATE"
    elif total>=8 and buyers>=5 and len(valid_amounts)>=5:confidence="LOW"
    else:confidence="VERY LOW"
    if risk:label="RISK FLAG"
    elif total==0:label="NO TRADE DATA"
    elif age<=45 and len(buys)>=3 and buyers>=3 and ratio is not None and ratio>=.75 and buy_volume>sell_volume:
        label="EARLY WATCH"
    elif confidence=="MODERATE" and score>=70 and ratio is not None and ratio>=.7:
        label="EARLY SIGNAL"
    elif total>=3:label="BUILDING"
    else:label="INSUFFICIENT"
    token.score=score;token.label=label
    token.reason="; ".join(risk or positives) or "Not enough observed evidence"
    return dict(age=age,score=score,label=label,confidence=confidence,
                buys=len(buys),sells=len(sells),wallets=buyers,volume=volume,
                volume_samples=len(valid_amounts),buy_volume=buy_volume,
                sell_volume=sell_volume,cap_change=cap_change,
                reason=token.reason,coverage="incomplete observed feed; no outcome calibration")

def db_open(path):
    db=sqlite3.connect(path)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("CREATE TABLE IF NOT EXISTS launches(mint TEXT PRIMARY KEY, name TEXT, symbol TEXT, creator TEXT, detected REAL)")
    db.execute("CREATE TABLE IF NOT EXISTS trades(mint TEXT, signature TEXT, seen REAL, side TEXT, sol REAL, wallet TEXT, cap_sol REAL, PRIMARY KEY(mint,signature))")
    db.execute("CREATE TABLE IF NOT EXISTS snapshots(mint TEXT, seen REAL, age REAL, label TEXT, score INTEGER, buys INTEGER, sells INTEGER, unique_buyers INTEGER, sol_volume REAL, cap_sol REAL, cap_change REAL, note TEXT)")
    db.commit()
    return db
