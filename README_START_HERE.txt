PUMP.FUN LIVE WEB MONITOR — CHATGPT-READABLE WEB API
======================================================
WHAT YOU GET
A web dashboard at /, JSON live candidate list at /api/live?mode=building,
feed status at /api/status, and token details at /api/coin/<mint>.
The API is public and read-only. It exposes observed public wallet addresses
and transactions; DO NOT add private keys or credentials to this project.

HOW TO HOST (requires a hosting account and deployment)
Option A: Put these files in a NEW GitHub repository. In Render, choose New >
Blueprint and select that repository. Render will read render.yaml.
Option B: Use any Python host with:
  pip install -r requirements.txt
  uvicorn app:app --host 0.0.0.0 --port $PORT
Or deploy with Dockerfile.

Free Render instances can sleep when idle. They also restart and lose in-memory
launch history. If the service sleeps, it cannot continuously monitor launches.
For reliable 24/7 monitoring use an always-on host. Only run ONE process/worker,
otherwise each worker opens separate feed subscriptions and data diverges.

HOW TO CONNECT IT TO CHATGPT
After deployment, copy the public URL, e.g.
https://YOUR-DEPLOYMENT.example/api/live?mode=building
and send it in this chat. I can attempt to fetch and analyze that URL
when you ask for an update. THIS IS NOT a persistent push connection:
I cannot automatically receive every event, monitor continuously in this
chat, or guarantee an update without a fresh request or supported automation.

DATA SOURCE AND LIMITS
PumpDev websocket (default wss://pumpdev.io/ws) is an external public
data provider. If you mean literally no outside services beyond Pump.fun,
this version does NOT meet that requirement. The data source can be changed
with environment variable PUMP_FEED_URL, but the message schema must match.
Feed endpoint, availability, limits and pricing may change.

Anonymous tier previously advertised 5 simultaneous token trade subscriptions.
This app rotates to the newest launches; it cannot monitor all token trades.
Scoring is rules-based, unvalidated, and uses incomplete observed trades.
Unique wallet addresses do not prove unique humans or organic activity.
Age is since server detection. No real trading, wallet, private keys,
Twitter analysis, or promise of profitable picks.

SECURITY
The public API has no authentication. Only use it for PUBLIC research data.
Protect or restrict the URL if you don't want the observed addresses exposed.
