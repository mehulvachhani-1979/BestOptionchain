# angel_proxy_ws.py  — WebSocket edition
# Run:  py -3.13 angel_proxy_ws.py
# Install: py -3.13 -m pip install flask flask-cors flask-socketio requests websocket-client

from flask import Flask, request, Response
from flask_cors import CORS
from flask_socketio import SocketIO, emit
import requests, json, threading, time, http.client, re, math, os, pickle, uuid, statistics
from datetime import datetime, timedelta
from collections import defaultdict

# ── App ───────────────────────────────────────────────────────────────────────
app    = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})
sio    = SocketIO(app, cors_allowed_origins="*", async_mode="threading", logger=False, engineio_logger=False)

ANGEL_BASE   = "apiconnect.angelone.in"
SCRIP_URL    = "https://margincalculator.angelone.in/OpenAPI_File/files/OpenAPIScripMaster.json"
SMARTSTREAM  = "smartapisocket.angelone.in"

# SmartAPI SDK -- needed for WebSocket live ticks
try:
    from SmartApi.smartWebSocketV2 import SmartWebSocketV2
    SDK_AVAILABLE = True
    print("[Proxy] SmartApi SDK found -- WebSocket enabled")
except ImportError:
    SmartWebSocketV2 = None
    SDK_AVAILABLE = False
    print("[Proxy] smartapi-python not found -- REST-only mode (no live ticks)")
    print("[Proxy] To enable live ticks: pip install smartapi-python")

# SmartStream exchange type + mode constants (must be defined before _do_subscribe)
NSE_EXCH = 1   # NSE cash segment
NFO_EXCH = 2   # NSE F&O segment
BSE_EXCH = 3   # BSE cash
WS_MODE  = 3   # SNAP_QUOTE — LTP + OI + OHLC

# ── Shared store ──────────────────────────────────────────────────────────────
store = {
    "api_key": "", "jwt": "", "client_code": "",
    "instruments": {}, "cache": {},
    "status": "idle", "error": "",
    "instruments_loaded": False, "instruments_loading": False,
    "index_tokens": {}, "stock_tokens": {},
    # MCX commodity options
    "mcx_instruments": {},   # {symbol: {expiry: {strike: {CE/PE: token}}}}
    "mcx_fut_tokens":  {},   # {symbol: futures_token}
    "mcx_loaded":      False,
}

# MCX module (lazy import — ok if file not present)
_mcx = None
def _get_mcx():
    global _mcx
    if _mcx is None:
        try:
            import mcx_module as m
            _mcx = m
        except ImportError:
            pass
    return _mcx

# ── SmartStream WebSocket state ───────────────────────────────────────────────
ws_state = {
    "ws":           None,        # websocket.WebSocketApp instance
    "thread":       None,
    "running":      False,
    "subscribed":   set(),       # token strings currently subscribed
    "ltp_cache":    {},          # token -> {ltp, oi, volume, ts}
    "pending_sub":  set(),       # tokens waiting to subscribe after connect
    "connected":    False,
}

# ═══════════════════════════════════════════════════════════════════════════════
# ANGEL ONE REST HELPERS  (identical to original proxy)
# ═══════════════════════════════════════════════════════════════════════════════

def angel_call(endpoint, payload=None, method="POST", api_key=None, jwt=None):
    ak  = api_key or store["api_key"]
    tok = jwt     or store["jwt"]
    conn = http.client.HTTPSConnection(ANGEL_BASE, timeout=15)
    headers = {
        "X-PrivateKey":     ak,
        "Accept":           "application/json",
        "X-SourceID":       "WEB",
        "X-ClientLocalIP":  "127.0.0.1",
        "X-ClientPublicIP": "127.0.0.1",
        "X-MACAddress":     "AA:BB:CC:DD:EE:FF",
        "X-UserType":       "USER",
        "Authorization":    "Bearer " + tok,
        "Content-Type":     "application/json",
    }
    body = json.dumps(payload) if payload else ""
    conn.request(method, endpoint, body, headers)
    res  = conn.getresponse()
    raw  = res.read().decode("utf-8")
    conn.close()
    print(f"  [{method}] {endpoint} -> {res.status} | {raw[:120]}")
    return json.loads(raw)

def angel_post(endpoint, payload, api_key=None, jwt=None):
    return angel_call(endpoint, payload, "POST", api_key, jwt)

# ── Instrument loading (same as original) ─────────────────────────────────────
FNO_STOCKS = []

def load_all_instruments():
    global FNO_STOCKS
    if store["instruments_loading"] or store["instruments_loaded"]: return
    store["instruments_loading"] = True
    print("Downloading full instrument list...")
    r    = requests.get(SCRIP_URL, timeout=60)
    data = r.json()
    print(f"Total instruments: {len(data)}")

    fno_set = set()
    for d in data:
        if d.get("instrumenttype") == "OPTSTK" and d.get("exch_seg") == "NFO":
            name = d.get("name","")
            if name: fno_set.add(name)
    FNO_STOCKS = sorted(fno_set)
    print(f"Auto-detected {len(FNO_STOCKS)} F&O stocks")

    INDEX_NAMES  = ["NIFTY","BANKNIFTY","FINNIFTY","MIDCPNIFTY","SENSEX"]
    all_names    = set(INDEX_NAMES + FNO_STOCKS)
    index_tokens = {"NIFTY":"99926000","BANKNIFTY":"99926009","FINNIFTY":"99926037","MIDCPNIFTY":"99926074"}
    stock_tokens = {}
    all_map      = {}

    for d in data:
        name  = d.get("name","")
        itype = d.get("instrumenttype","")
        exch  = d.get("exch_seg","")
        sym   = d.get("symbol","")
        if exch == "NSE" and name in FNO_STOCKS and name not in stock_tokens:
            if (sym.endswith("-EQ") or sym == name or itype in ("","EQ")):
                stock_tokens[name] = d.get("token","")
        if itype in ("OPTIDX","OPTSTK") and exch == "NFO" and name in all_names:
            expiry   = d.get("expiry","")
            strike   = int(float(d.get("strike",0)) / 100)
            opt_type = sym[-2:]
            token    = d.get("token","")
            if name not in all_map: all_map[name] = {}
            if expiry not in all_map[name]: all_map[name][expiry] = {}
            if strike not in all_map[name][expiry]: all_map[name][expiry][strike] = {}
            all_map[name][expiry][strike][opt_type] = token

    for name, expiry_map in all_map.items():
        store["instruments"][name] = expiry_map

    store["index_tokens"]      = index_tokens
    store["stock_tokens"]      = stock_tokens
    store["instruments_loaded"]  = True
    store["instruments_loading"] = False
    print(f"Loaded {len(all_map)} symbols")
    # Show what index symbols loaded — helps diagnose NIFTY not found issue
    for idx_sym in ["NIFTY","BANKNIFTY","FINNIFTY"]:
        n = len(all_map.get(idx_sym,{}))
        sample = list(all_map.get(idx_sym,{}).keys())[:5]
        print(f"  {idx_sym}: {n} expiries {'OK' if n else 'NOT FOUND'} | sample: {sample}")
    # Show sample of actual names in all_map for debugging
    sample_names = [k for k in list(all_map.keys())[:10]]
    print(f"  Sample symbol names in map: {sample_names}")

    # ── Load MCX commodity instruments ───────────────────────────────────────
    try:
        mcx_mod = _get_mcx()
        if mcx_mod:
            print("Loading MCX commodity instruments...")
            mcx_map, fut_tokens = mcx_mod.load_mcx_instruments(data)
            store["mcx_instruments"] = mcx_map
            store["mcx_fut_tokens"]  = fut_tokens
            store["mcx_loaded"]      = True
            print(f"MCX loaded: {list(mcx_map.keys())}")
        else:
            print("mcx_module.py not found — MCX disabled")
    except Exception as e:
        print(f"MCX load error: {e}")
        import traceback; traceback.print_exc()

# ── Black-Scholes & IV (same as original) ─────────────────────────────────────
def _bs_price(S, K, T, r, sigma, opt_type):
    if T <= 0 or sigma <= 0: return 0
    d1 = (math.log(S/K) + (r + 0.5*sigma**2)*T) / (sigma*math.sqrt(T))
    d2 = d1 - sigma*math.sqrt(T)
    def N(x): return 0.5*(1+math.erf(x/math.sqrt(2)))
    if opt_type == "CE": return S*N(d1) - K*math.exp(-r*T)*N(d2)
    else:                return K*math.exp(-r*T)*N(-d2) - S*N(-d1)

def expiry_to_years(expiry_str):
    try:
        mmap={"JAN":1,"FEB":2,"MAR":3,"APR":4,"MAY":5,"JUN":6,"JUL":7,"AUG":8,"SEP":9,"OCT":10,"NOV":11,"DEC":12}
        m = re.match(r"(\d{2})([A-Z]{3})(\d{4})", expiry_str)
        if not m: return 0
        exp_date   = datetime(int(m[3]), mmap[m[2]], int(m[1]), 15, 30, 0)
        diff_secs  = (exp_date - datetime.now()).total_seconds()
        diff_secs  = max(diff_secs, 1800)
        return diff_secs / (365.25 * 24 * 3600)
    except: return 1/365

def calc_iv(market_price, S, K, T, opt_type, r=0.065):
    if market_price <= 0 or S <= 0 or K <= 0 or T <= 0: return 0
    intrinsic = max(0, S-K) if opt_type=="CE" else max(0, K-S)
    if market_price < intrinsic or market_price > S*0.5: return 0
    lo, hi = 0.001, 10.0
    for _ in range(60):
        mid = (lo+hi)/2
        p   = _bs_price(S, K, T, r, mid, opt_type)
        if abs(p - market_price) < 0.005: return round(mid*100, 2)
        if p < market_price: lo = mid
        else: hi = mid
    return round(mid*100, 2) if 0.1 <= mid*100 <= 999 else 0

# ── OI cache (same as original) ───────────────────────────────────────────────
OI_CACHE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "oi_cache.pkl")
def _load_oi_cache():
    try:
        if os.path.exists(OI_CACHE_FILE):
            with open(OI_CACHE_FILE,"rb") as f: return pickle.load(f)
    except: pass
    return {}
def _save_oi_cache():
    try:
        with open(OI_CACHE_FILE,"wb") as f: pickle.dump(oi_prev, f)
    except: pass

oi_prev = _load_oi_cache()

def get_oi_change(key, current_oi):
    prev = oi_prev.get(key)
    oi_prev[key] = current_oi
    if len(oi_prev) % 10 == 0: _save_oi_cache()
    return current_oi - prev if prev is not None else 0

# ── Spot fetch ────────────────────────────────────────────────────────────────
def fetch_spot(symbol):
    token = store.get("index_tokens",{}).get(symbol) or store.get("stock_tokens",{}).get(symbol)
    if not token: return 0
    # Check ws_state.ltp_cache first (live tick data)
    cached = ws_state["ltp_cache"].get(token)
    if cached and cached.get("ltp", 0) > 0: return cached["ltp"]
    data = angel_post("/rest/secure/angelbroking/market/v1/quote/",{"mode":"LTP","exchangeTokens":{"NSE":[token]}})
    if data.get("status") and data.get("data",{}).get("fetched"):
        q = data["data"]["fetched"][0]
        # After hours: ltp=0, use close as spot fallback
        return q.get("ltp") or q.get("close", 0)
    return 0

# ── Expiry helpers ────────────────────────────────────────────────────────────
# Expiry schedule:
# NIFTY     = every Tuesday (weekly)
# FINNIFTY  = every Tuesday (weekly)
# BANKNIFTY = last Tuesday of month (monthly)
# MIDCPNIFTY= last Monday of month (monthly)
WEEKLY_SYMBOLS  = {"NIFTY", "FINNIFTY"}
MONTHLY_SYMBOLS = {"BANKNIFTY", "MIDCPNIFTY", "SENSEX"}

def _to_date(e):
    m = re.match(r"(\d{2})([A-Z]{3})(\d{4})", e)
    if not m: return datetime.max
    mm={"JAN":1,"FEB":2,"MAR":3,"APR":4,"MAY":5,"JUN":6,"JUL":7,"AUG":8,"SEP":9,"OCT":10,"NOV":11,"DEC":12}
    return datetime(int(m[3]), mm.get(m[2],1), int(m[1]))

def _last_tuesday_of_month(year, month):
    last = datetime(year+1,1,1)-timedelta(days=1) if month==12 else datetime(year,month+1,1)-timedelta(days=1)
    while last.weekday() != 1: last -= timedelta(days=1)   # 1=Tuesday
    return last

def _last_monday_of_month(year, month):
    last = datetime(year+1,1,1)-timedelta(days=1) if month==12 else datetime(year,month+1,1)-timedelta(days=1)
    while last.weekday() != 0: last -= timedelta(days=1)   # 0=Monday
    return last

def get_correct_expiry(symbol, instruments_override=None):
    """
    Returns nearest valid expiry for NSE F&O symbols (IST aware).
    NIFTY/FINNIFTY : every Tuesday (weekly)
    BANKNIFTY      : last Tuesday of month (monthly)
    MIDCPNIFTY     : last Monday of month (monthly)
    After 3:30 PM IST today's expiry is considered closed.
    Pass instruments_override to use a different instruments dict.
    """
    import pytz
    ist = pytz.timezone("Asia/Kolkata")
    now_ist  = datetime.now(ist).replace(tzinfo=None)
    today    = now_ist.replace(hour=0, minute=0, second=0, microsecond=0)

    # After 3:30 PM IST, today's expiry is done — start from tomorrow
    market_closed = (now_ist.hour > 15) or (now_ist.hour == 15 and now_ist.minute >= 30)
    min_date  = today + timedelta(days=1) if market_closed else today
    cutoff    = today + timedelta(days=180)  # ignore LEAPS beyond 6 months

    instr     = instruments_override if instruments_override is not None else store["instruments"]
    expiries  = list(instr.get(symbol, {}).keys())
    print(f"  [expiry_debug] {symbol}: {len(expiries)} expiries in {'override' if instruments_override else 'store'}, min_date={min_date.date()}, sample={sorted(expiries)[:5]}")
    if not expiries: return None

    # Parse all expiries, filter past + LEAPS
    parsed = []
    for e in expiries:
        d = _to_date(e)
        if d == datetime.max:  continue
        if d < min_date:       continue
        if d > cutoff:         continue
        parsed.append((e, d))

    if not parsed:
        # fallback: no LEAPS filter
        parsed = [(e, _to_date(e)) for e in expiries
                  if _to_date(e) != datetime.max and _to_date(e) >= min_date]

    if not parsed: return None

    # Sort ascending — nearest first
    parsed.sort(key=lambda x: x[1])
    result = parsed[0][0]
    stype  = "weekly" if symbol in WEEKLY_SYMBOLS else "monthly"
    print(f"  expiry {symbol}: {result} ({stype}) | {len(parsed)} options: {[e for e,_ in parsed[:4]]}")
    return result


def _on_data(wsapp, message):
    """Called by SmartWebSocketV2 for every tick (already parsed to dict)"""
    try:
        # message is a dict from SDK  e.g. {token, last_traded_price, open_interest, ...}
        token = str(message.get("token", ""))
        if not token: return

        ltp  = message.get("last_traded_price",  0) / 100.0
        oi   = message.get("open_interest",       0)
        vol  = message.get("volume_trade_for_day",0)
        high = message.get("high_price_of_the_day",0) / 100.0
        low  = message.get("low_price_of_the_day", 0) / 100.0
        cl   = message.get("closed_price",          0) / 100.0
        opn  = message.get("open_price_of_the_day", 0) / 100.0
        chg  = round(((ltp - cl) / cl * 100), 2) if cl > 0 else 0

        if ltp <= 0: return

        tick = {
            "token":   token,
            "ltp":     round(ltp, 2),
            "oi":      oi,
            "volume":  vol,
            "high":    round(high, 2),
            "low":     round(low, 2),
            "open":    round(opn, 2),
            "close":   round(cl, 2),
            "chgPct":  chg,
            "ts":      datetime.now().isoformat(),
        }
        ws_state["ltp_cache"][token] = tick
        # Instant broadcast per tick
        sio.emit("tick", tick)
    except Exception as e:
        print(f"[SmartStream] on_data error: {e}")

def _on_open(wsapp):
    print("[SmartStream] ✅ Connected & authenticated")
    ws_state["connected"] = True
    sio.emit("ws_status", {"connected": True, "subscribed": 0,
                            "msg": "SmartStream connected"})
    # Subscribe any tokens that were queued before connection
    if ws_state["pending_sub"]:
        _do_subscribe(ws_state["pending_sub"])
        ws_state["pending_sub"].clear()

def _on_error(wsapp, error):
    print(f"[SmartStream] ❌ Error: {error}")
    ws_state["connected"] = False
    sio.emit("ws_status", {"connected": False, "msg": str(error)})

def _on_close(wsapp):
    print("[SmartStream] Closed")
    ws_state["connected"] = False
    ws_state["subscribed"].clear()
    ws_state["running"]   = False
    sio.emit("ws_status", {"connected": False, "msg": "SmartStream closed"})

def _do_subscribe(tokens):
    """Actually call sws.subscribe() with split batches of max 1000"""
    global _sws_instance
    if not _sws_instance: return
    tokens = list(set(str(t) for t in tokens))
    # Split into NFO vs NSE based on token length heuristic
    # NFO option tokens are 5-6 digit, NSE index tokens are 8 digit (99926xxx)
    nfo = [t for t in tokens if not t.startswith("999")]
    nse = [t for t in tokens if t.startswith("999")]
    corr = f"proxy_{int(time.time())}"
    try:
        token_list = []
        if nfo: token_list.append({"exchangeType": NFO_EXCH, "tokens": nfo[:1000]})
        if nse: token_list.append({"exchangeType": NSE_EXCH, "tokens": nse[:50]})
        if token_list:
            _sws_instance.subscribe(corr, WS_MODE, token_list)
            ws_state["subscribed"].update(tokens)
            print(f"[SmartStream] Subscribed {len(tokens)} tokens (NFO:{len(nfo)} NSE:{len(nse)})")
            sio.emit("ws_status", {"connected": True,
                                    "subscribed": len(ws_state["subscribed"]),
                                    "msg": f"{len(ws_state['subscribed'])} tokens live"})
    except Exception as e:
        print(f"[SmartStream] Subscribe error: {e}")

def _start_smartstream():
    global _sws_instance
    if not SDK_AVAILABLE:
        print("[SmartStream] smartapi-python SDK not available — polling only")
        return
    if not store["jwt"] or not store["api_key"]:
        print("[SmartStream] No credentials — skipping")
        return
    if ws_state["running"]:
        return

    feed_token = store.get("feed_token", "")
    if not feed_token:
        print("[SmartStream] No feed_token — cannot connect. Ensure /connect fetches it.")
        return

    print(f"[SmartStream] Starting SmartWebSocketV2 (feedToken: {feed_token[:8]}...)")
    ws_state["running"] = True

    try:
        sws = SmartWebSocketV2(
            auth_token  = store["jwt"],
            api_key     = store["api_key"],
            client_code = store["client_code"],
            feed_token  = feed_token,
            max_retry_attempt = 10,
        )
        _sws_instance = sws
        sws.on_open  = _on_open
        sws.on_data  = _on_data
        sws.on_error = _on_error
        sws.on_close = _on_close

        def run():
            try:
                sws.connect()
            except Exception as e:
                print(f"[SmartStream] connect() error: {e}")
                ws_state["running"]   = False
                ws_state["connected"] = False

        t = threading.Thread(target=run, daemon=True)
        t.start()
        ws_state["thread"] = t
    except Exception as e:
        print(f"[SmartStream] Init error: {e}")
        ws_state["running"] = False

def ws_subscribe(tokens):
    """Public: subscribe tokens. Queues if WS not yet connected."""
    if not tokens: return
    new_tokens = set(str(t) for t in tokens) - ws_state["subscribed"]
    if not new_tokens: return
    if ws_state["connected"] and _sws_instance:
        _do_subscribe(new_tokens)
    else:
        ws_state["pending_sub"].update(new_tokens)
        if not ws_state["running"]:
            threading.Thread(target=_start_smartstream, daemon=True).start()

def ws_unsubscribe(tokens):
    global _sws_instance
    tokens = set(str(t) for t in tokens) & ws_state["subscribed"]
    if not tokens or not _sws_instance: return
    try:
        nfo = [t for t in tokens if not t.startswith("999")]
        nse = [t for t in tokens if t.startswith("999")]
        corr = f"unsub_{int(time.time())}"
        token_list = []
        if nfo: token_list.append({"exchangeType": NFO_EXCH, "tokens": list(nfo)})
        if nse: token_list.append({"exchangeType": NSE_EXCH, "tokens": list(nse)})
        if token_list:
            _sws_instance.unsubscribe(corr, WS_MODE, token_list)
        ws_state["subscribed"] -= tokens
    except Exception as e:
        print(f"[SmartStream] Unsubscribe error: {e}")

# ═══════════════════════════════════════════════════════════════════════════════
# OPTION CHAIN FETCH  (uses ws_state.ltp_cache for LTPs when available)
# ═══════════════════════════════════════════════════════════════════════════════

def calc_bs_delta(S, K, T, sigma, opt_type, r=0.065):
    """Black-Scholes delta — fallback when API returns no Greeks"""
    try:
        if T <= 0 or sigma <= 0 or S <= 0 or K <= 0: return 0
        import math
        d1 = (math.log(S/K) + (r + 0.5*sigma**2)*T) / (sigma*math.sqrt(T))
        N  = lambda x: 0.5*(1+math.erf(x/math.sqrt(2)))
        return round(N(d1) if opt_type=="CE" else N(d1)-1, 4)
    except: return 0

def calc_bs_gamma(S, K, T, sigma, r=0.065):
    try:
        import math
        if T <= 0 or sigma <= 0: return 0
        d1  = (math.log(S/K) + (r + 0.5*sigma**2)*T) / (sigma*math.sqrt(T))
        phi = math.exp(-0.5*d1**2) / math.sqrt(2*math.pi)
        return round(phi / (S * sigma * math.sqrt(T)), 6)
    except: return 0

def calc_bs_theta(S, K, T, sigma, opt_type, r=0.065):
    try:
        import math
        if T <= 0 or sigma <= 0: return 0
        d1  = (math.log(S/K) + (r + 0.5*sigma**2)*T) / (sigma*math.sqrt(T))
        d2  = d1 - sigma*math.sqrt(T)
        N   = lambda x: 0.5*(1+math.erf(x/math.sqrt(2)))
        phi = math.exp(-0.5*d1**2) / math.sqrt(2*math.pi)
        if opt_type == "CE":
            theta = (-S*phi*sigma/(2*math.sqrt(T)) - r*K*math.exp(-r*T)*N(d2)) / 365
        else:
            theta = (-S*phi*sigma/(2*math.sqrt(T)) + r*K*math.exp(-r*T)*N(-d2)) / 365
        return round(theta, 2)
    except: return 0

def calc_bs_vega(S, K, T, sigma, r=0.065):
    try:
        import math
        if T <= 0 or sigma <= 0: return 0
        d1  = (math.log(S/K) + (r + 0.5*sigma**2)*T) / (sigma*math.sqrt(T))
        phi = math.exp(-0.5*d1**2) / math.sqrt(2*math.pi)
        return round(S * phi * math.sqrt(T) / 100, 2)
    except: return 0

def fetch_option_greeks_fast(symbol, expiry):
    """Fetch Greeks from Angel One OptionGreeks API.
    Correct endpoint: /marketData/v1/optionGreek (no 's', marketData not derivatives)
    Known issue: Angel One returns monthly expiry data for weekly expiry requests.
    Fallback: calculate all greeks via Black-Scholes when API returns nothing.
    """
    greeks_map = {}
    try:
        # Correct endpoint — marketData not derivatives, no trailing 's'
        data = angel_post("/rest/secure/angelbroking/marketData/v1/optionGreek",
            {"name": symbol, "expirydate": expiry})

        print(f"  Greeks API → status={data.get('status')} items={len(data.get('data') or [])}")

        if data.get("status") and data.get("data"):
            for item in data["data"]:
                strike = int(float(item.get("strikePrice", 0)))
                otype  = item.get("optionType","").upper().strip()[:2]
                if not strike or not otype: continue
                key = f"{strike}_{otype}"
                greeks_map[key] = {
                    "iv":    round(float(item.get("impliedVolatility", 0) or 0), 2),
                    "delta": round(float(item.get("delta",  0) or 0), 4),
                    "gamma": round(float(item.get("gamma",  0) or 0), 6),
                    "theta": round(float(item.get("theta",  0) or 0), 2),
                    "vega":  round(float(item.get("vega",   0) or 0), 2),
                }
            print(f"  Greeks loaded: {len(greeks_map)} strikes from API")
        else:
            print(f"  Greeks API returned no data (weekly expiry issue?) — will use BS fallback")
    except Exception as e:
        print(f"  Greeks fetch error: {e} — will use BS fallback")

    return greeks_map


def fill_bs_greeks(greeks_map, sel_strikes, spot, expiry, chain_quotes, strikes_map):
    """Fill in missing greeks using Black-Scholes for all strikes not covered by API.
    Called after fetch_option_greeks_fast — fills gaps especially for weekly expiries.
    """
    T   = expiry_to_years(expiry)
    if T <= 0: return greeks_map

    filled = 0
    for strike in sel_strikes:
        d     = strikes_map.get(strike, {})
        ce_q  = chain_quotes.get(d.get("CE",""), {})
        pe_q  = chain_quotes.get(d.get("PE",""), {})
        ce_ltp = ce_q.get("ltp", 0)
        pe_ltp = pe_q.get("ltp", 0)

        for otype, ltp in [("CE", ce_ltp), ("PE", pe_ltp)]:
            key = f"{strike}_{otype}"
            existing = greeks_map.get(key, {})

            # Only fill if delta is 0 / missing
            if existing.get("delta", 0) != 0:
                continue

            # Get or calculate IV first
            iv_pct = existing.get("iv", 0)
            if not iv_pct and ltp > 0:
                iv_pct = calc_iv(ltp, spot, strike, T, otype)

            if iv_pct <= 0: continue
            sigma = iv_pct / 100.0

            greeks_map[key] = {
                "iv":    iv_pct,
                "delta": calc_bs_delta(spot, strike, T, sigma, otype),
                "gamma": calc_bs_gamma(spot, strike, T, sigma),
                "theta": calc_bs_theta(spot, strike, T, sigma, otype),
                "vega":  calc_bs_vega( spot, strike, T, sigma),
            }
            filled += 1

    if filled > 0:
        print(f"  BS fallback filled {filled} strikes with calculated greeks")
    return greeks_map

def fetch_option_chain(symbol, expiry):
    expiry_map = store["instruments"].get(symbol, {})
    if expiry not in expiry_map:
        raise Exception(f"No instruments for {symbol} {expiry}")

    strikes_map = expiry_map[expiry]
    all_strikes = sorted(strikes_map.keys())
    spot        = fetch_spot(symbol)
    atm         = min(all_strikes, key=lambda s: abs(s-spot))
    atm_idx     = all_strikes.index(atm)
    lo          = max(0, atm_idx - 25)
    hi          = min(len(all_strikes), atm_idx + 26)
    sel_strikes = all_strikes[lo:hi]

    # Collect tokens for REST quote (still needed for OI and non-cached prices)
    nfo_tokens = []
    for strike in sel_strikes:
        d = strikes_map[strike]
        if "CE" in d: nfo_tokens.append(d["CE"])
        if "PE" in d: nfo_tokens.append(d["PE"])

    # Subscribe these tokens to WebSocket for future real-time updates
    ws_subscribe(nfo_tokens)

    print(f"Fetching {len(nfo_tokens)} option tokens for {symbol} {expiry} (spot={spot})...")

    all_fetched = []
    for i in range(0, len(nfo_tokens), 50):
        batch = nfo_tokens[i:i+50]
        data  = angel_post("/rest/secure/angelbroking/market/v1/quote/",
                           {"mode":"FULL","exchangeTokens":{"NFO":batch}})
        if data.get("status") and data.get("data",{}).get("fetched"):
            all_fetched.extend(data["data"]["fetched"])
        time.sleep(0.25)

    quote_map = {q["symbolToken"]: q for q in all_fetched}

    T           = expiry_to_years(expiry)
    atm_strike  = min(sel_strikes, key=lambda s: abs(s-spot))
    atm_idx_sel = sel_strikes.index(atm_strike) if atm_strike in sel_strikes else len(sel_strikes)//2
    IV_RANGE    = 15

    greeks_map = {}
    try:
        greeks_map = fetch_option_greeks_fast(symbol, expiry)
        # Fill missing greeks via Black-Scholes (handles weekly expiry gap)
        greeks_map = fill_bs_greeks(greeks_map, sel_strikes, spot, expiry, quote_map, strikes_map)
    except Exception as e:
        print(f"Greeks error: {e}")
        import traceback; traceback.print_exc()

    chain = []
    for i, strike in enumerate(sel_strikes):
        d    = strikes_map[strike]
        ce_q = quote_map.get(d.get("CE",""), {})
        pe_q = quote_map.get(d.get("PE",""), {})
        if not ce_q and not pe_q: continue
        # After hours both ltps may be 0 — still show row if OI data is present
        if not ce_q.get("opnInterest") and not pe_q.get("opnInterest") and not ce_q.get("close") and not pe_q.get("close"): continue

        # Use WebSocket LTP if fresher than REST quote
        ce_tok = d.get("CE","")
        pe_tok = d.get("PE","")
        ce_ws  = ws_state["ltp_cache"].get(ce_tok, {})
        pe_ws  = ws_state["ltp_cache"].get(pe_tok, {})

        # After market hours ltp=0 — fall back to close (= last traded price)
        ce_ltp = ce_ws.get("ltp") or ce_q.get("ltp", 0) or ce_q.get("close", 0)
        pe_ltp = pe_ws.get("ltp") or pe_q.get("ltp", 0) or pe_q.get("close", 0)
        ce_oi  = ce_ws.get("oi")  or ce_q.get("opnInterest", 0)
        pe_oi  = pe_ws.get("oi")  or pe_q.get("opnInterest", 0)

        near_atm = abs(i - atm_idx_sel) <= IV_RANGE
        ceg = greeks_map.get(f"{strike}_CE", {})
        peg = greeks_map.get(f"{strike}_PE", {})

        ce_iv    = ceg.get("iv",0)    or (calc_iv(ce_ltp, spot, strike, T, "CE") if ce_ltp>0 and T>0 and near_atm else 0)
        pe_iv    = peg.get("iv",0)    or (calc_iv(pe_ltp, spot, strike, T, "PE") if pe_ltp>0 and T>0 and near_atm else 0)

        ce_key = f"{symbol}_{expiry}_{strike}_CE"
        pe_key = f"{symbol}_{expiry}_{strike}_PE"

        chain.append({
            "strike": strike,
            "call": {
                "ltp":      ce_ltp,
                "open":     ce_q.get("open", 0),
                "high":     ce_ws.get("high") or ce_q.get("high", 0),
                "low":      ce_ws.get("low")  or ce_q.get("low",  0),
                "prevClose":ce_q.get("close", 0),
                "close":    ce_q.get("close", 0),   # explicit close for after-hours
                "oi":       ce_oi,
                "oiChg":    get_oi_change(ce_key, ce_oi),
                "iv":       ce_iv,
                "delta":    ceg.get("delta", 0),
                "gamma":    ceg.get("gamma", 0),
                "theta":    ceg.get("theta", 0),
                "vega":     ceg.get("vega",  0),
                "vol":      ce_ws.get("volume") or ce_q.get("tradeVolume", 0),
                "chgPct":   ce_ws.get("chgPct") or ce_q.get("percentChange", 0),
                "token":    ce_tok,
            },
            "put": {
                "ltp":      pe_ltp,
                "open":     pe_q.get("open", 0),
                "high":     pe_ws.get("high") or pe_q.get("high", 0),
                "low":      pe_ws.get("low")  or pe_q.get("low",  0),
                "prevClose":pe_q.get("close", 0),
                "close":    pe_q.get("close", 0),   # explicit close for after-hours
                "oi":       pe_oi,
                "oiChg":    get_oi_change(pe_key, pe_oi),
                "iv":       pe_iv,
                "delta":    peg.get("delta", 0),
                "gamma":    peg.get("gamma", 0),
                "theta":    peg.get("theta", 0),
                "vega":     peg.get("vega",  0),
                "vol":      pe_ws.get("volume") or pe_q.get("tradeVolume", 0),
                "chgPct":   pe_ws.get("chgPct") or pe_q.get("percentChange", 0),
                "token":    pe_tok,
            },
        })

    return {"spot": spot, "chain": chain, "expiries": sorted(expiry_map.keys())}

# ═══════════════════════════════════════════════════════════════════════════════
# FLASK ROUTES  (identical interface to original proxy)
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/connect", methods=["POST","OPTIONS"])
def connect():
    if request.method == "OPTIONS": return _cors_ok()
    body    = request.get_json(silent=True) or {}
    api_key = body.get("apiKey","").strip()
    jwt     = body.get("jwt","").strip()
    if not api_key or not jwt:
        return _json({"status":"error","message":"Missing apiKey or jwt"},400)
    try:
        data = angel_call("/rest/secure/angelbroking/user/v1/getProfile",
                          method="GET", api_key=api_key, jwt=jwt)
        if not data.get("status"):
            return _json({"status":"error","message":data.get("message","Auth failed")},401)
    except Exception as e:
        return _json({"status":"error","message":str(e)},500)

    store["api_key"]     = api_key
    store["jwt"]         = jwt
    store["client_code"] = data.get("data",{}).get("clientcode","")
    store["status"]      = "connected"

    # Fetch feedToken — required for SmartStream WebSocket
    try:
        feed_data = angel_call(
            "/rest/secure/angelbroking/user/v1/getProfile",
            method="GET", api_key=api_key, jwt=jwt
        )
        # feedToken comes from generateSession — try to get it from a separate call
        feed_resp = angel_post(
            "/rest/auth/angelbroking/user/v1/loginByPassword",
            {},  # empty — just trying to get feedToken from profile
            api_key=api_key, jwt=jwt
        )
        # Most reliable: call the feedToken endpoint directly
    except: pass

    # feedToken fetch — dedicated endpoint
    try:
        ft_resp = angel_call(
            "/rest/secure/angelbroking/user/v1/getProfile",
            method="GET", api_key=api_key, jwt=jwt
        )
        # Try to extract feedToken from body if present
        ft = body.get("feedToken","").strip()
        if ft:
            store["feed_token"] = ft
            print(f"[feedToken] From request body: {ft[:8]}...")
        else:
            print("[feedToken] Not in request body. Add 'feedToken' field to /connect POST body.")
            store["feed_token"] = ""
    except Exception as e:
        print(f"[feedToken] Error: {e}")
        store["feed_token"] = ""

    if not store["instruments"]:
        threading.Thread(target=load_all_instruments, daemon=True).start()

    # Start SmartStream WebSocket (only if feed_token available)
    if store.get("feed_token") and not ws_state["running"]:
        threading.Thread(target=_start_smartstream, daemon=True).start()
    elif not store.get("feed_token"):
        print("[SmartStream] No feedToken — WebSocket disabled. Add feedToken to connect request.")

    name = data.get("data",{}).get("name","")
    return _json({
        "status":    "ok",
        "message":   f"Connected as {name}",
        "ws_ready":  bool(store.get("feed_token")),
        "feed_token_hint": "Pass feedToken in POST body to enable SmartStream"
                           if not store.get("feed_token") else "SmartStream starting",
    })

@app.route("/option-chain/<symbol>/<expiry>", methods=["GET","OPTIONS"])
def option_chain(symbol, expiry):
    if request.method == "OPTIONS": return _cors_ok()
    if not store["api_key"]:
        return _json({"status":"error","message":"Not connected"},401)
    if not store["instruments_loaded"]:
        return _json({"status":"loading","message":"Loading instruments..."},202)
    try:
        result = fetch_option_chain(symbol, expiry)
        return _json({"status":"ok","data":result})
    except Exception as e:
        return _json({"status":"error","message":str(e)},500)

@app.route("/expiries/<symbol>", methods=["GET","OPTIONS"])
def expiries(symbol):
    if request.method == "OPTIONS": return _cors_ok()
    exp = sorted(store["instruments"].get(symbol,{}).keys())
    return _json({"status":"ok","expiries":exp})

@app.route("/indices", methods=["GET","OPTIONS"])
def indices():
    if request.method == "OPTIONS": return _cors_ok()
    if not store["api_key"]:
        return _json({"status":"error","message":"Not connected"},401)
    try:
        idx_tokens = {"NIFTY":"99926000","BANKNIFTY":"99926009","FINNIFTY":"99926037"}
        # Also subscribe index tokens to WS
        ws_subscribe(list(idx_tokens.values()))
        # Try WS cache first
        result = {}
        for sym, tok in idx_tokens.items():
            cached = ws_state["ltp_cache"].get(tok)
            if cached:
                _ltp  = cached["ltp"]
                _cl   = cached.get("close",0)
                _chg  = round(_ltp - _cl, 2) if _cl else 0
                _chgp = round((_chg/_cl)*100, 2) if _cl else 0
                result[sym] = {"ltp":_ltp,"chg":_chg,"chgPct":_chgp,
                               "open":cached.get("open",0),"high":cached.get("high",0),
                               "low":cached.get("low",0),"close":_cl}

        # REST fallback for missing
        missing_tokens = [t for sym,t in idx_tokens.items() if sym not in result]
        if missing_tokens:
            data = angel_post("/rest/secure/angelbroking/market/v1/quote/",
                              {"mode":"FULL","exchangeTokens":{"NSE":list(idx_tokens.values())}})
            if data.get("status") and data.get("data",{}).get("fetched"):
                tok_rev = {v:k for k,v in idx_tokens.items()}
                for q in data["data"]["fetched"]:
                    sym = tok_rev.get(q.get("symbolToken",""),"")
                    if sym and sym not in result:
                        result[sym] = {"ltp":q.get("ltp",0),"chg":q.get("netChange",0),
                                       "chgPct":q.get("percentChange",0),"open":q.get("open",0),
                                       "high":q.get("high",0),"low":q.get("low",0),"close":q.get("close",0)}

        # VIX
        vix_info = {}
        vix_cached = ws_state["ltp_cache"].get("99926017")
        if vix_cached:
            _vltp = vix_cached["ltp"]
            _vcl  = vix_cached.get("close",0)
            _vchg = round(_vltp - _vcl, 2) if _vcl else 0
            _vchgp= round((_vchg/_vcl)*100,2) if _vcl else 0
            vix_info = {"ltp":_vltp,"chg":_vchg,"chgPct":_vchgp}
        else:
            ws_subscribe(["99926017"])
            vix_data = angel_post("/rest/secure/angelbroking/market/v1/quote/",
                                  {"mode":"FULL","exchangeTokens":{"NSE":["99926017"]}})
            if vix_data.get("status") and vix_data.get("data",{}).get("fetched"):
                q = vix_data["data"]["fetched"][0]
                vix_info = {"ltp":round(q.get("ltp",0),2),"chg":round(q.get("netChange",0),2),
                            "chgPct":round(q.get("percentChange",0),2)}

        print(f"Indices: {list(result.keys())} | VIX={vix_info.get('ltp',0)}")
        return _json({"status":"ok","data":result,"vix":vix_info,"advDec":{}})
    except Exception as e:
        return _json({"status":"error","message":str(e)},500)

@app.route("/ws-status", methods=["GET"])
def ws_status_route():
    return _json({
        "smartstream_connected": ws_state["connected"],
        "subscribed_tokens":     len(ws_state["subscribed"]),
        "cached_ticks":          len(ws_state["ltp_cache"]),
        "pending_sub":           len(ws_state["pending_sub"]),
        "ws_available":          WS_AVAILABLE,
    })

@app.route("/ws-subscribe", methods=["POST","OPTIONS"])
def ws_subscribe_route():
    """Manually subscribe tokens — used by browser to subscribe extra strikes"""
    if request.method == "OPTIONS": return _cors_ok()
    body   = request.get_json(silent=True) or {}
    tokens = body.get("tokens",[])
    ws_subscribe(tokens)
    return _json({"status":"ok","subscribed":len(tokens)})

def _json(obj, code=200):
    r = Response(json.dumps(obj), status=code, content_type="application/json")
    r.headers["Access-Control-Allow-Origin"] = "*"
    return r

def _cors_ok():
    r = Response(status=200)
    r.headers["Access-Control-Allow-Origin"]  = "*"
    r.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    r.headers["Access-Control-Allow-Headers"] = "*"
    return r

# ═══════════════════════════════════════════════════════════════════════════════
# SOCKET.IO — browser ↔ proxy real-time channel
# ═══════════════════════════════════════════════════════════════════════════════

@sio.on("connect")
def on_browser_connect():
    print(f"[SocketIO] Browser connected: {request.sid}")
    emit("ws_status", {
        "connected": ws_state["connected"],
        "subscribed": len(ws_state["subscribed"]),
        "msg": "Proxy connected. SmartStream: " + ("live" if ws_state["connected"] else "connecting..."),
    })

@sio.on("disconnect")
def on_browser_disconnect():
    print(f"[SocketIO] Browser disconnected: {request.sid}")

@sio.on("subscribe")
def on_browser_subscribe(data):
    """Browser sends list of tokens to subscribe"""
    tokens = data.get("tokens",[])
    ws_subscribe(tokens)
    emit("subscribed", {"tokens": tokens, "total": len(ws_state["subscribed"])})

@sio.on("unsubscribe")
def on_browser_unsubscribe(data):
    tokens = data.get("tokens",[])
    ws_unsubscribe(tokens)

@sio.on("get_tick")
def on_get_tick(data):
    """Browser requests cached tick for a specific token"""
    token = str(data.get("token",""))
    tick  = ws_state["ltp_cache"].get(token)
    emit("tick_snapshot", {"token": token, "tick": tick})

# ── Heartbeat: push all cached ticks to browser every 1s ──────────────────────
def tick_broadcast_loop():
    """Push all cached ticks to all browsers every second"""
    while True:
        time.sleep(1)
        if ws_state["ltp_cache"] and sio:
            try:
                sio.emit("tick_batch", ws_state["ltp_cache"])
            except: pass

# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════


# ═══════════════════════════════════════════════════════════════════════════════
# MCX COMMODITY ROUTES
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/mcx/symbols", methods=["GET"])
def mcx_symbols():
    mcx = _get_mcx()
    if not mcx: return _json({"status":"error","message":"MCX module not loaded"})
    loaded = store.get("mcx_instruments",{})
    result = {}
    for sym, spec in mcx.MCX_SYMBOLS.items():
        exp_map = loaded.get(sym,{})
        exps    = mcx.get_mcx_expiries(loaded, sym) if exp_map else []
        result[sym] = {
            "description": spec["description"],
            "lot_size":    spec["lot_size"],
            "strike_step": spec["strike_step"],
            "expiries":    exps,
            "loaded":      bool(exp_map),
        }
    return _json({"status":"ok","symbols":result,"mcx_loaded":store.get("mcx_loaded",False)})

@app.route("/mcx/expiries/<symbol>", methods=["GET","OPTIONS"])
def mcx_expiries(symbol):
    if request.method == "OPTIONS": return _cors_ok()
    mcx = _get_mcx()
    if not mcx: return _json({"status":"error","message":"MCX module not loaded"})
    mcx_map = store.get("mcx_instruments",{})
    exps    = mcx.get_mcx_expiries(mcx_map, symbol)
    return _json({"status":"ok","symbol":symbol,"expiries":exps})

@app.route("/mcx/chain/<symbol>/<expiry>", methods=["GET","OPTIONS"])
def mcx_chain(symbol, expiry):
    if request.method == "OPTIONS": return _cors_ok()
    if not store["api_key"]: return _json({"status":"error","message":"Not connected"},401)
    if not store.get("mcx_loaded"):
        return _json({"status":"loading","message":"MCX instruments loading…"},202)
    mcx = _get_mcx()
    if not mcx: return _json({"status":"error","message":"MCX module not loaded"})
    try:
        result = mcx.fetch_mcx_chain(
            symbol, expiry,
            store["mcx_instruments"],
            store["mcx_fut_tokens"],
            ws_state["ltp_cache"],
            angel_post,
        )
        # Subscribe MCX tokens to WS
        tokens = []
        for row in result.get("chain",[]):
            if row.get("call",{}).get("token"): tokens.append(row["call"]["token"])
            if row.get("put",{}).get("token"):  tokens.append(row["put"]["token"])
        if tokens: ws_subscribe(tokens)
        return _json({"status":"ok","data":result})
    except Exception as e:
        import traceback; traceback.print_exc()
        return _json({"status":"error","message":str(e)},500)

@app.route("/mcx/spot/<symbol>", methods=["GET"])
def mcx_spot(symbol):
    mcx = _get_mcx()
    if not mcx: return _json({"status":"error","message":"MCX module not loaded"})
    spot = mcx.get_mcx_spot(symbol, store["mcx_fut_tokens"], ws_state["ltp_cache"], angel_post)
    return _json({"status":"ok","symbol":symbol,"spot":spot})

@app.route("/status", methods=["GET"])
def status():
    return _json({
        "status":            store["status"],
        "connected":         bool(store["api_key"]),
        "symbols":           list(store["instruments"].keys()),
        "fno_count":         len(FNO_STOCKS),
        "instruments_loaded":store["instruments_loaded"],
        "mcx_loaded":        store.get("mcx_loaded", False),
        "mcx_symbols":       list(store.get("mcx_instruments",{}).keys()),
        "ws_connected":      ws_state["connected"],
        "ws_subscribed":     len(ws_state["subscribed"]),
        "ws_ticks_cached":   len(ws_state["ltp_cache"]),
        "error":             store["error"],
    })


@app.route("/strategies/status", methods=["GET"])
def strategies_status():
    ag = _get_agent()
    if not ag: return _json({"error":"Agent not loaded"})
    se = ag._get_strat_engine()
    if not se: return _json({"error":"Strategy engine not loaded"})
    return _json(se.get_status())

@app.route("/strategies/positions", methods=["GET"])
def strategies_positions():
    ag = _get_agent()
    if not ag: return _json({})
    se = ag._get_strat_engine()
    if not se: return _json({})
    return _json(se.get_active_positions())

@app.route("/strategies/close-all", methods=["POST","OPTIONS"])
def strategies_close_all():
    if request.method == "OPTIONS": return _cors_ok()
    ag = _get_agent()
    if not ag: return _json({"status":"error","message":"Agent not loaded"})
    se = ag._get_strat_engine()
    if se: se.force_close_all("dashboard_request")
    return _json({"status":"ok"})

# Keep M130 route for backward compatibility
@app.route("/m130/status", methods=["GET"])
def m130_status():
    ag = _get_agent()
    if not ag: return _json({"active":False})
    se = ag._get_strat_engine()
    if not se: return _json({"active":False})
    m130 = se.strategies.get("M130")
    if not m130: return _json({"active":False})
    return _json(m130.get_status())

@app.route("/m130/levels", methods=["GET"])
def m130_levels():
    ag = _get_agent()
    if not ag: return _json({})
    se = ag._get_strat_engine()
    if not se: return _json({})
    m130 = se.strategies.get("M130")
    if not m130: return _json({})
    return _json({"buy_level": m130.buy_level, "sell_level": m130.sell_level,
                  "position": m130.position, "entry_price": m130.entry_price,
                  "pnl": m130.pnl, "sl_hit": m130.sl_hit,
                  "re_entry_used": m130.re_entry_used,
                  "prev_high": getattr(m130,'prev_high',0),
                  "prev_low":  getattr(m130,'prev_low',0),
                  "scenario":  getattr(m130,'scenario',None),
                  "orb_high":  getattr(m130,'orb_high',0),
                  "orb_low":   getattr(m130,'orb_low',0)})


@app.route("/agent/reset-rules", methods=["POST","OPTIONS"])
def agent_reset_rules():
    """Force-update rules to correct paper trading values."""
    if request.method == "OPTIONS": return _cors_ok()
    ag = _get_agent()
    if not ag: return _json({"status":"error","message":"Agent not loaded"})

    patch = {
        # NSE thresholds — tuned for current market
        "pcr_bull":       1.10,
        "pcr_bear":       0.90,
        "vix_high":       15.0,
        "vix_low":        14.0,
        "ivr_sell_min":   40,
        "ivr_buy_max":    60,
        "min_oi":         10000,
        "min_volume":     100,
        "min_iv":         5.0,
        "max_iv":         80.0,
        "min_premium":    5.0,
        # Risk — paper mode generous
        "max_open_trades":  8,
        "max_daily_trades": 30,
        "max_daily_loss":   999999,
        "max_risk_pct":     1.5,
        "capital":          100000,
        # Exit rules
        "profit_target_pct": 50.0,
        "stop_loss_pct":     50.0,
        "sell_target_pct":   40.0,
        "sell_sl_pct":       80.0,
        "max_hold_minutes":  180,
        "delta_hedge_threshold": 8.0,
    }
    rules = ag.update_rules(patch)
    ag._log("Rules reset to paper trading defaults", "CONFIG")
    return _json({"status":"ok","message":"Rules updated","rules":rules})

# ═══════════════════════════════════════════════════════════════════════════════
# AUTONOMOUS TRADING AGENT — plug-in
# ═══════════════════════════════════════════════════════════════════════════════
_agent = None

def _get_agent():
    global _agent
    if _agent is None:
        try:
            from agent_engine import AgentEngine
            _agent = AgentEngine(
                proxy_store   = store,
                proxy_sio     = sio,
                ws_state_ref  = ws_state,
                angel_post_fn = angel_post,
                fetch_chain_fn= fetch_option_chain,
                fetch_spot_fn = fetch_spot,
            )
            print("[Agent] AgentEngine loaded OK")
        except Exception as e:
            print(f"[Agent] Load error: {e}")
    return _agent

@app.route("/agent/start", methods=["POST","OPTIONS"])
def agent_start():
    if request.method == "OPTIONS": return _cors_ok()
    if not store["api_key"]: return _json({"status":"error","message":"Not connected"},401)
    body = request.get_json(silent=True) or {}
    mode = body.get("mode","PAPER").upper()
    ag   = _get_agent()
    if not ag: return _json({"status":"error","message":"Agent engine not available"},500)
    ag.start(mode)
    return _json({"status":"ok","message":f"Agent started in {mode} mode"})

@app.route("/agent/stop", methods=["POST","OPTIONS"])
def agent_stop():
    if request.method == "OPTIONS": return _cors_ok()
    ag = _get_agent()
    if ag: ag.stop()
    return _json({"status":"ok"})

@app.route("/agent/pause", methods=["POST","OPTIONS"])
def agent_pause():
    if request.method == "OPTIONS": return _cors_ok()
    ag = _get_agent()
    if ag: ag.pause()
    return _json({"status":"ok","paused": ag.paused if ag else False})

@app.route("/agent/state", methods=["GET"])
def agent_state():
    ag = _get_agent()
    if not ag: return _json({"running":False,"message":"Agent not loaded"})
    return _json(ag.get_state())

@app.route("/agent/rules", methods=["GET","POST","OPTIONS"])
def agent_rules():
    if request.method == "OPTIONS": return _cors_ok()
    ag = _get_agent()
    if not ag: return _json({})
    if request.method == "POST":
        patch = request.get_json(silent=True) or {}
        rules = ag.update_rules(patch)
        return _json({"status":"ok","rules":rules})
    return _json(ag.rules)

@app.route("/agent/close-all", methods=["POST","OPTIONS"])
def agent_close_all():
    if request.method == "OPTIONS": return _cors_ok()
    ag = _get_agent()
    if ag: ag._force_close_all("manual_close_all")
    return _json({"status":"ok"})

@sio.on("agent_control")
def on_agent_control(data):
    ag = _get_agent()
    if not ag: return
    cmd = data.get("cmd","")
    if cmd == "start":   ag.start(data.get("mode","PAPER"))
    elif cmd == "stop":  ag.stop()
    elif cmd == "pause": ag.pause()
    elif cmd == "close_all": ag._force_close_all("dashboard_request")
    elif cmd == "evolve": ag._evolve()
    elif cmd == "reset_daily": ag.reset_daily()

@app.route("/login", methods=["POST", "OPTIONS"])
def proxy_login_oc():
    """Full login for option chain — stores feedToken and starts SmartStream."""
    if request.method == "OPTIONS":
        return _cors_ok()
    body    = request.get_json(silent=True) or {}
    api_key = body.get("apiKey", "").strip()
    code    = body.get("clientcode", "").strip()
    pin     = body.get("password", "").strip()
    totp    = body.get("totp", "").strip()
    if not api_key or not code or not pin or not totp:
        return _json({"status": False, "message": "Missing fields"}, 400)
    # Accept both 32-char secret key OR 6-digit code directly
    if len(totp) not in range(6, 65):
        return _json({"status": False, "message": "TOTP must be 6-digit code or 32-char secret key"}, 400)
    try:
        # Generate live TOTP code from 32-char secret key
        try:
            import pyotp
            totp_code = pyotp.TOTP(totp.upper().replace(" ", "")).now()
            print(f"[/login] TOTP generated from secret: {totp_code}")
        except ImportError:
            print("[/login] pyotp not installed — run: py -3.13 -m pip install pyotp")
            return _json({"status": False, "message": "pyotp not installed. Run: py -3.13 -m pip install pyotp"})
        except Exception as e:
            print(f"[/login] TOTP generation error: {e} — treating as raw 6-digit code")
            totp_code = totp  # fallback: treat as raw code if secret is invalid

        data = angel_call(
            "/rest/auth/angelbroking/user/v1/loginByPassword",
            payload={"clientcode": code, "password": pin, "totp": totp_code},
            method="POST", api_key=api_key, jwt="",
        )
        print(f"[/login] status={data.get('status')} keys={list(data.get('data',{}).keys())}")
        if not data.get("status"):
            return _json({"status": False, "message": data.get("message", "Login failed")})
        d = data["data"]
        store["api_key"]     = api_key
        store["jwt"]         = d["jwtToken"]
        store["client_code"] = code
        # feedToken — Angel One returns it in the login response
        feed_token = d.get("feedToken") or d.get("feed_token") or d.get("refreshToken") or ""
        store["feed_token"] = feed_token
        print(f"[/login] feedToken={'found:'+feed_token[:12] if feed_token else 'NOT IN RESPONSE — keys:'+str(list(d.keys()))}")
        if not store["instruments_loaded"] and not store["instruments_loading"]:
            threading.Thread(target=load_all_instruments, daemon=True).start()
        if feed_token and SDK_AVAILABLE and not ws_state["running"]:
            threading.Thread(target=_start_smartstream, daemon=True).start()
            print("[/login] SmartStream starting...")
        elif not feed_token:
            print("[/login] ⚠️ No feedToken in response — WS disabled, REST polling only")
        return _json({
            "status": True, "message": "Login successful",
            "sdk_available": SDK_AVAILABLE, "feed_token": bool(feed_token),
            "data": {"jwtToken": d["jwtToken"], "name": d.get("name", code), "feedToken": feed_token}
        })
    except Exception as e:
        import traceback; traceback.print_exc()
        return _json({"status": False, "message": str(e)}, 500)


@app.route("/optionchain")
def serve_optionchain():
    return Response(OPTION_CHAIN_HTML, content_type="text/html")


OPTION_CHAIN_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>Option Chain</title>
<link href="https://fonts.googleapis.com/css2?family=Share+Tech+Mono&family=Barlow+Condensed:wght@600;700;800&family=Barlow:wght@400;500;600&display=swap" rel="stylesheet">
<script src="https://cdnjs.cloudflare.com/ajax/libs/socket.io/4.7.5/socket.io.min.js"></script>
<style>
:root{
  --bg:#0b0e13;--panel:#0f1319;--card:#141920;--border:#1e2730;--border2:#263040;
  --up:#00d97e;--down:#ff3d5a;--atm:#f0b429;--ce:#2196f3;--pe:#e91e63;
  --text:#cdd9e8;--muted:#a0b4c8;--head:#cdd9e8;
  --mono:'Share Tech Mono',monospace;--cond:'Barlow Condensed',sans-serif;
}
*{box-sizing:border-box;margin:0;padding:0;}
body{background:var(--bg);color:var(--text);font-family:'Barlow',sans-serif;min-height:100vh;}

.topbar{background:var(--panel);border-bottom:1px solid var(--border);padding:0 14px;
  display:flex;align-items:center;gap:0;height:46px;position:sticky;top:0;z-index:100;}
.logo{font-family:var(--cond);font-size:17px;font-weight:800;letter-spacing:2px;margin-right:20px;}
.logo span{color:var(--ce);}
.sym-tabs{display:flex;gap:0;height:100%;}
.sym-tab{background:transparent;border:none;border-bottom:2px solid transparent;
  color:var(--muted);font-family:var(--cond);font-size:13px;font-weight:700;
  letter-spacing:.8px;padding:0 13px;cursor:pointer;transition:all .15s;white-space:nowrap;}
.sym-tab:hover{color:var(--text);}
.sym-tab.active{color:var(--text);border-bottom-color:var(--ce);}
.tb-right{margin-left:auto;display:flex;align-items:center;gap:16px;}
.spot-val{font-family:var(--mono);font-size:15px;font-weight:700;}
.spot-chg{font-family:var(--mono);font-size:11px;font-weight:600;white-space:nowrap;}
.spot-chg.up{color:var(--up);}
.spot-chg.dn{color:var(--down);}
.spot-chg.fl{color:var(--muted);}

/* Login */
.login-wrap{display:flex;justify-content:center;align-items:center;
  min-height:calc(100vh - 46px);padding:20px;}
.login-box{background:var(--card);border:1px solid var(--border);border-radius:12px;
  padding:26px;width:100%;max-width:420px;}
.login-title{font-family:var(--cond);font-size:17px;font-weight:800;letter-spacing:1px;margin-bottom:4px;}
.login-sub{font-size:11px;color:var(--muted);margin-bottom:18px;line-height:1.7;}
.fgrid{display:grid;grid-template-columns:1fr 1fr;gap:10px;}
.frow{display:flex;flex-direction:column;gap:5px;}
.frow label{font-size:9px;text-transform:uppercase;letter-spacing:1.2px;color:var(--muted);}
.frow input{background:#070b10;border:1px solid var(--border);border-radius:6px;color:var(--text);
  font-family:var(--mono);font-size:12px;padding:8px 10px;outline:none;transition:border-color .2s;}
.frow input:focus{border-color:var(--ce);}
.frow input::placeholder{color:var(--muted);}
.btn-login{width:100%;background:var(--ce);color:#000;border:none;border-radius:7px;
  padding:11px;font-family:var(--cond);font-size:13px;font-weight:800;letter-spacing:1px;
  cursor:pointer;margin-top:8px;transition:opacity .2s;}
.btn-login:hover{opacity:.85;}
.btn-login:disabled{background:var(--border);color:var(--muted);cursor:not-allowed;}
.lmsg{font-size:11px;margin-top:8px;min-height:16px;}

/* Expiry + controls */
.exp-bar{background:var(--panel);border-bottom:1px solid var(--border);
  padding:6px 14px;display:flex;align-items:center;gap:8px;flex-wrap:wrap;}
.exp-label{font-size:9px;text-transform:uppercase;letter-spacing:1.5px;color:var(--muted);margin-right:2px;}
.exp-btn{background:transparent;border:1px solid var(--border2);color:var(--muted);
  font-family:var(--cond);font-size:11px;font-weight:700;padding:3px 10px;border-radius:4px;
  cursor:pointer;transition:all .15s;}
.exp-btn:hover{border-color:var(--ce);color:var(--ce);}
.exp-btn.active{background:var(--ce);color:#000;border-color:var(--ce);}
.ctrl-bar{background:var(--panel);border-bottom:1px solid var(--border);
  padding:5px 14px;display:flex;align-items:center;gap:10px;}
.btn-ref{background:transparent;border:1px solid var(--border2);color:var(--muted);
  font-family:var(--cond);font-size:11px;font-weight:700;padding:3px 12px;border-radius:4px;cursor:pointer;transition:all .15s;}
.btn-ref:hover{border-color:var(--up);color:var(--up);}
.btn-ref.busy{border-color:var(--atm);color:var(--atm);}
.auto-sel{background:#0f1319;border:1px solid var(--border);color:var(--muted);
  font-size:10px;padding:2px 6px;border-radius:4px;font-family:var(--mono);}
.upd-time{font-size:10px;color:var(--muted);margin-left:auto;font-family:var(--mono);}

/* Stats */
.stats-bar{background:var(--card);border-bottom:1px solid var(--border);
  padding:6px 14px;display:flex;gap:20px;flex-wrap:wrap;align-items:center;}
.stat{display:flex;flex-direction:column;gap:1px;}
.stat-l{font-size:9px;text-transform:uppercase;letter-spacing:1px;color:var(--muted);}
.stat-v{font-family:var(--mono);font-size:12px;font-weight:700;}

/* ── Chain layout: CE scrolls left | Strike fixed | PE scrolls right ── */
.chain-outer{display:flex;width:100%;overflow:hidden;position:relative;}

/* CE pane — scrolls horizontally, content right-aligned so LTP stays closest to strike */
.ce-pane{flex:1;overflow-x:auto;overflow-y:hidden;scroll-snap-type:x mandatory;
  scrollbar-width:none;-webkit-overflow-scrolling:touch;}
.ce-pane::-webkit-scrollbar{display:none;}

/* Strike column — fixed center, never scrolls */
.strike-pane{flex:0 0 72px;z-index:10;background:var(--card);
  border-left:1px solid var(--border2);border-right:1px solid var(--border2);}

/* PE pane — scrolls horizontally, content left-aligned so LTP stays closest to strike */
.pe-pane{flex:1;overflow-x:auto;overflow-y:hidden;scroll-snap-type:x mandatory;
  scrollbar-width:none;-webkit-overflow-scrolling:touch;}
.pe-pane::-webkit-scrollbar{display:none;}

/* Sync row heights — each pane has its own table */
table.t-ce, table.t-strike, table.t-pe{
  border-collapse:collapse;font-size:11px;font-family:var(--mono);width:100%;}
table.t-ce{min-width:320px;}   /* wide enough to hold all CE cols */
table.t-pe{min-width:320px;}

/* CE table — columns read R→L (OI far left, LTP rightmost/nearest strike) */
table.t-ce td, table.t-ce th{white-space:nowrap;}
table.t-pe td, table.t-pe th{white-space:nowrap;}

/* Header group row */
.g-ce{background:rgba(33,150,243,.13);color:var(--ce);font-family:var(--cond);
  font-size:11px;font-weight:800;letter-spacing:2px;text-align:center;height:24px;padding:0 8px;}
.g-st{background:var(--card);color:var(--atm);font-family:var(--cond);
  font-size:11px;font-weight:800;letter-spacing:2px;text-align:center;height:24px;}
.g-pe{background:rgba(233,30,99,.13);color:var(--pe);font-family:var(--cond);
  font-size:11px;font-weight:800;letter-spacing:2px;text-align:center;height:24px;padding:0 8px;}

/* Col headers */
.h-ce{background:rgba(33,150,243,.06);padding:5px 7px;text-align:right;font-family:var(--cond);
  font-size:9px;font-weight:700;letter-spacing:.6px;color:var(--head);
  border-bottom:2px solid var(--border2);text-transform:uppercase;}
.h-st{background:var(--card);padding:5px 4px;text-align:center;font-family:var(--cond);
  font-size:9px;font-weight:700;color:var(--head);border-bottom:2px solid var(--border2);text-transform:uppercase;}
.h-pe{background:rgba(233,30,99,.06);padding:5px 7px;text-align:right;font-family:var(--cond);
  font-size:9px;font-weight:700;letter-spacing:.6px;color:var(--head);
  border-bottom:2px solid var(--border2);text-transform:uppercase;}

/* Body cells */
.cc{background:rgba(33,150,243,.03);text-align:right;padding:4px 7px;border-bottom:1px solid var(--border);}
.pc{background:rgba(233,30,99,.03);text-align:right;padding:4px 7px;border-bottom:1px solid var(--border);}
.sc{background:var(--card);text-align:center;padding:4px 6px;border-bottom:1px solid var(--border);
  font-family:var(--cond);font-size:12px;font-weight:800;color:var(--atm);white-space:nowrap;}

/* ATM highlight */
.atm-ce td.cc{background:rgba(240,180,41,.05)!important;border-top:1px solid rgba(240,180,41,.5);border-bottom:1px solid rgba(240,180,41,.5)!important;}
.atm-pe td.pc{background:rgba(240,180,41,.05)!important;border-top:1px solid rgba(240,180,41,.5);border-bottom:1px solid rgba(240,180,41,.5)!important;}
.atm-st td.sc{background:rgba(240,180,41,.16)!important;color:#fff;border-top:1px solid rgba(240,180,41,.5);border-bottom:1px solid rgba(240,180,41,.5)!important;}

.atm-badge{background:var(--atm);color:#000;font-size:7px;font-weight:800;
  padding:1px 4px;border-radius:2px;letter-spacing:1px;margin-left:3px;vertical-align:middle;}

/* Hover rows — sync via JS */
.cc-hover{background:rgba(33,150,243,.10)!important;}
.pc-hover{background:rgba(233,30,99,.10)!important;}
.sc-hover{background:rgba(240,180,41,.14)!important;}

.v-ltp{font-size:12px;font-weight:700;color:var(--text);}
.v-ltp.near-high{color:#00d97e;text-shadow:0 0 8px rgba(0,217,126,.45);}
.v-ltp.near-low {color:#ff3d5a;text-shadow:0 0 8px rgba(255,61,90,.45);}
.v-chg{font-size:10px;}
.v-pct{font-size:9px;padding:1px 4px;border-radius:3px;font-weight:700;}
.v-pct.up{background:rgba(0,217,126,.12);color:var(--up);}
.v-pct.dn{background:rgba(255,61,90,.12);color:var(--down);}
.v-pct.fl{color:var(--muted);}
.v-sm{font-size:10px;color:#9ab0c4;}
.v-hi{color:#6baed6;}.v-lo{color:#c46a7a;}
.oi-wrap{display:flex;align-items:center;gap:4px;justify-content:flex-end;}
.oi-bar{height:3px;border-radius:2px;min-width:1px;max-width:60px;}
.ce-bar{background:var(--ce);}.pe-bar{background:var(--pe);}

.state-msg{text-align:center;padding:60px 20px;font-family:var(--cond);font-size:14px;color:var(--muted);}
.state-msg .ico{font-size:28px;display:block;margin-bottom:10px;}
@keyframes spin{to{transform:rotate(360deg)}}
.spin{display:inline-block;width:12px;height:12px;border:2px solid rgba(33,150,243,.2);
  border-top-color:var(--ce);border-radius:50%;animation:spin .7s linear infinite;vertical-align:middle;margin-right:5px;}

/* WebSocket status dot */
.ws-dot{display:inline-block;width:7px;height:7px;border-radius:50%;margin-right:5px;vertical-align:middle;}
.ws-dot.live{background:var(--up);box-shadow:0 0 6px var(--up);}
.ws-dot.dead{background:var(--down);}
.ws-dot.wait{background:var(--atm);}
/* flash animation on tick update */
@keyframes flash{0%{background:rgba(0,217,126,.18)}100%{background:transparent}}
.tick-flash{animation:flash .4s ease-out;}
@keyframes flashdn{0%{background:rgba(255,61,90,.18)}100%{background:transparent}}
.tick-flash-dn{animation:flashdn .4s ease-out;}

/* ── MARKET BRAIN PANEL ── */
#brainPanel{
  background:#0c0f19;border-bottom:1px solid #1e2730;
  padding:10px 14px;display:none;
}
.brain-toggle{
  background:transparent;border:1px solid #263040;color:#a0b4c8;
  font-family:'Barlow Condensed',sans-serif;font-size:11px;font-weight:700;
  letter-spacing:.8px;padding:3px 12px;border-radius:4px;cursor:pointer;
  transition:all .15s;
}
.brain-toggle:hover{border-color:#2196f3;color:#2196f3;}
.brain-toggle.active{background:rgba(33,150,243,.15);border-color:#2196f3;color:#2196f3;}
.brain-grid{
  display:grid;
  grid-template-columns:220px 1fr 220px;
  gap:12px;margin-top:10px;align-items:start;
}
/* Mood arc */
.brain-mood{display:flex;flex-direction:column;align-items:center;gap:4px;}
.brain-mood-name{
  font-family:'Barlow Condensed',sans-serif;font-size:26px;font-weight:800;
  letter-spacing:2px;text-align:center;transition:color .5s;
}
.brain-mood-sub{font-size:10px;color:#a0b4c8;text-align:center;max-width:200px;line-height:1.5;}

/* Sentence */
.brain-sentence{
  background:#0f1319;border:1px solid #1e2730;border-radius:8px;
  padding:12px 16px;font-size:14px;font-weight:500;line-height:1.6;
  min-height:60px;display:flex;align-items:center;gap:10px;
}
.brain-emoji{font-size:24px;flex-shrink:0;}
.brain-hl{padding:1px 5px;border-radius:3px;font-weight:700;}

/* Right panel: signals */
.brain-signals{display:flex;flex-direction:column;gap:6px;}
.brain-sig{
  background:#0f1319;border-left:3px solid #263040;border-radius:0 6px 6px 0;
  padding:6px 10px;font-size:11px;line-height:1.5;
}
.brain-sig b{display:block;font-size:11px;font-weight:700;margin-bottom:1px;}
.brain-sig span{color:#a0b4c8;font-size:10px;}

/* Mini bars row */
.brain-bars{
  display:grid;grid-template-columns:1fr 1fr 1fr;
  gap:8px;margin-top:8px;
}
.brain-bar-card{
  background:#0f1319;border:1px solid #1e2730;border-radius:6px;
  padding:8px 10px;
}
.brain-bar-label{font-size:9px;text-transform:uppercase;letter-spacing:1px;color:#a0b4c8;margin-bottom:6px;}
.brain-bar-bg{height:6px;background:#1e2730;border-radius:3px;overflow:hidden;margin-bottom:4px;}
.brain-bar-fill{height:100%;border-radius:3px;transition:width .8s cubic-bezier(.34,1.56,.64,1);}
.brain-bar-vals{display:flex;justify-content:space-between;font-size:10px;font-family:'Share Tech Mono',monospace;}

/* Straddle mini chart */
.brain-chart-wrap{margin-top:8px;}
.brain-chart-label{font-size:9px;text-transform:uppercase;letter-spacing:1px;color:#a0b4c8;margin-bottom:4px;}

/* Timeline dots */
.brain-timeline{display:flex;gap:4px;margin-top:8px;flex-wrap:wrap;}
.brain-tl-dot{
  width:28px;height:28px;border-radius:50%;
  display:flex;flex-direction:column;align-items:center;justify-content:center;
  font-size:8px;font-family:'Share Tech Mono',monospace;
  cursor:default;border:2px solid transparent;
  transition:transform .2s;
}
.brain-tl-dot:hover{transform:scale(1.2);}

/* Auto-update badge */
.brain-auto{
  font-size:9px;color:#a0b4c8;font-family:'Share Tech Mono',monospace;
  display:flex;align-items:center;gap:5px;
}
.brain-pulse{width:6px;height:6px;border-radius:50%;background:#00d97e;animation:pulse2 2s infinite;}
@keyframes pulse2{0%,100%{opacity:1}50%{opacity:.3}}

</style>
</head>
<body>

<div class="topbar">
  <div class="logo">OPTION<span>CHAIN</span></div>
  <div class="sym-tabs">
    <button class="sym-tab active" onclick="selSym('NIFTY',this)">NIFTY</button>
    <button class="sym-tab" onclick="selSym('BANKNIFTY',this)">BANKNIFTY</button>
    <button class="sym-tab" onclick="selSym('FINNIFTY',this)">FINNIFTY</button>
    <button class="sym-tab" onclick="selSym('MIDCPNIFTY',this)">MIDCPNIFTY</button>
  </div>
  <!-- spot removed from topbar — shown in stats bar instead -->

  <div class="tb-right">
    <div style="display:flex;align-items:center;font-size:10px;color:var(--muted);font-family:var(--mono);">
      <span class="ws-dot wait" id="wsDot"></span>
      <span id="wsLbl">connecting…</span>
    </div>
  </div>
</div>

<div class="login-wrap" id="loginWrap">
  <div class="login-box">
    <div class="login-title">🔐 Angel One Login</div>
    <div class="login-sub">Same server as angel_proxy_ws — no CORS.<br>Enter your 32-character TOTP secret key — code auto-generated.</div>
    <div class="fgrid">
      <div class="frow"><label>Client Code</label><input id="lCode" placeholder="e.g. M58165803" autocomplete="off"></div>
      <div class="frow"><label>MPIN</label><input id="lPin" type="password" placeholder="4-digit MPIN"></div>
      <div class="frow"><label>API Key</label><input id="lKey" placeholder="SmartAPI key" autocomplete="off"></div>
      <div class="frow"><label>TOTP Secret Key (32-char)</label><input id="lTotp" type="text" placeholder="e.g. JBSWY3DPEHPK3PXP..." maxlength="64" autocomplete="off" style="font-size:10px;letter-spacing:.5px"></div>
    </div>
    <button class="btn-login" id="loginBtn" onclick="doLogin()">&#9654; Connect &amp; Load</button>
    <div class="lmsg" id="lmsg"></div>
  </div>
</div>

<div id="mainArea" style="display:none">
  <div class="exp-bar">
    <span class="exp-label">Expiry</span>
    <div id="expBtns" style="display:flex;gap:6px;flex-wrap:wrap;"></div>
  </div>
  <div class="ctrl-bar">
    <button class="btn-ref" id="refBtn" onclick="loadChain()">⟳ Refresh</button>
    <label style="display:flex;align-items:center;gap:5px;cursor:pointer">
      <input type="checkbox" id="autoChk" onchange="toggleAuto()" style="accent-color:var(--ce);cursor:pointer">
      <span style="font-size:10px;color:var(--muted)">Auto</span>
    </label>
    <select class="auto-sel" id="autoSel">
      <option value="15">15s</option><option value="30" selected>30s</option><option value="60">60s</option>
    </select>
    <button class="brain-toggle" id="brainToggle" onclick="toggleBrain()">🧠 MARKET BRAIN</button>
    <span class="upd-time" id="updTime"></span>
  </div>

  <!-- MARKET BRAIN PANEL -->
  <div id="brainPanel">
    <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px;">
      <div style="display:flex;align-items:center;gap:8px;">
        <span style="font-family:'Barlow Condensed',sans-serif;font-size:12px;font-weight:800;letter-spacing:1.5px;color:#a0b4c8;">MARKET BRAIN</span>
        <div class="brain-auto"><div class="brain-pulse"></div><span id="brainAutoLbl">auto-reading ATM…</span></div>
      </div>
      <div style="font-size:10px;color:#a0b4c8;font-family:'Share Tech Mono',monospace;" id="brainUpdTime">—</div>
    </div>
    <div class="brain-grid">
      <!-- MOOD METER -->
      <div class="brain-mood">
        <svg viewBox="0 0 200 110" style="width:200px;">
          <defs>
            <linearGradient id="bArcGrad" x1="0%" y1="0%" x2="100%" y2="0%">
              <stop offset="0%" stop-color="#ff2244"/>
              <stop offset="25%" stop-color="#ff6b35"/>
              <stop offset="50%" stop-color="#ffd166"/>
              <stop offset="75%" stop-color="#00d97e"/>
              <stop offset="100%" stop-color="#2196f3"/>
            </linearGradient>
          </defs>
          <path d="M 14 98 A 86 86 0 0 1 186 98" fill="none" stroke="#1e2730" stroke-width="12" stroke-linecap="round"/>
          <path d="M 14 98 A 86 86 0 0 1 186 98" fill="none" stroke="url(#bArcGrad)" stroke-width="12" stroke-linecap="round" opacity="0.35"/>
          <g id="bNeedle" transform="rotate(-90, 100, 98)">
            <line x1="100" y1="98" x2="100" y2="24" stroke="#ffffff" stroke-width="2" stroke-linecap="round" opacity="0.85"/>
            <circle cx="100" cy="98" r="5" fill="#ffffff" opacity="0.85"/>
            <circle cx="100" cy="98" r="2.5" fill="#0b0e13"/>
          </g>
          <text x="5"   y="108" fill="#ff2244" font-size="7" font-family="Share Tech Mono">PANIC</text>
          <text x="40"  y="68"  fill="#ff6b35" font-size="7" font-family="Share Tech Mono">FEAR</text>
          <text x="86"  y="52"  fill="#ffd166" font-size="7" font-family="Share Tech Mono">CALM</text>
          <text x="130" y="68"  fill="#00d97e" font-size="7" font-family="Share Tech Mono">GREED</text>
          <text x="162" y="108" fill="#2196f3" font-size="7" font-family="Share Tech Mono">🚀</text>
        </svg>
        <div class="brain-mood-name" id="bMoodName" style="color:#a0b4c8">— — —</div>
        <div class="brain-mood-sub" id="bMoodSub">Chain data will auto-populate this</div>
        <div class="brain-timeline" id="bTimeline"></div>
      </div>

      <!-- CENTER: SENTENCE + BARS -->
      <div>
        <div class="brain-sentence">
          <div class="brain-emoji">🧠</div>
          <div id="bSentence" style="flex:1;">Load the chain to read the market mood automatically.</div>
        </div>
        <div class="brain-bars">
          <div class="brain-bar-card">
            <div class="brain-bar-label">CE vs PE</div>
            <div class="brain-bar-bg"><div class="brain-bar-fill" id="bCeBar" style="width:50%;background:#2196f3;"></div></div>
            <div class="brain-bar-vals">
              <span style="color:#2196f3;" id="bCePct">CE —</span>
              <span style="color:#e91e63;" id="bPePct">PE —</span>
            </div>
          </div>
          <div class="brain-bar-card">
            <div class="brain-bar-label">IV / Fear</div>
            <div class="brain-bar-bg"><div class="brain-bar-fill" id="bIvBar" style="width:30%;background:#ffd166;"></div></div>
            <div class="brain-bar-vals">
              <span id="bIvNow" style="color:#ffd166;">— %</span>
              <span id="bStraddleVal" style="color:#a0b4c8;">₹—</span>
            </div>
          </div>
          <div class="brain-bar-card">
            <div class="brain-bar-label">Expected Range</div>
            <div class="brain-bar-bg"><div class="brain-bar-fill" id="bRangeBar" style="width:40%;background:#a0b4c8;"></div></div>
            <div class="brain-bar-vals">
              <span id="bRangeLow" style="color:#e91e63;">— </span>
              <span id="bRangeHigh" style="color:#00d97e;">—</span>
            </div>
          </div>
        </div>
        <div class="brain-chart-wrap">
          <div class="brain-chart-label">📉 Straddle (CE+PE) today</div>
          <canvas id="bStraddleChart" height="50" style="width:100%;border-radius:4px;"></canvas>
        </div>
      </div>

      <!-- RIGHT: SIGNALS -->
      <div class="brain-signals" id="bSignals">
        <div class="brain-sig" style="border-color:#263040;">
          <b style="color:#a0b4c8;">Waiting for chain data</b>
          <span>Open a chain to auto-read the market</span>
        </div>
      </div>
    </div>
  </div>
  <div class="stats-bar" id="statsBar"></div>
  <div class="chain-wrap" id="chainWrap">
    <div class="state-msg"><span class="ico">📊</span>Select expiry above</div>
  </div>
</div>

<script>
// BASE = '' means same origin — angel_proxy_ws serves this page AND the API routes
const BASE = '';
let S = { sym:'NIFTY', expiry:'', autoTimer:null, spot:0, expiriesLoaded:false, expiriesFor:'', chainLoaded:false };

// ── helpers ───────────────────────────────────────────────────────────────────
const $  = x => document.getElementById(x);
const val = x => $(x).value.trim();
const msg = (m,c) => { $('lmsg').textContent=m; $('lmsg').style.color=c; };

async function apiFetch(url, opts={}) {
  const r   = await fetch(BASE + url, opts);
  const txt = await r.text();
  try { return JSON.parse(txt); }
  catch(e) { throw new Error(`Server error at ${url}: ${txt.slice(0,120)}`); }
}
const apiGet  = url => apiFetch(url);
const apiPost = (url,body,hdrs={}) => apiFetch(url, {
  method:'POST', headers:{'Content-Type':'application/json',...hdrs}, body:JSON.stringify(body)
});

// ── Login — calls /login route we added to angel_proxy_ws ─────────────────────
async function doLogin() {
  const code=val('lCode'), pin=val('lPin'), key=val('lKey'), totp=val('lTotp');
  if (!code||!pin||!key||!totp) { msg('Fill all 4 fields','var(--down)'); return; }

  const btn=$('loginBtn');
  btn.disabled=true; btn.innerHTML='<span class="spin"></span>Connecting…';
  msg('Logging in…','var(--muted)');

  try {
    // POST to /login — the new route in angel_proxy_ws
    const r = await apiPost('/login', { clientcode:code, password:pin, totp, apiKey:key });
    if (!r.status) throw new Error(r.message || 'Login failed');

    // Show WS status
    const sdkOk = r.sdk_available, ftOk = r.feed_token;
    if (sdkOk && ftOk)       msg(`✅ Connected as ${r.data?.name||code} · WS starting`, 'var(--up)');
    else if (!sdkOk)         msg(`✅ Connected · ⚠️ Install smartapi-python for live WS`, 'var(--atm)');
    else if (!ftOk)          msg(`✅ Connected · ⚠️ No feedToken — polling mode`, 'var(--atm)');
    else                     msg(`✅ Connected as ${r.data?.name||code}`, 'var(--up)');

    initSocket();   // connect Socket.IO (works even in polling mode for tick_batch)

    // No auto-polling — WebSocket handles live updates
    // Manual refresh button still available
    setTimeout(() => {
      $('loginWrap').style.display = 'none';
      $('mainArea').style.display  = 'block';
      loadExpiries();
    }, 500);
  } catch(e) {
    msg('❌ ' + e.message, 'var(--down)');
  } finally {
    btn.disabled=false; btn.innerHTML='&#9654; Connect &amp; Load';
  }
}

// ── Symbol ────────────────────────────────────────────────────────────────────
function selSym(sym, el) {
  if (S.sym === sym) return; // same symbol — ignore
  S.sym           = sym;
  S.expiry        = '';
  S.expiriesLoaded= false;
  S.expiriesFor   = '';
  S.chainLoaded   = false;
  document.querySelectorAll('.sym-tab').forEach(t=>t.classList.remove('active'));
  el.classList.add('active');
  loadExpiries();
}

// ── Expiries ──────────────────────────────────────────────────────────────────
async function loadExpiries() {
  // Guard: don't re-enter if expiries already displayed
  if (S.expiriesLoaded && S.sym === S.expiriesFor) return;

  try {
    const st = await apiGet('/status');
    if (!st.instruments_loaded) {
      $('expBtns').innerHTML='<span style="color:var(--atm);font-size:11px"><span class="spin"></span>Loading instruments…</span>';
      // Retry but only if we haven't loaded yet
      if (!S.expiriesLoaded) setTimeout(loadExpiries, 2000);
      return;
    }
    const d   = await apiGet(`/expiries/${S.sym}`);
    // Sort ascending by actual date (format: 20MAY2026)
    const MON={JAN:0,FEB:1,MAR:2,APR:3,MAY:4,JUN:5,JUL:6,AUG:7,SEP:8,OCT:9,NOV:10,DEC:11};
    const toDate = e => { const m=e.match(/^(\d{2})([A-Z]{3})(\d{4})$/); return m?new Date(+m[3],MON[m[2]],+m[1]):new Date(0); };
    const exps = (d.expiries||[]).sort((a,b)=>toDate(a)-toDate(b)).slice(0,15);
    const box  = $('expBtns'); box.innerHTML='';
    S.expiriesLoaded = true;
    S.expiriesFor    = S.sym;
    exps.forEach((e,i)=>{
      const b=document.createElement('button');
      b.className='exp-btn'+(i===0?' active':'');
      b.textContent=fmtExp(e);
      b.onclick=()=>{
        document.querySelectorAll('.exp-btn').forEach(x=>x.classList.remove('active'));
        b.classList.add('active'); S.expiry=e; loadChain();
      };
      box.appendChild(b);
      if(i===0 && !S.chainLoaded){ S.expiry=e; loadChain(); }
    });
  } catch(e){ console.error('Expiries:',e); }
}

// ── Load chain ────────────────────────────────────────────────────────────────
let _chainLoading = false;
async function loadChain() {
  if (!S.expiry) return;
  if (_chainLoading) return;   // prevent concurrent fetches
  _chainLoading = true;
  const wrap=$('chainWrap'), btn=$('refBtn');
  btn.classList.add('busy'); btn.innerHTML='<span class="spin"></span>Loading…';
  // Only show loading spinner on first load — don't clear chain on refresh
  if (!wrap.querySelector('.chain-outer')) {
    wrap.innerHTML='<div class="state-msg"><span class="spin" style="width:22px;height:22px;border-width:3px"></span><br>Fetching chain…</div>';
  }
  try {
    const d = await apiGet(`/option-chain/${S.sym}/${S.expiry}`);
    if (d.status!=='ok') throw new Error(d.message||'Chain fetch failed');
    S.spot        = d.data.spot;
    S.chainLoaded = true;
    renderStats(d.data);
    renderChain(d.data);
    subscribeTokens(d.data.chain);   // subscribe WS ticks
    $('updTime').textContent='Updated '+new Date().toLocaleTimeString('en-IN');
    // Spot is in stats bar — renderStats already set it
    // spotChg will update on first WS tick
    const _sc=$('spotChg'); if(_sc){_sc.textContent='';_sc.className='spot-chg fl';}
  } catch(e){
    wrap.innerHTML=`<div class="state-msg"><span class="ico">⚠️</span>${e.message}</div>`;
  } finally {
    btn.classList.remove('busy'); btn.innerHTML='⟳ Refresh';
    _chainLoading = false;
  }
}

// ── Stats ─────────────────────────────────────────────────────────────────────
function renderStats(data) {
  const ch=data.chain;
  const totCE=ch.reduce((s,r)=>s+(r.call?.oi||0),0);
  const totPE=ch.reduce((s,r)=>s+(r.put?.oi||0),0);
  const pcr=totCE>0?(totPE/totCE).toFixed(2):'—';
  const pcrC=pcr>1.2?'var(--up)':pcr<0.8?'var(--down)':'var(--atm)';
  const atm=ch.reduce((a,b)=>Math.abs(b.strike-S.spot)<Math.abs(a.strike-S.spot)?b:a).strike;
  $('statsBar').innerHTML=`
    <div class="stat" id="statSpot">
      <div class="stat-l">Spot</div>
      <div class="stat-v" id="spotVal">${fmt(S.spot)}</div>
      <div class="spot-chg fl" id="spotChg" style="font-size:10px;font-family:var(--mono);font-weight:600;margin-top:2px;"></div>
    </div>
    <div class="stat"><div class="stat-l">ATM</div><div class="stat-v" style="color:var(--atm)">${atm}</div></div>
    <div class="stat"><div class="stat-l">PCR</div><div class="stat-v" style="color:${pcrC}">${pcr}</div></div>
    <div class="stat"><div class="stat-l">CE OI</div><div class="stat-v" style="color:var(--ce)">${fmtOI(totCE)}</div></div>
    <div class="stat"><div class="stat-l">PE OI</div><div class="stat-v" style="color:var(--pe)">${fmtOI(totPE)}</div></div>
    <div class="stat"><div class="stat-l">Symbol</div><div class="stat-v">${S.sym}</div></div>
    <div class="stat"><div class="stat-l">Expiry</div><div class="stat-v" style="color:var(--muted);font-size:10px">${fmtExp(S.expiry)}</div></div>`;
}

// ── Chain table ───────────────────────────────────────────────────────────────
function renderChain(data) {
  const ch=data.chain, spot=data.spot;
  if(!ch.length){ $('chainWrap').innerHTML='<div class="state-msg"><span class="ico">📭</span>No data</div>'; return; }
  const atmS=ch.reduce((a,b)=>Math.abs(b.strike-spot)<Math.abs(a.strike-spot)?b:a).strike;
  const mxCE=Math.max(...ch.map(r=>r.call?.oi||0),1);
  const mxPE=Math.max(...ch.map(r=>r.put?.oi||0),1);

  // CE cols: OI far left, LTP nearest strike
  const CE_COLS=['OI','P.Close','Low','High','Open','Chg%','Chg','LTP'];
  // PE cols: LTP nearest strike, OI far right
  const PE_COLS=['LTP','Chg','Chg%','Open','High','Low','P.Close','OI'];

  // Build 3 separate tables: CE | Strike | PE
  let ceH=`<table class="t-ce"><thead>
    <tr><th colspan="${CE_COLS.length}" class="g-ce">CALL — CE</th></tr>
    <tr>${CE_COLS.map(c=>`<th class="h-ce">${c}</th>`).join('')}</tr>
  </thead><tbody>`;

  let stH=`<table class="t-strike"><thead>
    <tr><th class="g-st">STK</th></tr>
    <tr><th class="h-st">STRIKE</th></tr>
  </thead><tbody>`;

  let peH=`<table class="t-pe"><thead>
    <tr><th colspan="${PE_COLS.length}" class="g-pe">PUT — PE</th></tr>
    <tr>${PE_COLS.map(c=>`<th class="h-pe">${c}</th>`).join('')}</tr>
  </thead><tbody>`;

  for(const row of ch){
    const{strike,call:c,put:p}=row;
    const isATM=strike===atmS;
    const cbW=Math.round(((c?.oi||0)/mxCE)*48);
    const pbW=Math.round(((p?.oi||0)/mxPE)*48);
    const ac=isATM?'atm-ce':'', ap=isATM?'atm-pe':'', as2=isATM?'atm-st':'';

    ceH+=`<tr class="${ac}" data-strike="${strike}">${ceCells(c,cbW,strike)}</tr>`;
    stH+=`<tr class="${as2}" data-strike="${strike}"><td class="sc">${strike}${isATM?'<span class="atm-badge">ATM</span>':''}</td></tr>`;
    peH+=`<tr class="${ap}" data-strike="${strike}">${peCells(p,pbW,strike)}</tr>`;
  }

  ceH+=`</tbody></table>`;
  stH+=`</tbody></table>`;
  peH+=`</tbody></table>`;

  $('chainWrap').innerHTML=`
    <div class="chain-outer">
      <div class="ce-pane" id="cePane">${ceH}</div>
      <div class="strike-pane">${stH}</div>
      <div class="pe-pane" id="pePane">${peH}</div>
    </div>`;

  // Scroll CE pane to right (so LTP column is visible near strike)
  const cp=$('cePane');
  if(cp) cp.scrollLeft=cp.scrollWidth;

  // Scroll ATM row into view
  requestAnimationFrame(()=>{
    const atmRow=document.querySelector('[data-strike="'+atmS+'"]');
    if(atmRow) atmRow.scrollIntoView({block:'center',behavior:'smooth'});
  });

  // Row hover sync across 3 tables
  document.querySelectorAll('.ce-pane tr[data-strike], .strike-pane tr[data-strike], .pe-pane tr[data-strike]')
    .forEach(tr=>{
      tr.addEventListener('mouseenter',()=>syncHover(tr.dataset.strike, true));
      tr.addEventListener('mouseleave',()=>syncHover(tr.dataset.strike, false));
    });
}

function syncHover(strike, on) {
  document.querySelectorAll(`tr[data-strike="${strike}"]`).forEach(tr=>{
    tr.querySelectorAll('.cc').forEach(td=>td.classList.toggle('cc-hover', on));
    tr.querySelectorAll('.pc').forEach(td=>td.classList.toggle('pc-hover', on));
    tr.querySelectorAll('.sc').forEach(td=>td.classList.toggle('sc-hover', on));
  });
}

// LTP range coloring — top 10% = green, bottom 10% = red, rest = neutral
function ltpRangeClass(ltp, high, low) {
  if (!high || !low || high <= low || !ltp) return '';
  const range = high - low;
  const pos   = (ltp - low) / range; // 0 = at low, 1 = at high
  if (pos >= 0.90) return 'near-high';
  if (pos <= 0.10) return 'near-low';
  return '';
}

function ceCells(q,barW,strike){
  if(!q||!q.ltp) return `<td class="cc">—</td>`.repeat(8);
  const pc=q.prevClose||q.close||0;
  const chg=pc>0?+(q.ltp-pc).toFixed(2):0;
  const pct=pc>0?+((chg/pc)*100).toFixed(2):0;
  const chgC=chg>0?'var(--up)':chg<0?'var(--down)':'var(--muted)';
  const pCls=chg>0?'up':chg<0?'dn':'fl';
  return `
    <td class="cc"><div class="oi-wrap"><div class="oi-bar ce-bar" style="width:${barW}px"></div><span class="ce-oi" style="font-size:10px">${fmtOI(q.oi)}</span></div></td>
    <td class="cc"><span class="v-sm">${fmt(pc)}</span></td>
    <td class="cc"><span class="v-sm v-lo ce-low">${fmt(q.low)}</span></td>
    <td class="cc"><span class="v-sm v-hi ce-high">${fmt(q.high)}</span></td>
    <td class="cc"><span class="v-sm">${fmt(q.open)}</span></td>
    <td class="cc"><span class="v-pct ${pCls} ce-pct">${pct>=0?'+':''}${pct}%</span></td>
    <td class="cc"><span class="v-chg ce-chg" style="color:${chgC}">${chg>=0?'+':''}${chg}</span></td>
    <td class="cc"><span class="v-ltp ce-ltp ${ltpRangeClass(q.ltp,q.high,q.low)}" data-val="${q.ltp}" data-high="${q.high}" data-low="${q.low}">${fmt(q.ltp)}</span></td>`;
}

function peCells(q,barW,strike){
  if(!q||!q.ltp) return `<td class="pc">—</td>`.repeat(8);
  const pc=q.prevClose||q.close||0;
  const chg=pc>0?+(q.ltp-pc).toFixed(2):0;
  const pct=pc>0?+((chg/pc)*100).toFixed(2):0;
  const chgC=chg>0?'var(--up)':chg<0?'var(--down)':'var(--muted)';
  const pCls=chg>0?'up':chg<0?'dn':'fl';
  return `
    <td class="pc"><span class="v-ltp pe-ltp ${ltpRangeClass(q.ltp,q.high,q.low)}" data-val="${q.ltp}" data-high="${q.high}" data-low="${q.low}">${fmt(q.ltp)}</span></td>
    <td class="pc"><span class="v-chg pe-chg" style="color:${chgC}">${chg>=0?'+':''}${chg}</span></td>
    <td class="pc"><span class="v-pct ${pCls} pe-pct">${pct>=0?'+':''}${pct}%</span></td>
    <td class="pc"><span class="v-sm">${fmt(q.open)}</span></td>
    <td class="pc"><span class="v-sm v-hi pe-high">${fmt(q.high)}</span></td>
    <td class="pc"><span class="v-sm v-lo pe-low">${fmt(q.low)}</span></td>
    <td class="pc"><span class="v-sm">${fmt(pc)}</span></td>
    <td class="pc"><div class="oi-wrap"><span class="pe-oi" style="font-size:10px">${fmtOI(q.oi)}</span><div class="oi-bar pe-bar" style="width:${barW}px"></div></div></td>`;
}

// ── Auto refresh ──────────────────────────────────────────────────────────────
function toggleAuto(){
  // Always clear existing timer first
  if(S.autoTimer){ clearInterval(S.autoTimer); S.autoTimer=null; }
  if($('autoChk').checked){
    const secs = parseInt($('autoSel').value) * 1000;
    S.autoTimer = setInterval(loadChain, secs);
    console.log('Auto refresh started:', secs/1000, 's');
  }
}

// When auto-interval dropdown changes, restart timer if auto is on
document.addEventListener('DOMContentLoaded', () => {
  const sel = document.getElementById('autoSel');
  if (sel) sel.addEventListener('change', () => {
    if (document.getElementById('autoChk')?.checked) toggleAuto();
  });
});

// ── WebSocket — live tick updates ────────────────────────────────────────────
// token → { side:'ce'|'pe', strike } — built when chain renders
let tokenMap = {};
let socket   = null;
let wsReady  = false;

function initSocket() {
  if (socket) { socket.disconnect(); socket = null; }
  socket = io('/', {
    transports: ['websocket', 'polling'],
    reconnection: true,
    reconnectionDelay: 3000,
    reconnectionDelayMax: 10000,
    reconnectionAttempts: 10,
    timeout: 20000,
  });

  socket.on('connect', () => {
    setWsDot('live', 'LIVE');
    wsReady = true;
    // Re-subscribe all tokens — NO page reload, just resubscribe
    const tokens = Object.keys(tokenMap);
    if (tokens.length) {
      socket.emit('subscribe', { tokens });
      setWsDot('live', `LIVE · ${tokens.length} tokens`);
    }
  });

  socket.on('reconnect', (attempt) => {
    console.log('WS reconnected after', attempt, 'attempts');
    wsReady = true;
    // Only resubscribe — never reload the chain on reconnect
    const tokens = Object.keys(tokenMap);
    if (tokens.length) {
      socket.emit('subscribe', { tokens });
      setWsDot('live', `LIVE · ${tokens.length} tokens`);
    }
  });

  socket.on('reconnect_attempt', () => {
    setWsDot('wait', 'reconnecting…');
  });

  socket.on('disconnect', () => { setWsDot('dead', 'disconnected'); wsReady = false; });
  socket.on('connect_error', () => { setWsDot('dead', 'WS error'); });

  // Single tick — immediate update
  socket.on('tick', tick => applyTick(tick));

  // Batch tick every ~1s — update all at once
  socket.on('tick_batch', batch => {
    for (const tok of Object.keys(batch)) applyTick(batch[tok]);
  });

  socket.on('ws_status', d => {
    if (d.connected) setWsDot('live', `LIVE · ${d.subscribed||0} tokens`);
    else             setWsDot('wait', d.msg || 'connecting…');
  });
}

function setWsDot(cls, label) {
  const dot = $('wsDot'), lbl = $('wsLbl');
  if (!dot) return;
  dot.className = `ws-dot ${cls}`;
  lbl.textContent = label;
}

// Apply a single tick to the DOM without re-rendering the whole table
function applyTick(tick) {
  const token = String(tick.token);
  const ltp   = tick.ltp;
  if (!ltp || ltp <= 0) return;

  // ── Index spot update — always first, no DOM row needed ──────────────────
  const IDX_TOKENS = {'99926000':'NIFTY','99926009':'BANKNIFTY','99926037':'FINNIFTY','99926074':'MIDCPNIFTY'};
  if (IDX_TOKENS[token]) {
    // Always cache the tick regardless of current symbol
    if (!tokenMap[token]) tokenMap[token] = { side:'idx', strike:0, prevClose:0 };
    const idxInfo = tokenMap[token];
    // First tick carries close = prev day close — store permanently
    if (tick.close && tick.close > 0 && !idxInfo.prevClose) idxInfo.prevClose = tick.close;

    // Only update topbar display for the currently selected symbol
    if (IDX_TOKENS[token] === S.sym) {
      S.spot = ltp;
      // spotWrap removed from topbar
      $('spotVal').textContent = fmt(ltp);
      const ipc  = idxInfo.prevClose || tick.close || 0;
      if (ipc > 0) {
        const ichg = +(ltp - ipc).toFixed(2);
        const ipct = +((ichg / ipc) * 100).toFixed(2);
        const isgn = ichg >= 0 ? '+' : '';
        const el   = $('spotChg');
        el.textContent = `${isgn}${fmt(ichg)}  (${isgn}${ipct}%)`;
        el.className   = `spot-chg ${ichg > 0 ? 'up' : ichg < 0 ? 'dn' : 'fl'}`;
      }
    }
    return; // index ticks have no chain row — done
  }

  // ── Option chain row update ───────────────────────────────────────────────
  const info = tokenMap[token];
  if (!info || info.side === 'idx') return;

  const { side, strike } = info;
  const pc   = info.prevClose || 0;
  const chg  = pc > 0 ? +(ltp - pc).toFixed(2) : 0;
  const pct  = pc > 0 ? +((chg / pc) * 100).toFixed(2) : 0;
  const chgC = chg > 0 ? 'var(--up)' : chg < 0 ? 'var(--down)' : 'var(--muted)';
  const pCls = chg > 0 ? 'up' : chg < 0 ? 'dn' : 'fl';

  const allRows = document.querySelectorAll(`tr[data-strike="${strike}"]`);
  const paneRow = Array.from(allRows).find(r => r.querySelector('.'+side+'-ltp'));
  if (!paneRow) return;

  // LTP
  const ltpEl = paneRow.querySelector(`.${side}-ltp`);
  if (ltpEl) {
    const prev = parseFloat(ltpEl.dataset.val || 0);
    if (tick.high) ltpEl.dataset.high = tick.high;
    if (tick.low)  ltpEl.dataset.low  = tick.low;
    ltpEl.textContent = fmt(ltp);
    ltpEl.dataset.val = ltp;
    const rc = ltpRangeClass(ltp, parseFloat(ltpEl.dataset.high||0), parseFloat(ltpEl.dataset.low||0));
    ltpEl.classList.remove('near-high','near-low');
    if (rc) ltpEl.classList.add(rc);
    if (ltp > prev) { ltpEl.closest('td').classList.remove('tick-flash','tick-flash-dn'); void ltpEl.closest('td').offsetWidth; ltpEl.closest('td').classList.add('tick-flash'); }
    else if (ltp < prev) { ltpEl.closest('td').classList.remove('tick-flash','tick-flash-dn'); void ltpEl.closest('td').offsetWidth; ltpEl.closest('td').classList.add('tick-flash-dn'); }
  }
  // Chg
  const chgEl = paneRow.querySelector(`.${side}-chg`);
  if (chgEl) { chgEl.textContent = (chg>=0?'+':'')+chg; chgEl.style.color=chgC; }
  // Chg%
  const pctEl = paneRow.querySelector(`.${side}-pct`);
  if (pctEl) { pctEl.textContent = (pct>=0?'+':'')+pct+'%'; pctEl.className=`v-pct ${pCls} ${side}-pct`; }
  // High / Low / OI
  if (tick.high) { const el=paneRow.querySelector(`.${side}-high`); if(el) el.textContent=fmt(tick.high); }
  if (tick.low)  { const el=paneRow.querySelector(`.${side}-low`);  if(el) el.textContent=fmt(tick.low);  }
  if (tick.oi)   { const el=paneRow.querySelector(`.${side}-oi`);   if(el) el.textContent=fmtOI(tick.oi); }
}

// Subscribe all visible tokens after chain renders
function subscribeTokens(chain) {
  tokenMap = {};
  const tokens = [];

  for (const row of chain) {
    if (row.call?.token) {
      tokenMap[String(row.call.token)] = { side: 'ce', strike: row.strike, prevClose: row.call.prevClose || row.call.close || 0 };
      tokens.push(String(row.call.token));
    }
    if (row.put?.token) {
      tokenMap[String(row.put.token)] = { side: 'pe', strike: row.strike, prevClose: row.put.prevClose || row.put.close || 0 };
      tokens.push(String(row.put.token));
    }
  }

  // Also subscribe the index spot token for the current symbol
  const IDX_MAP = {'NIFTY':'99926000','BANKNIFTY':'99926009','FINNIFTY':'99926037','MIDCPNIFTY':'99926074'};
  const idxTok = IDX_MAP[S.sym];
  if (idxTok && !tokenMap[idxTok]) {
    tokenMap[idxTok] = { side: 'idx', strike: 0, prevClose: 0 };
    tokens.push(idxTok);
  }

  if (socket && wsReady && tokens.length) {
    socket.emit('subscribe', { tokens });
    setWsDot('live', `LIVE · ${tokens.length} tokens`);
  }
}

// ── Formatters ────────────────────────────────────────────────────────────────
function fmtExp(e){const m=e.match(/^(\d{2})([A-Z]{3})(\d{4})$/);return m?`${m[1]}-${m[2]}-${m[3].slice(2)}`:e;}
function fmt(v){if(!v&&v!==0)return'—';const n=parseFloat(v);if(isNaN(n))return'—';return n.toLocaleString('en-IN',{minimumFractionDigits:2,maximumFractionDigits:2});}
function fmtOI(v){if(!v||v===0)return'—';if(v>=1e7)return(v/1e7).toFixed(2)+'Cr';if(v>=1e5)return(v/1e5).toFixed(2)+'L';if(v>=1e3)return(v/1e3).toFixed(1)+'K';return String(v);}
// ── MARKET BRAIN ─────────────────────────────────────────────────────────────
let brainOpen    = false;
let brainTimer   = null;
const bHistory   = [];   // {time, straddle, ceChg, peChg, score}

const BRAIN_MOODS = [
  {name:'PANIC',    color:'#ff2244', deg:-80, sub:'Heavy selling, IVs spiking. Avoid buying — high risk.'},
  {name:'FEAR',     color:'#ff6b35', deg:-40, sub:'Bears in control. PEs rising, market falling.'},
  {name:'NEUTRAL',  color:'#ffd166', deg:0,   sub:'Balanced market. Sellers collecting theta. Watch for breakout.'},
  {name:'GREED',    color:'#00d97e', deg:40,  sub:'Buyers active. Calls rising, puts fading. Upside bias.'},
  {name:'EUPHORIA', color:'#2196f3', deg:80,  sub:'FOMO rally 🚀 OTM calls expensive. Beware of sharp reversal.'},
];

function toggleBrain() {
  brainOpen = !brainOpen;
  const panel = document.getElementById('brainPanel');
  const btn   = document.getElementById('brainToggle');
  panel.style.display = brainOpen ? 'block' : 'none';
  btn.classList.toggle('active', brainOpen);
  if (brainOpen && S.chainLoaded) brainAnalyze();
}

function brainSetNeedle(deg) {
  document.getElementById('bNeedle')
    .setAttribute('transform', `rotate(${deg}, 100, 98)`);
}

function brainGetMoodIdx(score) {
  if (score < -60) return 0;
  if (score < -20) return 1;
  if (score <  20) return 2;
  if (score <  60) return 3;
  return 4;
}

// Called after every chain load OR every live tick batch
function brainAnalyze() {
  if (!brainOpen) return;
  const chain = window._lastChainData;
  if (!chain || !chain.chain || !chain.chain.length) return;

  const spot  = chain.spot || S.spot || 0;
  const rows  = chain.chain;
  // Find ATM row
  const atmRow = rows.reduce((a,b) =>
    Math.abs(b.strike - spot) < Math.abs(a.strike - spot) ? b : a);
  const atmStrike = atmRow.strike;

  // Get live LTPs from DOM (updated by WebSocket ticks)
  const atmCeEl = document.querySelector(`tr[data-strike="${atmStrike}"] .ce-ltp`);
  const atmPeEl = document.querySelector(`tr[data-strike="${atmStrike}"] .pe-ltp`);

  const ceLtp  = atmCeEl ? parseFloat(atmCeEl.dataset.val || atmCeEl.textContent.replace(/,/g,'')) || 0 : (atmRow.call?.ltp || 0);
  const peLtp  = atmPeEl ? parseFloat(atmPeEl.dataset.val || atmPeEl.textContent.replace(/,/g,'')) || 0 : (atmRow.put?.ltp  || 0);
  const ceOpen = atmRow.call?.open || atmRow.call?.prevClose || ceLtp;
  const peOpen = atmRow.put?.open  || atmRow.put?.prevClose  || peLtp;

  if (!ceLtp || !peLtp) return;

  // 1 OTM call/put for skew
  const otmCeRow = rows.find(r => r.strike === atmStrike + (S.sym === 'BANKNIFTY' ? 300 : S.sym === 'FINNIFTY' ? 50 : 100));
  const otmPeRow = rows.find(r => r.strike === atmStrike - (S.sym === 'BANKNIFTY' ? 300 : S.sym === 'FINNIFTY' ? 50 : 100));
  const otmCeLtp = otmCeRow?.call?.ltp || 0;
  const otmPeLtp = otmPeRow?.put?.ltp  || 0;

  const ceChgPct = ceOpen > 0 ? ((ceLtp - ceOpen) / ceOpen) * 100 : 0;
  const peChgPct = peOpen > 0 ? ((peLtp - peOpen) / peOpen) * 100 : 0;
  const stNow    = ceLtp + peLtp;
  const stOpen   = ceOpen + peOpen;
  const stChgPct = stOpen > 0 ? ((stNow - stOpen) / stOpen) * 100 : 0;
  const ivProxy  = spot > 0 ? (stNow / spot) * 100 : 0;
  const otmSkew  = otmPeLtp > 0 && otmCeLtp > 0 ? otmCeLtp / otmPeLtp : null;

  // Score
  let score = 0;
  score += (ceChgPct - peChgPct) * 0.6;
  score -= stChgPct * 0.4;
  if (otmSkew) score += (otmSkew - 1) * 20;
  score = Math.max(-100, Math.min(100, score));

  const moodIdx = brainGetMoodIdx(score);
  const mood    = BRAIN_MOODS[moodIdx];

  // Needle + name
  brainSetNeedle(mood.deg);
  const nm = document.getElementById('bMoodName');
  nm.textContent = mood.name;
  nm.style.color = mood.color;
  document.getElementById('bMoodSub').textContent = mood.sub;

  // CE/PE bar
  const ceA = Math.abs(ceChgPct), peA = Math.abs(peChgPct);
  const tot  = (ceA + peA) || 1;
  document.getElementById('bCeBar').style.width = (ceA/tot*100) + '%';
  document.getElementById('bCePct').textContent = 'CE ' + (ceChgPct >= 0 ? '+' : '') + ceChgPct.toFixed(1) + '%';
  document.getElementById('bCePct').style.color = ceChgPct >= 0 ? '#00d97e' : '#ff3d5a';
  document.getElementById('bPePct').textContent = 'PE ' + (peChgPct >= 0 ? '+' : '') + peChgPct.toFixed(1) + '%';
  document.getElementById('bPePct').style.color = peChgPct >= 0 ? '#ff3d5a' : '#00d97e';

  // IV bar
  const ivH = Math.min(95, Math.max(5, ivProxy * 60));
  document.getElementById('bIvBar').style.width  = ivH + '%';
  document.getElementById('bIvBar').style.background = ivProxy > 1.2 ? '#ff2244' : ivProxy > 0.8 ? '#ffd166' : '#00d97e';
  document.getElementById('bIvNow').textContent  = ivProxy.toFixed(2) + '%';
  document.getElementById('bStraddleVal').textContent = '₹' + stNow.toFixed(0);

  // Range bar
  const expMove = stNow;
  const rngLow  = spot - expMove, rngHigh = spot + expMove;
  document.getElementById('bRangeBar').style.width = '60%';
  document.getElementById('bRangeLow').textContent  = fmt(rngLow);
  document.getElementById('bRangeHigh').textContent = fmt(rngHigh);

  // Sentence
  const sentences = [
    `<span class="brain-hl" style="background:rgba(255,34,68,.2);color:#ff2244">PANIC</span> — High fear, IV spiking. Both CE &amp; PE sellers losing. Avoid naked positions.`,
    `Market feeling <span class="brain-hl" style="background:rgba(255,107,53,.2);color:#ff6b35">FEARFUL</span> — Bears winning. PE rising, smart money hedging. Risk of further fall.`,
    `Market is <span class="brain-hl" style="background:rgba(255,209,102,.2);color:#ffd166">NEUTRAL</span> — Balanced decay. Option sellers winning. Expect range <strong>±₹${expMove.toFixed(0)}</strong> unless news hits.`,
    `Showing <span class="brain-hl" style="background:rgba(0,217,126,.2);color:#00d97e">GREED</span> — Calls rising, puts fading. Buyers in control. Bias is UP.`,
    `<span class="brain-hl" style="background:rgba(33,150,243,.2);color:#2196f3">EUPHORIA</span> rally 🚀 — FOMO buying. OTM calls flying. ATM straddle ₹${stNow.toFixed(0)} — caution at tops.`,
  ];
  document.getElementById('bSentence').innerHTML = sentences[moodIdx];

  // Signals
  const sigs = [];
  if (ceChgPct > 8 && peChgPct < -8)
    sigs.push({icon:'🚀', color:'#00d97e', title:'Bull Move Confirmed', desc:'CE surging + PE dying = clear upside momentum.'});
  else if (peChgPct > 8 && ceChgPct < -8)
    sigs.push({icon:'📉', color:'#ff2244', title:'Bear Move Confirmed', desc:'PE surging + CE dying = clear downside momentum.'});

  if (stChgPct < -15)
    sigs.push({icon:'🧊', color:'#2196f3', title:'IV Crush', desc:'Straddle collapsing. Sellers winning. Avoid buying.'});
  else if (stChgPct > 15)
    sigs.push({icon:'⚡', color:'#ffd166', title:'IV Explosion!', desc:'Straddle rising fast. Big move expected. Avoid selling.'});

  if (ceChgPct < -5 && peChgPct < -5)
    sigs.push({icon:'💰', color:'#ffd166', title:'Theta Feast', desc:'Both sides falling. Range day. Short straddle/strangle working.'});

  if (otmSkew && otmSkew > 1.8)
    sigs.push({icon:'📞', color:'#2196f3', title:'Call Skew High', desc:`OTM CE/PE = ${otmSkew.toFixed(2)}. Bulls buying upside.`});
  else if (otmSkew && otmSkew < 0.6)
    sigs.push({icon:'🔻', color:'#ff6b35', title:'Put Skew High', desc:`OTM CE/PE = ${otmSkew.toFixed(2)}. Fear/hedge buying active.`});

  if (!sigs.length)
    sigs.push({icon:'⏳', color:'#a0b4c8', title:'Mixed Signals', desc:'Market undecided. Use mood meter and CE/PE bars to guide.'});

  document.getElementById('bSignals').innerHTML = sigs.map(s =>
    `<div class="brain-sig" style="border-color:${s.color}">
      <b style="color:${s.color}">${s.icon} ${s.title}</b>
      <span>${s.desc}</span>
    </div>`
  ).join('');

  // Straddle history + chart
  const t = new Date().toLocaleTimeString('en-IN', {hour:'2-digit', minute:'2-digit', hour12:false});
  bHistory.push({time:t, straddle:stNow, score});
  if (bHistory.length > 40) bHistory.shift();
  brainDrawChart();

  // Timeline dots
  const tl = document.getElementById('bTimeline');
  tl.innerHTML = bHistory.slice(-12).map((h, i) => {
    const m = BRAIN_MOODS[brainGetMoodIdx(h.score)];
    return `<div class="brain-tl-dot" title="${h.time}: ${m.name}" style="background:${m.color}22;border-color:${m.color};color:${m.color}">${h.time.slice(0,5)}</div>`;
  }).join('');

  // Update time
  document.getElementById('brainUpdTime').textContent = 'Updated ' + t;
  document.getElementById('brainAutoLbl').textContent = `ATM ${atmStrike} | CE ₹${ceLtp.toFixed(0)} PE ₹${peLtp.toFixed(0)}`;
}

function brainDrawChart() {
  const canvas = document.getElementById('bStraddleChart');
  if (!canvas || bHistory.length < 2) return;
  const W = canvas.offsetWidth || 400;
  canvas.width  = W * 2;
  canvas.height = 100;
  const ctx = canvas.getContext('2d');
  ctx.scale(2, 1);
  const w = W, h = 50;
  ctx.clearRect(0, 0, w, h);

  const vals = bHistory.map(h => h.straddle);
  const mn   = Math.min(...vals) * 0.97;
  const mx   = Math.max(...vals) * 1.03;
  const pad  = {l:4, r:4, t:4, b:16};

  const pts  = vals.map((v, i) => ({
    x: pad.l + (i / (vals.length-1)) * (w - pad.l - pad.r),
    y: pad.t + (1 - (v-mn)/(mx-mn)) * (h - pad.t - pad.b),
  }));

  // Fill
  const isRising = vals[vals.length-1] > vals[0];
  const grd = ctx.createLinearGradient(0, pad.t, 0, h-pad.b);
  if (isRising) { grd.addColorStop(0,'rgba(255,61,90,.25)'); grd.addColorStop(1,'rgba(255,61,90,.02)'); }
  else          { grd.addColorStop(0,'rgba(0,217,126,.25)'); grd.addColorStop(1,'rgba(0,217,126,.02)'); }

  ctx.beginPath();
  ctx.moveTo(pts[0].x, h-pad.b);
  ctx.lineTo(pts[0].x, pts[0].y);
  for (let i=1; i<pts.length; i++) {
    const cx = (pts[i-1].x + pts[i].x)/2;
    ctx.bezierCurveTo(cx, pts[i-1].y, cx, pts[i].y, pts[i].x, pts[i].y);
  }
  ctx.lineTo(pts[pts.length-1].x, h-pad.b);
  ctx.closePath();
  ctx.fillStyle = grd; ctx.fill();

  // Line
  ctx.beginPath();
  ctx.moveTo(pts[0].x, pts[0].y);
  for (let i=1; i<pts.length; i++) {
    const cx = (pts[i-1].x + pts[i].x)/2;
    ctx.bezierCurveTo(cx, pts[i-1].y, cx, pts[i].y, pts[i].x, pts[i].y);
  }
  ctx.strokeStyle = isRising ? '#ff3d5a' : '#00d97e';
  ctx.lineWidth   = 1.5; ctx.stroke();

  // Time labels (first + last)
  ctx.fillStyle = 'rgba(160,180,200,.6)';
  ctx.font = '8px Share Tech Mono';
  ctx.textAlign = 'left';
  ctx.fillText(bHistory[0].time, pad.l, h-3);
  ctx.textAlign = 'right';
  ctx.fillText(bHistory[bHistory.length-1].time, w-pad.r, h-3);
}

// Hook into existing loadChain — analyze after chain loads
const _origRenderChain = renderChain;
function renderChain(data) {
  _origRenderChain(data);
  window._lastChainData = data;
  if (brainOpen) setTimeout(brainAnalyze, 300);
}

// Hook into applyTick — re-analyze brain every 30 ticks
let _tickCount = 0;
const _origApplyTick = applyTick;
function applyTick(tick) {
  _origApplyTick(tick);
  if (brainOpen) {
    _tickCount++;
    if (_tickCount % 30 === 0) brainAnalyze();
  }
}

window.addEventListener('resize', brainDrawChart);
</script>
</body>
</html>"""
