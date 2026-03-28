"""
PiggyBac — Multi-chain fresh-wallet + dormant-wallet buy detector.
Monitors Base, Ethereum mainnet, and Solana for coordinated buying by:
  - Fresh wallets: created/funded in the last 24h
  - Dormant wallets: last active 6+ months ago, now suddenly buying

Signal is scored and tiered. Mixed fresh+dormant clusters are flagged as
the strongest signal.
"""

import os
import time
import logging
import threading
from collections import defaultdict
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

API_KEY = os.getenv("BASESCAN_API_KEY", "").strip()
SOLSCAN_API_KEY = os.getenv("SOLSCAN_API_KEY", "").strip().strip('"\'')
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

FRESH_WALLET_MAX_AGE_HOURS = 24        # wallet created <24h ago = fresh
DORMANT_WALLET_MIN_INACTIVE_DAYS = 180 # last active 6+ months ago = dormant
CLUSTER_MIN_WALLETS = 5                # min wallets (fresh+dormant) to alert
CLUSTER_TIME_WINDOW_HOURS = 2          # rolling window for clustering
SPEED_WINDOW_MINUTES = 4               # window for speed bonus scoring
POLL_INTERVAL_SECONDS = 30
MIN_BUY_VALUE_USD = 50                 # placeholder

ERC20_TRANSFER_TOPIC = (
    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
)

# ---------------------------------------------------------------------------
# Solana config
# ---------------------------------------------------------------------------

SOLSCAN_BASE_URL = "https://pro-api.solscan.io/v2.0"
SOLANA_POLL_INTERVAL_SECONDS = 30

# Known Solana DEX program IDs
SOLANA_DEX_PROGRAMS = {
    "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8": "Raydium AMM",
    "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK": "Raydium CLMM",
    "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc":  "Orca Whirlpool",
    "9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP": "Orca Token Swap",
    "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4":  "Jupiter v6",
    "JUP4Fb2cqiRUcaTHdrPC8h2gNsA2ETXiPDD33WcGuJB":  "Jupiter v4",
    "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P":  "Pump.fun",
}

# Known Solana CEX/bridge addresses for funding source detection
SOLANA_KNOWN_FUNDING_SOURCES = {
    "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM": "Binance",
    "5tzFkiKscjHK98YYXtN2sVPBTZM7HNpVRbPVdBbRjQwR": "Binance",
    "2ojv9BAiHUrvsm9gxDe7fJSzbNZSJcxZvf8dqmWGHG8S": "Binance",
    "H8sMJSCQxfKiFTCfDR3DUMLPwcRbM61LGFJ8N4dK3WjS": "Coinbase",
    "GJRs4FwHtemZ5ZE9x3FNvJ8TMwitKTh21yxdRPqn7npE": "Coinbase",
    "FWznbcNXWQuHTawe9RxvQ2LdCENssh12dsznf4RiouN5": "Kraken",
    "CuieVDEDtLo7FypA9SbLM9SaXEi5Wfv7R8HH6kHMh64d": "OKX",
    "A77HErqtfN1hLLpvZ9pBaGHR4PK4Ky1rkNy5P1o4gMd":  "Phantom Swap",
}

WSOL_MINT = "So11111111111111111111111111111111111111112"

# ---------------------------------------------------------------------------
# Chain definitions
# ---------------------------------------------------------------------------

CHAINS = {
    "base": {
        "name": "Base",
        "api_url": "https://api.basescan.org/api",
        "explorer_url": "https://basescan.org",
        "dexscreener_network": "base",
        "dex_routers": {
            "0x2626664c2603336E57B271c5C0b26F421741e481": "Uniswap V3 Router",
            "0x33128a8fC17869897dcE68Ed026d694621f6FDfD": "Uniswap V3 Factory",
            "0xcF77a3Ba9A5CA399B7c97c74d54e5b1Beb874E43": "Aerodrome Router",
            "0x327Df1E6de05895d2ab08513aaDD9313Fe505d86": "BaseSwap Router",
            "0x4752ba5DBc23f44D87826276BF6Fd6b1C372aD24": "Uniswap V2 Router",
        },
        "known_funding_sources": {
            "0x3304E22DDaa22bCdC5fCa2269b418046aE7b566A": "Coinbase",
            "0xA9D1e08C7793af67e9d92fe308d5697FB81d3E43": "Coinbase Prime",
            "0x77696bb39917C91A0c3908D577d5e322095425cA": "Base Bridge",
            "0x4200000000000000000000000000000000000010": "Base L2 Bridge",
            "0x6DfD7D42c20e9D73B174B00c1fdE2a29F3B99BC4": "Binance",
        },
    },
    "ethereum": {
        "name": "Ethereum",
        "api_url": "https://api.etherscan.io/api",
        "explorer_url": "https://etherscan.io",
        "dexscreener_network": "ethereum",
        "dex_routers": {
            "0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D": "Uniswap V2 Router",
            "0xE592427A0AEce92De3Edee1F18E0157C05861564": "Uniswap V3 Router",
            "0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45": "Uniswap V3 Router 2",
            "0xd9e1cE17f2641f24aE83637ab66a2cca9C378B9F": "SushiSwap Router",
            "0x1111111254EEB25477B68fb85Ed929f73A960582": "1inch Router",
        },
        "known_funding_sources": {
            "0x3f5CE5FBFe3E9af3971dD833D26bA9b5C936f0bE": "Binance",
            "0xD551234Ae421e3BCBA99A0Da6d736074f22192FF": "Binance",
            "0x564286362092D8e7936f0549571a803B203aAceD": "Binance",
            "0x71660c4005BA85c37ccec55d0C4493E66Fe775d3": "Coinbase",
            "0x503828976D22510aad0201ac7EC88293211D23Da": "Coinbase",
            "0x77696bb39917C91A0c3908D577d5e322095425cA": "Kraken",
            "0x267be1C1D684F78cb4F6a176C4911b741E4Ffdc0": "Kraken",
            "0x2910543Af39abA0Cd09dBb2D50200b3E800A63D2": "Kraken",
        },
    },
}

# ---------------------------------------------------------------------------
# Per-chain state
# ---------------------------------------------------------------------------

ALL_CHAIN_IDS = list(CHAINS.keys()) + ["solana"]

# chain_id -> token_address -> list of buy dicts
cluster_buys: dict[str, dict[str, list[dict]]] = {
    chain: defaultdict(list) for chain in ALL_CHAIN_IDS
}

# chain_id -> wallet_address -> {first_ts, last_ts}
wallet_cache: dict[str, dict[str, dict]] = {chain: {} for chain in ALL_CHAIN_IDS}

# chain_id -> token_address -> {name, symbol}
token_info_cache: dict[str, dict[str, dict]] = {chain: {} for chain in ALL_CHAIN_IDS}

# set of cluster keys already alerted
alerted_clusters: set[str] = set()

# chain_id -> last block processed (EVM chains only)
last_block_checked: dict[str, int] = {chain: 0 for chain in CHAINS}

# Solana: last seen transaction signature to avoid re-processing
solana_last_seen_sig: str = ""

# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------

def score_cluster(fresh_count: int, dormant_count: int, total: int, speed_count: int) -> tuple[int, str]:
    """
    Returns (score, tier_emoji).
    Tier:
      🔥    — basic signal (5+ wallets, mostly fresh)
      🔥🔥  — strong signal (10+ wallets OR mixed fresh+dormant)
      🔥🔥🔥 — massive cook (20+ wallets OR heavy mix)
    """
    score = total

    # Mixed fresh+dormant is the strongest signal
    if fresh_count > 0 and dormant_count > 0:
        score += dormant_count * 3  # dormant wallets weighted heavily

    # Speed bonus — many wallets in first few minutes
    if speed_count >= 3:
        score += speed_count * 2

    if total >= 20 or (fresh_count > 0 and dormant_count >= 3):
        tier = "🔥🔥🔥"
    elif total >= 10 or (fresh_count > 0 and dormant_count > 0):
        tier = "🔥🔥"
    else:
        tier = "🔥"

    return score, tier


# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------

NOW_TS = lambda: int(time.time())


def chain_get(chain_id: str, params: dict) -> dict | None:
    chain = CHAINS[chain_id]
    params["apikey"] = API_KEY
    try:
        r = requests.get(chain["api_url"], params=params, timeout=10)
        r.raise_for_status()
        data = r.json()
        if data.get("status") == "1" or data.get("message") == "OK":
            return data
        log.debug("[%s] API non-1 status: %s", chain_id, data.get("message"))
        return None
    except Exception as exc:
        log.warning("[%s] API request failed: %s", chain_id, exc)
        return None


def get_latest_block(chain_id: str) -> int:
    data = chain_get(chain_id, {"module": "proxy", "action": "eth_blockNumber"})
    if data:
        return int(data["result"], 16)
    return 0


def get_erc20_transfers(chain_id: str, from_block: int, to_block: int) -> list[dict]:
    data = chain_get(
        chain_id,
        {
            "module": "logs",
            "action": "getLogs",
            "fromBlock": from_block,
            "toBlock": to_block,
            "topic0": ERC20_TRANSFER_TOPIC,
            "page": 1,
            "offset": 1000,
        },
    )
    return data.get("result", []) if data else []


def get_wallet_timestamps(chain_id: str, address: str) -> dict | None:
    """
    Returns {first_ts, last_ts} for a wallet. Cached.
    Makes two API calls (first tx asc, last tx desc).
    """
    cache = wallet_cache[chain_id]
    if address in cache:
        return cache[address]

    base_params = {
        "module": "account",
        "action": "txlist",
        "address": address,
        "startblock": 0,
        "endblock": 99999999,
        "page": 1,
        "offset": 1,
    }

    first_data = chain_get(chain_id, {**base_params, "sort": "asc"})
    last_data = chain_get(chain_id, {**base_params, "sort": "desc"})

    if not first_data or not first_data.get("result"):
        return None

    first_ts = int(first_data["result"][0]["timeStamp"])
    last_ts = int(last_data["result"][0]["timeStamp"]) if last_data and last_data.get("result") else first_ts

    result = {"first_ts": first_ts, "last_ts": last_ts}
    cache[address] = result
    return result


def classify_wallet(chain_id: str, address: str) -> str | None:
    """
    Returns 'fresh', 'dormant', or None (not interesting).
    - fresh: first tx <24h ago
    - dormant: last tx >180 days ago, but now active again
    """
    ts = get_wallet_timestamps(chain_id, address)
    if not ts:
        return None

    now = NOW_TS()
    age_hours = (now - ts["first_ts"]) / 3600
    inactive_days = (now - ts["last_ts"]) / 86400

    if age_hours <= FRESH_WALLET_MAX_AGE_HOURS:
        return "fresh"
    if inactive_days >= DORMANT_WALLET_MIN_INACTIVE_DAYS:
        return "dormant"
    return None


def get_wallet_funding_source(chain_id: str, address: str) -> str | None:
    data = chain_get(
        chain_id,
        {
            "module": "account",
            "action": "txlist",
            "address": address,
            "startblock": 0,
            "endblock": 99999999,
            "page": 1,
            "offset": 5,
            "sort": "asc",
        },
    )
    if not data or not data.get("result"):
        return None
    funding_sources = CHAINS[chain_id]["known_funding_sources"]
    for tx in data["result"]:
        sender = tx.get("from", "").lower()
        for known_addr, label in funding_sources.items():
            if sender == known_addr.lower():
                return label
    return None


def get_token_info(chain_id: str, token_address: str) -> dict:
    cache = token_info_cache[chain_id]
    if token_address in cache:
        return cache[token_address]
    # P0: replace with real token lookup
    info = {"name": "Unknown", "symbol": "???"}
    cache[token_address] = info
    return info


# ---------------------------------------------------------------------------
# Telegram
# ---------------------------------------------------------------------------


def send_telegram(message: str) -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Telegram not configured — skipping alert")
        log.info("ALERT:\n%s", message)
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        r = requests.post(
            url,
            json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": message,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
            timeout=10,
        )
        r.raise_for_status()
    except Exception as exc:
        log.error("Telegram send failed: %s", exc)


def build_alert(chain_id: str, token_address: str, buys: list[dict]) -> str:
    chain = CHAINS[chain_id]
    info = get_token_info(chain_id, token_address)
    token_name = info["name"]
    token_symbol = info["symbol"]
    short_addr = token_address[:6] + "..." + token_address[-4:]
    explorer_url = f"{chain['explorer_url']}/token/{token_address}"
    dexscreener_url = (
        f"https://dexscreener.com/{chain['dexscreener_network']}/{token_address}"
    )

    fresh_buys = [b for b in buys if b["wallet_type"] == "fresh"]
    dormant_buys = [b for b in buys if b["wallet_type"] == "dormant"]

    # Speed: how many wallets bought within first SPEED_WINDOW_MINUTES
    if buys:
        earliest_ts = min(b["timestamp"] for b in buys)
        speed_cutoff = earliest_ts + SPEED_WINDOW_MINUTES * 60
        speed_count = sum(1 for b in buys if b["timestamp"] <= speed_cutoff)
    else:
        speed_count = 0

    score, tier = score_cluster(len(fresh_buys), len(dormant_buys), len(buys), speed_count)

    # Wallet lines — fresh first, then dormant
    wallet_lines = []
    for b in fresh_buys:
        short_w = b["wallet"][:6] + "..." + b["wallet"][-4:]
        funding = b.get("funding_source") or "unknown"
        tx_url = f"{chain['explorer_url']}/tx/{b['tx_hash']}"
        wallet_lines.append(f"  🆕 <a href='{tx_url}'>{short_w}</a> (funded via {funding})")

    for b in dormant_buys:
        short_w = b["wallet"][:6] + "..." + b["wallet"][-4:]
        tx_url = f"{chain['explorer_url']}/tx/{b['tx_hash']}"
        inactive_days = int((NOW_TS() - b["last_ts"]) / 86400)
        wallet_lines.append(f"  💤 <a href='{tx_url}'>{short_w}</a> (dormant {inactive_days}d)")

    wallets_str = "\n".join(wallet_lines)
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    chain_label = chain["name"]

    # Summary line
    summary_parts = []
    if fresh_buys:
        summary_parts.append(f"{len(fresh_buys)} fresh 🆕")
    if dormant_buys:
        summary_parts.append(f"{len(dormant_buys)} dormant 💤")
    summary = " + ".join(summary_parts)

    speed_str = f"{speed_count} wallets in first {SPEED_WINDOW_MINUTES}min" if speed_count > 1 else ""

    return (
        f"{tier} <b>PiggyBac Alert</b> [{chain_label}] — {ts}\n\n"
        f"<b>Token:</b> {token_name} ({token_symbol})\n"
        f"<b>Address:</b> <a href='{explorer_url}'>{short_addr}</a>\n"
        f"<b>Wallets:</b> {summary}\n"
        + (f"<b>Speed:</b> {speed_str}\n" if speed_str else "")
        + f"\n{wallets_str}\n\n"
        f"<a href='{dexscreener_url}'>DexScreener</a> | "
        f"<a href='{explorer_url}'>Explorer</a>"
    )


# ---------------------------------------------------------------------------
# Detection logic
# ---------------------------------------------------------------------------


def prune_old_buys(chain_id: str) -> None:
    cutoff = NOW_TS() - CLUSTER_TIME_WINDOW_HOURS * 3600
    buys = cluster_buys[chain_id]
    for token in list(buys.keys()):
        buys[token] = [b for b in buys[token] if b["timestamp"] >= cutoff]
        if not buys[token]:
            del buys[token]


def check_for_clusters(chain_id: str) -> None:
    for token_address, buys in cluster_buys[chain_id].items():
        if len(buys) < CLUSTER_MIN_WALLETS:
            continue
        # Deduplicate by wallet
        seen = {}
        for b in buys:
            seen[b["wallet"]] = b
        unique_buys = list(seen.values())
        if len(unique_buys) < CLUSTER_MIN_WALLETS:
            continue

        cluster_key = chain_id + token_address + str(
            sorted(b["wallet"] for b in unique_buys)
        )
        if cluster_key in alerted_clusters:
            continue
        alerted_clusters.add(cluster_key)

        fresh_count = sum(1 for b in unique_buys if b["wallet_type"] == "fresh")
        dormant_count = sum(1 for b in unique_buys if b["wallet_type"] == "dormant")
        log.info(
            "[%s] CLUSTER: %d fresh + %d dormant wallets bought %s",
            chain_id, fresh_count, dormant_count, token_address,
        )
        message = build_alert(chain_id, token_address, unique_buys)
        send_telegram(message)


def process_transfers(chain_id: str, transfers: list[dict]) -> None:
    now = NOW_TS()
    dex_routers_lower = {k.lower() for k in CHAINS[chain_id]["dex_routers"]}

    for tx in transfers:
        topics = tx.get("topics", [])
        if len(topics) < 3:
            continue

        to_addr = "0x" + topics[2][-40:]
        token_address = tx.get("address", "").lower()
        tx_hash = tx.get("transactionHash", "")
        raw_ts = tx.get("timeStamp", str(now))
        block_ts = int(raw_ts, 16) if raw_ts.startswith("0x") else int(raw_ts)

        if to_addr.lower() in dex_routers_lower:
            continue

        wallet_type = classify_wallet(chain_id, to_addr)
        if wallet_type is None:
            continue  # not fresh or dormant — skip

        funding_source = None
        last_ts = now
        ts_data = wallet_cache[chain_id].get(to_addr)
        if ts_data:
            last_ts = ts_data["last_ts"]

        if wallet_type == "fresh":
            funding_source = get_wallet_funding_source(chain_id, to_addr)

        log.info(
            "[%s] %s wallet buy: wallet=%s token=%s",
            chain_id, wallet_type, to_addr[:10], token_address[:10],
        )

        cluster_buys[chain_id][token_address].append(
            {
                "wallet": to_addr,
                "wallet_type": wallet_type,
                "timestamp": block_ts,
                "last_ts": last_ts,
                "tx_hash": tx_hash,
                "funding_source": funding_source,
                "estimated_eth": 0.01,  # placeholder — P0 fix
            }
        )


# ---------------------------------------------------------------------------
# Solana — Solscan v2 API helpers
# ---------------------------------------------------------------------------

SOLSCAN_HEADERS = {}


def _init_solscan_headers() -> None:
    global SOLSCAN_HEADERS
    SOLSCAN_HEADERS = {"token": SOLSCAN_API_KEY}


def solscan_get(endpoint: str, params: dict | None = None) -> dict | None:
    """GET request to Solscan Pro v2 API."""
    if not SOLSCAN_HEADERS:
        _init_solscan_headers()
    url = f"{SOLSCAN_BASE_URL}{endpoint}"
    try:
        r = requests.get(url, params=params or {}, headers=SOLSCAN_HEADERS, timeout=15)
        r.raise_for_status()
        data = r.json()
        if data.get("success") is True or "data" in data:
            return data
        log.debug("[solana] Solscan non-success: %s", data)
        return data  # return anyway, some endpoints don't have 'success'
    except Exception as exc:
        log.warning("[solana] Solscan request failed: %s %s", endpoint, exc)
        return None


def solscan_get_account_transfers(account: str, page: int = 1, page_size: int = 20) -> list[dict]:
    """Get recent token transfers for a Solana account."""
    data = solscan_get(
        f"/account/transfer",
        params={
            "address": account,
            "page": page,
            "page_size": page_size,
            "exclude_amount_zero": "true",
        },
    )
    if data and "data" in data:
        return data["data"] if isinstance(data["data"], list) else []
    return []


def solscan_get_defi_activities(page: int = 1, page_size: int = 100) -> list[dict]:
    """
    Get recent Solana DEX swap activities by querying /token/defi/activities for WSOL.
    Nearly every Solana token swap involves WSOL on one side, so this acts as
    a broad feed of all DEX swap activity. We pass platform[] to filter to our
    known DEX programs (Raydium, Orca, Jupiter, Pump.fun).

    The (non-existent) global /defi/activities endpoint has been replaced with
    /token/defi/activities?address=WSOL, which is the correct v2 approach.
    """
    # Build platform[] params as a list of (key, value) tuples so requests
    # sends multiple values for the same key name.
    params = [
        ("address", WSOL_MINT),
        ("activity_type[]", "ACTIVITY_TOKEN_SWAP"),
        ("activity_type[]", "ACTIVITY_AGG_TOKEN_SWAP"),
        ("page", page),
        ("page_size", page_size),
        ("sort_by", "block_time"),
        ("sort_order", "desc"),
    ]
    for program_id in SOLANA_DEX_PROGRAMS:
        params.append(("platform[]", program_id))

    url = f"{SOLSCAN_BASE_URL}/token/defi/activities"
    try:
        r = requests.get(url, params=params, headers=SOLSCAN_HEADERS, timeout=15)
        r.raise_for_status()
        data = r.json()
    except Exception as exc:
        log.warning("[solana] Solscan /token/defi/activities failed: %s", exc)
        return []

    if not data or "data" not in data:
        log.debug("[solana] No data in response: %s", data)
        return []

    activities = data["data"] if isinstance(data["data"], list) else []
    log.debug("[solana] Got %d swap activities from /token/defi/activities", len(activities))
    return activities


def solscan_get_wallet_first_last_tx(address: str) -> dict | None:
    """
    Get first and last transaction timestamps for a Solana wallet.
    Uses the /account/transactions endpoint. Cached.
    """
    cache = wallet_cache["solana"]
    if address in cache:
        return cache[address]

    # Get earliest transaction
    first_data = solscan_get(
        "/account/transactions",
        params={
            "address": address,
            "page": 1,
            "page_size": 1,
            "sort_by": "block_time",
            "sort_order": "asc",
        },
    )
    # Get latest transaction
    last_data = solscan_get(
        "/account/transactions",
        params={
            "address": address,
            "page": 1,
            "page_size": 1,
            "sort_by": "block_time",
            "sort_order": "desc",
        },
    )

    first_ts = None
    last_ts = None

    if first_data and "data" in first_data:
        items = first_data["data"] if isinstance(first_data["data"], list) else []
        if items:
            first_ts = items[0].get("block_time") or items[0].get("blockTime")
    if last_data and "data" in last_data:
        items = last_data["data"] if isinstance(last_data["data"], list) else []
        if items:
            last_ts = items[0].get("block_time") or items[0].get("blockTime")

    if first_ts is None:
        return None

    result = {"first_ts": int(first_ts), "last_ts": int(last_ts or first_ts)}
    cache[address] = result
    return result


def classify_solana_wallet(address: str) -> str | None:
    """Classify a Solana wallet as fresh, dormant, or uninteresting."""
    ts = solscan_get_wallet_first_last_tx(address)
    if not ts:
        return None

    now = NOW_TS()
    age_hours = (now - ts["first_ts"]) / 3600
    inactive_days = (now - ts["last_ts"]) / 86400

    if age_hours <= FRESH_WALLET_MAX_AGE_HOURS:
        return "fresh"
    if inactive_days >= DORMANT_WALLET_MIN_INACTIVE_DAYS:
        return "dormant"
    return None


def get_solana_funding_source(address: str) -> str | None:
    """Check first few transfers to see if wallet was funded by a known CEX."""
    transfers = solscan_get_account_transfers(address, page=1, page_size=5)
    for tx in transfers:
        sender = tx.get("from_address") or tx.get("src") or ""
        for known_addr, label in SOLANA_KNOWN_FUNDING_SOURCES.items():
            if sender == known_addr:
                return label
    return None


def get_solana_token_info(token_mint: str) -> dict:
    """Fetch token name/symbol from Solscan. Cached."""
    cache = token_info_cache["solana"]
    if token_mint in cache:
        return cache[token_mint]

    data = solscan_get(f"/token/meta", params={"address": token_mint})
    if data and "data" in data:
        meta = data["data"]
        info = {
            "name": meta.get("name") or "Unknown",
            "symbol": meta.get("symbol") or "???",
        }
    else:
        info = {"name": "Unknown", "symbol": "???"}

    cache[token_mint] = info
    return info


def build_solana_alert(token_address: str, buys: list[dict]) -> str:
    """Build a Telegram alert message for a Solana token cluster."""
    info = get_solana_token_info(token_address)
    token_name = info["name"]
    token_symbol = info["symbol"]
    short_addr = token_address[:6] + "..." + token_address[-4:]
    explorer_url = f"https://solscan.io/token/{token_address}"
    dexscreener_url = f"https://dexscreener.com/solana/{token_address}"

    fresh_buys = [b for b in buys if b["wallet_type"] == "fresh"]
    dormant_buys = [b for b in buys if b["wallet_type"] == "dormant"]

    if buys:
        earliest_ts = min(b["timestamp"] for b in buys)
        speed_cutoff = earliest_ts + SPEED_WINDOW_MINUTES * 60
        speed_count = sum(1 for b in buys if b["timestamp"] <= speed_cutoff)
    else:
        speed_count = 0

    score, tier = score_cluster(len(fresh_buys), len(dormant_buys), len(buys), speed_count)

    wallet_lines = []
    for b in fresh_buys:
        short_w = b["wallet"][:6] + "..." + b["wallet"][-4:]
        funding = b.get("funding_source") or "unknown"
        tx_url = f"https://solscan.io/tx/{b['tx_hash']}"
        wallet_lines.append(f"  🆕 <a href='{tx_url}'>{short_w}</a> (funded via {funding})")

    for b in dormant_buys:
        short_w = b["wallet"][:6] + "..." + b["wallet"][-4:]
        tx_url = f"https://solscan.io/tx/{b['tx_hash']}"
        inactive_days = int((NOW_TS() - b["last_ts"]) / 86400)
        wallet_lines.append(f"  💤 <a href='{tx_url}'>{short_w}</a> (dormant {inactive_days}d)")

    wallets_str = "\n".join(wallet_lines)
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    summary_parts = []
    if fresh_buys:
        summary_parts.append(f"{len(fresh_buys)} fresh 🆕")
    if dormant_buys:
        summary_parts.append(f"{len(dormant_buys)} dormant 💤")
    summary = " + ".join(summary_parts)

    speed_str = f"{speed_count} wallets in first {SPEED_WINDOW_MINUTES}min" if speed_count > 1 else ""

    return (
        f"{tier} <b>PiggyBac Alert</b> [Solana] — {ts}\n\n"
        f"<b>Token:</b> {token_name} ({token_symbol})\n"
        f"<b>Address:</b> <a href='{explorer_url}'>{short_addr}</a>\n"
        f"<b>Wallets:</b> {summary}\n"
        + (f"<b>Speed:</b> {speed_str}\n" if speed_str else "")
        + f"\n{wallets_str}\n\n"
        f"<a href='{dexscreener_url}'>DexScreener</a> | "
        f"<a href='{explorer_url}'>Solscan</a>"
    )


def check_solana_clusters() -> None:
    """Check Solana cluster_buys for alertable clusters."""
    for token_address, buys in cluster_buys["solana"].items():
        if len(buys) < CLUSTER_MIN_WALLETS:
            continue
        seen = {}
        for b in buys:
            seen[b["wallet"]] = b
        unique_buys = list(seen.values())
        if len(unique_buys) < CLUSTER_MIN_WALLETS:
            continue

        cluster_key = "solana" + token_address + str(
            sorted(b["wallet"] for b in unique_buys)
        )
        if cluster_key in alerted_clusters:
            continue
        alerted_clusters.add(cluster_key)

        fresh_count = sum(1 for b in unique_buys if b["wallet_type"] == "fresh")
        dormant_count = sum(1 for b in unique_buys if b["wallet_type"] == "dormant")
        log.info(
            "[solana] CLUSTER: %d fresh + %d dormant wallets bought %s",
            fresh_count, dormant_count, token_address,
        )
        message = build_solana_alert(token_address, unique_buys)
        send_telegram(message)


def process_solana_swaps(activities: list[dict]) -> None:
    """Process Solana DEX swap activities, classify wallets, record buys."""
    global solana_last_seen_sig
    now = NOW_TS()
    new_last_sig = solana_last_seen_sig
    processed = 0

    for act in activities:
        tx_hash = act.get("trans_id") or act.get("tx_hash") or ""
        if not tx_hash:
            continue

        # Skip if we've already seen this
        if tx_hash == solana_last_seen_sig:
            break

        if processed == 0:
            new_last_sig = tx_hash

        block_time = act.get("block_time") or act.get("blockTime") or now

        # Extract the signer/wallet
        wallet = act.get("from_address") or act.get("signer") or ""
        if not wallet:
            continue

        # Extract tokens involved — look for the token being bought (not SOL/WSOL)
        # Activity format varies; handle token1/token2 or routed_token fields
        token_bought = None

        # Try structured token fields
        token1 = act.get("token1") or ""
        token2 = act.get("token2") or ""
        # In a swap, one side is usually SOL/WSOL — the other is the token bought
        if token1 and token1 != WSOL_MINT:
            token_bought = token1
        elif token2 and token2 != WSOL_MINT:
            token_bought = token2

        # Try routed format
        if not token_bought:
            routed = act.get("routed_token") or act.get("token_address") or ""
            if routed and routed != WSOL_MINT:
                token_bought = routed

        if not token_bought:
            continue

        # Classify the wallet
        wallet_type = classify_solana_wallet(wallet)
        if wallet_type is None:
            processed += 1
            continue

        funding_source = None
        last_ts = now
        ts_data = wallet_cache["solana"].get(wallet)
        if ts_data:
            last_ts = ts_data["last_ts"]

        if wallet_type == "fresh":
            funding_source = get_solana_funding_source(wallet)

        dex_label = ""
        platform = act.get("platform") or act.get("program_id") or ""
        if platform in SOLANA_DEX_PROGRAMS:
            dex_label = SOLANA_DEX_PROGRAMS[platform]

        log.info(
            "[solana] %s wallet buy: wallet=%s token=%s dex=%s",
            wallet_type, wallet[:10], token_bought[:10], dex_label,
        )

        cluster_buys["solana"][token_bought].append(
            {
                "wallet": wallet,
                "wallet_type": wallet_type,
                "timestamp": int(block_time),
                "last_ts": last_ts,
                "tx_hash": tx_hash,
                "funding_source": funding_source,
                "dex": dex_label,
            }
        )
        processed += 1

    solana_last_seen_sig = new_last_sig
    return processed


def prune_solana_old_buys() -> None:
    cutoff = NOW_TS() - CLUSTER_TIME_WINDOW_HOURS * 3600
    buys = cluster_buys["solana"]
    for token in list(buys.keys()):
        buys[token] = [b for b in buys[token] if b["timestamp"] >= cutoff]
        if not buys[token]:
            del buys[token]


def scan_solana() -> None:
    """Main Solana scanner loop — polls Solscan /token/defi/activities for WSOL swaps."""
    key_preview = (SOLSCAN_API_KEY[:8] + "..." + SOLSCAN_API_KEY[-4:]) if len(SOLSCAN_API_KEY) > 12 else f"(len={len(SOLSCAN_API_KEY)})"
    log.info("[Solana] Starting up... API key preview: %s", key_preview)
    _init_solscan_headers()

    while True:
        try:
            activities = solscan_get_defi_activities(page=1, page_size=100)
            log.info("[Solana] Got %d swap activities from DEXes", len(activities))

            count = process_solana_swaps(activities)
            log.info("[Solana] Processed %d new swaps", count or 0)

            prune_solana_old_buys()
            check_solana_clusters()

        except Exception as exc:
            log.error("[solana] Error: %s", exc, exc_info=True)

        time.sleep(SOLANA_POLL_INTERVAL_SECONDS)


# ---------------------------------------------------------------------------
# Per-chain scan loop (EVM)
# ---------------------------------------------------------------------------


def scan_chain(chain_id: str) -> None:
    chain = CHAINS[chain_id]
    log.info("[%s] Starting up...", chain["name"])
    last_block_checked[chain_id] = get_latest_block(chain_id)
    log.info("[%s] Starting from block %d", chain["name"], last_block_checked[chain_id])

    while True:
        try:
            current_block = get_latest_block(chain_id)
            if current_block <= last_block_checked[chain_id]:
                time.sleep(POLL_INTERVAL_SECONDS)
                continue

            log.info(
                "[%s] Scanning blocks %d → %d",
                chain["name"],
                last_block_checked[chain_id] + 1,
                current_block,
            )
            transfers = get_erc20_transfers(
                chain_id, last_block_checked[chain_id] + 1, current_block
            )
            log.info("[%s] Got %d transfer events", chain["name"], len(transfers))

            process_transfers(chain_id, transfers)
            prune_old_buys(chain_id)
            check_for_clusters(chain_id)

            last_block_checked[chain_id] = current_block

        except Exception as exc:
            log.error("[%s] Error: %s", chain_id, exc, exc_info=True)

        time.sleep(POLL_INTERVAL_SECONDS)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    threads = []

    # Start EVM chain scanners (Base + Ethereum)
    if API_KEY:
        for chain_id in CHAINS:
            t = threading.Thread(target=scan_chain, args=(chain_id,), daemon=True)
            t.start()
            threads.append(t)
            time.sleep(2)  # stagger startup to avoid API rate limit spike
    else:
        log.warning("BASESCAN_API_KEY not set — skipping EVM chains")

    # Start Solana scanner
    if SOLSCAN_API_KEY:
        t = threading.Thread(target=scan_solana, daemon=True)
        t.start()
        threads.append(t)
        log.info("Solana scanner thread started")
    else:
        log.warning("SOLSCAN_API_KEY not set — skipping Solana")

    if not threads:
        log.error("No API keys configured. Set BASESCAN_API_KEY and/or SOLSCAN_API_KEY.")
        return

    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        log.info("Shutting down.")


if __name__ == "__main__":
    main()
