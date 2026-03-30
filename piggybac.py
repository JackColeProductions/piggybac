"""
PiggyBac — Multi-chain fresh-wallet + dormant-wallet buy detector.
Monitors Base, Ethereum mainnet, and Solana for coordinated buying by:
  - Fresh wallets: created/funded in the last 24h
  - Dormant wallets: last active 6+ months ago, now suddenly buying

Signal is scored and tiered. Mixed fresh+dormant clusters are flagged as
the strongest signal.
"""

import os
import re
import time
import logging
import threading
import concurrent.futures
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

API_KEY = os.getenv("BASESCAN_API_KEY", "").strip()  # kept for fallback, unused if Alchemy set
ALCHEMY_ETH_KEY = os.getenv("ALCHEMY_ETH_API_KEY", "").strip()
ALCHEMY_BASE_KEY = os.getenv("ALCHEMY_BASE_API_KEY", "").strip()
HELIUS_API_KEY = os.getenv("HELIUS_API_KEY", "").strip().strip('"\'')
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

# Set ENABLE_EVM_CHAINS=true in Railway env vars to re-enable Base + Ethereum scanning.
# Disabled by default until Alchemy quota resets — public RPCs rate-limit under this load.
ENABLE_EVM_CHAINS: bool = os.getenv("ENABLE_EVM_CHAINS", "false").lower() == "true"

FRESH_WALLET_MAX_AGE_HOURS = 24        # wallet created <24h ago = fresh
DORMANT_WALLET_MIN_INACTIVE_DAYS = 90  # last active 3+ months ago = dormant (ETH wallets rarely hit 180d)
CLUSTER_MIN_WALLETS = 7                # raised from 5 — higher conviction threshold
CLUSTER_TIME_WINDOW_MINUTES = 10       # ALL those wallets must buy within this window
POLL_INTERVAL_SECONDS = 20             # scan every 20s to stay near real-time
TOKEN_MAX_AGE_HOURS = 6                # skip tokens launched more than 6h ago
MIN_LIQUIDITY_USD = 2_000              # skip tokens with < $2k liquidity (Pump.fun starts near zero)
MIN_SELLS_H1 = 1                       # skip tokens with 0 sells in last hour (honeypot filter)
MIN_MCAP_USD = 5_000                   # skip tokens below $5K mcap (sub-$2K consistently flatlines)
MIN_BUYS_H1 = 75                       # skip tokens with < 75 buys/h (dead market)
HIGH_WALLET_THRESHOLD = 30             # warn if 30+ wallets all unknown funding
NAME_HISTORY_TTL_HOURS = 12            # how long to remember alerted token names
WALLET_HISTORY_TTL_HOURS = 12          # how long to remember cluster wallets
CONFIRM_PING_DELAY_SECONDS = 180       # 3-minute post-alert price check

# Wave 2: Wallet intelligence
DEPLOYER_LOW_BALANCE_SOL = 2.0         # warn if deployer has < 2 SOL
DEPLOYER_HIGH_HOLDINGS_PCT = 20.0      # warn if deployer holds > 20% of supply
MAX_WALLETS_TO_PROFILE = 3             # max dormant wallets to PnL-profile per alert
MAX_TOKENS_PER_WALLET = 5              # max historical tokens to score per wallet
ENRICHMENT_TIMEOUT_SECONDS = 12        # hard timeout for entire enrichment pipeline
MIN_CONVICTION_TO_ALERT = 20           # suppress alerts with conviction score below this
WALLET_PROFILE_CACHE_TTL = 86400       # 24h — wallet history doesn't change fast

ERC20_TRANSFER_TOPIC = (
    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
)

# Uniswap V3 Swap(address indexed sender, address indexed recipient, ...)
UNISWAP_V3_SWAP_TOPIC = (
    "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"
)
# Uniswap V2 Swap(address indexed sender, uint amount0In, uint amount1In, uint amount0Out, uint amount1Out, address indexed to)
UNISWAP_V2_SWAP_TOPIC = (
    "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"
)

# WETH address per chain (the token we treat as "native" — buying it = selling, not buying)
WETH_ADDRESSES = {
    "ethereum": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
    "base":     "0x4200000000000000000000000000000000000006",
}

# ---------------------------------------------------------------------------
# Solana config
# ---------------------------------------------------------------------------

HELIUS_API_URL = "https://api.helius.xyz/v0"
HELIUS_RPC_URL = "https://mainnet.helius-rpc.com"  # api-key appended at call time

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

ALCHEMY_URLS = {
    "ethereum": "https://eth-mainnet.g.alchemy.com/v2/",
    "base":     "https://base-mainnet.g.alchemy.com/v2/",
}

ALCHEMY_KEYS = {
    "ethereum": ALCHEMY_ETH_KEY,
    "base":     ALCHEMY_BASE_KEY,
}

# Public RPC endpoints (no API key required) — used when Alchemy quota is exhausted
PUBLIC_RPC_URLS = {
    "base":     "https://mainnet.base.org",
    "ethereum": "https://eth.llamarpc.com",
}

# Block explorer API config — used for wallet history + token metadata without Alchemy
ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY", "").strip()
EXPLORER_API_URLS = {
    "base":     "https://api.basescan.org/api",
    "ethereum": "https://api.etherscan.io/api",
}
EXPLORER_API_KEYS = {
    "base":     API_KEY,            # BASESCAN_API_KEY
    "ethereum": ETHERSCAN_API_KEY,
}


def alchemy_url(chain_id: str) -> str:
    return ALCHEMY_URLS[chain_id] + ALCHEMY_KEYS[chain_id]


CHAINS = {
    "base": {
        "name": "Base",
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

# chain_id -> pool_address -> (token0, token1) — populated via eth_call
pool_tokens_cache: dict[str, dict[str, tuple]] = {chain: {} for chain in CHAINS}

# chain_id -> wallet_address -> {first_ts, last_ts}
wallet_cache: dict[str, dict[str, dict]] = {chain: {} for chain in ALL_CHAIN_IDS}

# chain_id -> token_address -> {name, symbol}
token_info_cache: dict[str, dict[str, dict]] = {chain: {} for chain in ALL_CHAIN_IDS}

# set of cluster keys already alerted
alerted_clusters: set[str] = set()

# chain_id -> token_address -> last alert timestamp (Unix)
# Prevents re-alerting the same token within ALERT_COOLDOWN_SECONDS
token_last_alerted: dict[str, dict[str, int]] = {chain: {} for chain in ALL_CHAIN_IDS}
ALERT_COOLDOWN_SECONDS = 6 * 3600  # 6h fallback cooldown (session restart safety net)

# token_address -> age_hours (None = lookup failed / treat as unknown)
token_age_cache: dict[str, float | None] = {}

# token_address -> {name, symbol} cached from DexScreener (populated alongside age lookup)
dexscreener_name_cache: dict[str, dict] = {}

# token_address -> liquidity USD (None = unknown)
token_liquidity_cache: dict[str, float | None] = {}

# token_address -> sell count in last 1h (None = unknown)
token_sells_h1_cache: dict[str, int | None] = {}

# token_address -> price in USD at time of first DexScreener lookup
token_price_cache: dict[str, float | None] = {}

# token_address -> market cap USD at time of first DexScreener lookup
token_mcap_cache: dict[str, float | None] = {}

# token_address -> buy count in last 1h (from DexScreener txns.h1.buys)
token_buys_h1_cache: dict[str, int | None] = {}

# token_address -> {"website": url|None, "twitter": url|None}
token_socials_cache: dict[str, dict] = {}

# token_address -> top-10-holder % (float 0-100) or None
token_top_holders_cache: dict[str, float | None] = {}

# Tokens awaiting 24h performance review
# token_address -> {chain_id, network, alert_ts, price_usd, mcap_usd, token_name, token_symbol, explorer_url}
pending_reviews: dict[str, dict] = {}

# Improvement 2: Token name re-launch tracking
# normalized_name -> [{"name": str, "contract": str, "chain": str, "timestamp": float}]
recent_token_names: dict[str, list[dict]] = {}

# Improvement 4: Cluster wallet history
# wallet_address -> [{"token_name": str, "contract": str, "chain": str, "timestamp": float}]
wallet_cluster_history: dict[str, list[dict]] = {}

# Improvement 5: Raw wallet funder address cache (populated as side effect of funding source lookups)
# wallet_address -> funder_address (str) or None
wallet_funder_cache: dict[str, str | None] = {}

# Wave 2 caches
# token_address -> deployer wallet address (permanent — never changes)
token_deployer_cache: dict[str, str | None] = {}
# wallet_address -> (profile_dict, timestamp) — 24h TTL
wallet_profile_cache: dict[str, tuple[dict, float]] = {}

# chain_id -> last block processed (EVM chains only)
last_block_checked: dict[str, int] = {chain: 0 for chain in CHAINS}

# Solana: last seen tx signature per DEX program to avoid re-processing
solana_program_last_sig: dict[str, str] = {}

# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------

def fmt_price(v: float | None) -> str:
    """Format a USD price for display."""
    if v is None:
        return "N/A"
    if v < 0.000001:
        return f"${v:.2e}"
    if v < 0.01:
        return f"${v:.8f}"
    return f"${v:,.4f}"


def fmt_mcap(v: float | None) -> str:
    """Format a USD market cap for display."""
    if v is None:
        return "N/A"
    if v >= 1_000_000:
        return f"${v / 1_000_000:.2f}M"
    if v >= 1_000:
        return f"${v / 1_000:.1f}K"
    return f"${v:,.0f}"


# ---------------------------------------------------------------------------
# Improvement 2: Token name re-launch detection
# ---------------------------------------------------------------------------

def normalize_token_name(name: str) -> str:
    """Strip to lowercase alphanumeric only for fuzzy name matching."""
    return re.sub(r"[^a-z0-9]", "", name.lower().strip())


def _purge_old_name_history() -> None:
    cutoff = NOW_TS() - NAME_HISTORY_TTL_HOURS * 3600
    for key in list(recent_token_names.keys()):
        recent_token_names[key] = [e for e in recent_token_names[key] if e["timestamp"] > cutoff]
        if not recent_token_names[key]:
            del recent_token_names[key]


def check_name_relaunch(token_name: str, token_address: str, chain: str) -> str | None:
    """Return a warning line if this token name was alerted before, else None."""
    _purge_old_name_history()
    key = normalize_token_name(token_name)
    if not key:
        return None
    prev = [e for e in recent_token_names.get(key, []) if e["contract"] != token_address]
    if not prev:
        return None
    attempt = len(prev) + 1
    last = prev[-1]
    age_min = int((NOW_TS() - last["timestamp"]) / 60)
    age_str = f"{age_min}min ago" if age_min < 120 else f"{age_min // 60}h ago"
    suffix = " — may be the committed pump." if attempt >= 3 else ""
    return f"🔄 Re-launch #{attempt} of <b>{token_name}</b> (prev: {age_str}){suffix}"


def record_token_name(token_name: str, token_address: str, chain: str) -> None:
    key = normalize_token_name(token_name)
    if not key:
        return
    if key not in recent_token_names:
        recent_token_names[key] = []
    recent_token_names[key].append({"name": token_name, "contract": token_address, "chain": chain, "timestamp": NOW_TS()})


# ---------------------------------------------------------------------------
# Improvement 4: Cluster wallet memory + overlap detection
# ---------------------------------------------------------------------------

def _purge_old_wallet_history() -> None:
    cutoff = NOW_TS() - WALLET_HISTORY_TTL_HOURS * 3600
    for w in list(wallet_cluster_history.keys()):
        wallet_cluster_history[w] = [e for e in wallet_cluster_history[w] if e["timestamp"] > cutoff]
        if not wallet_cluster_history[w]:
            del wallet_cluster_history[w]


def check_wallet_overlap(wallets: list[str], token_address: str) -> str | None:
    """Return overlap warning if wallets in this cluster appeared in a previous cluster."""
    _purge_old_wallet_history()
    now = NOW_TS()
    overlap: dict[str, list[dict]] = {}
    for w in wallets:
        for entry in wallet_cluster_history.get(w, []):
            if entry["contract"] == token_address:
                continue
            if now - entry["timestamp"] < 60:
                continue
            prev_ca = entry["contract"]
            if prev_ca not in overlap:
                overlap[prev_ca] = []
            overlap[prev_ca].append(entry)

    if not overlap:
        return None

    best_ca = max(overlap, key=lambda ca: overlap[ca][-1]["timestamp"])
    entries = overlap[best_ca]
    count = len(set(e.get("wallet", "") for e in entries))
    last = entries[-1]
    age_min = int((now - last["timestamp"]) / 60)
    age_str = f"{age_min}min ago" if age_min < 120 else f"{age_min // 60}h ago"
    name = last.get("token_name", best_ca[:10])
    return f"🔗 {count} wallet{'s' if count > 1 else ''} also in <b>{name}</b> cluster ({age_str})"


def record_cluster_wallets(token_name: str, token_address: str, chain: str, wallets: list[str]) -> None:
    now = NOW_TS()
    for w in wallets:
        if w not in wallet_cluster_history:
            wallet_cluster_history[w] = []
        wallet_cluster_history[w].append({
            "wallet": w, "token_name": token_name, "contract": token_address,
            "chain": chain, "timestamp": now,
        })


# ---------------------------------------------------------------------------
# Improvement 5: Funder clustering (reuses existing API call data)
# ---------------------------------------------------------------------------

def analyze_funder_clustering(buys: list[dict]) -> str | None:
    """
    Group fresh wallets by their raw funder address. Returns a summary line or None.
    """
    funders: dict[str, list[str]] = {}
    for b in buys:
        if b.get("wallet_type") != "fresh":
            continue
        w = b["wallet"]
        funder = wallet_funder_cache.get(w)
        if funder is None:
            continue
        if funder not in funders:
            funders[funder] = []
        funders[funder].append(w)

    if not funders:
        return None

    total_fresh = sum(1 for b in buys if b.get("wallet_type") == "fresh")
    max_group = max(funders.values(), key=len)

    if len(max_group) >= 3:
        short = max_group[0][:6] + "..." + max_group[0][-4:]
        return f"💰 {len(max_group)}/{total_fresh} wallets funded by same source ({short})"
    if len(funders) >= 3:
        return f"💰 {len(funders)} different funders — diverse funding ✅"
    return None


def score_cluster(
    fresh_count: int,
    dormant_count: int,
    total: int,
    speed_count: int,
    all_funding_unknown: bool = False,
) -> tuple[int, str]:
    """
    Returns (score, tier_emoji).
    Tier:
      🔥    — basic signal (7+ wallets, mostly fresh)
      🔥🔥  — strong signal (10+ wallets OR mixed fresh+dormant)
      🔥🔥🔥 — massive cook (20+ wallets OR heavy mix)

    If all_funding_unknown=True AND total >= HIGH_WALLET_THRESHOLD, tier is
    capped at 🔥 — large clusters with 100% unknown funding are likely fake.
    """
    score = total

    # Mixed fresh+dormant is the strongest signal
    if fresh_count > 0 and dormant_count > 0:
        score += dormant_count * 3

    # Speed bonus
    if speed_count >= 3:
        score += speed_count * 2

    if total >= 20 or (fresh_count > 0 and dormant_count >= 3):
        tier = "🔥🔥🔥"
    elif total >= 10 or (fresh_count > 0 and dormant_count > 0):
        tier = "🔥🔥"
    else:
        tier = "🔥"

    # Cap tier for suspicious large clusters (Improvement 3)
    if all_funding_unknown and total >= HIGH_WALLET_THRESHOLD:
        tier = "🔥"

    return score, tier


# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------

NOW_TS = lambda: int(time.time())


# Methods that are Alchemy-specific and can't be called on public RPCs
_ALCHEMY_SPECIFIC = frozenset({"alchemy_getAssetTransfers", "alchemy_getTokenMetadata"})


def alchemy_rpc(chain_id: str, method: str, params: list) -> object:
    """JSON-RPC call — Alchemy first, automatic public RPC fallback on 429."""
    url = alchemy_url(chain_id) if ALCHEMY_KEYS[chain_id] else PUBLIC_RPC_URLS[chain_id]
    try:
        r = requests.post(
            url,
            json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
            timeout=10,
        )
        # On Alchemy quota exceeded (429), fall back to public RPC for standard methods
        if r.status_code == 429 and ALCHEMY_KEYS[chain_id] and method not in _ALCHEMY_SPECIFIC:
            log.debug("[%s] Alchemy quota exceeded for %s — retrying via public RPC", chain_id, method)
            r = requests.post(
                PUBLIC_RPC_URLS[chain_id],
                json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
                timeout=10,
            )
        if not r.ok:
            log.warning("[%s] EVM RPC %s HTTP %d: %s", chain_id, method, r.status_code, r.text[:300])
            return None
        data = r.json()
        if "error" in data:
            log.warning("[%s] EVM RPC %s error: %s", chain_id, method, data["error"])
            return None
        return data.get("result")
    except Exception as exc:
        log.warning("[%s] EVM RPC %s failed: %s", chain_id, method, exc)
        return None


def _parse_alchemy_ts(iso_str: str) -> int:
    """Parse Alchemy blockTimestamp ISO string to Unix int."""
    # format: "2024-01-01T00:00:00.000Z"
    try:
        return int(datetime.strptime(iso_str[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc).timestamp())
    except Exception:
        return 0


def get_latest_block(chain_id: str) -> int:
    result = alchemy_rpc(chain_id, "eth_blockNumber", [])
    return int(result, 16) if result else 0


MAX_BLOCKS_PER_SCAN = 5  # keeps eth_getLogs result set small on high-throughput chains like Base


def get_swap_logs(chain_id: str, from_block: int, to_block: int) -> list[dict]:
    """
    Fetch Uniswap V2 + V3 Swap event logs for a block range.
    Chunked to MAX_BLOCKS_PER_SCAN to stay within Alchemy result limits.
    Each returned log is tagged with '_swap_version': 'v2' or 'v3'.
    """
    all_results = []
    chunk_start = from_block
    while chunk_start <= to_block:
        chunk_end = min(chunk_start + MAX_BLOCKS_PER_SCAN - 1, to_block)
        for version, topic in [("v3", UNISWAP_V3_SWAP_TOPIC), ("v2", UNISWAP_V2_SWAP_TOPIC)]:
            result = alchemy_rpc(chain_id, "eth_getLogs", [{
                "fromBlock": hex(chunk_start),
                "toBlock": hex(chunk_end),
                "topics": [topic],
            }])
            if isinstance(result, list):
                for log in result:
                    log["_swap_version"] = version
                all_results.extend(result)
        chunk_start = chunk_end + 1
    return all_results


def get_pool_tokens(chain_id: str, pool_address: str) -> tuple[str, str] | None:
    """
    Returns (token0, token1) for a Uniswap V2/V3 pool via eth_call.
    Both use the same token0()/token1() ABI so one helper covers both.
    Results are cached to avoid repeated calls for the same pool.
    """
    cache = pool_tokens_cache[chain_id]
    addr = pool_address.lower()
    if addr in cache:
        return cache[addr]

    def call_selector(selector: str) -> str | None:
        result = alchemy_rpc(chain_id, "eth_call", [
            {"to": pool_address, "data": selector}, "latest"
        ])
        # Returns 32-byte ABI-encoded address: "0x" + 24 zero chars + 40 hex chars
        if result and len(result) == 66:
            return "0x" + result[26:].lower()
        return None

    t0 = call_selector("0x0dfe1681")  # token0()
    t1 = call_selector("0xd21220a7")  # token1()
    if t0 and t1:
        cache[addr] = (t0, t1)
        return (t0, t1)
    # Pool doesn't have token0()/token1() — not a standard Uni V2/V3 pool (Curve, Balancer, etc.)
    log.debug("[%s] pool %s has no token0/token1 — skipping", chain_id, pool_address[:10])
    cache[addr] = None  # type: ignore[assignment]  # cache miss to avoid re-querying
    return None


def _to_int256(raw: int) -> int:
    """Convert a 256-bit unsigned integer to signed int256 (two's complement)."""
    return raw - (1 << 256) if raw >= (1 << 255) else raw


def decode_swap_log(chain_id: str, log: dict) -> tuple[str, str] | None:
    """
    Decode a Uniswap V2 or V3 Swap log.
    Returns (recipient_address, token_bought_address) or None if undecidable.
    Filters out swaps where the user is buying WETH (i.e., selling a token).
    """
    topics = log.get("topics", [])
    if len(topics) < 3:
        return None

    # topics[2] is the recipient (indexed) for both V2 and V3
    recipient = "0x" + topics[2][-40:].lower()
    pool_address = log.get("address", "")
    tokens = get_pool_tokens(chain_id, pool_address)
    if not tokens:
        return None
    token0, token1 = tokens
    weth = WETH_ADDRESSES.get(chain_id, "")

    data = log.get("data", "")
    version = log.get("_swap_version", "v3")

    if version == "v3":
        # data = amount0 (int256 32B) + amount1 (int256 32B) + ...
        if len(data) < 130:  # "0x" + 128 hex chars
            return None
        amount0 = _to_int256(int(data[2:66], 16))
        amount1 = _to_int256(int(data[66:130], 16))
        # Negative amount = tokens flowed OUT of pool = user received (bought) that token
        if amount0 < 0:
            bought = token0
        elif amount1 < 0:
            bought = token1
        else:
            return None

    else:  # v2
        # data = amount0In + amount1In + amount0Out + amount1Out (uint256 x4)
        if len(data) < 258:  # "0x" + 256 hex chars
            return None
        amount0_out = int(data[130:194], 16)
        amount1_out = int(data[194:258], 16)
        if amount0_out > 0:
            bought = token0
        elif amount1_out > 0:
            bought = token1
        else:
            return None

    # Skip if the bought token is WETH — that means the user is selling, not buying
    if bought.lower() == weth.lower():
        return None

    return (recipient, bought)


def _explorer_tokentx(chain_id: str, direction: str, address: str,
                       order: str = "asc", max_count: int = 1) -> list[dict]:
    """
    Basescan/Etherscan tokentx fallback for _alchemy_asset_transfers.
    Returns transfers normalized to Alchemy-compatible format.
    """
    api_url = EXPLORER_API_URLS[chain_id]
    api_key = EXPLORER_API_KEYS[chain_id]
    sort = "asc" if order == "asc" else "desc"
    page_size = min(max(max_count * 4, 20), 100)
    try:
        params: dict = {
            "module": "account",
            "action": "tokentx",
            "address": address,
            "sort": sort,
            "page": 1,
            "offset": page_size,
        }
        if api_key:
            params["apikey"] = api_key
        r = requests.get(api_url, params=params, timeout=10)
        r.raise_for_status()
        data = r.json()
        if data.get("status") not in ("1", 1) or not data.get("result"):
            return []
        txs = data["result"]
    except Exception as exc:
        log.debug("[%s] Explorer tokentx failed for %s: %s", chain_id, address[:10], exc)
        return []

    addr_lower = address.lower()
    is_incoming = direction == "toAddress"
    filtered: list[dict] = []
    for tx in txs:
        if is_incoming and tx.get("to", "").lower() == addr_lower:
            filtered.append(tx)
        elif not is_incoming and tx.get("from", "").lower() == addr_lower:
            filtered.append(tx)
        if len(filtered) >= max_count:
            break

    # Normalize to Alchemy-compatible format so callers don't need to change
    normalized = []
    for tx in filtered:
        ts_unix = int(tx.get("timeStamp") or 0)
        ts_iso = (datetime.fromtimestamp(ts_unix, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
                  if ts_unix else "")
        normalized.append({
            "from": tx.get("from", ""),
            "to": tx.get("to", ""),
            "asset": tx.get("tokenSymbol", ""),
            "rawContract": {"address": tx.get("contractAddress", "")},
            "metadata": {"blockTimestamp": ts_iso},
        })
    return normalized


def _alchemy_asset_transfers(chain_id: str, direction: str, address: str,
                              order: str = "asc", max_count: int = 1) -> list[dict]:
    """Helper: get asset transfers in `direction` (fromAddress/toAddress) for an address."""
    if ALCHEMY_KEYS[chain_id]:
        result = alchemy_rpc(chain_id, "alchemy_getAssetTransfers", [{
            direction: address,
            "fromBlock": "0x0",
            "toBlock": "latest",
            "category": ["external", "erc20"],
            "withMetadata": True,
            "maxCount": hex(max_count),
            "order": order,
        }])
        if result and "transfers" in result:
            return result["transfers"]
        # Alchemy returned nothing (quota exceeded) — fall through to explorer API
    return _explorer_tokentx(chain_id, direction, address, order, max_count)


# Negative wallet cache — wallets confirmed not fresh/dormant in this session.
# Skip immediately to avoid re-querying Alchemy for every repeat encounter.
wallet_skip_cache: set[str] = set()


def _extract_ts(transfers: list[dict]) -> int | None:
    """Pull the first valid blockTimestamp from a transfer list."""
    for t in transfers:
        ts_str = (t.get("metadata") or {}).get("blockTimestamp", "")
        if ts_str:
            return _parse_alchemy_ts(ts_str)
    return None


def classify_wallet(chain_id: str, address: str) -> str | None:
    """
    Returns 'fresh', 'dormant', or None (not interesting).
    Optimised to use 1 Alchemy call for fresh wallets, 2 for dormant checks.
    Results are cached; uninteresting wallets go into wallet_skip_cache.
    """
    if address in wallet_skip_cache:
        return None

    cache = wallet_cache[chain_id]
    if address in cache:
        ts = cache[address]
        now = NOW_TS()
        if (now - ts["first_ts"]) / 3600 <= FRESH_WALLET_MAX_AGE_HOURS:
            return "fresh"
        if (now - ts["last_ts"]) / 86400 >= DORMANT_WALLET_MIN_INACTIVE_DAYS:
            return "dormant"
        wallet_skip_cache.add(address)
        return None

    now = NOW_TS()

    # --- Step 1: one call to get earliest incoming tx (cheapest freshness check) ---
    first_ts = _extract_ts(_alchemy_asset_transfers(chain_id, "toAddress", address, "asc", 1))
    if first_ts is None:
        # Fallback: wallet may have only sent, never received (rare)
        first_ts = _extract_ts(_alchemy_asset_transfers(chain_id, "fromAddress", address, "asc", 1))
    if first_ts is None:
        wallet_skip_cache.add(address)
        return None

    age_hours = (now - first_ts) / 3600
    if age_hours <= FRESH_WALLET_MAX_AGE_HOURS:
        # Fresh — cache and return without further calls
        cache[address] = {"first_ts": first_ts, "last_ts": now}
        return "fresh"

    # --- Step 2: one more call to check dormant (last outgoing tx) ---
    last_ts = _extract_ts(_alchemy_asset_transfers(chain_id, "fromAddress", address, "desc", 1))
    if last_ts is None:
        last_ts = first_ts

    cache[address] = {"first_ts": first_ts, "last_ts": last_ts}

    if (now - last_ts) / 86400 >= DORMANT_WALLET_MIN_INACTIVE_DAYS:
        return "dormant"

    wallet_skip_cache.add(address)
    return None


def get_wallet_funding_source(chain_id: str, address: str) -> str | None:
    """
    Look at the earliest incoming transfers to `address` and check if any
    came from a known CEX/bridge (funding source detection).
    Uses Alchemy alchemy_getAssetTransfers with order=asc to get earliest transfers.
    """
    transfers = _alchemy_asset_transfers(chain_id, "toAddress", address, "asc")
    if not transfers:
        return None
    funding_sources = CHAINS[chain_id]["known_funding_sources"]
    first_sender = (transfers[0].get("from") or "").lower()
    if first_sender:
        wallet_funder_cache[address] = first_sender  # capture raw funder (Improvement 5)
    for t in transfers:
        sender = (t.get("from") or "").lower()
        for known_addr, label in funding_sources.items():
            if sender == known_addr.lower():
                return label
    return None


def _decode_abi_string(hex_result: str) -> str:
    """Decode ABI-encoded string returned by eth_call for name()/symbol()."""
    try:
        if not hex_result or hex_result in ("0x", "0x0"):
            return ""
        data = bytes.fromhex(hex_result[2:])
        if len(data) < 64:
            # Short response — might be bytes32 fixed string
            return data.rstrip(b"\x00").decode("utf-8", errors="ignore").strip()
        offset = int.from_bytes(data[0:32], "big")
        length = int.from_bytes(data[offset:offset + 32], "big")
        return data[offset + 32: offset + 32 + length].decode("utf-8", errors="ignore").strip()
    except Exception:
        return ""


def get_token_info(chain_id: str, token_address: str) -> dict:
    """Fetch ERC-20 token name/symbol. Uses Alchemy if key set, else Basescan/eth_call."""
    cache = token_info_cache[chain_id]
    if token_address in cache:
        return cache[token_address]

    name: str | None = None
    symbol: str | None = None

    # Try Alchemy first if key is set
    if ALCHEMY_KEYS[chain_id]:
        result = alchemy_rpc(chain_id, "alchemy_getTokenMetadata", [token_address])
        if result:
            name = result.get("name") or None
            symbol = result.get("symbol") or None

    # Fall through to Basescan/Etherscan if Alchemy unavailable or returned nothing
    if not name or not symbol:
        try:
            params: dict = {
                "module": "token", "action": "tokeninfo",
                "contractaddress": token_address,
            }
            if EXPLORER_API_KEYS[chain_id]:
                params["apikey"] = EXPLORER_API_KEYS[chain_id]
            r = requests.get(EXPLORER_API_URLS[chain_id], params=params, timeout=8)
            r.raise_for_status()
            data = r.json()
            if data.get("status") == "1" and data.get("result"):
                item = data["result"][0] if isinstance(data["result"], list) else data["result"]
                name = name or item.get("tokenName") or item.get("name") or None
                symbol = symbol or item.get("symbol") or None
        except Exception as exc:
            log.debug("[%s] Explorer tokeninfo failed for %s: %s", chain_id, token_address[:10], exc)

    # Final fallback: eth_call to ERC-20 name() and symbol()
    if not name:
        name = _decode_abi_string(
            alchemy_rpc(chain_id, "eth_call", [{"to": token_address, "data": "0x06fdde03"}, "latest"]) or ""
        ) or None
    if not symbol:
        symbol = _decode_abi_string(
            alchemy_rpc(chain_id, "eth_call", [{"to": token_address, "data": "0x95d89b41"}, "latest"]) or ""
        ) or None

    info = {"name": name or "Unknown", "symbol": symbol or "???"}
    cache[token_address] = info
    return info


# ---------------------------------------------------------------------------
# Token age check (DexScreener — free, no auth)
# ---------------------------------------------------------------------------


def get_token_age_hours(token_address: str, dexscreener_network: str = "solana") -> float | None:
    """
    Return how many hours old a token is by querying DexScreener for its
    earliest pair creation time. Returns None if lookup fails (caller
    should treat as unknown / allow through).
    Also caches token name/symbol into dexscreener_name_cache as a side effect.
    """
    if token_address in token_age_cache:
        return token_age_cache[token_address]

    try:
        r = requests.get(
            f"https://api.dexscreener.com/latest/dex/tokens/{token_address}",
            timeout=8,
        )
        r.raise_for_status()
        data = r.json()
        pairs = data.get("pairs") or []
        if not pairs:
            token_age_cache[token_address] = None
            return None

        # Cache name/symbol from first pair while we're here
        if token_address not in dexscreener_name_cache:
            base = pairs[0].get("baseToken") or {}
            name = base.get("name") or ""
            symbol = base.get("symbol") or ""
            if name or symbol:
                dexscreener_name_cache[token_address] = {
                    "name": name or "Unknown",
                    "symbol": symbol or "???",
                }

        # Cache liquidity: sum across all pairs
        if token_address not in token_liquidity_cache:
            total_liq = 0.0
            for p in pairs:
                liq = (p.get("liquidity") or {}).get("usd") or 0
                total_liq += float(liq)
            token_liquidity_cache[token_address] = total_liq if total_liq > 0 else None

        # Cache sell + buy count in the last 1h across all pairs
        if token_address not in token_sells_h1_cache:
            total_sells = 0
            total_buys = 0
            for p in pairs:
                txns = p.get("txns") or {}
                total_sells += int((txns.get("h1") or {}).get("sells") or 0)
                total_buys += int((txns.get("h1") or {}).get("buys") or 0)
            token_sells_h1_cache[token_address] = total_sells
            token_buys_h1_cache[token_address] = total_buys if total_buys > 0 else None

        # Cache price, market cap, and socials from highest-liquidity pair
        if token_address not in token_price_cache:
            best = max(pairs, key=lambda p: (p.get("liquidity") or {}).get("usd") or 0)
            price = best.get("priceUsd")
            mcap = best.get("marketCap") or best.get("fdv")
            token_price_cache[token_address] = float(price) if price else None
            token_mcap_cache[token_address] = float(mcap) if mcap else None

            # Social links from DexScreener pair info block
            info_block = best.get("info") or {}
            website = next(
                (w.get("url") for w in (info_block.get("websites") or []) if w.get("url")),
                None,
            )
            twitter = next(
                (s.get("url") for s in (info_block.get("socials") or []) if s.get("type") == "twitter" and s.get("url")),
                None,
            )
            token_socials_cache[token_address] = {"website": website, "twitter": twitter}

        # Find the earliest pair creation timestamp (ms → s)
        earliest_ms = min(
            p["pairCreatedAt"] for p in pairs if p.get("pairCreatedAt")
        )
        age_hours = (NOW_TS() - earliest_ms / 1000) / 3600
        token_age_cache[token_address] = age_hours
        return age_hours
    except Exception as exc:
        log.debug("DexScreener age lookup failed for %s: %s", token_address[:10], exc)
        token_age_cache[token_address] = None
        return None


def is_token_too_old(token_address: str, network: str = "solana") -> bool:
    """Returns True if the token launched more than TOKEN_MAX_AGE_HOURS ago."""
    age = get_token_age_hours(token_address, network)
    if age is None:
        return False  # unknown age → allow through rather than silently drop
    return age > TOKEN_MAX_AGE_HOURS


def token_quality_fail_reason(token_address: str) -> str | None:
    """
    Returns a string describing why a token fails quality checks, or None if it passes.
    Checks (in order): liquidity, honeypot (no sells).
    Only applied when DexScreener data is available — unknown = allow through.
    """
    liq = token_liquidity_cache.get(token_address)
    if liq is not None and liq < MIN_LIQUIDITY_USD:
        return f"low liquidity (${liq:,.0f} < ${MIN_LIQUIDITY_USD:,})"

    sells = token_sells_h1_cache.get(token_address)
    if sells is not None and sells < MIN_SELLS_H1:
        return "no sells in last 1h (possible honeypot)"

    mcap = token_mcap_cache.get(token_address)
    if mcap is not None and mcap < MIN_MCAP_USD:
        return f"mcap too low (${mcap:,.0f} < ${MIN_MCAP_USD:,})"

    buys_h1 = token_buys_h1_cache.get(token_address)
    if buys_h1 is not None and buys_h1 < MIN_BUYS_H1:
        return f"low buy activity ({buys_h1} buys/h < {MIN_BUYS_H1})"

    return None


def schedule_performance_review(token_address: str, chain_id: str, network: str,
                                 token_name: str, token_symbol: str, explorer_url: str) -> None:
    """Record a token for a 24h performance review after an alert fires."""
    if token_address in pending_reviews:
        return  # already scheduled
    pending_reviews[token_address] = {
        "chain_id": chain_id,
        "network": network,
        "alert_ts": NOW_TS(),
        "price_usd": token_price_cache.get(token_address),
        "mcap_usd": token_mcap_cache.get(token_address),
        "token_name": token_name,
        "token_symbol": token_symbol,
        "explorer_url": explorer_url,
    }
    log.info("[review] Scheduled 24h review for %s (%s)", token_name, token_address[:10])


def run_performance_reviews() -> None:
    """
    Background thread: every 5 minutes checks if any alerted token has
    reached its 24h mark and sends a performance review to all subscribers.
    """
    REVIEW_DELAY = 24 * 3600  # 24 hours
    CHECK_INTERVAL = 5 * 60   # check every 5 minutes

    while True:
        time.sleep(CHECK_INTERVAL)
        now = NOW_TS()
        due = [addr for addr, r in list(pending_reviews.items())
               if now - r["alert_ts"] >= REVIEW_DELAY]

        for token_address in due:
            review = pending_reviews.pop(token_address, None)
            if not review:
                continue
            try:
                _send_performance_review(token_address, review)
            except Exception as exc:
                log.error("[review] Failed for %s: %s", token_address[:10], exc)


def _send_performance_review(token_address: str, review: dict) -> None:
    """Fetch current DexScreener data and send the 24h performance review."""
    name = review["token_name"]
    symbol = review["token_symbol"]
    explorer_url = review["explorer_url"]
    network = review["network"]
    dexscreener_url = f"https://dexscreener.com/{network}/{token_address}"
    short_addr = token_address[:6] + "..." + token_address[-4:]

    entry_price = review["price_usd"]
    entry_mcap = review["mcap_usd"]

    # Fetch fresh DexScreener data
    current_price = None
    current_mcap = None
    try:
        r = requests.get(
            f"https://api.dexscreener.com/latest/dex/tokens/{token_address}",
            timeout=8,
        )
        r.raise_for_status()
        pairs = r.json().get("pairs") or []
        if pairs:
            best = max(pairs, key=lambda p: (p.get("liquidity") or {}).get("usd") or 0)
            p = best.get("priceUsd")
            m = best.get("marketCap") or best.get("fdv")
            current_price = float(p) if p else None
            current_mcap = float(m) if m else None
    except Exception as exc:
        log.warning("[review] DexScreener fetch failed for %s: %s", token_address[:10], exc)

    # Calculate change
    if entry_price and current_price:
        pct = (current_price - entry_price) / entry_price * 100
        direction = "🚀" if pct >= 100 else ("📈" if pct > 0 else "📉")
        pct_str = f"{pct:+.1f}% {direction}"
    else:
        pct_str = "N/A"

    msg = (
        f"📊 <b>24h Review</b> — {name} ({symbol})\n"
        f"<b>Address:</b> <a href='{explorer_url}'>{short_addr}</a>\n\n"
        f"<b>At alert:</b>  Price {fmt_price(entry_price)} | MCap {fmt_mcap(entry_mcap)}\n"
        f"<b>Now:</b>       Price {fmt_price(current_price)} | MCap {fmt_mcap(current_mcap)}\n\n"
        f"<b>Performance: {pct_str}</b>"
    )
    keyboard = make_alert_keyboard(
        dexscreener_url=dexscreener_url,
        explorer_url=explorer_url,
    )
    log.info("[review] Sending 24h review for %s: %s", name, pct_str)
    send_telegram(msg, reply_markup=keyboard)


# ---------------------------------------------------------------------------
# Improvement 6: 3-minute confirmation ping
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Top holder concentration
# ---------------------------------------------------------------------------

def get_top_holder_pct(token_address: str, chain_id: str) -> float | None:
    """
    Return the % of supply held by the top 10 wallets. Cached.
    Solana: Helius RPC getTokenLargestAccounts + getTokenSupply.
    EVM: Basescan/Etherscan tokenholderlist + tokensupply.
    """
    if token_address in token_top_holders_cache:
        return token_top_holders_cache[token_address]

    pct: float | None = None
    try:
        if chain_id == "solana":
            largest = helius_rpc("getTokenLargestAccounts", [token_address])
            supply_res = helius_rpc("getTokenSupply", [token_address])
            if largest and supply_res:
                accounts = (largest.get("value") or [])[:10]
                total = float((supply_res.get("value") or {}).get("uiAmount") or 0)
                if total > 0:
                    top10 = sum(float(a.get("uiAmount") or 0) for a in accounts)
                    pct = (top10 / total) * 100
        else:
            # EVM — use Basescan (Base) or Etherscan (Ethereum)
            if chain_id == "base":
                api_url, api_key = "https://api.basescan.org/api", API_KEY
            else:
                api_url = "https://api.etherscan.io/api"
                api_key = os.getenv("ETHERSCAN_API_KEY", "").strip()
            if api_key:
                r = requests.get(api_url, params={
                    "module": "token", "action": "tokenholderlist",
                    "contractaddress": token_address,
                    "page": 1, "offset": 10, "apikey": api_key,
                }, timeout=8)
                r.raise_for_status()
                holders = r.json().get("result") or []
                r2 = requests.get(api_url, params={
                    "module": "stats", "action": "tokensupply",
                    "contractaddress": token_address, "apikey": api_key,
                }, timeout=8)
                r2.raise_for_status()
                total_raw = int(r2.json().get("result") or 0)
                if holders and total_raw > 0:
                    top10_raw = sum(int(h.get("TokenHolderQuantity") or 0) for h in holders)
                    pct = (top10_raw / total_raw) * 100
    except Exception as exc:
        log.debug("[%s] Top holder lookup failed for %s: %s", chain_id, token_address[:10], exc)

    token_top_holders_cache[token_address] = pct
    return pct


def _fmt_top_holders(pct: float | None) -> str:
    if pct is None:
        return ""
    flag = " 🚨" if pct >= 80 else (" ⚠️" if pct >= 60 else "")
    return f"Top 10 Hold: {pct:.0f}%{flag}"


def _fetch_dexscreener_price(token_address: str) -> tuple[float | None, float | None, int | None]:
    """Fetch (price, mcap, buys_5m) from DexScreener. Returns (None, None, None) on failure."""
    r = requests.get(
        f"https://api.dexscreener.com/latest/dex/tokens/{token_address}",
        timeout=8,
    )
    r.raise_for_status()
    pairs = r.json().get("pairs") or []
    if not pairs:
        return None, None, None
    best = max(pairs, key=lambda p: (p.get("liquidity") or {}).get("usd") or 0)
    price = best.get("priceUsd")
    mcap = best.get("marketCap") or best.get("fdv")
    txns = best.get("txns") or {}
    buys_5m = int((txns.get("m5") or {}).get("buys") or 0)
    return (float(price) if price else None), (float(mcap) if mcap else None), buys_5m


def _price_verdict(alert_price: float, current_price: float) -> str:
    pct = (current_price - alert_price) / alert_price * 100
    if pct >= 100:
        return f"🚀 +{pct:.1f}% — confirmed move!"
    if pct >= 20:
        return f"🚀 +{pct:.1f}% — confirmed move!"
    if pct >= 5:
        return f"📈 +{pct:.1f}% — up"
    if pct >= -5:
        return f"😐 {pct:+.1f}% — flat"
    if pct >= -20:
        return f"📉 {pct:.1f}% — fading"
    return f"💀 {pct:.1f}% — dumping"


def schedule_confirmation_ping(
    token_address: str,
    network: str,
    token_name: str,
    alert_price: float | None,
    alert_mcap: float | None,
    recipient_msg_ids: dict[str, int],
) -> None:
    """
    Spawn daemon threads for 3min, 15min, and 30min price checks.
    Each check replies to the original alert message.
    """
    def _ping(delay_seconds: int, label: str) -> None:
        time.sleep(delay_seconds)
        try:
            current_price, current_mcap, buys_5m = None, None, None
            try:
                current_price, current_mcap, buys_5m = _fetch_dexscreener_price(token_address)
            except Exception as exc:
                log.warning("[ping] DexScreener fetch failed for %s: %s", token_address[:10], exc)

            if alert_price and current_price:
                verdict = _price_verdict(alert_price, current_price)
            else:
                verdict = "❓ Price N/A"

            buys_str = f"  |  Buys/5min: {buys_5m}" if buys_5m is not None else ""
            msg = (
                f"⏱ <b>{label}</b> — {token_name}\n"
                f"Price: {fmt_price(current_price)} (was {fmt_price(alert_price)})\n"
                f"MCap: {fmt_mcap(current_mcap)}{buys_str}\n"
                f"<b>{verdict}</b>"
            )
            send_telegram(msg, reply_to_message_ids=recipient_msg_ids)
            log.info("[ping] %s sent for %s: %s", label, token_name, verdict)
        except Exception as exc:
            log.warning("[ping] %s failed for %s: %s", label, token_address[:10], exc)

    for delay, label in [
        (3 * 60,  "3min check"),
        (15 * 60, "15min check"),
        (30 * 60, "30min check"),
    ]:
        threading.Thread(target=_ping, args=(delay, label), daemon=True).start()


# ---------------------------------------------------------------------------
# Telegram
# ---------------------------------------------------------------------------


# Set of chat IDs to broadcast alerts to.
# Seeded from TELEGRAM_CHAT_ID env var; grows as users send /start.
telegram_subscribers: set[str] = set()


def _tg_send(
    chat_id: str,
    message: str,
    parse_mode: str = "HTML",
    reply_markup: dict | None = None,
    reply_to_message_id: int | None = None,
) -> int | None:
    """Send a single Telegram message to one chat_id. Returns message_id or None."""
    import json as _json
    payload: dict = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": parse_mode,
        "disable_web_page_preview": True,
    }
    if reply_markup:
        payload["reply_markup"] = _json.dumps(reply_markup)
    if reply_to_message_id:
        payload["reply_to_message_id"] = reply_to_message_id
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json=payload,
            timeout=10,
        )
        r.raise_for_status()
        return (r.json().get("result") or {}).get("message_id")
    except Exception as exc:
        log.error("Telegram send to %s failed: %s", chat_id, exc)
        return None


def send_telegram(
    message: str,
    reply_markup: dict | None = None,
    reply_to_message_ids: dict[str, int] | None = None,
) -> dict[str, int]:
    """Broadcast an alert to all subscribers. Returns {chat_id: message_id}."""
    if not TELEGRAM_BOT_TOKEN:
        log.warning("Telegram not configured — skipping alert")
        log.info("ALERT:\n%s", message)
        return {}
    recipients = list(telegram_subscribers)
    if not recipients:
        log.warning("No Telegram subscribers yet — skipping alert")
        return {}
    sent_ids: dict[str, int] = {}
    for chat_id in recipients:
        reply_to = (reply_to_message_ids or {}).get(chat_id)
        msg_id = _tg_send(chat_id, message, reply_markup=reply_markup, reply_to_message_id=reply_to)
        if msg_id:
            sent_ids[chat_id] = msg_id
    return sent_ids


def poll_telegram_commands() -> None:
    """
    Long-poll Telegram for incoming messages.
    Handles /start  — subscribes the user and sends a welcome message.
    Handles /stop   — unsubscribes the user.
    Runs as a daemon thread alongside the scanner threads.
    """
    if not TELEGRAM_BOT_TOKEN:
        return

    offset = 0
    log.info("[Telegram] Command listener started")

    while True:
        try:
            r = requests.get(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates",
                params={"offset": offset, "timeout": 30},
                timeout=35,
            )

            # 409 = another instance is already polling (Railway rolling deploy).
            # Back off and let the old instance die before retrying.
            if r.status_code == 409:
                log.warning("[Telegram] 409 Conflict — another instance polling, waiting 30s")
                time.sleep(30)
                continue

            r.raise_for_status()
            for update in r.json().get("result", []):
                offset = update["update_id"] + 1
                msg = update.get("message") or {}
                text = (msg.get("text") or "").strip()
                chat_id = str((msg.get("chat") or {}).get("id") or "")
                if not chat_id:
                    continue

                if text.startswith("/start"):
                    telegram_subscribers.add(chat_id)
                    log.info("[Telegram] New subscriber: %s (total: %d)", chat_id, len(telegram_subscribers))
                    _tg_send(chat_id,
                        "✅ <b>Subscribed to PiggyBac!</b>\n\n"
                        "You'll get alerts when 7+ fresh wallets buy the same token "
                        "within 10 minutes across Solana, Base, and Ethereum.\n\n"
                        "Send /stop to unsubscribe.",
                    )
                elif text.startswith("/stop"):
                    telegram_subscribers.discard(chat_id)
                    log.info("[Telegram] Unsubscribed: %s", chat_id)
                    _tg_send(chat_id, "❌ Unsubscribed from PiggyBac alerts.")

        except Exception as exc:
            log.warning("[Telegram] Polling error: %s", exc)
            time.sleep(5)


def make_alert_keyboard(
    dexscreener_url: str,
    explorer_url: str,
    pump_fun_url: str | None = None,
    socials: dict | None = None,
) -> dict:
    """Build a Telegram inline keyboard with URL buttons for an alert."""
    row1 = [
        {"text": "📊 Chart", "url": dexscreener_url},
        {"text": "🔍 Explorer", "url": explorer_url},
    ]
    if pump_fun_url:
        row1.append({"text": "🌊 Pump.fun", "url": pump_fun_url})
    rows = [row1]
    if socials:
        row2 = []
        if socials.get("website"):
            row2.append({"text": "🌐 Website", "url": socials["website"]})
        if socials.get("twitter"):
            row2.append({"text": "🐦 Twitter", "url": socials["twitter"]})
        if row2:
            rows.append(row2)
    return {"inline_keyboard": rows}


def _format_age(token_age_hours: float | None) -> str:
    if token_age_hours is None:
        return ""
    if token_age_hours < 1:
        return f"{int(token_age_hours * 60)}min old 🔴"
    if token_age_hours < 24:
        return f"{token_age_hours:.1f}h old"
    return f"{token_age_hours / 24:.1f}d old"


def _cex_diversity_note(buys: list[dict]) -> str:
    """Return a note about CEX funding diversity in the cluster."""
    sources = [b.get("funding_source") for b in buys if b.get("wallet_type") == "fresh"]
    known = [s for s in sources if s and s.lower() not in ("unknown", "none")]
    unique_cexes = len(set(known))
    all_unknown = all(s is None or s.lower() in ("unknown", "none") for s in sources)
    if unique_cexes >= 3:
        return f"✅ {unique_cexes} different CEXes funded these wallets"
    if unique_cexes >= 2:
        return f"⚡ {unique_cexes} different CEXes"
    if all_unknown and sources:
        return "⚠️ All funding sources unknown"
    return ""


def _gather_alert_signals(token_name: str, token_address: str, chain: str, buys: list[dict]) -> list[str]:
    """Collect all extra signal lines: CEX diversity, fake cluster warning, re-launch, overlap, funder."""
    signals = []
    sources = [b.get("funding_source") for b in buys if b.get("wallet_type") == "fresh"]
    known = [s for s in sources if s and s.lower() not in ("unknown", "none")]
    all_unknown = not known and bool(sources)
    unique_cexes = len(set(known))

    if unique_cexes >= 3:
        signals.append(f"✅ {unique_cexes} different CEXes funded these wallets")
    elif unique_cexes >= 2:
        signals.append(f"⚡ {unique_cexes} different CEXes")
    elif all_unknown:
        signals.append("⚠️ All funding sources unknown")
        if len(buys) >= HIGH_WALLET_THRESHOLD:
            signals.append(f"⚠️ {len(buys)} wallets all unknown — possible fake cluster")

    name_note = check_name_relaunch(token_name, token_address, chain)
    if name_note:
        signals.append(name_note)

    all_wallets = [b["wallet"] for b in buys]
    overlap_note = check_wallet_overlap(all_wallets, token_address)
    if overlap_note:
        signals.append(overlap_note)

    funder_note = analyze_funder_clustering(buys)
    if funder_note:
        signals.append(funder_note)

    return signals


def build_alert(
    chain_id: str,
    token_address: str,
    buys: list[dict],
    token_age_hours: float | None = None,
    enrichment: dict | None = None,
    conviction_level: str | None = None,
    conviction_score: int | None = None,
    conviction_reasons: list[str] | None = None,
) -> str:
    chain = CHAINS[chain_id]
    info = get_token_info(chain_id, token_address)
    token_name = info["name"]
    token_symbol = info["symbol"]
    top_holders_str = _fmt_top_holders(token_top_holders_cache.get(token_address))
    chain_label = chain["name"]

    fresh_buys = [b for b in buys if b["wallet_type"] == "fresh"]
    dormant_buys = [b for b in buys if b["wallet_type"] == "dormant"]

    sources = [b.get("funding_source") for b in fresh_buys]
    all_unknown = not any(s and s.lower() not in ("unknown", "none") for s in sources)
    _, tier = score_cluster(len(fresh_buys), len(dormant_buys), len(buys), len(buys), all_funding_unknown=all_unknown)

    age_str = _format_age(token_age_hours)
    ts = datetime.now(timezone.utc).strftime("%H:%M UTC")

    liq = token_liquidity_cache.get(token_address)
    liq_str = f"${liq:,.0f}" if liq is not None else "?"
    sells = token_sells_h1_cache.get(token_address)
    buys_1h = token_buys_h1_cache.get(token_address)
    mcap = token_mcap_cache.get(token_address)
    price = token_price_cache.get(token_address)

    wallet_lines = []
    for b in fresh_buys:
        short_w = b["wallet"][:6] + "..." + b["wallet"][-4:]
        funding = b.get("funding_source") or "unknown"
        tx_url = f"{chain['explorer_url']}/tx/{b['tx_hash']}"
        wallet_lines.append(f"  🆕 <a href='{tx_url}'>{short_w}</a> ({funding})")
    for b in dormant_buys:
        short_w = b["wallet"][:6] + "..." + b["wallet"][-4:]
        tx_url = f"{chain['explorer_url']}/tx/{b['tx_hash']}"
        inactive_days = int((NOW_TS() - b["last_ts"]) / 86400)
        wallet_lines.append(f"  💤 <a href='{tx_url}'>{short_w}</a> (dormant {inactive_days}d)")

    summary_parts = []
    if fresh_buys:
        summary_parts.append(f"{len(fresh_buys)} fresh 🆕")
    if dormant_buys:
        summary_parts.append(f"{len(dormant_buys)} dormant 💤")

    extra_signals = _gather_alert_signals(token_name, token_address, chain_id, buys)
    if enrichment and enrichment.get("dev_funded"):
        n = len(enrichment["dev_funded"])
        pct = n / len(buys) * 100
        extra_signals.insert(0, f"🚨 DEV FUNDED: {n}/{len(buys)} wallets funded by deployer ({pct:.0f}%)")

    liq_line = f"💧 Liq: {liq_str}  |  Buys/1h: {buys_1h or '?'}  |  Sells/1h: {sells or '?'}"
    if top_holders_str:
        liq_line += f"  |  {top_holders_str}"

    lines = []
    if conviction_level and conviction_score is not None:
        lines.append(_fmt_conviction_block(conviction_level, conviction_score, conviction_reasons or []))
        lines.append("")

    lines += [
        f"{tier} <b>PiggyBac Alert</b> [{chain_label}] — {ts}",
        "",
        f"🪙 <b>{token_name}</b> ({token_symbol})" + (f" · <b>{age_str}</b>" if age_str else ""),
        f"💰 MCap: <b>{fmt_mcap(mcap)}</b>  |  Price: <b>{fmt_price(price)}</b>",
        liq_line,
    ]

    # Deployer info
    if enrichment:
        deployer_line = _fmt_deployer_line(enrichment.get("deployer_info"))
        if deployer_line:
            lines.append(deployer_line)

    lines += [
        "",
        f"⚡ <b>{' + '.join(summary_parts)}</b> in {CLUSTER_TIME_WINDOW_MINUTES}min",
    ]
    for sig in extra_signals:
        if sig:
            lines.append(sig)

    # Dormant wallet PnL profiles
    if enrichment and enrichment.get("dormant_profiles"):
        lines.append("")
        lines.append("🏆 <b>Dormant wallet profiles:</b>")
        for p in enrichment["dormant_profiles"]:
            short_w = p["wallet"][:6] + "..." + p["wallet"][-4:]
            label = SCORE_EMOJI.get(p.get("score", "unknown"), "❓")
            analyzed = p.get("tokens_analyzed", 0)
            winners = p.get("winners", 0)
            detail = f"{winners}/{analyzed} winners" if analyzed > 0 else "no history"
            lines.append(f"  😴 {short_w} — {label}: {detail}")

    lines.append("")
    lines.extend(wallet_lines)

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Wave 2: Wallet intelligence — deployer info, wallet PnL, conviction scoring
# ---------------------------------------------------------------------------

def get_solana_deployer(token_address: str) -> str | None:
    """Return the wallet that created this Solana token (oldest signer). Cached permanently."""
    if token_address in token_deployer_cache:
        return token_deployer_cache[token_address]
    try:
        sigs = helius_rpc("getSignaturesForAddress", [token_address, {"limit": 1000}])
        if not sigs or not isinstance(sigs, list):
            token_deployer_cache[token_address] = None
            return None
        oldest_sig = sigs[-1]["signature"]
        tx = helius_rpc("getTransaction", [oldest_sig, {
            "encoding": "jsonParsed", "maxSupportedTransactionVersion": 0
        }])
        if not tx:
            token_deployer_cache[token_address] = None
            return None
        keys = (tx.get("transaction", {}).get("message", {}).get("accountKeys") or [])
        deployer = keys[0].get("pubkey") if keys else None
        token_deployer_cache[token_address] = deployer
        return deployer
    except Exception as exc:
        log.debug("[solana] Deployer lookup failed for %s: %s", token_address[:10], exc)
        token_deployer_cache[token_address] = None
        return None


def get_deployer_info(token_address: str, chain_id: str) -> dict | None:
    """
    Get deployer wallet balance and token holdings %.
    Solana only for now — EVM contract creator requires extra tracing.
    Returns dict with keys: address, balance, holdings_pct.
    """
    if chain_id != "solana":
        return None

    deployer = get_solana_deployer(token_address)
    if not deployer:
        return None

    result: dict = {"address": deployer, "balance": None, "holdings_pct": None}
    try:
        # Native SOL balance
        bal = helius_rpc("getBalance", [deployer])
        if bal is not None:
            result["balance"] = float(bal) / 1e9

        # Deployer's token holdings
        supply_res = helius_rpc("getTokenSupply", [token_address])
        total_supply = float((supply_res.get("value") or {}).get("uiAmount") or 0) if supply_res else 0
        if total_supply > 0:
            accounts = helius_rpc("getTokenAccountsByOwner", [
                deployer, {"mint": token_address}, {"encoding": "jsonParsed"}
            ])
            if accounts:
                holder_total = 0.0
                for acct in (accounts.get("value") or []):
                    amt = float(
                        (acct.get("account", {}).get("data", {})
                         .get("parsed", {}).get("info", {})
                         .get("tokenAmount") or {}).get("uiAmount") or 0
                    )
                    holder_total += amt
                if holder_total > 0:
                    result["holdings_pct"] = (holder_total / total_supply) * 100
    except Exception as exc:
        log.debug("[solana] Deployer info failed for %s: %s", token_address[:10], exc)

    return result


def _fmt_deployer_line(info: dict | None) -> str:
    """Format the deployer line for the alert."""
    if not info or info.get("balance") is None:
        return ""
    bal = info["balance"]
    holdings = info.get("holdings_pct")

    bal_str = f"{bal:.2f} SOL"
    bal_flag = " ⚠️" if bal < DEPLOYER_LOW_BALANCE_SOL else ""

    if holdings is not None:
        holdings_flag = " ⚠️" if holdings > DEPLOYER_HIGH_HOLDINGS_PCT else ""
        return f"👤 Deployer: {bal_str}{bal_flag} | holds {holdings:.1f}%{holdings_flag} of supply"
    return f"👤 Deployer: {bal_str}{bal_flag}"


def get_wallet_bought_tokens_solana(wallet_address: str) -> list[str]:
    """Get last MAX_TOKENS_PER_WALLET unique token mints bought by this Solana wallet."""
    data = helius_get(
        f"/addresses/{wallet_address}/transactions",
        {"type": "SWAP", "limit": 50},
    )
    if not data or not isinstance(data, list):
        return []
    tokens: list[str] = []
    seen: set[str] = set()
    for tx in data:
        for transfer in tx.get("tokenTransfers", []):
            mint = transfer.get("mint", "")
            if (transfer.get("toUserAccount") == wallet_address
                    and mint and mint != WSOL_MINT and mint not in seen):
                seen.add(mint)
                tokens.append(mint)
                if len(tokens) >= MAX_TOKENS_PER_WALLET:
                    return tokens
    return tokens


def _score_token_performance(token_address: str) -> str:
    """Classify a token as big_winner / winner / neutral / dead via DexScreener."""
    try:
        r = requests.get(
            f"https://api.dexscreener.com/latest/dex/tokens/{token_address}",
            timeout=5,
        )
        r.raise_for_status()
        pairs = r.json().get("pairs") or []
        if not pairs:
            return "dead"
        best_mcap = max(
            (float(p.get("marketCap") or p.get("fdv") or 0) for p in pairs), default=0
        )
        best_liq = max(
            (float((p.get("liquidity") or {}).get("usd") or 0) for p in pairs), default=0
        )
        if best_liq < 100:
            return "dead"
        if best_mcap >= 500_000:
            return "big_winner"
        if best_mcap >= 100_000:
            return "winner"
        return "neutral"
    except Exception:
        return "unknown"


def profile_wallet(wallet_address: str, chain_id: str) -> dict:
    """
    Score a wallet based on its token trading history.
    Cached for WALLET_PROFILE_CACHE_TTL seconds (24h).
    """
    now = NOW_TS()
    cached = wallet_profile_cache.get(wallet_address)
    if cached and now - cached[1] < WALLET_PROFILE_CACHE_TTL:
        return cached[0]

    if chain_id == "solana":
        tokens = get_wallet_bought_tokens_solana(wallet_address)
    else:
        # EVM: use Alchemy asset transfers as a proxy for token buys
        transfers = _alchemy_asset_transfers(chain_id, "toAddress", wallet_address, "desc", 20)
        seen: set[str] = set()
        tokens = []
        for t in transfers:
            addr = (t.get("rawContract") or {}).get("address", "")
            if addr and addr not in seen:
                seen.add(addr)
                tokens.append(addr)
                if len(tokens) >= MAX_TOKENS_PER_WALLET:
                    break

    if not tokens:
        profile = {"tokens_analyzed": 0, "winners": 0, "big_winners": 0,
                   "dead": 0, "score": "unknown"}
        wallet_profile_cache[wallet_address] = (profile, now)
        return profile

    scores = [_score_token_performance(t) for t in tokens]
    winners = scores.count("winner") + scores.count("big_winners")
    big_winners = scores.count("big_winner")
    dead = scores.count("dead")
    total = len([s for s in scores if s != "unknown"])

    win_rate = winners / total if total > 0 else 0
    rug_rate = dead / total if total > 0 else 0

    if big_winners >= 2 or (win_rate >= 0.5 and winners >= 2):
        score = "alpha"
    elif win_rate >= 0.3 or winners >= 1:
        score = "profitable"
    elif rug_rate >= 0.7:
        score = "rug_buyer"
    else:
        score = "neutral"

    profile = {
        "tokens_analyzed": total,
        "winners": winners,
        "big_winners": big_winners,
        "dead": dead,
        "score": score,
    }
    wallet_profile_cache[wallet_address] = (profile, now)
    return profile


SCORE_EMOJI = {
    "alpha":     "🏆 ALPHA",
    "profitable":"📈 Profitable",
    "neutral":   "😐 Neutral",
    "rug_buyer": "💀 Rug buyer ⚠️",
    "unknown":   "❓ Unknown",
}


def check_dev_funded_wallets(cluster_wallets: list[str], deployer_address: str) -> list[str]:
    """Return cluster wallets funded by the deployer (1-hop via wallet_funder_cache)."""
    if not deployer_address:
        return []
    deployer_lower = deployer_address.lower()
    return [w for w in cluster_wallets
            if (wallet_funder_cache.get(w) or "").lower() == deployer_lower]


def calculate_conviction(
    wallet_count: int,
    dormant_profiles: list[dict],
    all_funding_unknown: bool,
    funding_diversity: int,
    is_relaunch: bool,
    relaunch_attempt: int,
    wallet_overlap_count: int,
    deployer_info: dict | None,
    dev_funded_count: int,
    buys_h1: int | None,
    top_holders_pct: float | None,
) -> tuple[str, int, list[str]]:
    """
    Calculate conviction score (0-100) and level (LOW/MEDIUM/HIGH/VERY HIGH).
    Returns (level, score, reasons).
    """
    score = 50
    reasons: list[str] = []

    # Dormant wallet PnL history
    alpha_n = sum(1 for p in dormant_profiles if p.get("score") == "alpha")
    profit_n = sum(1 for p in dormant_profiles if p.get("score") == "profitable")
    rug_n = sum(1 for p in dormant_profiles if p.get("score") == "rug_buyer")

    if alpha_n >= 2:
        score += 25; reasons.append(f"+25: {alpha_n} alpha-history dormant wallets")
    elif alpha_n == 1:
        score += 15; reasons.append("+15: dormant wallet with alpha history")
    if profit_n >= 2:
        score += 12; reasons.append(f"+12: {profit_n} profitable dormant wallets")
    elif profit_n == 1:
        score += 6;  reasons.append("+6: dormant wallet with profitable history")
    if rug_n >= 2:
        score -= 20; reasons.append(f"-20: {rug_n} dormant wallets with rug history")

    # Funding diversity
    if funding_diversity >= 3:
        score += 10; reasons.append(f"+10: {funding_diversity} different funding sources")

    # Re-launch signals
    if is_relaunch and relaunch_attempt >= 3:
        score += 10; reasons.append(f"+10: attempt #{relaunch_attempt} — team committing")
    if wallet_overlap_count >= 2 and is_relaunch:
        score += 10; reasons.append("+10: wallet overlap + re-launch")

    # Deployer signals
    if deployer_info:
        bal = deployer_info.get("balance")
        holdings = deployer_info.get("holdings_pct")
        if bal is not None:
            if bal >= 10:
                score += 5;  reasons.append(f"+5: deployer has {bal:.1f} SOL")
            elif bal < DEPLOYER_LOW_BALANCE_SOL:
                score -= 10; reasons.append(f"-10: deployer low balance ({bal:.2f} SOL)")
        if holdings is not None and holdings > DEPLOYER_HIGH_HOLDINGS_PCT:
            score -= 10; reasons.append(f"-10: deployer holds {holdings:.1f}% of supply")

    # Dev-funded wallets
    dev_pct = dev_funded_count / wallet_count if wallet_count > 0 else 0
    if dev_pct >= 0.5:
        score -= 35; reasons.append(f"-35: {dev_funded_count}/{wallet_count} wallets funded by deployer")
    elif dev_pct >= 0.2:
        score -= 20; reasons.append(f"-20: {dev_funded_count} wallets funded by deployer")

    # Funding unknown
    if all_funding_unknown and wallet_count >= HIGH_WALLET_THRESHOLD:
        score -= 15; reasons.append("-15: 30+ wallets all unknown funding")
    elif all_funding_unknown:
        score -= 5;  reasons.append("-5: all funding unknown")

    # Top holder concentration
    if top_holders_pct is not None and top_holders_pct >= 80:
        score -= 10; reasons.append(f"-10: top 10 hold {top_holders_pct:.0f}% (insider concentration)")

    # Volume
    if buys_h1:
        if buys_h1 >= 500:
            score += 5;  reasons.append(f"+5: high volume ({buys_h1} buys/h)")
        elif buys_h1 < 100:
            score -= 5;  reasons.append(f"-5: low volume ({buys_h1} buys/h)")

    score = max(0, min(100, score))

    if score >= 75:   level = "VERY HIGH"
    elif score >= 55: level = "HIGH"
    elif score >= 35: level = "MEDIUM"
    else:             level = "LOW"

    return level, score, reasons


CONVICTION_EMOJI = {"VERY HIGH": "🟢🟢", "HIGH": "🟢", "MEDIUM": "🟡", "LOW": "🔴"}


def _fmt_conviction_block(level: str, score: int, reasons: list[str]) -> str:
    emoji = CONVICTION_EMOJI.get(level, "⚪")
    lines = [f"{emoji} <b>CONVICTION: {level} ({score}/100)</b>"]
    for r in reasons[:6]:  # cap at 6 lines to keep it concise
        lines.append(f"   {r}")
    return "\n".join(lines)


def enrich_cluster(token_address: str, chain_id: str, unique_buys: list[dict]) -> dict:
    """
    Run all Wave 2 enrichment with a hard ENRICHMENT_TIMEOUT_SECONDS timeout.
    Returns enrichment dict: deployer_info, dormant_profiles, dev_funded.
    Never raises — returns partial results on timeout.
    """
    dormant_buys = [b for b in unique_buys if b.get("wallet_type") == "dormant"]

    enrichment: dict = {
        "deployer_info": None,
        "dormant_profiles": [],
        "dev_funded": [],
    }

    def _get_deployer() -> dict | None:
        return get_deployer_info(token_address, chain_id)

    def _profile_dormants() -> list[dict]:
        results = []
        for b in dormant_buys[:MAX_WALLETS_TO_PROFILE]:
            try:
                p = profile_wallet(b["wallet"], chain_id)
                results.append({"wallet": b["wallet"], **p})
            except Exception:
                pass
        return results

    half_t = ENRICHMENT_TIMEOUT_SECONDS / 2
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as ex:
        f_deployer = ex.submit(_get_deployer)
        f_profiles = ex.submit(_profile_dormants)
        try:
            enrichment["deployer_info"] = f_deployer.result(timeout=half_t)
        except Exception as exc:
            log.debug("[enrich] Deployer lookup timed out for %s: %s", token_address[:10], exc)
        try:
            enrichment["dormant_profiles"] = f_profiles.result(timeout=ENRICHMENT_TIMEOUT_SECONDS)
        except Exception as exc:
            log.debug("[enrich] Wallet profiling timed out for %s: %s", token_address[:10], exc)

    # Dev-funded check is fast (uses cached funder data)
    deployer_addr = (enrichment["deployer_info"] or {}).get("address")
    if deployer_addr:
        enrichment["dev_funded"] = check_dev_funded_wallets(
            [b["wallet"] for b in unique_buys], deployer_addr
        )

    return enrichment


# ---------------------------------------------------------------------------
# Detection logic
# ---------------------------------------------------------------------------


def prune_old_buys(chain_id: str) -> None:
    cutoff = NOW_TS() - CLUSTER_TIME_WINDOW_MINUTES * 60
    buys = cluster_buys[chain_id]
    for token in list(buys.keys()):
        buys[token] = [b for b in buys[token] if b["timestamp"] >= cutoff]
        if not buys[token]:
            del buys[token]


def check_for_clusters(chain_id: str) -> None:
    network = CHAINS[chain_id]["dexscreener_network"]
    for token_address, buys in cluster_buys[chain_id].items():
        # Deduplicate by wallet first
        seen = {}
        for b in buys:
            seen[b["wallet"]] = b
        unique_buys = list(seen.values())

        if len(unique_buys) < CLUSTER_MIN_WALLETS:
            continue

        log.info("[%s] Potential cluster: %d wallets on %s — checking filters",
                 chain_id, len(unique_buys), token_address[:10])
        token_age = get_token_age_hours(token_address, network)
        if is_token_too_old(token_address, network):
            log.info("[%s] SKIP %s — token too old (%.1fh)", chain_id, token_address[:10], token_age or -1)
            continue

        fail_reason = token_quality_fail_reason(token_address)
        if fail_reason:
            log.info("[%s] Skipping token %s — %s", chain_id, token_address[:10], fail_reason)
            continue

        # Permanent session dedup — once alerted, never alert again for this token
        if token_address in alerted_clusters:
            continue
        # Fallback cooldown for container restarts (token_last_alerted survives within session)
        last_alert_ts = token_last_alerted[chain_id].get(token_address, 0)
        if NOW_TS() - last_alert_ts < ALERT_COOLDOWN_SECONDS:
            continue
        alerted_clusters.add(token_address)
        token_last_alerted[chain_id][token_address] = NOW_TS()

        fresh_count = sum(1 for b in unique_buys if b["wallet_type"] == "fresh")
        dormant_count = sum(1 for b in unique_buys if b["wallet_type"] == "dormant")
        log.info(
            "[%s] CLUSTER: %d fresh + %d dormant wallets bought %s (age %.1fh)",
            chain_id, fresh_count, dormant_count, token_address, token_age or -1,
        )
        get_top_holder_pct(token_address, chain_id)  # populate cache before alert
        enrichment = enrich_cluster(token_address, chain_id, unique_buys)

        # Conviction score — suppress very low conviction alerts
        sources = [b.get("funding_source") for b in unique_buys if b.get("wallet_type") == "fresh"]
        known_src = [s for s in sources if s and s.lower() not in ("unknown", "none")]
        relaunch_note = check_name_relaunch(
            (get_token_info(chain_id, token_address))["name"], token_address, chain_id
        )
        overlap_count = sum(1 for b in unique_buys
                            if wallet_cluster_history.get(b["wallet"]))
        c_level, c_score, c_reasons = calculate_conviction(
            wallet_count=len(unique_buys),
            dormant_profiles=enrichment["dormant_profiles"],
            all_funding_unknown=not known_src and bool(sources),
            funding_diversity=len(set(known_src)),
            is_relaunch=bool(relaunch_note),
            relaunch_attempt=len(recent_token_names.get(
                normalize_token_name((get_token_info(chain_id, token_address))["name"]), []
            )) + 1,
            wallet_overlap_count=overlap_count,
            deployer_info=enrichment["deployer_info"],
            dev_funded_count=len(enrichment["dev_funded"]),
            buys_h1=token_buys_h1_cache.get(token_address),
            top_holders_pct=token_top_holders_cache.get(token_address),
        )
        if c_score < MIN_CONVICTION_TO_ALERT:
            log.info("[%s] SUPPRESSED (conviction %d/100): %s", chain_id, c_score, token_address[:10])
            return

        message = build_alert(chain_id, token_address, unique_buys, token_age, enrichment, c_level, c_score, c_reasons)
        chain = CHAINS[chain_id]
        keyboard = make_alert_keyboard(
            dexscreener_url=f"https://dexscreener.com/{chain['dexscreener_network']}/{token_address}",
            explorer_url=f"{chain['explorer_url']}/token/{token_address}",
            socials=token_socials_cache.get(token_address),
        )
        sent_ids = send_telegram(message, reply_markup=keyboard)
        info = get_token_info(chain_id, token_address)
        record_token_name(info["name"], token_address, chain_id)
        record_cluster_wallets(info["name"], token_address, chain_id, [b["wallet"] for b in unique_buys])
        schedule_confirmation_ping(
            token_address, chain["dexscreener_network"], info["name"],
            token_price_cache.get(token_address), token_mcap_cache.get(token_address),
            sent_ids,
        )
        schedule_performance_review(
            token_address, chain_id, CHAINS[chain_id]["dexscreener_network"],
            info["name"], info["symbol"],
            f"{CHAINS[chain_id]['explorer_url']}/token/{token_address}",
        )


def process_swap_logs(chain_id: str, swap_logs: list[dict]) -> None:
    """
    Process Uniswap V2/V3 Swap logs: decode each swap, check if the buyer
    is a fresh/dormant wallet, and record it for cluster detection.
    """
    now = NOW_TS()
    dex_routers_lower = {k.lower() for k in CHAINS[chain_id]["dex_routers"]}
    decoded_count = 0
    interesting_count = 0

    for swap_log in swap_logs:
        decoded = decode_swap_log(chain_id, swap_log)
        if not decoded:
            continue
        decoded_count += 1

        wallet, token_address = decoded
        tx_hash = swap_log.get("transactionHash", "")

        # Skip if recipient is a known DEX router/aggregator, not a human wallet
        if wallet in dex_routers_lower:
            continue

        wallet_type = classify_wallet(chain_id, wallet)
        if wallet_type is None:
            continue

        last_ts = now
        ts_data = wallet_cache[chain_id].get(wallet)
        if ts_data:
            last_ts = ts_data["last_ts"]

        funding_source = None
        if wallet_type == "fresh":
            funding_source = get_wallet_funding_source(chain_id, wallet)

        version = swap_log.get("_swap_version", "")
        log.info(
            "[%s] %s wallet buy (uni%s): wallet=%s token=%s",
            chain_id, wallet_type, version, wallet[:10], token_address[:10],
        )
        interesting_count += 1

        cluster_buys[chain_id][token_address].append({
            "wallet": wallet,
            "wallet_type": wallet_type,
            "timestamp": now,
            "last_ts": last_ts,
            "tx_hash": tx_hash,
            "funding_source": funding_source,
        })

    log.info(
        "[%s] Swap processing: %d total, %d decoded, %d fresh/dormant",
        chain_id, len(swap_logs), decoded_count, interesting_count,
    )


# ---------------------------------------------------------------------------
# Solana — Helius API helpers
# ---------------------------------------------------------------------------


def helius_get(endpoint: str, params: dict | None = None, _retries: int = 3) -> list | dict | None:
    """GET request to Helius Enhanced Transactions API with 429 backoff."""
    url = f"{HELIUS_API_URL}{endpoint}"
    p = {"api-key": HELIUS_API_KEY}
    if params:
        p.update(params)
    for attempt in range(_retries):
        try:
            r = requests.get(url, params=p, timeout=15)
            if r.status_code == 429:
                wait = 2 ** attempt  # 1s, 2s, 4s
                log.debug("[solana] Helius 429 on %s — backing off %ds", endpoint, wait)
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r.json()
        except Exception as exc:
            if attempt < _retries - 1:
                time.sleep(1)
            else:
                log.warning("[solana] Helius GET %s failed: %s", endpoint, exc)
    return None


def helius_rpc(method: str, params: list, _retries: int = 3) -> object:
    """JSON-RPC call via Helius RPC endpoint with 429 backoff."""
    for attempt in range(_retries):
        try:
            r = requests.post(
                HELIUS_RPC_URL,
                params={"api-key": HELIUS_API_KEY},
                json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
                timeout=10,
            )
            if r.status_code == 429:
                wait = 2 ** attempt  # 1s, 2s, 4s
                log.debug("[solana] Helius RPC 429 on %s — backoff %ds", method, wait)
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r.json().get("result")
        except Exception as exc:
            if attempt < _retries - 1:
                time.sleep(1)
            else:
                log.warning("[solana] Helius RPC %s failed: %s", method, exc)
    return None


PUMPFUN_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"

# Tracks newly minted Pump.fun tokens so we start watching them immediately
# token_mint -> mint_timestamp
pumpfun_new_tokens: dict[str, int] = {}
pumpfun_last_mint_sig: str = ""


def helius_poll_pumpfun_launches() -> int:
    """
    Poll Pump.fun program for new token mints (PUMP_FUN_MINT transactions).
    Registers newly launched tokens in pumpfun_new_tokens so the swap scanner
    immediately tracks fresh wallet buys on them. Returns count of new launches.
    """
    global pumpfun_last_mint_sig
    params: dict = {"type": "UNKNOWN", "limit": 50}  # Pump.fun mints appear as UNKNOWN type
    if pumpfun_last_mint_sig:
        params["until"] = pumpfun_last_mint_sig

    data = helius_get(f"/addresses/{PUMPFUN_PROGRAM}/transactions", params=params)
    txs = data if isinstance(data, list) else []

    new_count = 0
    for tx in txs:
        sig = tx.get("signature", "")
        if not sig:
            continue
        if new_count == 0:
            pumpfun_last_mint_sig = sig

        # Look for token mint creation in tokenTransfers with no fromUserAccount
        # (minting from nothing = new token creation)
        for transfer in tx.get("tokenTransfers", []):
            mint = transfer.get("mint", "")
            from_acct = transfer.get("fromUserAccount", "")
            if mint and not from_acct and mint not in pumpfun_new_tokens:
                ts = tx.get("timestamp") or NOW_TS()
                pumpfun_new_tokens[mint] = int(ts)
                log.info("[solana] NEW Pump.fun launch: %s at %s", mint[:10], datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%H:%M:%S UTC"))
                new_count += 1

        new_count += 1  # count all txs processed for pagination

    return new_count


def helius_get_recent_swaps(program_id: str, until_sig: str = "") -> list[dict]:
    """
    Get recent SWAP transactions for a Solana DEX program via Helius
    Enhanced Transactions API. Returns newest-first list.
    `until_sig` stops fetching at (exclusive) this signature — used to
    retrieve only transactions newer than the last poll.
    """
    params: dict = {"type": "SWAP", "limit": 100}
    if until_sig:
        params["until"] = until_sig
    data = helius_get(f"/addresses/{program_id}/transactions", params=params)
    if isinstance(data, list):
        return data
    return []


def helius_get_wallet_timestamps(address: str) -> dict | None:
    """
    Returns {first_ts, last_ts} for a Solana wallet using Helius RPC
    getSignaturesForAddress. Cached.

    Strategy:
    - Fetch up to 1000 signatures (newest → oldest).
    - last_ts  = blockTime of the first (newest) signature.
    - first_ts = blockTime of the last  (oldest) in the batch.
    - If the batch is < 1000 entries we have the full history → first_ts
      is truly the wallet's first-ever transaction.
    - If the batch is 1000 entries the wallet is old/active; first_ts is
      a lower-bound approximation sufficient for dormant detection.
    """
    cache = wallet_cache["solana"]
    if address in cache:
        return cache[address]

    sigs = helius_rpc("getSignaturesForAddress", [address, {"limit": 1000}])
    if not sigs or not isinstance(sigs, list):
        return None

    last_ts = sigs[0].get("blockTime") if sigs else None
    first_ts = sigs[-1].get("blockTime") if sigs else last_ts

    if last_ts is None:
        return None

    result = {"first_ts": int(first_ts or last_ts), "last_ts": int(last_ts), "tx_count": len(sigs)}
    cache[address] = result
    return result


def classify_solana_wallet(address: str) -> str | None:
    """Classify a Solana wallet as 'fresh', 'dormant', or None."""
    ts = helius_get_wallet_timestamps(address)
    if not ts:
        return None

    now = NOW_TS()
    # Fresh: wallet has < 1000 total txs AND oldest known tx is < 24h
    age_hours = (now - ts["first_ts"]) / 3600
    inactive_days = (now - ts["last_ts"]) / 86400

    if ts["tx_count"] < 1000 and age_hours <= FRESH_WALLET_MAX_AGE_HOURS:
        return "fresh"
    if inactive_days >= DORMANT_WALLET_MIN_INACTIVE_DAYS:
        return "dormant"
    return None


def get_solana_funding_source(address: str) -> str | None:
    """
    Check earliest transactions to see if wallet was funded by a known CEX.
    Uses RPC getTransaction (cheaper than Enhanced API) on the 2 oldest sigs.
    """
    sigs = helius_rpc("getSignaturesForAddress", [address, {"limit": 10}])
    if not sigs:
        return None
    # Check only the 2 oldest signatures (reversed list = oldest last)
    oldest_sigs = [s["signature"] for s in reversed(sigs)][:2]
    first_sender_found = None
    for sig in oldest_sigs:
        result = helius_rpc("getTransaction", [sig, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}])
        if not result:
            continue
        for instruction in (result.get("transaction", {}).get("message", {}).get("instructions") or []):
            parsed = instruction.get("parsed")
            if not isinstance(parsed, dict):
                continue
            info = parsed.get("info") or {}
            sender = info.get("source") or info.get("authority") or ""
            if sender and not first_sender_found:
                first_sender_found = sender  # capture raw funder (Improvement 5)
            for known_addr, label in SOLANA_KNOWN_FUNDING_SOURCES.items():
                if sender == known_addr:
                    wallet_funder_cache[address] = sender
                    return label
        time.sleep(0.1)
    # Store raw funder even if no known CEX matched
    if first_sender_found:
        wallet_funder_cache[address] = first_sender_found
    return None


def get_solana_token_info(token_mint: str) -> dict:
    """Fetch token name/symbol via Helius token-metadata endpoint. Cached."""
    cache = token_info_cache["solana"]
    if token_mint in cache:
        return cache[token_mint]

    # Try DexScreener cache first (populated for free during age lookup)
    if token_mint in dexscreener_name_cache:
        info = dexscreener_name_cache[token_mint]
        cache[token_mint] = info
        return info

    try:
        r = requests.post(
            f"{HELIUS_API_URL}/token-metadata",
            params={"api-key": HELIUS_API_KEY},
            json={"mintAccounts": [token_mint]},
            timeout=10,
        )
        r.raise_for_status()
        results = r.json()
        if results and isinstance(results, list):
            meta = results[0]
            on_chain_meta = (meta.get("onChainMetadata") or {}).get("metadata") or {}
            legacy = meta.get("legacyMetadata") or {}
            # Try multiple paths: metadata.name, metadata.data.name, legacyMetadata.name
            name = (
                on_chain_meta.get("name")
                or (on_chain_meta.get("data") or {}).get("name")
                or legacy.get("name")
                or ""
            )
            symbol = (
                on_chain_meta.get("symbol")
                or (on_chain_meta.get("data") or {}).get("symbol")
                or legacy.get("symbol")
                or ""
            )
            if name or symbol:
                info = {"name": name.strip("\x00") or "Unknown", "symbol": symbol.strip("\x00") or "???"}
            else:
                info = {"name": "Unknown", "symbol": "???"}
        else:
            info = {"name": "Unknown", "symbol": "???"}
    except Exception as exc:
        log.debug("[solana] Token metadata fetch failed for %s: %s", token_mint[:10], exc)
        info = {"name": "Unknown", "symbol": "???"}

    # If Helius still returned Unknown, try DexScreener directly
    if info["name"] == "Unknown":
        try:
            r2 = requests.get(
                f"https://api.dexscreener.com/latest/dex/tokens/{token_mint}",
                timeout=8,
            )
            r2.raise_for_status()
            pairs = r2.json().get("pairs") or []
            if pairs:
                base = pairs[0].get("baseToken") or {}
                ds_name = base.get("name") or ""
                ds_symbol = base.get("symbol") or ""
                if ds_name:
                    info = {"name": ds_name, "symbol": ds_symbol or "???"}
                    dexscreener_name_cache[token_mint] = info
        except Exception:
            pass

    cache[token_mint] = info
    return info


def build_solana_alert(
    token_address: str,
    buys: list[dict],
    token_age_hours: float | None = None,
    enrichment: dict | None = None,
    conviction_level: str | None = None,
    conviction_score: int | None = None,
    conviction_reasons: list[str] | None = None,
) -> str:
    """Build a Telegram alert message for a Solana token cluster."""
    info = get_solana_token_info(token_address)
    token_name = info["name"]
    token_symbol = info["symbol"]

    fresh_buys = [b for b in buys if b["wallet_type"] == "fresh"]
    dormant_buys = [b for b in buys if b["wallet_type"] == "dormant"]

    sources = [b.get("funding_source") for b in fresh_buys]
    all_unknown = not any(s and s.lower() not in ("unknown", "none") for s in sources)
    _, tier = score_cluster(len(fresh_buys), len(dormant_buys), len(buys), len(buys), all_funding_unknown=all_unknown)

    age_str = _format_age(token_age_hours)
    ts = datetime.now(timezone.utc).strftime("%H:%M UTC")

    liq = token_liquidity_cache.get(token_address)
    liq_str = f"${liq:,.0f}" if liq is not None else "?"
    sells = token_sells_h1_cache.get(token_address)
    buys_1h = token_buys_h1_cache.get(token_address)
    mcap = token_mcap_cache.get(token_address)
    price = token_price_cache.get(token_address)
    top_holders_str = _fmt_top_holders(token_top_holders_cache.get(token_address))

    wallet_lines = []
    for b in fresh_buys:
        short_w = b["wallet"][:6] + "..." + b["wallet"][-4:]
        funding = b.get("funding_source") or "unknown"
        tx_url = f"https://solscan.io/tx/{b['tx_hash']}"
        wallet_lines.append(f"  🆕 <a href='{tx_url}'>{short_w}</a> ({funding})")
    for b in dormant_buys:
        short_w = b["wallet"][:6] + "..." + b["wallet"][-4:]
        tx_url = f"https://solscan.io/tx/{b['tx_hash']}"
        inactive_days = int((NOW_TS() - b["last_ts"]) / 86400)
        wallet_lines.append(f"  💤 <a href='{tx_url}'>{short_w}</a> (dormant {inactive_days}d)")

    summary_parts = []
    if fresh_buys:
        summary_parts.append(f"{len(fresh_buys)} fresh 🆕")
    if dormant_buys:
        summary_parts.append(f"{len(dormant_buys)} dormant 💤")

    extra_signals = _gather_alert_signals(token_name, token_address, "solana", buys)
    if enrichment and enrichment.get("dev_funded"):
        n = len(enrichment["dev_funded"])
        pct = n / len(buys) * 100
        extra_signals.insert(0, f"🚨 DEV FUNDED: {n}/{len(buys)} wallets funded by deployer ({pct:.0f}%)")

    liq_line = f"💧 Liq: {liq_str}  |  Buys/1h: {buys_1h or '?'}  |  Sells/1h: {sells or '?'}"
    if top_holders_str:
        liq_line += f"  |  {top_holders_str}"

    lines = []
    if conviction_level and conviction_score is not None:
        lines.append(_fmt_conviction_block(conviction_level, conviction_score, conviction_reasons or []))
        lines.append("")

    lines += [
        f"{tier} <b>PiggyBac Alert</b> [Solana] — {ts}",
        "",
        f"🪙 <b>{token_name}</b> ({token_symbol})" + (f" · <b>{age_str}</b>" if age_str else ""),
        f"💰 MCap: <b>{fmt_mcap(mcap)}</b>  |  Price: <b>{fmt_price(price)}</b>",
        liq_line,
    ]

    # Deployer info
    if enrichment:
        deployer_line = _fmt_deployer_line(enrichment.get("deployer_info"))
        if deployer_line:
            lines.append(deployer_line)

    lines += [
        "",
        f"⚡ <b>{' + '.join(summary_parts)}</b> in {CLUSTER_TIME_WINDOW_MINUTES}min",
    ]
    for sig in extra_signals:
        if sig:
            lines.append(sig)

    # Dormant wallet PnL profiles
    if enrichment and enrichment.get("dormant_profiles"):
        lines.append("")
        lines.append("🏆 <b>Dormant wallet profiles:</b>")
        for p in enrichment["dormant_profiles"]:
            short_w = p["wallet"][:6] + "..." + p["wallet"][-4:]
            label = SCORE_EMOJI.get(p.get("score", "unknown"), "❓")
            analyzed = p.get("tokens_analyzed", 0)
            winners = p.get("winners", 0)
            detail = f"{winners}/{analyzed} winners" if analyzed > 0 else "no history"
            lines.append(f"  😴 {short_w} — {label}: {detail}")

    lines.append("")
    lines.extend(wallet_lines)

    return "\n".join(lines)


def build_solana_alert_with_age(
    token_address: str,
    buys: list[dict],
    token_age_hours: float | None,
    enrichment: dict | None = None,
    conviction_level: str | None = None,
    conviction_score: int | None = None,
    conviction_reasons: list[str] | None = None,
) -> str:
    """Compatibility wrapper — delegates to build_solana_alert with age argument."""
    return build_solana_alert(
        token_address, buys, token_age_hours,
        enrichment, conviction_level, conviction_score, conviction_reasons,
    )


def check_solana_clusters() -> None:
    """Check Solana cluster_buys for alertable clusters."""
    for token_address, buys in cluster_buys["solana"].items():
        seen = {}
        for b in buys:
            seen[b["wallet"]] = b
        unique_buys = list(seen.values())

        if len(unique_buys) < CLUSTER_MIN_WALLETS:
            continue

        log.info("[solana] Potential cluster: %d wallets on %s — checking filters",
                 len(unique_buys), token_address[:10])
        token_age = get_token_age_hours(token_address, "solana")
        if is_token_too_old(token_address, "solana"):
            log.info("[solana] SKIP %s — token too old (%.1fh)", token_address[:10], token_age or -1)
            continue

        fail_reason = token_quality_fail_reason(token_address)
        if fail_reason:
            log.info("[solana] Skipping token %s — %s", token_address[:10], fail_reason)
            continue

        # Permanent session dedup — once alerted, never alert again for this token
        if token_address in alerted_clusters:
            continue
        # Fallback cooldown for container restarts
        last_alert_ts = token_last_alerted["solana"].get(token_address, 0)
        if NOW_TS() - last_alert_ts < ALERT_COOLDOWN_SECONDS:
            continue
        alerted_clusters.add(token_address)
        token_last_alerted["solana"][token_address] = NOW_TS()

        fresh_count = sum(1 for b in unique_buys if b["wallet_type"] == "fresh")
        dormant_count = sum(1 for b in unique_buys if b["wallet_type"] == "dormant")
        log.info(
            "[solana] CLUSTER: %d fresh + %d dormant wallets bought %s (age %.1fh)",
            fresh_count, dormant_count, token_address, token_age or -1,
        )
        get_top_holder_pct(token_address, "solana")  # populate cache before alert
        enrichment = enrich_cluster(token_address, "solana", unique_buys)

        # Conviction score — suppress very low conviction alerts
        sources = [b.get("funding_source") for b in unique_buys if b.get("wallet_type") == "fresh"]
        known_src = [s for s in sources if s and s.lower() not in ("unknown", "none")]
        relaunch_note = check_name_relaunch(
            get_solana_token_info(token_address)["name"], token_address, "solana"
        )
        overlap_count = sum(1 for b in unique_buys if wallet_cluster_history.get(b["wallet"]))
        c_level, c_score, c_reasons = calculate_conviction(
            wallet_count=len(unique_buys),
            dormant_profiles=enrichment["dormant_profiles"],
            all_funding_unknown=not known_src and bool(sources),
            funding_diversity=len(set(known_src)),
            is_relaunch=bool(relaunch_note),
            relaunch_attempt=len(recent_token_names.get(
                normalize_token_name(get_solana_token_info(token_address)["name"]), []
            )) + 1,
            wallet_overlap_count=overlap_count,
            deployer_info=enrichment["deployer_info"],
            dev_funded_count=len(enrichment["dev_funded"]),
            buys_h1=token_buys_h1_cache.get(token_address),
            top_holders_pct=token_top_holders_cache.get(token_address),
        )
        if c_score < MIN_CONVICTION_TO_ALERT:
            log.info("[solana] SUPPRESSED (conviction %d/100): %s", c_score, token_address[:10])
            continue

        message = build_solana_alert_with_age(token_address, unique_buys, token_age, enrichment, c_level, c_score, c_reasons)
        is_pump = any(b.get("dex") == "Pump.fun" for b in unique_buys)
        keyboard = make_alert_keyboard(
            dexscreener_url=f"https://dexscreener.com/solana/{token_address}",
            explorer_url=f"https://solscan.io/token/{token_address}",
            pump_fun_url=f"https://pump.fun/coin/{token_address}" if is_pump else None,
            socials=token_socials_cache.get(token_address),
        )
        sent_ids = send_telegram(message, reply_markup=keyboard)
        info = get_solana_token_info(token_address)
        record_token_name(info["name"], token_address, "solana")
        record_cluster_wallets(info["name"], token_address, "solana", [b["wallet"] for b in unique_buys])
        schedule_confirmation_ping(
            token_address, "solana", info["name"],
            token_price_cache.get(token_address), token_mcap_cache.get(token_address),
            sent_ids,
        )
        schedule_performance_review(
            token_address, "solana", "solana",
            info["name"], info["symbol"],
            f"https://solscan.io/token/{token_address}",
        )


def process_helius_swaps(txs: list[dict], program_id: str) -> int:
    """
    Process a batch of Helius enhanced SWAP transactions for one DEX program.
    Extracts wallet + token-bought, classifies wallet, records into cluster_buys.
    Returns count of new swaps processed.
    """
    now = NOW_TS()
    dex_label = SOLANA_DEX_PROGRAMS.get(program_id, program_id[:10])
    processed = 0

    for tx in txs:
        tx_hash = tx.get("signature") or ""
        if not tx_hash:
            continue

        block_time = tx.get("timestamp") or now
        wallet = tx.get("feePayer") or ""
        if not wallet:
            continue

        # Find the token being bought: look in tokenTransfers for a transfer
        # TO the feePayer that is not WSOL (i.e. the token received in the swap).
        token_bought = None
        for transfer in tx.get("tokenTransfers", []):
            mint = transfer.get("mint") or ""
            if transfer.get("toUserAccount") == wallet and mint and mint != WSOL_MINT:
                token_bought = mint
                break

        # Fall back to swap event outputs if tokenTransfers didn't resolve it
        if not token_bought:
            swap_event = (tx.get("events") or {}).get("swap") or {}
            for out in swap_event.get("tokenOutputs", []):
                mint = out.get("mint") or ""
                if mint and mint != WSOL_MINT:
                    token_bought = mint
                    break

        if not token_bought:
            continue

        wallet_type = classify_solana_wallet(wallet)
        time.sleep(0.15)  # gentle rate limit between RPC wallet lookups
        if wallet_type is None:
            processed += 1
            continue

        last_ts = now
        ts_data = wallet_cache["solana"].get(wallet)
        if ts_data:
            last_ts = ts_data["last_ts"]

        funding_source = None
        if wallet_type == "fresh":
            funding_source = get_solana_funding_source(wallet)

        log.info(
            "[solana] %s wallet buy: wallet=%s token=%s dex=%s",
            wallet_type, wallet[:10], token_bought[:10], dex_label,
        )

        cluster_buys["solana"][token_bought].append({
            "wallet": wallet,
            "wallet_type": wallet_type,
            "timestamp": int(block_time),
            "last_ts": last_ts,
            "tx_hash": tx_hash,
            "funding_source": funding_source,
            "dex": dex_label,
        })
        processed += 1

    return processed


def prune_solana_old_buys() -> None:
    cutoff = NOW_TS() - CLUSTER_TIME_WINDOW_MINUTES * 60
    buys = cluster_buys["solana"]
    for token in list(buys.keys()):
        buys[token] = [b for b in buys[token] if b["timestamp"] >= cutoff]
        if not buys[token]:
            del buys[token]


def scan_solana() -> None:
    """
    Main Solana scanner loop.
    Polls each DEX program via Helius Enhanced Transactions API for recent
    SWAP transactions. Uses per-program cursor (until_sig) to fetch only
    new transactions since the last poll.
    """
    key_preview = (HELIUS_API_KEY[:8] + "..." + HELIUS_API_KEY[-4:]) if len(HELIUS_API_KEY) > 12 else f"(len={len(HELIUS_API_KEY)})"
    log.info("[Solana] Starting up via Helius... key: %s", key_preview)

    # Quick connectivity check — single attempt, proceed regardless
    test = helius_rpc("getSlot", [])
    if test is not None:
        log.info("[Solana] Helius connected (slot %s). Monitoring %d DEX programs.", test, len(SOLANA_DEX_PROGRAMS))
    else:
        log.warning("[Solana] Helius connectivity check failed — starting anyway (will self-recover)")

    while True:
        total_new = 0
        try:
            # Check for brand new Pump.fun token launches first
            new_launches = helius_poll_pumpfun_launches()
            if new_launches:
                log.info("[solana] Pump.fun: %d new token launches detected", new_launches)
            time.sleep(0.5)

            for program_id, dex_name in SOLANA_DEX_PROGRAMS.items():
                until_sig = solana_program_last_sig.get(program_id, "")
                txs = helius_get_recent_swaps(program_id, until_sig=until_sig)

                if txs:
                    # Record newest sig so next poll fetches only newer txs
                    solana_program_last_sig[program_id] = txs[0].get("signature", until_sig)
                    count = process_helius_swaps(txs, program_id)
                    total_new += count
                    log.debug("[solana] %s: %d swaps, %d new wallets of interest", dex_name, len(txs), count)

                time.sleep(0.5)  # gentle rate-limit between programs

            log.info("[Solana] Poll complete — %d fresh/dormant swaps across all DEXes", total_new)
            prune_solana_old_buys()
            check_solana_clusters()

        except Exception as exc:
            log.error("[solana] Error: %s", exc, exc_info=True)

        time.sleep(POLL_INTERVAL_SECONDS)


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
            swap_logs = get_swap_logs(
                chain_id, last_block_checked[chain_id] + 1, current_block
            )
            log.info("[%s] Got %d swap events (V2+V3)", chain["name"], len(swap_logs))

            process_swap_logs(chain_id, swap_logs)
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

    # Seed subscriber list from env var so existing chat still receives alerts
    if TELEGRAM_CHAT_ID:
        telegram_subscribers.add(TELEGRAM_CHAT_ID)
        log.info("[Telegram] Seeded subscriber from TELEGRAM_CHAT_ID (%s)", TELEGRAM_CHAT_ID)

    # Start Telegram command listener (handles /start, /stop)
    if TELEGRAM_BOT_TOKEN:
        t = threading.Thread(target=poll_telegram_commands, daemon=True)
        t.start()
        threads.append(t)

    # Start 24h performance review scheduler
    t = threading.Thread(target=run_performance_reviews, daemon=True)
    t.start()
    threads.append(t)
    log.info("[review] 24h performance review scheduler started")

    # Start EVM chain scanners (Base + Ethereum) — disabled until Alchemy quota resets
    # Re-enable by setting ENABLE_EVM_CHAINS=true in Railway environment variables
    if ENABLE_EVM_CHAINS:
        for chain_id in CHAINS:
            chain_name = CHAINS[chain_id]["name"]
            if ALCHEMY_KEYS[chain_id]:
                log.info("[%s] Using Alchemy RPC", chain_name)
            else:
                log.info("[%s] No Alchemy key — using public RPC (%s) + Basescan/Etherscan API",
                         chain_name, PUBLIC_RPC_URLS[chain_id])
            t = threading.Thread(target=scan_chain, args=(chain_id,), daemon=True)
            t.start()
            threads.append(t)
            time.sleep(2)  # stagger startup to avoid rate limit spike
    else:
        log.info("[EVM] Base + Ethereum scanners disabled (set ENABLE_EVM_CHAINS=true to enable)")

    # Start Solana scanner
    if HELIUS_API_KEY:
        t = threading.Thread(target=scan_solana, daemon=True)
        t.start()
        threads.append(t)
        log.info("Solana scanner thread started")
    else:
        log.warning("HELIUS_API_KEY not set — skipping Solana")

    if not threads:
        log.error("No scanners started. Set HELIUS_API_KEY for Solana. EVM chains use public RPCs by default.")
        return

    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        log.info("Shutting down.")


if __name__ == "__main__":
    main()
