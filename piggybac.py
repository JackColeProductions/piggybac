"""
PiggyBac — Multi-chain fresh-wallet buy detector.
Monitors Base and Ethereum mainnet for coordinated buying by freshly-funded
wallets. When 5+ wallets created/funded in the last 24 hours all buy the same
token within a 2-hour window, fires a Telegram alert.
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

API_KEY = os.getenv("BASESCAN_API_KEY", "")  # works for both Etherscan + Basescan
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

FRESH_WALLET_MAX_AGE_HOURS = 24
CLUSTER_MIN_WALLETS = 5
CLUSTER_TIME_WINDOW_HOURS = 2
POLL_INTERVAL_SECONDS = 30
MIN_BUY_VALUE_USD = 50  # placeholder — used once ETH price is wired up

ERC20_TRANSFER_TOPIC = (
    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
)
UNISWAP_V3_SWAP_TOPIC = (
    "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"
)

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

# chain_id -> token_address -> list of buy dicts
fresh_wallet_buys: dict[str, dict[str, list[dict]]] = {
    chain: defaultdict(list) for chain in CHAINS
}

# chain_id -> wallet_address -> first_tx_timestamp
wallet_age_cache: dict[str, dict[str, int]] = {chain: {} for chain in CHAINS}

# chain_id -> token_address -> {name, symbol}
token_info_cache: dict[str, dict[str, dict]] = {chain: {} for chain in CHAINS}

# set of cluster keys already alerted
alerted_clusters: set[str] = set()

# chain_id -> last block processed
last_block_checked: dict[str, int] = {chain: 0 for chain in CHAINS}

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


def get_wallet_first_tx_timestamp(chain_id: str, address: str) -> int | None:
    cache = wallet_age_cache[chain_id]
    if address in cache:
        return cache[address]
    data = chain_get(
        chain_id,
        {
            "module": "account",
            "action": "txlist",
            "address": address,
            "startblock": 0,
            "endblock": 99999999,
            "page": 1,
            "offset": 1,
            "sort": "asc",
        },
    )
    if data and data.get("result"):
        ts = int(data["result"][0]["timeStamp"])
        cache[address] = ts
        return ts
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

    wallet_lines = []
    for b in buys:
        wallet = b["wallet"]
        short_w = wallet[:6] + "..." + wallet[-4:]
        funding = b.get("funding_source") or "unknown"
        tx_url = f"{chain['explorer_url']}/tx/{b['tx_hash']}"
        wallet_lines.append(
            f"  • <a href='{tx_url}'>{short_w}</a> (funded via {funding})"
        )

    wallets_str = "\n".join(wallet_lines)
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    chain_label = chain["name"]

    return (
        f"🐷 <b>PiggyBac Alert</b> [{chain_label}] — {ts}\n\n"
        f"<b>Token:</b> {token_name} ({token_symbol})\n"
        f"<b>Address:</b> <a href='{explorer_url}'>{short_addr}</a>\n"
        f"<b>Fresh wallets buying:</b> {len(buys)}\n\n"
        f"{wallets_str}\n\n"
        f"<a href='{dexscreener_url}'>DexScreener</a> | "
        f"<a href='{explorer_url}'>Explorer</a>"
    )


# ---------------------------------------------------------------------------
# Detection logic
# ---------------------------------------------------------------------------


def is_fresh_wallet(chain_id: str, wallet: str) -> bool:
    first_ts = get_wallet_first_tx_timestamp(chain_id, wallet)
    if first_ts is None:
        return False
    age_hours = (NOW_TS() - first_ts) / 3600
    return age_hours <= FRESH_WALLET_MAX_AGE_HOURS


def prune_old_buys(chain_id: str) -> None:
    cutoff = NOW_TS() - CLUSTER_TIME_WINDOW_HOURS * 3600
    buys = fresh_wallet_buys[chain_id]
    for token in list(buys.keys()):
        buys[token] = [b for b in buys[token] if b["timestamp"] >= cutoff]
        if not buys[token]:
            del buys[token]


def check_for_clusters(chain_id: str) -> None:
    for token_address, buys in fresh_wallet_buys[chain_id].items():
        if len(buys) < CLUSTER_MIN_WALLETS:
            continue
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
        log.info(
            "[%s] CLUSTER: %s fresh wallets bought %s",
            chain_id,
            len(unique_buys),
            token_address,
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

        if not is_fresh_wallet(chain_id, to_addr):
            continue

        funding_source = get_wallet_funding_source(chain_id, to_addr)

        log.info(
            "[%s] Fresh wallet buy: wallet=%s token=%s",
            chain_id,
            to_addr[:10],
            token_address[:10],
        )

        fresh_wallet_buys[chain_id][token_address].append(
            {
                "wallet": to_addr,
                "timestamp": block_ts,
                "tx_hash": tx_hash,
                "funding_source": funding_source,
                "estimated_eth": 0.01,  # placeholder — P0 fix
            }
        )


# ---------------------------------------------------------------------------
# Per-chain scan loop
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
    if not API_KEY:
        log.error("BASESCAN_API_KEY not set. Exiting.")
        return

    threads = []
    for chain_id in CHAINS:
        t = threading.Thread(target=scan_chain, args=(chain_id,), daemon=True)
        t.start()
        threads.append(t)
        time.sleep(2)  # stagger startup to avoid API rate limit spike

    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        log.info("Shutting down.")


if __name__ == "__main__":
    main()
