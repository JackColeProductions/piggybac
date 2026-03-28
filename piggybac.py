"""
PiggyBac — Base chain fresh-wallet buy detector.
Monitors ERC-20 transfer events, identifies wallets created <24h ago,
and fires a Telegram alert when 5+ fresh wallets buy the same token
within a 2-hour window.
"""

import os
import time
import logging
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

BASESCAN_API_KEY = os.getenv("BASESCAN_API_KEY", "")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

FRESH_WALLET_MAX_AGE_HOURS = 24
CLUSTER_MIN_WALLETS = 5
CLUSTER_TIME_WINDOW_HOURS = 2
POLL_INTERVAL_SECONDS = 30
MIN_BUY_VALUE_USD = 50  # placeholder — used once ETH price is wired up

BASESCAN_BASE_URL = "https://api.basescan.org/api"
WETH = "0x4200000000000000000000000000000000000006"

# ---------------------------------------------------------------------------
# Known addresses
# ---------------------------------------------------------------------------

# Major DEX routers/factories on Base — transfers FROM these are likely buys
DEX_ROUTERS = {
    "0x2626664c2603336E57B271c5C0b26F421741e481": "Uniswap V3 Router",
    "0x33128a8fC17869897dcE68Ed026d694621f6FDfD": "Uniswap V3 Factory",
    "0xcF77a3Ba9A5CA399B7c97c74d54e5b1Beb874E43": "Aerodrome Router",
    "0x327Df1E6de05895d2ab08513aaDD9313Fe505d86": "BaseSwap Router",
    "0x4752ba5DBc23f44D87826276BF6Fd6b1C372aD24": "Uniswap V2 Router",
}

# Known CEX hot wallets, bridges, faucets — funding from these = fresh wallet
KNOWN_FUNDING_SOURCES = {
    "0x3304E22DDaa22bCdC5fCa2269b418046aE7b566A": "Coinbase",
    "0xA9D1e08C7793af67e9d92fe308d5697FB81d3E43": "Coinbase Prime",
    "0x77696bb39917C91A0c3908D577d5e322095425cA": "Base Bridge",
    "0x4200000000000000000000000000000000000010": "Base L2 Bridge",
    "0x6DfD7D42c20e9D73B174B00c1fdE2a29F3B99BC4": "Binance",
}

# Uniswap V3 Swap event topic — P0: switch to this for real swap detection
UNISWAP_V3_SWAP_TOPIC = (
    "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"
)

# ---------------------------------------------------------------------------
# State (in-memory; P2 will move this to SQLite)
# ---------------------------------------------------------------------------

# token_address -> list of {wallet, timestamp, tx_hash, funding_source}
fresh_wallet_buys: dict[str, list[dict]] = defaultdict(list)

# wallet_address -> first_tx_timestamp (cache to avoid repeat API calls)
wallet_age_cache: dict[str, int] = {}

# token_address -> {name, symbol} (cache)
token_info_cache: dict[str, dict] = {}

# track which clusters we've already alerted on to avoid duplicates
alerted_clusters: set[str] = set()

# last block we processed
last_block_checked: int = 0

# ---------------------------------------------------------------------------
# Basescan helpers
# ---------------------------------------------------------------------------


def basescan_get(params: dict) -> dict | None:
    params["apikey"] = BASESCAN_API_KEY
    try:
        r = requests.get(BASESCAN_BASE_URL, params=params, timeout=10)
        r.raise_for_status()
        data = r.json()
        if data.get("status") == "1" or data.get("message") == "OK":
            return data
        log.debug("Basescan non-1 status: %s", data.get("message"))
        return None
    except Exception as exc:
        log.warning("Basescan request failed: %s", exc)
        return None


def get_latest_block() -> int:
    data = basescan_get({"module": "proxy", "action": "eth_blockNumber"})
    if data:
        return int(data["result"], 16)
    return 0


def get_erc20_transfers(from_block: int, to_block: int) -> list[dict]:
    """Fetch recent ERC-20 Transfer events across all tokens."""
    data = basescan_get(
        {
            "module": "logs",
            "action": "getLogs",
            "fromBlock": from_block,
            "toBlock": to_block,
            # ERC-20 Transfer topic
            "topic0": "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
            "page": 1,
            "offset": 1000,
        }
    )
    return data.get("result", []) if data else []


def get_wallet_first_tx_timestamp(address: str) -> int | None:
    """Return Unix timestamp of wallet's first-ever transaction, or None."""
    if address in wallet_age_cache:
        return wallet_age_cache[address]
    data = basescan_get(
        {
            "module": "account",
            "action": "txlist",
            "address": address,
            "startblock": 0,
            "endblock": 99999999,
            "page": 1,
            "offset": 1,
            "sort": "asc",
        }
    )
    if data and data.get("result"):
        ts = int(data["result"][0]["timeStamp"])
        wallet_age_cache[address] = ts
        return ts
    return None


def get_wallet_funding_source(address: str) -> str | None:
    """Return a label if the wallet's first incoming tx is from a known source."""
    data = basescan_get(
        {
            "module": "account",
            "action": "txlist",
            "address": address,
            "startblock": 0,
            "endblock": 99999999,
            "page": 1,
            "offset": 5,
            "sort": "asc",
        }
    )
    if not data or not data.get("result"):
        return None
    for tx in data["result"]:
        sender = tx.get("from", "").lower()
        for known_addr, label in KNOWN_FUNDING_SOURCES.items():
            if sender == known_addr.lower():
                return label
    return None


def get_token_info(token_address: str) -> dict:
    """Return {name, symbol} for a token contract. Cached."""
    if token_address in token_info_cache:
        return token_info_cache[token_address]
    # P0: replace with real Basescan token lookup
    info = {"name": "Unknown", "symbol": "???"}
    token_info_cache[token_address] = info
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


def build_alert(token_address: str, buys: list[dict]) -> str:
    info = get_token_info(token_address)
    token_name = info["name"]
    token_symbol = info["symbol"]
    short_addr = token_address[:6] + "..." + token_address[-4:]
    basescan_url = f"https://basescan.org/token/{token_address}"
    dexscreener_url = f"https://dexscreener.com/base/{token_address}"

    wallet_lines = []
    for b in buys:
        wallet = b["wallet"]
        short_w = wallet[:6] + "..." + wallet[-4:]
        funding = b.get("funding_source") or "unknown"
        tx_url = f"https://basescan.org/tx/{b['tx_hash']}"
        wallet_lines.append(
            f"  • <a href='{tx_url}'>{short_w}</a> (funded via {funding})"
        )

    wallets_str = "\n".join(wallet_lines)
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    return (
        f"🐷 <b>PiggyBac Alert</b> — {ts}\n\n"
        f"<b>Token:</b> {token_name} ({token_symbol})\n"
        f"<b>Address:</b> <a href='{basescan_url}'>{short_addr}</a>\n"
        f"<b>Fresh wallets buying:</b> {len(buys)}\n\n"
        f"{wallets_str}\n\n"
        f"<a href='{dexscreener_url}'>DexScreener</a> | "
        f"<a href='{basescan_url}'>Basescan</a>"
    )


# ---------------------------------------------------------------------------
# Detection logic
# ---------------------------------------------------------------------------

NOW_TS = lambda: int(time.time())


def is_fresh_wallet(wallet: str) -> bool:
    first_ts = get_wallet_first_tx_timestamp(wallet)
    if first_ts is None:
        return False
    age_hours = (NOW_TS() - first_ts) / 3600
    return age_hours <= FRESH_WALLET_MAX_AGE_HOURS


def prune_old_buys() -> None:
    """Drop buy records older than the cluster window."""
    cutoff = NOW_TS() - CLUSTER_TIME_WINDOW_HOURS * 3600
    for token in list(fresh_wallet_buys.keys()):
        fresh_wallet_buys[token] = [
            b for b in fresh_wallet_buys[token] if b["timestamp"] >= cutoff
        ]
        if not fresh_wallet_buys[token]:
            del fresh_wallet_buys[token]


def check_for_clusters() -> None:
    """Fire alerts for any token that has hit the threshold."""
    for token_address, buys in fresh_wallet_buys.items():
        if len(buys) < CLUSTER_MIN_WALLETS:
            continue
        # Deduplicate by wallet
        seen = {}
        for b in buys:
            seen[b["wallet"]] = b
        unique_buys = list(seen.values())
        if len(unique_buys) < CLUSTER_MIN_WALLETS:
            continue
        cluster_key = token_address + str(sorted(b["wallet"] for b in unique_buys))
        if cluster_key in alerted_clusters:
            continue
        alerted_clusters.add(cluster_key)
        log.info(
            "CLUSTER DETECTED: %s fresh wallets bought %s",
            len(unique_buys),
            token_address,
        )
        message = build_alert(token_address, unique_buys)
        send_telegram(message)


def process_transfers(transfers: list[dict]) -> None:
    now = NOW_TS()
    for tx in transfers:
        topics = tx.get("topics", [])
        if len(topics) < 3:
            continue

        # topics[1] = from (padded), topics[2] = to (padded)
        from_addr = "0x" + topics[1][-40:]
        to_addr = "0x" + topics[2][-40:]
        token_address = tx.get("address", "").lower()
        tx_hash = tx.get("transactionHash", "")
        block_ts = int(tx.get("timeStamp", now), 16) if tx.get("timeStamp", "").startswith("0x") else int(tx.get("timeStamp", now))

        # Skip if the recipient is a known DEX (LP add, not a buy)
        if to_addr.lower() in {k.lower() for k in DEX_ROUTERS}:
            continue

        # The buyer is `to_addr` — check if it's fresh
        if not is_fresh_wallet(to_addr):
            continue

        funding_source = get_wallet_funding_source(to_addr)

        # Placeholder ETH value — P0: parse WETH leg of swap
        estimated_eth = 0.01

        log.info(
            "Fresh wallet buy: wallet=%s token=%s tx=%s",
            to_addr[:10],
            token_address[:10],
            tx_hash[:10],
        )

        fresh_wallet_buys[token_address].append(
            {
                "wallet": to_addr,
                "timestamp": block_ts,
                "tx_hash": tx_hash,
                "funding_source": funding_source,
                "estimated_eth": estimated_eth,
            }
        )


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------


def main() -> None:
    global last_block_checked

    if not BASESCAN_API_KEY:
        log.error("BASESCAN_API_KEY not set. Exiting.")
        return

    log.info("PiggyBac starting up...")
    last_block_checked = get_latest_block()
    log.info("Starting from block %d", last_block_checked)

    while True:
        try:
            current_block = get_latest_block()
            if current_block <= last_block_checked:
                time.sleep(POLL_INTERVAL_SECONDS)
                continue

            log.info("Scanning blocks %d → %d", last_block_checked + 1, current_block)
            transfers = get_erc20_transfers(last_block_checked + 1, current_block)
            log.info("Got %d transfer events", len(transfers))

            process_transfers(transfers)
            prune_old_buys()
            check_for_clusters()

            last_block_checked = current_block

        except KeyboardInterrupt:
            log.info("Shutting down.")
            break
        except Exception as exc:
            log.error("Unexpected error: %s", exc, exc_info=True)

        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
