# PiggyBac — Claude Code Handoff

## What this is
A Telegram bot that monitors Base chain for **coordinated buying by freshly-funded wallets**. When 5+ wallets that were created/funded in the last 24 hours all buy the same token within a 2-hour window, it fires a Telegram alert with the token details, wallet list, funding sources, and links to DexScreener/Basescan.

## Why it matters
Fresh wallets converging on a single token is one of the strongest on-chain signals for insider/team accumulation before a push. Organic retail buys come from aged wallets with messy histories. Coordinated fresh wallet buys = someone knows something.

## Current state
- **Working MVP** in `piggybac.py` — runs locally, scans Base chain via Basescan API, sends alerts via Telegram
- **Known limitations that need fixing** (in priority order):

### P0 — Fix before first real run
1. **Swap detection is too naive**: Currently uses raw ERC-20 Transfer events. Needs to decode actual DEX swap events (Uniswap V3 `Swap` event topic: `0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67`). This will dramatically reduce false positives.
2. **No token metadata**: Alerts show contract addresses instead of token names/symbols. Add a Basescan token info lookup with caching.
3. **ETH value is hardcoded placeholder**: `estimated_eth = 0.01` is a dummy. Need to either parse the WETH leg of the swap or use a simple price lookup.

### P1 — Improve after first run
4. **Filter out non-buy transfers**: Token approvals, migrations, airdrops, and LP additions trigger false positives. Filter by checking if the `from` address is a known DEX pair/pool contract.
5. **Add contract age check**: If the token contract itself was deployed < 24h ago AND fresh wallets are buying, that's an even stronger signal. Flag it in the alert.
6. **Smarten up rate limiting**: During high-volume periods, batch wallet age lookups and skip wallets we've already classified as "not fresh" in the current session.

### P2 — Nice to have
7. **Persistent storage**: Currently all in-memory. Add SQLite to survive restarts and track historical alerts.
8. **Dashboard**: Simple web UI showing alert history, cluster details, hit rate.
9. **Multi-chain**: Add Ethereum mainnet support (same logic, different API endpoint).
10. **Polymarket integration**: Track fresh wallets on Polygon making large prediction market bets.

## Architecture
```
piggybac.py          — Single-file bot. Detection loop + Telegram integration.
.env.example         — Template for API keys (Basescan + Telegram)
requirements.txt     — Python dependencies
README.md            — Setup docs
CLAUDE.md            — This file. Full context for Claude Code.
```

The bot runs a simple loop:
1. Poll Basescan for recent ERC-20 transfer events (every 30s)
2. For each transfer TO a wallet, check wallet age via Basescan API
3. If wallet is <24h old, log as fresh wallet buy
4. If 5+ fresh wallets bought same token in 2h window → Telegram alert

## Tech stack
- Python 3.10+
- `requests` for HTTP
- `python-dotenv` for config
- Basescan API (free tier, 5 calls/sec)
- Telegram Bot API (free)

## Config / env vars
```
BASESCAN_API_KEY    — from basescan.org/apis
TELEGRAM_BOT_TOKEN  — from @BotFather on Telegram
TELEGRAM_CHAT_ID    — from @userinfobot on Telegram
```

## Key constants to tune
All at the top of `piggybac.py`:
- `FRESH_WALLET_MAX_AGE_HOURS = 24` — max wallet age to count as "fresh"
- `CLUSTER_MIN_WALLETS = 5` — threshold to trigger alert
- `CLUSTER_TIME_WINDOW_HOURS = 2` — rolling window for clustering
- `POLL_INTERVAL_SECONDS = 30` — scan frequency
- `MIN_BUY_VALUE_USD = 50` — ignore dust

## Deployment target
Railway.app or a cheap VPS. Needs to run 24/7 as a long-running process. No web server needed — it's a pure background worker.

## Commands
```bash
# Install
pip install -r requirements.txt

# Run locally
cp .env.example .env  # fill in your keys
python piggybac.py

# Deploy to Railway
railway init
railway up
```

## Important context
- Base chain block time is ~2 seconds
- Basescan free API: 5 calls/sec, 100k calls/day
- Known CEX hot wallets and bridge addresses are hardcoded in `KNOWN_FUNDING_SOURCES` dict — these may need updating
- The `DEX_ROUTERS` dict has major Base DEX addresses — add new ones as Base DEX landscape evolves
- WETH on Base: `0x4200000000000000000000000000000000000006`

## On-chain constants (Base mainnet)
```
WETH:               0x4200000000000000000000000000000000000006
Uniswap V3 Router:  0x2626664c2603336E57B271c5C0b26F421741e481
Uniswap V3 Factory: 0x33128a8fC17869897dcE68Ed026d694621f6FDfD
Aerodrome Router:   0xcF77a3Ba9A5CA399B7c97c74d54e5b1Beb874E43
BaseSwap Router:    0x327Df1E6de05895d2ab08513aaDD9313Fe505d86
Uniswap V3 Swap topic: 0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67
```
