# PiggyBac

Telegram bot that monitors Base chain for coordinated buying by freshly-funded wallets. When 5+ wallets created/funded in the last 24 hours all buy the same token within a 2-hour window, it sends a Telegram alert.

## Setup

```bash
pip install -r requirements.txt
cp .env.example .env
# Edit .env with your keys
python piggybac.py
```

## Required env vars

| Variable | How to get it |
|---|---|
| `BASESCAN_API_KEY` | basescan.org/apis (free) |
| `TELEGRAM_BOT_TOKEN` | Message @BotFather → /newbot |
| `TELEGRAM_CHAT_ID` | Message @userinfobot |

## Deploy to Railway

```bash
railway init
railway up
```

## Tuning

Edit the constants at the top of `piggybac.py`:

- `FRESH_WALLET_MAX_AGE_HOURS = 24` — wallet age cutoff
- `CLUSTER_MIN_WALLETS = 5` — wallets needed to trigger alert
- `CLUSTER_TIME_WINDOW_HOURS = 2` — rolling window
- `POLL_INTERVAL_SECONDS = 30` — scan frequency
