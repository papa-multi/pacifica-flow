# Pacifica Telegram Wallet Tracker

This Telegram bot is a minimal Pacifica Flow wallet tracker. The public bot label is `Pacifica profit pulse`. It is focused on:

- adding tracked wallets
- browsing the default wallet lists
- removing tracked wallets
- labeling tracked wallets
- viewing wallet-level open positions
- showing open-position count, first trade, last trade, total PnL, and total volume
- alerting on new opens, closes, increases, and reductions

## Runtime model

- `scripts/telegram_copilot_bot.js` runs the bot loop.
- `src/services/telegram_copilot/*` contains the reusable framework:
  - project config loader
  - persistent JSON state store
  - Telegram API helpers

## Project configuration

Pacifica Flow is configured by files under:

- `config/telegram-copilot/projects/pacifica-flow.json`
- `config/telegram-copilot/prompts/pacifica-flow.md`

To reuse the same bot for another project, add another project config and prompt file and change `PACIFICA_TELEGRAM_COPILOT_PROJECT_KEY`.

## Persistent state

The bot stores:

- watched wallets
- wallet labels
- baseline snapshots
- last-seen position keys
- last check time
- last open count
- Telegram update offset

State is stored in:

- `data/runtime/telegram_copilot_state.json`

If the file or directory is missing, the bot recreates them safely.

## Environment variables

Recommended variables live in `config/runtime.example.env`:

- `PACIFICA_TELEGRAM_COPILOT_ENABLED`
- `PACIFICA_TELEGRAM_COPILOT_BOT_TOKEN`
- `PACIFICA_TELEGRAM_COPILOT_PROJECT_KEY`
- `PACIFICA_TELEGRAM_COPILOT_CONFIG_DIR`
- `PACIFICA_TELEGRAM_COPILOT_STATE_FILE`
- `PACIFICA_TELEGRAM_COPILOT_ALLOWED_CHAT_IDS`
- `PACIFICA_TELEGRAM_COPILOT_API_BASE`
- `PACIFICA_TELEGRAM_COPILOT_POLL_TIMEOUT_SEC`
- `PACIFICA_TELEGRAM_COPILOT_WATCH_POLL_MS`

## Telegram UX

Supported entry points:

- `/start`
- `/help`
- `/lists`
- `/watch`
- `/add`
- `/unwatch`
- `/remove`
- `/watches`
- `/wallet`
- `/alerts`
- `/status`
- `/label`

Menu actions:

- `➕ Add wallet`
- `🔢 Wallet status`
- `📂 Wallets`
- `🚨 Alerts`
- `ℹ️ Help`

Behavior:

- `/watch <wallet>` starts tracking a wallet directly.
- `/watch` or `➕ Add wallet` opens the default wallet lists.
- The default lists are built from Pacifica Flow data:
  - top 50 wallets by number of trades
  - top 50 wallets by positive PnL
- Tapping a wallet from either list adds it to tracking automatically and then opens the alert setup prompt.
- `/unwatch <wallet>` stops tracking a wallet.
- `/label <wallet> <name>` updates the wallet label.
- `/wallet` shows a picker of saved wallets.
- `/wallet <wallet>` shows live open positions and portfolio stats.
- `/alerts` opens the alert editor for tracked wallets.
- The bot sends alerts when a tracked wallet opens, closes, increases, or reduces a position after the baseline snapshot.

## Production

A production unit is available at:

- `systemd/pacifica-telegram-copilot-bot.service`

It is copied and enabled by `scripts/systemd/install.sh`.
