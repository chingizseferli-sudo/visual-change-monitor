# Visual Change Monitor

Separate MVP bot for monitoring changes on manually added URLs and CSS selectors.

This project is intentionally separate from the existing Visual Monitor news bot. It uses separate Supabase tables:

- `change_sources`
- `change_snapshots`
- `change_events`
- `change_alerts`

## Purpose

The bot checks active `change_sources`, extracts the configured CSS selector, normalizes the selected text, calculates a SHA-256 hash, and detects changes. When content changes, it writes a snapshot, change event, alert record, and sends a Telegram notification.

## Setup

Install dependencies:

```bash
pip install -r requirements.txt
```

Copy environment file:

```bash
cp .env.example .env
```

Fill:

- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY`
- `CHANGE_BOT_TOKEN`
- `DEFAULT_TELEGRAM_CHAT_ID`

## Run

```bash
python -u change_monitor.py
```

## Railway

Start command:

```bash
python -u change_monitor.py
```

Use a separate Railway service/project from the news monitoring bot.

## DRY_RUN

Default in `.env.example`:

```env
DRY_RUN=true
```

When `DRY_RUN=true`, the bot:

- fetches sources
- extracts selector content
- calculates hash
- logs what would happen
- does not write snapshots/events/alerts
- does not update sources
- does not send Telegram messages

Set `DRY_RUN=false` only after table schema and test sources are ready.

## Schema Note

This bot assumes the change monitoring tables already exist in Supabase. It does not create tables or run migrations.

## MVP Limitations

- CSS selector is required.
- No admin UI yet.
- No snapshot retention yet.
- No advanced ignore rules yet.
- Domain limiter is in-memory per bot instance.
