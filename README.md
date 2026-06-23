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