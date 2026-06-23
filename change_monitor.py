import hashlib
import os
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from difflib import unified_diff
from email.utils import format_datetime
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv


UTC = timezone.utc


def utc_now():
    return datetime.now(UTC)


def iso_now():
    return utc_now().isoformat()


def parse_dt(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def as_bool(value):
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def as_int(name, default):
    try:
        return int(os.getenv(name, default))
    except (TypeError, ValueError):
        return default


def get_env():
    load_dotenv()
    config = {
        "supabase_url": os.getenv("SUPABASE_URL", "").strip().rstrip("/"),
        "service_key": os.getenv("SUPABASE_SERVICE_ROLE_KEY", "").strip(),
        "telegram_token": os.getenv("CHANGE_BOT_TOKEN", "").strip(),
        "default_chat_id": os.getenv("DEFAULT_TELEGRAM_CHAT_ID", "").strip(),
        "worker_count": as_int("WORKER_COUNT", 5),
        "loop_sleep_seconds": as_int("LOOP_SLEEP_SECONDS", 15),
        "due_batch_limit": as_int("DUE_BATCH_LIMIT", 50),
        "min_interval_minutes": as_int("MIN_INTERVAL_MINUTES", 5),
        "request_timeout_seconds": as_int("REQUEST_TIMEOUT_SECONDS", 20),
        "domain_min_delay_seconds": as_int("DOMAIN_MIN_DELAY_SECONDS", 10),
        "jitter_fast_min": as_int("JITTER_SECONDS_MIN_FAST", 0),
        "jitter_fast_max": as_int("JITTER_SECONDS_MAX_FAST", 20),
        "jitter_normal_min": as_int("JITTER_SECONDS_MIN_NORMAL", 10),
        "jitter_normal_max": as_int("JITTER_SECONDS_MAX_NORMAL", 45),
        "max_backoff_minutes": as_int("MAX_BACKOFF_MINUTES", 120),
        "user_agent": os.getenv("USER_AGENT", "VisualMonitorChangeBot/1.0").strip(),
        "snapshot_text_max_chars": as_int("SNAPSHOT_TEXT_MAX_CHARS", 50000),
        "dry_run": as_bool(os.getenv("DRY_RUN", "true")),
    }

    missing = []
    if not config["supabase_url"]:
        missing.append("SUPABASE_URL")
    if not config["service_key"]:
        missing.append("SUPABASE_SERVICE_ROLE_KEY")
    if missing:
        raise RuntimeError(f"Missing required env vars: {', '.join(missing)}")

    config["worker_count"] = max(1, config["worker_count"])
    config["due_batch_limit"] = max(1, config["due_batch_limit"])
    config["min_interval_minutes"] = max(5, config["min_interval_minutes"])
    return config


def supabase_headers(config, extra=None):
    headers = {
        "apikey": config["service_key"],
        "Authorization": f"Bearer {config['service_key']}",
        "Content-Type": "application/json",
    }
    if extra:
        headers.update(extra)
    return headers


def supabase_request(method, path, config, params=None, json=None):
    url = f"{config['supabase_url']}/rest/v1/{path.lstrip('/')}"
    response = requests.request(
        method,
        url,
        headers=supabase_headers(config),
        params=params,
        json=json,
        timeout=config["request_timeout_seconds"],
    )
    if response.status_code >= 400:
        raise RuntimeError(f"Supabase {method} {path} failed: {response.status_code} {response.text[:300]}")
    if not response.text:
        return None
    return response.json()


def get_domain(url):
    try:
        return urlparse(url).netloc.lower().removeprefix("www.")
    except Exception:
        return ""


def is_due(source):
    now = utc_now()
    next_check = parse_dt(source.get("next_check_at"))
    backoff_until = parse_dt(source.get("backoff_until"))

    if backoff_until and backoff_until > now:
        return False
    if not next_check:
        return True
    return next_check <= now


def get_due_sources(config):
    rows = supabase_request(
        "GET",
        "change_sources",
        config,
        params={
            "select": "*",
            "status": "eq.active",
            "order": "next_check_at.asc.nullsfirst,last_checked_at.asc.nullsfirst",
            "limit": str(config["due_batch_limit"] * 3),
        },
    ) or []
    due = [row for row in rows if is_due(row)]
    return due[: config["due_batch_limit"]]


def update_source(source_id, payload, config):
    if config["dry_run"]:
        print(f"[DRY_RUN] source update skipped: {source_id} | {payload}", flush=True)
        return None
    payload["updated_at"] = iso_now()
    return supabase_request(
        "PATCH",
        "change_sources",
        config,
        params={"id": f"eq.{source_id}"},
        json=payload,
    )


def insert_snapshot(source_id, content_text, content_hash, response_status, response_time_ms, config):
    payload = {
        "source_id": source_id,
        "content_text": content_text,
        "content_hash": content_hash,
        "captured_at": iso_now(),
        "response_status": response_status,
        "response_time_ms": response_time_ms,
    }
    if config["dry_run"]:
        print(f"[DRY_RUN] snapshot insert skipped: {source_id} | hash={content_hash}", flush=True)
        return {"id": None, **payload}
    rows = supabase_request(
        "POST",
        "change_snapshots",
        config,
        params={"select": "id"},
        json=payload,
    )
    return rows[0] if rows else None


def get_latest_snapshot(source_id, config):
    rows = supabase_request(
        "GET",
        "change_snapshots",
        config,
        params={
            "select": "id,content_hash,captured_at",
            "source_id": f"eq.{source_id}",
            "order": "captured_at.desc",
            "limit": "1",
        },
    ) or []
    return rows[0] if rows else None


def insert_change_event(source_id, old_snapshot_id, new_snapshot_id, old_hash, new_hash, diff_summary, config):
    payload = {
        "source_id": source_id,
        "old_snapshot_id": old_snapshot_id,
        "new_snapshot_id": new_snapshot_id,
        "old_hash": old_hash,
        "new_hash": new_hash,
        "diff_summary": diff_summary,
        "created_at": iso_now(),
    }
    if config["dry_run"]:
        print(f"[DRY_RUN] change event insert skipped: {source_id} | {diff_summary[:120]}", flush=True)
        return {"id": None, **payload}
    rows = supabase_request(
        "POST",
        "change_events",
        config,
        params={"select": "id,created_at"},
        json=payload,
    )
    return rows[0] if rows else None


def insert_alert(event_id, source_id, telegram_chat_id, config):
    payload = {
        "event_id": event_id,
        "source_id": source_id,
        "telegram_chat_id": telegram_chat_id,
        "status": "pending",
        "created_at": iso_now(),
    }
    if config["dry_run"]:
        print(f"[DRY_RUN] alert insert skipped: {source_id} | chat={telegram_chat_id}", flush=True)
        return {"id": None, **payload}
    rows = supabase_request(
        "POST",
        "change_alerts",
        config,
        params={"select": "id"},
        json=payload,
    )
    return rows[0] if rows else None


def update_alert(alert_id, payload, config):
    if config["dry_run"] or not alert_id:
        print(f"[DRY_RUN] alert update skipped: {alert_id} | {payload}", flush=True)
        return None
    return supabase_request(
        "PATCH",
        "change_alerts",
        config,
        params={"id": f"eq.{alert_id}"},
        json=payload,
    )


def send_telegram(config, chat_id, text):
    if config["dry_run"]:
        print(f"[DRY_RUN] Telegram skipped: chat={chat_id} | {text[:160]}", flush=True)
        return True, None
    if not config["telegram_token"]:
        return False, "CHANGE_BOT_TOKEN is missing"
    if not chat_id:
        return False, "telegram_chat_id is missing"

    response = requests.post(
        f"https://api.telegram.org/bot{config['telegram_token']}/sendMessage",
        json={
            "chat_id": chat_id,
            "text": text,
            "disable_web_page_preview": True,
        },
        timeout=config["request_timeout_seconds"],
    )
    if response.status_code == 200:
        return True, None
    return False, f"Telegram {response.status_code}: {response.text[:300]}"


def normalize_text(text, max_chars):
    normalized = " ".join((text or "").split()).strip()
    if max_chars and len(normalized) > max_chars:
        normalized = normalized[:max_chars]
    return normalized


def extract_selected_content(html, selector):
    if not selector or not selector.strip():
        raise ValueError("selector_missing")
    soup = BeautifulSoup(html or "", "lxml")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    elements = soup.select(selector)
    if not elements:
        raise ValueError("selector_missing")
    return "\n".join(element.get_text(" ", strip=True) for element in elements)


def calculate_hash(content_text):
    return hashlib.sha256((content_text or "").encode("utf-8")).hexdigest()


def calculate_next_check_at(interval_minutes, config):
    interval = max(config["min_interval_minutes"], int(interval_minutes or config["min_interval_minutes"]))
    if interval <= 5:
        jitter = random.randint(config["jitter_fast_min"], config["jitter_fast_max"])
    else:
        jitter = random.randint(config["jitter_normal_min"], config["jitter_normal_max"])
    return (utc_now() + timedelta(minutes=interval, seconds=jitter)).isoformat()


def calculate_backoff(reason, fail_count, config):
    base_minutes = {
        "timeout": 10,
        "http_403": 45,
        "http_429": 60,
        "server_error": 15,
        "selector_missing": 15,
        "network_error": 10,
        "unsupported_content_type": 15,
        "empty_content": 15,
    }.get(reason, 10)
    multiplier = 2 ** min(max(fail_count - 1, 0), 4)
    minutes = min(config["max_backoff_minutes"], base_minutes * multiplier)
    return (utc_now() + timedelta(minutes=minutes)).isoformat()


class DomainLimiter:
    def __init__(self, min_delay_seconds):
        self.min_delay_seconds = max(0, min_delay_seconds)
        self.last_request_at = {}
        self.lock = threading.Lock()

    def wait(self, domain):
        if not domain or self.min_delay_seconds <= 0:
            return
        with self.lock:
            now = time.monotonic()
            previous = self.last_request_at.get(domain)
            wait_for = 0
            if previous is not None:
                wait_for = max(0, self.min_delay_seconds - (now - previous))
            self.last_request_at[domain] = now + wait_for
        if wait_for > 0:
            print(f"Domain limiter: {domain} | wait={wait_for:.1f}s", flush=True)
            time.sleep(wait_for)


def fetch_source(source, config):
    url = str(source.get("url") or "").strip()
    if not url.startswith(("http://", "https://")):
        raise ValueError("invalid_url")

    headers = {
        "User-Agent": config["user_agent"],
        "Accept": "text/html,application/xhtml+xml",
    }
    if source.get("etag"):
        headers["If-None-Match"] = source["etag"]
    if source.get("last_modified"):
        headers["If-Modified-Since"] = source["last_modified"]

    started = time.monotonic()
    try:
        response = requests.get(
            url,
            headers=headers,
            timeout=config["request_timeout_seconds"],
            allow_redirects=True,
        )
    except requests.Timeout as exc:
        raise TimeoutError("timeout") from exc
    except requests.RequestException as exc:
        raise ConnectionError("network_error") from exc

    response_time_ms = int((time.monotonic() - started) * 1000)
    status = response.status_code
    if status == 304:
        return {
            "status": 304,
            "html": "",
            "etag": response.headers.get("ETag") or source.get("etag"),
            "last_modified": response.headers.get("Last-Modified") or source.get("last_modified"),
            "response_time_ms": response_time_ms,
        }
    if status == 403:
        raise PermissionError("http_403")
    if status == 429:
        raise PermissionError("http_429")
    if 500 <= status <= 599:
        raise RuntimeError("server_error")
    if status >= 400:
        raise RuntimeError(f"http_{status}")

    content_type = response.headers.get("Content-Type", "").lower()
    if content_type and "html" not in content_type and "text" not in content_type:
        raise ValueError("unsupported_content_type")

    return {
        "status": status,
        "html": response.text,
        "etag": response.headers.get("ETag"),
        "last_modified": response.headers.get("Last-Modified"),
        "response_time_ms": response_time_ms,
    }


def build_diff_summary(old_text, new_text, max_chars=1000):
    old_lines = (old_text or "").split()
    new_lines = (new_text or "").split()
    diff = unified_diff(old_lines, new_lines, lineterm="", n=2)
    interesting = [line for line in diff if line.startswith(("+", "-")) and not line.startswith(("+++", "---"))]
    summary = " ".join(interesting[:30]).strip()
    if not summary:
        summary = new_text[:max_chars]
    return summary[:max_chars]


def success_source_payload(source, content_hash, fetch_result, config, changed=False):
    payload = {
        "last_checked_at": iso_now(),
        "last_success_at": iso_now(),
        "next_check_at": calculate_next_check_at(source.get("interval_minutes"), config),
        "last_error": None,
        "consecutive_fail_count": 0,
        "backoff_until": None,
        "etag": fetch_result.get("etag"),
        "last_modified": fetch_result.get("last_modified"),
    }
    if content_hash:
        payload["content_hash"] = content_hash
    if changed:
        payload["last_changed_at"] = iso_now()
    return payload


def failure_source_payload(source, reason, config):
    fail_count = int(source.get("consecutive_fail_count") or 0) + 1
    backoff_until = calculate_backoff(reason, fail_count, config)
    return {
        "last_checked_at": iso_now(),
        "last_error": reason,
        "consecutive_fail_count": fail_count,
        "backoff_until": backoff_until,
        "next_check_at": backoff_until,
    }


def check_source(source, config, domain_limiter):
    source_id = source.get("id")
    name = source.get("name") or source.get("url") or source_id
    domain = source.get("domain") or get_domain(source.get("url"))
    print(f"Checking: {name} | {source.get('url')}", flush=True)

    try:
        domain_limiter.wait(domain)
        fetch_result = fetch_source(source, config)

        if fetch_result["status"] == 304:
            payload = success_source_payload(source, source.get("content_hash"), fetch_result, config)
            update_source(source_id, payload, config)
            print(f"No change (304): {name}", flush=True)
            return

        raw_content = extract_selected_content(fetch_result["html"], source.get("selector"))
        content_text = normalize_text(raw_content, config["snapshot_text_max_chars"])
        if not content_text:
            raise ValueError("empty_content")
        new_hash = calculate_hash(content_text)
        old_hash = source.get("content_hash")

        if not old_hash:
            snapshot = insert_snapshot(
                source_id,
                content_text,
                new_hash,
                fetch_result["status"],
                fetch_result["response_time_ms"],
                config,
            )
            payload = success_source_payload(source, new_hash, fetch_result, config)
            update_source(source_id, payload, config)
            print(f"Baseline created: {name} | snapshot={snapshot.get('id') if snapshot else None}", flush=True)
            return

        if new_hash == old_hash:
            payload = success_source_payload(source, new_hash, fetch_result, config)
            update_source(source_id, payload, config)
            print(f"No change: {name}", flush=True)
            return

        latest_snapshot = get_latest_snapshot(source_id, config)
        new_snapshot = insert_snapshot(
            source_id,
            content_text,
            new_hash,
            fetch_result["status"],
            fetch_result["response_time_ms"],
            config,
        )
        diff_summary = build_diff_summary(old_hash or "", content_text)
        event = insert_change_event(
            source_id,
            latest_snapshot.get("id") if latest_snapshot else None,
            new_snapshot.get("id") if new_snapshot else None,
            old_hash,
            new_hash,
            diff_summary,
            config,
        )
        chat_id = source.get("telegram_chat_id") or config["default_chat_id"]
        alert = insert_alert(event.get("id") if event else None, source_id, chat_id, config)

        message = (
            "🔔 Dəyişiklik aşkarlandı\n\n"
            f"Mənbə: {name}\n"
            f"URL: {source.get('url')}\n\n"
            "Dəyişən hissə:\n"
            f"{diff_summary}\n\n"
            f"Vaxt: {format_datetime(utc_now())}"
        )
        ok, error = send_telegram(config, chat_id, message)
        if ok:
            update_alert(alert.get("id") if alert else None, {"status": "sent", "sent_at": iso_now(), "error": None}, config)
        else:
            update_alert(alert.get("id") if alert else None, {"status": "failed", "error": error}, config)

        payload = success_source_payload(source, new_hash, fetch_result, config, changed=True)
        update_source(source_id, payload, config)
        print(f"Change detected: {name}", flush=True)

    except Exception as exc:
        reason = str(exc) or exc.__class__.__name__
        if isinstance(exc, TimeoutError):
            reason = "timeout"
        elif isinstance(exc, PermissionError):
            reason = str(exc)
        elif isinstance(exc, ConnectionError):
            reason = "network_error"
        payload = failure_source_payload(source, reason, config)
        update_source(source_id, payload, config)
        print(f"Check failed: {name} | {reason}", flush=True)


def run_loop():
    config = get_env()
    domain_limiter = DomainLimiter(config["domain_min_delay_seconds"])
    print(
        f"Visual Change Monitor started | dry_run={config['dry_run']} | workers={config['worker_count']}",
        flush=True,
    )

    while True:
        try:
            sources = get_due_sources(config)
            if sources:
                print(f"Due sources: {len(sources)}", flush=True)
                with ThreadPoolExecutor(max_workers=config["worker_count"]) as executor:
                    futures = [executor.submit(check_source, source, config, domain_limiter) for source in sources]
                    for future in as_completed(futures):
                        try:
                            future.result()
                        except Exception as exc:
                            print(f"Worker error: {exc}", flush=True)
            else:
                print("No due sources.", flush=True)
        except Exception as exc:
            print(f"Loop error: {exc}", flush=True)

        time.sleep(config["loop_sleep_seconds"])


if __name__ == "__main__":
    run_loop()
