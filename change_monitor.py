import hashlib
import os
import random
import json
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

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


def supabase_request(method, path, config, params=None, json=None, extra_headers=None):
    url = f"{config['supabase_url']}/rest/v1/{path.lstrip('/')}"
    response = requests.request(
        method,
        url,
        headers=supabase_headers(config, extra_headers),
        params=params,
        json=json,
        timeout=config["request_timeout_seconds"],
    )
    if response.status_code >= 400:
        raise RuntimeError(f"Supabase {method} {path} failed: {response.status_code} {response.text[:300]}")
    if not response.text:
        return None
    return response.json()


def require_inserted_row(rows, table_name, source_id):
    if rows and isinstance(rows, list) and rows[0].get("id"):
        return rows[0]
    raise RuntimeError(f"Supabase insert returned no id: {table_name} | source={source_id} | rows={rows}")


def get_domain(url):
    try:
        return urlparse(url).netloc.lower().removeprefix("www.")
    except Exception:
        return ""


def is_due(source):
    now = utc_now()
    due, _reason = get_due_decision(source, now)
    return due


def get_due_decision(source, now=None):
    now = now or utc_now()
    next_check = parse_dt(source.get("next_check_at"))
    backoff_until = parse_dt(source.get("backoff_until"))

    if backoff_until and backoff_until > now:
        return False, "backoff_active"
    if not next_check:
        return True, "no_next_check"
    if next_check <= now:
        return True, "due"
    return False, "next_check_future"


def source_queue_key(source):
    source_id = str(source.get("id") or "").strip()
    if source_id:
        return f"id:{source_id}"
    url = str(source.get("url") or "").strip().lower().rstrip("/")
    if url:
        return f"url:{url}"
    return f"unknown:{id(source)}"


def source_url_key(source):
    return str(source.get("url") or "").strip().lower().rstrip("/")


def unique_due_sources(due_sources, limit):
    unique = []
    seen_ids = set()
    seen_urls = set()
    duplicate_counts = {"id": 0, "url": 0}
    duplicate_samples = []

    for source in due_sources:
        queue_key = source_queue_key(source)
        url_key = source_url_key(source)
        duplicate_reason = None

        if queue_key in seen_ids:
            duplicate_reason = "id"
        elif url_key and url_key in seen_urls:
            duplicate_reason = "url"

        if duplicate_reason:
            duplicate_counts[duplicate_reason] += 1
            if len(duplicate_samples) < 5:
                duplicate_samples.append(
                    f"{source.get('name') or source.get('url')}:{duplicate_reason}:{source.get('id') or source.get('url')}"
                )
            continue

        seen_ids.add(queue_key)
        if url_key:
            seen_urls.add(url_key)
        unique.append(source)
        if len(unique) >= limit:
            break

    duplicate_counts = {key: value for key, value in duplicate_counts.items() if value}
    if duplicate_counts:
        print(
            f"Queue dedupe: removed={sum(duplicate_counts.values())} | reasons={duplicate_counts}",
            flush=True,
        )
        if duplicate_samples:
            print(f"Queue duplicate samples: {' | '.join(duplicate_samples)}", flush=True)
    return unique


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
    now = utc_now()
    due = []
    skip_counts = {}
    skip_samples = []
    for row in rows:
        is_row_due, reason = get_due_decision(row, now)
        if is_row_due:
            due.append(row)
        else:
            skip_counts[reason] = skip_counts.get(reason, 0) + 1
            if len(skip_samples) < 5:
                skip_samples.append(
                    f"{row.get('name') or row.get('url')}:{reason}:next={row.get('next_check_at')}:backoff={row.get('backoff_until')}"
                )
    unique_due = unique_due_sources(due, config["due_batch_limit"])
    print(
        f"Due scan: active_loaded={len(rows)} | due={len(due)} | queued={len(unique_due)} | skipped={skip_counts or {}}",
        flush=True,
    )
    if skip_samples and not unique_due:
        print(f"Due skip samples: {' | '.join(skip_samples)}", flush=True)
    return unique_due


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
        extra_headers={"Prefer": "return=representation"},
    )
    snapshot = require_inserted_row(rows, "change_snapshots", source_id)
    print(f"Snapshot inserted: {source_id} | snapshot={snapshot.get('id')}", flush=True)
    return snapshot


def get_latest_snapshot(source_id, config):
    rows = supabase_request(
        "GET",
        "change_snapshots",
        config,
        params={
            "select": "id,content_hash,content_text,captured_at",
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
        extra_headers={"Prefer": "return=representation"},
    )
    return require_inserted_row(rows, "change_events", source_id)


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
        extra_headers={"Prefer": "return=representation"},
    )
    return require_inserted_row(rows, "change_alerts", source_id)


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
            # Keep Telegram's native link preview enabled so the URL appears as a
            # normal clickable card with title/image when the target page provides it.
            "disable_web_page_preview": False,
        },
        timeout=config["request_timeout_seconds"],
    )
    if response.status_code == 200:
        return True, None
    return False, f"Telegram {response.status_code}: {response.text[:300]}"


def normalize_text(text, max_chars):
    lines = []
    for line in (text or "").splitlines():
        cleaned = " ".join(line.split()).strip()
        if cleaned:
            lines.append(cleaned)
    normalized = "\n".join(lines)
    if not normalized:
        normalized = " ".join((text or "").split()).strip()
    if max_chars and len(normalized) > max_chars:
        normalized = normalized[:max_chars]
    return normalized


TRACKING_QUERY_PARAMS = {
    "fbclid",
    "gclid",
    "mc_cid",
    "mc_eid",
    "utm_campaign",
    "utm_content",
    "utm_medium",
    "utm_source",
    "utm_term",
}


DATE_TIME_PATTERNS = [
    re.compile(r"\b\d{1,2}[./-]\d{1,2}[./-]\d{2,4}(?:\s+[01]?\d|2[0-3])[:.]\d{2}\b"),
    re.compile(r"\b(?:[01]?\d|2[0-3])[:.]\d{2}\s+\d{1,2}\s+[A-Za-zƏəĞğİıÖöŞşÜüÇç]+\s+\d{4}\b", re.IGNORECASE),
    re.compile(r"\b\d{1,2}\s+[A-Za-zƏəĞğİıÖöŞşÜüÇç]+\s+\d{4}(?:\s+(?:[01]?\d|2[0-3])[:.]\d{2})?\b", re.IGNORECASE),
    re.compile(r"\b(?:[01]?\d|2[0-3])[:.]\d{2}\b"),
]
CATEGORY_PREFIXES = {
    "xəbərlər", "xeberler", "son xəbərlər", "son xeberler", "elanlar", "elan",
    "vakansiyalar", "vakansiya", "tenderlər", "tenderler", "tender", "report",
    "gündəm", "gundem", "cəmiyyət", "cemiyyet", "siyasət", "siyaset",
    "iqtisadiyyat", "idman", "dünya", "dunya", "media", "təhsil", "tehsil",
}


def extract_published_text(text):
    compact = " ".join((text or "").split())
    for pattern in DATE_TIME_PATTERNS:
        match = pattern.search(compact)
        if match:
            return match.group(0).replace(".", ":") if re.fullmatch(r"(?:[01]?\d|2[0-3])\.\d{2}", match.group(0)) else match.group(0)
    return ""


def clean_item_title(raw_title, published_text=""):
    title = " ".join((raw_title or "").split()).strip()
    if not title:
        return ""

    if published_text:
        title = title.replace(published_text, " ")

    for pattern in DATE_TIME_PATTERNS:
        title = pattern.sub(" ", title)

    title = re.sub(r"\s+[-–—|•·]+\s+", " ", title)
    title = re.sub(r"^[\s:;,.\-–—|•·]+", "", title)
    title = re.sub(r"[\s:;,.\-–—|•·]+$", "", title)

    words = title.split()
    for size in range(min(3, len(words)), 0, -1):
        prefix = " ".join(words[:size]).casefold()
        if prefix in CATEGORY_PREFIXES:
            title = " ".join(words[size:]).strip()
            break
    for _ in range(3):
        parts = re.split(r"\s*[-–—|•·:]+\s*", title, maxsplit=1)
        if len(parts) != 2:
            break
        prefix = parts[0].strip().casefold()
        if prefix in CATEGORY_PREFIXES or (len(parts[0].strip()) <= 24 and parts[0].strip().isupper()):
            title = parts[1].strip()
            continue
        break

    lines = [line.strip() for line in re.split(r"[\n\r]+", title) if line.strip()]
    if len(lines) > 1:
        non_category = [line for line in lines if line.casefold() not in CATEGORY_PREFIXES]
        if non_category:
            title = max(non_category, key=len)

    return normalize_text(title, 300)

def normalize_item_url(href, base_url):
    raw = str(href or "").strip()
    if not raw:
        return ""
    absolute = urljoin(base_url or "", raw)
    parsed = urlparse(absolute)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return ""

    query_items = [
        (key, value)
        for key, value in parse_qsl(parsed.query, keep_blank_values=True)
        if key.lower() not in TRACKING_QUERY_PARAMS
    ]
    query = urlencode(query_items, doseq=True)
    path = parsed.path or "/"
    if path != "/" and path.endswith("/"):
        path = path.rstrip("/")
    return urlunparse((parsed.scheme, parsed.netloc.lower(), path, parsed.params, query, ""))


def extract_link_items(elements, base_url):
    items = []
    seen = set()
    for element in elements:
        anchors = [element] if getattr(element, "name", None) == "a" and element.get("href") else element.select("a[href]")
        for anchor in anchors:
            absolute_url = normalize_item_url(anchor.get("href"), base_url)
            if not absolute_url or absolute_url in seen:
                continue

            container = anchor.find_parent(["article", "li"]) or anchor.parent or element
            container_text = normalize_text(container.get_text(" ", strip=True), 800) if container else ""
            raw_title = normalize_text(anchor.get_text(" ", strip=True), 500)
            published_text = extract_published_text(container_text) or extract_published_text(raw_title)
            title = clean_item_title(raw_title, published_text)
            if not title:
                title = clean_item_title(container_text, published_text)
            if not title:
                title = absolute_url

            seen.add(absolute_url)
            items.append({
                "title": title,
                "url": absolute_url,
                "published": published_text,
                "published_text": published_text,
                "image": "",
            })
    return items

def serialize_link_items(link_items):
    normalized_items = []
    seen = set()
    for item in link_items or []:
        url = normalize_item_url(item.get("url"), "")
        if not url or url in seen:
            continue
        published = item.get("published") or item.get("published_text") or ""
        title = clean_item_title(item.get("title"), published) or url
        normalized_items.append({
            "title": title,
            "url": url,
            "published": published,
            "image": item.get("image") or "",
        })
        seen.add(url)

    return json.dumps(
        {"items": normalized_items},
        ensure_ascii=False,
        separators=(",", ":"),
    )

def parse_link_items_from_snapshot(text):
    items = []
    seen = set()
    raw_text = (text or "").strip()

    if not raw_text:
        return items

    if raw_text.startswith("{"):
        try:
            payload = json.loads(raw_text)
            raw_items = payload.get("items") if isinstance(payload, dict) else []
            if isinstance(raw_items, list):
                for raw_item in raw_items:
                    if not isinstance(raw_item, dict):
                        continue
                    published_text = raw_item.get("published") or raw_item.get("published_text") or ""
                    url = normalize_item_url(raw_item.get("url"), "")
                    if not url or url in seen:
                        continue
                    seen.add(url)
                    items.append({
                        "title": clean_item_title(raw_item.get("title"), published_text) or url,
                        "url": url,
                        "published": published_text,
                        "published_text": published_text,
                        "image": raw_item.get("image") or "",
                    })
                return items
        except (TypeError, ValueError, json.JSONDecodeError):
            pass

    # Backward compatibility: old snapshots were stored as:
    # title || url || published_text
    for raw_line in raw_text.splitlines():
        if " || " not in raw_line:
            continue
        parts = raw_line.split(" || ")
        if len(parts) < 2:
            continue
        title = parts[0].strip()
        url = parts[1].strip()
        published_text = parts[2].strip() if len(parts) > 2 else ""
        normalized_url = normalize_item_url(url, "")
        if not normalized_url or normalized_url in seen:
            continue
        seen.add(normalized_url)
        items.append({
            "title": clean_item_title(title, published_text) or normalized_url,
            "url": normalized_url,
            "published": published_text,
            "published_text": published_text,
            "image": "",
        })
    return items

def find_new_link_items(previous_text, current_text):
    previous_items = parse_link_items_from_snapshot(previous_text)
    current_items = parse_link_items_from_snapshot(current_text)
    previous_urls = {item["url"] for item in previous_items}
    new_items = [item for item in current_items if item["url"] not in previous_urls]
    return previous_items, current_items, new_items


def build_selected_content(html, selector, base_url, max_chars):
    if not selector or not selector.strip():
        raise ValueError("selector_missing")
    soup = BeautifulSoup(html or "", "lxml")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    elements = soup.select(selector)
    if not elements:
        raise ValueError("selector_missing")

    link_items = extract_link_items(elements, base_url)
    if link_items:
        # Structured snapshot: URL is now the primary comparison unit.
        # Do not trim this JSON by character count, because truncation would break parsing.
        content_text = serialize_link_items(link_items)
    else:
        raw_text = "\n".join(element.get_text(" ", strip=True) for element in elements)
        content_text = normalize_text(raw_text, max_chars)
        if max_chars and len(content_text) > max_chars:
            content_text = content_text[:max_chars]

    return content_text, link_items

def extract_selected_content(html, selector):
    content_text, _link_items = build_selected_content(html, selector, "", 0)
    return content_text


def calculate_hash(content_text):
    return hashlib.sha256((content_text or "").encode("utf-8")).hexdigest()


def hash_diagnostic_state(source, old_hash, new_hash):
    if not old_hash:
        return "baseline"
    if new_hash == old_hash:
        return "unchanged"

    last_changed_at = parse_dt(source.get("last_changed_at"))
    interval_minutes = int(source.get("interval_minutes") or 5)
    if last_changed_at and utc_now() - last_changed_at < timedelta(minutes=max(10, interval_minutes * 2)):
        return "possible_noisy_page"
    return "changed"


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


def fetch_source(source, config, force_full=False):
    url = str(source.get("url") or "").strip()
    if not url.startswith(("http://", "https://")):
        raise ValueError("invalid_url")

    headers = {
        "User-Agent": config["user_agent"],
        "Accept": "text/html,application/xhtml+xml",
    }
    if not force_full and source.get("etag"):
        headers["If-None-Match"] = source["etag"]
    if not force_full and source.get("last_modified"):
        headers["If-Modified-Since"] = source["last_modified"]

    print(f"Fetch started: {source.get('name') or url} | {url}", flush=True)
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
    print(
        f"Fetch completed: {source.get('name') or url} | status={status} | time={response_time_ms}ms",
        flush=True,
    )
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


def clean_change_lines(text):
    seen = set()
    lines = []
    for raw_line in (text or "").splitlines():
        line = " ".join(raw_line.split()).strip()
        if not line:
            continue
        key = line.casefold()
        if key in seen:
            continue
        seen.add(key)
        lines.append(line)
    if lines:
        return lines

    fallback = " ".join((text or "").split()).strip()
    return [fallback] if fallback else []


def compare_change_lines(old_text, new_text):
    old_lines = clean_change_lines(old_text)
    new_lines = clean_change_lines(new_text)
    old_set = {line.casefold() for line in old_lines}
    new_set = {line.casefold() for line in new_lines}
    added = [line for line in new_lines if line.casefold() not in old_set]
    removed = [line for line in old_lines if line.casefold() not in new_set]
    return added, removed


def truncate_item(text, max_chars=180):
    text = " ".join((text or "").split()).strip()
    if len(text) <= max_chars:
        return text
    return text[: max_chars - 1].rstrip() + "…"


def format_change_section(title, items, limit=10):
    if not items:
        return ""
    shown = [f"• {truncate_item(item)}" for item in items[:limit]]
    extra = len(items) - limit
    if extra > 0:
        shown.append(f"... və {extra} əlavə")
    return f"{title}:\n" + "\n".join(shown)


def build_diff_summary(old_text, new_text, noisy=False, max_chars=1200):
    added, removed = compare_change_lines(old_text, new_text)
    sections = [
        format_change_section("Yeni əlavə olunanlar", added),
        format_change_section("Silinənlər", removed),
    ]
    summary = "\n\n".join(section for section in sections if section).strip()
    if not summary:
        summary = "Mətn dəyişib, amma əlavə/silinən sətirlər ayrıca müəyyənləşdirilə bilmədi."
    if noisy:
        summary = f"{summary}\n\nQeyd: Səhifə tez-tez dəyişə bilər."
    return summary[:max_chars]


def build_link_diff_summary(new_items, limit=10):
    lines = ["Yeni linklər:"]
    for item in new_items[:limit]:
        title = clean_item_title(item.get("title"), item.get("published_text")) or item.get("url")
        published = item.get("published_text") or ""
        suffix = f" — {published}" if published else ""
        lines.append(f"- {truncate_item(title, 220)} — {item.get('url')}{suffix}")
    extra = len(new_items) - limit
    if extra > 0:
        lines.append(f"... və {extra} əlavə")
    return "\n".join(lines)


def format_baku_time(value=None):
    dt = value or utc_now()
    return dt.astimezone(timezone(timedelta(hours=4))).strftime("%d.%m.%Y %H:%M")


def build_telegram_message(name, url, diff_summary):
    summary = clean_item_title(diff_summary, "") or "Seçilmiş hissədə dəyişiklik var"
    return f"""
Yeni paylaşım

Başlıq:
{truncate_item(summary, 260)}

Mənbə:
{name or '-'}

Tarix və saat:
{format_baku_time()}

Link:
{url or '-'}
""".strip()


def build_link_telegram_message(name, source_url, new_items, limit=10):
    item = new_items[0] if new_items else {}
    title = clean_item_title(item.get("title"), item.get("published_text")) or item.get("url") or "Yeni paylaşım"
    item_url = item.get("url") or source_url or ""
    published_text = item.get("published_text") or item.get("published") or format_baku_time()
    extra = max(0, len(new_items) - 1)

    message = f"""
Yeni paylaşım

Başlıq:
{truncate_item(title, 260)}

Mənbə:
{name or '-'}

Tarix və saat:
{published_text or '-'}

Link:
{item_url or '-'}
""".strip()

    if extra > 0:
        message += f"\n\nDaha {extra} yeni paylaşım tapıldı."

    return message


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
            latest_snapshot = get_latest_snapshot(source_id, config)
            if source.get("content_hash") and not latest_snapshot:
                print(f"Snapshot missing after 304: {name} | retrying full fetch for recovery baseline", flush=True)
                uncached_source = {**source, "etag": None, "last_modified": None}
                fetch_result = fetch_source(uncached_source, config, force_full=True)
            else:
                payload = success_source_payload(source, source.get("content_hash"), fetch_result, config)
                update_source(source_id, payload, config)
                print(f"No change (304): {name}", flush=True)
                return

        content_text, link_items = build_selected_content(
            fetch_result["html"],
            source.get("selector"),
            source.get("url"),
            config["snapshot_text_max_chars"],
        )
        if not content_text:
            raise ValueError("empty_content")
        new_hash = calculate_hash(content_text)
        old_hash = source.get("content_hash")
        hash_state = hash_diagnostic_state(source, old_hash, new_hash)
        print(
            f"Selector extracted: {name} | selector={source.get('selector')} | chars={len(content_text)} | link_items_found={len(link_items)} | hash_state={hash_state} | hash={new_hash[:12]}",
            flush=True,
        )

        if not old_hash:
            if config["dry_run"]:
                print(f"[DRY_RUN] baseline would be created: {name} | hash_state=baseline | hash={new_hash[:12]}", flush=True)
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
            latest_snapshot = get_latest_snapshot(source_id, config)
            if not latest_snapshot:
                if config["dry_run"]:
                    print(f"[DRY_RUN] recovery baseline would be created: {name} | hash_state=unchanged | hash={new_hash[:12]}", flush=True)
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
                print(f"Recovery baseline created: {name} | snapshot={snapshot.get('id') if snapshot else None}", flush=True)
                return

            if config["dry_run"]:
                print(f"[DRY_RUN] no-change source update would be written: {name} | hash_state=unchanged", flush=True)
            payload = success_source_payload(source, new_hash, fetch_result, config)
            update_source(source_id, payload, config)
            print(f"No change: {name}", flush=True)
            return

        if config["dry_run"]:
            print(
                f"[DRY_RUN] change would be recorded: {name} | hash_state={hash_state} | old={str(old_hash)[:12]} | new={new_hash[:12]}",
                flush=True,
            )
        latest_snapshot = get_latest_snapshot(source_id, config)
        new_snapshot = insert_snapshot(
            source_id,
            content_text,
            new_hash,
            fetch_result["status"],
            fetch_result["response_time_ms"],
            config,
        )
        previous_text = latest_snapshot.get("content_text") if latest_snapshot else ""
        previous_link_items, current_link_items, new_link_items = find_new_link_items(previous_text or "", content_text)
        fallback_text_diff = not new_link_items
        print(
            f"Link diff: {name} | previous_link_count={len(previous_link_items)} | current_link_count={len(current_link_items)} | new_link_count={len(new_link_items)} | fallback_text_diff={fallback_text_diff}",
            flush=True,
        )
        if new_link_items:
            diff_summary = build_link_diff_summary(new_link_items)
        elif current_link_items:
            # Structured snapshots are JSON internally; never send raw JSON as a user-facing diff.
            diff_summary = build_link_diff_summary(current_link_items[:1])
        else:
            diff_summary = build_diff_summary(previous_text or "", content_text, noisy=hash_state == "possible_noisy_page")
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

        telegram_items = new_link_items or (current_link_items[:1] if current_link_items else [])
        message = (
            build_link_telegram_message(name, source.get("url"), telegram_items)
            if telegram_items
            else build_telegram_message(name, source.get("url"), diff_summary)
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
