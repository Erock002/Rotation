#!/usr/bin/env python3
"""
Rotation — Data-Centralized Music Telemetry, Analytics & Plexamp Playlist Engine
Powered by Tautulli + Plex Media Server
"""

import http.server
import socketserver
import urllib.request
import urllib.parse
import urllib.error
import sqlite3
import json
import os
import time
import datetime
import calendar
import hashlib
import hmac
import secrets
import http.cookies
import random
import threading
from concurrent.futures import ThreadPoolExecutor
import xml.etree.ElementTree as ET
import gzip
from http import HTTPStatus

os.environ.setdefault("TZ", "America/Chicago")
if hasattr(time, "tzset"):
    time.tzset()

PORT = int(os.environ.get("PORT", 8888))
_ENV_PLEX_URL = os.environ.get("PLEX_URL", "http://127.0.0.1:32400")
_ENV_PLEX_TOKEN = os.environ.get("PLEX_TOKEN", "")
_ENV_MACHINE_ID = os.environ.get("MACHINE_ID", "")
_ENV_MUSIC_SECTION_ID = os.environ.get("MUSIC_SECTION_ID", "1")
_ENV_SERVER_NAME = os.environ.get("SERVER_NAME", "Plex Server")
_ENV_NTFY_URL = os.environ.get("NTFY_URL", "")
_ENV_PUBLIC_URL = os.environ.get("PUBLIC_URL", "")
_ENV_APP_PASSWORD = os.environ.get("APP_PASSWORD", "")
_ENV_USER_HANDLE = os.environ.get("USER_HANDLE", "@you • Plexamp")
_ENV_TAUTULLI_DB = os.environ.get("TAUTULLI_DB", "")

PLEX_URL = _ENV_PLEX_URL
PLEX_TOKEN = _ENV_PLEX_TOKEN
MACHINE_ID = _ENV_MACHINE_ID
MUSIC_SECTION_ID = _ENV_MUSIC_SECTION_ID
SERVER_NAME = _ENV_SERVER_NAME
# Optional: monthly recap push notification (e.g. https://ntfy.sh/your-topic)
NTFY_URL = _ENV_NTFY_URL
PUBLIC_URL = _ENV_PUBLIC_URL
USER_HANDLE = _ENV_USER_HANDLE

DATA_DIR = os.path.dirname(os.path.abspath(__file__))
CACHE_DIR = os.environ.get("CACHE_DIR") or os.path.join(DATA_DIR, "cache")
os.makedirs(CACHE_DIR, exist_ok=True)
SYNC_STATE_FILE = os.path.join(CACHE_DIR, "playlist_sync_state.json")
CONFIG_FILE = os.path.join(CACHE_DIR, "config.json")
SESSION_SECRET_FILE = os.path.join(CACHE_DIR, ".session_secret")
UPLOADED_DB_FILE = os.path.join(CACHE_DIR, "tautulli_uploaded.db")
REMOTE_DB_FILE = os.path.join(CACHE_DIR, "tautulli_remote.db")
MAX_DB_UPLOAD_BYTES = 512 * 1024 * 1024

_LIB_STATS_CACHE = {"ts": 0, "data": {"total_tracks": 0, "total_artists": 0, "total_albums": 0}}
_WRAPPED_CACHE = {}
_DISCOVER_CACHE = {"ts": 0, "data": None}
_SYNC_LOCK = threading.Lock()
_CONFIG_LOCK = threading.Lock()
_SESSION_SECRET_KEY = None


def _hash_password(password, salt_hex=None):
    salt = bytes.fromhex(salt_hex) if salt_hex else secrets.token_bytes(16)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 100_000)
    return f"pbkdf2$100000${salt.hex()}${dk.hex()}"


def _verify_password(password, stored):
    if not stored or not isinstance(password, str):
        return False
    if stored.startswith("pbkdf2$"):
        parts = stored.split("$")
        if len(parts) != 4:
            return False
        _, iters_s, salt_hex, expected_hex = parts
        try:
            dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), bytes.fromhex(salt_hex), int(iters_s))
            return hmac.compare_digest(dk.hex(), expected_hex)
        except Exception:
            return False
    return hmac.compare_digest(password, stored)


def _get_session_secret():
    global _SESSION_SECRET_KEY
    if _SESSION_SECRET_KEY is not None:
        return _SESSION_SECRET_KEY
    with _CONFIG_LOCK:
        if _SESSION_SECRET_KEY is not None:
            return _SESSION_SECRET_KEY
        if os.path.exists(SESSION_SECRET_FILE):
            with open(SESSION_SECRET_FILE, "rb") as f:
                data = f.read().strip()
            if len(data) >= 32:
                _SESSION_SECRET_KEY = data
                return _SESSION_SECRET_KEY
        key = secrets.token_hex(32).encode("ascii")
        fd = os.open(SESSION_SECRET_FILE, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "wb") as f:
            f.write(key)
        _SESSION_SECRET_KEY = key
        return _SESSION_SECRET_KEY


def create_session_token(ttl=30 * 86400):
    exp = int(time.time()) + ttl
    nonce = secrets.token_hex(8)
    payload = f"{exp}:{nonce}"
    sig = hmac.new(_get_session_secret(), payload.encode("ascii"), hashlib.sha256).hexdigest()
    return f"{payload}:{sig}"


def verify_session_token(token):
    if not token or not isinstance(token, str):
        return False
    parts = token.split(":")
    if len(parts) != 3:
        return False
    exp_s, nonce, sig = parts
    try:
        if int(exp_s) < int(time.time()):
            return False
    except ValueError:
        return False
    payload = f"{exp_s}:{nonce}"
    expected = hmac.new(_get_session_secret(), payload.encode("ascii"), hashlib.sha256).hexdigest()
    return hmac.compare_digest(sig, expected)


def load_runtime_config():
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    return data
        except Exception:
            pass
    return {}


def save_runtime_config(cfg):
    with _CONFIG_LOCK:
        tmp = CONFIG_FILE + ".tmp"
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(cfg, f, indent=2)
        os.replace(tmp, CONFIG_FILE)


def get_active_password_spec():
    cfg = load_runtime_config()
    if cfg.get("password_hash"):
        return cfg["password_hash"]
    if cfg.get("password_cleared"):
        return ""
    return _ENV_APP_PASSWORD


def clear_all_caches():
    _WRAPPED_CACHE.clear()
    _DISCOVER_CACHE["ts"] = 0
    _DISCOVER_CACHE["data"] = None
    _LIB_STATS_CACHE["ts"] = 0
    _LIB_STATS_CACHE["data"] = {"total_tracks": 0, "total_artists": 0, "total_albums": 0}
    if "_FIRST_PLAY_CACHE" in globals():
        _FIRST_PLAY_CACHE.update({"ts": 0, "tracks": {}, "artists": {}})
    if "_EXTRAS_CACHE" in globals():
        _EXTRAS_CACHE.clear()


def apply_runtime_config():
    global PLEX_URL, PLEX_TOKEN, MACHINE_ID, MUSIC_SECTION_ID, SERVER_NAME, NTFY_URL, PUBLIC_URL, USER_HANDLE
    cfg = load_runtime_config()
    PLEX_URL = (cfg.get("plex_url") or _ENV_PLEX_URL).rstrip("/")
    PLEX_TOKEN = cfg.get("plex_token") if cfg.get("plex_token") else _ENV_PLEX_TOKEN
    MACHINE_ID = cfg.get("machine_id") if cfg.get("machine_id") else _ENV_MACHINE_ID
    MUSIC_SECTION_ID = str(cfg.get("music_section_id") or _ENV_MUSIC_SECTION_ID or "1")
    SERVER_NAME = cfg.get("server_name") or _ENV_SERVER_NAME or "Plex Server"
    NTFY_URL = cfg.get("ntfy_url") if "ntfy_url" in cfg else _ENV_NTFY_URL
    PUBLIC_URL = cfg.get("public_url") if "public_url" in cfg else _ENV_PUBLIC_URL
    USER_HANDLE = cfg.get("handle") or _ENV_USER_HANDLE or "@you • Plexamp"


def validate_tautulli_sqlite(path):
    if not path or not os.path.isfile(path):
        raise ValueError("Database file does not exist")
    size = os.path.getsize(path)
    with open(path, "rb") as f:
        header = f.read(64)
    sample = header.lstrip().decode("utf-8", errors="ignore").lower()
    if sample.startswith("<html") or sample.startswith("<!doc"):
        raise ValueError("Link returned a webpage (HTML) instead of a tautulli.db SQLite file — use a direct file download URL")
    if size < 100:
        raise ValueError("File is too small to be a valid tautulli.db SQLite database")
    if not header.startswith(b"SQLite format 3\x00"):
        raise ValueError("Not a valid SQLite database (missing SQLite format 3 header)")
    uri_path = urllib.request.pathname2url(os.path.abspath(path))
    conn = None
    try:
        try:
            conn = sqlite3.connect(f"file:{uri_path}?mode=ro", uri=True, timeout=5)
            conn.execute("SELECT 1 FROM sqlite_master LIMIT 1").fetchone()
        except sqlite3.OperationalError:
            if conn:
                conn.close()
            conn = sqlite3.connect(f"file:{uri_path}?mode=ro&immutable=1", uri=True, timeout=5)
        tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        required = {"session_history", "session_history_metadata"}
        missing = required - tables
        if missing:
            raise ValueError(f"SQLite file is missing Tautulli tables: {', '.join(sorted(missing))}")
        track_plays = conn.execute("SELECT COUNT(*) FROM session_history WHERE media_type='track'").fetchone()[0]
        return {"size_mb": round(size / (1024 * 1024), 2), "track_plays": int(track_plays)}
    finally:
        if conn:
            conn.close()


def get_db_candidates():
    cfg = load_runtime_config()
    return [
        cfg.get("db_path", ""),
        UPLOADED_DB_FILE,
        REMOTE_DB_FILE,
        _ENV_TAUTULLI_DB,
        "/tautulli/tautulli.db",
    ]


def get_db_path():
    for p in get_db_candidates():
        if p and os.path.isfile(p):
            return p
    raise FileNotFoundError("Could not locate tautulli.db — open Setup to upload, link, or mount your database")


def test_plex_connection(plex_url, plex_token):
    base = (plex_url or "").strip().rstrip("/")
    token = (plex_token or "").strip()
    if not base:
        raise ValueError("Enter your Plex Server URL first (e.g. http://192.168.1.50:32400)")
    parsed = urllib.parse.urlparse(base)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        raise ValueError("Plex URL must start with http:// or https://")
    if not token:
        raise ValueError("Enter your X-Plex-Token first")

    sep = "&" if "?" in base else "?"
    root_url = f"{base}{sep}X-Plex-Token={urllib.parse.quote(token, safe='')}"
    req = urllib.request.Request(root_url, headers={"Accept": "application/xml"})
    try:
        with urllib.request.urlopen(req, timeout=8) as resp:
            root = ET.fromstring(resp.read())
    except urllib.error.HTTPError as e:
        if e.code == 401:
            raise ValueError("Plex rejected this X-Plex-Token (HTTP 401 Unauthorized)")
        raise ValueError(f"Plex returned HTTP {e.code}")
    except Exception as e:
        raise ValueError(f"Could not reach Plex at {base}: {e}")

    machine_id = root.attrib.get("machineIdentifier", "")
    server_name = root.attrib.get("friendlyName", "Plex Server")

    sections = []
    try:
        sec_url = f"{base}/library/sections?X-Plex-Token={urllib.parse.quote(token, safe='')}"
        sec_root = ET.fromstring(urllib.request.urlopen(urllib.request.Request(sec_url, headers={"Accept": "application/xml"}), timeout=8).read())
        for d in sec_root.findall("Directory"):
            sections.append({
                "id": d.attrib.get("key", ""),
                "title": d.attrib.get("title", ""),
                "type": d.attrib.get("type", ""),
            })
    except Exception:
        pass

    music_sections = [s for s in sections if s["type"] == "artist"]
    return {
        "success": True,
        "server_name": server_name,
        "machine_id": machine_id,
        "music_sections": music_sections,
        "recommended_section_id": music_sections[0]["id"] if music_sections else "1",
    }


def fetch_db_from_url(url):
    clean_url = (url or "").strip()
    parsed = urllib.parse.urlparse(clean_url)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        raise ValueError("Enter a valid http:// or https:// direct download link to your tautulli.db")

    req = urllib.request.Request(clean_url, headers={"User-Agent": "Rotation-Setup/1.0"})
    tmp_path = REMOTE_DB_FILE + ".tmp"
    total = 0
    try:
        with urllib.request.urlopen(req, timeout=45) as resp:
            with open(tmp_path, "wb") as out:
                while True:
                    chunk = resp.read(65536)
                    if not chunk:
                        break
                    total += len(chunk)
                    if total > MAX_DB_UPLOAD_BYTES:
                        raise ValueError("Remote database exceeds 512 MB limit")
                    out.write(chunk)
        info = validate_tautulli_sqlite(tmp_path)
        os.replace(tmp_path, REMOTE_DB_FILE)
    except Exception:
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except OSError:
                pass
        raise

    cfg = load_runtime_config()
    cfg["db_path"] = REMOTE_DB_FILE
    cfg["db_url"] = clean_url
    save_runtime_config(cfg)
    clear_all_caches()
    return {"success": True, "db_path": REMOTE_DB_FILE, **info}


def get_setup_status():
    cfg = load_runtime_config()
    db_path = None
    db_plays = 0
    db_size_mb = 0
    db_error = ""
    try:
        cand = get_db_path()
        info = validate_tautulli_sqlite(cand)
        db_path = cand
        db_plays = info["track_plays"]
        db_size_mb = info["size_mb"]
    except Exception as e:
        db_error = str(e)

    if db_path == UPLOADED_DB_FILE:
        db_source = "uploaded"
    elif db_path == REMOTE_DB_FILE:
        db_source = "link"
    elif db_path:
        db_source = "local"
    else:
        db_source = "none"

    return {
        "db_configured": bool(db_path),
        "db_source": db_source,
        "db_path": db_path or "",
        "db_url": cfg.get("db_url", ""),
        "db_track_plays": db_plays,
        "db_size_mb": db_size_mb,
        "db_error": db_error,
        "plex_url": PLEX_URL,
        "has_plex_token": bool(PLEX_TOKEN),
        "machine_id": MACHINE_ID,
        "music_section_id": MUSIC_SECTION_ID,
        "server_name": SERVER_NAME,
        "handle": USER_HANDLE,
        "ntfy_url": NTFY_URL,
        "public_url": PUBLIC_URL,
    }


apply_runtime_config()


def load_sync_state():
    default_state = {
        "playlists": {
            "weekly_best": {"auto_update": True, "last_synced": 0, "playlist_rk": None, "track_count": 0},
            "weekly_throwbacks": {"auto_update": True, "last_synced": 0, "playlist_rk": None, "track_count": 0},
            "on_repeat_now": {"auto_update": False, "last_synced": 0, "playlist_rk": None, "track_count": 0},
            "late_night": {"auto_update": False, "last_synced": 0, "playlist_rk": None, "track_count": 0},
            "all_time_100": {"auto_update": False, "last_synced": 0, "playlist_rk": None, "track_count": 0},
            "morning_boost": {"auto_update": False, "last_synced": 0, "playlist_rk": None, "track_count": 0},
            "forgotten_gems": {"auto_update": False, "last_synced": 0, "playlist_rk": None, "track_count": 0},
            "on_this_day": {"auto_update": False, "last_synced": 0, "playlist_rk": None, "track_count": 0},
        },
        "recap_last_sent": "",
    }
    if os.path.exists(SYNC_STATE_FILE):
        try:
            with open(SYNC_STATE_FILE, "r") as f:
                saved = json.load(f)
            for k, v in default_state["playlists"].items():
                if k in saved.get("playlists", {}):
                    v.update(saved["playlists"][k])
            default_state["recap_last_sent"] = saved.get("recap_last_sent", "")
            return default_state
        except Exception:
            pass
    return default_state


def save_sync_state(state):
    try:
        with open(SYNC_STATE_FILE, "w") as f:
            json.dump(state, f, indent=2)
    except Exception:
        pass


def get_plex_library_totals():
    now = time.time()
    if now - _LIB_STATS_CACHE["ts"] < 600:
        return _LIB_STATS_CACHE["data"]
    totals = dict(_LIB_STATS_CACHE["data"])
    try:
        for t_id, key in [(8, "total_artists"), (9, "total_albums"), (10, "total_tracks")]:
            url = f"{PLEX_URL}/library/sections/{MUSIC_SECTION_ID}/all?type={t_id}&X-Plex-Container-Start=0&X-Plex-Container-Size=0&X-Plex-Token={PLEX_TOKEN}"
            req = urllib.request.urlopen(url, timeout=4)
            root = ET.fromstring(req.read())
            val = int(root.attrib.get("totalSize", totals[key]))
            totals[key] = val
        _LIB_STATS_CACHE["data"] = totals
        _LIB_STATS_CACHE["ts"] = now
    except Exception:
        pass
    return totals


def get_existing_plex_playlists():
    result = {}
    try:
        url = f"{PLEX_URL}/playlists?playlistType=audio&X-Plex-Token={PLEX_TOKEN}"
        root = ET.fromstring(urllib.request.urlopen(url, timeout=5).read())
        for pl in root.findall("Playlist"):
            title = (pl.attrib.get("title") or "").strip()
            result[title] = {
                "playlist_rk": pl.attrib.get("ratingKey"),
                "track_count": int(pl.attrib.get("leafCount", "0")),
                "smart": pl.attrib.get("smart") == "1",
            }
    except Exception:
        pass
    return result


LEGACY_PREFIX = ("Rotation:", "SonicReplay:")  # playlists were named "… SonicReplay: …" before the rename


def legacy_title(title):
    return title.replace(*LEGACY_PREFIX) if LEGACY_PREFIX[0] in title else None


def upsert_plex_playlist(title, rating_keys, album_rk=None):
    valid_rks = []
    if album_rk:
        url = f"{PLEX_URL}/library/metadata/{album_rk}/children?X-Plex-Token={PLEX_TOKEN}"
        root = ET.fromstring(urllib.request.urlopen(url, timeout=5).read())
        for tr in root.findall("Track"):
            rk = tr.attrib.get("ratingKey")
            if rk:
                valid_rks.append(str(rk))
    else:
        for rk in rating_keys:
            if rk:
                valid_rks.append(str(rk))

    if not valid_rks:
        raise ValueError("No valid track rating keys provided")

    seen = set()
    dedup_rks = []
    for k in valid_rks:
        if k not in seen:
            seen.add(k)
            dedup_rks.append(k)

    rks_csv = ",".join(dedup_rks[:200])
    uri = f"server://{MACHINE_ID}/com.plexapp.plugins.library/library/metadata/{rks_csv}"

    existing = get_existing_plex_playlists()
    old = legacy_title(title)
    if title not in existing and old and old in existing and not existing[old]["smart"]:
        # Rename the old "SonicReplay: …" playlist in place instead of creating a duplicate
        params = urllib.parse.urlencode({"title": title, "X-Plex-Token": PLEX_TOKEN})
        req = urllib.request.Request(f"{PLEX_URL}/playlists/{existing[old]['playlist_rk']}?{params}", method="PUT")
        urllib.request.urlopen(req, timeout=8).read()
        existing[title] = existing.pop(old)
    if title in existing and not existing[title]["smart"]:
        pl_rk = existing[title]["playlist_rk"]
        del_req = urllib.request.Request(f"{PLEX_URL}/playlists/{pl_rk}/items?X-Plex-Token={PLEX_TOKEN}", method="DELETE")
        urllib.request.urlopen(del_req, timeout=8)
        put_params = urllib.parse.urlencode({"uri": uri, "X-Plex-Token": PLEX_TOKEN})
        put_req = urllib.request.Request(f"{PLEX_URL}/playlists/{pl_rk}/items?{put_params}", method="PUT")
        resp = urllib.request.urlopen(put_req, timeout=8).read()
        root = ET.fromstring(resp)
        pl = root.find("Playlist")
        leaf_cnt = int(pl.attrib.get("leafCount", len(dedup_rks))) if pl is not None else len(dedup_rks)
        return {
            "success": True,
            "action": "updated",
            "playlist_rk": pl_rk,
            "title": title,
            "track_count": leaf_cnt,
        }
    else:
        params = urllib.parse.urlencode({
            "type": "audio",
            "title": title,
            "smart": "0",
            "uri": uri,
            "X-Plex-Token": PLEX_TOKEN,
        })
        req = urllib.request.Request(f"{PLEX_URL}/playlists?{params}", method="POST")
        resp = urllib.request.urlopen(req, timeout=8).read()
        root = ET.fromstring(resp)
        pl = root.find("Playlist")
        if pl is None:
            raise RuntimeError("Plex did not return a Playlist element")
        return {
            "success": True,
            "action": "created",
            "playlist_rk": pl.attrib.get("ratingKey"),
            "title": pl.attrib.get("title"),
            "track_count": int(pl.attrib.get("leafCount", len(dedup_rks))),
        }


def normalize_range(range_key):
    range_key = (range_key or "all").strip().lower()
    if range_key in ("all", "90d", "30d", "7d"):
        return range_key
    if range_key.isdigit() and len(range_key) == 4 and 2000 <= int(range_key) <= datetime.date.today().year + 1:
        return range_key
    return "all"


def get_time_filter(range_key):
    now = int(time.time())
    range_key = normalize_range(range_key)
    if range_key.isdigit():
        yr = int(range_key)
        start = int(datetime.datetime(yr, 1, 1, 0, 0, 0).timestamp())
        end = int(datetime.datetime(yr, 12, 31, 23, 59, 59).timestamp())
        return start, end, str(yr)
    elif range_key == "90d":
        return now - (90 * 86400), now + 3600, "Last 90 Days"
    elif range_key == "30d":
        return now - (30 * 86400), now + 3600, "Last 30 Days"
    elif range_key == "7d":
        return now - (7 * 86400), now + 3600, "Last 7 Days"
    else:
        return 0, now + 3600, "All-Time"


def open_tautulli_db():
    # Try read-only first so live WAL changes are visible on rw/direct mounts;
    # fall back to immutable=1 when /tautulli is bind-mounted :ro in Docker
    # (SQLite defers the failure past connect(), so probe sqlite_master here).
    db_path = get_db_path()
    for uri in (
        f"file:{db_path}?mode=ro",
        f"file:{db_path}?mode=ro&immutable=1",
    ):
        conn = None
        try:
            conn = sqlite3.connect(uri, uri=True, timeout=10)
            conn.execute("SELECT 1 FROM sqlite_master LIMIT 1").fetchone()
            conn.row_factory = sqlite3.Row
            return conn
        except Exception:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass
    # Never fall back to a read-write open: that could take locks on Tautulli's live database.
    raise sqlite3.OperationalError(f"Could not open {db_path} read-only")


PLAYS_SQL = """
    WITH plays AS (
        SELECT
            h.reference_id,
            MAX(h.id) as latest_id,
            MIN(h.started) as started,
            MAX(h.stopped) as stopped,
            SUM(MAX(0, h.stopped - h.started - COALESCE(h.paused_counter, 0))) as raw_sec,
            MAX(h.player) as player,
            MAX(h.product) as product,
            MAX(h.platform) as platform
        FROM session_history h
        WHERE h.media_type = 'track' AND h.started >= ? AND h.started <= ?
        GROUP BY h.reference_id
    )
    SELECT
        p.reference_id, p.started, p.stopped, p.raw_sec, p.player, p.product, p.platform,
        m.rating_key, m.parent_rating_key, m.grandparent_rating_key,
        m.title, m.parent_title, m.grandparent_title,
        m.thumb, m.parent_thumb, m.grandparent_thumb,
        m.year, m.genres, m.duration
    FROM plays p
    JOIN session_history_metadata m ON p.latest_id = m.id
    ORDER BY p.started ASC
"""


def listen_seconds(raw_sec, duration_ms):
    dur_sec = int(duration_ms or 0) / 1000.0 if duration_ms and int(duration_ms) > 0 else 210.0
    raw_sec = float(raw_sec or 0)
    max_reasonable = max(dur_sec * 2.5, 1200.0)
    listen = min(raw_sec, max_reasonable) if raw_sec > 0 else min(dur_sec, 180.0)
    return listen, dur_sec


_FIRST_PLAY_CACHE = {"ts": 0, "tracks": {}, "artists": {}}


def get_first_plays(conn):
    """All-time first play timestamp for every track and artist (so 'new' means new-ever, not new-in-range)."""
    if time.time() - _FIRST_PLAY_CACHE["ts"] < 120:
        return _FIRST_PLAY_CACHE["tracks"], _FIRST_PLAY_CACHE["artists"]
    tracks, artists = {}, {}
    rows = conn.execute(
        "SELECT m.title AS t, m.grandparent_title AS a, MIN(h.started) AS first_ts "
        "FROM session_history h JOIN session_history_metadata m ON m.id = h.id "
        "WHERE h.media_type = 'track' GROUP BY m.title, m.grandparent_title"
    ).fetchall()
    for r in rows:
        # normalise in Python so keys match compute_wrapped exactly (SQLite lower() is ASCII-only)
        a_key = (r["a"] or "Unknown Artist").strip().lower()
        t_key = f"{(r['t'] or 'Unknown Track').strip().lower()}|||{a_key}"
        ts = r["first_ts"] or 0
        if t_key not in tracks or ts < tracks[t_key]:
            tracks[t_key] = ts
        if a_key not in artists or ts < artists[a_key]:
            artists[a_key] = ts
    _FIRST_PLAY_CACHE.update({"ts": time.time(), "tracks": tracks, "artists": artists})
    return tracks, artists


def previous_window(range_key):
    """Matching earlier window to compare against (same length, same point in the year for the current year)."""
    now = datetime.datetime.now()
    if range_key in ("7d", "30d", "90d"):
        days = int(range_key[:-1])
        end = int(time.time()) - days * 86400
        return end - days * 86400, end, f"previous {days} days"
    if range_key.isdigit():
        yr = int(range_key)
        start = int(datetime.datetime(yr - 1, 1, 1).timestamp())
        if yr == now.year:
            try:
                same_day = now.replace(year=yr - 1)
            except ValueError:
                same_day = now.replace(year=yr - 1, day=28)
            return start, int(same_day.timestamp()), f"same point in {yr - 1}"
        return start, int(datetime.datetime(yr - 1, 12, 31, 23, 59, 59).timestamp()), str(yr - 1)
    return None


def window_totals(conn, start_ts, end_ts):
    total_sec, plays, tracks, artists = 0.0, 0, set(), set()
    for r in conn.execute(PLAYS_SQL, (start_ts, end_ts)):
        listen, _ = listen_seconds(r["raw_sec"], r["duration"])
        total_sec += listen
        plays += 1
        a = (r["grandparent_title"] or "Unknown Artist").strip().lower()
        tracks.add(f"{(r['title'] or 'Unknown Track').strip().lower()}|||{a}")
        artists.add(a)
    return {"plays": plays, "minutes": round(total_sec / 60.0, 1), "unique_tracks": len(tracks), "unique_artists": len(artists)}


def pct_change(cur, prev):
    if not prev:
        return None
    return round((cur - prev) / prev * 100, 1)


def get_available_years():
    try:
        conn = open_tautulli_db()
        rows = conn.execute(
            "SELECT DISTINCT strftime('%Y', started, 'unixepoch', 'localtime') AS y "
            "FROM session_history WHERE media_type = 'track' AND started > 0 ORDER BY y DESC"
        ).fetchall()
        conn.close()
        return [r["y"] for r in rows if r["y"]]
    except Exception:
        return []


def compute_persona(hourly, top_artists, total_plays, unique_tracks):
    morning = sum(hourly[h]["plays"] for h in range(5, 12))
    afternoon = sum(hourly[h]["plays"] for h in range(12, 18))
    evening = sum(hourly[h]["plays"] for h in range(18, 23))
    late_night = sum(hourly[h]["plays"] for h in [23, 0, 1, 2, 3, 4])

    slots = [
        ("Morning Listener (5 AM – 12 PM)", morning, "Most listening happens between 5 AM and 12 PM."),
        ("Afternoon Listener (12 PM – 6 PM)", afternoon, "Most listening happens between 12 PM and 6 PM."),
        ("Evening Listener (6 PM – 11 PM)", evening, "Most listening happens between 6 PM and 11 PM."),
        ("Late Night Listener (11 PM – 5 AM)", late_night, "Most listening happens between 11 PM and 5 AM.")
    ]
    slots.sort(key=lambda x: x[1], reverse=True)
    time_title, _, time_desc = slots[0]

    replay_ratio = total_plays / max(1, unique_tracks)
    top3_share = sum(a["plays"] for a in top_artists[:3]) / max(1, total_plays) * 100 if top_artists else 0
    top10_share = sum(a["plays"] for a in top_artists[:10]) / max(1, total_plays) * 100 if top_artists else 0

    if replay_ratio >= 5.0 or top3_share >= 25:
        style_title = "Heavy Repeat Listener"
        style_trait = f"Averaging {replay_ratio:.1f} plays per song, with your top 3 artists making up {top3_share:.1f}% of all plays."
    elif unique_tracks >= 2000:
        style_title = "Wide Library Explorer"
        style_trait = f"Played {unique_tracks:,} different songs with {replay_ratio:.1f} plays per song on average."
    else:
        style_title = "Favorites & New Mix"
        style_trait = f"Averaging {replay_ratio:.1f} plays per song across {unique_tracks:,} different songs."

    return {
        "title": f"{style_title} · {time_title}",
        "headline": style_title,
        "subheadline": time_title,
        "description": f"{time_desc} {style_trait}",
        "replay_ratio": round(replay_ratio, 2),
        "top3_artist_share": round(top3_share, 1),
        "top10_artist_share": round(top10_share, 1),
        "dayparts": {
            "morning_pct": round(morning / max(1, total_plays) * 100, 1),
            "afternoon_pct": round(afternoon / max(1, total_plays) * 100, 1),
            "evening_pct": round(evening / max(1, total_plays) * 100, 1),
            "late_night_pct": round(late_night / max(1, total_plays) * 100, 1),
        }
    }


def compute_wrapped(range_key="all", force=False):
    # immutable=1 reads can occasionally catch Tautulli mid-write ("database disk image is
    # malformed" / odd rows). A short retry gets a clean read almost every time.
    for attempt in range(3):
        try:
            return _compute_wrapped(range_key, force)
        except sqlite3.DatabaseError:
            if attempt == 2:
                raise
            time.sleep(0.5)


def _compute_wrapped(range_key="all", force=False):
    now_ts = int(time.time())
    range_key = normalize_range(range_key)
    cached = _WRAPPED_CACHE.get(range_key)
    if not force and cached and (now_ts - cached["ts"] < 15):
        return cached["data"]

    conn = open_tautulli_db()
    c = conn.cursor()

    start_ts, end_ts, range_label = get_time_filter(range_key)

    first_track_ts, first_artist_ts = get_first_plays(conn)
    c.execute(PLAYS_SQL, (start_ts, end_ts))
    rows = c.fetchall()

    tracks_map = {}
    artists_map = {}
    albums_map = {}
    hourly = {h: {"hour": h, "plays": 0, "sec": 0.0} for h in range(24)}
    weekdays = {d: {"day_num": d, "plays": 0, "sec": 0.0} for d in range(7)}
    # 7x24 heatmap: [day 0..6][hour 0..23]
    heatmap_7x24 = [[0 for _ in range(24)] for _ in range(7)]
    heatmap_7x24_mins = [[0.0 for _ in range(24)] for _ in range(7)]

    monthly_map = {}
    daily_map = {}
    daily_track_obsessions = {}
    genres_map = {}
    decades_map = {}
    platforms_map = {}
    seen_tracks_ever = set()
    seen_artists_ever = set()
    discovered_tracks = set()
    discovered_artists = set()

    late_night_tracks = {}
    morning_tracks = {}

    total_plays = 0
    total_sec = 0.0
    completed_plays = 0
    partial_plays = 0
    skipped_plays = 0
    first_play_ts = None
    last_play_ts = None

    for r in rows:
        title = (r["title"] or "Unknown Track").strip()
        artist = (r["grandparent_title"] or "Unknown Artist").strip()
        album = (r["parent_title"] or "Unknown Album").strip()
        started = r["started"] or 0
        listen_sec, dur_sec = listen_seconds(r["raw_sec"], r["duration"])

        # Completion vs Skip telemetry
        ratio = listen_sec / max(1.0, dur_sec)
        is_completed = ratio >= 0.70 or listen_sec >= 150
        is_skipped = ratio < 0.25 and listen_sec < 35
        if is_completed:
            completed_plays += 1
        elif is_skipped:
            skipped_plays += 1
        else:
            partial_plays += 1

        total_plays += 1
        total_sec += listen_sec

        if first_play_ts is None or started < first_play_ts:
            first_play_ts = started
        if last_play_ts is None or started > last_play_ts:
            last_play_ts = started

        dt = datetime.datetime.fromtimestamp(started)
        hr = dt.hour
        wd = (dt.weekday() + 1) % 7  # 0=Sun..6=Sat
        ym = dt.strftime("%Y-%m")
        ymd = dt.strftime("%Y-%m-%d")

        hourly[hr]["plays"] += 1
        hourly[hr]["sec"] += listen_sec
        weekdays[wd]["plays"] += 1
        weekdays[wd]["sec"] += listen_sec
        heatmap_7x24[wd][hr] += 1
        heatmap_7x24_mins[wd][hr] = round(heatmap_7x24_mins[wd][hr] + listen_sec / 60.0, 1)

        t_key = f"{title.lower()}|||{artist.lower()}"
        a_key = artist.lower()
        # "New" = first play EVER happened inside this range (not just first play within the range)
        is_new_track = t_key not in seen_tracks_ever and first_track_ts.get(t_key, started) >= start_ts
        seen_tracks_ever.add(t_key)
        is_new_artist = a_key not in seen_artists_ever and first_artist_ts.get(a_key, started) >= start_ts
        seen_artists_ever.add(a_key)
        if is_new_track:
            discovered_tracks.add(t_key)
        if is_new_artist:
            discovered_artists.add(a_key)

        thumb = r["parent_thumb"] or r["thumb"] or ""
        artist_thumb = r["grandparent_thumb"] or ""

        if ym not in monthly_map:
            monthly_map[ym] = {
                "month": ym,
                "label": dt.strftime("%b '%y"),
                "full_label": dt.strftime("%B %Y"),
                "plays": 0,
                "sec": 0.0,
                "new_tracks": 0,
                "repeat_plays": 0,
                "new_artists": 0,
                "days_set": set(),
                "tracks": {},
                "artists": {},
                "daily_map": {},
                "hourly_grid": {},
            }
        mm = monthly_map[ym]
        mm["plays"] += 1
        mm["sec"] += listen_sec
        mm["days_set"].add(ymd)
        
        day_num = dt.day
        if day_num not in mm["daily_map"]:
            mm["daily_map"][day_num] = {"day": day_num, "date": ymd, "plays": 0, "sec": 0.0}
        mm["daily_map"][day_num]["plays"] += 1
        mm["daily_map"][day_num]["sec"] += listen_sec

        dh_key = (day_num, hr)
        if dh_key not in mm["hourly_grid"]:
            mm["hourly_grid"][dh_key] = {"plays": 0, "sec": 0.0}
        mm["hourly_grid"][dh_key]["plays"] += 1
        mm["hourly_grid"][dh_key]["sec"] += listen_sec
        if is_new_track:
            mm["new_tracks"] += 1
        else:
            mm["repeat_plays"] += 1
        if is_new_artist:
            mm["new_artists"] += 1
        if t_key not in mm["tracks"]:
            mm["tracks"][t_key] = {"title": title, "artist": artist, "plays": 0, "sec": 0.0, "thumb": thumb}
        mm["tracks"][t_key]["plays"] += 1
        mm["tracks"][t_key]["sec"] += listen_sec
        if thumb:
            mm["tracks"][t_key]["thumb"] = thumb
        if a_key not in mm["artists"]:
            mm["artists"][a_key] = {"artist": artist, "plays": 0, "sec": 0.0, "thumb": artist_thumb or thumb}
        mm["artists"][a_key]["plays"] += 1
        mm["artists"][a_key]["sec"] += listen_sec
        if artist_thumb:
            mm["artists"][a_key]["thumb"] = artist_thumb

        if ymd not in daily_map:
            daily_map[ymd] = {"date": ymd, "plays": 0, "sec": 0.0}
        daily_map[ymd]["plays"] += 1
        daily_map[ymd]["sec"] += listen_sec

        obs_key = (ymd, t_key)
        if obs_key not in daily_track_obsessions:
            daily_track_obsessions[obs_key] = {
                "date": ymd,
                "title": title,
                "artist": artist,
                "album": album,
                "plays": 0,
                "sec": 0.0,
                "thumb": r["parent_thumb"] or r["thumb"] or "",
                "rk": r["rating_key"]
            }
        daily_track_obsessions[obs_key]["plays"] += 1
        daily_track_obsessions[obs_key]["sec"] += listen_sec
        daily_track_obsessions[obs_key]["rk"] = r["rating_key"]

        if t_key not in tracks_map:
            tracks_map[t_key] = {
                "key": t_key,
                "rk": r["rating_key"],
                "parent_rk": r["parent_rating_key"],
                "artist_rk": r["grandparent_rating_key"],
                "title": title,
                "artist": artist,
                "album": album,
                "plays": 0,
                "completed": 0,
                "skipped": 0,
                "sec": 0.0,
                "first_played": started,
                "last_played": started,
                "thumb": thumb,
                "artist_thumb": artist_thumb,
                "year": r["year"] or None,
                "genres": r["genres"] or "",
                "duration_sec": round(dur_sec, 1),
            }
        tm = tracks_map[t_key]
        tm["plays"] += 1
        tm["sec"] += listen_sec
        if is_completed:
            tm["completed"] += 1
        if is_skipped:
            tm["skipped"] += 1
        if started >= tm["last_played"]:
            tm["last_played"] = started
            tm["rk"] = r["rating_key"] or tm["rk"]
            tm["parent_rk"] = r["parent_rating_key"] or tm["parent_rk"]
            tm["artist_rk"] = r["grandparent_rating_key"] or tm["artist_rk"]
            if thumb:
                tm["thumb"] = thumb
            if artist_thumb:
                tm["artist_thumb"] = artist_thumb
        if started < tm["first_played"]:
            tm["first_played"] = started

        if hr >= 21 or hr <= 4:
            late_night_tracks[t_key] = late_night_tracks.get(t_key, 0) + 1
        elif 5 <= hr <= 11:
            morning_tracks[t_key] = morning_tracks.get(t_key, 0) + 1

        if a_key not in artists_map:
            artists_map[a_key] = {
                "artist": artist,
                "artist_rk": r["grandparent_rating_key"],
                "plays": 0,
                "completed": 0,
                "skipped": 0,
                "sec": 0.0,
                "tracks_set": set(),
                "albums_set": set(),
                "thumb": artist_thumb or thumb,
                "track_counts": {},
                "first_played": started,
                "last_played": started,
            }
        am = artists_map[a_key]
        am["plays"] += 1
        am["sec"] += listen_sec
        if is_completed:
            am["completed"] += 1
        if is_skipped:
            am["skipped"] += 1
        am["tracks_set"].add(title)
        am["albums_set"].add(album)
        am["track_counts"][title] = am["track_counts"].get(title, 0) + 1
        if started >= am["last_played"]:
            am["last_played"] = started
            if r["grandparent_rating_key"]:
                am["artist_rk"] = r["grandparent_rating_key"]
            if artist_thumb:
                am["thumb"] = artist_thumb

        alb_key = f"{album.lower()}|||{artist.lower()}"
        if alb_key not in albums_map:
            albums_map[alb_key] = {
                "album": album,
                "artist": artist,
                "album_rk": r["parent_rating_key"],
                "plays": 0,
                "sec": 0.0,
                "tracks_set": set(),
                "thumb": thumb,
                "year": r["year"] or None,
                "last_played": started,
            }
        albm = albums_map[alb_key]
        albm["plays"] += 1
        albm["sec"] += listen_sec
        albm["tracks_set"].add(title)
        if started >= albm["last_played"]:
            albm["last_played"] = started
            if r["parent_rating_key"]:
                albm["album_rk"] = r["parent_rating_key"]
            if thumb:
                albm["thumb"] = thumb

        raw_genres = (r["genres"] or "").replace(";", ",").split(",")
        for g in raw_genres:
            g_clean = g.strip()
            if g_clean:
                if g_clean not in genres_map:
                    genres_map[g_clean] = {"genre": g_clean, "plays": 0, "sec": 0.0}
                genres_map[g_clean]["plays"] += 1
                genres_map[g_clean]["sec"] += listen_sec

        yr = r["year"]
        if yr and str(yr).isdigit():
            yr_int = int(yr)
            if 1950 <= yr_int <= 2030:
                dec = f"{(yr_int // 10) * 10}s"
                if dec not in decades_map:
                    decades_map[dec] = {"decade": dec, "plays": 0, "sec": 0.0}
                decades_map[dec]["plays"] += 1
                decades_map[dec]["sec"] += listen_sec

        prod = (r["product"] or "Plexamp").strip()
        plat = (r["platform"] or r["player"] or "Device").strip()
        p_label = f"{prod} ({plat})" if prod.lower() != plat.lower() else prod
        if p_label not in platforms_map:
            platforms_map[p_label] = {"name": p_label, "plays": 0, "sec": 0.0}
        platforms_map[p_label]["plays"] += 1
        platforms_map[p_label]["sec"] += listen_sec

    comparison = None
    prev = previous_window(range_key)
    if prev:
        p_start, p_end, p_label = prev
        prev_tot = window_totals(conn, p_start, p_end)
        comparison = {"label": p_label, "previous": prev_tot}

    conn.close()

    all_tracks = []
    for t in tracks_map.values():
        days_ago = max(0, int((now_ts - t["last_played"]) / 86400))
        comp_pct = round(t["completed"] / max(1, t["plays"]) * 100, 1)
        skip_pct = round(t["skipped"] / max(1, t["plays"]) * 100, 1)
        all_tracks.append({
            "rk": t["rk"],
            "parent_rk": t["parent_rk"],
            "artist_rk": t["artist_rk"],
            "title": t["title"],
            "artist": t["artist"],
            "album": t["album"],
            "plays": t["plays"],
            "completed_pct": comp_pct,
            "skip_pct": skip_pct,
            "minutes": round(t["sec"] / 60.0, 1),
            "hours": round(t["sec"] / 3600.0, 2),
            "first_played": t["first_played"],
            "last_played": t["last_played"],
            "days_since_played": days_ago,
            "thumb": t["thumb"],
            "artist_thumb": t["artist_thumb"],
            "year": t["year"],
            "genre": t["genres"].replace(";", ",").split(",")[0].strip() if t["genres"] else "",
            "duration_sec": t["duration_sec"],
            "is_new": t["key"] in discovered_tracks,
        })
    all_tracks.sort(key=lambda x: (x["plays"], x["minutes"]), reverse=True)
    for idx, item in enumerate(all_tracks):
        item["rank"] = idx + 1

    top_artists_all = []
    for a in artists_map.values():
        top_song_tuple = sorted(a["track_counts"].items(), key=lambda x: x[1], reverse=True)
        top_song = top_song_tuple[0][0] if top_song_tuple else ""
        top_song_plays = top_song_tuple[0][1] if top_song_tuple else 0
        comp_pct = round(a["completed"] / max(1, a["plays"]) * 100, 1)
        top_artists_all.append({
            "artist": a["artist"],
            "artist_rk": a["artist_rk"],
            "plays": a["plays"],
            "minutes": round(a["sec"] / 60.0, 1),
            "hours": round(a["sec"] / 3600.0, 1),
            "unique_tracks": len(a["tracks_set"]),
            "unique_albums": len(a["albums_set"]),
            "avg_plays_per_track": round(a["plays"] / max(1, len(a["tracks_set"])), 1),
            "completion_pct": comp_pct,
            "thumb": a["thumb"],
            "top_track": top_song,
            "top_track_plays": top_song_plays,
            "share_pct": round(a["plays"] / max(1, total_plays) * 100, 2),
        })
    top_artists_all.sort(key=lambda x: (x["plays"], x["minutes"]), reverse=True)
    for idx, item in enumerate(top_artists_all):
        item["rank"] = idx + 1

    top_albums_all = []
    for alb in albums_map.values():
        top_albums_all.append({
            "album": alb["album"],
            "artist": alb["artist"],
            "album_rk": alb["album_rk"],
            "plays": alb["plays"],
            "minutes": round(alb["sec"] / 60.0, 1),
            "hours": round(alb["sec"] / 3600.0, 1),
            "unique_tracks": len(alb["tracks_set"]),
            "share_pct": round(alb["plays"] / max(1, total_plays) * 100, 2),
            "thumb": alb["thumb"],
            "year": alb["year"],
        })
    top_albums_all.sort(key=lambda x: (x["plays"], x["minutes"]), reverse=True)
    for idx, item in enumerate(top_albums_all):
        item["rank"] = idx + 1

    sorted_days = sorted(daily_map.keys())
    longest_streak = 0
    cur_streak = 0
    prev_date = None
    for d_str in sorted_days:
        d_obj = datetime.date.fromisoformat(d_str)
        if prev_date and (d_obj - prev_date).days == 1:
            cur_streak += 1
        else:
            cur_streak = 1
        if cur_streak > longest_streak:
            longest_streak = cur_streak
        prev_date = d_obj

    current_streak = 0
    if daily_map:
        chk = datetime.date.today()
        if chk.isoformat() not in daily_map:
            chk = chk - datetime.timedelta(days=1)
        while chk.isoformat() in daily_map:
            current_streak += 1
            chk = chk - datetime.timedelta(days=1)

    # Map each day to its #1 top played song
    day_top_track = {}
    for (ymd_k, _), obs in daily_track_obsessions.items():
        prev_tt = day_top_track.get(ymd_k)
        if not prev_tt or (obs["plays"], obs.get("sec", 0)) > (prev_tt["plays"], prev_tt.get("sec", 0)):
            day_top_track[ymd_k] = {
                "title": obs["title"],
                "artist": obs["artist"],
                "plays": obs["plays"],
                "sec": obs.get("sec", 0),
                "rk": obs["rk"],
                "thumb": obs["thumb"],
            }

    busiest_day = {"date": "N/A", "plays": 0, "minutes": 0, "top_track": ""}
    if daily_map:
        bd = max(daily_map.values(), key=lambda x: (x["sec"], x["plays"]))
        bd_tt = day_top_track.get(bd["date"])
        busiest_day = {
            "date": bd["date"],
            "plays": bd["plays"],
            "minutes": round(bd["sec"] / 60.0, 1),
            "top_track": f"{bd_tt['title']} ({bd_tt['plays']}×)" if bd_tt else ""
        }

    hourly_list = []
    for h in range(24):
        ampm = "AM" if h < 12 else "PM"
        h12 = 12 if h % 12 == 0 else h % 12
        hourly_list.append({
            "hour": h,
            "label": f"{h12}{ampm}",
            "plays": hourly[h]["plays"],
            "minutes": round(hourly[h]["sec"] / 60.0, 1),
        })

    day_names = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]
    weekday_list = []
    for d in range(7):
        weekday_list.append({
            "day": day_names[d],
            "plays": weekdays[d]["plays"],
            "minutes": round(weekdays[d]["sec"] / 60.0, 1),
        })

    peak_hr_obj = max(hourly_list, key=lambda x: x["plays"]) if hourly_list else {"label": "N/A", "plays": 0}
    peak_day_obj = max(weekday_list, key=lambda x: x["plays"]) if weekday_list else {"day": "N/A", "plays": 0}

    monthly_list = sorted(monthly_map.values(), key=lambda x: x["month"])
    for m in monthly_list:
        m["minutes"] = round(m.pop("sec") / 60.0, 1)
        m["hours"] = round(m["minutes"] / 60.0, 1)
        m["active_days"] = len(m.pop("days_set", set()))
        m_tracks = sorted(m.pop("tracks", {}).values(), key=lambda x: (x["plays"], x["sec"]), reverse=True)[:5]
        for mt in m_tracks:
            mt["minutes"] = round(mt.pop("sec") / 60.0, 1)
        m["top_tracks"] = m_tracks
        m_artists = sorted(m.pop("artists", {}).values(), key=lambda x: (x["plays"], x["sec"]), reverse=True)[:5]
        for ma in m_artists:
            ma["minutes"] = round(ma.pop("sec") / 60.0, 1)
        m["top_artists"] = m_artists

        yr_v, mo_v = [int(x) for x in m["month"].split("-")]
        _, num_days = calendar.monthrange(yr_v, mo_v)
        d_map = m.pop("daily_map", {})
        h_grid = m.pop("hourly_grid", {})

        # 24-hour acoustic matrix: 24 rows (hours 0..23), num_days columns (days 1..num_days)
        matrix_24h = []
        for h in range(24):
            row = []
            for d in range(1, num_days + 1):
                dh = h_grid.get((d, h), {"plays": 0, "sec": 0.0})
                row.append({
                    "day": d,
                    "hour": h,
                    "plays": dh["plays"],
                    "minutes": round(dh["sec"] / 60.0, 1)
                })
            matrix_24h.append(row)
        m["matrix_24h"] = matrix_24h

        # Daily sound spectrum array (1..num_days)
        daily_spectrum = []
        for d in range(1, num_days + 1):
            iso_d = f"{yr_v:04d}-{mo_v:02d}-{d:02d}"
            d_info = d_map.get(d, {"plays": 0, "sec": 0.0})
            tt = day_top_track.get(iso_d)
            daily_spectrum.append({
                "day": d,
                "date": iso_d,
                "plays": d_info["plays"],
                "minutes": round(d_info["sec"] / 60.0, 1),
                "top_track": f"{tt['title']} — {tt['artist']} ({tt['plays']}×)" if tt else "",
                "top_rk": tt["rk"] if tt else "",
                "top_title": tt["title"] if tt else "",
                "top_artist": tt["artist"] if tt else "",
                "top_thumb": tt["thumb"] if tt else "",
            })
        m["daily_spectrum"] = daily_spectrum

    # Last 60 days daily series (filling 0s for quiet days so the daily chart is continuous)
    daily_series_60d = []
    today_date = datetime.date.today()
    for i in range(59, -1, -1):
        d_obj = today_date - datetime.timedelta(days=i)
        d_key = d_obj.isoformat()
        info = daily_map.get(d_key, {"plays": 0, "sec": 0.0})
        tt = day_top_track.get(d_key)
        daily_series_60d.append({
            "date": d_key,
            "label": d_obj.strftime("%b %d"),
            "plays": info["plays"],
            "minutes": round(info["sec"] / 60.0, 1),
            "top_track": f"{tt['title']} — {tt['artist']} ({tt['plays']}×)" if tt else "",
            "top_rk": tt["rk"] if tt else "",
            "top_title": tt["title"] if tt else "",
            "top_artist": tt["artist"] if tt else "",
            "top_thumb": tt["thumb"] if tt else "",
        })

    # Full daily activity map for calendar heatmap
    daily_list = []
    for k, v in sorted(daily_map.items()):
        tt = day_top_track.get(k)
        daily_list.append({
            "date": k,
            "plays": v["plays"],
            "minutes": round(v["sec"] / 60.0, 1),
            "top_track": f"{tt['title']} — {tt['artist']} ({tt['plays']}×)" if tt else "",
            "top_rk": tt["rk"] if tt else "",
            "top_title": tt["title"] if tt else "",
            "top_artist": tt["artist"] if tt else "",
            "top_thumb": tt["thumb"] if tt else "",
        })

    genres_list = sorted(genres_map.values(), key=lambda x: x["plays"], reverse=True)[:15]
    for g in genres_list:
        g["minutes"] = round(g.pop("sec") / 60.0, 1)
        g["pct"] = round(g["plays"] / max(1, total_plays) * 100, 1)

    decades_list = sorted(decades_map.values(), key=lambda x: x["decade"])
    for d in decades_list:
        d["minutes"] = round(d.pop("sec") / 60.0, 1)
        d["pct"] = round(d["plays"] / max(1, total_plays) * 100, 1)

    platforms_list = sorted(platforms_map.values(), key=lambda x: x["plays"], reverse=True)[:6]
    for p in platforms_list:
        p["minutes"] = round(p.pop("sec") / 60.0, 1)
        p["pct"] = round(p["plays"] / max(1, total_plays) * 100, 1)

    forgotten_gems = [
        t for t in all_tracks
        if t["plays"] >= 5 and t["days_since_played"] >= 45
    ][:50]

    late_night_list = []
    for t_key, ln_plays in sorted(late_night_tracks.items(), key=lambda x: x[1], reverse=True)[:40]:
        if t_key in tracks_map:
            tm = tracks_map[t_key]
            late_night_list.append({
                "rk": tm["rk"],
                "title": tm["title"],
                "artist": tm["artist"],
                "album": tm["album"],
                "plays": ln_plays,
                "total_plays": tm["plays"],
                "thumb": tm["thumb"],
            })

    morning_list = []
    for t_key, m_plays in sorted(morning_tracks.items(), key=lambda x: x[1], reverse=True)[:40]:
        if t_key in tracks_map:
            tm = tracks_map[t_key]
            morning_list.append({
                "rk": tm["rk"],
                "title": tm["title"],
                "artist": tm["artist"],
                "album": tm["album"],
                "plays": m_plays,
                "total_plays": tm["plays"],
                "thumb": tm["thumb"],
            })

    binge_days = sorted(daily_track_obsessions.values(), key=lambda x: x["plays"], reverse=True)[:16]
    for b in binge_days:
        b["minutes"] = round(b.pop("sec") / 60.0, 1)

    persona = compute_persona(hourly, top_artists_all, total_plays, len(all_tracks))
    lib_totals = get_plex_library_totals()

    total_mins = round(total_sec / 60.0, 1)
    total_hrs = round(total_sec / 3600.0, 1)
    total_days = round(total_sec / 86400.0, 1)
    active_days_cnt = max(1, len(daily_map))

    # Weekday (Mon-Fri: 1..5) vs Weekend (Sun=0, Sat=6)
    wknd_plays = weekdays[0]["plays"] + weekdays[6]["plays"]
    wknd_mins = round((weekdays[0]["sec"] + weekdays[6]["sec"]) / 60.0, 1)
    wkdy_plays = sum(weekdays[d]["plays"] for d in range(1, 6))
    wkdy_mins = round(sum(weekdays[d]["sec"] for d in range(1, 6)) / 60.0, 1)

    peak_month_obj = max(monthly_list, key=lambda x: x["minutes"]) if monthly_list else {"label": "N/A", "full_label": "N/A", "minutes": 0, "hours": 0, "plays": 0}

    milestones = {
        "tracks_50plus": sum(1 for t in all_tracks if t["plays"] >= 50),
        "tracks_25plus": sum(1 for t in all_tracks if t["plays"] >= 25),
        "tracks_10plus": sum(1 for t in all_tracks if t["plays"] >= 10),
        "tracks_5plus": sum(1 for t in all_tracks if t["plays"] >= 5),
        "tracks_1play": sum(1 for t in all_tracks if t["plays"] == 1),
        "artists_100plus": sum(1 for a in top_artists_all if a["plays"] >= 100),
        "artists_50plus": sum(1 for a in top_artists_all if a["plays"] >= 50),
        "artists_10plus": sum(1 for a in top_artists_all if a["plays"] >= 10),
        "albums_25plus": sum(1 for al in top_albums_all if al["plays"] >= 25),
    }

    if comparison:
        pv = comparison["previous"]
        comparison["delta_pct"] = {
            "plays": pct_change(total_plays, pv["plays"]),
            "minutes": pct_change(total_sec / 60.0, pv["minutes"]),
            "unique_tracks": pct_change(len(all_tracks), pv["unique_tracks"]),
            "unique_artists": pct_change(len(top_artists_all), pv["unique_artists"]),
        }

    new_artist_list = [a for a in top_artists_all if a["artist"].lower() in discovered_artists][:12]
    discoveries = {
        "new_tracks": len(discovered_tracks),
        "new_artists": len(discovered_artists),
        "new_track_plays": sum(t["plays"] for t in all_tracks if t["is_new"]),
        "top_new_tracks": [t for t in all_tracks if t["is_new"]][:12],
        "top_new_artists": new_artist_list,
    }

    repeat_plays_total = max(0, total_plays - len(all_tracks))
    repeat_pct = round(repeat_plays_total / max(1, total_plays) * 100, 1)

    result = {
        "range": range_key,
        "range_label": range_label,
        "available_years": get_available_years(),
        "handle": USER_HANDLE,
        "server_name": SERVER_NAME,
        "comparison": comparison,
        "discoveries": discoveries,
        "summary": {
            "total_plays": total_plays,
            "total_minutes": total_mins,
            "total_hours": total_hrs,
            "total_days": total_days,
            "unique_tracks": len(all_tracks),
            "unique_artists": len(top_artists_all),
            "unique_albums": len(top_albums_all),
            "active_days": len(daily_map),
            "avg_mins_per_active_day": round(total_mins / active_days_cnt, 1),
            "avg_plays_per_active_day": round(total_plays / active_days_cnt, 1),
            "completion_rate_pct": round(completed_plays / max(1, total_plays) * 100, 1),
            "skip_rate_pct": round(skipped_plays / max(1, total_plays) * 100, 1),
            "completed_plays": completed_plays,
            "partial_plays": partial_plays,
            "skipped_plays": skipped_plays,
            "longest_streak": longest_streak,
            "current_streak": current_streak,
            "busiest_day": busiest_day,
            "peak_hour": peak_hr_obj,
            "peak_weekday": peak_day_obj,
            "peak_month": {
                "label": peak_month_obj.get("label", "N/A"),
                "full_label": peak_month_obj.get("full_label", "N/A"),
                "minutes": peak_month_obj.get("minutes", 0),
                "hours": peak_month_obj.get("hours", 0),
                "plays": peak_month_obj.get("plays", 0),
            },
            "weekday_minutes": wkdy_mins,
            "weekday_plays": wkdy_plays,
            "weekday_pct": round(wkdy_plays / max(1, total_plays) * 100, 1),
            "weekend_minutes": wknd_mins,
            "weekend_plays": wknd_plays,
            "weekend_pct": round(wknd_plays / max(1, total_plays) * 100, 1),
            "repeat_plays": repeat_plays_total,
            "repeat_pct": repeat_pct,
            "milestones": milestones,
            "first_play": first_play_ts,
            "last_play": last_play_ts,
            "persona": persona,
            "library": {
                "total_tracks": lib_totals["total_tracks"],
                "total_artists": lib_totals["total_artists"],
                "total_albums": lib_totals["total_albums"],
                "track_coverage_pct": round(len(all_tracks) / max(1, lib_totals["total_tracks"]) * 100, 1),
                "artist_coverage_pct": round(len(top_artists_all) / max(1, lib_totals["total_artists"]) * 100, 1),
                "album_coverage_pct": round(len(top_albums_all) / max(1, lib_totals["total_albums"]) * 100, 1),
            }
        },
        "top_tracks": all_tracks[:100],
        "all_tracks": all_tracks,
        "top_artists": top_artists_all[:60],
        "top_albums": top_albums_all[:60],
        "hourly": hourly_list,
        "weekdays": weekday_list,
        "heatmap_7x24": {
            "days": day_names,
            "plays": heatmap_7x24,
            "minutes": heatmap_7x24_mins,
        },
        "monthly": monthly_list,
        "daily_60d": daily_series_60d,
        "daily_all": daily_list,
        "genres": genres_list,
        "decades": decades_list,
        "platforms": platforms_list,
        "actions": {
            "forgotten_gems": forgotten_gems,
            "late_night_vibes": late_night_list,
            "morning_boost": morning_list,
            "binge_days": binge_days,
        }
    }
    _WRAPPED_CACHE[range_key] = {"ts": int(time.time()), "data": result}
    return result


def build_smart_playlists_catalog(shuffle_throwbacks=False):
    all_data = compute_wrapped("all")
    d30_data = compute_wrapped("30d")
    all_tracks = all_data["all_tracks"]

    recent_14d = [t for t in all_tracks if t["days_since_played"] <= 14]
    recent_14d.sort(key=lambda x: (x["plays"], -x["days_since_played"]), reverse=True)
    weekly_best_tracks = list(recent_14d[:35])
    seen_wb = {t["rk"] for t in weekly_best_tracks}
    for t in d30_data["top_tracks"]:
        if len(weekly_best_tracks) >= 35:
            break
        if t["rk"] not in seen_wb:
            weekly_best_tracks.append(t)
            seen_wb.add(t["rk"])
    if len(weekly_best_tracks) < 35:
        for t in all_data["top_tracks"]:
            if len(weekly_best_tracks) >= 35:
                break
            if t["rk"] not in seen_wb:
                weekly_best_tracks.append(t)
                seen_wb.add(t["rk"])

    throwback_pool = [t for t in all_tracks if t["plays"] >= 4 and t["days_since_played"] >= 35]
    if len(throwback_pool) < 35:
        throwback_pool = [t for t in all_tracks if t["days_since_played"] >= 21]
    if shuffle_throwbacks:
        random.shuffle(throwback_pool)
    else:
        iso_year, iso_week, _ = datetime.date.today().isocalendar()
        rng = random.Random(f"{iso_year}-W{iso_week}")
        pool_copy = list(throwback_pool[:120])
        rng.shuffle(pool_copy)
        throwback_pool = pool_copy
    weekly_throwbacks_tracks = throwback_pool[:35]

    on_repeat_tracks = d30_data["top_tracks"][:40]
    if not on_repeat_tracks:
        on_repeat_tracks = all_data["top_tracks"][:40]

    late_night_tracks = all_data["actions"]["late_night_vibes"][:35]
    morning_tracks = all_data["actions"]["morning_boost"][:35]
    forgotten_tracks = all_data["actions"]["forgotten_gems"][:40]

    # On This Day: what you played within ±3 days of today's date in previous years
    otd_counts = {}
    try:
        conn = open_tautulli_db()
        today = datetime.date.today()
        for back in range(1, 11):
            try:
                anchor = today.replace(year=today.year - back)
            except ValueError:
                anchor = today.replace(year=today.year - back, day=28)
            w_start = int(datetime.datetime.combine(anchor - datetime.timedelta(days=3), datetime.time.min).timestamp())
            w_end = int(datetime.datetime.combine(anchor + datetime.timedelta(days=3), datetime.time.max).timestamp())
            for r in conn.execute(PLAYS_SQL, (w_start, w_end)):
                k = ((r["title"] or "Unknown Track").strip().lower(), (r["grandparent_title"] or "Unknown Artist").strip().lower())
                otd_counts[k] = otd_counts.get(k, 0) + 1
        conn.close()
    except Exception:
        pass
    otd = [t for t in all_tracks if (t["title"].lower(), t["artist"].lower()) in otd_counts]
    otd.sort(key=lambda t: (otd_counts[(t["title"].lower(), t["artist"].lower())], t["plays"]), reverse=True)
    on_this_day_tracks = otd[:35]
    all_time_100 = all_data["top_tracks"][:100]

    existing_plex = get_existing_plex_playlists()
    sync_state = load_sync_state()

    definitions = [
        {
            "id": "weekly_best",
            "title": "🔥 Rotation: Weekly Best Of",
            "short_title": "Weekly Best Of",
            "badge": "WEEKLY HEAVY ROTATION",
            "icon": "🔥",
            "description": "Your heaviest rotation tracks from the past week & current hot streak.",
            "tracks": weekly_best_tracks,
        },
        {
            "id": "weekly_throwbacks",
            "title": "⏳ Rotation: What You Used To Listen To",
            "short_title": "What You Used To Listen To",
            "badge": "WEEKLY THROWBACKS",
            "icon": "⏳",
            "description": "Rotating weekly mix of former heavy-repeat tracks you haven't spun in 35+ days.",
            "tracks": weekly_throwbacks_tracks,
        },
        {
            "id": "on_repeat_now",
            "title": "⚡ Rotation: On Repeat (30 Days)",
            "short_title": "On Repeat (Last 30 Days)",
            "badge": "30-DAY TOP 40",
            "icon": "⚡",
            "description": "Your 40 most-streamed songs over the past 30 days.",
            "tracks": on_repeat_tracks,
        },
        {
            "id": "late_night",
            "title": "🌙 Rotation: Late-Night Vibes",
            "short_title": "Late-Night Vibes (9PM–4AM)",
            "badge": "AFTER HOURS",
            "icon": "🌙",
            "description": "The tracks that dominate your headphones between 9 PM and 4 AM.",
            "tracks": late_night_tracks,
        },
        {
            "id": "all_time_100",
            "title": "🏆 Rotation: All-Time Top 100",
            "short_title": "All-Time Top 100",
            "badge": "HALL OF FAME",
            "icon": "🏆",
            "description": "Your 100 most-played tracks of all time on Plex.",
            "tracks": all_time_100,
        },
        {
            "id": "morning_boost",
            "title": "☀️ Rotation: Morning Boost",
            "short_title": "Morning Boost (5AM–12PM)",
            "badge": "MORNINGS",
            "icon": "☀️",
            "description": "The songs you reach for before noon.",
            "tracks": morning_tracks,
        },
        {
            "id": "forgotten_gems",
            "title": "💎 Rotation: Forgotten Gems",
            "short_title": "Forgotten Gems",
            "badge": "REDISCOVER",
            "icon": "💎",
            "description": "Songs you played 5+ times that you haven't touched in 45+ days.",
            "tracks": forgotten_tracks,
        },
        {
            "id": "on_this_day",
            "title": "📅 Rotation: On This Day",
            "short_title": "On This Day (Past Years)",
            "badge": "MEMORY LANE",
            "icon": "📅",
            "description": "What you were playing this same week in previous years.",
            "tracks": on_this_day_tracks,
        },
    ]

    out = []
    for d in definitions:
        pid = d["id"]
        st = sync_state["playlists"].get(pid, {})
        plex_info = existing_plex.get(d["title"]) or existing_plex.get(legacy_title(d["title"]) or "")
        in_plex = plex_info is not None
        out.append({
            "id": pid,
            "title": d["title"],
            "short_title": d["short_title"],
            "badge": d["badge"],
            "icon": d["icon"],
            "description": d["description"],
            "in_plex": in_plex,
            "plex_rk": plex_info["playlist_rk"] if in_plex else st.get("playlist_rk"),
            "plex_track_count": plex_info["track_count"] if in_plex else 0,
            "auto_update": bool(st.get("auto_update", False)),
            "last_synced": st.get("last_synced", 0),
            "candidate_count": len(d["tracks"]),
            "tracks": [
                {
                    "rk": t["rk"],
                    "title": t["title"],
                    "artist": t["artist"],
                    "album": t.get("album", ""),
                    "plays": t.get("plays", t.get("total_plays", 0)),
                    "days_since_played": t.get("days_since_played", 0),
                    "thumb": t.get("thumb", ""),
                }
                for t in d["tracks"]
            ]
        })

    return {
        "connected": True,
        "server_name": SERVER_NAME,
        "playlists": out,
    }


def sync_smart_playlist_by_id(playlist_id, shuffle=False):
    with _SYNC_LOCK:
        return _sync_smart_playlist_by_id(playlist_id, shuffle)


def _sync_smart_playlist_by_id(playlist_id, shuffle=False):
    catalog = build_smart_playlists_catalog(shuffle_throwbacks=shuffle)
    target = None
    for p in catalog["playlists"]:
        if p["id"] == playlist_id:
            target = p
            break
    if not target:
        raise ValueError(f"Unknown smart playlist id: {playlist_id}")

    rks = [t["rk"] for t in target["tracks"] if t.get("rk")]
    res = upsert_plex_playlist(target["title"], rks)

    state = load_sync_state()
    if playlist_id not in state["playlists"]:
        state["playlists"][playlist_id] = {}
    state["playlists"][playlist_id]["last_synced"] = int(time.time())
    state["playlists"][playlist_id]["playlist_rk"] = res["playlist_rk"]
    state["playlists"][playlist_id]["track_count"] = res["track_count"]
    save_sync_state(state)
    return res


def compute_discover(force=False):
    if not force and _DISCOVER_CACHE["data"] and time.time() - _DISCOVER_CACHE["ts"] < 600:
        return _DISCOVER_CACHE["data"]
    wrapped = compute_wrapped("all")
    top_artists = wrapped["top_artists"][:15]

    played_by_artist = {}
    for t in wrapped["all_tracks"]:
        ak = t["artist"].lower()
        played_by_artist.setdefault(ak, set()).add(t["title"].lower().strip())

    artist_mastery = []
    unheard_pool = []

    def _fetch_leaves(a):
        if not a["artist_rk"]:
            return None
        try:
            url = f"{PLEX_URL}/library/metadata/{a['artist_rk']}/allLeaves?X-Plex-Token={PLEX_TOKEN}"
            return ET.fromstring(urllib.request.urlopen(url, timeout=6).read()).findall("Track")
        except Exception:
            return None

    with ThreadPoolExecutor(max_workers=6) as pool:
        leaves_by_artist = list(pool.map(_fetch_leaves, top_artists))

    for a, tracks in zip(top_artists, leaves_by_artist):
        artist_name = a["artist"]
        artist_rk = a["artist_rk"]
        try:
            if not tracks:
                continue

            played_set = played_by_artist.get(artist_name.lower(), set())
            total_lib = len(tracks)
            unheard_list = []
            played_cnt = 0

            for tr in tracks:
                t_title = (tr.attrib.get("title") or "").strip()
                t_rk = tr.attrib.get("ratingKey")
                t_album = tr.attrib.get("parentTitle") or ""
                t_thumb = tr.attrib.get("parentThumb") or tr.attrib.get("thumb") or a["thumb"]
                vc = int(tr.attrib.get("viewCount", "0"))

                if t_title.lower() in played_set or vc > 0:
                    played_cnt += 1
                else:
                    item = {
                        "rk": int(t_rk) if t_rk and t_rk.isdigit() else t_rk,
                        "title": t_title,
                        "artist": artist_name,
                        "album": t_album,
                        "thumb": t_thumb,
                    }
                    unheard_list.append(item)

            seen_u = set()
            dedup_unheard = []
            for u in unheard_list:
                uk = u["title"].lower()
                if uk not in seen_u:
                    seen_u.add(uk)
                    dedup_unheard.append(u)

            unheard_pool.extend(dedup_unheard[:5])
            pct = min(100.0, round(played_cnt / max(1, total_lib) * 100, 1))
            artist_mastery.append({
                "artist": artist_name,
                "artist_rk": artist_rk,
                "thumb": a["thumb"],
                "plays": a["plays"],
                "hours": a["hours"],
                "library_tracks": total_lib,
                "played_tracks": played_cnt,
                "unheard_count": len(dedup_unheard),
                "completion_pct": pct,
                "unheard_sample": dedup_unheard[:6],
            })
        except Exception:
            continue

    unplayed_albums = []
    try:
        url = f"{PLEX_URL}/library/sections/{MUSIC_SECTION_ID}/all?type=9&unwatched=1&sort=random&X-Plex-Container-Start=0&X-Plex-Container-Size=12&X-Plex-Token={PLEX_TOKEN}"
        req = urllib.request.urlopen(url, timeout=4)
        root = ET.fromstring(req.read())
        for d in root.findall("Directory"):
            unplayed_albums.append({
                "album_rk": d.attrib.get("ratingKey"),
                "album": d.attrib.get("title") or "Unknown Album",
                "artist": d.attrib.get("parentTitle") or "Unknown Artist",
                "year": d.attrib.get("year") or "",
                "thumb": d.attrib.get("thumb") or "",
            })
    except Exception:
        pass

    result = {
        "artist_mastery": artist_mastery,
        "unheard_top_artist_tracks": unheard_pool[:50],
        "unplayed_albums": unplayed_albums,
    }
    _DISCOVER_CACHE["data"] = result
    _DISCOVER_CACHE["ts"] = time.time()
    return result


_EXTRAS_CACHE = {}


def compute_extras(range_key="all"):
    """Data for the extra Story Studio visualizers, loaded only when those cards are opened:
    - links:    artists you play back to back (Constellations)
    - weekly:   each week's top artist / genre (Musical Genome)
    - forecast: last 7 days vs the 4 weeks before, per artist (Music Forecast)
    - pulse:    every play of the last 14 days with how much of the song you heard (Listening Pulse)
    """
    range_key = normalize_range(range_key)
    hit = _EXTRAS_CACHE.get(range_key)
    if hit and time.time() - hit["ts"] < 60:
        return hit["data"]
    start_ts, end_ts, label = get_time_filter(range_key)
    conn = open_tautulli_db()
    try:
        rows = conn.execute(PLAYS_SQL, (start_ts, end_ts)).fetchall()
        now = int(time.time())
        recent = conn.execute(PLAYS_SQL, (now - 35 * 86400, now + 3600)).fetchall()
    finally:
        conn.close()

    def artist_of(r):
        return (r["grandparent_title"] or "Unknown Artist").strip()

    # --- Constellations: back-to-back artist pairs (within 30 min of each other)
    plays_by_artist = {}
    pair_counts = {}
    prev = None
    for r in rows:
        a = artist_of(r)
        plays_by_artist[a] = plays_by_artist.get(a, 0) + 1
        if prev is not None:
            pa, p_end = prev
            if pa != a and (r["started"] or 0) - (p_end or 0) <= 1800:
                key = tuple(sorted((pa, a), key=str.lower))
                pair_counts[key] = pair_counts.get(key, 0) + 1
        prev = (a, r["stopped"] or r["started"])
    top_nodes = sorted(plays_by_artist.items(), key=lambda x: x[1], reverse=True)[:28]
    node_set = {a for a, _ in top_nodes}
    links = sorted(
        ({"a": k[0], "b": k[1], "count": c} for k, c in pair_counts.items() if k[0] in node_set and k[1] in node_set),
        key=lambda x: x["count"], reverse=True,
    )[:90]

    # --- Genome: each week's top artist and genre
    weeks = {}
    for r in rows:
        d = datetime.date.fromtimestamp(r["started"] or 0)
        wk = (d - datetime.timedelta(days=d.weekday())).isoformat()
        listen, _ = listen_seconds(r["raw_sec"], r["duration"])
        w = weeks.setdefault(wk, {"artists": {}, "genres": {}, "minutes": 0.0, "plays": 0})
        a = artist_of(r)
        w["artists"][a] = w["artists"].get(a, 0) + listen
        g = (r["genres"] or "").replace(";", ",").split(",")[0].strip()
        if g:
            w["genres"][g] = w["genres"].get(g, 0) + listen
        w["minutes"] += listen / 60.0
        w["plays"] += 1
    weekly = []
    for wk in sorted(weeks):
        w = weeks[wk]
        weekly.append({
            "week": wk,
            "top_artist": max(w["artists"], key=w["artists"].get) if w["artists"] else "",
            "top_genre": max(w["genres"], key=w["genres"].get) if w["genres"] else "",
            "minutes": round(w["minutes"], 1),
            "plays": w["plays"],
        })
    weekly = weekly[-53:]

    # --- Forecast + Pulse (always "now", whatever range is picked)
    last7, prior = {}, {}
    pulse = []
    for r in recent:
        st = r["started"] or 0
        a = artist_of(r)
        if st >= now - 7 * 86400:
            last7[a] = last7.get(a, 0) + 1
        else:
            prior[a] = prior.get(a, 0) + 1
        if st >= now - 14 * 86400:
            listen, dur = listen_seconds(r["raw_sec"], r["duration"])
            pulse.append([st, round(min(1.0, listen / max(1.0, dur)), 2)])
    total7 = sum(last7.values()) or 1
    forecast = []
    for a in set(last7) | set(prior):
        now_p = last7.get(a, 0)
        weekly_avg = prior.get(a, 0) / 4.0
        if now_p + weekly_avg < 3:
            continue
        forecast.append({
            "artist": a,
            "last7": now_p,
            "weekly_avg": round(weekly_avg, 1),
            "change_pct": round((now_p - weekly_avg) / max(1.0, weekly_avg) * 100),
            "chance_pct": round(now_p / total7 * 100),
            "new": a not in prior,
        })
    forecast.sort(key=lambda x: (x["last7"], x["weekly_avg"]), reverse=True)

    data = {
        "range": range_key,
        "range_label": label,
        "nodes": [{"artist": a, "plays": p} for a, p in top_nodes],
        "links": links,
        "weekly": weekly,
        "forecast": forecast[:10],
        "pulse": pulse,
        "pulse_days": 14,
    }
    _EXTRAS_CACHE[range_key] = {"ts": time.time(), "data": data}
    return data


def compute_artist_detail(name):
    data = compute_wrapped("all")
    key = (name or "").strip().lower()
    artist = next((a for a in data["top_artists"] if a["artist"].lower() == key), None)
    tracks = [t for t in data["all_tracks"] if t["artist"].lower() == key]
    if not tracks:
        raise ValueError("Artist not found in your history")
    if artist is None:
        plays = sum(t["plays"] for t in tracks)
        artist = {"artist": tracks[0]["artist"], "plays": plays, "thumb": tracks[0]["artist_thumb"],
                  "hours": round(sum(t["minutes"] for t in tracks) / 60, 1)}
    rank = next((i + 1 for i, a in enumerate(data["top_artists"]) if a["artist"].lower() == key), None)
    albums = {}
    for t in tracks:
        al = albums.setdefault(t["album"], {"album": t["album"], "plays": 0, "thumb": t["thumb"], "year": t["year"]})
        al["plays"] += t["plays"]
    # per-month plays for this artist (all months since your first play, zeros included)
    per_month = {}
    conn = open_tautulli_db()
    for r in conn.execute(PLAYS_SQL, (0, int(time.time()) + 3600)):
        if (r["grandparent_title"] or "Unknown Artist").strip().lower() == key:
            ym = datetime.datetime.fromtimestamp(r["started"] or 0).strftime("%Y-%m")
            per_month[ym] = per_month.get(ym, 0) + 1
    conn.close()
    monthly = [{"month": m["month"], "label": m["label"], "plays": per_month.get(m["month"], 0)} for m in data["monthly"]]
    return {
        "artist": artist["artist"],
        "thumb": artist.get("thumb", ""),
        "rank": rank,
        "plays": artist["plays"],
        "hours": artist.get("hours", 0),
        "unique_tracks": len(tracks),
        "first_played": min(t["first_played"] for t in tracks),
        "last_played": max(t["last_played"] for t in tracks),
        "top_tracks": sorted(tracks, key=lambda x: x["plays"], reverse=True)[:15],
        "albums": sorted(albums.values(), key=lambda x: x["plays"], reverse=True)[:10],
        "monthly": monthly[-24:],
    }


def compute_summary_widget():
    """Small payload for Homepage's customapi widget or phone widgets."""
    d7 = compute_wrapped("7d")
    d30 = compute_wrapped("30d")
    today_key = datetime.date.today().isoformat()
    today = next((d for d in d7["daily_60d"] if d["date"] == today_key), {"minutes": 0, "plays": 0})
    s7 = d7["summary"]
    return {
        "today_minutes": today["minutes"],
        "today_plays": today["plays"],
        "week_minutes": s7["total_minutes"],
        "week_hours": s7["total_hours"],
        "week_plays": s7["total_plays"],
        "current_streak": s7["current_streak"],
        "top_artist_30d": d30["top_artists"][0]["artist"] if d30["top_artists"] else "",
        "top_track_7d": f"{d7['top_tracks'][0]['title']} — {d7['top_tracks'][0]['artist']}" if d7["top_tracks"] else "",
    }


def build_monthly_recap_text(year, month):
    start = int(datetime.datetime(year, month, 1).timestamp())
    _, ndays = calendar.monthrange(year, month)
    data = compute_wrapped(str(year))
    ym = f"{year:04d}-{month:02d}"
    m = next((x for x in data["monthly"] if x["month"] == ym), None)
    if not m or not m["plays"]:
        return None, None
    title = f"Rotation - {m['full_label']} recap"  # plain ASCII: HTTP headers can't carry "·"
    lines = [
        f"{fmt_int(m['minutes'])} minutes · {fmt_int(m['plays'])} plays · {m['active_days']} active days",
    ]
    if m["top_artists"]:
        lines.append(f"Top artist: {m['top_artists'][0]['artist']} ({m['top_artists'][0]['plays']} plays)")
    if m["top_tracks"]:
        lines.append(f"Top song: {m['top_tracks'][0]['title']} — {m['top_tracks'][0]['artist']}")
    lines.append(f"New songs discovered: {m.get('new_tracks', 0)}")
    return title, "\n".join(lines)


def fmt_int(n):
    return f"{int(round(n or 0)):,}"


def send_recap(year, month):
    if not NTFY_URL:
        raise RuntimeError("NTFY_URL is not set")
    title, body = build_monthly_recap_text(year, month)
    if not title:
        raise RuntimeError("No plays that month")
    headers = {"Title": title, "Tags": "headphones"}
    if PUBLIC_URL:
        headers["Click"] = PUBLIC_URL
    req = urllib.request.Request(NTFY_URL, data=body.encode("utf-8"), headers=headers, method="POST")
    urllib.request.urlopen(req, timeout=10).read()
    return {"success": True, "title": title, "body": body}


def maybe_send_monthly_recap():
    if not NTFY_URL:
        return
    today = datetime.date.today()
    last_month_end = today.replace(day=1) - datetime.timedelta(days=1)
    key = last_month_end.strftime("%Y-%m")
    state = load_sync_state()
    if state.get("recap_last_sent") == key:
        return
    send_recap(last_month_end.year, last_month_end.month)
    state["recap_last_sent"] = key
    save_sync_state(state)


def auto_sync_worker():
    while True:
        try:
            time.sleep(60)
            state = load_sync_state()
            now = int(time.time())
            for pid, info in state.get("playlists", {}).items():
                if info.get("auto_update") and (now - info.get("last_synced", 0) >= 7 * 86400):
                    try:
                        sync_smart_playlist_by_id(pid, shuffle=False)
                    except Exception:
                        pass
            try:
                maybe_send_monthly_recap()
            except Exception as e:
                print(f"Monthly recap failed: {e}")
        except Exception:
            pass
        time.sleep(21600)


class ThreadedHTTPServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True
    allow_reuse_address = True


class WrappedHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass

    def maybe_gzip(self, body):
        if len(body) > 1024 and "gzip" in (self.headers.get("Accept-Encoding") or ""):
            return gzip.compress(body, compresslevel=5), True
        return body, False

    def send_json(self, data, status=HTTPStatus.OK, extra_headers=None):
        body, zipped = self.maybe_gzip(json.dumps(data, separators=(",", ":")).encode("utf-8"))
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        if zipped:
            self.send_header("Content-Encoding", "gzip")
        self.send_header("Vary", "Accept-Encoding")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        if extra_headers:
            for k, v in extra_headers:
                self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)

    def _get_cookie(self, name):
        raw = self.headers.get("Cookie", "")
        if not raw:
            return ""
        c = http.cookies.SimpleCookie()
        try:
            c.load(raw)
        except Exception:
            return ""
        return c[name].value if name in c else ""

    def _session_cookie_header(self, token, max_age=30 * 86400):
        parts = [
            f"rotation_session={token}",
            "Path=/",
            "HttpOnly",
            "SameSite=Lax",
            f"Max-Age={max_age}",
        ]
        if self.headers.get("X-Forwarded-Proto", "").lower() == "https":
            parts.append("Secure")
        return ("Set-Cookie", "; ".join(parts))

    def is_authenticated(self):
        if not get_active_password_spec():
            return True
        return verify_session_token(self._get_cookie("rotation_session"))

    def require_auth(self):
        if self.is_authenticated():
            return True
        self.send_json({"error": "Authentication required", "auth_required": True}, status=HTTPStatus.UNAUTHORIZED)
        return False

    def _auth_status_payload(self, authenticated):
        auth_required = bool(get_active_password_spec())
        if not authenticated:
            return {
                "authenticated": False,
                "auth_required": auth_required,
                "needs_setup": False,
            }
        status = get_setup_status()
        needs_setup = not status["db_configured"]
        return {
            "authenticated": True,
            "auth_required": auth_required,
            "needs_setup": needs_setup,
            "setup": status,
        }

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        qs = urllib.parse.parse_qs(parsed.query)

        if path == "/" or path == "/index.html":
            index_path = os.path.join(DATA_DIR, "index.html")
            if os.path.exists(index_path):
                with open(index_path, "rb") as f:
                    content = f.read()
                content, zipped = self.maybe_gzip(content)
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                if zipped:
                    self.send_header("Content-Encoding", "gzip")
                self.send_header("Content-Length", str(len(content)))
                self.send_header("Cache-Control", "no-store")
                self.end_headers()
                self.wfile.write(content)
            else:
                self.send_error(HTTPStatus.NOT_FOUND, "index.html not found")
            return

        if path == "/manifest.json":
            manifest = {
                "name": "Rotation",
                "short_name": "Rotation",
                "start_url": "/",
                "display": "standalone",
                "orientation": "any",
                "description": "Your Plexamp listening stats, smart playlists and shareable story cards.",
                "background_color": "#211f1a",
                "theme_color": "#211f1a",
                "icons": [
                    {"src": "/icon.svg", "sizes": "any", "type": "image/svg+xml", "purpose": "any"},
                    {"src": "/icon-192.png", "sizes": "192x192", "type": "image/png", "purpose": "any"},
                    {"src": "/icon-512.png", "sizes": "512x512", "type": "image/png", "purpose": "any"}
                ],
                "shortcuts": [
                    {"name": "Story Studio", "url": "/?story=1"},
                    {"name": "Last 7 Days", "url": "/?range=7d"},
                    {"name": "Plex Playlists", "url": "/?view=playlists"}
                ]
            }
            self.send_json(manifest)
            return

        if path in ["/sw.js", "/icon-192.png", "/icon-512.png"]:
            local_f = os.path.join(DATA_DIR, path.lstrip("/"))
            if os.path.exists(local_f):
                ct = "application/javascript" if path.endswith(".js") else "image/png"
                with open(local_f, "rb") as f:
                    data = f.read()
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", ct)
                self.send_header("Content-Length", str(len(data)))
                self.send_header("Cache-Control", "no-cache" if path.endswith(".js") else "public, max-age=604800")
                self.end_headers()
                self.wfile.write(data)
                return

        if path == "/icon.svg":
            svg = b"""<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 100 100"><rect width="100" height="100" rx="24" fill="#29261f"/><circle cx="50" cy="50" r="42" fill="#1c1a16"/><circle cx="50" cy="50" r="20" fill="none" stroke="#3d382f" stroke-width="0.7"/><circle cx="50" cy="50" r="23" fill="none" stroke="#3d382f" stroke-width="0.7"/><circle cx="50" cy="50" r="26" fill="none" stroke="#3d382f" stroke-width="0.7"/><circle cx="50" cy="50" r="29" fill="none" stroke="#3d382f" stroke-width="0.7"/><circle cx="50" cy="50" r="32" fill="none" stroke="#3d382f" stroke-width="0.7"/><circle cx="50" cy="50" r="35" fill="none" stroke="#3d382f" stroke-width="0.7"/><circle cx="50" cy="50" r="38" fill="none" stroke="#3d382f" stroke-width="0.7"/><g transform="rotate(45 50 50)"><path d="M16.17 37.69 A36 36 0 0 1 34.79 17.37" fill="none" stroke="#ede6d6" stroke-opacity="0.32" stroke-width="3" stroke-linecap="round"/><path d="M24.02 35.00 A30 30 0 0 1 34.10 24.56" fill="none" stroke="#ede6d6" stroke-opacity="0.2" stroke-width="2" stroke-linecap="round"/><path d="M83.83 62.31 A36 36 0 0 1 65.21 82.63" fill="none" stroke="#ede6d6" stroke-opacity="0.2" stroke-width="3" stroke-linecap="round"/><path d="M75.98 65.00 A30 30 0 0 1 65.90 75.44" fill="none" stroke="#ede6d6" stroke-opacity="0.12" stroke-width="2" stroke-linecap="round"/><circle cx="50" cy="50" r="16" fill="#d08a70"/><path d="M50 50 C50 45 50 41 50 37" stroke="#efeae0" stroke-width="1.8" fill="none" stroke-linecap="round"/><path d="M50 46 C44.5 45 41.5 41.5 41.5 37 C46.5 37 50 40.5 50 46Z" fill="#a7bb8c"/><path d="M50 42 C55.5 41 58.5 37.5 58.5 33 C53.5 33 50 36.5 50 42Z" fill="#93b08e"/></g><circle cx="50" cy="50" r="1.9" fill="#1c1a16"/></svg>"""
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "image/svg+xml")
            self.send_header("Content-Length", str(len(svg)))
            self.end_headers()
            self.wfile.write(svg)
            return

        if path == "/api/auth/status":
            self.send_json(self._auth_status_payload(self.is_authenticated()))
            return

        if path == "/api/wrapped":
            if not self.require_auth():
                return
            range_key = qs.get("range", ["all"])[0]
            try:
                data = compute_wrapped(range_key)
                self.send_json(data)
            except Exception as e:
                self.send_json({"error": str(e)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)
            return

        if path == "/api/extras":
            if not self.require_auth():
                return
            try:
                self.send_json(compute_extras(qs.get("range", ["all"])[0]))
            except Exception as e:
                self.send_json({"error": str(e)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)
            return

        if path == "/api/summary":
            if not self.require_auth():
                return
            try:
                self.send_json(compute_summary_widget())
            except Exception as e:
                self.send_json({"error": str(e)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)
            return

        if path == "/api/artist":
            if not self.require_auth():
                return
            try:
                self.send_json(compute_artist_detail(qs.get("name", [""])[0]))
            except ValueError as e:
                self.send_json({"error": str(e)}, status=HTTPStatus.NOT_FOUND)
            except Exception as e:
                self.send_json({"error": str(e)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)
            return

        if path == "/api/plex/playlists":
            if not self.require_auth():
                return
            shuffle = qs.get("shuffle", ["0"])[0] == "1"
            try:
                data = build_smart_playlists_catalog(shuffle_throwbacks=shuffle)
                self.send_json(data)
            except Exception as e:
                self.send_json({"error": str(e)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)
            return

        if path == "/api/discover":
            if not self.require_auth():
                return
            try:
                data = compute_discover(force=qs.get("force", ["0"])[0] == "1")
                self.send_json(data)
            except Exception as e:
                self.send_json({"error": str(e)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)
            return

        if path == "/api/thumb":
            if not self.require_auth():
                return
            thumb_path = qs.get("path", [""])[0]
            if not thumb_path or not thumb_path.startswith("/library/") or ".." in thumb_path:
                self.send_error(HTTPStatus.BAD_REQUEST)
                return
            cache_key = hashlib.md5(thumb_path.encode("utf-8")).hexdigest() + ".jpg"
            cache_file = os.path.join(CACHE_DIR, cache_key)
            if os.path.exists(cache_file):
                with open(cache_file, "rb") as f:
                    img = f.read()
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", "image/jpeg")
                self.send_header("Content-Length", str(len(img)))
                self.send_header("Cache-Control", "public, max-age=604800")
                self.end_headers()
                self.wfile.write(img)
                return

            try:
                encoded_url = urllib.parse.quote(thumb_path, safe="")
                transcode_url = f"{PLEX_URL}/photo/:/transcode?width=360&height=360&minSize=1&upscale=1&url={encoded_url}&X-Plex-Token={PLEX_TOKEN}"
                req = urllib.request.urlopen(transcode_url, timeout=5)
                img = req.read()
                with open(cache_file, "wb") as f:
                    f.write(img)
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", "image/jpeg")
                self.send_header("Content-Length", str(len(img)))
                self.send_header("Cache-Control", "public, max-age=604800")
                self.end_headers()
                self.wfile.write(img)
            except Exception:
                try:
                    parts = thumb_path.strip("/").split("/")
                    if len(parts) >= 3 and parts[0] == "library" and parts[1] == "metadata":
                        fallback_url = f"{PLEX_URL}/library/metadata/{parts[2]}/thumb?X-Plex-Token={PLEX_TOKEN}"
                        req = urllib.request.urlopen(fallback_url, timeout=5)
                        img = req.read()
                        with open(cache_file, "wb") as f:
                            f.write(img)
                        self.send_response(HTTPStatus.OK)
                        self.send_header("Content-Type", "image/jpeg")
                        self.send_header("Content-Length", str(len(img)))
                        self.end_headers()
                        self.wfile.write(img)
                        return
                except Exception:
                    pass
                self.send_error(HTTPStatus.NOT_FOUND)
            return

        if path == "/api/stream":
            if not self.require_auth():
                return
            rk = qs.get("rk", [""])[0]
            if not rk or not rk.isdigit():
                self.send_error(HTTPStatus.BAD_REQUEST)
                return
            try:
                meta_url = f"{PLEX_URL}/library/metadata/{rk}?X-Plex-Token={PLEX_TOKEN}"
                root = ET.fromstring(urllib.request.urlopen(meta_url, timeout=5).read())
                part = root.find(".//Part")
                if part is None:
                    self.send_error(HTTPStatus.NOT_FOUND, "Audio part not found")
                    return
                part_key = part.attrib.get("key")
                stream_url = f"{PLEX_URL}{part_key}?X-Plex-Token={PLEX_TOKEN}"
                req_headers = {}
                if "Range" in self.headers:
                    req_headers["Range"] = self.headers["Range"]
                req = urllib.request.Request(stream_url, headers=req_headers)
                with urllib.request.urlopen(req, timeout=15) as upstream:
                    self.send_response(upstream.status)
                    for hdr in ["Content-Type", "Content-Length", "Accept-Ranges", "Content-Range"]:
                        val = upstream.headers.get(hdr)
                        if val:
                            self.send_header(hdr, val)
                    self.end_headers()
                    while True:
                        chunk = upstream.read(65536)
                        if not chunk:
                            break
                        self.wfile.write(chunk)
            except (BrokenPipeError, ConnectionResetError):
                pass
            except Exception as e:
                try:
                    self.send_error(HTTPStatus.INTERNAL_SERVER_ERROR, str(e))
                except Exception:
                    pass
            return

        self.send_error(HTTPStatus.NOT_FOUND)

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path

        # Binary SQLite upload handled before JSON body decoding
        if path == "/api/setup/upload-db":
            if not self.require_auth():
                return
            try:
                length = int(self.headers.get("Content-Length", "0"))
            except ValueError:
                length = -1
            if length <= 0 or length > MAX_DB_UPLOAD_BYTES:
                self.send_json({"success": False, "error": "Invalid upload size (must be between 1 byte and 512 MB)"}, status=HTTPStatus.BAD_REQUEST)
                return
            tmp_path = UPLOADED_DB_FILE + ".tmp"
            remaining = length
            try:
                with open(tmp_path, "wb") as out:
                    while remaining > 0:
                        chunk = self.rfile.read(min(65536, remaining))
                        if not chunk:
                            break
                        out.write(chunk)
                        remaining -= len(chunk)
                if remaining != 0:
                    raise ValueError("Upload interrupted before full file was received")
                info = validate_tautulli_sqlite(tmp_path)
                os.replace(tmp_path, UPLOADED_DB_FILE)
                cfg = load_runtime_config()
                cfg["db_path"] = UPLOADED_DB_FILE
                save_runtime_config(cfg)
                clear_all_caches()
                self.send_json({"success": True, "db_path": UPLOADED_DB_FILE, "setup": get_setup_status(), **info})
            except Exception as e:
                if os.path.exists(tmp_path):
                    try:
                        os.remove(tmp_path)
                    except OSError:
                        pass
                self.send_json({"success": False, "error": str(e)}, status=HTTPStatus.BAD_REQUEST)
            return

        try:
            length = int(self.headers.get("Content-Length", "0"))
            if length > 2 * 1024 * 1024:
                self.send_json({"success": False, "error": "JSON payload too large"}, status=HTTPStatus.REQUEST_ENTITY_TOO_LARGE)
                return
            body = json.loads(self.rfile.read(length).decode("utf-8")) if length > 0 else {}
        except Exception:
            body = {}

        if path == "/api/auth/login":
            spec = get_active_password_spec()
            if not spec:
                self.send_json({"success": True, **self._auth_status_payload(True)})
                return
            pw = body.get("password", "")
            if not _verify_password(pw, spec):
                time.sleep(0.25)
                self.send_json({"success": False, "error": "Incorrect passcode"}, status=HTTPStatus.UNAUTHORIZED)
                return
            token = create_session_token()
            self.send_json(
                {"success": True, **self._auth_status_payload(True)},
                extra_headers=[self._session_cookie_header(token)],
            )
            return

        if path == "/api/auth/logout":
            self.send_json(
                {"success": True, **self._auth_status_payload(not bool(get_active_password_spec()))},
                extra_headers=[self._session_cookie_header("", max_age=0)],
            )
            return

        if not self.require_auth():
            return

        if path == "/api/setup/test-plex":
            try:
                plex_url = body.get("plex_url") or PLEX_URL
                plex_token = body.get("plex_token") or PLEX_TOKEN
                res = test_plex_connection(plex_url, plex_token)
                self.send_json(res)
            except Exception as e:
                self.send_json({"success": False, "error": str(e)}, status=HTTPStatus.BAD_REQUEST)
            return

        if path == "/api/setup/fetch-db":
            try:
                res = fetch_db_from_url(body.get("url", ""))
                res["setup"] = get_setup_status()
                self.send_json(res)
            except Exception as e:
                self.send_json({"success": False, "error": str(e)}, status=HTTPStatus.BAD_REQUEST)
            return

        if path == "/api/setup/save":
            try:
                cfg = load_runtime_config()
                db_path_in = (body.get("db_path") or "").strip()
                if db_path_in:
                    validate_tautulli_sqlite(db_path_in)
                    cfg["db_path"] = db_path_in

                if "plex_url" in body:
                    val = (body.get("plex_url") or "").strip().rstrip("/")
                    if val:
                        parsed_u = urllib.parse.urlparse(val)
                        if parsed_u.scheme not in ("http", "https") or not parsed_u.netloc:
                            raise ValueError("Plex URL must start with http:// or https://")
                        cfg["plex_url"] = val
                if body.get("plex_token"):
                    cfg["plex_token"] = str(body.get("plex_token")).strip()
                if "machine_id" in body:
                    cfg["machine_id"] = str(body.get("machine_id") or "").strip()
                if "music_section_id" in body:
                    sec = str(body.get("music_section_id") or "1").strip()
                    if sec:
                        cfg["music_section_id"] = sec
                if "server_name" in body:
                    sname = str(body.get("server_name") or "").strip()
                    if sname:
                        cfg["server_name"] = sname
                if "handle" in body:
                    h = str(body.get("handle") or "").strip()
                    if h:
                        cfg["handle"] = h
                if "ntfy_url" in body:
                    cfg["ntfy_url"] = str(body.get("ntfy_url") or "").strip()
                if "public_url" in body:
                    cfg["public_url"] = str(body.get("public_url") or "").strip()

                extra_hdrs = None
                if body.get("clear_password"):
                    cfg.pop("password_hash", None)
                    cfg["password_cleared"] = True
                    extra_hdrs = [self._session_cookie_header("", max_age=0)]
                elif body.get("password"):
                    pw_new = str(body.get("password"))
                    if len(pw_new) < 4:
                        raise ValueError("Passcode must be at least 4 characters")
                    cfg["password_hash"] = _hash_password(pw_new)
                    cfg.pop("password_cleared", None)
                    extra_hdrs = [self._session_cookie_header(create_session_token())]

                save_runtime_config(cfg)
                apply_runtime_config()
                clear_all_caches()
                self.send_json({"success": True, **self._auth_status_payload(True)}, extra_headers=extra_hdrs)
            except Exception as e:
                self.send_json({"success": False, "error": str(e)}, status=HTTPStatus.BAD_REQUEST)
            return

        if path == "/api/playlist/create":
            try:
                title = body.get("title", "Rotation Playlist")
                rks = body.get("rating_keys", [])
                album_rk = body.get("album_rk")
                res = upsert_plex_playlist(title, rks, album_rk=album_rk)
                self.send_json(res)
            except Exception as e:
                self.send_json({"success": False, "error": str(e)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)
            return

        if path == "/api/plex/sync-playlist":
            try:
                pid = body.get("id")
                shuffle = bool(body.get("shuffle", False))
                res = sync_smart_playlist_by_id(pid, shuffle=shuffle)
                self.send_json(res)
            except Exception as e:
                self.send_json({"success": False, "error": str(e)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)
            return

        if path == "/api/plex/sync-all":
            try:
                results = []
                for pid in [p["id"] for p in build_smart_playlists_catalog()["playlists"] if p["candidate_count"]]:
                    results.append(sync_smart_playlist_by_id(pid, shuffle=False))
                self.send_json({"success": True, "synced": len(results), "results": results})
            except Exception as e:
                self.send_json({"success": False, "error": str(e)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)
            return

        if path == "/api/recap/test":
            try:
                today = datetime.date.today()
                self.send_json(send_recap(today.year, today.month))
            except Exception as e:
                self.send_json({"success": False, "error": str(e)}, status=HTTPStatus.BAD_REQUEST)
            return

        if path == "/api/plex/toggle-auto":
            try:
                pid = body.get("id")
                auto_val = bool(body.get("auto_update", False))
                state = load_sync_state()
                if pid not in state["playlists"]:
                    state["playlists"][pid] = {}
                state["playlists"][pid]["auto_update"] = auto_val
                save_sync_state(state)
                self.send_json({"success": True, "id": pid, "auto_update": auto_val})
            except Exception as e:
                self.send_json({"success": False, "error": str(e)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)
            return

        self.send_error(HTTPStatus.NOT_FOUND)


if __name__ == "__main__":
    t = threading.Thread(target=auto_sync_worker, daemon=True)
    t.start()
    if not PLEX_TOKEN or not MACHINE_ID:
        print("WARNING: PLEX_TOKEN / MACHINE_ID not set — complete setup in the Rotation Setup modal or add them to .env")
    print(f"Starting Rotation (music stats & playlist engine) on port {PORT}...")
    with ThreadedHTTPServer(("0.0.0.0", PORT), WrappedHandler) as httpd:
        httpd.serve_forever()
