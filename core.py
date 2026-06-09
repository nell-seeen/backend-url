"""
backend.py — Production Music Backend v7.0 (Full-Stack Upgrade)
================================================================================
Single-file FastAPI + WebSocket music backend yang dioptimalkan khusus untuk
Termux Android. Semua endpoint & websocket lama tetap kompatibel 100%.

Upgrade v7.0 (di atas v6.0) — Full Production Stack:
  ── Upgrade #2: JWT Authentication System ──
  - JWTAuth: access token (15m) + refresh token (7d), HMAC-SHA256
  - SessionStore: multi-device session tracking (SQLite-backed)
  - POST /auth/login — username/password → access+refresh token
  - POST /auth/refresh — rotate refresh token
  - POST /auth/logout — revoke session
  - GET  /auth/me — current user info
  - WS Authorization: Bearer TOKEN header support
  - Optional auth mode: AUTH_REQUIRED=0 (default, backward compat)

  ── Upgrade #3: Advanced Rate Limiter ──
  - TokenBucketLimiter: burst + cooldown + temp-block per IP
  - Per-endpoint limits: search, stream, download, auth, ws
  - 429 Too Many Requests dengan Retry-After header
  - Pure asyncio, zero heavy dependencies

  ── Upgrade #4: Advanced Search Engine ──
  - 3-layer search: Cache → Local Index → YTMusic
  - RapidFuzz re-ranking + search score + weight system
  - Search history ranking (boost hasil pernah dicari)
  - Popular search ranking (boost hasil sering dicari)
  - GET /search/suggest — autocomplete suggestions
  - GET /search/popular — top queries dari history
  - GET /search/history — user search history (alias baru)

  ── Upgrade #5: Full Audio Proxy Engine ──
  - GET /audio/proxy/{videoId} — stream proxy tanpa ekspos URL Google
  - Range Request support (206 Partial Content)
  - Byte seeking, chunk streaming, auto-reconnect
  - Frontend hanya menerima /audio/proxy/{videoId}

  ── Upgrade #6: Auto Stream URL Refresh (enhanced) ──
  - Background worker lebih agresif (cek tiap 60 detik)
  - Broadcast stream_url_refreshed ke WS
  - Frontend tidak pernah lihat 403/expired

  ── Upgrade #7: Smart Recommendation Engine ──
  - Listening history analysis + favorite weighting
  - Artist/genre/track similarity via watch playlist chaining
  - Recommendation score system
  - GET /recommendations/similar/{videoId}
  - GET /recommendations/personal
  - GET /recommendations (enhanced, existing)

  ── Upgrade #8: Smart Radio Mode (enhanced) ──
  - RadioEngine: infinite queue generator
  - Artist-based, album-based, track-based radio
  - Autoplay chain dengan event radio_generated
  - GET /radio/start — start radio dari seed
  - GET /radio/status — status radio aktif

  ── Upgrade #9: Thumbnail Proxy + Cache Server ──
  - GET /thumb/{videoId} — thumbnail proxy
  - RAM cache + disk cache + TTL
  - Auto-refresh expired thumbnails
  - Frontend tidak langsung ke YouTube

  ── Upgrade #10: Absolute Frontend Synchronization ──
  - Semua WS events membawa state_version + server_time
  - Events baru: queue_add, queue_remove, queue_reorder, queue_clear
  - Events baru: favorite_add, favorite_remove
  - Events baru: history_add, history_remove
  - Events baru: settings_update, recommendation_update
  - Events baru: radio_generated, thumbnail_cached, stream_url_refreshed

Run:
    python backend.py
================================================================================
"""

# ─────────────────────────────────────────────────────────────────────────────
# 0. STANDARD LIBRARY IMPORTS
# ─────────────────────────────────────────────────────────────────────────────

import asyncio
import base64
import contextlib
import difflib
import gc
import hashlib
import hmac
import io
import json as _stdlib_json
import logging
import mimetypes
import os
import random
import re
import secrets
import shutil
import signal
import sqlite3
import struct
import sys
import threading
import time
import traceback
import urllib.parse
import urllib.request
import uuid
from collections import OrderedDict, defaultdict, deque
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from logging.handlers import RotatingFileHandler
from typing import Any, Awaitable, Callable, Dict, List, Optional, Set, Tuple

# ─────────────────────────────────────────────────────────────────────────────
# 1. LOGGING SETUP (Rotating + Categorized + Crash Dump)
# ─────────────────────────────────────────────────────────────────────────────

LOG_FILE = "logs.txt"
CRASH_DUMP_FILE = "crash_dump.log"

log_formatter = logging.Formatter(
    "[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
# Clear default handlers (idempotent on reload)
for _h in list(root_logger.handlers):
    root_logger.removeHandler(_h)

_file_handler = RotatingFileHandler(LOG_FILE, maxBytes=5 * 1024 * 1024, backupCount=3)
_file_handler.setFormatter(log_formatter)
root_logger.addHandler(_file_handler)

_console_handler = logging.StreamHandler(sys.stdout)
_console_handler.setFormatter(log_formatter)
root_logger.addHandler(_console_handler)

logger = logging.getLogger("backend")
dl_logger = logging.getLogger("download")
stream_logger = logging.getLogger("stream")
ws_logger = logging.getLogger("ws")
cache_logger = logging.getLogger("cache")
cleanup_logger = logging.getLogger("cleanup")


def _crash_dump(msg: str, exc: Optional[BaseException] = None):
    """Tulis crash trace ke file terpisah untuk forensik tanpa pollute log utama."""
    try:
        with open(CRASH_DUMP_FILE, "a", encoding="utf-8") as f:
            f.write(f"\n[{datetime.now().isoformat()}] {msg}\n")
            if exc is not None:
                f.write(traceback.format_exc())
            f.write("=" * 60 + "\n")
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────────────────────
# 2. OPTIONAL DEPENDENCY LAYER (Termux-safe fallback)
# ─────────────────────────────────────────────────────────────────────────────

# --- orjson (optional, fast JSON) ---
try:
    import orjson  # type: ignore

    def json_dumps(obj: Any) -> str:
        try:
            return orjson.dumps(obj, default=str).decode("utf-8")
        except Exception:
            return _stdlib_json.dumps(obj, default=str, ensure_ascii=False)

    def json_loads(s: Any) -> Any:
        if isinstance(s, (bytes, bytearray)):
            return orjson.loads(s)
        return orjson.loads(s.encode("utf-8") if isinstance(s, str) else s)

    HAVE_ORJSON = True
    logger.info("[OPT] orjson tersedia — JSON akselerasi aktif")
except Exception:
    HAVE_ORJSON = False

    def json_dumps(obj: Any) -> str:
        return _stdlib_json.dumps(obj, default=str, ensure_ascii=False)

    def json_loads(s: Any) -> Any:
        if isinstance(s, (bytes, bytearray)):
            s = s.decode("utf-8", errors="replace")
        return _stdlib_json.loads(s)

    logger.info("[OPT] orjson tidak tersedia — pakai json stdlib (fallback)")

# Expose json alias agar kode lama yang `import json` tetap kompatibel (di sini
# kita gunakan json_dumps/json_loads, tetapi tetap simpan modul stdlib).
json = _stdlib_json

# --- uvloop (optional, faster asyncio) ---
HAVE_UVLOOP = False
try:
    import uvloop  # type: ignore

    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    HAVE_UVLOOP = True
    logger.info("[OPT] uvloop aktif — event loop akselerasi")
except Exception:
    logger.info("[OPT] uvloop tidak tersedia — pakai asyncio default (fallback)")

# --- rapidfuzz (optional, fuzzy search) ---
HAVE_RAPIDFUZZ = False
try:
    from rapidfuzz import fuzz as _rf_fuzz  # type: ignore

    HAVE_RAPIDFUZZ = True

    def fuzzy_score(a: str, b: str) -> float:
        try:
            return float(_rf_fuzz.token_set_ratio(a or "", b or ""))
        except Exception:
            return 0.0

    logger.info("[OPT] rapidfuzz tersedia — fuzzy search akselerasi")
except Exception:
    def fuzzy_score(a: str, b: str) -> float:
        try:
            return difflib.SequenceMatcher(None, (a or "").lower(), (b or "").lower()).ratio() * 100.0
        except Exception:
            return 0.0

    logger.info("[OPT] rapidfuzz tidak tersedia — pakai difflib (fallback)")

# --- psutil (optional, system monitor) ---
try:
    import psutil  # type: ignore
except Exception:
    logger.warning("[OPT] psutil tidak terpasang — monitor RAM/CPU dinonaktifkan secara aman")
    psutil = None  # type: ignore

# --- cachetools (optional, LRU cache helper) ---
HAVE_CACHETOOLS = False
try:
    from cachetools import TTLCache, LRUCache  # type: ignore

    HAVE_CACHETOOLS = True
except Exception:
    # Fallback LRU/TTL ringan
    class LRUCache:  # type: ignore
        def __init__(self, maxsize: int = 128):
            self.maxsize = maxsize
            self._d: "OrderedDict[Any, Any]" = OrderedDict()

        def __contains__(self, k):
            return k in self._d

        def __getitem__(self, k):
            v = self._d[k]
            self._d.move_to_end(k)
            return v

        def get(self, k, default=None):
            try:
                return self.__getitem__(k)
            except KeyError:
                return default

        def __setitem__(self, k, v):
            if k in self._d:
                self._d.move_to_end(k)
            self._d[k] = v
            while len(self._d) > self.maxsize:
                self._d.popitem(last=False)

        def pop(self, k, *a):
            return self._d.pop(k, *a)

        def __len__(self):
            return len(self._d)

        def clear(self):
            self._d.clear()

    class TTLCache(LRUCache):  # type: ignore
        def __init__(self, maxsize: int = 128, ttl: int = 60):
            super().__init__(maxsize)
            self.ttl = ttl
            self._exp: Dict[Any, float] = {}

        def __getitem__(self, k):
            if k in self._exp and self._exp[k] < time.time():
                self.pop(k, None)
                self._exp.pop(k, None)
                raise KeyError(k)
            return super().__getitem__(k)

        def __setitem__(self, k, v):
            super().__setitem__(k, v)
            self._exp[k] = time.time() + self.ttl

        def pop(self, k, *a):
            self._exp.pop(k, None)
            return super().pop(k, *a)

# --- aiofiles (optional, async file IO) ---
HAVE_AIOFILES = False
try:
    import aiofiles  # type: ignore

    HAVE_AIOFILES = True
except Exception:
    aiofiles = None  # type: ignore
    logger.info("[OPT] aiofiles tidak tersedia — file IO pakai run_in_executor (fallback)")

# --- aiohttp (semi-optional but strongly recommended for new prefetch engine) ---
HAVE_AIOHTTP = False
try:
    import aiohttp  # type: ignore

    HAVE_AIOHTTP = True
except Exception:
    aiohttp = None  # type: ignore
    logger.warning("[OPT] aiohttp tidak tersedia — fallback ke urllib (lebih lambat)")

# --- aiosqlite (optional, async sqlite). Fallback ke sqlite3 di executor ---
HAVE_AIOSQLITE = False
try:
    import aiosqlite  # type: ignore

    HAVE_AIOSQLITE = True
except Exception:
    aiosqlite = None  # type: ignore
    logger.warning("[OPT] aiosqlite tidak tersedia — pakai sqlite3 via thread executor (fallback)")

# --- Hard required: fastapi, yt_dlp, ytmusicapi ---
try:
    import yt_dlp  # type: ignore
except ImportError:
    logger.error("yt_dlp tidak ditemukan. Jalankan: pip install yt-dlp")
    sys.exit(1)

try:
    from ytmusicapi import YTMusic  # type: ignore
except ImportError:
    logger.error("ytmusicapi tidak ditemukan. Jalankan: pip install ytmusicapi")
    sys.exit(1)

try:
    from fastapi import (
        FastAPI,
        WebSocket,
        WebSocketDisconnect,
        Query,
        HTTPException,
        Request,
        Header,
    )
    from fastapi.middleware.cors import CORSMiddleware
    from fastapi.middleware.gzip import GZipMiddleware
    from fastapi.responses import JSONResponse, StreamingResponse, FileResponse, Response
    import uvicorn  # type: ignore
except ImportError:
    logger.error(
        "fastapi/uvicorn tidak ditemukan. Jalankan: pip install fastapi uvicorn"
    )
    sys.exit(1)


# ─────────────────────────────────────────────────────────────────────────────
# 3. CONFIG & CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────

PORT = int(os.environ.get("PORT", "8000"))

# Directories
CACHE_DIR = "audio_cache"
DOWNLOAD_DIR = "downloads"
CHUNK_CACHE_DIR = "chunk_cache"   # first-chunk audio cache
THUMB_CACHE_DIR = "thumb_cache"
DB_FILE = "music_backend.db"
TEMP_DIR = "tmp"
for _d in (CACHE_DIR, DOWNLOAD_DIR, CHUNK_CACHE_DIR, THUMB_CACHE_DIR, TEMP_DIR):
    os.makedirs(_d, exist_ok=True)

# Legacy JSON files (untuk migrasi & kompatibilitas)
QUEUE_STATE_FILE = "queue_state.json"
PLAYLIST_FILE = "playlist.json"
FAVORITES_FILE = "favorites.json"
HISTORY_FILE = "recently_played.json"
SEARCH_HISTORY_FILE = "search_history.json"
PLAYBACK_STATE_FILE = "playback_state.json"

# Cache & limits — tuned untuk Android low-end
STREAM_CACHE_TTL = 3600
MAX_STREAM_CACHE = 256
MAX_HISTORY = 100
MAX_FAVORITES = 500
MAX_SEARCH_HISTORY = 200
MAX_QUEUE = 300

PREFETCH_DEPTH = int(os.environ.get("PREFETCH_DEPTH", "3"))   # next, next+1, next+2
PREFETCH_TTL = 1800
ADAPTIVE_PREFETCH_THRESHOLD = 0.7  # warmup next saat 70% playback
FIRST_CHUNK_BYTES = 256 * 1024     # 256 KB pre-buffer awal audio
CHUNK_CACHE_MAX_ITEMS = 64
RECOMMENDATION_CACHE_TTL = 1800
THUMB_CACHE_TTL = 7 * 24 * 3600

# Disk quota (MB) — soft cap. 0 = unlimited.
MAX_CACHE_SIZE_MB = int(os.environ.get("MAX_CACHE_SIZE_MB", "1024"))   # audio_cache + chunk_cache
MAX_DOWNLOAD_SIZE_MB = int(os.environ.get("MAX_DOWNLOAD_SIZE_MB", "0"))  # 0 = unlimited
LOW_STORAGE_THRESHOLD_MB = 100  # warning kalau free storage < 100 MB

# Worker pool: auto-tune dari RAM Android
def _auto_workers() -> int:
    try:
        if psutil:
            mem = psutil.virtual_memory().total / (1024 * 1024)  # MB
            if mem < 1500:
                return 2
            if mem < 3000:
                return 3
            if mem < 6000:
                return 4
        cpu = os.cpu_count() or 2
        return max(2, min(6, cpu))
    except Exception:
        return 3

MAX_WORKERS = int(os.environ.get("MAX_WORKERS", str(_auto_workers())))
MAX_CONCURRENT_DOWNLOADS = int(os.environ.get("MAX_CONCURRENT_DOWNLOADS", "2"))
MAX_CONCURRENT_EXTRACTIONS = int(os.environ.get("MAX_CONCURRENT_EXTRACTIONS", str(MAX_WORKERS)))

# Audio quality map
QUALITY_MAP = {
    "low":    "worstaudio[ext=m4a]/worstaudio/worst",
    "medium": "bestaudio[ext=m4a]/bestaudio[abr<=128]/bestaudio",
    "high":   "bestaudio[ext=m4a]/bestaudio/best",
    "auto":   "bestaudio[ext=m4a]/bestaudio/best",
}

START_TIME = time.time()

# ─────────────────────────────────────────────────────────────────────────────
# 3b. AUTH & RATE LIMIT CONFIG (v7.0)
# ─────────────────────────────────────────────────────────────────────────────

# Auth mode: set AUTH_REQUIRED=1 di env untuk aktifkan JWT wajib
AUTH_REQUIRED = os.environ.get("AUTH_REQUIRED", "0") == "1"
# JWT secret — GANTI di production! Default acak per-session kalau env tidak diset
JWT_SECRET = os.environ.get("JWT_SECRET", secrets.token_hex(32))
JWT_ACCESS_TTL = int(os.environ.get("JWT_ACCESS_TTL", "900"))    # 15 menit
JWT_REFRESH_TTL = int(os.environ.get("JWT_REFRESH_TTL", "604800"))  # 7 hari
JWT_ALGORITHM = "HS256"

# Default user (single-user mode kalau AUTH_REQUIRED aktif)
DEFAULT_USER = os.environ.get("AUTH_USER", "admin")
DEFAULT_PASS = os.environ.get("AUTH_PASS", "music2024")

# Rate limits per endpoint category (request/detik, burst max)
RATE_LIMITS: Dict[str, Tuple[float, int, int]] = {
    # category: (rate/sec, burst_max, block_seconds)
    "search":   (2.0, 10, 30),
    "stream":   (3.0, 8, 60),
    "download": (1.0, 4, 120),
    "auth":     (0.5, 5, 300),
    "ws":       (2.0, 20, 30),
    "thumb":    (10.0, 30, 10),
    "default":  (10.0, 50, 10),
}

# Thumb proxy config
THUMB_PROXY_URL_TEMPLATE = "https://i.ytimg.com/vi/{videoId}/maxresdefault.jpg"
THUMB_FALLBACK_URL_TEMPLATE = "https://i.ytimg.com/vi/{videoId}/hqdefault.jpg"
THUMB_RAM_MAX = 256  # items in RAM cache


# ─────────────────────────────────────────────────────────────────────────────
# 4. METRICS & MONITORING
# ─────────────────────────────────────────────────────────────────────────────

class Metrics:
    """Counter & gauge ringan thread-safe (atomic enough untuk Termux)."""

    def __init__(self):
        self._lock = threading.Lock()
        self.total_requests = 0
        self.slow_requests = 0
        self.cache_hit = 0
        self.cache_miss = 0
        self.stream_failures = 0
        self.ytdlp_failures = 0
        self.downloads_active = 0
        self.downloads_completed = 0
        self.downloads_failed = 0
        self.ws_messages_sent = 0
        self.ws_messages_recv = 0
        self.prefetch_done = 0
        self.prefetch_skipped = 0
        self.last_request_latencies: deque = deque(maxlen=200)
        self.ram_peak_mb = 0.0
        self.cpu_peak = 0.0

    def inc(self, name: str, by: int = 1):
        with self._lock:
            setattr(self, name, getattr(self, name, 0) + by)

    def record_latency(self, ms: float):
        with self._lock:
            self.last_request_latencies.append(ms)
            if ms > 1500:
                self.slow_requests += 1

    def snapshot(self) -> Dict:
        with self._lock:
            lats = list(self.last_request_latencies)
            avg_lat = sum(lats) / len(lats) if lats else 0.0
            p95 = sorted(lats)[int(len(lats) * 0.95) - 1] if len(lats) >= 20 else (max(lats) if lats else 0.0)
            cache_total = self.cache_hit + self.cache_miss
            hit_ratio = (self.cache_hit / cache_total) if cache_total else 0.0
            return {
                "total_requests": self.total_requests,
                "slow_requests": self.slow_requests,
                "avg_latency_ms": round(avg_lat, 2),
                "p95_latency_ms": round(p95, 2),
                "cache_hit": self.cache_hit,
                "cache_miss": self.cache_miss,
                "cache_hit_ratio": round(hit_ratio, 3),
                "stream_failures": self.stream_failures,
                "ytdlp_failures": self.ytdlp_failures,
                "downloads_active": self.downloads_active,
                "downloads_completed": self.downloads_completed,
                "downloads_failed": self.downloads_failed,
                "ws_messages_sent": self.ws_messages_sent,
                "ws_messages_recv": self.ws_messages_recv,
                "prefetch_done": self.prefetch_done,
                "prefetch_skipped": self.prefetch_skipped,
                "ram_peak_mb": round(self.ram_peak_mb, 2),
                "cpu_peak": round(self.cpu_peak, 2),
            }


metrics = Metrics()


# ─────────────────────────────────────────────────────────────────────────────
# 5. YTMUSIC CLIENT
# ─────────────────────────────────────────────────────────────────────────────

yt_lock = threading.Lock()
try:
    yt = YTMusic()
    logger.info("YTMusic client berhasil diinisialisasi")
except Exception as e:
    logger.error(f"Gagal menginisialisasi YTMusic: {e}")
    _crash_dump("YTMusic init failed", e)
    sys.exit(1)


# ─────────────────────────────────────────────────────────────────────────────
# 6. SHARED THREAD POOL EXECUTOR (Parallel extraction)
# ─────────────────────────────────────────────────────────────────────────────

EXTRACTOR_POOL = ThreadPoolExecutor(
    max_workers=MAX_CONCURRENT_EXTRACTIONS, thread_name_prefix="extractor"
)
IO_POOL = ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix="io")
DOWNLOAD_POOL = ThreadPoolExecutor(
    max_workers=MAX_CONCURRENT_DOWNLOADS, thread_name_prefix="downloader"
)

# Request deduplication: jika dua client minta stream yang sama bersamaan,
# share future yang sama supaya hanya satu extraction yt-dlp.
_inflight_extractions: Dict[str, "asyncio.Future"] = {}
_inflight_lock = asyncio.Lock()


# ─────────────────────────────────────────────────────────────────────────────
# 7. ASYNC SQLITE (Metadata, Downloads, Analytics, Cache index)
# ─────────────────────────────────────────────────────────────────────────────

_DB_LOCK = threading.Lock()

# Sinkronisasi: kalau aiosqlite tersedia, kita pakai. Kalau tidak, pakai
# sqlite3 di IO_POOL. Untuk konsistensi, semua akses via helper db_exec/db_fetch.

class Database:
    def __init__(self, path: str):
        self.path = path
        self._init_sync()

    def _init_sync(self):
        with _DB_LOCK:
            conn = sqlite3.connect(self.path, timeout=30, check_same_thread=False)
            try:
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("PRAGMA synchronous=NORMAL")
                conn.execute("PRAGMA temp_store=MEMORY")
                conn.execute("PRAGMA cache_size=-4000")  # ~4MB
                # Tables
                conn.executescript(
                    """
                    CREATE TABLE IF NOT EXISTS track_meta (
                        video_id TEXT PRIMARY KEY,
                        title TEXT, artist TEXT, album TEXT,
                        duration INTEGER, bitrate INTEGER,
                        thumbnail TEXT, year TEXT, browse_id TEXT,
                        explicit INTEGER DEFAULT 0,
                        updated_at REAL
                    );
                    CREATE INDEX IF NOT EXISTS idx_meta_title ON track_meta(title);
                    CREATE INDEX IF NOT EXISTS idx_meta_artist ON track_meta(artist);

                    CREATE TABLE IF NOT EXISTS downloads (
                        video_id TEXT PRIMARY KEY,
                        title TEXT, artist TEXT,
                        filename TEXT, filepath TEXT,
                        status TEXT,             -- queued|downloading|paused|completed|failed|cancelled
                        progress REAL DEFAULT 0,
                        total_bytes INTEGER DEFAULT 0,
                        downloaded_bytes INTEGER DEFAULT 0,
                        speed_bps REAL DEFAULT 0,
                        eta_seconds REAL DEFAULT 0,
                        error TEXT,
                        priority INTEGER DEFAULT 0,
                        started_at REAL, finished_at REAL,
                        updated_at REAL
                    );
                    CREATE INDEX IF NOT EXISTS idx_dl_status ON downloads(status);

                    CREATE TABLE IF NOT EXISTS cache_index (
                        key TEXT PRIMARY KEY,
                        video_id TEXT,
                        kind TEXT,               -- audio|chunk|thumb
                        path TEXT,
                        size_bytes INTEGER,
                        last_access REAL,
                        created_at REAL
                    );
                    CREATE INDEX IF NOT EXISTS idx_cache_last ON cache_index(last_access);

                    CREATE TABLE IF NOT EXISTS analytics (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        event TEXT, video_id TEXT,
                        value REAL, ts REAL
                    );
                    CREATE INDEX IF NOT EXISTS idx_an_ts ON analytics(ts);

                    CREATE TABLE IF NOT EXISTS recommendation_seed (
                        video_id TEXT PRIMARY KEY,
                        score REAL,
                        last_seen REAL
                    );

                    CREATE TABLE IF NOT EXISTS search_history (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        query TEXT, type TEXT, ts REAL
                    );
                    CREATE INDEX IF NOT EXISTS idx_sh_ts ON search_history(ts);

                    CREATE TABLE IF NOT EXISTS recently_played (
                        video_id TEXT PRIMARY KEY,
                        title TEXT, artist TEXT, thumbnail TEXT,
                        album TEXT, duration TEXT,
                        played_at REAL
                    );
                    CREATE INDEX IF NOT EXISTS idx_rp_ts ON recently_played(played_at);

                    CREATE TABLE IF NOT EXISTS favorites (
                        video_id TEXT PRIMARY KEY,
                        title TEXT, artist TEXT, thumbnail TEXT,
                        album TEXT, duration TEXT,
                        favorited_at REAL
                    );

                    CREATE TABLE IF NOT EXISTS playlist (
                        video_id TEXT PRIMARY KEY,
                        title TEXT, artist TEXT, thumbnail TEXT,
                        album TEXT, duration TEXT,
                        added_at REAL
                    );

                    CREATE TABLE IF NOT EXISTS kv (
                        k TEXT PRIMARY KEY,
                        v TEXT
                    );

                    CREATE TABLE IF NOT EXISTS auth_sessions (
                        session_id TEXT PRIMARY KEY,
                        username TEXT NOT NULL,
                        refresh_token TEXT UNIQUE,
                        device_info TEXT,
                        created_at REAL,
                        expires_at REAL,
                        last_used REAL
                    );
                    CREATE INDEX IF NOT EXISTS idx_sess_user ON auth_sessions(username);
                    CREATE INDEX IF NOT EXISTS idx_sess_exp ON auth_sessions(expires_at);

                    CREATE TABLE IF NOT EXISTS local_search_index (
                        video_id TEXT PRIMARY KEY,
                        title TEXT,
                        artist TEXT,
                        album TEXT,
                        duration TEXT,
                        thumbnail TEXT,
                        play_count INTEGER DEFAULT 0,
                        last_played REAL DEFAULT 0,
                        indexed_at REAL
                    );
                    CREATE INDEX IF NOT EXISTS idx_lsi_title ON local_search_index(title);
                    CREATE INDEX IF NOT EXISTS idx_lsi_artist ON local_search_index(artist);
                    CREATE INDEX IF NOT EXISTS idx_lsi_plays ON local_search_index(play_count);

                    -- v8.0: Global Event Store (up to 10000 events)
                    CREATE TABLE IF NOT EXISTS event_store (
                        event_id    INTEGER PRIMARY KEY AUTOINCREMENT,
                        state_version INTEGER NOT NULL,
                        event_type  TEXT NOT NULL,
                        payload     TEXT NOT NULL,
                        timestamp   REAL NOT NULL
                    );
                    CREATE INDEX IF NOT EXISTS idx_es_ts ON event_store(timestamp);
                    CREATE INDEX IF NOT EXISTS idx_es_sv ON event_store(state_version);

                    -- v8.0: Per-client ACK tracking
                    CREATE TABLE IF NOT EXISTS event_ack (
                        client_id   TEXT NOT NULL,
                        event_id    INTEGER NOT NULL,
                        acked_at    REAL NOT NULL,
                        PRIMARY KEY (client_id, event_id)
                    );
                    CREATE INDEX IF NOT EXISTS idx_ack_client ON event_ack(client_id);
                    """
                )
                conn.commit()
            finally:
                conn.close()

    def _execute_sync(self, sql: str, params: Tuple = ()):
        with _DB_LOCK:
            conn = sqlite3.connect(self.path, timeout=30, check_same_thread=False)
            try:
                cur = conn.execute(sql, params)
                conn.commit()
                return cur.lastrowid
            finally:
                conn.close()

    def _executemany_sync(self, sql: str, seq: List[Tuple]):
        with _DB_LOCK:
            conn = sqlite3.connect(self.path, timeout=30, check_same_thread=False)
            try:
                conn.executemany(sql, seq)
                conn.commit()
            finally:
                conn.close()

    def _fetch_sync(self, sql: str, params: Tuple = (), one: bool = False):
        with _DB_LOCK:
            conn = sqlite3.connect(self.path, timeout=30, check_same_thread=False)
            try:
                conn.row_factory = sqlite3.Row
                cur = conn.execute(sql, params)
                if one:
                    row = cur.fetchone()
                    return dict(row) if row else None
                return [dict(r) for r in cur.fetchall()]
            finally:
                conn.close()

    async def execute(self, sql: str, params: Tuple = ()):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(IO_POOL, self._execute_sync, sql, params)

    async def executemany(self, sql: str, seq: List[Tuple]):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(IO_POOL, self._executemany_sync, sql, seq)

    async def fetch_all(self, sql: str, params: Tuple = ()):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(IO_POOL, self._fetch_sync, sql, params, False)

    async def fetch_one(self, sql: str, params: Tuple = ()):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(IO_POOL, self._fetch_sync, sql, params, True)

    async def vacuum(self):
        try:
            await self.execute("VACUUM")
        except Exception as e:
            logger.warning(f"[DB] VACUUM gagal: {e}")


db = Database(DB_FILE)


# ─────────────────────────────────────────────────────────────────────────────
# 8. JSON STORE HELPERS (legacy compatibility — masih dipakai oleh sebagian fitur)
# ─────────────────────────────────────────────────────────────────────────────

_file_lock = threading.Lock()


def _load_json(path: str, default=None):
    if default is None:
        default = []
    try:
        with _file_lock:
            if os.path.exists(path):
                with open(path, "r", encoding="utf-8") as f:
                    return json_loads(f.read())
    except Exception as e:
        logger.warning(f"Gagal membaca JSON dari {path}: {e}")
    return default


def _save_json(path: str, data):
    try:
        with _file_lock:
            with open(path, "w", encoding="utf-8") as f:
                f.write(json_dumps(data))
    except Exception as e:
        logger.warning(f"Gagal menyimpan JSON ke {path}: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# 9. STREAM CACHE (RAM Pool, TTL, Disk Awareness)
# ─────────────────────────────────────────────────────────────────────────────

class StreamCache:
    """Thread-safe RAM URL pool dengan TTL, LRU eviction, plus deteksi file lokal."""

    def __init__(self, max_items: int = MAX_STREAM_CACHE, ttl: int = STREAM_CACHE_TTL):
        self._cache: "OrderedDict[str, Dict]" = OrderedDict()
        self._lock = threading.Lock()
        self._max = max_items
        self._ttl = ttl

    def get_local_path(self, video_id: str) -> Optional[str]:
        filename = f"{video_id}.m4a"
        filepath = os.path.join(CACHE_DIR, filename)
        try:
            if os.path.exists(filepath) and os.path.getsize(filepath) > 1024:
                return filepath
        except OSError:
            return None
        return None

    def get(self, key: str) -> Optional[str]:
        video_id = key.split(":")[0] if ":" in key else key
        local_file = self.get_local_path(video_id)
        if local_file:
            metrics.inc("cache_hit")
            cache_logger.info(f"[CACHE] HIT fisik: {local_file}")
            return f"http://localhost:{PORT}/stream/file/{video_id}"
        with self._lock:
            item = self._cache.get(key)
            if item and item["expires"] > time.time():
                self._cache.move_to_end(key)
                metrics.inc("cache_hit")
                cache_logger.info(f"[CACHE] HIT RAM: {key}")
                return item["url"]
            if item:
                self._cache.pop(key, None)
        metrics.inc("cache_miss")
        return None

    def set(self, key: str, url: str, ttl: Optional[int] = None):
        with self._lock:
            now = time.time()
            # Cleanup expired
            expired = [k for k, v in self._cache.items() if v["expires"] <= now]
            for k in expired:
                self._cache.pop(k, None)
            # LRU evict
            while len(self._cache) >= self._max:
                self._cache.popitem(last=False)
            self._cache[key] = {
                "url": url,
                "expires": now + (ttl or self._ttl),
                "created": now,
            }
            self._cache.move_to_end(key)
            cache_logger.info(f"[CACHE] STORE RAM: {key}")

    def delete(self, key: str):
        with self._lock:
            self._cache.pop(key, None)

    def cleanup(self):
        now = time.time()
        with self._lock:
            expired = [k for k, v in self._cache.items() if v["expires"] <= now]
            for k in expired:
                self._cache.pop(k, None)
        cache_logger.info(f"[CACHE] cleanup selesai; tersisa={len(self._cache)}")

    def size(self) -> int:
        with self._lock:
            return len(self._cache)

    def keys_snapshot(self) -> List[str]:
        with self._lock:
            return list(self._cache.keys())


stream_cache = StreamCache()

# Recommendation cache (TTL): seed_vid -> List[track]
recommendation_cache: "TTLCache" = TTLCache(maxsize=128, ttl=RECOMMENDATION_CACHE_TTL)
# Trending cache
trending_cache_box: Dict[str, Any] = {"ts": 0.0, "data": None}
# Search suggestion cache (TTL)
suggestion_cache: "TTLCache" = TTLCache(maxsize=256, ttl=600)
# Search result cache (TTL) — diperpanjang ke 900 detik agar search terasa instan
search_result_cache: "TTLCache" = TTLCache(maxsize=128, ttl=900)
# Bitrate metadata cache (small)
bitrate_cache: "LRUCache" = LRUCache(maxsize=512)
# Recommendation cache lock
_rec_lock = threading.Lock()


# ─────────────────────────────────────────────────────────────────────────────
# 9b. THUMBNAIL RAM CACHE (v7.0)
# ─────────────────────────────────────────────────────────────────────────────

class ThumbCache:
    """RAM + Disk thumbnail cache dengan TTL."""

    def __init__(self, max_ram: int = THUMB_RAM_MAX, ttl: int = THUMB_CACHE_TTL):
        self._ram: "OrderedDict[str, Tuple[bytes, str, float]]" = OrderedDict()
        self._lock = threading.Lock()
        self._max = max_ram
        self._ttl = ttl

    def get(self, video_id: str) -> Optional[Tuple[bytes, str]]:
        with self._lock:
            item = self._ram.get(video_id)
            if item:
                data, ct, exp = item
                if exp > time.time():
                    self._ram.move_to_end(video_id)
                    return data, ct
                else:
                    self._ram.pop(video_id, None)
        # disk fallback
        p = os.path.join(THUMB_CACHE_DIR, f"{video_id}.jpg")
        mp = os.path.join(THUMB_CACHE_DIR, f"{video_id}.meta")
        if os.path.exists(p) and os.path.exists(mp):
            try:
                with open(mp, "r") as f:
                    meta = json_loads(f.read())
                if meta.get("expires", 0) > time.time():
                    with open(p, "rb") as f:
                        data = f.read()
                    self.set_ram(video_id, data, meta.get("content_type", "image/jpeg"))
                    return data, meta.get("content_type", "image/jpeg")
            except Exception:
                pass
        return None

    def set_ram(self, video_id: str, data: bytes, content_type: str = "image/jpeg"):
        with self._lock:
            self._ram[video_id] = (data, content_type, time.time() + self._ttl)
            self._ram.move_to_end(video_id)
            while len(self._ram) > self._max:
                self._ram.popitem(last=False)

    def set(self, video_id: str, data: bytes, content_type: str = "image/jpeg"):
        self.set_ram(video_id, data, content_type)
        p = os.path.join(THUMB_CACHE_DIR, f"{video_id}.jpg")
        mp = os.path.join(THUMB_CACHE_DIR, f"{video_id}.meta")
        try:
            with open(p, "wb") as f:
                f.write(data)
            with open(mp, "w") as f:
                f.write(json_dumps({
                    "content_type": content_type,
                    "expires": time.time() + self._ttl,
                    "size": len(data),
                }))
        except Exception as e:
            cache_logger.warning(f"[THUMB] gagal disk write {video_id}: {e}")


thumb_cache = ThumbCache()


# ─────────────────────────────────────────────────────────────────────────────
# 9c. RATE LIMITER (v7.0) — Token Bucket per IP
# ─────────────────────────────────────────────────────────────────────────────

class TokenBucket:
    """Single-IP token bucket untuk rate limiting."""

    __slots__ = ("tokens", "last_refill", "blocked_until")

    def __init__(self, capacity: float):
        self.tokens: float = capacity
        self.last_refill: float = time.time()
        self.blocked_until: float = 0.0


class RateLimiter:
    """
    Per-IP token bucket rate limiter.
    Tidak memakai Redis atau library berat — murni asyncio + threading.Lock.
    """

    def __init__(self):
        self._buckets: Dict[str, Dict[str, TokenBucket]] = defaultdict(dict)
        self._lock = threading.Lock()

    def _get_or_create(self, ip: str, category: str) -> TokenBucket:
        with self._lock:
            if category not in self._buckets[ip]:
                rate, burst, _ = RATE_LIMITS.get(category, RATE_LIMITS["default"])
                self._buckets[ip][category] = TokenBucket(burst)
            return self._buckets[ip][category]

    def check(self, ip: str, category: str) -> Tuple[bool, float]:
        """
        Returns (allowed: bool, retry_after: float).
        Refill bucket berdasarkan waktu berlalu.
        """
        rate, burst, block_secs = RATE_LIMITS.get(category, RATE_LIMITS["default"])
        bucket = self._get_or_create(ip, category)
        now = time.time()

        with self._lock:
            # Apakah sedang di-block?
            if bucket.blocked_until > now:
                return False, bucket.blocked_until - now

            # Refill tokens
            elapsed = now - bucket.last_refill
            bucket.tokens = min(burst, bucket.tokens + elapsed * rate)
            bucket.last_refill = now

            if bucket.tokens >= 1.0:
                bucket.tokens -= 1.0
                return True, 0.0
            else:
                # Habis token → temp block
                bucket.blocked_until = now + block_secs
                return False, float(block_secs)

    def cleanup_stale(self):
        """Hapus IP yang sudah lama tidak aktif (jalankan periodik)."""
        cutoff = time.time() - 600
        with self._lock:
            stale = [ip for ip, cats in self._buckets.items()
                     if all(b.last_refill < cutoff for b in cats.values())]
            for ip in stale:
                del self._buckets[ip]


rate_limiter = RateLimiter()


def get_client_ip(request: "Request") -> str:
    """Ambil IP dari X-Forwarded-For atau langsung dari client."""
    xff = request.headers.get("x-forwarded-for")
    if xff:
        return xff.split(",")[0].strip()
    if request.client:
        return request.client.host or "unknown"
    return "unknown"


async def check_rate_limit(ip: str, category: str):
    """Raise HTTPException 429 jika rate limit terlampaui."""
    allowed, retry_after = rate_limiter.check(ip, category)
    if not allowed:
        raise HTTPException(
            status_code=429,
            detail=f"Rate limit terlampaui. Coba lagi dalam {retry_after:.0f} detik.",
            headers={"Retry-After": str(int(retry_after))},
        )


# ─────────────────────────────────────────────────────────────────────────────
# 9d. JWT AUTH SYSTEM (v7.0)
# ─────────────────────────────────────────────────────────────────────────────

class JWTAuth:
    """
    Lightweight JWT implementation tanpa dependency eksternal.
    Pure stdlib: hmac, hashlib, base64.
    Format: header.payload.signature (HS256)
    """

    @staticmethod
    def _b64encode(data: bytes) -> str:
        return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")

    @staticmethod
    def _b64decode(s: str) -> bytes:
        padding = 4 - len(s) % 4
        if padding != 4:
            s += "=" * padding
        return base64.urlsafe_b64decode(s)

    @classmethod
    def create_token(cls, payload: Dict, ttl: int = JWT_ACCESS_TTL) -> str:
        """Buat JWT access token."""
        header = cls._b64encode(json_dumps({"alg": JWT_ALGORITHM, "typ": "JWT"}).encode())
        now = int(time.time())
        full_payload = {**payload, "iat": now, "exp": now + ttl}
        body = cls._b64encode(json_dumps(full_payload).encode())
        signing_input = f"{header}.{body}"
        sig = hmac.new(
            JWT_SECRET.encode(), signing_input.encode(), hashlib.sha256
        ).digest()
        return f"{signing_input}.{cls._b64encode(sig)}"

    @classmethod
    def verify_token(cls, token: str) -> Optional[Dict]:
        """Verify dan decode JWT. Returns payload atau None jika invalid/expired."""
        try:
            parts = token.split(".")
            if len(parts) != 3:
                return None
            header_b64, body_b64, sig_b64 = parts
            signing_input = f"{header_b64}.{body_b64}"
            expected_sig = hmac.new(
                JWT_SECRET.encode(), signing_input.encode(), hashlib.sha256
            ).digest()
            actual_sig = cls._b64decode(sig_b64)
            if not hmac.compare_digest(expected_sig, actual_sig):
                return None
            payload = json_loads(cls._b64decode(body_b64))
            if payload.get("exp", 0) < time.time():
                return None
            return payload
        except Exception:
            return None

    @classmethod
    def create_refresh_token(cls) -> str:
        """Buat opaque refresh token (64-char hex)."""
        return secrets.token_hex(32)


jwt_auth = JWTAuth()


class SessionStore:
    """Multi-device session management dengan SQLite backend."""

    async def create_session(
        self, username: str, device_info: str = "unknown"
    ) -> Tuple[str, str]:
        """Buat session baru. Returns (access_token, refresh_token)."""
        session_id = uuid.uuid4().hex
        refresh_token = jwt_auth.create_refresh_token()
        access_token = jwt_auth.create_token({
            "sub": username,
            "sid": session_id,
            "type": "access",
        })
        now = time.time()
        await db.execute(
            "INSERT OR REPLACE INTO auth_sessions"
            "(session_id, username, refresh_token, device_info, created_at, expires_at, last_used) "
            "VALUES(?,?,?,?,?,?,?)",
            (session_id, username, refresh_token, device_info,
             now, now + JWT_REFRESH_TTL, now),
        )
        return access_token, refresh_token

    async def refresh_session(self, refresh_token: str) -> Optional[Tuple[str, str]]:
        """Rotate refresh token, kembalikan (new_access, new_refresh) atau None."""
        row = await db.fetch_one(
            "SELECT * FROM auth_sessions WHERE refresh_token=? AND expires_at>?",
            (refresh_token, time.time()),
        )
        if not row:
            return None
        username = row["username"]
        session_id = row["session_id"]
        new_refresh = jwt_auth.create_refresh_token()
        new_access = jwt_auth.create_token({
            "sub": username,
            "sid": session_id,
            "type": "access",
        })
        now = time.time()
        await db.execute(
            "UPDATE auth_sessions SET refresh_token=?, last_used=?, expires_at=? "
            "WHERE session_id=?",
            (new_refresh, now, now + JWT_REFRESH_TTL, session_id),
        )
        return new_access, new_refresh

    async def revoke_session(self, session_id: str):
        await db.execute("DELETE FROM auth_sessions WHERE session_id=?", (session_id,))

    async def revoke_all_sessions(self, username: str):
        await db.execute("DELETE FROM auth_sessions WHERE username=?", (username,))

    async def get_sessions(self, username: str) -> List[Dict]:
        return await db.fetch_all(
            "SELECT session_id, device_info, created_at, last_used "
            "FROM auth_sessions WHERE username=? AND expires_at>? ORDER BY last_used DESC",
            (username, time.time()),
        )

    async def cleanup_expired(self):
        await db.execute("DELETE FROM auth_sessions WHERE expires_at<?", (time.time(),))


session_store = SessionStore()


def _check_password(plain: str, stored: str) -> bool:
    """Verifikasi password. Mendukung plain text (dev) dan sha256 (prod)."""
    if stored.startswith("sha256:"):
        digest = hashlib.sha256(plain.encode()).hexdigest()
        return hmac.compare_digest(digest, stored[7:])
    return hmac.compare_digest(plain, stored)


async def _get_current_user(
    authorization: Optional[str] = Header(None),
) -> Optional[Dict]:
    """
    Ekstrak user dari Authorization header.
    Returns payload dict atau None jika tidak ada / invalid.
    """
    if not authorization:
        return None
    scheme, _, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or not token:
        return None
    return jwt_auth.verify_token(token)


async def _require_auth(
    authorization: Optional[str] = Header(None),
) -> Dict:
    """Dependency: wajib autentikasi. Raise 401 jika tidak ada token valid."""
    user = await _get_current_user(authorization)
    if user is None:
        raise HTTPException(
            status_code=401,
            detail="Autentikasi diperlukan",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return user


# ─────────────────────────────────────────────────────────────────────────────
# 10. FIRST-CHUNK AUDIO CACHE (RAM + Disk)
# ─────────────────────────────────────────────────────────────────────────────

class FirstChunkCache:
    """Cache 256KB pertama dari audio stream untuk instant playback start."""

    def __init__(self, max_items: int = CHUNK_CACHE_MAX_ITEMS):
        self._ram: "OrderedDict[str, bytes]" = OrderedDict()
        self._lock = threading.Lock()
        self._max = max_items

    def _disk_path(self, video_id: str) -> str:
        return os.path.join(CHUNK_CACHE_DIR, f"{video_id}.chunk")

    def get(self, video_id: str) -> Optional[bytes]:
        with self._lock:
            if video_id in self._ram:
                self._ram.move_to_end(video_id)
                return self._ram[video_id]
        # disk fallback
        p = self._disk_path(video_id)
        try:
            if os.path.exists(p):
                with open(p, "rb") as f:
                    data = f.read(FIRST_CHUNK_BYTES)
                with self._lock:
                    self._ram[video_id] = data
                    while len(self._ram) > self._max:
                        self._ram.popitem(last=False)
                return data
        except Exception:
            pass
        return None

    def set(self, video_id: str, data: bytes):
        if not data:
            return
        data = data[:FIRST_CHUNK_BYTES]
        with self._lock:
            self._ram[video_id] = data
            self._ram.move_to_end(video_id)
            while len(self._ram) > self._max:
                self._ram.popitem(last=False)
        try:
            p = self._disk_path(video_id)
            with open(p, "wb") as f:
                f.write(data)
        except Exception as e:
            cache_logger.warning(f"[CHUNK] gagal tulis chunk disk {video_id}: {e}")

    def delete(self, video_id: str):
        with self._lock:
            self._ram.pop(video_id, None)
        try:
            p = self._disk_path(video_id)
            if os.path.exists(p):
                os.remove(p)
        except Exception:
            pass


chunk_cache = FirstChunkCache()


# ─────────────────────────────────────────────────────────────────────────────
# 11. AIOHTTP SESSION POOL
# ─────────────────────────────────────────────────────────────────────────────

_aiohttp_session: Optional["aiohttp.ClientSession"] = None


async def get_http_session() -> Optional["aiohttp.ClientSession"]:
    global _aiohttp_session
    if not HAVE_AIOHTTP:
        return None
    if _aiohttp_session is None or _aiohttp_session.closed:
        connector = aiohttp.TCPConnector(
            limit=32,
            limit_per_host=8,
            keepalive_timeout=60,
            enable_cleanup_closed=True,
            ttl_dns_cache=300,
        )
        timeout = aiohttp.ClientTimeout(total=30, connect=10, sock_connect=10, sock_read=20)
        _aiohttp_session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={"User-Agent": "MusicBackend/4.0 (Termux)"},
        )
        logger.info("[HTTP] aiohttp session pool dibuka")
    return _aiohttp_session


async def http_get_json(url: str, timeout: int = 10) -> Optional[Any]:
    """Async JSON GET; auto fallback ke urllib jika aiohttp tidak ada."""
    sess = await get_http_session()
    if sess is not None:
        try:
            async with sess.get(url, timeout=aiohttp.ClientTimeout(total=timeout)) as r:
                if r.status != 200:
                    return None
                txt = await r.text()
                return json_loads(txt)
        except Exception as e:
            stream_logger.warning(f"[HTTP] aiohttp GET gagal {url}: {e}")
            return None
    # Fallback urllib
    def _sync():
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "MusicBackend/4.0"})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json_loads(r.read().decode("utf-8", errors="replace"))
        except Exception:
            return None
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(IO_POOL, _sync)


async def http_get_bytes(url: str, byte_range: Optional[Tuple[int, int]] = None, timeout: int = 15) -> Optional[bytes]:
    """Download partial bytes via Range (untuk first-chunk warmup)."""
    sess = await get_http_session()
    headers = {}
    if byte_range:
        headers["Range"] = f"bytes={byte_range[0]}-{byte_range[1]}"
    if sess is not None:
        try:
            async with sess.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=timeout)) as r:
                if r.status not in (200, 206):
                    return None
                return await r.read()
        except Exception as e:
            stream_logger.warning(f"[HTTP] range GET gagal: {e}")
            return None
    # fallback urllib
    def _sync():
        try:
            req = urllib.request.Request(url, headers={**headers, "User-Agent": "MusicBackend/4.0"})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.read()
        except Exception:
            return None
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(IO_POOL, _sync)


# ─────────────────────────────────────────────────────────────────────────────
# 12. YT-DLP HELPERS (extraction + parallel + dedupe)
# ─────────────────────────────────────────────────────────────────────────────

def get_ydl_opts(quality: str = "auto", output_path: Optional[str] = None,
                 progress_hook: Optional[Callable] = None) -> dict:
    fmt = QUALITY_MAP.get(quality, QUALITY_MAP["auto"])
    opts: Dict[str, Any] = {
        "format": fmt,
        "quiet": True,
        "no_warnings": True,
        "skip_download": output_path is None,
        "force_generic_extractor": False,
        "extract_flat": False,
        "socket_timeout": 15,
        "retries": 2,
        "nocheckcertificate": True,
        "geo_bypass": True,
        "concurrent_fragment_downloads": 1,
        "noprogress": True,
    }
    if output_path:
        opts["outtmpl"] = output_path
    if progress_hook:
        opts["progress_hooks"] = [progress_hook]
    return opts


def _sync_extract_stream(vid: str, quality: str = "auto") -> Optional[Dict]:
    """Run yt-dlp extract synchronously. Returns dict with url + metadata."""
    opts = get_ydl_opts(quality)
    sources = [
        f"https://music.youtube.com/watch?v={vid}",
        f"https://www.youtube.com/watch?v={vid}",
    ]
    for src in sources:
        try:
            with yt_dlp.YoutubeDL(opts) as ydl_inst:
                info = ydl_inst.extract_info(src, download=False)
                stream_url = info.get("url")
                if stream_url:
                    return {
                        "url": stream_url,
                        "duration": info.get("duration"),
                        "abr": info.get("abr"),
                        "filesize": info.get("filesize") or info.get("filesize_approx"),
                        "ext": info.get("ext"),
                        "title": info.get("title"),
                    }
        except Exception as e:
            stream_logger.warning(f"[YDL] gagal ekstrak {vid} dari {src}: {e}")
            metrics.inc("ytdlp_failures")
    return None


def extract_stream_url(vid: str, quality: str = "auto") -> Optional[str]:
    """Legacy sync wrapper (untuk kompatibilitas worker lama)."""
    info = _sync_extract_stream(vid, quality)
    return info["url"] if info else None


async def extract_stream_async(vid: str, quality: str = "auto") -> Optional[Dict]:
    """Async, deduped, paralel-safe extraction. Mengembalikan dict metadata + url."""
    key = f"{vid}:{quality}"
    loop = asyncio.get_running_loop()
    async with _inflight_lock:
        fut = _inflight_extractions.get(key)
        if fut is not None:
            # Sudah ada extraction berjalan untuk key ini — share saja
            return await asyncio.wait_for(asyncio.shield(fut), timeout=45)
        fut = loop.create_future()
        _inflight_extractions[key] = fut

    def _runner():
        try:
            result = _sync_extract_stream(vid, quality)
            loop.call_soon_threadsafe(fut.set_result, result)
        except Exception as e:
            loop.call_soon_threadsafe(fut.set_exception, e)

    EXTRACTOR_POOL.submit(_runner)
    try:
        return await asyncio.wait_for(fut, timeout=45)
    except Exception as e:
        stream_logger.warning(f"[YDL-ASYNC] {vid} gagal: {e}")
        return None
    finally:
        async with _inflight_lock:
            _inflight_extractions.pop(key, None)


def background_cache_audio(vid: str):
    """Mengunduh audio asinkron ke folder cache lokal demi menghemat kuota."""
    local_path = stream_cache.get_local_path(vid)
    if local_path:
        return
    temp_path = os.path.join(CACHE_DIR, f"{vid}.temp")
    final_path = os.path.join(CACHE_DIR, f"{vid}.m4a")
    if os.path.exists(temp_path):
        return
    cache_logger.info(f"[LOCAL_CACHE] unduh {vid} (cache offline)…")
    opts = get_ydl_opts("high", temp_path)
    url = f"https://music.youtube.com/watch?v={vid}"
    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            ydl.download([url])
        if os.path.exists(temp_path):
            os.rename(temp_path, final_path)
            cache_logger.info(f"[LOCAL_CACHE] sukses simpan {vid}")
            asyncio.run_coroutine_threadsafe(
                cache_index_register(vid, final_path, "audio"), _loop
            ) if _loop else None
    except Exception as e:
        cache_logger.warning(f"[LOCAL_CACHE] gagal {vid}: {e}")
        if os.path.exists(temp_path):
            with suppress(OSError):
                os.remove(temp_path)


async def cache_index_register(video_id: str, path: str, kind: str):
    try:
        size = os.path.getsize(path) if os.path.exists(path) else 0
        now = time.time()
        await db.execute(
            "INSERT OR REPLACE INTO cache_index(key, video_id, kind, path, size_bytes, last_access, created_at) "
            "VALUES(?,?,?,?,?,?,?)",
            (f"{kind}:{video_id}", video_id, kind, path, size, now, now),
        )
    except Exception as e:
        cache_logger.warning(f"[CACHE_IDX] gagal register {video_id}: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# 12b. STATE MANAGER & EVENT BUS (Spotify-style, v6.0)
# ─────────────────────────────────────────────────────────────────────────────

class StateManager:
    """
    Central state version counter — single source of truth versi integer.
    Setiap perubahan state (play/pause/seek/queue/favorite/download) wajib
    increment version ini sehingga frontend bisa detect desync.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._version: int = 0

    def get_version(self) -> int:
        with self._lock:
            return self._version

    def increment(self) -> int:
        with self._lock:
            self._version += 1
            return self._version

    def to_dict(self) -> Dict:
        return {"state_version": self.get_version()}


state_mgr = StateManager()


def increment_state_version() -> int:
    """Helper global — panggil ini setiap state berubah."""
    return state_mgr.increment()


# Event types yang dianggap "state-changing" — akan auto-increment state_version
# saat di-broadcast. Non-state events (heartbeat, processing, dll.) tidak increment.
STATE_CHANGE_EVENT_TYPES: frozenset = frozenset({
    # Playback
    "play", "pause", "seek", "playback_state", "playback_update",
    # Queue
    "queue_updated", "next_track", "prev_track", "queue_jumped",
    "queue_add", "queue_remove", "queue_reorder", "queue_clear",
    # Favorites
    "favorite_add", "favorite_remove",
    # History
    "history_add", "history_remove",
    # Queue settings
    "shuffle_changed", "repeat_changed", "autoplay_changed",
    # Downloads
    "download_status", "download_started", "download_completed", "download_failed",
    "download_start", "download_pause", "download_resume", "download_cancel",
    "download_complete",
    # Sleep timer
    "sleep_timer_fired",
    # Full snapshots
    "state_update", "initial_state",
    # Stream refresh (affects playback state)
    "stream_url_refreshed",
    # v7.0 new
    "radio_generated", "settings_update", "recommendation_update",
    "thumbnail_cached",
})


# ─────────────────────────────────────────────────────────────────────────────
# 12c. V8 SYNC ENGINE — Event Store, Replay, ACK, Resend, Mutation Queue
# ─────────────────────────────────────────────────────────────────────────────

EVENT_STORE_MAX = 10_000          # keep last 10k events in SQLite
CHECKPOINT_INTERVAL = 30.0        # seconds between checkpoint broadcasts
RESEND_INTERVAL = 5.0             # seconds between unacked-event resend sweeps
RESEND_MAX_RETRIES = 5            # drop after this many retries
RESEND_TIMEOUT = 30.0             # consider unacked after this many seconds
PLAYBACK_CLOCK_KEY = "pb_clock"   # kv store key for authoritative clock

# Global monotonic event-id counter (in-memory, SQLite AUTOINCREMENT is source of truth)
_event_id_lock = threading.Lock()

# Per-client pending unacked events: { cid: { event_id: {"event": dict, "retries": int, "sent_at": float} } }
_pending_acks: Dict[str, Dict[int, Dict]] = defaultdict(dict)
_pending_acks_lock: asyncio.Lock   # will be initialized in lifespan

# Global mutation queue — all state mutations serialized through here
_mutation_queue: "asyncio.Queue"  # initialized in lifespan

# Playback authoritative clock
_playback_clock: Dict[str, Any] = {
    "play_started_at": None,    # server timestamp when play started
    "paused_at": None,          # server timestamp when paused
    "seek_position": 0.0,       # position at last seek/play/pause
    "server_timestamp": 0.0,    # last update time
    "playing": False,
}
_pb_clock_lock = threading.Lock()


def _update_playback_clock(**kwargs):
    with _pb_clock_lock:
        _playback_clock.update(kwargs)
        _playback_clock["server_timestamp"] = time.time()


def get_authoritative_position() -> float:
    """Compute current playback position from server clock (not local timer)."""
    with _pb_clock_lock:
        c = _playback_clock
        if c["playing"] and c["play_started_at"] is not None:
            elapsed = time.time() - c["play_started_at"]
            return round(c["seek_position"] + elapsed, 3)
        return float(c["seek_position"])


class EventStore:
    """
    Persistent event store backed by SQLite.
    Stores last EVENT_STORE_MAX events; older events are pruned.
    Thread-safe via the shared Database executor.
    """

    async def persist(self, event_type: str, payload: Dict, state_version: int) -> int:
        """
        Insert event into SQLite; return assigned event_id.
        Prunes oldest events if over EVENT_STORE_MAX.
        """
        now = time.time()
        payload_str = json_dumps(payload)
        try:
            rowid = await db.execute(
                "INSERT INTO event_store(state_version, event_type, payload, timestamp) "
                "VALUES(?, ?, ?, ?)",
                (state_version, event_type, payload_str, now),
            )
            # Async prune (fire & forget)
            asyncio.create_task(self._prune())
            return rowid
        except Exception as e:
            logger.warning(f"[EVENT_STORE] persist fail: {e}")
            return -1

    async def _prune(self):
        """Keep only last EVENT_STORE_MAX events."""
        try:
            await db.execute(
                "DELETE FROM event_store WHERE event_id NOT IN "
                "(SELECT event_id FROM event_store ORDER BY event_id DESC LIMIT ?)",
                (EVENT_STORE_MAX,),
            )
        except Exception:
            pass

    async def get_events_after(self, from_event_id: int, limit: int = 1000) -> List[Dict]:
        """Return events with event_id > from_event_id, ordered ascending."""
        try:
            rows = await db.fetch_all(
                "SELECT event_id, state_version, event_type, payload, timestamp "
                "FROM event_store WHERE event_id > ? ORDER BY event_id ASC LIMIT ?",
                (from_event_id, limit),
            )
            result = []
            for r in rows:
                try:
                    payload = json_loads(r["payload"])
                except Exception:
                    payload = {}
                result.append({
                    "event_id": r["event_id"],
                    "state_version": r["state_version"],
                    "event_type": r["event_type"],
                    "server_timestamp": r["timestamp"],
                    **payload,
                })
            return result
        except Exception as e:
            logger.warning(f"[EVENT_STORE] get_events_after fail: {e}")
            return []

    async def get_latest_event_id(self) -> int:
        try:
            row = await db.fetch_one("SELECT MAX(event_id) as mid FROM event_store")
            return row["mid"] or 0 if row else 0
        except Exception:
            return 0


event_store = EventStore()


class EventBus:
    """
    Unified event bus — ALL state changes MUST go through emit().
    Responsibilities:
      1. Assign event_id + state_version + server_timestamp
      2. Persist to EventStore
      3. Broadcast to all WS clients (with event envelope)
      4. Track pending ACKs per client
    """

    async def emit(self, event_type: str, payload: Dict,
                   expected_state_version: Optional[int] = None) -> Dict:
        """
        Emit a state-changing event.
        Returns the enriched event dict (with event_id, state_version, server_timestamp).
        Raises HTTPException 409 on version mismatch (for mutation endpoints).
        """
        # Version validation for mutations
        if expected_state_version is not None:
            current_sv = state_mgr.get_version()
            if expected_state_version != current_sv:
                raise HTTPException(
                    status_code=409,
                    detail={
                        "error": "state_version_mismatch",
                        "expected": expected_state_version,
                        "latest_state_version": current_sv,
                    },
                )

        # Increment state version for state-changing events
        if event_type in STATE_CHANGE_EVENT_TYPES:
            sv = increment_state_version()
        else:
            sv = state_mgr.get_version()

        now = time.time()
        enriched = {
            **payload,
            "event_id": -1,            # will be updated after persist
            "state_version": sv,
            "server_timestamp": now,
            "type": event_type,
        }

        # Persist to SQLite
        event_id = await event_store.persist(event_type, enriched, sv)
        enriched["event_id"] = event_id

        # Broadcast via WSManager (which handles per-client delivery + ACK tracking)
        await ws_manager.broadcast_event(enriched)

        return enriched


event_bus = EventBus()


async def _mutation_queue_worker():
    """
    Single-consumer mutation worker.
    Serializes all state mutations to prevent race conditions.
    Each item: (coroutine_factory, future)
    """
    while True:
        try:
            coro_factory, fut = await _mutation_queue.get()
            try:
                result = await coro_factory()
                if not fut.done():
                    fut.set_result(result)
            except Exception as e:
                if not fut.done():
                    fut.set_exception(e)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.warning(f"[MUTATION_QUEUE] worker error: {e}")


async def enqueue_mutation(coro_factory) -> Any:
    """
    Enqueue a mutation coroutine for serialized execution.
    Returns the result of the coroutine.
    """
    loop = asyncio.get_running_loop()
    fut = loop.create_future()
    await _mutation_queue.put((coro_factory, fut))
    return await fut


# ─────────────────────────────────────────────────────────────────────────────
# 13. QUEUE MANAGER (Spotify-like, dengan undo + snapshot + autoplay)
# ─────────────────────────────────────────────────────────────────────────────

class QueueManager:
    def __init__(self):
        self._lock = threading.RLock()
        self.queue: List[Dict] = []
        self.current_index: int = -1
        self.shuffle: bool = False
        self.repeat: str = "none"
        self.autoplay: bool = True
        self._snapshots: deque = deque(maxlen=8)  # undo stack
        self._load()

    def _snapshot(self):
        try:
            with self._lock:
                self._snapshots.append(
                    {
                        "queue": list(self.queue),
                        "current_index": self.current_index,
                        "shuffle": self.shuffle,
                        "repeat": self.repeat,
                        "autoplay": self.autoplay,
                    }
                )
        except Exception:
            pass

    def undo(self) -> bool:
        with self._lock:
            if not self._snapshots:
                return False
            snap = self._snapshots.pop()
            self.queue = snap["queue"]
            self.current_index = snap["current_index"]
            self.shuffle = snap["shuffle"]
            self.repeat = snap["repeat"]
            self.autoplay = snap["autoplay"]
        self.save()
        return True

    def _load(self):
        try:
            if os.path.exists(QUEUE_STATE_FILE):
                with open(QUEUE_STATE_FILE, "r", encoding="utf-8") as f:
                    state = json_loads(f.read())
                self.queue = state.get("queue", [])
                self.current_index = state.get("current_index", -1)
                self.shuffle = state.get("shuffle", False)
                self.repeat = state.get("repeat", "none")
                self.autoplay = state.get("autoplay", True)
                logger.info(
                    f"[QUEUE] restore {len(self.queue)} item, idx={self.current_index}"
                )
        except Exception as e:
            logger.warning(f"[QUEUE] gagal restore: {e}")

    def save(self):
        try:
            with self._lock:
                state = {
                    "queue": self.queue,
                    "current_index": self.current_index,
                    "shuffle": self.shuffle,
                    "repeat": self.repeat,
                    "autoplay": self.autoplay,
                    "saved_at": time.time(),
                }
            with open(QUEUE_STATE_FILE, "w", encoding="utf-8") as f:
                f.write(json_dumps(state))
        except Exception as e:
            logger.warning(f"[QUEUE] gagal save: {e}")

    def add(self, track: Dict) -> int:
        self._snapshot()
        with self._lock:
            if track.get("videoId"):
                for idx, q in enumerate(self.queue):
                    if q.get("videoId") == track["videoId"]:
                        return idx
            if len(self.queue) >= MAX_QUEUE:
                self.queue.pop(0)
                if self.current_index > 0:
                    self.current_index -= 1
            self.queue.append(track)
            idx = len(self.queue) - 1
        self.save()
        return idx

    def add_next(self, track: Dict):
        self._snapshot()
        with self._lock:
            insert_at = self.current_index + 1
            self.queue.insert(insert_at, track)
        self.save()

    def remove(self, index: int) -> bool:
        self._snapshot()
        with self._lock:
            if 0 <= index < len(self.queue):
                self.queue.pop(index)
                if index < self.current_index:
                    self.current_index -= 1
                elif index == self.current_index:
                    self.current_index = min(self.current_index, len(self.queue) - 1)
                self.save()
                return True
        return False

    def clear(self):
        self._snapshot()
        with self._lock:
            self.queue = []
            self.current_index = -1
        self.save()

    def current(self) -> Optional[Dict]:
        with self._lock:
            if 0 <= self.current_index < len(self.queue):
                return self.queue[self.current_index]
        return None

    def next_track(self) -> Optional[Dict]:
        with self._lock:
            n = len(self.queue)
            if n == 0:
                return None
            if self.repeat == "one":
                return self.queue[self.current_index] if 0 <= self.current_index < n else None
            if self.shuffle:
                candidates = [i for i in range(n) if i != self.current_index]
                if not candidates:
                    return self.queue[self.current_index] if 0 <= self.current_index < n else None
                self.current_index = random.choice(candidates)
            else:
                next_idx = self.current_index + 1
                if next_idx >= n:
                    if self.repeat == "all":
                        next_idx = 0
                    else:
                        return None
                self.current_index = next_idx
            return self.queue[self.current_index]

    def prev_track(self) -> Optional[Dict]:
        with self._lock:
            n = len(self.queue)
            if n == 0:
                return None
            prev_idx = self.current_index - 1
            if prev_idx < 0:
                prev_idx = n - 1 if self.repeat == "all" else 0
            self.current_index = prev_idx
            return self.queue[self.current_index]

    def set_current(self, index: int) -> Optional[Dict]:
        with self._lock:
            if 0 <= index < len(self.queue):
                self.current_index = index
                self.save()
                return self.queue[index]
        return None

    def get_state(self) -> Dict:
        with self._lock:
            return {
                "queue": list(self.queue),
                "current_index": self.current_index,
                "shuffle": self.shuffle,
                "repeat": self.repeat,
                "autoplay": self.autoplay,
                "size": len(self.queue),
            }

    def peek_next(self) -> Optional[Dict]:
        with self._lock:
            n = len(self.queue)
            if n == 0:
                return None
            if self.shuffle:
                candidates = [i for i in range(n) if i != self.current_index]
                return self.queue[random.choice(candidates)] if candidates else None
            next_idx = self.current_index + 1
            if next_idx >= n:
                return self.queue[0] if self.repeat == "all" else None
            return self.queue[next_idx]

    def peek_upcoming(self, depth: int) -> List[Dict]:
        """Ambil sampai `depth` track berikutnya untuk prefetch agresif."""
        with self._lock:
            n = len(self.queue)
            if n == 0 or depth <= 0:
                return []
            if self.shuffle:
                pool = [self.queue[i] for i in range(n) if i != self.current_index]
                random.shuffle(pool)
                return pool[:depth]
            out: List[Dict] = []
            i = self.current_index
            for _ in range(depth):
                i += 1
                if i >= n:
                    if self.repeat == "all":
                        i = 0
                    else:
                        break
                out.append(self.queue[i])
                if len(out) >= depth:
                    break
            return out

    def reorder(self, from_idx: int, to_idx: int) -> bool:
        self._snapshot()
        with self._lock:
            n = len(self.queue)
            if not (0 <= from_idx < n) or not (0 <= to_idx < n):
                return False
            item = self.queue.pop(from_idx)
            self.queue.insert(to_idx, item)
            # adjust current_index
            if from_idx == self.current_index:
                self.current_index = to_idx
            elif from_idx < self.current_index <= to_idx:
                self.current_index -= 1
            elif to_idx <= self.current_index < from_idx:
                self.current_index += 1
            self.save()
            return True


queue_mgr = QueueManager()

# ─────────────────────────────────────────────────────────────────────────────
# 13b. QUEUE ASYNC LOCK (mencegah race condition saat banyak user aktif)
# ─────────────────────────────────────────────────────────────────────────────
# Semua operasi modifikasi queue dari HTTP endpoint wajib acquire lock ini.
# QueueManager._lock (threading.RLock) tetap dipakai untuk thread-safety internal.
# queue_async_lock (asyncio.Lock) dipakai di level endpoint async supaya tidak
# ada dua coroutine memodifikasi queue bersamaan.
queue_async_lock: asyncio.Lock  # akan diinisialisasi di lifespan


# ─────────────────────────────────────────────────────────────────────────────
# 14. PLAYBACK STATE
# ─────────────────────────────────────────────────────────────────────────────

class PlaybackState:
    def __init__(self):
        self._lock = threading.Lock()
        self.playing: bool = False
        self.current_video_id: Optional[str] = None
        self.position: float = 0.0
        self.duration: float = 0.0
        self.updated_at: float = time.time()
        self.sleep_timer_end: Optional[float] = None
        self._last_adaptive_trigger: float = 0.0  # untuk debounce adaptive prefetch
        self._load_state()

    def _load_state(self):
        try:
            if os.path.exists(PLAYBACK_STATE_FILE):
                with open(PLAYBACK_STATE_FILE, "r", encoding="utf-8") as f:
                    state = json_loads(f.read())
                self.playing = False
                self.current_video_id = state.get("current_video_id")
                self.position = state.get("position", 0.0)
                self.duration = state.get("duration", 0.0)
                logger.info(
                    f"[STATE_RECOVERY] pulih video={self.current_video_id} pos={self.position}"
                )
        except Exception as e:
            logger.warning(f"[STATE_RECOVERY] gagal: {e}")

    def save_state(self):
        try:
            with self._lock:
                state = {
                    "current_video_id": self.current_video_id,
                    "position": self.position,
                    "duration": self.duration,
                    "saved_at": time.time(),
                }
            with open(PLAYBACK_STATE_FILE, "w", encoding="utf-8") as f:
                f.write(json_dumps(state))
        except Exception as e:
            logger.warning(f"[STATE_RECOVERY] gagal save: {e}")

    def update(self, **kwargs):
        with self._lock:
            for k, v in kwargs.items():
                setattr(self, k, v)
            self.updated_at = time.time()
        self.save_state()

    def to_dict(self) -> Dict:
        with self._lock:
            return {
                "playing": self.playing,
                "current_video_id": self.current_video_id,
                "position": self.position,
                "duration": self.duration,
                "updated_at": self.updated_at,
                "sleep_timer_end": self.sleep_timer_end,
            }


playback = PlaybackState()


# ─────────────────────────────────────────────────────────────────────────────
# 15. WEBSOCKET MANAGER PRO (Reconnect token, heartbeat, broadcast batching)
# ─────────────────────────────────────────────────────────────────────────────

class WSManager:
    def __init__(self):
        self._clients: Dict[str, WebSocket] = {}
        self._client_meta: Dict[str, Dict] = {}
        # session_id -> cid mapping for resume
        self._sessions: Dict[str, str] = {}   # session_id -> cid
        self._lock = asyncio.Lock()
        self._broadcast_queue: "asyncio.Queue" = asyncio.Queue(maxsize=512)
        self._broadcaster_task: Optional[asyncio.Task] = None
        # Connection rate limit (anti-DDoS)
        self._conn_rate: Dict[str, deque] = defaultdict(lambda: deque(maxlen=20))

    async def start(self):
        if self._broadcaster_task is None or self._broadcaster_task.done():
            self._broadcaster_task = asyncio.create_task(self._broadcaster_loop())

    async def stop(self):
        if self._broadcaster_task:
            self._broadcaster_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._broadcaster_task

    async def _broadcaster_loop(self):
        """Aggregator: batching broadcast supaya nggak spam tiap event kecil."""
        while True:
            try:
                batch = [await self._broadcast_queue.get()]
                # collect up to 8 messages within 20ms
                deadline = time.time() + 0.02
                while len(batch) < 8 and time.time() < deadline:
                    try:
                        batch.append(self._broadcast_queue.get_nowait())
                    except asyncio.QueueEmpty:
                        await asyncio.sleep(0.005)
                await self._do_broadcast(batch)
            except asyncio.CancelledError:
                break
            except Exception as e:
                ws_logger.warning(f"[WS] broadcaster error: {e}")

    async def _do_broadcast(self, events: List[Dict]):
        # Auto-inject state_version ke setiap event yang belum punya (backward compat)
        current_sv = state_mgr.get_version()
        enriched = []
        for e in events:
            if "state_version" not in e:
                enriched.append({**e, "state_version": current_sv})
            else:
                enriched.append(e)
        # Kalau hanya 1 event, kirim tunggal supaya backward compatible
        payload = enriched[0] if len(enriched) == 1 else {"type": "batch", "events": enriched}
        msg = json_dumps(payload)
        async with self._lock:
            clients = list(self._clients.items())
        dead: List[str] = []
        for cid, ws in clients:
            try:
                await ws.send_text(msg)
                metrics.inc("ws_messages_sent")
            except Exception:
                dead.append(cid)
        if dead:
            async with self._lock:
                for cid in dead:
                    self._clients.pop(cid, None)
                    self._client_meta.pop(cid, None)

    async def _send_to_client(self, cid: str, event: Dict) -> bool:
        """Send a single event to a specific client. Returns True on success."""
        async with self._lock:
            ws = self._clients.get(cid)
        if ws is None:
            return False
        try:
            await ws.send_text(json_dumps(event))
            metrics.inc("ws_messages_sent")
            return True
        except Exception:
            async with self._lock:
                self._clients.pop(cid, None)
                self._client_meta.pop(cid, None)
            return False

    async def connect(self, ws: WebSocket, peer: str = "unknown",
                      session_id: Optional[str] = None) -> str:
        # rate limit per-IP
        rq = self._conn_rate[peer]
        now = time.time()
        rq.append(now)
        recent = sum(1 for t in rq if now - t < 10)
        if recent > 15:
            await ws.close(code=1013)
            ws_logger.warning(f"[WS] rate-limited {peer}")
            raise WebSocketDisconnect(1013)
        await ws.accept()
        cid = uuid.uuid4().hex
        async with self._lock:
            self._clients[cid] = ws
            self._client_meta[cid] = {
                "connected_at": now,
                "peer": peer,
                "session_id": session_id or cid,
                "last_event_id": 0,
            }
            if session_id:
                self._sessions[session_id] = cid
        ws_logger.info(f"[WS] connect cid={cid[:8]} peer={peer} total={len(self._clients)}")
        return cid

    async def disconnect_by_id(self, cid: str):
        async with self._lock:
            meta = self._client_meta.pop(cid, {})
            self._clients.pop(cid, None)
            # Keep session mapping for resume (don't delete from _sessions)
        ws_logger.info(f"[WS] disconnect cid={cid[:8]} total={len(self._clients)}")

    async def disconnect(self, ws: WebSocket):
        async with self._lock:
            dead = [cid for cid, w in self._clients.items() if w is ws]
            for cid in dead:
                self._clients.pop(cid, None)
                self._client_meta.pop(cid, None)

    async def resume_session(self, ws: WebSocket, session_id: str,
                              last_event_id: int, peer: str = "unknown") -> Tuple[str, List[Dict]]:
        """
        Resume an existing session. Returns (new_cid, missed_events).
        Missed events are events after last_event_id.
        """
        # Create new connection slot
        cid = await self.connect(ws, peer, session_id=session_id)
        # Fetch missed events
        missed = await event_store.get_events_after(last_event_id, limit=500)
        # Update client last_event_id
        async with self._lock:
            if cid in self._client_meta:
                self._client_meta[cid]["last_event_id"] = last_event_id
        return cid, missed

    async def broadcast(self, event: Dict):
        # v6.0: auto-increment state_version untuk state-changing events
        if event.get("type") in STATE_CHANGE_EVENT_TYPES and "state_version" not in event:
            sv = increment_state_version()
            event = {**event, "state_version": sv}
        try:
            self._broadcast_queue.put_nowait(event)
        except asyncio.QueueFull:
            # Drop oldest, push newest (anti backpressure)
            try:
                self._broadcast_queue.get_nowait()
                self._broadcast_queue.put_nowait(event)
            except Exception:
                pass

    async def broadcast_event(self, event: Dict):
        """
        v8.0: broadcast enriched event (with event_id) and track pending ACKs.
        For state-changing events, register in pending_acks for every client.
        """
        event_id = event.get("event_id", -1)
        event_type = event.get("type", "")

        # Send to all clients
        async with self._lock:
            clients = list(self._clients.items())
            client_metas = dict(self._client_meta)

        dead: List[str] = []
        for cid, ws in clients:
            try:
                await ws.send_text(json_dumps(event))
                metrics.inc("ws_messages_sent")
                # Track ACK for state-changing events
                if event_id > 0 and event_type in STATE_CHANGE_EVENT_TYPES:
                    async with _pending_acks_lock:
                        _pending_acks[cid][event_id] = {
                            "event": event,
                            "retries": 0,
                            "sent_at": time.time(),
                        }
            except Exception:
                dead.append(cid)

        if dead:
            async with self._lock:
                for cid in dead:
                    self._clients.pop(cid, None)
                    self._client_meta.pop(cid, None)

    async def ack_event(self, cid: str, event_id: int):
        """Mark an event as acknowledged by a client."""
        async with _pending_acks_lock:
            _pending_acks[cid].pop(event_id, None)
        # Persist ACK to DB (fire & forget)
        with suppress(Exception):
            asyncio.create_task(db.execute(
                "INSERT OR REPLACE INTO event_ack(client_id, event_id, acked_at) VALUES(?,?,?)",
                (cid, event_id, time.time()),
            ))

    def count(self) -> int:
        return len(self._clients)


ws_manager = WSManager()


# ─────────────────────────────────────────────────────────────────────────────
# 15b. FULL STATE SNAPSHOT HELPERS (v6.0)
# ─────────────────────────────────────────────────────────────────────────────

async def _build_full_state() -> Dict:
    """
    Bangun snapshot lengkap semua state backend.
    Digunakan oleh /bootstrap, /state (upgraded), dan WS initial_state event.
    """
    pb = playback.to_dict()
    qs = queue_mgr.get_state()

    # Current track
    current_song = None
    idx = qs.get("current_index", -1)
    q_list = qs.get("queue", [])
    if 0 <= idx < len(q_list):
        current_song = q_list[idx]

    # DB queries (async)
    favs = await db.fetch_all(
        "SELECT video_id as videoId, title, artist, thumbnail, album, duration "
        "FROM favorites ORDER BY favorited_at DESC LIMIT 50"
    )
    history = await db.fetch_all(
        "SELECT video_id as videoId, title, artist, thumbnail, album, duration, played_at "
        "FROM recently_played ORDER BY played_at DESC LIMIT 20"
    )

    # Downloads dari memory manager
    downloads = download_mgr.list_all()

    # Recommendations dari cache (no network call)
    recs: List[Dict] = []
    current_vid = pb.get("current_video_id")
    if current_vid:
        with suppress(Exception):
            with _rec_lock:
                cached_recs = recommendation_cache.get(current_vid) if hasattr(recommendation_cache, "get") else None
            if cached_recs:
                recs = cached_recs[:10]

    # Server info
    uptime_secs = int(time.time() - START_TIME)
    mem_mb = 0.0
    cpu_pct = 0.0
    if psutil:
        with suppress(Exception):
            proc = psutil.Process(os.getpid())
            mem_mb = round(proc.memory_info().rss / 1024 / 1024, 1)
        with suppress(Exception):
            cpu_pct = round(psutil.cpu_percent(interval=None), 1)

    return {
        "state_version": state_mgr.get_version(),
        "server": {
            "uptime": get_uptime_str(),
            "uptime_seconds": uptime_secs,
            "version": "6.0",
            "active_ws": ws_manager.count(),
            "memory_mb": mem_mb,
            "cpu_percent": cpu_pct,
        },
        "playback": {
            **pb,
            "current_song": current_song,
        },
        "queue": {
            **qs,
            "current_song": current_song,
        },
        "favorites": favs or [],
        "downloads": downloads,
        "history": history or [],
        "recommendations": recs,
        "settings": {
            "shuffle": qs.get("shuffle", False),
            "repeat": qs.get("repeat", "none"),
            "autoplay": qs.get("autoplay", True),
            "volume": 100,
        },
        # Legacy flat fields (backward compat dengan frontend lama)
        "current_song": current_song,
        "current_video_id": pb.get("current_video_id"),
        "is_playing": pb.get("playing", False),
        "position": pb.get("position", 0),
        "duration": pb.get("duration", 0),
        "queue_length": qs.get("size", 0),
        "current_index": qs.get("current_index", -1),
        "active_ws": ws_manager.count(),
        "server_time": time.time(),
    }


async def _build_sync_state() -> Dict:
    """Snapshot ringan untuk reconnect WebSocket — tanpa favorites/history/recommendations."""
    pb = playback.to_dict()
    qs = queue_mgr.get_state()
    downloads = download_mgr.list_all()
    return {
        "state_version": state_mgr.get_version(),
        "playback": pb,
        "queue": qs,
        "favorites": [],          # frontend harus re-fetch /favorites jika perlu
        "downloads": downloads,
        "server_time": time.time(),
    }


# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class DownloadTask:
    video_id: str
    title: str
    artist: str
    status: str = "queued"   # queued|downloading|paused|completed|failed|cancelled
    progress: float = 0.0
    total_bytes: int = 0
    downloaded_bytes: int = 0
    speed_bps: float = 0.0
    eta_seconds: float = 0.0
    filename: str = ""
    filepath: str = ""
    error: str = ""
    priority: int = 0
    started_at: float = 0.0
    finished_at: float = 0.0
    _cancel: bool = False
    _pause: bool = False

    def to_dict(self) -> Dict:
        d = self.__dict__.copy()
        d.pop("_cancel", None)
        d.pop("_pause", None)
        return d


class DownloadManager:
    def __init__(self):
        self._tasks: Dict[str, DownloadTask] = {}
        self._lock = threading.Lock()
        self._semaphore = threading.Semaphore(MAX_CONCURRENT_DOWNLOADS)

    def _sanitize(self, name: str) -> str:
        # Hapus karakter tidak aman, batasi panjang
        out = re.sub(r"[^\w\-\. ]", "_", (name or "").strip())
        return out[:120] or "track"

    def get(self, vid: str) -> Optional[DownloadTask]:
        with self._lock:
            return self._tasks.get(vid)

    def list_all(self) -> List[Dict]:
        with self._lock:
            return [t.to_dict() for t in self._tasks.values()]

    def cancel(self, vid: str) -> bool:
        with self._lock:
            t = self._tasks.get(vid)
            if not t or t.status in ("completed", "failed", "cancelled"):
                return False
            t._cancel = True
            return True

    def pause(self, vid: str) -> bool:
        with self._lock:
            t = self._tasks.get(vid)
            if not t or t.status not in ("downloading", "queued"):
                return False
            t._pause = True
            t.status = "paused"
            return True

    def resume(self, vid: str) -> bool:
        with self._lock:
            t = self._tasks.get(vid)
            if not t or t.status != "paused":
                return False
            t._pause = False
            t.status = "queued"
        # Schedule a new worker
        threading.Thread(target=self._run, args=(t,), daemon=True).start()
        return True

    def enqueue(self, vid: str, title: str, artist: str, priority: int = 0) -> DownloadTask:
        with self._lock:
            t = self._tasks.get(vid)
            if t and t.status in ("downloading", "queued", "paused"):
                return t
            t = DownloadTask(video_id=vid, title=title, artist=artist, priority=priority)
            self._tasks[vid] = t
        # persist
        asyncio.run_coroutine_threadsafe(
            db.execute(
                "INSERT OR REPLACE INTO downloads(video_id,title,artist,status,priority,updated_at) "
                "VALUES(?,?,?,?,?,?)",
                (vid, title, artist, "queued", priority, time.time()),
            ),
            _loop,
        ) if _loop else None
        threading.Thread(target=self._run, args=(t,), daemon=True).start()
        return t

    def _persist(self, t: DownloadTask):
        if _loop is None:
            return
        asyncio.run_coroutine_threadsafe(
            db.execute(
                "INSERT OR REPLACE INTO downloads("
                "video_id,title,artist,filename,filepath,status,progress,total_bytes,"
                "downloaded_bytes,speed_bps,eta_seconds,error,priority,started_at,finished_at,updated_at"
                ") VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (t.video_id, t.title, t.artist, t.filename, t.filepath, t.status,
                 t.progress, t.total_bytes, t.downloaded_bytes, t.speed_bps,
                 t.eta_seconds, t.error, t.priority, t.started_at, t.finished_at,
                 time.time()),
            ),
            _loop,
        )

    def _run(self, t: DownloadTask):
        with self._semaphore:
            if t._cancel or t._pause:
                return
            metrics.inc("downloads_active")
            t.status = "downloading"
            t.started_at = time.time()
            safe_title = self._sanitize(t.title)
            safe_artist = self._sanitize(t.artist)
            t.filename = f"{safe_artist} - {safe_title}.m4a"
            t.filepath = os.path.join(DOWNLOAD_DIR, t.filename)
            temp_path = os.path.join(DOWNLOAD_DIR, f"{t.video_id}_dl.temp")

            last_tick = time.time()
            last_bytes = 0

            def hook(d):
                nonlocal last_tick, last_bytes
                if t._cancel:
                    raise yt_dlp.utils.DownloadError("cancelled by user")
                if t._pause:
                    raise yt_dlp.utils.DownloadError("paused by user")
                if d.get("status") == "downloading":
                    total = d.get("total_bytes") or d.get("total_bytes_estimate") or 0
                    downloaded = d.get("downloaded_bytes", 0)
                    t.total_bytes = int(total or 0)
                    t.downloaded_bytes = int(downloaded or 0)
                    if total:
                        t.progress = round(downloaded / total * 100, 2)
                    now = time.time()
                    dt = now - last_tick
                    if dt >= 0.5:
                        t.speed_bps = (downloaded - last_bytes) / max(dt, 0.001)
                        if t.speed_bps > 0 and total:
                            t.eta_seconds = max(0, (total - downloaded) / t.speed_bps)
                        last_tick = now
                        last_bytes = downloaded
                        self._persist(t)
                        if _loop:
                            asyncio.run_coroutine_threadsafe(
                                ws_manager.broadcast({
                                    "type": "download_progress",
                                    "videoId": t.video_id,
                                    "progress": t.progress,
                                    "speed_bps": t.speed_bps,
                                    "eta_seconds": t.eta_seconds,
                                }),
                                _loop,
                            )

            opts = get_ydl_opts("high", temp_path, progress_hook=hook)
            opts["writethumbnail"] = False
            url = f"https://music.youtube.com/watch?v={t.video_id}"
            try:
                with yt_dlp.YoutubeDL(opts) as ydl:
                    ydl.download([url])
                if t._cancel:
                    raise RuntimeError("cancelled")
                if os.path.exists(temp_path):
                    if os.path.exists(t.filepath):
                        with suppress(OSError):
                            os.remove(t.filepath)
                    os.rename(temp_path, t.filepath)
                    t.status = "completed"
                    t.progress = 100.0
                    t.finished_at = time.time()
                    metrics.inc("downloads_completed")
                    dl_logger.info(f"[DOWNLOAD] OK {t.filepath}")
                    if _loop:
                        asyncio.run_coroutine_threadsafe(
                            ws_manager.broadcast({
                                "type": "download_status",
                                "videoId": t.video_id,
                                "status": "completed",
                                "filename": t.filename,
                            }),
                            _loop,
                        )
                else:
                    raise RuntimeError("file output tidak ditemukan")
            except Exception as e:
                msg = str(e)
                if "cancelled" in msg.lower():
                    t.status = "cancelled"
                elif "paused" in msg.lower():
                    t.status = "paused"
                    # Persist & exit; user bisa resume
                else:
                    t.status = "failed"
                    t.error = msg
                    metrics.inc("downloads_failed")
                    dl_logger.error(f"[DOWNLOAD] FAIL {t.video_id}: {e}")
                if t.status != "paused" and os.path.exists(temp_path):
                    with suppress(OSError):
                        os.remove(temp_path)
                if _loop:
                    asyncio.run_coroutine_threadsafe(
                        ws_manager.broadcast({
                            "type": "download_status",
                            "videoId": t.video_id,
                            "status": t.status,
                            "error": t.error if t.status == "failed" else None,
                        }),
                        _loop,
                    )
            finally:
                metrics.inc("downloads_active", -1)
                self._persist(t)


download_mgr = DownloadManager()


# ─────────────────────────────────────────────────────────────────────────────
# 17. METADATA HELPER
# ─────────────────────────────────────────────────────────────────────────────

def build_track_meta(t: Dict, fallback_artist: str = "Unknown", fallback_thumb: Optional[str] = None) -> Dict:
    artists_list = t.get("artists", [])
    artist_name = artists_list[0]["name"] if artists_list else t.get("artist", fallback_artist)
    artist_id = None
    if artists_list:
        artist_id = artists_list[0].get("id") or artists_list[0].get("browseId")
    thumbs = t.get("thumbnails", [])
    thumb = thumbs[-1]["url"] if thumbs else fallback_thumb

    duration_raw = t.get("duration") or t.get("duration_seconds")
    duration_str = ""
    if isinstance(duration_raw, int):
        m, s = divmod(duration_raw, 60)
        duration_str = f"{m}:{s:02d}"
    elif isinstance(duration_raw, str):
        duration_str = duration_raw

    return {
        "title": t.get("title") or t.get("name") or "",
        "artist": artist_name,
        "artistBrowseId": artist_id,
        "album": t.get("album", {}).get("name", "") if isinstance(t.get("album"), dict) else t.get("album", ""),
        "duration": duration_str,
        "year": str(t.get("year", "")),
        "thumbnail": thumb,
        "videoId": t.get("videoId"),
        "browseId": t.get("browseId"),
        "type": t.get("resultType", t.get("type", "song")),
        "explicit": t.get("isExplicit", False),
    }


# ─────────────────────────────────────────────────────────────────────────────
# 18. SYSTEM METRICS HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def get_uptime_str() -> str:
    secs = int(time.time() - START_TIME)
    h, rem = divmod(secs, 3600)
    m, s = divmod(rem, 60)
    return f"{h}h {m}m {s}s"


def get_memory_str() -> str:
    if psutil is None:
        return "N/A"
    try:
        proc = psutil.Process(os.getpid())
        mb = proc.memory_info().rss / 1024 / 1024
        if mb > metrics.ram_peak_mb:
            metrics.ram_peak_mb = mb
        return f"{mb:.1f}MB"
    except Exception:
        return "Error"


def get_cpu_str() -> str:
    if psutil is None:
        return "N/A"
    try:
        c = psutil.cpu_percent(interval=None)
        if c > metrics.cpu_peak:
            metrics.cpu_peak = c
        return f"{c:.1f}%"
    except Exception:
        return "Error"


def get_disk_str() -> Dict[str, Any]:
    try:
        usage = shutil.disk_usage(".")
        return {
            "total_mb": round(usage.total / (1024 * 1024), 1),
            "free_mb": round(usage.free / (1024 * 1024), 1),
            "used_mb": round(usage.used / (1024 * 1024), 1),
        }
    except Exception:
        return {"total_mb": None, "free_mb": None, "used_mb": None}


def dir_size_mb(path: str) -> float:
    total = 0
    try:
        for root, _, files in os.walk(path):
            for f in files:
                fp = os.path.join(root, f)
                with suppress(OSError):
                    total += os.path.getsize(fp)
    except Exception:
        return 0.0
    return total / (1024 * 1024)


# ─────────────────────────────────────────────────────────────────────────────
# 19. ADAPTIVE PREFETCH ENGINE
# ─────────────────────────────────────────────────────────────────────────────

prefetch_queue: deque = deque(maxlen=32)
_prefetch_done: "TTLCache" = TTLCache(maxsize=512, ttl=PREFETCH_TTL)
_prefetch_lock = threading.Lock()


def enqueue_prefetch(video_id: str):
    if not video_id:
        return
    with _prefetch_lock:
        if video_id in _prefetch_done:
            metrics.inc("prefetch_skipped")
            return
        if video_id not in prefetch_queue:
            prefetch_queue.append(video_id)


async def warmup_stream(video_id: str, depth_label: str = "next") -> bool:
    """Extract URL + cache + pre-buffer first chunk. Aman dipanggil paralel."""
    try:
        # Already cached?
        if stream_cache.get(f"{video_id}:auto"):
            return True
        info = await extract_stream_async(video_id, "auto")
        if not info or not info.get("url"):
            return False
        url = info["url"]
        stream_cache.set(f"{video_id}:auto", url)
        if info.get("abr"):
            bitrate_cache[video_id] = info.get("abr")
        # store track meta async
        with suppress(Exception):
            await db.execute(
                "INSERT OR REPLACE INTO track_meta(video_id,title,duration,bitrate,updated_at) "
                "VALUES(?, COALESCE((SELECT title FROM track_meta WHERE video_id=?), ?), ?, ?, ?)",
                (video_id, video_id, info.get("title"), info.get("duration"),
                 int(info.get("abr") or 0), time.time()),
            )
        # First-chunk warmup
        if not chunk_cache.get(video_id):
            data = await http_get_bytes(url, byte_range=(0, FIRST_CHUNK_BYTES - 1), timeout=12)
            if data:
                chunk_cache.set(video_id, data)
        metrics.inc("prefetch_done")
        stream_logger.info(f"[WARMUP/{depth_label}] {video_id} READY")
        return True
    except Exception as e:
        stream_logger.warning(f"[WARMUP] {video_id} gagal: {e}")
        return False


async def _adaptive_prefetch_loop():
    """Pantau playback; jika >= 70% durasi, prefetch next songs aggressively."""
    while True:
        try:
            await asyncio.sleep(2.0)
            st = playback.to_dict()
            dur = st.get("duration") or 0
            pos = st.get("position") or 0
            if dur > 10 and pos > 0:
                ratio = pos / dur
                if ratio >= ADAPTIVE_PREFETCH_THRESHOLD:
                    now = time.time()
                    if now - playback._last_adaptive_trigger > 15:
                        playback._last_adaptive_trigger = now
                        upcoming = queue_mgr.peek_upcoming(PREFETCH_DEPTH)
                        for tr in upcoming:
                            vid = tr.get("videoId")
                            if vid:
                                enqueue_prefetch(vid)
                        # Autoplay-aware: kalau autoplay aktif & queue dekat habis, prefetch radio
                        if queue_mgr.autoplay and len(upcoming) < PREFETCH_DEPTH:
                            ref = st.get("current_video_id")
                            if ref:
                                asyncio.create_task(_warm_recommendations(ref))
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.warning(f"[ADAPTIVE] error: {e}")


async def _warm_recommendations(seed_vid: str):
    try:
        with _rec_lock:
            cached = recommendation_cache.get(seed_vid) if hasattr(recommendation_cache, "get") else None
        if cached:
            return
        loop = asyncio.get_running_loop()

        def _fetch():
            with yt_lock:
                try:
                    wp = yt.get_watch_playlist(videoId=seed_vid, limit=8)
                    return [build_track_meta(t) for t in wp.get("tracks", []) if t.get("videoId") != seed_vid]
                except Exception:
                    return []

        tracks = await loop.run_in_executor(IO_POOL, _fetch)
        if tracks:
            with _rec_lock:
                recommendation_cache[seed_vid] = tracks
            # warmup top-2
            for tr in tracks[:2]:
                vid = tr.get("videoId")
                if vid:
                    enqueue_prefetch(vid)
    except Exception as e:
        logger.warning(f"[REC-WARM] {seed_vid} gagal: {e}")


async def _prefetch_consumer_loop():
    """Konsumsi prefetch_queue secara async dengan concurrency terbatas."""
    sem = asyncio.Semaphore(2)

    async def _worker(vid: str):
        async with sem:
            ok = await warmup_stream(vid, "prefetch")
            if ok:
                with _prefetch_lock:
                    _prefetch_done[vid] = True
            # Background fisik cache hanya kalau storage cukup
            try:
                free_mb = shutil.disk_usage(".").free / (1024 * 1024)
                if free_mb > LOW_STORAGE_THRESHOLD_MB * 2:
                    EXTRACTOR_POOL.submit(background_cache_audio, vid)
            except Exception:
                pass

    while True:
        try:
            vid = None
            with _prefetch_lock:
                if prefetch_queue:
                    vid = prefetch_queue.popleft()
            if vid:
                asyncio.create_task(_worker(vid))
            else:
                await asyncio.sleep(0.5)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.warning(f"[PREFETCH-CONSUMER] {e}")
            await asyncio.sleep(1.0)


# ─────────────────────────────────────────────────────────────────────────────
# 20. BACKGROUND WORKERS (Cleanup, Sleep Timer, Watchdog, Quota)
# ─────────────────────────────────────────────────────────────────────────────

_background_tasks: List[asyncio.Task] = []
_loop: Optional[asyncio.AbstractEventLoop] = None


async def _cleanup_loop():
    while True:
        try:
            await asyncio.sleep(600)
            stream_cache.cleanup()
            # GC manual ringan
            gc.collect()
            cleanup_logger.info(f"[CLEANUP] cache_items={stream_cache.size()} ram={get_memory_str()}")
        except asyncio.CancelledError:
            break
        except Exception as e:
            cleanup_logger.warning(f"[CLEANUP] error: {e}")


async def _sleep_timer_loop():
    while True:
        try:
            await asyncio.sleep(5)
            if playback.sleep_timer_end and time.time() >= playback.sleep_timer_end:
                playback.update(playing=False, sleep_timer_end=None)
                await ws_manager.broadcast({"type": "sleep_timer_fired", "playing": False})
                logger.info("[SLEEP] waktu tidur habis — pemutaran berhenti")
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.warning(f"[SLEEP] {e}")


async def _quota_loop():
    """Disk quota & low-storage cleanup."""
    while True:
        try:
            await asyncio.sleep(300)
            # cache size
            cache_mb = dir_size_mb(CACHE_DIR) + dir_size_mb(CHUNK_CACHE_DIR)
            if MAX_CACHE_SIZE_MB > 0 and cache_mb > MAX_CACHE_SIZE_MB:
                cleanup_logger.info(f"[QUOTA] cache {cache_mb:.0f}MB > limit {MAX_CACHE_SIZE_MB}MB — LRU evict")
                await _evict_cache_lru(int((cache_mb - MAX_CACHE_SIZE_MB) * 1024 * 1024) + (50 * 1024 * 1024))
            # low storage
            usage = shutil.disk_usage(".")
            free_mb = usage.free / (1024 * 1024)
            if free_mb < LOW_STORAGE_THRESHOLD_MB:
                cleanup_logger.warning(f"[STORAGE] free hanya {free_mb:.0f}MB — emergency cleanup")
                await _evict_cache_lru(200 * 1024 * 1024)
                _cleanup_temp_files()
            # orphan cleaner
            _cleanup_temp_files()
        except asyncio.CancelledError:
            break
        except Exception as e:
            cleanup_logger.warning(f"[QUOTA] {e}")


def _cleanup_temp_files():
    """Hapus .temp lama di CACHE_DIR & DOWNLOAD_DIR & TEMP_DIR."""
    now = time.time()
    for d in (CACHE_DIR, DOWNLOAD_DIR, TEMP_DIR):
        try:
            for f in os.listdir(d):
                if f.endswith(".temp"):
                    fp = os.path.join(d, f)
                    with suppress(OSError):
                        if now - os.path.getmtime(fp) > 3600:
                            os.remove(fp)
                            cleanup_logger.info(f"[ORPHAN] hapus {fp}")
        except Exception:
            pass


async def _evict_cache_lru(bytes_to_free: int):
    """Hapus file cache berdasarkan akses terlama via cache_index."""
    try:
        rows = await db.fetch_all(
            "SELECT key, path, size_bytes FROM cache_index ORDER BY last_access ASC"
        )
        freed = 0
        for row in rows:
            if freed >= bytes_to_free:
                break
            p = row["path"]
            if p and os.path.exists(p):
                with suppress(OSError):
                    sz = os.path.getsize(p)
                    os.remove(p)
                    freed += sz
                    await db.execute("DELETE FROM cache_index WHERE key=?", (row["key"],))
                    cleanup_logger.info(f"[LRU] evict {p}")
    except Exception as e:
        cleanup_logger.warning(f"[LRU] {e}")


async def _watchdog_loop():
    """Heartbeat watchdog; deteksi spike RAM/CPU."""
    while True:
        try:
            await asyncio.sleep(30)
            mem = get_memory_str()
            cpu = get_cpu_str()
            logger.info(f"[WATCHDOG] uptime={get_uptime_str()} mem={mem} cpu={cpu} "
                        f"ws={ws_manager.count()} cache={stream_cache.size()} "
                        f"dl_active={metrics.downloads_active}")
            # RAM spike → trigger GC
            if psutil:
                with suppress(Exception):
                    rss = psutil.Process(os.getpid()).memory_info().rss / (1024 * 1024)
                    if rss > 600:
                        gc.collect()
                        cleanup_logger.warning(f"[WATCHDOG] RAM spike {rss:.0f}MB — gc.collect()")
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.warning(f"[WATCHDOG] {e}")


# ─────────────────────────────────────────────────────────────────────────────
# 21. MIGRATION (JSON legacy -> SQLite, idempotent)
# ─────────────────────────────────────────────────────────────────────────────

async def _migrate_legacy_json():
    try:
        # favorites
        if os.path.exists(FAVORITES_FILE):
            favs = _load_json(FAVORITES_FILE)
            if isinstance(favs, list):
                rows = [(f.get("videoId"), f.get("title", ""), f.get("artist", ""),
                         f.get("thumbnail", ""), f.get("album", ""), f.get("duration", ""),
                         f.get("favorited_at", time.time())) for f in favs if f.get("videoId")]
                if rows:
                    await db.executemany(
                        "INSERT OR IGNORE INTO favorites(video_id,title,artist,thumbnail,album,duration,favorited_at) "
                        "VALUES(?,?,?,?,?,?,?)", rows)
        # recently played
        if os.path.exists(HISTORY_FILE):
            hist = _load_json(HISTORY_FILE)
            if isinstance(hist, list):
                rows = [(h.get("videoId"), h.get("title", ""), h.get("artist", ""),
                         h.get("thumbnail", ""), h.get("album", ""), h.get("duration", ""),
                         h.get("played_at", time.time())) for h in hist if h.get("videoId")]
                if rows:
                    await db.executemany(
                        "INSERT OR REPLACE INTO recently_played(video_id,title,artist,thumbnail,album,duration,played_at) "
                        "VALUES(?,?,?,?,?,?,?)", rows)
        # playlist
        if os.path.exists(PLAYLIST_FILE):
            pls = _load_json(PLAYLIST_FILE)
            if isinstance(pls, list):
                rows = [(p.get("videoId"), p.get("title", ""), p.get("artist", ""),
                         p.get("thumbnail", ""), p.get("album", ""), p.get("duration", ""),
                         p.get("added_at", time.time())) for p in pls if p.get("videoId")]
                if rows:
                    await db.executemany(
                        "INSERT OR IGNORE INTO playlist(video_id,title,artist,thumbnail,album,duration,added_at) "
                        "VALUES(?,?,?,?,?,?,?)", rows)
        # search history
        if os.path.exists(SEARCH_HISTORY_FILE):
            sh = _load_json(SEARCH_HISTORY_FILE)
            if isinstance(sh, list):
                rows = [(s.get("query", ""), s.get("type", "songs"),
                         s.get("timestamp", time.time())) for s in sh if s.get("query")]
                if rows:
                    await db.executemany(
                        "INSERT INTO search_history(query,type,ts) VALUES(?,?,?)", rows)
        logger.info("[MIGRATION] legacy JSON → SQLite selesai (idempotent)")
    except Exception as e:
        logger.warning(f"[MIGRATION] gagal: {e}")


async def _position_broadcast_loop():
    """v5.0: Broadcast posisi playback setiap 3 detik ke semua WS client.
    Frontend mendapat progress bar realtime tanpa polling.
    Hanya broadcast saat lagu sedang playing untuk hemat bandwidth.
    """
    while True:
        try:
            await asyncio.sleep(3.0)
            st = playback.to_dict()
            if st.get("playing") and ws_manager.count() > 0:
                # Hitung estimasi posisi terbaru berdasarkan waktu berlalu
                pos = st.get("position", 0.0)
                updated_at = st.get("updated_at", time.time())
                elapsed = time.time() - updated_at
                estimated_pos = pos + elapsed if st.get("playing") else pos
                dur = st.get("duration", 0.0)
                if dur and estimated_pos > dur:
                    estimated_pos = dur
                await ws_manager.broadcast({
                    "type": "position_sync",
                    "position": round(estimated_pos, 2),
                    "duration": dur,
                    "current_video_id": st.get("current_video_id"),
                    "ts": time.time(),
                })
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.warning(f"[POS_SYNC] {e}")


async def _stream_url_refresh_loop():
    """v5.0: Auto-refresh URL streaming yang akan expired dalam < 10 menit.
    Mencegah playback tiba-tiba berhenti karena URL YouTube expired.
    YouTube stream URL biasanya expire setelah 6 jam, tapi kita refresh
    saat TTL tersisa < 10 menit untuk zero-downtime playback.
    """
    while True:
        try:
            await asyncio.sleep(120)  # cek setiap 2 menit
            now = time.time()
            refresh_threshold = 600  # 10 menit sebelum expire
            to_refresh: List[str] = []
            with stream_cache._lock:
                for key, item in list(stream_cache._cache.items()):
                    remaining = item["expires"] - now
                    if 0 < remaining < refresh_threshold:
                        # Hanya refresh lagu yang sedang atau akan dimainkan
                        vid = key.split(":")[0]
                        current = playback.current_video_id
                        upcoming_ids = {
                            t.get("videoId") for t in queue_mgr.peek_upcoming(PREFETCH_DEPTH)
                        }
                        if vid == current or vid in upcoming_ids:
                            to_refresh.append((key, vid))

            for key, vid in to_refresh:
                try:
                    quality = key.split(":")[-1] if ":" in key else "auto"
                    stream_logger.info(f"[URL_REFRESH] refreshing expired URL: {vid}")
                    info = await extract_stream_async(vid, quality)
                    if info and info.get("url"):
                        stream_cache.set(key, info["url"])
                        # Update state jika ini lagu yang sedang main
                        if vid == playback.current_video_id:
                            await ws_manager.broadcast({
                                "type": "stream_url_refreshed",
                                "videoId": vid,
                                "ts": time.time(),
                            })
                        stream_logger.info(f"[URL_REFRESH] OK: {vid}")
                except Exception as e:
                    stream_logger.warning(f"[URL_REFRESH] gagal {vid}: {e}")
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.warning(f"[URL_REFRESH_LOOP] {e}")


# ─────────────────────────────────────────────────────────────────────────────
# 22. FASTAPI APP & LIFESPAN
# ─────────────────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    global _loop, queue_async_lock, _mutation_queue, _pending_acks_lock
    _loop = asyncio.get_running_loop()

    # Init async queue lock (harus di dalam event loop)
    queue_async_lock = asyncio.Lock()
    _pending_acks_lock = asyncio.Lock()

    # v8.0: Global mutation serialization queue
    _mutation_queue = asyncio.Queue(maxsize=256)

    # Migrate legacy JSON -> SQLite (sekali, idempotent)
    await _migrate_legacy_json()

    # Startup cleanup: bersihkan tmp/ dari sesi sebelumnya
    _cleanup_temp_files()
    logger.info("[STARTUP] tmp/ cleanup selesai")

    # Start WS broadcaster
    await ws_manager.start()

    # Aiohttp session
    if HAVE_AIOHTTP:
        await get_http_session()

    # Background loops
    _background_tasks.append(asyncio.create_task(_cleanup_loop()))
    _background_tasks.append(asyncio.create_task(_sleep_timer_loop()))
    _background_tasks.append(asyncio.create_task(_quota_loop()))
    _background_tasks.append(asyncio.create_task(_watchdog_loop()))
    _background_tasks.append(asyncio.create_task(_adaptive_prefetch_loop()))
    _background_tasks.append(asyncio.create_task(_prefetch_consumer_loop()))
    # v5.0 new background tasks
    _background_tasks.append(asyncio.create_task(_position_broadcast_loop()))
    _background_tasks.append(asyncio.create_task(_stream_url_refresh_loop()))
    # v7.0 new background tasks
    _background_tasks.append(asyncio.create_task(_rate_limiter_cleanup_loop()))
    _background_tasks.append(asyncio.create_task(_session_cleanup_loop()))
    _background_tasks.append(asyncio.create_task(_local_search_index_sync_loop()))
    # v8.0 new background tasks
    _background_tasks.append(asyncio.create_task(_mutation_queue_worker()))
    _background_tasks.append(asyncio.create_task(_checkpoint_broadcast_loop()))
    _background_tasks.append(asyncio.create_task(_resend_unacked_loop()))
    _background_tasks.append(asyncio.create_task(_event_store_cleanup_loop()))

    logger.info(
        f"[STARTUP] backend v8.0 listen :{PORT} | workers={MAX_WORKERS} "
        f"ext={MAX_CONCURRENT_EXTRACTIONS} dl={MAX_CONCURRENT_DOWNLOADS} "
        f"orjson={HAVE_ORJSON} uvloop={HAVE_UVLOOP} aiohttp={HAVE_AIOHTTP} "
        f"rapidfuzz={HAVE_RAPIDFUZZ} | queue={len(queue_mgr.queue)} items restored | "
        f"auth_required={AUTH_REQUIRED} | v8_sync=active"
    )
    try:
        yield
    finally:
        # Graceful shutdown
        logger.info("[SHUTDOWN] menutup koneksi…")
        queue_mgr.save()
        playback.save_state()
        for t in _background_tasks:
            t.cancel()
        for t in _background_tasks:
            with suppress(BaseException):
                await t
        await ws_manager.stop()
        if HAVE_AIOHTTP and _aiohttp_session and not _aiohttp_session.closed:
            with suppress(Exception):
                await _aiohttp_session.close()
        with suppress(Exception):
            EXTRACTOR_POOL.shutdown(wait=False, cancel_futures=True)
            IO_POOL.shutdown(wait=False, cancel_futures=True)
            DOWNLOAD_POOL.shutdown(wait=False, cancel_futures=True)
        logger.info("[SHUTDOWN] selesai")


app = FastAPI(title="Production Music Backend v8.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Content-Range", "Accept-Ranges", "Content-Length", "X-Request-ID"],
)
# GZip ringan untuk JSON besar (skip stream binary otomatis di FastAPI)
app.add_middleware(GZipMiddleware, minimum_size=1024)




# ─────────────────────────────────────────────────────────────────────────────
# 23. ENDPOINTS — MONITORING & SYSTEM INFO
# ─────────────────────────────────────────────────────────────────────────────





# ─────────────────────────────────────────────────────────────────────────────
# 23b. GLOBAL STATE ENDPOINT v5.0
# ─────────────────────────────────────────────────────────────────────────────



# ─────────────────────────────────────────────────────────────────────────────
# 23c. BOOTSTRAP ENDPOINT (v6.0) — single call untuk startup frontend
# ─────────────────────────────────────────────────────────────────────────────



# ─────────────────────────────────────────────────────────────────────────────
# 23d. FORCE SYNC ENDPOINT (v6.0) — untuk reconnect WebSocket
# ─────────────────────────────────────────────────────────────────────────────



# ─────────────────────────────────────────────────────────────────────────────
# 23e. CURRENT TRACK ENDPOINT (v6.0) — lightweight polling
# ─────────────────────────────────────────────────────────────────────────────





# ─────────────────────────────────────────────────────────────────────────────
# 24. SEARCH (with cache + fuzzy + history weighting)
# ─────────────────────────────────────────────────────────────────────────────





# ─────────────────────────────────────────────────────────────────────────────
# 25. STREAM (URL extraction + cache + prefetch + dedup)
# ─────────────────────────────────────────────────────────────────────────────



async def _warmup_first_chunk(v: str, url: str):
    try:
        if chunk_cache.get(v):
            return
        data = await http_get_bytes(url, byte_range=(0, FIRST_CHUNK_BYTES - 1), timeout=10)
        if data:
            chunk_cache.set(v, data)
    except Exception:
        pass






# ─────────────────────────────────────────────────────────────────────────────
# 26. LYRICS
# ─────────────────────────────────────────────────────────────────────────────



# ─────────────────────────────────────────────────────────────────────────────
# 27. EXPLORATION
# ─────────────────────────────────────────────────────────────────────────────











# ─────────────────────────────────────────────────────────────────────────────
# 28. PLAYLIST IMPORTER
# ─────────────────────────────────────────────────────────────────────────────



# ─────────────────────────────────────────────────────────────────────────────
# 29. LOCAL PLAYLIST (SQLite-backed)
# ─────────────────────────────────────────────────────────────────────────────







# ─────────────────────────────────────────────────────────────────────────────
# 30. FAVORITES (SQLite-backed)
# ─────────────────────────────────────────────────────────────────────────────









# ─────────────────────────────────────────────────────────────────────────────
# 31. RECENTLY PLAYED (SQLite-backed)
# ─────────────────────────────────────────────────────────────────────────────







# ─────────────────────────────────────────────────────────────────────────────
# 32. SEARCH HISTORY
# ─────────────────────────────────────────────────────────────────────────────





# ─────────────────────────────────────────────────────────────────────────────
# 33. DOWNLOAD MANAGER ENDPOINTS
# ─────────────────────────────────────────────────────────────────────────────

















# ─────────────────────────────────────────────────────────────────────────────
# 34. QUEUE CONTROL
# ─────────────────────────────────────────────────────────────────────────────



























# ─────────────────────────────────────────────────────────────────────────────
# 35. PLAYBACK
# ─────────────────────────────────────────────────────────────────────────────











# ─────────────────────────────────────────────────────────────────────────────
# 36. SLEEP TIMER
# ─────────────────────────────────────────────────────────────────────────────





# ─────────────────────────────────────────────────────────────────────────────
# 37. RECOMMENDATIONS (with cache + history weighting)
# ─────────────────────────────────────────────────────────────────────────────



# ─────────────────────────────────────────────────────────────────────────────
# 38. BATCH ENDPOINT
# ─────────────────────────────────────────────────────────────────────────────



# ─────────────────────────────────────────────────────────────────────────────
# 39. CACHE MANAGEMENT
# ─────────────────────────────────────────────────────────────────────────────





# ─────────────────────────────────────────────────────────────────────────────
# 40. WEBSOCKET ENDPOINT (with reconnect token + heartbeat)
# ─────────────────────────────────────────────────────────────────────────────



# ─────────────────────────────────────────────────────────────────────────────
# 40b. v7.0 BACKGROUND WORKERS
# ─────────────────────────────────────────────────────────────────────────────

async def _rate_limiter_cleanup_loop():
    """Bersihkan bucket IP stale setiap 10 menit."""
    while True:
        try:
            await asyncio.sleep(600)
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(IO_POOL, rate_limiter.cleanup_stale)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.warning(f"[RATE_LIMITER_CLEANUP] {e}")


async def _session_cleanup_loop():
    """Hapus expired JWT sessions dari DB setiap jam."""
    while True:
        try:
            await asyncio.sleep(3600)
            await session_store.cleanup_expired()
            logger.info("[SESSION] expired sessions dibersihkan")
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.warning(f"[SESSION_CLEANUP] {e}")


async def _local_search_index_sync_loop():
    """
    Sync recently_played dan favorites ke local_search_index setiap 5 menit.
    Ini yang bikin Layer 2 (local search) bisa kerja tanpa YTMusic.
    """
    while True:
        try:
            await asyncio.sleep(300)
            # Sync dari recently_played
            rows = await db.fetch_all(
                "SELECT video_id, title, artist, thumbnail, duration, played_at "
                "FROM recently_played ORDER BY played_at DESC LIMIT 100"
            )
            for r in rows:
                await db.execute(
                    "INSERT OR REPLACE INTO local_search_index"
                    "(video_id, title, artist, thumbnail, duration, play_count, last_played, indexed_at) "
                    "VALUES(?,?,?,?,?,"
                    "  COALESCE((SELECT play_count+1 FROM local_search_index WHERE video_id=?), 1),"
                    "  ?, ?)",
                    (r["video_id"], r.get("title",""), r.get("artist",""),
                     r.get("thumbnail"), r.get("duration",""),
                     r["video_id"], r.get("played_at", time.time()), time.time()),
                )
            # Sync dari favorites
            favs = await db.fetch_all(
                "SELECT video_id, title, artist, thumbnail, duration FROM favorites LIMIT 200"
            )
            for f in favs:
                await db.execute(
                    "INSERT OR IGNORE INTO local_search_index"
                    "(video_id, title, artist, thumbnail, duration, play_count, last_played, indexed_at) "
                    "VALUES(?,?,?,?,?,0,0,?)",
                    (f["video_id"], f.get("title",""), f.get("artist",""),
                     f.get("thumbnail"), f.get("duration",""), time.time()),
                )
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.warning(f"[SEARCH_IDX_SYNC] {e}")


# ─────────────────────────────────────────────────────────────────────────────
# 40c. v7.0 AUTH ENDPOINTS
# ─────────────────────────────────────────────────────────────────────────────













# ─────────────────────────────────────────────────────────────────────────────
# 40d. v7.0 AUDIO PROXY ENGINE
# ─────────────────────────────────────────────────────────────────────────────



async def _proxy_serve_file(filepath: str, request: Request, content_type: str):
    """Sajikan file lokal dengan Range support (reuse logika dari /stream/file)."""
    file_size = os.path.getsize(filepath)
    range_header = request.headers.get("range") or request.headers.get("Range")

    if not range_header:
        return FileResponse(
            filepath,
            media_type=content_type,
            headers={
                "Accept-Ranges": "bytes",
                "Content-Length": str(file_size),
                "Cache-Control": "public, max-age=3600",
            },
        )

    try:
        range_str = range_header.replace("bytes=", "")
        start_str, _, end_str = range_str.partition("-")
        start = int(start_str) if start_str else 0
        end = int(end_str) if end_str else file_size - 1
        end = min(end, file_size - 1)
        length = end - start + 1
    except Exception:
        raise HTTPException(416, "Invalid Range")

    async def _file_gen():
        loop = asyncio.get_running_loop()
        def _read():
            with open(filepath, "rb") as f:
                f.seek(start)
                remaining = length
                while remaining > 0:
                    chunk = f.read(min(65536, remaining))
                    if not chunk:
                        break
                    remaining -= len(chunk)
                    yield chunk
        for chunk in _read():
            yield chunk

    return StreamingResponse(
        _file_gen(),
        status_code=206,
        media_type=content_type,
        headers={
            "Content-Range": f"bytes {start}-{end}/{file_size}",
            "Accept-Ranges": "bytes",
            "Content-Length": str(length),
        },
    )


async def _proxy_stream_url(stream_url: str, request: Request, videoId: str):
    """
    Proxy streaming dari URL YouTube ke client dengan forwarding Range header.
    Mendukung byte-seeking, chunk streaming, auto-reconnect internal.
    """
    range_header = request.headers.get("range") or request.headers.get("Range")
    proxy_headers: Dict[str, str] = {
        "User-Agent": "Mozilla/5.0 (Linux; Android 11) AppleWebKit/537.36 Chrome/95",
        "Referer": "https://music.youtube.com/",
        "Origin": "https://music.youtube.com",
    }
    if range_header:
        proxy_headers["Range"] = range_header

    sess = await get_http_session()
    if sess is None:
        # Fallback ke redirect (kehilangan proxy protection, tapi tidak crash)
        from fastapi.responses import RedirectResponse
        return RedirectResponse(stream_url, status_code=302)

    try:
        resp = await sess.get(
            stream_url,
            headers=proxy_headers,
            timeout=aiohttp.ClientTimeout(total=None, connect=10, sock_read=30),
            allow_redirects=True,
        )

        status = resp.status
        resp_headers = {
            "Accept-Ranges": "bytes",
            "Cache-Control": "no-cache",
        }

        # Forward content headers
        for h in ("Content-Length", "Content-Range", "Content-Type"):
            val = resp.headers.get(h)
            if val:
                resp_headers[h] = val

        ct = resp.headers.get("Content-Type", "audio/mp4")

        async def _stream_gen():
            try:
                async for chunk in resp.content.iter_chunked(65536):
                    yield chunk
            except Exception as e:
                stream_logger.warning(f"[AUDIO_PROXY] stream interrupted {videoId}: {e}")
            finally:
                resp.release()

        return StreamingResponse(
            _stream_gen(),
            status_code=status,
            media_type=ct,
            headers=resp_headers,
        )
    except Exception as e:
        stream_logger.error(f"[AUDIO_PROXY] gagal proxy {videoId}: {e}")
        raise HTTPException(502, f"Audio proxy error: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# 40e. v7.0 THUMBNAIL PROXY
# ─────────────────────────────────────────────────────────────────────────────



# ─────────────────────────────────────────────────────────────────────────────
# 40f. v7.0 ADVANCED SEARCH ENGINE ENDPOINTS
# ─────────────────────────────────────────────────────────────────────────────







# ─────────────────────────────────────────────────────────────────────────────
# 40g. v7.0 SMART RECOMMENDATION ENGINE
# ─────────────────────────────────────────────────────────────────────────────





# ─────────────────────────────────────────────────────────────────────────────
# 40h. v7.0 SMART RADIO MODE
# ─────────────────────────────────────────────────────────────────────────────

_radio_active: Dict[str, Any] = {
    "active": False,
    "seed_type": None,   # "track" | "artist" | "album"
    "seed_id": None,
    "seed_name": None,
    "generated_count": 0,
    "started_at": None,
}
_radio_lock = asyncio.Lock()


async def _generate_radio_tracks(
    seed_id: str,
    seed_type: str = "track",
    limit: int = 8,
    exclude: Optional[Set[str]] = None,
) -> List[Dict]:
    """
    Hasilkan daftar lagu untuk radio berdasarkan seed.
    seed_type: "track" | "artist" | "album"
    """
    if exclude is None:
        exclude = set()
    exclude.add(seed_id)

    loop = asyncio.get_running_loop()

    def _fetch():
        tracks: List[Dict] = []
        seen = set(exclude)
        with yt_lock:
            try:
                if seed_type == "track":
                    wp = yt.get_watch_playlist(videoId=seed_id, limit=limit + 3)
                    for t in wp.get("tracks", []):
                        if t.get("videoId") and t["videoId"] not in seen:
                            tracks.append(build_track_meta(t))
                            seen.add(t["videoId"])
                elif seed_type == "artist":
                    # Ambil lagu dari artist, lalu chain
                    artist_data = yt.get_artist(seed_id)
                    songs = artist_data.get("songs", {}).get("results", [])
                    if songs:
                        for s in songs[:limit]:
                            if s.get("videoId") and s["videoId"] not in seen:
                                tracks.append(build_track_meta(s))
                                seen.add(s["videoId"])
                        # Chain dari lagu pertama
                        if tracks:
                            wp = yt.get_watch_playlist(videoId=tracks[0]["videoId"], limit=5)
                            for t in wp.get("tracks", []):
                                if t.get("videoId") and t["videoId"] not in seen:
                                    tracks.append(build_track_meta(t))
                                    seen.add(t["videoId"])
                elif seed_type == "album":
                    album_data = yt.get_album(seed_id)
                    for t in album_data.get("tracks", [])[:limit]:
                        if t.get("videoId") and t["videoId"] not in seen:
                            tracks.append(build_track_meta(t))
                            seen.add(t["videoId"])
            except Exception as e:
                logger.warning(f"[RADIO_GEN] {seed_type}={seed_id}: {e}")
        return tracks

    tracks = await loop.run_in_executor(IO_POOL, _fetch)
    return tracks










# ─────────────────────────────────────────────────────────────────────────────
# 40i. v7.0/v8.0 ENHANCED WS BROADCAST EVENTS (via event_bus)
# ─────────────────────────────────────────────────────────────────────────────

async def _broadcast_history_add(track: Dict):
    """Broadcast saat lagu ditambahkan ke history."""
    await event_bus.emit("history_add", {"track": track, "server_time": time.time()})


async def _broadcast_history_remove(video_id: str):
    """Broadcast saat history dihapus."""
    await event_bus.emit("history_remove", {"videoId": video_id, "server_time": time.time()})


async def _broadcast_settings_update(settings: Dict):
    """Broadcast saat settings diupdate."""
    await event_bus.emit("settings_update", {"settings": settings, "server_time": time.time()})


async def _broadcast_recommendation_update(tracks: List[Dict], seed: Optional[str] = None):
    """Broadcast saat recommendations diperbarui."""
    await event_bus.emit("recommendation_update", {
        "seed": seed, "count": len(tracks), "server_time": time.time()
    })


# ─────────────────────────────────────────────────────────────────────────────
# 40k. v8.0 BACKGROUND WORKERS — Checkpoint, Resend, Event Store Cleanup
# ─────────────────────────────────────────────────────────────────────────────

async def _checkpoint_broadcast_loop():
    """
    Every 30 seconds broadcast a checkpoint event so all clients can
    validate their synchronization state.
    """
    while True:
        try:
            await asyncio.sleep(CHECKPOINT_INTERVAL)
            latest_eid = await event_store.get_latest_event_id()
            sv = state_mgr.get_version()
            await ws_manager.broadcast({
                "type": "checkpoint",
                "state_version": sv,
                "event_id": latest_eid,
                "server_timestamp": time.time(),
                "server_position": get_authoritative_position(),
            })
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.warning(f"[CHECKPOINT] {e}")


async def _resend_unacked_loop():
    """
    Periodically resend state-changing events that have not been ACKed.
    Drops after RESEND_MAX_RETRIES retries.
    """
    while True:
        try:
            await asyncio.sleep(RESEND_INTERVAL)
            now = time.time()
            async with _pending_acks_lock:
                all_cids = list(_pending_acks.keys())
            for cid in all_cids:
                async with _pending_acks_lock:
                    pending = dict(_pending_acks.get(cid, {}))
                for event_id, info in pending.items():
                    if now - info["sent_at"] < RESEND_TIMEOUT:
                        continue
                    if info["retries"] >= RESEND_MAX_RETRIES:
                        # Give up — remove from pending
                        async with _pending_acks_lock:
                            _pending_acks[cid].pop(event_id, None)
                        ws_logger.warning(
                            f"[RESEND] drop event_id={event_id} cid={cid[:8]} "
                            f"after {RESEND_MAX_RETRIES} retries"
                        )
                        continue
                    # Attempt resend
                    ev = {**info["event"], "resent": True, "retry": info["retries"] + 1}
                    ok = await ws_manager._send_to_client(cid, ev)
                    async with _pending_acks_lock:
                        if cid in _pending_acks and event_id in _pending_acks[cid]:
                            if ok:
                                _pending_acks[cid][event_id]["retries"] += 1
                                _pending_acks[cid][event_id]["sent_at"] = now
                            else:
                                # Client gone — remove all pending for this client
                                _pending_acks.pop(cid, None)
                                break
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.warning(f"[RESEND_LOOP] {e}")


async def _event_store_cleanup_loop():
    """Periodic event store pruning (belt-and-suspenders, also done on every insert)."""
    while True:
        try:
            await asyncio.sleep(300)   # every 5 minutes
            await event_store._prune()
            # Also prune old ACK records (older than 24h)
            cutoff = time.time() - 86400
            await db.execute("DELETE FROM event_ack WHERE acked_at < ?", (cutoff,))
            logger.info("[EVENT_STORE] prune completed")
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.warning(f"[EVENT_STORE_CLEANUP] {e}")


# ─────────────────────────────────────────────────────────────────────────────
# 40l. v8.0 SYNC API ENDPOINTS
# ─────────────────────────────────────────────────────────────────────────────







# ─────────────────────────────────────────────────────────────────────────────
# 40j. v7.0 RATE LIMITER MIDDLEWARE (inject ke existing stream/search endpoints)
# ─────────────────────────────────────────────────────────────────────────────

# Middleware HTTP ringan untuk inject rate limiting ke semua endpoint
# (endpoint kritis sudah punya rate limiting explicit di dalam fungsinya)

# Settings endpoint v7.0




# ─────────────────────────────────────────────────────────────────────────────
# 41. GLOBAL EXCEPTION HOOK (anti-crash)
# ─────────────────────────────────────────────────────────────────────────────

def _excepthook(exc_type, exc_value, exc_tb):
    msg = "".join(traceback.format_exception(exc_type, exc_value, exc_tb))
    logger.error(f"[UNCAUGHT] {msg}")
    _crash_dump("uncaught", exc_value)


sys.excepthook = _excepthook


def _signal_handler(signum, frame):
    logger.info(f"[SIGNAL] received {signum} — graceful shutdown signaled")


with suppress(Exception):
    signal.signal(signal.SIGTERM, _signal_handler)
    signal.signal(signal.SIGINT, _signal_handler)


# ─────────────────────────────────────────────────────────────────────────────
# 42. MAIN ENTRY (Termux optimized uvicorn)
# ─────────────────────────────────────────────────────────────────────────────

