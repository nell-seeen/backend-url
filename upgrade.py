"""Routes: upgrade — Additive endpoints required by the upgrade spec.

Executed in core.__dict__ namespace by routes/__init__.py.

Implements (all NEW, no modifications to existing endpoints):
  GET  /health/details         — detailed health check
  GET  /metrics                — structured metrics
  GET  /audit                  — audit log
  GET  /capabilities           — server capability list
  GET  /permissions            — permission registry
  GET  /schema/events          — event type registry
  GET  /schema/errors          — error code registry
  GET  /frontend/docs          — human-readable API docs
  GET  /frontend/openapi       — OpenAPI JSON export
  GET  /frontend/manifest      — frontend manifest
  GET  /frontend/config        — frontend config
  GET  /frontend/sdk           — SDK generator (TS/JS/Dart/Python)
  GET  /ai/context             — AI-friendly context document
  GET  /session/recover        — session recovery helper
  GET  /sync/delta             — delta sync (from=event_id)
  POST /backup                 — full data backup
  POST /restore                — restore from backup
"""

import zipfile
import io

# ─────────────────────────────────────────────────────────────────────────────
# GET /health/details
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/health/details")
async def health_details():
    """Detailed health check — disk, memory, workers, queues, optional deps."""
    disk = get_disk_str()
    mem_mb = 0.0
    cpu_pct = 0.0
    if psutil:
        with suppress(Exception):
            proc = psutil.Process(os.getpid())
            mem_mb = round(proc.memory_info().rss / 1024 / 1024, 1)
        with suppress(Exception):
            cpu_pct = round(psutil.cpu_percent(interval=None), 1)

    cache_mb = round(dir_size_mb(CACHE_DIR), 2)
    dl_mb = round(dir_size_mb(DOWNLOAD_DIR), 2)
    chunk_mb = round(dir_size_mb(CHUNK_CACHE_DIR), 2)
    thumb_mb = round(dir_size_mb(THUMB_CACHE_DIR), 2)
    latest_eid = await event_store.get_latest_event_id()

    return {
        "status": "online",
        "version": "8.0",
        "uptime": get_uptime_str(),
        "uptime_seconds": int(time.time() - START_TIME),
        "server_time": time.time(),
        "state_version": state_mgr.get_version(),
        "latest_event_id": latest_eid,

        "system": {
            "memory_mb": mem_mb,
            "cpu_percent": cpu_pct,
            "disk": disk,
        },

        "storage": {
            "audio_cache_mb": cache_mb,
            "download_mb": dl_mb,
            "chunk_cache_mb": chunk_mb,
            "thumb_cache_mb": thumb_mb,
            "audio_cache_limit_mb": MAX_CACHE_SIZE_MB,
            "low_storage_threshold_mb": LOW_STORAGE_THRESHOLD_MB,
        },

        "workers": {
            "io_pool": MAX_WORKERS,
            "extractor_pool": MAX_CONCURRENT_EXTRACTIONS,
            "download_pool": MAX_CONCURRENT_DOWNLOADS,
            "prefetch_depth": PREFETCH_DEPTH,
        },

        "queues": {
            "queue_size": len(queue_mgr.queue),
            "prefetch_pending": len(prefetch_queue),
            "ws_clients": ws_manager.count(),
            "downloads_active": metrics.downloads_active,
        },

        "dependencies": {
            "required": {
                "fastapi": True,
                "yt_dlp": True,
                "ytmusicapi": True,
                "uvicorn": True,
            },
            "optional": {
                "orjson": HAVE_ORJSON,
                "uvloop": HAVE_UVLOOP,
                "rapidfuzz": HAVE_RAPIDFUZZ,
                "aiohttp": HAVE_AIOHTTP,
                "aiofiles": HAVE_AIOFILES,
                "aiosqlite": HAVE_AIOSQLITE,
                "cachetools": HAVE_CACHETOOLS,
                "psutil": psutil is not None,
            },
        },

        "features": {
            "auth_required": AUTH_REQUIRED,
            "rate_limiting": True,
            "jwt_auth": True,
            "stream_proxy": True,
            "thumb_proxy": True,
            "radio": True,
            "recommendations": True,
            "smart_queue": True,
            "sponsorblock": False,  # not yet implemented
            "lyrics": True,
            "event_store": True,
            "delta_sync": True,
            "session_resume": True,
        },
    }


# ─────────────────────────────────────────────────────────────────────────────
# GET /metrics
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/metrics")
async def get_metrics():
    """Structured metrics endpoint — equivalent of /monitor but namespaced."""
    snap = metrics.snapshot()
    disk = get_disk_str()
    mem_mb = 0.0
    cpu_pct = 0.0
    if psutil:
        with suppress(Exception):
            proc = psutil.Process(os.getpid())
            mem_mb = round(proc.memory_info().rss / 1024 / 1024, 1)
        with suppress(Exception):
            cpu_pct = round(psutil.cpu_percent(interval=None), 1)

    return {
        "success": True,
        "server_time": time.time(),
        "metrics": {
            "requests": {
                "total": snap["total_requests"],
                "slow": snap["slow_requests"],
                "avg_latency_ms": snap["avg_latency_ms"],
                "p95_latency_ms": snap["p95_latency_ms"],
            },
            "cache": {
                "hit": snap["cache_hit"],
                "miss": snap["cache_miss"],
                "hit_ratio": snap["cache_hit_ratio"],
                "ram_items": stream_cache.size(),
                "audio_mb": round(dir_size_mb(CACHE_DIR), 2),
                "chunk_mb": round(dir_size_mb(CHUNK_CACHE_DIR), 2),
            },
            "stream": {
                "failures": snap["stream_failures"],
                "ytdlp_failures": snap["ytdlp_failures"],
                "prefetch_done": snap["prefetch_done"],
                "prefetch_skipped": snap["prefetch_skipped"],
            },
            "downloads": {
                "active": snap["downloads_active"],
                "completed": snap["downloads_completed"],
                "failed": snap["downloads_failed"],
            },
            "websocket": {
                "messages_sent": snap["ws_messages_sent"],
                "messages_recv": snap["ws_messages_recv"],
                "clients": ws_manager.count(),
            },
            "system": {
                "memory_mb": mem_mb,
                "cpu_percent": cpu_pct,
                "ram_peak_mb": snap["ram_peak_mb"],
                "cpu_peak": snap["cpu_peak"],
                "disk": disk,
            },
        },
    }


# ─────────────────────────────────────────────────────────────────────────────
# GET /audit
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/audit")
async def get_audit(
    limit: int = Query(100, ge=1, le=1000),
    event_type: Optional[str] = Query(None),
    video_id: Optional[str] = Query(None),
):
    """
    GET /audit — audit log from the analytics table.
    Filter by event_type and/or video_id.
    """
    sql = "SELECT event, video_id, value, ts FROM analytics"
    params: List = []
    conditions: List[str] = []

    if event_type:
        conditions.append("event = ?")
        params.append(event_type)
    if video_id:
        conditions.append("video_id = ?")
        params.append(video_id)

    if conditions:
        sql += " WHERE " + " AND ".join(conditions)

    sql += " ORDER BY ts DESC LIMIT ?"
    params.append(limit)

    rows = await db.fetch_all(sql, tuple(params))
    return {
        "success": True,
        "count": len(rows),
        "audit": rows,
        "server_time": time.time(),
    }


# ─────────────────────────────────────────────────────────────────────────────
# GET /capabilities
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/capabilities")
async def capabilities():
    """
    GET /capabilities — machine-readable list of all server capabilities.
    AI agents and frontends can use this to discover what the backend supports.
    """
    return {
        "success": True,
        "capabilities": {
            # Core playback
            "playback": True,
            "queue": True,
            "shuffle": True,
            "repeat": True,
            "autoplay": True,
            "sleep_timer": True,

            # Search
            "search": True,
            "search_suggestions": True,
            "search_history": True,
            "search_popular": True,
            "local_search_index": True,

            # Streaming
            "stream": True,
            "stream_proxy": True,
            "stream_range_request": True,
            "stream_url_auto_refresh": True,
            "adaptive_prefetch": True,
            "first_chunk_cache": True,

            # Media
            "thumbnail_proxy": True,
            "lyrics": True,

            # Downloads
            "download": True,
            "download_pause_resume": True,
            "download_cancel": True,
            "download_retry": True,

            # Library
            "favorites": True,
            "history": True,
            "playlist": True,
            "playlist_import": True,

            # Recommendations & Radio
            "recommendations": True,
            "recommendations_personal": True,
            "recommendations_similar": True,
            "radio": True,
            "smart_queue": True,

            # Sync & Events
            "websocket": True,
            "event_store": True,
            "event_replay": True,
            "delta_sync": True,
            "session_resume": True,
            "checkpoint_broadcast": True,
            "mutation_queue": True,
            "authoritative_clock": True,
            "ack_system": True,
            "state_version": True,

            # Auth
            "jwt_auth": True,
            "multi_session": True,
            "optional_auth": not AUTH_REQUIRED,

            # API features
            "batch_api": True,
            "settings": True,
            "backup_restore": True,
            "cache_management": True,
            "self_documenting": True,
            "openapi_export": True,
            "sdk_generator": True,
            "ai_context": True,

            # Observability
            "health": True,
            "health_details": True,
            "metrics": True,
            "audit_log": True,
            "event_tracing": True,

            # Optional (runtime-detected)
            "orjson_acceleration": HAVE_ORJSON,
            "uvloop_acceleration": HAVE_UVLOOP,
            "rapidfuzz_search": HAVE_RAPIDFUZZ,
            "aiohttp_proxy": HAVE_AIOHTTP,
        },
        "limits": {
            "max_queue": MAX_QUEUE,
            "max_history": MAX_HISTORY,
            "max_favorites": MAX_FAVORITES,
            "max_search_history": MAX_SEARCH_HISTORY,
            "event_store_max": EVENT_STORE_MAX,
            "max_concurrent_downloads": MAX_CONCURRENT_DOWNLOADS,
            "max_cache_mb": MAX_CACHE_SIZE_MB,
            "prefetch_depth": PREFETCH_DEPTH,
        },
        "server_time": time.time(),
        "state_version": state_mgr.get_version(),
    }


# ─────────────────────────────────────────────────────────────────────────────
# GET /permissions
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/permissions")
async def permissions(authorization: Optional[str] = Header(None)):
    """
    GET /permissions — returns current user's effective permissions.
    In single-user / no-auth mode, all permissions are granted.
    """
    user = await _get_current_user(authorization)
    authenticated = user is not None

    # In this implementation, all authenticated users (or all users if AUTH_REQUIRED=false)
    # have full permissions. This structure is extensible for RBAC.
    has_access = authenticated or not AUTH_REQUIRED

    all_perms = [
        "playback:read", "playback:write",
        "queue:read", "queue:write",
        "search:read",
        "stream:read",
        "download:read", "download:write",
        "favorites:read", "favorites:write",
        "history:read", "history:write",
        "playlist:read", "playlist:write",
        "recommendations:read",
        "radio:read", "radio:write",
        "settings:read", "settings:write",
        "auth:read",
        "cache:read", "cache:write",
        "backup:write", "restore:write",
        "metrics:read", "audit:read",
    ]

    return {
        "success": True,
        "user": user.get("sub") if user else ("anonymous" if not AUTH_REQUIRED else None),
        "authenticated": authenticated,
        "auth_required": AUTH_REQUIRED,
        "permissions": all_perms if has_access else ["auth:login"],
        "server_time": time.time(),
    }


# ─────────────────────────────────────────────────────────────────────────────
# GET /schema/events
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/schema/events")
async def schema_events():
    """
    GET /schema/events — event type registry.
    Documents all possible WebSocket event types with payload schema.
    """
    return {
        "success": True,
        "description": "All WebSocket event types emitted by this server.",
        "envelope": {
            "event_id": "integer — monotonic event ID from EventStore",
            "state_version": "integer — server state version at time of event",
            "server_timestamp": "float — unix timestamp",
            "type": "string — event type (see events below)",
            "replayed": "boolean? — true if event was replayed after reconnect",
            "resent": "boolean? — true if event was resent after no ACK",
        },
        "state_change_events": [
            # Playback
            {"type": "play", "category": "playback", "payload": {"videoId": "str", "title": "str", "artist": "str", "duration": "float", "position": "float"}},
            {"type": "pause", "category": "playback", "payload": {"videoId": "str", "position": "float"}},
            {"type": "seek", "category": "playback", "payload": {"videoId": "str", "position": "float"}},
            {"type": "playback_state", "category": "playback", "payload": {"playing": "bool", "position": "float", "duration": "float", "current_video_id": "str"}},
            {"type": "playback_update", "category": "playback", "payload": {"playing": "bool", "position": "float"}},
            {"type": "stream_url_refreshed", "category": "playback", "payload": {"videoId": "str"}},

            # Queue
            {"type": "queue_updated", "category": "queue", "payload": {"queue": "list", "current_index": "int"}},
            {"type": "next_track", "category": "queue", "payload": {"track": "object", "current_index": "int"}},
            {"type": "prev_track", "category": "queue", "payload": {"track": "object", "current_index": "int"}},
            {"type": "queue_jumped", "category": "queue", "payload": {"index": "int", "track": "object"}},
            {"type": "queue_add", "category": "queue", "payload": {"track": "object", "index": "int"}},
            {"type": "queue_remove", "category": "queue", "payload": {"index": "int"}},
            {"type": "queue_reorder", "category": "queue", "payload": {"from": "int", "to": "int"}},
            {"type": "queue_clear", "category": "queue", "payload": {}},
            {"type": "shuffle_changed", "category": "queue", "payload": {"shuffle": "bool"}},
            {"type": "repeat_changed", "category": "queue", "payload": {"repeat": "str"}},
            {"type": "autoplay_changed", "category": "queue", "payload": {"autoplay": "bool"}},

            # Favorites
            {"type": "favorite_add", "category": "favorites", "payload": {"track": "object"}},
            {"type": "favorite_remove", "category": "favorites", "payload": {"videoId": "str"}},

            # History
            {"type": "history_add", "category": "history", "payload": {"track": "object"}},
            {"type": "history_remove", "category": "history", "payload": {"videoId": "str"}},

            # Downloads
            {"type": "download_started", "category": "downloads", "payload": {"videoId": "str", "title": "str"}},
            {"type": "download_completed", "category": "downloads", "payload": {"videoId": "str", "filename": "str"}},
            {"type": "download_failed", "category": "downloads", "payload": {"videoId": "str", "error": "str"}},
            {"type": "download_status", "category": "downloads", "payload": {"videoId": "str", "status": "str"}},

            # Sleep Timer
            {"type": "sleep_timer_fired", "category": "system", "payload": {"playing": "bool"}},

            # Settings
            {"type": "settings_update", "category": "settings", "payload": {"settings": "object"}},

            # Radio / Recommendations
            {"type": "radio_generated", "category": "radio", "payload": {"count": "int", "seed": "str"}},
            {"type": "recommendation_update", "category": "recommendations", "payload": {"count": "int", "seed": "str"}},

            # Snapshots
            {"type": "state_update", "category": "system", "payload": {"state_version": "int", "...": "full state"}},
            {"type": "initial_state", "category": "system", "payload": {"state_version": "int", "state": "object"}},
        ],
        "non_state_events": [
            {"type": "connected", "payload": {"client_id": "str", "session_id": "str", "version": "str", "state_version": "int"}},
            {"type": "position_sync", "payload": {"position": "float", "duration": "float", "current_video_id": "str"}},
            {"type": "heartbeat", "payload": {"ts": "float"}},
            {"type": "checkpoint", "payload": {"state_version": "int", "event_id": "int", "server_position": "float"}},
            {"type": "download_progress", "payload": {"videoId": "str", "progress": "float", "speed_bps": "float", "eta_seconds": "float"}},
            {"type": "batch", "payload": {"events": "list[event]"}},
            {"type": "thumbnail_cached", "payload": {"videoId": "str"}},
        ],
        "client_to_server": [
            {"type": "ack", "payload": {"event_id": "int"}, "description": "Acknowledge receipt of an event"},
            {"type": "ping", "payload": {}, "description": "Keepalive ping"},
            {"type": "get_state", "payload": {}, "description": "Request full state snapshot"},
            {"type": "get_queue", "payload": {}, "description": "Request queue state"},
        ],
        "server_time": time.time(),
    }


# ─────────────────────────────────────────────────────────────────────────────
# GET /schema/errors
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/schema/errors")
async def schema_errors():
    """
    GET /schema/errors — error code registry.
    Documents all HTTP error responses and their meaning.
    """
    return {
        "success": True,
        "description": "All possible error responses from this API.",
        "http_errors": {
            "400": {
                "meaning": "Bad Request — missing or invalid body/parameter",
                "examples": ["Invalid JSON body", "videoId wajib diisi", "username dan password wajib diisi"],
            },
            "401": {
                "meaning": "Unauthorized — token missing, invalid, or expired",
                "examples": ["Autentikasi diperlukan", "Tidak terautentikasi"],
                "headers": {"WWW-Authenticate": "Bearer"},
            },
            "404": {
                "meaning": "Not Found — resource does not exist",
                "examples": ["File tidak ditemukan", "Lagu tidak ada di queue"],
            },
            "409": {
                "meaning": "Conflict — state_version mismatch (optimistic concurrency)",
                "body": {
                    "error": "state_version_mismatch",
                    "expected": "int — version client sent",
                    "latest_state_version": "int — current server version",
                },
            },
            "416": {"meaning": "Range Not Satisfiable — invalid Range header"},
            "429": {
                "meaning": "Too Many Requests — rate limit exceeded",
                "headers": {"Retry-After": "seconds to wait"},
            },
            "500": {"meaning": "Internal Server Error — unexpected server failure"},
            "502": {"meaning": "Bad Gateway — upstream proxy error (audio proxy)"},
            "504": {"meaning": "Gateway Timeout — request processing exceeded 60 seconds"},
        },
        "websocket_close_codes": {
            "1013": "Rate limited — too many connections from this IP",
            "3000": "Unauthorized — AUTH_REQUIRED=1 but no valid token",
        },
        "standard_error_body": {
            "detail": "string — human-readable error message",
        },
        "server_time": time.time(),
    }


# ─────────────────────────────────────────────────────────────────────────────
# GET /frontend/docs
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/frontend/docs")
async def frontend_docs():
    """
    GET /frontend/docs — self-describing API documentation.
    AI agents and frontends can call this to understand the API without
    reading source code.
    """
    base = f"http://localhost:{PORT}"
    return {
        "success": True,
        "title": "Music Backend API v8.0",
        "description": (
            "Production music streaming backend optimized for Termux Android. "
            "Provides playback control, queue management, search (YouTube Music + local), "
            "downloads, favorites, history, radio, recommendations, and real-time sync "
            "via WebSocket with event replay."
        ),
        "base_url": base,
        "authentication": {
            "type": "JWT Bearer (optional unless AUTH_REQUIRED=1)",
            "auth_required": AUTH_REQUIRED,
            "login": f"POST {base}/auth/login",
            "refresh": f"POST {base}/auth/refresh",
            "header": "Authorization: Bearer <access_token>",
            "ws_token": f"{base}/ws?token=<access_token>",
        },
        "quick_start": [
            {"step": 1, "description": "Load full state", "method": "GET", "url": f"{base}/bootstrap"},
            {"step": 2, "description": "Connect WebSocket for realtime sync", "url": f"ws://localhost:{PORT}/ws"},
            {"step": 3, "description": "Search for music", "method": "GET", "url": f"{base}/search?q=song+name"},
            {"step": 4, "description": "Add to queue", "method": "POST", "url": f"{base}/queue/add", "body": {"videoId": "...", "title": "...", "artist": "..."}},
            {"step": 5, "description": "Start playback", "method": "POST", "url": f"{base}/playback/play", "body": {"videoId": "..."}},
        ],
        "endpoint_groups": {
            "health": [
                {"method": "GET", "path": "/health", "description": "Basic health check"},
                {"method": "GET", "path": "/health/details", "description": "Detailed health with disk/memory/workers"},
                {"method": "GET", "path": "/monitor", "description": "Legacy monitor endpoint"},
            ],
            "state_sync": [
                {"method": "GET", "path": "/bootstrap", "description": "Full state snapshot — call on startup"},
                {"method": "GET", "path": "/state", "description": "Full state (alias)"},
                {"method": "GET", "path": "/sync", "description": "Light sync snapshot for reconnect"},
                {"method": "GET", "path": "/sync/delta?from=<event_id>", "description": "Delta events since event_id"},
                {"method": "GET", "path": "/current", "description": "Current track only (lightweight polling)"},
            ],
            "events": [
                {"method": "GET", "path": "/events/replay?from_event_id=<n>", "description": "Replay events after event_id"},
                {"method": "GET", "path": "/events/latest", "description": "Latest event_id and state_version"},
            ],
            "search": [
                {"method": "GET", "path": "/search?q=<query>&type=songs", "description": "Search YouTube Music (3-layer: cache→local→YTMusic)"},
                {"method": "GET", "path": "/search/suggest?q=<query>", "description": "Autocomplete suggestions"},
                {"method": "GET", "path": "/search/popular", "description": "Top searched queries"},
                {"method": "GET", "path": "/search/history", "description": "User search history"},
            ],
            "playback": [
                {"method": "GET", "path": "/playback", "description": "Current playback state"},
                {"method": "GET", "path": "/playback/clock", "description": "Authoritative server clock"},
                {"method": "POST", "path": "/playback/play", "description": "Play a track", "body": {"videoId": "str"}},
                {"method": "POST", "path": "/playback/pause", "description": "Pause playback"},
                {"method": "POST", "path": "/playback/seek", "description": "Seek to position", "body": {"position": "float"}},
                {"method": "POST", "path": "/playback/update", "description": "Update playback state from client"},
            ],
            "queue": [
                {"method": "GET", "path": "/queue", "description": "Current queue state"},
                {"method": "GET", "path": "/queue/next", "description": "Advance to next track"},
                {"method": "GET", "path": "/queue/prev", "description": "Go to previous track"},
                {"method": "POST", "path": "/queue/add", "description": "Add track to queue"},
                {"method": "POST", "path": "/queue/add_next", "description": "Add track to play next"},
                {"method": "POST", "path": "/queue/remove", "description": "Remove track from queue by index"},
                {"method": "POST", "path": "/queue/clear", "description": "Clear the queue"},
                {"method": "POST", "path": "/queue/jump", "description": "Jump to queue index"},
                {"method": "POST", "path": "/queue/reorder", "description": "Reorder queue item"},
                {"method": "POST", "path": "/queue/shuffle", "description": "Toggle or set shuffle"},
                {"method": "POST", "path": "/queue/repeat", "description": "Set repeat mode (none/one/all)"},
                {"method": "POST", "path": "/queue/autoplay", "description": "Toggle autoplay"},
                {"method": "POST", "path": "/queue/undo", "description": "Undo last queue modification"},
            ],
            "stream": [
                {"method": "GET", "path": "/stream?videoId=<id>", "description": "Get stream URL (cached)"},
                {"method": "GET", "path": "/audio/proxy/{videoId}", "description": "Audio proxy with Range support (hides YouTube URL)"},
                {"method": "GET", "path": "/stream/file/{videoId}", "description": "Serve locally cached audio file"},
                {"method": "GET", "path": "/stream/chunk/{videoId}", "description": "First 256KB chunk for instant start"},
                {"method": "GET", "path": "/thumb/{videoId}", "description": "Thumbnail proxy with cache"},
            ],
            "downloads": [
                {"method": "GET", "path": "/downloads", "description": "List all download tasks"},
                {"method": "POST", "path": "/download", "description": "Queue a download", "body": {"videoId": "str", "title": "str", "artist": "str"}},
                {"method": "POST", "path": "/download/pause", "description": "Pause a download"},
                {"method": "POST", "path": "/download/resume", "description": "Resume a paused download"},
                {"method": "POST", "path": "/download/cancel", "description": "Cancel a download"},
                {"method": "POST", "path": "/download/retry", "description": "Retry a failed download"},
            ],
            "library": [
                {"method": "GET", "path": "/favorites", "description": "Get favorites list"},
                {"method": "POST", "path": "/favorites/add", "description": "Add to favorites"},
                {"method": "POST", "path": "/favorites/remove", "description": "Remove from favorites"},
                {"method": "GET", "path": "/recently_played", "description": "Get listening history"},
                {"method": "POST", "path": "/recently_played/add", "description": "Add entry to history"},
                {"method": "DELETE", "path": "/recently_played", "description": "Clear history"},
                {"method": "GET", "path": "/playlist", "description": "Get local playlist"},
                {"method": "POST", "path": "/playlist/import", "description": "Import YouTube Music playlist"},
            ],
            "recommendations_radio": [
                {"method": "GET", "path": "/recommendations?seed_video=<id>", "description": "Get recommendations"},
                {"method": "GET", "path": "/recommendations/personal", "description": "Personal recommendations based on history"},
                {"method": "GET", "path": "/recommendations/similar/{videoId}", "description": "Similar tracks"},
                {"method": "POST", "path": "/radio/start", "description": "Start radio from seed"},
                {"method": "POST", "path": "/radio/next", "description": "Generate next radio batch"},
                {"method": "POST", "path": "/radio/stop", "description": "Stop radio mode"},
                {"method": "GET", "path": "/radio/status", "description": "Radio status"},
            ],
            "auth": [
                {"method": "POST", "path": "/auth/login", "description": "Login — get access+refresh tokens"},
                {"method": "POST", "path": "/auth/refresh", "description": "Rotate refresh token"},
                {"method": "POST", "path": "/auth/logout", "description": "Logout current session"},
                {"method": "POST", "path": "/auth/logout_all", "description": "Logout all sessions"},
                {"method": "GET", "path": "/auth/me", "description": "Current user info"},
                {"method": "GET", "path": "/auth/sessions", "description": "List active sessions"},
            ],
            "observability": [
                {"method": "GET", "path": "/health", "description": "Basic health check"},
                {"method": "GET", "path": "/health/details", "description": "Detailed health"},
                {"method": "GET", "path": "/metrics", "description": "Performance metrics"},
                {"method": "GET", "path": "/audit", "description": "Audit log"},
            ],
            "admin": [
                {"method": "GET", "path": "/settings", "description": "Get settings"},
                {"method": "POST", "path": "/settings", "description": "Update settings"},
                {"method": "GET", "path": "/cache/stats", "description": "Cache stats"},
                {"method": "DELETE", "path": "/cache", "description": "Clear cache"},
                {"method": "POST", "path": "/backup", "description": "Create full backup"},
                {"method": "POST", "path": "/restore", "description": "Restore from backup"},
                {"method": "POST", "path": "/batch", "description": "Batch multiple API calls"},
            ],
            "discovery": [
                {"method": "GET", "path": "/capabilities", "description": "Server capabilities"},
                {"method": "GET", "path": "/permissions", "description": "Current user permissions"},
                {"method": "GET", "path": "/schema/events", "description": "Event type registry"},
                {"method": "GET", "path": "/schema/errors", "description": "Error code registry"},
                {"method": "GET", "path": "/frontend/docs", "description": "This document"},
                {"method": "GET", "path": "/frontend/openapi", "description": "OpenAPI JSON"},
                {"method": "GET", "path": "/frontend/manifest", "description": "Frontend manifest"},
                {"method": "GET", "path": "/frontend/config", "description": "Frontend configuration"},
                {"method": "GET", "path": "/frontend/sdk", "description": "SDK generator"},
                {"method": "GET", "path": "/ai/context", "description": "AI-friendly context"},
            ],
        },
        "websocket": {
            "url": f"ws://localhost:{PORT}/ws",
            "auth_url": f"ws://localhost:{PORT}/ws?token=<access_token>",
            "resume_url": f"ws://localhost:{PORT}/ws?session_id=<sid>&last_event_id=<n>",
            "description": (
                "Connect to receive realtime events. On connect receives 'connected' + 'initial_state'. "
                "Send {type:'ack', event_id:N} to confirm receipt. "
                "On reconnect, use session_id+last_event_id to replay missed events."
            ),
        },
        "server_time": time.time(),
        "state_version": state_mgr.get_version(),
    }


# ─────────────────────────────────────────────────────────────────────────────
# GET /frontend/openapi
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/frontend/openapi")
async def frontend_openapi():
    """
    GET /frontend/openapi — OpenAPI 3.0 JSON spec.
    Suitable for import into Swagger, Postman, Insomnia, etc.
    """
    server_url = f"http://localhost:{PORT}"

    openapi = {
        "openapi": "3.0.3",
        "info": {
            "title": "Music Backend API",
            "description": "Production music streaming backend optimized for Termux Android",
            "version": "8.0.0",
        },
        "servers": [{"url": server_url, "description": "Local server"}],
        "components": {
            "securitySchemes": {
                "bearerAuth": {
                    "type": "http",
                    "scheme": "bearer",
                    "bearerFormat": "JWT",
                }
            },
            "schemas": {
                "Track": {
                    "type": "object",
                    "properties": {
                        "videoId": {"type": "string"},
                        "title": {"type": "string"},
                        "artist": {"type": "string"},
                        "album": {"type": "string"},
                        "duration": {"type": "string"},
                        "thumbnail": {"type": "string"},
                        "type": {"type": "string"},
                        "explicit": {"type": "boolean"},
                    },
                },
                "PlaybackState": {
                    "type": "object",
                    "properties": {
                        "playing": {"type": "boolean"},
                        "current_video_id": {"type": "string", "nullable": True},
                        "position": {"type": "number"},
                        "duration": {"type": "number"},
                        "updated_at": {"type": "number"},
                    },
                },
                "QueueState": {
                    "type": "object",
                    "properties": {
                        "queue": {"type": "array", "items": {"$ref": "#/components/schemas/Track"}},
                        "current_index": {"type": "integer"},
                        "shuffle": {"type": "boolean"},
                        "repeat": {"type": "string", "enum": ["none", "one", "all"]},
                        "autoplay": {"type": "boolean"},
                        "size": {"type": "integer"},
                    },
                },
                "DownloadTask": {
                    "type": "object",
                    "properties": {
                        "video_id": {"type": "string"},
                        "title": {"type": "string"},
                        "status": {"type": "string", "enum": ["queued", "downloading", "paused", "completed", "failed", "cancelled"]},
                        "progress": {"type": "number"},
                        "speed_bps": {"type": "number"},
                        "eta_seconds": {"type": "number"},
                    },
                },
                "ErrorResponse": {
                    "type": "object",
                    "properties": {"detail": {"type": "string"}},
                },
            },
        },
        "paths": {
            "/health": {
                "get": {
                    "summary": "Basic health check",
                    "tags": ["health"],
                    "responses": {
                        "200": {
                            "description": "Server is online",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "object",
                                        "properties": {
                                            "status": {"type": "string"},
                                            "version": {"type": "string"},
                                            "uptime": {"type": "string"},
                                            "memory": {"type": "string"},
                                            "cpu": {"type": "string"},
                                            "ws_clients": {"type": "integer"},
                                        },
                                    }
                                }
                            },
                        }
                    },
                }
            },
            "/health/details": {
                "get": {
                    "summary": "Detailed health check",
                    "tags": ["health"],
                    "responses": {"200": {"description": "Detailed health data"}},
                }
            },
            "/bootstrap": {
                "get": {
                    "summary": "Full state snapshot for frontend startup",
                    "tags": ["sync"],
                    "responses": {"200": {"description": "Complete application state"}},
                }
            },
            "/state": {
                "get": {
                    "summary": "Full state snapshot",
                    "tags": ["sync"],
                    "responses": {"200": {"description": "Complete application state"}},
                }
            },
            "/sync": {
                "get": {
                    "summary": "Light sync snapshot for reconnect",
                    "tags": ["sync"],
                    "responses": {"200": {"description": "Sync state"}},
                }
            },
            "/sync/delta": {
                "get": {
                    "summary": "Delta events since event_id",
                    "tags": ["sync"],
                    "parameters": [
                        {"name": "from", "in": "query", "schema": {"type": "integer"}, "description": "Last known event_id"},
                        {"name": "limit", "in": "query", "schema": {"type": "integer", "default": 500}},
                    ],
                    "responses": {"200": {"description": "List of events since from"}},
                }
            },
            "/events/replay": {
                "get": {
                    "summary": "Replay events",
                    "tags": ["events"],
                    "parameters": [
                        {"name": "from_event_id", "in": "query", "schema": {"type": "integer"}},
                        {"name": "limit", "in": "query", "schema": {"type": "integer"}},
                    ],
                    "responses": {"200": {"description": "Events list"}},
                }
            },
            "/search": {
                "get": {
                    "summary": "Search music",
                    "tags": ["search"],
                    "parameters": [
                        {"name": "q", "in": "query", "required": True, "schema": {"type": "string"}},
                        {"name": "type", "in": "query", "schema": {"type": "string", "default": "songs"}},
                        {"name": "limit", "in": "query", "schema": {"type": "integer", "default": 20}},
                    ],
                    "responses": {"200": {"description": "Search results"}},
                }
            },
            "/playback/play": {
                "post": {
                    "summary": "Play a track",
                    "tags": ["playback"],
                    "requestBody": {
                        "required": True,
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "required": ["videoId"],
                                    "properties": {
                                        "videoId": {"type": "string"},
                                        "title": {"type": "string"},
                                        "artist": {"type": "string"},
                                        "duration": {"type": "number"},
                                    },
                                }
                            }
                        },
                    },
                    "responses": {"200": {"description": "Playback started"}},
                }
            },
            "/queue/add": {
                "post": {
                    "summary": "Add track to queue",
                    "tags": ["queue"],
                    "requestBody": {
                        "content": {
                            "application/json": {
                                "schema": {"$ref": "#/components/schemas/Track"}
                            }
                        }
                    },
                    "responses": {"200": {"description": "Track added"}},
                }
            },
            "/download": {
                "post": {
                    "summary": "Queue a download",
                    "tags": ["downloads"],
                    "requestBody": {
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "required": ["videoId"],
                                    "properties": {
                                        "videoId": {"type": "string"},
                                        "title": {"type": "string"},
                                        "artist": {"type": "string"},
                                    },
                                }
                            }
                        }
                    },
                    "responses": {"200": {"description": "Download queued"}},
                }
            },
            "/auth/login": {
                "post": {
                    "summary": "Login",
                    "tags": ["auth"],
                    "requestBody": {
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "required": ["username", "password"],
                                    "properties": {
                                        "username": {"type": "string"},
                                        "password": {"type": "string"},
                                        "device": {"type": "string"},
                                    },
                                }
                            }
                        }
                    },
                    "responses": {
                        "200": {"description": "Tokens issued"},
                        "401": {"description": "Invalid credentials"},
                    },
                }
            },
            "/batch": {
                "post": {
                    "summary": "Batch multiple API calls",
                    "tags": ["admin"],
                    "requestBody": {
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "properties": {
                                        "requests": {
                                            "type": "array",
                                            "items": {
                                                "type": "object",
                                                "properties": {
                                                    "method": {"type": "string"},
                                                    "path": {"type": "string"},
                                                    "body": {"type": "object"},
                                                },
                                            },
                                        }
                                    },
                                }
                            }
                        }
                    },
                    "responses": {"200": {"description": "Batch results"}},
                }
            },
        },
        "tags": [
            {"name": "health", "description": "Health & observability"},
            {"name": "sync", "description": "State sync & bootstrap"},
            {"name": "events", "description": "Event replay & tracking"},
            {"name": "search", "description": "Music search"},
            {"name": "playback", "description": "Playback control"},
            {"name": "queue", "description": "Queue management"},
            {"name": "stream", "description": "Audio streaming"},
            {"name": "downloads", "description": "Download management"},
            {"name": "library", "description": "Favorites, history, playlist"},
            {"name": "recommendations", "description": "Recommendations & radio"},
            {"name": "auth", "description": "Authentication"},
            {"name": "admin", "description": "Admin & configuration"},
        ],
    }

    return JSONResponse(content=openapi)


# ─────────────────────────────────────────────────────────────────────────────
# GET /frontend/manifest
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/frontend/manifest")
async def frontend_manifest():
    """
    GET /frontend/manifest — frontend integration manifest.
    Describes everything a frontend needs to integrate with this backend.
    """
    base = f"http://localhost:{PORT}"
    ws_base = f"ws://localhost:{PORT}"
    return {
        "name": "music-backend",
        "version": "8.0.0",
        "base_url": base,
        "ws_url": f"{ws_base}/ws",
        "api_docs_url": f"{base}/frontend/docs",
        "openapi_url": f"{base}/frontend/openapi",
        "capabilities_url": f"{base}/capabilities",
        "state_version": state_mgr.get_version(),
        "server_time": time.time(),

        "startup_sequence": [
            f"GET {base}/capabilities — detect features",
            f"GET {base}/bootstrap — load initial state",
            f"WS {ws_base}/ws — connect for realtime events",
            f"ACK events as received — send {{type:'ack', event_id:N}}",
        ],

        "reconnect_sequence": [
            f"WS {ws_base}/ws?session_id=<sid>&last_event_id=<n> — resume session",
            f"Server replays missed events automatically",
            f"OR: GET {base}/sync/delta?from=<last_event_id> — HTTP fallback",
        ],

        "state_model": {
            "versioned": True,
            "state_version_field": "state_version",
            "event_id_field": "event_id",
            "optimistic_concurrency": True,
            "delta_sync": True,
        },

        "entity_schemas": {
            "track": {
                "videoId": "string (unique ID)",
                "title": "string",
                "artist": "string",
                "album": "string",
                "duration": "string (M:SS format)",
                "thumbnail": "string (URL, use /thumb/{videoId} for proxy)",
                "type": "string (song|video|album|etc)",
                "explicit": "boolean",
            },
            "download": {
                "video_id": "string",
                "status": "queued|downloading|paused|completed|failed|cancelled",
                "progress": "float 0-100",
                "speed_bps": "float",
                "eta_seconds": "float",
            },
        },

        "polling_fallbacks": {
            "playback_position": f"GET {base}/current (every 1-3s if WS unavailable)",
            "state": f"GET {base}/state (on demand)",
        },

        "rate_limits": {
            "search": "2/s burst 10",
            "stream": "3/s burst 8",
            "download": "1/s burst 4",
            "default": "10/s burst 50",
        },
    }


# ─────────────────────────────────────────────────────────────────────────────
# GET /frontend/config
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/frontend/config")
async def frontend_config():
    """
    GET /frontend/config — runtime configuration for frontend.
    Frontends should call this on startup to get dynamic config.
    """
    return {
        "success": True,
        "config": {
            "api_version": "8.0",
            "auth_required": AUTH_REQUIRED,
            "features": {
                "lyrics": True,
                "radio_mode": True,
                "smart_queue": True,
                "recommendation": True,
                "sponsorblock": False,
                "downloads": True,
                "audio_proxy": HAVE_AIOHTTP,
                "thumbnail_proxy": True,
                "search_suggestions": True,
                "search_history": True,
            },
            "limits": {
                "max_queue_size": MAX_QUEUE,
                "max_search_results": 20,
                "prefetch_depth": PREFETCH_DEPTH,
                "stream_cache_ttl": STREAM_CACHE_TTL,
                "recommendation_cache_ttl": RECOMMENDATION_CACHE_TTL,
            },
            "endpoints": {
                "audio_stream": f"/audio/proxy/{{videoId}}",
                "thumbnail": f"/thumb/{{videoId}}",
                "stream_fallback": f"/stream?videoId={{videoId}}",
                "websocket": "/ws",
            },
            "poll_intervals_ms": {
                "position_sync": 3000,
                "state_refresh": 30000,
                "health_check": 60000,
            },
            "websocket": {
                "heartbeat_interval_ms": 30000,
                "reconnect_delay_ms": 1000,
                "max_reconnect_delay_ms": 30000,
                "session_resume": True,
            },
        },
        "server_time": time.time(),
        "state_version": state_mgr.get_version(),
    }


# ─────────────────────────────────────────────────────────────────────────────
# GET /frontend/sdk
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/frontend/sdk")
async def frontend_sdk(lang: str = Query("typescript", description="typescript|javascript|python|dart")):
    """
    GET /frontend/sdk?lang=typescript — generate SDK stub.
    Supported: typescript, javascript, python, dart.
    """
    base = f"http://localhost:{PORT}"
    ws_base = f"ws://localhost:{PORT}"

    lang = lang.lower().strip()

    if lang in ("typescript", "ts"):
        code = f"""// Music Backend SDK — TypeScript
// Auto-generated by GET /frontend/sdk?lang=typescript
// Version: 8.0.0

const BASE_URL = "{base}";
const WS_URL = "{ws_base}/ws";

export interface Track {{
  videoId: string;
  title: string;
  artist: string;
  album?: string;
  duration?: string;
  thumbnail?: string;
  type?: string;
  explicit?: boolean;
}}

export interface PlaybackState {{
  playing: boolean;
  current_video_id: string | null;
  position: number;
  duration: number;
  updated_at: number;
}}

export class MusicBackendClient {{
  private baseUrl: string;
  private token?: string;
  private ws?: WebSocket;
  private sessionId?: string;
  private lastEventId: number = 0;
  private listeners: Map<string, Function[]> = new Map();

  constructor(baseUrl = BASE_URL) {{
    this.baseUrl = baseUrl;
  }}

  setToken(token: string) {{ this.token = token; }}

  private headers(): Record<string, string> {{
    const h: Record<string, string> = {{"Content-Type": "application/json"}};
    if (this.token) h["Authorization"] = `Bearer ${{this.token}}`;
    return h;
  }}

  private async req<T>(method: string, path: string, body?: unknown): Promise<T> {{
    const res = await fetch(`${{this.baseUrl}}${{path}}`, {{
      method, headers: this.headers(),
      body: body ? JSON.stringify(body) : undefined,
    }});
    if (!res.ok) throw new Error(`HTTP ${{res.status}}: ${{await res.text()}}`);
    return res.json();
  }}

  // Auth
  async login(username: string, password: string, device = "sdk") {{
    const r = await this.req<any>("POST", "/auth/login", {{username, password, device}});
    this.token = r.access_token;
    return r;
  }}
  async me() {{ return this.req("GET", "/auth/me"); }}

  // State
  async bootstrap() {{ return this.req("GET", "/bootstrap"); }}
  async state() {{ return this.req("GET", "/state"); }}
  async current() {{ return this.req("GET", "/current"); }}
  async syncDelta(from: number) {{ return this.req("GET", `/sync/delta?from=${{from}}`); }}

  // Playback
  async play(videoId: string, extra?: Partial<Track>) {{
    return this.req("POST", "/playback/play", {{videoId, ...extra}});
  }}
  async pause() {{ return this.req("POST", "/playback/pause"); }}
  async seek(position: number) {{ return this.req("POST", "/playback/seek", {{position}}); }}
  async getPlayback() {{ return this.req("GET", "/playback"); }}

  // Queue
  async addToQueue(track: Track) {{ return this.req("POST", "/queue/add", track); }}
  async removeFromQueue(index: number) {{ return this.req("POST", "/queue/remove", {{index}}); }}
  async clearQueue() {{ return this.req("POST", "/queue/clear"); }}
  async nextTrack() {{ return this.req("GET", "/queue/next"); }}
  async prevTrack() {{ return this.req("GET", "/queue/prev"); }}
  async jumpTo(index: number) {{ return this.req("POST", "/queue/jump", {{index}}); }}
  async setShuffle(enabled: boolean) {{ return this.req("POST", "/queue/shuffle", {{enabled}}); }}
  async setRepeat(mode: "none"|"one"|"all") {{ return this.req("POST", "/queue/repeat", {{repeat: mode}}); }}

  // Search
  async search(q: string, type = "songs", limit = 20) {{
    return this.req("GET", `/search?q=${{encodeURIComponent(q)}}&type=${{type}}&limit=${{limit}}`);
  }}
  async suggest(q: string) {{ return this.req("GET", `/search/suggest?q=${{encodeURIComponent(q)}}`); }}

  // Audio URLs
  audioUrl(videoId: string) {{ return `${{this.baseUrl}}/audio/proxy/${{videoId}}`; }}
  thumbUrl(videoId: string) {{ return `${{this.baseUrl}}/thumb/${{videoId}}`; }}

  // Downloads
  async download(track: Track) {{ return this.req("POST", "/download", track); }}
  async downloads() {{ return this.req("GET", "/downloads"); }}

  // Library
  async favorites() {{ return this.req("GET", "/favorites"); }}
  async addFavorite(track: Track) {{ return this.req("POST", "/favorites/add", track); }}
  async removeFavorite(videoId: string) {{ return this.req("POST", "/favorites/remove", {{videoId}}); }}
  async history() {{ return this.req("GET", "/recently_played"); }}

  // Recommendations
  async recommendations(seedVideoId?: string) {{
    return this.req("GET", `/recommendations${{seedVideoId ? "?seed_video="+seedVideoId : ""}}`);
  }}

  // Radio
  async startRadio(seedId: string, seedType = "track") {{
    return this.req("POST", "/radio/start", {{seed_id: seedId, seed_type: seedType}});
  }}

  // Settings
  async getSettings() {{ return this.req("GET", "/settings"); }}
  async updateSettings(settings: Record<string, unknown>) {{
    return this.req("POST", "/settings", settings);
  }}

  // WebSocket
  on(event: string, fn: Function) {{
    this.listeners.set(event, [...(this.listeners.get(event) || []), fn]);
  }}

  private emit(event: string, data: unknown) {{
    (this.listeners.get(event) || []).forEach(fn => fn(data));
    (this.listeners.get("*") || []).forEach(fn => fn({{type: event, data}}));
  }}

  connectWS(sessionId?: string) {{
    let url = WS_URL;
    const params = new URLSearchParams();
    if (this.token) params.set("token", this.token);
    if (sessionId && this.lastEventId > 0) {{
      params.set("session_id", sessionId);
      params.set("last_event_id", String(this.lastEventId));
    }}
    if (params.toString()) url += "?" + params.toString();
    this.ws = new WebSocket(url);
    this.ws.onmessage = (e) => {{
      try {{
        const msg = JSON.parse(e.data);
        if (msg.event_id) {{
          this.lastEventId = Math.max(this.lastEventId, msg.event_id);
          // Auto-ACK
          this.ws?.send(JSON.stringify({{type: "ack", event_id: msg.event_id}}));
        }}
        if (msg.session_id) this.sessionId = msg.session_id;
        this.emit(msg.type || "message", msg);
      }} catch {{}}
    }};
    this.ws.onclose = () => {{
      this.emit("disconnected", {{}});
      setTimeout(() => this.connectWS(this.sessionId), 2000);
    }};
    this.ws.onerror = (e) => this.emit("error", e);
    return this.ws;
  }}

  disconnectWS() {{ this.ws?.close(); }}

  // Batch
  async batch(requests: Array<{{method: string; path: string; body?: unknown}}>) {{
    return this.req("POST", "/batch", {{requests}});
  }}
}}

export default MusicBackendClient;
"""
    elif lang in ("javascript", "js"):
        code = f"""// Music Backend SDK — JavaScript (ESM)
// Auto-generated by GET /frontend/sdk?lang=javascript

const BASE_URL = "{base}";
const WS_URL = "{ws_base}/ws";

export class MusicBackendClient {{
  constructor(baseUrl = BASE_URL) {{
    this.baseUrl = baseUrl;
    this.token = null;
    this.ws = null;
    this.sessionId = null;
    this.lastEventId = 0;
    this.listeners = new Map();
  }}

  setToken(token) {{ this.token = token; }}

  headers() {{
    const h = {{"Content-Type": "application/json"}};
    if (this.token) h["Authorization"] = `Bearer ${{this.token}}`;
    return h;
  }}

  async req(method, path, body) {{
    const res = await fetch(`${{this.baseUrl}}${{path}}`, {{
      method, headers: this.headers(),
      body: body ? JSON.stringify(body) : undefined,
    }});
    if (!res.ok) throw new Error(`HTTP ${{res.status}}`);
    return res.json();
  }}

  async login(username, password, device = "sdk") {{
    const r = await this.req("POST", "/auth/login", {{username, password, device}});
    this.token = r.access_token;
    return r;
  }}

  async bootstrap() {{ return this.req("GET", "/bootstrap"); }}
  async play(videoId, extra = {{}}) {{ return this.req("POST", "/playback/play", {{videoId, ...extra}}); }}
  async pause() {{ return this.req("POST", "/playback/pause"); }}
  async seek(position) {{ return this.req("POST", "/playback/seek", {{position}}); }}
  async addToQueue(track) {{ return this.req("POST", "/queue/add", track); }}
  async clearQueue() {{ return this.req("POST", "/queue/clear"); }}
  async nextTrack() {{ return this.req("GET", "/queue/next"); }}
  async search(q, type = "songs") {{ return this.req("GET", `/search?q=${{encodeURIComponent(q)}}&type=${{type}}`); }}
  audioUrl(videoId) {{ return `${{this.baseUrl}}/audio/proxy/${{videoId}}`; }}
  thumbUrl(videoId) {{ return `${{this.baseUrl}}/thumb/${{videoId}}`; }}
  async download(track) {{ return this.req("POST", "/download", track); }}
  async favorites() {{ return this.req("GET", "/favorites"); }}
  async addFavorite(track) {{ return this.req("POST", "/favorites/add", track); }}

  on(event, fn) {{
    this.listeners.set(event, [...(this.listeners.get(event) || []), fn]);
  }}

  emit(event, data) {{
    (this.listeners.get(event) || []).forEach(fn => fn(data));
  }}

  connectWS() {{
    let url = WS_URL;
    if (this.token) url += `?token=${{this.token}}`;
    this.ws = new WebSocket(url);
    this.ws.onmessage = (e) => {{
      try {{
        const msg = JSON.parse(e.data);
        if (msg.event_id) {{
          this.lastEventId = Math.max(this.lastEventId, msg.event_id);
          this.ws.send(JSON.stringify({{type: "ack", event_id: msg.event_id}}));
        }}
        if (msg.session_id) this.sessionId = msg.session_id;
        this.emit(msg.type || "message", msg);
      }} catch {{}}
    }};
    this.ws.onclose = () => setTimeout(() => this.connectWS(), 2000);
  }}

  async batch(requests) {{ return this.req("POST", "/batch", {{requests}}); }}
}}

export default MusicBackendClient;
"""
    elif lang == "python":
        code = f"""# Music Backend SDK — Python
# Auto-generated by GET /frontend/sdk?lang=python
# Requires: httpx (pip install httpx websockets)

import json
import asyncio
import threading
import httpx

BASE_URL = "{base}"
WS_URL = "{ws_base}/ws"

class MusicBackendClient:
    def __init__(self, base_url=BASE_URL):
        self.base_url = base_url.rstrip("/")
        self.token = None
        self._http = httpx.Client(timeout=30)

    def _headers(self):
        h = {{"Content-Type": "application/json"}}
        if self.token:
            h["Authorization"] = f"Bearer {{self.token}}"
        return h

    def _req(self, method, path, body=None):
        url = self.base_url + path
        r = self._http.request(method, url, json=body, headers=self._headers())
        r.raise_for_status()
        return r.json()

    def login(self, username, password, device="sdk"):
        r = self._req("POST", "/auth/login", {{"username": username, "password": password, "device": device}})
        self.token = r.get("access_token")
        return r

    def bootstrap(self): return self._req("GET", "/bootstrap")
    def state(self): return self._req("GET", "/state")
    def current(self): return self._req("GET", "/current")

    def play(self, video_id, **kwargs): return self._req("POST", "/playback/play", {{"videoId": video_id, **kwargs}})
    def pause(self): return self._req("POST", "/playback/pause")
    def seek(self, position): return self._req("POST", "/playback/seek", {{"position": position}})

    def add_to_queue(self, track): return self._req("POST", "/queue/add", track)
    def clear_queue(self): return self._req("POST", "/queue/clear")
    def next_track(self): return self._req("GET", "/queue/next")
    def prev_track(self): return self._req("GET", "/queue/prev")

    def search(self, q, type="songs", limit=20):
        return self._req("GET", f"/search?q={{q}}&type={{type}}&limit={{limit}}")

    def audio_url(self, video_id): return f"{{self.base_url}}/audio/proxy/{{video_id}}"
    def thumb_url(self, video_id): return f"{{self.base_url}}/thumb/{{video_id}}"

    def download(self, track): return self._req("POST", "/download", track)
    def downloads(self): return self._req("GET", "/downloads")
    def favorites(self): return self._req("GET", "/favorites")
    def add_favorite(self, track): return self._req("POST", "/favorites/add", track)
    def remove_favorite(self, video_id): return self._req("POST", "/favorites/remove", {{"videoId": video_id}})
    def history(self): return self._req("GET", "/recently_played")

    def recommendations(self, seed_video_id=None):
        path = "/recommendations"
        if seed_video_id:
            path += f"?seed_video={{seed_video_id}}"
        return self._req("GET", path)

    def start_radio(self, seed_id, seed_type="track"):
        return self._req("POST", "/radio/start", {{"seed_id": seed_id, "seed_type": seed_type}})

    def get_settings(self): return self._req("GET", "/settings")
    def update_settings(self, settings): return self._req("POST", "/settings", settings)

    def batch(self, requests): return self._req("POST", "/batch", {{"requests": requests}})

    def close(self): self._http.close()
"""
    elif lang == "dart":
        code = f"""// Music Backend SDK — Dart
// Auto-generated by GET /frontend/sdk?lang=dart
// Requires: http package (dart pub add http)

import 'dart:convert';
import 'package:http/http.dart' as http;

const String kBaseUrl = '{base}';
const String kWsUrl = '{ws_base}/ws';

class MusicBackendClient {{
  final String baseUrl;
  String? token;
  final _client = http.Client();

  MusicBackendClient({{this.baseUrl = kBaseUrl}});

  Map<String, String> get _headers => {{
    'Content-Type': 'application/json',
    if (token != null) 'Authorization': 'Bearer $token',
  }};

  Future<dynamic> _req(String method, String path, [Map? body]) async {{
    final uri = Uri.parse('$baseUrl$path');
    http.Response res;
    final bodyStr = body != null ? jsonEncode(body) : null;
    switch (method) {{
      case 'GET': res = await _client.get(uri, headers: _headers); break;
      case 'POST': res = await _client.post(uri, headers: _headers, body: bodyStr); break;
      case 'DELETE': res = await _client.delete(uri, headers: _headers); break;
      default: throw UnsupportedError('Method $method not supported');
    }}
    if (res.statusCode >= 400) throw Exception('HTTP ${{res.statusCode}}: ${{res.body}}');
    return jsonDecode(res.body);
  }}

  Future<Map> login(String username, String password, [String device = 'sdk']) async {{
    final r = await _req('POST', '/auth/login', {{'username': username, 'password': password, 'device': device}}) as Map;
    token = r['access_token'];
    return r;
  }}

  Future<dynamic> bootstrap() => _req('GET', '/bootstrap');
  Future<dynamic> state() => _req('GET', '/state');
  Future<dynamic> current() => _req('GET', '/current');

  Future<dynamic> play(String videoId, [Map<String, dynamic>? extra]) =>
    _req('POST', '/playback/play', {{'videoId': videoId, ...?extra}});
  Future<dynamic> pause() => _req('POST', '/playback/pause');
  Future<dynamic> seek(double position) => _req('POST', '/playback/seek', {{'position': position}});

  Future<dynamic> addToQueue(Map track) => _req('POST', '/queue/add', track);
  Future<dynamic> clearQueue() => _req('POST', '/queue/clear');
  Future<dynamic> nextTrack() => _req('GET', '/queue/next');
  Future<dynamic> prevTrack() => _req('GET', '/queue/prev');

  Future<dynamic> search(String q, [String type = 'songs', int limit = 20]) =>
    _req('GET', '/search?q=${{Uri.encodeQueryComponent(q)}}&type=$type&limit=$limit');

  String audioUrl(String videoId) => '$baseUrl/audio/proxy/$videoId';
  String thumbUrl(String videoId) => '$baseUrl/thumb/$videoId';

  Future<dynamic> download(Map track) => _req('POST', '/download', track);
  Future<dynamic> downloads() => _req('GET', '/downloads');
  Future<dynamic> favorites() => _req('GET', '/favorites');
  Future<dynamic> addFavorite(Map track) => _req('POST', '/favorites/add', track);
  Future<dynamic> removeFavorite(String videoId) => _req('POST', '/favorites/remove', {{'videoId': videoId}});
  Future<dynamic> history() => _req('GET', '/recently_played');

  Future<dynamic> recommendations([String? seedVideoId]) {{
    final path = seedVideoId != null ? '/recommendations?seed_video=$seedVideoId' : '/recommendations';
    return _req('GET', path);
  }}

  Future<dynamic> getSettings() => _req('GET', '/settings');
  Future<dynamic> updateSettings(Map settings) => _req('POST', '/settings', settings);
  Future<dynamic> batch(List requests) => _req('POST', '/batch', {{'requests': requests}});

  void dispose() => _client.close();
}}
"""
    else:
        return JSONResponse(
            {"error": f"Unsupported language '{lang}'. Use: typescript, javascript, python, dart"},
            status_code=400
        )

    ext_map = {"typescript": "ts", "javascript": "js", "python": "py", "dart": "dart"}
    ext = ext_map.get(lang, "txt")
    return Response(
        content=code,
        media_type="text/plain",
        headers={"Content-Disposition": f'attachment; filename="music-backend-sdk.{ext}"'},
    )


# ─────────────────────────────────────────────────────────────────────────────
# GET /ai/context
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/ai/context")
async def ai_context():
    """
    GET /ai/context — AI-friendly context document.
    Provides everything an AI agent (ChatGPT, Claude, Gemini, Cursor, etc.)
    needs to understand and control this backend without reading source code.
    """
    base = f"http://localhost:{PORT}"
    ws_base = f"ws://localhost:{PORT}"
    pb = playback.to_dict()
    qs = queue_mgr.get_state()

    return {
        "system": "Music Backend API v8.0",
        "description": (
            "This is a self-contained music streaming backend for YouTube Music. "
            "It runs on Android (Termux), Linux VPS, or cloud platforms. "
            "No Docker or external services required. "
            "Single command: python backend/main.py"
        ),
        "current_state": {
            "playing": pb.get("playing", False),
            "current_track_id": pb.get("current_video_id"),
            "position_seconds": pb.get("position", 0),
            "duration_seconds": pb.get("duration", 0),
            "queue_size": qs.get("size", 0),
            "shuffle": qs.get("shuffle", False),
            "repeat": qs.get("repeat", "none"),
            "state_version": state_mgr.get_version(),
        },
        "how_to_play_music": [
            f"1. Search: GET {base}/search?q=song+name",
            f"2. Get videoId from results",
            f"3. Play: POST {base}/playback/play with body {{\"videoId\": \"id\"}}",
            f"4. Audio stream available at: {base}/audio/proxy/{{videoId}}",
        ],
        "how_to_manage_queue": [
            f"Add track: POST {base}/queue/add with track object",
            f"Next: GET {base}/queue/next",
            f"Prev: GET {base}/queue/prev",
            f"Clear: POST {base}/queue/clear",
            f"View: GET {base}/queue",
        ],
        "how_to_sync": [
            f"Get full state: GET {base}/bootstrap",
            f"Connect WebSocket: {ws_base}/ws",
            f"Resume after disconnect: {ws_base}/ws?session_id=X&last_event_id=N",
            f"Delta catch-up: GET {base}/sync/delta?from=N",
        ],
        "key_endpoints": {
            f"GET {base}/search?q=<query>": "Search music (required: q param)",
            f"POST {base}/playback/play": "Play track (body: {videoId, title, artist})",
            f"POST {base}/playback/pause": "Pause",
            f"POST {base}/queue/add": "Add to queue",
            f"GET {base}/queue/next": "Skip to next",
            f"GET {base}/bootstrap": "Full app state",
            f"GET {base}/favorites": "User favorites",
            f"POST {base}/download": "Download track (body: {videoId, title})",
            f"GET {base}/recommendations": "Get recommendations",
            f"POST {base}/radio/start": "Start radio (body: {seed_id, seed_type})",
            f"GET {base}/health": "Health check",
        },
        "track_object_schema": {
            "videoId": "string (required) — YouTube video ID",
            "title": "string — track title",
            "artist": "string — artist name",
            "album": "string? — album name",
            "duration": "string? — format M:SS",
            "thumbnail": "string? — image URL (or use /thumb/{videoId})",
        },
        "websocket_protocol": {
            "connect": f"{ws_base}/ws",
            "on_connect": "Receive 'connected' + 'initial_state' messages",
            "events": "All state changes broadcast as JSON events with event_id + state_version",
            "ack": "Send {type:'ack', event_id:N} to confirm receipt",
            "keepalive": "Send {type:'ping'} every 30s",
        },
        "authentication": {
            "required": AUTH_REQUIRED,
            "type": "JWT Bearer token",
            "get_token": f"POST {base}/auth/login with {{username, password}}",
            "use_token": "Add 'Authorization: Bearer <token>' header",
        },
        "feature_flags": {
            "lyrics": True,
            "radio_mode": True,
            "smart_queue": True,
            "recommendation": True,
            "sponsorblock": False,
        },
        "server_time": time.time(),
    }


# ─────────────────────────────────────────────────────────────────────────────
# GET /session/recover
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/session/recover")
async def session_recover(
    session_id: Optional[str] = Query(None),
    last_event_id: int = Query(0, ge=0),
):
    """
    GET /session/recover — HTTP fallback for session recovery.
    Returns missed events and current state for clients that cannot
    use WebSocket resume (e.g. HTTP-only environments).
    """
    latest_eid = await event_store.get_latest_event_id()
    missed_events: List[Dict] = []
    if last_event_id > 0:
        missed_events = await event_store.get_events_after(last_event_id, limit=500)

    full_state = await _build_full_state()

    return {
        "success": True,
        "session_id": session_id,
        "recovered": True,
        "state_version": state_mgr.get_version(),
        "latest_event_id": latest_eid,
        "missed_events_count": len(missed_events),
        "missed_events": missed_events,
        "state": full_state,
        "server_time": time.time(),
    }


# ─────────────────────────────────────────────────────────────────────────────
# GET /sync/delta — delta sync (alternative to /events/replay with simpler API)
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/sync/delta")
async def sync_delta(
    from_: int = Query(0, ge=0, alias="from"),
    limit: int = Query(500, ge=1, le=2000),
    request: Request = None,
):
    """
    GET /sync/delta?from=<last_event_id>
    Returns events since last_event_id + current state_version.
    Frontends use this as HTTP fallback when WebSocket is unavailable.
    """
    if request:
        ip = get_client_ip(request)
        await check_rate_limit(ip, "default")

    events = await event_store.get_events_after(from_, limit=limit)
    latest_eid = await event_store.get_latest_event_id()
    sv = state_mgr.get_version()

    return {
        "success": True,
        "from": from_,
        "to": latest_eid,
        "state_version": sv,
        "events": events,
        "count": len(events),
        "has_more": len(events) == limit,
        "server_time": time.time(),
    }


# ─────────────────────────────────────────────────────────────────────────────
# POST /backup
# ─────────────────────────────────────────────────────────────────────────────

@app.post("/backup")
async def create_backup(request: Request):
    """
    POST /backup — create a full data backup.
    Returns a ZIP file containing:
    - SQLite database snapshot (exported as SQL dump)
    - queue_state.json
    - playback_state.json
    - settings (from kv table)
    - metadata (timestamp, version)
    """
    ip = get_client_ip(request)
    await check_rate_limit(ip, "default")

    try:
        # Gather all data
        now = time.time()
        timestamp = int(now)

        # DB tables export
        rows_favorites = await db.fetch_all(
            "SELECT * FROM favorites ORDER BY favorited_at DESC"
        )
        rows_history = await db.fetch_all(
            "SELECT * FROM recently_played ORDER BY played_at DESC"
        )
        rows_playlist = await db.fetch_all(
            "SELECT * FROM playlist ORDER BY added_at DESC"
        )
        rows_search_history = await db.fetch_all(
            "SELECT * FROM search_history ORDER BY ts DESC LIMIT 200"
        )
        rows_settings = await db.fetch_all("SELECT * FROM kv")

        backup_data = {
            "metadata": {
                "version": "8.0",
                "backup_timestamp": now,
                "backup_timestamp_iso": datetime.utcfromtimestamp(now).isoformat() + "Z",
                "state_version": state_mgr.get_version(),
                "queue_size": len(queue_mgr.queue),
            },
            "favorites": rows_favorites,
            "history": rows_history,
            "playlist": rows_playlist,
            "search_history": rows_search_history,
            "settings": rows_settings,
            "queue_state": queue_mgr.get_state(),
            "playback_state": playback.to_dict(),
        }

        # Create in-memory ZIP
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("backup.json", json_dumps(backup_data))
            # Include raw queue state file if exists
            if os.path.exists(QUEUE_STATE_FILE):
                with open(QUEUE_STATE_FILE, "rb") as f:
                    zf.writestr("queue_state.json", f.read())
            if os.path.exists(PLAYBACK_STATE_FILE):
                with open(PLAYBACK_STATE_FILE, "rb") as f:
                    zf.writestr("playback_state.json", f.read())
        buf.seek(0)

        filename = f"music_backup_{timestamp}.zip"
        return StreamingResponse(
            iter([buf.read()]),
            media_type="application/zip",
            headers={
                "Content-Disposition": f'attachment; filename="{filename}"',
                "X-Backup-Timestamp": str(timestamp),
                "X-State-Version": str(state_mgr.get_version()),
            },
        )
    except Exception as e:
        logger.error(f"[BACKUP] failed: {e}")
        raise HTTPException(500, f"Backup failed: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# POST /restore
# ─────────────────────────────────────────────────────────────────────────────

@app.post("/restore")
async def restore_backup(request: Request):
    """
    POST /restore — restore from backup JSON.
    Body: JSON backup data (the backup.json from the ZIP) or multipart ZIP.
    For safety, only restores library data (favorites, history, playlist, settings).
    Does NOT reset active playback or overwrite event store.
    """
    ip = get_client_ip(request)
    await check_rate_limit(ip, "default")

    content_type = request.headers.get("content-type", "")

    try:
        if "application/json" in content_type:
            try:
                data = await request.json()
            except Exception:
                raise HTTPException(400, "Invalid JSON body")
        else:
            # Try to read raw body as JSON
            body = await request.body()
            try:
                data = json_loads(body)
            except Exception:
                raise HTTPException(400, "Could not parse backup data. Send backup.json as application/json body.")

        # Validate
        if not isinstance(data, dict):
            raise HTTPException(400, "Invalid backup format")

        meta = data.get("metadata", {})
        backup_version = meta.get("version", "unknown")

        restored: Dict[str, int] = {}

        # Restore favorites
        favs = data.get("favorites", [])
        if isinstance(favs, list):
            count = 0
            for f in favs:
                vid = f.get("video_id") or f.get("videoId")
                if not vid:
                    continue
                await db.execute(
                    "INSERT OR IGNORE INTO favorites(video_id,title,artist,thumbnail,album,duration,favorited_at) "
                    "VALUES(?,?,?,?,?,?,?)",
                    (vid, f.get("title",""), f.get("artist",""), f.get("thumbnail",""),
                     f.get("album",""), f.get("duration",""), f.get("favorited_at", time.time())),
                )
                count += 1
            restored["favorites"] = count

        # Restore history
        hist = data.get("history", [])
        if isinstance(hist, list):
            count = 0
            for h in hist:
                vid = h.get("video_id") or h.get("videoId")
                if not vid:
                    continue
                await db.execute(
                    "INSERT OR REPLACE INTO recently_played(video_id,title,artist,thumbnail,album,duration,played_at) "
                    "VALUES(?,?,?,?,?,?,?)",
                    (vid, h.get("title",""), h.get("artist",""), h.get("thumbnail",""),
                     h.get("album",""), h.get("duration",""), h.get("played_at", time.time())),
                )
                count += 1
            restored["history"] = count

        # Restore playlist
        playlist_data = data.get("playlist", [])
        if isinstance(playlist_data, list):
            count = 0
            for p in playlist_data:
                vid = p.get("video_id") or p.get("videoId")
                if not vid:
                    continue
                await db.execute(
                    "INSERT OR IGNORE INTO playlist(video_id,title,artist,thumbnail,album,duration,added_at) "
                    "VALUES(?,?,?,?,?,?,?)",
                    (vid, p.get("title",""), p.get("artist",""), p.get("thumbnail",""),
                     p.get("album",""), p.get("duration",""), p.get("added_at", time.time())),
                )
                count += 1
            restored["playlist"] = count

        # Restore settings
        settings_rows = data.get("settings", [])
        if isinstance(settings_rows, list):
            count = 0
            for row in settings_rows:
                k = row.get("k") or row.get("key")
                v = row.get("v") or row.get("value")
                if k and v is not None:
                    await db.execute(
                        "INSERT OR REPLACE INTO kv(k, v) VALUES(?, ?)",
                        (k, v if isinstance(v, str) else json_dumps(v)),
                    )
                    count += 1
            restored["settings"] = count

        # Emit event
        sv = increment_state_version()
        await ws_manager.broadcast({
            "type": "state_update",
            "state_version": sv,
            "reason": "restore",
            "server_time": time.time(),
        })

        logger.info(f"[RESTORE] completed: {restored} from backup v{backup_version}")

        return {
            "success": True,
            "message": "Restore completed successfully",
            "backup_version": backup_version,
            "restored": restored,
            "state_version": state_mgr.get_version(),
            "server_time": time.time(),
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[RESTORE] failed: {e}")
        raise HTTPException(500, f"Restore failed: {e}")
