"""Routes: upgrade2 — Final Polish (additive only, no breaking changes).

Executed in core.__dict__ namespace by routes/__init__.py.

Implements:
  1.  Universal Response Wrapper middleware + helper
  2.  GET  /schema/models          — model schema registry
  3.  GET  /schema/version         — schema versioning
  4.  GET  /frontend/sdk/typescript
  GET  /frontend/sdk/javascript
  GET  /frontend/sdk/dart
  GET  /frontend/sdk/python
  5.  GET  /feature-flags          — read feature flags
  POST /feature-flags             — set feature flags
  PATCH /feature-flags            — partial update
  6.  POST /plugins/reload
  POST /plugins/enable
  POST /plugins/disable
  GET  /plugins                   — list plugins
  7.  GET  /discover               — API discovery mode
  8.  GET  /frontend/generator     — frontend generator hints
  9.  GET  /debug/state
  GET  /debug/events
  GET  /debug/cache
  GET  /debug/workers
  GET  /debug/plugins
  10. GET  /workers                — worker monitoring
  11. GET  /cache                  — cache registry (new alias)
  POST /cache/clear               — alias for DELETE /cache
  12. Startup validation (runs once on first request, stored in kv)
"""

# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────

_SCHEMA_VERSION  = "4.0"
_API_VERSION     = "8.0"
_EVENT_VERSION   = "4.0"
_PLUGINS_DIR     = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "plugins")
_DEBUG_ENABLED   = os.environ.get("DEBUG_MODE", "1") == "1"   # on by default, set 0 to disable

# ─────────────────────────────────────────────────────────────────────────────
# UNIVERSAL RESPONSE WRAPPER HELPER
# ─────────────────────────────────────────────────────────────────────────────

def _wrap(data: Any = None, *, error: Any = None, event_id: int = 0) -> Dict:
    """
    Build the universal response envelope.
    All NEW endpoints use this; OLD endpoints are untouched (backward compat).
    """
    return {
        "success": error is None,
        "data": data,
        "error": error,
        "event_id": event_id,
        "state_version": state_mgr.get_version(),
        "server_time": time.time(),
    }


def _wrap_err(code: str, message: str, event_id: int = 0) -> Dict:
    return _wrap(None, error={"code": code, "message": message}, event_id=event_id)


# ─────────────────────────────────────────────────────────────────────────────
# STARTUP VALIDATION  (runs once, results stored in kv table)
# ─────────────────────────────────────────────────────────────────────────────

_startup_validation_done = False
_startup_report: Dict[str, Any] = {}


async def _run_startup_validation():
    global _startup_validation_done, _startup_report
    if _startup_validation_done:
        return
    _startup_validation_done = True
    checks: Dict[str, Any] = {}

    # 1. Database
    try:
        row = await db.fetch_one("SELECT COUNT(*) as c FROM sqlite_master WHERE type='table'")
        checks["database"] = {"ok": True, "tables": row["c"] if row else 0}
    except Exception as e:
        checks["database"] = {"ok": False, "error": str(e)}

    # 2. Storage dirs
    for d in (CACHE_DIR, DOWNLOAD_DIR, CHUNK_CACHE_DIR, THUMB_CACHE_DIR, TEMP_DIR):
        writable = os.access(d, os.W_OK)
        checks[f"storage_{d}"] = {"ok": writable, "path": d}

    # 3. Plugin dir
    plugin_dir_ok = os.path.isdir(_PLUGINS_DIR)
    checks["plugin_dir"] = {"ok": plugin_dir_ok, "path": _PLUGINS_DIR}

    # 4. Schema
    checks["schema"] = {"ok": True, "version": _SCHEMA_VERSION, "api_version": _API_VERSION}

    # 5. WebSocket manager
    checks["websocket"] = {"ok": True, "clients": ws_manager.count()}

    # 6. Worker pools
    checks["workers"] = {
        "ok": True,
        "io_pool": MAX_WORKERS,
        "extractor_pool": MAX_CONCURRENT_EXTRACTIONS,
        "download_pool": MAX_CONCURRENT_DOWNLOADS,
    }

    # 7. Feature flags reachable
    try:
        await _ensure_feature_flags()
        checks["feature_flags"] = {"ok": True}
    except Exception as e:
        checks["feature_flags"] = {"ok": False, "error": str(e)}

    all_ok = all(v.get("ok", False) for v in checks.values())
    _startup_report = {
        "ok": all_ok,
        "checks": checks,
        "timestamp": time.time(),
        "version": _API_VERSION,
    }
    # Persist to kv
    with suppress(Exception):
        await db.execute(
            "INSERT OR REPLACE INTO kv(k, v) VALUES(?, ?)",
            ("startup_validation", json_dumps(_startup_report)),
        )
    logger.info(f"[STARTUP_VALIDATION] ok={all_ok} checks={list(checks.keys())}")


# ─────────────────────────────────────────────────────────────────────────────
# FEATURE FLAGS  (stored in kv table, key "feature_flags")
# ─────────────────────────────────────────────────────────────────────────────

_DEFAULT_FLAGS: Dict[str, bool] = {
    "lyrics":          True,
    "radio":           True,
    "recommendation":  True,
    "smart_queue":     True,
    "sponsorblock":    False,
    "downloads":       True,
    "search_history":  True,
    "auth":            True,
    "audio_proxy":     True,
    "thumbnail_proxy": True,
    "debug":           _DEBUG_ENABLED,
    "analytics":       True,
    "backup":          True,
    "plugin_system":   True,
}

_flags_cache: Optional[Dict[str, bool]] = None
_flags_lock = asyncio.Lock() if False else None   # initialized lazily


async def _ensure_flags_lock():
    global _flags_lock
    if _flags_lock is None:
        _flags_lock = asyncio.Lock()


async def _load_feature_flags() -> Dict[str, bool]:
    global _flags_cache
    try:
        row = await db.fetch_one("SELECT v FROM kv WHERE k='feature_flags'")
        if row:
            stored = json_loads(row["v"])
            # Merge with defaults so new flags are always present
            merged = {**_DEFAULT_FLAGS, **stored}
            _flags_cache = merged
            return merged
    except Exception:
        pass
    _flags_cache = dict(_DEFAULT_FLAGS)
    return _flags_cache


async def _ensure_feature_flags() -> Dict[str, bool]:
    if _flags_cache is not None:
        return _flags_cache
    return await _load_feature_flags()


async def _save_feature_flags(flags: Dict[str, bool]):
    global _flags_cache
    _flags_cache = flags
    await db.execute(
        "INSERT OR REPLACE INTO kv(k, v) VALUES(?, ?)",
        ("feature_flags", json_dumps(flags)),
    )


@app.get("/feature-flags")
async def get_feature_flags():
    """GET /feature-flags — read all feature flags (runtime, no restart needed)."""
    flags = await _ensure_feature_flags()
    return _wrap({"flags": flags})


@app.post("/feature-flags")
async def set_feature_flags(request: Request):
    """POST /feature-flags — replace all feature flags."""
    try:
        data = await request.json()
    except Exception:
        raise HTTPException(400, "Invalid JSON")
    if not isinstance(data, dict):
        raise HTTPException(400, "Body must be a JSON object")
    # Merge with defaults; only allow boolean values
    current = await _ensure_feature_flags()
    updated = {**current}
    for k, v in data.items():
        if isinstance(k, str) and isinstance(v, bool):
            updated[k] = v
    await _save_feature_flags(updated)
    await ws_manager.broadcast({"type": "feature_flags_updated", "flags": updated, "server_time": time.time()})
    return _wrap({"flags": updated, "message": "Feature flags updated"})


@app.patch("/feature-flags")
async def patch_feature_flags(request: Request):
    """PATCH /feature-flags — partial update of feature flags."""
    try:
        data = await request.json()
    except Exception:
        raise HTTPException(400, "Invalid JSON")
    if not isinstance(data, dict):
        raise HTTPException(400, "Body must be a JSON object")
    current = await _ensure_feature_flags()
    changed = {}
    for k, v in data.items():
        if isinstance(k, str) and isinstance(v, bool):
            current[k] = v
            changed[k] = v
    await _save_feature_flags(current)
    await ws_manager.broadcast({"type": "feature_flags_updated", "flags": current, "changed": changed, "server_time": time.time()})
    return _wrap({"flags": current, "changed": changed, "message": f"{len(changed)} flag(s) updated"})


# ─────────────────────────────────────────────────────────────────────────────
# PLUGIN SYSTEM
# ─────────────────────────────────────────────────────────────────────────────

class PluginRegistry:
    """
    Lightweight plugin loader — scans plugins/ directory.
    Each plugin is a Python package or module with a register(app, core) hook.
    Pure Python, no external dependencies, Termux-safe.
    """

    def __init__(self):
        self._plugins: Dict[str, Dict] = {}   # name -> meta
        self._lock = threading.Lock()

    def _scan_dir(self) -> List[str]:
        """Return list of plugin names found in plugins/ directory."""
        if not os.path.isdir(_PLUGINS_DIR):
            return []
        names = []
        for entry in sorted(os.scandir(_PLUGINS_DIR), key=lambda e: e.name):
            if entry.is_dir() and not entry.name.startswith("_"):
                names.append(entry.name)
            elif entry.is_file() and entry.name.endswith(".py") and not entry.name.startswith("_"):
                names.append(entry.name[:-3])
        return names

    def _load_one(self, name: str) -> Dict:
        """Try to load a single plugin. Returns meta dict."""
        meta: Dict = {
            "name": name,
            "enabled": False,
            "loaded": False,
            "error": None,
            "version": "unknown",
            "description": "",
            "apis": [],
            "events": [],
            "workers": [],
        }
        # Check if disabled via feature flags (sync check via _flags_cache)
        flags = _flags_cache or _DEFAULT_FLAGS
        if not flags.get("plugin_system", True):
            meta["error"] = "plugin_system feature flag disabled"
            return meta

        # Try package path first, then .py file
        pkg_path = os.path.join(_PLUGINS_DIR, name, "__init__.py")
        mod_path = os.path.join(_PLUGINS_DIR, f"{name}.py")
        if os.path.exists(pkg_path):
            src_path = pkg_path
        elif os.path.exists(mod_path):
            src_path = mod_path
        else:
            meta["error"] = "plugin file not found"
            return meta

        try:
            import importlib.util as _ilu
            spec = _ilu.spec_from_file_location(f"plugins.{name}", src_path)
            mod = _ilu.module_from_spec(spec)
            spec.loader.exec_module(mod)

            # Call register hook if present
            if hasattr(mod, "register"):
                # Pass app and a simple context object
                ctx = _PluginContext(name, meta)
                mod.register(app, ctx)

            meta["version"] = getattr(mod, "__version__", "1.0")
            meta["description"] = getattr(mod, "__description__", "")
            meta["enabled"] = True
            meta["loaded"] = True
            logger.info(f"[PLUGIN] loaded: {name} v{meta['version']}")
        except Exception as e:
            meta["error"] = str(e)
            logger.warning(f"[PLUGIN] failed to load {name}: {e}")
        return meta

    def load_all(self):
        """Scan and load all plugins. Called during startup."""
        os.makedirs(_PLUGINS_DIR, exist_ok=True)
        names = self._scan_dir()
        with self._lock:
            for name in names:
                if name not in self._plugins:
                    meta = self._load_one(name)
                    self._plugins[name] = meta
        logger.info(f"[PLUGIN] total discovered: {len(names)}, loaded: {sum(1 for p in self._plugins.values() if p['loaded'])}")

    def reload_plugin(self, name: str) -> Dict:
        meta = self._load_one(name)
        with self._lock:
            self._plugins[name] = meta
        return meta

    def enable_plugin(self, name: str) -> Dict:
        with self._lock:
            if name in self._plugins:
                self._plugins[name]["enabled"] = True
            else:
                meta = self._load_one(name)
                self._plugins[name] = meta
        return self._plugins.get(name, {"error": "not found"})

    def disable_plugin(self, name: str) -> bool:
        with self._lock:
            if name in self._plugins:
                self._plugins[name]["enabled"] = False
                return True
        return False

    def list_all(self) -> List[Dict]:
        with self._lock:
            return list(self._plugins.values())

    def get(self, name: str) -> Optional[Dict]:
        with self._lock:
            return self._plugins.get(name)


class _PluginContext:
    """Context object passed to plugin register(app, ctx) hook."""

    def __init__(self, name: str, meta: Dict):
        self.name = name
        self._meta = meta

    def register_api(self, path: str, description: str = ""):
        self._meta["apis"].append({"path": path, "description": description})

    def register_event(self, event_type: str, description: str = ""):
        self._meta["events"].append({"type": event_type, "description": description})

    def register_worker(self, worker_name: str, description: str = ""):
        self._meta["workers"].append({"name": worker_name, "description": description})

    def emit_event(self, event_type: str, payload: Dict):
        """Fire-and-forget event broadcast from plugin."""
        if _loop:
            asyncio.run_coroutine_threadsafe(
                ws_manager.broadcast({"type": event_type, **payload, "source": f"plugin:{self.name}"}),
                _loop,
            )

    def log(self, msg: str):
        logger.info(f"[PLUGIN:{self.name}] {msg}")


plugin_registry = PluginRegistry()
# Load plugins immediately (runs at route load time, before first request)
plugin_registry.load_all()


@app.get("/plugins")
async def list_plugins():
    """GET /plugins — list all discovered plugins and their status."""
    await _run_startup_validation()
    plugins = plugin_registry.list_all()
    return _wrap({
        "plugins": plugins,
        "total": len(plugins),
        "loaded": sum(1 for p in plugins if p.get("loaded")),
        "enabled": sum(1 for p in plugins if p.get("enabled")),
        "plugins_dir": _PLUGINS_DIR,
    })


@app.post("/plugins/reload")
async def reload_plugin(request: Request):
    """POST /plugins/reload — reload a plugin by name (hot reload)."""
    try:
        data = await request.json()
    except Exception:
        raise HTTPException(400, "Invalid JSON")
    name = (data.get("name") or "").strip()
    if not name:
        raise HTTPException(400, "Plugin name required")
    meta = plugin_registry.reload_plugin(name)
    return _wrap({"plugin": meta, "reloaded": meta.get("loaded", False)})


@app.post("/plugins/enable")
async def enable_plugin(request: Request):
    """POST /plugins/enable — enable a plugin by name."""
    try:
        data = await request.json()
    except Exception:
        raise HTTPException(400, "Invalid JSON")
    name = (data.get("name") or "").strip()
    if not name:
        raise HTTPException(400, "Plugin name required")
    meta = plugin_registry.enable_plugin(name)
    return _wrap({"plugin": meta, "enabled": meta.get("enabled", False)})


@app.post("/plugins/disable")
async def disable_plugin(request: Request):
    """POST /plugins/disable — disable a plugin by name (no restart needed)."""
    try:
        data = await request.json()
    except Exception:
        raise HTTPException(400, "Invalid JSON")
    name = (data.get("name") or "").strip()
    if not name:
        raise HTTPException(400, "Plugin name required")
    ok = plugin_registry.disable_plugin(name)
    return _wrap({"name": name, "disabled": ok})


# ─────────────────────────────────────────────────────────────────────────────
# UNIFIED EVENT NAMING — standardized SCREAMING_SNAKE_CASE aliases
# ─────────────────────────────────────────────────────────────────────────────
# Old event names are preserved 100%. These are ADDITIONAL canonical names
# that fire alongside the original events via the event_bus compatibility shim.

_EVENT_ALIAS_MAP: Dict[str, str] = {
    # Old name          →  New canonical name
    "play":              "PLAYBACK_STARTED",
    "pause":             "PLAYBACK_PAUSED",
    "resume":            "PLAYBACK_RESUMED",
    "playback_state":    "PLAYBACK_STOPPED",   # used when playing=False
    "queue_updated":     "QUEUE_UPDATED",
    "queue_clear":       "QUEUE_CLEARED",
    "queue_reorder":     "QUEUE_REORDERED",
    "download_started":  "DOWNLOAD_STARTED",
    "download_progress": "DOWNLOAD_PROGRESS",
    "download_completed":"DOWNLOAD_FINISHED",
    "download_complete": "DOWNLOAD_FINISHED",
    "history_add":       "HISTORY_UPDATED",
    "favorite_add":      "FAVORITES_UPDATED",
    "favorite_remove":   "FAVORITES_UPDATED",
    "settings_update":   "SETTINGS_UPDATED",
    "radio_generated":   "RADIO_UPDATED",
    "recommendation_update": "RECOMMENDATIONS_UPDATED",
    "stream_url_refreshed": "STREAM_REFRESHED",
}

# Inject alias broadcast into ws_manager.broadcast (non-invasive monkey-patch)
_original_broadcast = ws_manager.broadcast.__func__ if hasattr(ws_manager.broadcast, "__func__") else None


async def _alias_broadcast_shim(self, event: Dict):
    """Wrap original broadcast to also emit canonical event name."""
    # Call original
    orig_type = event.get("type", "")
    # Fire original
    await WSManager.broadcast(self, event)
    # Fire canonical alias if different
    canonical = _EVENT_ALIAS_MAP.get(orig_type)
    if canonical and canonical != orig_type:
        alias_event = {**event, "type": canonical, "_original_type": orig_type}
        await WSManager.broadcast(self, alias_event)


# Only patch once (idempotent)
if not getattr(ws_manager, "_alias_patched", False):
    import types as _types
    ws_manager.broadcast = _types.MethodType(_alias_broadcast_shim, ws_manager)
    ws_manager._alias_patched = True


# ─────────────────────────────────────────────────────────────────────────────
# SCHEMA MODELS REGISTRY
# ─────────────────────────────────────────────────────────────────────────────

_MODEL_SCHEMAS: Dict[str, Dict] = {
    "Track": {
        "description": "A playable music track",
        "fields": {
            "videoId":    {"type": "string", "required": True,  "description": "YouTube video ID"},
            "title":      {"type": "string", "required": False, "description": "Track title"},
            "artist":     {"type": "string", "required": False, "description": "Artist name"},
            "album":      {"type": "string", "required": False, "description": "Album name"},
            "duration":   {"type": "string", "required": False, "description": "Duration string M:SS"},
            "thumbnail":  {"type": "string", "required": False, "description": "Thumbnail URL"},
            "type":       {"type": "string", "required": False, "description": "song|video|album|etc"},
            "explicit":   {"type": "boolean","required": False, "description": "Explicit content flag"},
            "browseId":   {"type": "string", "required": False, "description": "YT Browse ID"},
            "artistBrowseId": {"type": "string", "required": False, "description": "Artist Browse ID"},
        },
        "example": {
            "videoId": "dQw4w9WgXcQ",
            "title": "Never Gonna Give You Up",
            "artist": "Rick Astley",
            "album": "Whenever You Need Somebody",
            "duration": "3:32",
            "thumbnail": "https://i.ytimg.com/vi/dQw4w9WgXcQ/hqdefault.jpg",
            "type": "song",
            "explicit": False,
        },
    },

    "QueueItem": {
        "description": "An item in the playback queue (extends Track)",
        "fields": {
            "videoId":   {"type": "string",  "required": True},
            "title":     {"type": "string",  "required": False},
            "artist":    {"type": "string",  "required": False},
            "album":     {"type": "string",  "required": False},
            "duration":  {"type": "string",  "required": False},
            "thumbnail": {"type": "string",  "required": False},
            "_queue_index": {"type": "integer", "required": False, "description": "Position in queue (runtime only)"},
        },
    },

    "QueueState": {
        "description": "Current state of the playback queue",
        "fields": {
            "queue":         {"type": "array[QueueItem]", "required": True},
            "current_index": {"type": "integer",          "required": True,  "description": "Index of currently playing track (-1 if none)"},
            "shuffle":       {"type": "boolean",          "required": True},
            "repeat":        {"type": "string",           "required": True,  "enum": ["none", "one", "all"]},
            "autoplay":      {"type": "boolean",          "required": True},
            "size":          {"type": "integer",          "required": True},
        },
    },

    "PlaybackState": {
        "description": "Current playback status",
        "fields": {
            "playing":          {"type": "boolean", "required": True},
            "current_video_id": {"type": "string",  "required": False, "nullable": True},
            "position":         {"type": "number",  "required": True,  "description": "Current position in seconds"},
            "duration":         {"type": "number",  "required": True,  "description": "Track duration in seconds"},
            "updated_at":       {"type": "number",  "required": True,  "description": "Unix timestamp of last update"},
            "sleep_timer_end":  {"type": "number",  "required": False, "nullable": True},
        },
    },

    "Download": {
        "description": "A download task",
        "fields": {
            "video_id":         {"type": "string",  "required": True},
            "title":            {"type": "string",  "required": False},
            "artist":           {"type": "string",  "required": False},
            "status":           {"type": "string",  "required": True,
                                 "enum": ["queued","downloading","paused","completed","failed","cancelled"]},
            "progress":         {"type": "number",  "required": True,  "description": "0.0 – 100.0"},
            "total_bytes":      {"type": "integer", "required": False},
            "downloaded_bytes": {"type": "integer", "required": False},
            "speed_bps":        {"type": "number",  "required": False},
            "eta_seconds":      {"type": "number",  "required": False},
            "filename":         {"type": "string",  "required": False},
            "filepath":         {"type": "string",  "required": False},
            "error":            {"type": "string",  "required": False, "nullable": True},
            "started_at":       {"type": "number",  "required": False},
            "finished_at":      {"type": "number",  "required": False},
        },
    },

    "Playlist": {
        "description": "A local playlist entry",
        "fields": {
            "video_id":  {"type": "string", "required": True},
            "title":     {"type": "string", "required": False},
            "artist":    {"type": "string", "required": False},
            "album":     {"type": "string", "required": False},
            "duration":  {"type": "string", "required": False},
            "thumbnail": {"type": "string", "required": False},
            "added_at":  {"type": "number", "required": False},
        },
    },

    "Session": {
        "description": "An authenticated user session",
        "fields": {
            "session_id":  {"type": "string", "required": True},
            "username":    {"type": "string", "required": True},
            "device_info": {"type": "string", "required": False},
            "created_at":  {"type": "number", "required": True},
            "last_used":   {"type": "number", "required": True},
            "expires_at":  {"type": "number", "required": True},
        },
    },

    "SearchResult": {
        "description": "A search result item (conforms to Track schema + search metadata)",
        "fields": {
            "videoId":   {"type": "string",  "required": True},
            "title":     {"type": "string",  "required": True},
            "artist":    {"type": "string",  "required": False},
            "album":     {"type": "string",  "required": False},
            "duration":  {"type": "string",  "required": False},
            "thumbnail": {"type": "string",  "required": False},
            "source":    {"type": "string",  "required": False, "description": "cache|local|ytmusic"},
            "playable":  {"type": "boolean", "required": True},
        },
    },

    "Event": {
        "description": "A server-sent WebSocket event envelope",
        "fields": {
            "type":             {"type": "string",  "required": True},
            "event_id":         {"type": "integer", "required": True},
            "state_version":    {"type": "integer", "required": True},
            "server_timestamp": {"type": "number",  "required": True},
            "replayed":         {"type": "boolean", "required": False},
            "resent":           {"type": "boolean", "required": False},
        },
    },

    "UniversalResponse": {
        "description": "Standard API response envelope",
        "fields": {
            "success":       {"type": "boolean", "required": True},
            "data":          {"type": "any",     "required": False, "nullable": True},
            "error":         {"type": "object",  "required": False, "nullable": True,
                              "schema": {"code": "string", "message": "string"}},
            "event_id":      {"type": "integer", "required": True},
            "state_version": {"type": "integer", "required": True},
            "server_time":   {"type": "number",  "required": True},
        },
    },

    "FeatureFlags": {
        "description": "Runtime feature flag map",
        "fields": {
            "lyrics":          {"type": "boolean"},
            "radio":           {"type": "boolean"},
            "recommendation":  {"type": "boolean"},
            "smart_queue":     {"type": "boolean"},
            "sponsorblock":    {"type": "boolean"},
            "downloads":       {"type": "boolean"},
            "search_history":  {"type": "boolean"},
            "auth":            {"type": "boolean"},
            "audio_proxy":     {"type": "boolean"},
            "thumbnail_proxy": {"type": "boolean"},
            "debug":           {"type": "boolean"},
            "analytics":       {"type": "boolean"},
            "backup":          {"type": "boolean"},
            "plugin_system":   {"type": "boolean"},
        },
    },
}


@app.get("/schema/models")
async def schema_models():
    """GET /schema/models — full model schema registry for AI/frontend code generation."""
    return _wrap({
        "models": _MODEL_SCHEMAS,
        "count": len(_MODEL_SCHEMAS),
        "schema_version": _SCHEMA_VERSION,
    })


# ─────────────────────────────────────────────────────────────────────────────
# SCHEMA VERSION
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/schema/version")
async def schema_version_endpoint():
    """GET /schema/version — API, schema, and event version numbers."""
    return _wrap({
        "api_version":    _API_VERSION,
        "schema_version": _SCHEMA_VERSION,
        "event_version":  _EVENT_VERSION,
        "backend_version": "8.0",
    })


# ─────────────────────────────────────────────────────────────────────────────
# SDK GENERATOR — per-language dedicated endpoints
# ─────────────────────────────────────────────────────────────────────────────

def _build_typescript_sdk() -> str:
    base = f"http://localhost:{PORT}"
    ws_base = f"ws://localhost:{PORT}"
    lines: List[str] = [
        "// Music Backend SDK — TypeScript",
        f"// Auto-generated from GET /frontend/sdk/typescript",
        f"// API version: {_API_VERSION}  Schema: {_SCHEMA_VERSION}",
        "",
    ]
    # Interfaces from model registry
    for model_name, model in _MODEL_SCHEMAS.items():
        lines.append(f"/** {model.get('description', model_name)} */")
        lines.append(f"export interface {model_name} {{")
        for field, info in model.get("fields", {}).items():
            ts_type = {
                "string": "string", "number": "number", "boolean": "boolean",
                "integer": "number", "any": "unknown",
            }.get(info.get("type", "string").split("[")[0], "unknown")
            if info.get("type", "").startswith("array["):
                inner = info["type"][6:-1]
                ts_type = f"{inner}[]"
            nullable = "?" if (info.get("nullable") or not info.get("required", True)) else ""
            comment = f" // {info['description']}" if info.get("description") else ""
            lines.append(f"  {field}{nullable}: {ts_type};{comment}")
        lines.append("}")
        lines.append("")

    lines += [
        f'const BASE_URL = "{base}";',
        f'const WS_URL   = "{ws_base}/ws";',
        "",
        "export class MusicBackendClient {",
        "  private baseUrl: string;",
        "  private token?: string;",
        "  private ws?: WebSocket;",
        "  private sessionId?: string;",
        "  private lastEventId = 0;",
        "  private listeners = new Map<string, Function[]>();",
        "",
        "  constructor(baseUrl = BASE_URL) { this.baseUrl = baseUrl; }",
        "  setToken(t: string) { this.token = t; }",
        "",
        "  private headers(): Record<string, string> {",
        '    const h: Record<string, string> = {"Content-Type": "application/json"};',
        '    if (this.token) h["Authorization"] = `Bearer ${this.token}`;',
        "    return h;",
        "  }",
        "",
        "  private async req<T>(method: string, path: string, body?: unknown): Promise<T> {",
        "    const r = await fetch(`${this.baseUrl}${path}`, {",
        "      method, headers: this.headers(),",
        "      body: body ? JSON.stringify(body) : undefined });",
        "    if (!r.ok) throw new Error(`HTTP ${r.status}`);",
        "    const json = await r.json();",
        "    // Unwrap universal response envelope if present",
        '    return ("success" in json && "data" in json) ? json.data : json;',
        "  }",
        "",
        "  // Auth",
        "  async login(u: string, p: string, d = 'sdk') {",
        "    const r: any = await fetch(`${this.baseUrl}/auth/login`, {",
        '      method: "POST", headers: this.headers(),',
        "      body: JSON.stringify({username:u, password:p, device:d}) }).then(r=>r.json());",
        "    if (r.access_token) this.token = r.access_token;",
        "    return r;",
        "  }",
        "  async me() { return this.req('GET', '/auth/me'); }",
        "",
        "  // State",
        "  async bootstrap()           { return this.req<UniversalResponse>('GET', '/bootstrap'); }",
        "  async state()               { return this.req('GET', '/state'); }",
        "  async current()             { return this.req('GET', '/current'); }",
        "  async syncDelta(from: number) { return this.req('GET', `/sync/delta?from=${from}`); }",
        "  async featureFlags()        { return this.req<FeatureFlags>('GET', '/feature-flags'); }",
        "",
        "  // Playback",
        "  async play(videoId: string, extra?: Partial<Track>) { return this.req('POST', '/playback/play', {videoId, ...extra}); }",
        "  async pause()               { return this.req('POST', '/playback/pause'); }",
        "  async seek(position: number){ return this.req('POST', '/playback/seek', {position}); }",
        "  async getPlayback()         { return this.req<PlaybackState>('GET', '/playback'); }",
        "",
        "  // Queue",
        "  async addToQueue(t: Track)  { return this.req('POST', '/queue/add', t); }",
        "  async clearQueue()          { return this.req('POST', '/queue/clear'); }",
        "  async nextTrack()           { return this.req('GET', '/queue/next'); }",
        "  async prevTrack()           { return this.req('GET', '/queue/prev'); }",
        "  async jumpTo(i: number)     { return this.req('POST', '/queue/jump', {index:i}); }",
        "  async setShuffle(v: boolean){ return this.req('POST', '/queue/shuffle', {enabled:v}); }",
        "  async setRepeat(m: 'none'|'one'|'all') { return this.req('POST', '/queue/repeat', {repeat:m}); }",
        "",
        "  // Search",
        "  async search(q: string, type='songs', limit=20) {",
        "    return this.req<SearchResult[]>('GET', `/search?q=${encodeURIComponent(q)}&type=${type}&limit=${limit}`);",
        "  }",
        "  async suggest(q: string)    { return this.req('GET', `/search/suggest?q=${encodeURIComponent(q)}`); }",
        "",
        "  // Media URLs",
        "  audioUrl(videoId: string)   { return `${this.baseUrl}/audio/proxy/${videoId}`; }",
        "  thumbUrl(videoId: string)   { return `${this.baseUrl}/thumb/${videoId}`; }",
        "",
        "  // Downloads",
        "  async download(t: Track)    { return this.req<Download>('POST', '/download', t); }",
        "  async downloads()           { return this.req<Download[]>('GET', '/downloads'); }",
        "",
        "  // Library",
        "  async favorites()           { return this.req('GET', '/favorites'); }",
        "  async addFavorite(t: Track) { return this.req('POST', '/favorites/add', t); }",
        "  async removeFavorite(id: string) { return this.req('POST', '/favorites/remove', {videoId:id}); }",
        "  async history()             { return this.req('GET', '/recently_played'); }",
        "",
        "  // Recommendations & Radio",
        "  async recommendations(seed?: string) { return this.req('GET', `/recommendations${seed?'?seed_video='+seed:''}`); }",
        "  async startRadio(seedId: string, seedType='track') { return this.req('POST', '/radio/start', {seed_id:seedId,seed_type:seedType}); }",
        "",
        "  // Settings & Feature Flags",
        "  async getSettings()         { return this.req('GET', '/settings'); }",
        "  async updateSettings(s: Record<string,unknown>) { return this.req('POST', '/settings', s); }",
        "  async patchFlags(f: Partial<FeatureFlags>) { return this.req('PATCH', '/feature-flags', f); }",
        "",
        "  // WebSocket",
        "  on(ev: string, fn: Function) {",
        "    this.listeners.set(ev, [...(this.listeners.get(ev)||[]), fn]);",
        "  }",
        "  private emit(ev: string, d: unknown) {",
        "    (this.listeners.get(ev)||[]).forEach(f=>f(d));",
        "    (this.listeners.get('*')||[]).forEach(f=>f({type:ev,data:d}));",
        "  }",
        "  connectWS(sessionId?: string) {",
        "    let url = WS_URL;",
        "    const p = new URLSearchParams();",
        "    if (this.token) p.set('token', this.token);",
        "    if (sessionId && this.lastEventId>0) { p.set('session_id',sessionId); p.set('last_event_id',String(this.lastEventId)); }",
        "    if (p.toString()) url += '?'+p.toString();",
        "    this.ws = new WebSocket(url);",
        "    this.ws.onmessage = e => {",
        "      try {",
        "        const m = JSON.parse(e.data);",
        "        if (m.event_id) { this.lastEventId=Math.max(this.lastEventId,m.event_id); this.ws!.send(JSON.stringify({type:'ack',event_id:m.event_id})); }",
        "        if (m.session_id) this.sessionId = m.session_id;",
        "        this.emit(m.type||'message', m);",
        "      } catch {}",
        "    };",
        "    this.ws.onclose = () => { this.emit('disconnected',{}); setTimeout(()=>this.connectWS(this.sessionId),2000); };",
        "    return this.ws;",
        "  }",
        "  disconnectWS() { this.ws?.close(); }",
        "  async batch(reqs: {method:string;path:string;body?:unknown}[]) { return this.req('POST','/batch',{requests:reqs}); }",
        "}",
        "",
        "export default MusicBackendClient;",
    ]
    return "\n".join(lines)


def _build_javascript_sdk() -> str:
    base = f"http://localhost:{PORT}"
    ws_base = f"ws://localhost:{PORT}"
    return f"""// Music Backend SDK — JavaScript (ESM)
// Auto-generated from GET /frontend/sdk/javascript
// API version: {_API_VERSION}  Schema: {_SCHEMA_VERSION}

const BASE_URL = "{base}";
const WS_URL   = "{ws_base}/ws";

export class MusicBackendClient {{
  constructor(baseUrl = BASE_URL) {{
    this.baseUrl = baseUrl; this.token = null;
    this.ws = null; this.sessionId = null; this.lastEventId = 0;
    this.listeners = new Map();
  }}
  setToken(t) {{ this.token = t; }}
  headers() {{
    const h = {{"Content-Type":"application/json"}};
    if (this.token) h["Authorization"] = `Bearer ${{this.token}}`;
    return h;
  }}
  async req(method, path, body) {{
    const r = await fetch(`${{this.baseUrl}}${{path}}`, {{
      method, headers: this.headers(), body: body ? JSON.stringify(body) : undefined }});
    if (!r.ok) throw new Error(`HTTP ${{r.status}}`);
    const j = await r.json();
    return ("success" in j && "data" in j) ? j.data : j;
  }}
  async login(u, p, d="sdk") {{
    const r = await fetch(`${{this.baseUrl}}/auth/login`, {{
      method:"POST", headers:this.headers(), body:JSON.stringify({{username:u,password:p,device:d}}) }}).then(r=>r.json());
    if (r.access_token) this.token = r.access_token; return r;
  }}
  async bootstrap() {{ return this.req("GET","/bootstrap"); }}
  async state()     {{ return this.req("GET","/state"); }}
  async current()   {{ return this.req("GET","/current"); }}
  async syncDelta(from) {{ return this.req("GET",`/sync/delta?from=${{from}}`); }}
  async featureFlags()  {{ return this.req("GET","/feature-flags"); }}
  async play(videoId, extra={{}}) {{ return this.req("POST","/playback/play",{{videoId,...extra}}); }}
  async pause()     {{ return this.req("POST","/playback/pause"); }}
  async seek(pos)   {{ return this.req("POST","/playback/seek",{{position:pos}}); }}
  async addToQueue(t) {{ return this.req("POST","/queue/add",t); }}
  async clearQueue()  {{ return this.req("POST","/queue/clear"); }}
  async nextTrack()   {{ return this.req("GET","/queue/next"); }}
  async prevTrack()   {{ return this.req("GET","/queue/prev"); }}
  async search(q, type="songs", limit=20) {{
    return this.req("GET",`/search?q=${{encodeURIComponent(q)}}&type=${{type}}&limit=${{limit}}`);
  }}
  audioUrl(videoId) {{ return `${{this.baseUrl}}/audio/proxy/${{videoId}}`; }}
  thumbUrl(videoId) {{ return `${{this.baseUrl}}/thumb/${{videoId}}`; }}
  async download(t) {{ return this.req("POST","/download",t); }}
  async downloads() {{ return this.req("GET","/downloads"); }}
  async favorites() {{ return this.req("GET","/favorites"); }}
  async addFavorite(t) {{ return this.req("POST","/favorites/add",t); }}
  async removeFavorite(id) {{ return this.req("POST","/favorites/remove",{{videoId:id}}); }}
  async history() {{ return this.req("GET","/recently_played"); }}
  async recommendations(seed) {{
    return this.req("GET",`/recommendations${{seed?"?seed_video="+seed:""}}`);
  }}
  async startRadio(seedId, seedType="track") {{
    return this.req("POST","/radio/start",{{seed_id:seedId,seed_type:seedType}});
  }}
  async patchFlags(f) {{ return this.req("PATCH","/feature-flags",f); }}
  on(ev, fn) {{ this.listeners.set(ev,[...(this.listeners.get(ev)||[]),fn]); }}
  emit(ev, d) {{ (this.listeners.get(ev)||[]).forEach(f=>f(d)); }}
  connectWS() {{
    let url = WS_URL;
    if (this.token) url+=`?token=${{this.token}}`;
    this.ws = new WebSocket(url);
    this.ws.onmessage = e => {{
      try {{
        const m = JSON.parse(e.data);
        if (m.event_id) {{ this.lastEventId=Math.max(this.lastEventId,m.event_id); this.ws.send(JSON.stringify({{type:"ack",event_id:m.event_id}})); }}
        if (m.session_id) this.sessionId = m.session_id;
        this.emit(m.type||"message",m);
      }} catch {{}}
    }};
    this.ws.onclose = () => setTimeout(()=>this.connectWS(),2000);
    return this.ws;
  }}
  disconnectWS() {{ this.ws?.close(); }}
  async batch(reqs) {{ return this.req("POST","/batch",{{requests:reqs}}); }}
}}
export default MusicBackendClient;
"""


def _build_python_sdk() -> str:
    base = f"http://localhost:{PORT}"
    return f"""# Music Backend SDK — Python
# Auto-generated from GET /frontend/sdk/python
# API version: {_API_VERSION}  Schema: {_SCHEMA_VERSION}
# Requires: pip install httpx

import httpx

BASE_URL = "{base}"

class MusicBackendClient:
    def __init__(self, base_url=BASE_URL):
        self.base_url = base_url.rstrip("/")
        self.token = None
        self._c = httpx.Client(timeout=30)

    def _h(self):
        h = {{"Content-Type": "application/json"}}
        if self.token: h["Authorization"] = f"Bearer {{self.token}}"
        return h

    def _req(self, method, path, body=None):
        r = self._c.request(method, self.base_url+path, json=body, headers=self._h())
        r.raise_for_status()
        j = r.json()
        return j.get("data", j) if ("success" in j and "data" in j) else j

    def login(self, username, password, device="sdk"):
        r = self._c.post(self.base_url+"/auth/login", json={{"username":username,"password":password,"device":device}}, headers=self._h()).json()
        self.token = r.get("access_token"); return r

    def bootstrap(self):      return self._req("GET", "/bootstrap")
    def state(self):          return self._req("GET", "/state")
    def current(self):        return self._req("GET", "/current")
    def sync_delta(self, from_id): return self._req("GET", f"/sync/delta?from={{from_id}}")
    def feature_flags(self):  return self._req("GET", "/feature-flags")
    def patch_flags(self, f): return self._req("PATCH", "/feature-flags", f)

    def play(self, video_id, **kw): return self._req("POST", "/playback/play", {{"videoId":video_id,**kw}})
    def pause(self):          return self._req("POST", "/playback/pause")
    def seek(self, pos):      return self._req("POST", "/playback/seek", {{"position":pos}})
    def get_playback(self):   return self._req("GET", "/playback")

    def add_to_queue(self, t): return self._req("POST", "/queue/add", t)
    def clear_queue(self):    return self._req("POST", "/queue/clear")
    def next_track(self):     return self._req("GET", "/queue/next")
    def prev_track(self):     return self._req("GET", "/queue/prev")
    def jump_to(self, i):     return self._req("POST", "/queue/jump", {{"index":i}})
    def set_shuffle(self, v): return self._req("POST", "/queue/shuffle", {{"enabled":v}})
    def set_repeat(self, m):  return self._req("POST", "/queue/repeat", {{"repeat":m}})

    def search(self, q, type="songs", limit=20):
        return self._req("GET", f"/search?q={{q}}&type={{type}}&limit={{limit}}")
    def suggest(self, q):     return self._req("GET", f"/search/suggest?q={{q}}")

    def audio_url(self, vid): return f"{{self.base_url}}/audio/proxy/{{vid}}"
    def thumb_url(self, vid): return f"{{self.base_url}}/thumb/{{vid}}"

    def download(self, t):    return self._req("POST", "/download", t)
    def downloads(self):      return self._req("GET", "/downloads")
    def favorites(self):      return self._req("GET", "/favorites")
    def add_favorite(self, t): return self._req("POST", "/favorites/add", t)
    def remove_favorite(self, vid): return self._req("POST", "/favorites/remove", {{"videoId":vid}})
    def history(self):        return self._req("GET", "/recently_played")

    def recommendations(self, seed=None):
        p = f"/recommendations?seed_video={{seed}}" if seed else "/recommendations"
        return self._req("GET", p)
    def start_radio(self, seed_id, seed_type="track"):
        return self._req("POST", "/radio/start", {{"seed_id":seed_id,"seed_type":seed_type}})

    def get_settings(self):   return self._req("GET", "/settings")
    def update_settings(self, s): return self._req("POST", "/settings", s)
    def schema_models(self):  return self._req("GET", "/schema/models")
    def capabilities(self):   return self._req("GET", "/capabilities")
    def batch(self, reqs):    return self._req("POST", "/batch", {{"requests":reqs}})
    def close(self):          self._c.close()
"""


def _build_dart_sdk() -> str:
    base = f"http://localhost:{PORT}"
    return f"""// Music Backend SDK — Dart
// Auto-generated from GET /frontend/sdk/dart
// API version: {_API_VERSION}  Schema: {_SCHEMA_VERSION}
// Add to pubspec.yaml:  http: ^1.0.0

import 'dart:convert';
import 'package:http/http.dart' as http;

const String kBaseUrl = '{base}';

class MusicBackendClient {{
  final String baseUrl;
  String? token;
  final _c = http.Client();
  MusicBackendClient({{this.baseUrl = kBaseUrl}});

  Map<String,String> get _h => {{
    'Content-Type':'application/json',
    if (token != null) 'Authorization':'Bearer $token',
  }};

  Future<dynamic> _req(String method, String path, [Map? body]) async {{
    final uri = Uri.parse('$baseUrl$path');
    final b = body != null ? jsonEncode(body) : null;
    http.Response r;
    switch(method) {{
      case 'GET':   r = await _c.get(uri, headers:_h); break;
      case 'POST':  r = await _c.post(uri, headers:_h, body:b); break;
      case 'PATCH': r = await _c.patch(uri, headers:_h, body:b); break;
      case 'DELETE':r = await _c.delete(uri, headers:_h); break;
      default: throw UnsupportedError(method);
    }}
    if (r.statusCode >= 400) throw Exception('HTTP ${{r.statusCode}}: ${{r.body}}');
    final j = jsonDecode(r.body);
    if (j is Map && j.containsKey('success') && j.containsKey('data')) return j['data'];
    return j;
  }}

  Future<Map> login(String u, String p, [String d='sdk']) async {{
    final r = await _c.post(Uri.parse('$baseUrl/auth/login'),
      headers:_h, body:jsonEncode({{'username':u,'password':p,'device':d}}));
    final j = jsonDecode(r.body) as Map;
    token = j['access_token']; return j;
  }}

  Future<dynamic> bootstrap()         => _req('GET','/bootstrap');
  Future<dynamic> state()             => _req('GET','/state');
  Future<dynamic> current()           => _req('GET','/current');
  Future<dynamic> syncDelta(int from) => _req('GET','/sync/delta?from=$from');
  Future<dynamic> featureFlags()      => _req('GET','/feature-flags');
  Future<dynamic> patchFlags(Map f)   => _req('PATCH','/feature-flags',f);

  Future<dynamic> play(String videoId, [Map? extra]) =>
    _req('POST','/playback/play',{{'videoId':videoId,...?extra}});
  Future<dynamic> pause()       => _req('POST','/playback/pause');
  Future<dynamic> seek(double p)=> _req('POST','/playback/seek',{{'position':p}});
  Future<dynamic> getPlayback() => _req('GET','/playback');

  Future<dynamic> addToQueue(Map t) => _req('POST','/queue/add',t);
  Future<dynamic> clearQueue()      => _req('POST','/queue/clear');
  Future<dynamic> nextTrack()       => _req('GET','/queue/next');
  Future<dynamic> prevTrack()       => _req('GET','/queue/prev');
  Future<dynamic> jumpTo(int i)     => _req('POST','/queue/jump',{{'index':i}});

  Future<dynamic> search(String q,[String type='songs',int limit=20]) =>
    _req('GET','/search?q=${{Uri.encodeQueryComponent(q)}}&type=$type&limit=$limit');
  Future<dynamic> suggest(String q) =>
    _req('GET','/search/suggest?q=${{Uri.encodeQueryComponent(q)}}');

  String audioUrl(String v) => '$baseUrl/audio/proxy/$v';
  String thumbUrl(String v) => '$baseUrl/thumb/$v';

  Future<dynamic> download(Map t)        => _req('POST','/download',t);
  Future<dynamic> downloads()            => _req('GET','/downloads');
  Future<dynamic> favorites()            => _req('GET','/favorites');
  Future<dynamic> addFavorite(Map t)     => _req('POST','/favorites/add',t);
  Future<dynamic> removeFavorite(String id) => _req('POST','/favorites/remove',{{'videoId':id}});
  Future<dynamic> history()              => _req('GET','/recently_played');

  Future<dynamic> recommendations([String? seed]) {{
    final p = seed != null ? '/recommendations?seed_video=$seed' : '/recommendations';
    return _req('GET',p);
  }}
  Future<dynamic> startRadio(String seedId,[String seedType='track']) =>
    _req('POST','/radio/start',{{'seed_id':seedId,'seed_type':seedType}});

  Future<dynamic> getSettings()          => _req('GET','/settings');
  Future<dynamic> updateSettings(Map s)  => _req('POST','/settings',s);
  Future<dynamic> schemaModels()         => _req('GET','/schema/models');
  Future<dynamic> capabilities()         => _req('GET','/capabilities');
  Future<dynamic> batch(List reqs)       => _req('POST','/batch',{{'requests':reqs}});

  void dispose() => _c.close();
}}
"""


@app.get("/frontend/sdk/typescript")
async def sdk_typescript():
    code = _build_typescript_sdk()
    return Response(code, media_type="text/plain",
                    headers={"Content-Disposition": "attachment; filename=music-backend-sdk.ts"})


@app.get("/frontend/sdk/javascript")
async def sdk_javascript():
    code = _build_javascript_sdk()
    return Response(code, media_type="text/plain",
                    headers={"Content-Disposition": "attachment; filename=music-backend-sdk.js"})


@app.get("/frontend/sdk/python")
async def sdk_python():
    code = _build_python_sdk()
    return Response(code, media_type="text/plain",
                    headers={"Content-Disposition": "attachment; filename=music_backend_sdk.py"})


@app.get("/frontend/sdk/dart")
async def sdk_dart():
    code = _build_dart_sdk()
    return Response(code, media_type="text/plain",
                    headers={"Content-Disposition": "attachment; filename=music_backend_sdk.dart"})


# ─────────────────────────────────────────────────────────────────────────────
# API DISCOVERY MODE
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/discover")
async def api_discover():
    """
    GET /discover — complete machine-readable discovery document.
    AI agents can call this once to understand everything the backend supports.
    """
    await _run_startup_validation()
    flags = await _ensure_feature_flags()
    plugins = plugin_registry.list_all()

    # Collect registered routes from FastAPI app
    routes_list = []
    for route in app.routes:
        if hasattr(route, "methods") and hasattr(route, "path"):
            for method in (route.methods or []):
                routes_list.append({
                    "method": method,
                    "path": route.path,
                    "name": getattr(route, "name", ""),
                    "summary": (getattr(route, "summary", "") or
                                (route.endpoint.__doc__ or "").strip().split("\n")[0][:80]
                                if hasattr(route, "endpoint") else ""),
                })

    return _wrap({
        "routes": routes_list,
        "models": list(_MODEL_SCHEMAS.keys()),
        "events": {
            "state_change": list(STATE_CHANGE_EVENT_TYPES),
            "canonical": list(_EVENT_ALIAS_MAP.values()),
            "legacy": list(_EVENT_ALIAS_MAP.keys()),
        },
        "plugins": [{"name": p["name"], "loaded": p.get("loaded"), "version": p.get("version")} for p in plugins],
        "features": flags,
        "schema_version": _SCHEMA_VERSION,
        "api_version": _API_VERSION,
        "routes_count": len(routes_list),
        "models_count": len(_MODEL_SCHEMAS),
    })


# ─────────────────────────────────────────────────────────────────────────────
# FRONTEND GENERATOR MODE
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/frontend/generator")
async def frontend_generator():
    """
    GET /frontend/generator — AI frontend generation hints.
    Returns page/component/route/model structure for AI-generated React/Next.js apps.
    """
    flags = await _ensure_feature_flags()
    return _wrap({
        "framework_hints": ["React", "Next.js", "Vue", "Flutter"],
        "pages": [
            {"name": "Home",         "path": "/",           "description": "Now playing + queue"},
            {"name": "Search",       "path": "/search",     "description": "Search results",     "requires_feature": None},
            {"name": "Library",      "path": "/library",    "description": "Favorites + history"},
            {"name": "Downloads",    "path": "/downloads",  "description": "Download manager"},
            {"name": "Playlist",     "path": "/playlist",   "description": "Local playlist"},
            {"name": "Settings",     "path": "/settings",   "description": "App settings"},
            {"name": "Radio",        "path": "/radio",      "description": "Radio mode",          "requires_feature": "radio",         "enabled": flags.get("radio")},
            {"name": "Lyrics",       "path": "/lyrics",     "description": "Track lyrics",        "requires_feature": "lyrics",        "enabled": flags.get("lyrics")},
            {"name": "Explore",      "path": "/explore",    "description": "Trending + explore"},
            {"name": "Debug",        "path": "/debug",      "description": "Debug dashboard",     "requires_feature": "debug",         "enabled": flags.get("debug")},
        ],
        "components": [
            {"name": "NowPlayingBar",   "description": "Sticky bottom bar showing current track + controls"},
            {"name": "TrackCard",       "description": "Reusable card for a single track (thumbnail + title + artist)"},
            {"name": "QueueList",       "description": "Draggable/reorderable queue list"},
            {"name": "SearchBar",       "description": "Search input with suggestions dropdown"},
            {"name": "ProgressBar",     "description": "Audio seek bar with position/duration"},
            {"name": "VolumeControl",   "description": "Volume slider"},
            {"name": "DownloadItem",    "description": "Download task row with progress bar"},
            {"name": "FavoriteButton",  "description": "Toggle button for favorite status"},
            {"name": "ShuffleRepeat",   "description": "Shuffle + repeat mode controls"},
            {"name": "LyricsPanel",     "description": "Synced lyrics display",                  "requires_feature": "lyrics"},
            {"name": "RadioCard",       "description": "Radio seed picker",                       "requires_feature": "radio"},
            {"name": "RecommendationRow","description": "Horizontal scroll list of recommendations"},
        ],
        "state_management": {
            "recommended": "Zustand or Redux Toolkit",
            "slices": ["playback", "queue", "favorites", "downloads", "search", "settings"],
            "realtime_source": "WebSocket /ws",
            "bootstrap_endpoint": "/bootstrap",
            "polling_fallback": "/current (every 3s)",
        },
        "routing": [
            {"path": "/",            "component": "Home"},
            {"path": "/search",      "component": "Search"},
            {"path": "/library",     "component": "Library"},
            {"path": "/downloads",   "component": "Downloads"},
            {"path": "/settings",    "component": "Settings"},
            {"path": "/playlist",    "component": "Playlist"},
            {"path": "/radio",       "component": "Radio",   "guard": "feature:radio"},
            {"path": "/debug",       "component": "Debug",   "guard": "feature:debug"},
        ],
        "models": _MODEL_SCHEMAS,
        "api_base": f"http://localhost:{PORT}",
        "ws_url": f"ws://localhost:{PORT}/ws",
        "sdk_urls": {
            "typescript": f"http://localhost:{PORT}/frontend/sdk/typescript",
            "javascript":  f"http://localhost:{PORT}/frontend/sdk/javascript",
            "python":      f"http://localhost:{PORT}/frontend/sdk/python",
            "dart":        f"http://localhost:{PORT}/frontend/sdk/dart",
        },
    })


# ─────────────────────────────────────────────────────────────────────────────
# DEBUG ENDPOINTS
# ─────────────────────────────────────────────────────────────────────────────

def _assert_debug():
    if not _DEBUG_ENABLED:
        raise HTTPException(403, "Debug mode is disabled (set DEBUG_MODE=1 to enable)")


@app.get("/debug/state")
async def debug_state():
    _assert_debug()
    full = await _build_full_state()
    return _wrap({
        "full_state": full,
        "playback_clock": dict(_playback_clock),
        "authoritative_position": get_authoritative_position(),
        "state_version": state_mgr.get_version(),
        "latest_event_id": await event_store.get_latest_event_id(),
    })


@app.get("/debug/events")
async def debug_events(limit: int = Query(50, ge=1, le=500)):
    _assert_debug()
    events = await event_store.get_events_after(0, limit=limit)
    latest = await event_store.get_latest_event_id()
    return _wrap({
        "events": events,
        "count": len(events),
        "latest_event_id": latest,
        "state_version": state_mgr.get_version(),
    })


@app.get("/debug/cache")
async def debug_cache():
    _assert_debug()
    cache_keys = stream_cache.keys_snapshot()
    chunk_keys = list(chunk_cache._ram.keys())
    rows = await db.fetch_all(
        "SELECT key, video_id, kind, size_bytes, last_access FROM cache_index ORDER BY last_access DESC LIMIT 50"
    )
    return _wrap({
        "stream_cache": {
            "count": stream_cache.size(),
            "keys": cache_keys[:50],
        },
        "chunk_cache": {
            "count": len(chunk_keys),
            "keys": chunk_keys[:50],
        },
        "disk_index": rows,
        "audio_cache_mb": round(dir_size_mb(CACHE_DIR), 2),
        "chunk_cache_mb": round(dir_size_mb(CHUNK_CACHE_DIR), 2),
        "thumb_cache_mb": round(dir_size_mb(THUMB_CACHE_DIR), 2),
        "download_mb": round(dir_size_mb(DOWNLOAD_DIR), 2),
    })


@app.get("/debug/workers")
async def debug_workers():
    _assert_debug()
    import concurrent.futures as _cf
    def _pool_info(pool: "_cf.ThreadPoolExecutor") -> Dict:
        with suppress(Exception):
            return {
                "max_workers": pool._max_workers,
                "pending_work_items": len(pool._work_queue.queue) if hasattr(pool, "_work_queue") else -1,
                "threads_alive": sum(1 for t in pool._threads if t.is_alive()) if hasattr(pool, "_threads") else -1,
            }
        return {"max_workers": "unknown"}

    return _wrap({
        "extractor_pool": _pool_info(EXTRACTOR_POOL),
        "io_pool": _pool_info(IO_POOL),
        "download_pool": _pool_info(DOWNLOAD_POOL),
        "prefetch_queue_depth": len(prefetch_queue),
        "downloads_active": metrics.downloads_active,
        "ws_broadcaster_alive": not (ws_manager._broadcaster_task is None or ws_manager._broadcaster_task.done()),
        "background_tasks": [
            {"name": str(t.get_name()), "done": t.done(), "cancelled": t.cancelled()}
            for t in _background_tasks
        ],
    })


@app.get("/debug/plugins")
async def debug_plugins():
    _assert_debug()
    return _wrap({
        "plugins": plugin_registry.list_all(),
        "plugins_dir": _PLUGINS_DIR,
        "plugins_dir_exists": os.path.isdir(_PLUGINS_DIR),
        "alias_map": _EVENT_ALIAS_MAP,
        "alias_patch_active": getattr(ws_manager, "_alias_patched", False),
    })


# ─────────────────────────────────────────────────────────────────────────────
# WORKER MONITORING
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/workers")
async def workers_status():
    """GET /workers — background task and thread pool status."""
    import concurrent.futures as _cf

    running = []
    queued  = []
    failed  = []

    # asyncio background tasks
    for t in _background_tasks:
        entry = {"name": t.get_name(), "kind": "asyncio_task"}
        if t.done():
            exc = t.exception() if not t.cancelled() else None
            if exc:
                entry["error"] = str(exc)
                failed.append(entry)
            else:
                entry["status"] = "done"
                queued.append(entry)   # done but not "running"
        else:
            entry["status"] = "running"
            running.append(entry)

    # Download tasks
    for dl in download_mgr.list_all():
        entry = {
            "name": f"download:{dl['video_id']}",
            "kind": "download",
            "title": dl.get("title", ""),
            "progress": dl.get("progress", 0),
            "status": dl.get("status", ""),
        }
        if dl["status"] in ("downloading", "queued"):
            running.append(entry)
        elif dl["status"] in ("failed", "cancelled"):
            failed.append(entry)
        elif dl["status"] == "paused":
            queued.append(entry)

    # Prefetch queue
    with _prefetch_lock:
        for vid in list(prefetch_queue):
            queued.append({"name": f"prefetch:{vid}", "kind": "prefetch", "status": "queued"})

    return _wrap({
        "running": running,
        "queued":  queued,
        "failed":  failed,
        "summary": {
            "running": len(running),
            "queued":  len(queued),
            "failed":  len(failed),
        },
    })


# ─────────────────────────────────────────────────────────────────────────────
# CACHE REGISTRY  (GET /cache — new read endpoint, POST /cache/clear — alias)
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/cache")
async def cache_registry():
    """
    GET /cache — cache registry overview.
    Alias: GET /cache/stats already existed; this is the richer registry.
    """
    cache_keys = stream_cache.keys_snapshot()
    chunk_keys = list(chunk_cache._ram.keys())
    thumb_keys_count = len(thumb_cache._ram)

    db_rows = await db.fetch_all(
        "SELECT kind, COUNT(*) as cnt, SUM(size_bytes) as total_bytes "
        "FROM cache_index GROUP BY kind"
    )
    index_summary = {r["kind"]: {"count": r["cnt"], "total_bytes": r["total_bytes"] or 0} for r in db_rows}

    return _wrap({
        "ram": {
            "stream_url_cache": {"count": stream_cache.size(), "sample_keys": cache_keys[:10]},
            "chunk_cache":      {"count": len(chunk_keys), "sample_keys": chunk_keys[:10]},
            "thumb_cache":      {"count": thumb_keys_count},
            "search_result_cache": {"count": len(search_result_cache._d) if hasattr(search_result_cache, "_d") else "n/a"},
            "recommendation_cache": {"count": len(recommendation_cache._d) if hasattr(recommendation_cache, "_d") else "n/a"},
        },
        "disk": {
            "audio_mb":  round(dir_size_mb(CACHE_DIR), 2),
            "chunk_mb":  round(dir_size_mb(CHUNK_CACHE_DIR), 2),
            "thumb_mb":  round(dir_size_mb(THUMB_CACHE_DIR), 2),
            "index":     index_summary,
            "limit_mb":  MAX_CACHE_SIZE_MB,
        },
        "ttl": {
            "stream_url_ttl_s":       STREAM_CACHE_TTL,
            "prefetch_ttl_s":         PREFETCH_TTL,
            "recommendation_ttl_s":   RECOMMENDATION_CACHE_TTL,
            "thumb_ttl_s":            THUMB_CACHE_TTL,
        },
    })


@app.post("/cache/clear")
async def cache_clear_post(request: Request):
    """POST /cache/clear — clear cache (alias for DELETE /cache, body: {videoId?})."""
    try:
        data = await request.json()
    except Exception:
        data = {}
    v = data.get("videoId") or data.get("video_id")
    if v:
        stream_cache.delete(f"{v}:auto")
        stream_cache.delete(v)
        chunk_cache.delete(v)
        fp = stream_cache.get_local_path(v)
        if fp and os.path.exists(fp):
            with suppress(OSError):
                os.remove(fp)
        await db.execute("DELETE FROM cache_index WHERE video_id=?", (v,))
        return _wrap({"cleared": "single", "videoId": v})
    stream_cache.cleanup()
    return _wrap({"cleared": "all_expired", "remaining": stream_cache.size()})


# ─────────────────────────────────────────────────────────────────────────────
# STARTUP VALIDATION ENDPOINT
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/startup/validation")
async def get_startup_validation():
    """GET /startup/validation — result of the startup checks."""
    await _run_startup_validation()
    return _wrap(_startup_report)
