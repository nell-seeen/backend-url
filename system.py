"""Routes: system. Executed in core.__dict__ namespace by routes/__init__.py."""

@app.middleware("http")
async def log_requests(request: Request, call_next):
    metrics.inc("total_requests")
    rid = uuid.uuid4().hex[:10]
    start = time.time()
    logger.info(f"[REQ:{rid}] {request.method} {request.url.path}")
    try:
        response = await asyncio.wait_for(call_next(request), timeout=60)
    except asyncio.TimeoutError:
        logger.error(f"[TIMEOUT:{rid}] {request.url.path}")
        return JSONResponse({"error": "Batas waktu request habis"}, status_code=504)
    except Exception as e:
        logger.error(f"[ERR:{rid}] {request.url.path} {e}")
        _crash_dump(f"REQ {request.url.path}", e)
        return JSONResponse({"error": "internal error"}, status_code=500)
    finally:
        latency = (time.time() - start) * 1000
        metrics.record_latency(latency)
    response.headers["X-Request-ID"] = rid
    return response

@app.get("/health")
async def health():
    return {
        "status": "online",
        "version": "4.0",
        "uptime": get_uptime_str(),
        "memory": get_memory_str(),
        "cpu": get_cpu_str(),
        "ws_clients": ws_manager.count(),
    }

@app.get("/monitor")
async def monitor():
    snap = metrics.snapshot()
    disk = get_disk_str()
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
        # Field lama (kompatibilitas)
        "total_requests": snap["total_requests"],
        "active_connections": ws_manager.count(),
        "cache_items": stream_cache.size(),
        "queue_size": len(queue_mgr.queue),
        "ram_usage": get_memory_str(),
        "cpu_usage": get_cpu_str(),
        # Field baru (advanced monitor) — v4.0
        "uptime": get_uptime_str(),
        "metrics": snap,
        "disk": disk,
        "cache_size_mb": round(dir_size_mb(CACHE_DIR), 2),
        "chunk_cache_mb": round(dir_size_mb(CHUNK_CACHE_DIR), 2),
        "download_size_mb": round(dir_size_mb(DOWNLOAD_DIR), 2),
        "extractor_pool": MAX_CONCURRENT_EXTRACTIONS,
        "io_pool": MAX_WORKERS,
        "download_pool": MAX_CONCURRENT_DOWNLOADS,
        "features": {
            "orjson": HAVE_ORJSON,
            "uvloop": HAVE_UVLOOP,
            "rapidfuzz": HAVE_RAPIDFUZZ,
            "aiohttp": HAVE_AIOHTTP,
            "aiofiles": HAVE_AIOFILES,
        },
        # v5.0: numeric stats untuk Apple Music / iOS frontend
        "uptime_seconds": uptime_secs,
        "memory_mb": mem_mb,
        "cpu_percent": cpu_pct,
        "active_ws": ws_manager.count(),
        "downloads_active": metrics.downloads_active,
    }

@app.get("/state")
async def global_state():
    """
    v6.0 FULL SNAPSHOT — semua state yang dibutuhkan frontend.
    Backward compatible: semua field lama tetap ada.
    """
    full = await _build_full_state()

    # Tambahkan legacy fields agar frontend v5 tidak rusak
    pb = playback.to_dict()
    qs = queue_mgr.get_state()
    uptime_secs = int(time.time() - START_TIME)
    mem_mb = full["server"].get("memory_mb", 0.0)
    cpu_pct = full["server"].get("cpu_percent", 0.0)

    return {
        # ── v6.0 fields ──
        **full,
        # ── legacy v5.0 fields (tetap ada untuk backward compat) ──
        "current_video_id": pb.get("current_video_id"),
        "is_playing": pb.get("playing", False),
        "position": pb.get("position", 0),
        "duration": pb.get("duration", 0),
        "shuffle": qs.get("shuffle", False),
        "repeat": qs.get("repeat", "none"),
        "autoplay": qs.get("autoplay", True),
        "queue_length": qs.get("size", 0),
        "current_index": qs.get("current_index", -1),
        "sleep_timer_end": pb.get("sleep_timer_end"),
        "volume": 100,
        "uptime": uptime_secs,
        "uptime_str": get_uptime_str(),
        "memory_mb": mem_mb,
        "cpu_percent": cpu_pct,
        "active_ws": ws_manager.count(),
        "cache_items": stream_cache.size(),
        "downloads_active": metrics.downloads_active,
        "server_time": time.time(),
        "version": "6.0",
    }

@app.get("/bootstrap")
async def bootstrap():
    """
    GET /bootstrap — Frontend cukup 1 request ini saat startup.
    Mengembalikan SEMUA state: playback, queue, favorites, downloads,
    history, recommendations, settings, dan server info.
    Format: { success, timestamp, state_version, data: { ... } }
    """
    full = await _build_full_state()
    return {
        "success": True,
        "timestamp": time.time(),
        "state_version": full["state_version"],
        "data": full,
    }

@app.get("/sync")
async def force_sync():
    """
    GET /sync — Snapshot ringan untuk force-resync saat reconnect.
    Berisi: state_version, playback, queue, favorites (kosong), downloads.
    Frontend memanggil ini setelah WebSocket reconnect jika perlu.
    """
    sync = await _build_sync_state()
    return {
        "success": True,
        "timestamp": time.time(),
        "state_version": sync["state_version"],
        **sync,
    }

@app.get("/current")
async def get_current():
    """
    GET /current — Super-ringan untuk polling posisi playback.
    Tidak perlu full state, hanya info track aktif.
    """
    pb = playback.to_dict()
    qs = queue_mgr.get_state()
    current_song = None
    idx = qs.get("current_index", -1)
    q_list = qs.get("queue", [])
    if 0 <= idx < len(q_list):
        current_song = q_list[idx]
    return {
        "success": True,
        "timestamp": time.time(),
        "state_version": state_mgr.get_version(),
        "playing": pb.get("playing", False),
        "position": pb.get("position", 0),
        "duration": pb.get("duration", 0),
        "track": current_song,
        "current_video_id": pb.get("current_video_id"),
    }

