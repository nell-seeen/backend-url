"""Routes: radio. Executed in core.__dict__ namespace by routes/__init__.py."""

@app.post("/radio/start")
async def radio_start(request: Request):
    """
    POST /radio/start
    Body: { "seed_id": str, "seed_type": "track"|"artist"|"album", "seed_name": str }
    Mulai radio mode dari seed. Mengisi queue dan aktifkan infinite mode.
    """
    try:
        data = await request.json()
    except Exception:
        raise HTTPException(400, "Invalid JSON")

    seed_id = (data.get("seed_id") or "").strip()
    seed_type = data.get("seed_type", "track")
    seed_name = data.get("seed_name", seed_id)
    auto_queue = data.get("auto_queue", True)

    if not seed_id:
        raise HTTPException(400, "seed_id diperlukan")
    if seed_type not in ("track", "artist", "album"):
        raise HTTPException(400, "seed_type harus track|artist|album")

    # Current queue exclude list
    current_ids = {t.get("videoId") for t in queue_mgr.queue if t.get("videoId")}

    tracks = await _generate_radio_tracks(seed_id, seed_type, limit=10, exclude=current_ids)

    if not tracks:
        raise HTTPException(503, "Gagal menghasilkan radio tracks")

    async with _radio_lock:
        _radio_active["active"] = True
        _radio_active["seed_type"] = seed_type
        _radio_active["seed_id"] = seed_id
        _radio_active["seed_name"] = seed_name
        _radio_active["generated_count"] = len(tracks)
        _radio_active["started_at"] = time.time()

    if auto_queue:
        async with queue_async_lock:
            for t in tracks:
                queue_mgr.add(t)
        await ws_manager.broadcast({
            "type": "radio_generated",
            "seed_id": seed_id,
            "seed_type": seed_type,
            "seed_name": seed_name,
            "count": len(tracks),
            "server_time": time.time(),
        })

    return {
        "status": "started",
        "seed_type": seed_type,
        "seed_id": seed_id,
        "seed_name": seed_name,
        "tracks_added": len(tracks),
        "tracks": tracks,
    }

@app.post("/radio/next")
async def radio_next():
    """
    POST /radio/next
    Generate dan tambahkan lagu berikutnya ke queue (infinite radio mode).
    """
    async with _radio_lock:
        if not _radio_active.get("active"):
            raise HTTPException(400, "Radio belum aktif. Gunakan POST /radio/start dulu.")
        seed_id = _radio_active["seed_id"]
        seed_type = _radio_active["seed_type"]
        seed_name = _radio_active["seed_name"]

    current_ids = {t.get("videoId") for t in queue_mgr.queue if t.get("videoId")}
    new_tracks = await _generate_radio_tracks(seed_id, seed_type, limit=6, exclude=current_ids)

    if not new_tracks:
        return {"status": "no_new_tracks"}

    async with queue_async_lock:
        for t in new_tracks:
            queue_mgr.add(t)

    async with _radio_lock:
        _radio_active["generated_count"] = _radio_active.get("generated_count", 0) + len(new_tracks)

    await ws_manager.broadcast({
        "type": "radio_generated",
        "seed_id": seed_id,
        "seed_type": seed_type,
        "seed_name": seed_name,
        "count": len(new_tracks),
        "server_time": time.time(),
    })

    return {
        "status": "generated",
        "tracks_added": len(new_tracks),
        "tracks": new_tracks,
    }

@app.get("/radio/status")
async def radio_status():
    """GET /radio/status — Status radio yang sedang aktif."""
    return {**_radio_active}

@app.post("/radio/stop")
async def radio_stop():
    """POST /radio/stop — Hentikan radio mode."""
    async with _radio_lock:
        _radio_active["active"] = False
    return {"status": "stopped"}

