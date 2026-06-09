"""Routes: queue. Executed in core.__dict__ namespace by routes/__init__.py."""

@app.get("/queue")
async def get_queue():
    return queue_mgr.get_state()

@app.post("/queue/add")
async def queue_add(request: Request):
    data = await request.json()
    expected_sv = data.pop("expected_state_version", None)
    async with queue_async_lock:
        idx = queue_mgr.add(data)
    ev = await event_bus.emit("queue_add", {"action": "add", "index": idx, "track": data}, expected_sv)
    for tr in queue_mgr.peek_upcoming(PREFETCH_DEPTH):
        if tr.get("videoId"):
            enqueue_prefetch(tr["videoId"])
    return {"status": "added", "index": idx, "event_id": ev["event_id"], "state_version": ev["state_version"]}

@app.post("/queue/add_next")
async def queue_add_next(request: Request):
    data = await request.json()
    async with queue_async_lock:
        queue_mgr.add_next(data)
    ev = await event_bus.emit("queue_add", {"action": "add_next", "track": data})
    if data.get("videoId"):
        enqueue_prefetch(data["videoId"])
    return {"status": "added_next", "event_id": ev["event_id"], "state_version": ev["state_version"]}

@app.post("/queue/remove")
async def queue_remove(request: Request):
    data = await request.json()
    idx = data.get("index", -1)
    async with queue_async_lock:
        ok = queue_mgr.remove(idx)
    ev = await event_bus.emit("queue_remove", {"action": "remove", "index": idx})
    return {"status": "removed" if ok else "invalid_index", "event_id": ev["event_id"], "state_version": ev["state_version"]}

@app.post("/queue/clear")
async def queue_clear():
    async with queue_async_lock:
        queue_mgr.clear()
    ev = await event_bus.emit("queue_clear", {"action": "clear"})
    return {"status": "cleared", "event_id": ev["event_id"], "state_version": ev["state_version"]}

@app.post("/queue/jump")
async def queue_jump(request: Request):
    data = await request.json()
    idx = data.get("index", 0)
    async with queue_async_lock:
        track = queue_mgr.set_current(idx)
    if track:
        ev = await event_bus.emit("queue_updated", {
            "action": "jump",
            "index": idx,
            "track": track,
            "queue": queue_mgr.get_state(),
        })
        if track.get("videoId"):
            asyncio.create_task(warmup_stream(track["videoId"], "jump"))
        for tr in queue_mgr.peek_upcoming(PREFETCH_DEPTH):
            if tr.get("videoId"):
                enqueue_prefetch(tr["videoId"])
        return {"status": "ok", "track": track, "event_id": ev["event_id"], "state_version": ev["state_version"]}
    raise HTTPException(400, "Invalid index")

@app.post("/queue/reorder")
async def queue_reorder(request: Request):
    data = await request.json()
    fi = int(data.get("from", -1))
    ti = int(data.get("to", -1))
    async with queue_async_lock:
        ok = queue_mgr.reorder(fi, ti)
    if ok:
        ev = await event_bus.emit("queue_reorder", {"action": "reorder", "from": fi, "to": ti})
        return {"status": "ok", "event_id": ev["event_id"], "state_version": ev["state_version"]}
    return {"status": "invalid"}

@app.post("/queue/undo")
async def queue_undo():
    ok = queue_mgr.undo()
    if ok:
        ev = await event_bus.emit("queue_updated", {"action": "undo"})
        return {"status": "ok", "event_id": ev["event_id"], "state_version": ev["state_version"]}
    return {"status": "no_snapshot"}

@app.get("/queue/next")
async def queue_next():
    async with queue_async_lock:
        track = queue_mgr.next_track()
    if track:
        queue_mgr.save()
        ev = await event_bus.emit("next_track", {"track": track})
        # warmup current + further upcoming
        if track.get("videoId"):
            asyncio.create_task(warmup_stream(track["videoId"], "next"))
        for tr in queue_mgr.peek_upcoming(PREFETCH_DEPTH):
            if tr.get("videoId"):
                enqueue_prefetch(tr["videoId"])
        return track

    if queue_mgr.autoplay:
        logger.info("[AUTOPLAY] queue habis — radio mode")
        ref_id = playback.current_video_id
        if not ref_id and queue_mgr.queue:
            ref_id = queue_mgr.queue[-1].get("videoId")
        if ref_id:
            try:
                # cek recommendation_cache
                radio_tracks: List[Dict] = []
                with _rec_lock:
                    cached = recommendation_cache.get(ref_id) if hasattr(recommendation_cache, "get") else None
                if cached:
                    radio_tracks = cached
                else:
                    loop = asyncio.get_running_loop()

                    def _fetch_radio_seeds():
                        with yt_lock:
                            wp = yt.get_watch_playlist(videoId=ref_id, limit=6)
                            return [build_track_meta(t) for t in wp.get("tracks", []) if t.get("videoId") != ref_id]

                    radio_tracks = await loop.run_in_executor(IO_POOL, _fetch_radio_seeds)
                    with _rec_lock:
                        if radio_tracks:
                            recommendation_cache[ref_id] = radio_tracks

                if radio_tracks:
                    first_added_idx = None
                    for t in radio_tracks:
                        idx = queue_mgr.add(t)
                        if first_added_idx is None:
                            first_added_idx = idx
                    if first_added_idx is not None:
                        new_track = queue_mgr.set_current(first_added_idx)
                        queue_mgr.save()
                        await event_bus.emit("queue_updated", {"action": "autoplay_triggered"})
                        await event_bus.emit("next_track", {"track": new_track})
                        if new_track and new_track.get("videoId"):
                            asyncio.create_task(warmup_stream(new_track["videoId"], "radio"))
                        for tr in queue_mgr.peek_upcoming(PREFETCH_DEPTH):
                            if tr.get("videoId"):
                                enqueue_prefetch(tr["videoId"])
                        return new_track
            except Exception as e:
                logger.error(f"[AUTOPLAY] {e}")

    return {"status": "end_of_queue"}

@app.get("/queue/prev")
async def queue_prev():
    async with queue_async_lock:
        track = queue_mgr.prev_track()
    if track:
        queue_mgr.save()
        await event_bus.emit("prev_track", {"track": track})
        if track.get("videoId"):
            asyncio.create_task(warmup_stream(track["videoId"], "prev"))
        return track
    return {"status": "beginning_of_queue"}

@app.post("/queue/shuffle")
async def toggle_shuffle(request: Request):
    data = await request.json()
    queue_mgr.shuffle = data.get("shuffle", not queue_mgr.shuffle)
    queue_mgr.save()
    ev = await event_bus.emit("shuffle_changed", {"shuffle": queue_mgr.shuffle})
    return {"shuffle": queue_mgr.shuffle, "event_id": ev["event_id"], "state_version": ev["state_version"]}

@app.post("/queue/repeat")
async def set_repeat(request: Request):
    data = await request.json()
    mode = data.get("repeat", "none")
    if mode not in ("none", "one", "all"):
        raise HTTPException(400, "repeat must be none|one|all")
    queue_mgr.repeat = mode
    queue_mgr.save()
    ev = await event_bus.emit("repeat_changed", {"repeat": mode})
    return {"repeat": mode, "event_id": ev["event_id"], "state_version": ev["state_version"]}

@app.post("/queue/autoplay")
async def set_autoplay(request: Request):
    data = await request.json()
    queue_mgr.autoplay = data.get("autoplay", True)
    queue_mgr.save()
    ev = await event_bus.emit("autoplay_changed", {"autoplay": queue_mgr.autoplay})
    return {"autoplay": queue_mgr.autoplay, "event_id": ev["event_id"], "state_version": ev["state_version"]}

