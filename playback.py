"""Routes: playback. Executed in core.__dict__ namespace by routes/__init__.py."""

@app.get("/playback")
async def get_playback():
    return playback.to_dict()

@app.post("/playback/update")
async def update_playback(request: Request):
    data = await request.json()
    expected_sv = data.pop("expected_state_version", None)
    fields = {k: v for k, v in data.items()
              if k in ("playing", "current_video_id", "position", "duration")}
    playback.update(**fields)
    # Update authoritative clock
    if "playing" in fields:
        if fields["playing"]:
            _update_playback_clock(
                playing=True,
                play_started_at=time.time(),
                seek_position=fields.get("position", playback.position),
                paused_at=None,
            )
        else:
            _update_playback_clock(
                playing=False,
                paused_at=time.time(),
                seek_position=get_authoritative_position(),
            )
    ev = await event_bus.emit("playback_state", {**playback.to_dict()}, expected_sv)
    return {"status": "ok", "event_id": ev["event_id"], "state_version": ev["state_version"]}

@app.post("/playback/play")
async def playback_play():
    playback.update(playing=True)
    _update_playback_clock(
        playing=True,
        play_started_at=time.time(),
        seek_position=playback.position,
        paused_at=None,
    )
    ev = await event_bus.emit("play", {**playback.to_dict()})
    return {"playing": True, "event_id": ev["event_id"], "state_version": ev["state_version"]}

@app.post("/playback/pause")
async def playback_pause():
    _update_playback_clock(
        playing=False,
        paused_at=time.time(),
        seek_position=get_authoritative_position(),
    )
    playback.update(playing=False)
    ev = await event_bus.emit("pause", {**playback.to_dict()})
    return {"playing": False, "event_id": ev["event_id"], "state_version": ev["state_version"]}

@app.post("/playback/seek")
async def playback_seek(request: Request):
    data = await request.json()
    pos = data.get("position", 0)
    playback.update(position=pos)
    _update_playback_clock(
        seek_position=pos,
        play_started_at=time.time() if playback.playing else None,
        paused_at=None if playback.playing else time.time(),
    )
    ev = await event_bus.emit("seek", {
        "position": pos,
        "current_video_id": playback.current_video_id,
        "ts": time.time(),
    })
    return {"position": pos, "event_id": ev["event_id"], "state_version": ev["state_version"]}

@app.post("/sleep_timer")
async def set_sleep_timer(request: Request):
    data = await request.json()
    minutes = data.get("minutes", 0)
    if minutes <= 0:
        playback.update(sleep_timer_end=None)
        return {"status": "cancelled"}
    end_at = time.time() + minutes * 60
    playback.update(sleep_timer_end=end_at)
    return {"status": "set", "end_at": end_at, "minutes": minutes}

@app.get("/sleep_timer")
async def get_sleep_timer():
    end = playback.sleep_timer_end
    if not end:
        return {"active": False}
    remaining = max(0, end - time.time())
    return {"active": remaining > 0, "remaining_seconds": remaining, "end_at": end}

@app.get("/playback/clock")
async def playback_clock():
    """
    GET /playback/clock — Authoritative server playback clock.
    Frontend should use server_position (not local timer) for accurate sync.
    """
    with _pb_clock_lock:
        clock = dict(_playback_clock)
    return {
        **clock,
        "server_position": get_authoritative_position(),
        "server_timestamp": time.time(),
        "state_version": state_mgr.get_version(),
    }

