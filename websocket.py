"""Routes: websocket. Executed in core.__dict__ namespace by routes/__init__.py."""

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    peer = "unknown"
    try:
        peer = ws.client.host if ws.client else "unknown"
    except Exception:
        pass

    # v7.0: rate limit WS connect per IP
    allowed, retry = rate_limiter.check(peer, "ws")
    if not allowed:
        await ws.close(code=1013)
        ws_logger.warning(f"[WS] rate-limited {peer}")
        return

    # v7.0: optional auth via query param token (WS tidak bisa kirim header dari browser)
    ws_user: Optional[Dict] = None
    if AUTH_REQUIRED:
        token = ws.query_params.get("token")
        if token:
            ws_user = jwt_auth.verify_token(token)
        if ws_user is None:
            await ws.close(code=3000)
            ws_logger.warning(f"[WS] unauthorized {peer}")
            return

    # v8.0: check for session resume request via query params
    resume_session_id = ws.query_params.get("session_id")
    resume_last_event_id = int(ws.query_params.get("last_event_id", "0") or "0")
    is_resume = bool(resume_session_id and resume_last_event_id > 0)

    try:
        if is_resume:
            cid, missed_events = await ws_manager.resume_session(
                ws, resume_session_id, resume_last_event_id, peer
            )
        else:
            cid = await ws_manager.connect(ws, peer)
    except Exception:
        return

    latest_event_id = await event_store.get_latest_event_id()

    try:
        # Send connected handshake (backward compat)
        await ws.send_text(json_dumps({
            "type": "connected",
            "client_id": cid,
            "session_id": resume_session_id or cid,
            "playback": playback.to_dict(),
            "queue": queue_mgr.get_state(),
            "server_time": time.time(),
            "version": "8.0",
            "state_version": state_mgr.get_version(),
            "latest_event_id": latest_event_id,
            "user": ws_user.get("sub") if ws_user else None,
            "resumed": is_resume,
        }))

        if is_resume and missed_events:
            # Replay missed events
            ws_logger.info(f"[WS] resume {cid[:8]} — replaying {len(missed_events)} missed events")
            for ev in missed_events:
                try:
                    await ws.send_text(json_dumps({**ev, "replayed": True}))
                except Exception:
                    break
        elif not is_resume:
            # Fresh connection: send full initial state
            try:
                initial = await _build_full_state()
                await ws.send_text(json_dumps({
                    "type": "initial_state",
                    "state_version": initial["state_version"],
                    "latest_event_id": latest_event_id,
                    "state": initial,
                }))
            except Exception as e:
                ws_logger.warning(f"[WS] gagal kirim initial_state ke {cid[:8]}: {e}")
    except Exception:
        pass

    try:
        while True:
            try:
                raw = await asyncio.wait_for(ws.receive_text(), timeout=30)
                metrics.inc("ws_messages_recv")
                try:
                    msg = json_loads(raw)
                except Exception:
                    continue
                event_type = msg.get("type", "")
                ack_id = msg.get("ack_id")

                # ── v8.0: ACK processing ──
                if event_type == "ack":
                    ev_id = msg.get("event_id")
                    if ev_id:
                        await ws_manager.ack_event(cid, int(ev_id))
                    continue

                # ── v8.0: Session resume via WS message ──
                elif event_type == "resume":
                    sess_id = msg.get("session_id")
                    last_eid = int(msg.get("last_event_id", 0) or 0)
                    if sess_id and last_eid >= 0:
                        missed = await event_store.get_events_after(last_eid, limit=500)
                        await ws.send_text(json_dumps({
                            "type": "resume_ack",
                            "session_id": sess_id,
                            "replayed": len(missed),
                            "latest_event_id": await event_store.get_latest_event_id(),
                            "state_version": state_mgr.get_version(),
                        }))
                        for ev in missed:
                            try:
                                await ws.send_text(json_dumps({**ev, "replayed": True}))
                            except Exception:
                                break

                elif event_type == "ping":
                    await ws.send_text(json_dumps({
                        "type": "pong",
                        "server_time": time.time(),
                        "state_version": state_mgr.get_version(),
                        "latest_event_id": await event_store.get_latest_event_id(),
                        "ts": time.time(),
                        "ack_id": ack_id,
                    }))
                elif event_type == "playback_update":
                    playback.update(**{k: v for k, v in msg.items()
                                       if k in ("playing", "position", "duration", "current_video_id")})
                    # Update authoritative clock
                    if "playing" in msg:
                        if msg["playing"]:
                            _update_playback_clock(
                                playing=True,
                                play_started_at=time.time(),
                                seek_position=msg.get("position", playback.position),
                                paused_at=None,
                            )
                        else:
                            _update_playback_clock(
                                playing=False,
                                paused_at=time.time(),
                                seek_position=get_authoritative_position(),
                            )
                    await event_bus.emit("playback_state", {**playback.to_dict()})
                elif event_type == "queue_add":
                    async with queue_async_lock:
                        idx = queue_mgr.add(msg.get("track", {}))
                    await event_bus.emit("queue_updated", {"action": "add", "index": idx})
                elif event_type == "get_state":
                    await ws.send_text(json_dumps({
                        "type": "state",
                        "playback": playback.to_dict(),
                        "queue": queue_mgr.get_state(),
                        "server_time": time.time(),
                        "state_version": state_mgr.get_version(),
                        "latest_event_id": await event_store.get_latest_event_id(),
                        "ack_id": ack_id,
                    }))
                elif event_type == "get_full_state":
                    # v5.0 backward compat + v8.0 upgrade: full state snapshot
                    full = await _build_full_state()
                    await ws.send_text(json_dumps({
                        "type": "full_state",
                        "state_version": full["state_version"],
                        "latest_event_id": await event_store.get_latest_event_id(),
                        **full,
                        "ack_id": ack_id,
                    }))
                elif event_type == "request_sync":
                    # v6.0+: client minta resync (setelah reconnect)
                    sync = await _build_sync_state()
                    await ws.send_text(json_dumps({
                        "type": "state_update",
                        "state_version": sync["state_version"],
                        "latest_event_id": await event_store.get_latest_event_id(),
                        "state": sync,
                        "ack_id": ack_id,
                    }))
                elif event_type == "prefetch":
                    vid = msg.get("videoId")
                    if vid:
                        enqueue_prefetch(vid)
                        await ws.send_text(json_dumps({"type": "prefetch_queued", "videoId": vid, "ack_id": ack_id}))

                if ack_id and event_type not in ("ping", "get_state", "prefetch", "ack", "resume"):
                    with suppress(Exception):
                        await ws.send_text(json_dumps({"type": "ack", "ack_id": ack_id}))

            except asyncio.TimeoutError:
                # Heartbeat with server clock
                try:
                    await ws.send_text(json_dumps({
                        "type": "heartbeat",
                        "ts": time.time(),
                        "state_version": state_mgr.get_version(),
                        "server_position": get_authoritative_position(),
                    }))
                except Exception:
                    break
    except WebSocketDisconnect:
        ws_logger.info(f"[WS] client {cid[:8]} disconnect normal")
    except Exception as e:
        ws_logger.warning(f"[WS] error {cid[:8]}: {e}")
    finally:
        await ws_manager.disconnect_by_id(cid)

