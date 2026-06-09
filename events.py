"""Routes: events. Executed in core.__dict__ namespace by routes/__init__.py."""

@app.get("/events/replay")
async def events_replay(
    from_event_id: int = Query(0, ge=0),
    limit: int = Query(500, ge=1, le=2000),
    request: Request = None,
):
    """
    GET /events/replay?from_event_id=123
    Returns all events after from_event_id, ordered ascending.
    Frontend uses this to catch up after reconnect without full bootstrap.
    """
    if request:
        ip = get_client_ip(request)
        await check_rate_limit(ip, "default")
    events = await event_store.get_events_after(from_event_id, limit=limit)
    latest_eid = await event_store.get_latest_event_id()
    return {
        "from_event_id": from_event_id,
        "events": events,
        "count": len(events),
        "latest_event_id": latest_eid,
        "state_version": state_mgr.get_version(),
        "server_timestamp": time.time(),
    }

@app.get("/events/latest")
async def events_latest():
    """GET /events/latest — Returns current event_id and state_version."""
    return {
        "latest_event_id": await event_store.get_latest_event_id(),
        "state_version": state_mgr.get_version(),
        "server_timestamp": time.time(),
    }

