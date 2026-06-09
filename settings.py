"""Routes: settings. Executed in core.__dict__ namespace by routes/__init__.py."""

@app.get("/settings")
async def get_settings():
    """GET /settings — Ambil semua settings dari KV store."""
    rows = await db.fetch_all("SELECT k, v FROM kv WHERE k LIKE 'setting:%'")
    settings = {}
    for r in rows:
        key = r["k"].replace("setting:", "", 1)
        try:
            settings[key] = json_loads(r["v"])
        except Exception:
            settings[key] = r["v"]
    return {"settings": settings}

@app.post("/settings")
async def update_settings(request: Request):
    """POST /settings — Update settings. Body: { key: value, ... }"""
    try:
        data = await request.json()
    except Exception:
        raise HTTPException(400, "Invalid JSON")

    for key, value in data.items():
        if not isinstance(key, str) or len(key) > 64:
            continue
        await db.execute(
            "INSERT OR REPLACE INTO kv(k, v) VALUES(?, ?)",
            (f"setting:{key}", json_dumps(value)),
        )

    await _broadcast_settings_update(data)
    return {"status": "updated", "keys": list(data.keys())}

