"""Routes: downloads. Executed in core.__dict__ namespace by routes/__init__.py."""

@app.post("/download")
async def download_track(request: Request):
    ip = get_client_ip(request)
    await check_rate_limit(ip, "download")

    data = await request.json()
    vid = data.get("videoId")
    title = data.get("title", "Song")
    artist = data.get("artist", "Unknown")
    priority = int(data.get("priority", 0))
    if not vid:
        raise HTTPException(400, "Parameter videoId wajib")
    t = download_mgr.enqueue(vid, title, artist, priority)
    return {"status": "queued", "videoId": vid, "task": t.to_dict()}

@app.get("/download/status")
async def download_status(v: str = Query("")):
    t = download_mgr.get(v)
    if not t:
        row = await db.fetch_one("SELECT * FROM downloads WHERE video_id=?", (v,))
        if not row:
            return {"status": "not_started"}
        return row
    return t.to_dict()

@app.post("/download/cancel")
async def download_cancel(request: Request):
    data = await request.json()
    vid = data.get("videoId")
    ok = download_mgr.cancel(vid)
    return {"status": "cancelled" if ok else "not_found", "videoId": vid}

@app.post("/download/pause")
async def download_pause(request: Request):
    data = await request.json()
    vid = data.get("videoId")
    ok = download_mgr.pause(vid)
    return {"status": "paused" if ok else "not_found", "videoId": vid}

@app.post("/download/resume")
async def download_resume(request: Request):
    data = await request.json()
    vid = data.get("videoId")
    ok = download_mgr.resume(vid)
    return {"status": "resumed" if ok else "not_found", "videoId": vid}

@app.post("/download/retry")
async def download_retry(request: Request):
    data = await request.json()
    vid = data.get("videoId")
    row = await db.fetch_one("SELECT * FROM downloads WHERE video_id=?", (vid,))
    if not row:
        raise HTTPException(404, "task tidak ditemukan")
    t = download_mgr.enqueue(vid, row.get("title", ""), row.get("artist", ""),
                              row.get("priority", 0))
    return {"status": "retrying", "task": t.to_dict()}

@app.get("/downloads")
async def get_all_downloads():
    """Daftar file fisik berhasil + status task aktif."""
    try:
        files_out = []
        for file in os.listdir(DOWNLOAD_DIR):
            if file.endswith(".temp"):
                continue
            fp = os.path.join(DOWNLOAD_DIR, file)
            try:
                size = os.path.getsize(fp)
            except OSError:
                continue
            if size <= 1024:
                continue
            files_out.append({
                "filename": file,
                "size_mb": round(size / (1024 * 1024), 2),
                "filepath": fp,
            })
        return files_out
    except Exception as e:
        return {"error": str(e)}

@app.get("/downloads/tasks")
async def get_download_tasks():
    """Tasks aktif + history dari SQLite."""
    rows = await db.fetch_all(
        "SELECT * FROM downloads ORDER BY updated_at DESC LIMIT 100"
    )
    return {"tasks": rows or [], "active": download_mgr.list_all()}

