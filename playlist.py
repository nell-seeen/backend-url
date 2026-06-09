"""Routes: playlist. Executed in core.__dict__ namespace by routes/__init__.py."""

@app.post("/playlist/import")
async def import_ytmusic_playlist(request: Request):
    data = await request.json()
    url_or_id = (data.get("url_or_id") or "").strip()
    target = data.get("target", "local_playlist")
    if not url_or_id:
        raise HTTPException(400, "Parameter url_or_id tidak boleh kosong")

    playlist_id = url_or_id
    if "list=" in url_or_id:
        with suppress(Exception):
            parsed = urllib.parse.urlparse(url_or_id)
            queries = urllib.parse.parse_qs(parsed.query)
            playlist_id = queries.get("list", [url_or_id])[0]

    loop = asyncio.get_running_loop()

    def _fetch_playlist():
        with yt_lock:
            try:
                pd = yt.get_playlist(playlist_id, limit=100)
                tracks = pd.get("tracks", [])
                return {
                    "title": pd.get("title", "Imported Playlist"),
                    "description": pd.get("description", ""),
                    "thumbnail": (pd.get("thumbnails", [{}])[-1].get("url") if pd.get("thumbnails") else None),
                    "tracks": [build_track_meta(t) for t in tracks],
                }
            except Exception as e:
                raise HTTPException(500, f"Gagal mengekstrak playlist: {e}")

    try:
        playlist_data = await loop.run_in_executor(IO_POOL, _fetch_playlist)
        tracks_to_add = playlist_data["tracks"]

        if target == "active_queue":
            added_count = 0
            for t in tracks_to_add:
                queue_mgr.add(t)
                added_count += 1
            await ws_manager.broadcast({"type": "queue_updated", "action": "batch_import"})
            return {"status": "success",
                    "message": f"Berhasil mengimpor {added_count} lagu ke Antrean Aktif."}
        else:
            added = 0
            rows = []
            for t in tracks_to_add:
                if not t.get("videoId"):
                    continue
                rows.append((t["videoId"], t.get("title", ""), t.get("artist", ""),
                             t.get("thumbnail"), t.get("album", ""), t.get("duration", ""),
                             time.time()))
                added += 1
            if rows:
                await db.executemany(
                    "INSERT OR IGNORE INTO playlist(video_id,title,artist,thumbnail,album,duration,added_at) "
                    "VALUES(?,?,?,?,?,?,?)", rows)
            return {
                "status": "success",
                "message": f"Berhasil mengimpor {added} lagu ke Playlist Lokal.",
                "playlist_title": playlist_data["title"],
            }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[IMPORT_PLAYLIST] {e}")
        raise HTTPException(500, str(e))

@app.get("/playlist")
async def get_local_playlist():
    rows = await db.fetch_all(
        "SELECT video_id as videoId, title, artist, thumbnail, album, duration, added_at "
        "FROM playlist ORDER BY added_at DESC"
    )
    return rows or []

@app.post("/add_playlist")
async def add_to_playlist(request: Request):
    data = await request.json()
    vid = data.get("videoId")
    if not vid:
        raise HTTPException(400, "Missing videoId")
    entry = {
        "videoId": vid,
        "title": data.get("title", "Unknown"),
        "artist": data.get("artist", "Unknown"),
        "thumbnail": data.get("thumbnail"),
        "album": data.get("album", ""),
        "duration": data.get("duration", ""),
        "added_at": time.time(),
    }
    existing = await db.fetch_one("SELECT video_id FROM playlist WHERE video_id=?", (vid,))
    if existing:
        return {"status": "exists", "entry": entry}
    await db.execute(
        "INSERT INTO playlist(video_id,title,artist,thumbnail,album,duration,added_at) "
        "VALUES(?,?,?,?,?,?,?)",
        (vid, entry["title"], entry["artist"], entry["thumbnail"],
         entry["album"], entry["duration"], entry["added_at"]),
    )
    return {"status": "added", "entry": entry}

@app.post("/remove_playlist")
async def remove_from_playlist(request: Request):
    data = await request.json()
    vid = data.get("videoId")
    if not vid:
        raise HTTPException(400, "Missing videoId")
    await db.execute("DELETE FROM playlist WHERE video_id=?", (vid,))
    return {"status": "removed", "videoId": vid}

@app.get("/favorites")
async def get_favorites():
    rows = await db.fetch_all(
        "SELECT video_id as videoId, title, artist, thumbnail, album, duration, favorited_at "
        "FROM favorites ORDER BY favorited_at DESC LIMIT ?", (MAX_FAVORITES,)
    )
    return rows or []

@app.post("/favorites/add")
async def add_favorite(request: Request):
    data = await request.json()
    vid = data.get("videoId")
    if not vid:
        raise HTTPException(400, "Missing videoId")
    existing = await db.fetch_one("SELECT video_id FROM favorites WHERE video_id=?", (vid,))
    if existing:
        return {"status": "exists"}
    await db.execute(
        "INSERT INTO favorites(video_id,title,artist,thumbnail,album,duration,favorited_at) "
        "VALUES(?,?,?,?,?,?,?)",
        (vid, data.get("title", ""), data.get("artist", ""), data.get("thumbnail"),
         data.get("album", ""), data.get("duration", ""), time.time()),
    )
    ev = await event_bus.emit("favorite_add", {
        "videoId": vid,
        "title": data.get("title", ""),
        "artist": data.get("artist", ""),
        "thumbnail": data.get("thumbnail"),
    })
    return {"status": "added", "event_id": ev["event_id"], "state_version": ev["state_version"]}

@app.post("/favorites/remove")
async def remove_favorite(request: Request):
    data = await request.json()
    vid = data.get("videoId")
    await db.execute("DELETE FROM favorites WHERE video_id=?", (vid,))
    ev = await event_bus.emit("favorite_remove", {"videoId": vid})
    return {"status": "removed", "event_id": ev["event_id"], "state_version": ev["state_version"]}

@app.get("/favorites/check")
async def check_favorite(v: str = Query("")):
    row = await db.fetch_one("SELECT video_id FROM favorites WHERE video_id=?", (v,))
    return {"favorited": bool(row)}

@app.get("/recently_played")
async def get_recently_played(limit: int = Query(20, ge=1, le=100)):
    rows = await db.fetch_all(
        "SELECT video_id as videoId, title, artist, thumbnail, album, duration, played_at "
        "FROM recently_played ORDER BY played_at DESC LIMIT ?", (limit,)
    )
    return rows or []

@app.post("/recently_played/add")
async def add_recently_played(request: Request):
    data = await request.json()
    vid = data.get("videoId")
    if not vid:
        raise HTTPException(400, "Missing videoId")
    await db.execute(
        "INSERT OR REPLACE INTO recently_played(video_id,title,artist,thumbnail,album,duration,played_at) "
        "VALUES(?,?,?,?,?,?,?)",
        (vid, data.get("title", ""), data.get("artist", ""), data.get("thumbnail"),
         data.get("album", ""), data.get("duration", ""), time.time()),
    )
    # trim
    await db.execute(
        "DELETE FROM recently_played WHERE video_id IN ("
        "SELECT video_id FROM recently_played ORDER BY played_at DESC LIMIT -1 OFFSET ?)",
        (MAX_HISTORY,),
    )
    # update recommendation seed
    await db.execute(
        "INSERT OR REPLACE INTO recommendation_seed(video_id,score,last_seen) "
        "VALUES(?, COALESCE((SELECT score FROM recommendation_seed WHERE video_id=?), 0)+1, ?)",
        (vid, vid, time.time()),
    )
    # v7.0: broadcast history_add
    asyncio.create_task(_broadcast_history_add({
        "videoId": vid,
        "title": data.get("title", ""),
        "artist": data.get("artist", ""),
        "thumbnail": data.get("thumbnail"),
    }))
    return {"status": "added"}

@app.delete("/recently_played")
async def clear_recently_played():
    await db.execute("DELETE FROM recently_played")
    asyncio.create_task(_broadcast_history_remove("*"))
    return {"status": "cleared"}

