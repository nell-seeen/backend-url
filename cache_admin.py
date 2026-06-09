"""Routes: cache_admin. Executed in core.__dict__ namespace by routes/__init__.py."""

@app.post("/batch")
async def batch_request(request: Request):
    data = await request.json()
    requests_list = data.get("requests", [])
    if not isinstance(requests_list, list) or len(requests_list) > 10:
        raise HTTPException(400, "requests must be a list of up to 10 items")

    async def _execute_internal(req_item: Dict):
        path = req_item.get("path", "")
        params = req_item.get("params", {})
        try:
            if path == "/trending":
                return {"path": path, "status": 200, "body": await trending()}
            elif path == "/recently_played":
                limit = int(params.get("limit", 20))
                return {"path": path, "status": 200, "body": await get_recently_played(limit)}
            elif path == "/favorites":
                return {"path": path, "status": 200, "body": await get_favorites()}
            elif path == "/playlist":
                return {"path": path, "status": 200, "body": await get_local_playlist()}
            elif path == "/queue":
                return {"path": path, "status": 200, "body": await get_queue()}
            elif path == "/playback":
                return {"path": path, "status": 200, "body": await get_playback()}
            elif path == "/recommendations":
                seed = params.get("seed_video", "")
                limit = int(params.get("limit", 10))
                return {"path": path, "status": 200, "body": await recommendations(seed, limit)}
            elif path == "/monitor":
                return {"path": path, "status": 200, "body": await monitor()}
            elif path == "/state":
                return {"path": path, "status": 200, "body": await global_state()}
            elif path == "/bootstrap":
                return {"path": path, "status": 200, "body": await bootstrap()}
            elif path == "/sync":
                return {"path": path, "status": 200, "body": await force_sync()}
            elif path == "/current":
                return {"path": path, "status": 200, "body": await get_current()}
            else:
                return {"path": path, "status": 404, "error": f"Path '{path}' tidak dikenali"}
        except Exception as e:
            logger.error(f"[BATCH] {path}: {e}")
            return {"path": path, "status": 500, "error": str(e)}

    results = await asyncio.gather(*[_execute_internal(r) for r in requests_list])
    return {"results": list(results)}

@app.delete("/cache")
async def clear_cache(v: Optional[str] = Query(None)):
    if v:
        stream_cache.delete(f"{v}:auto")
        stream_cache.delete(v)
        chunk_cache.delete(v)
        filepath = stream_cache.get_local_path(v)
        if filepath and os.path.exists(filepath):
            with suppress(OSError):
                os.remove(filepath)
        await db.execute("DELETE FROM cache_index WHERE video_id=?", (v,))
        return {"status": "cleared", "videoId": v}
    stream_cache.cleanup()
    return {"status": "cleanup_done", "remaining": stream_cache.size()}

@app.get("/cache/stats")
async def cache_stats():
    return {
        "stream_cache_items": stream_cache.size(),
        "audio_cache_mb": round(dir_size_mb(CACHE_DIR), 2),
        "chunk_cache_mb": round(dir_size_mb(CHUNK_CACHE_DIR), 2),
        "download_mb": round(dir_size_mb(DOWNLOAD_DIR), 2),
        "limit_mb": MAX_CACHE_SIZE_MB,
        "first_chunk_items": len(chunk_cache._ram),
        "metrics": metrics.snapshot(),
    }

