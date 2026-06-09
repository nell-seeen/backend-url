"""Routes: stream. Executed in core.__dict__ namespace by routes/__init__.py."""

@app.get("/stream")
async def stream_audio(
    v: str = Query(""),
    quality: str = Query("auto"),
    ws_id: Optional[str] = Query(None),
    request: Request = None,
):
    if request:
        ip = get_client_ip(request)
        await check_rate_limit(ip, "stream")

    if not v:
        raise HTTPException(400, "Missing video id")

    # 1) Local physical cache?
    local_file = stream_cache.get_local_path(v)
    if local_file:
        stream_logger.info(f"[STREAM] hit fisik {v}")
        # trigger upcoming prefetch
        for tr in queue_mgr.peek_upcoming(PREFETCH_DEPTH):
            if tr.get("videoId"):
                enqueue_prefetch(tr["videoId"])
        return {
            "url": f"http://localhost:{PORT}/stream/file/{v}",
            "cached": True, "local": True,
        }

    # 2) RAM cache?
    cache_key = f"{v}:{quality}"
    cached_url = stream_cache.get(cache_key)
    if cached_url:
        for tr in queue_mgr.peek_upcoming(PREFETCH_DEPTH):
            if tr.get("videoId"):
                enqueue_prefetch(tr["videoId"])
        return {"url": cached_url, "cached": True, "local": False}

    # 3) Extract (dedup-safe)
    await ws_manager.broadcast({"type": "download-start", "videoId": v})
    try:
        await ws_manager.broadcast({"type": "processing", "videoId": v})
        info = await asyncio.wait_for(extract_stream_async(v, quality), timeout=35)
        if info and info.get("url"):
            url = info["url"]
            stream_cache.set(cache_key, url)
            if info.get("abr"):
                bitrate_cache[v] = info["abr"]
            await ws_manager.broadcast({"type": "completed", "videoId": v})

            # Aggressive prefetch upcoming
            for tr in queue_mgr.peek_upcoming(PREFETCH_DEPTH):
                if tr.get("videoId"):
                    enqueue_prefetch(tr["videoId"])

            # Background offline cache
            EXTRACTOR_POOL.submit(background_cache_audio, v)

            # Warmup first chunk (async, non-blocking)
            asyncio.create_task(_warmup_first_chunk(v, url))

            return {
                "url": url,
                "cached": False,
                "quality": quality,
                "local": False,
                "bitrate": info.get("abr"),
                "duration": info.get("duration"),
            }
        metrics.inc("stream_failures")
        await ws_manager.broadcast({"type": "failed", "videoId": v, "message": "stream tidak ditemukan"})
        raise HTTPException(500, "Could not extract stream URL")
    except asyncio.TimeoutError:
        metrics.inc("stream_failures")
        await ws_manager.broadcast({"type": "failed", "videoId": v, "message": "timeout"})
        raise HTTPException(504, "Stream extraction timeout")
    except HTTPException:
        raise
    except Exception as e:
        metrics.inc("stream_failures")
        stream_logger.error(f"[STREAM] {v} error: {e}")
        await ws_manager.broadcast({"type": "failed", "videoId": v, "message": str(e)})
        raise HTTPException(500, str(e))

@app.get("/stream/chunk/{videoId}")
async def stream_first_chunk(videoId: str):
    """First-chunk audio (256KB) untuk instant playback start. RAM/Disk cache."""
    data = chunk_cache.get(videoId)
    if not data:
        raise HTTPException(404, "chunk belum siap")
    return Response(
        content=data,
        media_type="audio/mp4",
        headers={
            "Accept-Ranges": "bytes",
            "Content-Length": str(len(data)),
            "Cache-Control": "public, max-age=60",
        },
    )

@app.get("/stream/file/{videoId}")
async def serve_stream_file(videoId: str, request: Request):
    """Sajikan file audio lokal dengan dukungan HTTP Range (progressive playback)."""
    filepath = stream_cache.get_local_path(videoId)
    if not filepath or not os.path.exists(filepath):
        raise HTTPException(404, "Berkas cache offline tidak ditemukan")

    file_size = os.path.getsize(filepath)
    range_header = request.headers.get("range") or request.headers.get("Range")

    # Update last_access
    with suppress(Exception):
        await db.execute(
            "UPDATE cache_index SET last_access=? WHERE key=?",
            (time.time(), f"audio:{videoId}"),
        )

    if not range_header:
        return FileResponse(
            filepath,
            media_type="audio/mp4",
            filename=f"{videoId}.m4a",
            headers={"Accept-Ranges": "bytes", "Content-Length": str(file_size)},
        )

    # Parse Range: "bytes=start-end"
    try:
        m = re.match(r"bytes=(\d*)-(\d*)", range_header)
        if not m:
            raise ValueError("bad range")
        start_s, end_s = m.group(1), m.group(2)
        start = int(start_s) if start_s else 0
        end = int(end_s) if end_s else file_size - 1
        end = min(end, file_size - 1)
        if start > end or start >= file_size:
            return Response(status_code=416, headers={"Content-Range": f"bytes */{file_size}"})
        length = end - start + 1
    except Exception:
        return Response(status_code=416, headers={"Content-Range": f"bytes */{file_size}"})

    CHUNK = 64 * 1024

    async def _iter():
        try:
            with open(filepath, "rb") as f:
                f.seek(start)
                remaining = length
                while remaining > 0:
                    buf = f.read(min(CHUNK, remaining))
                    if not buf:
                        break
                    remaining -= len(buf)
                    yield buf
        except Exception as e:
            stream_logger.warning(f"[RANGE] iter error {videoId}: {e}")

    return StreamingResponse(
        _iter(),
        status_code=206,
        media_type="audio/mp4",
        headers={
            "Content-Range": f"bytes {start}-{end}/{file_size}",
            "Accept-Ranges": "bytes",
            "Content-Length": str(length),
        },
    )

