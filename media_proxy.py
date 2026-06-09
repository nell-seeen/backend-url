"""Routes: media_proxy. Executed in core.__dict__ namespace by routes/__init__.py."""

@app.get("/audio/proxy/{videoId}")
async def audio_proxy(videoId: str, request: Request):
    """
    GET /audio/proxy/{videoId}
    Full audio proxy — frontend tidak pernah menerima URL Google langsung.
    Support: Range Request, 206 Partial Content, byte seeking, chunk streaming.
    """
    ip = get_client_ip(request)
    await check_rate_limit(ip, "stream")

    if not videoId:
        raise HTTPException(400, "videoId diperlukan")

    # 1) Local cache file?
    local_path = stream_cache.get_local_path(videoId)
    if local_path and os.path.exists(local_path):
        return await _proxy_serve_file(local_path, request, "audio/mp4")

    # 2) Get stream URL (from cache or extract)
    cache_key = f"{videoId}:auto"
    stream_url = stream_cache.get(cache_key)
    if not stream_url:
        info = await asyncio.wait_for(extract_stream_async(videoId, "auto"), timeout=35)
        if not info or not info.get("url"):
            raise HTTPException(503, "Gagal mendapatkan stream URL")
        stream_url = info["url"]
        stream_cache.set(cache_key, stream_url)
        if info.get("abr"):
            bitrate_cache[videoId] = info["abr"]
        # Warmup first chunk
        asyncio.create_task(_warmup_first_chunk(videoId, stream_url))

    # 3) Proxy request ke stream URL dengan forwarding Range header
    return await _proxy_stream_url(stream_url, request, videoId)

@app.get("/thumb/{videoId}")
async def thumb_proxy(videoId: str, request: Request):
    """
    GET /thumb/{videoId}
    Thumbnail proxy dengan RAM+disk cache.
    Frontend tidak perlu akses langsung ke YouTube.
    """
    ip = get_client_ip(request)
    await check_rate_limit(ip, "thumb")

    if not videoId:
        raise HTTPException(400, "videoId diperlukan")

    # 1) RAM/disk cache hit
    cached = thumb_cache.get(videoId)
    if cached:
        data, ct = cached
        return Response(
            content=data,
            media_type=ct,
            headers={
                "Cache-Control": f"public, max-age={THUMB_CACHE_TTL}",
                "X-Cache": "HIT",
            },
        )

    # 2) Fetch dari YouTube
    urls = [
        THUMB_PROXY_URL_TEMPLATE.format(videoId=videoId),
        THUMB_FALLBACK_URL_TEMPLATE.format(videoId=videoId),
        f"https://i.ytimg.com/vi/{videoId}/mqdefault.jpg",
    ]

    sess = await get_http_session()
    data: Optional[bytes] = None
    ct = "image/jpeg"

    for url in urls:
        try:
            if sess:
                async with sess.get(url, timeout=aiohttp.ClientTimeout(total=8)) as r:
                    if r.status == 200:
                        data = await r.read()
                        ct = r.headers.get("Content-Type", "image/jpeg")
                        break
            else:
                def _sync_fetch(u):
                    req = urllib.request.Request(u, headers={"User-Agent": "MusicBackend/7.0"})
                    with urllib.request.urlopen(req, timeout=8) as r:
                        return r.read()
                loop = asyncio.get_running_loop()
                data = await loop.run_in_executor(IO_POOL, _sync_fetch, url)
                if data:
                    break
        except Exception:
            continue

    if not data:
        raise HTTPException(404, "Thumbnail tidak tersedia")

    # 3) Cache dan broadcast
    thumb_cache.set(videoId, data, ct)
    asyncio.create_task(ws_manager.broadcast({
        "type": "thumbnail_cached",
        "videoId": videoId,
        "server_time": time.time(),
    }))

    return Response(
        content=data,
        media_type=ct,
        headers={
            "Cache-Control": f"public, max-age={THUMB_CACHE_TTL}",
            "X-Cache": "MISS",
        },
    )

