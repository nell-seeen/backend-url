"""Routes: media. Executed in core.__dict__ namespace by routes/__init__.py."""

@app.get("/lyrics")
async def lyrics(
    v: str = Query(""),
    title: str = Query(""),
    artist: str = Query(""),
):
    if not v:
        return {"lyrics": "Video ID tidak valid.", "synced": False}

    # 1) Coba LRCLIB via aiohttp (lebih cepat dari urllib)
    if title and artist:
        clean_title = title.split("(")[0].split("-")[0].strip()
        clean_artist = artist.split(",")[0].strip()
        url = (
            "https://lrclib.net/api/get?"
            f"track_name={urllib.parse.quote(clean_title)}"
            f"&artist_name={urllib.parse.quote(clean_artist)}"
        )
        data = await http_get_json(url, timeout=6)
        if data:
            if data.get("syncedLyrics"):
                return {"lyrics": data["syncedLyrics"], "synced": True, "source": "LRCLIB"}
            if data.get("plainLyrics"):
                return {"lyrics": data["plainLyrics"], "synced": False, "source": "LRCLIB"}

    # 2) Fallback YTMusic
    loop = asyncio.get_running_loop()

    def _fetch():
        try:
            with yt_lock:
                wp = yt.get_watch_playlist(videoId=v)
                lid = wp.get("lyrics")
                if lid:
                    ld = yt.get_lyrics(lid)
                    text = (ld or {}).get("lyrics", "").strip()
                    if text:
                        return {"lyrics": text, "synced": False, "source": "YouTube Music"}
        except Exception:
            pass
        return {"lyrics": "Lirik tidak tersedia.", "synced": False}

    return await loop.run_in_executor(IO_POOL, _fetch)

@app.get("/related")
async def related(v: str = Query("")):
    if not v:
        return {"tracks": []}
    loop = asyncio.get_running_loop()

    def _fetch():
        with yt_lock:
            try:
                wp = yt.get_watch_playlist(videoId=v, limit=15)
                tracks = wp.get("tracks", [])
                return {"tracks": [build_track_meta(t) for t in tracks if t.get("videoId") != v]}
            except Exception as e:
                logger.error(f"[RELATED] {e}")
                return {"tracks": [], "error": str(e)}

    return await loop.run_in_executor(IO_POOL, _fetch)

@app.get("/trending")
async def trending():
    now = time.time()
    if trending_cache_box["data"] and (now - trending_cache_box["ts"] < 1800):
        return trending_cache_box["data"]
    loop = asyncio.get_running_loop()

    def _fetch():
        with yt_lock:
            try:
                pl = yt.get_playlist("PL4fGSI1pDJn5kI81J1fYxT5vRUXf-5p_S", limit=20)
                tracks = pl.get("tracks", [])
                return {"tracks": [build_track_meta(t) for t in tracks]}
            except Exception:
                try:
                    results = yt.search("Trending Hits", filter="songs", limit=15)
                    return {"tracks": [build_track_meta(r) for r in results]}
                except Exception as e:
                    return {"tracks": [], "error": str(e)}

    data = await loop.run_in_executor(IO_POOL, _fetch)
    trending_cache_box["data"] = data
    trending_cache_box["ts"] = now
    return data

@app.get("/artist_details")
async def artist_details(id: str = Query("")):
    if not id:
        raise HTTPException(400, "Missing artist browseId")
    loop = asyncio.get_running_loop()

    def _fetch():
        with yt_lock:
            try:
                ad = yt.get_artist(id)
                songs_section = ad.get("songs", {})
                songs_list = songs_section.get("results", []) if songs_section else []
                if not songs_list:
                    bid = songs_section.get("browseId") if songs_section else None
                    if bid:
                        songs_list = yt.get_playlist(bid, limit=10).get("tracks", [])
                thumbs = ad.get("thumbnails", [{}])
                thumb = thumbs[-1].get("url") if thumbs else None
                return {
                    "name": ad.get("name"),
                    "description": ad.get("description", ""),
                    "thumbnail": thumb,
                    "tracks": [build_track_meta(s, fallback_artist=ad.get("name", "Unknown"),
                                                fallback_thumb=thumb) for s in songs_list[:12]],
                }
            except Exception as e:
                logger.error(f"[ARTIST] {e}")
                return {"error": str(e)}

    return await loop.run_in_executor(IO_POOL, _fetch)

@app.get("/album_details")
async def album_details(id: str = Query("")):
    if not id:
        raise HTTPException(400, "Missing album browseId")
    loop = asyncio.get_running_loop()

    def _fetch():
        with yt_lock:
            try:
                ad = yt.get_album(id)
                thumbs = ad.get("thumbnails", [{}])
                thumb = thumbs[-1].get("url") if thumbs else None
                tracks = [build_track_meta(t, fallback_thumb=thumb) for t in ad.get("tracks", [])]
                return {
                    "title": ad.get("title"),
                    "artist": ad.get("artist", "Unknown"),
                    "year": str(ad.get("year", "")),
                    "thumbnail": thumb,
                    "explicit": ad.get("isExplicit", False),
                    "tracks": tracks,
                }
            except Exception as e:
                return {"error": str(e)}

    return await loop.run_in_executor(IO_POOL, _fetch)

@app.get("/playlist_details")
async def playlist_details(
    id: str = Query(""),
    page: int = Query(1, ge=1),
    limit: int = Query(25, ge=1, le=100),
):
    if not id:
        raise HTTPException(400, "Missing playlist browseId")
    loop = asyncio.get_running_loop()

    def _fetch():
        with yt_lock:
            try:
                pd = yt.get_playlist(id, limit=limit * page)
                tracks = pd.get("tracks", [])
                offset = (page - 1) * limit
                page_tracks = tracks[offset:offset + limit]
                thumbs = pd.get("thumbnails", [])
                thumb = thumbs[-1]["url"] if thumbs else None
                return {
                    "title": pd.get("title"),
                    "description": pd.get("description", ""),
                    "thumbnail": thumb,
                    "total": len(tracks),
                    "page": page,
                    "tracks": [build_track_meta(t) for t in page_tracks],
                }
            except Exception as e:
                return {"error": str(e)}

    return await loop.run_in_executor(IO_POOL, _fetch)

