"""Routes: search. Executed in core.__dict__ namespace by routes/__init__.py."""

@app.get("/search")
async def search(
    q: str = Query(""),
    type: str = Query("songs"),
    page: int = Query(1, ge=1),
    limit: int = Query(15, ge=1, le=50),
    request: Request = None,
):
    if request:
        ip = get_client_ip(request)
        await check_rate_limit(ip, "search")

    q = (q or "").strip()
    if not q:
        return []

    cache_key = f"{q}::{type}::{page}::{limit}"
    cached = None
    with suppress(Exception):
        cached = search_result_cache.get(cache_key)
    if cached is not None:
        return cached

    # Save search history (async, fire & forget)
    with suppress(Exception):
        asyncio.create_task(db.execute(
            "INSERT INTO search_history(query,type,ts) VALUES(?,?,?)",
            (q, type, time.time()),
        ))

    loop = asyncio.get_running_loop()

    def _search():
        with yt_lock:
            offset = (page - 1) * limit
            results = yt.search(q, filter=type, limit=limit + offset)
            results = results[offset:offset + limit] if offset < len(results) else results[:limit]
            data = []
            for i in results:
                item_type = i.get("resultType", type)
                artists_list = i.get("artists", [])
                artist_name = artists_list[0]["name"] if artists_list else i.get("artist", "Unknown")
                browse_id = i.get("browseId")
                artist_id = None
                if artists_list:
                    artist_id = artists_list[0].get("id") or artists_list[0].get("browseId")
                if item_type == "artist" and not browse_id:
                    browse_id = i.get("id") or artist_id
                thumbs = i.get("thumbnails", [])
                duration_raw = i.get("duration") or i.get("duration_seconds")
                duration_str = ""
                if isinstance(duration_raw, int):
                    m, s = divmod(duration_raw, 60)
                    duration_str = f"{m}:{s:02d}"
                elif isinstance(duration_raw, str):
                    duration_str = duration_raw
                data.append({
                    "title": i.get("title") or i.get("album") or i.get("artist") or i.get("name"),
                    "artist": artist_name,
                    "artistBrowseId": artist_id,
                    "album": i.get("album", {}).get("name", "") if isinstance(i.get("album"), dict) else i.get("album", ""),
                    "duration": duration_str,
                    "year": str(i.get("year", "")),
                    "videoId": i.get("videoId"),
                    "browseId": browse_id,
                    "thumbnail": thumbs[-1]["url"] if thumbs else None,
                    "type": item_type,
                    "explicit": i.get("isExplicit", False),
                })
            # Fuzzy re-rank ringan: boost item dengan title/artist mirip query
            try:
                ql = q.lower()
                def score(it):
                    s1 = fuzzy_score(ql, it.get("title", ""))
                    s2 = fuzzy_score(ql, it.get("artist", ""))
                    return max(s1, s2 * 0.85)
                data.sort(key=score, reverse=True)
            except Exception:
                pass
            return data

    result = await loop.run_in_executor(IO_POOL, _search)
    with suppress(Exception):
        search_result_cache[cache_key] = result
    return result

@app.get("/search/suggestions")
async def search_suggestions(q: str = Query("")):
    q = (q or "").strip()
    if not q:
        return []
    with suppress(Exception):
        cached = suggestion_cache.get(q)
        if cached is not None:
            return cached

    def _fetch_suggestions():
        with yt_lock:
            try:
                return yt.get_search_suggestions(q)
            except Exception as e:
                logger.error(f"[SUGGESTIONS] {e}")
                return []

    loop = asyncio.get_running_loop()
    res = await loop.run_in_executor(IO_POOL, _fetch_suggestions)
    with suppress(Exception):
        suggestion_cache[q] = res
    return res

@app.get("/search_history")
async def get_search_history(limit: int = Query(20)):
    rows = await db.fetch_all(
        "SELECT query, type, ts as timestamp FROM search_history ORDER BY ts DESC LIMIT ?",
        (limit,),
    )
    return rows or []

@app.delete("/search_history")
async def clear_search_history():
    await db.execute("DELETE FROM search_history")
    return {"status": "cleared"}

@app.get("/search/suggest")
async def search_suggest(q: str = Query(""), request: Request = None):
    """
    GET /search/suggest?q=...
    Autocomplete suggestions: local history → YTMusic suggestions.
    Ringan, cached, cocok untuk debounced typing.
    """
    q = (q or "").strip()
    if not q or len(q) < 2:
        return {"suggestions": []}

    ip = get_client_ip(request) if request else "unknown"
    await check_rate_limit(ip, "search")

    # Cache hit
    cache_key = f"suggest:{q}"
    with suppress(Exception):
        cached = suggestion_cache.get(cache_key)
        if cached is not None:
            return {"suggestions": cached}

    suggestions: List[str] = []

    # Layer 1: local search history yang cocok
    rows = await db.fetch_all(
        "SELECT DISTINCT query FROM search_history WHERE query LIKE ? ORDER BY ts DESC LIMIT 5",
        (f"%{q}%",),
    )
    for r in rows:
        if r.get("query") and r["query"] not in suggestions:
            suggestions.append(r["query"])

    # Layer 2: local search index (title/artist)
    idx_rows = await db.fetch_all(
        "SELECT title, artist FROM local_search_index "
        "WHERE title LIKE ? OR artist LIKE ? ORDER BY play_count DESC LIMIT 5",
        (f"%{q}%", f"%{q}%"),
    )
    for r in idx_rows:
        for field in ("title", "artist"):
            v = r.get(field, "")
            if v and v not in suggestions and q.lower() in v.lower():
                suggestions.append(v)

    # Layer 3: YTMusic suggestions (jika belum cukup)
    if len(suggestions) < 5:
        def _yt_suggest():
            with yt_lock:
                try:
                    return yt.get_search_suggestions(q)
                except Exception:
                    return []
        loop = asyncio.get_running_loop()
        yt_sug = await loop.run_in_executor(IO_POOL, _yt_suggest)
        for s in yt_sug:
            if s and s not in suggestions:
                suggestions.append(s)

    suggestions = suggestions[:10]
    with suppress(Exception):
        suggestion_cache[cache_key] = suggestions
    return {"suggestions": suggestions}

@app.get("/search/popular")
async def search_popular(limit: int = Query(10, ge=1, le=50)):
    """
    GET /search/popular
    Top queries berdasarkan frekuensi search dari history.
    """
    rows = await db.fetch_all(
        "SELECT query, COUNT(*) as count FROM search_history "
        "GROUP BY query ORDER BY count DESC, MAX(ts) DESC LIMIT ?",
        (limit,),
    )
    return {"popular": [{"query": r["query"], "count": r["count"]} for r in rows]}

@app.get("/search/history")
async def search_history_v2(limit: int = Query(20, ge=1, le=100)):
    """
    GET /search/history — alias baru untuk /search_history
    Mengembalikan riwayat pencarian user dengan timestamp.
    """
    rows = await db.fetch_all(
        "SELECT query, type, ts as timestamp FROM search_history ORDER BY ts DESC LIMIT ?",
        (limit,),
    )
    return {"history": rows or []}

