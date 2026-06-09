"""Routes: recommendation. Executed in core.__dict__ namespace by routes/__init__.py."""

@app.get("/recommendations")
async def recommendations(seed_video: str = Query(""), limit: int = Query(10)):
    # Cache hit
    if seed_video:
        with suppress(Exception):
            cached = recommendation_cache.get(seed_video) if hasattr(recommendation_cache, "get") else None
            if cached:
                return {"tracks": cached[:limit], "seed": [seed_video], "cached": True}

    loop = asyncio.get_running_loop()

    async def _seed_candidates() -> List[str]:
        seeds: List[str] = []
        if seed_video:
            seeds.append(seed_video)
        # weighting: top played
        if not seeds:
            rows = await db.fetch_all(
                "SELECT video_id FROM recommendation_seed ORDER BY score DESC, last_seen DESC LIMIT 5"
            )
            seeds.extend([r["video_id"] for r in rows if r.get("video_id")])
        if not seeds:
            rows = await db.fetch_all(
                "SELECT video_id FROM recently_played ORDER BY played_at DESC LIMIT 3"
            )
            seeds.extend([r["video_id"] for r in rows if r.get("video_id")])
        if not seeds:
            rows = await db.fetch_all(
                "SELECT video_id FROM favorites ORDER BY favorited_at DESC LIMIT 3"
            )
            seeds.extend([r["video_id"] for r in rows if r.get("video_id")])
        # dedupe, preserve order
        out = []
        seen = set()
        for s in seeds:
            if s and s not in seen:
                seen.add(s); out.append(s)
        return out

    seeds = await _seed_candidates()

    def _fetch(seeds_inner: List[str]):
        all_tracks = []
        seen = set(seeds_inner)
        with yt_lock:
            for vid in seeds_inner[:3]:
                try:
                    wp = yt.get_watch_playlist(videoId=vid, limit=limit)
                    for t in wp.get("tracks", []):
                        if t.get("videoId") and t["videoId"] not in seen:
                            all_tracks.append(build_track_meta(t))
                            seen.add(t["videoId"])
                except Exception:
                    pass
        random.shuffle(all_tracks)
        return all_tracks

    all_tracks = await loop.run_in_executor(IO_POOL, _fetch, seeds)
    if seed_video and all_tracks:
        with _rec_lock:
            recommendation_cache[seed_video] = all_tracks
    return {"tracks": all_tracks[:limit], "seed": seeds}

@app.get("/recommendations/similar/{videoId}")
async def recommendations_similar(videoId: str, limit: int = Query(10, ge=1, le=30)):
    """
    GET /recommendations/similar/{videoId}
    Rekomendasi berdasarkan kemiripan dengan track tertentu (watch playlist chaining).
    """
    if not videoId:
        raise HTTPException(400, "videoId diperlukan")

    # Cache hit
    with suppress(Exception):
        with _rec_lock:
            cached = recommendation_cache.get(videoId) if hasattr(recommendation_cache, "get") else None
        if cached:
            return {"tracks": cached[:limit], "seed": videoId, "cached": True}

    loop = asyncio.get_running_loop()

    def _fetch():
        tracks: List[Dict] = []
        seen: Set[str] = {videoId}
        with yt_lock:
            try:
                # Dapatkan related tracks langsung
                wp = yt.get_watch_playlist(videoId=videoId, limit=limit + 5)
                for t in wp.get("tracks", []):
                    vid = t.get("videoId")
                    if vid and vid not in seen:
                        tracks.append(build_track_meta(t))
                        seen.add(vid)
            except Exception as e:
                logger.warning(f"[REC_SIMILAR] {videoId}: {e}")
        return tracks

    tracks = await loop.run_in_executor(IO_POOL, _fetch)

    # Score berdasarkan history (boost jika pernah diputar)
    if tracks:
        history_ids: Set[str] = set()
        with suppress(Exception):
            rows = await db.fetch_all(
                "SELECT video_id FROM recently_played ORDER BY played_at DESC LIMIT 50"
            )
            history_ids = {r["video_id"] for r in rows}

        fav_ids: Set[str] = set()
        with suppress(Exception):
            rows = await db.fetch_all("SELECT video_id FROM favorites LIMIT 200")
            fav_ids = {r["video_id"] for r in rows}

        def _score(t: Dict) -> float:
            vid = t.get("videoId", "")
            s = 0.0
            if vid in history_ids:
                s += 20.0
            if vid in fav_ids:
                s += 30.0
            return s

        tracks.sort(key=_score, reverse=True)

        with _rec_lock:
            recommendation_cache[videoId] = tracks

    return {"tracks": tracks[:limit], "seed": videoId, "cached": False}

@app.get("/recommendations/personal")
async def recommendations_personal(limit: int = Query(15, ge=1, le=50)):
    """
    GET /recommendations/personal
    Rekomendasi personal berdasarkan:
    - Listening history (play_count weighted)
    - Favorites analysis
    - Top artists
    """
    # Get seeds: top played + favorites
    seeds: List[str] = []

    rows = await db.fetch_all(
        "SELECT video_id FROM recommendation_seed ORDER BY score DESC, last_seen DESC LIMIT 3"
    )
    seeds.extend([r["video_id"] for r in rows if r.get("video_id")])

    if not seeds:
        rows = await db.fetch_all(
            "SELECT video_id FROM recently_played ORDER BY played_at DESC LIMIT 3"
        )
        seeds.extend([r["video_id"] for r in rows if r.get("video_id")])

    if not seeds:
        rows = await db.fetch_all(
            "SELECT video_id FROM favorites ORDER BY favorited_at DESC LIMIT 3"
        )
        seeds.extend([r["video_id"] for r in rows if r.get("video_id")])

    if not seeds:
        return {"tracks": [], "message": "Belum ada history. Putar beberapa lagu dulu!"}

    loop = asyncio.get_running_loop()

    def _fetch_personal(seeds_list: List[str]):
        all_tracks: List[Dict] = []
        seen: Set[str] = set(seeds_list)
        with yt_lock:
            for seed_vid in seeds_list[:3]:
                try:
                    wp = yt.get_watch_playlist(videoId=seed_vid, limit=10)
                    for t in wp.get("tracks", []):
                        vid = t.get("videoId")
                        if vid and vid not in seen:
                            all_tracks.append(build_track_meta(t))
                            seen.add(vid)
                except Exception:
                    pass
        return all_tracks

    all_tracks = await loop.run_in_executor(IO_POOL, _fetch_personal, seeds)
    random.shuffle(all_tracks)

    # Cache per seed
    if seeds and all_tracks:
        with _rec_lock:
            recommendation_cache[seeds[0]] = all_tracks

    return {
        "tracks": all_tracks[:limit],
        "seeds": seeds,
        "type": "personal",
    }

