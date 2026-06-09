"""Routes: auth. Executed in core.__dict__ namespace by routes/__init__.py."""

@app.post("/auth/login")
async def auth_login(request: Request):
    """
    POST /auth/login
    Body: { "username": str, "password": str, "device": str (optional) }
    Returns: { access_token, refresh_token, expires_in, user }
    """
    ip = get_client_ip(request)
    await check_rate_limit(ip, "auth")

    try:
        data = await request.json()
    except Exception:
        raise HTTPException(400, "Invalid JSON body")

    username = (data.get("username") or "").strip()
    password = (data.get("password") or "").strip()
    device = (data.get("device") or "unknown")[:64]

    if not username or not password:
        raise HTTPException(400, "username dan password wajib diisi")

    # Verifikasi kredensial (single-user mode)
    stored_pass = os.environ.get("AUTH_PASS", DEFAULT_PASS)
    if not (hmac.compare_digest(username, DEFAULT_USER) and _check_password(password, stored_pass)):
        raise HTTPException(401, "Username atau password salah")

    access_token, refresh_token = await session_store.create_session(username, device)

    return {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "token_type": "bearer",
        "expires_in": JWT_ACCESS_TTL,
        "user": {"username": username},
    }

@app.post("/auth/refresh")
async def auth_refresh(request: Request):
    """
    POST /auth/refresh
    Body: { "refresh_token": str }
    Returns: { access_token, refresh_token, expires_in }
    """
    ip = get_client_ip(request)
    await check_rate_limit(ip, "auth")

    try:
        data = await request.json()
    except Exception:
        raise HTTPException(400, "Invalid JSON body")

    rt = (data.get("refresh_token") or "").strip()
    if not rt:
        raise HTTPException(400, "refresh_token wajib diisi")

    result = await session_store.refresh_session(rt)
    if result is None:
        raise HTTPException(401, "Refresh token tidak valid atau sudah expired")

    new_access, new_refresh = result
    return {
        "access_token": new_access,
        "refresh_token": new_refresh,
        "token_type": "bearer",
        "expires_in": JWT_ACCESS_TTL,
    }

@app.post("/auth/logout")
async def auth_logout(request: Request, authorization: Optional[str] = Header(None)):
    """
    POST /auth/logout
    Header: Authorization: Bearer <access_token>
    Merevoke session yang aktif.
    """
    user = await _get_current_user(authorization)
    if user:
        sid = user.get("sid")
        if sid:
            await session_store.revoke_session(sid)
    return {"status": "logged_out"}

@app.post("/auth/logout_all")
async def auth_logout_all(authorization: Optional[str] = Header(None)):
    """Revoke semua session user (logout dari semua device)."""
    user = await _get_current_user(authorization)
    if user:
        await session_store.revoke_all_sessions(user.get("sub", ""))
    return {"status": "all_sessions_revoked"}

@app.get("/auth/me")
async def auth_me(authorization: Optional[str] = Header(None)):
    """
    GET /auth/me
    Returns info user yang sedang login, atau anonymous jika auth tidak diaktifkan.
    """
    user = await _get_current_user(authorization)
    if user:
        sessions = await session_store.get_sessions(user.get("sub", ""))
        return {
            "authenticated": True,
            "username": user.get("sub"),
            "session_id": user.get("sid"),
            "sessions_count": len(sessions),
            "issued_at": user.get("iat"),
            "expires_at": user.get("exp"),
        }
    if AUTH_REQUIRED:
        raise HTTPException(401, "Tidak terautentikasi")
    return {"authenticated": False, "username": "anonymous", "auth_required": False}

@app.get("/auth/sessions")
async def auth_sessions(authorization: Optional[str] = Header(None)):
    """Daftar sesi aktif user yang sedang login."""
    user = await _get_current_user(authorization)
    if not user:
        if AUTH_REQUIRED:
            raise HTTPException(401, "Tidak terautentikasi")
        return {"sessions": []}
    sessions = await session_store.get_sessions(user.get("sub", ""))
    return {"sessions": sessions}

