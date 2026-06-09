"""Entry point. Run: python -m backend.main (or python backend/main.py).

Behavior is 100% identical to the original single-file backend.py:
- core.py defines all classes, helpers, FastAPI app, lifespan, background tasks
- routes/*.py contain the original route handlers, exec'd into core's namespace
- This file mirrors the original `if __name__ == "__main__":` block.
"""
from backend import core            # initialize app, classes, helpers
from backend import routes as _routes  # registers all @app.* handlers
# Re-export commonly used names so `from backend.main import app` works
app = core.app

# Bring everything the original __main__ block references into local scope.
# The original block was top-level in backend.py, so it saw all module globals.
globals().update({k: v for k, v in vars(core).items() if not k.startswith("__")})

if __name__ == "__main__":
        print("=" * 68)
        print("  Music Backend v8.0 — Enterprise Sync Layer (JWT + Proxy + Radio)")
        print(f"  Listening on: http://0.0.0.0:{PORT}")
        print(f"  Features: orjson={HAVE_ORJSON} uvloop={HAVE_UVLOOP} "
              f"aiohttp={HAVE_AIOHTTP} rapidfuzz={HAVE_RAPIDFUZZ}")
        print(f"  Workers: io={MAX_WORKERS} ext={MAX_CONCURRENT_EXTRACTIONS} "
              f"dl={MAX_CONCURRENT_DOWNLOADS} prefetch_depth={PREFETCH_DEPTH}")
        print(f"  Auth: required={AUTH_REQUIRED} | Rate limiter: active")
        print(f"  v8.0 sync: EventStore | EventBus | Replay | ACK/Resend |")
        print(f"             SessionResume | MutationQueue | AuthoritativeClock |")
        print(f"             Checkpoint | VersionValidation | MultiDevice")
        print(f"  Endpoints: /events/replay /events/latest /playback/clock")
        print(f"             /bootstrap /sync /state /ws (session_id + last_event_id)")
        print("=" * 68)
    
        uvicorn.run(
            app,
            host="0.0.0.0",
            port=PORT,
            log_level="warning",
            access_log=False,
            loop="uvloop" if HAVE_UVLOOP else "asyncio",
            timeout_keep_alive=30,
            limit_concurrency=80,
            limit_max_requests=None,
            h11_max_incomplete_event_size=64 * 1024,
        )
