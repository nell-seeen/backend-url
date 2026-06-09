"""Route loader.

Each sibling .py file contains FastAPI route handlers extracted verbatim from
the original backend.py. They reference module-level names (e.g. `app`,
`queue_async_lock`, helper functions) that live in `backend.core`. To preserve
100% identical behavior (including late-bound globals reassigned in lifespan),
we exec each route file into `core.__dict__` so the handler functions get
`core` as their `__globals__`.
"""
from pathlib import Path
from backend import core

_DIR = Path(__file__).parent
# Deterministic load order
_ORDER = [
    "system.py",
    "auth.py",
    "search.py",
    "stream.py",
    "media.py",
    "playlist.py",
    "downloads.py",
    "queue.py",
    "playback.py",
    "recommendation.py",
    "cache_admin.py",
    "websocket.py",
    "media_proxy.py",
    "radio.py",
    "events.py",
    "settings.py",
    "upgrade.py",    # v9.0 additive endpoints
    "upgrade2.py",   # v9.1 final polish — MUST be last
]
for _name in _ORDER:
    _p = _DIR / _name
    if not _p.exists():
        continue
    _src = _p.read_text(encoding="utf-8")
    exec(compile(_src, str(_p), "exec"), core.__dict__)
