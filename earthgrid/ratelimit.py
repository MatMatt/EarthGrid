"""Built-in request rate limiter — no external dependencies.

Protects EarthGrid nodes from abuse without requiring nginx/reverse proxy config.
Uses a sliding window counter per IP address.
"""
import time
from collections import defaultdict
from threading import Lock

from fastapi import Request, HTTPException
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse


class RateLimitMiddleware(BaseHTTPMiddleware):
    """Simple sliding-window rate limiter per client IP.

    Args:
        app: ASGI application.
        requests_per_minute: Max requests per IP per minute (default: 120).
        burst: Max requests in a 2-second burst window (default: 20).
    """

    def __init__(self, app, requests_per_minute: int = 120, burst: int = 20):
        super().__init__(app)
        self.rpm = requests_per_minute
        self.burst = burst
        self.windows: dict[str, list[float]] = defaultdict(list)
        self.lock = Lock()
        self._last_cleanup = time.monotonic()

    def _client_ip(self, request: Request) -> str:
        # Respect X-Forwarded-For if behind reverse proxy
        forwarded = request.headers.get("x-forwarded-for")
        if forwarded:
            return forwarded.split(",")[0].strip()
        return request.client.host if request.client else "unknown"

    def _cleanup(self, now: float):
        """Remove old entries every 60 seconds."""
        if now - self._last_cleanup < 60:
            return
        self._last_cleanup = now
        cutoff = now - 60
        stale = [ip for ip, times in self.windows.items() if not times or times[-1] < cutoff]
        for ip in stale:
            del self.windows[ip]

    async def dispatch(self, request: Request, call_next):
        # Skip rate limiting for health checks
        if request.url.path in ("/health", "/"):
            return await call_next(request)

        now = time.monotonic()
        ip = self._client_ip(request)

        with self.lock:
            self._cleanup(now)
            times = self.windows[ip]

            # Remove entries older than 60s
            cutoff_60 = now - 60
            while times and times[0] < cutoff_60:
                times.pop(0)

            # Check per-minute limit
            if len(times) >= self.rpm:
                return JSONResponse(
                    status_code=429,
                    content={"detail": "Rate limit exceeded. Try again later."},
                    headers={"Retry-After": "60"},
                )

            # Check burst limit (last 2 seconds)
            cutoff_2 = now - 2
            recent = sum(1 for t in times if t >= cutoff_2)
            if recent >= self.burst:
                return JSONResponse(
                    status_code=429,
                    content={"detail": "Too many requests. Slow down."},
                    headers={"Retry-After": "2"},
                )

            times.append(now)

        return await call_next(request)
