import os
import time
from collections import defaultdict, deque

from fastapi import Depends, FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, RedirectResponse

from backend.app.routes import dashboard, revenue_leaks, stripe_connector
from backend.app.security import require_api_key

app = FastAPI(title="HE Revenue Leaks API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

_RATE_LIMIT_PER_MIN = int(os.getenv("HE_RATE_LIMIT_PER_MIN", "120"))
_hits = defaultdict(deque)


@app.get("/")
def root_redirect():
    return RedirectResponse(url="/dashboard", status_code=307)


@app.middleware("http")
async def request_log_and_rate_limit(request: Request, call_next):
    start = time.time()

    if request.url.path.startswith("/api/"):
        api_key = request.headers.get("X-API-Key", "")
        now = time.time()
        q = _hits[api_key]
        while q and (now - q[0]) > 60:
            q.popleft()
        if len(q) >= _RATE_LIMIT_PER_MIN:
            return JSONResponse(status_code=429, content={"code": "rate_limited", "message": "Too many requests"})
        q.append(now)

    response = await call_next(request)
    elapsed_ms = int((time.time() - start) * 1000)
    print(f"[api] {request.method} {request.url.path} status={response.status_code} ms={elapsed_ms}")
    return response


app.include_router(dashboard.router)
app.include_router(stripe_connector.router, prefix="/api", dependencies=[Depends(require_api_key)])
app.include_router(revenue_leaks.router, prefix="/api", dependencies=[Depends(require_api_key)])
