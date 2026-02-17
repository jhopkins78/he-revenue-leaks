from fastapi import Depends, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from backend.app.routes import stripe_connector
from backend.app.routes import revenue_leaks
from backend.app.security import require_api_key

app = FastAPI(title="HE Revenue Leaks API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(stripe_connector.router, prefix="/api", dependencies=[Depends(require_api_key)])
app.include_router(revenue_leaks.router, prefix="/api", dependencies=[Depends(require_api_key)])
