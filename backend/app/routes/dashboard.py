from fastapi import APIRouter
from fastapi.responses import FileResponse
from pathlib import Path

router = APIRouter()

FRONTEND_INDEX = Path(__file__).resolve().parents[3] / "frontend" / "index.html"


@router.get("/dashboard")
def dashboard():
    if FRONTEND_INDEX.exists():
        return FileResponse(str(FRONTEND_INDEX))
    return {"status": "missing_frontend", "message": f"{FRONTEND_INDEX} not found"}
