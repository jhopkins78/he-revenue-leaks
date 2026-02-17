from fastapi import APIRouter
from fastapi.responses import FileResponse
from pathlib import Path

router = APIRouter()


@router.get("/dashboard")
def dashboard():
    p = Path("frontend/index.html")
    if p.exists():
        return FileResponse(str(p))
    return {"status": "missing_frontend", "message": "frontend/index.html not found"}
