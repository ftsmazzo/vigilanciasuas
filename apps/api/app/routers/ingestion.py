from datetime import datetime
from pathlib import Path
from uuid import uuid4

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile, status

from ..config import settings
from ..deps import get_current_user
from ..models import User

router = APIRouter(prefix="/ingestion", tags=["ingestion"])

ALLOWED_EXTENSIONS = {".csv", ".xlsx"}


@router.post("/upload")
async def upload_raw_file(
    file: UploadFile = File(...),
    source: str = Form("manual"),
    current_user: User = Depends(get_current_user),
):
    extension = Path(file.filename or "").suffix.lower()
    if extension not in ALLOWED_EXTENSIONS:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Formato inválido. Envie CSV ou XLSX.",
        )

    upload_dir = Path(settings.upload_dir)
    upload_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    safe_name = f"{timestamp}_{uuid4().hex}{extension}"
    target = upload_dir / safe_name

    content = await file.read()
    target.write_bytes(content)

    return {
        "status": "uploaded",
        "stored_filename": safe_name,
        "original_filename": file.filename,
        "source": source,
        "size_bytes": len(content),
        "uploaded_by": current_user.email,
    }
