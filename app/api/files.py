from __future__ import annotations

from fastapi import APIRouter, Depends, File, Path, Query, UploadFile
from fastapi.responses import FileResponse
from starlette.background import BackgroundTask

from app.api.deps import get_coordinator
from app.core.validation import FILE_ID_PATTERN
from app.runtime import Coordinator
from app.schemas.files import DeleteResult, ErrorResponse, FileDetail, FileList

router = APIRouter(prefix="/api/v1/files", tags=["files"])

_ERRORS = {
    400: {"model": ErrorResponse}, 404: {"model": ErrorResponse}, 409: {"model": ErrorResponse},
    413: {"model": ErrorResponse}, 500: {"model": ErrorResponse}, 503: {"model": ErrorResponse},
}
FileId = Path(..., pattern=FILE_ID_PATTERN, description="32-char hex file id")


@router.post("/upload", status_code=201, response_model=FileDetail, responses=_ERRORS)
def upload_file(file: UploadFile = File(...), coord: Coordinator = Depends(get_coordinator)) -> dict:
    """Chunk, place (consistent hashing) and replicate a file; returns its committed metadata."""
    record = coord.upload.upload(file.filename, file.file)
    return coord.files.describe(record.file_id)


@router.get("", response_model=FileList)
def list_files(
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    name: str | None = Query(None, max_length=255, description="substring filter on filename"),
    coord: Coordinator = Depends(get_coordinator),
) -> dict:
    """Committed files only (uploads in progress, failed and deleted files are excluded)."""
    return coord.files.list_files(limit, offset, name)


@router.get("/{file_id}", response_model=FileDetail, responses=_ERRORS)
def get_file(file_id: str = FileId, coord: Coordinator = Depends(get_coordinator)) -> dict:
    return coord.files.describe(file_id)


@router.get("/{file_id}/download", responses={**_ERRORS, 200: {"content": {"application/octet-stream": {}}}})
def download_file(file_id: str = FileId, coord: Coordinator = Depends(get_coordinator)) -> FileResponse:
    """Reassemble from verified replicas; the body is sent only after the whole-file checksum matches."""
    record, path, served = coord.download.prepare(file_id)
    return FileResponse(
        path,
        media_type="application/octet-stream",
        filename=record.filename,
        headers={"X-Checksum-SHA256": record.checksum or "", "X-File-Id": record.file_id,
                 "X-Chunks-Served-By": ",".join(f"{i}:{n}" for i, n in sorted(served.items()))[:4000]},
        background=BackgroundTask(path.unlink, missing_ok=True),
    )


@router.delete("/{file_id}", response_model=DeleteResult, responses=_ERRORS)
def delete_file(file_id: str = FileId, coord: Coordinator = Depends(get_coordinator)) -> dict:
    """state=DELETED when all replicas are gone; state=DELETING with pending_cleanup otherwise."""
    return coord.deletion.delete_file(file_id)
