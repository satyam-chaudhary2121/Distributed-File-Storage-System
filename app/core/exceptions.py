"""Domain exceptions. Each maps to an HTTP status in the API layer."""

from __future__ import annotations

from typing import Any


class DFSError(Exception):
    status_code = 500
    code = "INTERNAL_ERROR"

    def __init__(self, message: str = "", *, details: dict[str, Any] | None = None) -> None:
        self.message = message or self.code
        self.details = details or {}
        super().__init__(self.message)

    def to_dict(self) -> dict[str, Any]:
        return {"code": self.code, "message": self.message, "details": self.details}


class ValidationFailed(DFSError):
    status_code = 400
    code = "VALIDATION_ERROR"


class Unauthorized(DFSError):
    status_code = 401
    code = "UNAUTHORIZED"


class NotFound(DFSError):
    status_code = 404
    code = "NOT_FOUND"


class Conflict(DFSError):
    status_code = 409
    code = "CONFLICT"


class InvalidStateTransition(Conflict):
    code = "INVALID_STATE_TRANSITION"


class Gone(DFSError):
    status_code = 410
    code = "GONE"


class PayloadTooLarge(DFSError):
    status_code = 413
    code = "PAYLOAD_TOO_LARGE"


class InsufficientNodes(DFSError):
    status_code = 503
    code = "INSUFFICIENT_NODES"


class UploadFailed(DFSError):
    status_code = 503
    code = "UPLOAD_FAILED"


class ChunkUnavailable(DFSError):
    """No reachable replica right now, but valid copies may exist on down nodes."""

    status_code = 503
    code = "CHUNK_UNAVAILABLE"


class DataLoss(DFSError):
    """Every known replica of a chunk is corrupted or missing."""

    status_code = 500
    code = "DATA_LOSS"


class IntegrityFailure(DFSError):
    status_code = 500
    code = "INTEGRITY_FAILURE"
