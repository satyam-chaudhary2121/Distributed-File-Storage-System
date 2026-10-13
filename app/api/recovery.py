from __future__ import annotations

from fastapi import APIRouter, Body, Depends

from app.api.deps import get_coordinator
from app.runtime import Coordinator
from app.schemas.cluster import RecoveryTriggerRequest

router = APIRouter(prefix="/api/v1/recovery", tags=["recovery"])


@router.get("/status")
def recovery_status(coord: Coordinator = Depends(get_coordinator)) -> dict:
    return coord.recovery.status()


@router.post("/trigger")
def trigger_recovery(body: RecoveryTriggerRequest = Body(default_factory=RecoveryTriggerRequest),
                     coord: Coordinator = Depends(get_coordinator)) -> dict:
    """Run one recovery cycle now. ``scrub`` verifies every replica's bytes; ``force`` ignores backoff."""
    return coord.recovery.run_cycle(scrub=body.scrub, force=body.force, trigger="api")
