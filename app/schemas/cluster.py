from __future__ import annotations

from pydantic import BaseModel


class RecoveryTriggerRequest(BaseModel):
    scrub: bool = False
    force: bool = False
