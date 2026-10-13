from __future__ import annotations

import hmac

from fastapi import Header, Request

from app.core.exceptions import Unauthorized
from app.runtime import Coordinator


def get_coordinator(request: Request) -> Coordinator:
    return request.app.state.coordinator


def require_cluster_token(request: Request, x_cluster_token: str | None = Header(default=None)) -> None:
    """Internal endpoints (node registration/heartbeat) require the shared cluster secret."""
    secret = request.app.state.coordinator.settings.cluster_secret
    if not x_cluster_token or not hmac.compare_digest(x_cluster_token.encode(), secret.encode()):
        raise Unauthorized("missing or invalid X-Cluster-Token")
