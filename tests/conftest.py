from __future__ import annotations

import sys
from pathlib import Path
from typing import Callable, Iterator

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.process_manager import LocalCluster  # noqa: E402
from tests.helpers import FAST_ENV  # noqa: E402


@pytest.hookimpl(tryfirst=True, hookwrapper=True)
def pytest_runtest_makereport(item, call):  # type: ignore[no-untyped-def]
    outcome = yield
    rep = outcome.get_result()
    setattr(item, "rep_" + rep.when, rep)


@pytest.fixture
def cluster_factory(tmp_path: Path, request: pytest.FixtureRequest) -> Iterator[Callable[..., LocalCluster]]:
    """Start real clusters (coordinator + N node processes); all are shut down after the test."""
    created: list[LocalCluster] = []

    def make(num_nodes: int = 3, env: dict[str, str] | None = None, start: bool = True) -> LocalCluster:
        cluster = LocalCluster(tmp_path / f"cluster{len(created)}", num_nodes=num_nodes,
                               settings_env={**FAST_ENV, **(env or {})})
        created.append(cluster)
        if start:
            cluster.start()
        return cluster

    yield make
    failed = getattr(request.node, "rep_call", None) is not None and request.node.rep_call.failed
    for cluster in created:
        cluster.shutdown()
        if failed:
            for log in sorted((cluster.base_dir / "logs").glob("*.log")):
                tail = log.read_text(errors="replace").splitlines()[-40:]
                sys.stderr.write(f"\n===== {log.name} (last 40 lines) =====\n" + "\n".join(tail) + "\n")
