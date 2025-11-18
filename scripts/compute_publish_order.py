#!/usr/bin/env python3
"""
Compute a dependency-respecting publish order for the Cargo workspace.
Outputs crate names one per line.
"""

from __future__ import annotations

import json
import subprocess
from collections import defaultdict, deque
from typing import Dict, List, Set


def load_metadata() -> dict:
    metadata_json = subprocess.check_output(
        ["cargo", "metadata", "--format-version", "1", "--locked"],
        text=True,
    )
    return json.loads(metadata_json)


def compute_order(metadata: dict) -> List[str]:
    workspace: Set[str] = set(metadata["workspace_members"])
    packages: Dict[str, dict] = {
        pkg["id"]: pkg for pkg in metadata["packages"] if pkg["id"] in workspace
    }
    name_to_id = {pkg["name"]: pkg_id for pkg_id, pkg in packages.items()}

    edges: Dict[str, Set[str]] = defaultdict(set)  # dep -> dependents
    indegree: Dict[str, int] = {pkg_id: 0 for pkg_id in packages}

    for pkg_id, pkg in packages.items():
        for dep in pkg.get("dependencies", []):
            dep_id = dep.get("package") or name_to_id.get(dep["name"])
            if dep_id in workspace:
                edges[dep_id].add(pkg_id)
                indegree[pkg_id] += 1

    queue = deque(
        sorted(
            [pkg_id for pkg_id, deg in indegree.items() if deg == 0],
            key=lambda pid: packages[pid]["name"],
        )
    )

    order: List[str] = []
    while queue:
        current = queue.popleft()
        order.append(packages[current]["name"])
        for nxt in sorted(edges[current], key=lambda pid: packages[pid]["name"]):
            indegree[nxt] -= 1
            if indegree[nxt] == 0:
                queue.append(nxt)

    if len(order) != len(packages):
        raise SystemExit("Unable to compute publish order; cycle detected?")

    return order


def main() -> None:
    metadata = load_metadata()
    order = compute_order(metadata)
    print("\n".join(order))


if __name__ == "__main__":
    main()
