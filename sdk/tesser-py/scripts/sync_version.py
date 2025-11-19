#!/usr/bin/env python3
"""Sync the Python SDK version with the workspace Cargo version."""

from __future__ import annotations

from pathlib import Path
import sys

try:  # Python 3.11+
    import tomllib
except ModuleNotFoundError:  # pragma: no cover
    import tomli as tomllib  # type: ignore


def normalize_version(version: str) -> str:
    if "-" not in version:
        return version
    base, pre = version.split("-", 1)
    pre = pre.replace("alpha", "a").replace("beta", "b")
    pre = pre.replace("rc", "rc").replace(".", "")
    return f"{base}{pre}"


def read_workspace_version(cargo_path: Path) -> str:
    with cargo_path.open("rb") as handle:
        data = tomllib.load(handle)
    if "workspace" in data and "package" in data["workspace"]:
        return data["workspace"]["package"]["version"]
    if "package" in data:
        return data["package"]["version"]
    raise RuntimeError("Unable to determine version from Cargo.toml")


def update_pyproject(pyproject_path: Path, new_version: str) -> None:
    lines = pyproject_path.read_text(encoding="utf-8").splitlines()
    updated = []
    replaced = False
    for line in lines:
        if line.strip().startswith("version =") and not replaced:
            updated.append(f'version = "{new_version}"')
            replaced = True
        else:
            updated.append(line)
    if not replaced:
        raise RuntimeError("Failed to update version in pyproject.toml")
    pyproject_path.write_text("\n".join(updated) + "\n", encoding="utf-8")


def main() -> None:
    scripts_dir = Path(__file__).resolve().parent
    project_root = scripts_dir.parents[2]
    cargo_path = project_root / "Cargo.toml"
    pyproject_path = scripts_dir.parent / "pyproject.toml"

    if not cargo_path.exists():
        print(f"Cargo.toml not found at {cargo_path}", file=sys.stderr)
        sys.exit(1)

    rust_version = read_workspace_version(cargo_path)
    python_version = normalize_version(rust_version)

    update_pyproject(pyproject_path, python_version)
    print(f"Synced Python version to {python_version}")


if __name__ == "__main__":
    main()
