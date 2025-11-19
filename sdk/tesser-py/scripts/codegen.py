#!/usr/bin/env python3
"""Generate Python gRPC stubs from the shared proto definition."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path


SDK_ROOT = Path(__file__).resolve().parents[1]
PROJECT_ROOT = SDK_ROOT.parents[1]
PROTO_DIR = PROJECT_ROOT / "tesser-rpc" / "proto"
OUT_DIR = SDK_ROOT / "src" / "tesser" / "protos"


def run() -> None:
    if not PROTO_DIR.exists():
        print(f"error: proto directory not found at {PROTO_DIR}", file=sys.stderr)
        sys.exit(1)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUT_DIR / "__init__.py").touch()

    proto_files = sorted(PROTO_DIR.glob("*.proto"))
    if not proto_files:
        print("error: no proto files found", file=sys.stderr)
        sys.exit(1)

    cmd = [
        sys.executable,
        "-m",
        "grpc_tools.protoc",
        f"-I{PROTO_DIR}",
        f"--python_out={OUT_DIR}",
        f"--pyi_out={OUT_DIR}",
        f"--grpc_python_out={OUT_DIR}",
        *map(str, proto_files),
    ]

    print("Generating Python stubs...")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(result.stderr, file=sys.stderr)
        sys.exit(result.returncode)

    for suffix in ("py", "pyi"):
        path = OUT_DIR / f"tesser_pb2.{suffix}"
        if path.exists():
            path.write_text(_rewrite_imports(path.read_text()))
        path = OUT_DIR / f"tesser_pb2_grpc.{suffix}"
        if path.exists():
            path.write_text(_rewrite_imports(path.read_text()))

    print("Protobuf generation complete.")


def _rewrite_imports(content: str) -> str:
    content = content.replace("import tesser_pb2 as", "from . import tesser_pb2 as")
    content = content.replace("import tesser_pb2\n", "from . import tesser_pb2\n")
    return content


if __name__ == "__main__":
    run()
