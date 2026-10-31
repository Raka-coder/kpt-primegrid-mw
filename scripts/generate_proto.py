"""Generate Python gRPC stubs from the single canonical .proto file."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PROTO_ROOT = ROOT / "proto"
PROTO_FILE = PROTO_ROOT / "primegrid" / "primegrid.proto"
OUTPUT = ROOT / "src"


def main() -> int:
    command = [
        sys.executable,
        "-m",
        "grpc_tools.protoc",
        f"-I{PROTO_ROOT}",
        f"--python_out={OUTPUT}",
        f"--grpc_python_out={OUTPUT}",
        str(PROTO_FILE),
    ]
    subprocess.run(command, check=True, cwd=ROOT)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
