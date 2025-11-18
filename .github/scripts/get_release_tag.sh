#!/usr/bin/env bash
set -euo pipefail

# Return the most recent tag if it exists; otherwise fall back to the workspace version.
latest_tag=$(git tag --list --sort=-creatordate | head -n1)
if [[ -n "${latest_tag}" ]]; then
  echo "${latest_tag}"
  exit 0
fi

version=$(
  python - <<'PY'
import pathlib
import tomllib

data = tomllib.loads(pathlib.Path("Cargo.toml").read_text())
print(data["workspace"]["package"]["version"])
PY
)
echo "v${version}"
