#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_ACTIVATE="$ROOT_DIR/airflow_env/bin/activate"

if [[ ! -f "$VENV_ACTIVATE" ]]; then
  echo "Airflow virtualenv not found at $VENV_ACTIVATE" >&2
  exit 1
fi

# shellcheck disable=SC1090
source "$VENV_ACTIVATE"
cd "$ROOT_DIR"

echo "Airflow environment activated in $ROOT_DIR"
if [[ $# -gt 0 ]]; then
  exec "${SHELL:-/bin/bash}" -lc "$*"
fi

exec "${SHELL:-/bin/bash}" -i
