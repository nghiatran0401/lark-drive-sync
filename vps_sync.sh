#!/usr/bin/env bash
set -euo pipefail

# One-script runner for VPS setup + long-running sync.
# Usage examples:
#   bash vps_sync.sh prepare
#   bash vps_sync.sh start 2
#   bash vps_sync.sh status
#   bash vps_sync.sh logs
#   bash vps_sync.sh stop

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="${ROOT_DIR}/.venv"
REPORT_DIR=""
RUN_LOG=""
MAPPING_OUT=""
FAILED_OUT=""
PID_FILE=""

cmd="${1:-help}"
concurrency="${2:-${SIMPLE_SYNC_CONCURRENCY:-2}}"

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1"
    exit 1
  fi
}

ensure_env_file() {
  if [[ ! -f "${ROOT_DIR}/.env" ]]; then
    echo "Missing .env in project root: ${ROOT_DIR}/.env"
    echo "Create it first, then run again."
    exit 1
  fi
}

load_runtime_paths() {
  ensure_env_file
  set +u
  # shellcheck disable=SC1091
  source "${ROOT_DIR}/.env"
  set -u

  local raw_profile
  raw_profile="${DRIVE_PROFILE:-${DRIVE_ACCOUNT_ID:-default-drive}}"
  local slug
  slug="$(printf "%s" "${raw_profile}" | tr '[:upper:]' '[:lower:]' | sed -E 's/[^a-z0-9._-]+/-/g; s/^-+//; s/-+$//')"
  if [[ -z "${slug}" ]]; then
    slug="default-drive"
  fi

  REPORT_DIR="${ROOT_DIR}/reports/drives/${slug}"
  RUN_LOG="${REPORT_DIR}/run.log"
  MAPPING_OUT="${SIMPLE_SYNC_MAPPING_OUT:-${REPORT_DIR}/mappings.csv}"
  FAILED_OUT="${SIMPLE_SYNC_FAILED_OUT:-${REPORT_DIR}/failed_items.csv}"
  PID_FILE="${ROOT_DIR}/.sync-${slug}.pid"
}

validate_env_vars() {
  set +u
  # shellcheck disable=SC1091
  source "${ROOT_DIR}/.env"
  set -u

  local missing=0

  for k in DRIVE_ACCOUNT_ID DRIVE_ROOT_FOLDER_ID LARK_ROOT_FOLDER_ID; do
    if [[ -z "${!k:-}" ]]; then
      echo "Missing required .env variable: ${k}"
      missing=1
    fi
  done

  if [[ -z "${GOOGLE_ACCESS_TOKEN:-}" ]]; then
    if [[ -z "${GOOGLE_CLIENT_ID:-}" || -z "${GOOGLE_CLIENT_SECRET:-}" || -z "${GOOGLE_REFRESH_TOKEN:-}" ]]; then
      echo "Missing Google auth in .env."
      echo "Set GOOGLE_ACCESS_TOKEN or set GOOGLE_CLIENT_ID + GOOGLE_CLIENT_SECRET + GOOGLE_REFRESH_TOKEN."
      missing=1
    fi
  fi

  if [[ -z "${LARK_USER_ACCESS_TOKEN:-}" && -z "${LARK_ACCESS_TOKEN:-}" ]]; then
    if [[ -z "${LARK_APP_ID:-}" || -z "${LARK_APP_SECRET:-}" ]]; then
      echo "Missing Lark auth in .env."
      echo "Set one of: LARK_USER_ACCESS_TOKEN, LARK_ACCESS_TOKEN, or LARK_APP_ID + LARK_APP_SECRET."
      missing=1
    fi
  fi

  if [[ "${missing}" -ne 0 ]]; then
    exit 1
  fi
}

prepare() {
  require_cmd python3
  load_runtime_paths

  mkdir -p "${REPORT_DIR}"

  if [[ ! -d "${VENV_DIR}" ]]; then
    python3 -m venv "${VENV_DIR}"
  fi

  # shellcheck disable=SC1091
  source "${VENV_DIR}/bin/activate"
  python -m ensurepip --upgrade >/dev/null 2>&1 || true
  python -m pip install --upgrade pip setuptools wheel
  python -m pip install -e .

  validate_env_vars
  echo "Prepare complete."
}

is_running() {
  if [[ -f "${PID_FILE}" ]]; then
    local pid
    pid="$(cat "${PID_FILE}")"
    if ps -p "${pid}" >/dev/null 2>&1; then
      return 0
    fi
  fi
  return 1
}

start_sync() {
  load_runtime_paths
  validate_env_vars
  mkdir -p "${REPORT_DIR}"

  if is_running; then
    echo "Sync is already running (pid=$(cat "${PID_FILE}"))."
    exit 0
  fi

  # shellcheck disable=SC1091
  source "${VENV_DIR}/bin/activate"

  : > "${RUN_LOG}"
  nohup env LARK_TOKEN_MODE="${LARK_TOKEN_MODE:-auto}" PYTHONPATH=src \
    python -m migration.cli --concurrency "${concurrency}" --mapping-out "${MAPPING_OUT}" --failed-out "${FAILED_OUT}" \
    >> "${RUN_LOG}" 2>&1 &

  echo $! > "${PID_FILE}"
  echo "Started sync with concurrency=${concurrency}, pid=$(cat "${PID_FILE}")"
  echo "Log: ${RUN_LOG}"
}

stop_sync() {
  load_runtime_paths
  if ! is_running; then
    echo "Sync is not running."
    rm -f "${PID_FILE}"
    exit 0
  fi
  local pid
  pid="$(cat "${PID_FILE}")"
  kill "${pid}" || true
  sleep 1
  if ps -p "${pid}" >/dev/null 2>&1; then
    kill -9 "${pid}" || true
  fi
  rm -f "${PID_FILE}"
  echo "Stopped sync."
}

status_sync() {
  load_runtime_paths
  if is_running; then
    local pid
    pid="$(cat "${PID_FILE}")"
    echo "Running (pid=${pid})"
  else
    echo "Not running"
  fi

  if [[ -f "${MAPPING_OUT}" ]]; then
    echo "mappings_lines=$(wc -l < "${MAPPING_OUT}" | tr -d ' ')"
  fi
  if [[ -f "${FAILED_OUT}" ]]; then
    echo "failed_lines=$(wc -l < "${FAILED_OUT}" | tr -d ' ')"
  fi
  echo "profile_report_dir=${REPORT_DIR}"
}

show_logs() {
  load_runtime_paths
  mkdir -p "${REPORT_DIR}"
  touch "${RUN_LOG}"
  tail -n 80 -f "${RUN_LOG}"
}

help_text() {
  cat <<'EOF'
Usage:
  bash vps_sync.sh prepare
  bash vps_sync.sh start [concurrency]
  bash vps_sync.sh status
  bash vps_sync.sh logs
  bash vps_sync.sh stop

Notes:
  - Run `prepare` once after cloning to VPS.
  - Keep credentials in `.env` at project root.
  - Set `DRIVE_PROFILE` (e.g. nuoiemmedia, sucmanh2000) to isolate runs.
  - `start` runs in background with nohup and writes `reports/drives/<profile>/run.log`.
EOF
}

case "${cmd}" in
  prepare) prepare ;;
  start) start_sync ;;
  stop) stop_sync ;;
  status) status_sync ;;
  logs) show_logs ;;
  help|-h|--help) help_text ;;
  *)
    echo "Unknown command: ${cmd}"
    help_text
    exit 1
    ;;
esac
