#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." &>/dev/null && pwd)"

BE_OUTPUT_SCRIPT="${ROOT_DIR}/output/be/bin/stop_be.sh"
FE_OUTPUT_SCRIPT="${ROOT_DIR}/output/fe/bin/stop_fe.sh"
BE_SCRIPT="${BE_OUTPUT_SCRIPT}"
FE_SCRIPT="${FE_OUTPUT_SCRIPT}"

[[ -f "${BE_SCRIPT}" ]] || {
    echo "Cannot find BE stop script: ${BE_SCRIPT}"
    exit 1
}

[[ -f "${FE_SCRIPT}" ]] || {
    echo "Cannot find FE stop script: ${FE_SCRIPT}"
    exit 1
}

echo "Using FE script: ${FE_SCRIPT}"
echo "Using BE script: ${BE_SCRIPT}"

STOP_ARGS=("$@")
if [[ ${#STOP_ARGS[@]} -eq 0 ]]; then
    STOP_ARGS=(--grace)
fi

echo "Stopping FE..."
FE_PIDFILE="$(dirname "${FE_SCRIPT}")/fe.pid"
if [[ -f "${FE_PIDFILE}" ]]; then
    FE_PID="$(<"${FE_PIDFILE}")"
    if [[ -n "${FE_PID}" ]] && kill -0 "${FE_PID}" >/dev/null 2>&1; then
        "${FE_SCRIPT}" "${STOP_ARGS[@]}"
    else
        echo "Skip stopping FE: stale pid file ${FE_PIDFILE} (PID ${FE_PID:-<empty>} not running)."
        rm -f "${FE_PIDFILE}"
    fi
else
    echo "Skip stopping FE: ${FE_PIDFILE} does not exist (FE may not be running)."
fi

echo "Stopping BE..."
"${BE_SCRIPT}" "${STOP_ARGS[@]}"

echo "Stopped FE and BE."
