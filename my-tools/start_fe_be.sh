#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." &>/dev/null && pwd)"

BE_OUTPUT_SCRIPT="${ROOT_DIR}/output/be/bin/start_be.sh"
FE_OUTPUT_SCRIPT="${ROOT_DIR}/output/fe/bin/start_fe.sh"
BE_SCRIPT="${BE_OUTPUT_SCRIPT}"
FE_SCRIPT="${FE_OUTPUT_SCRIPT}"

[[ -f "${BE_SCRIPT}" ]] || {
    echo "Cannot find BE start script: ${BE_SCRIPT}"
    exit 1
}

[[ -f "${FE_SCRIPT}" ]] || {
    echo "Cannot find FE start script: ${FE_SCRIPT}"
    exit 1
}

echo "Using BE script: ${BE_SCRIPT}"
echo "Using FE script: ${FE_SCRIPT}"

start_or_skip() {
    local name="$1"
    local script="$2"
    local pidfile="$3"

    if [[ -f "${pidfile}" ]]; then
        local pid
        pid="$(<"${pidfile}")"
        if [[ -n "${pid}" ]] && kill -0 "${pid}" >/dev/null 2>&1; then
            echo "Skip ${name}: already running as process ${pid}."
            return 0
        fi
        echo "Found stale ${name} pid file: ${pidfile} (PID ${pid:-<empty>}), removing it."
        rm -f "${pidfile}"
    fi

    echo "Starting ${name}..."
    if "${script}" --daemon; then
        echo "Started ${name}."
    else
        echo "Failed to start ${name}, continue with next component."
    fi
}

start_or_skip "BE" "${BE_SCRIPT}" "$(dirname "${BE_SCRIPT}")/be.pid"
start_or_skip "FE" "${FE_SCRIPT}" "$(dirname "${FE_SCRIPT}")/fe.pid"

echo "Start flow finished (running services were skipped)."
