#!/usr/bin/env bash
set -euo pipefail

if test $# -gt 0; then
    echo "$0 does not accept arguments" >&2
    exit 32
fi

set -x

HEALTH_D=${HEALTH_D:-/etc/health.d/}

if test -d "${HEALTH_D}"; then
    for health_script in "${HEALTH_D}"/*; do
        "${health_script}" &>> /var/log/container-health.log || exit 1
    done
fi
