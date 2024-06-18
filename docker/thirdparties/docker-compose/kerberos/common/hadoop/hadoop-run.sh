#!/usr/bin/env bash
set -euo pipefail

if test $# -gt 0; then
    echo "$0 does not accept arguments" >&2
    exit 32
fi

set -x

HADOOP_INIT_D=${HADOOP_INIT_D:-/etc/hadoop-init.d/}

echo "Applying hadoop init.d scripts from ${HADOOP_INIT_D}"
if test -d "${HADOOP_INIT_D}"; then
    for init_script in "${HADOOP_INIT_D}"*; do
        "${init_script}"
    done
fi

trap exit INT

echo "Running services with supervisord"

supervisord -c /etc/supervisord.conf
