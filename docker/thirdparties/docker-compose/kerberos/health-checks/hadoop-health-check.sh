#!/usr/bin/env bash
set -euo pipefail

if test $# -gt 0; then
    echo "$0 does not accept arguments" >&2
    exit 32
fi

# Supervisord is not running
if ! test -f /tmp/supervisor.sock; then
    exit 0
fi

# Check if all Hadoop services are running
FAILED=$(supervisorctl status | grep -v RUNNING || true)

if [ "$FAILED" == "" ]; then
  exit 0
else
  echo "Some of the services are failing: ${FAILED}"
  exit 1
fi
