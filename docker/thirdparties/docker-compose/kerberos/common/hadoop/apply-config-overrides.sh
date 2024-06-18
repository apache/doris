#!/usr/bin/env bash

# test whether OVERRIDES_DIR is set
if [[ -n "${OVERRIDES_DIR+x}" ]]; then
    echo "The OVERRIDES_DIR (${OVERRIDES_DIR}) support is disabled as it was deemed unused." >&2
    echo "It is being removed." >&2
    exit 16
fi

if test -e /overrides; then
    find /overrides >&2
    echo "The /overrides handling is disabled as it was deemed unused." >&2
    echo "It is being removed." >&2
    exit 17
fi
