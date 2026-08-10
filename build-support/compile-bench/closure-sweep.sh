#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# Safety net for header-closure surgery (Phase 3 of the BE build-speed work).
#
# Runs syntax_sweep.py in natural-closure mode (-fsyntax-only, PCH stripped)
# over every first-party TU and archives the failure list, so that header
# edits can be gated on "no NEW failures versus a recorded baseline".
# Baseline failures are pre-existing "compiles only via PCH symbol leak"
# debt; they matter the moment pch.h stops including the leaking header.
#
# Usage:
#   closure-sweep.sh baseline <tag> [syntax_sweep.py args...]
#       Full sweep; archive results under be/compile-bench-results/sweeps/<tag>/.
#       Exit 0 even if TUs fail (failures are the recorded debt).
#
#   closure-sweep.sh diff <base-tag> <tag> [syntax_sweep.py args...]
#       Full sweep archived as <tag>, then compared against <base-tag>:
#       prints regressions (new failures) and improvements (fixed ones).
#       Exit 1 iff there is at least one regression.
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
SWEEPS="$ROOT/be/compile-bench-results/sweeps"

usage() {
    grep '^#   closure-sweep.sh' "$0" >&2
    exit 2
}

[ $# -ge 2 ] || usage
mode=$1
case "$mode" in
baseline) base=""; tag=$2; shift 2 ;;
diff) [ $# -ge 3 ] || usage; base=$2; tag=$3; shift 3 ;;
*) usage ;;
esac

out="$SWEEPS/$tag"
mkdir -p "$out"

python3 "$ROOT/build-support/compile-bench/syntax_sweep.py" \
    --no-pch \
    --fail-list "$out/fails.list" \
    --fail-log "$out/fail-errors.log" \
    "$@" 2>&1 | tee "$out/sweep.log"
sweep_rc=${PIPESTATUS[0]}
touch "$out/fails.list" # sweep writes it only on failures

echo
echo "== sweep '$tag': $(wc -l <"$out/fails.list" | tr -d ' ') failing TU(s), results in ${out#"$ROOT"/} =="

if [ "$mode" = baseline ]; then
    exit 0
fi

basef="$SWEEPS/$base/fails.list"
if [ ! -f "$basef" ]; then
    echo "ERROR: baseline '$base' not found at $basef" >&2
    exit 2
fi
regressions=$(comm -13 <(sort "$basef") <(sort "$out/fails.list"))
fixed=$(comm -23 <(sort "$basef") <(sort "$out/fails.list"))
if [ -n "$fixed" ]; then
    echo "-- fixed vs '$base':"
    echo "$fixed" | sed 's/^/     /'
fi
if [ -n "$regressions" ]; then
    echo "-- REGRESSIONS vs '$base':"
    echo "$regressions" | sed 's/^/     /' | tee "$out/regressions.list"
    exit 1
fi
echo "-- no regressions vs '$base'"
exit 0
