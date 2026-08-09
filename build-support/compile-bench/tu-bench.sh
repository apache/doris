#!/usr/bin/env bash
# Single-TU compile timing probe (no-PCH baseline).
#
# Looks up the TU's real compile command in compile_commands.json, strips the
# binary PCH (keeps the textual -include of cmake_pch.hxx so the source still
# sees the same forced headers), redirects -o to an output dir, then reports:
#   wall seconds, max RSS, weak-def count, total .text bytes
# One line per run is appended to $OUT_DIR/results.tsv; the .o, /usr/bin/time
# log and (by default) an -ftime-trace JSON stay in $OUT_DIR for analysis.
#
# Usage:
#   build-support/compile-bench/tu-bench.sh <label> <source-path> [build-dir]
#
#   <source-path>  absolute, or relative to the repo root. Unity TUs work too:
#                  pass the generated .cxx path under the build dir.
#   [build-dir]    defaults to be/build_Release_compile_bench
#
# Env:
#   TU_BENCH_OUT=<dir>    output dir (default: <build-dir>/tu-bench)
#   TU_BENCH_TRACE=OFF    disable -ftime-trace (default ON, matching the
#                         RESEARCH.md probe baseline numbers)
#
# Timing hygiene: run on an idle machine, one TU at a time. Concurrent builds
# invalidate the numbers (see plan-doc/compile-opt/HANDOFF.md).

set -euo pipefail

if [[ $# -lt 2 ]]; then
    grep '^#' "$0" | sed 's/^# \{0,1\}//' | head -25
    exit 1
fi

LABEL="$1"
SRC="$2"
DORIS_HOME="$(cd "$(dirname "$0")/../.." && pwd)"
BUILD_DIR="${3:-${DORIS_HOME}/be/build_Release_compile_bench}"
OUT_DIR="${TU_BENCH_OUT:-${BUILD_DIR}/tu-bench}"
TRACE="${TU_BENCH_TRACE:-ON}"

[[ "${SRC}" = /* ]] || SRC="${DORIS_HOME}/${SRC}"
CCJSON="${BUILD_DIR}/compile_commands.json"
[[ -f "${CCJSON}" ]] || { echo "ERROR: ${CCJSON} not found (configure first)" >&2; exit 1; }
mkdir -p "${OUT_DIR}"

# Extract the compile command, strip binary PCH, redirect -o. Token-level
# rewrite in python (shell-splitting the escaped command string is too fragile).
OBJ="${OUT_DIR}/${LABEL}.o"
CMDFILE="${OUT_DIR}/${LABEL}.cmd"
python3 - "${CCJSON}" "${SRC}" "${OBJ}" > "${CMDFILE}" <<'PYEOF'
import json, shlex, sys
ccjson, src, obj = sys.argv[1], sys.argv[2], sys.argv[3]
entries = [e for e in json.load(open(ccjson)) if e["file"] == src]
if not entries:
    sys.exit(f"ERROR: no compile_commands.json entry for {src}")
e = entries[0]
toks = shlex.split(e["command"])
out = []
i = 0
while i < len(toks):
    # binary PCH load: -Xclang -include-pch -Xclang <path>
    if (toks[i] == "-Xclang" and i + 3 < len(toks)
            and toks[i + 1] == "-include-pch" and toks[i + 2] == "-Xclang"):
        i += 4
        continue
    if toks[i] == "-o" and i + 1 < len(toks):
        out += ["-o", obj]
        i += 2
        continue
    out.append(toks[i])
    i += 1
print(e["directory"])
print(" ".join(shlex.quote(t) for t in out))
PYEOF

WORK_DIR="$(head -1 "${CMDFILE}")"
CMD="$(tail -1 "${CMDFILE}")"

TRACE_ARGS=""
if [[ "${TRACE}" == "ON" ]]; then
    TRACE_ARGS="-ftime-trace=${OUT_DIR}/${LABEL}.json -ftime-trace-granularity=100"
fi

TIME_LOG="${OUT_DIR}/${LABEL}.time"
(cd "${WORK_DIR}" && /usr/bin/time -v bash -c "${CMD} ${TRACE_ARGS}" 2> "${TIME_LOG}")

WALL_RAW="$(grep 'Elapsed (wall clock)' "${TIME_LOG}" | awk '{print $NF}')"
# h:mm:ss or m:ss.ss -> seconds
WALL_S="$(echo "${WALL_RAW}" | awk -F: '{ s=0; for (i=1; i<=NF; i++) s = s*60 + $i; printf "%.2f", s }')"
MAX_RSS_KB="$(grep 'Maximum resident' "${TIME_LOG}" | awk '{print $NF}')"
# Use the toolchain's llvm-nm (sits next to the clang++ in the command).
NM="$(dirname "$(echo "${CMD}" | awk '{print $1}')")/llvm-nm"
[[ -x "${NM}" ]] || NM="nm"
WEAK="$(${NM} "${OBJ}" | grep -cE ' [VvWw] ' || true)"
TEXT="$(size -A "${OBJ}" | awk '$1 ~ /^\.text/{s+=$2} END{print s+0}')"

RESULTS="${OUT_DIR}/results.tsv"
[[ -f "${RESULTS}" ]] || printf 'timestamp\tlabel\twall_s\tmax_rss_kb\tweak_defs\ttext_bytes\tsrc\n' > "${RESULTS}"
printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$(date +%Y-%m-%dT%H:%M:%S)" "${LABEL}" "${WALL_S}" "${MAX_RSS_KB}" "${WEAK}" "${TEXT}" "${SRC}" \
    | tee -a "${RESULTS}"
