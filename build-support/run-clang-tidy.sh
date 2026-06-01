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

##############################################################
# This script runs clang-tidy on newly added or modified C++
# code, reporting only warnings on changed lines to avoid
# noise from the existing codebase.
#
# Usage:
#   build-support/run-clang-tidy.sh [options]
#
# Options:
#   --base <ref>       Git ref to diff against (default: auto-detect)
#   --files <f1> <f2>  Check specific files (all lines, no filtering)
#   --build-dir <dir>  Path to build dir with compile_commands.json
#   --fix              Apply clang-tidy auto-fixes (note: fixes may
#                      apply to entire diagnostics, not just changed lines)
#   --full             Report all warnings in changed files, not just
#                      warnings on changed lines
#   -h, --help         Show this help
##############################################################

set -eo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
DORIS_HOME=$(cd "${ROOT}/.." && pwd)
export DORIS_HOME

# Directories excluded from clang-tidy (third-party / generated code)
EXCLUDED_PATTERNS=(
    "be/src/apache-orc/"
    "be/src/clucene/"
    "be/src/gutil/"
    "be/src/glibc-compatibility/"
    "be/src/util/mustache/"
    "be/src/util/sse2neo.h"
    "be/src/util/sse2neon.h"
    "be/src/util/utf8_check.cpp"
    "cloud/src/common/defer.h"
    "contrib/"
)

# C++ file extensions to check
CPP_EXTENSIONS="c|h|cc|cpp|cxx|hpp|hxx|hh"

usage() {
    local exit_code="${1:-0}"
    echo "Usage: $(basename "$0") [options]"
    echo ""
    echo "Run clang-tidy on newly added/modified C++ code."
    echo "By default, only reports warnings on changed lines."
    echo ""
    echo "Options:"
    echo "  --base <ref>       Git ref to diff against (default: auto-detect)"
    echo "  --files <f1> ...   Check specific files (all lines, no line filter)"
    echo "  --build-dir <dir>  Path to build dir with compile_commands.json"
    echo "  --fix              Apply clang-tidy auto-fixes (note: fixes may apply"
    echo "                     to entire diagnostics, not just changed lines)"
    echo "  --full             Report all warnings in changed files"
    echo "  -h, --help         Show this help"
    exit "${exit_code}"
}

# Parse arguments
BASE_REF=""
MERGE_BASE=""
SPECIFIC_FILES=()
BUILD_DIR=""
FIX_MODE=""
FULL_FILE_MODE=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --base)
            BASE_REF="$2"
            shift 2
            ;;
        --files)
            shift
            while [[ $# -gt 0 && ! "$1" =~ ^-- ]]; do
                SPECIFIC_FILES+=("$1")
                shift
            done
            ;;
        --build-dir)
            BUILD_DIR="$2"
            shift 2
            ;;
        --fix)
            FIX_MODE="-fix"
            shift
            ;;
        --full)
            FULL_FILE_MODE=true
            shift
            ;;
        -h|--help)
            usage
            ;;
        *)
            echo "Unknown option: $1"
            usage 1
            ;;
    esac
done

# Find clang-tidy
CLANG_TIDY="${CLANG_TIDY_BINARY:-$(command -v clang-tidy 2>/dev/null || true)}"
if [[ -z "${CLANG_TIDY}" ]]; then
    echo "Error: clang-tidy not found. Please install clang-tidy or set CLANG_TIDY_BINARY."
    exit 1
fi

echo "Using clang-tidy: ${CLANG_TIDY}"
echo "  Version: $("${CLANG_TIDY}" --version 2>&1 | head -1)"

# Find compile_commands.json
if [[ -z "${BUILD_DIR}" ]]; then
    for candidate in \
        "${DORIS_HOME}/be/build_ASAN" \
        "${DORIS_HOME}/be/build_Release" \
        "${DORIS_HOME}/be/ut_build_ASAN" \
        "${DORIS_HOME}/be/ut_build_Release" \
        "${DORIS_HOME}/cloud/build_ASAN" \
        "${DORIS_HOME}/cloud/build_Release" \
        "${DORIS_HOME}/cloud/ut_build_ASAN" \
        "${DORIS_HOME}/cloud/ut_build_Release"; do
        if [[ -f "${candidate}/compile_commands.json" ]]; then
            BUILD_DIR="${candidate}"
            break
        fi
    done
fi

if [[ -z "${BUILD_DIR}" || ! -f "${BUILD_DIR}/compile_commands.json" ]]; then
    echo "Error: compile_commands.json not found."
    echo "Please build BE first: ./build.sh --be -j\${DORIS_PARALLELISM}"
    echo "Or specify --build-dir <path>"
    exit 1
fi

echo "Using compile_commands.json from: ${BUILD_DIR}"

# Determine files to check
cd "${DORIS_HOME}"

FILES_TO_CHECK=()

if [[ ${#SPECIFIC_FILES[@]} -gt 0 ]]; then
    FILES_TO_CHECK=("${SPECIFIC_FILES[@]}")
else
    if [[ -n "${BASE_REF}" ]]; then
        # Validate the base ref before proceeding
        if ! git rev-parse --verify "${BASE_REF}" &>/dev/null; then
            echo "Error: invalid git ref '${BASE_REF}'. Check for typos or fetch the remote first."
            exit 1
        fi
        # Use merge-base so we only see files changed on THIS branch,
        # not files that changed on the base branch after we forked.
        MERGE_BASE=$(git merge-base "${BASE_REF}" HEAD)
        mapfile -t FILES_TO_CHECK < <(git diff --name-only --diff-filter=ACMR "${MERGE_BASE}" -- 'be/src/' 'be/test/' 'cloud/src/' 'cloud/test/')
    else
        mapfile -t STAGED < <(git diff --cached --name-only --diff-filter=ACMR -- 'be/src/' 'be/test/' 'cloud/src/' 'cloud/test/' 2>/dev/null || true)
        mapfile -t UNSTAGED < <(git diff --name-only --diff-filter=ACMR -- 'be/src/' 'be/test/' 'cloud/src/' 'cloud/test/' 2>/dev/null || true)
        mapfile -t FILES_TO_CHECK < <(printf '%s\n' "${STAGED[@]}" "${UNSTAGED[@]}" | sort -u | grep -v '^$')
    fi
fi

# Filter to C++ files only, excluding third-party code
FILTERED_FILES=()
for f in "${FILES_TO_CHECK[@]}"; do
    if [[ -z "$f" ]]; then
        continue
    fi
    if ! echo "$f" | grep -qE "\.(${CPP_EXTENSIONS})$"; then
        continue
    fi
    excluded=false
    for pattern in "${EXCLUDED_PATTERNS[@]}"; do
        if [[ "$f" == *"${pattern}"* ]]; then
            excluded=true
            break
        fi
    done
    if [[ "${excluded}" == "false" && -f "$f" ]]; then
        FILTERED_FILES+=("$f")
    fi
done

if [[ ${#FILTERED_FILES[@]} -eq 0 ]]; then
    echo "No changed C++ files to check."
    exit 0
fi

echo ""
echo "Files to check (${#FILTERED_FILES[@]}):"
for f in "${FILTERED_FILES[@]}"; do
    echo "  ${f}"
done
echo ""

# ---------------------------------------------------------------
# Build a map of changed line ranges per file from git diff.
# This allows us to filter clang-tidy output to only report
# warnings on lines the developer actually touched.
# ---------------------------------------------------------------
declare -A CHANGED_LINES_MAP

build_changed_lines_map() {
    local diff_output
    if [[ ${#SPECIFIC_FILES[@]} -gt 0 ]]; then
        # --files mode: no line filtering
        return
    fi

    if [[ -n "${BASE_REF}" ]]; then
        diff_output=$(git diff -U0 "${MERGE_BASE}" -- "${FILTERED_FILES[@]}")
    else
        # Combine staged + unstaged diffs
        local staged_diff unstaged_diff
        staged_diff=$(git diff --cached -U0 -- "${FILTERED_FILES[@]}" 2>/dev/null || true)
        unstaged_diff=$(git diff -U0 -- "${FILTERED_FILES[@]}" 2>/dev/null || true)
        diff_output="${staged_diff}"$'\n'"${unstaged_diff}"
    fi

    # Parse unified diff hunks to extract changed line ranges.
    # Hunk headers: @@ -old_start[,old_count] +new_start[,new_count] @@
    # We collect new_start..new_start+new_count-1 as changed lines.
    local current_file=""
    while IFS= read -r line; do
        if [[ "$line" =~ ^diff\ --git\ a/(.+)\ b/(.+)$ ]]; then
            current_file="${BASH_REMATCH[2]}"
        elif [[ "$line" =~ ^@@.*\+([0-9]+)(,([0-9]+))?.* ]]; then
            local start="${BASH_REMATCH[1]}"
            local count="${BASH_REMATCH[3]:-1}"
            if [[ "${count}" -eq 0 ]]; then
                continue
            fi
            local end=$((start + count - 1))
            # Append range to the file's entry (space-separated "start:end" pairs)
            CHANGED_LINES_MAP["${current_file}"]+="${start}:${end} "
        fi
    done <<< "${diff_output}"
}

# Check if a line number falls within any changed range for a file.
# Files not present in the diff (e.g. headers included by changed files) are
# treated as "not changed" so their diagnostics are filtered out.
is_line_changed() {
    local file="$1"
    local line_num="$2"
    local ranges="${CHANGED_LINES_MAP["${file}"]}"

    if [[ -z "${ranges}" ]]; then
        # No range info: file was not in the diff.
        # In --files mode, build_changed_lines_map is not called, so this function
        # is never reached (full-file mode is used instead). For diff-based runs,
        # treat unknown files as not changed to avoid header noise.
        return 1
    fi

    for range in ${ranges}; do
        local start="${range%%:*}"
        local end="${range##*:}"
        if [[ "${line_num}" -ge "${start}" && "${line_num}" -le "${end}" ]]; then
            return 0
        fi
    done
    return 1
}

if [[ "${FULL_FILE_MODE}" == "false" && ${#SPECIFIC_FILES[@]} -eq 0 ]]; then
    echo "Line-level filtering: only warnings on changed lines will be reported."
    echo "Use --full to see all warnings in changed files."
    echo ""
    build_changed_lines_map
fi

# Run clang-tidy
TIDY_ARGS=(
    -p "${BUILD_DIR}"
    --config-file="${DORIS_HOME}/.clang-tidy"
)

if [[ -n "${FIX_MODE}" ]]; then
    TIDY_ARGS+=("${FIX_MODE}")
fi

HAS_WARNINGS=0
TOTAL_WARNINGS=0

for f in "${FILTERED_FILES[@]}"; do
    echo "=== Checking: ${f} ==="
    TIDY_EXIT=0
    OUTPUT=$("${CLANG_TIDY}" "${TIDY_ARGS[@]}" "${f}" 2>&1) || TIDY_EXIT=$?

    # Detect real analysis failures (missing compile command, broken AST, etc.)
    # but NOT promoted warnings. WarningsAsErrors: "*" in .clang-tidy causes
    # normal findings to be emitted as "error: ... [-warnings-as-errors]"; those
    # are diagnostics, not analysis failures.
    if echo "${OUTPUT}" | grep -E "(^|: )(error|fatal error):" | grep -qv "\-warnings-as-errors\]"; then
        echo "${OUTPUT}"
        echo ""
        echo "Error: clang-tidy could not analyze ${f} (see above)."
        HAS_WARNINGS=1
        TOTAL_WARNINGS=$((TOTAL_WARNINGS + 1))
        continue
    fi

    # Match both "warning:" lines and promoted "error: ... [-warnings-as-errors]"
    # lines as diagnostics.
    DIAG_PATTERN="(warning:|error:.*-warnings-as-errors)"
    if echo "${OUTPUT}" | grep -qE "${DIAG_PATTERN}"; then
        if [[ "${FULL_FILE_MODE}" == "true" || ${#SPECIFIC_FILES[@]} -gt 0 ]]; then
            # Full mode or --files: show all warnings
            echo "${OUTPUT}"
            COUNT=$(echo "${OUTPUT}" | grep -cE "${DIAG_PATTERN}" || true)
            TOTAL_WARNINGS=$((TOTAL_WARNINGS + COUNT))
            HAS_WARNINGS=1
        else
            # Line-level filtering: only show warnings on changed lines
            FILE_WARNINGS=""
            FILE_COUNT=0
            while IFS= read -r wline; do
                # Parse "filepath:line:col: warning/error: ..."
                if [[ "$wline" =~ ^([^:]+):([0-9]+):[0-9]+:\ (warning|error): ]]; then
                    local_file="${BASH_REMATCH[1]}"
                    local_line="${BASH_REMATCH[2]}"
                    # Normalize: clang-tidy may emit absolute or relative paths
                    local_file_rel="${local_file#"${DORIS_HOME}/"}"
                    if is_line_changed "${local_file_rel}" "${local_line}"; then
                        FILE_WARNINGS+="${wline}"$'\n'
                        FILE_COUNT=$((FILE_COUNT + 1))
                    fi
                fi
            done <<< "$(echo "${OUTPUT}" | grep -E "${DIAG_PATTERN}")"

            if [[ ${FILE_COUNT} -gt 0 ]]; then
                echo "${FILE_WARNINGS}"
                TOTAL_WARNINGS=$((TOTAL_WARNINGS + FILE_COUNT))
                HAS_WARNINGS=1
            else
                echo "  No warnings on changed lines."
            fi
        fi
    else
        echo "  No warnings."
    fi
    echo ""
done

echo "========================================"
if [[ ${HAS_WARNINGS} -eq 1 ]]; then
    echo "clang-tidy found ${TOTAL_WARNINGS} warning(s) on changed lines."
    echo "Please fix the warnings above, or add // NOLINT(check-name) with justification."
    exit 1
else
    echo "clang-tidy: all checks passed."
    exit 0
fi
