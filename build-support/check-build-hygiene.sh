#!/bin/bash
#
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
# Unified entry for the BE build-hygiene gates. Runs every seconds-level,
# zero-build-dependency check that protects the backend's compile-time
# invariants:
#
#   check-header-deps.py             header layering rules, third-party bans,
#                                    the two-axis closure/reach budgets and the
#                                    pch include lock
#   check-extern-template-pairing.py extern template declarations and explicit
#                                    instantiation definitions stay paired in
#                                    both directions
#   check-unity-skip-coverage.py     every src .cpp a test #includes is opted
#                                    out of unity batching
#
# All checks are pure text scans and run in about a second combined. Every
# failure message carries the mechanism (why the edge/entry is expensive) and
# the fix path. All checks run even when an early one fails, so one pass shows
# everything there is to fix.
#
# Mounted at configure time by be/CMakeLists.txt (ENABLE_BUILD_HYGIENE, default
# ON), which is what makes a violation a build error rather than advice. To run
# by hand:
#
#   build-support/check-build-hygiene.sh
#
# Exit code: 0 -- all checks passed; 1 -- at least one violation (details on
# stderr); 2 -- environment problem (no Python 3 interpreter).
#
# Self-tests: build-support/tests/test-build-hygiene-*.sh (run.sh runs them).

set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# The gates are python3 sources, so they need a Python 3 interpreter -- which is
# not the interpreter the build itself uses. env.sh exports PYTHON as
# DORIS_BUILD_PYTHON_VERSION, defaulting to `python`; on the official build-env
# image (FROM almalinux:8) that is python2.7, under which every gate dies with a
# SyntaxError on its first f-string. So resolve an interpreter here and never
# read PYTHON.
#
# The probe order covers the two shapes that exist in practice: `python3` on
# developer boxes and most distros, and /usr/libexec/platform-python -- the
# RHEL8/AlmaLinux8 system interpreter (3.6), which the build-env image ships
# while carrying no /usr/bin/python3 at all.
#
# BUILD_HYGIENE_PYTHON names an interpreter explicitly (a venv, a non-standard
# prefix). It selects which python runs the gates; it cannot switch them off --
# that is -DENABLE_BUILD_HYGIENE=OFF.
is_python3() {
    "$1" -c 'import sys; sys.exit(0 if sys.version_info >= (3, 6) else 1)' \
        >/dev/null 2>&1
}

PYTHON=""
if [ -n "${BUILD_HYGIENE_PYTHON:-}" ]; then
    # An explicit choice is honoured strictly: if it is unusable, say so rather
    # than quietly running the gates under some other interpreter.
    if is_python3 "${BUILD_HYGIENE_PYTHON}"; then
        PYTHON="${BUILD_HYGIENE_PYTHON}"
    else
        echo "check-build-hygiene: BUILD_HYGIENE_PYTHON=${BUILD_HYGIENE_PYTHON} is not found, or is not Python >= 3.6." >&2
        echo "Point it at a Python 3 interpreter, or re-run with -DENABLE_BUILD_HYGIENE=OFF to skip the gates." >&2
        exit 2
    fi
else
    for candidate in python3 python /usr/libexec/platform-python; do
        if is_python3 "${candidate}"; then
            PYTHON="${candidate}"
            break
        fi
    done
    if [ -z "${PYTHON}" ]; then
        echo "check-build-hygiene: no Python >= 3.6 found (tried python3, python, /usr/libexec/platform-python); the build-hygiene gates need one." >&2
        echo "Install python3 or set BUILD_HYGIENE_PYTHON, or re-run with -DENABLE_BUILD_HYGIENE=OFF to skip the gates." >&2
        exit 2
    fi
fi

FAILURES=0
for check in \
    check-header-deps.py \
    check-extern-template-pairing.py \
    check-unity-skip-coverage.py; do
    if ! "${PYTHON}" "${SCRIPT_DIR}/${check}"; then
        FAILURES=$((FAILURES + 1))
    fi
done

if [ "${FAILURES}" -ne 0 ]; then
    echo "" >&2
    echo "build hygiene: ${FAILURES} check(s) failed (details above)." >&2
    echo "Each message ends with its fix path; the budget/whitelist tables live in the" >&2
    echo "check scripts themselves, so a deliberate change is a one-line, reviewed diff." >&2
    exit 1
fi
# Name the resolved interpreter: it is the one thing about this run that varies
# per environment, and the configure log is where you look when a gate behaves
# differently in CI than it did locally.
echo "build hygiene: all checks passed (python: ${PYTHON} $("${PYTHON}" -c 'import sys; print("%d.%d.%d" % sys.version_info[:3])'))"
