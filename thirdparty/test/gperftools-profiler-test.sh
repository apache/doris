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

# Run after building gperftools on Linux. Exercise the real SIGPROF callback,
# without tcmalloc, as in the default jemalloc BE. libgcc unwinding can enter
# dl_iterate_phdr under the profiler's signal lock and deadlock with the loader.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." &>/dev/null && pwd)"
TP_INSTALL_DIR="${TP_INSTALL_DIR:-${ROOT}/installed}"

if [[ "$(uname -s)" != Linux ]]; then
    echo 'SKIP: profiler unwinder regression requires Linux'
    exit 0
fi
case "$(uname -m)" in
    aarch64|x86_64) ;;
    *) echo 'SKIP: profiler unwinder regression requires ARM64 or x86_64'; exit 0 ;;
esac

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT
"${CXX:-g++}" -std=c++17 -O2 -g -fno-omit-frame-pointer \
    -I"${TP_INSTALL_DIR}/gperftools/include" \
    "${ROOT}/test/gperftools-profiler-test.cpp" \
    "${TP_INSTALL_DIR}/gperftools/lib/libprofiler.a" \
    -Wl,--wrap=_Unwind_Backtrace -rdynamic -pthread -ldl -o "${work}/profiler-test"

# Do not let the developer's environment override the default being tested.
unset TCMALLOC_STACKTRACE_METHOD CPUPROFILE CPUPROFILE_REALTIME CPUPROFILE_FREQUENCY
TCMALLOC_STACKTRACE_METHOD_VERBOSE=1 timeout 20s "${work}/profiler-test" "${work}/cpu.prof"
