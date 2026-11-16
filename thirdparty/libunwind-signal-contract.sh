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

set -euo pipefail

if [[ $# -ne 2 || ( "$1" != publish && "$1" != verify ) ]]; then
    echo "usage: $0 {publish|verify} INSTALLED_DIR" >&2
    exit 2
fi

action="$1"
installed_dir="$2"
archive="${installed_dir}/lib64/libunwind.a"
marker="${installed_dir}/lib64/libunwind-signal-contract.txt"
patch="$(dirname "${BASH_SOURCE[0]}")/patches/libunwind-1.8.3-doris-phdr-cache.patch"

if [[ ! -s "${archive}" ]]; then
    echo "libunwind signal contract: installed archive is missing or empty: ${archive}" >&2
    exit 1
fi

# This version certifies the producer's --disable-debug-frame configuration and the current
# PHDR/mini-debug patch. Bind it to the installed bytes, not to a disposable source directory.
contract="doris-libunwind-1.8.3-signal-v1-no-debug-frame"
patch_hash="$(sha256sum "${patch}" | cut -d ' ' -f1)"
archive_hash="$(sha256sum "${archive}" | cut -d ' ' -f1)"

if [[ "${action}" == publish ]]; then
    staged="$(mktemp "${marker}.tmp.XXXXXX")"
    trap 'rm -f "${staged}"' EXIT
    printf '%s\n%s\n%s\n' "${contract}" "${patch_hash}" "${archive_hash}" >"${staged}"
    mv -f "${staged}" "${marker}"
elif [[ ! -f "${marker}" ]] ||
    ! cmp -s "${marker}" <(printf '%s\n%s\n%s\n' "${contract}" "${patch_hash}" "${archive_hash}"); then
    echo 'libunwind signal contract: installed archive is stale or uncertified; rebuild libunwind with --disable-debug-frame and the current Doris patch' >&2
    exit 1
fi
