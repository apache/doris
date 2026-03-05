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

JFS_META_DSN="${JFS_META_DSN:-}"
JFS_VOLUME="${JFS_VOLUME:-cluster}"
JFS_STORAGE="${JFS_STORAGE:-file}"
JFS_BUCKET="${JFS_BUCKET:-/tmp/jfs-bucket}"
JFS_BIN_DIR="${JFS_BIN_DIR:-/tmp/jfs-bin}"
JFS_CLI="${JFS_CLI:-${JFS_BIN_DIR}/juicefs}"

if [[ -z "${JFS_META_DSN}" ]]; then
    echo "ERROR: JFS_META_DSN is empty."
    echo "Example: export JFS_META_DSN='mysql://user:pwd@(127.0.0.1:3316)/juicefs_meta'"
    exit 1
fi

mkdir -p "${JFS_BUCKET}"
chmod 777 "${JFS_BUCKET}" 2>/dev/null || true

if [[ ! -x "${JFS_CLI}" ]]; then
    mkdir -p "${JFS_BIN_DIR}"
    curl -sSL https://d.juicefs.com/install | sh -s -- "${JFS_BIN_DIR}"
fi

"${JFS_CLI}" version

status_output="$(mktemp /tmp/jfs-status.XXXXXX)"
if "${JFS_CLI}" status "${JFS_META_DSN}" >"${status_output}" 2>&1; then
    echo "JuiceFS metadata is already formatted."
    rm -f "${status_output}"
    exit 0
fi

if grep -q "database is not formatted" "${status_output}"; then
    "${JFS_CLI}" format \
        --storage "${JFS_STORAGE}" \
        --bucket "${JFS_BUCKET}" \
        "${JFS_META_DSN}" \
        "${JFS_VOLUME}"
    rm -f "${status_output}"
    exit 0
fi

echo "ERROR: failed to query JuiceFS metadata status."
cat "${status_output}"
rm -f "${status_output}"
exit 1
