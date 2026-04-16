#!/bin/bash
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

set -eo pipefail

# Snapshot all Hive Docker named volumes into a single tarball.
#
# Usage:
#   snapshot-hive-baseline.sh <volume_prefix> <output_path>
#
# Example:
#   bash snapshot-hive-baseline.sh doris--syt--hive3 /tmp/hive3-baseline.tar.gz
#
# Prerequisites:
#   - All Hive containers must be STOPPED before running this script.
#   - The 4 named volumes (<prefix>-{namenode,datanode,pgdata,state}) must exist.

VOLUME_PREFIX="${1:?Usage: $0 <volume_prefix> <output_path>}"
OUTPUT_PATH="${2:?Usage: $0 <volume_prefix> <output_path>}"

echo "[snapshot] volume prefix: ${VOLUME_PREFIX}"
echo "[snapshot] output: ${OUTPUT_PATH}"

# Verify all 4 volumes exist
for suffix in namenode datanode pgdata state; do
    vol="${VOLUME_PREFIX}-${suffix}"
    if ! sudo docker volume inspect "${vol}" >/dev/null 2>&1; then
        echo "ERROR: volume ${vol} does not exist" >&2
        exit 1
    fi
done

_t0=$(date +%s)

# Mount all 4 volumes read-only into a single alpine container and tar them
# in one pass. Copy only the small state volume to ephemeral container storage
# so legacy baseline.version files can be dropped from newly exported tarballs.
sudo docker run --rm \
    -v "${VOLUME_PREFIX}-namenode:/snapshot/namenode:ro" \
    -v "${VOLUME_PREFIX}-datanode:/snapshot/datanode:ro" \
    -v "${VOLUME_PREFIX}-pgdata:/snapshot/pgdata:ro" \
    -v "${VOLUME_PREFIX}-state:/snapshot/state:ro" \
    alpine sh -c '
        mkdir -p /work/state
        cp -a /snapshot/state/. /work/state/
        rm -f /work/state/baseline.version
        tar czf - -C /snapshot namenode datanode pgdata -C /work state
    ' \
    > "${OUTPUT_PATH}"

size=$(du -h "${OUTPUT_PATH}" | cut -f1)
echo "[snapshot] done took=$(( $(date +%s) - _t0 ))s size=${size}"
echo "[snapshot] saved to ${OUTPUT_PATH}"
