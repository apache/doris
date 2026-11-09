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

set -euo pipefail

fixture_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
mkdir -p "${fixture_dir}/jars"
repository="${MAVEN_REPOSITORY_URL:-https://repo.maven.apache.org/maven2}"

sha256_file() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | awk '{print $1}'
    else
        shasum -a 256 "$1" | awk '{print $1}'
    fi
}

# These isolated reference jars must not replace the existing Spark fixture's SDK.
while read -r module expected_sha256; do
    filename="${module}-1.12.0.jar"
    destination="${fixture_dir}/jars/${filename}"
    if [[ -f "${destination}" ]] && [[ "$(sha256_file "${destination}")" == "${expected_sha256}" ]]; then
        # Spark needs other-user read access; readable shared caches may belong to another user.
        if [[ -z "$(find -L "${destination}" -prune -perm -004 -print)" ]]; then
            chmod 0644 "${destination}"
        fi
        continue
    fi
    temporary="$(mktemp "${destination}.tmp.XXXXXX")"
    trap 'rm -f "${temporary}"' EXIT
    curl -fSL --retry 3 --connect-timeout 15 --max-time 300 \
        "${repository%/}/org/apache/iceberg/${module}/1.12.0/${filename}" -o "${temporary}"
    if [[ "$(sha256_file "${temporary}")" != "${expected_sha256}" ]]; then
        echo "Checksum mismatch for Iceberg encryption fixture ${filename}" >&2
        exit 1
    fi
    # Spark's non-root container user must read these host-created public jars.
    chmod 0644 "${temporary}"
    mv -f "${temporary}" "${destination}"
    trap - EXIT
done <<'ARTIFACTS'
iceberg-spark-runtime-4.0_2.13 3f50e93e8ed322ae0da0e380f1b6c27116404329c62e55f0586697b1b8378306
iceberg-aws-bundle dd7e49ffa4ac9b215bed846581991259e3aec9bb2fa08d6cec029c46cd66d957
ARTIFACTS
