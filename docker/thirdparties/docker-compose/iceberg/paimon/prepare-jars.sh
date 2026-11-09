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

# Keep the Paimon runtime separate from the versioned Spark data archive.
while read -r module expected_sha256; do
    filename="${module}-2.1.0.jar"
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
        "${repository%/}/org/apache/paimon/${module}/2.1.0/${filename}" -o "${temporary}"
    if [[ "$(sha256_file "${temporary}")" != "${expected_sha256}" ]]; then
        echo "Checksum mismatch for Paimon Spark fixture ${filename}" >&2
        exit 1
    fi
    # Spark's non-root container user must read these host-created public jars.
    chmod 0644 "${temporary}"
    mv -f "${temporary}" "${destination}"
    trap - EXIT
done <<'ARTIFACTS'
paimon-spark-4.0_2.13 9a244ac682880f3bd9b42066ffedb090dc148e54bc8c41c651bb6039ad393f52
paimon-s3 5fbc5bad63220230c8e30c16c828af10aa03ea67f203b1e23e3e325b92571633
ARTIFACTS
