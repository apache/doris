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

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
jar_dir="${PAIMON_JAR_DIR:-${script_dir}/cache/paimon}"
mkdir -p "${jar_dir}"
repository="${MAVEN_REPOSITORY_URL:-https://repo.maven.apache.org/maven2}"

sha256_file() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | awk '{print $1}'
    else
        shasum -a 256 "$1" | awk '{print $1}'
    fi
}

# Runtime-generated VECTOR fixtures require Paimon 2.1 plus its index factory.
while read -r module version expected_sha256; do
    filename="${module}-${version}.jar"
    destination="${jar_dir}/${filename}"
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
        "${repository%/}/org/apache/paimon/${module}/${version}/${filename}" -o "${temporary}"
    if [[ "$(sha256_file "${temporary}")" != "${expected_sha256}" ]]; then
        echo "Checksum mismatch for Paimon fixture ${filename}" >&2
        exit 1
    fi
    # Spark's non-root container user must read these host-created public jars.
    chmod 0644 "${temporary}"
    mv -f "${temporary}" "${destination}"
    trap - EXIT
done <<'ARTIFACTS'
paimon-spark-4.0_2.13 2.1.0 9a244ac682880f3bd9b42066ffedb090dc148e54bc8c41c651bb6039ad393f52
paimon-s3 2.1.0 5fbc5bad63220230c8e30c16c828af10aa03ea67f203b1e23e3e325b92571633
paimon-vector 2.1.0 1542654c735f7747af598a331d8abb323d7c52f205c0f38c797ccf13041a93f0
paimon-vector-index-java 0.5.0 bb260bb6c96297c7ad1d599686af56ee3b8a4fe1abe3d6fcda42831c3e4f5ed2
ARTIFACTS
