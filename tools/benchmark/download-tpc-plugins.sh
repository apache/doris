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
BENCHMARK_ROOT=$(cd "$(dirname "$0")" && pwd)
cd "${BENCHMARK_ROOT}"
# Match fe/pom.xml's Trino SPI version. These are the complete upstream plugin
# distributions, including runtime dependencies, not standalone connector JARs.
version=435
mkdir -p target
for connector in tpch tpcds; do
    # Both values in the fixed connector list are covered.
    # shellcheck disable=SC2249
    case "${connector}" in
        tpch) checksum=59d99220b5f47a63606f3bbcd101b7a8e3a160438b6f2fdc361b91badf4695e1 ;;
        tpcds) checksum=38b9027d8201cb37ff14d051840aeb41eb29ad11bc89a862d294ce9049ae85e8 ;;
    esac
    archive="target/trino-${connector}-${version}.zip"
    curl --fail --location --retry 3 \
        "https://repo.maven.apache.org/maven2/io/trino/trino-${connector}/${version}/trino-${connector}-${version}.zip" \
        --output "${archive}"
    echo "${checksum}  ${archive}" | sha256sum --check
    unzip -oq "${archive}" -d target
done
echo 'Copy the entire target/trino-tpch-435 and target/trino-tpcds-435 directories'
echo 'into plugins/trino_plugins/ on every FE and BE, then restart those nodes.'
