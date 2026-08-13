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

BENCHMARK_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
FE_DIR=$(cd -- "${BENCHMARK_DIR}/.." && pwd)
CLASSPATH_FILE=$(mktemp)
trap 'rm -f "${CLASSPATH_FILE}"' EXIT

(
    cd "${FE_DIR}"
    mvn -Pbenchmark -pl fe-benchmark -am compile -DskipTests -Dskip.clean=true
    mvn -Pbenchmark -pl fe-benchmark -am dependency:build-classpath \
        -Dskip.clean=true \
        -DincludeScope=test \
        -Dmdep.outputFile="${CLASSPATH_FILE}"
)

REACTOR_CLASSES=$(find "${FE_DIR}" -type d -path '*/target/classes' -printf '%p:')
DEPENDENCY_CLASSES=$(tr -d '\n' < "${CLASSPATH_FILE}")
BENCHMARK_FILTER=${BENCHMARK_FILTER:-'HivePartitionValuesSizeBenchmark|IcebergCacheSizeBenchmark|PaimonCacheSizeBenchmark|MetaCacheSoftValueBenchmark'}

BENCHMARK_CLASSES=(
    org.apache.doris.datasource.hive.HivePartitionValuesSizeBenchmark
    org.apache.doris.datasource.iceberg.IcebergCacheSizeBenchmark
    org.apache.doris.datasource.paimon.PaimonCacheSizeBenchmark
    org.apache.doris.datasource.metacache.MetaCacheSoftValueBenchmark
)

for BENCHMARK_CLASS in "${BENCHMARK_CLASSES[@]}"; do
    if [[ "${BENCHMARK_CLASS##*.}" =~ ${BENCHMARK_FILTER} ]]; then
        java \
            -Xms1g \
            -Xmx4g \
            -classpath "${REACTOR_CLASSES}${DEPENDENCY_CLASSES}" \
            "${BENCHMARK_CLASS}" \
            "$@"
    fi
done
