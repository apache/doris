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
BENCHMARK_PATTERN=${1:-'(HiveFileListingSizeBenchmark|IcebergPartitionSizeBenchmark|IcebergManifestSizeBenchmark|PaimonPartitionViewSizeBenchmark)'}
if [[ $# -gt 0 ]]; then
    shift
fi

CLASSPATH_FILE=$(mktemp)
trap 'rm -f "${CLASSPATH_FILE}"' EXIT

(
    cd "${FE_DIR}"
    mvn -Pbenchmark -pl fe-benchmark -am test-compile -DskipTests
    mvn -Pbenchmark -pl fe-benchmark -am dependency:build-classpath \
        -DincludeScope=test \
        -Dmdep.outputFile="${CLASSPATH_FILE}"
)

REACTOR_CLASSES=$(find "${FE_DIR}" -type d -path '*/target/classes' -printf '%p:')
DEPENDENCY_CLASSES=$(tr -d '\n' < "${CLASSPATH_FILE}")

java \
    -Djol.magicFieldOffset=true \
    --add-opens=java.base/java.lang=ALL-UNNAMED \
    --add-opens=java.base/java.util=ALL-UNNAMED \
    --add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
    -classpath "${REACTOR_CLASSES}${DEPENDENCY_CLASSES}" \
    org.openjdk.jmh.Main "${BENCHMARK_PATTERN}" "$@"
