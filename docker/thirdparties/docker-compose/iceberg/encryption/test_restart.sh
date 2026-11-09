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

if [[ $# != 3 ]]; then
    echo "Usage: $0 COMPOSE_FILE PROJECT_NAME FIXTURE_URL" >&2
    exit 2
fi
compose_file=$1
project_name=$2
fixture_url=${3%/}
compose=(docker compose -f "${compose_file}" -p "${project_name}")

wait_for_fixture() {
    for attempt in {1..60}; do
        if curl --noproxy '*' --fail --silent --max-time 2 "${fixture_url}/health" >/dev/null; then
            return
        fi
        sleep 1
    done
    echo "Encryption fixture did not become ready" >&2
    return 1
}

"${compose[@]}" up -d encryption-fixture
wait_for_fixture
curl --noproxy '*' --fail --show-error --silent --max-time 120 -X POST "${fixture_url}/reset"
# Retain HMS and object storage while Moto forgets its keys, as in a normal stop/up cycle.
"${compose[@]}" stop encryption-fixture encryption-kms
"${compose[@]}" up -d encryption-fixture
wait_for_fixture
# Never retry reset: purge used to drop the HMS entry before failing, hiding the bug on retry.
curl --noproxy '*' --fail --show-error --silent --max-time 120 -X POST "${fixture_url}/reset"
curl --noproxy '*' --fail --show-error --silent --max-time 120 "${fixture_url}/verify"
