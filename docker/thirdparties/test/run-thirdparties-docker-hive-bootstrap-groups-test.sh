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

set -eo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." &>/dev/null && pwd)"
. "${ROOT}/docker-compose/hive/scripts/bootstrap/bootstrap-groups.sh"

fail() {
    echo "FAIL: $*" >&2
    exit 1
}

assert_eq() {
    local expected="$1"
    shift
    local actual
    actual="$("$@")"
    [[ "${actual}" == "${expected}" ]] || fail "expected '${expected}', got '${actual}' from: $*"
}

assert_selected() {
    local groups="$1"
    local kind="$2"
    local relative_path="$3"

    bootstrap_item_selected "${groups}" "${kind}" "${relative_path}" || \
        fail "expected selected: groups=${groups}, kind=${kind}, path=${relative_path}"
}

assert_not_selected() {
    local groups="$1"
    local kind="$2"
    local relative_path="$3"

    if bootstrap_item_selected "${groups}" "${kind}" "${relative_path}"; then
        fail "expected not selected: groups=${groups}, kind=${kind}, path=${relative_path}"
    fi
}

assert_eq "common" bootstrap_item_group run_sh "data/multi_catalog/test_complex_types/run.sh"
assert_selected "common,hive2_only" run_sh "data/multi_catalog/test_complex_types/run.sh"
assert_selected "common,hive3_only" run_sh "data/multi_catalog/test_complex_types/run.sh"

assert_eq "hive3_only" bootstrap_item_group run_sh "data/multi_catalog/test_wide_table/run.sh"
assert_not_selected "common,hive2_only" run_sh "data/multi_catalog/test_wide_table/run.sh"
assert_selected "common,hive3_only" run_sh "data/multi_catalog/test_wide_table/run.sh"

echo "PASS"
