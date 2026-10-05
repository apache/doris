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

fail() {
    echo "FAIL: $*" >&2
    exit 1
}

assert_eq() {
    local expected="$1"
    local actual="$2"
    [[ "${actual}" == "${expected}" ]] || fail "expected '${expected}', got '${actual}'"
}

(
    unset DORIS_THIRDPARTY_REPOSITORY_URL
    . "${ROOT}/vars.sh"
    assert_eq "https://doris-regression-hk.oss-cn-hongkong.aliyuncs.com/regression/datalake/thirdparty/juicefs" \
        "${DORIS_THIRDPARTY_REPOSITORY_URL}"
    assert_eq "https://doris-regression-hk.oss-cn-hongkong.aliyuncs.com/regression/datalake/thirdparty/juicefs/juicefs-hadoop-1.3.1.jar" \
        "${JUICEFS_DOWNLOAD}"
)

echo "PASS"
