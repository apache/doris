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
. "${ROOT}/juicefs-helpers.sh"

fail() {
    echo "FAIL: $*" >&2
    exit 1
}

assert_eq() {
    local expected="$1"
    local actual="$2"
    [[ "${actual}" == "${expected}" ]] || fail "expected '${expected}', got '${actual}'"
}

assert_lines() {
    local expected="$1"
    shift
    local actual
    actual="$("$@")"
    [[ "${actual}" == "${expected}" ]] || fail "expected lines:\n${expected}\nactual:\n${actual}"
}

assert_lines $'https://doris-thirdparty-repo.bj.bcebos.com/thirdparty/juicefs-hadoop-1.3.1.jar\nhttps://repo1.maven.org/maven2/io/juicefs/juicefs-hadoop/1.3.1/juicefs-hadoop-1.3.1.jar' \
    juicefs_hadoop_jar_download_urls 1.3.1

(
    unset JUICEFS_THIRDPARTY_REPOSITORY_URL
    unset REPOSITORY_URL
    unset JUICEFS_DEFAULT_THIRDPARTY_REPOSITORY_URL
    s3BucketName="doris-regression-bj"
    s3Endpoint="oss-cn-beijing.aliyuncs.com"
    . "${ROOT}/juicefs-helpers.sh"
    assert_eq "https://doris-regression-bj.oss-cn-beijing.aliyuncs.com/regression/datalake/thirdparty/juicefs" \
        "$(juicefs_thirdparty_repository_url)"
)

REPOSITORY_URL="https://mirror.example.com/thirdparty/" \
assert_lines $'https://mirror.example.com/thirdparty/juicefs-hadoop-1.3.1.jar\nhttps://repo1.maven.org/maven2/io/juicefs/juicefs-hadoop/1.3.1/juicefs-hadoop-1.3.1.jar' \
    juicefs_hadoop_jar_download_urls 1.3.1

assert_eq "https://doris-thirdparty-repo.bj.bcebos.com/thirdparty/juicefs-1.3.1-linux-amd64.tar.gz" \
    "$(juicefs_cli_archive_mirror_url 1.3.1)"

REPOSITORY_URL="https://mirror.example.com/thirdparty/" \
assert_lines $'https://mirror.example.com/thirdparty/juicefs-1.3.1-linux-amd64.tar.gz\nhttps://github.com/juicedata/juicefs/releases/download/v1.3.1/juicefs-1.3.1-linux-amd64.tar.gz' \
    juicefs_cli_archive_download_urls 1.3.1

echo "PASS"
