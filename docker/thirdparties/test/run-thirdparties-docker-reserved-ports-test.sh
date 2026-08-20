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
. "${ROOT}/reserved-ports.sh"

fail() {
    echo "FAIL: $*" >&2
    exit 1
}

assert_component_ports() {
    local component="$1"
    local expected="$2"
    local actual
    actual="$(thirdparty_component_reserved_ports "${component}")"
    [[ "${actual}" == "${expected}" ]] || \
        fail "${component}: expected '${expected}', got '${actual}'"
}

assert_component_ports mysql "3316"
assert_component_ports pg "5442"
assert_component_ports oracle "1521"
assert_component_ports sqlserver "1433"
assert_component_ports clickhouse "8123"
assert_component_ports es "19200,29200,39200,59200"
assert_component_ports hive2 "8020,9083,10000,5432,50070,50075"
assert_component_ports hive3 "8320,9383,13000,5732"
assert_component_ports kafka "12181,19193"
assert_component_ports iceberg "11000,18181,19001"
assert_component_ports iceberg-rest "19181,19182,19183,19184,19185,19186,20870,20020,20864"
assert_component_ports hudi "19083,19100,19101,18080"
assert_component_ports mariadb "3326"
assert_component_ports db2 "50000"
assert_component_ports oceanbase "2881,2883"
assert_component_ports lakesoul "5432"
assert_component_ports kerberos "5588,8520,9583,9566,9564,9567,9570,6688,8620,9683,9666,9664,9667,9670"
assert_component_ports minio "19000"
assert_component_ports ranger "8983,6081,33061"
assert_component_ports polaris "20181,20182,20001,20002"
assert_component_ports unknown ""

actual="$(thirdparty_reserved_ports_for_components "mysql,db2,hive2,lakesoul")"
[[ "${actual}" == "65535,3316,50000,8020,9083,10000,5432,50070,50075" ]] || \
    fail "merged ports: got '${actual}'"

echo "PASS"
