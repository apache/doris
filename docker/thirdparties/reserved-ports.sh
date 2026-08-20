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

thirdparty_component_reserved_ports() {
    case "$1" in
    mysql) echo "3316" ;;
    pg) echo "5442" ;;
    oracle) echo "1521" ;;
    sqlserver) echo "1433" ;;
    clickhouse) echo "8123" ;;
    es) echo "19200,29200,39200,59200" ;;
    hive2) echo "8020,9083,10000,5432,50070,50075" ;;
    hive3) echo "8320,9383,13000,5732" ;;
    kafka) echo "12181,19193" ;;
    iceberg) echo "11000,18181,19001" ;;
    iceberg-rest) echo "19181,19182,19183,19184,19185,19186,20870,20020,20864" ;;
    hudi) echo "19083,19100,19101,18080" ;;
    mariadb) echo "3326" ;;
    db2) echo "50000" ;;
    oceanbase) echo "2881,2883" ;;
    lakesoul) echo "5432" ;;
    kerberos) echo "5588,8520,9583,9566,9564,9567,9570,6688,8620,9683,9666,9664,9667,9670" ;;
    minio) echo "19000" ;;
    ranger) echo "8983,6081,33061" ;;
    polaris) echo "20181,20182,20001,20002" ;;
    *) echo "" ;;
    esac
}

thirdparty_reserved_ports_for_components() {
    local components="$1"
    local component
    local component_ports
    local port
    local reserved_ports="65535"
    local old_ifs="${IFS}"

    IFS=','
    for component in ${components}; do
        component_ports="$(thirdparty_component_reserved_ports "${component}")"
        for port in ${component_ports}; do
            [[ -n "${port}" ]] || continue
            if [[ ",${reserved_ports}," != *",${port},"* ]]; then
                reserved_ports="${reserved_ports},${port}"
            fi
        done
    done
    IFS="${old_ifs}"

    echo "${reserved_ports}"
}
