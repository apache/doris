// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

suite("test_overflow") {

    def table1 = "test_overflow"

    sql "drop table if exists ${table1}"

    sql """
    CREATE TABLE IF NOT EXISTS test_overflow (
      `k1` decimalv3(38, 18) NULL COMMENT "",
      `k2` decimalv3(38, 18) NULL COMMENT "",
      `k3` decimalv3(38, 18) NULL COMMENT "",
      `v1` decimalv3(38, 37) NULL COMMENT "",
      `v2` decimalv3(38, 37) NULL COMMENT "",
      `v3` decimalv3(38, 37) NULL COMMENT "",
      `v4` INT NULL COMMENT ""
    ) ENGINE=OLAP
    COMMENT "OLAP"
    DISTRIBUTED BY HASH(`k1`, `k2`, `k3`) BUCKETS 8
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "in_memory" = "false",
    "storage_format" = "V2"
    )
    """

    sql """insert into test_overflow values(11111111111111111111.1,11111111111111111111.2,11111111111111111111.3, 1.1,1.2,1.3,9)
    """
    qt_select_all "select * from test_overflow order by k1"

    sql " SET check_overflow_for_decimal = true; "
    qt_select_check_overflow1 "select k1 * k2, k1 * k3, k1 * k2 * k3, k1 * v4, k1*50 from test_overflow;"
    qt_select_check_overflow2 "select v1, k1*10, v1 +k1*10 from test_overflow"
    qt_select_check_overflow3 "select `k1`, cast (`k1` as DECIMALV3(38, 36)) from test_overflow;"

    sql " SET check_overflow_for_decimal = false; "
    qt_select_not_check_overflow1 "select k1 * k2, k1 * k3, k1 * k2 * k3, k1 * v4, k1*50 from test_overflow;"
    qt_select_not_check_overflow2 "select v1, k1*10, v1 +k1*10 from test_overflow"
    sql "drop table if exists ${table1}"
}
