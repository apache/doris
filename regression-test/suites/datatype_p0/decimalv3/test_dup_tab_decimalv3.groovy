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

suite("test_dup_tab_decimalv3") {

    def table1 = "test_dup_tab_decimalv3"

    sql "drop table if exists ${table1}"

    sql """
    CREATE TABLE IF NOT EXISTS `${table1}` (
      `decimal32_key` decimalv3(8, 5) NULL COMMENT "",
      `decimal64_key` decimalv3(16, 5) NULL COMMENT "",
      `decimal128_key` decimalv3(38, 5) NULL COMMENT "",
      `decimal32_value` decimalv3(8, 5) NULL COMMENT "",
      `decimal64_value` decimalv3(16, 5) NULL COMMENT "",
      `decimal128_value` decimalv3(38, 5) NULL COMMENT ""
    ) ENGINE=OLAP
    DUPLICATE KEY(`decimal32_key`, `decimal64_key`, `decimal128_key`)
    COMMENT "OLAP"
    DISTRIBUTED BY HASH(`decimal32_key`, `decimal64_key`, `decimal128_key`) BUCKETS 8
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "in_memory" = "false",
    "storage_format" = "V2"
    )
    """
    sql "set enable_vectorized_engine = true"

    sql """insert into ${table1} values(1.1,1.2,1.3,1.4,1.5,1.6),
            (1.1,1.2,1.3,1.4,1.5,1.6),
            (1.1,1.2,1.3,1.4,1.5,1.6),
            (1.1,1.2,1.3,1.4,1.5,1.6),
            (1.1,1.2,1.3,1.4,1.5,1.6),
            (2.1,1.2,1.3,1.4,1.5,1.6),
            (2.1,1.2,1.3,1.4,1.5,1.6),
            (2.1,1.2,1.3,1.4,1.5,1.6),
            (NULL, NULL, NULL, NULL, NULL, NULL)
    """
    qt_select_all "select * from ${table1} order by decimal32_key"

    qt_select_pred_decimal32_key "select * from ${table1} where decimal32_key = 1.1 order by decimal32_key"
    qt_select_pred_decimal32_key "select * from ${table1} where decimal32_key < 1.1111111111111111111 order by decimal32_key"

    qt_select_pred_decimal32_value "select * from ${table1} where decimal32_value = 1.4 order by decimal32_key"
    qt_select_pred_decimal32_value "select * from ${table1} where decimal32_value < 1.444444444444444 order by decimal32_key"

    qt_select_pred_decimal64_key "select * from ${table1} where decimal64_key = 1.2 order by decimal32_key"
    qt_select_pred_decimal64_key "select * from ${table1} where decimal64_key < 1.222222222222222222 order by decimal32_key"

    qt_select_pred_decimal64_value "select * from ${table1} where decimal64_value = 1.5 order by decimal32_key"
    qt_select_pred_decimal64_value "select * from ${table1} where decimal64_value < 1.5555555555555555 order by decimal32_key"

    qt_select_pred_decimal128_key "select * from ${table1} where decimal128_key = 1.3 order by decimal32_key"
    qt_select_pred_decimal128_key "select * from ${table1} where decimal128_key < 1.333333333333 order by decimal32_key"

    qt_select_pred_decimal128_value "select * from ${table1} where decimal128_value = 1.6 order by decimal32_key"
    qt_select_pred_decimal128_value "select * from ${table1} where decimal128_value < 1.666666666666 order by decimal32_key"


    qt_select_between_pred_decimal32_key "select * from ${table1} where decimal32_key between 1.0991 and 1.1111 order by decimal32_key"
    qt_select_in_pred_decimal32_key1 "select * from ${table1} where decimal32_key in(1.1111) order by decimal32_key"
    qt_select_in_pred_decimal32_key2 "select * from ${table1} where decimal32_key in(1.10000000000) order by decimal32_key"
    qt_select_in_pred_decimal32_key3 "select * from ${table1} where decimal32_key in(1.1, 1.4) order by decimal32_key"

    qt_select_between_pred_decimal64_key "select * from ${table1} where decimal64_key between 1.1991 and 1.2111 order by decimal32_key"
    qt_select_in_pred_decimal64_key1 "select * from ${table1} where decimal64_key in(1.2111) order by decimal32_key"
    qt_select_in_pred_decimal64_key2 "select * from ${table1} where decimal64_key in(1.20000000000) order by decimal32_key"
    qt_select_in_pred_decimal64_key3 "select * from ${table1} where decimal64_key in(1.2, 1.4) order by decimal32_key"

    qt_select_between_pred_decimal128_key "select * from ${table1} where decimal128_key between 1.2991 and 1.3111 order by decimal32_key"
    qt_select_in_pred_decimal128_key1 "select * from ${table1} where decimal128_key in(1.3111) order by decimal32_key"
    qt_select_in_pred_decimal128_key2 "select * from ${table1} where decimal128_key in(1.30000000000) order by decimal32_key"
    qt_select_in_pred_decimal128_key3 "select * from ${table1} where decimal128_key in(1.3, 1.4) order by decimal32_key"

    sql "drop table if exists ${table1}"
}
