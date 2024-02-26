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

suite("test_uniq_tab_decimalv2") {

    def table1 = "test_uniq_tab_decimalv2"

    sql "drop table if exists ${table1}"

    sql """
    CREATE TABLE IF NOT EXISTS `${table1}` (
      `decimal_key1` decimalv2(8, 5) NULL COMMENT "",
      `decimal_key2` decimalv2(16, 5) NULL COMMENT "",
      `decimal_value1` decimalv2(8, 5) NULL COMMENT "",
      `decimal_value2` decimalv2(16, 5) NULL COMMENT ""
    ) ENGINE=OLAP
    UNIQUE KEY(`decimal_key1`, `decimal_key2`)
    COMMENT "OLAP"
    DISTRIBUTED BY HASH(`decimal_key1`, `decimal_key2`) BUCKETS 8
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "in_memory" = "false",
    "storage_format" = "V2"
    )
    """

    sql """insert into ${table1} values(1.1,1.2,1.3,1.4),
            (1.1,1.2,1.3,1.4),
            (1.1,1.2,1.3,1.4),
            (1.1,1.2,1.3,1.4),
            (1.1,1.2,1.3,1.4),
            (2.1,1.2,1.3,1.4),
            (2.1,1.2,1.3,1.4),
            (2.1,1.2,1.3,1.4),
            (NULL, NULL, NULL, NULL)
    """
    qt_select_all "select * from ${table1} order by decimal_key1"

    qt_select_pred_decimal32_key "select * from ${table1} where decimal_key1 = 1.1 order by decimal_key1"
    qt_select_pred_decimal32_key "select * from ${table1} where decimal_key1 < 1.1111111111111111111 order by decimal_key1"
    sql "drop table if exists ${table1}"
}
