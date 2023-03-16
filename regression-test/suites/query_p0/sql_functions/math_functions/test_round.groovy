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

suite("test_round") {
    qt_select "SELECT round(10.12345)"
    qt_select "SELECT round(10.12345, 2)"
    qt_select "SELECT round_bankers(10.12345)"
    qt_select "SELECT round_bankers(10.12345, 2)"

    def tableTest = "test_query_db.test"
    qt_truncate "select truncate(k1, 1), truncate(k2, 1), truncate(k3, 1), truncate(k5, 1), truncate(k8, 1), truncate(k9, 1) from ${tableTest} order by 1;"

    def tableName = "test_round"
    sql """DROP TABLE IF EXISTS `${tableName}`"""
    sql """ CREATE TABLE `${tableName}` (
        `col1` DECIMALV3(6,3) COMMENT "",
        `col2` DECIMALV3(16,5) COMMENT "",
        `col3` DECIMALV3(32,5) COMMENT "")
        DUPLICATE KEY(`col1`) DISTRIBUTED BY HASH(`col1`)
        PROPERTIES ( "replication_num" = "1" ); """

    sql """ insert into `${tableName}` values(16.025, 16.025, 16.025); """
    qt_select """ SELECT round(col1), round(col2), round(col3) FROM `${tableName}`; """
    qt_select """ SELECT floor(col1), floor(col2), floor(col3) FROM `${tableName}`; """
    qt_select """ SELECT ceil(col1), ceil(col2), ceil(col3) FROM `${tableName}`; """
    qt_select """ SELECT round_bankers(col1), round_bankers(col2), round_bankers(col3) FROM `${tableName}`; """

    qt_select """ SELECT round(col1, 2), round(col2, 2), round(col3, 2) FROM `${tableName}`; """
    qt_select """ SELECT floor(col1, 2), floor(col2, 2), floor(col3, 2) FROM `${tableName}`; """
    qt_select """ SELECT ceil(col1, 2), ceil(col2, 2), ceil(col3, 2) FROM `${tableName}`; """
    qt_select """ SELECT truncate(col1, 2), truncate(col2, 2), truncate(col3, 2) FROM `${tableName}`; """
    qt_select """ SELECT round_bankers(col1, 2), round_bankers(col2, 2), round_bankers(col3, 2) FROM `${tableName}`; """

    qt_select """ SELECT round(col1, -1), round(col2, -1), round(col3, -1) FROM `${tableName}`; """
    qt_select """ SELECT floor(col1, -1), floor(col2, -1), floor(col3, -1) FROM `${tableName}`; """
    qt_select """ SELECT ceil(col1, -1), ceil(col2, -1), ceil(col3, -1) FROM `${tableName}`; """
    qt_select """ SELECT truncate(col1, -1), truncate(col2, -1), truncate(col3, -1) FROM `${tableName}`; """
    qt_select """ SELECT round_bankers(col1, -1), round_bankers(col2, -1), round_bankers(col3, -1) FROM `${tableName}`; """

    sql """ DROP TABLE IF EXISTS `${tableName}` """

    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

    qt_nereids_round_arg1 "SELECT round(10.12345)"
    qt_nereids_round_arg2 "SELECT round(10.12345, 2)"
    qt_nereids_round_bankers_arg1 "SELECT round_bankers(10.12345)"
    qt_nereids_round_bankers_arg2 "SELECT round_bankers(10.12345, 2)"

}
