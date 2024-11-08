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

suite("test_mow_with_null_sequence", "inverted_index") {
    def tableName = "test_null_sequence"
        sql """ DROP TABLE IF EXISTS $tableName """
        sql """
            CREATE TABLE `$tableName` (
                    `c_custkey` varchar(20) NOT NULL COMMENT "",
                    `c_name` varchar(20) NOT NULL COMMENT "",
                    `c_address` varchar(20) NOT NULL COMMENT "",
                    `c_date` date NULL COMMENT "",
                    INDEX c_custkey_idx(c_custkey) USING INVERTED COMMENT 'c_custkey index',
                    INDEX c_name_idx(c_name) USING INVERTED PROPERTIES("parser"="english") COMMENT 'c_name index',
                    INDEX c_address_idx(c_address) USING INVERTED PROPERTIES("parser"="standard") COMMENT 'c_address index',
                    INDEX c_date_index(c_date) USING INVERTED COMMENT 'c_date_index index'
            )
            UNIQUE KEY (`c_custkey`)
            DISTRIBUTED BY HASH(`c_custkey`) BUCKETS 10
            PROPERTIES (
                    "function_column.sequence_col" = 'c_date',
                    "replication_num" = "1",
                    "enable_unique_key_merge_on_write" = "true"
             );
        """

        sql """ set enable_common_expr_pushdown = true """

        sql """ insert into $tableName values('a', 'zhang san', 'address1', NULL) """
        sql """ insert into $tableName values('a', 'zhang si', 'address2', '2022-10-20') """
        sql """ insert into $tableName values('a', 'li si', 'address3', NULL) """
        sql """ insert into $tableName values('ab', 'zhang yi', 'address4', NULL) """
        sql """ insert into $tableName values('ab', 'li yi', 'address5', '2022-10-20') """
        sql """ insert into $tableName values('ab', 'zhang si yi', 'address6', '2022-11-20') """
        sql """ insert into $tableName values('ab', 'zhang ke', 'address6', '2022-11-15') """
        sql """ insert into $tableName values('aa1234', 'li yi ke', 'address4', '2022-12-11') """
        sql """ insert into $tableName values('aa1234', 'li ke', 'address5', NULL) """
        sql """ insert into $tableName values('aa1235', 'li wu', 'address6', NULL) """

        qt_sql1 """ select * from $tableName order by c_custkey """
        qt_sql2 """ select * from $tableName where c_custkey='a' order by c_custkey """
        qt_sql3 """ select * from $tableName where c_custkey='ab' order by c_custkey """
        qt_sql4 """ select * from $tableName where c_custkey>'aa' order by c_custkey """

        qt_sql5 """ select * from $tableName where c_name match_any 'zhang' order by c_custkey """
        qt_sql6 """ select * from $tableName where c_name match_any 'zhang' and c_date is not null order by c_custkey """
        qt_sql7 """ select * from $tableName where c_name match_any 'zhang' and c_date is null order by c_custkey """
        qt_sql8 """ select * from $tableName where c_name match_any 'zhang' and c_date > '2022-10-20' order by c_custkey """

        qt_sql9 """ select * from $tableName where c_name match_any 'li' order by c_custkey """
        qt_sql10 """ select * from $tableName where c_name match_any 'li' and c_date is not null order by c_custkey """
        qt_sql11 """ select * from $tableName where c_name match_any 'li' and c_date is null order by c_custkey """
        qt_sql12 """ select * from $tableName where c_name match_any 'li' and c_date > '2022-10-20' order by c_custkey """

        qt_sql13 """ select * from $tableName where c_name match_any 'wu' order by c_custkey """
        qt_sql14 """ select * from $tableName where c_name match_any 'wu' and c_date is not null order by c_custkey """
        qt_sql15 """ select * from $tableName where c_name match_any 'wu' and c_date > '2022-10-20' order by c_custkey """

        qt_sql16 """ select * from $tableName where c_name match_all 'zhang si' order by c_custkey """
        qt_sql17 """ select * from $tableName where c_name match_all 'zhang si' and c_date < '2022-11-20' order by c_custkey """
        qt_sql18 """ select * from $tableName where c_name match_all 'zhang yi' order by c_custkey """
        qt_sql19 """ select * from $tableName where c_name match_all 'zhang yi' and c_date is not null order by c_custkey """
        
        qt_sql20 """ select * from $tableName where c_name match_all 'li yi' order by c_custkey """
        qt_sql21 """ select * from $tableName where c_name match_all 'li yi' and (c_date < '2022-11-20' or c_date >= '2022-12-11') order by c_custkey """

        sql """ DROP TABLE IF EXISTS $tableName """

        sql """
            CREATE TABLE `$tableName` (
                    `c_custkey` varchar(20) NOT NULL COMMENT "",
                    `c_name` varchar(20) NOT NULL COMMENT "",
                    `c_address` varchar(20) NOT NULL COMMENT "",
                    `c_int` int NULL COMMENT "",
                    INDEX c_custkey_idx(c_custkey) USING INVERTED COMMENT 'c_custkey index',
                    INDEX c_name_idx(c_name) USING INVERTED PROPERTIES("parser"="english") COMMENT 'c_name index',
                    INDEX c_address_idx(c_address) USING INVERTED PROPERTIES("parser"="standard") COMMENT 'c_address index',
                    INDEX c_int_index(c_int) USING INVERTED COMMENT 'c_int_index index'
            )
            UNIQUE KEY (`c_custkey`)
            DISTRIBUTED BY HASH(`c_custkey`) BUCKETS 10
            PROPERTIES (
                    "function_column.sequence_col" = 'c_int',
                    "replication_num" = "1",
                    "enable_unique_key_merge_on_write" = "true"
             );
        """
        sql """ insert into $tableName values('a', 'zhang san', 'address1', NULL) """
        sql """ insert into $tableName values('a', 'zhang si', 'address2', 100) """
        sql """ insert into $tableName values('a', 'li si', 'address3', NULL) """

        sql """ insert into $tableName values('ab', 'li san', 'address4', NULL) """
        sql """ insert into $tableName values('ab', 'li yi', 'address5', -10) """
        sql """ insert into $tableName values('ab', 'zhang yi', 'address6', 110) """
        sql """ insert into $tableName values('ab', 'zhang san yi', 'address6', 100) """

        sql """ insert into $tableName values('aa1234', 'wu ke', 'address4', -1) """
        sql """ insert into $tableName values('aa1234', 'li ke', 'address5', NULL) """

        sql """ insert into $tableName values('aa1235', 'li yi ke', 'address6', NULL) """
        sql """ insert into $tableName values('aa1235', 'zhang ke', 'address6', -1) """

        sql """ insert into $tableName values('aa1236', 'wu si', 'address6', NULL) """
        sql """ insert into $tableName values('aa1236', 'wu yi', 'address6', 0) """
        sql """ insert into $tableName values('aa1236', 'zhang ke', 'address6', NULL) """

        qt_sql22 """ select * from $tableName order by c_custkey """
        qt_sql23 """ select * from $tableName where c_custkey='a' order by c_custkey """
        qt_sql24 """ select * from $tableName where c_custkey='ab' order by c_custkey """
        qt_sql25 """ select * from $tableName where c_custkey>'aa' order by c_custkey """

        qt_sql26 """ select * from $tableName where c_name match_any 'zhang' order by c_custkey """
        qt_sql27 """ select * from $tableName where c_name match_any 'zhang' and c_int is not null order by c_custkey """
        qt_sql28 """ select * from $tableName where c_name match_any 'zhang' and c_int is null order by c_custkey """
        qt_sql29 """ select * from $tableName where c_name match_any 'zhang' and c_int > 0 order by c_custkey """

        qt_sql30 """ select * from $tableName where c_name match_any 'li' order by c_custkey """
        qt_sql31 """ select * from $tableName where c_name match_any 'li' and c_int is not null order by c_custkey """
        qt_sql32 """ select * from $tableName where c_name match_any 'li' and c_int is null order by c_custkey """
        qt_sql33 """ select * from $tableName where c_name match_any 'li' and c_int < 0 order by c_custkey """

        qt_sql34 """ select * from $tableName where c_name match_any 'wu' order by c_custkey """
        qt_sql35 """ select * from $tableName where c_name match_any 'wu' and c_int is not null order by c_custkey """
        qt_sql36 """ select * from $tableName where c_name match_any 'wu' and c_int >= 0 order by c_custkey """

        qt_sql37 """ select * from $tableName where c_name match_all 'zhang si' order by c_custkey """
        qt_sql38 """ select * from $tableName where c_name match_all 'zhang si' and c_int < 0 order by c_custkey """
        qt_sql39 """ select * from $tableName where c_name match_all 'zhang yi' order by c_custkey """
        qt_sql40 """ select * from $tableName where c_name match_all 'zhang yi' and c_int > 100 order by c_custkey """
        
        qt_sql41 """ select * from $tableName where c_name match_all 'li yi' order by c_custkey """
        qt_sql42 """ select * from $tableName where c_name match_all 'li yi' and c_int < 0 order by c_custkey """
        qt_sql43 """ select * from $tableName where c_name match_all 'li yi' and (c_int < 0 or c_int is null) order by c_custkey """

}
