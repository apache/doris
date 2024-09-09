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

suite("test_mow_with_null_sequence") {
    def tableName = "test_null_sequence"
        sql """ DROP TABLE IF EXISTS $tableName """
        sql """
            CREATE TABLE `$tableName` (
                    `c_custkey` varchar(20) NOT NULL COMMENT "",
                    `c_name` varchar(20) NOT NULL COMMENT "",
                    `c_address` varchar(20) NOT NULL COMMENT "",
                    `c_date` date NULL COMMENT ""
            )
            UNIQUE KEY (`c_custkey`)
            CLUSTER BY(`c_name`, `c_date`)
            DISTRIBUTED BY HASH(`c_custkey`) BUCKETS 10
            PROPERTIES (
                    "function_column.sequence_col" = 'c_date',
                    "replication_num" = "1",
                    "enable_unique_key_merge_on_write" = "true"
             );
        """


        sql """ insert into $tableName values('a', 'abc', 'address1', NULL) """
        sql """ insert into $tableName values('a', 'abc', 'address2', '2022-10-20') """
        sql """ insert into $tableName values('a', 'abc', 'address3', NULL) """
        sql """ insert into $tableName values('ab', 'abc', 'address4', NULL) """
        sql """ insert into $tableName values('ab', 'abc', 'address5', '2022-10-20') """
        sql """ insert into $tableName values('ab', 'abc', 'address6', '2022-11-20') """
        sql """ insert into $tableName values('ab', 'abc', 'address6', '2022-11-15') """
        sql """ insert into $tableName values('aa1234', 'abc', 'address4', '2022-12-11') """
        sql """ insert into $tableName values('aa1234', 'abc', 'address5', NULL) """
        sql """ insert into $tableName values('aa1235', 'abc', 'address6', NULL) """

        order_qt_sql "select * from $tableName"

        sql """ DROP TABLE IF EXISTS $tableName """

        sql """
            CREATE TABLE `$tableName` (
                    `c_custkey` varchar(20) NOT NULL COMMENT "",
                    `c_name` varchar(20) NOT NULL COMMENT "",
                    `c_address` varchar(20) NOT NULL COMMENT "",
                    `c_int` int NULL COMMENT ""
            )
            UNIQUE KEY (`c_custkey`)
            CLUSTER BY(`c_int`, `c_name`)
            DISTRIBUTED BY HASH(`c_custkey`) BUCKETS 10
            PROPERTIES (
                    "function_column.sequence_col" = 'c_int',
                    "replication_num" = "1",
                    "enable_unique_key_merge_on_write" = "true"
             );
        """
        sql """ insert into $tableName values('a', 'abc', 'address1', NULL) """
        sql """ insert into $tableName values('a', 'abc', 'address2', 100) """
        sql """ insert into $tableName values('a', 'abc', 'address3', NULL) """

        sql """ insert into $tableName values('ab', 'abc', 'address4', NULL) """
        sql """ insert into $tableName values('ab', 'abc', 'address5', -10) """
        sql """ insert into $tableName values('ab', 'abc', 'address6', 110) """
        sql """ insert into $tableName values('ab', 'abc', 'address6', 100) """

        sql """ insert into $tableName values('aa1234', 'abc', 'address4', -1) """
        sql """ insert into $tableName values('aa1234', 'abc', 'address5', NULL) """

        sql """ insert into $tableName values('aa1235', 'abc', 'address6', NULL) """
        sql """ insert into $tableName values('aa1235', 'abc', 'address6', -1) """

        sql """ insert into $tableName values('aa1236', 'abc', 'address6', NULL) """
        sql """ insert into $tableName values('aa1236', 'abc', 'address6', 0) """
        sql """ insert into $tableName values('aa1236', 'abc', 'address6', NULL) """

        order_qt_sql "select * from $tableName"

        // sql """ DROP TABLE IF EXISTS $tableName """
}
