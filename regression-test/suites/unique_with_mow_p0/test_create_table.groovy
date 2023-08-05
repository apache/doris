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

suite("test_create_table") {
    def tableName = "unique_mow_create_table"
    sql """ DROP TABLE IF EXISTS ${tableName} """

    // duplicate table with enable_unique_key_merge_on_write property
    test {
        sql """
            CREATE TABLE `$tableName` (
                    `c_custkey` int(11) NOT NULL COMMENT "",
                    `c_name` varchar(26) NOT NULL COMMENT "",
                    `c_address` varchar(41) NOT NULL COMMENT "",
                    `c_city` varchar(11) NOT NULL COMMENT ""
            )
            DUPLICATE KEY (`c_custkey`)
            DISTRIBUTED BY HASH(`c_custkey`) BUCKETS 1
            PROPERTIES (
                    "replication_num" = "1",
                    "enable_unique_key_merge_on_write" = "true"
             );
        """
        exception "enable_unique_key_merge_on_write property only support unique key table"
    }

    // duplicate table with enable_unique_key_merge_on_write property
    test {
        sql """
            CREATE TABLE `$tableName` (
                    `c_custkey` int(11) NOT NULL COMMENT "",
                    `c_name` varchar(26) NOT NULL COMMENT "",
                    `c_address` varchar(41) NOT NULL COMMENT "",
                    `c_city` varchar(11) NOT NULL COMMENT ""
            )
            DUPLICATE KEY (`c_custkey`)
            DISTRIBUTED BY HASH(`c_custkey`) BUCKETS 1
            PROPERTIES (
                    "replication_num" = "1",
                    "enable_unique_key_merge_on_write" = "false"
             );
        """
        exception "enable_unique_key_merge_on_write property only support unique key table"
    }

    // agg table with enable_unique_key_merge_on_write property
    test {
        sql """
            CREATE TABLE `$tableName` (
                    `c_custkey` int(11) NOT NULL COMMENT "",
                    `c_name` varchar(26) max NOT NULL COMMENT "",
                    `c_address` varchar(41) max NOT NULL COMMENT "",
                    `c_city` varchar(11) max NOT NULL COMMENT ""
            )
            AGGREGATE KEY (`c_custkey`)
            DISTRIBUTED BY HASH(`c_custkey`) BUCKETS 1
            PROPERTIES (
                    "replication_num" = "1",
                    "enable_unique_key_merge_on_write" = "true"
             );
        """
        exception "enable_unique_key_merge_on_write property only support unique key table"
    }

    // agg table with enable_unique_key_merge_on_write property
    test {
        sql """
            CREATE TABLE `$tableName` (
                    `c_custkey` int(11) NOT NULL COMMENT "",
                    `c_name` varchar(26) max NOT NULL COMMENT "",
                    `c_address` varchar(41) max NOT NULL COMMENT "",
                    `c_city` varchar(11) max NOT NULL COMMENT ""
            )
            AGGREGATE KEY (`c_custkey`)
            DISTRIBUTED BY HASH(`c_custkey`) BUCKETS 1
            PROPERTIES (
                    "replication_num" = "1",
                    "enable_unique_key_merge_on_write" = "false"
             );
        """
        exception "enable_unique_key_merge_on_write property only support unique key table"
    }

    // unique table with enable_unique_key_merge_on_write  and enable_single_replica_compaction property
    test {
        sql """
            CREATE TABLE `$tableName` (
                    `c_custkey` int(11) NOT NULL COMMENT "",
                    `c_name` varchar(26) NOT NULL COMMENT "",
                    `c_address` varchar(41) NOT NULL COMMENT "",
                    `c_city` varchar(11) NOT NULL COMMENT ""
            )
            UNIQUE KEY (`c_custkey`)
            DISTRIBUTED BY HASH(`c_custkey`) BUCKETS 1
            PROPERTIES (
                    "replication_num" = "1",
                    "enable_unique_key_merge_on_write" = "true",
                    "enable_single_replica_compaction" = "true"
             );
        """
        exception "enable_single_replica_compaction property is not supported for merge-on-write table"
    }
}
