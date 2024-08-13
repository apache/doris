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
    def tableName = "cluster_key_test_create_table"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    onFinish {
        try_sql("DROP TABLE IF EXISTS ${tableName}")
    }


    // mor unique table with cluster keys
    test {
        sql """
            CREATE TABLE `$tableName` (
                    `c_custkey` int(11) NOT NULL COMMENT "",
                    `c_name` varchar(26) NOT NULL COMMENT "",
                    `c_address` varchar(41) NOT NULL COMMENT "",
                    `c_city` varchar(11) NOT NULL COMMENT ""
            )
            UNIQUE KEY (`c_custkey`)
            CLUSTER BY (`c_name`, `c_address`)
            DISTRIBUTED BY HASH(`c_custkey`) BUCKETS 1
            PROPERTIES (
                    "replication_num" = "1",
                    "enable_unique_key_merge_on_write" = "false"
             );
        """
        // test mor table
        exception "Cluster keys only support unique keys table which enabled enable_unique_key_merge_on_write"
    }

    // mor unique table with cluster keys
    test {
        sql """
            CREATE TABLE `$tableName` (
                    `c_custkey` int(11) NOT NULL COMMENT "",
                    `c_name` varchar(26) NOT NULL COMMENT "",
                    `c_address` varchar(41) NOT NULL COMMENT "",
                    `c_city` varchar(11) NOT NULL COMMENT ""
            )
            UNIQUE KEY (`c_custkey`)
            CLUSTER BY (`c_name`, `c_address`)
            DISTRIBUTED BY HASH(`c_custkey`) BUCKETS 1
            PROPERTIES (
                    "replication_num" = "1",
                    "enable_unique_key_merge_on_write" = "false"
             );
        """
        exception "Cluster keys only support unique keys table which enabled enable_unique_key_merge_on_write"
    }

    // mow unique table with invalid cluster keys
    test {
        sql """
            CREATE TABLE `$tableName` (
                    `c_custkey` int(11) NOT NULL COMMENT "",
                    `c_name` varchar(26) NOT NULL COMMENT "",
                    `c_address` varchar(41) NOT NULL COMMENT "",
                    `c_city` varchar(11) NOT NULL COMMENT ""
            )
            UNIQUE KEY (`c_custkey`)
            CLUSTER BY (`c_name`, `c_addresses`)
            DISTRIBUTED BY HASH(`c_custkey`) BUCKETS 1
            PROPERTIES (
                    "replication_num" = "1",
                    "enable_unique_key_merge_on_write" = "true"
             );
        """
        exception "Key cluster column[c_addresses] doesn't exist"
    }

    // mow unique table with duplicate cluster keys
    test {
        sql """
            CREATE TABLE `$tableName` (
                    `c_custkey` int(11) NOT NULL COMMENT "",
                    `c_name` varchar(26) NOT NULL COMMENT "",
                    `c_address` varchar(41) NOT NULL COMMENT "",
                    `c_city` varchar(11) NOT NULL COMMENT ""
            )
            UNIQUE KEY (`c_custkey`)
            CLUSTER BY (`c_name`, `c_address`, `c_name`)
            DISTRIBUTED BY HASH(`c_custkey`) BUCKETS 1
            PROPERTIES (
                    "replication_num" = "1",
                    "enable_unique_key_merge_on_write" = "true"
             );
        """
        exception "Duplicate cluster key column[c_name]"
    }

    // mow unique table with same cluster and unique keys
    test {
        sql """
            CREATE TABLE `$tableName` (
                    `c_custkey` int(11) NOT NULL COMMENT "",
                    `c_name` varchar(26) NOT NULL COMMENT "",
                    `c_address` varchar(41) NOT NULL COMMENT "",
                    `c_city` varchar(11) NOT NULL COMMENT ""
            )
            UNIQUE KEY (`c_custkey`)
            CLUSTER BY (`c_custkey`)
            DISTRIBUTED BY HASH(`c_custkey`) BUCKETS 1
            PROPERTIES (
                    "replication_num" = "1",
                    "enable_unique_key_merge_on_write" = "true"
             );
        """
        exception "Unique keys and cluster keys should be different"
    }

    // mow unique table with short key is part of unique keys
    test {
        sql """
            CREATE TABLE `$tableName` (
                    `c_custkey` int(11) NOT NULL COMMENT "",
                    `c_age` int(11) NOT NULL COMMENT "",
                    `c_name` varchar(26) NOT NULL COMMENT "",
                    `c_address` varchar(41) NOT NULL COMMENT "",
                    `c_city` varchar(11) NOT NULL COMMENT ""
            )
            UNIQUE KEY (`c_custkey`, `c_age`,  `c_name`)
            CLUSTER BY (`c_custkey`, `c_age`,  `c_address`)
            DISTRIBUTED BY HASH(`c_custkey`) BUCKETS 1
            PROPERTIES (
                    "replication_num" = "1",
                    "enable_unique_key_merge_on_write" = "true",
                    "short_key" = "2"
             );
        """
        exception "2 short keys is a part of unique keys"
    }

    // mow unique table with short key is part of unique keys
    test {
        sql """
            CREATE TABLE `$tableName` (
                    `c_custkey` int(11) NOT NULL COMMENT "",
                    `c_age` int(11) NOT NULL COMMENT "",
                    `c_name` varchar(100) NOT NULL COMMENT "",
                    `c_address` varchar(100) NOT NULL COMMENT "",
                    `c_city` varchar(11) NOT NULL COMMENT ""
            )
            UNIQUE KEY (`c_custkey`, `c_age`,  `c_name`, `c_address`)
            CLUSTER BY (`c_custkey`, `c_age`,  `c_name`, `c_city`)
            DISTRIBUTED BY HASH(`c_custkey`) BUCKETS 1
            PROPERTIES (
                    "replication_num" = "1",
                    "enable_unique_key_merge_on_write" = "true"
             );
        """
        exception "3 short keys is a part of unique keys"
    }

    // success to create mow unique table with cluster keys
    sql """
        CREATE TABLE `$tableName` (
                `c_custkey` int(11) NOT NULL COMMENT "",
                `c_name` varchar(26) NOT NULL COMMENT "",
                `c_address` varchar(41) NOT NULL COMMENT "",
                `c_city` varchar(11) NOT NULL COMMENT ""
        )
        UNIQUE KEY (`c_custkey`)
        CLUSTER BY (`c_name`, `c_city`, `c_address`)
        DISTRIBUTED BY HASH(`c_custkey`) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    // test legacy planner
    sql """set enable_nereids_planner=false;"""
    // duplicate table with cluster keys
    test {
        sql """
            CREATE TABLE `$tableName` (
                    `c_custkey` int(11) NOT NULL COMMENT "",
                    `c_name` varchar(26) NOT NULL COMMENT "",
                    `c_address` varchar(41) NOT NULL COMMENT "",
                    `c_city` varchar(11) NOT NULL COMMENT ""
            )
            DUPLICATE KEY (`c_custkey`)
            CLUSTER BY (`c_name`, `c_address`)
            DISTRIBUTED BY HASH(`c_custkey`) BUCKETS 1
            PROPERTIES (
                    "replication_num" = "1"
             );
        """
        exception "Syntax error"
    }

    // test nereids planner
    sql """set enable_nereids_planner=true;"""
    sql """set enable_nereids_dml=true;"""
    sql """set enable_fallback_to_original_planner=false;"""
    // duplicate table with cluster keys
    test {
        sql """
            CREATE TABLE `$tableName` (
                    `c_custkey` int(11) NOT NULL COMMENT "",
                    `c_name` varchar(26) NOT NULL COMMENT "",
                    `c_address` varchar(41) NOT NULL COMMENT "",
                    `c_city` varchar(11) NOT NULL COMMENT ""
            )
            DUPLICATE KEY (`c_custkey`)
            CLUSTER BY (`c_name`, `c_address`)
            DISTRIBUTED BY HASH(`c_custkey`) BUCKETS 1
            PROPERTIES (
                    "replication_num" = "1"
             );
        """
        exception "Cluster keys only support unique keys table"
    }

    // cluster key contains complex type
    test {
        sql """
            CREATE TABLE `$tableName` (
                `c_custkey` int(11) NOT NULL COMMENT "",
                `c_name` varchar(26) NOT NULL COMMENT "",
                `c_address` varchar(41) NOT NULL COMMENT "",
                `c_city` variant NOT NULL COMMENT ""
            )
            UNIQUE KEY (`c_custkey`)
            CLUSTER BY (`c_name`, `c_city`, `c_address`)
            DISTRIBUTED BY HASH(`c_custkey`) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "true"
            );
        """
        exception "Variant type should not be used in key column"
    }
}
