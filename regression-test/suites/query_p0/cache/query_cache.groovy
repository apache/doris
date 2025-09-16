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

import java.util.stream.Collectors

suite("query_cache") {
    def tableName = "table_3_undef_partitions2_keys3_properties4_distributed_by53"

    def test = {
        sql "set enable_query_cache=false"

        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
        CREATE TABLE ${tableName} (
            `pk` int NULL,
            `col_varchar_10__undef_signed` varchar(10) NULL,
            `col_int_undef_signed` int NULL,
            `col_varchar_1024__undef_signed` varchar(1024) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`pk`, `col_varchar_10__undef_signed`)
        DISTRIBUTED BY HASH(`pk`) BUCKETS 10
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "min_load_replica_num" = "-1",
            "is_being_synced" = "false",
            "storage_medium" = "hdd",
            "storage_format" = "V2",
            "inverted_index_storage_format" = "V3",
            "light_schema_change" = "true",
            "disable_auto_compaction" = "false",
            "enable_single_replica_compaction" = "false",
            "group_commit_interval_ms" = "10000",
            "group_commit_data_bytes" = "134217728"
        )
    """

        sql """
        INSERT INTO ${tableName}(pk, col_varchar_10__undef_signed, col_int_undef_signed, col_varchar_1024__undef_signed) 
        VALUES
            (0, "mean", null, "p"),
            (1, "is", 6, "what"),
            (2, "one", null, "e")
    """

        // First complex query - Run without cache
        order_qt_query_cache1 """
        SELECT 
            MIN(`pk`) AS field1,
            MAX(`pk`) AS field2,
            `pk` AS field3
        FROM ${tableName} AS alias1
        WHERE (
            alias1.col_varchar_1024__undef_signed LIKE CONCAT('aEIovabVCD', '%')
            AND (
                (alias1.`pk` = 154 OR (
                    alias1.col_varchar_1024__undef_signed LIKE CONCAT('lWpWJPFqXM', '%')
                    AND alias1.`pk` = 111
                ))
                AND (
                    alias1.col_varchar_10__undef_signed != 'IfGTFZuqZr'
                    AND alias1.col_varchar_1024__undef_signed > 'with'
                )
                AND alias1.`pk` IS NULL
            )
            AND alias1.col_int_undef_signed < 7
        )
        GROUP BY field3
    """

        // Simple query - Run without cache
        order_qt_query_cache2 """
        SELECT
            MIN(`pk`) AS field1,
            MAX(`pk`) AS field2,
            `pk` AS field3
        FROM ${tableName}
        GROUP BY field3
    """

        // Enable query cache
        sql "set enable_query_cache=true"

        // Run the same complex query with cache enabled
        order_qt_query_cache3 """
        SELECT 
            MIN(`pk`) AS field1,
            MAX(`pk`) AS field2,
            `pk` AS field3
        FROM ${tableName} AS alias1
        WHERE (
            alias1.col_varchar_1024__undef_signed LIKE CONCAT('aEIovabVCD', '%')
            AND (
                (alias1.`pk` = 154 OR (
                    alias1.col_varchar_1024__undef_signed LIKE CONCAT('lWpWJPFqXM', '%')
                    AND alias1.`pk` = 111
                ))
                AND (
                    alias1.col_varchar_10__undef_signed != 'IfGTFZuqZr'
                    AND alias1.col_varchar_1024__undef_signed > 'with'
                )
                AND alias1.`pk` IS NULL
            )
            AND alias1.col_int_undef_signed < 7
        )
        GROUP BY field3
    """

        // Run the same simple query with cache enabled
        order_qt_query_cache4 """
        SELECT
            MIN(`pk`) AS field1,
            MAX(`pk`) AS field2,
            `pk` AS field3
        FROM ${tableName}
        GROUP BY field3
    """

        // Run both queries again to test cache hit
        order_qt_query_cache5 """
        SELECT 
            MIN(`pk`) AS field1,
            MAX(`pk`) AS field2,
            `pk` AS field3
        FROM ${tableName} AS alias1
        WHERE (
            alias1.col_varchar_1024__undef_signed LIKE CONCAT('aEIovabVCD', '%')
            AND (
                (alias1.`pk` = 154 OR (
                    alias1.col_varchar_1024__undef_signed LIKE CONCAT('lWpWJPFqXM', '%')
                    AND alias1.`pk` = 111
                ))
                AND (
                    alias1.col_varchar_10__undef_signed != 'IfGTFZuqZr'
                    AND alias1.col_varchar_1024__undef_signed > 'with'
                )
                AND alias1.`pk` IS NULL
            )
            AND alias1.col_int_undef_signed < 7
        )
        GROUP BY field3
    """

        order_qt_query_cache6 """
        SELECT
            MIN(`pk`) AS field1,
            MAX(`pk`) AS field2,
            `pk` AS field3
        FROM ${tableName}
        GROUP BY field3
    """

        order_qt_query_cache7 """
        SELECT
        col_int_undef_signed,
            MIN(`col_int_undef_signed`) AS field1,
            MAX(`col_int_undef_signed`) AS field2,
            COUNT(`col_int_undef_signed`) AS field3,
            SUM(`col_int_undef_signed`) AS field4
        FROM ${tableName}
        GROUP BY col_int_undef_signed
    """

        // reorder the order_qt_query_cache7 select list to test the cache hit
        order_qt_query_cache8 """
 SELECT
    COUNT(`col_int_undef_signed`) AS field3,  -- Count of col_int_undef_signed (Original field3)
    col_int_undef_signed,                      -- The original unsigned integer column (Original col_int_undef_signed)
    SUM(`col_int_undef_signed`) AS field4,     -- Sum of col_int_undef_signed (Original field4)
    MIN(`col_int_undef_signed`) AS field1,     -- Minimum value of col_int_undef_signed (Original field1)
    MAX(`col_int_undef_signed`) AS field2      -- Maximum value of col_int_undef_signed (Original field2). Note: Trailing comma removed to avoid syntax error.
FROM ${tableName}
GROUP BY col_int_undef_signed;
    """
    }

    sql "set enable_nereids_distribute_planner=false"
    test()

    sql "set enable_nereids_distribute_planner=true"
    test()
} 
