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

suite("condition_cache") {
    def tableName = "table_condition_cache"

    def test = {
        sql "set enable_condition_cache=false"

        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
        CREATE TABLE ${tableName} (
            `id` int NULL,
            `name` varchar(50) NULL,
            `age` int NULL,
            `score` double NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`, `name`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 10
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "min_load_replica_num" = "-1",
            "is_being_synced" = "false",
            "storage_medium" = "hdd",
            "storage_format" = "V2",
            "inverted_index_storage_format" = "V3",
            "light_schema_change" = "true",
            "disable_auto_compaction" = "false",
            "enable_single_replica_compaction" = "false"
        )
    """

        sql """
        INSERT INTO ${tableName}(id, name, age, score) 
        VALUES
            (1, "Alice", 25, 85.5),
            (2, "Bob", 30, 90.0),
            (3, "Charlie", 22, 75.5),
            (4, "David", 28, 92.0),
            (5, "Eve", 26, 88.0)
    """

        // First query with WHERE condition - Run without cache
        order_qt_condition_cache1 """
        SELECT 
            id, 
            name, 
            age, 
            score 
        FROM ${tableName} 
        WHERE age > 25 AND score > 85
    """

        // Second query with different WHERE condition - Run without cache
        order_qt_condition_cache2 """
        SELECT
            id,
            name,
            age,
            score
        FROM ${tableName}
        WHERE name LIKE 'A%' OR score < 80
    """

        // Enable condition cache
        sql "set enable_condition_cache=true"

        // Run the same first query with cache enabled
        order_qt_condition_cache3 """
        SELECT 
            id, 
            name, 
            age, 
            score 
        FROM ${tableName} 
        WHERE age > 25 AND score > 85
    """

        // Run the same second query with cache enabled
        order_qt_condition_cache4 """
        SELECT
            id,
            name,
            age,
            score
        FROM ${tableName}
        WHERE name LIKE 'A%' OR score < 80
    """

        // Run both queries again to test cache hit
        order_qt_condition_cache5 """
        SELECT 
            id, 
            name, 
            age, 
            score 
        FROM ${tableName} 
        WHERE age > 25 AND score > 85
    """

        order_qt_condition_cache6 """
        SELECT
            id,
            name,
            age,
            score
        FROM ${tableName}
        WHERE name LIKE 'A%' OR score < 80
    """
    }

    sql "set enable_nereids_distribute_planner=false"
    test()

    sql "set enable_nereids_distribute_planner=true"
    test()
}