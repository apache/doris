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
    def joinTableName = "table_join_condition_cache"

    def test = {
        sql "set enable_condition_cache=false"
        sql "set runtime_filter_type=0"

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

        // Create join table
        sql """ DROP TABLE IF EXISTS ${joinTableName} """
        sql """
        CREATE TABLE ${joinTableName} (
            `id` int NULL,
            `department` varchar(50) NULL,
            `position` varchar(50) NULL,
            `salary` double NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`, `department`)
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
        INSERT INTO ${joinTableName}(id, department, position, salary) 
        VALUES
            (1, "Engineering", "Developer", 100000),
            (2, "Marketing", "Manager", 120000),
            (3, "HR", "Specialist", 80000),
            (4, "Engineering", "Senior Developer", 140000),
            (5, "Finance", "Analyst", 95000)
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

        // Test delete operation impact on condition cache
        // Delete some data
        sql "DELETE FROM ${tableName} WHERE age = 30"  // Delete Bob's record

        // Run the same queries after delete to see if cache is invalidated
        order_qt_condition_delete1 """
        SELECT 
            id, 
            name, 
            age, 
            score 
        FROM ${tableName} 
        WHERE age > 25 AND score > 85
    """

        order_qt_condition_delete2 """
        SELECT
            id,
            name,
            age,
            score
        FROM ${tableName}
        WHERE name LIKE 'A%' OR score < 80
    """
    
    // rebuild table to skip the delete operation
    sql "create table temp like ${tableName}" 
    sql "insert into temp select * from ${tableName}" 
    sql "drop table ${tableName}"
    sql "alter table temp rename ${tableName}" 

        // Test with two-table join and runtime_filter set to bloom filter (6)
        // First, disable condition cache and reset runtime_filter
        sql "set enable_condition_cache=false"
        sql "set runtime_filter_type=2"

        // Run join query without condition cache
        order_qt_join_no_cache """
        SELECT 
            t1.id, 
            t1.name, 
            t1.age, 
            t2.department, 
            t2.position
        FROM ${tableName} t1 
        JOIN ${joinTableName} t2 ON t1.id = t2.id
        WHERE t1.age > 25 AND t2.salary > 90000
    """

        // Enable condition cache with bloom filter runtime_filter
        sql "set enable_condition_cache=true"

        // Run the same join query with condition cache enabled
        order_qt_join_cache1 """
        SELECT 
            t1.id, 
            t1.name, 
            t1.age, 
            t2.department, 
            t2.position
        FROM ${tableName} t1 
        JOIN ${joinTableName} t2 ON t1.id = t2.id
        WHERE t1.age > 25 AND t2.salary > 90000
    """

        // Run the same join query again to test cache hit
        order_qt_join_cache2 """
        SELECT 
            t1.id, 
            t1.name, 
            t1.age, 
            t2.department, 
            t2.position
        FROM ${tableName} t1 
        JOIN ${joinTableName} t2 ON t1.id = t2.id
        WHERE t1.age > 25 AND t2.salary > 90000
    """

    sql "set runtime_filter_type=12"

    // Run the same join query again after changing runtime_filter_type
    order_qt_join_cache3 """
        SELECT 
            t1.id, 
            t1.name, 
            t1.age, 
            t2.department, 
            t2.position
        FROM ${tableName} t1 
        JOIN ${joinTableName} t2 ON t1.id = t2.id
        WHERE t1.age > 25 AND t2.salary > 90000
    """
        // Test different join query with same tables and bloom filter
        order_qt_join_diff_cond """
        SELECT 
            t1.id, 
            t1.name, 
            t2.department, 
            t2.salary
        FROM ${tableName} t1 
        JOIN ${joinTableName} t2 ON t1.id = t2.id
        WHERE t1.score > 85 AND t2.department = 'Engineering'
    """

        // Test data modification impact on join condition cache
        sql "INSERT INTO ${tableName}(id, name, age, score) VALUES (6, 'Frank', 32, 91.0)"
        sql "INSERT INTO ${joinTableName}(id, department, position, salary) VALUES (6, 'Engineering', 'Team Lead', 150000)"

        // Run the same join query after data modification
        order_qt_join_after_mod """
        SELECT 
            t1.id, 
            t1.name, 
            t1.age, 
            t2.department, 
            t2.position
        FROM ${tableName} t1 
        JOIN ${joinTableName} t2 ON t1.id = t2.id
        WHERE t1.age > 25 AND t2.salary > 90000
    """
    }

    sql "set enable_nereids_distribute_planner=false"
    test()

    sql "set enable_nereids_distribute_planner=true"
    test()
}
