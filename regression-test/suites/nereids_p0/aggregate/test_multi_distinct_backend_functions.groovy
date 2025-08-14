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

suite("test_multi_distinct_backend_functions") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

    def tableName = "multi_distinct_backend_test"

    sql "DROP TABLE IF EXISTS ${tableName}"
    
    sql """
        CREATE TABLE ${tableName} (
            id INT,
            name VARCHAR(50),
            value INT
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 3
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        )
    """

    sql """
        INSERT INTO ${tableName} VALUES
        (1, 'alice', 100),
        (1, 'bob', 200),
        (2, 'charlie', 300),
        (2, 'david', 400),
        (3, 'eve', 500)
    """
    sql "sync"

    // Test that multi_distinct_collect_list backend function works
    test {
        sql """
            SELECT 
                id,
                collect_list(DISTINCT name) as names,
                collect_list(DISTINCT CAST(value AS STRING)) as values_str
            FROM ${tableName}
            GROUP BY id
            ORDER BY id
        """
        // Should not throw "Agg Function multi_distinct_collect_list(...) is not implemented"
    }

    // Test that multi_distinct_array_agg backend function works
    test {
        sql """
            SELECT 
                id,
                array_agg(DISTINCT name) as names_array,
                array_agg(DISTINCT value) as values_array
            FROM ${tableName}
            GROUP BY id
            ORDER BY id
        """
        // Should not throw "Agg Function multi_distinct_array_agg(...) is not implemented"
    }

    // Test collect_list with limit parameter in multi-distinct scenario
    test {
        sql """
            SELECT 
                id,
                collect_list(DISTINCT name, 1) as names_limit1,
                collect_list(DISTINCT CAST(value AS STRING), 2) as values_limit2
            FROM ${tableName}
            GROUP BY id
            ORDER BY id
        """
        // Should work with limit parameter
    }

    // Verify results are correct
    qt_backend_collect_list """
        SELECT 
            id,
            size(collect_list(DISTINCT name)) as name_count,
            size(collect_list(DISTINCT CAST(value AS STRING))) as value_count
        FROM ${tableName}
        GROUP BY id
        ORDER BY id
    """

    qt_backend_array_agg """
        SELECT 
            id,
            size(array_agg(DISTINCT name)) as name_count,
            size(array_agg(DISTINCT value)) as value_count
        FROM ${tableName}
        GROUP BY id
        ORDER BY id
    """

    // Test that the functions produce the same results as their non-multi-distinct counterparts
    // when there's only one distinct operation
    qt_single_vs_multi_collect_list """
        SELECT 
            id,
            collect_list(DISTINCT name) as single_distinct,
            collect_list(name) as non_distinct
        FROM ${tableName}
        GROUP BY id
        ORDER BY id
    """

    qt_single_vs_multi_array_agg """
        SELECT 
            id,
            array_agg(DISTINCT value) as single_distinct,
            array_agg(value) as non_distinct
        FROM ${tableName}
        GROUP BY id
        ORDER BY id
    """

    sql "DROP TABLE IF EXISTS ${tableName}"
}
