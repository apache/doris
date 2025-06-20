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

suite("test_multi_distinct_collect_list") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

    def tableName = "multi_distinct_collect_test"

    sql "DROP TABLE IF EXISTS ${tableName}"
    
    sql """
        CREATE TABLE ${tableName} (
            group_id INT,
            category VARCHAR(20),
            value VARCHAR(50),
            condition1 INT,
            condition2 VARCHAR(10),
            numeric_val INT
        ) DUPLICATE KEY(group_id)
        DISTRIBUTED BY HASH(group_id) BUCKETS 3
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        )
    """

    sql """
        INSERT INTO ${tableName} VALUES
        (1, 'A', 'value1', 1, 'OA', 10),
        (1, 'A', 'value2', 1, 'OA', 20),
        (1, 'A', 'value3', 2, 'OA', 30),
        (1, 'A', 'value4', 1, 'OTHER', 40),
        (2, 'B', 'value5', 1, 'OA', 50),
        (2, 'B', 'value6', 2, 'OA', 60),
        (2, 'B', 'value7', 1, 'OTHER', 70),
        (3, 'C', 'value8', 1, 'OA', 80),
        (3, 'C', 'value9', 1, 'OA', 90)
    """
    sql "sync"

    // Test 1: Basic multi-distinct collect_list functionality
    qt_multi_distinct_collect_list_basic """
        SELECT 
            group_id,
            collect_list(DISTINCT CASE WHEN condition1 = 1 AND condition2 = 'OA' THEN value END) as list1,
            collect_list(DISTINCT CASE WHEN condition1 = 2 AND condition2 = 'OA' THEN value END) as list2
        FROM ${tableName}
        GROUP BY group_id
        ORDER BY group_id
    """

    // Test 2: Basic multi-distinct array_agg functionality  
    qt_multi_distinct_array_agg_basic """
        SELECT 
            group_id,
            array_agg(DISTINCT CASE WHEN condition1 = 1 AND condition2 = 'OA' THEN value END) as agg1,
            array_agg(DISTINCT CASE WHEN condition1 = 2 AND condition2 = 'OA' THEN value END) as agg2
        FROM ${tableName}
        GROUP BY group_id
        ORDER BY group_id
    """

    // Test 3: Mixed multi-distinct functions
    qt_multi_distinct_mixed """
        SELECT 
            group_id,
            count(DISTINCT CASE WHEN condition1 = 1 THEN value END) as count_distinct,
            collect_list(DISTINCT CASE WHEN condition2 = 'OA' THEN value END) as collect_distinct,
            array_agg(DISTINCT CASE WHEN condition1 = 1 THEN numeric_val END) as array_distinct,
            sum(DISTINCT CASE WHEN condition2 = 'OA' THEN numeric_val END) as sum_distinct
        FROM ${tableName}
        GROUP BY group_id
        ORDER BY group_id
    """

    // Test 4: collect_list with limit parameter in multi-distinct scenario
    qt_multi_distinct_collect_list_with_limit """
        SELECT 
            group_id,
            collect_list(DISTINCT CASE WHEN condition1 = 1 THEN value END, 2) as list1_limit,
            collect_list(DISTINCT CASE WHEN condition2 = 'OA' THEN value END, 3) as list2_limit
        FROM ${tableName}
        GROUP BY group_id
        ORDER BY group_id
    """

    // Test 5: Complex CASE expressions with multi-distinct
    qt_multi_distinct_complex_case """
        SELECT 
            category,
            collect_list(DISTINCT 
                CASE 
                    WHEN condition1 = 1 AND condition2 = 'OA' THEN CONCAT('type1_', value)
                    WHEN condition1 = 2 AND condition2 = 'OA' THEN CONCAT('type2_', value)
                    ELSE NULL
                END
            ) as complex_list1,
            array_agg(DISTINCT 
                CASE 
                    WHEN numeric_val > 50 THEN CONCAT('high_', CAST(numeric_val AS STRING))
                    WHEN numeric_val <= 50 THEN CONCAT('low_', CAST(numeric_val AS STRING))
                    ELSE NULL
                END
            ) as complex_agg1
        FROM ${tableName}
        GROUP BY category
        ORDER BY category
    """

    // Test 6: Multi-distinct with GROUP BY multiple columns
    qt_multi_distinct_group_by_multiple """
        SELECT 
            group_id,
            category,
            collect_list(DISTINCT CASE WHEN condition1 = 1 THEN value END) as list_by_condition1,
            collect_list(DISTINCT CASE WHEN condition2 = 'OA' THEN value END) as list_by_condition2
        FROM ${tableName}
        GROUP BY group_id, category
        ORDER BY group_id, category
    """

    // Test 7: Verify that single distinct operations still work
    qt_single_distinct_collect_list """
        SELECT 
            group_id,
            collect_list(DISTINCT value) as all_distinct_values
        FROM ${tableName}
        GROUP BY group_id
        ORDER BY group_id
    """

    qt_single_distinct_array_agg """
        SELECT 
            group_id,
            array_agg(DISTINCT numeric_val) as all_distinct_numbers
        FROM ${tableName}
        GROUP BY group_id
        ORDER BY group_id
    """

    // Test 8: Test with NULL values in CASE expressions
    sql """
        INSERT INTO ${tableName} VALUES
        (4, 'D', NULL, 1, 'OA', NULL),
        (4, 'D', 'value10', NULL, 'OA', 100)
    """
    sql "sync"

    qt_multi_distinct_with_nulls """
        SELECT 
            group_id,
            collect_list(DISTINCT CASE WHEN condition1 = 1 THEN value END) as list_with_nulls,
            array_agg(DISTINCT CASE WHEN condition2 = 'OA' THEN numeric_val END) as agg_with_nulls
        FROM ${tableName}
        WHERE group_id = 4
        GROUP BY group_id
        ORDER BY group_id
    """

    // Cleanup
    sql "DROP TABLE IF EXISTS ${tableName}"
}
