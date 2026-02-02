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

suite("test_pythonudaf_basic") {
    def pyPath = """${context.file.parent}/udaf_scripts/pyudaf.zip"""
    scp_udf_file_to_all_be(pyPath)
    def runtime_version = "3.8.10"
    log.info("Python zip path: ${pyPath}".toString())
    
    try {
        sql """ DROP TABLE IF EXISTS test_pythonudaf_basic """
        sql """
        CREATE TABLE IF NOT EXISTS test_pythonudaf_basic (
            `user_id`     INT      NOT NULL COMMENT "User ID",
            `int_col`     INT               COMMENT "Integer column (nullable for NULL tests)",
            `bigint_col`  BIGINT   NOT NULL COMMENT "BigInt column",
            `double_col`  DOUBLE   NOT NULL COMMENT "Double column",
            `category`    VARCHAR(10) NOT NULL COMMENT "Category for grouping"
            )
            DISTRIBUTED BY HASH(user_id) PROPERTIES("replication_num" = "1");
        """
        
        // Insert test data
        StringBuilder sb = new StringBuilder()
        int i = 1
        for (; i < 10; i++) {
            sb.append("""
                (${i}, ${i * 10}, ${i * 100}, ${i * 1.5}, 'cat${i % 3}'),
            """)
        }
        sb.append("""
                (${i}, ${i * 10}, ${i * 100}, ${i * 1.5}, 'cat${i % 3}')
            """)
        sql """ INSERT INTO test_pythonudaf_basic VALUES
             ${sb.toString()}
            """
        qt_select_default """ SELECT * FROM test_pythonudaf_basic ORDER BY user_id; """

        // ========================================
        // Test 1: Basic SumInt UDAF
        // ========================================
        sql """ DROP FUNCTION IF EXISTS python_udaf_sum_int(int) """
        
        sql """ CREATE AGGREGATE FUNCTION python_udaf_sum_int(int) RETURNS bigint PROPERTIES (
            "file"="file://${pyPath}",
            "symbol"="sum_int.SumInt",
            "type"="PYTHON_UDF",
            "always_nullable" = "true",
            "runtime_version" = "${runtime_version}"
        ); """

        // Test basic aggregation - sum all int_col values
        qt_select_sum_all """ SELECT python_udaf_sum_int(int_col) as result FROM test_pythonudaf_basic; """
        
        // Test with GROUP BY
        qt_select_sum_group """ SELECT category, python_udaf_sum_int(int_col) as sum_result 
                                FROM test_pythonudaf_basic 
                                GROUP BY category 
                                ORDER BY category; """
        
        // Test with multiple aggregates in same query
        qt_select_sum_multiple """ SELECT category, 
                                          python_udaf_sum_int(int_col) as py_sum,
                                          sum(int_col) as native_sum
                                   FROM test_pythonudaf_basic 
                                   GROUP BY category 
                                   ORDER BY category; """
        
        // Test with NULL handling - insert some NULL values
        sql """ INSERT INTO test_pythonudaf_basic VALUES (11, NULL, 1100, 16.5, 'cat2'); """
        sql """ INSERT INTO test_pythonudaf_basic VALUES (12, NULL, 1200, 18.0, 'cat0'); """
        
        qt_select_sum_with_null """ SELECT python_udaf_sum_int(int_col) as result FROM test_pythonudaf_basic; """
        
        qt_select_sum_group_with_null """ SELECT category, python_udaf_sum_int(int_col) as sum_result 
                                          FROM test_pythonudaf_basic 
                                          GROUP BY category 
                                          ORDER BY category; """

        // ========================================
        // Test 2: AvgDouble UDAF
        // ========================================
        sql """ DROP FUNCTION IF EXISTS python_udaf_avg_double(double) """
        
        sql """ CREATE AGGREGATE FUNCTION python_udaf_avg_double(double) RETURNS double PROPERTIES (
            "file"="file://${pyPath}",
            "symbol"="sum_int.AvgDouble",
            "type"="PYTHON_UDF",
            "always_nullable" = "true",
            "runtime_version" = "${runtime_version}"
        ); """

        qt_select_avg_all """ SELECT python_udaf_avg_double(double_col) as result FROM test_pythonudaf_basic; """
        
        qt_select_avg_group """ SELECT category, 
                                       python_udaf_avg_double(double_col) as py_avg,
                                       avg(double_col) as native_avg
                                FROM test_pythonudaf_basic 
                                GROUP BY category 
                                ORDER BY category; """

        // ========================================
        // Test 3: Window Function Support
        // ========================================
        qt_select_window_partition """ SELECT user_id, category, int_col,
                                              python_udaf_sum_int(int_col) OVER(PARTITION BY category) as sum_by_category
                                       FROM test_pythonudaf_basic 
                                       WHERE int_col IS NOT NULL
                                       ORDER BY category, user_id; """

        qt_select_window_order """ SELECT user_id, category, int_col,
                                          python_udaf_sum_int(int_col) OVER(PARTITION BY category ORDER BY user_id) as running_sum
                                   FROM test_pythonudaf_basic 
                                   WHERE int_col IS NOT NULL
                                   ORDER BY category, user_id; """

        qt_select_window_rows """ SELECT user_id, category, int_col,
                                         python_udaf_sum_int(int_col) OVER(
                                             PARTITION BY category 
                                             ORDER BY user_id 
                                             ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
                                         ) as window_sum
                                  FROM test_pythonudaf_basic 
                                  WHERE int_col IS NOT NULL
                                  ORDER BY category, user_id; """

        // ========================================
        // Test 4: Global Function
        // ========================================
        sql """ DROP GLOBAL FUNCTION IF EXISTS python_udaf_sum_int_global(int) """
        
        sql """ CREATE GLOBAL AGGREGATE FUNCTION python_udaf_sum_int_global(int) RETURNS bigint PROPERTIES (
            "file"="file://${pyPath}",
            "symbol"="sum_int.SumInt",
            "type"="PYTHON_UDF",
            "always_nullable" = "true",
            "runtime_version" = "${runtime_version}"
        ); """

        qt_select_global_1 """ SELECT python_udaf_sum_int_global(int_col) as result FROM test_pythonudaf_basic; """
        qt_select_global_2 """ SELECT category, python_udaf_sum_int_global(int_col) as sum_result 
                               FROM test_pythonudaf_basic 
                               GROUP BY category 
                               ORDER BY category; """

    } finally {
        try_sql("DROP GLOBAL FUNCTION IF EXISTS python_udaf_sum_int_global(int);")
        try_sql("DROP FUNCTION IF EXISTS python_udaf_avg_double(double);")
        try_sql("DROP FUNCTION IF EXISTS python_udaf_sum_int(int);")
        try_sql("DROP TABLE IF EXISTS test_pythonudaf_basic")
    }
}
