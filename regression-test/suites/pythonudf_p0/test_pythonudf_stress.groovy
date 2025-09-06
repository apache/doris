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

suite("test_pythonudf_stress") {
    // Stress test for Python UDF - configurable dataset size
    // This test is designed to push Python UDF to its limits
    
    def scalarPyPath = """${context.file.parent}/udf_scripts/python_udf_scalar_ops.zip"""
    def vectorPyPath = """${context.file.parent}/udf_scripts/python_udf_vector_ops.zip"""
    scp_udf_file_to_all_be(scalarPyPath)
    scp_udf_file_to_all_be(vectorPyPath)
    def runtime_version = "3.10.12"

    sql "CREATE DATABASE IF NOT EXISTS test_pythonudf_stress"
    sql "USE test_pythonudf_stress"
    
    // Configuration: Adjust these for different stress levels
    def TOTAL_ROWS = 5000000      // 5 million rows (change to 10M, 50M for extreme stress)
    def BATCH_SIZE = 50000        // Insert batch size
    def BUCKETS = 32              // Number of buckets for distribution
    
    log.info("\n" + "=" * 80 + "\nPYTHON UDF STRESS TEST\n" + "=" * 80 + "\nConfiguration:\n" +
        "  Total Rows: ${TOTAL_ROWS}\n" +
        "  Batch Size: ${BATCH_SIZE}\n" +
        "  Buckets: ${BUCKETS}\n" +
        "=" * 80)
    
    try {
        // ==================== Create Stress Test Table ====================
        sql """ DROP TABLE IF EXISTS python_udf_stress_table; """
        sql """
        CREATE TABLE python_udf_stress_table (
            id BIGINT,
            category INT,
            value1 INT,
            value2 INT,
            price DOUBLE,
            discount DOUBLE,
            name STRING,
            description STRING,
            email STRING,
            is_active BOOLEAN,
            created_date DATE
        ) ENGINE=OLAP 
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS ${BUCKETS}
        PROPERTIES("replication_num" = "1");
        """
        
        log.info("Loading ${TOTAL_ROWS} rows using streamLoad from CSV file...")
        def loadStartTime = System.currentTimeMillis()
        
        streamLoad {
            db 'test_pythonudf_stress'
            table "python_udf_stress_table"
            set 'column_separator', '\t'
            file 'benchmark_data_5m.csv'
            time 300000 // 300 seconds (5 minutes) timeout
            
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(json.NumberTotalRows, json.NumberLoadedRows)
                assertTrue(json.NumberLoadedRows > 0 && json.LoadBytes > 0)
            }
        }
        
        def loadEndTime = System.currentTimeMillis()
        log.info("Data loaded in ${loadEndTime - loadStartTime} ms (${String.format('%.2f', TOTAL_ROWS / ((loadEndTime - loadStartTime) / 1000.0))} rows/sec)")
        
        sql "sync"
        
        def rowCount = sql "SELECT COUNT(*) FROM python_udf_stress_table"
        log.info("Verified row count: ${rowCount[0][0]}")
        
        // ==================== Define UDFs ====================
        
        // Scalar UDF - Simple
        sql """ DROP FUNCTION IF EXISTS py_calc_final_price(DOUBLE, DOUBLE); """
        sql """
        CREATE FUNCTION py_calc_final_price(DOUBLE, DOUBLE) 
        RETURNS DOUBLE 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "file" = "file://${scalarPyPath}",
            "symbol" = "python_udf_scalar_ops.calculate_discount_price",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        );
        """
        
        // Scalar UDF - Complex
        sql """ DROP FUNCTION IF EXISTS py_extract_domain(STRING); """
        sql """
        CREATE FUNCTION py_extract_domain(STRING) 
        RETURNS STRING 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "file" = "file://${scalarPyPath}",
            "symbol" = "python_udf_scalar_ops.extract_domain",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        );
        """
        
        // Vector UDF - Simple
        sql """ DROP FUNCTION IF EXISTS py_vec_multiply(INT, INT); """
        sql """
        CREATE FUNCTION py_vec_multiply(INT, INT) 
        RETURNS INT 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "file" = "file://${vectorPyPath}",
            "symbol" = "python_udf_vector_ops.multiply_by_constant",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        );
        """
        
        // Vector UDF - String
        sql """ DROP FUNCTION IF EXISTS py_vec_upper(STRING); """
        sql """
        CREATE FUNCTION py_vec_upper(STRING) 
        RETURNS STRING 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "file" = "file://${vectorPyPath}",
            "symbol" = "python_udf_vector_ops.to_uppercase",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        );
        """
        
        log.info("=" * 80)
        log.info("STRESS TEST EXECUTION")
        log.info("=" * 80)
        
        // ==================== Stress Test 1: Full Table Scan with Scalar UDF ====================
        log.info("Test 1: Full table scan with scalar UDF (${TOTAL_ROWS} rows)")
        
        def test1Start = System.currentTimeMillis()
        def result1 = sql """
        SELECT COUNT(*), AVG(final_price)
        FROM (
            SELECT id, py_calc_final_price(price, discount) AS final_price
            FROM python_udf_stress_table
        ) t;
        """
        def test1End = System.currentTimeMillis()
        def test1Time = test1End - test1Start
        log.info("  Result: ${result1[0]}")
        log.info("  Time: ${test1Time} ms")
        log.info("  Throughput: ${String.format('%.2f', TOTAL_ROWS / (test1Time / 1000.0))} rows/sec")
        
        // ==================== Stress Test 2: Full Table Scan with Vector UDF ====================
        log.info("Test 2: Full table scan with vector UDF (${TOTAL_ROWS} rows)")
        
        def test2Start = System.currentTimeMillis()
        def result2 = sql """
        SELECT COUNT(*), AVG(result)
        FROM (
            SELECT id, py_vec_multiply(value1, 10) AS result
            FROM python_udf_stress_table
        ) t;
        """
        def test2End = System.currentTimeMillis()
        def test2Time = test2End - test2Start
        log.info("  Result: ${result2[0]}")
        log.info("  Time: ${test2Time} ms")
        log.info("  Throughput: ${String.format('%.2f', TOTAL_ROWS / (test2Time / 1000.0))} rows/sec")
        log.info("  Speedup vs Scalar: ${String.format('%.2f', test1Time / (test2Time * 1.0))}x")
        
        // ==================== Stress Test 3: String Processing with Vector UDF ====================
        log.info("Test 3: String processing with vector UDF (${TOTAL_ROWS} rows)")
        
        def test3Start = System.currentTimeMillis()
        def result3 = sql """
        SELECT COUNT(DISTINCT upper_name)
        FROM (
            SELECT py_vec_upper(name) AS upper_name
            FROM python_udf_stress_table
        ) t;
        """
        def test3End = System.currentTimeMillis()
        def test3Time = test3End - test3Start
        log.info("  Result: ${result3[0]}")
        log.info("  Time: ${test3Time} ms")
        log.info("  Throughput: ${String.format('%.2f', TOTAL_ROWS / (test3Time / 1000.0))} rows/sec")
        
        // ==================== Stress Test 4: Complex Aggregation ====================
        log.info("Test 4: Complex aggregation with multiple UDFs")
        
        def test4Start = System.currentTimeMillis()
        def result4 = sql """
        SELECT 
            category,
            COUNT(*) AS cnt,
            AVG(py_calc_final_price(price, discount)) AS avg_final_price,
            AVG(py_vec_multiply(value1, 10)) AS avg_multiplied
        FROM python_udf_stress_table
        GROUP BY category
        ORDER BY category
        LIMIT 20;
        """
        def test4End = System.currentTimeMillis()
        def test4Time = test4End - test4Start
        log.info("  Processed ${result4.size()} groups")
        log.info("  Time: ${test4Time} ms")
        
        // ==================== Stress Test 5: Join with UDF ====================
        log.info("Test 5: Self-join with UDF (limited to 1M rows)")
        
        def test5Start = System.currentTimeMillis()
        def result5 = sql """
        SELECT COUNT(*)
        FROM (
            SELECT 
                a.id,
                py_vec_multiply(a.value1, b.value2) AS result
            FROM python_udf_stress_table a
            JOIN python_udf_stress_table b ON a.category = b.category
            WHERE a.id < 100000 AND b.id < 100000
        ) t;
        """
        def test5End = System.currentTimeMillis()
        def test5Time = test5End - test5Start
        log.info("  Result: ${result5[0]}")
        log.info("  Time: ${test5Time} ms")
        
        // ==================== Stress Test 6: Concurrent UDF Calls ====================
        log.info("Test 6: Multiple UDFs in single query")
        
        def test6Start = System.currentTimeMillis()
        def result6 = sql """
        SELECT COUNT(*)
        FROM (
            SELECT 
                id,
                py_calc_final_price(price, discount) AS final_price,
                py_extract_domain(email) AS domain,
                py_vec_multiply(value1, 5) AS vec_result,
                py_vec_upper(name) AS upper_name
            FROM python_udf_stress_table
            LIMIT 500000
        ) t;
        """
        def test6End = System.currentTimeMillis()
        def test6Time = test6End - test6Start
        log.info("  Result: ${result6[0]}")
        log.info("  Time: ${test6Time} ms")
        log.info("  Throughput: ${String.format('%.2f', 500000 / (test6Time / 1000.0))} rows/sec")
        
        // ==================== Stress Test 7: Filter with UDF ====================
        log.info("Test 7: Filter with UDF predicate")
        
        def test7Start = System.currentTimeMillis()
        def result7 = sql """
        SELECT COUNT(*)
        FROM python_udf_stress_table
        WHERE py_vec_multiply(value1, 2) > 1000
        LIMIT 100000;
        """
        def test7End = System.currentTimeMillis()
        def test7Time = test7End - test7Start
        log.info("  Result: ${result7[0]}")
        log.info("  Time: ${test7Time} ms")
        
        // ==================== Final Summary ====================
        log.info("=" * 80 + "\nSTRESS TEST SUMMARY\n" + "=" * 80 + "\nDataset: ${TOTAL_ROWS} rows\n\n" +
            "Test Results:\n" +
            "  1. Scalar UDF full scan:        ${test1Time} ms (${String.format('%.2f', TOTAL_ROWS / (test1Time / 1000.0))} rows/sec)\n" +
            "  2. Vector UDF full scan:        ${test2Time} ms (${String.format('%.2f', TOTAL_ROWS / (test2Time / 1000.0))} rows/sec)\n" +
            "  3. Vector string processing:    ${test3Time} ms (${String.format('%.2f', TOTAL_ROWS / (test3Time / 1000.0))} rows/sec)\n" +
            "  4. Complex aggregation:         ${test4Time} ms\n" +
            "  5. Join with UDF:               ${test5Time} ms\n" +
            "  6. Multiple UDFs:               ${test6Time} ms (${String.format('%.2f', 500000 / (test6Time / 1000.0))} rows/sec)\n" +
            "  7. Filter with UDF:             ${test7Time} ms\n\n" +
            "Performance Metrics:\n" +
            "  Vector vs Scalar speedup:       ${String.format('%.2fx', test1Time / (test2Time * 1.0))}\n" +
            "  Total test time:                ${(test7End - test1Start) / 1000.0} seconds\n" +
            "=" * 80)
        
    } finally {
        // Cleanup
        log.info("Cleaning up stress test resources...")
        
        try_sql("DROP FUNCTION IF EXISTS py_calc_final_price(DOUBLE, DOUBLE);")
        try_sql("DROP FUNCTION IF EXISTS py_extract_domain(STRING);")
        try_sql("DROP FUNCTION IF EXISTS py_vec_multiply(INT, INT);")
        try_sql("DROP FUNCTION IF EXISTS py_vec_upper(STRING);")
        
        try_sql("DROP TABLE IF EXISTS python_udf_stress_table;")
        try_sql("DROP DATABASE IF EXISTS test_pythonudf_stress;")
        log.info("Stress test cleanup completed.")
    }
}
