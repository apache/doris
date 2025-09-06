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

suite("test_pythonudf_performance_comparison") {
    // Quick performance comparison: Scalar vs Vector Python UDF
    // Lightweight test for quick performance checks
    
    def scalarPyPath = """${context.file.parent}/udf_scripts/python_udf_scalar_ops.zip"""
    def vectorPyPath = """${context.file.parent}/udf_scripts/python_udf_vector_ops.zip"""
    scp_udf_file_to_all_be(scalarPyPath)
    scp_udf_file_to_all_be(vectorPyPath)
    def runtime_version = "3.10.12"

    sql "CREATE DATABASE IF NOT EXISTS test_pythonudf_performance_comparison"
    sql "USE test_pythonudf_performance_comparison"
    
    // Quick test with smaller dataset
    def TEST_ROWS = 100000  // 100K rows for quick testing
    
    log.info("=" * 80)
    log.info("PYTHON UDF PERFORMANCE COMPARISON")
    log.info("Quick test with ${TEST_ROWS} rows")
    log.info("=" * 80)
    
    try {
        // Create test table
        sql """ DROP TABLE IF EXISTS perf_comparison_table; """
        sql """
        CREATE TABLE perf_comparison_table (
            id INT,
            val1 INT,
            val2 INT,
            price DOUBLE,
            discount DOUBLE,
            text STRING
        ) ENGINE=OLAP 
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 10
        PROPERTIES("replication_num" = "1");
        """
        
        // Load test data using streamLoad from CSV file
        log.info("Loading ${TEST_ROWS} rows using streamLoad from CSV file...")
        def loadStartTime = System.currentTimeMillis()
        
        streamLoad {
            db 'test_pythonudf_performance_comparison'
            table "perf_comparison_table"
            
            // Set column separator to tab
            set 'column_separator', '\t'
            
            // File path relative to regression-test/data/pythonudf_p0/
            file 'benchmark_data_100k.csv'
            
            time 60000 // 60 seconds timeout
            
            // Custom check callback
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
        log.info("Data loaded in ${loadEndTime - loadStartTime} ms")
        
        sql "sync"
        
        def actualRows = sql "SELECT COUNT(*) FROM perf_comparison_table"
        log.info("Verified row count: ${actualRows[0][0]}\nData ready. Starting performance tests...\n")
        
        // Define test cases
        def testCases = [
            [
                name: "Integer Multiplication",
                scalar_symbol: "python_udf_scalar_ops.multiply_with_default",
                vector_symbol: "python_udf_vector_ops.multiply_by_constant",
                params: "(INT, INT, INT)",
                returns: "INT",
                query: "SELECT COUNT(*) FROM (SELECT id, {UDF}(val1, 10, 1) AS result FROM perf_comparison_table) t"
            ],
            [
                name: "Price Calculation",
                scalar_symbol: "python_udf_scalar_ops.calculate_discount_price",
                vector_symbol: "python_udf_vector_ops.calculate_discount",
                params: "(DOUBLE, DOUBLE)",
                returns: "DOUBLE",
                query: "SELECT COUNT(*) FROM (SELECT id, {UDF}(price, discount) AS result FROM perf_comparison_table) t"
            ],
            [
                name: "String Length",
                scalar_symbol: "python_udf_scalar_ops.string_length_custom",
                vector_symbol: "python_udf_vector_ops.string_length",
                params: "(STRING)",
                returns: "INT",
                query: "SELECT COUNT(*) FROM (SELECT id, {UDF}(text) AS result FROM perf_comparison_table) t"
            ]
        ]
        
        def results = []
        
        testCases.each { testCase ->
            log.info("-" * 80)
            log.info("Test: ${testCase.name}")
            log.info("-" * 80)
            
            // Test Scalar UDF
            sql """ DROP FUNCTION IF EXISTS py_scalar_test${testCase.params}; """
            sql """
            CREATE FUNCTION py_scalar_test${testCase.params}
            RETURNS ${testCase.returns}
            PROPERTIES (
                "type" = "PYTHON_UDF",
                "file" = "file://${scalarPyPath}",
                "symbol" = "${testCase.scalar_symbol}",
                "runtime_version" = "${runtime_version}",
                "always_nullable" = "true"
            );
            """
            
            def scalarQuery = testCase.query.replace("{UDF}", "py_scalar_test")
            
            // Warm up
            sql scalarQuery
            
            // Actual test - run 3 times and take average
            def scalarTimes = []
            for (int i = 0; i < 3; i++) {
                def start = System.currentTimeMillis()
                sql scalarQuery
                def end = System.currentTimeMillis()
                scalarTimes.add(end - start)
            }
            def scalarAvg = scalarTimes.sum() / scalarTimes.size()
            
            log.info("  Scalar UDF: ${scalarTimes} ms, Avg: ${scalarAvg} ms")
            
            // Test Vector UDF
            sql """ DROP FUNCTION IF EXISTS py_vector_test${testCase.params}; """
            sql """
            CREATE FUNCTION py_vector_test${testCase.params}
            RETURNS ${testCase.returns}
            PROPERTIES (
                "type" = "PYTHON_UDF",
                "file" = "file://${vectorPyPath}",
                "symbol" = "${testCase.vector_symbol}",
                "runtime_version" = "${runtime_version}",
                "always_nullable" = "true",
                "vectorized" = "true"
            );
            """
            
            def vectorQuery = testCase.query.replace("{UDF}", "py_vector_test")
            
            // Warm up
            sql vectorQuery
            
            // Actual test - run 3 times and take average
            def vectorTimes = []
            for (int i = 0; i < 3; i++) {
                def start = System.currentTimeMillis()
                sql vectorQuery
                def end = System.currentTimeMillis()
                vectorTimes.add(end - start)
            }
            def vectorAvg = vectorTimes.sum() / vectorTimes.size()
            
            log.info("  Vector UDF: ${vectorTimes} ms, Avg: ${vectorAvg} ms")
            
            def speedup = scalarAvg / vectorAvg
            def improvement = ((scalarAvg - vectorAvg) / scalarAvg * 100)
            
            log.info("  Speedup: ${String.format('%.2f', speedup)}x")
            log.info("  Improvement: ${String.format('%.1f', improvement)}%")
            
            results.add([
                name: testCase.name,
                scalar: scalarAvg,
                vector: vectorAvg,
                speedup: speedup,
                improvement: improvement
            ])
            
            // Cleanup
            sql """ DROP FUNCTION IF EXISTS py_scalar_test${testCase.params}; """
            sql """ DROP FUNCTION IF EXISTS py_vector_test${testCase.params}; """
        }
        
        // Print summary
        def summary = new StringBuilder()
        summary.append("\n" + "=" * 80 + "\n")
        summary.append("PERFORMANCE COMPARISON SUMMARY\n")
        summary.append("=" * 80 + "\n")
        summary.append(String.format("%-30s %12s %12s %10s %12s", "Test Case", "Scalar(ms)", "Vector(ms)", "Speedup", "Improvement") + "\n")
        summary.append("-" * 80 + "\n")
        
        results.each { r ->
            summary.append(String.format("%-30s %12.1f %12.1f %9.2fx %11.1f%%", 
                r.name, r.scalar, r.vector, r.speedup, r.improvement) + "\n")
        }
        
        def avgSpeedup = results.collect { it.speedup }.sum() / results.size()
        def avgImprovement = results.collect { it.improvement }.sum() / results.size()
        
        summary.append("-" * 80 + "\n")
        summary.append(String.format("%-30s %12s %12s %9.2fx %11.1f%%", 
            "AVERAGE", "-", "-", avgSpeedup, avgImprovement) + "\n")
        summary.append("=" * 80)
        
        log.info(summary.toString())
        
    } finally {
        // Cleanup
        try_sql("DROP TABLE IF EXISTS perf_comparison_table;")
        try_sql("DROP DATABASE IF EXISTS test_pythonudf_performance_comparison;")
        log.info("\nPerformance comparison completed.")
    }
}
