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

suite("test_pythonudf_benchmark") {
    // Benchmark test for Python UDF with large dataset

    return
    
    def scalarPyPath = """${context.file.parent}/udf_scripts/python_udf_scalar_ops.zip"""
    def vectorPyPath = """${context.file.parent}/udf_scripts/python_udf_vector_ops.zip"""
    scp_udf_file_to_all_be(scalarPyPath)
    scp_udf_file_to_all_be(vectorPyPath)
    def runtime_version = "3.10.12"

    sql "CREATE DATABASE IF NOT EXISTS test_pythonudf_benchmark"
    sql "USE test_pythonudf_benchmark"
    
    log.info("Python scalar module path: ${scalarPyPath}".toString())
    log.info("Python vector module path: ${vectorPyPath}".toString())
    
    try {
        // ==================== Create Large Test Table ====================
        sql """ DROP TABLE IF EXISTS python_udf_benchmark_table; """
        sql """
        CREATE TABLE python_udf_benchmark_table (
            id BIGINT,
            int_val INT,
            double_val DOUBLE,
            string_val STRING,
            email STRING,
            bool_val BOOLEAN,
            date_val DATE
        ) ENGINE=OLAP 
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 10
        PROPERTIES("replication_num" = "1");
        """
        
        log.info("Creating benchmark table with large dataset...")
        
        // Load 1 million rows using streamLoad (much faster)
        def totalRows = 1000000
        
        log.info("Loading ${totalRows} rows using streamLoad from CSV file...")
        def loadStartTime = System.currentTimeMillis()
        
        streamLoad {
            db 'test_pythonudf_benchmark'
            table "python_udf_benchmark_table"
            set 'column_separator', '\t'
            file 'benchmark_data_1m.csv'
            time 120000 // 120 seconds timeout
            
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
        log.info("Data loaded in ${loadEndTime - loadStartTime} ms (${String.format('%.2f', totalRows / ((loadEndTime - loadStartTime) / 1000.0))} rows/sec)")
        
        sql "sync"
        
        // Verify row count
        def rowCount = sql "SELECT COUNT(*) FROM python_udf_benchmark_table"
        log.info("Verified row count: ${rowCount[0][0]}")
        
        // ==================== Benchmark 1: Simple Scalar UDF ====================
        log.info("=== Benchmark 1: Simple Scalar UDF (multiply_with_default) ===")
        
        sql """ DROP FUNCTION IF EXISTS py_multiply(INT, INT, INT); """
        sql """
        CREATE FUNCTION py_multiply(INT, INT, INT) 
        RETURNS INT 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "file" = "file://${scalarPyPath}",
            "symbol" = "python_udf_scalar_ops.multiply_with_default",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        );
        """
        
        def startTime1 = System.currentTimeMillis()
        sql """
        SELECT COUNT(*) 
        FROM (
            SELECT id, py_multiply(int_val, 2, 1) AS result
            FROM python_udf_benchmark_table
        ) t;
        """
        def endTime1 = System.currentTimeMillis()
        log.info("Scalar UDF (simple): ${endTime1 - startTime1} ms for ${totalRows} rows")
        
        // ==================== Benchmark 2: Complex Scalar UDF ====================
        log.info("=== Benchmark 2: Complex Scalar UDF (Levenshtein distance) ===")
        
        sql """ DROP FUNCTION IF EXISTS py_levenshtein(STRING, STRING); """
        sql """
        CREATE FUNCTION py_levenshtein(STRING, STRING) 
        RETURNS INT 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "file" = "file://${scalarPyPath}",
            "symbol" = "python_udf_scalar_ops.levenshtein_distance",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        );
        """
        
        def startTime2 = System.currentTimeMillis()
        sql """
        SELECT COUNT(*) 
        FROM (
            SELECT id, py_levenshtein(string_val, 'test_string_50') AS distance
            FROM python_udf_benchmark_table
            LIMIT 100000
        ) t;
        """
        def endTime2 = System.currentTimeMillis()
        log.info("Scalar UDF (complex): ${endTime2 - startTime2} ms for 100000 rows")
        
        // ==================== Benchmark 3: String Processing Scalar UDF ====================
        log.info("=== Benchmark 3: String Processing Scalar UDF (extract_domain) ===")
        
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
        
        def startTime3 = System.currentTimeMillis()
        sql """
        SELECT COUNT(*) 
        FROM (
            SELECT id, py_extract_domain(email) AS domain
            FROM python_udf_benchmark_table
        ) t;
        """
        def endTime3 = System.currentTimeMillis()
        log.info("Scalar UDF (string): ${endTime3 - startTime3} ms for ${totalRows} rows")
        
        // ==================== Benchmark 4: Simple Vector UDF ====================
        log.info("=== Benchmark 4: Simple Vector UDF (add_constant) ===")
        
        sql """ DROP FUNCTION IF EXISTS py_vec_add(INT, INT); """
        sql """
        CREATE FUNCTION py_vec_add(INT, INT) 
        RETURNS INT 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "file" = "file://${vectorPyPath}",
            "symbol" = "python_udf_vector_ops.add_constant",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true",
            "vectorized" = "true"
        );
        """
        
        def startTime4 = System.currentTimeMillis()
        sql """
        SELECT COUNT(*) 
        FROM (
            SELECT id, py_vec_add(int_val, 100) AS result
            FROM python_udf_benchmark_table
        ) t;
        """
        def endTime4 = System.currentTimeMillis()
        log.info("Vector UDF (simple): ${endTime4 - startTime4} ms for ${totalRows} rows")
        
        // ==================== Benchmark 5: Complex Vector UDF ====================
        log.info("=== Benchmark 5: Complex Vector UDF (string_length) ===")
        
        sql """ DROP FUNCTION IF EXISTS py_vec_strlen(STRING); """
        sql """
        CREATE FUNCTION py_vec_strlen(STRING) 
        RETURNS INT 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "file" = "file://${vectorPyPath}",
            "symbol" = "python_udf_vector_ops.string_length",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true",
            "vectorized" = "true"
        );
        """
        
        def startTime5 = System.currentTimeMillis()
        sql """
        SELECT COUNT(*) 
        FROM (
            SELECT id, py_vec_strlen(string_val) AS len
            FROM python_udf_benchmark_table
        ) t;
        """
        def endTime5 = System.currentTimeMillis()
        log.info("Vector UDF (string): ${endTime5 - startTime5} ms for ${totalRows} rows")
        
        // ==================== Benchmark 6: Scalar UDF with Grouping ====================
        log.info("=== Benchmark 6: Scalar UDF with Grouping ===")
        
        def startTime6 = System.currentTimeMillis()
        sql """
        SELECT 
            int_val % 100 AS bucket,
            COUNT(*) AS cnt,
            SUM(int_val) AS total
        FROM python_udf_benchmark_table
        WHERE py_multiply(int_val, 2, 1) > 1000
        GROUP BY int_val % 100
        ORDER BY bucket
        LIMIT 10;
        """
        def endTime6 = System.currentTimeMillis()
        log.info("Scalar UDF with Grouping (WHERE clause): ${endTime6 - startTime6} ms")
        
        // ==================== Benchmark 7: Vector UDF with Grouping ====================
        log.info("=== Benchmark 7: Vector UDF with Grouping ===")
        
        def startTime7 = System.currentTimeMillis()
        sql """
        SELECT 
            int_val % 100 AS bucket,
            COUNT(*) AS cnt,
            SUM(int_val) AS total
        FROM python_udf_benchmark_table
        WHERE py_vec_add(int_val, 100) > 1000
        GROUP BY int_val % 100
        ORDER BY bucket
        LIMIT 10;
        """
        def endTime7 = System.currentTimeMillis()
        log.info("Vector UDF with Grouping (WHERE clause): ${endTime7 - startTime7} ms")
        
        // ==================== Benchmark 8: Multiple UDFs in Single Query ====================
        log.info("=== Benchmark 8: Multiple UDFs in Single Query ===")
        
        def startTime8 = System.currentTimeMillis()
        sql """
        SELECT COUNT(*) 
        FROM (
            SELECT 
                id,
                py_multiply(int_val, 2, 1) AS mul_result,
                py_extract_domain(email) AS domain,
                py_vec_add(int_val, 100) AS vec_result
            FROM python_udf_benchmark_table
            LIMIT 100000
        ) t;
        """
        def endTime8 = System.currentTimeMillis()
        log.info("Multiple UDFs: ${endTime8 - startTime8} ms for 100000 rows")
        
        // ==================== Benchmark 9: Filter with UDF ====================
        log.info("=== Benchmark 9: Filter with UDF ===")
        
        sql """ DROP FUNCTION IF EXISTS py_is_prime(INT); """
        sql """
        CREATE FUNCTION py_is_prime(INT) 
        RETURNS BOOLEAN 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "file" = "file://${scalarPyPath}",
            "symbol" = "python_udf_scalar_ops.is_prime",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        );
        """
        
        def startTime9 = System.currentTimeMillis()
        sql """
        SELECT COUNT(*) 
        FROM python_udf_benchmark_table
        WHERE py_is_prime(int_val) = true
        LIMIT 10000;
        """
        def endTime9 = System.currentTimeMillis()
        log.info("Filter with UDF: ${endTime9 - startTime9} ms")
        
        // ==================== Benchmark Summary ====================
        log.info("\n" + "=" * 80 + "\nBENCHMARK SUMMARY\n" + "=" * 80 + "\nDataset size: ${totalRows} rows\n" +
            "1. Scalar UDF (simple multiply):        ${endTime1 - startTime1} ms\n" +
            "2. Scalar UDF (complex Levenshtein):    ${endTime2 - startTime2} ms (100K rows)\n" +
            "3. Scalar UDF (string extract_domain):  ${endTime3 - startTime3} ms\n" +
            "4. Vector UDF (simple add):             ${endTime4 - startTime4} ms\n" +
            "5. Vector UDF (string length):          ${endTime5 - startTime5} ms\n" +
            "6. Aggregation with Scalar UDF:         ${endTime6 - startTime6} ms\n" +
            "7. Aggregation with Vector UDF:         ${endTime7 - startTime7} ms\n" +
            "8. Multiple UDFs in query:              ${endTime8 - startTime8} ms (100K rows)\n" +
            "9. Filter with UDF:                     ${endTime9 - startTime9} ms\n" +
            "=" * 80)
        
        // Calculate throughput
        def throughput1 = totalRows / ((endTime1 - startTime1) / 1000.0)
        def throughput4 = totalRows / ((endTime4 - startTime4) / 1000.0)
        log.info("Scalar UDF throughput: ${String.format('%.2f', throughput1)} rows/sec")
        log.info("Vector UDF throughput: ${String.format('%.2f', throughput4)} rows/sec")
        log.info("Vector speedup: ${String.format('%.2f', (endTime1 - startTime1) / (endTime4 - startTime4))}x")
        
    } finally {
        // Cleanup
        log.info("Cleaning up benchmark resources...")
        
        try_sql("DROP FUNCTION IF EXISTS py_multiply(INT, INT, INT);")
        try_sql("DROP FUNCTION IF EXISTS py_levenshtein(STRING, STRING);")
        try_sql("DROP FUNCTION IF EXISTS py_extract_domain(STRING);")
        try_sql("DROP FUNCTION IF EXISTS py_vec_add(INT, INT);")
        try_sql("DROP FUNCTION IF EXISTS py_vec_strlen(STRING);")
        try_sql("DROP FUNCTION IF EXISTS py_is_prime(INT);")
        
        try_sql("DROP TABLE IF EXISTS python_udf_benchmark_table;")
        try_sql("DROP DATABASE IF EXISTS test_pythonudf_benchmark;")
        log.info("Benchmark cleanup completed.")
    }
}
