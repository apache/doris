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

suite("test_pythonudf_module_scalar") {
    // Comprehensive test for scalar Python UDF using module mode
    
    def pyPath = """${context.file.parent}/udf_scripts/python_udf_scalar_ops.zip"""
    scp_udf_file_to_all_be(pyPath)
    def runtime_version = "3.8.10"
    
    log.info("Python module path: ${pyPath}".toString())
    
    try {
        // Create test table with diverse data types
        sql """ DROP TABLE IF EXISTS scalar_module_test_table; """
        sql """
        CREATE TABLE scalar_module_test_table (
            id INT,
            int_a INT,
            int_b INT,
            int_c INT,
            double_a DOUBLE,
            double_b DOUBLE,
            string_a STRING,
            string_b STRING,
            bool_a BOOLEAN,
            bool_b BOOLEAN,
            date_a DATE,
            date_b DATE
        ) ENGINE=OLAP 
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO scalar_module_test_table VALUES
        (1, 10, 20, 30, 100.0, 10.0, 'hello world', 'test@example.com', true, true, '2024-01-15', '2024-01-20'),
        (2, 5, 15, 25, 200.0, 20.0, 'foo bar baz', 'user@domain.com', false, true, '2024-02-10', '2024-03-15'),
        (3, 100, 50, 25, 150.0, 0.0, 'racecar', 'admin@test.org', true, false, '2023-12-01', '2024-01-01'),
        (4, 7, 3, 11, 80.0, 5.0, 'a man a plan a canal panama', 'info@company.net', false, false, '2024-06-15', '2024-06-15'),
        (5, 17, 19, 23, 300.0, 15.0, 'python udf test', 'contact@site.io', true, true, '2024-03-01', '2024-12-31');
        """
        
        // ==================== Numeric Operations Tests ====================
        
        // Test 1: Add three numbers
        sql """ DROP FUNCTION IF EXISTS py_add_three(INT, INT, INT); """
        sql """
        CREATE FUNCTION py_add_three(INT, INT, INT) 
        RETURNS INT 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "file" = "file://${pyPath}",
            "symbol" = "python_udf_scalar_ops.add_three_numbers",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        );
        """
        
        qt_add_three """ 
        SELECT 
            id,
            int_a, int_b, int_c,
            py_add_three(int_a, int_b, int_c) AS result
        FROM scalar_module_test_table
        ORDER BY id;
        """
        
        // Test 2: Safe division with precision
        sql """ DROP FUNCTION IF EXISTS py_safe_div(DOUBLE, DOUBLE, INT); """
        sql """
        CREATE FUNCTION py_safe_div(DOUBLE, DOUBLE, INT) 
        RETURNS DOUBLE 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "file" = "file://${pyPath}",
            "symbol" = "python_udf_scalar_ops.safe_divide_with_precision",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        );
        """
        
        qt_safe_div """ 
        SELECT 
            id,
            double_a, double_b,
            py_safe_div(double_a, double_b, 2) AS result
        FROM scalar_module_test_table
        ORDER BY id;
        """
        
        // Test 3: Calculate discount price
        sql """ DROP FUNCTION IF EXISTS py_discount(DOUBLE, DOUBLE); """
        sql """
        CREATE FUNCTION py_discount(DOUBLE, DOUBLE) 
        RETURNS DOUBLE 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "file" = "file://${pyPath}",
            "symbol" = "python_udf_scalar_ops.calculate_discount_price",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        );
        """
        
        qt_discount """ 
        SELECT 
            id,
            double_a,
            py_discount(double_a, 10.0) AS price_10_off,
            py_discount(double_a, 25.0) AS price_25_off
        FROM scalar_module_test_table
        ORDER BY id;
        """
        
        // Test 4: Compound interest
        sql """ DROP FUNCTION IF EXISTS py_compound_interest(DOUBLE, DOUBLE, INT); """
        sql """
        CREATE FUNCTION py_compound_interest(DOUBLE, DOUBLE, INT) 
        RETURNS DOUBLE 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "file" = "file://${pyPath}",
            "symbol" = "python_udf_scalar_ops.compound_interest",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        );
        """
        
        qt_compound_interest """ 
        SELECT 
            id,
            double_a,
            py_compound_interest(double_a, 5.0, 10) AS future_value
        FROM scalar_module_test_table
        ORDER BY id;
        """
        
        // Test 5: Calculate BMI
        sql """ DROP FUNCTION IF EXISTS py_bmi(DOUBLE, DOUBLE); """
        sql """
        CREATE FUNCTION py_bmi(DOUBLE, DOUBLE) 
        RETURNS DOUBLE 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "file" = "file://${pyPath}",
            "symbol" = "python_udf_scalar_ops.calculate_bmi",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        );
        """
        
        qt_bmi """ 
        SELECT 
            id,
            py_bmi(70.0, 1.75) AS bmi_normal,
            py_bmi(90.0, 1.75) AS bmi_overweight
        FROM scalar_module_test_table
        ORDER BY id;
        """
        
        // Test 6: Fibonacci number
        sql """ DROP FUNCTION IF EXISTS py_fibonacci(INT); """
        sql """
        CREATE FUNCTION py_fibonacci(INT) 
        RETURNS INT 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "file" = "file://${pyPath}",
            "symbol" = "python_udf_scalar_ops.fibonacci",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        );
        """
        
        qt_fibonacci """ 
        SELECT 
            id,
            int_a,
            py_fibonacci(int_a) AS fib_result
        FROM scalar_module_test_table
        WHERE int_a <= 20
        ORDER BY id;
        """
        
        // Test 7: Is prime number
        sql """ DROP FUNCTION IF EXISTS py_is_prime(INT); """
        sql """
        CREATE FUNCTION py_is_prime(INT) 
        RETURNS BOOLEAN 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "file" = "file://${pyPath}",
            "symbol" = "python_udf_scalar_ops.is_prime",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        );
        """
        
        qt_is_prime """ 
        SELECT 
            id,
            int_a, int_b, int_c,
            py_is_prime(int_a) AS a_is_prime,
            py_is_prime(int_b) AS b_is_prime,
            py_is_prime(int_c) AS c_is_prime
        FROM scalar_module_test_table
        ORDER BY id;
        """
        
        // Test 8: GCD (Greatest Common Divisor)
        sql """ DROP FUNCTION IF EXISTS py_gcd(INT, INT); """
        sql """
        CREATE FUNCTION py_gcd(INT, INT) 
        RETURNS INT 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "file" = "file://${pyPath}",
            "symbol" = "python_udf_scalar_ops.gcd",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        );
        """
        
        qt_gcd """ 
        SELECT 
            id,
            int_a, int_b,
            py_gcd(int_a, int_b) AS gcd_result
        FROM scalar_module_test_table
        ORDER BY id;
        """
        
        // Test 9: LCM (Least Common Multiple)
        sql """ DROP FUNCTION IF EXISTS py_lcm(INT, INT); """
        sql """
        CREATE FUNCTION py_lcm(INT, INT) 
        RETURNS INT 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "file" = "file://${pyPath}",
            "symbol" = "python_udf_scalar_ops.lcm",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        );
        """
        
        qt_lcm """ 
        SELECT 
            id,
            int_a, int_b,
            py_lcm(int_a, int_b) AS lcm_result
        FROM scalar_module_test_table
        ORDER BY id;
        """
        
        // ==================== String Operations Tests ====================
        
        // Test 10: Reverse string
        sql """ DROP FUNCTION IF EXISTS py_reverse(STRING); """
        sql """
        CREATE FUNCTION py_reverse(STRING) 
        RETURNS STRING 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "file" = "file://${pyPath}",
            "symbol" = "python_udf_scalar_ops.reverse_string",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        );
        """
        
        qt_reverse """ 
        SELECT 
            id,
            string_a,
            py_reverse(string_a) AS reversed
        FROM scalar_module_test_table
        ORDER BY id;
        """
        
        // Test 11: Count vowels
        sql """ DROP FUNCTION IF EXISTS py_count_vowels(STRING); """
        sql """
        CREATE FUNCTION py_count_vowels(STRING) 
        RETURNS INT 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "file" = "file://${pyPath}",
            "symbol" = "python_udf_scalar_ops.count_vowels",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        );
        """
        
        qt_count_vowels """ 
        SELECT 
            id,
            string_a,
            py_count_vowels(string_a) AS vowel_count
        FROM scalar_module_test_table
        ORDER BY id;
        """
        
        // Test 12: Count words
        sql """ DROP FUNCTION IF EXISTS py_count_words(STRING); """
        sql """
        CREATE FUNCTION py_count_words(STRING) 
        RETURNS INT 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "file" = "file://${pyPath}",
            "symbol" = "python_udf_scalar_ops.count_words",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        );
        """
        
        qt_count_words """ 
        SELECT 
            id,
            string_a,
            py_count_words(string_a) AS word_count
        FROM scalar_module_test_table
        ORDER BY id;
        """
        
        // Test 13: Capitalize words
        sql """ DROP FUNCTION IF EXISTS py_capitalize(STRING); """
        sql """
        CREATE FUNCTION py_capitalize(STRING) 
        RETURNS STRING 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "file" = "file://${pyPath}",
            "symbol" = "python_udf_scalar_ops.capitalize_words",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        );
        """
        
        qt_capitalize """ 
        SELECT 
            id,
            string_a,
            py_capitalize(string_a) AS capitalized
        FROM scalar_module_test_table
        ORDER BY id;
        """
        
        // Test 14: Is palindrome
        sql """ DROP FUNCTION IF EXISTS py_is_palindrome(STRING); """
        sql """
        CREATE FUNCTION py_is_palindrome(STRING) 
        RETURNS BOOLEAN 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "file" = "file://${pyPath}",
            "symbol" = "python_udf_scalar_ops.is_palindrome",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        );
        """
        
        qt_is_palindrome """ 
        SELECT 
            id,
            string_a,
            py_is_palindrome(string_a) AS is_palindrome
        FROM scalar_module_test_table
        ORDER BY id;
        """
        
        // Test 15: String similarity
        sql """ DROP FUNCTION IF EXISTS py_similarity(STRING, STRING); """
        sql """
        CREATE FUNCTION py_similarity(STRING, STRING) 
        RETURNS DOUBLE 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "file" = "file://${pyPath}",
            "symbol" = "python_udf_scalar_ops.string_similarity",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        );
        """
        
        qt_similarity """ 
        SELECT 
            id,
            string_a,
            py_similarity(string_a, 'hello') AS similarity_to_hello
        FROM scalar_module_test_table
        ORDER BY id;
        """
        
        // Test 16: Mask email
        sql """ DROP FUNCTION IF EXISTS py_mask_email(STRING); """
        sql """
        CREATE FUNCTION py_mask_email(STRING) 
        RETURNS STRING 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "file" = "file://${pyPath}",
            "symbol" = "python_udf_scalar_ops.mask_email",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        );
        """
        
        qt_mask_email """ 
        SELECT 
            id,
            string_b,
            py_mask_email(string_b) AS masked_email
        FROM scalar_module_test_table
        ORDER BY id;
        """
        
        // Test 17: Extract domain from email
        sql """ DROP FUNCTION IF EXISTS py_extract_domain(STRING); """
        sql """
        CREATE FUNCTION py_extract_domain(STRING) 
        RETURNS STRING 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "file" = "file://${pyPath}",
            "symbol" = "python_udf_scalar_ops.extract_domain",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        );
        """
        
        qt_extract_domain """ 
        SELECT 
            id,
            string_b,
            py_extract_domain(string_b) AS domain
        FROM scalar_module_test_table
        ORDER BY id;
        """
        
        // Test 18: Levenshtein distance
        sql """ DROP FUNCTION IF EXISTS py_levenshtein(STRING, STRING); """
        sql """
        CREATE FUNCTION py_levenshtein(STRING, STRING) 
        RETURNS INT 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "file" = "file://${pyPath}",
            "symbol" = "python_udf_scalar_ops.levenshtein_distance",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        );
        """
        
        qt_levenshtein """ 
        SELECT 
            id,
            string_a,
            py_levenshtein(string_a, 'hello world') AS edit_distance
        FROM scalar_module_test_table
        ORDER BY id;
        """
        
        // ==================== Date/Time Operations Tests ====================
        
        // Test 19: Days between dates
        sql """ DROP FUNCTION IF EXISTS py_days_between(DATE, DATE); """
        sql """
        CREATE FUNCTION py_days_between(DATE, DATE) 
        RETURNS INT 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "file" = "file://${pyPath}",
            "symbol" = "python_udf_scalar_ops.days_between_dates",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        );
        """
        
        qt_days_between """ 
        SELECT 
            id,
            date_a, date_b,
            py_days_between(date_a, date_b) AS days_diff
        FROM scalar_module_test_table
        ORDER BY id;
        """
        
        // Test 20: Is weekend
        sql """ DROP FUNCTION IF EXISTS py_is_weekend(DATE); """
        sql """
        CREATE FUNCTION py_is_weekend(DATE) 
        RETURNS BOOLEAN 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "file" = "file://${pyPath}",
            "symbol" = "python_udf_scalar_ops.is_weekend",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        );
        """
        
        qt_is_weekend """ 
        SELECT 
            id,
            date_a,
            py_is_weekend(date_a) AS is_weekend
        FROM scalar_module_test_table
        ORDER BY id;
        """
        
        // Test 21: Get quarter
        sql """ DROP FUNCTION IF EXISTS py_get_quarter(DATE); """
        sql """
        CREATE FUNCTION py_get_quarter(DATE) 
        RETURNS INT 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "file" = "file://${pyPath}",
            "symbol" = "python_udf_scalar_ops.get_quarter",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        );
        """
        
        qt_get_quarter """ 
        SELECT 
            id,
            date_a,
            py_get_quarter(date_a) AS quarter
        FROM scalar_module_test_table
        ORDER BY id;
        """
        
        // Test 22: Age in years
        sql """ DROP FUNCTION IF EXISTS py_age(DATE, DATE); """
        sql """
        CREATE FUNCTION py_age(DATE, DATE) 
        RETURNS INT 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "file" = "file://${pyPath}",
            "symbol" = "python_udf_scalar_ops.age_in_years",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        );
        """
        
        qt_age """ 
        SELECT 
            id,
            py_age('1990-01-01', date_a) AS age
        FROM scalar_module_test_table
        ORDER BY id;
        """
        
        // ==================== Boolean/Conditional Operations Tests ====================
        
        // Test 23: Is in range
        sql """ DROP FUNCTION IF EXISTS py_in_range(INT, INT, INT); """
        sql """
        CREATE FUNCTION py_in_range(INT, INT, INT) 
        RETURNS BOOLEAN 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "file" = "file://${pyPath}",
            "symbol" = "python_udf_scalar_ops.is_in_range",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        );
        """
        
        qt_in_range """ 
        SELECT 
            id,
            int_a,
            py_in_range(int_a, 10, 50) AS in_range_10_50
        FROM scalar_module_test_table
        ORDER BY id;
        """
        
        // Test 24: XOR operation
        sql """ DROP FUNCTION IF EXISTS py_xor(BOOLEAN, BOOLEAN); """
        sql """
        CREATE FUNCTION py_xor(BOOLEAN, BOOLEAN) 
        RETURNS BOOLEAN 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "file" = "file://${pyPath}",
            "symbol" = "python_udf_scalar_ops.xor_operation",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        );
        """
        
        qt_xor """ 
        SELECT 
            id,
            bool_a, bool_b,
            py_xor(bool_a, bool_b) AS xor_result
        FROM scalar_module_test_table
        ORDER BY id;
        """
        
        // ==================== Complex/Mixed Operations Tests ====================
        
        // Test 25: Calculate grade
        sql """ DROP FUNCTION IF EXISTS py_grade(DOUBLE); """
        sql """
        CREATE FUNCTION py_grade(DOUBLE) 
        RETURNS STRING 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "file" = "file://${pyPath}",
            "symbol" = "python_udf_scalar_ops.calculate_grade",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        );
        """
        
        qt_grade """ 
        SELECT 
            id,
            double_a,
            py_grade(double_a) AS grade
        FROM scalar_module_test_table
        ORDER BY id;
        """
        
        // Test 26: Categorize age
        sql """ DROP FUNCTION IF EXISTS py_categorize_age(INT); """
        sql """
        CREATE FUNCTION py_categorize_age(INT) 
        RETURNS STRING 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "file" = "file://${pyPath}",
            "symbol" = "python_udf_scalar_ops.categorize_age",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        );
        """
        
        qt_categorize_age """ 
        SELECT 
            id,
            int_a,
            py_categorize_age(int_a) AS age_category
        FROM scalar_module_test_table
        ORDER BY id;
        """
        
        // Test 27: Calculate tax
        sql """ DROP FUNCTION IF EXISTS py_tax(DOUBLE, DOUBLE); """
        sql """
        CREATE FUNCTION py_tax(DOUBLE, DOUBLE) 
        RETURNS DOUBLE 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "file" = "file://${pyPath}",
            "symbol" = "python_udf_scalar_ops.calculate_tax",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        );
        """
        
        qt_tax """ 
        SELECT 
            id,
            double_a,
            py_tax(double_a, 15.0) AS tax_15_percent
        FROM scalar_module_test_table
        ORDER BY id;
        """
        
        // Test 28: Truncate string with suffix
        sql """ DROP FUNCTION IF EXISTS py_truncate(STRING, INT, STRING); """
        sql """
        CREATE FUNCTION py_truncate(STRING, INT, STRING) 
        RETURNS STRING 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "file" = "file://${pyPath}",
            "symbol" = "python_udf_scalar_ops.truncate_string",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        );
        """
        
        qt_truncate """ 
        SELECT 
            id,
            string_a,
            py_truncate(string_a, 10, '...') AS truncated
        FROM scalar_module_test_table
        ORDER BY id;
        """
        
        // ==================== Edge Cases and NULL Handling Tests ====================
        
        // Test 29: NULL handling in numeric operations
        sql """ DROP TABLE IF EXISTS null_test_table; """
        sql """
        CREATE TABLE null_test_table (
            id INT,
            val1 INT,
            val2 INT,
            val3 INT
        ) ENGINE=OLAP 
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO null_test_table VALUES
        (1, 10, 20, 30),
        (2, NULL, 20, 30),
        (3, 10, NULL, 30),
        (4, 10, 20, NULL),
        (5, NULL, NULL, NULL);
        """
        
        qt_null_handling """ 
        SELECT 
            id,
            val1, val2, val3,
            py_add_three(val1, val2, val3) AS sum_result
        FROM null_test_table
        ORDER BY id;
        """
        
        // Test 30: Empty string handling
        sql """ DROP TABLE IF EXISTS string_edge_test; """
        sql """
        CREATE TABLE string_edge_test (
            id INT,
            str_val STRING
        ) ENGINE=OLAP 
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO string_edge_test VALUES
        (1, 'normal string'),
        (2, ''),
        (3, '   '),
        (4, 'a'),
        (5, NULL);
        """
        
        qt_string_edge """ 
        SELECT 
            id,
            str_val,
            py_reverse(str_val) AS reversed,
            py_count_vowels(str_val) AS vowels,
            py_count_words(str_val) AS words
        FROM string_edge_test
        ORDER BY id;
        """
        
    } finally {
        // Cleanup all functions
        try_sql("DROP FUNCTION IF EXISTS py_add_three(INT, INT, INT);")
        try_sql("DROP FUNCTION IF EXISTS py_safe_div(DOUBLE, DOUBLE, INT);")
        try_sql("DROP FUNCTION IF EXISTS py_discount(DOUBLE, DOUBLE);")
        try_sql("DROP FUNCTION IF EXISTS py_compound_interest(DOUBLE, DOUBLE, INT);")
        try_sql("DROP FUNCTION IF EXISTS py_bmi(DOUBLE, DOUBLE);")
        try_sql("DROP FUNCTION IF EXISTS py_fibonacci(INT);")
        try_sql("DROP FUNCTION IF EXISTS py_is_prime(INT);")
        try_sql("DROP FUNCTION IF EXISTS py_gcd(INT, INT);")
        try_sql("DROP FUNCTION IF EXISTS py_lcm(INT, INT);")
        try_sql("DROP FUNCTION IF EXISTS py_reverse(STRING);")
        try_sql("DROP FUNCTION IF EXISTS py_count_vowels(STRING);")
        try_sql("DROP FUNCTION IF EXISTS py_count_words(STRING);")
        try_sql("DROP FUNCTION IF EXISTS py_capitalize(STRING);")
        try_sql("DROP FUNCTION IF EXISTS py_is_palindrome(STRING);")
        try_sql("DROP FUNCTION IF EXISTS py_similarity(STRING, STRING);")
        try_sql("DROP FUNCTION IF EXISTS py_mask_email(STRING);")
        try_sql("DROP FUNCTION IF EXISTS py_extract_domain(STRING);")
        try_sql("DROP FUNCTION IF EXISTS py_levenshtein(STRING, STRING);")
        try_sql("DROP FUNCTION IF EXISTS py_days_between(DATE, DATE);")
        try_sql("DROP FUNCTION IF EXISTS py_is_weekend(DATE);")
        try_sql("DROP FUNCTION IF EXISTS py_get_quarter(DATE);")
        try_sql("DROP FUNCTION IF EXISTS py_age(DATE, DATE);")
        try_sql("DROP FUNCTION IF EXISTS py_in_range(INT, INT, INT);")
        try_sql("DROP FUNCTION IF EXISTS py_xor(BOOLEAN, BOOLEAN);")
        try_sql("DROP FUNCTION IF EXISTS py_grade(DOUBLE);")
        try_sql("DROP FUNCTION IF EXISTS py_categorize_age(INT);")
        try_sql("DROP FUNCTION IF EXISTS py_tax(DOUBLE, DOUBLE);")
        try_sql("DROP FUNCTION IF EXISTS py_truncate(STRING, INT, STRING);")
        
        // Cleanup tables
        try_sql("DROP TABLE IF EXISTS scalar_module_test_table;")
        try_sql("DROP TABLE IF EXISTS null_test_table;")
        try_sql("DROP TABLE IF EXISTS string_edge_test;")
    }
}
