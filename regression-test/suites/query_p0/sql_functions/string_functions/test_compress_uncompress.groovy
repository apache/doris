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

suite("test_compress_uncompress") {
    // Drop the existing table
    sql "DROP TABLE IF EXISTS test_compression"

    // Create the test table
    sql """
        CREATE TABLE test_compression (
            k0 INT,                      -- Primary key
            text_col STRING,             -- String column for input data
            binary_col STRING            -- Binary column for compressed data
        )
        DISTRIBUTED BY HASH(k0)
        PROPERTIES (
            "replication_num" = "1"
        );
    """

    // Insert test data with various cases
    sql """
        INSERT INTO test_compression VALUES
        (1, 'Hello, world!', COMPRESS('Hello, world!')),        -- Plain string
        (2, 'Doris测试中文字符', COMPRESS('Doris测试中文字符')),  -- Chinese characters
        (3, NULL, NULL),                                        -- Null values
        (4, '', COMPRESS('')),                                  -- Empty string
        (5, NULL, 'invalid_compressed_data'),                   -- Invalid binary data
        (6, REPEAT('a', 50), COMPRESS(REPEAT('a', 50)));        -- Short repeated string
    """

    // Test 1: Verify that UNCOMPRESS can correctly restore the original data
    order_qt_restore_original_data """
        SELECT 
            k0,
            UNCOMPRESS(binary_col) AS decompressed_data
        FROM test_compression
        WHERE binary_col IS NOT NULL
        ORDER BY k0;
    """

    // Test 2: Verify that UNCOMPRESS returns NULL for NULL input
    order_qt_uncompress_null_input """
        SELECT 
            k0, 
            UNCOMPRESS(binary_col) AS decompressed_data
        FROM test_compression
        WHERE binary_col IS NULL
        ORDER BY k0;
    """

    // Test 3: Verify that UNCOMPRESS handles invalid binary data gracefully
    order_qt_uncompress_invalid_data """
        SELECT 
            k0, 
            UNCOMPRESS(binary_col) AS decompressed_data
        FROM test_compression
        WHERE k0 = 5
        ORDER BY k0;
    """

    // Test 4: Verify that COMPRESS and UNCOMPRESS work correctly with empty strings
    order_qt_compress_empty_string """
        SELECT 
            k0,  
            UNCOMPRESS(binary_col) AS decompressed_data
        FROM test_compression
        WHERE k0 = 4
        ORDER BY k0;
    """

    // Test 5: Verify that COMPRESS and UNCOMPRESS work correctly with repeated strings
    order_qt_compress_repeated_string """
        SELECT 
            k0,  
            UNCOMPRESS(binary_col) AS decompressed_data
        FROM test_compression
        WHERE k0 = 6
        ORDER BY k0;
    """

    // Additional tests using SELECT UNCOMPRESS(COMPRESS()) directly

    // Test 6: Verify that COMPRESS and UNCOMPRESS work with a single character string
    order_qt_compress_single_char """
        SELECT 
            UNCOMPRESS(COMPRESS('x')) AS decompressed_data
        LIMIT 1;
    """
    
    // Test 7: Verify that COMPRESS handles NULL text values correctly
    order_qt_compress_null_text """
        SELECT 
            UNCOMPRESS(COMPRESS(NULL)) AS decompressed_data
        LIMIT 1;
    """

    // Test 8: Verify that COMPRESS and UNCOMPRESS work with long repeated strings
    order_qt_compress_large_repeated_string """
        SELECT 
            UNCOMPRESS(COMPRESS(REPEAT('a', 100))) AS decompressed_data
        LIMIT 1;
    """

    // Test 9: Verify that COMPRESS and UNCOMPRESS work with an empty string
    order_qt_compress_empty_string_direct """
        SELECT 
            UNCOMPRESS(COMPRESS('')) AS decompressed_data
        LIMIT 1;
    """

    // Test 10: Verify that COMPRESS and UNCOMPRESS work with the string 'Hello, world!'
    order_qt_compress_string_direct """
        SELECT 
            UNCOMPRESS(COMPRESS('Hello, world!')) AS decompressed_data
        LIMIT 1;
    """

    // Test 11: Verify that COMPRESS and UNCOMPRESS work with a numeric value
    order_qt_compress_numeric_direct """
        SELECT 
            UNCOMPRESS(COMPRESS('12345')) AS decompressed_data
        LIMIT 1;
    """

	// Test 12: Multiple COMPRESS calls that COMPRESS the text_col field multiple times directly from the table
    order_qt_compress_multiple_calls_from_table """
        SELECT
            k0,
            COMPRESS(text_col) AS comp1,
            binary_col AS comp2
        FROM test_compression
        ORDER BY k0;
    """

	// Test 13: multiple COMPRESS and UNCOMPRESS calls
    order_qt_compress_uncompress_multiple_calls_from_table """
        SELECT
            k0,
            text_col AS result1,
            UNCOMPRESS(binary_col) AS result2
        FROM test_compression
        ORDER BY k0;
    """
}
