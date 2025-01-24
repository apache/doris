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

    // Insert test data with various cases (removing special characters)
    sql """
        INSERT INTO test_compression VALUES
        (1, 'Hello, world!', NULL),        -- Plain string
        (2, 'Doris测试中文字符', NULL),     -- Chinese characters
        (3, NULL, NULL),                   -- Null values
        (4, '', NULL),                     -- Empty string
        (5, NULL, 'invalid_compressed_data'), -- Invalid binary data
        (6, REPEAT('a', 50), NULL);        -- Short repeated string
    """

    // Nullable column test: UNCOMPRESS on nullable column
    order_qt_nullable "SELECT k0, UNCOMPRESS(binary_col) AS original_data FROM test_compression ORDER BY k0;"

    // Non-nullable column test: COMPRESS on non-nullable column
    order_qt_not_nullable "SELECT k0, COMPRESS(text_col) AS compressed_data FROM test_compression ORDER BY k0;"

    // Nullable column with no null values: COMPRESS with explicit nullable conversion
    order_qt_nullable_no_null "SELECT k0, COMPRESS(NULLABLE(text_col)) AS compressed_nullable_data FROM test_compression ORDER BY k0;"

    // Constant nullable test: COMPRESS and UNCOMPRESS on NULL
    order_qt_const_nullable """
        SELECT 
            COMPRESS(NULL) AS compressed_null,
            UNCOMPRESS(NULL) AS decompressed_null;
    """

    // Constant non-nullable test: COMPRESS and UNCOMPRESS on constant string
    order_qt_const_not_nullable """
        SELECT 
            COMPRESS('Doris') AS compressed_const,
            UNCOMPRESS(COMPRESS('Doris')) AS decompressed_const;
    """

    // Constant nullable with no null values: COMPRESS and UNCOMPRESS on explicit nullable constant
    order_qt_const_nullable_no_null """
        SELECT 
            COMPRESS(NULLABLE('Doris Test')) AS compressed_nullable_const,
            UNCOMPRESS(COMPRESS(NULLABLE('Doris Test'))) AS decompressed_nullable_const;
    """

    // Edge case test: Short repeated strings
    order_qt_edge_cases """
        SELECT k0, UNCOMPRESS(COMPRESS(text_col)) AS decompressed_text
        FROM test_compression
        WHERE k0 = 6
        ORDER BY k0;
    """
}
