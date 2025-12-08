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

suite("test_pythonudf_base_data_type") {
    def pyPath = """${context.file.parent}/udf_scripts/pyudf.zip"""
    scp_udf_file_to_all_be(pyPath)
    def runtime_version = "3.8.10"
    log.info("Python Zip path: ${pyPath}".toString())

    // TEST INLINE CASE
    try {
        sql """
            DROP FUNCTION IF EXISTS row_to_csv_all(
                BOOLEAN,
                TINYINT,
                SMALLINT,
                INT,
                BIGINT,
                LARGEINT,
                FLOAT,
                DOUBLE,
                DECIMAL,
                DECIMAL,
                DECIMAL,
                DATE,
                DATETIME,
                CHAR,
                VARCHAR,
                STRING
            );
        """
        sql """
CREATE FUNCTION row_to_csv_all(
    BOOLEAN,
    TINYINT,
    SMALLINT,
    INT,
    BIGINT,
    LARGEINT,
    FLOAT,
    DOUBLE,
    DECIMAL,
    DECIMAL,
    DECIMAL,
    DATE,
    DATETIME,
    CHAR,
    VARCHAR,
    STRING
)
RETURNS STRING
PROPERTIES (
    "type" = "PYTHON_UDF",
    "symbol" = "row_to_csv_all_impl",
    "always_nullable" = "true",
    "runtime_version" = "${runtime_version}"
)
AS \$\$
def row_to_csv_all_impl(
    bool_col, tinyint_col, smallint_col, int_col, bigint_col, largeint_col,
    float_col, double_col, decimal32_col, decimal64_col, decimal128_col,
    date_col, datetime_col, char_col, varchar_col, string_col
):
    cols = [
        bool_col, tinyint_col, smallint_col, int_col, bigint_col, largeint_col,
        float_col, double_col, decimal32_col, decimal64_col, decimal128_col,
        date_col, datetime_col, char_col, varchar_col, string_col
    ]
    
    def safe_str(x):
        return 'NULL' if x is None else str(x)
    
    return ','.join(safe_str(col) for col in cols)
\$\$;
        """
        sql """ DROP TABLE IF EXISTS test_datatype_table; """
        sql """
            CREATE TABLE test_datatype_table (
                id INT,
                bool_value BOOLEAN,
                tinyint_value TINYINT,
                smallint_value SMALLINT,
                int_value INT,
                bigint_value BIGINT,
                largeint_value LARGEINT,
                float_value float,
                double_value DOUBLE,
                decimal32_value DECIMAL(8, 2),
                decimal64_value DECIMAL(16, 2),
                decimal128_value DECIMAL(32, 8),
                -- decimal256_value DECIMAL(64, 10),
                date_value DATE,
                datetime_value DATETIME,
                char_value CHAR(100),
                varchar_value VARCHAR(100),
                string_value STRING
            ) ENGINE=OLAP 
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES("replication_num" = "1");
        """
        sql """
            INSERT INTO test_datatype_table VALUES
            (1, TRUE, 127, 32767, 2147483647, 9223372036854775807, 170141183460469231731687303715884105727,
            1.23, 4.56789, 123456.78, 12345678901.2345, 123456789012345678901.234567890,
            '2023-01-01', '2023-01-01 12:34:56', 'char_data_1', 'varchar_data_1', 'string_data_1'),

            (2, FALSE, -128, -32768, -2147483648, -9223372036854775808, -170141183460469231731687303715884105728,
            -2.34, -5.6789, -987654.32, -98765432.109876543, -987654321098765432.10987654321,
            '2024-05-15', '2024-05-15 08:22:10', 'char_data_2', 'varchar_data_2', 'string_data_2'),

            (3, TRUE, 0, 0, 0, 0, 0,
            0.0, 0.0, 0.00, 0.00, 0.00000000,
            '2025-10-15', '2025-10-15 00:00:00', 'char_zero', 'varchar_zero', 'string_zero'),

            (4, FALSE, 100, 20000, 300000000, 4000000000000000000, 99999999999999999999999999999999999999,
            3.14, 2.71828, 999999.99, 99999999999999.99, 99999999999999999999999.999999999999999,
            '2022-12-31', '2022-12-31 23:59:59', 'char_max', 'varchar_max', 'string_max'),

            (5, TRUE, -50, -10000, -100000000, -5000000000000000000, -99999999999999999999999999999999999999,
            -1.41, -0.57721, -0.01, -0.01, -0.000000001,
            '2021-07-04', '2021-07-04 14:30:00', 'char_neg', 'varchar_neg', 'string_neg');
        """

        qt_select_1 """
            SELECT row_to_csv_all(
                bool_value,
                tinyint_value,
                smallint_value,
                int_value,
                bigint_value,
                largeint_value,
                float_value,
                double_value,
                decimal32_value,
                decimal64_value,
                decimal128_value,
                date_value,
                datetime_value,
                char_value,
                varchar_value,
                string_value
            ) AS csv_row
            FROM test_datatype_table;
        """
    } finally {
        try_sql("""DROP FUNCTION IF EXISTS row_to_csv_all(
                BOOLEAN,
                TINYINT,
                SMALLINT,
                INT,
                BIGINT,
                LARGEINT,
                FLOAT,
                DOUBLE,
                DECIMAL,
                DECIMAL,
                DECIMAL,
                DATE,
                DATETIME,
                CHAR,
                VARCHAR,
                STRING
            );""")
        try_sql("DROP TABLE IF EXISTS test_datatype_table;")
    }

    // TEST MODULE CASE
    try {
        sql """
            DROP FUNCTION IF EXISTS row_to_csv_all(
                BOOLEAN,
                TINYINT,
                SMALLINT,
                INT,
                BIGINT,
                LARGEINT,
                FLOAT,
                DOUBLE,
                DECIMAL,
                DECIMAL,
                DECIMAL,
                DATE,
                DATETIME,
                CHAR,
                VARCHAR,
                STRING
            );
        """
        sql """
            CREATE FUNCTION row_to_csv_all(
                BOOLEAN,
                TINYINT,
                SMALLINT,
                INT,
                BIGINT,
                LARGEINT,
                FLOAT,
                DOUBLE,
                DECIMAL,
                DECIMAL,
                DECIMAL,
                DATE,
                DATETIME,
                CHAR,
                VARCHAR,
                STRING
            )
            RETURNS STRING
            PROPERTIES (
                "type" = "PYTHON_UDF",
                "file" = "file://${pyPath}",
                "symbol" = "python_udf_data_type.row_to_csv_all_impl",
                "always_nullable" = "true",
                "runtime_version" = "${runtime_version}"
            );
        """
        sql """ DROP TABLE IF EXISTS test_datatype_table; """
        sql """
            CREATE TABLE test_datatype_table (
                id INT,
                bool_value BOOLEAN,
                tinyint_value TINYINT,
                smallint_value SMALLINT,
                int_value INT,
                bigint_value BIGINT,
                largeint_value LARGEINT,
                float_value float,
                double_value DOUBLE,
                decimal32_value DECIMAL(8, 2),
                decimal64_value DECIMAL(16, 2),
                decimal128_value DECIMAL(32, 8),
                -- decimal256_value DECIMAL(64, 10),
                date_value DATE,
                datetime_value DATETIME,
                char_value CHAR(100),
                varchar_value VARCHAR(100),
                string_value STRING
            ) ENGINE=OLAP 
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES("replication_num" = "1");
        """
        sql """
            INSERT INTO test_datatype_table VALUES
            (1, TRUE, 127, 32767, 2147483647, 9223372036854775807, 170141183460469231731687303715884105727,
            1.23, 4.56789, 123456.78, 12345678901.2345, 123456789012345678901.234567890,
            '2023-01-01', '2023-01-01 12:34:56', 'char_data_1', 'varchar_data_1', 'string_data_1'),

            (2, FALSE, -128, -32768, -2147483648, -9223372036854775808, -170141183460469231731687303715884105728,
            -2.34, -5.6789, -987654.32, -98765432.109876543, -987654321098765432.10987654321,
            '2024-05-15', '2024-05-15 08:22:10', 'char_data_2', 'varchar_data_2', 'string_data_2'),

            (3, TRUE, 0, 0, 0, 0, 0,
            0.0, 0.0, 0.00, 0.00, 0.00000000,
            '2025-10-15', '2025-10-15 00:00:00', 'char_zero', 'varchar_zero', 'string_zero'),

            (4, FALSE, 100, 20000, 300000000, 4000000000000000000, 99999999999999999999999999999999999999,
            3.14, 2.71828, 999999.99, 99999999999999.99, 99999999999999999999999.999999999999999,
            '2022-12-31', '2022-12-31 23:59:59', 'char_max', 'varchar_max', 'string_max'),

            (5, TRUE, -50, -10000, -100000000, -5000000000000000000, -99999999999999999999999999999999999999,
            -1.41, -0.57721, -0.01, -0.01, -0.000000001,
            '2021-07-04', '2021-07-04 14:30:00', 'char_neg', 'varchar_neg', 'string_neg');
        """

        qt_select_2 """
            SELECT row_to_csv_all(
                bool_value,
                tinyint_value,
                smallint_value,
                int_value,
                bigint_value,
                largeint_value,
                float_value,
                double_value,
                decimal32_value,
                decimal64_value,
                decimal128_value,
                date_value,
                datetime_value,
                char_value,
                varchar_value,
                string_value
            ) AS csv_row
            FROM test_datatype_table;
        """
    } finally {
        try_sql("""DROP FUNCTION IF EXISTS row_to_csv_all(
                BOOLEAN,
                TINYINT,
                SMALLINT,
                INT,
                BIGINT,
                LARGEINT,
                FLOAT,
                DOUBLE,
                DECIMAL,
                DECIMAL,
                DECIMAL,
                DATE,
                DATETIME,
                CHAR,
                VARCHAR,
                STRING
            );""")
        try_sql("DROP TABLE IF EXISTS test_datatype_table;")
    }
}
