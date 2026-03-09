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

suite("test_default_expr") {
    
    def testConstantFoldingConsistency = { String querySql ->
        def baseRes = sql querySql

        sql "SET debug_skip_fold_constant = true;"
        def skipFoldConstantRes = sql querySql
        assertTrue(skipFoldConstantRes == baseRes, 
            "Results differ with debug_skip_fold_constant=true for query: ${querySql}")

        sql "SET debug_skip_fold_constant = false;"
        sql "SET enable_fold_constant_by_be = true;"
        def beFoldConstantRes = sql querySql
        assertTrue(beFoldConstantRes == baseRes, 
            "Results differ with enable_fold_constant_by_be=true for query: ${querySql}")

        sql "SET debug_skip_fold_constant = default;"
        sql "SET enable_fold_constant_by_be = default;" 
    }
    
    def tableName = "test_default_scalar"
    def aggTableName = "test_default_agg"
    def notNullTableName = "test_default_not_null"
    

    // Test 1: Scalar type constant default value test
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE ${tableName} (
            c_bool                   BOOLEAN                               NULL     DEFAULT 1,
            c_tinyint                TINYINT                               NULL     DEFAULT 7,
            c_smallint               SMALLINT                              NULL     DEFAULT 32000,
            c_int                    INT                                   NULL     DEFAULT 2147483647,
            c_bigint                 BIGINT                                NULL     DEFAULT 9223372036854775807,
            c_largeint               LARGEINT                              NULL     DEFAULT '170141183460469231731687303715884105727',
            c_float                  FLOAT                                 NULL     DEFAULT 3.125,
            c_double                 DOUBLE                                NULL     DEFAULT 2.718281828,
            c_decimal                DECIMAL(27, 9)                        NULL     DEFAULT '123456789.123456789',
            c_decimal_compact        DECIMAL(18, 4)                        NULL     DEFAULT '99999.1234',
            c_char                   CHAR(8)                               NULL     DEFAULT 'charDemo',
            c_varchar                VARCHAR(32)                           NULL     DEFAULT '',
            c_string                 STRING                                NULL     DEFAULT 'plain string',
            c_tz                     TIMESTAMPTZ(6)                        NULL     DEFAULT '2025-10-25 11:22:33.666777+08:00',
            c_datetime               DATETIME                              NULL     DEFAULT '2025-10-25 11:22:33',
            c_date                   DATE                                  NULL     DEFAULT '2025-10-31',
            c_json                   JSON                                  NULL,
            c_ipv4                   IPV4                                  NULL     DEFAULT '192.168.1.1',
            c_ipv6                   IPV6                                  NULL     DEFAULT '2001:db8::1',
            c_array_int              ARRAY<INT>                            NULL     DEFAULT '[]',
            c_array_string           ARRAY<STRING>                         NULL,
            c_map_str_int            MAP<STRING, INT>                      NULL,
            c_struct                 STRUCT<f1:INT,f2:STRING,f3:BOOLEAN>   NULL,
            c_variant                VARIANT                               NULL
        ) PROPERTIES ( 'replication_num' = '1')
    """
    sql "INSERT INTO ${tableName} (c_bool) VALUES (0)"
    sql "INSERT INTO ${tableName} (c_bool) VALUES (1)"
    sql "INSERT INTO ${tableName} (c_bool) VALUES (0)"

    qt_scalar_defaults """
        SELECT
            DEFAULT(c_bool),
            DEFAULT(c_tinyint),
            DEFAULT(c_smallint),
            DEFAULT(c_int),
            DEFAULT(c_bigint),
            DEFAULT(c_largeint),
            DEFAULT(c_float),
            DEFAULT(c_double),
            DEFAULT(c_decimal),
            DEFAULT(c_decimal_compact),
            DEFAULT(c_char),
            DEFAULT(c_varchar),
            DEFAULT(c_string),
            DEFAULT(c_tz),
            DEFAULT(c_datetime),
            DEFAULT(c_date),
            DEFAULT(c_json),
            DEFAULT(c_ipv4),
            DEFAULT(c_ipv6),
            DEFAULT(c_array_int),
            DEFAULT(c_array_string),
            DEFAULT(c_map_str_int),
            DEFAULT(c_struct),
            DEFAULT(c_variant)
        FROM ${tableName}
        LIMIT 1
    """

    def result = sql """
        SELECT
            DEFAULT(c_bool),
            DEFAULT(c_tinyint),
            DEFAULT(c_smallint),
            DEFAULT(c_int),
            DEFAULT(c_bigint),
            DEFAULT(c_largeint),
            DEFAULT(c_float),
            DEFAULT(c_double),
            DEFAULT(c_decimal),
            DEFAULT(c_decimal_compact),
            DEFAULT(c_char),
            DEFAULT(c_varchar),
            DEFAULT(c_string),
            DEFAULT(c_datetime),
            DEFAULT(c_date),
            DEFAULT(c_json),
            DEFAULT(c_ipv4),
            DEFAULT(c_ipv6),
            DEFAULT(c_array_int),
            DEFAULT(c_array_string),
            DEFAULT(c_map_str_int),
            DEFAULT(c_struct),
            DEFAULT(c_variant)
        FROM ${tableName}
    """
    def firstRow = result[0]
    for (int i = 1; i < result.size(); i++) {
        assertTrue(result[i] == firstRow, "Row ${i} should equal first row")
    }

    // Test 2: Aggregate type default value test
    sql "DROP TABLE IF EXISTS ${aggTableName}"
    sql """
        CREATE TABLE ${aggTableName} (
            k_id          INT             NOT NULL COMMENT 'Aggregate key',
            bitmap_col    BITMAP          BITMAP_UNION,
            hll_col       HLL             HLL_UNION
        ) AGGREGATE KEY(k_id)
        PROPERTIES ( 'replication_num' = '1' )
    """
    sql "INSERT INTO ${aggTableName} (k_id) VALUES (1)"
    sql "INSERT INTO ${aggTableName} (k_id) VALUES (2)"
    sql "INSERT INTO ${aggTableName} (k_id) VALUES (3)"

    test {
        sql "SELECT DEFAULT(bitmap_col) FROM ${aggTableName} LIMIT 1"
        exception "Cannot find column information"

        sql """ SELECT DEFAULT(hll_col) FROM ${aggTableName} LIMIT 1 """
        exception "Cannot find column information"
    }

    // Test 3: Non-constant default value test (CURRENT_TIMESTAMP, CURRENT_DATE)
    sql "DROP TABLE IF EXISTS test_default_time"
    sql """
        CREATE TABLE test_default_time(
            tm DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            dt DATE DEFAULT CURRENT_DATE,
            tznn TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
            tzn TIMESTAMPTZ(6) NULL DEFAULT CURRENT_TIMESTAMP(4)
        ) PROPERTIES( 'replication_num' = '1' );
    """

    sql """
        INSERT INTO test_default_time (tm, tznn) VALUES
        ('2025-10-10 12:13:14', '2025-10-10 12:13:14'),
        ('2025-10-10 12:13:14', '2025-10-10 12:13:14');
    """

    def curTimeResult = sql """
        SELECT
            CAST(DEFAULT(tm) AS DATETIME(6)),
            DEFAULT(dt),
            CAST(DEFAULT(tznn) AS DATETIME(6)),
            CAST(DEFAULT(tzn) AS DATETIME(4))
        FROM test_default_time;
    """
    firstRow = curTimeResult[0]
    for (int i = 0; i < firstRow.size(); i++) {
        assertTrue(curTimeResult[1][i] == firstRow[i], "Row ${i} should equal first row")
    }
    // assertTrue(curTimeResult[0][0] == curTimeResult[0][1])

    // qt_non_const_defaults """
    //     SELECT DEFAULT(tm), DEFAULT(dt), DEFAULT(tznn), DEFAULT(tzn) FROM test_default_time;
    // """

    // Test 4: NOT NULL column has no default value error test
    sql "DROP TABLE IF EXISTS ${notNullTableName}"
    sql """
        CREATE TABLE ${notNullTableName} (
            id           INT        NOT NULL,
            required_col VARCHAR(50) NOT NULL,
            optional_col VARCHAR(50) NULL
        ) PROPERTIES ( 'replication_num' = '1' )
    """
    sql "INSERT INTO ${notNullTableName} (id, required_col) VALUES (1, 'test')"
    sql "INSERT INTO ${notNullTableName} (id, required_col) VALUES (2, 'test2')"
    sql "INSERT INTO ${notNullTableName} (id, required_col) VALUES (3, 'test3')"

    test {
        sql "SELECT DEFAULT(required_col) FROM ${notNullTableName} LIMIT 1"
        exception "has no default value and does not allow NULL or column is auto-increment"
    }

    qt_null_column_default """
        SELECT DEFAULT(optional_col) FROM ${notNullTableName} LIMIT 1
    """
    
    testConstantFoldingConsistency """
        SELECT DEFAULT(optional_col) FROM ${notNullTableName} LIMIT 1
    """

    def nullColResult = sql """
        SELECT DEFAULT(optional_col) FROM ${notNullTableName}
    """
    def nullColFirstRow = nullColResult[0]
    for (int i = 1; i < nullColResult.size(); i++) {
        assertTrue(nullColResult[i] == nullColFirstRow, "Row ${i} should equal first row")
    }

    // Test 5: Numeric type boundary default value test
    def boundaryTableName = "test_default_boundary"
    sql "DROP TABLE IF EXISTS ${boundaryTableName}"
    
    sql """
        CREATE TABLE ${boundaryTableName} (
            c_tinyint_min    TINYINT    NULL DEFAULT -128,
            c_tinyint_max    TINYINT    NULL DEFAULT 127,
            c_smallint_min   SMALLINT   NULL DEFAULT -32768,
            c_smallint_max   SMALLINT   NULL DEFAULT 32767,
            c_int_min        INT        NULL DEFAULT -2147483648,
            c_int_max        INT        NULL DEFAULT 2147483647,
            c_bigint_min     BIGINT     NULL DEFAULT -9223372036854775808,
            c_bigint_max     BIGINT     NULL DEFAULT 9223372036854775807,
            c_float_zero     FLOAT      NULL DEFAULT 0.0,
            c_double_zero    DOUBLE     NULL DEFAULT 0.0,
            c_decimal_zero   DECIMAL(10,2) NULL DEFAULT 0.00,
            c_empty_string   STRING     NULL DEFAULT '',
            c_null_default   STRING     NULL DEFAULT NULL
        ) PROPERTIES ( 'replication_num' = '1' )
    """

    sql "INSERT INTO ${boundaryTableName} (c_tinyint_min) VALUES (-1)"
    sql "INSERT INTO ${boundaryTableName} (c_tinyint_min) VALUES (0)"
    sql "INSERT INTO ${boundaryTableName} (c_tinyint_min) VALUES (1)"

    qt_boundary_defaults """
        SELECT
            DEFAULT(c_tinyint_min),
            DEFAULT(c_tinyint_max),
            DEFAULT(c_smallint_min),
            DEFAULT(c_smallint_max),
            DEFAULT(c_int_min),
            DEFAULT(c_int_max),
            DEFAULT(c_bigint_min),
            DEFAULT(c_bigint_max),
            DEFAULT(c_float_zero),
            DEFAULT(c_double_zero),
            DEFAULT(c_decimal_zero),
            DEFAULT(c_empty_string),
            DEFAULT(c_null_default)
        FROM ${boundaryTableName}
        LIMIT 1
    """
    
    testConstantFoldingConsistency """
        SELECT
            DEFAULT(c_tinyint_min),
            DEFAULT(c_tinyint_max),
            DEFAULT(c_smallint_min),
            DEFAULT(c_smallint_max),
            DEFAULT(c_int_min),
            DEFAULT(c_int_max),
            DEFAULT(c_bigint_min),
            DEFAULT(c_bigint_max),
            DEFAULT(c_float_zero),
            DEFAULT(c_double_zero),
            DEFAULT(c_decimal_zero),
            DEFAULT(c_empty_string),
            DEFAULT(c_null_default)
        FROM ${boundaryTableName}
        LIMIT 1
    """

    def boundaryResult = sql """
        SELECT
            DEFAULT(c_tinyint_min),
            DEFAULT(c_tinyint_max),
            DEFAULT(c_smallint_min),
            DEFAULT(c_smallint_max),
            DEFAULT(c_int_min),
            DEFAULT(c_int_max),
            DEFAULT(c_bigint_min),
            DEFAULT(c_bigint_max),
            DEFAULT(c_float_zero),
            DEFAULT(c_double_zero),
            DEFAULT(c_decimal_zero),
            DEFAULT(c_empty_string),
            DEFAULT(c_null_default)
        FROM ${boundaryTableName}
    """
    def boundaryFirstRow = boundaryResult[0]
    for (int i = 1; i < boundaryResult.size(); i++) {
        assertTrue(boundaryResult[i] == boundaryFirstRow, "Row ${i} should equal first row")
    }

    // Test 6: Complex type default value test
    def complexTableName = "test_default_complex"
    sql "DROP TABLE IF EXISTS ${complexTableName}"
    
    sql """
        CREATE TABLE ${complexTableName} (
            id                INT            NOT NULL,
            c_array_empty     ARRAY<INT>     NULL DEFAULT '[]',
            c_array_null      ARRAY<STRING>  NULL,
            c_map_null        MAP<STRING, INT> NULL,
            c_struct_null     STRUCT<id:INT,name:STRING> NULL,
            c_json_null       JSON           NULL,
            c_variant_null    VARIANT        NULL
        ) PROPERTIES ( 'replication_num' = '1' )
    """

    sql "INSERT INTO ${complexTableName} (id) VALUES (1)"
    sql "INSERT INTO ${complexTableName} (id) VALUES (2)"
    sql "INSERT INTO ${complexTableName} (id) VALUES (3)"

    qt_complex_defaults """
        SELECT
            DEFAULT(c_array_empty),
            DEFAULT(c_array_null),
            DEFAULT(c_map_null),
            DEFAULT(c_struct_null),
            DEFAULT(c_json_null),
            DEFAULT(c_variant_null)
        FROM ${complexTableName}
        LIMIT 1
    """
    
    testConstantFoldingConsistency """
        SELECT
            DEFAULT(c_array_empty),
            DEFAULT(c_array_null),
            DEFAULT(c_map_null),
            DEFAULT(c_struct_null),
            DEFAULT(c_json_null),
            DEFAULT(c_variant_null)
        FROM ${complexTableName}
        LIMIT 1
    """

    def complexResult = sql """
        SELECT
            DEFAULT(c_array_empty),
            DEFAULT(c_array_null),
            DEFAULT(c_map_null),
            DEFAULT(c_struct_null),
            DEFAULT(c_json_null),
            DEFAULT(c_variant_null)
        FROM ${complexTableName}
    """
    def complexFirstRow = complexResult[0]
    for (int i = 1; i < complexResult.size(); i++) {
        assertTrue(complexResult[i] == complexFirstRow, "Row ${i} should equal first row")
    }

    // Test 7: Error case test - illegal parameter
    test {
        sql "SELECT DEFAULT(123)"
        exception "mismatched input"
    }

    test {
        sql "SELECT DEFAULT('literal_string')"
        exception "mismatched input"
    }

    test {
        sql "SELECT DEFAULT(c_bool + 1) FROM ${tableName} LIMIT 1"
        exception "missing ')' at '+'"
    }

    def baseDefault = sql """
        SELECT DEFAULT(c_bool)
        FROM ${tableName}
        LIMIT 1
    """
    def tableAliasDefault = sql """
        SELECT DEFAULT(t.c_bool)
        FROM ${tableName} t
        LIMIT 1
    """
    def aliasDefault = sql """
        SELECT DEFAULT(x)
        FROM (
            SELECT c_bool AS x
            FROM ${tableName}
        ) t
        LIMIT 1
    """
    def nestedAliasDefault = sql """
        SELECT DEFAULT(x)
        FROM (
            SELECT x
            FROM (
                SELECT c_bool AS x
                FROM ${tableName}
            ) inner_view
        ) outer_view
        LIMIT 1
    """
    def cteAliasDefault = sql """
        WITH cte AS (
            SELECT c_bool AS x
            FROM ${tableName}
        )
        SELECT DEFAULT(x)
        FROM cte
        LIMIT 1
    """
    assertTrue(aliasDefault == baseDefault,
            "DEFAULT() should return the same value for aliased columns in subqueries")
    assertTrue(tableAliasDefault == baseDefault,
            "DEFAULT() should return the same value when referenced through a table alias")
    assertTrue(nestedAliasDefault == baseDefault,
            "DEFAULT() should return the same value for nested aliased subqueries")
    assertTrue(cteAliasDefault == baseDefault,
            "DEFAULT() should return the same value when alias comes from a CTE")

    sql "SET enable_fold_constant_by_be = false;"
    test {
        sql "SELECT DEFAULT(c_bool, c_int) FROM ${tableName} LIMIT 1"
        exception "missing ')' at ','"
    }

    sql "SET enable_fold_constant_by_be = true;"
    test {
        sql "SELECT DEFAULT(c_bool, c_int) FROM ${tableName} LIMIT 1"
        exception "missing ')' at ','"
    }
    sql "SET enable_fold_constant_by_be = default;"

    // Test 8: Date and time format default value test
    def dateTimeTableName = "test_default_datetime_formats"
    sql "DROP TABLE IF EXISTS ${dateTimeTableName}"
    
    sql """
        CREATE TABLE ${dateTimeTableName} (
            c_date_std       DATE        NULL DEFAULT '2025-01-01',
            c_datetime_std   DATETIME    NULL DEFAULT '2025-01-01 12:30:45'
        ) PROPERTIES ( 'replication_num' = '1' )
    """

    sql "INSERT INTO ${dateTimeTableName} (c_date_std) VALUES ('2024-01-01')"
    sql "INSERT INTO ${dateTimeTableName} (c_date_std) VALUES ('2024-02-01')"
    sql "INSERT INTO ${dateTimeTableName} (c_date_std) VALUES ('2024-03-01')"

    qt_datetime_format_defaults """
        SELECT
            DEFAULT(c_date_std),
            DEFAULT(c_datetime_std)
        FROM ${dateTimeTableName}
        LIMIT 1
    """
    
    testConstantFoldingConsistency """
        SELECT
            DEFAULT(c_date_std),
            DEFAULT(c_datetime_std)
        FROM ${dateTimeTableName}
        LIMIT 1
    """

    def dateTimeResult = sql """
        SELECT
            DEFAULT(c_date_std),
            DEFAULT(c_datetime_std)
        FROM ${dateTimeTableName}
    """
    def dateTimeFirstRow = dateTimeResult[0]
    for (int i = 1; i < dateTimeResult.size(); i++) {
        assertTrue(dateTimeResult[i] == dateTimeFirstRow, "Row ${i} should equal first row")
    }

    // Test 9: IP address type default value test
    def ipTableName = "test_default_ip"
    sql "DROP TABLE IF EXISTS ${ipTableName}"
    sql """
        CREATE TABLE ${ipTableName} (
            c_ipv4_localhost   IPV4  NULL DEFAULT '127.0.0.1',
            c_ipv4_private     IPV4  NULL DEFAULT '10.0.0.1',
            c_ipv6_localhost   IPV6  NULL DEFAULT '::1',
            c_ipv6_example     IPV6  NULL DEFAULT '2001:db8:85a3::8a2e:370:7334'
        ) PROPERTIES ( 'replication_num' = '1' )
    """

    sql "INSERT INTO ${ipTableName} (c_ipv4_localhost) VALUES ('192.168.1.1')"
    sql "INSERT INTO ${ipTableName} (c_ipv4_localhost) VALUES ('10.0.0.1')"
    sql "INSERT INTO ${ipTableName} (c_ipv4_localhost) VALUES ('172.16.0.1')"

    qt_ip_defaults """
        SELECT
            DEFAULT(c_ipv4_localhost),
            DEFAULT(c_ipv4_private),
            DEFAULT(c_ipv6_localhost),
            DEFAULT(c_ipv6_example)
        FROM ${ipTableName}
        LIMIT 1
    """
    
    testConstantFoldingConsistency """
        SELECT
            DEFAULT(c_ipv4_localhost),
            DEFAULT(c_ipv4_private),
            DEFAULT(c_ipv6_localhost),
            DEFAULT(c_ipv6_example)
        FROM ${ipTableName}
        LIMIT 1
    """

    def ipResult = sql """
        SELECT
            DEFAULT(c_ipv4_localhost),
            DEFAULT(c_ipv4_private),
            DEFAULT(c_ipv6_localhost),
            DEFAULT(c_ipv6_example)
        FROM ${ipTableName}
    """
    def ipFirstRow = ipResult[0]
    for (int i = 1; i < ipResult.size(); i++) {
        assertTrue(ipResult[i] == ipFirstRow, "Row ${i} should equal first row")
    }

    // Test 10: String type length test
    def stringTableName = "test_default_strings"
    sql "DROP TABLE IF EXISTS ${stringTableName}"
    sql """
        CREATE TABLE ${stringTableName} (
            c_char_fixed    CHAR(10)     NULL DEFAULT 'fixed',
            c_varchar_var   VARCHAR(100) NULL DEFAULT 'variable length string',
            c_string_long   STRING       NULL DEFAULT 'very long string that can be much longer than varchar',
            c_char_empty    CHAR(5)      NULL DEFAULT '',
            c_varchar_empty VARCHAR(20)  NULL DEFAULT ''
        ) PROPERTIES ( 'replication_num' = '1' )
    """

    sql "INSERT INTO ${stringTableName} (c_char_fixed) VALUES ('test')"
    sql "INSERT INTO ${stringTableName} (c_char_fixed) VALUES ('hello')"
    sql "INSERT INTO ${stringTableName} (c_char_fixed) VALUES ('world')"

    qt_string_defaults """
        SELECT
            DEFAULT(c_char_fixed),
            DEFAULT(c_varchar_var),
            DEFAULT(c_string_long),
            DEFAULT(c_char_empty),
            DEFAULT(c_varchar_empty)
        FROM ${stringTableName}
        LIMIT 1
    """
    
    testConstantFoldingConsistency """
        SELECT
            DEFAULT(c_char_fixed),
            DEFAULT(c_varchar_var),
            DEFAULT(c_string_long),
            DEFAULT(c_char_empty),
            DEFAULT(c_varchar_empty)
        FROM ${stringTableName}
        LIMIT 1
    """

    def stringResult = sql """
        SELECT
            DEFAULT(c_char_fixed),
            DEFAULT(c_varchar_var),
            DEFAULT(c_string_long),
            DEFAULT(c_char_empty),
            DEFAULT(c_varchar_empty)
        FROM ${stringTableName}
    """
    def stringFirstRow = stringResult[0]
    for (int i = 1; i < stringResult.size(); i++) {
        assertTrue(stringResult[i] == stringFirstRow, "Row ${i} should equal first row")
    }

    // Test11: PI and E test
    def pieTableName = "test_default_pi_e"
    sql "DROP TABLE IF EXISTS ${pieTableName}"
    sql """
        CREATE TABLE ${pieTableName} (
            id     INT     NOT NULL,
            c_pi   DOUBLE  NULL DEFAULT PI,
            c_e    DOUBLE  NULL DEFAULT E
        ) PROPERTIES ( 'replication_num' = '1' )
    """
    sql "INSERT INTO ${pieTableName} VALUES (1, NULL, NULL)"
    sql "INSERT INTO ${pieTableName} VALUES (2, NULL, NULL)"
    sql "INSERT INTO ${pieTableName} VALUES (3, NULL, NULL)"
    qt_pi_e_defaults """
        SELECT
            DEFAULT(c_pi),
            DEFAULT(c_e)
        FROM ${pieTableName}
        LIMIT 1
    """
    
    testConstantFoldingConsistency """
        SELECT
            DEFAULT(c_pi),
            DEFAULT(c_e)
        FROM ${pieTableName}
        LIMIT 1
    """

    def pieResult = sql """
        SELECT
            DEFAULT(c_pi),
            DEFAULT(c_e)
        FROM ${pieTableName}
    """
    def pieFirstRow = pieResult[0]
    for (int i = 1; i < pieResult.size(); i++) {
        assertTrue(pieResult[i] == pieFirstRow, "Row ${i} should equal first row")
    }

    // Test 12: Empty table test
    def emptyTableName = "test_default_empty_table"
    sql "DROP TABLE IF EXISTS ${emptyTableName}"
    sql """
        CREATE TABLE ${emptyTableName} (
            c_int      INT     NULL DEFAULT 100,
            c_string   STRING  NULL DEFAULT 'empty table test'
        ) PROPERTIES ( 'replication_num' = '1' );
    """

    qt_empty_table_defaults """
        SELECT
            DEFAULT(c_int),
            DEFAULT(c_string)
        FROM ${emptyTableName}
        LIMIT 1
    """
    
    testConstantFoldingConsistency """
        SELECT
            DEFAULT(c_int),
            DEFAULT(c_string)
        FROM ${emptyTableName}
        LIMIT 1
    """

    sql "INSERT INTO ${emptyTableName} (c_int) VALUES (1)"
    sql "INSERT INTO ${emptyTableName} (c_int) VALUES (2)"
    sql "INSERT INTO ${emptyTableName} (c_int) VALUES (3)"

    def emptyResult = sql """
        SELECT
            DEFAULT(c_int),
            DEFAULT(c_string)
        FROM ${emptyTableName}
    """
    def emptyFirstRow = emptyResult[0]
    for (int i = 1; i < emptyResult.size(); i++) {
        assertTrue(emptyResult[i] == emptyFirstRow, "Row ${i} should equal first row")
    }

    // Test 13: Same col_name in different tables
    def tableA = "test_default_table_a"
    def tableB = "test_default_table_b"
    sql "DROP TABLE IF EXISTS ${tableA}"
    sql "DROP TABLE IF EXISTS ${tableB}"
    sql """
        CREATE TABLE ${tableA} (
            id          INT             NOT NULL DEFAULT '1',
            val         VARCHAR(20)     NOT NULL DEFAULT 'hello',
            dt          DATE            NOT NULL DEFAULT '2026-01-13'
        ) PROPERTIES ('replication_num' = '1' )
    """
    sql """
        CREATE TABLE ${tableB} (
            id          BIGINT          NOT NULL DEFAULT '1',
            val         DOUBLE          NOT NULL DEFAULT PI,
            dt          DATETIME        NOT NULL DEFAULT '2023-12-31 23:59:59'
        ) PROPERTIES ('replication_num' = '1' )
    """
    sql "INSERT INTO ${tableA}(id) VALUES (1), (2), (3);"
    sql "INSERT INTO ${tableB}(id) VALUES (1), (2), (3);"
    qt_same_col_name """SELECT
                            DEFAULT(a.val), DEFAULT(a.dt),
                            DEFAULT(b.val), DEFAULT(b.dt)
                        FROM ${tableA} AS a
                        JOIN ${tableB} AS b
                        ON a.id = b.id;"""
    
    testConstantFoldingConsistency """SELECT
                            DEFAULT(a.val), DEFAULT(a.dt),
                            DEFAULT(b.val), DEFAULT(b.dt)
                        FROM ${tableA} AS a
                        JOIN ${tableB} AS b
                        ON a.id = b.id;"""

    // aotu-increment column
    sql """ DROP TABLE IF EXISTS test_auto_inc """
    sql """
        CREATE TABLE test_auto_inc (
            id BIGINT NOT NULL AUTO_INCREMENT,
            value BIGINT NOT NULL
        ) PROPERTIES ('replication_num' = '1' );
    """
    sql """ INSERT INTO test_auto_inc(value) VALUES (1); """
    test {
        sql """ SELECT DEFAULT(id) FROM test_auto_inc; """
        exception "has no default value and does not allow NULL or column is auto-increment"
    }

    // generate column
    sql """ DROP TABLE IF EXISTS test_gen_col """
    sql """ 
        CREATE TABLE test_gen_col (
            a INT,
            b INT AS (a + 10)
        ) PROPERTIES ('replication_num' = '1' );
    """
    sql """ INSERT INTO test_gen_col(a) VALUES (1); """
    test {
        sql """ SELECT DEFAULT(b) FROM test_gen_col; """
        exception "DEFAULT cannot be used on generated column"
    }
}