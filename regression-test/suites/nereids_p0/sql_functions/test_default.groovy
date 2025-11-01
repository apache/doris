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

suite("test_default") {
    
    def tableName = "test_default_scalar"
    def aggTableName = "test_default_agg"
    def nonConstDefaultTableName = "test_default_non_const"
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

    qt_agg_defaults """
        SELECT
            DEFAULT(bitmap_col),
            DEFAULT(hll_col)
        FROM ${aggTableName}
        LIMIT 1
    """

    // Test 3: Non-constant default value test (CURRENT_TIMESTAMP, CURRENT_DATE)
    sql "DROP TABLE IF EXISTS ${nonConstDefaultTableName}"
    sql """
        CREATE TABLE ${nonConstDefaultTableName} (
            null_dt     DATETIME NULL     DEFAULT CURRENT_TIMESTAMP,
            not_null_dt DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            null_d      DATE     NULL     DEFAULT CURRENT_DATE,
            not_null_d  DATE     NOT NULL DEFAULT CURRENT_DATE
        ) PROPERTIES ( 'replication_num' = '1' )
    """

    sql "INSERT INTO ${nonConstDefaultTableName} (null_dt) VALUES (NULL)"

    qt_non_const_defaults """
        SELECT
            DEFAULT(null_dt), 
            DEFAULT(not_null_dt),
            DEFAULT(null_d), 
            DEFAULT(not_null_d)
        FROM ${nonConstDefaultTableName}
        LIMIT 1
    """

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

    test {
        sql "SELECT DEFAULT(required_col) FROM ${notNullTableName} LIMIT 1"
        exception "Column 'required_col' is NOT NULL but has no default value"
    }

    qt_null_column_default """
        SELECT DEFAULT(optional_col) FROM ${notNullTableName} LIMIT 1
    """

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

    // Test 7: Error case test - illegal parameter
    test {
        sql "SELECT DEFAULT(123)"
        exception "DEFAULT function requires a column reference"
    }

    test {
        sql "SELECT DEFAULT('literal_string')"
        exception "DEFAULT function requires a column reference"
    }

    test {
        sql "SELECT DEFAULT(c_bool + 1) FROM ${tableName} LIMIT 1"
        exception "DEFAULT function requires a column reference"
    }

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

    qt_datetime_format_defaults """
        SELECT
            DEFAULT(c_date_std),
            DEFAULT(c_datetime_std)
        FROM ${dateTimeTableName}
        LIMIT 1
    """

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

    qt_ip_defaults """
        SELECT
            DEFAULT(c_ipv4_localhost),
            DEFAULT(c_ipv4_private),
            DEFAULT(c_ipv6_localhost),
            DEFAULT(c_ipv6_example)
        FROM ${ipTableName}
        LIMIT 1
    """

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
}