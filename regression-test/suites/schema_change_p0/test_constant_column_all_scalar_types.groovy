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

suite("test_constant_column_all_scalar_types") {
    sql "SET enable_decimal256 = true"
    sql "SET time_zone = '+00:00'"

    sql "DROP TABLE IF EXISTS test_constant_column_all_scalar_types"
    sql """
        CREATE TABLE test_constant_column_all_scalar_types (
            id INT NOT NULL,
            generation VARCHAR(16) NOT NULL
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "light_schema_change" = "true",
            "disable_auto_compaction" = "true"
        )
    """

    // This rowset physically contains none of the columns added below. Every value read from it
    // must therefore be materialized by ConstantColumnReader from the latest tablet schema.
    sql """
        INSERT INTO test_constant_column_all_scalar_types VALUES
            (1, 'old'),
            (2, 'old')
    """

    sql """
        ALTER TABLE test_constant_column_all_scalar_types
            ADD COLUMN c_bool BOOLEAN NOT NULL DEFAULT "true",
            ADD COLUMN c_tinyint TINYINT NOT NULL DEFAULT "7",
            ADD COLUMN c_smallint SMALLINT NOT NULL DEFAULT "32000",
            ADD COLUMN c_int INT NOT NULL DEFAULT "123456789",
            ADD COLUMN c_bigint BIGINT NOT NULL DEFAULT "9223372036854775807",
            ADD COLUMN c_largeint LARGEINT NOT NULL
                DEFAULT "170141183460469231731687303715884105727",
            ADD COLUMN c_float FLOAT NOT NULL DEFAULT "3.125",
            ADD COLUMN c_double DOUBLE NOT NULL DEFAULT "2.718281828",
            ADD COLUMN c_decimalv2 DECIMALV2(27, 9) NOT NULL
                DEFAULT "123456789.123456789",
            ADD COLUMN c_decimal32 DECIMAL(9, 2) NOT NULL DEFAULT "1234567.89",
            ADD COLUMN c_decimal64 DECIMAL(18, 4) NOT NULL DEFAULT "12345678901234.5678",
            ADD COLUMN c_decimal128 DECIMAL(38, 9) NOT NULL
                DEFAULT "12345678901234567890123456789.123456789",
            ADD COLUMN c_decimal256 DECIMAL(76, 18) NOT NULL
                DEFAULT "1234567890123456789012345678901234567890123456789012345678.123456789012345678",
            ADD COLUMN c_date DATE NOT NULL DEFAULT "2025-01-02",
            ADD COLUMN c_datetime DATETIME NOT NULL DEFAULT "2025-01-02 03:04:05",
            ADD COLUMN c_datetimev2 DATETIMEV2(6) NOT NULL
                DEFAULT "2025-01-02 03:04:05.123456",
            ADD COLUMN c_timestamp_ns TIMESTAMP_NS NOT NULL
                DEFAULT "2025-01-02 03:04:05.123456789",
            ADD COLUMN c_timestamptz TIMESTAMPTZ(6) NOT NULL
                DEFAULT "2025-01-02 03:04:05.123456+00:00",
            ADD COLUMN c_char CHAR(8) NOT NULL DEFAULT "oldchar",
            ADD COLUMN c_varchar VARCHAR(32) NOT NULL DEFAULT "old-varchar",
            ADD COLUMN c_string STRING NOT NULL DEFAULT "old-string",
            ADD COLUMN c_ipv4 IPV4 NOT NULL DEFAULT "192.168.1.1",
            ADD COLUMN c_ipv6 IPV6 NOT NULL DEFAULT "2001:db8::1",
            ADD COLUMN c_json JSON NULL,
            ADD COLUMN c_array ARRAY<INT> NOT NULL DEFAULT "[]"
    """
    waitForSchemaChangeDone {
        sql """
            SHOW ALTER TABLE COLUMN
            WHERE TableName = 'test_constant_column_all_scalar_types'
            ORDER BY CreateTime DESC LIMIT 1
        """
        time 600
    }

    order_qt_old_integer_defaults """
        SELECT id, c_bool, c_tinyint, c_smallint, c_int, c_bigint, c_largeint
        FROM test_constant_column_all_scalar_types
        ORDER BY id
    """
    order_qt_old_floating_and_decimal_defaults """
        SELECT id, c_float, c_double, c_decimalv2, c_decimal32, c_decimal64,
               c_decimal128, c_decimal256
        FROM test_constant_column_all_scalar_types
        ORDER BY id
    """
    order_qt_old_temporal_defaults """
        SELECT id, c_date, c_datetime, c_datetimev2, c_timestamp_ns, c_timestamptz
        FROM test_constant_column_all_scalar_types
        ORDER BY id
    """
    order_qt_old_string_ip_and_nullable_defaults """
        SELECT id, c_char, c_varchar, c_string, c_ipv4, c_ipv6,
               c_json IS NULL, c_array
        FROM test_constant_column_all_scalar_types
        ORDER BY id
    """

    // Write every added type physically with values distinct from the defaults, so the same scan
    // mixes constant-backed old rowsets and ordinary physical readers.
    sql """
        INSERT INTO test_constant_column_all_scalar_types (
            id, generation, c_bool, c_tinyint, c_smallint, c_int, c_bigint, c_largeint,
            c_float, c_double, c_decimalv2, c_decimal32, c_decimal64, c_decimal128,
            c_decimal256, c_date, c_datetime, c_datetimev2, c_timestamp_ns, c_timestamptz,
            c_char, c_varchar, c_string, c_ipv4, c_ipv6, c_json, c_array
        ) VALUES (
            3, 'new', false, -7, -32000, -123456789, -9223372036854775807, -123456789,
            1.25, 9.5, -987654321.987654321, -12345.67, -12345678901234.5678,
            -12345678901234567890123456789.123456789,
            -1234567890123456789012345678901234567890123456789012345678.123456789012345678,
            '2026-02-03', '2026-02-03 04:05:06', '2026-02-03 04:05:06.654321',
            '2026-02-03 04:05:06.987654321', '2026-02-03 04:05:06.654321+00:00',
            'newchar', 'new-varchar', 'new-string', '10.0.0.1', '2001:db8::2',
            '{"k":3}', [3, 4]
        )
    """

    order_qt_mixed_integer_values """
        SELECT id, generation, c_bool, c_tinyint, c_smallint, c_int, c_bigint, c_largeint
        FROM test_constant_column_all_scalar_types
        ORDER BY id
    """
    order_qt_mixed_floating_and_decimal_values """
        SELECT id, generation, c_float, c_double, c_decimalv2, c_decimal32, c_decimal64,
               c_decimal128, c_decimal256
        FROM test_constant_column_all_scalar_types
        ORDER BY id
    """
    order_qt_mixed_temporal_values """
        SELECT id, generation, c_date, c_datetime, c_datetimev2, c_timestamp_ns, c_timestamptz
        FROM test_constant_column_all_scalar_types
        ORDER BY id
    """
    order_qt_mixed_string_ip_and_nullable_values """
        SELECT id, generation, c_char, c_varchar, c_string, c_ipv4, c_ipv6,
               CAST(c_json AS STRING), c_array
        FROM test_constant_column_all_scalar_types
        ORDER BY id
    """

    order_qt_default_predicates_across_type_families """
        SELECT id
        FROM test_constant_column_all_scalar_types
        WHERE c_bool = true
          AND c_int = 123456789
          AND c_decimal128 = 12345678901234567890123456789.123456789
          AND c_timestamp_ns = '2025-01-02 03:04:05.123456789'
          AND c_timestamptz = '2025-01-02 03:04:05.123456+00:00'
          AND c_varchar = 'old-varchar'
          AND c_ipv4 = '192.168.1.1'
          AND c_ipv6 = '2001:db8::1'
          AND c_json IS NULL
          AND cardinality(c_array) = 0
        ORDER BY id
    """

    order_qt_physical_predicates_across_type_families """
        SELECT id
        FROM test_constant_column_all_scalar_types
        WHERE c_bool = false
          AND c_int < 0
          AND c_decimal128 < 0
          AND c_timestamp_ns > '2026-01-01 00:00:00.000000000'
          AND c_timestamptz > '2026-01-01 00:00:00+00:00'
          AND c_varchar = 'new-varchar'
          AND c_ipv4 = '10.0.0.1'
          AND c_ipv6 = '2001:db8::2'
          AND c_json IS NOT NULL
          AND cardinality(c_array) = 2
        ORDER BY id
    """

    sql "SET enable_decimal256 = false"
}
