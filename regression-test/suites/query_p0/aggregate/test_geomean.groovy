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

suite("test_geomean") {
    sql """ DROP TABLE IF EXISTS test_geomean_int """
    sql """ DROP TABLE IF EXISTS test_geomean_bigint """
    sql """ DROP TABLE IF EXISTS test_geomean_smallint """
    sql """ DROP TABLE IF EXISTS test_geomean_tinyint """
    sql """ DROP TABLE IF EXISTS test_geomean_float """
    sql """ DROP TABLE IF EXISTS test_geomean_double """
    sql """ DROP TABLE IF EXISTS test_geomean_single """

    sql """ SET enable_nereids_planner=true """
    sql """ SET enable_fallback_to_original_planner=false """

    // Create test tables
    sql """
        CREATE TABLE test_geomean_int (
          `id` INT,
          `value` INT
        );
    """
    sql """
        CREATE TABLE test_geomean_bigint (
          `id` INT,
          `value` BIGINT
        );
    """
    sql """
        CREATE TABLE test_geomean_smallint (
          `id` INT,
          `value` SMALLINT
        );
    """
    sql """
        CREATE TABLE test_geomean_tinyint (
          `id` INT,
          `value` TINYINT
        );
    """
    sql """
        CREATE TABLE test_geomean_float (
          `id` INT,
          `value` FLOAT
        );
    """
    sql """
        CREATE TABLE test_geomean_double (
          `id` INT,
          `value` DOUBLE
        );
    """
    sql """
        CREATE TABLE test_geomean_single (
          `id` INT,
          `value` DOUBLE
        );
    """

    // Test empty table
    qt_sql_empty_1 """ SELECT GEOMEAN(value) FROM test_geomean_int """
    // Expected: NULL (no non-NULL values)

    // Insert test data for INT
    sql """ INSERT INTO test_geomean_int VALUES
        (1, 2), (1, 4), (1, 8), -- Positive values
        (2, 0), (2, 16), -- Contains zero
        (3, -1), -- Single negative
        (4, -2), (4, -8), -- Two negatives (even)
        (5, -2), (5, -8), (5, -4), -- Three negatives (odd)
        (6, 2147483647), -- Max INT
        (7, NULL) -- Only NULL
    """

    // Insert test data for BIGINT
    sql """ INSERT INTO test_geomean_bigint VALUES
        (1, 2), (1, 4), (1, 8), -- Positive values
        (2, 0), (2, 16), -- Contains zero
        (3, -1), -- Single negative
        (4, -2), (4, -8), -- Two negatives (even)
        (5, -2), (5, -8), (5, -4), -- Three negatives (odd)
        (6, 9223372036854775807), -- Max BIGINT
        (7, NULL) -- Only NULL
    """

    // Insert test data for SMALLINT
    sql """ INSERT INTO test_geomean_smallint VALUES
        (1, 2), (1, 4), (1, 8), -- Positive values
        (2, 0), (2, 16), -- Contains zero
        (3, -1), -- Single negative
        (4, -2), (4, -8), -- Two negatives (even)
        (5, -2), (5, -8), (5, -4), -- Three negatives (odd)
        (6, 32767), -- Max SMALLINT
        (7, NULL) -- Only NULL
    """

    // Insert test data for TINYINT
    sql """ INSERT INTO test_geomean_tinyint VALUES
        (1, 2), (1, 4), (1, 8), -- Positive values
        (2, 0), (2, 16), -- Contains zero
        (3, -1), -- Single negative
        (4, -2), (4, -8), -- Two negatives (even)
        (5, -2), (5, -8), (5, -4), -- Three negatives (odd)
        (6, 127), -- Max TINYINT
        (7, NULL) -- Only NULL
    """

    // Insert test data for FLOAT
    sql """ INSERT INTO test_geomean_float VALUES
        (1, 2.5), (1, 4.75), (1, 8.25), -- Positive values
        (2, 0.0), (2, 16.5), -- Contains zero
        (3, -1.5), -- Single negative
        (4, -2.5), (4, -8.25), -- Two negatives (even)
        (5, -2.5), (5, -8.25), (5, -4.75), -- Three negatives (odd)
        (6, 3.4028235e38), -- Max FLOAT
        (7, NULL) -- Only NULL
    """

    // Insert test data for DOUBLE
    sql """ INSERT INTO test_geomean_double VALUES
        (1, 2.5), (1, 4.75), (1, 8.25), -- Positive values
        (2, 0.0), (2, 16.5), -- Contains zero
        (3, -1.5), -- Single negative
        (4, -2.5), (4, -8.25), -- Two negatives (even)
        (5, -2.5), (5, -8.25), (5, -4.75), -- Three negatives (odd)
        (6, 1.7976931348623157e308), -- Max DOUBLE
        (7, 2.2250738585072014e-308), -- Min positive DOUBLE
        (8, NULL) -- Only NULL
    """

    // Insert test data for single row
    sql """ INSERT INTO test_geomean_single VALUES
        (1, 5.0) -- Single positive value
    """

    // Test INT values
    qt_sql_int_1 """ SELECT GEOMEAN(value) FROM test_geomean_int WHERE id = 1 """
    // Expected: (2 * 4 * 8)^(1/3) = 4.0
    qt_sql_int_2 """ SELECT GEOMEAN(value) FROM test_geomean_int WHERE id = 2 """
    // Expected: 0.0 (contains zero)
    qt_sql_int_3 """ SELECT GEOMEAN(value) FROM test_geomean_int WHERE id = 3 """
    // Expected: -1 (single negative)
    qt_sql_int_4 """ SELECT GEOMEAN(value) FROM test_geomean_int WHERE id = 4 """
    // Expected: (2 * 8)^(1/2) = 4.0 (two negatives, even)
    qt_sql_int_5 """ SELECT GEOMEAN(value) FROM test_geomean_int WHERE id = 5 """
    // Expected: -(2 * 8 * 4)^(1/3) = -4.0 (three negatives, odd)
    qt_sql_int_6 """ SELECT GEOMEAN(value) FROM test_geomean_int WHERE id = 6 """
    // Expected: 2147483647.0000021
    qt_sql_int_7 """ SELECT GEOMEAN(value) FROM test_geomean_int WHERE id = 7 """
    // Expected: NULL (only NULL)

    // Test BIGINT values
    qt_sql_bigint_1 """ SELECT GEOMEAN(value) FROM test_geomean_bigint WHERE id = 1 """
    // Expected: (2 * 4 * 8)^(1/3) = 4.0
    qt_sql_bigint_2 """ SELECT GEOMEAN(value) FROM test_geomean_bigint WHERE id = 2 """
    // Expected: 0.0 (contains zero)
    qt_sql_bigint_3 """ SELECT GEOMEAN(value) FROM test_geomean_bigint WHERE id = 3 """
    // Expected: -1 (single negative)
    qt_sql_bigint_4 """ SELECT GEOMEAN(value) FROM test_geomean_bigint WHERE id = 4 """
    // Expected: (2 * 8)^(1/2) = 4.0 (two negatives, even)
    qt_sql_bigint_5 """ SELECT GEOMEAN(value) FROM test_geomean_bigint WHERE id = 5 """
    // Expected: -(2 * 8 * 4)^(1/3) = -4.0 (three negatives, odd)
    qt_sql_bigint_6 """ SELECT GEOMEAN(value) FROM test_geomean_bigint WHERE id = 6 """
    // Expected: 9.223372036854745e+18
    qt_sql_bigint_7 """ SELECT GEOMEAN(value) FROM test_geomean_bigint WHERE id = 7 """
    // Expected: NULL (only NULL)

    // Test SMALLINT values
    qt_sql_smallint_1 """ SELECT GEOMEAN(value) FROM test_geomean_smallint WHERE id = 1 """
    // Expected: (2 * 4 * 8)^(1/3) = 4.0
    qt_sql_smallint_2 """ SELECT GEOMEAN(value) FROM test_geomean_smallint WHERE id = 2 """
    // Expected: 0.0 (contains zero)
    qt_sql_smallint_3 """ SELECT GEOMEAN(value) FROM test_geomean_smallint WHERE id = 3 """
    // Expected: -1 (single negative)
    qt_sql_smallint_4 """ SELECT GEOMEAN(value) FROM test_geomean_smallint WHERE id = 4 """
    // Expected: (2 * 8)^(1/2) = 4.0 (two negatives, even)
    qt_sql_smallint_5 """ SELECT GEOMEAN(value) FROM test_geomean_smallint WHERE id = 5 """
    // Expected: -(2 * 8 * 4)^(1/3) = -4.0 (three negatives, odd)
    qt_sql_smallint_6 """ SELECT GEOMEAN(value) FROM test_geomean_smallint WHERE id = 6 """
    // Expected: 32767.000000000004
    qt_sql_smallint_7 """ SELECT GEOMEAN(value) FROM test_geomean_smallint WHERE id = 7 """
    // Expected: NULL (only NULL)

    // Test TINYINT values
    qt_sql_tinyint_1 """ SELECT GEOMEAN(value) FROM test_geomean_tinyint WHERE id = 1 """
    // Expected: (2 * 4 * 8)^(1/3) = 4.0
    qt_sql_tinyint_2 """ SELECT GEOMEAN(value) FROM test_geomean_tinyint WHERE id = 2 """
    // Expected: 0.0 (contains zero)
    qt_sql_tinyint_3 """ SELECT GEOMEAN(value) FROM test_geomean_tinyint WHERE id = 3 """
    // Expected: -1 (single negative)
    qt_sql_tinyint_4 """ SELECT GEOMEAN(value) FROM test_geomean_tinyint WHERE id = 4 """
    // Expected: (2 * 8)^(1/2) = 4.0 (two negatives, even)
    qt_sql_tinyint_5 """ SELECT GEOMEAN(value) FROM test_geomean_tinyint WHERE id = 5 """
    // Expected: -(2 * 8 * 4)^(1/3) = -4.0 (three negatives, odd)
    qt_sql_tinyint_6 """ SELECT GEOMEAN(value) FROM test_geomean_tinyint WHERE id = 6 """
    // Expected: 126.99999999999999
    qt_sql_tinyint_7 """ SELECT GEOMEAN(value) FROM test_geomean_tinyint WHERE id = 7 """
    // Expected: NULL (only NULL)

    // Test FLOAT values
    qt_sql_float_1 """ SELECT GEOMEAN(value) FROM test_geomean_float WHERE id = 1 """
    // Expected: (2.5 * 4.75 * 8.25)^(1/3) ≈ 4.609946185082068
    qt_sql_float_2 """ SELECT GEOMEAN(value) FROM test_geomean_float WHERE id = 2 """
    // Expected: 0.0 (contains zero)
    qt_sql_float_3 """ SELECT GEOMEAN(value) FROM test_geomean_float WHERE id = 3 """
    // Expected: -1.5 (single negative)
    qt_sql_float_4 """ SELECT GEOMEAN(value) FROM test_geomean_float WHERE id = 4 """
    // Expected: (2.5 * 8.25)^(1/2) ≈ 4.541475531146237
    qt_sql_float_5 """ SELECT GEOMEAN(value) FROM test_geomean_float WHERE id = 5 """
    // Expected: -(2.5 * 8.25 * 4.75)^(1/3) ≈ -4.609946185082068
    qt_sql_float_6 """ SELECT GEOMEAN(value) FROM test_geomean_float WHERE id = 6 """
    // Expected: 3.4028234663852844e+38
    qt_sql_float_7 """ SELECT GEOMEAN(value) FROM test_geomean_float WHERE id = 7 """
    // Expected: NULL (only NULL)

    // Test DOUBLE values
    qt_sql_double_1 """ SELECT GEOMEAN(value) FROM test_geomean_double WHERE id = 1 """
    // Expected: (2.5 * 4.75 * 8.25)^(1/3) ≈ 4.609946185082068
    qt_sql_double_2 """ SELECT GEOMEAN(value) FROM test_geomean_double WHERE id = 2 """
    // Expected: 0.0 (contains zero)
    qt_sql_double_3 """ SELECT GEOMEAN(value) FROM test_geomean_double WHERE id = 3 """
    // Expected: -1.5 (single negative)
    qt_sql_double_4 """ SELECT GEOMEAN(value) FROM test_geomean_double WHERE id = 4 """
    // Expected: (2.5 * 8.25)^(1/2) ≈ 4.541475531146237
    qt_sql_double_5 """ SELECT GEOMEAN(value) FROM test_geomean_double WHERE id = 5 """
    // Expected: -(2.5 * 8.25 * 4.75)^(1/3) ≈ -4.609946185082068
    qt_sql_double_6 """ SELECT GEOMEAN(value) FROM test_geomean_double WHERE id = 6 """
    // Expected: 1.7976931348622732e+308
    qt_sql_double_7 """ SELECT GEOMEAN(value) FROM test_geomean_double WHERE id = 7 """
    // Expected: 2.2250738585072626e-308
    qt_sql_double_8 """ SELECT GEOMEAN(value) FROM test_geomean_double WHERE id = 8 """
    // Expected: NULL (only NULL)

    // Test single row
    qt_sql_single_1 """ SELECT GEOMEAN(value) FROM test_geomean_single """
    // Expected: 4.999999999999999

    // Test scalar corner cases
    qt_sql_scalar_1 """ SELECT GEOMEAN(1) """
    // Expected: 1.0
    qt_sql_scalar_2 """ SELECT GEOMEAN(-1) """
    // Expected: -1.0
    qt_sql_scalar_3 """ SELECT GEOMEAN(0) """
    // Expected: 0.0
    qt_sql_scalar_4 """ SELECT GEOMEAN(1.7976931348623157e308) """
    // Expected: 1.7976931348622732e+308
    qt_sql_scalar_5 """ SELECT GEOMEAN(2.2250738585072014e-308) """
    // Expected: 2.2250738585072626e-308
    qt_sql_scalar_6 """ SELECT GEOMEAN(CAST(9223372036854775807 AS BIGINT)) """
    // Expected: 9.223372036854745e+18
    qt_sql_scalar_7 """ SELECT GEOMEAN(CAST(32767 AS SMALLINT)) """
    // Expected: 32767.000000000004
    qt_sql_scalar_8 """ SELECT GEOMEAN(CAST(127 AS TINYINT)) """
    // Expected: 126.99999999999999
    qt_sql_scalar_9 """ SELECT GEOMEAN(CAST(3.4028235e38 AS FLOAT)) """
    // Expected: 3.4028234663852844e+38

    // Test invalid parameter types
    test {
        sql """ SELECT GEOMEAN('invalid') """
        exception "GEOMEAN requires numeric parameter"
    }
    test {
        sql """ SELECT GEOMEAN(true) """
        exception "GEOMEAN requires numeric parameter"
    }
    test {
        sql """ SELECT GEOMEAN(value, 1) FROM test_geomean_int """
        exception "Can not found function 'GEOMEAN' which has 2 arity. Candidate functions are: [GEOMEAN(Expression)]"
    }

    // Test odd number of negatives with even n
    sql """ INSERT INTO test_geomean_double VALUES
        (9, -1.0), (9, 1.0),
        (10, -1.0), (10, -1.0), (10, 1.0), (10, 1.0)
    """

    test {
        sql """ SELECT GEOMEAN(value) FROM test_geomean_double WHERE id = 9 """
        exception "Geometric mean is undefined for odd number of negatives with even n"
    }
}