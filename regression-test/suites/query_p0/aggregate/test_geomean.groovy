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
    sql """ DROP TABLE IF EXISTS test_geomean_double """
    sql """ DROP TABLE IF EXISTS test_geomean_single """

    sql """ SET enable_nereids_planner=true """
    sql """ SET enable_fallback_to_original_planner=false """

    // Create test tables
    sql """
        CREATE TABLE test_geomean_double (
          `id` INT,
          `value` DOUBLE
        ) ENGINE=OLAP
        DUPLICATE KEY (`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 4
        PROPERTIES (
          "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql """
        CREATE TABLE test_geomean_single (
          `id` INT,
          `value` DOUBLE
        ) ENGINE=OLAP
        DUPLICATE KEY (`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 4
        PROPERTIES (
          "replication_allocation" = "tag.location.default: 1"
        );
    """

    // Test empty table
    qt_sql_empty_1 """ SELECT GEOMEAN(value) FROM test_geomean_double """
    // Expected: NULL (no non-NULL values)

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

    // Test invalid parameter types
    test {
        sql """ SELECT GEOMEAN('invalid') """
        exception "GEOMEAN requires dobule parameter"
    }
    test {
        sql """ SELECT GEOMEAN(true) """
        exception "GEOMEAN requires dobule parameter"
    }
    test {
        sql """ SELECT GEOMEAN(value, 1) FROM test_geomean_double """
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