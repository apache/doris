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
    sql """ DROP TABLE IF EXISTS test_geomean_double """

    sql """ SET enable_nereids_planner=true """
    sql """ SET enable_fallback_to_original_planner=false """

    // Create test tables
    sql """
        CREATE TABLE test_geomean_int (
          `id` INT,
          `value` INT
        ) ENGINE=OLAP
        DUPLICATE KEY (`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 4
        PROPERTIES (
          "replication_allocation" = "tag.location.default: 1"
        );
    """
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

    // Test empty table
    // agg function without group by should return NULL
    qt_sql_empty_1 "SELECT GEOMEAN(value) FROM test_geomean_int"
    // agg function with group by should return empty set
    qt_sql_empty_2 "SELECT GEOMEAN(value) FROM test_geomean_int GROUP BY id"

    // Insert test data for INT
    sql """
        INSERT INTO test_geomean_int VALUES
        (1, 2),
        (1, 4),
        (1, 8),
        (2, 0),
        (2, 16),
        (3, 2147483647), -- Max INT
        (4, 1),
        (5, -1), -- Negative value
        (6, -2),
        (6, -8), -- Multiple negative values
        (7, 0),
        (7, -1); -- Zero and negative
    """

    // Insert test data for DOUBLE
    sql """
        INSERT INTO test_geomean_double VALUES
        (1, 2.5),
        (1, 4.75),
        (1, 8.25),
        (2, 0.0),
        (2, 16.5),
        (3, 1.7976931348623157e308), -- Max DOUBLE
        (4, 2.2250738585072014e-308), -- Min positive DOUBLE
        (5, -1.5), -- Negative value
        (6, -2.5),
        (6, -8.25), -- Multiple negative values
        (7, 0.0),
        (7, -1.5); -- Zero and negative
    """

    // Test INT values
    qt_sql_int_1 "SELECT GEOMEAN(value) FROM test_geomean_int"
    qt_sql_int_2 "SELECT GEOMEAN(non_nullable(value)) FROM test_geomean_int WHERE value IS NOT NULL"
    qt_sql_int_3 """
        SELECT id, GEOMEAN(value) AS geometric_mean
        FROM test_geomean_int
        GROUP BY id
        ORDER BY id
    """
    qt_sql_int_4 """
        SELECT id, GEOMEAN(non_nullable(value)) AS geometric_mean
        FROM test_geomean_int
        WHERE value IS NOT NULL
        GROUP BY id
        ORDER BY id
    """

    // Expected results for qt_sql_int_3 and qt_sql_int_4:
    // id=1: [2, 4, 8] -> (2 * 4 * 8)^(1/3) = 4.0
    // id=2: [0, 16] -> 0.0
    // id=3: [2147483647] -> 2147483647
    // id=4: [1] -> 1
    // id=5: [-1] -> NaN
    // id=6: [-2, -8] -> NaN
    // id=7: [0, -1] -> NaN

    // Test DOUBLE values
    qt_sql_double_1 "SELECT GEOMEAN(value) FROM test_geomean_double"
    qt_sql_double_2 "SELECT GEOMEAN(non_nullable(value)) FROM test_geomean_double WHERE value IS NOT NULL"
    qt_sql_double_3 """
        SELECT id, GEOMEAN(value) AS geometric_mean
        FROM test_geomean_double
        GROUP BY id
        ORDER BY id
    """
    qt_sql_double_4 """
        SELECT id, GEOMEAN(non_nullable(value)) AS geometric_mean
        FROM test_geomean_double
        WHERE value IS NOT NULL
        GROUP BY id
        ORDER BY id
    """

    // Expected results for qt_sql_double_3 and qt_sql_double_4:
    // id=1: [2.5, 4.75, 8.25] -> (2.5 * 4.75 * 8.25)^(1/3) ≈ 4.614
    // id=2: [0.0, 16.5] -> 0.0
    // id=3: [1.7976931348623157e308] -> 1.7976931348623157e308
    // id=4: [2.2250738585072014e-308] -> 2.2250738585072014e-308
    // id=5: [-1.5] -> NaN
    // id=6: [-2.5, -8.25] -> NaN
    // id=7: [0.0, -1.5] -> NaN

    // Test with NULL values
    sql """ TRUNCATE TABLE test_geomean_int """
    sql """
        INSERT INTO test_geomean_int VALUES
        (1, 2),
        (1, NULL),
        (1, 8),
        (2, NULL),
        (2, NULL),
        (3, 4),
        (3, 16),
        (4, 0),
        (5, -1),
        (6, -2),
        (6, -8),
        (7, 0),
        (7, -1);
    """

    sql """ TRUNCATE TABLE test_geomean_double """
    sql """
        INSERT INTO test_geomean_double VALUES
        (1, 2.5),
        (1, NULL),
        (1, 8.25),
        (2, NULL),
        (2, NULL),
        (3, 4.75),
        (3, 16.5),
        (4, 0.0),
        (5, -1.5),
        (6, -2.5),
        (6, -8.25),
        (7, 0.0),
        (7, -1.5);
    """

    // Test INT with NULL
    qt_sql_int_null_1 "SELECT GEOMEAN(value) FROM test_geomean_int"
    qt_sql_int_null_2 "SELECT GEOMEAN(non_nullable(value)) FROM test_geomean_int WHERE value IS NOT NULL"
    qt_sql_int_null_3 """
        SELECT id, GEOMEAN(value) AS geometric_mean
        FROM test_geomean_int
        GROUP BY id
        ORDER BY id
    """
    qt_sql_int_null_4 """
        SELECT id, GEOMEAN(non_nullable(value)) AS geometric_mean
        FROM test_geomean_int
        WHERE value IS NOT NULL
        GROUP BY id
        ORDER BY id
    """

    // Expected results for qt_sql_int_null_3 and qt_sql_int_null_4:
    // id=1: [2, NULL, 8] -> (2 * 8)^(1/2) = 4.0
    // id=2: [NULL, NULL] -> NULL
    // id=3: [4, 16] -> (4 * 16)^(1/2) = 8.0
    // id=4: [0] -> 0.0
    // id=5: [-1] -> NaN
    // id=6: [-2, -8] -> NaN
    // id=7: [0, -1] -> NaN

    // Test DOUBLE with NULL
    qt_sql_double_null_1 "SELECT GEOMEAN(value) FROM test_geomean_double"
    qt_sql_double_null_2 "SELECT GEOMEAN(non_nullable(value)) FROM test_geomean_double WHERE value IS NOT NULL"
    qt_sql_double_null_3 """
        SELECT id, GEOMEAN(value) AS geometric_mean
        FROM test_geomean_double
        GROUP BY id
        ORDER BY id
    """
    qt_sql_double_null_4 """
        SELECT id, GEOMEAN(non_nullable(value)) AS geometric_mean
        FROM test_geomean_double
        WHERE value IS NOT NULL
        GROUP BY id
        ORDER BY id
    """

    // Expected results for qt_sql_double_null_3 and qt_sql_double_null_4:
    // id=1: [2.5, NULL, 8.25] -> (2.5 * 8.25)^(1/2) ≈ 4.541
    // id=2: [NULL, NULL] -> NULL
    // id=3: [4.75, 16.5] -> (4.75 * 16.5)^(1/2) ≈ 8.854
    // id=4: [0.0] -> 0.0
    // id=5: [-1.5] -> NaN
    // id=6: [-2.5, -8.25] -> NaN
    // id=7: [0.0, -1.5] -> NaN

    // Test invalid parameter types
    test {
        sql """SELECT GEOMEAN('invalid')"""
        exception "GEOMEAN requires numeric parameter"
    }
    test {
        sql """SELECT GEOMEAN(true)"""
        exception "GEOMEAN requires numeric parameter"
    }
    test {
        sql """SELECT GEOMEAN(value, 1) FROM test_geomean_int"""
        exception "GEOMEAN expects exactly one parameter"
    }
}