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

suite("test_regr_avgx") {
    sql """ DROP TABLE IF EXISTS test_regr_avgx_int """
    sql """ DROP TABLE IF EXISTS test_regr_avgx_double """

    sql """ SET enable_nereids_planner=true """
    sql """ SET enable_fallback_to_original_planner=false """

    sql """
        CREATE TABLE test_regr_avgx_int (
            id INT,
            x INT,
            y INT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 4
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql """
        CREATE TABLE test_regr_avgx_double (
            id INT,
            x DOUBLE,
            y DOUBLE
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 4
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """

    // Empty table: verify NULL
    qt_sql_empty_1 "select regr_avgx(y, x) from test_regr_avgx_int"

    // Empty table with group by: verify empty result
    qt_sql_empty_2 "select regr_avgx(y, x) from test_regr_avgx_int group by id"

    // Base dataset (int)
    sql """
        INSERT INTO test_regr_avgx_int VALUES
            -- id=1: one row
            (1, 10, 20),

            -- id=2: multiple rows
            (2, 1, 2),
            (2, 2, 4),
            (2, 3, 6),

            -- id=3: contains NULL, will be filtered out
            (3, 1, NULL),
            (3, NULL, 2),
            (3, 2, 5),
            (3, 3, 7),

            -- id=4: all rows contain NULL, regr_avgx should be NULL
            (4, NULL, 1),
            (4, 2, NULL),

            -- id=5: constant x
            (5, 5, 1),
            (5, 5, 2),
            (5, 5, 3);
    """

    // Base dataset (double)
    sql """
        INSERT INTO test_regr_avgx_double VALUES
            -- id=1: one row
            (1, 10.5, 20.5),

            -- id=2: multiple rows
            (2, 1.1, 2.1),
            (2, 2.2, 4.2),
            (2, 3.3, 6.3),

            -- id=3: contains NULL, will be filtered out
            (3, 1.1, NULL),
            (3, NULL, 2.2),
            (3, 2.2, 5.2),
            (3, 3.3, 7.2),

            -- id=4: all rows contain NULL, regr_avgx should be NULL
            (4, NULL, 1.1),
            (4, 2.2, NULL),

            -- id=5: constant x
            (5, 5.5, 1.1),
            (5, 5.5, 2.2),
            (5, 5.5, 3.3);
    """

    // Integer inputs
    qt_sql_int_1 "select regr_avgx(y, x) from test_regr_avgx_int"

    // AVGX(x) = avg(x) over rows where both x and y are non-NULL
    qt_sql_int_2 """
        select id, regr_avgx(y, x), avg(x)
        from test_regr_avgx_int
        where x is not null and y is not null
        group by id
        order by id
    """

    // Mix non_nullable
    qt_sql_int_3 "select regr_avgx(non_nullable(y), non_nullable(x)) from test_regr_avgx_int where id = 2"

    // Literal
    qt_sql_int_4 "select regr_avgx(10, x) from test_regr_avgx_int where id = 2"
    qt_sql_int_5 "select regr_avgx(y, 3) from test_regr_avgx_int where id = 2"

    // All rows invalid (no valid x/y pairs): verify NULL
    qt_sql_int_6 "select regr_avgx(y, x) from test_regr_avgx_int where id = 4"

    // Double inputs
    qt_sql_double_1 "select regr_avgx(y, x) from test_regr_avgx_double"

    // AVGX(x) = avg(x) over rows where both x and y are non-NULL
    qt_sql_double_2 """
        select id, regr_avgx(y, x), avg(x)
        from test_regr_avgx_double
        where x is not null and y is not null
        group by id
        order by id
    """

    // Mix non_nullable
    qt_sql_double_3 "select regr_avgx(non_nullable(y), non_nullable(x)) from test_regr_avgx_double where id = 2"

    // Literal
    qt_sql_double_4 "select regr_avgx(10, x) from test_regr_avgx_double where id = 2"
    qt_sql_double_5 "select regr_avgx(y, 3) from test_regr_avgx_double where id = 2"

    // All rows invalid (no valid x/y pairs): verify NULL
    qt_sql_double_6 "select regr_avgx(y, x) from test_regr_avgx_double where id = 4"

    // String type inputs (compile-time cast only, no table needed)
    qt_sql_string_1 "select regr_avgx('5', '3')"
    qt_sql_string_2 "select regr_avgx(1, '3')"

    // Boolean type inputs
    qt_sql_bool_1 "select regr_avgx(true, false)"

    // NULL literal inputs
    qt_sql_null_1 "select regr_avgx(NULL, 1)"
    qt_sql_null_2 "select regr_avgx(1, NULL)"

    // Exception inputs
    test {
        sql """select regr_avgx(cast([1, 2, 3] as array<int>), 1);"""
        exception "Can not find the compatibility function signature: regr_avgx("
    }
    test {
        sql """select regr_avgx(1, cast([1, 2, 3] as array<int>));"""
        exception "Can not find the compatibility function signature: regr_avgx("
    }
}
