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

suite("test_regr_slope") {
    sql """ DROP TABLE IF EXISTS test_regr_slope_int """
    sql """ DROP TABLE IF EXISTS test_regr_slope_double """
    sql """ DROP TABLE IF EXISTS test_regr_slope_nullable_col """


    sql """ SET enable_nereids_planner=true """
    sql """ SET enable_fallback_to_original_planner=false """

    sql """
        CREATE TABLE test_regr_slope_int (
          `id` int,
          `x` int,
          `y` int
        ) ENGINE=OLAP
        DUPLICATE KEY (`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 4
        PROPERTIES (
          "replication_allocation" = "tag.location.default: 1"
        );
        """
    sql """
        CREATE TABLE test_regr_slope_double (
          `id` int,
          `x` double,
          `y` double
        ) ENGINE=OLAP
        DUPLICATE KEY (`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 4
        PROPERTIES (
          "replication_allocation" = "tag.location.default: 1"
        );
        """
    
    // no value
    // agg function without group by should return null
    qt_sql_empty_1 "select regr_slope(y,x) from test_regr_slope_int"
    // agg function with group by should return empty set
    qt_sql_empty_2 "select regr_slope(y,x) from test_regr_slope_int group by id"

    sql """ TRUNCATE TABLE test_regr_slope_int """

    sql """
        INSERT INTO test_regr_slope_int VALUES
        (1, 18, 13),
        (2, 14, 27),
        (3, 12, 2),
        (4, 5, 6),
        (5, 10, 20);
        """

    sql """
        INSERT INTO test_regr_slope_double VALUES
        (1, 18.27123456, 13.27123456),
        (2, 14.65890846, 27.65890846),
        (3, 12.25345846, 2.253458468),
        (4, 5.890846835, 6.890846835),
        (5, 10.14345678, 20.14345678);
        """

    // parameter is literal and columns
    // _xxx can help use to distinguish the result
    qt_sql_int_1 "select regr_slope(10, x) from test_regr_slope_int"

    // column and literal
    qt_sql_int_2 "select regr_slope(x, 4) from test_regr_slope_int"

    // int value
    qt_sql_int_3 "select regr_slope(y,x) from test_regr_slope_int"

    // qt_sql_int_3 tests Nullable input column, qt_sql_int_4 test non-Nullable input column
    qt_sql_int_4 "select regr_slope(non_nullable(y), non_nullable(x)) from test_regr_slope_int"
    // qt_sql_int_4 tests mix nullable 
    qt_sql_int_5 "select regr_slope(y, non_nullable(x)) from test_regr_slope_int"

    sql """ TRUNCATE TABLE test_regr_slope_int """

    // Insert random null values
    sql """
      INSERT INTO test_regr_slope_int VALUES
        (1, 18, 13),
        (1, NULL, 14),
        (1, 19, NULL),
        (2, 14, 27),
        (2, NULL, NULL);
    """

    // Insert random value
    // group of id = 3 shoud NOT be null
    // group of id = 4 || 5 shoud be null since its size is 1
    sql """
      INSERT INTO test_regr_slope_int VALUES
        (3, 12, 2),
        (3, 13, 10),
        (3, 14, 20),
        (4, 5, 6),
        (5, 10, 20);
    """

    // parameter is literal and columns
    // _xxx can help use to distinguish the result
    qt_sql_int_6 "select regr_slope(10, x) from test_regr_slope_int"
  
    // column and literal
    qt_sql_int_7 "select regr_slope(x, 4) from test_regr_slope_int"

    // int value
    qt_sql_int_8 "select regr_slope(y,x) from test_regr_slope_int"
    qt_sql_int_8 "select regr_slope(y,x) from test_regr_slope_int group by id order by id"

    // qt_sql_int_3 tests Nullable input column, qt_sql_int_4 test non-Nullable input column
    qt_sql_int_9 "select regr_slope(non_nullable(y), non_nullable(x)) from test_regr_slope_int where id >= 3"
    qt_sql_int_9 "select regr_slope(non_nullable(y), non_nullable(x)) from test_regr_slope_int where id >= 3 group by id order by id"
    // qt_sql_int_4 tests mix nullable 
    qt_sql_int_10 "select regr_slope(y, non_nullable(x)) from test_regr_slope_int where id >= 3"
    qt_sql_int_10 "select regr_slope(y, non_nullable(x)) from test_regr_slope_int where id >= 3 group by id order by id"

    //// Repeat same thing for double ////

    // parameter is literal and columns
    // _xxx can help use to distinguish the result
    qt_sql_double_1 "select regr_slope(10, x) from test_regr_slope_double"

    // column and literal
    qt_sql_double_2 "select regr_slope(x, 4) from test_regr_slope_double"

    // int value
    qt_sql_double_3 "select regr_slope(y,x) from test_regr_slope_double"
    qt_sql_double_3 "select regr_slope(y,x) from test_regr_slope_double group by id order by id"

    // qt_sql_int_3 tests Nullable input column, qt_sql_int_4 test non-Nullable input column
    qt_sql_double_4 "select regr_slope(non_nullable(y), non_nullable(x)) from test_regr_slope_double"
    qt_sql_double_4 "select regr_slope(non_nullable(y), non_nullable(x)) from test_regr_slope_double group by id order by id"
    // qt_sql_int_4 tests mix nullable 
    qt_sql_double_5 "select regr_slope(y, non_nullable(x)) from test_regr_slope_double"

    sql """ TRUNCATE TABLE test_regr_slope_double """

    // Insert random null values
    sql """
      INSERT INTO test_regr_slope_double VALUES
        (1, 18, 13),
        (1, NULL, 14),
        (1, 19, NULL),
        (2, 14, 27),
        (2, NULL, NULL);
    """

    // Insert random value
    // group of id = 3 shoud NOT be null
    // group of id = 4 || 5 shoud be null since its size is 1
    sql """
      INSERT INTO test_regr_slope_double VALUES
        (3, 12, 2),
        (3, 13, 10),
        (3, 14, 20),
        (4, 5, 6),
        (5, 10, 20);
    """

    // parameter is literal and columns
    // _xxx can help use to distinguish the result
    qt_sql_double_6 "select regr_slope(10, x) from test_regr_slope_double"

    // column and literal
    qt_sql_double_7 "select regr_slope(x, 4) from test_regr_slope_double"

    // int value
    qt_sql_double_8 "select regr_slope(y,x) from test_regr_slope_double"
    qt_sql_double_8 "select regr_slope(y,x) from test_regr_slope_double group by id order by id"
    
    // qt_sql_int_3 tests Nullable input column, qt_sql_int_4 test non-Nullable input column
    qt_sql_double_9 "select regr_slope(non_nullable(y), non_nullable(x)) from test_regr_slope_double where id >= 3"
    qt_sql_double_9 "select regr_slope(non_nullable(y), non_nullable(x)) from test_regr_slope_double where id >= 3 group by id order by id"
    // qt_sql_int_4 tests mix nullable 
    qt_sql_double_10 "select regr_slope(y, non_nullable(x)) from test_regr_slope_double where id >= 3"
    qt_sql_double_10 "select regr_slope(y, non_nullable(x)) from test_regr_slope_double where id >= 3 group by id order by id"

    // exception test
    test{
        sql """select regr_slope('range', 1);"""
        exception "regr_slope requires numeric for first parameter"
    }

    test{
        sql """select regr_slope(1, 'hello');"""
        exception "regr_slope requires numeric for second parameter"
    }

    test{
        sql """select regr_slope(y, 'hello') from test_regr_slope_int;"""
        exception "regr_slope requires numeric for second parameter"
    }

    test{
        sql """select regr_slope(1, true);"""
        exception "regr_slope requires numeric for second parameter"
    }

}
