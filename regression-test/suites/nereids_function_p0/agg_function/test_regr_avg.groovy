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

suite("test_regr_avg") {
    sql """ DROP TABLE IF EXISTS test_regr_avg_int """
    sql """ DROP TABLE IF EXISTS test_regr_avg_double """


    sql """ SET enable_nereids_planner=true """
    sql """ SET enable_fallback_to_original_planner=false """

    sql """
        CREATE TABLE test_regr_avg_int (
          `id` int,
          `x` int,
          `y` int,
        ) ENGINE=OLAP
        Duplicate KEY (`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 4
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
        """
    sql """
        CREATE TABLE test_regr_avg_double (
          `id` int,
          `x` double,
          `y` double,
        ) ENGINE=OLAP
        Duplicate KEY (`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 4
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
        """

    // empty table
    qt_sql "select regr_avgx(y, x) from test_regr_avg_int"
    qt_sql "select regr_avgy(y, x) from test_regr_avg_int"

    sql """
        insert into test_regr_avg_int values
        (1, 1, 18),
        (2, 6, 7),
        (3, 13, 3),
        (4, 9, 6),
        (5, 4, 7),
        (6, 2, null)
        """

    sql """
        insert into test_regr_avg_double values
        (1, 1.78444821, 3.84632932),
        (2, 8.24962937, 2.86328742),
        (3, 12.25345846, 8.83742920),
        (4, 5.890846835, 6.94757922),
        (5, 10.32902433, 9.93725392)
        """

    // regr_avgx
    qt_sql "select regr_avgx(y, x) from test_regr_avg_int"
    qt_sql "select regr_avgx(x, y) from test_regr_avg_int"

    qt_sql "select regr_avgx(y, x) from test_regr_avg_double"
    qt_sql "select regr_avgx(x, y) from test_regr_avg_double"

    // regr_avgy
    qt_sql "select regr_avgy(y, x) from test_regr_avg_int"
    qt_sql "select regr_avgy(x, y) from test_regr_avg_int"

    qt_sql "select regr_avgy(y, x) from test_regr_avg_double"
    qt_sql "select regr_avgy(x, y) from test_regr_avg_double"

    // non_nullable and non_nullable
    qt_sql "select regr_avgx(non_nullable(y), non_nullable(x)) from test_regr_avg_int"
    
    // nullable and non_nullable
    qt_sql "select regr_avgx(y, non_nullable(x)) from test_regr_avg_double"

    // exception test
    test{
        sql """select regr_avgx('regr_avgx', 5);"""
        exception "regr_avgx requires numeric for first parameter: regr_avgx('regr_avgx', 5)"
    }

    test{
        sql """select regr_avgx(5, 'regr_avgx');"""
        exception "regr_avgx requires numeric for second parameter: regr_avgx(5, 'regr_avgx')"
    }

    test{
        sql """select regr_avgy('regr_avgy', 5);"""
        exception "regr_avgy requires numeric for first parameter: regr_avgy('regr_avgy', 5)"
    }

    test{
        sql """select regr_avgy(5, 'regr_avgy');"""
        exception "regr_avgy requires numeric for second parameter: regr_avgy(5, 'regr_avgy')"
    }
}
