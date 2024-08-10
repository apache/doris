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

suite("test_regr_sxx") {
    sql """ DROP TABLE IF EXISTS test_regr_sxx_int """
    sql """ DROP TABLE IF EXISTS test_regr_sxx_double """
    sql """ DROP TABLE IF EXISTS test_regr_sxx_nullalble_col """


    sql """ SET enable_nereids_planner=true """
    sql """ SET enable_fallback_to_original_planner=false """

    sql """
        CREATE TABLE test_regr_sxx_int (
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
        CREATE TABLE test_regr_sxx_double (
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
    sql """
        CREATE TABLE test_regr_sxx_nullalble_col (
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
    // no value
    qt_sql "select regr_sxx(y,x) from test_regr_sxx_int"
    sql """ truncate table test_regr_sxx_int """
    
    sql """
        insert into test_regr_sxx_int values
        (1, 18, 13),
        (2, 14, 27),
        (3, 12, 2),
        (4, 5, 6),
        (5, 10, 20);
        """

    sql """
        insert into test_regr_sxx_double values
        (1, 18.27123456, 13.27123456),
        (2, 14.65890846, 27.65890846),
        (3, 12.25345846, 2.253458468),
        (4, 5.890846835, 6.890846835),
        (5, 10.14345678, 20.14345678);
        """

    sql """
        insert into test_regr_sxx_nullalble_col values
        (1, 18, 13),
        (2, 14, 27),
        (3, 5, 7),
        (4, 10, 20);
        """

    // value is null
    sql """select regr_sxx(NULL, NULL);"""

    // parameter is literal and columns
    qt_sql "select regr_sxx(10,x) from test_regr_sxx_int"
    sql """ truncate table test_regr_sxx_int """

    // int value
    qt_sql "select regr_sxx(y,x) from test_regr_sxx_int"
    sql """ truncate table test_regr_sxx_int """

    // double value
    qt_sql "select regr_sxx(y,x) from test_regr_sxx_double"
    sql """ truncate table test_regr_sxx_double """

    // nullable and non_nullable
    qt_sql "select regr_sxx(y,non_nullable(x)) from test_regr_sxx_nullalble_col"

    // non_nullable and nullable
    qt_sql "select regr_sxx(non_nullable(y),x) from test_regr_sxx_nullalble_col"
    
    // non_nullable and non_nullable
    qt_sql "select regr_sxx(non_nullable(y),non_nullable(x)) from test_regr_sxx_nullalble_col"
    sql """ truncate table test_regr_sxx_nullalble_col """

    // exception test
	test{
		sql """select regr_sxx('range', 1);"""
		exception "regr_sxx requires numeric for first parameter"
	}

    test{
		sql """select regr_sxx(1, 'hello');"""
		exception "regr_sxx requires numeric for second parameter"
	}
    
    test{
		sql """select regr_sxx(y, 'hello') from test_regr_sxx_int;"""
		exception "regr_sxx requires numeric for second parameter"
	}

    test{
		sql """select regr_sxx(1, true);"""
		exception "regr_sxx requires numeric for second parameter"
	}

}
