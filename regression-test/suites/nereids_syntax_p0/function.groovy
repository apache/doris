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

suite("nereids_function") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

    order_qt_max """
        SELECT max(lo_discount), max(lo_extendedprice) AS max_extendedprice FROM lineorder;
    """

    order_qt_min """
        SELECT min(lo_discount), min(lo_extendedprice) AS min_extendedprice FROM lineorder;
    """

    order_qt_max_and_min """
        SELECT max(lo_extendedprice), min(lo_discount) FROM lineorder;
    """

    order_qt_count """
        SELECT count(c_city), count(*) AS custdist FROM customer;
    """

    order_qt_distinct_count """
        SELECT count(distinct c_custkey + 1) AS custdist FROM customer group by c_city;
    """

    order_qt_distinct_count_group_by_distributed_key """
        SELECT c_custkey, count(distinct c_custkey + 1) AS custdist FROM customer group by c_custkey;
    """

    order_qt_avg """
        SELECT avg(lo_tax), avg(lo_extendedprice) AS avg_extendedprice FROM lineorder;
    """

    qt_string_arithmetic """
        SELECT "1" + "2", "1" - "2";
    """

    // variable length function
    test {
        sql "select coalesce(1), coalesce(null, 2), coalesce(null, 3), coalesce(4, null, 2)"
        result([[1, 2, 3, 4]])
    }

    // nested function
    test {
        sql "select cast(date('1994-01-01') + interval '1' YEAR as varchar)"
        result([["1995-01-01"]])
    }

    test {
        sql "select substring(substring('1994-01-01', 5), 3)"
        result([["1-01"]])
    }

    // numbers: table valued function
    test {
        sql "select `number` from numbers(number = 10)"
        result([[0L], [1L], [2L], [3L], [4L], [5L], [6L], [7L], [8L], [9L]])
    }

    // numbers: table valued function
    test {
        sql """
            select
                a.number as num1, b.number as num2
            from
                numbers("number" = "10") a
                inner join numbers("number" = "10") b on a.number=b.number order by 1,2;
        """
        result([[0L, 0L], [1L, 1L], [2L, 2L], [3L, 3L], [4L, 4L], [5L, 5L], [6L, 6L], [7L, 7L], [8L, 8L], [9L, 9L]])
    }

    qt_subquery1 """ select * from numbers("number" = "10") where number = (select number from numbers("number" = "10") where number=1); """
    qt_subquery2 """ select * from numbers("number" = "10") where number in (select number from numbers("number" = "10") where number>5); """
    qt_subquery3 """ select a.number from numbers("number" = "10") a where number in (select number from numbers("number" = "10") b where a.number=b.number); """

    test {
        sql """select `number` from numbers("number" = -1, 'backend_num' = `1`)"""
        result([])
    }

    test {
        sql "select `number` from numbers(a=2)"
        exception "Can not build NumbersTableValuedFunction by numbers('a' = '2')"
    }

    test {
        sql """select a.`number` from numbers(number = 3) a"""
        result([[0L], [1L], [2L]])
    }

    test {
        sql """select b.number from (select * from numbers(number = 3) a)b"""
        result([[0L], [1L], [2L]])
    }

    test {
        sql "select from_unixtime(1249488000, 'yyyyMMdd')"
        result([["20090806"]])
    }

    test {
        sql "select convert_to('abc', cast(number as varchar)) from numbers('number'='1')"
        exception "must be a constant"
    }

    test {
        sql """select "1" == "123", "%%" == "%%" """
        result([[false, true]])
    }
    
    qt_floor """
        SELECT floor(2.1);
    """

    qt_ceil """
        SELECT ceil(2.1);
    """

    test {
        sql "select left('abcd', 3), right('abcd', 3)"
        result([['abc', 'bcd']])
    }
    
    // test window_funnel function
    sql """ DROP TABLE IF EXISTS window_funnel_test """
    sql """
        CREATE TABLE IF NOT EXISTS window_funnel_test (
            xwho varchar(50) NULL COMMENT 'xwho',
            xwhen datetimev2(3) COMMENT 'xwhen',
            xwhat int NULL COMMENT 'xwhat'
        )
        DUPLICATE KEY(xwho)
        DISTRIBUTED BY HASH(xwho) BUCKETS 3
        PROPERTIES (
        "replication_num" = "1"
        );
    """

    sql "INSERT into window_funnel_test (xwho, xwhen, xwhat) VALUES('1', '2022-03-12 10:41:00.111111', 1)"
    sql "INSERT INTO window_funnel_test (xwho, xwhen, xwhat) VALUES('1', '2022-03-12 13:28:02.111111', 2)"
    sql "INSERT INTO window_funnel_test (xwho, xwhen, xwhat) VALUES('1', '2022-03-12 16:15:01.111111', 3)"
    sql "INSERT INTO window_funnel_test (xwho, xwhen, xwhat) VALUES('1', '2022-03-12 19:05:04.111111', 4)"

    qt_window_funnel """
        select
            window_funnel(
                1,
                'default',
                t.xwhen,
                t.xwhat = 1,
                t.xwhat = 2
                ) AS level
        from window_funnel_test t;
    """
    qt_window_funnel """
        select
            window_funnel(
                20000,
                'default',
                t.xwhen,
                t.xwhat = 1,
                t.xwhat = 2
            ) AS level
        from window_funnel_test t;
    """

    test {
        sql "select cast(1.2 as integer);"
        result([[1]])
    }

    qt_regexp_extract_all """
        SELECT regexp_extract_all('AbCdE', '([[:lower:]]+)C([[:lower:]]+)')
    """
}

