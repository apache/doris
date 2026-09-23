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

suite("test_quarters_add") {
    // this table has nothing todo. just make it eaiser to generate query
    sql " drop table if exists hits_two_args_quar_add "
    sql """ create table hits_two_args_quar_add(
                nothing boolean
            )
            properties("replication_num" = "1");
    """
    sql "insert into hits_two_args_quar_add values(true);"

    sql " drop table if exists quarter_add_table"
    sql """
        create table quarter_add_table (
            k0 int,
            d1 date not null,
            d2 date null,
            dt1 datetime(3) not null,
            dt2 datetime(6) null,
            c1 int not null,
            c2 int null
        )
        DISTRIBUTED BY HASH(k0)
        PROPERTIES
        (
            "replication_num" = "1"
        );
    """

    order_qt_empty_nullable "select quarters_add(d2, c2) from quarter_add_table"
    order_qt_empty_not_nullable "select quarters_add(dt1, c1) from quarter_add_table"
    order_qt_empty_partial_nullable "select quarters_add(dt2, c1) from quarter_add_table"

    sql """insert into quarter_add_table values (1, '2020-12-12', null, '2020-12-12', null, 1, null),
    (2, '2020-12-12', null, '2020-12-12', null, 1, null), (3, '2020-12-12', null, '2020-12-12', null, 1, null)"""
    order_qt_all_null "select quarters_add(dt2, c2) from quarter_add_table"

    sql "truncate table quarter_add_table"
    sql """ insert into quarter_add_table values
            (1, '2020-12-12', '2020-12-12', '2020-12-12 12:12:12.123456', '2020-12-12 12:12:12.123456', 1, 1),
            (2, '4000-12-31', '4000-12-31', '4000-12-31 12:12:12.123456', '4000-12-31 12:12:12.123456', -1, -1),
            (3, '1900-01-01', '1900-01-01', '1900-01-01 00:00:00.000001', '1900-01-01 00:00:00.000001', 100, 100),
            (4, '0900-12-01', '0900-12-01', '0900-12-01 12:12:12.123456', '0900-12-01 12:12:12.123456', -100, -100),
            (5, '1900-02-28', '1900-02-28', '1900-02-28 00:00:00.000001', '1900-02-28 00:00:00.000001', 4, 4),
            (6, '2000-02-28', '2000-02-28', '2000-02-28 23:59:59.999000', '2000-02-28 23:59:59.999000', -400, -400),
            (7, '2000-02-29', '2000-02-29', '2000-02-29 23:59:59.999000', '2000-02-29 23:59:59.999000', 123, 123),
            (8, '2100-02-28', null, '2020-12-12 12:12:12.123456', null, 2, null);
    """

    /// all values. consider nullity.
    order_qt_nullable_d """
        SELECT quarters_add(t.quarter_add_table, t.ARG2) as result
        FROM (
            SELECT hits_two_args_quar_add.nothing, TABLE1.quarter_add_table, TABLE1.order1, TABLE2.ARG2, TABLE2.order2
            FROM hits_two_args_quar_add
            CROSS JOIN (
                SELECT d2 as quarter_add_table, k0 as order1
                FROM quarter_add_table
            ) as TABLE1
            CROSS JOIN (
                SELECT c2 as ARG2, k0 as order2
                FROM quarter_add_table
            ) as TABLE2
        )t;
    """
    order_qt_partial_nullable_dt """
        SELECT quarters_add(t.quarter_add_table, t.ARG2) as result
        FROM (
            SELECT hits_two_args_quar_add.nothing, TABLE1.quarter_add_table, TABLE1.order1, TABLE2.ARG2, TABLE2.order2
            FROM hits_two_args_quar_add
            CROSS JOIN (
                SELECT dt2 as quarter_add_table, k0 as order1
                FROM quarter_add_table
            ) as TABLE1
            CROSS JOIN (
                SELECT c1 as ARG2, k0 as order2
                FROM quarter_add_table
            ) as TABLE2
        )t;
    """
    order_qt_not_null_dt """
        SELECT quarters_add(t.quarter_add_table, t.ARG2) as result
        FROM (
            SELECT hits_two_args_quar_add.nothing, TABLE1.quarter_add_table, TABLE1.order1, TABLE2.ARG2, TABLE2.order2
            FROM hits_two_args_quar_add
            CROSS JOIN (
                SELECT d1 as quarter_add_table, k0 as order1
                FROM quarter_add_table
            ) as TABLE1
            CROSS JOIN (
                SELECT c1 as ARG2, k0 as order2
                FROM quarter_add_table
            ) as TABLE2
        )t;
    """
    order_qt_partial_nullable_d """
        SELECT quarters_add(t.quarter_add_table, t.ARG2) as result
        FROM (
            SELECT hits_two_args_quar_add.nothing, TABLE1.quarter_add_table, TABLE1.order1, TABLE2.ARG2, TABLE2.order2
            FROM hits_two_args_quar_add
            CROSS JOIN (
                SELECT dt1 as quarter_add_table, k0 as order1
                FROM quarter_add_table
            ) as TABLE1
            CROSS JOIN (
                SELECT c2 as ARG2, k0 as order2
                FROM quarter_add_table
            ) as TABLE2
        )t;
    """

    /// nullables
    order_qt_not_nullable "select quarters_add(d1, c1) from quarter_add_table"
    order_qt_partial_nullable "select quarters_add(dt2, c2) from quarter_add_table"
    order_qt_nullable_no_null "select quarters_add(dt1, nullable(c1)) from quarter_add_table"

    /// consts. most by BE-UT
    order_qt_const_nullable "select quarters_add(NULL, NULL) from quarter_add_table"
    order_qt_partial_const_nullable "select quarters_add(NULL, c1) from quarter_add_table"
    order_qt_const_not_nullable "select quarters_add('1800-12-31', 100) from quarter_add_table"
    order_qt_const_other_nullable "select quarters_add('9000-12-31', c2) from quarter_add_table"
    order_qt_const_other_not_nullable "select quarters_add(dt1, 10) from quarter_add_table"
    order_qt_const_nullable_no_null "select quarters_add(nullable('2015-10-10'), nullable(-100))"
    order_qt_const_nullable_no_null_multirows "select quarters_add(nullable('9999-01-01'), nullable(-100)) from quarter_add_table"
    order_qt_const_partial_nullable_no_null "select quarters_add('1234-01-01', nullable(-100))"

    /// folding
    check_fold_consistency "quarters_add('2000-02-29', 3)"
    check_fold_consistency "quarters_add('2000-02-29', -300)"
    check_fold_consistency "quarters_add('0000-01-01', 1000)"
    check_fold_consistency "quarters_add('1999-02-28', 4)"
    check_fold_consistency "quarters_add('1900-02-28', -400)"

    /// special grammar
    qt_datediff1 "select date_sub('2020-12-12', interval 1 quarter)"
    qt_datediff2 "select date_add('2020-12-12', interval 1 quarter)"

    // Exception test cases for boundary conditions on BE
    sql "set debug_skip_fold_constant=true;"
    test {
        sql """select quarters_add('9999-12-31', 1) from hits_two_args_quar_add;"""
        check { result, exception, startTime, endTime ->
            assertTrue(exception != null)
        }
    }

    test {
        sql """select quarters_add('0000-01-01', -1) from hits_two_args_quar_add;"""
        check { result, exception, startTime, endTime ->
            assertTrue(exception != null)
        }
    }

    test {
        sql """select quarters_add('2023-01-01', 40000) from hits_two_args_quar_add;"""
        check { result, exception, startTime, endTime ->
            assertTrue(exception != null)
        }
    }

    test {
        sql """select quarters_add('2023-01-01', -40000) from hits_two_args_quar_add;"""
        check { result, exception, startTime, endTime ->
            assertTrue(exception != null)
        }
    }

    // Exception test cases for nullable scenarios
    test {
        sql """select quarters_add(nullable('9999-12-31'), 1) from hits_two_args_quar_add;"""
        check { result, exception, startTime, endTime ->
            assertTrue(exception != null)
        }
    }

    test {
        sql """select quarters_add('9999-12-31', nullable(1)) from hits_two_args_quar_add;"""
        check { result, exception, startTime, endTime ->
            assertTrue(exception != null)
        }
    }

    test {
        sql """select quarters_add(nullable('0000-01-01'), nullable(-1)) from hits_two_args_quar_add;"""
        check { result, exception, startTime, endTime ->
            assertTrue(exception != null)
        }
    }

    test {
        sql """select quarters_add(nullable('2023-01-01'), nullable(40000)) from hits_two_args_quar_add;"""
        check { result, exception, startTime, endTime ->
            assertTrue(exception != null)
        }
    }

    test {
        sql """select quarters_add(nullable('2023-01-01'), nullable(-40000)) from hits_two_args_quar_add;"""
        check { result, exception, startTime, endTime ->
            assertTrue(exception != null)
        }
    }

    // Int32 multiplication wraps these to -1 and +2 months. The BE unit test covers
    // the full numeric boundary set; P0 covers folding and constant/vector execution.
    def intervals = [1431655765, 1431655766]
    def types = ["date", "datetime(6)", "timestamptz(6)", "timestamp_ns"]
    for (def skipFold : [false, true]) {
        sql "set debug_skip_fold_constant = ${skipFold}"
        for (def type : types) {
            for (def function : ["quarters_add", "quarters_sub"]) {
                for (def quarters : intervals) {
                    test {
                        sql "select ${function}(cast('2023-01-01' as ${type}), cast(${quarters} as int))"
                        exception "out of range"
                    }
                }
            }
        }
        test {
            sql "select quarters_add(date '9999-12-01', 1)"
            exception "out of range"
        }
        test {
            sql "select quarters_sub(date '0000-01-01', 1)"
            exception "out of range"
        }
    }

    sql "set debug_skip_fold_constant = false"
    sql "drop table if exists test_quarters_overflow"
    sql """
        create table test_quarters_overflow (
            q int not null,
            d date,
            dt datetime(6),
            tz timestamptz(6),
            ns timestamp_ns
        )
        duplicate key(q)
        distributed by hash(q) buckets 1
        properties("replication_num" = "1")
    """
    def rows = intervals.collect { quarters ->
        "(${quarters}, '2023-01-01', '2023-01-01 12:34:56.123456', " +
                "'2023-01-01 12:34:56.123456', '2023-01-01 12:34:56.123456789')"
    }
    sql "insert into test_quarters_overflow values ${rows.join(',')}"
    for (def column : ["d", "dt", "tz", "ns"]) {
        for (def function : ["quarters_add", "quarters_sub"]) {
            for (def quarters : intervals) {
                test {
                    sql "select ${function}(${column}, q) from test_quarters_overflow where q = ${quarters}"
                    exception "out of range"
                }
            }
        }
    }
}
