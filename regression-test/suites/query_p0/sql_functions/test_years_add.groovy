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

suite("test_years_add") {
    // this table has nothing todo. just make it easier to generate query
    sql "drop table if exists hits_years_add"
    sql """
        create table hits_years_add(
            nothing boolean
        )
        properties("replication_num" = "1");
    """
    sql "insert into hits_years_add values(true);"

    sql "drop table if exists dates_tbl"
    sql """
        create table dates_tbl (
            k0 int,
            dt_val datetime not null,
            dt_null datetime null
        )
        DISTRIBUTED BY HASH(k0)
        PROPERTIES
        (
            "replication_num" = "1"
        );
    """

    sql "drop table if exists years_tbl"
    sql """
        create table years_tbl (
            k0 int,
            years_val int not null,
            years_null int null
        )
        DISTRIBUTED BY HASH(k0)
        PROPERTIES
        (
            "replication_num" = "1"
        );
    """

    // Empty table tests
    order_qt_empty_nullable "select years_add(dt_null, years_val) from dates_tbl, years_tbl"
    order_qt_empty_not_nullable "select years_add(dt_val, years_val) from dates_tbl, years_tbl"
    order_qt_empty_partial_nullable "select years_add(dt_val, years_null) from dates_tbl, years_tbl"

    // Insert test data
    sql """
        insert into dates_tbl values 
        (1, '2020-01-01 00:00:00', '2020-01-01 00:00:00'),    -- regular date
        (2, '2020-02-29 00:00:00', '2020-02-29 00:00:00'),    -- leap year date
        (3, '2000-12-31 23:59:59', '2000-12-31 23:59:59'),    -- century leap year
        (4, '2023-06-15 12:30:45', '2023-06-15 12:30:45'),    -- date with time
        (5, '1999-12-31 00:00:00', null)                       -- null value
    """

    sql """
        insert into years_tbl values 
        (1, 0, 0),       -- no change
        (2, 1, 1),       -- add one year
        (3, -1, -1),     -- subtract one year
        (4, 4, 4),       -- leap year cycle
        (5, 100, null)   -- null value
    """

    // All values cross join test
    order_qt_nullable """
        SELECT years_add(t.dt, t.years) as result
        FROM (
            SELECT hits_years_add.nothing, TABLE1.dt, TABLE1.order1, TABLE2.years, TABLE2.order2
            FROM hits_years_add
            CROSS JOIN (
                SELECT dt_null as dt, k0 as order1
                FROM dates_tbl
            ) as TABLE1
            CROSS JOIN (
                SELECT years_null as years, k0 as order2
                FROM years_tbl
            ) as TABLE2
        )t;
    """

    // Nullable tests
    order_qt_not_nullable "select years_add(dt_val, years_val) from dates_tbl, years_tbl"
    order_qt_partial_nullable "select years_add(dt_val, years_null) from dates_tbl, years_tbl"
    order_qt_nullable_no_null "select years_add(dt_val, nullable(years_val)) from dates_tbl, years_tbl"

    // Constant tests
    order_qt_const_nullable "select years_add(NULL, NULL) from dates_tbl"
    order_qt_partial_const_nullable "select years_add(NULL, years_val) from years_tbl"
    order_qt_const_not_nullable "select years_add('2020-01-01 00:00:00', 1) from dates_tbl"
    order_qt_const_other_nullable "select years_add('2020-01-01 00:00:00', years_null) from years_tbl"
    order_qt_const_other_not_nullable "select years_add(dt_val, 1) from dates_tbl"
    order_qt_const_nullable_no_null "select years_add(nullable('2020-01-01 00:00:00'), nullable(1))"
    order_qt_const_nullable_no_null_multirows "select years_add(nullable('2020-01-01 00:00:00'), nullable(1)) from dates_tbl"
    order_qt_const_partial_nullable_no_null "select years_add('2020-01-01 00:00:00', nullable(1))"

    // Constant folding tests
    check_fold_consistency "years_add('2020-01-01 00:00:00', 1)"
    check_fold_consistency "years_add('2020-02-29 00:00:00', 1)"  // leap year to non-leap year
    check_fold_consistency "years_add('2000-12-31 23:59:59', 100)"
    check_fold_consistency "years_add('1999-12-31 00:00:00', -10)"
    check_fold_consistency "years_add('2023-06-15 12:30:45', 0)"

    // Exception test cases for boundary conditions on BE
    sql "set debug_skip_fold_constant=true;"
    test {
        sql """select years_add('9999-12-31', 1) from hits_years_add;"""
        check { result, exception, startTime, endTime ->
            assertTrue(exception != null)
        }
    }

    test {
        sql """select years_add('0000-01-01', -1) from hits_years_add;"""
        check { result, exception, startTime, endTime ->
            assertTrue(exception != null)
        }
    }

    test {
        sql """select years_add('2023-01-01', 10000) from hits_years_add;"""
        check { result, exception, startTime, endTime ->
            assertTrue(exception != null)
        }
    }

    test {
        sql """select years_add('2023-01-01', -10000) from hits_years_add;"""
        check { result, exception, startTime, endTime ->
            assertTrue(exception != null)
        }
    }

    // Exception test cases for nullable scenarios
    test {
        sql """select years_add(nullable('9999-12-31'), 1) from hits_years_add;"""
        check { result, exception, startTime, endTime ->
            assertTrue(exception != null)
        }
    }

    test {
        sql """select years_add('9999-12-31', nullable(1)) from hits_years_add;"""
        check { result, exception, startTime, endTime ->
            assertTrue(exception != null)
        }
    }

    test {
        sql """select years_add(nullable('0000-01-01'), nullable(-1)) from hits_years_add;"""
        check { result, exception, startTime, endTime ->
            assertTrue(exception != null)
        }
    }

    test {
        sql """select years_add(nullable('2023-01-01'), nullable(10000)) from hits_years_add;"""
        check { result, exception, startTime, endTime ->
            assertTrue(exception != null)
        }
    }

    test {
        sql """select years_add(nullable('2023-01-01'), nullable(-10000)) from hits_years_add;"""
        check { result, exception, startTime, endTime ->
            assertTrue(exception != null)
        }
    }
}
