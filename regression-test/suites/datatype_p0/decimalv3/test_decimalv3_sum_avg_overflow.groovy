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

suite("test_decimalv3_sum_avg_overflow") {
    sql "drop table if exists test_decimalv3_sum_avg_overflow"
    sql """
        CREATE TABLE test_decimalv3_sum_avg_overflow (
            k1 int,
            c1 decimalv3(38, 0)
        )
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """
    // 10^38 - 1, near Decimal128 max for precision 38
    sql """
        insert into test_decimalv3_sum_avg_overflow values
            (1, 99999999999999999999999999999999999999),
            (1, 1),
            (2, 10),
            (2, 20),
            (2, 30);
    """

    sql "set check_overflow_for_decimal=true;"
    sql "set enable_decimal256=false;"

    test {
        sql "select sum(c1) from test_decimalv3_sum_avg_overflow where k1 = 1;"
        exception "Arithmetic overflow"
    }

    test {
        sql "select avg(c1) from test_decimalv3_sum_avg_overflow where k1 = 1;"
        exception "Arithmetic overflow"
    }

    test {
        sql "select k1, sum(c1) from test_decimalv3_sum_avg_overflow group by k1 order by k1;"
        exception "Arithmetic overflow"
    }

    order_qt_sum_ok """
        select k1, sum(c1) from test_decimalv3_sum_avg_overflow where k1 = 2 group by k1;
    """
    order_qt_avg_ok """
        select k1, avg(c1) from test_decimalv3_sum_avg_overflow where k1 = 2 group by k1;
    """

    // Window path
    test {
        sql """
            select sum(c1) over (partition by k1 order by k1 rows between unbounded preceding and current row)
            from test_decimalv3_sum_avg_overflow where k1 = 1;
        """
        exception "Arithmetic overflow"
    }

    // Distinct path
    test {
        sql "select sum(distinct c1) from test_decimalv3_sum_avg_overflow where k1 = 1;"
        exception "Arithmetic overflow"
    }

    // Nullable
    sql "drop table if exists test_decimalv3_sum_avg_overflow_null"
    sql """
        CREATE TABLE test_decimalv3_sum_avg_overflow_null (
            k1 int,
            c1 decimalv3(38, 0) null
        )
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql """
        insert into test_decimalv3_sum_avg_overflow_null values
            (1, 99999999999999999999999999999999999999),
            (1, null),
            (1, 1);
    """
    test {
        sql "select sum(c1) from test_decimalv3_sum_avg_overflow_null;"
        exception "Arithmetic overflow"
    }

    // check off: wraparound allowed (legacy behavior)
    sql "set check_overflow_for_decimal=false;"
    qt_sum_wrap """
        select sum(c1) is not null from test_decimalv3_sum_avg_overflow where k1 = 1;
    """

    // enable_decimal256 expands intermediate/result to Decimal256, so the same values fit.
    sql "set check_overflow_for_decimal=true;"
    sql "set enable_decimal256=true;"
    order_qt_sum_decimal256 """
        select sum(c1) from test_decimalv3_sum_avg_overflow where k1 = 1;
    """
    order_qt_avg_decimal256 """
        select avg(c1) from test_decimalv3_sum_avg_overflow where k1 = 1;
    """
}
