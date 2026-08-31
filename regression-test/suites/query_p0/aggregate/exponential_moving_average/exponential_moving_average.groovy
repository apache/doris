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

suite("exponential_moving_average") {
    // Prepare main test table (dropped first to keep the environment for debugging)
    sql "drop table if exists ema_test;"
    sql """
        create table ema_test (
            id      int,
            v       double,
            t       double
        )
        duplicate key (id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1");
    """
    sql """
        insert into ema_test values
            (1,  95, 1), (2,  95, 2), (3,  95, 3), (4,  96, 4), (5,  96, 5),
            (6,  96, 6), (7,  96, 7), (8,  97, 8), (9,  97, 9), (10, 97, 10),
            (11, 97, 11), (12, 98, 12), (13, 98, 13), (14, 98, 14), (15, 98, 15),
            (16, 99, 16), (17, 99, 17), (18, 99, 18), (19, 100, 19), (20, 100, 20);
    """

    // Basic aggregate: result matches ClickHouse's exponentialMovingAverage(5)(v, t)
    qt_basic """
        select exponential_moving_average(5.0, v, t) from ema_test;
    """

    test {
        sql """
            select exponential_moving_average(v, v, t) from ema_test;
        """
        exception "The half_decay argument of exponential_moving_average must be a constant"
    }

    // Single-row: EMA of one value (v=95, t=1) with half_decay=1
    // state={95,1}, sum_weights(1)=1/(1-0.5)=2, result=95/2=47.5
    qt_single_row """
        select exponential_moving_average(1.0, v, t) from ema_test where id = 1;
    """

    // GROUP BY: two groups, group 1 has two rows, group 2 has one row
    sql "drop table if exists ema_group_test;"
    sql """
        create table ema_group_test (
            grp int,
            v   double,
            t   double
        )
        duplicate key (grp)
        distributed by hash(grp) buckets 1
        properties("replication_num" = "1");
    """
    sql """
        insert into ema_group_test values
            (1, 10, 1), (1, 20, 3), (2, 100, 1);
    """
    order_qt_group_by """
        select grp, exponential_moving_average(1.0, v, t)
        from ema_group_test group by grp order by grp;
    """

    // half_decay = 0 edge case
    qt_half_decay_zero """
        select exponential_moving_average(0.0, v, t) from ema_test where id = 1;
    """

    // Negative values
    sql "drop table if exists ema_neg_test;"
    sql """
        create table ema_neg_test (
            id int,
            v double,
            t double
        )
        duplicate key (id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1");
    """
    sql """
        insert into ema_neg_test values (1, -10, 1), (2, 10, 5);
    """
    qt_negative """
        select exponential_moving_average(2.0, v, t) from ema_neg_test;
    """

    // NULL handling
    sql "drop table if exists ema_null_test;"
    sql """
        create table ema_null_test (
            id int,
            v double null,
            t double null
        )
        duplicate key (id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1");
    """
    sql """
        insert into ema_null_test values (1, 10, 1), (2, null, 2), (3, 20, 3);
    """
    qt_null """
        select exponential_moving_average(1.0, v, t) from ema_null_test;
    """

    // Empty result set
    qt_empty """
        select exponential_moving_average(1.0, v, t) from ema_null_test where v > 100;
    """

    // Duplicate times: same time values are summed directly
    sql "drop table if exists ema_dup_time_test;"
    sql """
        create table ema_dup_time_test (
            id int,
            v double,
            t double
        )
        duplicate key (id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1");
    """
    sql """
        insert into ema_dup_time_test values (1, 10, 5), (2, 20, 5), (3, 30, 10);
    """
    qt_dup_time """
        select exponential_moving_average(1.0, v, t) from ema_dup_time_test;
    """

    // Cumulative window function
    sql "drop table if exists ema_window_test;"
    sql """
        create table ema_window_test (
            id int,
            t double,
            v double
        )
        duplicate key (id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1");
    """
    sql """
        insert into ema_window_test values (1, 0, 10), (2, 1, 10), (3, 2, 10);
    """
    order_qt_window """
        select t, exponential_moving_average(1.0, v, t)
            over (order by t rows between unbounded preceding and current row) as ema
        from ema_window_test order by t;
    """
}
