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
    // Prepare test table (dropped first to keep the environment for debugging)
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
            (1,  95, 1),
            (2,  95, 2),
            (3,  95, 3),
            (4,  96, 4),
            (5,  96, 5),
            (6,  96, 6),
            (7,  96, 7),
            (8,  97, 8),
            (9,  97, 9),
            (10, 97, 10),
            (11, 97, 11),
            (12, 98, 12),
            (13, 98, 13),
            (14, 98, 14),
            (15, 98, 15),
            (16, 99, 16),
            (17, 99, 17),
            (18, 99, 18),
            (19, 100, 19),
            (20, 100, 20);
    """

    // Basic aggregate: result matches ClickHouse's exponentialMovingAverage(5)(v, t)
    // = 92.25779635374204
    qt_basic """
        select exponential_moving_average(5.0, v, t) from ema_test;
    """

    // Single-row: EMA of one value (v=10, t=1) with half_decay=1
    // state={10,1}, sum_weights(1)=1/(1-0.5)=2, result=10/2=5.0
    qt_single_row """
        select exponential_moving_average(1.0, v, t)
        from ema_test where id = 1;
    """

    // Cumulative window: each row gets EMA over all preceding rows (inclusive)
    // With half_decay=1 and constant value=10 at times 0,1,2:
    //   t=0: state={10,0}, result=5.0
    //   t=1: state={15,1}, result=7.5
    //   t=2: state={17.5,2}, result=8.75
    sql "drop table if exists ema_window_test;"

    sql """
        create table ema_window_test (
            t double,
            v double
        )
        duplicate key (t)
        distributed by hash(t) buckets 1
        properties("replication_num" = "1");
    """

    sql """
        insert into ema_window_test values (0, 10), (1, 10), (2, 10);
    """

    order_qt_window """
        select
            t,
            exponential_moving_average(1.0, v, t)
                over (order by t rows between unbounded preceding and current row) as ema
        from ema_window_test
        order by t;
    """
}
