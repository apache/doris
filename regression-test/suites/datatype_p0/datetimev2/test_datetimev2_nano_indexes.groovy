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

suite("test_datetimev2_nano_indexes") {
    sql "set inverted_index_skip_threshold = 0"
    sql "drop table if exists test_datetimev2_nano_indexes"
    sql """
        create table test_datetimev2_nano_indexes (
            id int,
            dt datetimev2(9),
            index idx_dt(dt) using inverted
        )
        duplicate key(id)
        distributed by hash(id) buckets 1
        properties(
            "replication_num" = "1",
            "bloom_filter_columns" = "dt"
        )
    """
    sql """
        insert into test_datetimev2_nano_indexes values
        (1, '1677-09-21 00:12:43.145224192'),
        (2, '1969-12-31 23:59:59.999999999'),
        (3, '1970-01-01 00:00:00.000000000'),
        (4, '1970-01-01 00:00:00.000000001'),
        (5, '2262-04-11 23:47:16.854775807'),
        (6, null)
    """

    order_qt_index_eq """
        select id, dt from test_datetimev2_nano_indexes
        where dt in (
            '1677-09-21 00:12:43.145224192',
            '1970-01-01 00:00:00.000000000',
            '2262-04-11 23:47:16.854775807')
        order by id
    """
    order_qt_index_ne """
        select id, dt from test_datetimev2_nano_indexes
        where dt != '1970-01-01 00:00:00.000000000'
        order by dt
    """
    order_qt_index_gt """
        select id, dt from test_datetimev2_nano_indexes
        where dt > '1970-01-01 00:00:00.000000000'
        order by dt
    """
    order_qt_index_ge """
        select id, dt from test_datetimev2_nano_indexes
        where dt >= '1677-09-21 00:12:43.145224192'
        order by dt
    """
    order_qt_index_lt """
        select id, dt from test_datetimev2_nano_indexes
        where dt < '2262-04-11 23:47:16.854775807'
        order by dt
    """
    order_qt_index_le """
        select id, dt from test_datetimev2_nano_indexes
        where dt <= '1970-01-01 00:00:00.000000000'
        order by dt
    """
    order_qt_index_min_epoch_max """
        select
            min(dt),
            max(dt),
            count(if(dt = '1970-01-01 00:00:00.000000000', 1, null))
        from test_datetimev2_nano_indexes
    """
}
