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

suite("test_datetimev2_nano_mtmv") {
    sql "drop materialized view if exists test_datetimev2_nano_multi_mtmv"
    sql "drop table if exists test_datetimev2_nano_mtmv_left"
    sql "drop table if exists test_datetimev2_nano_mtmv_right"

    sql """
        create table test_datetimev2_nano_mtmv_left (
            id int,
            dt datetimev2(9)
        )
        duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        create table test_datetimev2_nano_mtmv_right (
            id int,
            dt datetimev2(9),
            label string
        )
        duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into test_datetimev2_nano_mtmv_left values
        (1, '1677-09-21 00:12:43.145224192'),
        (2, '1969-12-31 23:59:59.999999999'),
        (3, '1970-01-01 00:00:00.000000000'),
        (4, '1970-01-01 00:00:00.123456789'),
        (5, '2262-04-11 23:47:16.854775807'),
        (6, null)
    """
    sql """
        insert into test_datetimev2_nano_mtmv_right values
        (1, '1677-09-21 00:12:43.145224192', 'minimum'),
        (2, '1969-12-31 23:59:59.999999999', 'before-epoch'),
        (3, '1970-01-01 00:00:00.000000000', 'epoch'),
        (4, '1970-01-01 00:00:00.123456789', 'normal'),
        (5, '2262-04-11 23:47:16.854775807', 'maximum'),
        (6, null, 'null')
    """

    sql """
        create materialized view test_datetimev2_nano_multi_mtmv
        build deferred refresh auto on manual
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
        as
        select l.id, l.dt, r.label
        from test_datetimev2_nano_mtmv_left l
        join test_datetimev2_nano_mtmv_right r
          on l.id = r.id and l.dt <=> r.dt
    """
    sql "refresh materialized view test_datetimev2_nano_multi_mtmv auto"
    waitingMTMVTaskFinishedByMvName("test_datetimev2_nano_multi_mtmv")
    order_qt_multi_table_mtmv """
        select id, dt, label
        from test_datetimev2_nano_multi_mtmv
        order by id
    """

    sql """
        insert into test_datetimev2_nano_mtmv_left values
        (100, '1970-02-30 00:00:00.000000000')
    """
    sql """
        insert into test_datetimev2_nano_mtmv_left values
        (101, '1677-09-21 00:12:43.145224191')
    """
    sql """
        insert into test_datetimev2_nano_mtmv_left values
        (102, '2262-04-11 23:47:16.854775808')
    """
    sql """
        insert into test_datetimev2_nano_mtmv_left values
        (7, convert_tz(
            cast('1970-01-01 08:00:00.000000000' as datetimev2(9)),
            'Asia/Shanghai', 'UTC'))
    """
    sql """
        insert into test_datetimev2_nano_mtmv_right values
        (7, convert_tz(
            cast('1970-01-01 08:00:00.000000000' as datetimev2(9)),
            'Asia/Shanghai', 'UTC'), 'timezone-converted')
    """
    sql "refresh materialized view test_datetimev2_nano_multi_mtmv auto"
    waitingMTMVTaskFinishedByMvName("test_datetimev2_nano_multi_mtmv")
    order_qt_multi_table_mtmv_after_refresh """
        select id, dt, label
        from test_datetimev2_nano_multi_mtmv
        order by id
    """
}
