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

suite("test_timestamp_ns_join") {
    sql "drop table if exists timestamp_ns_join_left"
    sql "drop table if exists timestamp_ns_join_right"
    sql "drop table if exists timestamp_ns_join_datetimev2"
    for (def tableName : ["timestamp_ns_join_left", "timestamp_ns_join_right"]) {
        sql """
            create table ${tableName} (
                id int,
                dt timestamp_ns
            )
            duplicate key(id)
            distributed by hash(id) buckets 2
            properties("replication_num" = "1")
        """
    }
    sql """
        create table timestamp_ns_join_datetimev2 (
            id int,
            dt datetimev2(6)
        )
        duplicate key(id)
        distributed by hash(id) buckets 2
        properties("replication_num" = "1")
    """
    sql """
        insert into timestamp_ns_join_left values
        (1, '1677-09-21 00:12:43.145224192'),
        (2, '1970-01-01 00:00:00.000000000'),
        (3, '1970-01-01 00:00:00.000000001'),
        (4, null)
    """
    sql """
        insert into timestamp_ns_join_right values
        (11, '1677-09-21 00:12:43.145224192'),
        (12, '1970-01-01 00:00:00.000000001'),
        (13, '2262-04-11 23:47:16.854775807'),
        (14, null)
    """
    sql """
        insert into timestamp_ns_join_datetimev2 values
        (20, '0001-01-01 00:00:00'),
        (21, '1677-09-21 00:12:43.145224'),
        (22, '1970-01-01 00:00:00.000000'),
        (23, '1970-01-01 00:00:00.000001'),
        (24, '2262-04-11 23:47:16.854775'),
        (25, '2262-04-11 23:47:16.854776'),
        (99, '9999-12-31 23:59:59.999999'),
        (100, null)
    """

    order_qt_inner_join """
        select l.id, l.dt, r.id
        from timestamp_ns_join_left l
        join timestamp_ns_join_right r on l.dt = r.dt
        order by l.id, r.id
    """
    order_qt_left_join """
        select l.id, l.dt, r.id
        from timestamp_ns_join_left l
        left join timestamp_ns_join_right r on l.dt = r.dt
        order by l.id, r.id
    """

    order_qt_mixed_inner_join_timestamp_ns_datetimev2 """
        select l.id, l.dt, r.id, r.dt
        from timestamp_ns_join_left l
        join timestamp_ns_join_datetimev2 r on l.dt = r.dt
        order by l.id, r.id
    """
    order_qt_mixed_inner_join_datetimev2_timestamp_ns """
        select l.id, l.dt, r.id, r.dt
        from timestamp_ns_join_datetimev2 l
        join timestamp_ns_join_left r on l.dt = r.dt
        order by l.id, r.id
    """
    order_qt_mixed_left_join """
        select l.id, l.dt, r.id, r.dt
        from timestamp_ns_join_left l
        left join timestamp_ns_join_datetimev2 r on l.dt = r.dt
        order by l.id, r.id
    """
    order_qt_mixed_null_safe_inner_join """
        select l.id, l.dt, r.id, r.dt
        from timestamp_ns_join_left l
        join timestamp_ns_join_datetimev2 r on l.dt <=> r.dt
        order by l.id, r.id
    """

    sql "drop table if exists timestamp_ns_asof_left"
    sql "drop table if exists timestamp_ns_asof_right"
    for (def tableName : ["timestamp_ns_asof_left", "timestamp_ns_asof_right"]) {
        sql """
            create table ${tableName} (
                id int,
                k int,
                dt timestamp_ns not null
            )
            duplicate key(id)
            distributed by hash(id) buckets 1
            properties("replication_num" = "1")
        """
    }
    sql """
        insert into timestamp_ns_asof_left values
        (1, 1, '1677-09-21 00:12:43.145224192'),
        (2, 1, '1969-12-31 23:59:59.999999999'),
        (3, 1, '1970-01-01 00:00:00.000000000'),
        (4, 1, '1970-01-01 00:00:00.000000001'),
        (5, 1, '2262-04-11 23:47:16.854775807')
    """
    sql """
        insert into timestamp_ns_asof_right values
        (11, 1, '1677-09-21 00:12:43.145224192'),
        (12, 1, '1969-12-31 23:59:59.999999999'),
        (13, 1, '1970-01-01 00:00:00.000000000'),
        (14, 1, '1970-01-01 00:00:00.000000001'),
        (15, 1, '2262-04-11 23:47:16.854775807')
    """
    order_qt_timestamp_ns_asof_ge """
        select l.id, l.dt, r.id, r.dt
        from timestamp_ns_asof_left l
        asof left join timestamp_ns_asof_right r
        match_condition(l.dt >= r.dt)
        on l.k = r.k
        order by l.id
    """
    order_qt_timestamp_ns_asof_gt """
        select l.id, l.dt, r.id, r.dt
        from timestamp_ns_asof_left l
        asof left join timestamp_ns_asof_right r
        match_condition(l.dt > r.dt)
        on l.k = r.k
        order by l.id
    """
    order_qt_timestamp_ns_asof_le """
        select l.id, l.dt, r.id, r.dt
        from timestamp_ns_asof_left l
        asof left join timestamp_ns_asof_right r
        match_condition(l.dt <= r.dt)
        on l.k = r.k
        order by l.id
    """
    order_qt_timestamp_ns_asof_lt """
        select l.id, l.dt, r.id, r.dt
        from timestamp_ns_asof_left l
        asof left join timestamp_ns_asof_right r
        match_condition(l.dt < r.dt)
        on l.k = r.k
        order by l.id
    """

    sql "drop table if exists timestamp_ns_asof_nullable_left"
    sql "drop table if exists timestamp_ns_asof_nullable_right"
    for (def tableName : ["timestamp_ns_asof_nullable_left", "timestamp_ns_asof_nullable_right"]) {
        sql """
            create table ${tableName} (
                id int,
                k int,
                dt timestamp_ns
            )
            duplicate key(id)
            distributed by hash(id) buckets 1
            properties("replication_num" = "1")
        """
    }
    sql """
        insert into timestamp_ns_asof_nullable_left values
        (1, 1, '1970-01-01 00:00:00.000000000'),
        (2, 1, null)
    """
    sql """
        insert into timestamp_ns_asof_nullable_right values
        (11, 1, '1969-12-31 23:59:59.999999999'),
        (12, 1, null)
    """
    order_qt_timestamp_ns_asof_nullable """
        select l.id, l.dt, r.id, r.dt
        from timestamp_ns_asof_nullable_left l
        asof left join timestamp_ns_asof_nullable_right r
        match_condition(l.dt >= r.dt)
        on l.k = r.k
        order by l.id
    """

    qt_timestamp_ns_asof_datetimev2 """
        select l.id, r.id
        from (
            select 1 as id,
                   cast('2024-01-01 00:00:00.000000001' as timestamp_ns) as dt
        ) l
        asof left join (
            select 1 as id,
                   cast('2024-01-01 00:00:00.000000' as datetimev2(6)) as dt
        ) r
        match_condition(l.dt >= r.dt)
        on l.id = r.id
    """
    qt_datetimev2_asof_timestamp_ns """
        select l.id, r.id
        from (
            select 1 as id,
                   cast('2024-01-01 00:00:00.000000' as datetimev2(6)) as dt
        ) l
        asof left join (
            select 1 as id,
                   cast('2024-01-01 00:00:00.000000001' as timestamp_ns) as dt
        ) r
        match_condition(l.dt >= r.dt)
        on l.id = r.id
    """
}
