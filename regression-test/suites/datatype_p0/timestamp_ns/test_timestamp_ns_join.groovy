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
    sql "set time_zone = '+00:00'"
    sql "set enable_strict_cast = false"
    sql "drop table if exists timestamp_ns_join_left"
    sql "drop table if exists timestamp_ns_join_right"
    sql "drop table if exists timestamp_ns_join_datetimev2"
    sql "drop table if exists timestamp_ns_join_temporal_safe"
    sql "drop table if exists timestamp_ns_join_temporal_overflow"
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
        (4, null),
        (5, '2024-02-29 00:00:00.000000000')
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
        (22, '1970-01-01 00:00:00.000000'),
        (23, '1970-01-01 00:00:00.000001'),
        (24, '2262-04-11 23:47:16.854775'),
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

    sql """
            create table timestamp_ns_join_temporal_safe (
                id int,
                datev2_value datev2,
                datetimev2_value datetimev2(6),
                timestamptz_value timestamptz(6)
            )
            duplicate key(id)
            distributed by hash(id) buckets 1
            properties("replication_num" = "1")
    """
    sql """
            insert into timestamp_ns_join_temporal_safe values
            (1, '1970-01-01', '1970-01-01 00:00:00.000000',
                cast('1970-01-01 00:00:00.000000+00:00' as timestamptz(6))),
            (2, '2024-02-29', '2024-02-29 00:00:00.000000',
                cast('2024-02-29 00:00:00.000000+00:00' as timestamptz(6))),
            (3, null, null, null)
    """

    order_qt_mixed_inner_join_datev2 """
            select l.id, r.id
            from timestamp_ns_join_left l
            join timestamp_ns_join_temporal_safe r on l.dt = r.datev2_value
            order by l.id, r.id
    """
    order_qt_mixed_inner_join_datetimev2 """
            select l.id, r.id
            from timestamp_ns_join_left l
            join timestamp_ns_join_temporal_safe r on l.dt = r.datetimev2_value
            order by l.id, r.id
    """
    order_qt_mixed_inner_join_timestamptz """
            select l.id, r.id
            from timestamp_ns_join_left l
            join timestamp_ns_join_temporal_safe r on l.dt = r.timestamptz_value
            order by l.id, r.id
    """

    explain {
        sql """
                shape plan
                select l.id
                from timestamp_ns_join_left l
                join timestamp_ns_join_temporal_safe r on l.dt = r.datetimev2_value
        """
        contains "hashJoin"
    }

    sql """
            create table timestamp_ns_join_temporal_overflow (
                id int,
                datev2_value datev2,
                datetimev2_value datetimev2(6),
                timestamptz_value timestamptz(6)
            )
            duplicate key(id)
            distributed by hash(id) buckets 1
            properties("replication_num" = "1")
    """
    sql """
            insert into timestamp_ns_join_temporal_overflow values
            (1, '9999-12-31', '2262-04-11 23:47:16.854776',
                cast('2262-04-11 23:47:16.854776+00:00' as timestamptz(6)))
    """

    for (def columnName : ["datev2_value", "datetimev2_value"]) {
        test {
            sql """
                    select l.id
                    from timestamp_ns_join_left l
                    join timestamp_ns_join_temporal_overflow r on l.dt = r.${columnName}
            """
            exception "TIMESTAMP_NS overflow"
        }
    }
    test {
        sql """
                select l.id
                from timestamp_ns_join_left l
                join timestamp_ns_join_temporal_overflow r on l.dt = r.timestamptz_value
        """
        exception "can not cast timestamptz"
    }

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

    sql "drop table if exists timestamp_ns_asof_fallible_datetime"
    sql """
        create table timestamp_ns_asof_fallible_datetime (
            id int,
            k int,
            dt datetimev2(6)
        ) duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql "drop table if exists timestamp_ns_asof_fallible_timestamp"
    sql """
        create table timestamp_ns_asof_fallible_timestamp (
            id int,
            k int,
            ts timestamp_ns
        ) duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into timestamp_ns_asof_fallible_datetime values
        (1, 1, '2500-01-01 00:00:00'), (2, 1, '1600-01-01 00:00:00'),
        (3, 1, '2024-01-01 00:00:00'), (4, 1, null)
    """
    sql """
        insert into timestamp_ns_asof_fallible_timestamp values
        (1, 1, '2024-01-01 00:00:00')
    """
    // Keep the build side nonempty so the probe expression is evaluated rather than short-circuited.
    // Strict temporal casts on either side must return the original overflow, including in ASAN.
    for (def joinType : ["asof inner join", "asof left join"]) {
        for (def op : [">=", ">", "<=", "<"]) {
            for (def id : [1, 2]) {
                test {
                    sql """
                        select l.id, r.id
                        from (select * from timestamp_ns_asof_fallible_datetime where id = ${id}) l
                        ${joinType} timestamp_ns_asof_fallible_timestamp r
                        match_condition(l.dt ${op} r.ts) on l.k = r.k
                    """
                    exception "TIMESTAMP_NS overflow"
                }
                test {
                    sql """
                        select l.id, r.id
                        from timestamp_ns_asof_fallible_timestamp l
                        ${joinType}
                            (select * from timestamp_ns_asof_fallible_datetime where id = ${id}) r
                        match_condition(l.ts ${op} r.dt) on l.k = r.k
                    """
                    exception "TIMESTAMP_NS overflow"
                }
            }
        }
    }
    order_qt_asof_after_conversion_errors """
        select l.id, r.id
        from (select * from timestamp_ns_asof_fallible_datetime where id in (3, 4)) l
        asof left join timestamp_ns_asof_fallible_timestamp r
        match_condition(l.dt >= r.ts) on l.k = r.k
        order by l.id, r.id
    """
    qt_asof_nullable_build_after_conversion_errors """
        select l.id, r.id
        from timestamp_ns_asof_fallible_timestamp l
        asof left join
            (select * from timestamp_ns_asof_fallible_datetime where id in (3, 4)) r
        match_condition(l.ts >= r.dt) on l.k = r.k
        order by l.id, r.id
    """

    sql "drop table if exists timestamp_ns_asof_derived_probe"
    sql """
        create table timestamp_ns_asof_derived_probe (
            id int, k int, ts timestamp_ns
        ) duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql "drop table if exists timestamp_ns_asof_derived_build"
    sql """
        create table timestamp_ns_asof_derived_build (
            id int, k int, dt datetimev2(6) not null
        ) duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into timestamp_ns_asof_derived_probe values
        (1, 1, '2024-01-01'), (2, 1, '1801-01-01'), (3, 2, '2024-01-01'), (4, 1, null)
    """
    sql """
        insert into timestamp_ns_asof_derived_build values
        (11, 1, '2200-01-01'), (12, 1, '2201-01-01'), (13, 1, '2202-01-01')
    """
    // Real derived keys (1800..1802) fit TIMESTAMP_NS. The NOT NULL mock dt (1970)
    // would produce 1570 and overflow if the match expression were evaluated on it.
    for (def joinType : ["inner", "left"]) {
        for (def comparison : [["ge", ">="], ["gt", ">"], ["le", "<="], ["lt", "<"]]) {
            def name = comparison[0]
            def op = comparison[1]
            "order_qt_asof_not_null_derived_${joinType}_${name}" """
                select l.id, r.id, r.dt
                from timestamp_ns_asof_derived_probe l
                asof ${joinType} join timestamp_ns_asof_derived_build r
                match_condition(l.ts ${op} years_sub(r.dt, 400)) on l.k = r.k
                order by l.id, r.id
            """
        }
        "order_qt_asof_not_null_derived_empty_${joinType}" """
            select l.id, r.id
            from timestamp_ns_asof_derived_probe l
            asof ${joinType} join
                (select * from timestamp_ns_asof_derived_build where id < 0) r
            match_condition(l.ts >= years_sub(r.dt, 400)) on l.k = r.k
            order by l.id, r.id
        """
    }
}
