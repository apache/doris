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

suite("test_timestamp_ns_mixed_datetime_expressions") {
    sql "set time_zone = '+08:00'"
    sql "set enable_strict_cast = false"
    sql "drop table if exists timestamp_ns_mixed_datetime_expressions"
    sql """
        create table timestamp_ns_mixed_datetime_expressions (
            id int,
            ts timestamp_ns,
            dt datetimev2(6)
        )
        duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into timestamp_ns_mixed_datetime_expressions values
        (1, '2024-02-29 12:34:56.123456789', '2024-02-29 12:34:56.123456'),
        (2, '2024-02-29 12:34:56.123456000', '2024-02-29 12:34:56.123456'),
        (3, '1969-12-31 23:59:59.999999999', '1969-12-31 23:59:59.999999'),
        (4, '1677-09-21 00:12:43.145224192', '1700-01-01 00:00:00.000000'),
        (5, '2262-04-11 23:47:16.854775807', '2200-01-01 00:00:00.000000'),
        (6, '1970-01-01 00:00:00.000000000', '2025-01-01 00:00:00.000000'),
        (7, null, null)
    """

    // Mixed comparisons use the exact heterogeneous BE comparator unless an exactly representable
    // constant can be converted to the column type. Exercise both operand orders and NULL semantics.
    order_qt_mixed_comparisons """
        select id,
               ts = dt, dt = ts,
               ts != dt, dt != ts,
               ts > dt, dt > ts,
               ts >= dt, dt >= ts,
               ts < dt, dt < ts,
               ts <= dt, dt <= ts,
               ts <=> dt, dt <=> ts
        from timestamp_ns_mixed_datetime_expressions
        order by id
    """
    order_qt_mixed_between_and_boolean """
        select id,
               ts between seconds_sub(dt, 1) and seconds_add(dt, 1),
               ts not between seconds_sub(dt, 1) and seconds_add(dt, 1),
               (ts > dt or ts = dt),
               not (ts <=> dt)
        from timestamp_ns_mixed_datetime_expressions
        order by id
    """
    order_qt_mixed_string_functions """
        select id,
               concat(ts, '|', dt),
               concat(dt, '|', ts),
               concat_ws('|', ts, dt),
               concat_ws('|', dt, ts)
        from timestamp_ns_mixed_datetime_expressions
        order by id
    """
    qt_mixed_range_literals """
        select
            cast('1677-09-21 00:12:43.145224192' as timestamp_ns)
                < cast('1700-01-01 00:00:00.000000' as datetimev2(6)),
            cast('2262-04-11 23:47:16.854775807' as timestamp_ns)
                > cast('2200-01-01 00:00:00.000000' as datetimev2(6)),
            cast('1970-01-01 00:00:00.000000000' as timestamp_ns)
                < cast('2200-12-31 23:59:59.999999' as datetimev2(6)),
            cast('1970-01-01 00:00:00.000000000' as timestamp_ns)
                > cast('1700-01-01 00:00:00.000000' as datetimev2(6))
    """

    // Ordinary numeric arithmetic keeps the existing numeric cast semantics for each temporal type.
    qt_mixed_numeric_arithmetic """
        select
            cast('2024-02-29 12:34:56.123456789' as timestamp_ns)
                + cast('2024-02-29 12:34:56.123456' as datetimev2(6)),
            cast('2024-02-29 12:34:56.123456789' as timestamp_ns)
                - cast('2024-02-29 12:34:56.123456' as datetimev2(6)),
            cast('2024-02-29 12:34:56.123456789' as timestamp_ns)
                / cast('2024-02-29 12:34:56.123456' as datetimev2(6)),
            cast('2024-02-29 12:34:56.123456789' as timestamp_ns)
                % cast('2024-02-29 12:34:56.123456' as datetimev2(6))
    """
    order_qt_mixed_numeric_arithmetic_rows """
        select id, ts + dt, ts - dt, ts / dt, ts % dt
        from timestamp_ns_mixed_datetime_expressions
        where id between 1 and 3
        order by id
    """
    order_qt_parallel_interval_arithmetic """
        select id,
               microseconds_add(ts, 1), microseconds_add(dt, 1),
               seconds_sub(ts, 1), seconds_sub(dt, 1),
               date_add(ts, interval '1 02:03:04.000005' day_microsecond),
               date_add(dt, interval '1 02:03:04.000005' day_microsecond)
        from timestamp_ns_mixed_datetime_expressions
        where id between 1 and 3
        order by id
    """

    // Difference functions execute directly on TIMESTAMP_NS and DATETIMEV2 physical arguments.
    qt_mixed_diff_to_timestamp_ns """
        select
            datediff(
                ts, cast('2024-02-28 12:34:56.123456' as datetimev2(6))),
            timediff(
                ts, cast('2024-02-29 12:34:55.123456' as datetimev2(6))),
            nanoseconds_diff(
                ts, cast('2024-02-29 12:34:56.123456' as datetimev2(6))),
            microseconds_diff(
                ts, cast('2024-02-29 12:34:56.123455' as datetimev2(6))),
            milliseconds_diff(
                ts, cast('2024-02-29 12:34:56.122456' as datetimev2(6))),
            seconds_diff(
                ts, cast('2024-02-29 12:34:55.123456' as datetimev2(6))),
            minutes_diff(
                ts, cast('2024-02-29 12:33:56.123456' as datetimev2(6))),
            hours_diff(
                ts, cast('2024-02-29 11:34:56.123456' as datetimev2(6))),
            days_diff(
                ts, cast('2024-02-28 12:34:56.123456' as datetimev2(6))),
            weeks_diff(
                ts, cast('2024-02-22 12:34:56.123456' as datetimev2(6))),
            months_diff(
                ts, cast('2024-01-29 12:34:56.123456' as datetimev2(6))),
            quarters_diff(
                ts, cast('2023-11-29 12:34:56.123456' as datetimev2(6))),
            years_diff(
                ts, cast('2023-02-28 12:34:56.123456' as datetimev2(6)))
        from timestamp_ns_mixed_datetime_expressions
        where id = 1
    """

    // Both physical argument orders are registered without requiring a common temporal type.
    qt_mixed_diff_to_datetimev2 """
        select
            datediff(
                dt, cast('2024-02-28 12:34:56.123456000' as timestamp_ns)),
            timediff(
                dt, cast('2024-02-29 12:34:55.123456000' as timestamp_ns)),
            nanoseconds_diff(
                dt, cast('2024-02-29 12:34:56.123456789' as timestamp_ns)),
            microseconds_diff(
                dt, cast('2024-02-29 12:34:56.123455000' as timestamp_ns)),
            seconds_diff(
                dt, cast('2024-02-29 12:34:55.123456000' as timestamp_ns)),
            months_diff(
                dt, cast('2024-01-29 12:34:56.123456000' as timestamp_ns)),
            years_diff(
                dt, cast('2023-02-28 12:34:56.123456000' as timestamp_ns))
        from timestamp_ns_mixed_datetime_expressions
        where id = 1
    """
    order_qt_mixed_column_datediff """
        select id, datediff(ts, dt), datediff(dt, ts)
        from timestamp_ns_mixed_datetime_expressions
        order by id
    """

    // All floor/ceil units share the same TIMESTAMP_NS coercion rule for the custom origin.
    qt_mixed_floor_ceil_origin """
        select
            year_floor(
                ts, 1,
                cast('2024-03-01 01:02:03.123456' as datetimev2(6))),
            year_ceil(
                ts, 1,
                cast('2024-03-01 01:02:03.123456' as datetimev2(6))),
            quarter_floor(
                ts, 1,
                cast('2024-03-01 01:02:03.123456' as datetimev2(6))),
            quarter_ceil(
                ts, 1,
                cast('2024-03-01 01:02:03.123456' as datetimev2(6))),
            month_floor(
                ts, 1,
                cast('2024-03-01 01:02:03.123456' as datetimev2(6))),
            month_ceil(
                ts, 1,
                cast('2024-03-01 01:02:03.123456' as datetimev2(6))),
            week_floor(
                ts, 1,
                cast('2024-03-01 01:02:03.123456' as datetimev2(6))),
            week_ceil(
                ts, 1,
                cast('2024-03-01 01:02:03.123456' as datetimev2(6))),
            day_floor(
                ts, 1,
                cast('2024-03-01 01:02:03.123456' as datetimev2(6))),
            day_ceil(
                ts, 1,
                cast('2024-03-01 01:02:03.123456' as datetimev2(6))),
            hour_floor(
                ts, 1,
                cast('2024-03-01 01:02:03.123456' as datetimev2(6))),
            hour_ceil(
                ts, 1,
                cast('2024-03-01 01:02:03.123456' as datetimev2(6))),
            minute_floor(
                ts, 1,
                cast('2024-03-01 01:02:03.123456' as datetimev2(6))),
            minute_ceil(
                ts, 1,
                cast('2024-03-01 01:02:03.123456' as datetimev2(6))),
            second_floor(
                ts, 1,
                cast('2024-03-01 01:02:03.123456' as datetimev2(6))),
            second_ceil(
                ts, 1,
                cast('2024-03-01 01:02:03.123456' as datetimev2(6)))
        from timestamp_ns_mixed_datetime_expressions
        where id = 1
    """
    qt_mixed_floor_datetimev2_target """
        select
            second_floor(
                dt, 1,
                cast('2024-03-01 01:02:03.123456000' as timestamp_ns)),
            second_ceil(
                dt, 1,
                cast('2024-03-01 01:02:03.123456000' as timestamp_ns))
        from timestamp_ns_mixed_datetime_expressions
        where id = 1
    """

    // Other homogeneous temporal families: range/sequence, FIELD, conditional expressions,
    // collections and set operations. Structs can retain heterogeneous field types; ARRAY/MAP
    // elements require a common type.
    qt_mixed_range_and_field """
        select
            array_range(
                ts,
                cast('2024-02-29 12:34:59.123456' as datetimev2(6)), interval 1 second),
            sequence(
                ts,
                cast('2024-02-29 12:34:59.123456' as datetimev2(6)), interval 1 second),
            field(
                ts,
                cast('2024-02-29 12:34:56.123456' as datetimev2(6)),
                cast('2024-02-29 12:34:56.123457' as datetimev2(6)))
        from timestamp_ns_mixed_datetime_expressions
        where id = 1
    """

    def mixedLiteralSql = """
        select
            cast('2024-02-29 12:34:56.123456789' as timestamp_ns)
                > cast('2024-02-29 12:34:56.123456' as datetimev2(6)),
            datediff(
                cast('2024-03-01 00:00:00.000000001' as timestamp_ns),
                cast('2024-02-29 23:59:59.999999' as datetimev2(6))),
            greatest(
                cast('2024-02-29 12:34:56.123456789' as timestamp_ns),
                cast('2024-02-29 12:34:56.123456' as datetimev2(6))),
            field(
                cast('2024-02-29 12:34:56.123456789' as timestamp_ns),
                cast('2024-02-29 12:34:56.123456' as datetimev2(6))),
            array(
                cast('2024-02-29 12:34:56.123456789' as timestamp_ns),
                cast('2024-02-29 12:34:56.123456' as datetimev2(6)))
    """
    sql "set debug_skip_fold_constant = false"
    qt_mixed_literals_fold mixedLiteralSql
    sql "set debug_skip_fold_constant = true"
    qt_mixed_literals_no_fold mixedLiteralSql
    sql "set debug_skip_fold_constant = false"

    def mixedCollectionLiteralSql = """
        select
            array_contains(
                array(cast('2024-01-02 03:04:05.123456789' as timestamp_ns)),
                cast('2024-01-02 03:04:05.123456' as datetimev2(6))),
            array_position(
                array(cast('2024-01-02 03:04:05.123456000' as timestamp_ns)),
                cast('2024-01-02 03:04:05.123456' as datetimev2(6))),
            array_remove(
                array(cast('2024-01-02 03:04:05.123456000' as timestamp_ns)),
                cast('2024-01-02 03:04:05.123456' as datetimev2(6))),
            array_pushback(
                array(cast('2024-01-02 03:04:05.123456789' as timestamp_ns)),
                cast('2024-01-02 03:04:05.123456' as datetimev2(6))),
            array_pushfront(
                array(cast('2024-01-02 03:04:05.123456789' as timestamp_ns)),
                cast('2024-01-02 03:04:05.123456' as datetimev2(6))),
            map_contains_key(
                map(cast('2024-01-02 03:04:05.123456000' as timestamp_ns), 1),
                cast('2024-01-02 03:04:05.123456' as datetimev2(6))),
            map_contains_value(
                map(1, cast('2024-01-02 03:04:05.123456000' as timestamp_ns)),
                cast('2024-01-02 03:04:05.123456' as datetimev2(6))),
            array_contains(
                array(cast('2024-01-02 03:04:05.123456' as datetimev2(6))),
                cast('2024-01-02 03:04:05.123456000' as timestamp_ns))
    """
    qt_mixed_collection_literals_fold mixedCollectionLiteralSql
    sql "set debug_skip_fold_constant = true"
    qt_mixed_collection_literals_no_fold mixedCollectionLiteralSql
    sql "set debug_skip_fold_constant = false"

    order_qt_mixed_value_expressions """
        select id,
               if(id % 2 = 0, ts,
                  cast('2024-02-29 12:34:56.123456' as datetimev2(6))),
               ifnull(ts, cast('2024-02-29 12:34:56.123456' as datetimev2(6))),
               coalesce(ts, cast('2024-02-29 12:34:56.123456' as datetimev2(6))),
               nvl(ts, cast('2024-02-29 12:34:56.123456' as datetimev2(6))),
               nullif(ts, cast('2024-02-29 12:34:56.123456' as datetimev2(6))),
               greatest(ts, cast('2024-02-29 12:34:56.123456' as datetimev2(6))),
               least(ts, cast('2024-02-29 12:34:56.123456' as datetimev2(6))),
               case when id % 2 = 0 then ts
                    else cast('2024-02-29 12:34:56.123456' as datetimev2(6)) end
        from timestamp_ns_mixed_datetime_expressions
        order by id
    """
    order_qt_mixed_in_literals """
        select id,
               ts in (cast('2024-02-29 12:34:56.123456' as datetimev2(6))),
               ts not in (cast('2024-02-29 12:34:56.123456' as datetimev2(6)))
        from timestamp_ns_mixed_datetime_expressions
        order by id
    """
    order_qt_mixed_complex_constructors """
        select id,
               array(ts, cast('2024-02-29 12:34:56.123456' as datetimev2(6))),
               map('ts', ts, 'dt',
                   cast('2024-02-29 12:34:56.123456' as datetimev2(6))),
               named_struct('ts', ts, 'dt', dt),
               array_zip(array(ts), array(dt))
        from timestamp_ns_mixed_datetime_expressions
        order by id
    """
    order_qt_mixed_union_explicit_cast """
        select value from (
            select id, ts as value
            from timestamp_ns_mixed_datetime_expressions
            union all
            select 100,
                   cast(cast('2024-02-29 12:34:56.123456' as datetimev2(6))
                        as timestamp_ns)
        ) mixed_union_literal
        order by value
    """
    order_qt_mixed_join """
        select l.id, r.id
        from (select * from timestamp_ns_mixed_datetime_expressions where id <= 3 or id = 7) l
        join (select * from timestamp_ns_mixed_datetime_expressions where id <= 3 or id = 7) r
          on l.ts = r.dt
        order by l.id, r.id
    """
    explain {
        sql """
            shape plan
            select l.id
            from (select * from timestamp_ns_mixed_datetime_expressions where id <= 3 or id = 7) l
            join (select * from timestamp_ns_mixed_datetime_expressions where id <= 3 or id = 7) r
              on l.ts = r.dt
        """
        contains "hashJoin"
    }

    // All difference functions accept mixed columns without narrowing DATETIMEV2.
    def diffFunctions = [
        "timediff", "nanoseconds_diff", "microseconds_diff", "milliseconds_diff",
        "seconds_diff", "minutes_diff", "hours_diff", "days_diff",
        "weeks_diff", "months_diff", "quarters_diff", "years_diff"
    ]
    diffFunctions.each { functionName ->
        "order_qt_mixed_column_${functionName}" """
            select id, ${functionName}(ts, dt), ${functionName}(dt, ts)
            from timestamp_ns_mixed_datetime_expressions
            order by id
        """
    }

    def floorCeilFunctions = [
        "year_floor", "year_ceil", "quarter_floor", "quarter_ceil",
        "month_floor", "month_ceil", "week_floor", "week_ceil",
        "day_floor", "day_ceil", "hour_floor", "hour_ceil",
        "minute_floor", "minute_ceil", "second_floor", "second_ceil"
    ]
    floorCeilFunctions.each { functionName ->
        "order_qt_mixed_column_${functionName}" """
            select id, ${functionName}(ts, 1, dt), ${functionName}(dt, 1, ts)
            from timestamp_ns_mixed_datetime_expressions
            where id between 1 and 3
            order by id
        """
    }

    for (def rangeFunction : ["array_range", "sequence"]) {
        "order_qt_mixed_column_${rangeFunction}" """
            select id, ${rangeFunction}(ts, dt, interval 1 second)
            from timestamp_ns_mixed_datetime_expressions
            where id in (2, 3)
            order by id
        """
    }

    order_qt_mixed_in_columns """
        select id, ts in (dt), ts not in (dt), dt in (ts), dt not in (ts)
        from timestamp_ns_mixed_datetime_expressions
        order by id
    """
    order_qt_mixed_column_value_expressions """
        select id,
               case when id = 1 then ts else dt end,
               if(id = 1, ts, dt),
               ifnull(ts, dt),
               coalesce(ts, dt),
               nvl(ts, dt),
               nullif(ts, dt),
               greatest(ts, dt),
               least(ts, dt),
               array(ts, dt),
               map('ts', ts, 'dt', dt),
               array_contains(array(ts), dt)
        from timestamp_ns_mixed_datetime_expressions
        order by id
    """
    order_qt_mixed_union_columns """
        select value from (
            select ts as value from timestamp_ns_mixed_datetime_expressions
            union all
            select dt from timestamp_ns_mixed_datetime_expressions
        ) mixed_union_columns
        order by value
    """
    order_qt_mixed_union_literal """
        select value from (
            select ts as value from timestamp_ns_mixed_datetime_expressions
            union all
            select cast('2024-02-29 12:34:56.123456' as datetimev2(6))
        ) mixed_union_literal
        order by value
    """

    sql "drop table if exists timestamp_ns_mixed_date_families"
    sql """
        create table timestamp_ns_mixed_date_families (
            id int,
            ts timestamp_ns,
            d date,
            d2 datev2,
            dt datetime,
            tz timestamptz(6)
        )
        duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into timestamp_ns_mixed_date_families values
        (1, '2024-01-02 00:00:00.000000000', '2024-01-02', '2024-01-02',
            '2024-01-02 00:00:00', cast('2024-01-02 00:00:00+08:00' as timestamptz(6))),
        (2, '2024-01-02 00:00:00.000000001', '2024-01-02', '2024-01-02',
            '2024-01-02 00:00:00', cast('2024-01-02 00:00:00+08:00' as timestamptz(6))),
        (3, '1677-09-21 00:12:43.145224192', '1677-09-21', '1677-09-21',
            '1700-01-01 00:00:00', cast('1700-01-01 00:00:00+08:00' as timestamptz(6))),
        (4, '2262-04-11 23:47:16.854775807', '2262-04-11', '2262-04-11',
            '2200-01-01 00:00:00', cast('2200-01-01 00:00:00+08:00' as timestamptz(6))),
        (5, '1970-01-01 00:00:00.000000000', '2500-01-01', '2500-01-01',
            '2200-01-01 00:00:00', cast('2200-01-01 00:00:00+08:00' as timestamptz(6))),
        (6, null, null, null, null, null)
    """
    order_qt_mixed_date_family_comparisons """
        select id,
               ts = d, d = ts, ts < d, d < ts,
               ts = d2, d2 = ts, ts < d2, d2 < ts,
               ts = dt, dt = ts, ts < dt, dt < ts,
               ts = tz, tz = ts, ts < tz, tz < ts
        from timestamp_ns_mixed_date_families
        order by id
    """
    order_qt_mixed_date_family_in """
        select id,
               ts in (d), d in (ts),
               ts in (d2), d2 in (ts),
               ts in (dt), dt in (ts),
               ts in (tz), tz in (ts)
        from timestamp_ns_mixed_date_families
        order by id
    """
    order_qt_mixed_date_family_boundary_literals """
        select id,
               ts < cast('2500-01-01' as datev2),
               ts in (cast('2500-01-01' as datev2), cast('1970-01-01' as datev2), null),
               ts not in (cast('2500-01-01' as datev2), cast('1970-01-01' as datev2), null)
        from timestamp_ns_mixed_date_families
        order by id
    """

    order_qt_mixed_datetime_family_functions """
        select id,
               seconds_diff(ts, dt), seconds_diff(dt, ts),
               seconds_diff(ts, tz), seconds_diff(tz, ts),
               nullif(ts, d), nullif(d2, ts),
               nullif(ts, dt), nullif(tz, ts),
               greatest(ts, dt), least(ts, tz)
        from timestamp_ns_mixed_date_families
        order by id
    """

    sql "drop table if exists timestamp_ns_mixed_datetime_overflow"
    sql """
        create table timestamp_ns_mixed_datetime_overflow (
            id int,
            dt datetime,
            dtv2 datetimev2(6),
            tz timestamptz(6)
        )
        duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into timestamp_ns_mixed_datetime_overflow values
        (1, '2500-01-01 00:00:00', '2500-01-01 00:00:00.000000',
            cast('2262-04-11 23:47:16.854776+08:00' as timestamptz(6)))
    """

    // Comparisons, differences and NULLIF do not need a common result type, so values outside the
    // TIMESTAMP_NS epoch-nanosecond range remain executable.
    qt_mixed_datetime_overflow_without_narrowing """
        select
            cast('1970-01-01 00:00:00' as timestamp_ns) = dt,
            cast('2262-04-11 23:47:16.854775807' as timestamp_ns) < dtv2,
            cast('2262-04-11 23:47:16.854775807' as timestamp_ns)
                < cast('9999-05-11 23:47:16' as datetimev2(6)),
            cast('2262-04-11 23:47:16.854775807' as timestamp_ns) < tz,
            datediff(cast('1970-01-01 00:00:00' as timestamp_ns), dtv2),
            seconds_diff(cast('1970-01-01 00:00:00' as timestamp_ns), dtv2),
            nullif(cast('2262-04-11 23:47:16.854775807' as timestamp_ns), dtv2),
            nullif(dtv2, cast('2262-04-11 23:47:16.854775807' as timestamp_ns))
        from timestamp_ns_mixed_datetime_overflow
    """

    // Expressions that produce a common temporal value still normalize to TIMESTAMP_NS and retain
    // strict range checking.
    test {
        sql """
            select greatest(cast('1970-01-01 00:00:00' as timestamp_ns), tz)
            from timestamp_ns_mixed_datetime_overflow
        """
        exception "can not cast timestamptz"
    }
    test {
        sql """
            select case when id = 0 then cast('1970-01-01 00:00:00' as timestamp_ns)
                        else dtv2 end
            from timestamp_ns_mixed_datetime_overflow
        """
        exception "TIMESTAMP_NS overflow"
    }
    test {
        sql """
            select array(cast('1970-01-01 00:00:00' as timestamp_ns), dtv2)
            from timestamp_ns_mixed_datetime_overflow
        """
        exception "TIMESTAMP_NS overflow"
    }
    test {
        sql """
            select cast('1970-01-01 00:00:00' as timestamp_ns)
            union all
            select dtv2 from timestamp_ns_mixed_datetime_overflow
        """
        exception "TIMESTAMP_NS overflow"
    }

    // Single-temporal-argument functions do not perform mixed-type resolution and are covered by
    // test_timestamp_ns_functions.groovy. Explicit casts remain the user-controlled escape hatch.
    order_qt_explicit_cast_directions """
        select id,
               datediff(ts, cast(dt as timestamp_ns)),
               datediff(cast(ts as datetimev2(6)), dt),
               ts = cast(dt as timestamp_ns),
               cast(ts as datetimev2(6)) = dt
        from timestamp_ns_mixed_datetime_expressions
        where id between 1 and 5
        order by id
    """
}
