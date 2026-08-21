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
        (4, '1677-09-21 00:12:43.145224192', '1677-09-21 00:12:43.145224'),
        (5, '2262-04-11 23:47:16.854775807', '2262-04-11 23:47:16.854776'),
        (6, '1970-01-01 00:00:00.000000000', '2500-01-01 00:00:00.000000'),
        (7, null, null)
    """

    // Scalar comparisons have an exact mixed physical-type kernel. Exercise every comparison
    // operator, both operand orders, nanosecond remainders, signed epoch values and NULL.
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
    qt_mixed_range_literals """
        select
            cast('1677-09-21 00:12:43.145224192' as timestamp_ns)
                > cast('1677-09-21 00:12:43.145224' as datetimev2(6)),
            cast('2262-04-11 23:47:16.854775807' as timestamp_ns)
                < cast('2262-04-11 23:47:16.854776' as datetimev2(6)),
            cast('1970-01-01 00:00:00.000000000' as timestamp_ns)
                < cast('9999-12-31 23:59:59.999999' as datetimev2(6)),
            cast('1970-01-01 00:00:00.000000000' as timestamp_ns)
                > cast('0001-01-01 00:00:00.000000' as datetimev2(6))
    """

    // Ordinary arithmetic follows the existing date-like numeric compatibility behavior. It is
    // deliberately distinct from interval arithmetic and from the exact comparison semantics.
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

    // A DATETIMEV2 literal inside the TIMESTAMP_NS range is promoted to TIMESTAMP_NS. Cover all
    // two-temporal-argument diff functions using the same mixed declared types.
    qt_mixed_diff_to_timestamp_ns """
        select
            datediff(
                ts, cast('2024-02-28 12:34:56.123456' as datetimev2(6))),
            timediff(
                ts, cast('2024-02-29 12:34:55.123456' as datetimev2(6))),
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

    // In the reverse direction an exactly representable TIMESTAMP_NS literal is demoted to the
    // DATETIMEV2 column/literal scale. Keep a representative for every diff return contract.
    qt_mixed_diff_to_datetimev2 """
        select
            datediff(
                dt, cast('2024-02-28 12:34:56.123456000' as timestamp_ns)),
            timediff(
                dt, cast('2024-02-29 12:34:55.123456000' as timestamp_ns)),
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

    // All floor/ceil units share the same exact-literal coercion rule for the custom origin.
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
            array(
                cast('2024-02-29 12:34:56.123456789' as timestamp_ns),
                cast('2024-02-29 12:34:56.123456' as datetimev2(6)))
    """
    sql "set debug_skip_fold_constant = false"
    qt_mixed_literals_fold mixedLiteralSql
    sql "set debug_skip_fold_constant = true"
    qt_mixed_literals_no_fold mixedLiteralSql
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
        from timestamp_ns_mixed_datetime_expressions l
        join timestamp_ns_mixed_datetime_expressions r on l.ts = r.dt
        order by l.id, r.id
    """
    explain {
        sql """
            shape plan
            select l.id
            from timestamp_ns_mixed_datetime_expressions l
            join timestamp_ns_mixed_datetime_expressions r on l.ts = r.dt
        """
        contains "NestedLoopJoin"
    }

    // Two non-literal temporal columns cannot be converted to either existing type without a
    // possible range or precision loss. Every homogeneous diff signature must reject the pair.
    def diffFunctions = [
        "datediff", "timediff", "microseconds_diff", "milliseconds_diff",
        "seconds_diff", "minutes_diff", "hours_diff", "days_diff",
        "weeks_diff", "months_diff", "quarters_diff", "years_diff"
    ]
    diffFunctions.each { functionName ->
        test {
            sql """
                select ${functionName}(ts, dt)
                from timestamp_ns_mixed_datetime_expressions
            """
            exception "Can not find the compatibility function signature"
        }
    }

    def floorCeilFunctions = [
        "year_floor", "year_ceil", "quarter_floor", "quarter_ceil",
        "month_floor", "month_ceil", "week_floor", "week_ceil",
        "day_floor", "day_ceil", "hour_floor", "hour_ceil",
        "minute_floor", "minute_ceil", "second_floor", "second_ceil"
    ]
    floorCeilFunctions.each { functionName ->
        test {
            sql """
                select ${functionName}(ts, 1, dt)
                from timestamp_ns_mixed_datetime_expressions
            """
            exception "Can not find the compatibility function signature"
        }
    }

    for (def rangeFunction : ["array_range", "sequence"]) {
        test {
            sql """
                select ${rangeFunction}(ts, dt, interval 1 second)
                from timestamp_ns_mixed_datetime_expressions
            """
            exception "Can not find the compatibility function signature"
        }
    }

    test {
        sql """
            select ts in (dt)
            from timestamp_ns_mixed_datetime_expressions
        """
        exception "unsupported in predicate"
    }
    test {
        sql """
            select case when id = 1 then ts else dt end
            from timestamp_ns_mixed_datetime_expressions
        """
        exception "Cannot find common type for case when"
    }
    for (def expression : [
            "if(id = 1, ts, dt)",
            "ifnull(ts, dt)",
            "coalesce(ts, dt)",
            "nvl(ts, dt)",
            "nullif(ts, dt)",
            "greatest(ts, dt)",
            "least(ts, dt)",
            "array(ts, dt)",
            "map('ts', ts, 'dt', dt)"]) {
        test {
            sql """
                select ${expression}
                from timestamp_ns_mixed_datetime_expressions
            """
            exception "Can not find the compatibility function signature"
        }
    }
    test {
        sql """
            select ts from timestamp_ns_mixed_datetime_expressions
            union all
            select dt from timestamp_ns_mixed_datetime_expressions
        """
        exception "Can not find compatible type"
    }
    test {
        sql """
            select ts from timestamp_ns_mixed_datetime_expressions
            union all
            select cast('2024-02-29 12:34:56.123456' as datetimev2(6))
        """
        exception "Can not find compatible type"
    }

    // Literal conversions must also reject values that cannot be represented by the selected
    // target because of TIMESTAMP_NS range or DATETIMEV2 scale.
    test {
        sql """
            select datediff(
                ts, cast('2500-01-01 00:00:00.000000' as datetimev2(6)))
            from timestamp_ns_mixed_datetime_expressions
        """
        exception "Can not find the compatibility function signature"
    }
    test {
        sql """
            select datediff(
                dt, cast('2024-02-29 12:34:56.123456001' as timestamp_ns))
            from timestamp_ns_mixed_datetime_expressions
        """
        exception "Can not find the compatibility function signature"
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
