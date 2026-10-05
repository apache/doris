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

suite("test_timestamp_ns_expressions") {
    sql "set time_zone = '+08:00'"
    sql "drop table if exists timestamp_ns_expressions"
    sql """
        create table timestamp_ns_expressions (
            id int,
            value timestamp_ns,
            fallback timestamp_ns
        )
        duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into timestamp_ns_expressions values
        (1, '1677-09-21 00:12:43.145224192', '1970-01-01 00:00:00.000000000'),
        (2, '1969-12-31 23:59:59.999999999', '1970-01-01 00:00:00.000000001'),
        (3, '1970-01-01 00:00:00.000000000', '2024-02-29 12:34:56.123456789'),
        (4, '1970-01-01 00:00:00.000000001', null),
        (5, '2262-04-11 23:47:16.854775807', '2262-04-11 23:47:16.854775807'),
        (6, null, '1970-01-01 00:00:00.000000000'),
        (7, null, null)
    """

    order_qt_comparisons """
        select id,
               value = fallback,
               value != fallback,
               value > fallback,
               value >= fallback,
               value < fallback,
               value <= fallback,
               value <=> fallback
        from timestamp_ns_expressions
        order by id
    """
    order_qt_null_predicates """
        select id, value is null, value is not null
        from timestamp_ns_expressions
        order by id
    """
    order_qt_in_not_in """
        select id,
               value in (
                   cast('1677-09-21 00:12:43.145224192' as timestamp_ns),
                   cast('1970-01-01 00:00:00.000000000' as timestamp_ns)),
               value not in (
                   cast('1677-09-21 00:12:43.145224192' as timestamp_ns),
                   cast('1970-01-01 00:00:00.000000000' as timestamp_ns)),
               value in (
                   cast('1970-01-01 00:00:00.000000000' as timestamp_ns), null),
               value not in (
                   cast('1970-01-01 00:00:00.000000000' as timestamp_ns), null)
        from timestamp_ns_expressions
        order by id
    """
    order_qt_between """
        select id,
               value between cast('1969-12-31 23:59:59.999999999' as timestamp_ns)
                         and cast('1970-01-01 00:00:00.000000001' as timestamp_ns),
               value not between cast('1969-12-31 23:59:59.999999999' as timestamp_ns)
                             and cast('1970-01-01 00:00:00.000000001' as timestamp_ns)
        from timestamp_ns_expressions
        order by id
    """
    order_qt_boolean_composition """
        select id,
               (value is null or value >= cast('1970-01-01 00:00:00.000000000' as timestamp_ns)),
               not (value <=> fallback),
               (value is not null and fallback is not null)
        from timestamp_ns_expressions
        order by id
    """

    order_qt_condition_result """
        select id,
               if(id % 2 = 0, value, fallback),
               ifnull(value, fallback),
               coalesce(value, fallback,
                        cast('1970-01-01 00:00:00.000000000' as timestamp_ns)),
               nullif(value, fallback),
               case
                   when id = 1 then value
                   when id = 2 then fallback
                   when id = 3 then cast('1970-01-01 00:00:00.000000001' as timestamp_ns)
                   else null
               end
        from timestamp_ns_expressions
        order by id
    """

    // Keep explicit casts here to cover nested casts in value-producing expressions.
    order_qt_mixed_condition_result """
        select id,
               if(id % 2 = 0, value,
                  cast(cast('2024-02-29 12:34:56.123456' as datetimev2(6)) as timestamp_ns)),
               case when id % 2 = 0 then value
                    else cast(cast('2024-02-29 12:34:56.123456' as datetimev2(6)) as timestamp_ns) end
        from timestamp_ns_expressions
        order by id
    """

    sql "drop table if exists timestamp_ns_mixed_temporal"
    sql """
        create table timestamp_ns_mixed_temporal (
            id int,
            ts timestamp_ns,
            dt datetimev2(6),
            tz timestamptz(6),
            time_text varchar(32)
        )
        duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into timestamp_ns_mixed_temporal values
        (1, '2024-02-29 12:34:56.123456789', '2024-02-29 12:34:56.123456',
            cast('2024-02-29 04:34:56.123456+00:00' as timestamptz(6)), '12:34:56.123456'),
        (2, '2024-02-29 12:34:56.123456000', '2024-02-29 12:34:56.123456',
            cast('2024-02-29 04:34:56.123456+00:00' as timestamptz(6)), '12:34:56.123456'),
        (3, null, '2024-02-29 12:34:56.123456',
            cast('2024-02-29 04:34:56.123456+00:00' as timestamptz(6)), '12:34:56.123456')
    """

    // Mixed TIMESTAMP_NS/DATETIMEV2 comparisons normalize DATETIMEV2 to TIMESTAMP_NS.
    // Keep explicit TIMESTAMPTZ casts here to retain direct cast coverage.
    // Row 1 deliberately differs only below microsecond precision, while row 2 is exactly equal.
    order_qt_mixed_temporal_comparisons """
        select id,
               ts = dt, ts != dt, ts > dt, ts >= dt, ts < dt, ts <= dt, ts <=> dt,
               ts = cast(tz as timestamp_ns), ts != cast(tz as timestamp_ns),
               ts > cast(tz as timestamp_ns), ts >= cast(tz as timestamp_ns),
               ts < cast(tz as timestamp_ns), ts <= cast(tz as timestamp_ns),
               ts <=> cast(tz as timestamp_ns)
        from timestamp_ns_mixed_temporal
        order by id
    """
    order_qt_mixed_temporal_comparisons_overflow """
            select ts < cast('9999-12-31 23:59:59.999999' as datetimev2(6))
            from timestamp_ns_mixed_temporal
            where id = 1
        """
    order_qt_mixed_temporal_comparisons_underflow """
            select ts > cast('1600-01-01 00:00:00.000000' as datetimev2(6))
            from timestamp_ns_mixed_temporal
            where id = 1
    """
    qt_mixed_datetime_range_comparisons """
        select ts > dt,
               ts < cast('2024-02-29 12:34:56.123457' as datetimev2(6))
        from timestamp_ns_mixed_temporal
        where id = 1
    """
    order_qt_mixed_temporal_in """
        select id,
               ts in (cast(dt as timestamp_ns), cast(tz as timestamp_ns),
                      cast(time_text as time(6))),
               ts not in (cast(dt as timestamp_ns), cast(tz as timestamp_ns),
                          cast(time_text as time(6)))
        from timestamp_ns_mixed_temporal
        order by id
    """
    qt_mixed_time_comparisons """
        select
            cast('1677-09-21 00:12:43.145224192' as timestamp_ns)
                < cast('12:34:56.123456' as time(6)),
            cast('2262-04-11 23:47:16.854775807' as timestamp_ns)
                > cast('12:34:56.123456' as time(6))
    """

    order_qt_mixed_temporal_conditions """
        select id,
               if(id = 1, ts, cast(dt as timestamp_ns)),
               if(id = 1, ts, cast(tz as timestamp_ns)),
               cast(if(id = 1, ts, cast(time_text as time(6))) as time(6)),
               ifnull(ts, cast(dt as timestamp_ns)),
               ifnull(ts, cast(tz as timestamp_ns)),
               cast(ifnull(ts, cast(time_text as time(6))) as time(6)),
               coalesce(ts, cast(dt as timestamp_ns), cast(tz as timestamp_ns)),
               nullif(ts, cast(dt as timestamp_ns)),
               nullif(ts, cast(tz as timestamp_ns)),
               case when id = 1 then ts
                    when id = 2 then cast(dt as timestamp_ns)
                    else cast(tz as timestamp_ns) end,
               cast(case when id = 1 then ts
                         when id = 2 then cast(dt as timestamp_ns)
                         when id = 3 then cast(tz as timestamp_ns)
                         else cast(time_text as time(6)) end as time(6))
        from timestamp_ns_mixed_temporal
        order by id
    """

    order_qt_mixed_temporal_functions """
        select id,
               datediff(ts, cast(dt as timestamp_ns)),
               timediff(ts, cast(dt as timestamp_ns)),
               seconds_diff(ts, cast(dt as timestamp_ns)),
               datediff(ts, cast(tz as timestamp_ns)),
               timediff(ts, cast(tz as timestamp_ns)),
               seconds_diff(ts, cast(tz as timestamp_ns)),
               field(ts,
                     cast(cast('2024-02-29 12:34:56.123456' as datetimev2(6)) as timestamp_ns),
                     cast(cast('2024-02-29 04:34:56.123456+00:00' as timestamptz(6))
                          as timestamp_ns),
                     cast('12:34:56.123456' as time(6)))
        from timestamp_ns_mixed_temporal
        order by id
    """

    def mixedTemporalConstantsSql = """
        select
            cast('2024-02-29 12:34:56.123456789' as timestamp_ns)
                > cast('2024-02-29 12:34:56.123456' as datetimev2(6)),
            cast('2024-02-29 12:34:56.123456789' as timestamp_ns)
                > cast(cast('2024-02-29 04:34:56.123456+00:00' as timestamptz(6))
                       as timestamp_ns),
            cast('1677-09-21 00:12:43.145224192' as timestamp_ns)
                < cast('12:34:56.123456' as time(6)),
            if(true, cast('2024-02-29 12:34:56.123456789' as timestamp_ns),
                     cast(cast('2024-02-29 12:34:56.123456' as datetimev2(6)) as timestamp_ns)),
            if(true, cast('2024-02-29 12:34:56.123456789' as timestamp_ns),
                     cast(cast('2024-02-29 04:34:56.123456+00:00' as timestamptz(6))
                          as timestamp_ns)),
            cast(if(false, cast('2024-02-29 12:34:56.123456789' as timestamp_ns),
                           cast('12:34:56.123456' as time(6))) as time(6)),
            ifnull(cast(null as timestamp_ns),
                   cast(cast('2024-02-29 12:34:56.123456' as datetimev2(6)) as timestamp_ns)),
            coalesce(cast(null as timestamp_ns),
                     cast(cast('2024-02-29 04:34:56.123456+00:00' as timestamptz(6))
                          as timestamp_ns)),
            nullif(cast('2024-02-29 12:34:56.123456789' as timestamp_ns),
                   cast(cast('2024-02-29 12:34:56.123456' as datetimev2(6)) as timestamp_ns)),
            datediff(cast('2024-02-29 12:34:56.123456789' as timestamp_ns),
                     cast(cast('2024-02-29 04:34:56.123456+00:00' as timestamptz(6))
                          as timestamp_ns)),
            timediff(cast('2024-02-29 12:34:56.123456789' as timestamp_ns),
                     cast(cast('2024-02-29 04:34:56.123456+00:00' as timestamptz(6))
                          as timestamp_ns)),
            seconds_diff(cast('2024-02-29 12:34:56.123456789' as timestamp_ns),
                         cast(cast('2024-02-29 04:34:56.123456+00:00' as timestamptz(6))
                              as timestamp_ns))
    """
    qt_mixed_temporal_constants mixedTemporalConstantsSql
    testFoldConst(mixedTemporalConstantsSql)

    sql "set debug_skip_fold_constant = false"
    qt_folded_conditions """
        select
            if(true,
               cast('1970-01-01 00:00:00.000000001' as timestamp_ns),
               cast('1970-01-01 00:00:00.000000002' as timestamp_ns)),
            coalesce(null, cast('1970-01-01 00:00:00.000000001' as timestamp_ns)),
            cast('1970-01-01 00:00:00.000000001' as timestamp_ns)
                in (cast('1970-01-01 00:00:00.000000001' as timestamp_ns))
    """
    sql "set debug_skip_fold_constant = true"
    qt_runtime_conditions """
        select
            if(true,
               cast('1970-01-01 00:00:00.000000001' as timestamp_ns),
               cast('1970-01-01 00:00:00.000000002' as timestamp_ns)),
            coalesce(null, cast('1970-01-01 00:00:00.000000001' as timestamp_ns)),
            cast('1970-01-01 00:00:00.000000001' as timestamp_ns)
                in (cast('1970-01-01 00:00:00.000000001' as timestamp_ns))
    """
    sql "set debug_skip_fold_constant = false"
}
