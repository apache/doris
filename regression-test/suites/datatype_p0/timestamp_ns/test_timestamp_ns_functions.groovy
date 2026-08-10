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

suite("test_timestamp_ns_functions") {
    sql "set time_zone = '+08:00'"
    sql "drop table if exists timestamp_ns_functions"
    sql """
        create table timestamp_ns_functions (
            id int,
            value timestamp_ns
        )
        duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into timestamp_ns_functions values
        (1, '1677-09-21 00:12:43.145224192'),
        (2, '1969-12-31 23:59:59.999999999'),
        (3, '1970-01-01 00:00:00.000000000'),
        (4, '1970-01-01 00:00:00.000000001'),
        (5, '2024-02-29 12:34:56.123456789'),
        (6, '2262-04-11 23:47:16.854775807'),
        (7, null)
    """

    order_qt_extract_calendar """
        select id,
               year(value), century(value), quarter(value), month(value), day(value),
               dayofmonth(value), dayofweek(value), dayofyear(value), weekday(value),
               week(value), week(value, 3), weekofyear(value), yearweek(value), yearweek(value, 3),
               dayname(value), monthname(value)
        from timestamp_ns_functions
        order by id
    """
    order_qt_extract_time """
        select id,
               hour(value), minute(value), second(value), microsecond(value),
               year_month(value), day_hour(value), day_minute(value), day_second(value),
               day_microsecond(value), hour_minute(value), hour_second(value),
               hour_microsecond(value), minute_second(value), minute_microsecond(value),
               second_microsecond(value)
        from timestamp_ns_functions
        order by id
    """
    order_qt_format_and_convert """
        select id,
               date_format(value, '%Y-%m-%d %H:%i:%s.%f'),
               time_format(value, '%H:%i:%s.%f'),
               to_iso8601(value),
               cast(to_json(value) as string),
               date(value), datev2(value), to_date(value), to_datev2(value),
               timestamp(value), time(value),
               to_days(value), to_seconds(value), unix_timestamp(value),
               second_timestamp(value), millisecond_timestamp(value), microsecond_timestamp(value),
               last_day(value), to_monday(value)
        from timestamp_ns_functions
        order by id
    """

    order_qt_simple_add_sub """
        select id,
               microseconds_add(value, 1), microseconds_sub(value, 1),
               milliseconds_add(value, 1), milliseconds_sub(value, 1),
               seconds_add(value, 1), seconds_sub(value, 1),
               minutes_add(value, 1), minutes_sub(value, 1),
               hours_add(value, 1), hours_sub(value, 1),
               days_add(value, 1), days_sub(value, 1),
               weeks_add(value, 1), weeks_sub(value, 1),
               months_add(value, 1), months_sub(value, 1),
               quarters_add(value, 1), quarters_sub(value, 1),
               years_add(value, 1), years_sub(value, 1)
        from timestamp_ns_functions
        where id between 2 and 5
        order by id
    """

    order_qt_compound_add_sub """
        select id,
               date_add(value, interval '1 02' day_hour),
               date_sub(value, interval '1 02' day_hour),
               date_add(value, interval '1 02:03' day_minute),
               date_sub(value, interval '1 02:03' day_minute),
               date_add(value, interval '1 02:03:04' day_second),
               date_sub(value, interval '1 02:03:04' day_second),
               date_add(value, interval '1 02:03:04.000005' day_microsecond),
               date_sub(value, interval '1 02:03:04.000005' day_microsecond),
               date_add(value, interval '2:03' hour_minute),
               date_sub(value, interval '2:03' hour_minute),
               date_add(value, interval '2:03:04' hour_second),
               date_sub(value, interval '2:03:04' hour_second),
               date_add(value, interval '2:03:04.000005' hour_microsecond),
               date_sub(value, interval '2:03:04.000005' hour_microsecond),
               date_add(value, interval '3:04' minute_second),
               date_sub(value, interval '3:04' minute_second),
               date_add(value, interval '3:04.000005' minute_microsecond),
               date_sub(value, interval '3:04.000005' minute_microsecond),
               date_add(value, interval '4.000005' second_microsecond),
               date_sub(value, interval '4.000005' second_microsecond),
               date_add(value, interval '1-2' year_month),
               date_sub(value, interval '1-2' year_month)
        from timestamp_ns_functions
        where id = 5
        order by id
    """

    order_qt_add_time_sub_time """
        select id,
               add_time(value, cast('01:02:03.123456' as time(6))),
               sub_time(value, cast('01:02:03.123456' as time(6)))
        from timestamp_ns_functions
        where id between 2 and 5
        order by id
    """

    qt_diff_units """
        select
            timediff(
                cast('1970-01-01 00:00:01.123456789' as timestamp_ns),
                cast('1969-12-31 23:59:59.999999999' as timestamp_ns)),
            microseconds_diff(
                cast('1970-01-01 00:00:00.000001999' as timestamp_ns),
                cast('1970-01-01 00:00:00.000000000' as timestamp_ns)),
            milliseconds_diff(
                cast('1970-01-01 00:00:00.001999999' as timestamp_ns),
                cast('1970-01-01 00:00:00.000000000' as timestamp_ns)),
            seconds_diff(
                cast('1970-01-01 00:00:01.999999999' as timestamp_ns),
                cast('1970-01-01 00:00:00.000000000' as timestamp_ns)),
            minutes_diff(
                cast('1970-01-01 00:01:59.999999999' as timestamp_ns),
                cast('1970-01-01 00:00:00.000000000' as timestamp_ns)),
            hours_diff(
                cast('1970-01-01 01:59:59.999999999' as timestamp_ns),
                cast('1970-01-01 00:00:00.000000000' as timestamp_ns)),
            days_diff(cast('2024-03-01 12:00:00.000000001' as timestamp_ns),
                      cast('2024-02-29 12:00:00.000000000' as timestamp_ns)),
            weeks_diff(cast('2024-03-07 12:00:00.000000001' as timestamp_ns),
                       cast('2024-02-29 12:00:00.000000000' as timestamp_ns)),
            months_diff(cast('2024-03-29 12:00:00.000000001' as timestamp_ns),
                        cast('2024-02-29 12:00:00.000000000' as timestamp_ns)),
            quarters_diff(cast('2024-05-29 12:00:00.000000001' as timestamp_ns),
                          cast('2024-02-29 12:00:00.000000000' as timestamp_ns)),
            years_diff(cast('2025-02-28 12:00:00.000000001' as timestamp_ns),
                       cast('2024-02-29 12:00:00.000000000' as timestamp_ns)),
            datediff(cast('2024-03-01 00:00:00.000000001' as timestamp_ns),
                     cast('2024-02-29 23:59:59.999999999' as timestamp_ns))
    """

    order_qt_trunc """
        select id,
               date_trunc(value, 'year'), date_trunc(value, 'quarter'),
               date_trunc(value, 'month'), date_trunc(value, 'week'),
               date_trunc(value, 'day'), date_trunc(value, 'hour'),
               date_trunc(value, 'minute'), date_trunc(value, 'second')
        from timestamp_ns_functions
        where id between 2 and 5
        order by id
    """
    order_qt_floor """
        select id,
               year_floor(value), quarter_floor(value), month_floor(value), week_floor(value),
               day_floor(value), hour_floor(value), minute_floor(value), second_floor(value)
        from timestamp_ns_functions
        where id between 2 and 5
        order by id
    """
    order_qt_ceil """
        select id,
               year_ceil(value), quarter_ceil(value), month_ceil(value), week_ceil(value),
               day_ceil(value), hour_ceil(value), minute_ceil(value), second_ceil(value)
        from timestamp_ns_functions
        where id between 2 and 5
        order by id
    """
    qt_floor_ceil_period_origin """
        select
            second_floor(cast('1970-01-01 00:00:02.123456789' as timestamp_ns), 2),
            second_ceil(cast('1970-01-01 00:00:02.123456789' as timestamp_ns), 2),
            second_floor(
                cast('1970-01-01 00:00:02.123456789' as timestamp_ns),
                cast('1970-01-01 00:00:00.500000000' as timestamp_ns)),
            second_ceil(
                cast('1970-01-01 00:00:02.123456789' as timestamp_ns), 2,
                cast('1970-01-01 00:00:00.500000000' as timestamp_ns))
    """

    qt_sequence_second """
        select sequence(
            cast('1969-12-31 23:59:58.000000001' as timestamp_ns),
            cast('1970-01-01 00:00:02.000000001' as timestamp_ns),
            interval 1 second)
    """
    qt_sequence_month """
        select sequence(
            cast('2024-01-31 12:34:56.123456789' as timestamp_ns),
            cast('2024-05-01 12:34:56.123456789' as timestamp_ns),
            interval 1 month)
    """
    qt_convert_tz """
        select convert_tz(cast('2024-02-29 12:34:56.123456789' as timestamp_ns),
                          '+08:00', '+00:00')
    """
    qt_comparison_helpers """
        select
            least(cast('1969-12-31 23:59:59.999999999' as timestamp_ns),
                  cast('1970-01-01 00:00:00.000000001' as timestamp_ns)),
            greatest(cast('1969-12-31 23:59:59.999999999' as timestamp_ns),
                     cast('1970-01-01 00:00:00.000000001' as timestamp_ns)),
            field(cast('1970-01-01 00:00:00.000000001' as timestamp_ns),
                  cast('1969-12-31 23:59:59.999999999' as timestamp_ns),
                  cast('1970-01-01 00:00:00.000000001' as timestamp_ns))
    """
    qt_aggregate_helpers """
        select min(value), max(value), min_by(value, id), max_by(value, id)
        from timestamp_ns_functions
    """
    qt_event_aggregates """
        select
            sequence_count('(?1)(?2)', value, id = 2, id = 3),
            sequence_match('(?1)(?2)', value, id = 2, id = 3),
            window_funnel(86400, 'default', value, id = 2, id = 3),
            topn_weighted(value, cast(id as bigint), 3)
        from timestamp_ns_functions
    """
    order_qt_window_functions """
        select id,
               lag(value) over(order by id),
               lead(value) over(order by id),
               first_value(value) over(order by id rows between unbounded preceding and current row),
               last_value(value) over(order by id rows between current row and unbounded following)
        from timestamp_ns_functions
        order by id
    """

    // Arithmetic and truncation never wrap across the signed epoch-nanosecond domain.
    for (def boundarySql : [
            "select microseconds_sub(cast('1677-09-21 00:12:43.145224192' as timestamp_ns), 1)",
            "select seconds_add(cast('2262-04-11 23:47:16.854775807' as timestamp_ns), 1)",
            "select date_trunc(cast('1677-09-21 00:12:43.145224192' as timestamp_ns), 'second')",
            "select second_floor(cast('1677-09-21 00:12:43.145224192' as timestamp_ns))",
            "select second_ceil(cast('2262-04-11 23:47:16.854775807' as timestamp_ns))"]) {
        test {
            sql boundarySql
            exception "out of range"
        }
    }
}
