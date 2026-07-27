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

suite("test_datetimev2_nano_runtime_cast_and_operators") {
    sql "set time_zone = '+08:00'"
    sql "drop table if exists test_datetimev2_nano_runtime_cast"
    sql """
        create table test_datetimev2_nano_runtime_cast (
            id int,
            string_value string,
            integer_value bigint,
            decimal_value decimal(23, 9),
            double_value double,
            date_value date,
            datetime6_value datetime(6),
            datetime9_value datetime(9)
        )
        duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into test_datetimev2_nano_runtime_cast values
        (1, '1677-09-21 00:12:43.145224192',
            16770921001243, 16770921001243.145224192, 16770921001243,
            '1677-09-21', '1677-09-21 00:12:43.145225',
            '1677-09-21 00:12:43.145224192'),
        (2, '1969-12-31 23:59:59.999999999',
            19691231235959, 19691231235959.999999999, 19691231235959,
            '1969-12-31', '1969-12-31 23:59:59.999999',
            '1969-12-31 23:59:59.999999999'),
        (3, '1970-01-01 00:00:00.000000000',
            19700101000000, 19700101000000.000000000, 19700101000000,
            '1970-01-01', '1970-01-01 00:00:00.000000',
            '1970-01-01 00:00:00.000000000'),
        (4, '2024-02-29 12:34:56.123456789',
            20240229123456, 20240229123456.123456789, 20240229123456.125,
            '2024-02-29', '2024-02-29 12:34:56.123457',
            '2024-02-29 12:34:56.123456789'),
        (5, '2262-04-11 23:47:16.854775807',
            22620411234716, 22620411234716.854775807, 22620411234716,
            '2262-04-11', '2262-04-11 23:47:16.854775',
            '2262-04-11 23:47:16.854775807'),
        (6, 'not-a-datetime',
            99991231235959, 99991231235959.999999999, 99991231235959,
            '1677-09-20', '2262-04-11 23:47:16.854776', null),
        (7, null, null, null, null, null, null, null)
    """

    order_qt_runtime_cast_to_nano """
        select id,
               cast(string_value as datetime(9)),
               cast(integer_value as datetime(9)),
               cast(decimal_value as datetime(9)),
               cast(double_value as datetime(9)),
               cast(date_value as datetime(9)),
               cast(datetime6_value as datetime(9))
        from test_datetimev2_nano_runtime_cast
        order by id
    """

    order_qt_runtime_cast_from_nano """
        select id,
               cast(datetime9_value as string),
               cast(datetime9_value as date),
               cast(datetime9_value as datetime(6)),
               cast(datetime9_value as time(6)),
               cast(datetime9_value as bigint),
               cast(datetime9_value as double)
        from test_datetimev2_nano_runtime_cast
        order by id
    """

    sql "drop table if exists test_datetimev2_nano_rounding_source"
    sql """
        create table test_datetimev2_nano_rounding_source (
            id int,
            value string
        )
        duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into test_datetimev2_nano_rounding_source values
        (1, '1677-09-21 00:12:43.145224191'),
        (2, '1677-09-21 00:12:43.145224192'),
        (3, '1970-01-01 00:00:00.123456745'),
        (4, '1970-01-01 00:00:00.123456755'),
        (5, '2262-04-11 23:47:16.854775807'),
        (6, '2262-04-11 23:47:16.8547758075'),
        (7, '1970-02-30 00:00:00.000000000'),
        (8, '2023-08-17T01:41:18.123456789Z'),
        (9, '2023-08-17 01:41:18.123456789'),
        (10, '2023-08-17T01:41:18.123456789America/Los_Angeles'),
        (11, '1677-09-21T00:12:43.145224192+14:00'),
        (12, '2262-04-11T23:47:16.854775807-01:00')
    """
    order_qt_runtime_rounding_overflow_and_timezone """
        select id,
               cast(value as datetime(7)),
               cast(value as datetime(8)),
               cast(value as datetime(9))
        from test_datetimev2_nano_rounding_source
        order by id
    """

    order_qt_nano_scale_reduction_boundaries """
        select id,
               cast(datetime9_value as datetime(7)),
               cast(datetime9_value as datetime(8)),
               cast(datetime9_value as datetime(9))
        from test_datetimev2_nano_runtime_cast
        where id in (1, 3, 5)
        order by id
    """

    order_qt_runtime_time_functions """
        select id,
               time(datetime9_value),
               timediff(datetime9_value,
                        cast('1970-01-01 00:00:00.000000000' as datetime(9))),
               to_days(datetime9_value),
               months_between(datetime9_value, date(datetime9_value))
        from test_datetimev2_nano_runtime_cast
        where id between 1 and 5
        order by id
    """

    qt_utc_timestamp_nano_scale """
        select length(substring_index(cast(utc_timestamp(7) as string), '.', -1)),
               length(substring_index(cast(utc_timestamp(8) as string), '.', -1)),
               length(substring_index(cast(utc_timestamp(9) as string), '.', -1))
    """

    order_qt_microsecond_arithmetic_preserves_scale """
        select microseconds_add(
                   cast('2024-02-29 12:34:56.123' as datetime(3)), 1),
               microseconds_sub(
                   cast('2024-02-29 12:34:56.1234567' as datetime(7)), 1),
               milliseconds_add(
                   cast('2024-02-29 12:34:56.12345678' as datetime(8)), 1),
               milliseconds_sub(
                   cast('2024-02-29 12:34:56.123456789' as datetime(9)), 1),
               date_add(
                   cast('2024-02-29 12:34:56.123456789' as datetime(9)),
                   interval '1.000001' second_microsecond),
               date_sub(
                   cast('2024-02-29 12:34:56.123456789' as datetime(9)),
                   interval '1 00:00:00.000001' day_microsecond)
    """

    sql "drop table if exists test_datetimev2_nano_operators"
    sql """
        create table test_datetimev2_nano_operators (
            id int,
            group_id int,
            event_id int,
            dt datetime(9)
        )
        duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into test_datetimev2_nano_operators values
        (1, 1, 1, '1969-12-31 23:59:59.999999999'),
        (2, 1, 2, '1970-01-01 00:00:00.000000000'),
        (3, 1, 3, '1970-01-01 00:00:00.000000001'),
        (4, 2, 1, '2024-02-29 12:34:56.123456789'),
        (5, 2, 2, '2024-02-29 12:34:57.123456789'),
        (6, 2, 3, '2262-04-11 23:47:16.854775807'),
        (7, 2, 4, null)
    """

    order_qt_window_operators """
        select id, dt,
               lag(dt, 1, null) over(partition by group_id order by dt),
               lead(dt, 1, null) over(partition by group_id order by dt),
               first_value(dt) over(partition by group_id order by dt
                                    rows between unbounded preceding and unbounded following),
               last_value(dt) over(partition by group_id order by dt
                                   rows between unbounded preceding and unbounded following)
        from test_datetimev2_nano_operators
        order by id
    """

    order_qt_set_operators """
        (select dt from test_datetimev2_nano_operators where id <= 4)
        union
        (select dt from test_datetimev2_nano_operators where id >= 4)
        order by 1
    """
    order_qt_intersect_operator """
        (select dt from test_datetimev2_nano_operators where id <= 4)
        intersect
        (select dt from test_datetimev2_nano_operators where id >= 4)
        order by 1
    """
    order_qt_except_operator """
        (select dt from test_datetimev2_nano_operators where id <= 4)
        except
        (select dt from test_datetimev2_nano_operators where id >= 4)
        order by 1
    """

    order_qt_distinct_and_conditional_expressions """
        select distinct
               coalesce(dt, cast('1970-01-01 00:00:00.000000000' as datetime(9))),
               case
                   when dt < cast('1970-01-01 00:00:00.000000000' as datetime(9)) then dt
                   else cast('1970-01-01 00:00:00.000000000' as datetime(9))
               end,
               nullif(dt, cast('1970-01-01 00:00:00.000000000' as datetime(9)))
        from test_datetimev2_nano_operators
        order by 1, 2, 3
    """

    order_qt_sequence_aggregates """
        select group_id,
               sequence_match('(?1)(?2)(?3)', dt,
                              event_id = 1, event_id = 2, event_id = 3),
               sequence_count('(?1)(?2)', dt, event_id = 1, event_id = 2),
               window_funnel(2, 'default', dt,
                             event_id = 1, event_id = 2, event_id = 3)
        from test_datetimev2_nano_operators
        where dt is not null
        group by group_id
        order by group_id
    """

    order_qt_cte_subquery """
        with nano_values as (
            select id, dt
            from test_datetimev2_nano_operators
            where dt >= cast('1970-01-01 00:00:00.000000000' as datetime(9))
        )
        select id, dt
        from nano_values
        where dt in (select max(dt) from nano_values)
        order by id
    """

    sql "drop view if exists test_datetimev2_nano_view"
    sql """
        create view test_datetimev2_nano_view as
        select id, dt
        from test_datetimev2_nano_operators
        where dt is not null
    """
    order_qt_view_round_trip """
        select id, dt
        from test_datetimev2_nano_view
        order by id
    """

    sql "drop table if exists test_datetimev2_nano_ctas"
    sql """
        create table test_datetimev2_nano_ctas
        properties("replication_num" = "1")
        as
        select id, dt
        from test_datetimev2_nano_operators
        where id in (1, 2, 3, 6, 7)
    """
    order_qt_ctas_round_trip """
        select id, dt
        from test_datetimev2_nano_ctas
        order by id
    """
    sql """
        insert overwrite table test_datetimev2_nano_ctas
        select id, dt
        from test_datetimev2_nano_operators
        where id in (1, 3, 6, 7)
    """
    order_qt_insert_overwrite_round_trip """
        select id, dt
        from test_datetimev2_nano_ctas
        order by id
    """

    sql "drop table if exists test_datetimev2_nano_update"
    sql """
        create table test_datetimev2_nano_update (
            id int,
            dt datetime(9),
            value int
        )
        unique key(id)
        distributed by hash(id) buckets 1
        properties(
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true"
        )
    """
    sql """
        insert into test_datetimev2_nano_update values
        (1, '1677-09-21 00:12:43.145224192', 1),
        (2, '1970-01-01 00:00:00.000000000', 2),
        (3, '2262-04-11 23:47:16.854775807', 3)
    """
    sql """
        update test_datetimev2_nano_update
        set dt = '1970-01-01 00:00:00.000000001', value = 20
        where dt = cast('1970-01-01 00:00:00.000000000' as datetime(9))
    """
    order_qt_update_with_nano_predicate """
        select id, dt, value
        from test_datetimev2_nano_update
        order by id
    """

    sql "drop table if exists test_datetimev2_nano_complex_storage"
    sql """
        create table test_datetimev2_nano_complex_storage (
            id int,
            map_value map<int, datetime(9)>,
            struct_value struct<minimum:datetime(9), epoch:datetime(9), maximum:datetime(9)>
        )
        duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into test_datetimev2_nano_complex_storage values
        (1,
         map(
             1, cast('1677-09-21 00:12:43.145224192' as datetime(9)),
             2, cast('1970-01-01 00:00:00.000000000' as datetime(9)),
             3, cast('2262-04-11 23:47:16.854775807' as datetime(9))),
         named_struct(
             'minimum', cast('1677-09-21 00:12:43.145224192' as datetime(9)),
             'epoch', cast('1970-01-01 00:00:00.000000000' as datetime(9)),
             'maximum', cast('2262-04-11 23:47:16.854775807' as datetime(9)))),
        (2, null, null)
    """
    order_qt_complex_storage_round_trip """
        select id, map_value, struct_value,
               element_at(map_value, 2),
               struct_element(struct_value, 'epoch')
        from test_datetimev2_nano_complex_storage
        order by id
    """
}
