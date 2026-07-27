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

suite("test_datetimev2_nano") {
    sql "drop table if exists test_datetimev2_nano"
    sql """
        create table test_datetimev2_nano (
            id int,
            dt7 datetimev2(7),
            dt8 datetimev2(8),
            dt9 datetimev2(9)
        )
        duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """

    sql """
        insert into test_datetimev2_nano values
        (1, '1677-09-21 00:12:43.1452242',
            '1677-09-21 00:12:43.14522420',
            '1677-09-21 00:12:43.145224192'),
        (2, '1969-12-31 23:59:59.9999999',
            '1969-12-31 23:59:59.99999999',
            '1969-12-31 23:59:59.999999999'),
        (3, '1970-01-01 00:00:00.0000000',
            '1970-01-01 00:00:00.00000000',
            '1970-01-01 00:00:00.000000000'),
        (4, '1970-01-01 00:00:00.0000001',
            '1970-01-01 00:00:00.00000001',
            '1970-01-01 00:00:00.000000001'),
        (5, '2262-04-11 23:47:16.8547758',
            '2262-04-11 23:47:16.85477580',
            '2262-04-11 23:47:16.854775807')
    """

    order_qt_storage """
        select id, dt7, dt8, dt9
        from test_datetimev2_nano
        order by dt9
    """

    qt_rounding """
        select
            cast('1677-09-21 00:12:43.145224192' as datetimev2(9)),
            cast('1970-01-01 00:00:00.12345675' as datetimev2(7)),
            cast('1970-01-01 00:00:00.999999995' as datetimev2(8)),
            cast('1970-01-01 00:00:00.123456789' as datetimev2(9)),
            cast('2262-04-11 23:47:16.854775807' as datetimev2(9))
    """

    order_qt_functions """
        select id, date(dt9), year(dt9), microsecond(dt9),
               microseconds_add(dt9, if(id = 5, -1, 1)),
               seconds_add(dt9, if(id = 5, -1, 1))
        from test_datetimev2_nano
        where id in (1, 3, 5)
        order by id
    """

    qt_diff """
        select
            microseconds_diff(
                cast('1970-01-01 00:00:00.000000000' as datetimev2(9)),
                cast('1677-09-21 00:12:43.145224192' as datetimev2(9))),
            microseconds_diff(
                cast('1970-01-01 00:00:00.000001999' as datetimev2(9)),
                cast('1970-01-01 00:00:00.000000001' as datetimev2(9))),
            seconds_diff(
                cast('1970-01-01 00:00:01.000000000' as datetimev2(9)),
                cast('1970-01-01 00:00:00.000000001' as datetimev2(9))),
            microseconds_diff(
                cast('2262-04-11 23:47:16.854775807' as datetimev2(9)),
                cast('1970-01-01 00:00:00.000000000' as datetimev2(9)))
    """

    qt_cast_and_aggregate """
        select
            cast(cast('1677-09-21 00:12:43.145224192' as datetimev2(9)) as string),
            cast(cast('1970-01-01 00:00:00.123456789' as datetimev2(9)) as datetimev2(6)),
            cast(cast('1970-01-01 00:00:00.123456' as datetimev2(6)) as datetimev2(9)),
            cast(cast('2262-04-11 23:47:16.854775807' as datetimev2(9)) as string),
            min(dt9), max(dt9), count(distinct dt9)
        from test_datetimev2_nano
    """

    order_qt_filter """
        select id, dt9
        from test_datetimev2_nano
        where dt9 in (
            cast('1677-09-21 00:12:43.145224192' as datetimev2(9)),
            cast('1970-01-01 00:00:00.000000000' as datetimev2(9)),
            cast('2262-04-11 23:47:16.854775807' as datetimev2(9)))
        order by id
    """

    qt_timezone_and_unix_timestamp """
        select
            convert_tz(cast('1677-09-21 00:12:43.145224192' as datetimev2(9)),
                       'UTC', 'UTC'),
            convert_tz(cast('1970-01-01 00:00:00.123456789' as datetimev2(9)),
                       'UTC', 'Asia/Shanghai'),
            convert_tz(cast('2262-04-11 23:47:16.854775807' as datetimev2(9)),
                       'UTC', 'UTC'),
            unix_timestamp(cast('1677-09-21 00:12:43.145224192' as datetimev2(9))),
            unix_timestamp(cast('1970-01-01 00:00:00.000000000' as datetimev2(9))),
            unix_timestamp(cast('2262-04-11 23:47:16.854775807' as datetimev2(9)))
    """

    qt_out_of_range """
        select
            cast('1677-09-21 00:12:43.145224192' as datetimev2(9)),
            cast('1677-09-21 00:12:43.145224191' as datetimev2(9)),
            cast('1970-01-01 00:00:00.000000000' as datetimev2(9)),
            cast('2262-04-11 23:47:16.854775808' as datetimev2(9)),
            cast('2262-04-11 23:47:16.854775807' as datetimev2(9))
    """

    qt_all_comparisons """
        select
            cast('1677-09-21 00:12:43.145224192' as datetimev2(9))
                = cast('1677-09-21 00:12:43.145224192' as datetimev2(9)),
            cast('1677-09-21 00:12:43.145224192' as datetimev2(9))
                < cast('1970-01-01 00:00:00.000000000' as datetimev2(9)),
            cast('1969-12-31 23:59:59.999999999' as datetimev2(9))
                = cast('1969-12-31 23:59:59.999999999' as datetimev2(9)),
            cast('1969-12-31 23:59:59.999999999' as datetimev2(9))
                <> cast('1970-01-01 00:00:00.000000000' as datetimev2(9)),
            cast('1969-12-31 23:59:59.999999999' as datetimev2(9))
                < cast('1970-01-01 00:00:00.000000000' as datetimev2(9)),
            cast('1970-01-01 00:00:00.000000001' as datetimev2(9))
                > cast('1970-01-01 00:00:00.000000000' as datetimev2(9)),
            cast('1970-01-01 00:00:00.000000001' as datetimev2(9))
                >= cast('1970-01-01 00:00:00.000000001' as datetimev2(9)),
            cast('1970-01-01 00:00:00.000000001' as datetimev2(9))
                <= cast('1970-01-01 00:00:00.000000001' as datetimev2(9)),
            cast('1970-01-01 00:00:00.000000000' as datetimev2(9))
                < cast('2262-04-11 23:47:16.854775807' as datetimev2(9)),
            cast('2262-04-11 23:47:16.854775807' as datetimev2(9))
                = cast('2262-04-11 23:47:16.854775807' as datetimev2(9))
    """

    sql "drop table if exists test_datetimev2_nano_relational"
    sql """
        create table test_datetimev2_nano_relational (
            id int,
            dt datetimev2(9),
            payload varchar(16)
        )
        duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into test_datetimev2_nano_relational values
        (1, '1677-09-21 00:12:43.145224192', 'minimum'),
        (2, '1969-12-31 23:59:59.999999999', 'before'),
        (3, '1970-01-01 00:00:00.000000000', 'epoch-a'),
        (4, '1970-01-01 00:00:00.000000001', 'after'),
        (5, '2262-04-11 23:47:16.854775807', 'maximum'),
        (6, '1970-01-01 00:00:00.000000000', 'epoch-b'),
        (7, null, 'null')
    """

    qt_sort_limit """
        select id, dt
        from test_datetimev2_nano_relational
        order by dt asc nulls last, id
        limit 7
    """

    order_qt_group_by """
        select dt, count(*), min(id), max(id)
        from test_datetimev2_nano_relational
        group by dt
        order by dt nulls first
    """

    order_qt_hash_join """
        select l.id, r.id, l.dt
        from test_datetimev2_nano_relational l
        join test_datetimev2_nano_relational r on l.dt = r.dt
        order by l.id, r.id
    """

    qt_relational_aggregates """
        select
            count(dt),
            count(distinct dt),
            min(dt),
            max(dt),
            approx_count_distinct(dt)
        from test_datetimev2_nano_relational
    """

    sql "drop table if exists test_datetimev2_nano_unique"
    sql """
        create table test_datetimev2_nano_unique (
            dt datetimev2(9),
            value int
        )
        unique key(dt)
        distributed by hash(dt) buckets 1
        properties(
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true"
        )
    """
    sql """
        insert into test_datetimev2_nano_unique values
        ('1677-09-21 00:12:43.145224192', 1),
        ('1969-12-31 23:59:59.999999999', 2),
        ('1970-01-01 00:00:00.000000000', 3),
        ('1970-01-01 00:00:00.000000001', 4),
        ('2262-04-11 23:47:16.854775807', 5)
    """
    sql """
        insert into test_datetimev2_nano_unique values
        ('1970-01-01 00:00:00.000000000', 20)
    """
    order_qt_unique_key """
        select dt, value
        from test_datetimev2_nano_unique
        order by dt
    """

    sql "drop table if exists test_datetimev2_nano_aggregate"
    sql """
        create table test_datetimev2_nano_aggregate (
            dt datetimev2(9),
            amount bigint sum
        )
        aggregate key(dt)
        distributed by hash(dt) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into test_datetimev2_nano_aggregate values
        ('1677-09-21 00:12:43.145224192', 1),
        ('1969-12-31 23:59:59.999999999', 2),
        ('1970-01-01 00:00:00.000000000', 3),
        ('1970-01-01 00:00:00.000000000', 4),
        ('1970-01-01 00:00:00.000000001', 5),
        ('2262-04-11 23:47:16.854775807', 6)
    """
    order_qt_aggregate_key """
        select dt, amount
        from test_datetimev2_nano_aggregate
        order by dt
    """

    sql "drop table if exists test_datetimev2_nano_partition"
    sql """
        create table test_datetimev2_nano_partition (
            dt datetimev2(9),
            value int
        )
        duplicate key(dt)
        partition by range(dt) (
            partition p_minimum values less than ('1677-09-21 00:12:43.145224193'),
            partition p_before_epoch values less than ('1970-01-01 00:00:00.000000000'),
            partition p_epoch values less than ('1970-01-01 00:00:00.000000002'),
            partition p_before_maximum values less than ('2262-04-11 23:47:16.854775807'),
            partition p_after_epoch values less than MAXVALUE
        )
        distributed by hash(dt) buckets 1
        properties("replication_num" = "1")
    """
    order_qt_range_partition """
        select partition_name, partition_description
        from information_schema.partitions
        where table_schema = database()
          and table_name = 'test_datetimev2_nano_partition'
        order by partition_name
    """
    sql "drop table if exists test_datetimev2_nano_complex"
    sql """
        create table test_datetimev2_nano_complex (
            id int,
            values_array array<datetimev2(9)>
        )
        duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into test_datetimev2_nano_complex values
        (1, array(
            cast('2262-04-11 23:47:16.854775807' as datetimev2(9)),
            cast('1970-01-01 00:00:00.000000000' as datetimev2(9)),
            cast('1677-09-21 00:12:43.145224192' as datetimev2(9))
        )),
        (2, null)
    """
    order_qt_array_functions """
        select id,
               array_sort(values_array)
        from test_datetimev2_nano_complex
        order by id
    """

    sql "drop table if exists test_datetimev2_nano_index"
    sql """
        create table test_datetimev2_nano_index (
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
        insert into test_datetimev2_nano_index values
        (1, '1677-09-21 00:12:43.145224192'),
        (2, '1970-01-01 00:00:00.000000000'),
        (3, '2262-04-11 23:47:16.854775807')
    """
    order_qt_index_predicates """
        select id, dt
        from test_datetimev2_nano_index
        where dt in (
            cast('1677-09-21 00:12:43.145224192' as datetimev2(9)),
            cast('1970-01-01 00:00:00.000000000' as datetimev2(9)),
            cast('2262-04-11 23:47:16.854775807' as datetimev2(9)))
        order by id
    """

    sql "set debug_skip_fold_constant = false"
    qt_cast_with_constant_folding """
        select
            cast('1677-09-21 00:12:43.145224192' as datetimev2(9)),
            cast('1970-01-01 00:00:00.123456789' as datetimev2(7)),
            cast('1970-01-01 00:00:00.123456789' as datetimev2(8)),
            cast('1970-01-01 00:00:00.123456789' as datetimev2(9)),
            cast('2262-04-11 23:47:16.854775807' as datetimev2(9))
    """
    sql "set debug_skip_fold_constant = true"
    qt_cast_without_constant_folding """
        select
            cast('1677-09-21 00:12:43.145224192' as datetimev2(9)),
            cast('1970-01-01 00:00:00.123456789' as datetimev2(7)),
            cast('1970-01-01 00:00:00.123456789' as datetimev2(8)),
            cast('1970-01-01 00:00:00.123456789' as datetimev2(9)),
            cast('2262-04-11 23:47:16.854775807' as datetimev2(9))
    """
    sql "set debug_skip_fold_constant = false"

    qt_calendar_arithmetic """
        select
            microseconds_add(
                cast('1677-09-21 00:12:43.145224192' as datetimev2(9)), 1),
            months_add(
                cast('1970-01-01 00:00:00.000000000' as datetimev2(9)), 1),
            microseconds_sub(
                cast('2262-04-11 23:47:16.854775807' as datetimev2(9)), 1)
    """

    test {
        sql """
            create table test_datetimev2_nano_invalid_scale (
                id int,
                dt datetimev2(10)
            )
            distributed by hash(id) buckets 1
            properties("replication_num" = "1")
        """
        exception "Scale of Datetime must between 0 and 9"
    }
}
