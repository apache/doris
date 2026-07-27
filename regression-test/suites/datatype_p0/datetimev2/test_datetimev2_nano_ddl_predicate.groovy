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

suite("test_datetimev2_nano_ddl_predicate") {
    sql "drop table if exists test_datetimev2_nano_ddl_duplicate"
    sql """
        create table test_datetimev2_nano_ddl_duplicate (
            dt7 datetime(7),
            dt8 datetime(8),
            dt9 datetime(9),
            id int
        )
        duplicate key(dt7, dt8)
        distributed by hash(dt9) buckets 4
        properties("replication_num" = "1")
    """
    sql """
        insert into test_datetimev2_nano_ddl_duplicate values
        ('1677-09-21 00:12:43.1452242',
         '1677-09-21 00:12:43.14522420',
         '1677-09-21 00:12:43.145224192', 1),
        ('1969-12-31 23:59:59.9999999',
         '1969-12-31 23:59:59.99999999',
         '1969-12-31 23:59:59.999999999', 2),
        ('1970-01-01 00:00:00.0000000',
         '1970-01-01 00:00:00.00000000',
         '1970-01-01 00:00:00.000000000', 3),
        ('1970-01-01 00:00:00.1234568',
         '1970-01-01 00:00:00.12345679',
         '1970-01-01 00:00:00.123456789', 4),
        ('2262-04-11 23:47:16.8547758',
         '2262-04-11 23:47:16.85477580',
         '2262-04-11 23:47:16.854775807', 5),
        (null, null, null, 6)
    """
    order_qt_datetime_alias_scales """
        select dt7, dt8, dt9, id
        from test_datetimev2_nano_ddl_duplicate
        order by id
    """

    sql "drop table if exists test_datetimev2_nano_ddl_aggregate"
    sql """
        create table test_datetimev2_nano_ddl_aggregate (
            dt datetime(9),
            max_dt datetime(9) max,
            min_dt datetime(7) min
        )
        aggregate key(dt)
        distributed by hash(dt) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into test_datetimev2_nano_ddl_aggregate values
        ('1677-09-21 00:12:43.145224192',
         '1677-09-21 00:12:43.145224192',
         '1677-09-21 00:12:43.1452242'),
        ('1970-01-01 00:00:00.000000000',
         '1970-01-01 00:00:00.000000001',
         '1969-12-31 23:59:59.9999999'),
        ('1970-01-01 00:00:00.000000000',
         '2262-04-11 23:47:16.854775807',
         '1970-01-01 00:00:00.0000000'),
        ('2262-04-11 23:47:16.854775807',
         '2262-04-11 23:47:16.854775807',
         '2262-04-11 23:47:16.8547758')
    """
    order_qt_aggregate_key_and_values """
        select dt, max_dt, min_dt
        from test_datetimev2_nano_ddl_aggregate
        order by dt
    """

    sql "drop table if exists test_datetimev2_nano_ddl_unique"
    sql """
        create table test_datetimev2_nano_ddl_unique (
            dt datetime(9),
            value_dt datetime(8),
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
        insert into test_datetimev2_nano_ddl_unique values
        ('1677-09-21 00:12:43.145224192', '1677-09-21 00:12:43.14522420', 1),
        ('1970-01-01 00:00:00.000000000', '1970-01-01 00:00:00.00000000', 2),
        ('2262-04-11 23:47:16.854775807', '2262-04-11 23:47:16.85477580', 3)
    """
    sql """
        insert into test_datetimev2_nano_ddl_unique values
        ('1970-01-01 00:00:00.000000000', '1970-01-01 00:00:00.00000001', 20)
    """
    order_qt_unique_mow_primary_semantics """
        select dt, value_dt, value
        from test_datetimev2_nano_ddl_unique
        order by dt
    """

    test {
        sql """
            create table test_datetimev2_nano_scale_overflow (
                id int,
                dt datetime(10)
            )
            distributed by hash(id) buckets 1
            properties("replication_num" = "1")
        """
        exception "Scale of Datetime must between 0 and 9"
    }

    sql """
        insert into test_datetimev2_nano_ddl_duplicate values
        ('1970-02-30 00:00:00.0000000',
         '1970-02-30 00:00:00.00000000',
         '1970-02-30 00:00:00.000000000', 100)
    """
    sql """
        insert into test_datetimev2_nano_ddl_duplicate values
        ('1677-09-21 00:12:43.1452241',
         '1677-09-21 00:12:43.14522419',
         '1677-09-21 00:12:43.145224191', 101)
    """
    sql """
        insert into test_datetimev2_nano_ddl_duplicate values
        ('2262-04-11 23:47:16.8547759',
         '2262-04-11 23:47:16.85477581',
         '2262-04-11 23:47:16.854775808', 102)
    """
    order_qt_invalid_and_overflow_values_become_null """
        select id, dt7, dt8, dt9
        from test_datetimev2_nano_ddl_duplicate
        where id between 100 and 102
        order by id
    """

    qt_current_timestamp_precision """
        select
            length(substring_index(cast(current_timestamp(7) as string), '.', -1)),
            length(substring_index(cast(current_timestamp(8) as string), '.', -1)),
            length(substring_index(cast(current_timestamp(9) as string), '.', -1))
    """

    sql "drop table if exists test_datetimev2_nano_current_default"
    sql """
        create table test_datetimev2_nano_current_default (
            id int,
            dt7 datetime(7) default current_timestamp(7),
            dt8 datetime(8) default current_timestamp(8),
            dt9 datetime(9) default current_timestamp(9)
        )
        unique key(id)
        distributed by hash(id) buckets 1
        properties(
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true"
        )
    """
    sql "insert into test_datetimev2_nano_current_default(id) values (1)"
    qt_current_timestamp_default """
        select id,
               dt7 is not null,
               dt8 is not null,
               dt9 is not null,
               length(substring_index(cast(dt7 as string), '.', -1)),
               length(substring_index(cast(dt8 as string), '.', -1)),
               length(substring_index(cast(dt9 as string), '.', -1))
        from test_datetimev2_nano_current_default
        order by id
    """

    sql "drop table if exists test_datetimev2_nano_partition_bucket"
    sql """
        create table test_datetimev2_nano_partition_bucket (
            dt datetime(9),
            id int
        )
        duplicate key(dt)
        partition by range(dt) (
            partition p_before_epoch values less than
                ('1970-01-01 00:00:00.000000000'),
            partition p_epoch values less than
                ('1970-01-01 00:00:00.000000002'),
            partition p_after_epoch values less than MAXVALUE
        )
        distributed by hash(dt) buckets 4
        properties("replication_num" = "1")
    """
    sql """
        insert into test_datetimev2_nano_partition_bucket values
        ('1677-09-21 00:12:43.145224192', 1),
        ('1969-12-31 23:59:59.999999999', 2),
        ('1970-01-01 00:00:00.000000000', 3),
        ('1970-01-01 00:00:00.000000001', 4),
        ('1970-01-01 00:00:00.123456789', 5),
        ('2262-04-11 23:47:16.854775807', 6)
    """
    explain {
        sql """
            select *
            from test_datetimev2_nano_partition_bucket
            where dt = cast('1970-01-01 00:00:00.000000000' as datetime(9))
        """
        contains "partitions=1/3 (p_epoch)"
        contains "tablets=1/4"
    }
    explain {
        sql """
            select *
            from test_datetimev2_nano_partition_bucket
            where dt >= cast('2262-04-11 23:47:16.854775807' as datetime(9))
        """
        contains "partitions=1/3 (p_after_epoch)"
    }

    order_qt_all_storage_predicates """
        select id,
               dt = cast('1970-01-01 00:00:00.000000000' as datetime(9)),
               dt != cast('1970-01-01 00:00:00.000000000' as datetime(9)),
               dt > cast('1970-01-01 00:00:00.000000000' as datetime(9)),
               dt >= cast('1970-01-01 00:00:00.000000000' as datetime(9)),
               dt < cast('1970-01-01 00:00:00.000000000' as datetime(9)),
               dt <= cast('1970-01-01 00:00:00.000000000' as datetime(9)),
               dt is null,
               dt is not null,
               dt in (
                   cast('1677-09-21 00:12:43.145224192' as datetime(9)),
                   cast('1970-01-01 00:00:00.000000000' as datetime(9)),
                   cast('2262-04-11 23:47:16.854775807' as datetime(9))),
               dt not in (
                   cast('1677-09-21 00:12:43.145224192' as datetime(9)),
                   cast('1970-01-01 00:00:00.000000000' as datetime(9)),
                   cast('2262-04-11 23:47:16.854775807' as datetime(9)))
        from test_datetimev2_nano_partition_bucket
        order by id
    """

    // Numeric constants use datetime literal coercion, not the physical epoch-nanosecond Int64.
    // Since 10 is not a valid YYYYMMDDHHMMSS value, constant folding produces an empty scan.
    order_qt_numeric_predicate """
        select id, dt
        from test_datetimev2_nano_partition_bucket
        where dt > 10
        order by id
    """
    explain {
        sql """
            select *
            from test_datetimev2_nano_partition_bucket
            where dt > 10
        """
        contains "VEMPTYSET"
    }

    order_qt_topn """
        select id, dt
        from test_datetimev2_nano_partition_bucket
        order by dt desc nulls last
        limit 3
    """

    sql "drop table if exists test_datetimev2_nano_generated"
    sql """
        create table test_datetimev2_nano_generated (
            id int,
            dt datetime(9),
            dt_date date generated always as (date(dt)),
            dt_plus_one datetime(9) generated always as (seconds_add(dt, 1))
        )
        duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into test_datetimev2_nano_generated(id, dt) values
        (1, '1677-09-21 00:12:43.145224192'),
        (2, '1970-01-01 00:00:00.000000000'),
        (3, '1970-01-01 00:00:00.123456789'),
        (4, '2262-04-11 23:47:15.854775807'),
        (5, null)
    """
    order_qt_generated_columns """
        select id, dt, dt_date, dt_plus_one
        from test_datetimev2_nano_generated
        order by id
    """

    order_qt_literals_timezone_and_functions """
        select
            cast('1677-09-21 00:12:43.145224192' as datetime(9)),
            cast('1970-01-01 00:00:00.000000000' as datetime(9)),
            cast('1970-02-30 00:00:00.000000000' as datetime(9)),
            cast('2262-04-11 23:47:16.854775808' as datetime(9)),
            cast('2262-04-11 23:47:16.854775807' as datetime(9)),
            convert_tz(
                cast('1970-01-01 08:00:00.123456789' as datetime(9)),
                'Asia/Shanghai', 'UTC'),
            date_format(
                cast('1970-01-01 00:00:00.123456789' as datetime(9)),
                '%Y-%m-%d %H:%i:%s.%f'),
            date_trunc(
                cast('1970-01-01 00:00:00.123456789' as datetime(9)),
                'second'),
            least(
                cast('1677-09-21 00:12:43.145224192' as datetime(9)),
                cast('1970-01-01 00:00:00.000000000' as datetime(9))),
            greatest(
                cast('1970-01-01 00:00:00.000000000' as datetime(9)),
                cast('2262-04-11 23:47:16.854775807' as datetime(9)))
    """

    order_qt_json_round_trip """
        select
            cast(
                json_extract_string(
                    json_object(
                        'dt',
                        cast(
                            cast('1677-09-21 00:12:43.145224192' as datetime(9))
                            as string)),
                    '\$.dt')
                as datetime(9)),
            cast(
                json_extract_string(
                    json_object(
                        'dt',
                        cast(
                            cast('1970-01-01 00:00:00.000000000' as datetime(9))
                            as string)),
                    '\$.dt')
                as datetime(9)),
            cast(
                json_extract_string(
                    json_object(
                        'dt',
                        cast(
                            cast('2262-04-11 23:47:16.854775807' as datetime(9))
                            as string)),
                    '\$.dt')
                as datetime(9))
    """

    sql "set enable_agg_state = true"
    sql "drop table if exists test_datetimev2_nano_agg_state"
    sql """
        create table test_datetimev2_nano_agg_state (
            id int,
            dt_state agg_state<max(datetime(9) not null)> generic
        )
        aggregate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into test_datetimev2_nano_agg_state values
        (1, max_state(cast('1677-09-21 00:12:43.145224192' as datetime(9)))),
        (1, max_state(cast('1970-01-01 00:00:00.000000000' as datetime(9)))),
        (1, max_state(cast('2262-04-11 23:47:16.854775807' as datetime(9))))
    """
    order_qt_aggregate_state """
        select id, max_merge(dt_state)
        from test_datetimev2_nano_agg_state
        group by id
        order by id
    """
}
