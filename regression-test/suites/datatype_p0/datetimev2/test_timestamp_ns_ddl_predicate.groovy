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

suite("test_timestamp_ns_ddl_predicate") {
    for (def datetimeType : ["datetime", "datetimev2"]) {
        for (def invalidScale : [7, 8, 9]) {
            test {
                sql "select cast('1970-01-01 00:00:00.123456789' as ${datetimeType}(${invalidScale}))"
                exception "between 0 and 6"
            }
        }
    }

    sql "drop table if exists test_datetime_scale_above_six"
    test {
        sql """
            create table test_datetime_scale_above_six (
                id int,
                dt datetimev2(7)
            )
            distributed by hash(id) buckets 1
            properties("replication_num" = "1")
        """
        exception "between 0 and 6"
    }

    sql "drop table if exists test_timestamp_ns_values"
    // The legacy golden tags and dt7/dt8/dt9 names refer to input widths, not DATETIMEV2 scales.
    sql """
        create table test_timestamp_ns_values (
            id int,
            ts timestamp_ns,
            dt7 timestamp_ns,
            dt8 timestamp_ns,
            dt9 timestamp_ns
        )
        duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into test_timestamp_ns_values values
        (1,
         '1970-01-01 00:00:00.123456789',
         '1970-01-01 00:00:00.123456789',
         '1970-01-01 00:00:00.123456789',
         '1970-01-01 00:00:00.123456789')
    """
    order_qt_timestamp_ns_alias_values """
        select id, ts, dt7, dt8, dt9,
               ts = dt7, dt7 = dt8, dt8 = dt9,
               cast(dt7 as timestamp_ns) = dt7,
               cast(dt8 as timestamp_ns) = dt8
        from test_timestamp_ns_values
        order by id
    """
    order_qt_timestamp_ns_alias_schema """
        select ordinal_position, column_name, data_type, is_nullable
        from information_schema.columns
        where table_schema = database()
          and table_name = 'test_timestamp_ns_values'
        order by ordinal_position
    """

    test {
        sql "select cast('1970-01-01 00:00:00.000000001' as timestamp_ns(9))"
        exception "timestamp_ns does not support precision"
    }

    sql "drop table if exists test_timestamp_ns_ddl_duplicate"
    sql """
        create table test_timestamp_ns_ddl_duplicate (
            dt7 timestamp_ns,
            dt8 timestamp_ns,
            dt9 timestamp_ns,
            id int
        )
        duplicate key(dt7, dt8)
        distributed by hash(dt9) buckets 4
        properties("replication_num" = "1")
    """
    sql """
        insert into test_timestamp_ns_ddl_duplicate values
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
        from test_timestamp_ns_ddl_duplicate
        order by id
    """
    sql "drop table if exists test_timestamp_ns_ddl_aggregate"
    sql """
        create table test_timestamp_ns_ddl_aggregate (
            dt timestamp_ns,
            max_dt timestamp_ns max,
            min_dt timestamp_ns min
        )
        aggregate key(dt)
        distributed by hash(dt) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into test_timestamp_ns_ddl_aggregate values
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
        from test_timestamp_ns_ddl_aggregate
        order by dt
    """

    sql "drop table if exists test_timestamp_ns_ddl_unique"
    sql """
        create table test_timestamp_ns_ddl_unique (
            dt timestamp_ns,
            value_dt timestamp_ns,
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
        insert into test_timestamp_ns_ddl_unique values
        ('1677-09-21 00:12:43.145224192', '1677-09-21 00:12:43.14522420', 1),
        ('1970-01-01 00:00:00.000000000', '1970-01-01 00:00:00.00000000', 2),
        ('2262-04-11 23:47:16.854775807', '2262-04-11 23:47:16.85477580', 3)
    """
    sql """
        insert into test_timestamp_ns_ddl_unique values
        ('1970-01-01 00:00:00.000000000', '1970-01-01 00:00:00.00000001', 20)
    """
    order_qt_unique_mow_primary_semantics """
        select dt, value_dt, value
        from test_timestamp_ns_ddl_unique
        order by dt
    """

    test {
        sql """
            create table test_timestamp_ns_scale_overflow (
                id int,
                dt datetime(10)
            )
            distributed by hash(id) buckets 1
            properties("replication_num" = "1")
        """
        exception "between 0 and 6"
    }

    sql """
        insert into test_timestamp_ns_ddl_duplicate values
        ('1970-02-30 00:00:00.0000000',
         '1970-02-30 00:00:00.00000000',
         '1970-02-30 00:00:00.000000000', 100)
    """
    sql """
        insert into test_timestamp_ns_ddl_duplicate values
        ('1677-09-21 00:12:43.1452241',
         '1677-09-21 00:12:43.14522419',
         '1677-09-21 00:12:43.145224191', 101)
    """
    sql """
        insert into test_timestamp_ns_ddl_duplicate values
        ('2262-04-11 23:47:16.8547759',
         '2262-04-11 23:47:16.85477581',
         '2262-04-11 23:47:16.854775808', 102)
    """
    order_qt_invalid_and_overflow_values_become_null """
        select id, dt7, dt8, dt9
        from test_timestamp_ns_ddl_duplicate
        where id between 100 and 102
        order by id
    """

    sql "drop table if exists test_timestamp_ns_partition_bucket"
    sql """
        create table test_timestamp_ns_partition_bucket (
            dt timestamp_ns,
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
        insert into test_timestamp_ns_partition_bucket values
        ('1677-09-21 00:12:43.145224192', 1),
        ('1969-12-31 23:59:59.999999999', 2),
        ('1970-01-01 00:00:00.000000000', 3),
        ('1970-01-01 00:00:00.000000001', 4),
        ('1970-01-01 00:00:00.123456789', 5),
        ('2262-04-11 23:47:16.854775807', 6),
        (null, 7)
    """
    explain {
        sql """
            select *
            from test_timestamp_ns_partition_bucket
            where dt = cast('1970-01-01 00:00:00.000000000' as timestamp_ns)
        """
        contains "partitions=1/3 (p_epoch)"
        contains "tablets=1/4"
    }
    explain {
        sql """
            select *
            from test_timestamp_ns_partition_bucket
            where dt >= cast('2262-04-11 23:47:16.854775807' as timestamp_ns)
        """
        contains "partitions=1/3 (p_after_epoch)"
    }
    explain {
        sql """
            select *
            from test_timestamp_ns_partition_bucket
            where dt is null
        """
        contains "partitions=1/3 (p_before_epoch)"
    }

    sql "drop table if exists test_timestamp_ns_list_partition_rounding"
    sql """
        create table test_timestamp_ns_list_partition_rounding (
            dt7 timestamp_ns,
            dt8 timestamp_ns,
            dt9 timestamp_ns,
            id int
        )
        duplicate key(dt7, dt8, dt9)
        partition by list(dt7, dt8, dt9) (
            partition p_round values in
                (('1970-01-01 00:00:00.12345675',
                  '1970-01-01 00:00:00.123456785',
                  '1970-01-01 00:00:00.1234567895')),
            partition p_carry values in
                (('1970-01-01 00:00:00.99999995',
                  '1970-01-01 00:00:00.999999995',
                  '1970-01-01 00:00:00.9999999995'))
        )
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into test_timestamp_ns_list_partition_rounding values
        ('1970-01-01 00:00:00.12345675',
         '1970-01-01 00:00:00.123456785',
         '1970-01-01 00:00:00.1234567895', 1),
        ('1970-01-01 00:00:00.99999995',
         '1970-01-01 00:00:00.999999995',
         '1970-01-01 00:00:00.9999999995', 2)
    """
    order_qt_list_partition_scale_rounding """
        select dt7, dt8, dt9, id
        from test_timestamp_ns_list_partition_rounding
        order by id
    """

    order_qt_nullable_first_range """
        select id, dt
        from test_timestamp_ns_partition_bucket
        where dt is null
        order by id
    """

    order_qt_all_storage_predicates """
        select id,
               dt = cast('1970-01-01 00:00:00.000000000' as timestamp_ns),
               dt != cast('1970-01-01 00:00:00.000000000' as timestamp_ns),
               dt > cast('1970-01-01 00:00:00.000000000' as timestamp_ns),
               dt >= cast('1970-01-01 00:00:00.000000000' as timestamp_ns),
               dt < cast('1970-01-01 00:00:00.000000000' as timestamp_ns),
               dt <= cast('1970-01-01 00:00:00.000000000' as timestamp_ns),
               dt is null,
               dt is not null,
               dt in (
                   cast('1677-09-21 00:12:43.145224192' as timestamp_ns),
                   cast('1970-01-01 00:00:00.000000000' as timestamp_ns),
                   cast('2262-04-11 23:47:16.854775807' as timestamp_ns)),
               dt not in (
                   cast('1677-09-21 00:12:43.145224192' as timestamp_ns),
                   cast('1970-01-01 00:00:00.000000000' as timestamp_ns),
                   cast('2262-04-11 23:47:16.854775807' as timestamp_ns))
        from test_timestamp_ns_partition_bucket
        order by id
    """

    order_qt_topn """
        select id, dt
        from test_timestamp_ns_partition_bucket
        order by dt desc nulls last
        limit 3
    """

    order_qt_json_round_trip """
        select
            cast(
                json_extract_string(
                    json_object(
                        'dt',
                        cast(
                            cast('1677-09-21 00:12:43.145224192' as timestamp_ns)
                            as string)),
                    '\$.dt')
                as timestamp_ns),
            cast(
                json_extract_string(
                    json_object(
                        'dt',
                        cast(
                            cast('1970-01-01 00:00:00.000000000' as timestamp_ns)
                            as string)),
                    '\$.dt')
                as timestamp_ns),
            cast(
                json_extract_string(
                    json_object(
                        'dt',
                        cast(
                            cast('2262-04-11 23:47:16.854775807' as timestamp_ns)
                            as string)),
                    '\$.dt')
                as timestamp_ns)
    """

    sql "set enable_agg_state = true"
    sql "drop table if exists test_timestamp_ns_agg_state"
    sql """
        create table test_timestamp_ns_agg_state (
            id int,
            dt_state agg_state<max(timestamp_ns not null)> generic
        )
        aggregate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into test_timestamp_ns_agg_state values
        (1, max_state(cast('1677-09-21 00:12:43.145224192' as timestamp_ns))),
        (1, max_state(cast('1970-01-01 00:00:00.000000000' as timestamp_ns))),
        (1, max_state(cast('2262-04-11 23:47:16.854775807' as timestamp_ns)))
    """
    order_qt_aggregate_state """
        select id, max_merge(dt_state)
        from test_timestamp_ns_agg_state
        group by id
        order by id
    """
}
