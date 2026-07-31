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

suite("test_timestamp_ns") {
    sql "drop table if exists test_timestamp_ns"
    // dt7/dt8/dt9 describe the inserted fractional widths; every column is TIMESTAMP_NS.
    sql """
        create table test_timestamp_ns (
            id int,
            dt7 timestamp_ns,
            dt8 timestamp_ns,
            dt9 timestamp_ns
        )
        duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """

    sql """
        insert into test_timestamp_ns values
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
        from test_timestamp_ns
        order by dt9
    """

    sql "drop table if exists test_timestamp_ns_row_store"
    sql """
        create table test_timestamp_ns_row_store (
            id int,
            dt timestamp_ns
        )
        duplicate key(id)
        distributed by hash(id) buckets 1
        properties(
            "replication_num" = "1",
            "store_row_column" = "true"
        )
    """
    sql """
        insert into test_timestamp_ns_row_store values
        (1, '1677-09-21 00:12:43.145224192'),
        (2, '1970-01-01 00:00:00.123456789'),
        (3, '2262-04-11 23:47:16.854775807')
    """
    order_qt_row_store """
        select id, dt
        from test_timestamp_ns_row_store
        order by id
    """

    qt_rounding """
        select
            cast('1677-09-21 00:12:43.145224192' as timestamp_ns),
            cast('1970-01-01 00:00:00.12345675' as timestamp_ns),
            cast('1970-01-01 00:00:00.999999995' as timestamp_ns),
            cast('1970-01-01 00:00:00.123456789' as timestamp_ns),
            cast('2262-04-11 23:47:16.854775807' as timestamp_ns)
    """

    qt_cast_and_aggregate """
        select
            cast(cast('1677-09-21 00:12:43.145224192' as timestamp_ns) as string),
            cast(cast('2262-04-11 23:47:16.854775807' as timestamp_ns) as string),
            min(dt9), max(dt9), count(distinct dt9)
        from test_timestamp_ns
    """

    // Cross-family casts and datetime scalar functions are implemented in follow-up changes.
    test {
        sql "select cast(dt9 as datetimev2(6)) from test_timestamp_ns"
        exception "cannot cast"
    }
    test {
        sql "select seconds_add(dt9, 1) from test_timestamp_ns"
        exception "Can not find the compatibility function signature: seconds_add"
    }

    order_qt_filter """
        select id, dt9
        from test_timestamp_ns
        where dt9 in (
            cast('1677-09-21 00:12:43.145224192' as timestamp_ns),
            cast('1970-01-01 00:00:00.000000000' as timestamp_ns),
            cast('2262-04-11 23:47:16.854775807' as timestamp_ns))
        order by id
    """

    qt_out_of_range """
        select
            cast('1677-09-21 00:12:43.145224192' as timestamp_ns),
            cast('1677-09-21 00:12:43.145224191' as timestamp_ns),
            cast('1970-01-01 00:00:00.000000000' as timestamp_ns),
            cast('2262-04-11 23:47:16.854775808' as timestamp_ns),
            cast('2262-04-11 23:47:16.854775807' as timestamp_ns)
    """

    qt_all_comparisons """
        select
            cast('1677-09-21 00:12:43.145224192' as timestamp_ns)
                = cast('1677-09-21 00:12:43.145224192' as timestamp_ns),
            cast('1677-09-21 00:12:43.145224192' as timestamp_ns)
                < cast('1970-01-01 00:00:00.000000000' as timestamp_ns),
            cast('1969-12-31 23:59:59.999999999' as timestamp_ns)
                = cast('1969-12-31 23:59:59.999999999' as timestamp_ns),
            cast('1969-12-31 23:59:59.999999999' as timestamp_ns)
                <> cast('1970-01-01 00:00:00.000000000' as timestamp_ns),
            cast('1969-12-31 23:59:59.999999999' as timestamp_ns)
                < cast('1970-01-01 00:00:00.000000000' as timestamp_ns),
            cast('1970-01-01 00:00:00.000000001' as timestamp_ns)
                > cast('1970-01-01 00:00:00.000000000' as timestamp_ns),
            cast('1970-01-01 00:00:00.000000001' as timestamp_ns)
                >= cast('1970-01-01 00:00:00.000000001' as timestamp_ns),
            cast('1970-01-01 00:00:00.000000001' as timestamp_ns)
                <= cast('1970-01-01 00:00:00.000000001' as timestamp_ns),
            cast('1970-01-01 00:00:00.000000000' as timestamp_ns)
                < cast('2262-04-11 23:47:16.854775807' as timestamp_ns),
            cast('2262-04-11 23:47:16.854775807' as timestamp_ns)
                = cast('2262-04-11 23:47:16.854775807' as timestamp_ns)
    """

    sql "drop table if exists test_timestamp_ns_relational"
    sql """
        create table test_timestamp_ns_relational (
            id int,
            dt timestamp_ns,
            payload varchar(16)
        )
        duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into test_timestamp_ns_relational values
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
        from test_timestamp_ns_relational
        order by dt asc nulls last, id
        limit 7
    """

    order_qt_group_by """
        select dt, count(*), min(id), max(id)
        from test_timestamp_ns_relational
        group by dt
        order by dt nulls first
    """

    order_qt_hash_join """
        select l.id, r.id, l.dt
        from test_timestamp_ns_relational l
        join test_timestamp_ns_relational r on l.dt = r.dt
        order by l.id, r.id
    """

    qt_relational_aggregates """
        select
            count(dt),
            count(distinct dt),
            min(dt),
            max(dt),
            approx_count_distinct(dt)
        from test_timestamp_ns_relational
    """

    sql "drop table if exists test_timestamp_ns_unique"
    sql """
        create table test_timestamp_ns_unique (
            dt timestamp_ns,
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
        insert into test_timestamp_ns_unique values
        ('1677-09-21 00:12:43.145224192', 1),
        ('1969-12-31 23:59:59.999999999', 2),
        ('1970-01-01 00:00:00.000000000', 3),
        ('1970-01-01 00:00:00.000000001', 4),
        ('2262-04-11 23:47:16.854775807', 5)
    """
    sql """
        insert into test_timestamp_ns_unique values
        ('1970-01-01 00:00:00.000000000', 20)
    """
    order_qt_unique_key """
        select dt, value
        from test_timestamp_ns_unique
        order by dt
    """

    sql "drop table if exists test_timestamp_ns_aggregate"
    sql """
        create table test_timestamp_ns_aggregate (
            dt timestamp_ns,
            amount bigint sum
        )
        aggregate key(dt)
        distributed by hash(dt) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into test_timestamp_ns_aggregate values
        ('1677-09-21 00:12:43.145224192', 1),
        ('1969-12-31 23:59:59.999999999', 2),
        ('1970-01-01 00:00:00.000000000', 3),
        ('1970-01-01 00:00:00.000000000', 4),
        ('1970-01-01 00:00:00.000000001', 5),
        ('2262-04-11 23:47:16.854775807', 6)
    """
    order_qt_aggregate_key """
        select dt, amount
        from test_timestamp_ns_aggregate
        order by dt
    """

    sql "drop table if exists test_timestamp_ns_partition"
    sql """
        create table test_timestamp_ns_partition (
            dt timestamp_ns,
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
          and table_name = 'test_timestamp_ns_partition'
        order by partition_name
    """
    sql "drop table if exists test_timestamp_ns_complex"
    sql """
        create table test_timestamp_ns_complex (
            id int,
            values_array array<timestamp_ns>
        )
        duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into test_timestamp_ns_complex values
        (1, array(
            cast('2262-04-11 23:47:16.854775807' as timestamp_ns),
            cast('1970-01-01 00:00:00.000000000' as timestamp_ns),
            cast('1677-09-21 00:12:43.145224192' as timestamp_ns)
        )),
        (2, null)
    """
    order_qt_array_functions """
        select id,
               array_sort(values_array)
        from test_timestamp_ns_complex
        order by id
    """

    sql "drop table if exists test_timestamp_ns_index"
    sql """
        create table test_timestamp_ns_index (
            id int,
            dt timestamp_ns,
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
        insert into test_timestamp_ns_index values
        (1, '1677-09-21 00:12:43.145224192'),
        (2, '1970-01-01 00:00:00.000000000'),
        (3, '2262-04-11 23:47:16.854775807')
    """
    order_qt_index_predicates """
        select id, dt
        from test_timestamp_ns_index
        where dt in (
            cast('1677-09-21 00:12:43.145224192' as timestamp_ns),
            cast('1970-01-01 00:00:00.000000000' as timestamp_ns),
            cast('2262-04-11 23:47:16.854775807' as timestamp_ns))
        order by id
    """

    sql "set debug_skip_fold_constant = false"
    qt_cast_with_constant_folding """
        select
            cast('1677-09-21 00:12:43.145224192' as timestamp_ns),
            cast('1970-01-01 00:00:00.123456789' as timestamp_ns),
            cast('1970-01-01 00:00:00.123456789' as timestamp_ns),
            cast('1970-01-01 00:00:00.123456789' as timestamp_ns),
            cast('1970-01-01 00:00:00.1234567894' as timestamp_ns),
            cast('1970-01-01 00:00:00.1234567895' as timestamp_ns),
            cast('1970-01-01 00:00:00.9999999995' as timestamp_ns),
            cast('2262-04-11 23:47:16.854775807' as timestamp_ns)
    """
    sql "set debug_skip_fold_constant = true"
    qt_cast_without_constant_folding """
        select
            cast('1677-09-21 00:12:43.145224192' as timestamp_ns),
            cast('1970-01-01 00:00:00.123456789' as timestamp_ns),
            cast('1970-01-01 00:00:00.123456789' as timestamp_ns),
            cast('1970-01-01 00:00:00.123456789' as timestamp_ns),
            cast('1970-01-01 00:00:00.1234567894' as timestamp_ns),
            cast('1970-01-01 00:00:00.1234567895' as timestamp_ns),
            cast('1970-01-01 00:00:00.9999999995' as timestamp_ns),
            cast('2262-04-11 23:47:16.854775807' as timestamp_ns)
    """
    sql "set debug_skip_fold_constant = false"

    sql "drop table if exists test_timestamp_ns_cast_input"
    sql """
        create table test_timestamp_ns_cast_input (
            id int,
            value varchar(64)
        )
        duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into test_timestamp_ns_cast_input values
        (1, '2024-01-01 00:00:00.123456789'),
        (2, '2024-01-01 00:00:00.123.456')
    """
    def originalEnableStrictCast = sql("select @@enable_strict_cast")[0][0]
    try {
        sql "set enable_strict_cast = false"
        order_qt_nonconstant_permissive_cast """
            select id, cast(value as timestamp_ns)
            from test_timestamp_ns_cast_input
            order by id
        """

        sql "set enable_strict_cast = true"
        test {
            sql """
                select cast(value as timestamp_ns)
                from test_timestamp_ns_cast_input
                where id = 2
            """
            exception "2024-01-01 00:00:00.123.456"
        }
    } finally {
        sql "set enable_strict_cast = ${originalEnableStrictCast}"
    }

    test {
        sql """
            create table test_timestamp_ns_invalid_scale (
                id int,
                dt datetimev2(10)
            )
            distributed by hash(id) buckets 1
            properties("replication_num" = "1")
        """
        exception "between 0 and 6"
    }
}
