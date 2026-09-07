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

suite("test_timestamp_ns_partition_bucket") {
    sql "drop table if exists timestamp_ns_partition_bucket"
    sql """
        create table timestamp_ns_partition_bucket (
            dt timestamp_ns,
            id int
        )
        duplicate key(dt)
        partition by range(dt) (
            partition p_before_epoch values less than ('1970-01-01 00:00:00.000000000'),
            partition p_epoch values less than ('1970-01-01 00:00:00.000000002'),
            partition p_after_epoch values less than MAXVALUE
        )
        distributed by hash(dt) buckets 4
        properties("replication_num" = "1")
    """
    sql """
        insert into timestamp_ns_partition_bucket values
        ('1677-09-21 00:12:43.145224192', 1),
        ('1969-12-31 23:59:59.999999999', 2),
        ('1970-01-01 00:00:00.000000000', 3),
        ('1970-01-01 00:00:00.000000001', 4),
        ('1970-01-01 00:00:00.123456789', 5),
        ('2262-04-11 23:47:16.854775807', 6),
        (null, 7)
    """

    sql "set debug_skip_fold_constant = false"
    explain {
        sql """
            select * from timestamp_ns_partition_bucket
            where dt = cast('1970-01-01 00:00:00.000000000' as timestamp_ns)
        """
        contains "partitions=1/3 (p_epoch)"
        contains "tablets=1/4"
    }
    explain {
        sql """
            select * from timestamp_ns_partition_bucket
            where dt >= cast('2262-04-11 23:47:16.854775807' as timestamp_ns)
        """
        contains "partitions=1/3 (p_after_epoch)"
    }
    explain {
        sql "select * from timestamp_ns_partition_bucket where dt is null"
        contains "partitions=1/3 (p_before_epoch)"
    }
    explain {
        sql """
            select * from timestamp_ns_partition_bucket
            where cast(dt as datetimev2(6)) =
                  cast('1970-01-01 00:00:00.000000' as datetimev2(6))
        """
        contains "partitions=3/3"
    }
    order_qt_lossy_cast_does_not_prune_raw_timestamp_ns """
        select id, dt from timestamp_ns_partition_bucket
        where cast(dt as datetimev2(6)) =
              cast('1970-01-01 00:00:00.000000' as datetimev2(6))
        order by id
    """
    explain {
        sql """
            select * from timestamp_ns_partition_bucket
            where date(dt) in (date '1969-12-31')
        """
        contains "partitions=1/3 (p_before_epoch)"
    }

    sql "set debug_skip_fold_constant = true"
    explain {
        sql """
            select * from timestamp_ns_partition_bucket
            where dt = cast('1970-01-01 00:00:00.000000000' as timestamp_ns)
        """
        contains "partitions=1/3 (p_epoch)"
        contains "tablets=4/4"
    }
    sql "set debug_skip_fold_constant = false"

    sql "set experimental_enable_virtual_slot_for_cse = true"
    explain {
        sql """
            verbose select date_trunc(dt, 'second')
            from timestamp_ns_partition_bucket
            where date_trunc(dt, 'second') >= cast('1970-01-01 00:00:00' as timestamp_ns)
              and date_trunc(dt, 'second') < cast('1970-01-02 00:00:00' as timestamp_ns)
        """
        contains "__DORIS_VIRTUAL_COL__"
        contains "type=timestamp_ns"
        contains "date_trunc"
    }
    sql "set experimental_enable_virtual_slot_for_cse = false"

    order_qt_partition_rows "select id, dt from timestamp_ns_partition_bucket order by id"
    order_qt_nullable_first_range """
        select id, dt from timestamp_ns_partition_bucket where dt is null order by id
    """
    order_qt_information_schema_timestamp_ns_precision """
        select data_type, column_type, numeric_precision, numeric_scale,
               datetime_precision, decimal_digits, is_nullable
        from information_schema.columns
        where table_schema = '${context.dbName}'
          and table_name = 'timestamp_ns_partition_bucket'
          and column_name = 'dt'
    """

    sql "drop table if exists timestamp_ns_list_partition_rounding"
    sql """
        create table timestamp_ns_list_partition_rounding (
            dt timestamp_ns,
            id int
        )
        duplicate key(dt)
        partition by list(dt) (
            partition p_round values in ('1970-01-01 00:00:00.1234567895'),
            partition p_carry values in ('1970-01-01 00:00:00.9999999995')
        )
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into timestamp_ns_list_partition_rounding values
        ('1970-01-01 00:00:00.1234567895', 1),
        ('1970-01-01 00:00:00.9999999995', 2)
    """
    order_qt_list_partition_rounding """
        select dt, id from timestamp_ns_list_partition_rounding order by id
    """

    sql "drop table if exists timestamp_ns_auto_range_boundary"
    sql """
        create table timestamp_ns_auto_range_boundary (
            id int,
            dt timestamp_ns not null
        )
        duplicate key(id)
        auto partition by range (date_trunc(dt, 'day')) ()
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into timestamp_ns_auto_range_boundary values
        (1, '1677-09-21 00:12:43.145224192'),
        (2, '1677-09-21 00:12:43.145224193'),
        (3, '1677-09-21 23:59:59.999999999'),
        (4, '2262-04-11 23:47:16.854775807')
    """
    order_qt_auto_range_boundary_rows """
        select id, dt from timestamp_ns_auto_range_boundary order by id
    """
    order_qt_auto_range_boundary_partitions """
        select partition_name, partition_description
        from information_schema.partitions
        where table_schema = '${context.dbName}'
          and table_name = 'timestamp_ns_auto_range_boundary'
        order by partition_name
    """

    sql "drop table if exists datetimev2_cast_timestamp_ns_prune"
    sql """
        create table datetimev2_cast_timestamp_ns_prune (
            id int,
            dt datetimev2(6) not null
        )
        duplicate key(id)
        partition by range(dt) (
            partition p1 values less than ('2024-01-02 00:00:00.000000'),
            partition p2 values less than ('2024-01-03 00:00:00.000000'),
            partition p3 values less than MAXVALUE
        )
        distributed by hash(id) buckets 3
        properties("replication_num" = "1")
    """
    sql """
        insert into datetimev2_cast_timestamp_ns_prune values
        (1, '2024-01-01 00:00:00.000001'),
        (2, '2024-01-02 00:00:00.000002'),
        (3, '2024-01-03 00:00:00.000003'),
        (4, '9999-12-31 23:59:59.999999')
    """

    // Strict casts still fail when the out-of-range partition is scanned directly. Partition
    // pruning may skip that failure when a necessary DATETIMEV2 predicate excludes the partition.
    sql "set enable_strict_cast = true"
    test {
        sql """
            select id, cast(dt as timestamp_ns)
            from datetimev2_cast_timestamp_ns_prune partition(p3)
            where id = 4
            order by id
        """
        exception "TIMESTAMP_NS overflow"
    }
    explain {
        sql """
            select id from datetimev2_cast_timestamp_ns_prune
            where cast(dt as timestamp_ns) in (
                cast('2024-01-01 00:00:00.000001000' as timestamp_ns),
                cast('2024-01-02 00:00:00.000002000' as timestamp_ns))
        """
        contains "partitions=2/3 (p1,p2)"
    }
    // The query executes in strict mode without evaluating the overflow in p3.
    order_qt_datetimev2_cast_timestamp_ns_aligned """
        select id from datetimev2_cast_timestamp_ns_prune
        where cast(dt as timestamp_ns) in (
            cast('2024-01-01 00:00:00.000001000' as timestamp_ns),
            cast('2024-01-02 00:00:00.000002000' as timestamp_ns))
        order by id
    """
    explain {
        sql """
            select id from datetimev2_cast_timestamp_ns_prune
            where cast(dt as timestamp_ns) =
                  cast('2024-01-01 00:00:00.000001000' as timestamp_ns)
        """
        contains "partitions=1/3 (p1)"
    }
    order_qt_datetimev2_cast_timestamp_ns_eq """
        select id from datetimev2_cast_timestamp_ns_prune
        where cast(dt as timestamp_ns) =
              cast('2024-01-01 00:00:00.000001000' as timestamp_ns)
        order by id
    """
    explain {
        sql """
            select id from datetimev2_cast_timestamp_ns_prune
            where cast(dt as timestamp_ns) =
                  cast('2024-01-02 00:00:00.000002001' as timestamp_ns)
        """
        contains "VEMPTYSET"
    }
    qt_datetimev2_cast_timestamp_ns_unreachable """
        select id from datetimev2_cast_timestamp_ns_prune
        where cast(dt as timestamp_ns) =
              cast('2024-01-02 00:00:00.000002001' as timestamp_ns)
    """
    explain {
        sql """
            select id from datetimev2_cast_timestamp_ns_prune
            where cast(dt as timestamp_ns) in (
                cast('2024-01-01 00:00:00.000001001' as timestamp_ns),
                cast('2024-01-02 00:00:00.000002001' as timestamp_ns))
        """
        contains "VEMPTYSET"
    }
    qt_datetimev2_cast_timestamp_ns_unreachable_in """
        select id from datetimev2_cast_timestamp_ns_prune
        where cast(dt as timestamp_ns) in (
            cast('2024-01-01 00:00:00.000001001' as timestamp_ns),
            cast('2024-01-02 00:00:00.000002001' as timestamp_ns))
        order by id
    """

    sql "set enable_strict_cast = false"
    qt_datetimev2_cast_timestamp_ns_eq """
            select id, dt = cast('2024-01-01 00:00:00.000001000' as timestamp_ns)
            from datetimev2_cast_timestamp_ns_prune partition(p3)
            where id = 4
            order by id
        """
    explain {
        sql """
            select id from datetimev2_cast_timestamp_ns_prune
            where dt = cast('2024-01-01 00:00:00.000001000' as timestamp_ns)
        """
        contains "partitions=1/3 (p1)"
    }
    sql """
        select id from datetimev2_cast_timestamp_ns_prune
        where dt = cast('2024-01-01 00:00:00.000001000' as timestamp_ns)
        order by id
    """

    sql "drop table if exists datetimev2_cast_timestamp_ns_nullable_prune"
    sql """
        create table datetimev2_cast_timestamp_ns_nullable_prune (
            id int,
            dt datetimev2(6)
        )
        duplicate key(id)
        partition by list(dt) (
            partition p_null values in (NULL),
            partition p_value values in ('2024-01-01 00:00:00.000001')
        )
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into datetimev2_cast_timestamp_ns_nullable_prune values
        (1, NULL),
        (2, '2024-01-01 00:00:00.000001')
    """

    // Removing the cast turns NULL into FALSE for an unreachable nanosecond option. Such a
    // truth-preserving rewrite is valid for a positive filter, but not below three-valued parents.
    explain {
        sql """
            select id from datetimev2_cast_timestamp_ns_nullable_prune
            where (cast(dt as timestamp_ns) in (
                cast('2024-01-01 00:00:00.000001001' as timestamp_ns))) is null
        """
        contains "partitions=1/2 (p_null)"
        notContains "VEMPTYSET"
    }
    explain {
        sql """
            select id from datetimev2_cast_timestamp_ns_nullable_prune
            where (cast(dt as timestamp_ns) =
                cast('2024-01-01 00:00:00.000001001' as timestamp_ns)) is null
        """
        contains "partitions=1/2 (p_null)"
        notContains "VEMPTYSET"
    }
    explain {
        sql """
            select id from datetimev2_cast_timestamp_ns_nullable_prune
            where coalesce(cast(dt as timestamp_ns) in (
                cast('2024-01-01 00:00:00.000001001' as timestamp_ns)), true)
        """
        contains "partitions=1/2 (p_null)"
        notContains "VEMPTYSET"
    }
    explain {
        sql """
            select id from datetimev2_cast_timestamp_ns_nullable_prune
            where not (cast(dt as timestamp_ns) in (
                cast('2024-01-01 00:00:00.000001001' as timestamp_ns)))
        """
        contains "partitions=1/2 (p_value)"
    }
}
