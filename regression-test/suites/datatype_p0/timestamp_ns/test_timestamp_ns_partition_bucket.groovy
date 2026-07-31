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

    order_qt_partition_rows "select id, dt from timestamp_ns_partition_bucket order by id"
    order_qt_nullable_first_range """
        select id, dt from timestamp_ns_partition_bucket where dt is null order by id
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
}
