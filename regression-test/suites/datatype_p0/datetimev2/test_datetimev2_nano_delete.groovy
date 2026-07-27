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

suite("test_datetimev2_nano_delete") {
    sql "drop table if exists test_datetimev2_nano_delete_dup"
    sql """
        create table test_datetimev2_nano_delete_dup (
            dt datetimev2(9),
            value int
        )
        duplicate key(dt)
        distributed by hash(dt) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into test_datetimev2_nano_delete_dup values
        ('1677-09-21 00:12:43.145224192', 1),
        ('1970-01-01 00:00:00.000000000', 2),
        ('1970-01-01 00:00:00.000000001', 3),
        ('2262-04-11 23:47:16.854775807', 4)
    """
    order_qt_dup_before_delete """
        select dt, value from test_datetimev2_nano_delete_dup order by dt, value
    """
    sql """
        delete from test_datetimev2_nano_delete_dup
        where dt = '1970-01-01 00:00:00.000000000'
    """
    order_qt_dup_delete_epoch """
        select dt, value from test_datetimev2_nano_delete_dup order by dt, value
    """
    sql """
        delete from test_datetimev2_nano_delete_dup
        where dt in (
            '1677-09-21 00:12:43.145224192',
            '1970-01-01 00:00:00.000000001',
            '2262-04-11 23:47:16.854775807')
    """
    order_qt_dup_delete_boundaries """
        select count(*) from test_datetimev2_nano_delete_dup
    """

    sql "drop table if exists test_datetimev2_nano_delete_unique"
    sql """
        create table test_datetimev2_nano_delete_unique (
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
        insert into test_datetimev2_nano_delete_unique values
        ('1677-09-21 00:12:43.145224192', 1),
        ('1970-01-01 00:00:00.000000000', 2),
        ('1970-01-01 00:00:00.000000001', 3),
        ('2262-04-11 23:47:16.854775807', 4)
    """
    sql """
        delete from test_datetimev2_nano_delete_unique
        where dt >= '1970-01-01 00:00:00.000000000'
          and dt < '2262-04-11 23:47:16.854775807'
    """
    order_qt_unique_delete_range """
        select dt, value from test_datetimev2_nano_delete_unique order by dt
    """

    sql "drop table if exists test_datetimev2_nano_delete_aggregate"
    sql """
        create table test_datetimev2_nano_delete_aggregate (
            dt datetimev2(9),
            dt_replace datetimev2(9) replace,
            dt_replace_if_not_null datetimev2(9) replace_if_not_null,
            dt_min datetimev2(9) min,
            dt_max datetimev2(9) max,
            amount bigint sum
        )
        aggregate key(dt)
        distributed by hash(dt) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into test_datetimev2_nano_delete_aggregate values
        ('1677-09-21 00:12:43.145224192',
         '1677-09-21 00:12:43.145224192',
         '1677-09-21 00:12:43.145224192',
         '1677-09-21 00:12:43.145224192',
         '1677-09-21 00:12:43.145224192', 1),
        ('1970-01-01 00:00:00.000000000',
         '1970-01-01 00:00:00.000000001',
         '1970-01-01 00:00:00.000000001',
         '1677-09-21 00:12:43.145224192',
         '2262-04-11 23:47:16.854775807', 2),
        ('1970-01-01 00:00:00.000000000',
         '1970-01-01 00:00:00.000000002',
         null,
         '1970-01-01 00:00:00.000000000',
         '1970-01-01 00:00:00.000000000', 3),
        ('2262-04-11 23:47:16.854775807',
         '2262-04-11 23:47:16.854775807',
         '2262-04-11 23:47:16.854775807',
         '2262-04-11 23:47:16.854775807',
         '2262-04-11 23:47:16.854775807', 4)
    """
    order_qt_aggregate_before_delete """
        select * from test_datetimev2_nano_delete_aggregate order by dt
    """
    sql """
        delete from test_datetimev2_nano_delete_aggregate
        where dt = '1970-01-01 00:00:00.000000000'
    """
    order_qt_aggregate_delete_epoch """
        select * from test_datetimev2_nano_delete_aggregate order by dt
    """

    sql "drop table if exists test_datetimev2_nano_invalid_aggregate"
    test {
        sql """
            create table test_datetimev2_nano_invalid_aggregate (
                dt datetimev2(9),
                value datetimev2(9) sum
            )
            aggregate key(dt)
            distributed by hash(dt) buckets 1
            properties("replication_num" = "1")
        """
        exception "Aggregate type SUM"
    }
}
