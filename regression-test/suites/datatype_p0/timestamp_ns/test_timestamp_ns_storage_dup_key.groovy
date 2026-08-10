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

suite("test_timestamp_ns_storage_dup_key") {
    sql "drop table if exists timestamp_ns_storage_dup_key"
    sql """
        create table timestamp_ns_storage_dup_key (
            dt timestamp_ns,
            value_dt timestamp_ns,
            id int
        )
        duplicate key(dt)
        distributed by hash(dt) buckets 4
        properties("replication_num" = "1")
    """
    sql """
        insert into timestamp_ns_storage_dup_key values
        ('1677-09-21 00:12:43.145224192', '1677-09-21 00:12:43.145224200', 1),
        ('1969-12-31 23:59:59.999999999', '1969-12-31 23:59:59.999999990', 2),
        ('1970-01-01 00:00:00.000000000', '1970-01-01 00:00:00.000000000', 3),
        ('1970-01-01 00:00:00.000000001', '1970-01-01 00:00:00.123456789', 4),
        ('2262-04-11 23:47:16.854775807', '2262-04-11 23:47:16.854775800', 5),
        (null, null, 6)
    """

    order_qt_all "select dt, value_dt, id from timestamp_ns_storage_dup_key order by id"
    order_qt_eq "select id from timestamp_ns_storage_dup_key where dt = '1970-01-01 00:00:00.000000000' order by id"
    order_qt_ne "select id from timestamp_ns_storage_dup_key where dt != '1970-01-01 00:00:00.000000000' order by id"
    order_qt_gt "select id from timestamp_ns_storage_dup_key where dt > '1970-01-01 00:00:00.000000000' order by id"
    order_qt_ge "select id from timestamp_ns_storage_dup_key where dt >= '1970-01-01 00:00:00.000000000' order by id"
    order_qt_lt "select id from timestamp_ns_storage_dup_key where dt < '1970-01-01 00:00:00.000000000' order by id"
    order_qt_le "select id from timestamp_ns_storage_dup_key where dt <= '1970-01-01 00:00:00.000000000' order by id"
    order_qt_in """
        select id from timestamp_ns_storage_dup_key
        where dt in ('1677-09-21 00:12:43.145224192', '2262-04-11 23:47:16.854775807')
        order by id
    """
    order_qt_not_in """
        select id from timestamp_ns_storage_dup_key
        where dt not in ('1677-09-21 00:12:43.145224192', '2262-04-11 23:47:16.854775807')
        order by id
    """
    order_qt_is_null "select id from timestamp_ns_storage_dup_key where dt is null order by id"
    order_qt_is_not_null "select id from timestamp_ns_storage_dup_key where dt is not null order by id"
    qt_min_max_count "select min(dt), max(dt), count(dt) from timestamp_ns_storage_dup_key"
    order_qt_cast_datetimev2_6 """
        select id, cast(dt as datetimev2(6))
        from timestamp_ns_storage_dup_key
        order by id
    """

    sql "drop table if exists timestamp_ns_storage_dup_key_row_store"
    sql """
        create table timestamp_ns_storage_dup_key_row_store (
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
        insert into timestamp_ns_storage_dup_key_row_store values
        (1, '1677-09-21 00:12:43.145224192'),
        (2, '1970-01-01 00:00:00.123456789'),
        (3, '2262-04-11 23:47:16.854775807')
    """
    order_qt_row_store "select id, dt from timestamp_ns_storage_dup_key_row_store order by id"

    sql """
        insert into timestamp_ns_storage_dup_key values
        ('1970-02-30 00:00:00.000000000', '1970-02-30 00:00:00.000000000', 100),
        ('1677-09-21 00:12:43.145224191', '1677-09-21 00:12:43.145224191', 101),
        ('2262-04-11 23:47:16.854775808', '2262-04-11 23:47:16.854775808', 102)
    """
    order_qt_invalid_values """
        select id, dt, value_dt
        from timestamp_ns_storage_dup_key
        where id between 100 and 102
        order by id
    """
}
