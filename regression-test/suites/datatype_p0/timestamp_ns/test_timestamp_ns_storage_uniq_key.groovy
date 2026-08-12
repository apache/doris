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

suite("test_timestamp_ns_storage_uniq_key") {
    sql "drop table if exists timestamp_ns_storage_uniq_key"
    sql """
        create table timestamp_ns_storage_uniq_key (
            dt timestamp_ns,
            value_dt timestamp_ns,
            value int
        )
        unique key(dt)
        distributed by hash(dt) buckets 2
        properties(
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true"
        )
    """
    sql """
        insert into timestamp_ns_storage_uniq_key values
        ('1677-09-21 00:12:43.145224192', '1677-09-21 00:12:43.145224200', 1),
        ('1969-12-31 23:59:59.999999999', '1969-12-31 23:59:59.999999990', 2),
        ('1970-01-01 00:00:00.000000000', '1970-01-01 00:00:00.000000000', 3),
        ('1970-01-01 00:00:00.000000001', '1970-01-01 00:00:00.123456789', 4),
        ('2262-04-11 23:47:16.854775807', '2262-04-11 23:47:16.854775800', 5)
    """
    sql """
        insert into timestamp_ns_storage_uniq_key values
        ('1970-01-01 00:00:00.000000000', '1970-01-01 00:00:00.000000001', 30)
    """

    order_qt_all "select dt, value_dt, value from timestamp_ns_storage_uniq_key order by dt"
    order_qt_eq "select value from timestamp_ns_storage_uniq_key where dt = '1970-01-01 00:00:00.000000000' order by value"
    order_qt_ne "select value from timestamp_ns_storage_uniq_key where dt != '1970-01-01 00:00:00.000000000' order by value"
    order_qt_gt "select value from timestamp_ns_storage_uniq_key where dt > '1970-01-01 00:00:00.000000000' order by value"
    order_qt_ge "select value from timestamp_ns_storage_uniq_key where dt >= '1970-01-01 00:00:00.000000000' order by value"
    order_qt_lt "select value from timestamp_ns_storage_uniq_key where dt < '1970-01-01 00:00:00.000000000' order by value"
    order_qt_le "select value from timestamp_ns_storage_uniq_key where dt <= '1970-01-01 00:00:00.000000000' order by value"
    order_qt_in """
        select value from timestamp_ns_storage_uniq_key
        where dt in ('1677-09-21 00:12:43.145224192', '2262-04-11 23:47:16.854775807')
        order by value
    """
    order_qt_not_in """
        select value from timestamp_ns_storage_uniq_key
        where dt not in ('1677-09-21 00:12:43.145224192', '2262-04-11 23:47:16.854775807')
        order by value
    """
}
