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

suite("test_timestamp_ns_primary_key") {
    sql "drop table if exists timestamp_ns_primary_key"
    sql """
        create table timestamp_ns_primary_key (
            dt timestamp_ns not null,
            id int not null,
            value varchar(16)
        )
        unique key(dt, id)
        distributed by hash(dt) buckets 2
        properties(
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true"
        )
    """
    sql """
        alter table timestamp_ns_primary_key
        add constraint timestamp_ns_primary_key_constraint primary key(dt, id)
    """
    sql """
        insert into timestamp_ns_primary_key values
        ('1677-09-21 00:12:43.145224192', 1, 'minimum'),
        ('1969-12-31 23:59:59.999999999', 2, 'before'),
        ('1970-01-01 00:00:00.000000000', 3, 'epoch'),
        ('1970-01-01 00:00:00.000000001', 4, 'after'),
        ('2262-04-11 23:47:16.854775807', 5, 'maximum')
    """

    order_qt_key_range_scan """
        select dt, id, value
        from timestamp_ns_primary_key
        where dt >= '1969-12-31 23:59:59.999999999'
          and dt < '1970-01-01 00:00:00.000000001'
        order by dt, id
    """
    order_qt_full_key_lookup """
        select dt, id, value
        from timestamp_ns_primary_key
        where dt = '1970-01-01 00:00:00.000000001' and id = 4
        order by dt, id
    """
}
