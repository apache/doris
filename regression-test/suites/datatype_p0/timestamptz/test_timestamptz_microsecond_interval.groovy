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

suite("test_timestamptz_microsecond_interval") {
    sql "set time_zone = '+08:00'"
    sql "drop table if exists test_timestamptz_microsecond_interval"
    sql """
        create table test_timestamptz_microsecond_interval (
            id int,
            ts timestamptz(3)
        )
        duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into test_timestamptz_microsecond_interval values
            (1, '2024-01-02 03:04:05.123 +08:00')
    """

    order_qt_timestamptz_microsecond_interval """
        select id,
            date_add(ts, interval '1 02:03:04.123456' day_microsecond),
            date_sub(ts, interval '1 02:03:04.123456' day_microsecond),
            date_add(ts, interval '02:03:04.123456' hour_microsecond),
            date_sub(ts, interval '02:03:04.123456' hour_microsecond),
            date_add(ts, interval '03:04.123456' minute_microsecond),
            date_sub(ts, interval '03:04.123456' minute_microsecond)
        from test_timestamptz_microsecond_interval
        order by id
    """
}
