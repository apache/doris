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
            ts0 timestamptz(0),
            ts3 timestamptz(3),
            ts6 timestamptz(6)
        )
        duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into test_timestamptz_microsecond_interval values
            (1,
                '2024-01-02 03:04:05 +08:00',
                '2024-01-02 03:04:05.123 +08:00',
                '2024-01-02 03:04:05.123456 +08:00')
    """

    ["ts0", "ts3", "ts6"].each { column ->
        explain {
            sql """
                select
                    date_add(${column}, interval '1 02:03:04.123456' day_microsecond) as day_add,
                    date_sub(${column}, interval '1 02:03:04.123456' day_microsecond) as day_sub,
                    date_add(${column}, interval '02:03:04.123456' hour_microsecond) as hour_add,
                    date_sub(${column}, interval '02:03:04.123456' hour_microsecond) as hour_sub,
                    date_add(${column}, interval '03:04.123456' minute_microsecond) as minute_add,
                    date_sub(${column}, interval '03:04.123456' minute_microsecond) as minute_sub
                from test_timestamptz_microsecond_interval
            """
            checkSlotTypeOf("day_add", "timestamptz(6)")
            checkSlotTypeOf("day_sub", "timestamptz(6)")
            checkSlotTypeOf("hour_add", "timestamptz(6)")
            checkSlotTypeOf("hour_sub", "timestamptz(6)")
            checkSlotTypeOf("minute_add", "timestamptz(6)")
            checkSlotTypeOf("minute_sub", "timestamptz(6)")
        }
    }

    order_qt_timestamptz_microsecond_interval_scale_0 """
        select id,
            date_add(ts0, interval '1 02:03:04.123456' day_microsecond),
            date_sub(ts0, interval '1 02:03:04.123456' day_microsecond),
            date_add(ts0, interval '02:03:04.123456' hour_microsecond),
            date_sub(ts0, interval '02:03:04.123456' hour_microsecond),
            date_add(ts0, interval '03:04.123456' minute_microsecond),
            date_sub(ts0, interval '03:04.123456' minute_microsecond)
        from test_timestamptz_microsecond_interval
        order by id
    """

    order_qt_timestamptz_microsecond_interval_scale_3 """
        select id,
            date_add(ts3, interval '1 02:03:04.123456' day_microsecond),
            date_sub(ts3, interval '1 02:03:04.123456' day_microsecond),
            date_add(ts3, interval '02:03:04.123456' hour_microsecond),
            date_sub(ts3, interval '02:03:04.123456' hour_microsecond),
            date_add(ts3, interval '03:04.123456' minute_microsecond),
            date_sub(ts3, interval '03:04.123456' minute_microsecond)
        from test_timestamptz_microsecond_interval
        order by id
    """

    order_qt_timestamptz_microsecond_interval_scale_6 """
        select id,
            date_add(ts6, interval '1 02:03:04.123456' day_microsecond),
            date_sub(ts6, interval '1 02:03:04.123456' day_microsecond),
            date_add(ts6, interval '02:03:04.123456' hour_microsecond),
            date_sub(ts6, interval '02:03:04.123456' hour_microsecond),
            date_add(ts6, interval '03:04.123456' minute_microsecond),
            date_sub(ts6, interval '03:04.123456' minute_microsecond)
        from test_timestamptz_microsecond_interval
        order by id
    """
}
