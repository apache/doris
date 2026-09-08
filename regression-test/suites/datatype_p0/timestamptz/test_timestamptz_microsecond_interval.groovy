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

    def microsecondExpressions = { String column ->
        [
            day_add: "date_add(${column}, interval '1 02:03:04.123456' day_microsecond)",
            day_sub: "date_sub(${column}, interval '1 02:03:04.123456' day_microsecond)",
            hour_add: "date_add(${column}, interval '02:03:04.123456' hour_microsecond)",
            hour_sub: "date_sub(${column}, interval '02:03:04.123456' hour_microsecond)",
            minute_add: "date_add(${column}, interval '03:04.123456' minute_microsecond)",
            minute_sub: "date_sub(${column}, interval '03:04.123456' minute_microsecond)",
            second_add: "date_add(${column}, interval '04.123456' second_microsecond)",
            second_sub: "date_sub(${column}, interval '04.123456' second_microsecond)",
            microseconds_add: "microseconds_add(${column}, 123456)",
            microseconds_sub: "microseconds_sub(${column}, 123456)",
            milliseconds_add: "milliseconds_add(${column}, 123)",
            milliseconds_sub: "milliseconds_sub(${column}, 123)"
        ]
    }

    def runtimeQuery = { String column ->
        def expressions = microsecondExpressions(column)
        """
            select id,
                ${expressions.values().join(",\n                ")}
            from test_timestamptz_microsecond_interval
            order by id
        """
    }

    ["ts0", "ts3", "ts6"].each { column ->
        def expressions = microsecondExpressions(column)
        explain {
            sql """
                select
                    ${expressions.collect { alias, expression ->
                        "${expression} as ${alias}"
                    }.join(",\n                    ")}
                from test_timestamptz_microsecond_interval
            """
            expressions.keySet().each { alias ->
                checkSlotTypeOf(alias, "timestamptz(6)")
            }
        }
    }

    order_qt_timestamptz_microsecond_interval_scale_0 runtimeQuery("ts0")
    order_qt_timestamptz_microsecond_interval_scale_3 runtimeQuery("ts3")
    order_qt_timestamptz_microsecond_interval_scale_6 runtimeQuery("ts6")
}
