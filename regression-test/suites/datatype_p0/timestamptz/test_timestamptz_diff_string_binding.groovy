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

suite("test_timestamptz_diff_string_binding") {
    // Which overload a *_diff call binds on string input (apache/doris#66120). The session zones
    // below are chosen so that the instant (TIMESTAMPTZ) and civil (DATETIMEV2) differences
    // disagree, so each result pins the binding.
    def originalZone = sql("select @@time_zone")[0][0]
    try {
        sql "set time_zone = 'America/Los_Angeles'"

        // Literals with an explicit zone bind TIMESTAMPTZ whether the offset is written -0700 or
        // -07:00: 2021-03-15 07:00:00 UTC minus 2021-03-14 08:00:00 UTC is 23 elapsed hours, not
        // the 24 civil hours between the wall-clock values.
        assertEquals([[23L]], sql("select hours_diff('2021-03-15 00:00:00-0700', '2021-03-14 00:00:00-08:00')"))
        assertEquals([[-23L]], sql("select hours_diff('2021-03-14 00:00:00-08:00', '2021-03-15 00:00:00-0700')"))
        assertEquals([[23L]], sql("select hours_diff('2021-03-15 00:00:00-07:00', '2021-03-14 00:00:00-08:00')"))
        assertEquals([[23L]], sql("""
            select hours_diff(cast('2021-03-15 00:00:00-0700' as timestamptz(6)),
                              cast('2021-03-14 00:00:00-08:00' as timestamptz(6)))
        """))

        // A zoned literal beside a zone-less one keeps TIMESTAMPTZ: 2021-03-14 01:30:00 PST is
        // 09:30:00 UTC, one elapsed hour before 03:30:00-07:00, although the wall clock moved two
        // hours across the spring-forward gap.
        assertEquals([[1L]], sql("select hours_diff('2021-03-14 03:30:00-07:00', '2021-03-14 01:30:00')"))
        assertEquals([[-1L]], sql("select hours_diff('2021-03-14 01:30:00', '2021-03-14 03:30:00-07:00')"))

        // Zone-less literals bind DATETIMEV2: the two civil hours.
        assertEquals([[2L]], sql("select hours_diff('2021-03-14 03:30:00', '2021-03-14 01:30:00')"))

        // String-typed slots carry no zone to inspect and bind DATETIMEV2 as well, so the same
        // values give the same civil result from a VARCHAR column ...
        sql "drop table if exists tz_diff_string_binding"
        sql """
            create table tz_diff_string_binding (id int, a varchar(32), b varchar(32))
            duplicate key(id) distributed by hash(id) buckets 1
            properties('replication_num' = '1')
        """
        sql """
            insert into tz_diff_string_binding values
            (1, '2021-03-14 03:30:00', '2021-03-14 01:30:00'),
            (2, '2025-08-01 00:00:00', '2025-05-30 11:40:09'),
            (3, '2025-08-02 00:00:00', '2024-09-30 19:40:02')
        """
        assertEquals([[2L]], sql("select hours_diff(a, b) from tz_diff_string_binding where id = 1"))

        // ... and the day counts of the issue's reproducer, which bound TIMESTAMPTZ before and
        // came out one day short under a positive offset.
        sql "set time_zone = '+08:00'"
        assertEquals([[63], [306]], sql("""
            select datediff(a, b) from tz_diff_string_binding where id in (2, 3) order by id
        """))
        assertEquals([[63]], sql("""
            select datediff(a, b)
            from (select '2025-08-01 00:00:00' as a, '2025-05-30 11:40:09' as b) t
        """))
        assertEquals([[63], [306]], sql("""
            select datediff(a, b)
            from (select '2025-08-01 00:00:00' as a, '2025-05-30 11:40:09' as b
                  union all select '2025-08-02 00:00:00', '2024-09-30 19:40:02') t
            order by 1
        """))
    } finally {
        sql "set time_zone = '${originalZone}'"
        sql "drop table if exists tz_diff_string_binding"
    }
}
