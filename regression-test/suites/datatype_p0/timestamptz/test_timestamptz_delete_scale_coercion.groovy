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

suite("test_timestamptz_delete_scale_coercion", "nonConcurrent") {
    sql "set time_zone = '+00:00'"
    sql "drop table if exists timestamptz_delete_scale_coercion"
    sql """
        create table timestamptz_delete_scale_coercion (
            ts_tz timestamptz(6),
            value int
        ) unique key(ts_tz)
        distributed by hash(ts_tz) buckets 16
        properties("replication_num" = "1")
    """

    for (boolean mode : [true, false]) {
        setGlobalVarTemporary([enable_new_type_coercion_behavior: mode], {
            sql "truncate table timestamptz_delete_scale_coercion"
            sql """
                insert into timestamptz_delete_scale_coercion values
                    (null, -1),
                    ('0000-01-01 00:00:00 +00:00', 0),
                    ('0000-01-01 00:00:00.000000 +00:00', 0),
                    ('0000-01-01 00:00:00.000001 +00:00', 0),
                    ('0000-01-01 00:00:00.123456 +00:00', 0),
                    ('0000-01-01 00:00:00.999999 +00:00', 10),
                    ('2023-08-08 20:20:20 +00:00', 8),
                    ('2023-08-08 20:20:20.000000 +00:00', 8),
                    ('2023-08-08 20:20:20.000001 +00:00', 8),
                    ('2023-08-08 20:20:20.123456 +00:00', 8),
                    ('2023-08-08 20:20:20.999999 +00:00', 8),
                    ('9999-12-31 23:59:59 +00:00', 9998),
                    ('9999-12-31 23:59:59.000000 +00:00', 9998),
                    ('9999-12-31 23:59:59.000001 +00:00', 9998),
                    ('9999-12-31 23:59:59.123456 +00:00', 9998),
                    ('9999-12-31 23:59:59.999999 +00:00', 9999)
            """

            // A scale-0 literal must not demote the scale-6 key to DATETIME.
            sql """
                delete from timestamptz_delete_scale_coercion
                where ts_tz = '0000-01-01 00:00:00 +00:00'
            """
            // Repeating the DELETE must also work when there is no matching row.
            sql """
                delete from timestamptz_delete_scale_coercion
                where ts_tz = '0000-01-01 00:00:00 +00:00'
            """
            "qt_delete_min_${mode}" """
                select * from timestamptz_delete_scale_coercion order by 1, 2
            """

            sql """
                delete from timestamptz_delete_scale_coercion
                where ts_tz = '2023-08-09 04:20:20 +08:00'
            """
            "qt_delete_offset_${mode}" """
                select * from timestamptz_delete_scale_coercion order by 1, 2
            """

            sql """
                delete from timestamptz_delete_scale_coercion
                where ts_tz = '9999-12-31 23:59:59 +00:00'
            """
            "qt_delete_max_${mode}" """
                select * from timestamptz_delete_scale_coercion order by 1, 2
            """

            sql """
                delete from timestamptz_delete_scale_coercion
                where ts_tz in ('0000-01-01 00:00:00 +00:00',
                               '0000-01-01 00:00:00.000001 +00:00',
                               '2023-08-08 20:20:20.123456 +00:00',
                               '9999-12-31 23:59:59.999999 +00:00')
            """
            "qt_delete_in_${mode}" """
                select * from timestamptz_delete_scale_coercion order by 1, 2
            """

            sql """
                delete from timestamptz_delete_scale_coercion
                where ts_tz < '0000-01-01 00:00:00.999 +00:00'
            """
            "qt_delete_less_${mode}" """
                select * from timestamptz_delete_scale_coercion order by 1, 2
            """

            sql """
                delete from timestamptz_delete_scale_coercion
                where ts_tz >= '9999-12-31 23:59:59.123 +00:00'
            """
            "qt_delete_greater_equal_${mode}" """
                select * from timestamptz_delete_scale_coercion order by 1, 2
            """
        })
    }
}
