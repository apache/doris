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

suite("test_datetimev2_nano_cast_compatibility") {
    sql "set enable_strict_cast = false"
    sql "set time_zone = '+08:00'"

    sql "drop table if exists test_datetimev2_nano_cast_string"
    sql """
        create table test_datetimev2_nano_cast_string (
            id int,
            value string
        )
        duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into test_datetimev2_nano_cast_string values
        (1, '1677-09-21 00:12:43.145224192'),
        (2, '1969-12-31 23:59:59.999999999'),
        (3, '1970-01-01 00:00:00.000000000'),
        (4, '2024-02-29 12:34:56.123456789'),
        (5, '2262-04-11 23:47:16.854775807'),
        (6, '0000-01-01 00:00:00.000000000'),
        (7, '9999-12-31 23:59:59.999999999'),
        (8, '2023-02-29 00:00:00.000000000'),
        (9, '1677-09-21 00:12:43.145224191'),
        (10, '2262-04-11 23:47:16.854775808'),
        (11, '2023-08-17T01:41:18.123456789Z'),
        (12, '2023-08-17T01:41:18.123456789America/Los_Angeles'),
        (13, '1677-09-21T00:12:43.145224192+14:00'),
        (14, '2262-04-11T23:47:16.854775807-01:00'),
        (15, null)
    """
    order_qt_string_to_nano_scales """
        select id,
               cast(value as datetime(7)),
               cast(value as datetime(8)),
               cast(value as datetime(9))
        from test_datetimev2_nano_cast_string
        order by id
    """

    sql "drop table if exists test_datetimev2_nano_cast_civil"
    sql """
        create table test_datetimev2_nano_cast_civil (
            id int,
            date_value date,
            datetime_value datetime(6),
            time_string string
        )
        duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into test_datetimev2_nano_cast_civil values
        (1, '1677-09-20', '1677-09-21 00:12:43.145224', '00:12:43.145224'),
        (2, '1677-09-21', '1677-09-21 00:12:43.145225', '23:59:59.999999'),
        (3, '1970-01-01', '1970-01-01 00:00:00.000000', '00:00:00.000000'),
        (4, '2024-02-29', '2024-02-29 12:34:56.123456', '12:34:56.123456'),
        (5, '2262-04-11', '2262-04-11 23:47:16.854775', '-128:00:00.000000'),
        (6, '2262-04-12', '2262-04-11 23:47:16.854776', '500:00:00.000000'),
        (7, '0000-01-01', '0000-01-01 00:00:00.000000', '01:02:03.000000'),
        (8, '9999-12-31', '9999-12-31 23:59:59.999999', null),
        (9, null, null, null)
    """
    order_qt_civil_to_nano """
        select id,
               cast(date_value as datetime(9)),
               cast(datetime_value as datetime(7)),
               cast(datetime_value as datetime(8)),
               cast(datetime_value as datetime(9)),
               cast(cast(cast(time_string as time(6)) as datetime(9)) as time(6))
        from test_datetimev2_nano_cast_civil
        order by id
    """

    sql "drop table if exists test_datetimev2_nano_cast_from_nano"
    sql """
        create table test_datetimev2_nano_cast_from_nano (
            id int,
            value datetime(9)
        )
        duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into test_datetimev2_nano_cast_from_nano values
        (1, '1677-09-21 00:12:43.145224192'),
        (2, '1969-12-31 23:59:59.999999999'),
        (3, '1970-01-01 00:00:00.000000000'),
        (4, '2024-02-29 12:34:56.123456789'),
        (5, '2262-04-11 23:47:16.854775807'),
        (6, null)
    """
    order_qt_nano_to_other_types """
        select id,
               cast(value as string),
               cast(value as date),
               cast(value as datetime(0)),
               cast(value as datetime(3)),
               cast(value as datetime(6)),
               cast(value as datetime(7)),
               cast(value as datetime(8)),
               cast(value as time(6)),
               cast(value as bigint),
               cast(value as largeint),
               cast(value as float),
               cast(value as double),
               cast(value as timestamptz(6))
        from test_datetimev2_nano_cast_from_nano
        order by id
    """

    sql "drop table if exists test_datetimev2_nano_cast_timestamptz"
    sql """
        create table test_datetimev2_nano_cast_timestamptz (
            id int,
            value timestamptz(6)
        )
        duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into test_datetimev2_nano_cast_timestamptz values
        (1, '2024-02-29 04:34:56.123456+00:00'),
        (2, '1677-09-20 10:12:43.145225+00:00'),
        (3, '1677-09-20 10:12:43.145224+00:00'),
        (4, '2262-04-11 09:47:16.854775+00:00'),
        (5, '2262-04-11 09:47:16.854776+00:00'),
        (6, null)
    """
    sql "set time_zone = '+14:00'"
    order_qt_timestamptz_to_nano_after_timezone_conversion """
        select id, cast(value as datetime(9))
        from test_datetimev2_nano_cast_timestamptz
        order by id
    """

    sql "set time_zone = '+08:00'"
    sql "set enable_strict_cast = true"
    test {
        sql "select cast('2023-02-29 00:00:00.000000000' as datetime(9))"
        exception "invalid"
    }
    test {
        sql """
            select cast(datetime_value as datetime(9))
            from test_datetimev2_nano_cast_civil
            where id = 8
        """
        exception "overflow"
    }
    sql "set enable_strict_cast = false"
}
