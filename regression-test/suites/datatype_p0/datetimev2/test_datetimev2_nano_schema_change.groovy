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

suite("test_datetimev2_nano_schema_change") {
    sql "drop table if exists test_datetimev2_nano_schema_change"
    sql """
        create table test_datetimev2_nano_schema_change (
            id int,
            dt datetime(6)
        )
        duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into test_datetimev2_nano_schema_change values
        (1, '1677-09-21 00:12:43.145225'),
        (2, '1969-12-31 23:59:59.999999'),
        (3, '1970-01-01 00:00:00.000000'),
        (4, '1970-01-01 00:00:00.123456'),
        (5, '2262-04-11 23:47:16.854775'),
        (6, null)
    """

    sql """
        alter table test_datetimev2_nano_schema_change
        modify column dt datetime(9) null
    """
    waitForSchemaChangeDone {
        sql """
            show alter table column
            where IndexName = 'test_datetimev2_nano_schema_change'
            order by CreateTime desc
            limit 1
        """
        time 600
    }
    order_qt_schema_change_micro_to_nano """
        select id, dt
        from test_datetimev2_nano_schema_change
        order by id
    """

    sql """
        insert into test_datetimev2_nano_schema_change values
        (7, '1677-09-21 00:12:43.145224192'),
        (8, '1970-01-01 00:00:00.000000001'),
        (9, '1970-01-01 00:00:00.123456789'),
        (10, '2262-04-11 23:47:16.854775807')
    """
    order_qt_schema_change_nano_values """
        select id, dt
        from test_datetimev2_nano_schema_change
        order by id
    """

    sql """
        alter table test_datetimev2_nano_schema_change
        modify column dt datetime(7) null
    """
    waitForSchemaChangeDone {
        sql """
            show alter table column
            where IndexName = 'test_datetimev2_nano_schema_change'
            order by CreateTime desc
            limit 1
        """
        time 600
    }
    order_qt_schema_change_nano_scale """
        select id, dt
        from test_datetimev2_nano_schema_change
        order by id
    """

    sql """
        alter table test_datetimev2_nano_schema_change
        modify column dt datetime(6) null
    """
    waitForSchemaChangeDone {
        sql """
            show alter table column
            where IndexName = 'test_datetimev2_nano_schema_change'
            order by CreateTime desc
            limit 1
        """
        time 600
    }
    order_qt_schema_change_nano_to_micro """
        select id, dt
        from test_datetimev2_nano_schema_change
        order by id
    """

    sql """
        alter table test_datetimev2_nano_schema_change
        add column dt9 datetime(9) null default
            '1970-01-01 00:00:00.000000000'
    """
    order_qt_schema_change_add_nano """
        select id, dt, dt9
        from test_datetimev2_nano_schema_change
        order by id
    """

    sql """
        insert into test_datetimev2_nano_schema_change(id, dt, dt9) values
        (100, '1970-02-30 00:00:00.0000000',
              '1970-02-30 00:00:00.000000000')
    """
    sql """
        insert into test_datetimev2_nano_schema_change(id, dt, dt9) values
        (101, '1677-09-21 00:12:43.1452241',
              '1677-09-21 00:12:43.145224191')
    """
    sql """
        insert into test_datetimev2_nano_schema_change(id, dt, dt9) values
        (102, '2262-04-11 23:47:16.8547759',
              '2262-04-11 23:47:16.854775808')
    """
    sql """
        insert into test_datetimev2_nano_schema_change(id, dt, dt9) values
        (103,
         cast(convert_tz(
             cast('1970-01-01 08:00:00.0000000' as datetime(7)),
             'Asia/Shanghai', 'UTC') as datetime(7)),
         cast(convert_tz(
             cast('1970-01-01 08:00:00.000000000' as datetime(9)),
             'Asia/Shanghai', 'UTC') as datetime(9)))
    """
    order_qt_schema_change_timezone_value """
        select id, dt, dt9
        from test_datetimev2_nano_schema_change
        where id = 103
        order by id
    """
}
