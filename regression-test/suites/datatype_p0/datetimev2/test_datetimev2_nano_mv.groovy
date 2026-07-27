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

suite("test_datetimev2_nano_mv") {
    sql "drop table if exists test_datetimev2_nano_mv"
    sql """
        create table test_datetimev2_nano_mv (
            id int,
            name varchar(20),
            dt datetimev2(9)
        )
        duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into test_datetimev2_nano_mv values
        (1, 'minimum', '1677-09-21 00:12:43.145224192'),
        (1, 'epoch', '1970-01-01 00:00:00.000000000'),
        (1, 'after', '1970-01-01 00:00:00.000000001'),
        (2, 'epoch', '1970-01-01 00:00:00.000000000'),
        (2, 'maximum', '2262-04-11 23:47:16.854775807')
    """
    order_qt_mv_base """
        select * from test_datetimev2_nano_mv order by id, dt, name
    """

    sql """
        drop materialized view if exists mv_datetimev2_nano
        on test_datetimev2_nano_mv
    """
    create_sync_mv(context.dbName, "test_datetimev2_nano_mv", "mv_datetimev2_nano", """
        select id as mv_id, max(dt) as mv_max, min(dt) as mv_min
        from test_datetimev2_nano_mv
        group by id
    """)

    mv_rewrite_success("""
        select id, max(dt), min(dt)
        from test_datetimev2_nano_mv
        group by id
        order by id
    """, "mv_datetimev2_nano")

    order_qt_mv_aggregate """
        select id, max(dt), min(dt)
        from test_datetimev2_nano_mv
        group by id
        order by id
    """
}
