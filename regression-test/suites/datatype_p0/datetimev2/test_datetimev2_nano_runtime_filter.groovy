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

suite("test_datetimev2_nano_runtime_filter") {
    sql "drop table if exists test_datetimev2_nano_rf_left"
    sql "drop table if exists test_datetimev2_nano_rf_right"
    sql """
        create table test_datetimev2_nano_rf_left (
            id int,
            dt datetimev2(9)
        )
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        create table test_datetimev2_nano_rf_right (
            id int,
            dt datetimev2(9)
        )
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into test_datetimev2_nano_rf_left values
        (1, '1677-09-21 00:12:43.145224192'),
        (2, '1970-01-01 00:00:00.000000000'),
        (3, '1970-01-01 00:00:00.000000001'),
        (4, '2262-04-11 23:47:16.854775807')
    """
    sql """
        insert into test_datetimev2_nano_rf_right values
        (10, '1677-09-21 00:12:43.145224192'),
        (20, '1970-01-01 00:00:00.000000000'),
        (30, '1969-12-31 23:59:59.999999999'),
        (40, '2262-04-11 23:47:16.854775807')
    """
    sql "set enable_runtime_filter_prune = false"

    explain {
        sql """
            verbose
            select l.id, l.dt, r.id, r.dt
            from test_datetimev2_nano_rf_left l
            join test_datetimev2_nano_rf_right r on l.dt = r.dt
        """
        contains "runtime filters: RF000"
    }
    order_qt_rf_default """
        select l.id, l.dt, r.id, r.dt
        from test_datetimev2_nano_rf_left l
        join test_datetimev2_nano_rf_right r on l.dt = r.dt
        order by l.dt, l.id, r.id
    """

    sql "set runtime_filter_type = 1"
    explain {
        sql """
            verbose
            select l.id
            from test_datetimev2_nano_rf_left l
            join test_datetimev2_nano_rf_right r on l.dt = r.dt
        """
        contains "runtime filters: RF000[in]"
    }
    order_qt_rf_in """
        select l.id, l.dt, r.id
        from test_datetimev2_nano_rf_left l
        join test_datetimev2_nano_rf_right r on l.dt = r.dt
        order by l.dt, l.id, r.id
    """

    sql "set runtime_filter_type = 2"
    explain {
        sql """
            verbose
            select l.id
            from test_datetimev2_nano_rf_left l
            join test_datetimev2_nano_rf_right r on l.dt = r.dt
        """
        contains "runtime filters: RF000[bloom]"
    }
    order_qt_rf_bloom """
        select l.id, l.dt, r.id
        from test_datetimev2_nano_rf_left l
        join test_datetimev2_nano_rf_right r on l.dt = r.dt
        order by l.dt, l.id, r.id
    """

    sql "set runtime_filter_type = 4"
    explain {
        sql """
            verbose
            select l.id
            from test_datetimev2_nano_rf_left l
            join test_datetimev2_nano_rf_right r on l.dt = r.dt
        """
        contains "runtime filters: RF000[min_max]"
    }
    order_qt_rf_min_max """
        select l.id, l.dt, r.id
        from test_datetimev2_nano_rf_left l
        join test_datetimev2_nano_rf_right r on l.dt = r.dt
        order by l.dt, l.id, r.id
    """
}
