
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

suite("test_datev1") {
    def tbName = "test_datev1_exprs"

    sql """set enable_nereids_planner=true"""
    sql """set enable_fallback_to_original_planner=false"""
    sql """set enable_nereids_dml=true"""

    sql "DROP TABLE IF EXISTS ${tbName}"
    sql """
            create table ${tbName}(k1 datetimev1, k2 int) distributed by hash(k1) buckets 1 properties("replication_num" = "1");
        """
    sql """ insert into ${tbName} values("2016-11-04 00:00:01", 1); """

    qt_sql1 """ select dt
             from
             (
             select cast(k1 as datev1) as dt
             from ${tbName}
             ) r; """
    qt_sql2 """ select dt
             from
             (
             select cast(k1 as date) as dt
             from ${tbName}
             ) r; """
    sql "DROP TABLE ${tbName}"

    tbName = "test_datev1_runtime_filter"
    sql "DROP TABLE IF EXISTS ${tbName}"
    sql """
            CREATE TABLE IF NOT EXISTS ${tbName} (
                c0 int,
                c2 datev1,
                c3 datetimev1
            )
            DISTRIBUTED BY HASH(c0) BUCKETS 5 properties("replication_num" = "1");
        """
    sql "insert into ${tbName} values(1, '2000-01-01', '2000-01-01 11:11:11')"
    sql "insert into ${tbName} values(2, '2000-02-02', '2000-02-02 11:11:11')"
    sql "insert into ${tbName} values(3, '2000-03-02', '2000-03-02 11:11:11')"

    qt_sql1 "select * from ${tbName} ORDER BY c2"

    sql " set runtime_filter_type = 1; "
    qt_sql2 "select * from ${tbName} a, ${tbName} b WHERE a.c3 = b.c3 ORDER BY a.c2"

    sql " set runtime_filter_type = 2; "
    qt_sql2 "select * from ${tbName} a, ${tbName} b WHERE a.c3 = b.c3 ORDER BY a.c2"

    sql " set runtime_filter_type = 4; "
    qt_sql2 "select * from ${tbName} a, ${tbName} b WHERE a.c3 = b.c3 ORDER BY a.c2"

    sql " set runtime_filter_type = 8; "
    qt_sql2 "select * from ${tbName} a, ${tbName} b WHERE a.c3 = b.c3 ORDER BY a.c2"

    sql " set runtime_filter_wait_time_ms = 0; "

    sql " set runtime_filter_type = 1; "
    qt_sql2 "select * from ${tbName} a, ${tbName} b WHERE a.c3 = b.c3 ORDER BY a.c2"

    sql " set runtime_filter_type = 2; "
    qt_sql2 "select * from ${tbName} a, ${tbName} b WHERE a.c3 = b.c3 ORDER BY a.c2"

    sql " set runtime_filter_type = 4; "
    qt_sql2 "select * from ${tbName} a, ${tbName} b WHERE a.c3 = b.c3 ORDER BY a.c2"

    sql " set runtime_filter_type = 8; "
    qt_sql2 "select * from ${tbName} a, ${tbName} b WHERE a.c3 = b.c3 ORDER BY a.c2"

    sql "DROP TABLE ${tbName}"
}
