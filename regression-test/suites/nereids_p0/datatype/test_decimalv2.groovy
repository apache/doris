
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

suite("test_decimalv2") {
    def tbName = "test_decimalv2_exprs"

    sql """set enable_nereids_planner=true"""
    sql """set enable_fallback_to_original_planner=false"""
    sql """set enable_nereids_dml=true"""

    sql "DROP TABLE IF EXISTS ${tbName}"
    sql """
            create table ${tbName}(k1 decimalv2(10,3), k2 int) distributed by hash(k1) buckets 1 properties("replication_num" = "1");
        """
    sql """ insert into ${tbName} values("1.23", 1); """

    qt_sql1 """ select dt
             from
             (
             select cast(k1 as decimalv2(5,3)) as dt
             from ${tbName}
             ) r; """
    qt_sql2 """ select dt
             from
             (
             select cast(k1 as decimal(5,3)) as dt
             from ${tbName}
             ) r; """
    sql "DROP TABLE ${tbName}"

    tbName = "test_decimalv2_runtime_filter"
    sql "DROP TABLE IF EXISTS ${tbName}"
    sql """
            CREATE TABLE IF NOT EXISTS ${tbName} (
                c0 int,
                c2 decimalv2(10, 3)
            )
            DISTRIBUTED BY HASH(c0) BUCKETS 5 properties("replication_num" = "1");
        """
    sql "insert into ${tbName} values(1, '1.23')"
    sql "insert into ${tbName} values(2, '2.34')"
    sql "insert into ${tbName} values(3, '3.45')"

    qt_sql1 "select * from ${tbName} ORDER BY c0"

    sql " set runtime_filter_type = 1; "
    qt_sql2 "select * from ${tbName} a, ${tbName} b WHERE a.c2 = b.c2 ORDER BY a.c0"

    sql " set runtime_filter_type = 2; "
    qt_sql2 "select * from ${tbName} a, ${tbName} b WHERE a.c2 = b.c2 ORDER BY a.c0"

    sql " set runtime_filter_type = 4; "
    qt_sql2 "select * from ${tbName} a, ${tbName} b WHERE a.c2 = b.c2 ORDER BY a.c0"

    sql " set runtime_filter_type = 8; "
    qt_sql2 "select * from ${tbName} a, ${tbName} b WHERE a.c2 = b.c2 ORDER BY a.c0"

    sql " set runtime_filter_wait_time_ms = 0; "

    sql " set runtime_filter_type = 1; "
    qt_sql2 "select * from ${tbName} a, ${tbName} b WHERE a.c2 = b.c2 ORDER BY a.c0"

    sql " set runtime_filter_type = 2; "
    qt_sql2 "select * from ${tbName} a, ${tbName} b WHERE a.c2 = b.c2 ORDER BY a.c0"

    sql " set runtime_filter_type = 4; "
    qt_sql2 "select * from ${tbName} a, ${tbName} b WHERE a.c2 = b.c2 ORDER BY a.c0"

    sql " set runtime_filter_type = 8; "
    qt_sql2 "select * from ${tbName} a, ${tbName} b WHERE a.c2 = b.c2 ORDER BY a.c0"

    sql "DROP TABLE ${tbName}"
}
