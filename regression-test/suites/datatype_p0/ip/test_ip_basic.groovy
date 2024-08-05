
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

suite("test_ip_basic") {
    sql """ DROP TABLE IF EXISTS t0 """
    sql """ DROP TABLE IF EXISTS t1 """
    sql """ DROP TABLE IF EXISTS t2 """

    sql """ SET enable_nereids_planner=true """
    sql """ SET enable_fallback_to_original_planner=false """

    // create table t0
    sql """
        CREATE TABLE `t0` (
          `id` int,
          `ip_v4` ipv4,
          `ip_v6` ipv6
        ) ENGINE=OLAP
        DISTRIBUTED BY HASH(`id`) BUCKETS 4
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "enable_duplicate_without_keys_by_default" = "true"
        );
        """

    // create table t1
    sql """
        CREATE TABLE `t1` (
          `id` int,
          `ip_v4` ipv4,
          `ip_v6` ipv6
        ) ENGINE=OLAP
        DISTRIBUTED BY HASH(`id`) BUCKETS 4
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "enable_duplicate_without_keys_by_default" = "true"
        );
        """

    // create table t2
    sql """
        CREATE TABLE `t2` (
          `id` int,
          `ip_v4` ipv4,
          `ip_v6` ipv6
        ) ENGINE=OLAP
        DISTRIBUTED BY HASH(`id`) BUCKETS 4
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "enable_duplicate_without_keys_by_default" = "true"
        );
        """

    // insert data into t0
    sql """
        insert into t0 values
        (0, NULL, NULL),
        (1, '0.0.0.0', '::'),
        (2, '192.168.0.1', '::1'),
        (2, '192.168.0.1', '::1'),
        (3, '127.0.0.1', '2001:1b70:a1:610::b102:2'),
        (3, '127.0.0.1', '2001:1b70:a1:610::b102:2'),
        (3, '127.0.0.1', '2001:1b70:a1:610::b102:2'),
        (4, '255.255.255.255', 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff'),
        (4, '255.255.255.255', 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff'),
        (4, '255.255.255.255', 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff'),
        (4, '255.255.255.255', 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff');
        """

    // insert data into t1
    sql """
        insert into t1 values
        (0, NULL, NULL),
        (1, '0.0.0.0', '::'),
        (2, '192.168.0.1', '::1'),
        (2, '192.168.0.1', '::1'),
        (3, '127.0.0.1', '2001:1b70:a1:610::b102:2'),
        (3, '127.0.0.1', '2001:1b70:a1:610::b102:2'),
        (3, '127.0.0.1', '2001:1b70:a1:610::b102:2'),
        (4, '255.255.255.255', 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff'),
        (4, '255.255.255.255', 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff'),
        (4, '255.255.255.255', 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff'),
        (4, '255.255.255.255', 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff');
        """

    // insert data into t2
    streamLoad {
        db 'regression_test_datatype_p0_ip'
        table 't2'

        set 'column_separator', ','

        file 'test_data/test.csv'

        time 10000 // limit inflight 10s

        // stream load action will check result, include Success status, and NumberTotalRows == NumberLoadedRows
    }

    sql """sync"""

    // order by
    qt_sql1 "select ip_v4 from t0 order by ip_v4"
    qt_sql2 "select ip_v6 from t0 order by ip_v6"
    qt_sql3 "select ip_v4 from t0 order by ip_v4 desc"
    qt_sql4 "select ip_v6 from t0 order by ip_v6 desc"
    qt_sql5 "select ip_v4 from t0 order by ip_v4 limit 1 offset 1"
    qt_sql6 "select ip_v6 from t0 order by ip_v6 limit 1 offset 1"
    qt_sql7 "select ip_v4 from t0 order by ip_v4 desc limit 1 offset 1"
    qt_sql8 "select ip_v6 from t0 order by ip_v6 desc limit 1 offset 1"
    qt_sql8_2 "select ip_v6 from t0 order by ip_v6 desc limit 1 offset 1"
    qt_sql8_3 "select ip_v6 from t0 order by ip_v6 desc limit 1 offset 1"

    // group by and agg
    qt_sql9 "select ip_v4, count(ip_v4) as cnt from t0 group by ip_v4 order by ip_v4"
    qt_sql10 "select ip_v6, count(ip_v6) as cnt from t0 group by ip_v6 order by ip_v6"
    qt_sql11 "select count(ip_v4) as cnt, min(ip_v4), max(ip_v4) from t0 group by ip_v4 order by min(ip_v4)"
    qt_sql12 "select count(ip_v6) as cnt, min(ip_v6), max(ip_v6) from t0 group by ip_v6 order by min(ip_v6)"

    // join
    qt_sql13 "select t0.id, t0.ip_v4, t0.ip_v6, t1.id, t1.ip_v4, t1.ip_v6 from t0 join t1 on t0.ip_v4=t1.ip_v4 and t0.ip_v6=t1.ip_v6 order by t0.id, t1.id"

    // join and group by
    qt_sql14 "select t0.ip_v4, count(*) as cnt from t0 join t1 on t0.ip_v4=t1.ip_v4 and t0.ip_v6=t1.ip_v6 group by t0.ip_v4 order by cnt"

    // order by
    qt_sql15 "select ip_v4 from t2 order by ip_v4"
    qt_sql16 "select ip_v6 from t2 order by ip_v6"
    qt_sql17 "select ip_v4, ip_v6 from t2 order by ip_v4, ip_v6 limit 20 offset 50"

    sql "DROP TABLE t0"
    sql "DROP TABLE t1"
    sql "DROP TABLE t2"
}
