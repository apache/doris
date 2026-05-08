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

suite("query_stats_test") {

    def tbName = "stats_table"
    sql """ DROP TABLE IF EXISTS ${tbName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tbName} (
            `k0` boolean null comment "",
            `k1` tinyint(4) null comment "",
            `k2` smallint(6) null comment "",
            `k3` int(11) null comment "",
            `k4` bigint(20) null comment "",
            `k5` decimal(9, 3) null comment "",
            `k6` char(5) null comment "",
            `k10` date null comment "",
            `k11` datetime null comment "",
            `k7` varchar(20) null comment "",
            `k8` double max null comment "",
            `k9` float sum null comment "",
            `k12` string replace null comment "",
            `k13` largeint(40) replace null comment ""
        ) engine=olap
        DISTRIBUTED BY HASH(`k1`) BUCKETS 1 properties("replication_num" = "1")
        """
    sql "admin set frontend config (\"enable_query_hit_stats\"=\"true\");"
    sql "set enable_nereids_planner = true"
    sql "set enable_query_cache = false"

    // Insert data so Nereids produces a real OlapScan instead of VEMPTYSET
    // (an empty table is optimized away before translation, producing no scan node,
    // which means QueryStatsRecorder finds nothing to record).
    sql """INSERT INTO ${tbName} VALUES
        (true,  1,   100,  1000,  10000,  123.456, 'A1234', '2024-01-01', '2024-01-01 10:00:00', 'alpha',   1.23,  4.56, 'text1',  12345678901234567890),
        (false, -5,  200,  2000,  20000,  234.567, 'B2345', '2024-02-02', '2024-02-02 11:30:00', 'beta',    2.34,  5.67, 'text2',  22345678901234567890),
        (true,  10, -300,  3000,  30000,  345.678, 'C3456', '2024-03-03', '2024-03-03 12:45:00', 'gamma',   3.45,  6.78, 'text3',  32345678901234567890),
        (NULL,   0,    0,     0,      0,    0.000, 'D4567', '2024-04-04', '2024-04-04 00:00:00', 'delta',   0.00,  0.00, 'text4',  42345678901234567890),
        (false, 127, 32767, 2147483647, 9223372036854775807, 999.999, 'E5678', '2024-05-05', '2024-05-05 23:59:59', 'epsilon', 9.99, 9.99, 'text5', 52345678901234567890)"""

    sql "clean all query stats"

    explain {
        sql("select k1 from ${tbName} where k1 = 1")
    }

    qt_sql "show query stats from ${tbName}"

    sql "select k1 from ${tbName} where k0 = 1"
    sql "select k4 from ${tbName} where k2 = 1991"

    qt_sql "show query stats from ${tbName}"
    qt_sql "show query stats from ${tbName} all"
    qt_sql "show query stats from ${tbName} all verbose"

    // Verify that a single query touching both a SELECT column (k3) and a WHERE column (k5)
    // increments the table query count by exactly 1.
    // QueryStatsRecorder accumulates both queryHit and filterHit into a single StatsDelta
    // per table and calls addStats() once, so the table count is always 1 per query.
    sql "clean all query stats"
    sql "select k3 from ${tbName} where k5 = 1.0"
    // table count must be 1 (one query ran), not 2
    qt_sql "show query stats from ${tbName} all"
    // k3: queryHit=1 (SELECT), k5: filterHit=1 (WHERE), all others 0
    qt_sql "show query stats from ${tbName}"

    sql "admin set frontend config (\"enable_query_hit_stats\"=\"false\");"
    sql "set enable_nereids_planner = false"
    sql "set enable_query_cache = true"
}
