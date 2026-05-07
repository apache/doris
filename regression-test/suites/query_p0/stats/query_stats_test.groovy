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
    // increments the table query count by exactly 1, not 2.
    // The table count tracks how many queries touched the table — k3 and k5 are column-level
    // counters (queryHit and filterHit) and are independent of the table count.
    // If addStats were called twice (once for queryHit, once for filterHit), the count would
    // wrongly show 2. mergeFilterStats prevents this by updating filter counters only,
    // without re-incrementing the table query count.
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
