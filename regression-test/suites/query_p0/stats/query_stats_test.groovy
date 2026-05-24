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

    def origNereids = sql("select @@enable_nereids_planner")[0][0]
    def origCache   = sql("select @@enable_query_cache")[0][0]

    sql "admin set all frontends config (\"enable_query_hit_stats\"=\"true\");"
    sql "set enable_nereids_planner = true"
    sql "set enable_query_cache = false"

    sql """INSERT INTO ${tbName} VALUES
            (true,  1,   100,  1000,  10000,  123.456, 'A1234', '2024-01-01', '2024-01-01 10:00:00', 'alpha',   1.23,  4.56, 'text1',  12345678901234567890),
            (false, -5,  200,  2000,  20000,  234.567, 'B2345', '2024-02-02', '2024-02-02 11:30:00', 'beta',    2.34,  5.67, 'text2',  22345678901234567890),
            (true,  10, -300,  3000,  30000,  345.678, 'C3456', '2024-03-03', '2024-03-03 12:45:00', 'gamma',   3.45,  6.78, 'text3',  32345678901234567890),
            (NULL,   0,    0,     0,      0,    0.000, 'D4567', '2024-04-04', '2024-04-04 00:00:00', 'delta',   0.00,  0.00, 'text4',  42345678901234567890),
            (false, 127, 32767, 2147483647, 9223372036854775807, 999.999, 'E5678', '2024-05-05', '2024-05-05 23:59:59', 'epsilon', 9.99, 9.99, 'text5', 52345678901234567890)"""

    sql "clean all query stats"

    explain { sql("select k1 from ${tbName} where k1 = 1") }
    def explainStats = sql "show query stats from ${tbName}"
    for (row in explainStats) {
        assertEquals(0, row[1] as int)
        assertEquals(0, row[2] as int)
    }

    sql "select k1 from ${tbName} where k0 = 1"
    sql "select k4 from ${tbName} where k2 = 1991"
    def stats1 = sql "show query stats from ${tbName}"
    assertTrue((stats1.find { it[0] == "k1" }?.[1] as int) >= 1)
    assertTrue((stats1.find { it[0] == "k0" }?.[2] as int) >= 1)
    assertTrue((stats1.find { it[0] == "k4" }?.[1] as int) >= 1)
    assertTrue((stats1.find { it[0] == "k2" }?.[2] as int) >= 1)
    assertEquals(0, stats1.find { it[0] == "k1" }?.[2] as int)
    assertEquals(0, stats1.find { it[0] == "k0" }?.[1] as int)
    for (col in ["k3", "k5", "k6", "k10", "k11", "k7", "k8", "k9", "k12", "k13"]) {
        def row = stats1.find { it[0] == col }
        assertNotNull(row)
        assertEquals(0, row[1] as int)
        assertEquals(0, row[2] as int)
    }
    def allStats1 = sql "show query stats from ${tbName} all"
    assertTrue((allStats1[0][1] as int) >= 2)
    def verboseStats1 = sql "show query stats from ${tbName} all verbose"
    assertTrue(verboseStats1.size() > 0)

    sql "clean all query stats"
    sql "select k3 from ${tbName} where k5 = 1.0"
    def stats2 = sql "show query stats from ${tbName}"
    assertTrue((stats2.find { it[0] == "k3" }?.[1] as int) >= 1)
    assertTrue((stats2.find { it[0] == "k5" }?.[2] as int) >= 1)
    assertEquals(0, stats2.find { it[0] == "k3" }?.[2] as int)
    assertEquals(0, stats2.find { it[0] == "k5" }?.[1] as int)
    for (col in ["k0", "k1", "k2", "k4", "k6", "k10", "k11", "k7", "k8", "k9", "k12", "k13"]) {
        def row = stats2.find { it[0] == col }
        assertNotNull(row)
        assertEquals(0, row[1] as int)
        assertEquals(0, row[2] as int)
    }
    def allStats2 = sql "show query stats from ${tbName} all"
    assertTrue((allStats2[0][1] as int) >= 1)

    sql "clean all query stats"
    sql "select k1 from ${tbName} where k1 = 1"
    def stats3 = sql "show query stats from ${tbName}"
    assertTrue((stats3.find { it[0] == "k1" }?.[1] as int) >= 1)
    assertTrue((stats3.find { it[0] == "k1" }?.[2] as int) >= 1)
    for (col in ["k0", "k2", "k3", "k4", "k5", "k6", "k10", "k11", "k7", "k8", "k9", "k12", "k13"]) {
        def row = stats3.find { it[0] == col }
        assertNotNull(row)
        assertEquals(0, row[1] as int)
        assertEquals(0, row[2] as int)
    }

    sql "clean all query stats"
    sql "insert into ${tbName} select * from ${tbName} where k1 > 0"
    def insertStats = sql "show query stats from ${tbName}"
    for (row in insertStats) {
        assertEquals(0, row[1] as int)
        assertEquals(0, row[2] as int)
    }

    sql "clean all query stats"
    sql "select k1 as x from ${tbName} where k2 = 1"
    def aliasResult = sql "show query stats from ${tbName}"
    assertEquals(0, aliasResult.find { it[0] == "k1" }?.[1] as int)
    assertTrue((aliasResult.find { it[0] == "k2" }?.[2] as int) >= 1)

    sql "clean all query stats"
    sql "select a.k1 from ${tbName} a, ${tbName} b where a.k1 = b.k1"
    def joinResult = sql "show query stats from ${tbName} all"
    assertTrue((joinResult[0][1] as int) >= 1)

    sql "admin set all frontends config (\"enable_query_hit_stats\"=\"false\");"
    sql "set enable_nereids_planner = ${origNereids}"
    sql "set enable_query_cache = ${origCache}"
}
