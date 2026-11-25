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

suite("test_subcolumn_stats", "p0") {

    def waitRowCountReported = { String tbl, long expected ->
        for (int i = 0; i < 60; i++) {
            def res = sql """SHOW DATA FROM ${tbl}"""
            def tableStats = sql """SHOW TABLE STATS ${tbl}"""
            if (tableStats.size() > 0 && tableStats[0][2].toLong() == expected) {
                return
            }
            sleep(1000)
        }
    }

    sql """DROP TABLE IF EXISTS subcol_stats_tbl"""
    sql """
        CREATE TABLE subcol_stats_tbl (
            id INT NOT NULL,
            p INT NOT NULL,
            v VARIANT<'uid': INT, 'profile.city': STRING, 'dyn*': INT> NULL,
            s STRUCT<age: INT, addr: STRUCT<city: VARCHAR(32), zip: INT>> NULL,
            arr ARRAY<INT> NULL
        ) DUPLICATE KEY(id, p)
        PARTITION BY RANGE(p) (
            PARTITION p1 VALUES LESS THAN (100),
            PARTITION p2 VALUES LESS THAN (200)
        )
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES("replication_num" = "1")
    """

    List<String> rows = []
    for (int i = 1; i <= 200; i++) {
        int partVal = i <= 100 ? 10 : 110
        int uid = (i % 20) + 1
        String city = ["Beijing", "Shanghai", "Shenzhen", "Hangzhou"][i % 4]
        int age = 20 + (i % 10)
        int zip = 10000 + (i % 5)
        String jsonDoc = "CAST('{\"uid\": ${uid}, \"profile\": {\"city\": \"${city}\"}}' AS JSON)"
        if (i % 10 == 0) {
            rows.add("(${i}, ${partVal}, ${jsonDoc}, NULL, [1, 2])")
        } else {
            rows.add("(${i}, ${partVal}, ${jsonDoc}, "
                    + "named_struct('age', ${age}, 'addr', named_struct('city', '${city}', 'zip', ${zip})), [1, 2])")
        }
    }
    sql "INSERT INTO subcol_stats_tbl VALUES " + rows.join(", ")

    // 1. Full ANALYZE collects scalar columns + predefined VARIANT subcolumns + nested STRUCT scalar subcolumns.
    sql """ANALYZE TABLE subcol_stats_tbl WITH SYNC"""

    def allStats = sql """SHOW COLUMN STATS subcol_stats_tbl"""
    def colMap = [:]
    for (def row : allStats) {
        colMap[row[0]] = row
    }
    assertTrue(colMap.containsKey("id"))
    assertTrue(colMap.containsKey("p"))
    assertTrue(colMap.containsKey("v.uid"))
    assertTrue(colMap.containsKey("v.profile.city"))
    assertTrue(colMap.containsKey("s.age"))
    assertTrue(colMap.containsKey("s.addr.city"))
    assertTrue(colMap.containsKey("s.addr.zip"))
    assertFalse(colMap.containsKey("v.dyn*"))
    assertFalse(colMap.containsKey("arr"))

    assertEquals("200.0", colMap["v.uid"][2])
    assertEquals("20.0", colMap["v.uid"][3])
    assertEquals("0.0", colMap["v.uid"][4])
    assertEquals("1", colMap["v.uid"][7])
    assertEquals("20", colMap["v.uid"][8])

    assertEquals("200.0", colMap["v.profile.city"][2])
    assertEquals("4.0", colMap["v.profile.city"][3])
    assertEquals("'Beijing'", colMap["v.profile.city"][7])
    assertEquals("'Shenzhen'", colMap["v.profile.city"][8])

    assertEquals("200.0", colMap["s.age"][2])
    assertEquals("9.0", colMap["s.age"][3])
    assertEquals("20.0", colMap["s.age"][4])

    assertEquals("200.0", colMap["s.addr.zip"][2])
    assertEquals("5.0", colMap["s.addr.zip"][3])
    assertEquals("20.0", colMap["s.addr.zip"][4])
    assertEquals("10000", colMap["s.addr.zip"][7])
    assertEquals("10004", colMap["s.addr.zip"][8])

    // 2. Verify Nereids CBO uses collected subcolumn NDV and min/max bounds in EXPLAIN without unknown-stats fallback.
    def explainVariantPhys = (sql """
        EXPLAIN PHYSICAL PLAN SELECT id FROM subcol_stats_tbl WHERE CAST(v['uid'] AS INT) = 5
    """).collect { it[0].toString() }.join("\n")
    logger.info("explainVariantPhys:\n" + explainVariantPhys)
    assertTrue(explainVariantPhys.contains("stats=10"), explainVariantPhys)

    def explainVariantBanner = (sql """
        EXPLAIN SELECT id FROM subcol_stats_tbl WHERE CAST(v['uid'] AS INT) = 5
    """).collect { it[0].toString() }.join("\n")
    assertFalse(explainVariantBanner.contains("planned with unknown column statistics"), explainVariantBanner)

    def explainVariantStrPhys = (sql """
        EXPLAIN PHYSICAL PLAN SELECT id FROM subcol_stats_tbl WHERE CAST(v['profile']['city'] AS STRING) = 'Beijing'
    """).collect { it[0].toString() }.join("\n")
    logger.info("explainVariantStrPhys:\n" + explainVariantStrPhys)
    assertTrue(explainVariantStrPhys.contains("stats=50"), explainVariantStrPhys)

    def explainStructPhys = (sql """
        EXPLAIN PHYSICAL PLAN SELECT id FROM subcol_stats_tbl WHERE s.addr.zip = 10001
    """).collect { it[0].toString() }.join("\n")
    logger.info("explainStructPhys:\n" + explainStructPhys)
    assertTrue(explainStructPhys.contains("stats=36"), explainStructPhys)

    def explainStructBanner = (sql """
        EXPLAIN SELECT id FROM subcol_stats_tbl WHERE s.addr.zip = 10001
    """).collect { it[0].toString() }.join("\n")
    assertFalse(explainStructBanner.contains("planned with unknown column statistics"), explainStructBanner)

    def explainUncollectedBanner = (sql """
        EXPLAIN SELECT id FROM subcol_stats_tbl WHERE CAST(v['uncollected_key'] AS INT) = 5
    """).collect { it[0].toString() }.join("\n")
    assertTrue(explainUncollectedBanner.contains("planned with unknown column statistics"), explainUncollectedBanner)

    // 3. Explicit root-column and subcolumn ANALYZE with block sampling + partition-scoped sampling.
    waitRowCountReported("subcol_stats_tbl", 200L)
    sql """DROP STATS subcol_stats_tbl(v, s)"""
    def afterDropRoot = sql """SHOW COLUMN STATS subcol_stats_tbl"""
    assertEquals(2, afterDropRoot.size())

    sql """ANALYZE TABLE subcol_stats_tbl(v, `s.addr.zip`) WITH SAMPLE ROWS 200 WITH SYNC"""
    def sampledSubcols = sql """SHOW COLUMN CACHED STATS subcol_stats_tbl(v, `s.addr.zip`)"""
    assertEquals(3, sampledSubcols.size())

    sql """ANALYZE TABLE subcol_stats_tbl PARTITION(p1) (`v.uid`, `s.age`) WITH SAMPLE ROWS 100 WITH SYNC"""
    def partSubcols = sql """SHOW COLUMN STATS subcol_stats_tbl(`v.uid`, `s.age`) PARTITION(p1)"""
    assertEquals(2, partSubcols.size())

    // 4. Manual stats injection via ALTER TABLE ... MODIFY COLUMN on a subcolumn.
    sql """
        ALTER TABLE subcol_stats_tbl MODIFY COLUMN `s.age`
        SET STATS ('row_count'='200', 'ndv'='10', 'num_nulls'='20', 'min_value'='20', 'max_value'='29', 'data_size'='800')
    """
    def injectedAge = sql """SHOW COLUMN CACHED STATS subcol_stats_tbl(`s.age`)"""
    assertEquals(1, injectedAge.size())
    assertEquals("10.0", injectedAge[0][3])

    // 5. Validation errors for nonexistent subcolumns and unsupported complex columns without scalar subcolumns.
    test {
        sql """ANALYZE TABLE subcol_stats_tbl(`v.not_declared`) WITH SYNC"""
        exception "Unknown column"
    }

    sql """SET enable_analyze_complex_type_column = true"""
    try {
        test {
            sql """ANALYZE TABLE subcol_stats_tbl(arr) WITH SYNC"""
            exception "Contains unsupported column type"
        }
    } finally {
        sql """SET enable_analyze_complex_type_column = false"""
    }
}
