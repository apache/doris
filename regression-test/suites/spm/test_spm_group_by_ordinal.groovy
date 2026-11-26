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

suite("test_spm_group_by_ordinal", "p0") {
    sql "SET enable_spm_rewrite = true"
    sql "SET enable_spm_fallback = false"
    sql "SET enable_sql_cache = false"
    sql "DROP TABLE IF EXISTS spm_group_by_ordinal"
    sql """CREATE TABLE spm_group_by_ordinal (k INT, v INT)
        DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1")"""
    sql "INSERT INTO spm_group_by_ordinal VALUES (0, 10), (1, 10), (2, 10), (3, 10)"

    def dropOwnBaselines = {
        sql("SHOW BASELINE PLANS").findAll { it[1].toString().contains("spm_group_by_ordinal") }
                .each { row -> sql "DROP BASELINE PLAN ${row[0]}" }
    }
    dropOwnBaselines()
    def cases = [
        ["constant", true,
            "SELECT 1 AS c, COUNT(*) AS n FROM spm_group_by_ordinal GROUP BY 1 ORDER BY 1",
            "SELECT 2 AS c, COUNT(*) AS n FROM spm_group_by_ordinal GROUP BY 1 ORDER BY 1"],
        ["modulo", true,
            "SELECT k % 2 AS c, COUNT(*) AS n FROM spm_group_by_ordinal GROUP BY 1 ORDER BY 1",
            "SELECT k % 3 AS c, COUNT(*) AS n FROM spm_group_by_ordinal GROUP BY 1 ORDER BY 1"],
        ["empty", true,
            "SELECT 1 AS c, COUNT(*) AS n FROM spm_group_by_ordinal WHERE k < 0 GROUP BY 1 ORDER BY 1",
            "SELECT 2 AS c, COUNT(*) AS n FROM spm_group_by_ordinal WHERE k < 0 GROUP BY 1 ORDER BY 1"],
        ["having", true,
            "SELECT k % 2 AS c, v, COUNT(*) AS n FROM spm_group_by_ordinal"
                + " GROUP BY 2, 1 HAVING COUNT(*) > 1 ORDER BY 1, 2",
            "SELECT k % 3 AS c, v, COUNT(*) AS n FROM spm_group_by_ordinal"
                + " GROUP BY 2, 1 HAVING COUNT(*) > 1 ORDER BY 1, 2"],
        ["same_alias", true,
            "SELECT k % 2 AS c, v AS c, COUNT(*) AS n FROM spm_group_by_ordinal GROUP BY 2, 1 ORDER BY 1, 2",
            "SELECT k % 3 AS c, v AS c, COUNT(*) AS n FROM spm_group_by_ordinal GROUP BY 2, 1 ORDER BY 1, 2"],
        ["same_alias_topn", true,
            "SELECT k % 2 AS c, v AS c, COUNT(*) AS n FROM spm_group_by_ordinal"
                + " GROUP BY 2, 1 ORDER BY 1 DESC, 2 LIMIT 2",
            "SELECT k % 3 AS c, v AS c, COUNT(*) AS n FROM spm_group_by_ordinal"
                + " GROUP BY 2, 1 ORDER BY 1 DESC, 2 LIMIT 2"],
        ["two_keys", true,
            "SELECT k % 2 AS c, v % 3 AS d, COUNT(*) AS n FROM spm_group_by_ordinal GROUP BY 2, 1 ORDER BY 1, 2",
            "SELECT k % 3 AS c, v % 4 AS d, COUNT(*) AS n FROM spm_group_by_ordinal GROUP BY 2, 1 ORDER BY 1, 2"],
        ["rollup", false,
            "SELECT k % 2 AS c, COUNT(*) AS n FROM spm_group_by_ordinal GROUP BY ROLLUP(1) ORDER BY 1",
            "SELECT k % 3 AS c, COUNT(*) AS n FROM spm_group_by_ordinal GROUP BY ROLLUP(1) ORDER BY 1"],
        ["cube", false,
            "SELECT k % 2 AS c, COUNT(*) AS n FROM spm_group_by_ordinal GROUP BY CUBE(1) ORDER BY 1",
            "SELECT k % 3 AS c, COUNT(*) AS n FROM spm_group_by_ordinal GROUP BY CUBE(1) ORDER BY 1"],
        ["grouping_sets", false,
            "SELECT k % 2 AS c, COUNT(*) AS n FROM spm_group_by_ordinal GROUP BY GROUPING SETS((1), ()) ORDER BY 1",
            "SELECT k % 3 AS c, COUNT(*) AS n FROM spm_group_by_ordinal GROUP BY GROUPING SETS((1), ()) ORDER BY 1"]
    ]
    try {
        cases.each { entry ->
            long id = sql('CREATE GLOBAL BASELINE PLAN "' + entry[2] + '"')[0][0] as long
            String provenance = "SELECT plan_frozen FROM __internal_schema.spm_baselines WHERE id = ${id}"
            assertEquals(entry[1], sql(provenance)[0][0], entry[0])
            "qt_${entry[0]}_frozen"(provenance)
            [entry[2], entry[3]].eachWithIndex { query, index ->
                def explain = sql("EXPLAIN ${query}").toString()
                assertTrue(explain.contains("SPM baseline hit: id=${id},"), explain)
                def rewritten = sql(query)
                "qt_${entry[0]}_${index}_spm"(query)
                sql "SET enable_spm_rewrite = false"
                def original = sql(query)
                assertEquals(original, rewritten, query)
                "qt_${entry[0]}_${index}_original"(query)
                sql "SET enable_spm_rewrite = true"
            }
            sql "DROP BASELINE PLAN ${id}"
        }
    } finally {
        dropOwnBaselines()
    }
}
