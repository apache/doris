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

suite("test_spm_grouping_set_ordinal", "p0") {
    sql "SET enable_spm_rewrite = true"
    sql "SET enable_spm_fallback = false"
    sql "SET enable_sql_cache = false"
    sql "DROP TABLE IF EXISTS spm_grouping_set_ordinal"
    sql """CREATE TABLE spm_grouping_set_ordinal (k INT, v INT)
        DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1")"""
    sql "INSERT INTO spm_grouping_set_ordinal VALUES (0,10),(1,10),(2,20),(3,20),(NULL,10)"

    def dropOwnBaselines = {
        sql("SHOW BASELINE PLANS").findAll { it[1].toString().contains("spm_grouping_set_ordinal") }
                .each { row -> sql "DROP BASELINE PLAN ${row[0]}" }
    }
    dropOwnBaselines()
    def cases = [
        ["grouping_rollup",
            "SELECT k % 2 AS c, GROUPING(k % 2) AS g, COUNT(*) AS n FROM spm_grouping_set_ordinal"
                + " GROUP BY ROLLUP(1) ORDER BY g,c",
            "SELECT k % 3 AS c, GROUPING(k % 3) AS g, COUNT(*) AS n FROM spm_grouping_set_ordinal"
                + " GROUP BY ROLLUP(1) ORDER BY g,c"],
        ["grouping_first",
            "SELECT GROUPING(k % 2) AS g, k % 2 AS c, COUNT(*) AS n FROM spm_grouping_set_ordinal"
                + " GROUP BY ROLLUP(2) ORDER BY g,c",
            "SELECT GROUPING(k % 3) AS g, k % 3 AS c, COUNT(*) AS n FROM spm_grouping_set_ordinal"
                + " GROUP BY ROLLUP(2) ORDER BY g,c"],
        ["constant",
            "SELECT 1 AS c, GROUPING(1) AS g, COUNT(*) AS n FROM spm_grouping_set_ordinal"
                + " GROUP BY ROLLUP(1) ORDER BY g,c",
            "SELECT 2 AS c, GROUPING(2) AS g, COUNT(*) AS n FROM spm_grouping_set_ordinal"
                + " GROUP BY ROLLUP(1) ORDER BY g,c"],
        ["constant_empty",
            "SELECT 1 AS c, GROUPING(1) AS g, COUNT(*) AS n FROM spm_grouping_set_ordinal WHERE k < 0"
                + " GROUP BY ROLLUP(1) ORDER BY g,c",
            "SELECT 2 AS c, GROUPING(2) AS g, COUNT(*) AS n FROM spm_grouping_set_ordinal WHERE k < 0"
                + " GROUP BY ROLLUP(1) ORDER BY g,c"],
        ["cube_id",
            "SELECT k % 2 AS c, v % 3 AS d, GROUPING_ID(v % 3,k % 2) AS g, COUNT(*) AS n"
                + " FROM spm_grouping_set_ordinal GROUP BY CUBE(1, 2) ORDER BY g,c,d",
            "SELECT k % 3 AS c, v % 4 AS d, GROUPING_ID(v % 4,k % 3) AS g, COUNT(*) AS n"
                + " FROM spm_grouping_set_ordinal GROUP BY CUBE(1, 2) ORDER BY g,c,d"],
        ["set_members",
            "SELECT k % 2 AS c, v, GROUPING_ID(v,k % 2) AS g, COUNT(*) AS n FROM spm_grouping_set_ordinal"
                + " GROUP BY GROUPING SETS((1,2),(1),(2),()) ORDER BY g,c,v",
            "SELECT k % 3 AS c, v, GROUPING_ID(v,k % 3) AS g, COUNT(*) AS n FROM spm_grouping_set_ordinal"
                + " GROUP BY GROUPING SETS((1,2),(1),(2),()) ORDER BY g,c,v"],
        ["duplicate_sets",
            "SELECT k % 2 AS c, GROUPING(k % 2) AS g, COUNT(*) AS n FROM spm_grouping_set_ordinal"
                + " GROUP BY GROUPING SETS((1),(1),()) ORDER BY g,c",
            "SELECT k % 3 AS c, GROUPING(k % 3) AS g, COUNT(*) AS n FROM spm_grouping_set_ordinal"
                + " GROUP BY GROUPING SETS((1),(1),()) ORDER BY g,c"],
        ["direct_expression",
            "SELECT k % 2 AS c, GROUPING(k % 2) AS g, COUNT(*) AS n FROM spm_grouping_set_ordinal"
                + " GROUP BY ROLLUP(k % 2) ORDER BY g,c",
            "SELECT k % 3 AS c, GROUPING(k % 3) AS g, COUNT(*) AS n FROM spm_grouping_set_ordinal"
                + " GROUP BY ROLLUP(k % 3) ORDER BY g,c"],
        ["having",
            "SELECT k % 2 AS c, COUNT(*) AS n FROM spm_grouping_set_ordinal"
                + " GROUP BY ROLLUP(1) HAVING GROUPING(k % 2) = 0 ORDER BY c",
            "SELECT k % 3 AS c, COUNT(*) AS n FROM spm_grouping_set_ordinal"
                + " GROUP BY ROLLUP(1) HAVING GROUPING(k % 3) = 0 ORDER BY c"],
        ["order_key",
            "SELECT k % 2 AS c, COUNT(*) AS n FROM spm_grouping_set_ordinal GROUP BY ROLLUP(1)"
                + " ORDER BY GROUPING(k % 2),c",
            "SELECT k % 3 AS c, COUNT(*) AS n FROM spm_grouping_set_ordinal GROUP BY ROLLUP(1)"
                + " ORDER BY GROUPING(k % 3),c"]
    ]
    try {
        cases.each { entry ->
            long id = sql('CREATE GLOBAL BASELINE PLAN "' + entry[1] + '"')[0][0] as long
            String provenance = "SELECT plan_frozen FROM __internal_schema.spm_baselines WHERE id = ${id}"
            assertEquals(true, sql(provenance)[0][0], entry[0])
            "qt_${entry[0]}_frozen"(provenance)
            [entry[1], entry[2]].eachWithIndex { query, index ->
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
