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

suite("test_spm_placeholder_context", "p0") {
    sql "set enable_spm_rewrite = true"
    sql "set enable_spm_fallback = false"
    sql "DROP TABLE IF EXISTS spm_placeholder_context"
    sql """CREATE TABLE spm_placeholder_context (k INT, v INT)
        DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1")"""
    sql "INSERT INTO spm_placeholder_context VALUES (-1, 2), (0, 1), (1, 3), (2, 0)"

    def dropOwnBaselines = {
        sql("SHOW BASELINE PLANS").findAll { it[1].toString().contains("spm_placeholder_context") }
                .each { row -> sql "DROP BASELINE PLAN ${row[0]}" }
    }
    dropOwnBaselines()

    def cases = [
        ["array", "element_at(array(NULL), 1)", "element_at(array(TRUE), 1)"],
        ["map", "element_at(map(7, NULL), 7)", "element_at(map(7, TRUE), 7)"],
        ["repeat", "element_at(array_repeat(NULL, 2), 1)",
                "element_at(array_repeat(TRUE, 3), 2)"],
        ["concat", "element_at(array_concat(array(NULL), array_repeat(NULL, 2)), 1)",
                "element_at(array_concat(array(TRUE), array_repeat(TRUE, 3)), 2)"],
        ["map_values", "element_at(map_values(map(7, NULL)), 1)",
                "element_at(map_values(map(7, TRUE)), 1)"],
        ["null_container", "element_at(array_concat(NULL, array_sort(NULL)), 1)",
                "element_at(array_concat(NULL, array_sort(NULL)), 2)"],
        ["wrapper", "element_at(array_reverse_sort(array(NULL)), 1)",
                "element_at(array_reverse_sort(array(TRUE)), 1)"],
        ["conditional", "element_at(array(CASE WHEN k = NULL THEN NULL ELSE NULL END), 1)",
                "element_at(array(CASE WHEN k = 1 THEN TRUE ELSE FALSE END), 1)"]
    ]
    try {
        cases.each { entry ->
            String bind = "SELECT k, k > 0 OR ${entry[1]} AS c FROM spm_placeholder_context ORDER BY k"
            String variant = "SELECT k, k > 0 OR ${entry[2]} AS c FROM spm_placeholder_context ORDER BY k"
            long id = sql('CREATE GLOBAL BASELINE PLAN "' + bind + '" WITH "' + bind + '"')[0][0] as Long
            [bind, variant].eachWithIndex { query, index ->
                def explain = sql("EXPLAIN ${query}").toString()
                assertTrue(explain.contains("SPM baseline hit: id=" + id), explain)
                def rewritten = sql(query)
                "qt_${entry[0]}_${index}_spm"(query)
                sql "set enable_spm_rewrite = false"
                def original = sql(query)
                assertEquals(original, rewritten, "SPM must preserve values and ordering: " + query)
                "qt_${entry[0]}_${index}_original"(query)
                sql "set enable_spm_rewrite = true"
            }
            sql "DROP BASELINE PLAN ${id}"
        }

        def ordinals = [
            ["sort", "SELECT k * (0 - 1) AS c FROM spm_placeholder_context ORDER BY 1 LIMIT 3",
                    "SELECT k * (3 - 1) AS c FROM spm_placeholder_context ORDER BY 1 LIMIT 3"],
            ["group", "SELECT 1 AS c, count(*) AS n FROM spm_placeholder_context GROUP BY 1 ORDER BY 1",
                    "SELECT 2 AS c, count(*) AS n FROM spm_placeholder_context GROUP BY 1 ORDER BY 1"],
            ["having", "SELECT k, sum(v + 1) AS c FROM spm_placeholder_context GROUP BY k"
                    + " HAVING count(*) > 0 ORDER BY 2 DESC, 1 LIMIT 3",
                    "SELECT k, sum(v + 2) AS c FROM spm_placeholder_context GROUP BY k"
                    + " HAVING count(*) > 0 ORDER BY 2 DESC, 1 LIMIT 3"]
        ]
        ordinals.each { entry ->
            long id = sql('CREATE GLOBAL BASELINE PLAN "' + entry[1] + '" WITH "' + entry[1] + '"')[0][0] as Long
            def planSql = sql("SELECT plan_sql FROM __internal_schema.spm_baselines WHERE id = ${id}")[0][0]
            assertEquals(entry[1], planSql.toString())
            [entry[1], entry[2]].eachWithIndex { query, index ->
                def explain = sql("EXPLAIN ${query}").toString()
                assertTrue(explain.contains("SPM baseline hit: id=" + id), explain)
                def rewritten = sql(query)
                "qt_${entry[0]}_${index}_spm"(query)
                sql "set enable_spm_rewrite = false"
                def original = sql(query)
                assertEquals(original, rewritten, "SPM must preserve values and ordering: " + query)
                "qt_${entry[0]}_${index}_original"(query)
                sql "set enable_spm_rewrite = true"
            }
            sql "DROP BASELINE PLAN ${id}"
        }
    } finally {
        dropOwnBaselines()
    }
}
