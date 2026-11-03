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

// Matrix of lightweight Variant ALTERs: variant_max_subcolumns_count 0 -> N -> 0 -> up -> down,
// interleaved with additive template ADDs and a heavy schema change, on DUP, MOW and MOW row-store
// tables. Every step loads, compacts and compares exact values against a control table that never
// had any Variant ALTER. Template values always match the template type, so the heavy schema change
// rewrite (old values cast to the template type) leaves the rendered values unchanged.
suite("test_variant_light_alter_matrix", "p0") {
    long seed = (context.config.otherConfigs.get("variantBudgetSeed") ?: "20261104") as long
    Random rnd = new Random(seed)
    int n = 2 + rnd.nextInt(6)
    logger.info("variant light alter matrix seed=${seed} n=${n}")
    sql "set default_variant_enable_doc_mode = false"

    def props = { int count -> "PROPERTIES(\"variant_max_subcolumns_count\" = \"${count}\")" }
    def vtype = { List<String> templates, int count ->
        "variant<" + (templates + [props(count)]).join(", ") + ">"
    }
    def fullCompaction = { String t ->
        try {
            trigger_and_wait_compaction(t, "full", 300, new String[0], [], new String[0], true)
        } catch (Throwable e) {
            def backendIp = [:]
            def backendPort = [:]
            getBackendIpHttpPort(backendIp, backendPort)
            for (def tablet : sql_return_maparray("SHOW TABLETS FROM ${t}")) {
                def (code, out, err) = be_show_tablet_status(backendIp[tablet.BackendId], backendPort[tablet.BackendId],
                        tablet.TabletId)
                def status = parseJson(out.trim())
                int nonEmpty = status.rowsets.count { (it.split(" ")[1] as int) > 0 }
                String last = status["last full status"]
                if (code != 0 || last == null || !last.contains("E-2008") || nonEmpty != 1) {
                    throw e
                }
            }
        }
    }
    def waitSchemaChange = { String t ->
        waitForSchemaChangeDone {
            sql "SHOW ALTER TABLE COLUMN WHERE TableName='${t}' ORDER BY createtime DESC LIMIT 1"
            time 600
        }
    }

    int nextKey = 1
    def jsonRow = { int key ->
        def obj = [k: key]
        if (rnd.nextInt(4) != 0) obj["m"] = rnd.nextInt(1000)
        if (rnd.nextInt(3) != 0) obj["g${rnd.nextInt(3)}"] = "s${rnd.nextInt(1000)}"
        for (int i = 0; i < 6; i++) {
            if (rnd.nextInt(3) == 0) obj["p${i}"] = rnd.nextInt(100)
        }
        groovy.json.JsonOutput.toJson(obj)
    }

    for (String model : ["dup", "mow", "mow_row_store"]) {
        def tbl = "test_variant_light_alter_matrix_${model}"
        def ctl = "test_variant_light_alter_matrix_ctl_${model}"
        def keyClause = model == "dup" ? "DUPLICATE KEY(k)" : "UNIQUE KEY(k)"
        def extra = model == "dup" ? "" : "\"enable_unique_key_merge_on_write\" = \"true\","
        if (model == "mow_row_store") extra += "\"store_row_column\" = \"true\","
        List<String> templates = []
        int count = 0
        sql "DROP TABLE IF EXISTS ${tbl}"
        sql "DROP TABLE IF EXISTS ${ctl}"
        sql """CREATE TABLE ${tbl} (k INT, c INT, v ${vtype(templates, count)}) ${keyClause}
               DISTRIBUTED BY HASH(k) BUCKETS 1
               PROPERTIES (${extra} "replication_num" = "1", "disable_auto_compaction" = "true")"""
        sql """CREATE TABLE ${ctl} (k INT, c INT, v ${vtype([], 0)}) ${keyClause}
               DISTRIBUTED BY HASH(k) BUCKETS 1
               PROPERTIES (${extra} "replication_num" = "1", "disable_auto_compaction" = "true")"""

        def load = { int rows ->
            List<String> values = []
            for (int i = 0; i < rows; i++) {
                int key = nextKey++
                values << "(${key}, ${key % 7}, parse_to_variant('${jsonRow(key)}'))"
            }
            // MOW: overwrite an existing key so old and new rowsets both carry it.
            if (model != "dup" && nextKey > rows + 1) {
                int old = 1 + rnd.nextInt(nextKey - rows - 1)
                values << "(${old}, ${old % 7}, parse_to_variant('${jsonRow(old)}'))"
            }
            def stmt = values.join(",")
            for (t in [tbl, ctl]) {
                sql "INSERT INTO ${t} VALUES ${stmt}"
                def hit = sql "SELECT COUNT(*) FROM ${t} WHERE k BETWEEN ${nextKey - rows} AND ${nextKey - 1} AND CAST(v['k'] AS BIGINT) = k"
                assertEquals(rows as long, hit[0][0] as long, "seed=${seed} ${t} sentinel")
            }
        }
        def same = { String query, String stage ->
            def actual = sql query.replace("@T", tbl)
            def expected = sql query.replace("@T", ctl)
            assertEquals(expected, actual, "seed=${seed} ${model} ${stage}: ${query}")
        }
        def verify = { String stage ->
            same("SELECT k, c, CAST(v AS STRING) FROM @T ORDER BY k", stage)
            same("SELECT k, CAST(v['m'] AS BIGINT) FROM @T WHERE v['m'] IS NOT NULL ORDER BY k", stage)
            same("SELECT k FROM @T WHERE CAST(v['m'] AS BIGINT) > 500 ORDER BY k", stage)
            same("SELECT k, CAST(v['g1'] AS STRING) FROM @T WHERE CAST(v['g1'] AS STRING) LIKE 's1%' ORDER BY k", stage)
            same("SELECT k, CAST(v['p3'] AS INT) FROM @T WHERE CAST(v['p3'] AS INT) < 50 ORDER BY k", stage)
            same("SELECT COUNT(*), SUM(CAST(v['m'] AS BIGINT)) FROM @T", stage)
            if (model == "mow_row_store") {
                // Point queries read the row store column.
                for (int i = 0; i < 3; i++) {
                    int key = 1 + rnd.nextInt(nextKey - 1)
                    same("SELECT k, c, CAST(v AS STRING) FROM @T WHERE k = ${key}", stage + " point k=${key}")
                }
            }
            def create = sql "SHOW CREATE TABLE ${tbl}"
            assertTrue(create[0][1].contains("\"variant_max_subcolumns_count\" = \"${count}\""),
                    "seed=${seed} ${model} ${stage} count=${count}: ${create[0][1]}")
            for (String tpl : templates) {
                assertTrue(create[0][1].contains(tpl.find(/'[^']*'/)), "${model} ${stage} template ${tpl}")
            }
        }
        def lightAlter = { String stage ->
            sql "ALTER TABLE ${tbl} MODIFY COLUMN v ${vtype(templates, count)}"
            waitSchemaChange(tbl)
            verify(stage + " after alter")
            load(15)
            verify(stage + " after load")
            for (t in [tbl, ctl]) {
                trigger_and_wait_compaction(t, "cumulative", 300)
                fullCompaction(t)
            }
            verify(stage + " after compaction")
        }

        load(20)
        load(20)
        verify("initial")
        // 0 -> template ADD (count stays 0)
        templates = ["MATCH_NAME 'm':bigint"]
        lightAlter("template add m")
        // 0 -> N
        count = n
        lightAlter("count 0->${n}")
        // N -> 0
        count = 0
        lightAlter("count ${n}->0")
        // heavy schema change after template ADD: rewrites history with the template type
        for (t in [tbl, ctl]) {
            sql "ALTER TABLE ${t} MODIFY COLUMN c BIGINT"
            waitSchemaChange(t)
        }
        verify("heavy sc")
        load(10)
        verify("heavy sc after load")
        // second template ADD + 0 -> N+1 (up)
        templates = templates + ["MATCH_NAME_GLOB 'g*':string"]
        lightAlter("template add g*")
        count = n + 1
        lightAlter("count 0->${n + 1}")
        // down to 1
        count = 1
        lightAlter("count ${n + 1}->1")
        for (t in [tbl, ctl]) {
            sql "DROP TABLE IF EXISTS ${t}"
        }
    }
}
