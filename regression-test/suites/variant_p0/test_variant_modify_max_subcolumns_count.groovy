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

// Lightweight variant_max_subcolumns_count ALTER: 0 -> N, N -> 0, N -> larger/smaller M and 1,
// each followed by loads, MOW update/delete/partial update, cumulative + full compaction, queries
// and heavy schema change. Oracle: an identical control table that never had the budget ALTER.
suite("test_variant_modify_max_subcolumns_count", "p0") {
    long seed = (context.config.otherConfigs.get("variantBudgetSeed") ?: "20261103") as long
    Random rnd = new Random(seed)
    int distinctPaths = 8
    int n = 1 + rnd.nextInt(distinctPaths + 4)
    int m = 1 + rnd.nextInt(distinctPaths + 4)
    List<Integer> budgets = [n, 0, m, m + 1 + rnd.nextInt(4), Math.max(1, m - 1 - rnd.nextInt(4)), 1,
                             distinctPaths + 1]
    logger.info("variant budget regression seed=${seed} budgets=${budgets} distinctPaths=${distinctPaths}")
    // Pin non-DOC for this connection: fuzzy sessions may default new VARIANT columns to DOC mode, and
    // variant_enable_doc_mode can not be declared together with variant_max_subcolumns_count.
    sql "set default_variant_enable_doc_mode = false"

    def variantType = { int count ->
        "variant<properties(\"variant_max_subcolumns_count\" = \"${count}\")>"
    }
    // Every generated row carries its key as v['k'], so a NULL read can not pass as NULL == NULL.
    def assertLoaded = { String t, int lo, int hi ->
        def hit = sql "SELECT COUNT(*) FROM ${t} WHERE k BETWEEN ${lo} AND ${hi} AND CAST(v['k'] AS BIGINT) = k"
        assertEquals((hi - lo + 1) as long, hit[0][0] as long, "seed=${seed} ${t} v['k'] sentinel keys ${lo}..${hi}")
    }
    // Full compaction must succeed, except E-2008 (no suitable version) when cumulative already left a
    // single non-empty rowset; any other failure still fails.
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
                logger.info("full compaction of ${t} failed: last full status=${last}, rowsets=${status.rowsets}")
                if (code != 0 || last == null || !last.contains("E-2008") || nonEmpty != 1) {
                    throw e
                }
            }
        }
    }
    def nextKey = 100
    def jsonRow = { int key ->
        def obj = [:]
        for (int i = 0; i < distinctPaths; i++) {
            if (rnd.nextInt(3) != 0) {
                obj["p${i}"] = rnd.nextBoolean() ? rnd.nextInt(1000) : "s${rnd.nextInt(1000)}"
            }
        }
        obj["k"] = key
        groovy.json.JsonOutput.toJson(obj)
    }

    for (String model : ["dup", "mow_row_store"]) {
        def tbl = "test_variant_budget_alter_${model}"
        def ctl = "test_variant_budget_ctl_${model}"
        def keyClause = model == "dup" ? "DUPLICATE KEY(k)" : "UNIQUE KEY(k)"
        def extraProps = model == "dup" ? "" :
                "\"enable_unique_key_merge_on_write\" = \"true\", \"store_row_column\" = \"true\","
        for (t in [tbl, ctl]) {
            sql "DROP TABLE IF EXISTS ${t}"
            sql """
                CREATE TABLE ${t} (
                    k INT,
                    c1 INT,
                    c2 SMALLINT,
                    v ${variantType(0)}
                ) ${keyClause}
                DISTRIBUTED BY HASH(k) BUCKETS 1
                PROPERTIES (${extraProps} "replication_num" = "1", "disable_auto_compaction" = "true");
            """
            // Old-only sentinel: path lost with value old is never written again.
            sql """INSERT INTO ${t} VALUES (1, 1, 1, parse_to_variant('{"lost": "old", "p0": 1}'))"""
            def lostLoaded = sql "SELECT CAST(v['lost'] AS STRING) FROM ${t} WHERE k = 1"
            assertEquals("old", lostLoaded[0][0], "seed=${seed} ${t} lost sentinel after load")
        }

        def loadBoth = { int rows ->
            List<String> values = []
            for (int i = 0; i < rows; i++) {
                int key = nextKey++
                values << "(${key}, ${key % 7}, ${key % 5}, parse_to_variant('${jsonRow(key)}'))"
            }
            def stmt = values.join(",")
            for (t in [tbl, ctl]) {
                sql "INSERT INTO ${t} VALUES ${stmt}"
            }
            for (t in [tbl, ctl]) {
                assertLoaded(t, nextKey - rows, nextKey - 1)
            }
            logger.info("variant budget loaded keys ${nextKey - rows}..${nextKey - 1} into ${tbl}/${ctl}")
        }
        def compactBoth = {
            for (t in [tbl, ctl]) {
                // Cumulative may legitimately find no candidate after one new rowset.
                trigger_and_wait_compaction(t, "cumulative", 300)
                fullCompaction(t)
            }
        }
        def sameResult = { String query ->
            def actual = sql query.replace("@T", tbl)
            def expected = sql query.replace("@T", ctl)
            assertEquals(expected, actual, "seed=${seed} query=${query}")
        }
        def verify = { String stage ->
            logger.info("variant budget verify ${model} ${stage}")
            sameResult "SELECT k, c1, c2, CAST(v AS STRING) FROM @T ORDER BY k"
            sameResult "SELECT k, CAST(v['p0'] AS STRING), CAST(v['p3'] AS STRING), CAST(v['p7'] AS STRING) FROM @T ORDER BY k"
            sameResult "SELECT k FROM @T WHERE CAST(v['p1'] AS INT) > 500 ORDER BY k"
            sameResult "SELECT COUNT(*) FROM @T WHERE v['nonexistent'] IS NOT NULL"
            sameResult "SELECT COUNT(*) FROM @T WHERE v['p2'] IS NOT NULL"
            def lost = sql "SELECT CAST(v['lost'] AS STRING), CAST(v AS STRING) FROM ${tbl} WHERE k = 1"
            assertEquals(1, lost.size(), "seed=${seed} stage=${stage}")
            assertEquals("old", lost[0][0], "lost:old at ${stage}, seed=${seed}")
            if (model != "dup") {
                // Unique-key full-key equality is the row-store point query path.
                sameResult "SELECT * FROM @T WHERE k = 1"
                sameResult "SELECT * FROM @T WHERE k = ${nextKey - 1}"
            }
        }

        loadBoth(20)
        verify("initial")
        test {
            sql "ALTER TABLE ${tbl} MODIFY COLUMN v ${variantType(n)} AFTER k"
            exception "Can not change variant properties together with column position"
        }

        budgets.eachWithIndex { int budget, int step ->
            sql "ALTER TABLE ${tbl} MODIFY COLUMN v ${variantType(budget)}"
            def create = sql "SHOW CREATE TABLE ${tbl}"
            assertTrue(budget == 0 || create[0][1].contains("\"variant_max_subcolumns_count\" = \"${budget}\""),
                    "seed=${seed} step=${step} budget=${budget}: ${create[0][1]}")
            verify("step ${step} budget ${budget} after alter")
            loadBoth(10 + rnd.nextInt(10))
            if (model != "dup") {
                int updateKey = 100 + rnd.nextInt(nextKey - 100)
                int deleteKey = 100 + rnd.nextInt(nextKey - 100)
                logger.info("variant budget step ${step} update ${updateKey} delete ${deleteKey}")
                def updatedJson = jsonRow(updateKey)
                for (t in [tbl, ctl]) {
                    sql "INSERT INTO ${t} VALUES (${updateKey}, 0, 0, parse_to_variant('${updatedJson}'))"
                    assertLoaded(t, updateKey, updateKey)
                }
                for (t in [tbl, ctl]) {
                    sql "DELETE FROM ${t} WHERE k = ${deleteKey}"
                    sql "set enable_unique_key_partial_update = true"
                    sql "INSERT INTO ${t} (k, c1) VALUES (1, ${step + 10})"
                    sql "set enable_unique_key_partial_update = false"
                }
            }
            verify("step ${step} budget ${budget} after load")
            compactBoth()
            verify("step ${step} budget ${budget} after compaction")
            if (step == 0 || step == budgets.size() - 1) {
                def column = step == 0 ? "c1 BIGINT" : "c2 INT"
                for (t in [tbl, ctl]) {
                    sql "ALTER TABLE ${t} MODIFY COLUMN ${column}"
                    waitForSchemaChangeDone {
                        sql "SHOW ALTER TABLE COLUMN WHERE TableName='${t}' ORDER BY createtime DESC LIMIT 1"
                        time 600
                    }
                }
                verify("step ${step} budget ${budget} after heavy schema change")
                loadBoth(5)
                compactBoth()
                verify("step ${step} budget ${budget} after heavy schema change compaction")
            }
        }
        for (t in [tbl, ctl]) {
            sql "DROP TABLE IF EXISTS ${t}"
        }
    }

    // Templated column: 'b*' STRING with a field_pattern inverted index keeps its typed, indexed
    // paths while the dynamic budget moves 0 -> N -> 0 -> up/down. Exact oracle from generated rows;
    // every template query must return the same rows with the inverted index on and off.
    def templatedType = { int count, String extra = "" ->
        "variant<'b*' : string, ${extra}properties(\"variant_max_subcolumns_count\" = \"${count}\")>"
    }
    def tplRows = [:]
    def tplKey = 1000
    def tplJson = { int key ->
        def obj = [:]
        for (int i = 0; i < 3; i++) {
            if (rnd.nextInt(3) != 0) {
                obj["b${i}"] = "bv${rnd.nextInt(5)}".toString()
            }
        }
        for (int i = 0; i < distinctPaths; i++) {
            if (rnd.nextInt(3) != 0) {
                obj["d${i}"] = rnd.nextInt(1000)
            }
        }
        obj["k"] = key
        tplRows[key] = obj
        groovy.json.JsonOutput.toJson(obj)
    }
    def tplTable = "test_variant_budget_alter_templated"
    sql "DROP TABLE IF EXISTS ${tplTable}"
    sql """
        CREATE TABLE ${tplTable} (
            k INT,
            c1 INT,
            v ${templatedType(0)},
            INDEX idx_v_b (v) USING INVERTED PROPERTIES("field_pattern" = "b*")
        ) DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num" = "1", "disable_auto_compaction" = "true");
    """
    def tplLoad = { int rows ->
        List<String> values = []
        for (int i = 0; i < rows; i++) {
            int key = tplKey++
            values << "(${key}, ${key % 7}, parse_to_variant('${tplJson(key)}'))"
        }
        sql "INSERT INTO ${tplTable} VALUES ${values.join(',')}"
        assertLoaded(tplTable, tplKey - rows, tplKey - 1)
    }
    def withAndWithoutIndex = { String query, String stage ->
        sql "set enable_inverted_index_query = true"
        def indexed = sql query
        sql "set enable_inverted_index_query = false"
        def scanned = sql query
        sql "set enable_inverted_index_query = true"
        assertEquals(scanned, indexed, "seed=${seed} ${stage} index on/off: ${query}")
        indexed
    }
    def tplVerify = { String stage ->
        logger.info("variant templated budget verify ${stage}")
        def expectedKeys = { String path, String value ->
            tplRows.findAll { k, obj -> obj[path] == value }.keySet().sort().collect { [it] }
        }
        for (int i = 0; i < 3; i++) {
            String value = "bv${rnd.nextInt(5)}"
            def actual = withAndWithoutIndex(
                    "SELECT k FROM ${tplTable} WHERE v['b${i}'] = '${value}' ORDER BY k", stage)
            assertEquals(expectedKeys("b${i}", value), actual, "seed=${seed} ${stage} b${i}=${value}")
        }
        def present = withAndWithoutIndex(
                "SELECT COUNT(*) FROM ${tplTable} WHERE v['b0'] IS NOT NULL", stage)
        assertEquals((tplRows.count { k, obj -> obj.containsKey("b0") }) as long, present[0][0] as long,
                "seed=${seed} ${stage} b0 present")
        def d1 = sql "SELECT k, CAST(v['d1'] AS INT) FROM ${tplTable} WHERE v['d1'] IS NOT NULL ORDER BY k"
        def expectedD1 = tplRows.findAll { k, obj -> obj.containsKey("d1") }
                .collect { k, obj -> [k, obj["d1"]] }.sort { it[0] }
        assertEquals(expectedD1.toString(), d1.toString(), "seed=${seed} ${stage} d1")
        def total = sql "SELECT COUNT(*) FROM ${tplTable}"
        assertEquals(tplRows.size(), total[0][0] as int, "seed=${seed} ${stage} rows")
    }
    tplLoad(20)
    tplVerify("templated initial")
    (budgets + [0, n]).eachWithIndex { int budget, int step ->
        sql "ALTER TABLE ${tplTable} MODIFY COLUMN v ${templatedType(budget)}"
        def create = sql "SHOW CREATE TABLE ${tplTable}"
        assertTrue(create[0][1].contains("idx_v_b"), "seed=${seed} templated step ${step}: ${create[0][1]}")
        tplVerify("templated step ${step} budget ${budget} after alter")
        tplLoad(5 + rnd.nextInt(10))
        tplVerify("templated step ${step} budget ${budget} after load")
        trigger_and_wait_compaction(tplTable, "cumulative", 300)
        fullCompaction(tplTable)
        tplVerify("templated step ${step} budget ${budget} after compaction")
        if (step == 1) {
            sql "ALTER TABLE ${tplTable} MODIFY COLUMN c1 BIGINT"
            waitForSchemaChangeDone {
                sql "SHOW ALTER TABLE COLUMN WHERE TableName='${tplTable}' ORDER BY createtime DESC LIMIT 1"
                time 600
            }
            tplVerify("templated step ${step} after heavy schema change")
            tplLoad(5)
            fullCompaction(tplTable)
            tplVerify("templated step ${step} after heavy schema change compaction")
        }
    }
    sql "DROP TABLE IF EXISTS ${tplTable}"

    // ADDed templates over pre-existing dynamic data: old n* cells are dynamic BIGINT (exact match
    // with the added BIGINT template, so compaction may keep a typed leaf) and old m* cells are
    // dynamic BIGINT under an added STRING template (mismatch: old values stay uncast in a
    // carrier). Oracle: an identical control table that never gets the templates.
    def addTable = "test_variant_budget_add_template"
    def addCtl = "test_variant_budget_add_template_ctl"
    def addKey = 5000
    def addRows = [:]
    def addJson = { int key, boolean afterAdd ->
        def obj = [:]
        if (rnd.nextInt(4) != 0) {
            obj["n1"] = rnd.nextInt(1000)
        }
        if (rnd.nextInt(4) != 0) {
            obj["m1"] = afterAdd ? "m${rnd.nextInt(1000)}".toString() : rnd.nextInt(1000)
        }
        obj["d0"] = rnd.nextInt(1000)
        obj["k"] = key
        addRows[key] = obj
        groovy.json.JsonOutput.toJson(obj)
    }
    for (t in [addTable, addCtl]) {
        sql "DROP TABLE IF EXISTS ${t}"
        sql """
            CREATE TABLE ${t} (k INT, v ${variantType(2)})
            DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES ("replication_num" = "1", "disable_auto_compaction" = "true");
        """
    }
    def addLoad = { int rows, boolean afterAdd ->
        List<String> values = []
        for (int i = 0; i < rows; i++) {
            int key = addKey++
            values << "(${key}, parse_to_variant('${addJson(key, afterAdd)}'))"
        }
        for (t in [addTable, addCtl]) {
            sql "INSERT INTO ${t} VALUES ${values.join(',')}"
            assertLoaded(t, addKey - rows, addKey - 1)
        }
    }
    def addVerify = { String stage ->
        logger.info("variant add template verify ${stage}")
        // Whole values use an exact oracle: old m* cells keep their BIGINT values (old values are never cast), while the control
        // table's dynamic m1 may be promoted to a common type by its own compaction.
        String wholeQuery = "SELECT k, CAST(v AS STRING) FROM @T ORDER BY k"
        def whole = sql wholeQuery.replace("@T", addTable)
        assertEquals(addRows.keySet().sort(), whole.collect { it[0] as int }, "seed=${seed} ${stage} keys")
        for (def row : whole) {
            assertEquals(addRows[row[0] as int], new groovy.json.JsonSlurper().parseText(row[1]),
                    "seed=${seed} ${stage} exact value of k=${row[0]}")
        }
        for (String query : [
                wholeQuery,
                "SELECT k, CAST(v['n1'] AS BIGINT), CAST(v['m1'] AS STRING) FROM @T ORDER BY k",
                "SELECT k FROM @T WHERE CAST(v['n1'] AS BIGINT) > 500 ORDER BY k",
                "SELECT COUNT(*) FROM @T WHERE v['m1'] IS NOT NULL"]) {
            sql "set enable_inverted_index_query = true"
            def indexed = sql query.replace("@T", addTable)
            sql "set enable_inverted_index_query = false"
            def scanned = sql query.replace("@T", addTable)
            sql "set enable_inverted_index_query = true"
            def expected = sql query.replace("@T", addCtl)
            if (query != wholeQuery) {
                assertEquals(expected, indexed, "seed=${seed} ${stage}: ${query}")
            }
            assertEquals(scanned, indexed, "seed=${seed} ${stage} index on/off: ${query}")
        }
    }
    addLoad(20, false)
    for (t in [addTable, addCtl]) {
        fullCompaction(t)
    }
    addLoad(10, false)
    addVerify("add-template before ADD")
    sql "ALTER TABLE ${addTable} MODIFY COLUMN v variant<'n*' : bigint, 'm*' : string, properties(\"variant_max_subcolumns_count\" = \"2\")>"
    addVerify("add-template after ADD")
    [0, 1, n, 0].eachWithIndex { int budget, int step ->
        sql "ALTER TABLE ${addTable} MODIFY COLUMN v variant<'n*' : bigint, 'm*' : string, properties(\"variant_max_subcolumns_count\" = \"${budget}\")>"
        sql "ALTER TABLE ${addCtl} MODIFY COLUMN v ${variantType(budget)}"
        addLoad(5 + rnd.nextInt(10), true)
        addVerify("add-template step ${step} budget ${budget} after load")
        for (t in [addTable, addCtl]) {
            trigger_and_wait_compaction(t, "cumulative", 300)
            fullCompaction(t)
        }
        addVerify("add-template step ${step} budget ${budget} after compaction")
    }
    for (t in [addTable, addCtl]) {
        sql "ALTER TABLE ${t} ADD COLUMN c9 INT DEFAULT '0'"
        sql "ALTER TABLE ${t} MODIFY COLUMN c9 BIGINT DEFAULT '0'"
        waitForSchemaChangeDone {
            sql "SHOW ALTER TABLE COLUMN WHERE TableName='${t}' ORDER BY createtime DESC LIMIT 1"
            time 600
        }
    }
    // Intended behavior: heavy SC rewrites old values under the current template, so old BIGINT m*
    // cells become STRING under the added 'm*' : string template (n* already match BIGINT).
    addRows.each { rk, obj -> if (obj["m1"] instanceof Integer) { obj["m1"] = obj["m1"].toString() } }
    addVerify("add-template after heavy schema change")
    for (t in [addTable, addCtl]) {
        fullCompaction(t)
    }
    addVerify("add-template after heavy schema change compaction")
    for (t in [addTable, addCtl]) {
        sql "DROP TABLE IF EXISTS ${t}"
    }

    // DOC mode: the parser normalizes DOC budgets to 0, and switching DOC off stays immutable.
    def docTable = "test_variant_budget_alter_doc"
    sql "DROP TABLE IF EXISTS ${docTable}"
    sql """
        CREATE TABLE ${docTable} (k INT, v variant<properties("variant_enable_doc_mode" = "true")>)
        DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ("replication_num" = "1");
    """
    test {
        // The parser zeroes DOC-only sparse settings, so the first immutable difference reported (same
        // field order as master's Column.checkSchemaChangeAllowed) is the sparse statistics size.
        sql "ALTER TABLE ${docTable} MODIFY COLUMN v ${variantType(n)}"
        exception "Can not change variant max sparse column statistics size"
    }
    sql "DROP TABLE IF EXISTS ${docTable}"
}
