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

// ===========================================================================================
// Category 09: Window / Distinct Regression — WD01-WD04
// ===========================================================================================
//
// Purpose
//   Add a more focused regression set for window / distinct patterns:
//   - Window guard: ensure pruning is not triggered incorrectly under window wrappers
//   - Distinct + Window: ensure DISTINCT aggregation keeps the correct prune / no-prune behavior when wrapped by a window
//
// Case List
//   WD01: window over inner agg, child distribution already satisfied -> no pruning (negative)
//   WD02: count(distinct) + window, high NDV -> pruning works (positive)
//   WD03: count(distinct) + avg + window, high NDV -> pruning still works (positive)
//   WD04: count(distinct) + window, low NDV -> no pruning (negative)
// ===========================================================================================
suite("window_distinct_regression", "agg_shuffle_prune_func") {
    sql """set enable_nereids_planner=true;"""
    sql """set enable_fallback_to_original_planner=false;"""
    sql """set enable_sql_cache=false;"""
    sql """set enable_query_cache=false;"""
    sql """set agg_phase=0;"""
    sql """set agg_shuffle_use_parent_key=false;"""
    sql """set detail_shape_nodes='PhysicalHashAggregate,PhysicalDistribute,PhysicalWindow';"""

    def dbName = context.config.getDbNameByFile(context.file)
    sql "create database if not exists ${dbName};"
    sql "use ${dbName};"

    def tptn = 8
    sql "set parallel_fragment_exec_instance_num=1;"
    sql "set parallel_pipeline_task_num=${tptn};"
    sql "set be_number_for_test=1;"

    def highNdv = (tptn * 512 + tptn * 64).toString()
    def lowNdv = "4"
    def rowCount = "480000"
    def esc = { Object v -> v == null ? "null" : v.toString().replace("\\", "\\\\").replace("'", "\\'") }
    def ss = { String t, String c, String n, String nn, String ds, String mi, String ma, String hv ->
        def hvc = hv == null ? "" : ", 'hot_values'='${esc(hv)}'"
        sql "alter table ${t} modify column ${c} set stats ('row_count'='${rowCount}','ndv'='${esc(n)}','num_nulls'='${esc(nn)}','data_size'='${esc(ds)}','min_value'='${esc(mi)}','max_value'='${esc(ma)}'${hvc});"
    }
    def extractSigns = { String e ->
        def s = []
        def m = (e =~ /orderedShuffledColumns=\[([0-9,\s-]*)\]/)
        while (m.find()) {
            s << "[" + m.group(1).replaceAll("\\s+", "") + "]"
        }
        s
    }
    def extractSingle = { String e ->
        def ids = []
        def m = (e =~ /orderedShuffledColumns=\[([0-9,\s-]*)\]/)
        while (m.find()) {
            def r = m.group(1).trim()
            if (r.length() == 0) continue
            def p = r.split(",").collect { it.trim() }.findAll { it.length() > 0 }
            if (p.size() == 1) ids << Integer.parseInt(p[0])
        }
        ids
    }
    def extractIdLists = { String e ->
        def ids = []
        def m = (e =~ /orderedShuffledColumns=\[([0-9,\s-]*)\]/)
        while (m.find()) {
            def r = m.group(1).trim()
            ids << (r.length() == 0 ? [] : r.split(",").collect { it.trim() }.findAll { it.length() > 0 }.collect { Integer.parseInt(it) })
        }
        ids
    }
    def exprId = { String e, String c ->
        def m = (e =~ ("\\b" + java.util.regex.Pattern.quote(c) + "#(\\d+)\\b"))
        m.find() ? Integer.parseInt(m.group(1)) : null
    }
    def subtreeText = { List<String> lines, int rootIndex ->
        int rootIndent = lines[rootIndex].indexOf("Physical")
        def collected = []
        for (int i = rootIndex; i < lines.size(); i++) {
            def line = lines[i]
            int indent = line.indexOf("Physical")
            if (i > rootIndex && indent >= 0 && indent <= rootIndent) {
                break
            }
            collected << line
        }
        collected.join("\n")
    }
    def parseOrderedIds = { String line ->
        def m = (line =~ /orderedShuffledColumns=\[([0-9,\s-]*)\]/)
        if (!m.find()) {
            return null
        }
        def r = m.group(1).trim()
        return (r.length() == 0 ? [] : r.split(",").collect { it.trim() }.findAll { it.length() > 0 }.collect { Integer.parseInt(it) })
    }
    def findGlobalDistinctAggChildDistributes = { String e ->
        def lines = e.readLines()
        def ctxs = []
        for (int i = 0; i < lines.size(); i++) {
            def line = lines[i]
            if (!(line.contains("PhysicalHashAggregate")
                    && (line.contains("aggPhase=DISTINCT_GLOBAL") || line.contains("aggPhase=GLOBAL")))) {
                continue
            }
            int aggIndent = line.indexOf("PhysicalHashAggregate")
            for (int j = i + 1; j < lines.size(); j++) {
                def childLine = lines[j]
                int childIndent = childLine.indexOf("Physical")
                if (childIndent < 0) {
                    continue
                }
                if (childIndent <= aggIndent) {
                    break
                }
                if (childLine.contains("PhysicalDistribute") && childLine.contains("orderedShuffledColumns=")) {
                    ctxs << [ids: parseOrderedIds(childLine), subtree: subtreeText(lines, j)]
                    break
                }
            }
        }
        ctxs
    }
    def findScopedExprId = { String e, String c ->
        def m = (e =~ ("\\b" + java.util.regex.Pattern.quote(c) + "#(\\d+)\\b"))
        m.find() ? Integer.parseInt(m.group(1)) : null
    }
    def assertDistinctShufflePrunedToSingle = { String id, String eOff, String eOn, String c ->
        def offCtxs = findGlobalDistinctAggChildDistributes(eOff)
        def onCtxs = findGlobalDistinctAggChildDistributes(eOn)
        assertTrue(!offCtxs.isEmpty() && !onCtxs.isEmpty(),
            "${id}: missing target global/distinct-global agg-child distribute candidates, off=${offCtxs}, on=${onCtxs}")
        def onTargetPairs = onCtxs.collect { ctx ->
            [ctx: ctx, target: findScopedExprId(ctx.subtree, c)]
        }.findAll { it.target != null && it.ctx.ids.size() == 1 && it.ctx.ids[0] == it.target }
        assertTrue(onTargetPairs.size() == 1,
            "${id}: expected exactly one pruned target agg-child shuffle for ${c}, got on=${onCtxs}")
        def onCtx = onTargetPairs[0].ctx
        def target = onTargetPairs[0].target
        assertTrue(target != null, "${id}: missing exprId for ${c}")
        def offMatches = offCtxs.findAll { it.ids.size() > 1 && it.ids.contains(target) }
        assertTrue(!offMatches.isEmpty(),
            "${id}: expected at least one pre-prune global/distinct-global agg-child shuffle containing ${c}/${target}, got off=${offCtxs}")
        assertTrue(onCtx.ids.size() == 1 && onCtx.ids[0] == target,
            "${id}: expected the target global/distinct-global agg-child shuffle to prune to [${c}]/${target}, got on=${onCtx}")
    }
    def norm = { rows ->
        rows.collect { r -> r.collect { v -> v == null ? "NULL" : v.toString() }.join("||") }.sort()
    }
    def explainText = { String q -> (sql "explain physical plan " + q).collect { it[0].toString() }.join("\n") }
    def runCase = { String id, String q, boolean expectChange, int ap, String selectedCol ->
        sql "set agg_phase=${ap};"
        sql "set enable_shuffle_key_prune=false;"
        def eOff = explainText(q)
        def oS = extractSigns(eOff)
        sql "set enable_shuffle_key_prune=true;"
        def eOn = explainText(q)
        def nS = extractSigns(eOn)
        def changed = (oS.toString() != nS.toString())
        logger.info("${id}: oS=${oS}, nS=${nS}, changed=${changed}")
        if (expectChange) {
            assertTrue(changed, "${id}: shuffle signs should change")
            if (selectedCol != null) {
                assertDistinctShufflePrunedToSingle(id, eOff, eOn, selectedCol)
            }
        } else {
            assertTrue(!changed, "${id}: shuffle signs should stay unchanged")
        }
        sql "set enable_shuffle_key_prune=false;"; def rOff = sql q
        sql "set enable_shuffle_key_prune=true;"; def rOn = sql q
        assertTrue(norm(rOff) == norm(rOn), "${id}: results differ")
    }

    // ===== Table setup =====
    sql "drop table if exists t_09_window_guard;"
    sql "drop table if exists t_09_distinct_window;"
    sql """create table t_09_window_guard (
            id bigint, a bigint, b bigint, c bigint, d bigint, e bigint, f bigint, v bigint
        ) duplicate key(id)
        distributed by hash(a) buckets 4
        properties ("replication_num"="1");"""
    sql """create table t_09_distinct_window (
            id bigint, a bigint, b bigint, c bigint, d bigint, e bigint, f bigint, g bigint, v bigint
        ) duplicate key(id)
        distributed by hash(id) buckets 4
        properties ("replication_num"="1");"""

    sql """insert into t_09_window_guard
        select number, number % 220, number % 3900, number % 3600, number % 4000, number % 3400, number % 120, number
        from numbers("number"="2000");"""
    sql """insert into t_09_distinct_window
        select number,
               number % 220,
               number % 3900,
               number % 3600,
               number % 4000,
               number % 3400,
               number % 3200,
               number % 3000,
               number
        from numbers("number"="2000");"""

    // window guard: child distribution is already satisfied
    ss("t_09_window_guard", "id", "60000", "0", "1600000", "0", "1999", "")
    ss("t_09_window_guard", "a", "220", "0", "1600000", "0", "219", "")
    ss("t_09_window_guard", "b", highNdv, "0", "1600000", "0", "3899", "")
    ss("t_09_window_guard", "c", highNdv, "0", "1600000", "0", "3599", "")
    ss("t_09_window_guard", "d", highNdv, "0", "1600000", "0", "3999", "")
    ss("t_09_window_guard", "e", highNdv, "0", "1600000", "0", "3399", "")
    ss("t_09_window_guard", "f", highNdv, "0", "1600000", "0", "119", "")
    ss("t_09_window_guard", "v", "60000", "0", "1600000", "0", "1999", "")

    def setupDistinctStats = { String goodNdv ->
        ss("t_09_distinct_window", "id", "60000", "0", "1600000", "0", "1999", "")
        ss("t_09_distinct_window", "a", goodNdv, "0", "1600000", "0", "219", "")
        ss("t_09_distinct_window", "b", highNdv, "0", "1600000", "0", "3899", "1 :0.10")
        ss("t_09_distinct_window", "c", highNdv, "0", "1600000", "0", "3599", "1 :0.10")
        ss("t_09_distinct_window", "d", highNdv, "0", "1600000", "0", "3999", "1 :0.10")
        ss("t_09_distinct_window", "e", highNdv, "0", "1600000", "0", "3399", "1 :0.10")
        ss("t_09_distinct_window", "f", highNdv, "0", "1600000", "0", "3199", "1 :0.10")
        ss("t_09_distinct_window", "g", highNdv, "0", "1600000", "0", "2999", "")
        ss("t_09_distinct_window", "v", "60000", "0", "1600000", "0", "1999", "")
    }

    // WD01: window over inner agg, child already satisfied, no pruning
    runCase("WD01", """
        select t.a, t.sum_v,
               row_number() over (partition by t.sum_v order by t.a desc, t.b) as rn
        from (
            select a, b, c, d, e, f, sum(v) as sum_v
            from t_09_window_guard
            group by a, b, c, d, e, f
        ) t
    """, false, 0, null)

    // WD02: count(distinct) + window, high NDV -> prune
    setupDistinctStats(highNdv)
    runCase("WD02", """
        select t.a, t.b, t.cd,
               row_number() over (partition by t.a order by t.cd desc, t.b) as rn
        from (
            select a, b, c, d, e, f, count(distinct g) as cd
            from t_09_distinct_window
            group by a, b, c, d, e, f
        ) t
    """, true, 2, "a")

    // WD03: count(distinct) + avg + window, high NDV -> prune
    setupDistinctStats(highNdv)
    runCase("WD03", """
        select t.a, t.b, t.cd, t.av,
               row_number() over (partition by t.a order by t.cd desc, t.b) as rn
        from (
            select a, b, c, d, e, f, count(distinct g) as cd, avg(v) as av
            from t_09_distinct_window
            group by a, b, c, d, e, f
        ) t
    """, true, 2, "a")

    // WD04: count(distinct) + window, low NDV -> no pruning
    setupDistinctStats(lowNdv)
    runCase("WD04", """
        select t.a, t.b, t.cd,
               row_number() over (partition by t.a order by t.cd desc, t.b) as rn
        from (
            select a, b, c, d, e, f, count(distinct g) as cd
            from t_09_distinct_window
            group by a, b, c, d, e, f
        ) t
    """, false, 2, null)

    sql """set agg_phase=0;"""
    sql """set enable_shuffle_key_prune=true;"""
    sql """set agg_shuffle_use_parent_key=true;"""
    sql """set detail_shape_nodes='';"""
}
