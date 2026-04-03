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
    def exprId = { String e, String c ->
        def m = (e =~ ("\\b" + java.util.regex.Pattern.quote(c) + "#(\\d+)\\b"))
        m.find() ? Integer.parseInt(m.group(1)) : null
    }
    def assertSelected = { String id, String e, String c ->
        def ids = extractSingle(e)
        def target = exprId(e, c)
        assertTrue(target != null, "${id}: missing exprId for ${c}")
        assertTrue(ids.contains(target), "${id}: expected ${c} as single shuffle key, but ids=${ids}")
    }
    def norm = { rows ->
        rows.collect { r -> r.collect { v -> v == null ? "NULL" : v.toString() }.join("||") }.sort()
    }
    def runCase = { String id, String q, boolean expectChange, int ap, String selectedCol ->
        sql "set agg_phase=${ap};"
        sql "set enable_shuffle_key_prune=false;"
        def eOff = (sql "explain physical plan " + q).toString()
        def oS = extractSigns(eOff)
        sql "set enable_shuffle_key_prune=true;"
        def eOn = (sql "explain physical plan " + q).toString()
        def nS = extractSigns(eOn)
        def changed = (eOff != eOn)
        logger.info("${id}: oS=${oS}, nS=${nS}, changed=${changed}")
        if (expectChange) {
            assertTrue(changed, "${id}: plan should change")
            if (selectedCol != null) {
                assertSelected(id, eOn, selectedCol)
            }
        } else {
            assertTrue(!changed, "${id}: plan should stay unchanged")
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
}
