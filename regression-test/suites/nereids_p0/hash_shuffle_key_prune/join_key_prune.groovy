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
// Category 07: Pure Join Shuffle Key Optimization — JK01-JK08
// ===========================================================================================
//
// Purpose
//   Validate pure Join shuffle-key optimization outside Agg scenarios.
//   When two tables use Shuffle Hash Join and the join key contains multiple columns,
//   pruning can reduce the number of columns participating in hash computation and lower shuffle cost.
//
// Note
//   Join shuffle-key pruning is cost-based, but we keep the environment deterministic with fixed statistics
//   and be_number_for_test=1, then use hard assertions for validation.
//
// Case List
//   JK01: All join keys on both sides have high NDV + no skew -> can be pruned (positive)
//   JK02: All join keys are skewed, but step 2 still removes the string key when numeric combined NDV is high -> pruning works (positive)
//   JK03: Numeric columns are skewed while varchar is balanced -> single-key selection can still prune on varchar (positive)
//   JK04: Left table lacks statistics -> conservative -> no pruning (negative)
//   JK05: Left table has hot_values=null -> not sampled -> conservative -> no pruning (negative)
//   JK06: Verify the switch works -> OFF keeps the full join shuffle key, ON can prune
//   JK07: join [shuffle[skew(...)] ] hint -> explicit skew guard -> no pruning (negative)
//   JK08: All keys are skewed and numeric combined NDV is too low for step 2 -> no pruning (negative)
//
// Validation
//   Fix join reorder and turn off runtime filters to keep the plan stable.
//   Compare OFF/ON plan changes and result consistency.
// ===========================================================================================
suite("join_key_prune", "agg_shuffle_prune_func") {
    sql """set enable_nereids_planner=true;"""
    sql """set enable_fallback_to_original_planner=false;"""
    sql """set enable_sql_cache=false;"""
    sql """set enable_query_cache=false;"""
    sql """set agg_phase=0;"""

    def dbName = context.config.getDbNameByFile(context.file)
    sql "create database if not exists ${dbName};"
    sql "use ${dbName};"

    def tptn = 8
    sql "set parallel_fragment_exec_instance_num=1;"
    sql "set parallel_pipeline_task_num=${tptn};"
    sql "set be_number_for_test=1;"

    def qn = (tptn*512 + tptn*64).toString()
    def R = "480000"
    def esc = { Object v -> v==null?"null":v.toString().replace("\\","\\\\").replace("'","\\'") }
    def ss = { String t,String c,String n,String nn,String ds,String mi,String ma,String hv ->
        def hvc = hv==null ? "" : ", 'hot_values'='${esc(hv)}'"
        sql "alter table ${t} modify column ${c} set stats ('row_count'='${R}','ndv'='${esc(n)}','num_nulls'='${esc(nn)}','data_size'='${esc(ds)}','min_value'='${esc(mi)}','max_value'='${esc(ma)}'${hvc});"
    }
    def extractSigns = { String e ->
        def s=[]; def m=(e=~/orderedShuffledColumns=\[([0-9,\s-]*)\]/); while(m.find()){s<<"["+m.group(1).replaceAll("\\s+","")+"]"}; s
    }
    def extractKeyCounts = { String e ->
        def c=[]; def m=(e=~/orderedShuffledColumns=\[([0-9,\s-]*)\]/)
        while(m.find()){
            def r=m.group(1).trim()
            def p=r.length()==0 ? [] : r.split(",").collect{it.trim()}.findAll{it.length()>0}
            c<<p.size()
        }
        c
    }
    def parseOrderedIds = { String line ->
        def m = (line =~ /orderedShuffledColumns=\[([0-9,\s-]*)\]/)
        if (!m.find()) {
            return null
        }
        def r = m.group(1).trim()
        return (r.length()==0 ? [] : r.split(",").collect{it.trim()}.findAll{it.length()>0}.collect{Integer.parseInt(it)})
    }
    def subtreeText = { List<String> lines, int rootIndex, int stopIndent ->
        int rootIndent = lines[rootIndex].indexOf("Physical")
        def collected = []
        for (int i = rootIndex; i < lines.size(); i++) {
            def line = lines[i]
            int indent = line.indexOf("Physical")
            if (i > rootIndex && indent >= 0 && indent <= stopIndent) {
                break
            }
            collected << line
        }
        collected.join("\n")
    }
    def findScopedExprId = { String e,String c ->
        def m=(e=~("\\b"+java.util.regex.Pattern.quote(c)+"#(\\d+)\\b"))
        m.find()?Integer.parseInt(m.group(1)):null
    }
    def findFirstJoinChildShuffleCtxs = { String e ->
        def lines = e.readLines()
        int joinIndex = -1
        for (int i = 0; i < lines.size(); i++) {
            if (lines[i].contains("PhysicalHashJoin")) {
                joinIndex = i
                break
            }
        }
        if (joinIndex < 0) {
            return null
        }
        int joinIndent = lines[joinIndex].indexOf("PhysicalHashJoin")
        def childRootIndices = []
        Integer childRootIndent = null
        for (int i = joinIndex + 1; i < lines.size(); i++) {
            def line = lines[i]
            int indent = line.indexOf("Physical")
            if (indent < 0) {
                continue
            }
            if (indent <= joinIndent) {
                break
            }
            if (childRootIndent == null) {
                childRootIndent = indent
            }
            if (indent == childRootIndent) {
                childRootIndices << i
            }
        }
        assertTrue(childRootIndices.size() >= 2, "missing join child roots in plan:\n${e}")
        def childCtxs = childRootIndices.take(2).collect { idx ->
            def childText = subtreeText(lines, idx, joinIndent)
            def childLines = childText.readLines()
            def distLine = childLines.find { it.contains("PhysicalDistribute") && it.contains("orderedShuffledColumns=") }
            assertTrue(distLine != null, "missing distribute under join child:\n${childText}")
            [ids: parseOrderedIds(distLine), subtree: childText]
        }
        [left: childCtxs[0], right: childCtxs[1]]
    }
    def assertJoinChildCols = { String id, Map ctx, List<String> cols, String side ->
        def expectedIds = cols.collect { c ->
            def eid = findScopedExprId(ctx.subtree, c)
            assertTrue(eid != null, "${id}: missing exprId for ${side}.${c}")
            eid
        }
        assertTrue(ctx.ids.size() == expectedIds.size() && ctx.ids == expectedIds,
            "${id}: expected ${side} shuffle cols ${cols}/${expectedIds}, got ${ctx.ids}")
    }
    def norm = { rows -> rows.collect{r->r.collect{v->v==null?"NULL":v.toString()}.join("||")}.sort() }
    def explainText = { String q -> (sql "explain physical plan " + q).collect { it[0].toString() }.join("\n") }

    // Join shuffle-key prune test helper
    def runJoin = { String id,String q,boolean expChg,List<String> offLeftCols,List<String> offRightCols,List<String> onLeftCols,List<String> onRightCols ->
        sql "set enable_shuffle_key_prune=false;"
        def eOff=explainText(q); def oS=extractSigns(eOff); def offCtx=findFirstJoinChildShuffleCtxs(eOff)
        sql "set enable_shuffle_key_prune=true;"
        def eOn=explainText(q); def nS=extractSigns(eOn); def onCtx=findFirstJoinChildShuffleCtxs(eOn)
        def changed=(eOff!=eOn)
        logger.info("${id}: oS=${oS}, nS=${nS}, offCtx=${offCtx}, onCtx=${onCtx}, changed=${changed}")
        assertJoinChildCols(id + "-OFF", offCtx.left, offLeftCols, "left")
        assertJoinChildCols(id + "-OFF", offCtx.right, offRightCols, "right")
        assertJoinChildCols(id + "-ON", onCtx.left, onLeftCols, "left")
        assertJoinChildCols(id + "-ON", onCtx.right, onRightCols, "right")
        if(expChg){assertTrue(changed,"${id}: signs should change")}
        else{assertTrue(oS.toString()==nS.toString(),"${id}: should stay unchanged")}
        sql "set enable_shuffle_key_prune=false;"; def rOff=sql q
        sql "set enable_shuffle_key_prune=true;"; def rOn=sql q
        assertTrue(norm(rOff)==norm(rOn),"${id}: results differ")
    }
    def runJoinSizesStable = { String id,String q ->
        sql "set enable_shuffle_key_prune=false;"
        def eOff=explainText(q); def offCtx=findFirstJoinChildShuffleCtxs(eOff)
        sql "set enable_shuffle_key_prune=true;"
        def eOn=explainText(q); def onCtx=findFirstJoinChildShuffleCtxs(eOn)
        def offSizes = [offCtx.left.ids.size(), offCtx.right.ids.size()]
        def onSizes = [onCtx.left.ids.size(), onCtx.right.ids.size()]
        logger.info("${id}: offSizes=${offSizes}, onSizes=${onSizes}, offCtx=${offCtx}, onCtx=${onCtx}")
        assertTrue(offSizes == onSizes, "${id}: shuffle-key sizes should stay unchanged, off=${offSizes}, on=${onSizes}")
        sql "set enable_shuffle_key_prune=false;"; def rOff=sql q
        sql "set enable_shuffle_key_prune=true;"; def rOn=sql q
        assertTrue(norm(rOff)==norm(rOn),"${id}: results differ")
    }

    // ===== Table setup =====
    sql "drop table if exists t_07_left;"
    sql "drop table if exists t_07_right;"
    sql "drop table if exists t_07_no_stats;"
    sql """create table t_07_left (id bigint,a bigint,b bigint,c bigint,d bigint,e bigint,f varchar(32),v bigint)
        duplicate key(id) distributed by hash(id) buckets 4 properties ("replication_num"="1");"""
    sql """create table t_07_right (id bigint,a bigint,b bigint,c bigint,d bigint,e bigint,f varchar(32),v bigint)
        duplicate key(id) distributed by hash(id) buckets 4 properties ("replication_num"="1");"""
    sql """create table t_07_no_stats (id bigint,a bigint,b bigint,c bigint,d bigint,e bigint,f varchar(32),v bigint)
        duplicate key(id) distributed by hash(id) buckets 4 properties ("replication_num"="1");"""
    sql """insert into t_07_left select number,number%5000,number%4800,number%4600,
        number%4400,number%4200,concat('s',cast(number%4000 as string)),number from numbers("number"="2000");"""
    sql "insert into t_07_right select * from t_07_left;"
    sql "insert into t_07_no_stats select * from t_07_left;"

    def baseStats = { String t ->
        ss(t,"id","60000","0","1600000","0","1999","")
        ss(t,"a","5000","0","1600000","0","4999","")
        ss(t,"b","4800","0","1600000","0","4799","")
        ss(t,"c","4600","0","1600000","0","4599","")
        ss(t,"d","4400","0","1600000","0","4399","")
        ss(t,"e","4200","0","1600000","0","4199","")
        ss(t,"f","4000","0","3200000","s0","s3999","")
        ss(t,"v","60000","0","1600000","0","1999","")
    }
    baseStats("t_07_left"); baseStats("t_07_right")

    // Fix join order and disable runtime filters to keep the plan structure stable
    sql "set disable_join_reorder=true;"
    sql "set runtime_filter_mode=OFF;"

    def jkSql = """select l.a,l.v,r.v from t_07_left l join t_07_right r
        on l.a=r.a and l.b=r.b and l.c=r.c and l.d=r.d and l.e=r.e and l.f=r.f"""

    // JK01: high NDV without skew -> positive
    ["a","b","c","d","e","f"].each{c-> ss("t_07_left",c,qn,"0","1600000","0","9999",""); ss("t_07_right",c,qn,"0","1600000","0","9999","")}
    runJoin("JK01",jkSql,true,["a","b","c","d","e","f"],["a","b","c","d","e","f"],["a"],["a"])

    // JK02: all keys are skewed, but step 2 still prunes the string key because the numeric combined NDV is high
    ["a","b","c","d","e"].each{c-> ss("t_07_left",c,qn,"0","1600000","0","9999","1 :0.06"); ss("t_07_right",c,qn,"0","1600000","0","9999","1 :0.06")}
    ss("t_07_left","f",qn,"0","3200000","s0","s3999","x :0.06"); ss("t_07_right","f",qn,"0","3200000","s0","s3999","x :0.06")
    runJoin("JK02",jkSql,true,["a","b","c","d","e","f"],["a","b","c","d","e","f"],["a","b","c","d","e"],["a","b","c","d","e"])

    // JK03: numeric columns are skewed while varchar is balanced -> pruning can still reduce the join key set
    ["a","b","c","d","e"].each{c-> ss("t_07_left",c,qn,"0","1600000","0","9999","1 :0.06"); ss("t_07_right",c,qn,"0","1600000","0","9999","1 :0.06")}
    ss("t_07_left","f",qn,"0","3200000","s0","s3999",""); ss("t_07_right","f",qn,"0","3200000","s0","s3999","")
    runJoin("JK03",jkSql,true,["a","b","c","d","e","f"],["a","b","c","d","e","f"],["f"],["f"])

    // JK08: all keys are skewed and the numeric combined NDV is too low, so step 2 also fails
    ["a","b","c","d","e"].each{c-> ss("t_07_left",c,"4","0","1600000","0","3","1 :0.06"); ss("t_07_right",c,"4","0","1600000","0","3","1 :0.06")}
    ss("t_07_left","f","4","0","3200000","s0","s3","x :0.06"); ss("t_07_right","f","4","0","3200000","s0","s3","x :0.06")
    runJoin("JK08",jkSql,false,["a","b","c","d","e","f"],["a","b","c","d","e","f"],["a","b","c","d","e","f"],["a","b","c","d","e","f"])

    // JK04: missing stats -> negative
    runJoin("JK04","select l.a,l.v,r.v from t_07_no_stats l join [shuffle] t_07_right r on l.a=r.a and l.b=r.b and l.c=r.c and l.d=r.d and l.e=r.e and l.f=r.f",false,
        ["a","b","c","d","e","f"],["a","b","c","d","e","f"],["a","b","c","d","e","f"],["a","b","c","d","e","f"])

    // JK05: hotValues=null -> negative
    ["a","b","c","d","e"].each{c-> ss("t_07_left",c,qn,"0","1600000","0","9999",null)}
    ss("t_07_left","f",qn,"0","3200000","s0","s3999",null)
    ["a","b","c","d","e","f"].each{c-> ss("t_07_right",c,qn,"0","1600000","0","9999","")}
    runJoin("JK05",jkSql,false,["a","b","c","d","e","f"],["a","b","c","d","e","f"],["a","b","c","d","e","f"],["a","b","c","d","e","f"])

    // JK06: switch-effect validation
    // Scenario: all join keys on both sides have high NDV + no skew (same stats as JK01),
    //           which is the most typical "prunable" case.
    //           Validate that OFF keeps the full 6-column shuffle key while ON prunes some columns.
    // Steps:
    //   1) Reset statistics on both sides (JK05 polluted the left-side stats to hot_values=null)
    //   2) Reuse runJoin to validate plan changes + result consistency
    //   3) Additionally check key counts: OFF keeps all 6 keys, ON uses fewer than 6

    // Step 1: reset both tables' stats — high NDV + empty hot_values = no skew
    ["a","b","c","d","e"].each{c->
        ss("t_07_left", c, qn, "0", "1600000", "0", "9999", "")
        ss("t_07_right", c, qn, "0", "1600000", "0", "9999", "")
    }
    ss("t_07_left", "f", qn, "0", "3200000", "s0", "s3999", "")
    ss("t_07_right", "f", qn, "0", "3200000", "s0", "s3999", "")

    // Step 2: reuse runJoin to validate plan changes + result consistency (expChg=true means we expect a change)
    runJoin("JK06", jkSql, true,["a","b","c","d","e","f"],["a","b","c","d","e","f"],["a"],["a"])

    // JK07: skew join hint should block join shuffle key prune.
    // The skew hint may add an internal exprId to the distribute key, so compare sizes like the FE unit test does.
    runJoinSizesStable("JK07", """select l.a,l.v,r.v from t_07_left l join [shuffle[skew(l.a(1,2))]] t_07_right r
        on l.a=r.a and l.b=r.b and l.c=r.c and l.d=r.d and l.e=r.e and l.f=r.f""")

    sql "set enable_shuffle_key_prune=true;"
    sql "set disable_join_reorder=false;"
    sql "set runtime_filter_mode=GLOBAL;"
}
