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
// Category 06: Join Paths and Parent Requests — J01-J12, P01-P02
// ===========================================================================================
//
// Purpose
//   Validate the interaction between Shuffle Key Pruning and Join operators, and verify the correctness of
//   Parent Requests (where the parent asks the child to adjust its shuffle key to remove extra exchanges).
//
// Core Background
//   - When Agg output is consumed by Join, the Agg shuffle key can be adjusted to a subset of the Join keys,
//     avoiding the re-shuffle exchange before the Join.
//   - tByB = distributed by hash(b) -> if group by includes b, distribution may already be satisfied -> no pruning
//   - tByX = distributed by hash(x) -> x is not in group by -> shuffle is required -> pruning is possible
//
// J Group Case List
//   J01: scan JOIN agg(tByB) -> distribution already satisfied -> no pruning (negative)
//   J02: scan JOIN agg(tByX) -> shuffle required -> pruning works (positive)
//   J03: agg(tByB) JOIN agg(tByB) -> both sides already satisfied -> no pruning (negative)
//   J04: agg(tByX) JOIN agg(tByX) -> both sides require shuffle -> pruning works (positive)
//   J05: LEFT JOIN + agg(tByX) -> same as J02 -> pruning works (positive)
//   J06: LEFT SEMI JOIN -> pruning works (positive)
//   J07: LEFT ANTI JOIN -> pruning works (positive)
//   J08: agg(tByB) JOIN scan on a non-group-by column -> requirement not satisfied -> no pruning (negative)
//   J09: scan JOIN scan -> no agg -> no pruning (negative)
//   J10: agg on join output + all skewed columns -> no safe key -> no pruning (negative)
//   J11: parent-key-subset positive case -> agg shuffle key is adjusted to a subset of join keys
//   J12: parent-key-subset negative case -> low-NDV columns do not qualify -> no adjustment
//
// P Group Case List
//   P01: subquery agg -> parent agg -> inner distribution already satisfied -> no pruning (negative)
//   P02: subquery agg -> window(partition by) -> inner distribution already satisfied -> no pruning (negative)
//
// Validation
//   Use run() to check orderedShuffledColumns changes;
//   use runParent() to check how agg_shuffle_use_parent_key adjusts shuffle keys.
// ===========================================================================================
suite("join_parent", "agg_shuffle_prune_func") {
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
    def ln = "4"
    def R = "480000"
    def esc = { Object v -> v==null?"null":v.toString().replace("\\","\\\\").replace("'","\\'") }
    def ss = { String t,String c,String n,String nn,String ds,String mi,String ma,String hv ->
        def hvc = hv==null ? "" : ", 'hot_values'='${esc(hv)}'"
        sql "alter table ${t} modify column ${c} set stats ('row_count'='${R}','ndv'='${esc(n)}','num_nulls'='${esc(nn)}','data_size'='${esc(ds)}','min_value'='${esc(mi)}','max_value'='${esc(ma)}'${hvc});"
    }
    def extractSigns = { String e ->
        def s=[]; def m=(e=~/orderedShuffledColumns=\[([0-9,\s-]*)\]/); while(m.find()){s<<"["+m.group(1).replaceAll("\\s+","")+"]"}; s
    }
    def extractSingle = { String e ->
        def ids=[]; def m=(e=~/orderedShuffledColumns=\[([0-9,\s-]*)\]/)
        while(m.find()){def r=m.group(1).trim(); if(r.length()==0)continue; def p=r.split(",").collect{it.trim()}.findAll{it.length()>0}; if(p.size()==1)ids<<Integer.parseInt(p[0])}; ids
    }
    def exprId = { String e,String c -> def m=(e=~("\\b"+java.util.regex.Pattern.quote(c)+"#(\\d+)\\b")); m.find()?Integer.parseInt(m.group(1)):null }
    def norm = { rows -> rows.collect{r->r.collect{v->v==null?"NULL":v.toString()}.join("||")}.sort() }
    def parseOrderedIds = { String line ->
        def m = (line =~ /orderedShuffledColumns=\[([0-9,\s-]*)\]/)
        if (!m.find()) {
            return null
        }
        def r = m.group(1).trim()
        return (r.length() == 0) ? Collections.emptyList()
                : r.split(",").collect { it.trim() }.findAll { it.length() > 0 }.collect { Integer.parseInt(it) }
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
    def findScopedExprId = { String e, String c ->
        def m = (e =~ ("\\b" + java.util.regex.Pattern.quote(c) + "#(\\d+)\\b"))
        m.find() ? Integer.parseInt(m.group(1)) : null
    }
    def findGlobalAggChildShuffleCtx = { String e ->
        def lines = e.readLines()
        for (int i = 0; i < lines.size(); i++) {
            def line = lines[i]
            if (!(line.contains("PhysicalHashAggregate") && line.contains("aggPhase=GLOBAL"))) {
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
                    return [ids: parseOrderedIds(childLine), subtree: subtreeText(lines, j)]
                }
            }
        }
        return null
    }
    def assertGlobalAggChildCols = { String id, String e, List<String> cols ->
        def ctx = findGlobalAggChildShuffleCtx(e)
        assertTrue(ctx != null, "${id}: missing agg-side PhysicalDistribute")
        def expectedIds = cols.collect { c ->
            def eid = findScopedExprId(ctx.subtree, c)
            assertTrue(eid != null, "${id}: missing exprId for ${c}")
            eid
        }
        def actualIds = ctx.ids
        assertTrue(actualIds == expectedIds,
            "${id}: expected agg-side shuffle cols ${cols}/${expectedIds}, got ${actualIds}")
    }
    def findAllGlobalAggChildShuffleCtxs = { String e ->
        def lines = e.readLines()
        def allCtxs = []
        for (int i = 0; i < lines.size(); i++) {
            def line = lines[i]
            if (!(line.contains("PhysicalHashAggregate") && line.contains("aggPhase=GLOBAL"))) {
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
                    allCtxs << [ids: parseOrderedIds(childLine), subtree: subtreeText(lines, j)]
                    break
                }
            }
        }
        allCtxs
    }
    def assertGlobalAggShapes = { String id, List<Map> ctxs, List<List<String>> expectedColsByAgg ->
        assertTrue(ctxs.size() == expectedColsByAgg.size(),
            "${id}: expected ${expectedColsByAgg.size()} agg-side shuffles, got ${ctxs.size()} -> ${ctxs.collect { it.ids }}")
        for (int i = 0; i < expectedColsByAgg.size(); i++) {
            def expectedIds = expectedColsByAgg[i].collect { c ->
                def eid = findScopedExprId(ctxs[i].subtree, c)
                assertTrue(eid != null, "${id}: missing exprId for agg${i}.${c}")
                eid
            }
            assertTrue(ctxs[i].ids == expectedIds,
                "${id}: expected agg${i} shuffle cols ${expectedColsByAgg[i]}/${expectedIds}, got ${ctxs[i].ids}")
        }
    }
    def explainText = { String q -> (sql "explain physical plan " + q).collect { it[0].toString() }.join("\n") }
    def run = { String id,String q,int ap,boolean single,boolean reqSh,Boolean expChg,List<List<String>> onExpectedAggCols ->
        sql "set agg_phase=${ap};"; sql "set disable_nereids_rules='';"
        sql "set enable_shuffle_key_prune=false;"
        def eOff=explainText(q); def oS=extractSigns(eOff); def offAggCtxs=findAllGlobalAggChildShuffleCtxs(eOff)
        sql "set enable_shuffle_key_prune=true;"
        def eOn=explainText(q); def nS=extractSigns(eOn); def onAggCtxs=findAllGlobalAggChildShuffleCtxs(eOn)
        def chg=(expChg==null?single:expChg)
        if(chg){
            assertTrue(oS.toString()!=nS.toString(),"${id}: signs should change")
            if(single && onExpectedAggCols==null){
                assertTrue(onAggCtxs.any{it.ids.size()==1}, "${id}: expect at least one single-key agg-side shuffle, got ${onAggCtxs.collect { it.ids }}")
            }
        } else {
            assertTrue(oS.toString()==nS.toString(),"${id}: should stay unchanged")
        }
        if(onExpectedAggCols!=null){
            assertGlobalAggShapes(id + "-ON", onAggCtxs, onExpectedAggCols)
        }
        if(reqSh){
            assertTrue(!onAggCtxs.isEmpty() || nS.size()>0,"${id}: need shuffle")
        }
        sql "set enable_shuffle_key_prune=false;"; def rOff=sql q
        sql "set enable_shuffle_key_prune=true;"; def rOn=sql q
        assertTrue(norm(rOff)==norm(rOn),"${id}: results differ")
        logger.info("${id}: oS=${oS}, nS=${nS}, offAggIds=${offAggCtxs.collect { it.ids }}, onAggIds=${onAggCtxs.collect { it.ids }}")
    }
    def runParent = { String id,String q,boolean chg,List<String> offCols,List<String> onCols ->
        sql "set agg_phase=0;"; sql "set disable_nereids_rules='';"
        sql "set enable_shuffle_key_prune=false;"; sql "set disable_join_reorder=true;"; sql "set runtime_filter_mode=OFF;"
        sql "set agg_shuffle_use_parent_key=false;"
        def eOff=explainText(q); def oS=extractSigns(eOff)
        sql "set agg_shuffle_use_parent_key=true;"
        def eOn=explainText(q); def nS=extractSigns(eOn)
        assertTrue(nS.size()>0,"${id}: need shuffle")
        if(offCols!=null) assertGlobalAggChildCols(id + "-OFF", eOff, offCols)
        if(onCols!=null) assertGlobalAggChildCols(id + "-ON", eOn, onCols)
        if(chg){assertTrue(oS.toString()!=nS.toString(),"${id}: signs should change")}
        else{assertTrue(oS.toString()==nS.toString(),"${id}: should stay unchanged")}
        sql "set agg_shuffle_use_parent_key=false;"; def rOff=sql q
        sql "set agg_shuffle_use_parent_key=true;"; def rOn=sql q
        assertTrue(norm(rOff)==norm(rOn),"${id}: results differ")
        logger.info("${id}: oS=${oS}, nS=${nS}")
        sql "set agg_shuffle_use_parent_key=true;"; sql "set runtime_filter_mode=GLOBAL;"; sql "set disable_join_reorder=false;"
    }

    // ===== Table setup =====
    def tByX="t_06_byX"; def tByB="t_06_byB"; def tScan="t_06_scan"
    def tScanHot="t_06_scan_hot"; def tScanLow="t_06_scan_low"
    [tByX,tByB,tScan,tScanHot,tScanLow].each{sql "drop table if exists ${it};"}

    sql """create table ${tByX} (x bigint,b bigint,c bigint,d bigint,e bigint,f bigint,g bigint,v bigint,s_key string,null_k bigint)
        duplicate key(x) distributed by hash(x) buckets 4 properties ("replication_num"="1");"""
    sql """create table ${tByB} (x bigint,b bigint,c bigint,d bigint,e bigint,f bigint,g bigint,v bigint,s_key string,null_k bigint)
        duplicate key(x) distributed by hash(b) buckets 4 properties ("replication_num"="1");"""
    sql """create table ${tScan} (b bigint,c bigint,d bigint,e bigint,f bigint,g bigint,rv bigint)
        duplicate key(b) distributed by hash(b) buckets 4 properties ("replication_num"="1");"""
    sql """create table ${tScanHot} (b bigint,c bigint,d bigint,e bigint,f bigint,g bigint,rv bigint)
        duplicate key(b) distributed by hash(rv) buckets 4 properties ("replication_num"="1");"""
    sql """create table ${tScanLow} (b bigint,c bigint,d bigint,e bigint,f bigint,g bigint,rv bigint)
        duplicate key(b) distributed by hash(rv) buckets 4 properties ("replication_num"="1");"""

    sql """insert into ${tByX} select
        (bitand(murmur_hash3_32(concat(cast(number as string),'_x')),2147483647)%4500)*100000+1,
        (bitand(murmur_hash3_32(concat(cast(number as string),'_b')),2147483647)%220)*100000+2,
        (bitand(murmur_hash3_32(concat(cast(number as string),'_c')),2147483647)%3900)*100000+3,
        (bitand(murmur_hash3_32(concat(cast(number as string),'_d')),2147483647)%3600)*100000+4,
        (bitand(murmur_hash3_32(concat(cast(number as string),'_e')),2147483647)%4000)*100000+5,
        (bitand(murmur_hash3_32(concat(cast(number as string),'_f')),2147483647)%3400)*100000+6,
        (bitand(murmur_hash3_32(concat(cast(number as string),'_g')),2147483647)%120)*100000+7,
        number,concat('s_',cast(number%3200 as string)),if(number%10<8,null,number%2800+1)
        from numbers("number"="2000");"""
    sql "insert into ${tByB} select * from ${tByX};"
    sql "insert into ${tScan} select b,c,d,e,f,g,v from ${tByX};"
    sql "insert into ${tScanHot} select * from ${tScan};"
    sql "insert into ${tScanLow} select * from ${tScan};"

    // stats injection
    def wideStats = { String t ->
        ss(t,"x","4500","0","1600000","1","449900001","")
        ss(t,"b","220","0","1600000","2","21900002","")
        ss(t,"c",qn,"0","1600000","3","389900003","")
        ss(t,"d",qn,"0","1600000","4","359900004","")
        ss(t,"e",qn,"0","1600000","5","399900005","")
        ss(t,"f",qn,"0","1600000","6","339900006","")
        ss(t,"g",qn,"0","1600000","7","11900007","")
        ss(t,"v","60000","0","1600000","0","1999","")
        ss(t,"s_key","3200","0","3200000","s_0","s_3199","")
        ss(t,"null_k","2800","16000","1600000","1","2800","")
    }
    wideStats(tByX); wideStats(tByB)
    def scanStats = { String t,String cn,String dn,String en,String fn,String gn ->
        ss(t,"b","220","0","1600000","2","21900002","")
        ss(t,"c",cn,"0","1600000","3","389900003","")
        ss(t,"d",dn,"0","1600000","4","359900004","")
        ss(t,"e",en,"0","1600000","5","399900005","")
        ss(t,"f",fn,"0","1600000","6","339900006","")
        ss(t,"g",gn,"0","1600000","7","11900007","")
        ss(t,"rv","60000","0","1600000","0","1999","")
    }
    scanStats(tScan,qn,qn,qn,qn,qn)
    scanStats(tScanHot,qn,qn,qn,qn,qn)
    scanStats(tScanLow,ln,ln,ln,qn,qn)

    // J01-J09
    run("J01","select s.b,s.c,s.rv,a.sum_v from ${tScan} s join (select b,c,d,e,f,g,sum(v) as sum_v from ${tByB} group by b,c,d,e,f,g) a on s.b=a.b and s.c=a.c and s.d=a.d and s.e=a.e and s.f=a.f and s.g=a.g",0,false,false,false,null)
    run("J02","select s.b,s.c,s.rv,a.sum_v from ${tScan} s join (select b,c,d,e,f,g,sum(v) as sum_v from ${tByX} group by b,c,d,e,f,g) a on s.b=a.b and s.c=a.c and s.d=a.d and s.e=a.e and s.f=a.f and s.g=a.g",0,true,true,true,[["c"]])
    run("J03","select l.b,l.c,l.d,l.e,l.f,l.g,l.sv,r.cv from (select b,c,d,e,f,g,sum(v) as sv from ${tByB} group by b,c,d,e,f,g) l join (select b,c,d,e,f,g,count(*) as cv from ${tByB} group by b,c,d,e,f,g) r on l.b=r.b and l.c=r.c and l.d=r.d and l.e=r.e and l.f=r.f and l.g=r.g",0,false,false,false,null)
    run("J04","select l.b,l.c,l.d,l.e,l.f,l.g,l.sv,r.cv from (select b,c,d,e,f,g,sum(v) as sv from ${tByX} group by b,c,d,e,f,g) l join (select b,c,d,e,f,g,count(*) as cv from ${tByX} group by b,c,d,e,f,g) r on l.b=r.b and l.c=r.c and l.d=r.d and l.e=r.e and l.f=r.f and l.g=r.g",0,true,true,true,[["c"],["c"]])
    run("J05","select s.b,s.c,s.rv,a.sum_v from ${tScan} s left join (select b,c,d,e,f,g,sum(v) as sum_v from ${tByX} group by b,c,d,e,f,g) a on s.b=a.b and s.c=a.c and s.d=a.d and s.e=a.e and s.f=a.f and s.g=a.g",0,true,true,true,[["c"]])
    run("J06","select s.b,s.c,s.rv from ${tScan} s left semi join (select b,c,d,e,f,g,sum(v) as sum_v from ${tByX} group by b,c,d,e,f,g) a on s.b=a.b and s.c=a.c and s.d=a.d and s.e=a.e and s.f=a.f and s.g=a.g",0,true,true,true,[["c"]])
    run("J07","select s.b,s.c,s.rv from ${tScan} s left anti join (select b,c,d,e,f,g,sum(v) as sum_v from ${tByX} group by b,c,d,e,f,g) a on s.b=a.b and s.c=a.c and s.d=a.d and s.e=a.e and s.f=a.f and s.g=a.g",0,true,true,true,[["c"]])
    run("J08","select a.sum_v,s.rv from (select b,c,d,e,f,g,sum(v) as sum_v from ${tByB} group by b,c,d,e,f,g) a join ${tScan} s on a.sum_v=s.rv",0,false,false,false,null)
    run("J09","select j.b,j.c,j.d,j.e,j.f,j.g,count(*) as cv from (select l.b,l.c,l.d,l.e,l.f,l.g from ${tScan} l join ${tScan} r on l.b=r.b and l.c=r.c and l.d=r.d and l.e=r.e and l.f=r.f and l.g=r.g) j group by j.b,j.c,j.d,j.e,j.f,j.g",0,false,false,false,null)

    // J10: root agg on join output, temporarily inject hot_values into all columns -> block agg self-pruning -> negative
    ["c","d","e","f","g"].each{c-> ss(tScan,c,qn,"0","1600000","3","389900003","1 :0.30") }
    run("J10","select j.c,j.d,j.e,j.f,j.g,count(*) as cv from (select l.b,l.c,l.d,l.e,l.f,l.g from ${tScan} l join ${tScan} r on l.b=r.b and l.c=r.c and l.d=r.d and l.e=r.e and l.f=r.f and l.g=r.g) j group by j.c,j.d,j.e,j.f,j.g",0,false,true,false,null)
    scanStats(tScan,qn,qn,qn,qn,qn) // restore

    // J11: parent-key-subset positive case
    runParent("J11","select t.c,t.d,t.e,t.cv,s.rv from (select b,c,d,e,f,g,count(*) as cv from ${tScanHot} group by b,c,d,e,f,g) t join [shuffle] ${tScanHot} s on t.c=s.c and t.d=s.d and t.e=s.e",true,["b","c","d","e","f","g"],["c","d","e"])
    // J12: parent-key-subset low-NDV negative case
    runParent("J12","select t.c,t.d,t.e,t.cv,s.rv from (select b,c,d,e,f,g,count(*) as cv from ${tScanLow} group by b,c,d,e,f,g) t join [shuffle] ${tScanLow} s on t.c=s.c and t.d=s.d and t.e=s.e",false,["b","c","d","e","f","g"],["b","c","d","e","f","g"])

    // P01/P02
    run("P01","select t.sv,max(t.cv) from (select b,c,d,e,f,g,sum(v) as sv,count(*) as cv from ${tByB} group by b,c,d,e,f,g) t group by t.sv",0,false,false,false,null)
    run("P02","select t.b,t.sv,row_number() over (partition by t.sv order by t.b desc,t.c) as rn from (select b,c,d,e,f,g,sum(v) as sv from ${tByB} group by b,c,d,e,f,g) t",0,false,false,false,null)
}
