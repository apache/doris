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
// Category 01: Hot-Value Thresholds and Boundary Cases — A01, S01, S02, FC1, B01-B04
// ===========================================================================================
//
// Purpose
//   Validate how Agg Shuffle Key Pruning handles hot_values:
//   1. hot_values='' (empty string) -> means "sampled but no hot values" -> isBalanced()=true -> can be chosen as a shuffle key
//   2. hot_values=null -> means "not sampled" -> conservative fallback, no pruning
//   3. hot_values='value :ratio' -> ratio >= 5% -> isBalanced()=false -> skip the column (treat it as skewed)
//   4. null ratio >= 5% -> isBalanced()=false -> skip the column (treat it as skewed)
//   5. max(null_ratio, hot_ratio) >= 5% -> overall considered skewed
//
// Test Case List
//   A01: hot_key with 30% skew + good_key without hot values -> good_key is selected (positive)
//   S01: hot_values='' empty string -> treated as sampled without hot values -> pruning works (positive)
//   S02: hot_values=null -> treated as not sampled -> conservative fallback, no pruning (negative)
//   FC1: full analyze produces hot_values=null -> no pruning (negative)
//   B01: hot_ratio=4.9% (just below the 5% threshold) -> column is kept and selected as the shuffle key
//   B02: hot_ratio=5.0% (equal to the threshold) -> column is skipped
//   B03: null ratio 10% -> treated as skewed -> column is skipped
//   B04: max(null ratio 10%, hot_ratio 4.9%) = 10% -> treated as skewed -> column is skipped
//
// Validation
//   For each case, compare the orderedShuffledColumns change in explain physical plan between OFF and ON,
//   and verify that OFF/ON execution results are identical.
// ===========================================================================================
suite("hotvalue_boundary", "agg_shuffle_prune_func") {

    sql """set enable_nereids_planner=true;"""
    sql """set enable_fallback_to_original_planner=false;"""
    sql """set enable_sql_cache=false;"""
    sql """set enable_query_cache=false;"""

    def dbName = context.config.getDbNameByFile(context.file)
    sql "create database if not exists ${dbName};"
    sql "use ${dbName};"

    // Fix the number of pipeline tasks to 8 so NDV-threshold calculation stays deterministic
    def tptn = 8
    sql "set parallel_fragment_exec_instance_num=1;"
    sql "set parallel_pipeline_task_num=${tptn};"
    sql "set be_number_for_test=1;"

    // ======== Shared helper functions ========

    // R: manually injected row_count (480k rows, far above the NDV threshold, so the optimizer sees shuffle as necessary)
    def R = "480000"

    // esc: escape backslashes and single quotes in strings for SQL construction
    def esc = { Object v -> v==null?"null":v.toString().replace("\\","\\\\").replace("'","\\'") }

    // ss: manually inject statistics for one column (alter table ... set stats)
    //   t: table name, c: column name, n: NDV, nn: null count, ds: data_size
    //   mi/ma: min/max value, hv: hot_values string
    //   when hv=null, the hot_values field is not injected (to simulate full analyze)
    //   when hv="", inject hot_values='' (meaning "sampled but no hot values")
    //   when hv="val :ratio", inject the specified hot value
    def ss = { String t,String c,String n,String nn,String ds,String mi,String ma,String hv ->
        def hvc = hv==null ? "" : ", 'hot_values'='${esc(hv)}'"
        sql "alter table ${t} modify column ${c} set stats ('row_count'='${R}','ndv'='${esc(n)}','num_nulls'='${esc(nn)}','data_size'='${esc(ds)}','min_value'='${esc(mi)}','max_value'='${esc(ma)}'${hvc});"
    }

    // extractSigns: extract all orderedShuffledColumns lists from explain output
    //   returns values such as ["[1,2,3]", "[4]"] to compare OFF/ON plan changes
    def extractSigns = { String e ->
        def s=[]; def m=(e=~/orderedShuffledColumns=\[([0-9,\s-]*)\]/); while(m.find()){s<<"["+m.group(1).replaceAll("\\s+","")+"]"}; s
    }

    // extractSingle: extract orderedShuffledColumns entries that contain only one column
    //   returns exprId lists, e.g. [4] means only the column with exprId=4 was selected as the single shuffle key
    def extractSingle = { String e ->
        def ids=[]; def m=(e=~/orderedShuffledColumns=\[([0-9,\s-]*)\]/)
        while(m.find()){def r=m.group(1).trim(); if(r.length()==0)continue; def p=r.split(",").collect{it.trim()}.findAll{it.length()>0}; if(p.size()==1)ids<<Integer.parseInt(p[0])}; ids
    }

    // exprId: find the exprId that corresponds to a column name in explain output (e.g. "good_key#7" -> 7)
    def exprId = { String e,String c -> def m=(e=~("\\b"+java.util.regex.Pattern.quote(c)+"#(\\d+)\\b")); m.find()?Integer.parseInt(m.group(1)):null }

    // norm: normalize query results into comparable string lists (handle nulls and compare after sorting)
    def norm = { rows -> rows.collect{r->r.collect{v->v==null?"NULL":v.toString()}.join("||")}.sort() }

    // showRow: get the show column stats row for a column
    def showRow = { String t,String c -> def r=sql("show column stats ${t}(${c})"); assertEquals(1,r.size()); r[0] }

    // run: shared test execution helper
    //   id: case id (such as "A01")
    //   q: SQL under test
    //   chg: whether the plan is expected to change (true=positive/pruning works, false=negative/no pruning)
    //   sel: expected column name selected as the shuffle key (null means do not check)
    //   exc: list of columns expected to be excluded
    //   single: whether pruning is expected to reduce to a single column
    def run = { String id,String q,boolean chg,String sel,List<String> exc,boolean single ->
        sql "set agg_phase=0;"; sql "set disable_nereids_rules='';"
        // First disable pruning to get the original plan
        sql "set enable_shuffle_key_prune=false;"
        def eOff=(sql "explain physical plan "+q).toString(); def oS=extractSigns(eOff)
        // Then enable pruning to get the optimized plan
        sql "set enable_shuffle_key_prune=true;"
        def eOn=(sql "explain physical plan "+q).toString(); def nS=extractSigns(eOn); def si=extractSingle(eOn)
        // Validate whether the plan changes or not
        if(chg){assertTrue(oS.toString()!=nS.toString(),"${id}: signs should change"); if(single)assertTrue(si.size()>0,"${id}: expect single")}
        else{assertTrue(oS.toString()==nS.toString(),"${id}: should stay unchanged")}
        // Validate that the selected shuffle key is correct
        if(sel!=null){def eid=exprId(eOn,sel); assertTrue(eid!=null&&si.contains(eid),"${id}: ${sel} not selected")}
        // Validate that excluded columns do not appear in the shuffle key
        exc.each{c->def eid=exprId(eOn,c); if(eid!=null)assertTrue(!si.contains(eid),"${id}: ${c} excluded")}
        // Validate that OFF/ON query results are identical (functional correctness)
        sql "set enable_shuffle_key_prune=false;"; def rOff=sql q
        sql "set enable_shuffle_key_prune=true;"; def rOn=sql q
        assertTrue(norm(rOff)==norm(rOn),"${id}: results differ")
        logger.info("${id}: oS=${oS}, nS=${nS}, si=${si}")
    }

    // ===== Table setup =====
    // t_01_simple: simple table used for A01/S01/S02
    //   hot_key: the column that will be injected with skew
    //   good_key: a safe column with no hot values, expected to be chosen as the shuffle key
    sql "drop table if exists t_01_simple;"
    sql """create table t_01_simple (x bigint, hot_key int, good_key bigint, v bigint)
        duplicate key(x) distributed by hash(x) buckets 4 properties ("replication_num"="1");"""
    sql """insert into t_01_simple select number*100000+1, if(number<600,1,cast(number%2000 as int)+2),
        (number%6000)*100000+7, number from numbers("number"="2000");"""

    // t_01_boundary: boundary-condition table used for B01-B04
    //   hot_below_key:    hot_ratio=4.9% (just below the 5% threshold)
    //   hot_boundary_key: hot_ratio=5.0% (equal to the 5% threshold)
    //   hot_above_key:    hot_ratio=5.1% (just above the 5% threshold)
    //   null10_key:       null ratio=10% (above the 5% threshold)
    //   maxmix_key:       null ratio 10% + hot_ratio 4.9%, max=10% (above the 5% threshold)
    //   bi_good:          safe bigint column
    //   vc16_good:        safe varchar(16) column
    sql "drop table if exists t_01_boundary;"
    sql """create table t_01_boundary (x bigint, hot_below_key int, hot_boundary_key int, hot_above_key int,
        null10_key int, maxmix_key int, bi_good bigint, vc16_good varchar(16), v bigint)
        duplicate key(x) distributed by hash(x) buckets 4 properties ("replication_num"="1");"""
    sql """insert into t_01_boundary select number*100000+1,
        cast(number%2000 as int)+2, cast(number%2000 as int)+2, cast(number%2000 as int)+2,
        cast(number%2000 as int)+2, cast(number%2000 as int)+2,
        (number%6000)*100000+7, concat('vc_',lpad(cast(number%7000 as string),4,'0')),
        number from numbers("number"="2000");"""

    // ===== A01: hot_key has 30% skew, good_key has no hot values -> good_key is selected =====
    // hot_key: NDV=7000, hot_values='1 :0.30' -> value 1 takes 30%, isBalanced()=false -> skipped
    // good_key: NDV=9000, hot_values='' -> no hot values, isBalanced()=true -> selected
    ss("t_01_simple","x","60000","0","1600000","1","199900001","")
    ss("t_01_simple","hot_key","7000","0","400000","1","7000","1 :0.30")
    ss("t_01_simple","good_key","9000","0","800000","7","599900007","")
    ss("t_01_simple","v","60000","0","1600000","0","1999","")

    run("A01","""select count(hot_key),count(good_key),count(sum_v) from (
        select hot_key,good_key,sum(v) as sum_v from t_01_simple group by hot_key,good_key) t""",
        true,"good_key",["hot_key"],true)

    // ===== S01: hot_values='' -> means "sampled but no hot values" -> pruning works =====
    // hot_key: hot='1 :0.10' -> exactly at the threshold -> skewed -> skipped
    // good_key: hot='' -> sampled without hot values -> balanced -> selected
    ss("t_01_simple","hot_key","7000","0","400000","1","7000","1 :0.10")
    ss("t_01_simple","good_key","9000","0","400000","1","9000","")
    run("S01","""select count(hot_key),count(good_key),count(sum_v) from (
        select hot_key,good_key,sum(v) as sum_v from t_01_simple group by hot_key,good_key) t""",
        true,"good_key",["hot_key"],true)

    // ===== S02: hot_values=null -> means "not sampled" -> conservative fallback, no pruning =====
    // good_key receives hot_values=null (do not inject the hot_values field) -> isBalanced() returns false
    // so the optimizer cannot confirm that good_key is safe -> no optimization
    ss("t_01_simple","hot_key","7000","0","400000","1","7000","1 :0.10")
    ss("t_01_simple","good_key","9000","0","400000","1","9000",null)
    run("S02","""select count(hot_key),count(good_key),count(sum_v) from (
        select hot_key,good_key,sum(v) as sum_v from t_01_simple group by hot_key,good_key) t""",
        false,null,[],false)

    // ===== FC1: full analyze produces hot_values=null -> no pruning =====
    // Full analyze does not generate hot_values, so the hot_values field in show column stats is null.
    // The analyzed dataset is intentionally kept above the NDV threshold, so "hot_values=null" is the blocker,
    // rather than "NDV is too small to prune".
    sql "drop table if exists t_01_full;"
    sql """create table t_01_full (x bigint, skew_key int, good_key int, v bigint)
        duplicate key(x) distributed by hash(x) buckets 4 properties ("replication_num"="1");"""
    sql """insert into t_01_full select number*100000+1, if(number<600,1,cast(number%5000 as int)+2),
        cast(number as int)+1, number from numbers("number"="6000");"""
    sql "analyze table t_01_full with sync;"
    // Verify: hot_values is indeed null after full analyze, and good_key NDV is high enough to be prunable
    def fStats = showRow("t_01_full","skew_key")
    assertEquals("null", fStats[17].toString())
    def goodStats = showRow("t_01_full","good_key")
    assertEquals("null", goodStats[17].toString())
    assertTrue(goodStats[3].toString().toDouble() > tptn * 512,
        "FC1: good_key NDV should exceed the balance threshold, got ${goodStats[3]}")
    run("FC1","""select count(skew_key),count(good_key),count(sum_v) from (
        select skew_key,good_key,sum(v) as sum_v from t_01_full group by skew_key,good_key) t""",
        false,null,[],false)

    // ===== B group: hot_values boundary tests =====
    // Skew rule: skew_ratio = max(null_ratio, max_hot_ratio)
    // A column is considered skewed when skew_ratio >= 5%
    def nullTen = "48000" // 480000*0.10 = 48000 -> null ratio = 10%
    ss("t_01_boundary","x","60000","0","1600000","1","199900001","")
    ss("t_01_boundary","v","60000","0","1600000","0","1999","")
    // hot_below_key:    hot_ratio=4.9% < 5.0% threshold -> balanced=true
    ss("t_01_boundary","hot_below_key","7000","0","420000","1","7000","1 :0.049")
    // hot_boundary_key: hot_ratio=5.0% = 5.0% threshold -> balanced=false (equality does not pass)
    ss("t_01_boundary","hot_boundary_key","7000","0","420000","1","7000","1 :0.05")
    // hot_above_key:    hot_ratio=5.1% > 5.0% threshold -> balanced=false
    ss("t_01_boundary","hot_above_key","7000","0","420000","1","7000","1 :0.051")
    // null10_key:       null count=48000, ratio 10% -> skewed
    ss("t_01_boundary","null10_key","7000",nullTen,"420000","1","7000","")
    // maxmix_key:       null 10% + hot 4.9% -> max(10%, 4.9%) = 10% -> skewed
    ss("t_01_boundary","maxmix_key","7000",nullTen,"420000","1","7000","1 :0.049")
    // bi_good:          bigint, no hot values, NDV=9200 > threshold -> safe column
    ss("t_01_boundary","bi_good","9200","0","840000","1","9200","")
    // vc16_good:        varchar(16), no hot values -> safe column (but lower priority than bigint)
    ss("t_01_boundary","vc16_good","8100","0","1680000","vc_0000","vc_6999","")

    // B01: hot_below(4.9%) is balanced + hot_above(5.1%) is skewed -> choose hot_below_key
    //   hot_below_key is balanced=true, hot_above_key is balanced=false
    //   bigint priority note: although bi_good is bigint, hot_below_key is also balanced and is encountered first
    run("B01","""select hot_below_key,hot_above_key,vc16_good,sum(v) from t_01_boundary
        group by hot_below_key,hot_above_key,vc16_good""",true,"hot_below_key",["hot_above_key"],true)

    // B02: hot_boundary(5.0%) is skewed -> skip it and choose bi_good
    //   5.0% equals the threshold, so isBalanced() returns false
    run("B02","""select hot_boundary_key,bi_good,vc16_good,sum(v) from t_01_boundary
        group by hot_boundary_key,bi_good,vc16_good""",true,"bi_good",["hot_boundary_key"],true)

    // B03: null ratio 10% -> treated as skewed -> skip it and choose bi_good
    //   null_ratio = 48000/480000 = 10% >= 5% -> skewed
    run("B03","""select null10_key,bi_good,vc16_good,sum(v) from t_01_boundary
        group by null10_key,bi_good,vc16_good""",true,"bi_good",["null10_key"],true)

    // B04: max(null 10%, hot 4.9%) = 10% -> overall treated as skewed -> skip it and choose bi_good
    //   Even though hot_ratio does not exceed the threshold, null_ratio already does -> skewed
    run("B04","""select maxmix_key,bi_good,vc16_good,sum(v) from t_01_boundary
        group by maxmix_key,bi_good,vc16_good""",true,"bi_good",["maxmix_key"],true)

}
