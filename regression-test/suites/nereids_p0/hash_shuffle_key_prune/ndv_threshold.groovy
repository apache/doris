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
// Category 02: NDV Threshold — C01-C03
// ===========================================================================================
//
// Purpose
//   Validate the NDV (Number of Distinct Values) threshold logic of Shuffle Key Pruning:
//   only columns with sufficiently large NDV can be chosen as shuffle keys; otherwise the data distribution
//   is too concentrated and may cause hash skew.
//
// NDV Threshold Formula
//   ndvThreshold = totalPipelineTaskNum * 512
//   where totalPipelineTaskNum = parallel_pipeline_task_num (fixed to 8 here)
//   so ndvThreshold = 8 * 512 = 4096
//
// Case List
//   C01: NDV = threshold - 1 (just below the threshold) -> candidate column is skipped and falls back to bi_good (overall pruning still happens)
//   C02: NDV = threshold (equal to the threshold) -> candidate column is skipped and falls back to bi_good (overall pruning still happens; equality does not pass)
//   C03: NDV > threshold (above the threshold) -> the column can be selected (positive)
//
// Validation
//   Inject different NDV values into the target columns, check whether orderedShuffledColumns changes,
//   and verify that skipped/selected columns match expectations.
// ===========================================================================================
suite("ndv_threshold", "agg_shuffle_prune_func") {
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

    // NDV threshold = pipeline_task_num * 512 = 4096
    // Only columns with NDV > ndvThreshold are allowed to be selected as shuffle keys
    def ndvThreshold = tptn * 512
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
    def run = { String id,String q,boolean chg,String sel,List<String> exc,boolean single ->
        sql "set agg_phase=0;"; sql "set disable_nereids_rules='';"
        sql "set enable_shuffle_key_prune=false;"
        def eOff=(sql "explain physical plan "+q).toString(); def oS=extractSigns(eOff)
        sql "set enable_shuffle_key_prune=true;"
        def eOn=(sql "explain physical plan "+q).toString(); def nS=extractSigns(eOn); def si=extractSingle(eOn)
        if(chg){assertTrue(oS.toString()!=nS.toString(),"${id}: signs should change"); if(single)assertTrue(si.size()>0,"${id}: expect single")}
        else{assertTrue(oS.toString()==nS.toString(),"${id}: should stay unchanged")}
        if(sel!=null){def eid=exprId(eOn,sel); assertTrue(eid!=null&&si.contains(eid),"${id}: ${sel} not selected")}
        exc.each{c->def eid=exprId(eOn,c); if(eid!=null)assertTrue(!si.contains(eid),"${id}: ${c} excluded")}
        sql "set enable_shuffle_key_prune=false;"; def rOff=sql q
        sql "set enable_shuffle_key_prune=true;"; def rOn=sql q
        assertTrue(norm(rOff)==norm(rOn),"${id}: results differ")
        logger.info("${id}: oS=${oS}, nS=${nS}, si=${si}")
    }

    // ===== Table setup =====
    // The table contains columns with three NDV levels: below / equal / above threshold,
    // plus one stable balanced column bi_good
    sql "drop table if exists t_02_ndv;"
    sql """create table t_02_ndv (x bigint, ndv_below_key int, ndv_eq_key int, ndv_above_key int,
        ratio8_key int, bi_good bigint, v bigint)
        duplicate key(x) distributed by hash(x) buckets 4 properties ("replication_num"="1");"""
    sql """insert into t_02_ndv select number*100000+1,
        cast(number%5000 as int)+1, cast(number%5000 as int)+1, cast(number%6000 as int)+1,
        cast(number%9000 as int)+1, (number%6000)*100000+7,
        number from numbers("number"="2000");"""

    // ndvBelow = 4095 (just below the 4096 threshold)
    def ndvBelow = Math.max(1, ndvThreshold - 1)
    // ndvAbove = 4096 + 1024 = 5120 (clearly above the threshold)
    def ndvAbove = ndvThreshold + tptn * 128

    ss("t_02_ndv","x","60000","0","1600000","1","199900001","")
    ss("t_02_ndv","v","60000","0","1600000","0","1999","")
    ss("t_02_ndv","ndv_below_key","${ndvBelow}","0","420000","1","${ndvBelow}","")
    ss("t_02_ndv","ndv_eq_key","${ndvThreshold}","0","420000","1","${ndvThreshold}","")
    ss("t_02_ndv","ndv_above_key","${ndvAbove}","0","420000","1","${ndvAbove}","")
    ss("t_02_ndv","ratio8_key","3000","0","420000","1","3000","")
    ss("t_02_ndv","bi_good","9200","0","840000","1","9200","")

    // C01: ndv_below_key has NDV=4095 < threshold 4096 -> does not meet the NDV threshold -> skipped
    //   bi_good has NDV=9200 > threshold -> qualifies -> selected as the shuffle key
    //   Expectation: plan changes, bi_good is selected, ndv_below_key is excluded
    run("C01","select ndv_below_key,bi_good,sum(v) from t_02_ndv group by ndv_below_key,bi_good",
        true,"bi_good",["ndv_below_key"],true)

    // C02: ndv_eq_key has NDV=4096 = threshold -> does not qualify (equality does not pass; it must be strictly greater)
    //   Expectation: plan changes, bi_good is selected, ndv_eq_key is excluded
    run("C02","select ndv_eq_key,bi_good,sum(v) from t_02_ndv group by ndv_eq_key,bi_good",
        true,"bi_good",["ndv_eq_key"],true)

    // C03: ndv_above_key has NDV=5120 > threshold -> meets the NDV threshold
    //   ratio8_key has NDV=3000 < threshold 4096 -> insufficient NDV and gets skipped
    //   Expectation: plan changes, ndv_above_key is selected, ratio8_key is excluded
    run("C03","select ndv_above_key,ratio8_key,sum(v) from t_02_ndv group by ndv_above_key,ratio8_key",
        true,"ndv_above_key",["ratio8_key"],true)

}
