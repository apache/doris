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
// Category 04: Fallback and Conservative Logic — R01-R02, SK01, N01-N02, E01-E02
// ===========================================================================================
//
// Purpose
//   Validate fallback (no optimization) behavior of Shuffle Key Pruning in special scenarios:
//   1. ROLLUP/GROUPING SETS -> the Repeat operator produces multiple group-by key sets -> no pruning
//   2. All candidate columns are skewed -> no safe column can be chosen -> no pruning
//   3. Missing/partial column statistics -> conservative fallback -> no pruning
//   4. String fallback: when step 1 cannot pick a single key, step 2 removes string-typed columns
//
// Case List
//   R01: GROUP BY ROLLUP(b,c,d,e,f) -> Repeat operator -> no pruning (negative)
//   R02: GROUP BY GROUPING SETS -> same behavior -> no pruning (negative)
//   SK01: k1~k5 all have hot_values=0.8 -> all skewed -> no pruning (negative)
//   N01: Enable partition stats and leave some scanned partitions without stats -> conservative -> no pruning (negative)
//   N02: Only column b is analyzed -> other columns miss stats -> no pruning (negative)
//   E01: All non-string columns are skewed / low-NDV -> step 2 removes strings and keeps a 3-column composite key (positive)
//   E02: Only string columns remain -> no usable key after removing strings -> no pruning (negative)
//
// Validation
//   Check whether the plan changes and verify result consistency.
// ===========================================================================================
suite("degrade_fallback", "agg_shuffle_prune_func") {
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

    def R = "480000"
    def nullSkew = "48000"
    def esc = { Object v -> v==null?"null":v.toString().replace("\\","\\\\").replace("'","\\'") }
    def ss = { String t,String c,String n,String nn,String ds,String mi,String ma,String hv ->
        def hvc = hv==null ? "" : ", 'hot_values'='${esc(hv)}'"
        sql "alter table ${t} modify column ${c} set stats ('row_count'='${R}','ndv'='${esc(n)}','num_nulls'='${esc(nn)}','data_size'='${esc(ds)}','min_value'='${esc(mi)}','max_value'='${esc(ma)}'${hvc});"
    }
    def extractSigns = { String e ->
        def s=[]; def m=(e=~/orderedShuffledColumns=\[([0-9,\s-]*)\]/); while(m.find()){s<<"["+m.group(1).replaceAll("\\s+","")+"]"}; s
    }
    def extractIdLists = { String e ->
        def ids=[]; def m=(e=~/orderedShuffledColumns=\[([0-9,\s-]*)\]/)
        while(m.find()){def r=m.group(1).trim(); if(r.length()==0){ids<<[];continue}
            ids<<r.split(",").collect{it.trim()}.findAll{it.length()>0}.collect{Integer.parseInt(it)}}; ids
    }
    def exprId = { String e,String c -> def m=(e=~("\\b"+java.util.regex.Pattern.quote(c)+"#(\\d+)\\b")); m.find()?Integer.parseInt(m.group(1)):null }
    def norm = { rows -> rows.collect{r->r.collect{v->v==null?"NULL":v.toString()}.join("||")}.sort() }

    def runEx = { String id,String q,int ap,boolean single,boolean reqShuffle,Boolean expChg,String sel,def exc ->
        sql "set agg_phase=${ap};"; sql "set disable_nereids_rules='';"
        sql "set enable_shuffle_key_prune=false;"
        def eOff=(sql "explain physical plan "+q).toString(); def oS=extractSigns(eOff)
        sql "set enable_shuffle_key_prune=true;"
        def eOn=(sql "explain physical plan "+q).toString(); def nS=extractSigns(eOn)
        def si=[]; extractIdLists(eOn).findAll{it.size()==1}.each{si<<it[0]}
        def shouldChg = (expChg==null ? single : expChg)
        if(shouldChg){assertTrue(oS.toString()!=nS.toString(),"${id}: signs should change"); if(single)assertTrue(si.size()>0,"${id}: expect single")}
        else{assertTrue(oS.toString()==nS.toString(),"${id}: should stay unchanged"); if(reqShuffle)assertTrue(nS.size()>0,"${id}: need shuffle")}
        sql "set enable_shuffle_key_prune=false;"; def rOff=sql q
        sql "set enable_shuffle_key_prune=true;"; def rOn=sql q
        assertTrue(norm(rOff)==norm(rOn),"${id}: results differ")
        logger.info("${id}: oS=${oS}, nS=${nS}")
    }

    def runAdvanced = { String id,String q,boolean chg,List<String> selCols,List<String> exc,Integer keyCount ->
        sql "set agg_phase=0;"; sql "set disable_nereids_rules='';"
        sql "set enable_shuffle_key_prune=false;"
        def eOff=(sql "explain physical plan "+q).toString(); def oS=extractSigns(eOff)
        sql "set enable_shuffle_key_prune=true;"
        def eOn=(sql "explain physical plan "+q).toString(); def nS=extractSigns(eOn); def idLists=extractIdLists(eOn)
        if(chg){assertTrue(oS.toString()!=nS.toString(),"${id}: signs should change")}
        else{assertTrue(oS.toString()==nS.toString(),"${id}: should stay unchanged")}
        def matchedIds = null
        if(selCols!=null && !selCols.isEmpty()){
            def selectedIds = selCols.collect { c ->
                def eid = exprId(eOn, c)
                assertTrue(eid!=null, "${id}: missing exprId for ${c}")
                eid
            }
            matchedIds = idLists.find { it.size()==selectedIds.size() && (it as Set)==(selectedIds as Set) }
            assertTrue(matchedIds!=null, "${id}: expected selected cols ${selCols}, got ${idLists}")
        }
        if(keyCount!=null){
            def counts=idLists.collect{it.size()}
            assertTrue(counts.any{it==keyCount},"${id}: expect keyCount ${keyCount}")
            if(matchedIds!=null){assertTrue(matchedIds.size()==keyCount,"${id}: selected cols size should be ${keyCount}, got ${matchedIds}")}
        }
        if(exc!=null && !exc.isEmpty()){
            def excludedPairs = exc.collect { c ->
                def eid = exprId(eOn, c)
                assertTrue(eid!=null, "${id}: missing exprId for ${c}")
                [c, eid]
            }
            if(matchedIds!=null){
                excludedPairs.each { p ->
                    assertTrue(!matchedIds.contains(p[1]), "${id}: excluded ${p[0]} was selected in ${matchedIds}")
                }
            } else if(keyCount!=null){
                def candidateLists = idLists.findAll { it.size()==keyCount }
                assertTrue(candidateLists.any { ids -> excludedPairs.every { p -> !ids.contains(p[1]) } },
                    "${id}: expected a ${keyCount}-key shuffle without excluded cols ${exc}, got ${candidateLists}")
            }
        }
        sql "set enable_shuffle_key_prune=false;"; def rOff=sql q
        sql "set enable_shuffle_key_prune=true;"; def rOn=sql q
        assertTrue(norm(rOff)==norm(rOn),"${id}: results differ")
        logger.info("${id}: oS=${oS}, nS=${nS}")
    }
    def promoteKnownStats = { String t, Map<String, String> ndvMap ->
        ss(t, "x", ndvMap.getOrDefault("x", "12000"), "0", "1600000", "1", "199900001", "")
        ndvMap.each { c, ndv ->
            if (c != "x") {
                ss(t, c, ndv, "0", "1600000", "1", ndv, "")
            }
        }
        if (ndvMap.containsKey("v")) {
            ss(t, "v", ndvMap.get("v"), "0", "1600000", "0", "1999", "")
        }
    }

    // ===== R group: repeat -> no optimization =====
    sql "drop table if exists t_04_repeat;"
    sql """create table t_04_repeat (x bigint, b bigint, c bigint, d bigint, e bigint, f bigint, g bigint, v bigint)
        duplicate key(x) distributed by hash(x) buckets 4 properties ("replication_num"="1");"""
    sql """insert into t_04_repeat select
        (bitand(murmur_hash3_32(concat(cast(number as string),'_x')),2147483647)%4500)*100000+1,
        (bitand(murmur_hash3_32(concat(cast(number as string),'_b')),2147483647)%220)*100000+2,
        (bitand(murmur_hash3_32(concat(cast(number as string),'_c')),2147483647)%3900)*100000+3,
        (bitand(murmur_hash3_32(concat(cast(number as string),'_d')),2147483647)%3600)*100000+4,
        (bitand(murmur_hash3_32(concat(cast(number as string),'_e')),2147483647)%4000)*100000+5,
        (bitand(murmur_hash3_32(concat(cast(number as string),'_f')),2147483647)%3400)*100000+6,
        (bitand(murmur_hash3_32(concat(cast(number as string),'_g')),2147483647)%120)*100000+7,
        number from numbers("number"="2000");"""
    sql "analyze table t_04_repeat with sync;"
    // Promote stats above the prune threshold so Repeat is the only blocker.
    promoteKnownStats("t_04_repeat", ["b":"5200","c":"5600","d":"6000","e":"6400","f":"6800","g":"7200","v":"60000"])
    runEx("R01","select b,c,d,e,f,sum(v) from t_04_repeat group by rollup(b,c,d,e,f)",0,false,true,false,null,[])
    runEx("R02","select b,c,d,e,f,sum(v) from t_04_repeat group by grouping sets ((b,c,d,e,f),(b,c,d,e),(b,c,d),(b,c),(b))",0,false,true,false,null,[])

    // ===== SK group: all skewed -> no optimization =====
    sql "drop table if exists t_04_allskew;"
    sql """create table t_04_allskew (x bigint, k1 bigint, k2 bigint, k3 bigint, k4 bigint, k5 bigint, v bigint)
        duplicate key(x) distributed by hash(x) buckets 4 properties ("replication_num"="1");"""
    sql """insert into t_04_allskew select number*100000+1,
        if(number%10<8,1,number%3600+2), if(number%10<8,2,number%3400+2),
        if(number%10<8,3,number%3200+2), if(number%10<8,4,number%3000+2),
        if(number%10<8,5,number%2800+2), number from numbers("number"="2000");"""
    ["k1","k2","k3","k4","k5"].each{c-> ss("t_04_allskew",c,"9000","0","1600000","1","9000","${c=='k1'?1:c=='k2'?2:c=='k3'?3:c=='k4'?4:5} :0.8")}
    ss("t_04_allskew","x","60000","0","1600000","1","199900001","")
    ss("t_04_allskew","v","60000","0","1600000","0","1999","")
    runEx("SK01","select k1,k2,k3,k4,k5,sum(v) from t_04_allskew group by k1,k2,k3,k4,k5",0,false,true,false,null,[])

    // ===== N group: missing/partial stats -> conservative =====
    def oldAutoAnalyze = sql("""show global variables like 'enable_auto_analyze'""")[0][1].toString()
    def oldPartitionAnalyze = sql("""show global variables like 'enable_partition_analyze'""")[0][1].toString()
    try {
        sql """set global enable_auto_analyze=false;"""
        sql """set global enable_partition_analyze=true;"""
        sql "drop table if exists t_04_stale;"
        sql "drop table if exists t_04_partial;"
        sql """create table t_04_stale (part_key int, x bigint, b bigint, c bigint, d bigint, e bigint, f bigint, v bigint)
            duplicate key(part_key, x, b, c) partition by range(part_key) (
                partition p1 values [("0"),("100")), partition p2 values [("100"),("200")), partition p3 values [("200"),("300"))
            ) distributed by hash(x) buckets 4 properties ("replication_num"="1");"""
        sql """create table t_04_partial (x bigint, b bigint, c bigint, d bigint, e bigint, f bigint, v bigint)
            duplicate key(x) distributed by hash(x) buckets 4 properties ("replication_num"="1");"""
        sql """insert into t_04_stale select 10, number*100000+1,
            (bitand(murmur_hash3_32(concat(cast(number as string),'_b')),2147483647)%180)*100000+2,
            (bitand(murmur_hash3_32(concat(cast(number as string),'_c')),2147483647)%2600)*100000+3,
            (bitand(murmur_hash3_32(concat(cast(number as string),'_d')),2147483647)%2400)*100000+4,
            (bitand(murmur_hash3_32(concat(cast(number as string),'_e')),2147483647)%2200)*100000+5,
            (bitand(murmur_hash3_32(concat(cast(number as string),'_f')),2147483647)%2000)*100000+6,
            number from numbers("number"="6000");"""
        sql """insert into t_04_partial select number*100000+1,
            (bitand(murmur_hash3_32(concat(cast(number as string),'_b')),2147483647)%180)*100000+2,
            (bitand(murmur_hash3_32(concat(cast(number as string),'_c')),2147483647)%2600)*100000+3,
            (bitand(murmur_hash3_32(concat(cast(number as string),'_d')),2147483647)%2400)*100000+4,
            (bitand(murmur_hash3_32(concat(cast(number as string),'_e')),2147483647)%2200)*100000+5,
            (bitand(murmur_hash3_32(concat(cast(number as string),'_f')),2147483647)%2000)*100000+6,
            number from numbers("number"="6000");"""
        // With partition-level stats enabled, collect only p1 so the scan sees one analyzed partition and one
        // partition without stats; that partial merge path should fall back conservatively.
        sql "analyze table t_04_stale partition(p1) with sync;"
        sql """insert into t_04_stale select 110,
            (bitand(murmur_hash3_32(concat(cast(number as string),'_qx')),2147483647)%3500)*100000+1,
            if(number%10<8,1,(bitand(murmur_hash3_32(concat(cast(number as string),'_qb')),2147483647)%900)+2)*100000+2,
            (bitand(murmur_hash3_32(concat(cast(number as string),'_qc')),2147483647)%2800)*100000+3,
            (bitand(murmur_hash3_32(concat(cast(number as string),'_qd')),2147483647)%2600)*100000+4,
            (bitand(murmur_hash3_32(concat(cast(number as string),'_qe')),2147483647)%2400)*100000+5,
            (bitand(murmur_hash3_32(concat(cast(number as string),'_qf')),2147483647)%2200)*100000+6,
            number from numbers("number"="2000");"""
        def p1PartitionStats = sql """show column stats t_04_stale (b) partition(p1)"""
        assertEquals(1, p1PartitionStats.size())
        def p2PartitionStats = sql """show column stats t_04_stale (b) partition(p2)"""
        assertEquals(0, p2PartitionStats.size())
        def tableLevelStats = sql """show column stats t_04_stale (b)"""
        assertEquals(0, tableLevelStats.size())
        runEx("N01","select b,c,d,e,f,sum(v) from t_04_stale where part_key in (10,110) group by b,c,d,e,f",0,false,true,false,null,[])
        sql "analyze table t_04_partial(b) with sync;"
        // Keep b itself prunable and leave c/d/e/f without stats, so the failure is really "partial stats".
        ss("t_04_partial","x","12000","0","1600000","1","199900001","")
        ss("t_04_partial","b","5200","0","1600000","1","5200","")
        ss("t_04_partial","v","60000","0","1600000","0","1999","")
        runEx("N02","select b,c,d,e,f,sum(v) from t_04_partial group by b,c,d,e,f",0,false,true,false,null,[])
    } finally {
        sql """set global enable_partition_analyze=${oldPartitionAnalyze};"""
        sql """set global enable_auto_analyze=${oldAutoAnalyze};"""
    }

    // ===== E group: string fallback =====
    sql "drop table if exists t_04_fallback;"
    sql """create table t_04_fallback (x bigint, ratio8_key int, ndv_eq_key int, d_skew datev2,
        vc16_hot varchar(16), s_hot string, v bigint)
        duplicate key(x) distributed by hash(x) buckets 4 properties ("replication_num"="1");"""
    sql """insert into t_04_fallback select number*100000+1,
        if(number%10==0,1,cast(number%9000 as int)+2), cast(number%5000 as int)+1,
        if(number%10==0,null,cast(concat('2024-02-',lpad(cast(((number%28)+1) as string),2,'0')) as datev2)),
        if(number%10==0,'hot_vc16',concat('vh_',lpad(cast(number as string),10,'0'))),
        if(number%10==0,'hot_string',concat('s_hot_',cast(number%16000 as string))),
        number from numbers("number"="2000");"""
    ss("t_04_fallback","x","60000","0","1600000","1","199900001","")
    ss("t_04_fallback","ratio8_key","9000","0","420000","1","9000","1 :0.05")
    ss("t_04_fallback","ndv_eq_key","800","0","420000","1","800","")
    ss("t_04_fallback","d_skew","8600",nullSkew,"420000","2024-02-01","2024-12-31","")
    ss("t_04_fallback","vc16_hot","8100","0","1680000","vh_0000000001","vh_9999999999","hot_vc16 :0.05")
    ss("t_04_fallback","s_hot","9000","0","21000000","s_hot_1","s_hot_16000","hot_string :0.05")
    ss("t_04_fallback","v","60000","0","1600000","0","1999","")

    // E01: after removing string columns, a 3-column composite key remains
    runAdvanced("E01","select ratio8_key,ndv_eq_key,d_skew,vc16_hot,s_hot,sum(v) from t_04_fallback group by ratio8_key,ndv_eq_key,d_skew,vc16_hot,s_hot",
        true,["ratio8_key","ndv_eq_key","d_skew"],["vc16_hot","s_hot"],3)
    // E02: after removing string columns, no usable key remains -> fallback
    runAdvanced("E02","select vc16_hot,s_hot,sum(v) from t_04_fallback group by vc16_hot,s_hot",
        false,[],[],null)

}
