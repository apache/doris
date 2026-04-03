// ===========================================================================================
// Category 05: Expression Keys and Bucket Variations — EX01-EX04, BK01-BK02
// ===========================================================================================
//
// Purpose
//   Validate the following two categories:
//   A. Expression Keys: pruning behavior when GROUP BY contains expressions rather than plain column references
//   B. Bucket-count variations: how different bucket counts affect pruning logic
//
// EX Group Case List
//   EX01: cast(b as bigint) -> type cast folds back to the original slot -> pruning works normally (positive)
//   EX02: substr(s_key,1,8) -> string slicing creates an expression slot -> can be selected (positive)
//         Columns b, c, d, and e are injected with 80% skew to ensure they are excluded
//   EX03: coalesce(null_k,-1) -> unknown stats -> uncertainty -> no pruning (negative)
//   EX04: date_trunc(dt_key,'day') -> unstable distribution -> no pruning (negative)
//
// BK Group Case List
//   BK01: 1-bucket table -> pruning still works (positive)
//   BK02: 32-bucket table -> pruning still works (positive)
//   Note: bucket count does not affect pruning logic (pruning applies to shuffle exchange and is independent of buckets)
//
// Validation
//   Check changes in orderedShuffledColumns in the plan and verify result consistency.
// ===========================================================================================
suite("expr_bucket", "agg_shuffle_prune_func") {
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
    def extractSingle = { String e ->
        def ids=[]; def m=(e=~/orderedShuffledColumns=\[([0-9,\s-]*)\]/)
        while(m.find()){def r=m.group(1).trim(); if(r.length()==0)continue; def p=r.split(",").collect{it.trim()}.findAll{it.length()>0}; if(p.size()==1)ids<<Integer.parseInt(p[0])}; ids
    }
    def norm = { rows -> rows.collect{r->r.collect{v->v==null?"NULL":v.toString()}.join("||")}.sort() }

    def runEx = { String id,String q,int ap,boolean single,boolean reqShuffle,Boolean expChg,String sel,def exc ->
        def exprId = { String e,String c -> def m=(e=~("\\b"+java.util.regex.Pattern.quote(c)+"#(\\d+)\\b")); m.find()?Integer.parseInt(m.group(1)):null }
        sql "set agg_phase=${ap};"; sql "set disable_nereids_rules='';"
        sql "set enable_shuffle_key_prune=false;"
        def eOff=(sql "explain physical plan "+q).toString(); def oS=extractSigns(eOff)
        sql "set enable_shuffle_key_prune=true;"
        def eOn=(sql "explain physical plan "+q).toString(); def nS=extractSigns(eOn); def si=extractSingle(eOn)
        def shouldChg = (expChg==null ? single : expChg)
        if(shouldChg){assertTrue(oS.toString()!=nS.toString(),"${id}: signs should change"); if(single)assertTrue(si.size()>0,"${id}: expect single")}
        else{assertTrue(oS.toString()==nS.toString(),"${id}: should stay unchanged"); if(reqShuffle)assertTrue(nS.size()>0,"${id}: need shuffle")}
        if(sel!=null){def eid=exprId(eOn,sel); assertTrue(eid!=null&&si.contains(eid),"${id}: ${sel} not selected")}
        exc.each{c->def eid=exprId(eOn,c); if(eid!=null)assertTrue(!si.contains(eid),"${id}: ${c} excluded")}
        sql "set enable_shuffle_key_prune=false;"; def rOff=sql q
        sql "set enable_shuffle_key_prune=true;"; def rOn=sql q
        assertTrue(norm(rOff)==norm(rOn),"${id}: results differ")
        logger.info("${id}: oS=${oS}, nS=${nS}, si=${si}")
    }

    // ===== EX group table setup =====
    sql "drop table if exists t_05_expr;"
    sql """create table t_05_expr (x bigint, b bigint, c bigint, d bigint, e bigint,
        s_key string, null_k bigint, dt_key datetimev2(0), v bigint)
        duplicate key(x) distributed by hash(x) buckets 4 properties ("replication_num"="1");"""
    sql """insert into t_05_expr select
        (bitand(murmur_hash3_32(concat(cast(number as string),'_x')),2147483647)%12000)*100000+1,
        (bitand(murmur_hash3_32(concat(cast(number as string),'_b')),2147483647)%5200)*100000+2,
        (bitand(murmur_hash3_32(concat(cast(number as string),'_c')),2147483647)%5600)*100000+3,
        (bitand(murmur_hash3_32(concat(cast(number as string),'_d')),2147483647)%6000)*100000+4,
        (bitand(murmur_hash3_32(concat(cast(number as string),'_e')),2147483647)%6400)*100000+5,
        concat('expr_',cast(number%6800 as string)),
        if(number%20==0,null,number%7200+1),
        cast(concat('2024-01-',lpad(cast(((number%28)+1) as string),2,'0'),' 00:00:00') as datetimev2(0)),
        number from numbers("number"="2000");"""

    def applyExpr = { Map<String,String> hv ->
        ss("t_05_expr","x","12000","0","1600000","100001","1199900001","")
        ss("t_05_expr","b","5200","0","1600000","2","519900002",hv.getOrDefault("b",""))
        ss("t_05_expr","c","5600","0","1600000","3","559900003",hv.getOrDefault("c",""))
        ss("t_05_expr","d","6000","0","1600000","4","599900004",hv.getOrDefault("d",""))
        ss("t_05_expr","e","6400","0","1600000","5","639900005",hv.getOrDefault("e",""))
        ss("t_05_expr","s_key","6800","0","6400000","expr_0","expr_6799",hv.getOrDefault("s_key",""))
        ss("t_05_expr","null_k","7200",nullSkew,"1600000","1","7200",hv.getOrDefault("null_k",""))
        ss("t_05_expr","dt_key","28","0","1600000","2024-01-01 00:00:00","2024-01-28 00:00:00",hv.getOrDefault("dt_key",""))
        ss("t_05_expr","v","60000","0","1600000","0","1999","")
    }
    def guard = ["b":"2 :0.8","c":"3 :0.8","d":"4 :0.8","e":"5 :0.8"] as Map<String,String>

    // EX01: cast(b as bigint) -> folds back to a slot -> prune
    applyExpr([:] as Map<String,String>)
    runEx("EX01","select cast(b as bigint) as b1,c,d,e,s_key,sum(v) from t_05_expr group by cast(b as bigint),c,d,e,s_key",0,true,true,true,null,[])

    // EX02: substr -> expression slot -> can be selected
    applyExpr(guard)
    runEx("EX02","select substr(s_key,1,8) as s8,b,c,d,e,sum(v) from t_05_expr group by substr(s_key,1,8),b,c,d,e",0,true,true,true,"s8",["b","c","d","e"])

    // EX03: coalesce -> unknown stats -> no pruning
    applyExpr(guard)
    runEx("EX03","select coalesce(null_k,-1) as nk,b,c,d,e,sum(v) from t_05_expr group by coalesce(null_k,-1),b,c,d,e",0,false,true,false,null,[])

    // EX04: date_trunc -> unstable -> no pruning
    applyExpr(guard)
    runEx("EX04","select date_trunc(dt_key,'day') as day_k,b,c,d,e,sum(v) from t_05_expr group by date_trunc(dt_key,'day'),b,c,d,e",0,false,true,false,null,[])

    // ===== BK group table setup =====
    sql "drop table if exists t_05_bk1;"
    sql "drop table if exists t_05_bk32;"
    sql """create table t_05_bk1 (x bigint, b bigint, c bigint, d bigint, e bigint, f bigint, g bigint, v bigint)
        duplicate key(x) distributed by hash(x) buckets 1 properties ("replication_num"="1");"""
    sql """create table t_05_bk32 (x bigint, b bigint, c bigint, d bigint, e bigint, f bigint, g bigint, v bigint)
        duplicate key(x) distributed by hash(x) buckets 32 properties ("replication_num"="1");"""
    sql """insert into t_05_bk1 select
        (bitand(murmur_hash3_32(concat(cast(number as string),'_x')),2147483647)%12000)*100000+1,
        (bitand(murmur_hash3_32(concat(cast(number as string),'_b')),2147483647)%5200)*100000+2,
        (bitand(murmur_hash3_32(concat(cast(number as string),'_c')),2147483647)%5600)*100000+3,
        (bitand(murmur_hash3_32(concat(cast(number as string),'_d')),2147483647)%6000)*100000+4,
        (bitand(murmur_hash3_32(concat(cast(number as string),'_e')),2147483647)%6400)*100000+5,
        (bitand(murmur_hash3_32(concat(cast(number as string),'_f')),2147483647)%6800)*100000+6,
        (bitand(murmur_hash3_32(concat(cast(number as string),'_g')),2147483647)%7200)*100000+7,
        number from numbers("number"="2000");"""
    sql "insert into t_05_bk32 select * from t_05_bk1;"
    def bkStats = { String t ->
        ss(t,"x","12000","0","1600000","100001","1199900001","")
        ss(t,"b","5200","0","1600000","2","519900002","")
        ss(t,"c","5600","0","1600000","3","559900003","")
        ss(t,"d","6000","0","1600000","4","599900004","")
        ss(t,"e","6400","0","1600000","5","639900005","")
        ss(t,"f","6800","0","1600000","6","679900006","")
        ss(t,"g","7200","0","1600000","7","719900007","")
        ss(t,"v","60000","0","1600000","0","1999","")
    }
    bkStats("t_05_bk1")
    bkStats("t_05_bk32")

    runEx("BK01","select b,c,d,e,f,g,sum(v) from t_05_bk1 group by b,c,d,e,f,g",0,true,true,true,null,[])
    runEx("BK02","select b,c,d,e,f,g,sum(v) from t_05_bk32 group by b,c,d,e,f,g",0,true,true,true,null,[])

}
