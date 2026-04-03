// ===========================================================================================
// Category 03: Type Priority Selection — D01-D09, H01-H07
// ===========================================================================================
//
// Purpose
//   Validate the type-priority ordering logic of Shuffle Key Pruning. When multiple columns all satisfy
//   the NDV and balanced conditions, the optimizer chooses the best column based on the hash-computation
//   efficiency of the data type.
//
// Type Priority (highest to lowest)
//   bigint/largeint > decimal > datev2/datetimev2 > char > varchar > string
//   If the highest-priority candidate is skewed, skip it and choose the next one.
//
// D Group Case List
//   D01: largeint is available vs skewed columns -> choose li_good (positive)
//   D02: decimal is available vs skewed columns -> choose de_good (positive)
//   D03: datev2 is available vs skewed columns -> choose d_good (positive)
//   D04: datetimev2 is available vs skewed columns -> choose dt_good (positive)
//   D05: char(8) is available vs skewed columns -> choose ch8_good (positive)
//   D06: ch8_low is skewed -> skip char and choose vc16_good (positive)
//   D07: ch8_low + vc16_hot are both skewed -> choose s_good (string fallback)
//   D08: s_hot is skewed -> skip string and choose bi_good (positive)
//   D09: Distribution is already satisfied (distributed by hash(li_good)) -> no change (negative)
//
// H Group Case List (basic-type smoke tests)
//   H01: int/bigint/decimal/string -> choose bi_key (bigint has priority)
//   H02: bigint + hot_num (skewed) -> skip hot_num and choose bi_key
//   H03: bigint + null_num (high null ratio) -> skip null_num and choose bi_key
//   H04: 8 mixed-type columns -> still choose bi_key (bigint has priority)
//   H05: Artificially lower bigint NDV and raise string NDV -> choose s_key (string wins on NDV)
//   H06: hot_str is skewed -> skip it and choose bi_key
//   H07: hash(bi_key) table -> distribution already satisfied -> no change (negative)
//
// Validation
//   Control optimizer choices by injecting NDV and hot_values for different columns,
//   then verify plan changes and the selected column.
// ===========================================================================================
suite("type_priority", "agg_shuffle_prune_func") {
    sql """set enable_nereids_planner=true;"""
    sql """set enable_fallback_to_original_planner=false;"""
    sql """set enable_sql_cache=false;"""
    sql """set enable_query_cache=false;"""
    sql """set agg_phase=0;"""
    sql """set enable_adaptive_pipeline_task_serial_read_on_limit=false;"""

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

    // ===== D group table setup (type priority) =====
    sql "drop table if exists t_03_dtype;"
    sql """create table t_03_dtype (
        x bigint, ratio8_key int, ndv_below_key int, ndv_eq_key int,
        bi_good bigint, li_good largeint, de_good decimal(20,4),
        d_good datev2, d_skew datev2, dt_good datetimev2(0),
        ch8_good char(8), ch8_low char(8), vc16_good varchar(16), vc16_hot varchar(16),
        vc128_good varchar(128), s_good string, s_hot string, v bigint
    ) duplicate key(x) distributed by hash(x) buckets 4 properties ("replication_num"="1");"""

    sql "drop table if exists t_03_dtype_sat;"
    sql """create table t_03_dtype_sat (
        x bigint, ratio8_key int, ndv_below_key int, ndv_eq_key int,
        bi_good bigint, li_good largeint, de_good decimal(20,4),
        d_good datev2, d_skew datev2, dt_good datetimev2(0),
        ch8_good char(8), ch8_low char(8), vc16_good varchar(16), vc16_hot varchar(16),
        vc128_good varchar(128), s_good string, s_hot string, v bigint
    ) duplicate key(x) distributed by hash(li_good) buckets 4 properties ("replication_num"="1");"""

    sql """insert into t_03_dtype select number*100000+1,
        cast(number%4000 as int)+1, cast(number%5000 as int)+1, cast(number%5000 as int)+1,
        number*100000+2, cast(number*100000+3 as largeint), cast(number%19000 as decimal(20,4))/10,
        cast(concat('2024-01-',lpad(cast(((number%28)+1) as string),2,'0')) as datev2),
        cast(concat('2024-02-',lpad(cast(((number%28)+1) as string),2,'0')) as datev2),
        cast(concat('2024-03-',lpad(cast(((number%28)+1) as string),2,'0'),' 00:00:00') as datetimev2(0)),
        concat('c',lpad(cast(number%9999999 as string),7,'0')),
        concat('l',lpad(cast(number%9999999 as string),7,'0')),
        concat('vc_',lpad(cast(number as string),10,'0')),
        concat('vh_',lpad(cast(number as string),10,'0')),
        concat('vlong_',lpad(cast(number as string),18,'0')),
        concat('s_good_',cast(number%16000 as string)),
        concat('s_hot_',cast(number%16000 as string)),
        number from numbers("number"="2000");"""
    sql "insert into t_03_dtype_sat select * from t_03_dtype;"

    def dStats = { String t ->
        ss(t,"x","60000","0","1600000","1","199900001","")
        ss(t,"ratio8_key","9000","0","420000","1","9000","1 :0.10")
        ss(t,"ndv_below_key","9000","0","420000","1","9000","")
        ss(t,"ndv_eq_key","9100","0","420000","1","9100","")
        ss(t,"bi_good","9200","0","840000","1","9200","")
        ss(t,"li_good","9300","0","840000","1","9300","")
        ss(t,"de_good","9100","0","840000","1.0000","9100.0000","")
        ss(t,"d_good","8600","0","420000","2024-01-01","2024-12-31","")
        ss(t,"d_skew","8600",nullSkew,"420000","2024-02-01","2024-12-31","")
        ss(t,"dt_good","8700","0","840000","2024-03-01 00:00:00","2024-03-28 23:59:00","")
        ss(t,"ch8_good","8200","0","840000","c0000001","c9999999","")
        ss(t,"ch8_low","8300","0","840000","l0000001","l9999999","l0000001 :0.10")
        ss(t,"vc16_good","8100","0","1680000","vc_0000000001","vc_9999999999","")
        ss(t,"vc16_hot","8100","0","1680000","vh_0000000001","vh_9999999999","hot_vc16 :0.10")
        ss(t,"vc128_good","8050","0","13440000","vlong_000000000000000001","vlong_999999999999999999","")
        ss(t,"s_good","8000","0","21000000","s_good_1","s_good_16000","")
        ss(t,"s_hot","9000","0","21000000","s_hot_1","s_hot_16000","hot_string :0.10")
        ss(t,"v","60000","0","1600000","0","1999","")
    }
    dStats("t_03_dtype")
    dStats("t_03_dtype_sat")

    // D01-D09
    run("D01","select ratio8_key,d_skew,li_good,vc16_good,s_good,sum(v) from t_03_dtype group by ratio8_key,d_skew,li_good,vc16_good,s_good",true,"li_good",["ratio8_key","d_skew"],true)
    run("D02","select ratio8_key,d_skew,de_good,vc16_good,s_good,sum(v) from t_03_dtype group by ratio8_key,d_skew,de_good,vc16_good,s_good",true,"de_good",["ratio8_key","d_skew"],true)
    run("D03","select ratio8_key,d_skew,d_good,vc16_good,s_good,sum(v) from t_03_dtype group by ratio8_key,d_skew,d_good,vc16_good,s_good",true,"d_good",["ratio8_key","d_skew"],true)
    run("D04","select ratio8_key,d_skew,dt_good,vc16_good,s_good,sum(v) from t_03_dtype group by ratio8_key,d_skew,dt_good,vc16_good,s_good",true,"dt_good",["ratio8_key","d_skew"],true)
    run("D05","select ratio8_key,d_skew,ch8_good,vc16_good,vc128_good,s_good,sum(v) from t_03_dtype group by ratio8_key,d_skew,ch8_good,vc16_good,vc128_good,s_good",true,"ch8_good",["ratio8_key","d_skew"],true)
    run("D06","select ratio8_key,d_skew,ch8_low,vc16_good,vc128_good,s_good,sum(v) from t_03_dtype group by ratio8_key,d_skew,ch8_low,vc16_good,vc128_good,s_good",true,"vc16_good",["ratio8_key","d_skew","ch8_low"],true)
    run("D07","select ratio8_key,d_skew,ch8_low,vc16_hot,s_good,sum(v) from t_03_dtype group by ratio8_key,d_skew,ch8_low,vc16_hot,s_good",true,"s_good",["ratio8_key","d_skew","ch8_low","vc16_hot"],true)
    run("D08","select s_hot,bi_good,vc16_good,sum(v) from t_03_dtype group by s_hot,bi_good,vc16_good",true,"bi_good",["s_hot"],true)
    run("D09","select li_good,d_good,dt_good,ch8_good,vc16_good,s_good,de_good,bi_good,sum(v) from t_03_dtype_sat group by li_good,d_good,dt_good,ch8_good,vc16_good,s_good,de_good,bi_good",false,null,[],false)

    // ===== H group table setup (basic-type smoke tests) =====
    sql "drop table if exists t_03_type;"
    sql """create table t_03_type (x bigint, i_key int, bi_key bigint, de_key decimal(20,4),
        fl_key float, db_key double, vc_key varchar(64), s_key string,
        hot_num bigint, hot_str string, null_num bigint, l1 bigint, v bigint)
        duplicate key(x) distributed by hash(x) buckets 4 properties ("replication_num"="1");"""
    sql "drop table if exists t_03_type_sat;"
    sql """create table t_03_type_sat (x bigint, i_key int, bi_key bigint, de_key decimal(20,4),
        fl_key float, db_key double, vc_key varchar(64), s_key string,
        hot_num bigint, hot_str string, null_num bigint, l1 bigint, v bigint)
        duplicate key(x) distributed by hash(bi_key) buckets 4 properties ("replication_num"="1");"""
    sql """insert into t_03_type select
        (bitand(murmur_hash3_32(concat(cast(number as string),'_x')),2147483647)%18000)*100000+1,
        cast((bitand(murmur_hash3_32(concat(cast(number as string),'_i')),2147483647)%220) as int),
        (bitand(murmur_hash3_32(concat(cast(number as string),'_bi')),2147483647)%5500)*100000+2,
        cast((bitand(murmur_hash3_32(concat(cast(number as string),'_de')),2147483647)%4200) as decimal(20,4))/10,
        cast((bitand(murmur_hash3_32(concat(cast(number as string),'_fl')),2147483647)%180) as float)/10,
        cast((bitand(murmur_hash3_32(concat(cast(number as string),'_db')),2147483647)%160) as double)/10,
        concat('vc_',cast((bitand(murmur_hash3_32(concat(cast(number as string),'_vc')),2147483647)%3200) as string)),
        concat('s_',cast((bitand(murmur_hash3_32(concat(cast(number as string),'_s')),2147483647)%3000) as string)),
        if(number%10<7,1,(bitand(murmur_hash3_32(concat(cast(number as string),'_hn')),2147483647)%7000)+2),
        if(number%10<7,'hot_str',concat('hs_',cast((bitand(murmur_hash3_32(concat(cast(number as string),'_hs')),2147483647)%7000) as string))),
        if(number%10<7,null,(bitand(murmur_hash3_32(concat(cast(number as string),'_nn')),2147483647)%6500)+1),
        (bitand(murmur_hash3_32(concat(cast(number as string),'_l1')),2147483647)%120)+1,
        bitand(murmur_hash3_32(concat(cast(number as string),'_v')),2147483647)
        from numbers("number"="2000");"""
    sql "insert into t_03_type_sat select * from t_03_type;"

    def hStats = { String t ->
        ss(t,"x","60000","0","1600000","1","199900001","")
        ss(t,"i_key","220","0","80000","0","219","")
        ss(t,"bi_key","5500","0","160000","2","549900002","")
        ss(t,"de_key","4200","0","160000","0.0000","419.9000","")
        ss(t,"fl_key","180","0","80000","0.0","17.9","")
        ss(t,"db_key","160","0","160000","0.0","15.9","")
        ss(t,"vc_key","3200","0","640000","vc_0","vc_3199","")
        ss(t,"s_key","3000","0","800000","s_0","s_2999","")
        ss(t,"hot_num","7000","0","160000","1","7001","1 :0.7")
        ss(t,"hot_str","7000","0","800000","hot_str","hs_6999","hot_str :0.7")
        ss(t,"null_num","6500","33600","160000","1","6500","")
        ss(t,"l1","120","0","160000","1","120","")
        ss(t,"v","60000","0","1600000","0","2147483647","")
    }
    hStats("t_03_type")
    hStats("t_03_type_sat")

    run("H01","select i_key,bi_key,de_key,s_key,l1,sum(v) from t_03_type group by i_key,bi_key,de_key,s_key,l1",true,"bi_key",["s_key"],true)
    run("H02","select bi_key,de_key,hot_num,s_key,l1,sum(v) from t_03_type group by bi_key,de_key,hot_num,s_key,l1",true,"bi_key",["hot_num"],true)
    run("H03","select bi_key,de_key,null_num,vc_key,l1,sum(v) from t_03_type group by bi_key,de_key,null_num,vc_key,l1",true,"bi_key",["null_num"],true)
    run("H04","select i_key,bi_key,de_key,fl_key,db_key,vc_key,s_key,l1,sum(v) from t_03_type group by i_key,bi_key,de_key,fl_key,db_key,vc_key,s_key,l1",true,"bi_key",["s_key"],true)

    // H05: string dominant
    ss("t_03_type","bi_key","260","0","160000","2","25900002","")
    ss("t_03_type","de_key","240","0","160000","0.0000","23.9000","")
    ss("t_03_type","vc_key","320","0","640000","svc_0","svc_319","")
    ss("t_03_type","s_key","8000","0","1000000","dom_0","dom_7999","")
    run("H05","select i_key,bi_key,de_key,vc_key,s_key,l1,sum(v) from t_03_type group by i_key,bi_key,de_key,vc_key,s_key,l1",true,"s_key",[],true)

    // H06: hot string -> restore the standard stats first
    hStats("t_03_type")
    ss("t_03_type","hot_str","7000","0","800000","hot_string","hdom_6999","hot_string :0.7")
    run("H06","select bi_key,de_key,hot_str,vc_key,l1,sum(v) from t_03_type group by bi_key,de_key,hot_str,vc_key,l1",true,"bi_key",["hot_str"],true)

    // H07: distribution already satisfied -> no change
    run("H07","select i_key,bi_key,de_key,fl_key,db_key,vc_key,s_key,l1,sum(v) from t_03_type_sat group by i_key,bi_key,de_key,fl_key,db_key,vc_key,s_key,l1",false,null,[],false)

}
