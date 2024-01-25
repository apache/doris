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

suite("test_aggregate_all_functions") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

    sql "set batch_size = 4096"
    
    // APPROX_COUNT_DISTINCT_ACTION
    def tableName_01 = "approx_count_distince"
    sql "DROP TABLE IF EXISTS ${tableName_01}"
    sql """
	    CREATE TABLE IF NOT EXISTS ${tableName_01} (
	        id int,
	        group_type VARCHAR(10)
	    )
	    DISTRIBUTED BY HASH(id) BUCKETS 1
	    PROPERTIES (
	      "replication_num" = "1"
	    ) 
           """
    sql "INSERT INTO ${tableName_01} values(1,'beijing'), (2,'xian'), (3,'xian')"
    qt_select1 "select approx_count_distinct(id) from ${tableName_01} group by group_type order by approx_count_distinct(id) asc"
    sql "DROP TABLE IF EXISTS ${tableName_01}"
    
    
    // AVG
    def tableName_02 = "avg"
    sql "DROP TABLE IF EXISTS ${tableName_02}"
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName_02} (
            id int,
	    level int,
	    group_type VARCHAR(10)
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
          "replication_num" = "1"
        ) 
        """
    sql "INSERT INTO ${tableName_02} values(1,100,'beijing'), (2,70,'xian'), (3,90,'xian') ,(4,100,'beijing') ,(5,140,'xian') ,(6,100,'beijing')"
    qt_select2 "select group_type,AVG(level) from ${tableName_02} group by group_type order by group_type"
    qt_select3 "select group_type,AVG(distinct level) from ${tableName_02} group by group_type order by group_type"
    sql "DROP TABLE IF EXISTS ${tableName_02}"


    
    // BITMAP_UNION group_bitmap_xor
    def tableName_03 = "pv_bitmap"
    def tableName_04 = "bitmap_base"
    sql "DROP TABLE IF EXISTS ${tableName_03}"

    sql """
	CREATE TABLE IF NOT EXISTS ${tableName_03} (
	 `dt` int(11) NULL COMMENT "",
	 `page` varchar(10) NULL COMMENT "",
	 `user_id` bitmap BITMAP_UNION NULL COMMENT ""
	) ENGINE=OLAP
	AGGREGATE KEY(`dt`, `page`)
	COMMENT "OLAP"
	DISTRIBUTED BY HASH(`dt`) BUCKETS 2
	PROPERTIES (
          "replication_num" = "1"
        )
    """

    sql "DROP TABLE IF EXISTS ${tableName_04}"

    sql """
	CREATE TABLE IF NOT EXISTS ${tableName_04} (
	 `dt` int(11) NULL COMMENT "",
	 `page` varchar(10) NULL COMMENT "",
	 `user_id_bitmap` bitmap BITMAP_UNION NULL COMMENT "",
	 `user_id_int` int(11) REPLACE NULL COMMENT "",
	 `user_id_str` string REPLACE NULL COMMENT ""
	) ENGINE=OLAP
	AGGREGATE KEY(`dt`, `page`)
	COMMENT "OLAP"
	DISTRIBUTED BY HASH(`dt`) BUCKETS 2
	PROPERTIES (
	  "replication_num" = "1"
	)
        """

    sql "INSERT INTO ${tableName_04} values(20220201,'meituan',to_bitmap(10000),10000,'zhangsan'), (20220201,'meituan',to_bitmap(10001),10001,'lisi'), (20220202,'eleme',to_bitmap(10001),10001,'lisi') ,(20220201,'eleme',to_bitmap(10001),10001,'lisi') ,(20220203,'meituan',to_bitmap(10000),10000,'zhangsan') ,(20220203,'meituan',to_bitmap(10001),10001,'lisi')"

    sql "insert into ${tableName_03} select dt,page,user_id_bitmap user_id from ${tableName_04}"
    sql "insert into ${tableName_03} select dt,page,bitmap_union(user_id_bitmap) user_id from ${tableName_04} group by dt,page"
    sql "insert into ${tableName_03} select dt,page,to_bitmap(user_id_int) user_id from ${tableName_04}"
    sql "insert into ${tableName_03} select dt,page,bitmap_hash(user_id_str) user_id from ${tableName_04}"

    qt_select_all1 "select *, bitmap_to_string(user_id) from pv_bitmap order by 1,2;"
    qt_select_all2 "select *, bitmap_to_string(user_id) from pv_bitmap where dt = 20220202 order by 1,2;"

    qt_bitmap_intersect "select dt, bitmap_to_string(bitmap_intersect(user_id_bitmap)) from ${tableName_04} group by dt order by dt"

    qt_select4 "select bitmap_union_count(user_id) from  ${tableName_03}"
    qt_select5 "select bitmap_count(bitmap_union(user_id)) FROM ${tableName_03}"
    qt_select6 "select bitmap_union_count(user_id) from  ${tableName_03} group by page order by 1;"
    qt_select7 "select bitmap_to_string(bitmap_union(user_id)) FROM ${tableName_03} group by page order by 1;"
    qt_group_bitmap_xor "select dt, bitmap_to_string(group_bitmap_xor(user_id_bitmap)) from ${tableName_04} group by dt order by dt"

    sql "DROP TABLE IF EXISTS ${tableName_03}"
    sql "DROP TABLE IF EXISTS ${tableName_04}"

    
    // COUNT
    def tableName_05 = "count"

    sql "DROP TABLE IF EXISTS ${tableName_05}"
 
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName_05} (
            id int,
	    group_type VARCHAR(10)
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
          "replication_num" = "1"
        ) 
        """
    sql "INSERT INTO ${tableName_05} values(1,'beijing'), (2,'xian'), (2,'xian') ,(4,'beijing') ,(5,'xian') ,(6,'beijing')"
    qt_select6 "select group_type,count(*) from ${tableName_05} group by group_type order by group_type"
    qt_select7 "select group_type,count(id) from ${tableName_05} group by group_type order by group_type"
    qt_select8 "select group_type,count(distinct id) from ${tableName_05} group by group_type order by group_type"
    sql "DROP TABLE IF EXISTS ${tableName_05}"

    
    // group_concat
    def tableName_06 = "group_concat"

   
    sql "DROP TABLE IF EXISTS ${tableName_06}"
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName_06} (
            id int,
            name varchar(10)
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
          "replication_num" = "1"
        ) 
    """
    sql "INSERT INTO ${tableName_06} values(1,'beijing'),(2,'beijing'), (1,'xian'), (2,'chengdu') ,(2,'shanghai')"

    qt_select9 "select GROUP_CONCAT(name) from ${tableName_06}"
    qt_select10 "select GROUP_CONCAT(name,' ') from ${tableName_06}"    
    qt_select11 "select GROUP_CONCAT(name,NULL) from ${tableName_06}"
    qt_select12 "select GROUP_CONCAT(name) from ${tableName_06} group by id order by id"
    qt_select13 "select GROUP_CONCAT(name,' ') from ${tableName_06} group by id order by id"
    qt_select14 "select GROUP_CONCAT(name,NULL) from ${tableName_06} group by id order by id"
    sql "DROP TABLE IF EXISTS ${tableName_06}"
    
    
    // HLL_UNION_AGG
    def tableName_07 = "hll_union_agg"
    def tableName_08 = "hll_table"

    sql "DROP TABLE IF EXISTS ${tableName_07}"
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName_07} (
            id int,
	    group_type VARCHAR(10)
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
          "replication_num" = "1"
        ) 
        """

    sql "INSERT INTO ${tableName_07} values(1,'beijing'), (2,'xian'), (2,'xian') ,(4,'beijing') ,(5,'xian') ,(6,'beijing')"
    sql "DROP TABLE IF EXISTS ${tableName_08}"
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName_08} (
            id int,
	    group_type hll hll_union
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
          "replication_num" = "1"
        ) 
        """
    sql "INSERT INTO ${tableName_08} select id,hll_hash(group_type) from ${tableName_07}"
    
    qt_select15 "select id,HLL_UNION_AGG(group_type) from ${tableName_08} group by id order by id"
    
    sql "DROP TABLE IF EXISTS ${tableName_07}"
    sql "DROP TABLE IF EXISTS ${tableName_08}"
    
    
    // MAX
    def tableName_09 = "max"

    sql "DROP TABLE IF EXISTS ${tableName_09}"
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName_09} (
            id int,
	    level int
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
          "replication_num" = "1"
        ) 
        """
    sql "INSERT INTO ${tableName_09} values(1,10), (2,8), (2,441) ,(3,10) ,(5,29) ,(6,101)"
    
    qt_select16 "select id,MAX(level) from ${tableName_09} group by id order by id"
    qt_select17 "select MAX(level) from ${tableName_09}"
    
    sql "DROP TABLE IF EXISTS ${tableName_09}"
    
    // MIN
    def tableName_11 = "min"

    sql "DROP TABLE IF EXISTS ${tableName_11}"
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName_11} (
            id int,
	    level int
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
          "replication_num" = "1"
        ) 
        """
    sql "INSERT INTO ${tableName_11} values(1,10), (2,8), (2,441) ,(3,10) ,(5,29) ,(6,101)"

    qt_select18 "select id,MIN(level) from ${tableName_11} group by id order by id"
    qt_select19 "select MIN(level) from ${tableName_11}"


    sql "DROP TABLE IF EXISTS ${tableName_11}"
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName_11} (
          `k1` int(11) NULL,
          `a1` int(11) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`) BUCKETS 10
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
        )
        """
    sql "INSERT INTO ${tableName_11} values(1,1),(2,2),(3,3),(4,null),(null,5)"

    qt_select "select * from (select k1 from ${tableName_11} union select null) t order by k1"
    qt_select "select * from (select k1,a1 from ${tableName_11} union select null,null) t order by k1, a1"

    qt_select "select min(k1) from (select k1 from ${tableName_11} union select null) t"
    qt_select "select min(k1) from (select k1,a1 from ${tableName_11} union select null,null) t"

    sql "DROP TABLE IF EXISTS ${tableName_11}"
    
    // PERCENTILE
    def tableName_13 = "percentile"

    sql "DROP TABLE IF EXISTS ${tableName_13}"
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName_13} (
            id int,
	    level int
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
          "replication_num" = "1"
        ) 
        """
    sql "INSERT INTO ${tableName_13} values(1,10), (2,8), (2,441) ,(3,10) ,(5,29) ,(6,101)"

    qt_select20 "select id,percentile(level,0.5) from ${tableName_13} group by id order by id"
    qt_select21 "select id,percentile(level,0.55) from ${tableName_13} group by id order by id"
    qt_select22 "select id,percentile(level,0.805) from ${tableName_13} group by id order by id"

    sql "DROP TABLE IF EXISTS ${tableName_13}"

    
    // PERCENTILE_APPROX
    def tableName_14 = "percentile_approx"

    sql "DROP TABLE IF EXISTS ${tableName_14}"
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName_14} (
            id int,
	    level int
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
          "replication_num" = "1"
        ) 
       """

    sql "INSERT INTO ${tableName_14} values(1,10), (2,8), (2,441) ,(3,10) ,(5,29) ,(6,101)"

    qt_select23 "select id,PERCENTILE_APPROX(level,0.5) from ${tableName_14} group by id order by id"
    qt_select24 "select id,PERCENTILE_APPROX(level,0.55) from ${tableName_14} group by id order by id"
    qt_select25 "select id,PERCENTILE_APPROX(level,0.805) from ${tableName_14} group by id order by id"
    qt_select26 "select id,PERCENTILE_APPROX(level,0.5,2048) from ${tableName_14} group by id order by id"
    qt_select27 "select id,PERCENTILE_APPROX(level,0.55,2048) from ${tableName_14} group by id order by id"
    qt_select28 "select id,PERCENTILE_APPROX(level,0.805,2048) from ${tableName_14} group by id order by id"

    sql "DROP TABLE IF EXISTS ${tableName_14}"
    
    
    // STDDEV STDDEV_POP
    def tableName_15 = "stddev"
 
    sql "DROP TABLE IF EXISTS ${tableName_15}"
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName_15} (
            id int,
	    level int
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
          "replication_num" = "1"
        ) 
    """

    sql "INSERT INTO ${tableName_15} values(1,10), (2,8), (2,441) ,(1,10) ,(3,29) ,(3,101)"

    qt_select29 "select id,stddev(level) from ${tableName_15} group by id order by id"
    qt_select30 "select id,stddev_pop(level) from ${tableName_15} group by id order by id"

    sql "DROP TABLE IF EXISTS ${tableName_15}"

    
    // STDDEV_SAMP
    def tableName_16 = "stddev_samp"

    sql "DROP TABLE IF EXISTS ${tableName_16}" 
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName_16} (
            id int,
	    level int
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
          "replication_num" = "1"
        ) 
        """

    sql "INSERT INTO ${tableName_16} values(1,10), (2,8), (2,441) ,(1,10) ,(3,29) ,(3,101)"
    qt_select31 "select id,stddev_samp(level) from ${tableName_16} group by id order by id"

    sql "DROP TABLE IF EXISTS ${tableName_16}"
   
    
    // SUM
    def tableName_17 = "sum"
  
    sql "DROP TABLE IF EXISTS ${tableName_17}" 
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName_17} (
            id int,
	    level int
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
          "replication_num" = "1"
        ) 
        """
    sql "INSERT INTO ${tableName_17} values(1,10), (2,8), (2,441) ,(1,10) ,(3,29) ,(3,101)"

    qt_select32 "select id,sum(level) from ${tableName_17} group by id order by id"
    qt_select33 "select sum(level) from ${tableName_17}"

    sql "DROP TABLE IF EXISTS ${tableName_17}"
    
    
    // TOPN
    def tableName_18 = "topn"

    sql "DROP TABLE IF EXISTS ${tableName_18}"	 
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName_18} (
            id int,
	    level int
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
          "replication_num" = "1"
        ) 
        """
    sql "INSERT INTO ${tableName_18} values(1,10), (2,18), (2,441) ,(1,10) ,(3,29) ,(3,101),(1,11), (2,18), (2,41) ,(1,13) ,(3,4) ,(3,12)"

    //     // qt_select34 "select id,topn(level,2) from ${tableName_18} group by id order by id"
    //     // qt_select35 "select id,topn(level,2,100) from ${tableName_18} group by id order by id"
    //     // qt_select36 "select topn(level,2,100) from ${tableName_18}"

    sql "DROP TABLE IF EXISTS ${tableName_18}"
    
    
    // VAR_SAMP VARIANCE_SAMP
    def tableName_19 = "var_samp"

    sql "DROP TABLE IF EXISTS ${tableName_19}"
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName_19} (
            id int,
	    level int
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
          "replication_num" = "1"
        ) 
        """
    sql "INSERT INTO ${tableName_19} values(1,10), (2,8), (2,441) ,(1,10) ,(3,29) ,(3,101)"

    qt_select37 "select id,var_samp(level) from ${tableName_19} group by id order by id"
    qt_select38 "select id,variance_samp(level) from ${tableName_19} group by id order by id"

    sql "DROP TABLE IF EXISTS ${tableName_19}"
    
    
    // VARIANCE VAR_POP VARIANCE_POP
    def tableName_20 = "variance"

    sql "DROP TABLE IF EXISTS ${tableName_20}"	 
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName_20} (
            id int,
	    level int
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
          "replication_num" = "1"
        ) 
        """
    sql "INSERT INTO ${tableName_20} values(1,10), (2,8), (2,441) ,(1,10) ,(3,29) ,(3,101)"

    qt_select39 "select id,VARIANCE(level) from ${tableName_20} group by id order by id"
    qt_select40 "select id,VAR_POP(level) from ${tableName_20} group by id order by id"
    qt_select41 "select id,VARIANCE_POP(level) from ${tableName_20} group by id order by id"

    sql "DROP TABLE IF EXISTS ${tableName_20}"
    
    
    // MAX_BY
    def tableName_10 = "max_by"
    
    sql "DROP TABLE IF EXISTS ${tableName_10}" 
    sql """
	CREATE TABLE IF NOT EXISTS ${tableName_10} (
	    id int,
	    level int
	)
	DISTRIBUTED BY HASH(id) BUCKETS 1
	PROPERTIES (
	  "replication_num" = "1"
	) 
	"""
    sql "INSERT INTO ${tableName_10} values(1,10), (2,8), (2,441) ,(3,10) ,(5,29) ,(6,101)"

    qt_select42 "select MAX_BY(id,level) from ${tableName_10}"
    
    sql "DROP TABLE IF EXISTS ${tableName_10}"
    
    // MIN_BY
    def tableName_12 = "min_by"

    sql "DROP TABLE IF EXISTS ${tableName_12}"
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName_12} (
            id int,
	    level int
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
          "replication_num" = "1"
        ) 
        """
    sql "INSERT INTO ${tableName_12} values(1,10), (2,8), (2,441) ,(3,10) ,(5,29) ,(6,101)"

    qt_select43 "select MIN_BY(id,level) from ${tableName_12}"
       
    sql "DROP TABLE IF EXISTS ${tableName_10}"

    qt_select44 """select sum(distinct k1), sum(distinct k2), sum(distinct k3), sum(distinct cast(k4 as largeint)), sum(distinct k5), sum(distinct k8), sum(distinct k9) from nereids_test_query_db.test  """

    qt_select45 """select * from ${tableName_12} order by id,level"""

    qt_select46 """select * from ${tableName_12} where id>=5 and id <=5 and level >10  order by id,level;"""

    qt_select47 """select count(*) from ${tableName_12}"""

    def tableName_21 = "quantile_state_agg_test"

    sql "DROP TABLE IF EXISTS ${tableName_21}"

    sql """
        CREATE TABLE IF NOT EXISTS ${tableName_21} (
        	 `dt` int(11) NULL COMMENT "",
        	 `id` int(11) NULL COMMENT "",
        	 `price` quantile_state QUANTILE_UNION NOT NULL COMMENT ""
        	) ENGINE=OLAP
        	AGGREGATE KEY(`dt`, `id`)
        	COMMENT "OLAP"
        	DISTRIBUTED BY HASH(`dt`) BUCKETS 1
        	PROPERTIES (
                  "replication_num" = "1"
            );
        """
    sql """INSERT INTO ${tableName_21} values(20220201,0, to_quantile_state(1, 2048))"""
    sql """INSERT INTO ${tableName_21} values(20220201,1, to_quantile_state(-1, 2048)),
            (20220201,1, to_quantile_state(0, 2048)),(20220201,1, to_quantile_state(1, 2048)),
            (20220201,1, to_quantile_state(2, 2048)),(20220201,1, to_quantile_state(3, 2048))
        """

    List rows = new ArrayList()
    for (int i = 0; i < 5000; ++i) {
        rows.add([20220202, 2 , i])
    }
    streamLoad {
        table "${tableName_21}"
        set 'label', UUID.randomUUID().toString()
        set 'columns', 'dt, id, price, price=to_quantile_state(price, 2048)'
        inputIterator rows.iterator()
    }

    sql """sync"""
    qt_select48 """select dt, id, quantile_percent(quantile_union(price), 0) from ${tableName_21} group by dt, id order by dt, id"""

    qt_select49 """select dt, id, quantile_percent(quantile_union(price), 0.5) from ${tableName_21} group by dt, id order by dt, id"""
    qt_select50 """select dt, id, quantile_percent(quantile_union(price), 1) from ${tableName_21} group by dt, id order by dt, id"""
}
