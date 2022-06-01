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

suite("sql_action", "aggregate") {

    sql "set enable_vectorized_engine = true"
    sql "set batch_size = 4096"
    
    // APPROX_COUNT_DISTINCT_ACTION
    def tableName_01 = "approx_count_distince"

    try {
        sql "DROP TABLE IF EXISTS ${tableName_01}"

        // multi-line sql
        def result1 = sql """
                        CREATE TABLE IF NOT EXISTS ${tableName_01} (
                            id int,
			    group_type VARCHAR(10)
                        )
                        DISTRIBUTED BY HASH(id) BUCKETS 1
                        PROPERTIES (
                          "replication_num" = "1"
                        ) 
                        """

        // DDL/DML return 1 row and 1 column, the only value is update row count
        assertTrue(result1.size() == 1)
        assertTrue(result1[0].size() == 1)
        assertTrue(result1[0][0] == 0, "Create table should update 0 rows")

        def result2 = sql "INSERT INTO ${tableName_01} values(1,'beijing'), (2,'xian'), (3,'xian')"
        assertTrue(result2.size() == 1)
        assertTrue(result2[0].size() == 1)
        assertTrue(result2[0][0] == 3, "Insert should update 3 rows")

	def result3 = sql "select approx_count_distinct(id) from regression_test.${tableName_01} group by group_type"
	assertTrue(result3.size() == 2)
	assertTrue(result3[0][0] == 1)
	assertTrue(result3[1][0] == 2, "Approx count distinct result is 3")
    } finally {
        try_sql("DROP TABLE IF EXISTS ${tableName_01}")
    }
    
    // AVG
    def tableName_02 = "avg"

    try {
        sql "DROP TABLE IF EXISTS ${tableName_02}"
	 
        def result1 = sql """
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

        // DDL/DML return 1 row and 1 column, the only value is update row count
        assertTrue(result1.size() == 1)
        assertTrue(result1[0].size() == 1)
        assertTrue(result1[0][0] == 0, "Create table should update 0 rows")

        def result2 = sql "INSERT INTO ${tableName_02} values(1,100,'beijing'), (2,70,'xian'), (3,90,'xian') ,(4,100,'beijing') ,(5,140,'xian') ,(6,100,'beijing')"
        assertTrue(result2.size() == 1)
        assertTrue(result2[0].size() == 1)
        assertTrue(result2[0][0] == 6, "Insert should update 6 rows")

        def result3 = sql "select group_type,AVG(level) from regression_test.${tableName_02} group by group_type order by group_type"
        assertTrue(result3.size() == 2)
        assertTrue(result3[0][0] == 'beijing')
        assertTrue(result3[0][1] == 100)
        assertTrue(result3[1][0] == 'xian')
        assertTrue(result3[1][1] == 100, "AVG result is 2")

        def result4 = sql "select group_type,AVG(distinct level) from regression_test.${tableName_02} group by group_type order by group_type"
        assertTrue(result4.size() == 2)
        assertTrue(result4[0][0] == 'beijing')
        assertTrue(result4[0][1] == 100)
        assertTrue(result4[1][0] == 'xian')
        assertTrue(result4[1][1] == 100, "AVG distinct result is 2")
    } finally {
	try_sql("DROP TABLE IF EXISTS ${tableName_02}")
    }

    // BITMAP_UNION
    def tableName_03 = "pv_bitmap"
    def tableName_04 = "bitmap_base"
    try {
        sql "DROP TABLE IF EXISTS ${tableName_03}"

        def result1 = sql """
			CREATE TABLE ${tableName_03} (
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
        // DDL/DML return 1 row and 1 column, the only value is update row count
        assertTrue(result1.size() == 1)
        assertTrue(result1[0].size() == 1)
        assertTrue(result1[0][0] == 0, "Create table should update 0 rows")

	sql "DROP TABLE IF EXISTS ${tableName_04}"

	def result2 = sql """
			CREATE TABLE ${tableName_04} (
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
        // DDL/DML return 1 row and 1 column, the only value is update row count
        assertTrue(result2.size() == 1)
        assertTrue(result2[0].size() == 1)
        assertTrue(result2[0][0] == 0, "Create table should update 0 rows")

        def result3 = sql "INSERT INTO ${tableName_04} values(20220201,'meituan',to_bitmap(10000),10000,'zhangsan'), (20220201,'meituan',to_bitmap(10001),10001,'lisi'), (20220202,'eleme',to_bitmap(10001),10001,'lisi') ,(20220201,'eleme',to_bitmap(10001),10001,'lisi') ,(20220203,'meituan',to_bitmap(10000),10000,'zhangsan') ,(20220203,'meituan',to_bitmap(10001),10001,'lisi')"
        assertTrue(result3.size() == 1)
        assertTrue(result3[0].size() == 1)
        assertTrue(result3[0][0] == 6, "Insert should update 6 rows")

        def result4 = sql "insert into ${tableName_03} select dt,page,user_id_bitmap user_id from ${tableName_04}"
        assertTrue(result4.size() == 1)
        assertTrue(result4[0].size() == 1)
        assertTrue(result4[0][0] == 4, "Insert should update 4 rows")

        def result5 = sql "insert into ${tableName_03} select dt,page,bitmap_union(user_id_bitmap) user_id from ${tableName_04} group by dt,page"
        assertTrue(result5.size() == 1)
        assertTrue(result5[0].size() == 1)
        assertTrue(result5[0][0] == 4, "Insert should update 4 rows")

        def result6 = sql "insert into ${tableName_03} select dt,page,to_bitmap(user_id_int) user_id from ${tableName_04}"
        assertTrue(result6.size() == 1)
        assertTrue(result6[0].size() == 1)
        assertTrue(result6[0][0] == 4, "Insert should update 4 rows")

        def result7 = sql "insert into ${tableName_03} select dt,page,bitmap_hash(user_id_str) user_id from ${tableName_04}"
        assertTrue(result7.size() == 1)
        assertTrue(result7[0].size() == 1)
        assertTrue(result7[0][0] == 4, "Insert should update 4 rows")

	def result8 = sql "select bitmap_union_count(user_id) from  ${tableName_03}"
        assertTrue(result8.size() == 1)
        assertTrue(result8[0].size() == 1)
        assertTrue(result8[0][0] == 3, "Bitmap union count result 3")

        def result9 = sql "select bitmap_count(bitmap_union(user_id)) FROM ${tableName_03}"
        assertTrue(result9.size() == 1)
        assertTrue(result9[0].size() == 1)
        assertTrue(result9[0][0] == 3, "Bitmap count and bitmap union result 3")

    } finally {
        try_sql("DROP TABLE IF EXISTS ${tableName_03}")
        try_sql("DROP TABLE IF EXISTS ${tableName_04}")
    }

    // COUNT
    def tableName_05 = "count"

    try {
        sql "DROP TABLE IF EXISTS ${tableName_05}"
	 
        def result1 = sql """
                        CREATE TABLE IF NOT EXISTS ${tableName_05} (
                            id int,
			    group_type VARCHAR(10)
                        )
                        DISTRIBUTED BY HASH(id) BUCKETS 1
                        PROPERTIES (
                          "replication_num" = "1"
                        ) 
                        """

        // DDL/DML return 1 row and 1 column, the only value is update row count
        assertTrue(result1.size() == 1)
        assertTrue(result1[0].size() == 1)
        assertTrue(result1[0][0] == 0, "Create table should update 0 rows")

        def result2 = sql "INSERT INTO ${tableName_05} values(1,'beijing'), (2,'xian'), (2,'xian') ,(4,'beijing') ,(5,'xian') ,(6,'beijing')"
        assertTrue(result2.size() == 1)
        assertTrue(result2[0].size() == 1)
        assertTrue(result2[0][0] == 6, "Insert should update 6 rows")

        def result3 = sql "select group_type,count(*) from regression_test.${tableName_05} group by group_type order by group_type"
        assertTrue(result3.size() == 2)
        assertTrue(result3[0][0] == 'beijing')
        assertTrue(result3[0][1] == 3)
        assertTrue(result3[1][0] == 'xian')
        assertTrue(result3[1][1] == 3, "Count * result is 2")
        
        def result4 = sql "select group_type,count(id) from regression_test.${tableName_05} group by group_type order by group_type"
        assertTrue(result4.size() == 2)
        assertTrue(result4[0][0] == 'beijing')
        assertTrue(result4[0][1] == 3)
        assertTrue(result4[1][0] == 'xian')
        assertTrue(result4[1][1] == 3, "Count id result is 2")

        def result5 = sql "select group_type,count(distinct id) from regression_test.${tableName_05} group by group_type order by group_type"
        assertTrue(result5.size() == 2)
        assertTrue(result5[0][0] == 'beijing')
        assertTrue(result5[0][1] == 3)
        assertTrue(result5[1][0] == 'xian')
        assertTrue(result5[1][1] == 2, "Count distinct result is 2")
    } finally {
	try_sql("DROP TABLE IF EXISTS ${tableName_05}")
    }
    
    // group_concat
    def tableName_06 = "group_concat"

    try {
        sql "DROP TABLE IF EXISTS ${tableName_06}"
	 
        def result1 = sql """
                        CREATE TABLE IF NOT EXISTS ${tableName_06} (
                            id int,
                            name varchar(10)
                        )
                        DISTRIBUTED BY HASH(id) BUCKETS 1
                        PROPERTIES (
                          "replication_num" = "1"
                        ) 
                        """

        // DDL/DML return 1 row and 1 column, the only value is update row count
        assertTrue(result1.size() == 1)
        assertTrue(result1[0].size() == 1)
        assertTrue(result1[0][0] == 0, "Create table should update 0 rows")

        def result2 = sql "INSERT INTO ${tableName_06} values(1,'beijing'),(2,'beijing'), (1,'xian'), (2,'chengdu') ,(2,'shanghai')"
        assertTrue(result2.size() == 1)
        assertTrue(result2[0].size() == 1)
        assertTrue(result2[0][0] == 5, "Insert should update 5 rows")

        def result3 = sql "select GROUP_CONCAT(name) from ${tableName_06}"
        assertTrue(result3.size() == 1)
        assertTrue(result3[0][0] == 'beijing, xian, beijing, chengdu, shanghai', "Group concat result is 1")
        
        def result4 = sql "select GROUP_CONCAT(name,' ') from ${tableName_06}"
        assertTrue(result4.size() == 1)
        assertTrue(result4[0][0] == 'beijing xian beijing chengdu shanghai', "Group concat result is 1")
        
        def result7 = sql "select GROUP_CONCAT(name,NULL) from ${tableName_06}"
        assertTrue(result7.size() == 1)
        assertTrue(result7[0][0] == null, "Group concat result is 1")
        
        def result8 = sql "select GROUP_CONCAT(name) from ${tableName_06} group by id order by id"
        assertTrue(result8.size() == 2)
        assertTrue(result8[0][0] == 'beijing, xian')
        assertTrue(result8[1][0] == 'beijing, chengdu, shanghai', "Group concat result is 2")
        
        def result9 = sql "select GROUP_CONCAT(name,' ') from ${tableName_06} group by id order by id"
        assertTrue(result9.size() == 2)
        assertTrue(result9[0][0] == 'beijing xian')
        assertTrue(result9[1][0] == 'beijing chengdu shanghai', "Group concat result is 2")
         
        
        def result12 = sql "select GROUP_CONCAT(name,NULL) from ${tableName_06} group by id order by id"
        assertTrue(result12.size() == 2)
        assertTrue(result12[0][0] == null)
        assertTrue(result12[1][0] == null, "Group concat result is 1")
    } finally {
	try_sql("DROP TABLE IF EXISTS ${tableName_06}")
    }
    
    // HLL_UNION_AGG
    def tableName_07 = "hll_union_agg"
    def tableName_08 = "hll_table"

    try {
        sql "DROP TABLE IF EXISTS ${tableName_07}"
	 
        def result1 = sql """
                        CREATE TABLE IF NOT EXISTS ${tableName_07} (
                            id int,
			    group_type VARCHAR(10)
                        )
                        DISTRIBUTED BY HASH(id) BUCKETS 1
                        PROPERTIES (
                          "replication_num" = "1"
                        ) 
                        """

        // DDL/DML return 1 row and 1 column, the only value is update row count
        assertTrue(result1.size() == 1)
        assertTrue(result1[0].size() == 1)
        assertTrue(result1[0][0] == 0, "Create table should update 0 rows")

        def result2 = sql "INSERT INTO ${tableName_07} values(1,'beijing'), (2,'xian'), (2,'xian') ,(4,'beijing') ,(5,'xian') ,(6,'beijing')"
        assertTrue(result2.size() == 1)
        assertTrue(result2[0].size() == 1)
        assertTrue(result2[0][0] == 6, "Insert should update 6 rows")

        sql "DROP TABLE IF EXISTS ${tableName_08}"
	 
        def result3 = sql """
                        CREATE TABLE IF NOT EXISTS ${tableName_08} (
                            id int,
			    group_type hll hll_union
                        )
                        DISTRIBUTED BY HASH(id) BUCKETS 1
                        PROPERTIES (
                          "replication_num" = "1"
                        ) 
                        """

        // DDL/DML return 1 row and 1 column, the only value is update row count
        assertTrue(result3.size() == 1)
        assertTrue(result3[0].size() == 1)
        assertTrue(result3[0][0] == 0, "Create table should update 0 rows")
        
        def result4 = sql "INSERT INTO ${tableName_08} select id,hll_hash(group_type) from ${tableName_07}"
        assertTrue(result4.size() == 1)
        assertTrue(result4[0].size() == 1)
        assertTrue(result4[0][0] == 6, "Insert should update 6 rows")

        def result5 = sql "select id,HLL_UNION_AGG(group_type) from regression_test.${tableName_08} group by id order by id"
        assertTrue(result5.size() == 5)
        assertTrue(result5[0][0] == 1)
        assertTrue(result5[0][1] == 1)
        assertTrue(result5[1][0] == 2)
        assertTrue(result5[1][1] == 1)
        assertTrue(result5[2][0] == 4)
        assertTrue(result5[2][1] == 1)
        assertTrue(result5[3][0] == 5)
        assertTrue(result5[3][1] == 1)
        assertTrue(result5[4][0] == 6)
        assertTrue(result5[4][1] == 1, "HLL_UNION_AGG result is 6")

    } finally {
	try_sql("DROP TABLE IF EXISTS ${tableName_07}")
	try_sql("DROP TABLE IF EXISTS ${tableName_08}")
    }
    
    // MAX
    def tableName_09 = "max"

    try {
        sql "DROP TABLE IF EXISTS ${tableName_09}"
	 
        def result1 = sql """
                        CREATE TABLE IF NOT EXISTS ${tableName_09} (
                            id int,
			    level int
                        )
                        DISTRIBUTED BY HASH(id) BUCKETS 1
                        PROPERTIES (
                          "replication_num" = "1"
                        ) 
                        """

        // DDL/DML return 1 row and 1 column, the only value is update row count
        assertTrue(result1.size() == 1)
        assertTrue(result1[0].size() == 1)
        assertTrue(result1[0][0] == 0, "Create table should update 0 rows")

        def result2 = sql "INSERT INTO ${tableName_09} values(1,10), (2,8), (2,441) ,(3,10) ,(5,29) ,(6,101)"
        assertTrue(result2.size() == 1)
        assertTrue(result2[0].size() == 1)
        assertTrue(result2[0][0] == 6, "Insert should update 6 rows")


        def result3 = sql "select id,MAX(level) from regression_test.${tableName_09} group by id order by id"
        assertTrue(result3.size() == 5)
        assertTrue(result3[0][0] == 1)
        assertTrue(result3[0][1] == 10)
        assertTrue(result3[1][0] == 2)
        assertTrue(result3[1][1] == 441)
        assertTrue(result3[2][0] == 3)
        assertTrue(result3[2][1] == 10)
        assertTrue(result3[3][0] == 5)
        assertTrue(result3[3][1] == 29)
        assertTrue(result3[4][0] == 6)
        assertTrue(result3[4][1] == 101, "Max result is 6")
        
        def result4 = sql "select MAX(level) from regression_test.${tableName_09}"
        assertTrue(result4.size() == 1)
        assertTrue(result4[0][0] == 441, "Max result is 1")

    } finally {
	try_sql("DROP TABLE IF EXISTS ${tableName_09}")
    }
    
  
    
    // MIN
    def tableName_11 = "min"

    try {
        sql "DROP TABLE IF EXISTS ${tableName_11}"
	 
        def result1 = sql """
                        CREATE TABLE IF NOT EXISTS ${tableName_11} (
                            id int,
			    level int
                        )
                        DISTRIBUTED BY HASH(id) BUCKETS 1
                        PROPERTIES (
                          "replication_num" = "1"
                        ) 
                        """

        // DDL/DML return 1 row and 1 column, the only value is update row count
        assertTrue(result1.size() == 1)
        assertTrue(result1[0].size() == 1)
        assertTrue(result1[0][0] == 0, "Create table should update 0 rows")

        def result2 = sql "INSERT INTO ${tableName_11} values(1,10), (2,8), (2,441) ,(3,10) ,(5,29) ,(6,101)"
        assertTrue(result2.size() == 1)
        assertTrue(result2[0].size() == 1)
        assertTrue(result2[0][0] == 6, "Insert should update 6 rows")


        def result3 = sql "select id,MIN(level) from regression_test.${tableName_11} group by id order by id"
        assertTrue(result3.size() == 5)
        assertTrue(result3[0][0] == 1)
        assertTrue(result3[0][1] == 10)
        assertTrue(result3[1][0] == 2)
        assertTrue(result3[1][1] == 8)
        assertTrue(result3[2][0] == 3)
        assertTrue(result3[2][1] == 10)
        assertTrue(result3[3][0] == 5)
        assertTrue(result3[3][1] == 29)
        assertTrue(result3[4][0] == 6)
        assertTrue(result3[4][1] == 101, "Min result is 6")
        
        def result4 = sql "select MIN(level) from regression_test.${tableName_11}"
        assertTrue(result4.size() == 1)
        assertTrue(result4[0][0] == 8, "Min result is 1")

    } finally {
	try_sql("DROP TABLE IF EXISTS ${tableName_11}")
    }
    
    // PERCENTILE
    def tableName_13 = "percentile"

    try {
        sql "DROP TABLE IF EXISTS ${tableName_13}"
	 
        def result1 = sql """
                        CREATE TABLE IF NOT EXISTS ${tableName_13} (
                            id int,
			    level int
                        )
                        DISTRIBUTED BY HASH(id) BUCKETS 1
                        PROPERTIES (
                          "replication_num" = "1"
                        ) 
                        """

        // DDL/DML return 1 row and 1 column, the only value is update row count
        assertTrue(result1.size() == 1)
        assertTrue(result1[0].size() == 1)
        assertTrue(result1[0][0] == 0, "Create table should update 0 rows")

        def result2 = sql "INSERT INTO ${tableName_13} values(1,10), (2,8), (2,441) ,(3,10) ,(5,29) ,(6,101)"
        assertTrue(result2.size() == 1)
        assertTrue(result2[0].size() == 1)
        assertTrue(result2[0][0] == 6, "Insert should update 6 rows")


        def result3 = sql "select id,percentile(level,0.5) from regression_test.${tableName_13} group by id order by id"
        assertTrue(result3.size() == 5)
        assertTrue(result3[0][0] == 1)
        assertTrue(result3[0][1] == 10)
        assertTrue(result3[1][0] == 2)
        assertTrue(result3[1][1] == 224.5)
        assertTrue(result3[2][0] == 3)
        assertTrue(result3[2][1] == 10)
        assertTrue(result3[3][0] == 5)
        assertTrue(result3[3][1] == 29)
        assertTrue(result3[4][0] == 6)
        assertTrue(result3[4][1] == 101, "percentile result is 6")
        
        def result4 = sql "select id,percentile(level,0.55) from regression_test.${tableName_13} group by id order by id"
        assertTrue(result4.size() == 5)
        assertTrue(result4[0][0] == 1)
        assertTrue(result4[0][1] == 10)
        assertTrue(result4[1][0] == 2)
        assertTrue(result4[1][1] == 246.15)
        assertTrue(result4[2][0] == 3)
        assertTrue(result4[2][1] == 10)
        assertTrue(result4[3][0] == 5)
        assertTrue(result4[3][1] == 29)
        assertTrue(result4[4][0] == 6)
        assertTrue(result4[4][1] == 101, "percentile result is 6")
        
        def result5 = sql "select id,percentile(level,0.805) from regression_test.${tableName_13} group by id order by id"
        assertTrue(result5.size() == 5)
        assertTrue(result5[0][0] == 1)
        assertTrue(result5[0][1] == 10)
        assertTrue(result5[1][0] == 2)
        assertTrue(result5[1][1] == 356.565)
        assertTrue(result5[2][0] == 3)
        assertTrue(result5[2][1] == 10)
        assertTrue(result5[3][0] == 5)
        assertTrue(result5[3][1] == 29)
        assertTrue(result5[4][0] == 6)
        assertTrue(result5[4][1] == 101, "percentile result is 6")

    } finally {
	try_sql("DROP TABLE IF EXISTS ${tableName_13}")
    }
    
    // PERCENTILE_APPROX
    def tableName_14 = "percentile_approx"

    try {
        sql "DROP TABLE IF EXISTS ${tableName_14}"
	 
        def result1 = sql """
                        CREATE TABLE IF NOT EXISTS ${tableName_14} (
                            id int,
			    level int
                        )
                        DISTRIBUTED BY HASH(id) BUCKETS 1
                        PROPERTIES (
                          "replication_num" = "1"
                        ) 
                        """

        // DDL/DML return 1 row and 1 column, the only value is update row count
        assertTrue(result1.size() == 1)
        assertTrue(result1[0].size() == 1)
        assertTrue(result1[0][0] == 0, "Create table should update 0 rows")

        def result2 = sql "INSERT INTO ${tableName_14} values(1,10), (2,8), (2,441) ,(3,10) ,(5,29) ,(6,101)"
        assertTrue(result2.size() == 1)
        assertTrue(result2[0].size() == 1)
        assertTrue(result2[0][0] == 6, "Insert should update 6 rows")


        def result3 = sql "select id,PERCENTILE_APPROX(level,0.5) from regression_test.${tableName_14} group by id order by id"
        assertTrue(result3.size() == 5)
        assertTrue(result3[0][0] == 1)
        assertTrue(result3[0][1] == 10)
        assertTrue(result3[1][0] == 2)
        assertTrue(result3[1][1] == 224.5)
        assertTrue(result3[2][0] == 3)
        assertTrue(result3[2][1] == 10)
        assertTrue(result3[3][0] == 5)
        assertTrue(result3[3][1] == 29)
        assertTrue(result3[4][0] == 6)
        assertTrue(result3[4][1] == 101, "percentile result is 6")
        
        def result4 = sql "select id,PERCENTILE_APPROX(level,0.55) from regression_test.${tableName_14} group by id order by id"
        assertTrue(result4.size() == 5)
        assertTrue(result4[0][0] == 1)
        assertTrue(result4[0][1] == 10)
        assertTrue(result4[1][0] == 2)
        assertTrue(result4[1][1] == 267.80001831054688)
        assertTrue(result4[2][0] == 3)
        assertTrue(result4[2][1] == 10)
        assertTrue(result4[3][0] == 5)
        assertTrue(result4[3][1] == 29)
        assertTrue(result4[4][0] == 6)
        assertTrue(result4[4][1] == 101, "percentile result is 6")
        
        def result5 = sql "select id,PERCENTILE_APPROX(level,0.805) from regression_test.${tableName_14} group by id order by id"
        assertTrue(result5.size() == 5)
        assertTrue(result5[0][0] == 1)
        assertTrue(result5[0][1] == 10)
        assertTrue(result5[1][0] == 2)
        assertTrue(result5[1][1] == 441)
        assertTrue(result5[2][0] == 3)
        assertTrue(result5[2][1] == 10)
        assertTrue(result5[3][0] == 5)
        assertTrue(result5[3][1] == 29)
        assertTrue(result5[4][0] == 6)
        assertTrue(result5[4][1] == 101, "percentile result is 6")
        
        def result6 = sql "select id,PERCENTILE_APPROX(level,0.5,2048) from regression_test.${tableName_14} group by id order by id"
        assertTrue(result6.size() == 5)
        assertTrue(result6[0][0] == 1)
        assertTrue(result6[0][1] == 10)
        assertTrue(result6[1][0] == 2)
        assertTrue(result6[1][1] == 224.5)
        assertTrue(result6[2][0] == 3)
        assertTrue(result6[2][1] == 10)
        assertTrue(result6[3][0] == 5)
        assertTrue(result6[3][1] == 29)
        assertTrue(result6[4][0] == 6)
        assertTrue(result6[4][1] == 101, "percentile result is 6")
        
        def result7 = sql "select id,PERCENTILE_APPROX(level,0.55,2048) from regression_test.${tableName_14} group by id order by id"
        assertTrue(result7.size() == 5)
        assertTrue(result7[0][0] == 1)
        assertTrue(result7[0][1] == 10)
        assertTrue(result7[1][0] == 2)
        assertTrue(result7[1][1] == 267.80001831054688)
        assertTrue(result7[2][0] == 3)
        assertTrue(result7[2][1] == 10)
        assertTrue(result7[3][0] == 5)
        assertTrue(result7[3][1] == 29)
        assertTrue(result7[4][0] == 6)
        assertTrue(result7[4][1] == 101, "percentile result is 6")
        
        def result8 = sql "select id,PERCENTILE_APPROX(level,0.805,2048) from regression_test.${tableName_14} group by id order by id"
        assertTrue(result8.size() == 5)
        assertTrue(result8[0][0] == 1)
        assertTrue(result8[0][1] == 10)
        assertTrue(result8[1][0] == 2)
        assertTrue(result8[1][1] == 441)
        assertTrue(result8[2][0] == 3)
        assertTrue(result8[2][1] == 10)
        assertTrue(result8[3][0] == 5)
        assertTrue(result8[3][1] == 29)
        assertTrue(result8[4][0] == 6)
        assertTrue(result8[4][1] == 101, "percentile result is 6")

    } finally {
	try_sql("DROP TABLE IF EXISTS ${tableName_14}")
    }
    
    
    // STDDEV STDDEV_POP
    def tableName_15 = "stddev"

    try {
        sql "DROP TABLE IF EXISTS ${tableName_15}"
	 
        def result1 = sql """
                        CREATE TABLE IF NOT EXISTS ${tableName_15} (
                            id int,
			    level int
                        )
                        DISTRIBUTED BY HASH(id) BUCKETS 1
                        PROPERTIES (
                          "replication_num" = "1"
                        ) 
                        """

        // DDL/DML return 1 row and 1 column, the only value is update row count
        assertTrue(result1.size() == 1)
        assertTrue(result1[0].size() == 1)
        assertTrue(result1[0][0] == 0, "Create table should update 0 rows")

        def result2 = sql "INSERT INTO ${tableName_15} values(1,10), (2,8), (2,441) ,(1,10) ,(3,29) ,(3,101)"
        assertTrue(result2.size() == 1)
        assertTrue(result2[0].size() == 1)
        assertTrue(result2[0][0] == 6, "Insert should update 6 rows")


        def result3 = sql "select id,stddev(level) from regression_test.${tableName_15} group by id order by id"
        assertTrue(result3.size() == 3)
        assertTrue(result3[0][0] == 1)
        assertTrue(result3[0][1] == 0)
        assertTrue(result3[1][0] == 2)
        assertTrue(result3[1][1] == 216.5)
        assertTrue(result3[2][0] == 3)
        assertTrue(result3[2][1] == 36, "stddev result is 3")
        
        def result4 = sql "select id,stddev_pop(level) from regression_test.${tableName_15} group by id order by id"
        assertTrue(result4.size() == 3)
        assertTrue(result4[0][0] == 1)
        assertTrue(result4[0][1] == 0)
        assertTrue(result4[1][0] == 2)
        assertTrue(result4[1][1] == 216.5)
        assertTrue(result4[2][0] == 3)
        assertTrue(result4[2][1] == 36, "Stddev pop result is 3")

    } finally {
	try_sql("DROP TABLE IF EXISTS ${tableName_15}")
    }
    
    // STDDEV_SAMP
    def tableName_16 = "stddev_samp"

    try {
        sql "DROP TABLE IF EXISTS ${tableName_16}"
	 
        def result1 = sql """
                        CREATE TABLE IF NOT EXISTS ${tableName_16} (
                            id int,
			    level int
                        )
                        DISTRIBUTED BY HASH(id) BUCKETS 1
                        PROPERTIES (
                          "replication_num" = "1"
                        ) 
                        """

        // DDL/DML return 1 row and 1 column, the only value is update row count
        assertTrue(result1.size() == 1)
        assertTrue(result1[0].size() == 1)
        assertTrue(result1[0][0] == 0, "Create table should update 0 rows")

        def result2 = sql "INSERT INTO ${tableName_16} values(1,10), (2,8), (2,441) ,(1,10) ,(3,29) ,(3,101)"
        assertTrue(result2.size() == 1)
        assertTrue(result2[0].size() == 1)
        assertTrue(result2[0][0] == 6, "Insert should update 6 rows")


        def result3 = sql "select id,stddev_samp(level) from regression_test.${tableName_16} group by id order by id"
        assertTrue(result3.size() == 3)
        assertTrue(result3[0][0] == 1)
        assertTrue(result3[0][1] == 0)
        assertTrue(result3[1][0] == 2)
        assertTrue(result3[1][1] == 306.17723625377511)
        assertTrue(result3[2][0] == 3)
        assertTrue(result3[2][1] == 50.911688245431421, "stddev result is 3")

    } finally {
	try_sql("DROP TABLE IF EXISTS ${tableName_16}")
    }
    
    // SUM
    def tableName_17 = "sum"

    try {
        sql "DROP TABLE IF EXISTS ${tableName_17}"
	 
        def result1 = sql """
                        CREATE TABLE IF NOT EXISTS ${tableName_17} (
                            id int,
			    level int
                        )
                        DISTRIBUTED BY HASH(id) BUCKETS 1
                        PROPERTIES (
                          "replication_num" = "1"
                        ) 
                        """

        // DDL/DML return 1 row and 1 column, the only value is update row count
        assertTrue(result1.size() == 1)
        assertTrue(result1[0].size() == 1)
        assertTrue(result1[0][0] == 0, "Create table should update 0 rows")

        def result2 = sql "INSERT INTO ${tableName_17} values(1,10), (2,8), (2,441) ,(1,10) ,(3,29) ,(3,101)"
        assertTrue(result2.size() == 1)
        assertTrue(result2[0].size() == 1)
        assertTrue(result2[0][0] == 6, "Insert should update 6 rows")


        def result3 = sql "select id,sum(level) from regression_test.${tableName_17} group by id order by id"
        assertTrue(result3.size() == 3)
        assertTrue(result3[0][0] == 1)
        assertTrue(result3[0][1] == 20)
        assertTrue(result3[1][0] == 2)
        assertTrue(result3[1][1] == 449)
        assertTrue(result3[2][0] == 3)
        assertTrue(result3[2][1] == 130, "Sum result is 3")
        
        def result4 = sql "select sum(level) from regression_test.${tableName_17}"
        assertTrue(result4.size() == 1)
        assertTrue(result4[0][0] == 599, "Sum result is 1")

    } finally {
	try_sql("DROP TABLE IF EXISTS ${tableName_17}")
    }
    
    // TOPN
    def tableName_18 = "topn"

    try {
        sql "DROP TABLE IF EXISTS ${tableName_18}"
	 
        def result1 = sql """
                        CREATE TABLE IF NOT EXISTS ${tableName_18} (
                            id int,
			    level int
                        )
                        DISTRIBUTED BY HASH(id) BUCKETS 1
                        PROPERTIES (
                          "replication_num" = "1"
                        ) 
                        """

        // DDL/DML return 1 row and 1 column, the only value is update row count
        assertTrue(result1.size() == 1)
        assertTrue(result1[0].size() == 1)
        assertTrue(result1[0][0] == 0, "Create table should update 0 rows")

        def result2 = sql "INSERT INTO ${tableName_18} values(1,10), (2,18), (2,441) ,(1,10) ,(3,29) ,(3,101),(1,11), (2,18), (2,41) ,(1,13) ,(3,4) ,(3,12)"
        assertTrue(result2.size() == 1)
        assertTrue(result2[0].size() == 1)
        assertTrue(result2[0][0] == 12, "Insert should update 12 rows")


        def result3 = sql "select id,topn(level,2) from regression_test.${tableName_18} group by id order by id"
        assertTrue(result3.size() == 3)
        assertTrue(result3[0][0] == 1)
        assertTrue(result3[0][1] == '{"10":2,"13":1}')
        assertTrue(result3[1][0] == 2)
        assertTrue(result3[1][1] == '{"18":2,"441":1}')
        assertTrue(result3[2][0] == 3)
        assertTrue(result3[2][1] == '{"4":1,"29":1}', "TopN result is 3")
        
        def result4 = sql "select id,topn(level,2,100) from regression_test.${tableName_18} group by id order by id"
        assertTrue(result4.size() == 3)
        assertTrue(result4[0][0] == 1)
        assertTrue(result4[0][1] == '{"10":2,"13":1}')
        assertTrue(result4[1][0] == 2)
        assertTrue(result4[1][1] == '{"18":2,"441":1}')
        assertTrue(result4[2][0] == 3)
        assertTrue(result4[2][1] == '{"4":1,"29":1}', "TopN result is 3")
        
        def result5 = sql "select topn(level,2,100) from regression_test.${tableName_18}"
        assertTrue(result5.size() == 1)
        assertTrue(result5[0][0] == '{"18":2,"10":2}', "TopN result is 1")

    } finally {
	try_sql("DROP TABLE IF EXISTS ${tableName_18}")
    }
    
    // VAR_SAMP VARIANCE_SAMP
    def tableName_19 = "var_samp"

    try {
        sql "DROP TABLE IF EXISTS ${tableName_19}"
	 
        def result1 = sql """
                        CREATE TABLE IF NOT EXISTS ${tableName_19} (
                            id int,
			    level int
                        )
                        DISTRIBUTED BY HASH(id) BUCKETS 1
                        PROPERTIES (
                          "replication_num" = "1"
                        ) 
                        """

        // DDL/DML return 1 row and 1 column, the only value is update row count
        assertTrue(result1.size() == 1)
        assertTrue(result1[0].size() == 1)
        assertTrue(result1[0][0] == 0, "Create table should update 0 rows")

        def result2 = sql "INSERT INTO ${tableName_19} values(1,10), (2,8), (2,441) ,(1,10) ,(3,29) ,(3,101)"
        assertTrue(result2.size() == 1)
        assertTrue(result2[0].size() == 1)
        assertTrue(result2[0][0] == 6, "Insert should update 6 rows")


        def result3 = sql "select id,var_samp(level) from regression_test.${tableName_19} group by id order by id"
        assertTrue(result3.size() == 3)
        assertTrue(result3[0][0] == 1)
        assertTrue(result3[0][1] == 0)
        assertTrue(result3[1][0] == 2)
        assertTrue(result3[1][1] == 93744.5)
        assertTrue(result3[2][0] == 3)
        assertTrue(result3[2][1] == 2592, "Vari samp result is 3")
 
	def result4 = sql "select id,variance_samp(level) from regression_test.${tableName_19} group by id order by id"
        assertTrue(result4.size() == 3)
        assertTrue(result4[0][0] == 1)
        assertTrue(result4[0][1] == 0)
        assertTrue(result4[1][0] == 2)
        assertTrue(result4[1][1] == 93744.5)
        assertTrue(result4[2][0] == 3)
        assertTrue(result4[2][1] == 2592, "Variance samp result is 3")

    } finally {
	try_sql("DROP TABLE IF EXISTS ${tableName_19}")
    }
    
    // VARIANCE VAR_POP VARIANCE_POP
    def tableName_20 = "variance"

    try {
        sql "DROP TABLE IF EXISTS ${tableName_20}"
	 
        def result1 = sql """
                        CREATE TABLE IF NOT EXISTS ${tableName_20} (
                            id int,
			    level int
                        )
                        DISTRIBUTED BY HASH(id) BUCKETS 1
                        PROPERTIES (
                          "replication_num" = "1"
                        ) 
                        """

        // DDL/DML return 1 row and 1 column, the only value is update row count
        assertTrue(result1.size() == 1)
        assertTrue(result1[0].size() == 1)
        assertTrue(result1[0][0] == 0, "Create table should update 0 rows")

        def result2 = sql "INSERT INTO ${tableName_20} values(1,10), (2,8), (2,441) ,(1,10) ,(3,29) ,(3,101)"
        assertTrue(result2.size() == 1)
        assertTrue(result2[0].size() == 1)
        assertTrue(result2[0][0] == 6, "Insert should update 6 rows")


        def result3 = sql "select id,VARIANCE(level) from regression_test.${tableName_20} group by id order by id"
        assertTrue(result3.size() == 3)
        assertTrue(result3[0][0] == 1)
        assertTrue(result3[0][1] == 0)
        assertTrue(result3[1][0] == 2)
        assertTrue(result3[1][1] == 46872.25)
        assertTrue(result3[2][0] == 3)
        assertTrue(result3[2][1] == 1296, "stddev result is 3")
        
        def result4 = sql "select id,VAR_POP(level) from regression_test.${tableName_20} group by id order by id"
        assertTrue(result4.size() == 3)
        assertTrue(result4[0][0] == 1)
        assertTrue(result4[0][1] == 0)
        assertTrue(result4[1][0] == 2)
        assertTrue(result4[1][1] == 46872.25)
        assertTrue(result4[2][0] == 3)
        assertTrue(result4[2][1] == 1296, "stddev result is 3")
        
        def result5 = sql "select id,VARIANCE_POP(level) from regression_test.${tableName_20} group by id order by id"
        assertTrue(result5.size() == 3)
        assertTrue(result5[0][0] == 1)
        assertTrue(result5[0][1] == 0)
        assertTrue(result5[1][0] == 2)
        assertTrue(result5[1][1] == 46872.25)
        assertTrue(result5[2][0] == 3)
        assertTrue(result5[2][1] == 1296, "stddev result is 3")
 

    } finally {
	try_sql("DROP TABLE IF EXISTS ${tableName_20}")
    }
    
    // MAX_BY
    def tableName_10 = "max_by"

    try {
        sql "DROP TABLE IF EXISTS ${tableName_10}"
	 
        def result1 = sql """
                        CREATE TABLE IF NOT EXISTS ${tableName_10} (
                            id int,
			    level int
                        )
                        DISTRIBUTED BY HASH(id) BUCKETS 1
                        PROPERTIES (
                          "replication_num" = "1"
                        ) 
                        """

        // DDL/DML return 1 row and 1 column, the only value is update row count
        assertTrue(result1.size() == 1)
        assertTrue(result1[0].size() == 1)
        assertTrue(result1[0][0] == 0, "Create table should update 0 rows")

        def result2 = sql "INSERT INTO ${tableName_10} values(1,10), (2,8), (2,441) ,(3,10) ,(5,29) ,(6,101)"
        assertTrue(result2.size() == 1)
        assertTrue(result2[0].size() == 1)
        assertTrue(result2[0][0] == 6, "Insert should update 6 rows")

        def result3 = sql "select MAX_BY(id,level) from regression_test.${tableName_10}"
        assertTrue(result3.size() == 1)
        assertTrue(result3[0][0] == 2, "Max by result is 1")

    } finally {
	try_sql("DROP TABLE IF EXISTS ${tableName_10}")
    }
    
    // MIN_BY
    def tableName_12 = "min_by"

    try {
        sql "DROP TABLE IF EXISTS ${tableName_12}"
     
        def result1 = sql """
                        CREATE TABLE IF NOT EXISTS ${tableName_12} (
                            id int,
                	    level int
                        )
                        DISTRIBUTED BY HASH(id) BUCKETS 1
                        PROPERTIES (
                          "replication_num" = "1"
                        ) 
                        """

        // DDL/DML return 1 row and 1 column, the only value is update row count
        assertTrue(result1.size() == 1)
        assertTrue(result1[0].size() == 1)
        assertTrue(result1[0][0] == 0, "Create table should update 0 rows")

        def result2 = sql "INSERT INTO ${tableName_12} values(1,10), (2,8), (2,441) ,(3,10) ,(5,29) ,(6,101)"
        assertTrue(result2.size() == 1)
        assertTrue(result2[0].size() == 1)
        assertTrue(result2[0][0] == 6, "Insert should update 6 rows")

        def result3 = sql "select MIN_BY(id,level) from regression_test.${tableName_12}"
        assertTrue(result3.size() == 1)
        assertTrue(result3[0][0] == 2, "Min by result is 1")

    } finally {
    	try_sql("DROP TABLE IF EXISTS ${tableName_10}")
    }

}
