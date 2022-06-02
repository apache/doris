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

suite("sql_action", "table") {

    sql "set enable_vectorized_engine = true"
    sql "set batch_size = 4096"
	    
    // explode_bitmap
    def tableName_1 = "explode_bitmap"

    sql "DROP TABLE IF EXISTS ${tableName_1}"
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName_1} (
            id int
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
          "replication_num" = "1"
        ) 
        """
    sql "INSERT INTO ${tableName_1} values(1), (2), (3) ,(4) ,(5) ,(6)"

    qt_select1 "select id from ${tableName_1} order by id"  
    qt_select2 "select id, e1 from ${tableName_1} lateral view explode_bitmap(bitmap_empty()) tmp1 as e1 order by id, e1"
    qt_select3 "select id, e1 from ${tableName_1} lateral view explode_bitmap(bitmap_from_string('1')) tmp1 as e1 order by id, e1"
    qt_select4 "select id, e1 from ${tableName_1} lateral view explode_bitmap(bitmap_from_string('1,2')) tmp1 as e1 order by id, e1"
    qt_select5 "select id, e1 from ${tableName_1} lateral view explode_bitmap(bitmap_from_string('1,1000')) tmp1 as e1 order by id, e1"
    qt_select6 """
	select id, e1, e2 from ${tableName_1}
	lateral view explode_bitmap(bitmap_from_string("1,1000")) tmp1 as e1
	lateral view explode_split("a,b", ",") tmp2 as e2 order by id, e1, e2
	"""
    try_sql "DROP TABLE IF EXISTS ${tableName_1}"
    
    // explode_split
    def tableName_2 = "explode_split"

    sql "DROP TABLE IF EXISTS ${tableName_2}"
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName_2} (
            id int,
            str string
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
          "replication_num" = "1"
        ) 
        """
                
    sql "INSERT INTO ${tableName_2} values(1,''), (2,null), (3,',') ,(4,'1') ,(5,'1,2,3') ,(6,'a, b, c')"

    qt_select7 "select id, e1 from ${tableName_2} lateral view explode_split(str, ',') tmp1 as e1 where id = 1 order by id, e1"
    qt_select8 "select id, e1 from ${tableName_2} lateral view explode_split(str, ',') tmp1 as e1 where id = 2 order by id, e1"
    qt_select9 "select id, e1 from ${tableName_2} lateral view explode_split(str, ',') tmp1 as e1 where id = 3 order by id, e1"
    qt_select10 "select id, e1 from ${tableName_2} lateral view explode_split(str, ',') tmp1 as e1 where id = 4 order by id, e1"
    qt_select11 "select id, e1 from ${tableName_2} lateral view explode_split(str, ',') tmp1 as e1 where id = 5 order by id, e1"
    qt_select12 "select id, e1 from ${tableName_2} lateral view explode_split(str, ',') tmp1 as e1 where id = 6 order by id, e1"

    sql "DROP TABLE IF EXISTS ${tableName_2}"
    
    // explode_json_array
    def tableName_3 = "explode_json_array"

    sql "DROP TABLE IF EXISTS ${tableName_3}"
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName_3} (
            id int
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
          "replication_num" = "1"
        ) 
        """
    sql "INSERT INTO ${tableName_3} values(1), (2), (3)"

    qt_select13 "select id from ${tableName_3} order by id"
    qt_select14 "select id, e1 from ${tableName_3} lateral view explode_json_array_int('[1,2,3]') tmp1 as e1 order by id, e1"
    qt_select15 "select id, e1 from ${tableName_3} lateral view explode_json_array_int('[1,\"b\",3]') tmp1 as e1 order by id, e1"
    qt_select16 "select id, e1 from ${tableName_3} lateral view explode_json_array_int('[]') tmp1 as e1 order by id, e1"
    qt_select17 "select id, e1 from ${tableName_3} lateral view explode_json_array_int('[\"a\",\"b\",\"c\"]') tmp1 as e1 order by id, e1"
    qt_select18 "select id, e1 from ${tableName_3} lateral view explode_json_array_int('{\"a\": 3}') tmp1 as e1 order by id, e1"
    qt_select19 "select id, e1 from ${tableName_3} lateral view explode_json_array_double('[]') tmp1 as e1 order by id, e1"
    qt_select20 "select id, e1 from ${tableName_3} lateral view explode_json_array_double('[1,2,3]') tmp1 as e1 order by id, e1"    
    qt_select21 "select id, e1 from ${tableName_3} lateral view explode_json_array_double('[1,\"b\",3]') tmp1 as e1 order by id, e1"     
    qt_select22 "select id, e1 from ${tableName_3} lateral view explode_json_array_double('[1.0,2.0,3.0]') tmp1 as e1 order by id, e1"       
    qt_select23 "select id, e1 from ${tableName_3} lateral view explode_json_array_double('[\"a\",\"b\",\"c\"]') tmp1 as e1 order by id, e1"               
    qt_select24 "select id, e1 from ${tableName_3} lateral view explode_json_array_double('{\"a\": 3}') tmp1 as e1 order by id, e1"     
    qt_select25 "select id, e1 from ${tableName_3} lateral view explode_json_array_string('[]') tmp1 as e1 order by id, e1"
    qt_select26 "select id, e1 from ${tableName_3} lateral view explode_json_array_string('[1.0,2.0,3.0]') tmp1 as e1 order by id, e1"
    qt_select27 "select id, e1 from ${tableName_3} lateral view explode_json_array_string('[1,\"b\",3]') tmp1 as e1 order by id, e1"
    qt_select28 "select id, e1 from ${tableName_3} lateral view explode_json_array_string('[\"a\",\"b\",\"c\"]') tmp1 as e1 order by id, e1"
    qt_select29 "select id, e1 from ${tableName_3} lateral view explode_json_array_string('{\"a\": 3}') tmp1 as e1 order by id, e1"
    sql "DROP TABLE IF EXISTS ${tableName_3}"
    
    // outer combinator
    def tableName_4 = "outer_combinator"

    sql "DROP TABLE IF EXISTS ${tableName_4}"
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName_4} (
            id int,
            str string
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
          "replication_num" = "1"
        ) 
        """      
         
    qt_select30 "select e1 from (select 1 k1) as t lateral view explode_numbers(0) tmp1 as e1"
    
    sql "DROP TABLE IF EXISTS ${tableName_4}"
    
    	
}
