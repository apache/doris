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

suite("test_aggregate_collect") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_vectorized_engine=true"
    sql "SET enable_fallback_to_original_planner=false" 
    def tableName = "collect_test"
    def tableCTAS = "collect_test_ctas"
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql "DROP TABLE IF EXISTS ${tableCTAS}"
    sql """
	    CREATE TABLE IF NOT EXISTS ${tableName} (
	        c_int INT,
	        c_string VARCHAR(10),
          c_date Date,
          c_decimal DECIMAL(10, 2),
          c_string_not_null VARCHAR(10) NOT NULL
	    )
	    DISTRIBUTED BY HASH(c_int) BUCKETS 1
	    PROPERTIES (
	      "replication_num" = "1"
	    ) 
    """
    sql "INSERT INTO ${tableName} values(1,'hello','2022-07-04',1.23,'hello'), (2,NULL,NULL,NULL,'hello')"
    sql "INSERT INTO ${tableName} values(1,'hello','2022-07-04',1.23,'hello'), (2,NULL,NULL,NULL,'hello')"

    qt_select "select c_int,collect_list(c_string),collect_list(c_date),collect_list(c_decimal) from ${tableName} group by c_int order by c_int"
    qt_select "select c_int,collect_set(c_string),collect_set(c_date),collect_set(c_decimal) from ${tableName} group by c_int order by c_int"

    // test without GROUP BY
    qt_select "select collect_list(c_string),collect_list(c_string_not_null) from ${tableName}"
    qt_select "select collect_set(c_string),collect_set(c_string_not_null) from ${tableName}"

    sql """ CREATE TABLE ${tableCTAS} PROPERTIES("replication_num" = "1") AS SELECT 1,collect_list(c_int),collect_set(c_string),collect_list(c_date),collect_set(c_decimal),collect_list(c_string_not_null) FROM ${tableName} """
    qt_select "SELECT * from ${tableCTAS}"

    // topn_array
    def tableName_12 = "topn_array"

    sql "DROP TABLE IF EXISTS ${tableName_12}"
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName_12} (
            id int,
	          level int,
            dt datev2,
            num decimal(27,9)
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
          "replication_num" = "1"
        ) 
        """
    sql "INSERT INTO ${tableName_12} values(1,10,'2022-11-1',6.8754576), (2,8,'2022-11-3',0.576), (2,10,'2022-11-2',1.234) ,(3,10,'2022-11-2',0.576) ,(5,29,'2022-11-2',6.8754576) ,(6,8,'2022-11-1',6.8754576)"

    // Nereids does't support array function
    // qt_select43 "select topn_array(level,2) from ${tableName_12}"
    // Nereids does't support array function
    // qt_select44 "select topn_array(level,2,100) from ${tableName_12}" 
    // Nereids does't support array function
    // qt_select45 "select topn_array(dt,2,100) from ${tableName_12}"  
    // Nereids does't support array function
    // qt_select46 "select topn_array(num,2,100) from ${tableName_12}"  
    sql "DROP TABLE IF EXISTS ${tableName_12}"    
}
