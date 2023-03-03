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

suite("orderby_nullliteral", "query") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_vectorized_engine=true"
    sql "SET enable_fallback_to_original_planner=false" 

    def tableName = "test_orderby_nullliteral"
    sql "DROP TABLE IF EXISTS ${tableName}"
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

    qt_select "select * from ${tableName} order by null"
}