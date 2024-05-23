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


suite("test_index_empty_string_arr", "array_contains_inverted_index"){
    def timeout = 60000
    def delta_time = 1000
    def alter_res = "null"
    def useTime = 0
    // here some variable to control inverted index query
    sql """ set enable_profile=true"""
    sql """ set enable_pipeline_x_engine=true;"""
    sql """ set enable_inverted_index_query=true"""
    sql """ set enable_common_expr_pushdown=true """
    sql """ set enable_common_expr_pushdown_for_inverted_index=true """

    def indexTblName = "test_index_empty_string_arr"

    sql "DROP TABLE IF EXISTS ${indexTblName}"
    // create 1 replica table
    sql """
	CREATE TABLE IF NOT EXISTS ${indexTblName}(
	    `id` int(11) NOT NULL,
        `a` ARRAY<text> NULL DEFAULT "[]",
        `b` ARRAY<text> NULL DEFAULT "[]",
        INDEX a_idx(`a`) USING INVERTED COMMENT '',
        INDEX b_idx(`b`) USING INVERTED PROPERTIES("parser" = "english") COMMENT ''
	) ENGINE=OLAP
	DUPLICATE KEY(`id`)
	COMMENT 'OLAP'
	DISTRIBUTED BY HASH(`id`) BUCKETS 1
	PROPERTIES(
 	    "replication_allocation" = "tag.location.default: 1"
	);
    """
    
    sql """ 
        INSERT INTO $indexTblName VALUES
        (1, [''], ['1']),
        (2, ['2'], ['']);
    """ 

    qt_sql "SELECT count() FROM $indexTblName WHERE array_contains(a,'');"
    qt_sql "SELECT count() FROM $indexTblName WHERE array_contains(b,'');"
}
