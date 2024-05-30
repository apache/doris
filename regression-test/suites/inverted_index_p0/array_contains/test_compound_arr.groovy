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


suite("test_compound_arr", "array_contains_inverted_index"){
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

    def indexTblName = "test_compound_arr"

    sql "DROP TABLE IF EXISTS ${indexTblName}"

    sql """
	CREATE TABLE IF NOT EXISTS ${indexTblName}(
	    `id` int(11) NOT NULL,
        `a` ARRAY<text> NULL DEFAULT "[]",
        `b` ARRAY<text> NULL DEFAULT "[]",
        `c` ARRAY<text> NULL DEFAULT "[]",
        INDEX a_idx(`a`) USING INVERTED COMMENT '',
        INDEX b_idx(`b`) USING INVERTED COMMENT '',
        INDEX c_idx(`c`) USING INVERTED COMMENT ''
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
        (1, ['1'], ['1'], ['1']),
        (2, ['2'], ['2'], ['2']),
        (3, ['3'], ['3'], ['3']),
        (4, ['4'], ['4'], ['4']),
        (5, ['5'], ['5'], ['5']),
        (6, ['6'], ['6'], ['6']),
        (7, ['7'], ['7'], ['7']),
        (8, ['8'], ['8'], ['8']),
        (9, ['9'], ['9'], ['9']),
        (10, ['10'], ['10'], ['10']);
    """ 

    qt_sql """SELECT count() FROM $indexTblName WHERE (id >= 2 AND id < 9) and (array_contains(a, '2') or array_contains(b, '5') and array_contains(c, '5'));"""
    qt_sql "SELECT count() FROM $indexTblName WHERE (id >= 2 AND id < 9) and (array_contains(a, '2') or array_contains(b, '5') or array_contains(c, '6'));"
}
