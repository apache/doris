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


suite("test_char_replace_array_contains_arr", "array_contains_inverted_index") {
    // prepare test table
    def indexTblName = "test_char_replace_array_contains_arr"
    // here some variable to control inverted index query
    sql """ set enable_profile=true"""
    sql """ set enable_pipeline_x_engine=true;"""
    sql """ set enable_inverted_index_query=true"""
    sql """ set enable_common_expr_pushdown=true """
    sql """ set enable_common_expr_pushdown_for_inverted_index=true """

    sql "DROP TABLE IF EXISTS ${indexTblName}"
    // create 1 replica table
    sql """
	CREATE TABLE IF NOT EXISTS ${indexTblName}(
		`id` int(11) NULL,
		`a` array<text> NULL,
        `b` array<string> NULL,
        `c` array<string> NULL,
        INDEX a_idx(`a`) USING INVERTED PROPERTIES("parser" = "unicode", "support_phrase" = "true") COMMENT '',
		INDEX b_idx(`b`) USING INVERTED PROPERTIES("parser" = "unicode", "support_phrase" = "true", "char_filter_type" = "char_replace", "char_filter_pattern" = "._", "char_filter_replacement" = " ") COMMENT '',
        INDEX c_idx(`c`) USING INVERTED PROPERTIES("parser" = "unicode", "support_phrase" = "true", "char_filter_type" = "char_replace", "char_filter_pattern" = "._") COMMENT ''
	) ENGINE=OLAP
	DUPLICATE KEY(`id`)
	COMMENT 'OLAP'
	DISTRIBUTED BY HASH(`id`) BUCKETS 1
	PROPERTIES(
 		"replication_allocation" = "tag.location.default: 1"
	);
    """
    
    def var_result = sql "show variables"
    logger.info("show variales result: " + var_result )

    sql """INSERT INTO ${indexTblName} VALUES
        (1, ['GET /images/hm_bg.jpg HTTP/1.0'], ['GET /images/hm_bg.jpg HTTP/1.0'], ['GET /images/hm_bg.jpg HTTP/1.0']),
        (2, ['GET /images/hm_bg.jpg HTTP/1.0'], ['GET /images/hm_bg.jpg HTTP/1.0'], ['GET /images/hm_bg.jpg HTTP/1.0']),
        (3, ['GET /images/hm_bg.jpg HTTP/1.0'], ['GET /images/hm_bg.jpg HTTP/1.0'], ['GET /images/hm_bg.jpg HTTP/1.0']),
        (4, ['GET /images/hm_bg.jpg HTTP/1.0'], ['GET /images/hm_bg.jpg HTTP/1.0'], ['GET /images/hm_bg.jpg HTTP/1.0']),
        (5, ['GET /images/hm_bg.jpg HTTP/1.0'], ['GET /images/hm_bg.jpg HTTP/1.0'], ['GET /images/hm_bg.jpg HTTP/1.0']),
        (6, ['GET /images/hm_bg.jpg HTTP/1.0'], ['GET /images/hm_bg.jpg HTTP/1.0'], ['GET /images/hm_bg.jpg HTTP/1.0']),
        (7, ['GET /images/hm_bg.jpg HTTP/1.0'], ['GET /images/hm_bg.jpg HTTP/1.0'], ['GET /images/hm_bg.jpg HTTP/1.0']),
        (8, ['GET /images/hm_bg.jpg HTTP/1.0'], ['GET /images/hm_bg.jpg HTTP/1.0'], ['GET /images/hm_bg.jpg HTTP/1.0']),
        (9, ['GET /images/hm_bg.jpg HTTP/1.0'], ['GET /images/hm_bg.jpg HTTP/1.0'], ['GET /images/hm_bg.jpg HTTP/1.0']),
        (10, ['GET /images/hm_bg.jpg HTTP/1.0'], ['GET /images/hm_bg.jpg HTTP/1.0'], ['GET /images/hm_bg.jpg HTTP/1.0'])
    """

    qt_sql "SELECT count() FROM ${indexTblName} where array_contains(a, 'hm')";
    qt_sql "SELECT count() FROM ${indexTblName} where array_contains(a, 'bg')";
    qt_sql "SELECT count() FROM ${indexTblName} where array_contains(a, 'jpg')";
    qt_sql "SELECT count() FROM ${indexTblName} where array_contains(a, '1')";
    qt_sql "SELECT count() FROM ${indexTblName} where array_contains(a, '0')";

    qt_sql "SELECT count() FROM ${indexTblName} where array_contains(b, 'hm')";
    qt_sql "SELECT count() FROM ${indexTblName} where array_contains(b, 'bg')";
    qt_sql "SELECT count() FROM ${indexTblName} where array_contains(b, 'jpg')";
    qt_sql "SELECT count() FROM ${indexTblName} where array_contains(b, '1')";
    qt_sql "SELECT count() FROM ${indexTblName} where array_contains(b, '0')";

    qt_sql "SELECT count() FROM ${indexTblName} where array_contains(c, 'hm')";
    qt_sql "SELECT count() FROM ${indexTblName} where array_contains(c, 'bg')";
    qt_sql "SELECT count() FROM ${indexTblName} where array_contains(c, 'jpg')";
    qt_sql "SELECT count() FROM ${indexTblName} where array_contains(c, '1')";
    qt_sql "SELECT count() FROM ${indexTblName} where array_contains(c, '0')";

    qt_sql "SELECT count() FROM ${indexTblName} where array_contains(a, 'hm_bg')";
    qt_sql "SELECT count() FROM ${indexTblName} where array_contains(a, 'hm_bg')";
    qt_sql "SELECT count() FROM ${indexTblName} where array_contains(a, 'hm_bg')";
    qt_sql "SELECT count() FROM ${indexTblName} where array_contains(a, 'hm bg')";
    qt_sql "SELECT count() FROM ${indexTblName} where array_contains(a, 'hm bg')";
    qt_sql "SELECT count() FROM ${indexTblName} where array_contains(a, 'hm bg')";

    qt_sql "SELECT count() FROM ${indexTblName} where array_contains(b, 'hm_bg')";
    qt_sql "SELECT count() FROM ${indexTblName} where array_contains(b, 'hm_bg')";
    qt_sql "SELECT count() FROM ${indexTblName} where array_contains(b, 'hm_bg')";
    qt_sql "SELECT count() FROM ${indexTblName} where array_contains(b, 'hm bg')";
    qt_sql "SELECT count() FROM ${indexTblName} where array_contains(b, 'hm bg')";
    qt_sql "SELECT count() FROM ${indexTblName} where array_contains(b, 'hm bg')";

    qt_sql "SELECT count() FROM ${indexTblName} where array_contains(c, 'hm_bg')";
    qt_sql "SELECT count() FROM ${indexTblName} where array_contains(c, 'hm_bg')";
    qt_sql "SELECT count() FROM ${indexTblName} where array_contains(c, 'hm_bg')";
    qt_sql "SELECT count() FROM ${indexTblName} where array_contains(c, 'hm bg')";
    qt_sql "SELECT count() FROM ${indexTblName} where array_contains(c, 'hm bg')";
    qt_sql "SELECT count() FROM ${indexTblName} where array_contains(c, 'hm bg')";
}
