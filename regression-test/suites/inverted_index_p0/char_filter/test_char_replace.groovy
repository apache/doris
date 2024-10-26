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


suite("test_char_replace") {
    // prepare test table

    def timeout = 60000
    def delta_time = 1000
    def alter_res = "null"
    def useTime = 0

    def indexTblName = "test_char_replace"

    sql "DROP TABLE IF EXISTS ${indexTblName}"
    // create 1 replica table
    sql """
	CREATE TABLE IF NOT EXISTS ${indexTblName}(
		`id` int(11) NULL,
		`a` text NULL,
        `b` string NULL,
        `c` string NULL,
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
    sql """ set enable_common_expr_pushdown = true """
    def var_result = sql "show variables"
    logger.info("show variales result: " + var_result )

    sql """INSERT INTO ${indexTblName} VALUES
        (1, 'GET /images/hm_bg.jpg HTTP/1.0', 'GET /images/hm_bg.jpg HTTP/1.0', 'GET /images/hm_bg.jpg HTTP/1.0'),
        (2, 'GET /images/hm_bg.jpg HTTP/1.0', 'GET /images/hm_bg.jpg HTTP/1.0', 'GET /images/hm_bg.jpg HTTP/1.0'),
        (3, 'GET /images/hm_bg.jpg HTTP/1.0', 'GET /images/hm_bg.jpg HTTP/1.0', 'GET /images/hm_bg.jpg HTTP/1.0'),
        (4, 'GET /images/hm_bg.jpg HTTP/1.0', 'GET /images/hm_bg.jpg HTTP/1.0', 'GET /images/hm_bg.jpg HTTP/1.0'),
        (5, 'GET /images/hm_bg.jpg HTTP/1.0', 'GET /images/hm_bg.jpg HTTP/1.0', 'GET /images/hm_bg.jpg HTTP/1.0'),
        (6, 'GET /images/hm_bg.jpg HTTP/1.0', 'GET /images/hm_bg.jpg HTTP/1.0', 'GET /images/hm_bg.jpg HTTP/1.0'),
        (7, 'GET /images/hm_bg.jpg HTTP/1.0', 'GET /images/hm_bg.jpg HTTP/1.0', 'GET /images/hm_bg.jpg HTTP/1.0'),
        (8, 'GET /images/hm_bg.jpg HTTP/1.0', 'GET /images/hm_bg.jpg HTTP/1.0', 'GET /images/hm_bg.jpg HTTP/1.0'),
        (9, 'GET /images/hm_bg.jpg HTTP/1.0', 'GET /images/hm_bg.jpg HTTP/1.0', 'GET /images/hm_bg.jpg HTTP/1.0'),
        (10, 'GET /images/hm_bg.jpg HTTP/1.0', 'GET /images/hm_bg.jpg HTTP/1.0', 'GET /images/hm_bg.jpg HTTP/1.0')
    """

    qt_sql "SELECT count() FROM ${indexTblName} where a match 'hm'";
    qt_sql "SELECT count() FROM ${indexTblName} where a match 'bg'";
    qt_sql "SELECT count() FROM ${indexTblName} where a match 'jpg'";
    qt_sql "SELECT count() FROM ${indexTblName} where a match '1'";
    qt_sql "SELECT count() FROM ${indexTblName} where a match '0'";
    
    qt_sql "SELECT count() FROM ${indexTblName} where b match 'hm'";
    qt_sql "SELECT count() FROM ${indexTblName} where b match 'bg'";
    qt_sql "SELECT count() FROM ${indexTblName} where b match 'jpg'";
    qt_sql "SELECT count() FROM ${indexTblName} where b match '1'";
    qt_sql "SELECT count() FROM ${indexTblName} where b match '0'";

    qt_sql "SELECT count() FROM ${indexTblName} where c match 'hm'";
    qt_sql "SELECT count() FROM ${indexTblName} where c match 'bg'";
    qt_sql "SELECT count() FROM ${indexTblName} where c match 'jpg'";
    qt_sql "SELECT count() FROM ${indexTblName} where c match '1'";
    qt_sql "SELECT count() FROM ${indexTblName} where c match '0'";

    qt_sql "SELECT count() FROM ${indexTblName} where a match_any 'hm_bg'";
    qt_sql "SELECT count() FROM ${indexTblName} where a match_all 'hm_bg'";
    qt_sql "SELECT count() FROM ${indexTblName} where a match_phrase 'hm_bg'";
    qt_sql "SELECT count() FROM ${indexTblName} where a match_any 'hm bg'";
    qt_sql "SELECT count() FROM ${indexTblName} where a match_all 'hm bg'";
    qt_sql "SELECT count() FROM ${indexTblName} where a match_phrase 'hm bg'";

    qt_sql "SELECT count() FROM ${indexTblName} where b match_any 'hm_bg'";
    qt_sql "SELECT count() FROM ${indexTblName} where b match_all 'hm_bg'";
    qt_sql "SELECT count() FROM ${indexTblName} where b match_phrase 'hm_bg'";
    qt_sql "SELECT count() FROM ${indexTblName} where b match_any 'hm bg'";
    qt_sql "SELECT count() FROM ${indexTblName} where b match_all 'hm bg'";
    qt_sql "SELECT count() FROM ${indexTblName} where b match_phrase 'hm bg'";

    qt_sql "SELECT count() FROM ${indexTblName} where c match_any 'hm_bg'";
    qt_sql "SELECT count() FROM ${indexTblName} where c match_all 'hm_bg'";
    qt_sql "SELECT count() FROM ${indexTblName} where c match_phrase 'hm_bg'";
    qt_sql "SELECT count() FROM ${indexTblName} where c match_any 'hm bg'";
    qt_sql "SELECT count() FROM ${indexTblName} where c match_all 'hm bg'";
    qt_sql "SELECT count() FROM ${indexTblName} where c match_phrase 'hm bg'";
}
