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


suite("test_parser_with_none_stopwords", "array_contains_inverted_index"){
    // prepare test table
    def indexTblName = "unicode_test1_arr_no_stopwords"
    sql """ set enable_profile = true; """
    sql """ set enable_inverted_index_query=true; """
    sql """ set enable_common_expr_pushdown=true; """
    sql """ set enable_common_expr_pushdown_for_inverted_index=true; """
    sql "DROP TABLE IF EXISTS ${indexTblName}"
    sql """
	CREATE TABLE IF NOT EXISTS ${indexTblName}(
		`id`int(11)NULL,
		`c` ARRAY<text> NULL,
		INDEX c_idx(`c`) USING INVERTED PROPERTIES("parser"="none", "stopwords" = "none") COMMENT ''
	) ENGINE=OLAP
	DUPLICATE KEY(`id`)
	COMMENT 'OLAP'
	DISTRIBUTED BY HASH(`id`) BUCKETS 1
	PROPERTIES(
 		"replication_allocation" = "tag.location.default: 1"
	);
    """

    sql "INSERT INTO $indexTblName VALUES (1, ['the']), (2, ['there be']), (3, ['then but ']), (4, ['at it was here']), (5, ['the will']), (6, null), (7, []), (8, ['']), (9, [' '])"
    qt_sql "SELECT * FROM $indexTblName WHERE array_contains(c, 'the') ORDER BY id";
    qt_sql "SELECT * FROM $indexTblName WHERE array_contains(c, 'there') ORDER BY id";
    qt_sql "SELECT * FROM $indexTblName WHERE array_contains(c, 'then') ORDER BY id";
    qt_sql "SELECT * FROM $indexTblName WHERE array_contains(c, 'at') ORDER BY id";
    qt_sql "SELECT * FROM $indexTblName WHERE array_contains(c, 'the will') ORDER BY id";
    qt_sql "SELECT * FROM $indexTblName WHERE array_contains(c, 'there be') ORDER BY id";
    qt_sql "SELECT * FROM $indexTblName WHERE array_contains(c, 'then but ') ORDER BY id";
    qt_sql "SELECT * FROM $indexTblName WHERE array_contains(c, 'then but') ORDER BY id";
    qt_sql "SELECT * FROM $indexTblName WHERE array_contains(c, 'at it was here') ORDER BY id";
    qt_sql "SELECT * FROM $indexTblName WHERE array_contains(c, '') ORDER BY id";
    qt_sql "SELECT * FROM $indexTblName WHERE array_contains(c, ' ') ORDER BY id";
    qt_sql "SELECT * FROM $indexTblName WHERE array_contains(c, '[]') ORDER BY id";


    def indexTblName2 = "english_test2_arr"

    sql "DROP TABLE IF EXISTS ${indexTblName2}"
    sql """
	CREATE TABLE IF NOT EXISTS ${indexTblName2}(
		`id`int(11)NULL,
		`c` array<text> NULL,
		INDEX c_idx(`c`) USING INVERTED PROPERTIES("parser"="none", "stopwords" = "none") COMMENT ''
	) ENGINE=OLAP
	DUPLICATE KEY(`id`)
	COMMENT 'OLAP'
	DISTRIBUTED BY HASH(`id`) BUCKETS 1
	PROPERTIES(
                "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql "INSERT INTO $indexTblName VALUES (1, ['the']), (2, ['there be']), (3, ['then but ']), (4, ['at it was here']), (5, ['the will']), (6, null), (7, []), (8, ['']), (9, [' '])"
    qt_sql "SELECT * FROM $indexTblName WHERE array_contains(c, 'the') ORDER BY id";
    qt_sql "SELECT * FROM $indexTblName WHERE array_contains(c, 'there') ORDER BY id";
    qt_sql "SELECT * FROM $indexTblName WHERE array_contains(c, 'then') ORDER BY id";
    qt_sql "SELECT * FROM $indexTblName WHERE array_contains(c, 'at') ORDER BY id";
    qt_sql "SELECT * FROM $indexTblName WHERE array_contains(c, 'the will') ORDER BY id";
    qt_sql "SELECT * FROM $indexTblName WHERE array_contains(c, 'there be') ORDER BY id";
    qt_sql "SELECT * FROM $indexTblName WHERE array_contains(c, 'then but ') ORDER BY id";
    qt_sql "SELECT * FROM $indexTblName WHERE array_contains(c, 'then but') ORDER BY id";
    qt_sql "SELECT * FROM $indexTblName WHERE array_contains(c, 'at it was here') ORDER BY id";
    qt_sql "SELECT * FROM $indexTblName WHERE array_contains(c, '') ORDER BY id";
    qt_sql "SELECT * FROM $indexTblName WHERE array_contains(c, ' ') ORDER BY id";
    qt_sql "SELECT * FROM $indexTblName WHERE array_contains(c, '[]') ORDER BY id";

    def indexTblName3 = "none_test3_arr"

    sql "DROP TABLE IF EXISTS ${indexTblName3}"
    sql """
	CREATE TABLE IF NOT EXISTS ${indexTblName3}(
		`id`int(11)NULL,
		`c` array<text> NULL,
		INDEX c_idx(`c`) USING INVERTED PROPERTIES("parser"="none", "stopwords" = "none") COMMENT ''
	) ENGINE=OLAP
	DUPLICATE KEY(`id`)
	COMMENT 'OLAP'
	DISTRIBUTED BY HASH(`id`) BUCKETS 1
	PROPERTIES(
                "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql "INSERT INTO $indexTblName VALUES (1, ['the']), (2, ['there be']), (3, ['then but ']), (4, ['at it was here']), (5, ['the will']), (6, null), (7, []), (8, ['']), (9, [' '])"
    qt_sql "SELECT * FROM $indexTblName WHERE array_contains(c, 'the') ORDER BY id";
    qt_sql "SELECT * FROM $indexTblName WHERE array_contains(c, 'there') ORDER BY id";
    qt_sql "SELECT * FROM $indexTblName WHERE array_contains(c, 'then') ORDER BY id";
    qt_sql "SELECT * FROM $indexTblName WHERE array_contains(c, 'at') ORDER BY id";
    qt_sql "SELECT * FROM $indexTblName WHERE array_contains(c, 'the will') ORDER BY id";
    qt_sql "SELECT * FROM $indexTblName WHERE array_contains(c, 'there be') ORDER BY id";
    qt_sql "SELECT * FROM $indexTblName WHERE array_contains(c, 'then but ') ORDER BY id";
    qt_sql "SELECT * FROM $indexTblName WHERE array_contains(c, 'then but') ORDER BY id";
    qt_sql "SELECT * FROM $indexTblName WHERE array_contains(c, 'at it was here') ORDER BY id";
    qt_sql "SELECT * FROM $indexTblName WHERE array_contains(c, '') ORDER BY id";
    qt_sql "SELECT * FROM $indexTblName WHERE array_contains(c, ' ') ORDER BY id";
    qt_sql "SELECT * FROM $indexTblName WHERE array_contains(c, '[]') ORDER BY id";

}
