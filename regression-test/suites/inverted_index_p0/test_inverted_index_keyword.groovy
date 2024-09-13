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


suite("test_inverted_index_keyword"){
    // prepare test table


    def timeout = 60000
    def delta_time = 1000
    def alter_res = "null"
    def useTime = 0

    def indexTblName = "test_inverted_index_keyword"

    sql "DROP TABLE IF EXISTS ${indexTblName}"
    // create 1 replica table
    sql """
	CREATE TABLE IF NOT EXISTS ${indexTblName}(
		`id`int(11)NULL,
		`c` text NULL,
		INDEX c_idx(`c`) USING INVERTED COMMENT ''
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
        (1, '330204195805121025'),
        (2, '36'),
        (2, '330225197806187713'),
        (2, '330227195911020791'),
        (2, '330224196312012744'),
        (2, '330205196003131214'),
        (2, '330224197301242119'),
        (2, '3302哈哈1645676'),
        (2, '330225196202011579'),
        (2, '33022719660610183x'),
        (2, '330225197801043198'),
        (3, '中国'),
        (3, '美国'),
        (3, '英国'),
        (3, '体育'),
        (3, '体育场'),
        (3, '中国人'),
        (3, '北京市'),
        (3, '我在北京市'),
        (3, '我在西安市')
    """
    sql """ set enable_common_expr_pushdown = true; """
    qt_sql "SELECT * FROM ${indexTblName} where c match '330204195805121025'";
    qt_sql "SELECT * FROM ${indexTblName} where c match '36'";
    qt_sql "SELECT * FROM ${indexTblName} where c match '330225197806187713'";
    qt_sql "SELECT * FROM ${indexTblName} where c match '330227195911020791'";
    qt_sql "SELECT * FROM ${indexTblName} where c match '330224196312012744'";
    qt_sql "SELECT * FROM ${indexTblName} where c match '330205196003131214'";
    qt_sql "SELECT * FROM ${indexTblName} where c match '330224197301242119'";
    qt_sql "SELECT * FROM ${indexTblName} where c match '3302哈哈1645676'";
    qt_sql "SELECT * FROM ${indexTblName} where c match '330225196202011579'";
    qt_sql "SELECT * FROM ${indexTblName} where c match '33022719660610183x'";
    qt_sql "SELECT * FROM ${indexTblName} where c match '330225197801043198'";
    qt_sql "SELECT * FROM ${indexTblName} where c match '中国'";
    qt_sql "SELECT * FROM ${indexTblName} where c match '美国'";
    qt_sql "SELECT * FROM ${indexTblName} where c match '英国'";
    qt_sql "SELECT * FROM ${indexTblName} where c match '体育'";
    qt_sql "SELECT * FROM ${indexTblName} where c match '体育场'";
    qt_sql "SELECT * FROM ${indexTblName} where c match '中国人'";
    qt_sql "SELECT * FROM ${indexTblName} where c match '北京市'";
    qt_sql "SELECT * FROM ${indexTblName} where c match '我在北京市'";
    qt_sql "SELECT * FROM ${indexTblName} where c match '我在西安市'";
}
