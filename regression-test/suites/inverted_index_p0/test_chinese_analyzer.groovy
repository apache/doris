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


suite("test_chinese_analyzer"){
    // prepare test table


    def timeout = 60000
    def delta_time = 1000
    def alter_res = "null"
    def useTime = 0

    def indexTblName = "chinese_analyzer_test"

    sql "DROP TABLE IF EXISTS ${indexTblName}"
    // create 1 replica table
    sql """
	CREATE TABLE IF NOT EXISTS ${indexTblName}(
		`id`int(11)NULL,
		`c` text NULL,
		INDEX c_idx(`c`) USING INVERTED PROPERTIES("parser"="chinese","parser_mode"="fine_grained") COMMENT ''
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

    sql "INSERT INTO $indexTblName VALUES (1, '我来到北京清华大学'), (2, '我爱你中国'), (3, '人民可以得到更多实惠');"
    qt_sql "SELECT * FROM $indexTblName WHERE c MATCH '我爱你' ORDER BY id;"
    qt_sql "SELECT * FROM $indexTblName WHERE c MATCH '清华' ORDER BY id;"
    qt_sql "SELECT * FROM $indexTblName WHERE c MATCH '大学' ORDER BY id;"
    qt_sql "SELECT * FROM $indexTblName WHERE c MATCH '清华大学' ORDER BY id;"
    qt_sql "SELECT * FROM $indexTblName WHERE c MATCH '人民' ORDER BY id;"

    def indexTblName2 = "chinese_analyzer_test2"

    sql "DROP TABLE IF EXISTS ${indexTblName2}"
    // create 1 replica table
    sql """
	CREATE TABLE IF NOT EXISTS ${indexTblName2}(
		`id`int(11)NULL,
		`c` text NULL,
		INDEX c_idx(`c`) USING INVERTED PROPERTIES("parser"="chinese","parser_mode"="coarse_grained") COMMENT ''
	) ENGINE=OLAP
	DUPLICATE KEY(`id`)
	COMMENT 'OLAP'
	DISTRIBUTED BY HASH(`id`) BUCKETS 1
	PROPERTIES(
                "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql "INSERT INTO $indexTblName2 VALUES (1, '我来到北京清华大学'), (2, '我爱你中国'), (3, '人民可以得到更多实惠');"
    qt_sql "SELECT * FROM $indexTblName2 WHERE c MATCH '我爱你' ORDER BY id;"
    qt_sql "SELECT * FROM $indexTblName2 WHERE c MATCH_PHRASE '我爱你' ORDER BY id;"
    qt_sql "SELECT * FROM $indexTblName2 WHERE c MATCH_PHRASE '我爱你 中国' ORDER BY id;"
    qt_sql "SELECT * FROM $indexTblName2 WHERE c MATCH_PHRASE '北京 大学' ORDER BY id;"
    qt_sql "SELECT * FROM $indexTblName2 WHERE c MATCH '清华' ORDER BY id;"
    qt_sql "SELECT * FROM $indexTblName2 WHERE c MATCH '大学' ORDER BY id;"
    qt_sql "SELECT * FROM $indexTblName2 WHERE c MATCH '清华大学' ORDER BY id;"
    qt_sql "SELECT * FROM $indexTblName2 WHERE c MATCH '人民' ORDER BY id;"

    def indexTblName3 = "chinese_analyzer_test3"

    sql "DROP TABLE IF EXISTS ${indexTblName3}"
    // create 1 replica table
    sql """
	CREATE TABLE IF NOT EXISTS ${indexTblName3}(
		`id`int(11)NULL,
		`c` text NULL,
		INDEX c_idx(`c`) USING INVERTED PROPERTIES("parser"="unicode", "support_phrase"="true") COMMENT ''
	) ENGINE=OLAP
	DUPLICATE KEY(`id`)
	COMMENT 'OLAP'
	DISTRIBUTED BY HASH(`id`) BUCKETS 1
	PROPERTIES(
                "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql """ set enable_common_expr_pushdown = true """

    sql "INSERT INTO $indexTblName3 VALUES (1, '我来到北京清华大学'), (2, '我爱你中国'), (3, '人民可以得到更多实惠'), (4, '陕西省西安市高新区创业大厦A座，我的手机号码是12345678901,邮箱是12345678@qq.com，,ip是1.1.1.1，this information is created automatically.');"
    qt_sql "SELECT * FROM $indexTblName3 WHERE c MATCH_PHRASE '我爱你' ORDER BY id;"
    qt_sql "SELECT * FROM $indexTblName3 WHERE c MATCH_PHRASE '我爱你 中国' ORDER BY id;"
    qt_sql "SELECT * FROM $indexTblName3 WHERE c MATCH_PHRASE '北京 大学' ORDER BY id;"
    qt_sql "SELECT * FROM $indexTblName3 WHERE c MATCH_ALL'我爱你' ORDER BY id;"
    qt_sql "SELECT * FROM $indexTblName3 WHERE c MATCH_ALL '清华' ORDER BY id;"
    qt_sql "SELECT * FROM $indexTblName3 WHERE c MATCH_ALL '大学' ORDER BY id;"
    qt_sql "SELECT * FROM $indexTblName3 WHERE c MATCH_ALL '清华大学' ORDER BY id;"
    qt_sql "SELECT * FROM $indexTblName3 WHERE c MATCH_ALL '人民' ORDER BY id;"
    qt_sql "SELECT * FROM $indexTblName3 WHERE c MATCH_ALL '陕西' ORDER BY id;"
    qt_sql "SELECT * FROM $indexTblName3 WHERE c MATCH_ALL '12345678901' ORDER BY id;"
    qt_sql "SELECT * FROM $indexTblName3 WHERE c MATCH_ALL '12345678' ORDER BY id;"
    qt_sql "SELECT * FROM $indexTblName3 WHERE c MATCH_PHRASE '1.1.1.1' ORDER BY id;"
    qt_sql "SELECT * FROM $indexTblName3 WHERE c MATCH_PHRASE '陕西西安' ORDER BY id;"
    qt_sql "SELECT * FROM $indexTblName3 WHERE c MATCH_PHRASE '陕西省西安市' ORDER BY id;"
    qt_sql "SELECT * FROM $indexTblName3 WHERE c MATCH 'information' ORDER BY id;"
}
