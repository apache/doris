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
    qt_sql "SELECT * FROM $indexTblName WHERE c MATCH '我' ORDER BY id;"
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
    qt_sql "SELECT * FROM $indexTblName2 WHERE c MATCH '我' ORDER BY id;"
    qt_sql "SELECT * FROM $indexTblName2 WHERE c MATCH '清华' ORDER BY id;"
    qt_sql "SELECT * FROM $indexTblName2 WHERE c MATCH '大学' ORDER BY id;"
    qt_sql "SELECT * FROM $indexTblName2 WHERE c MATCH '清华大学' ORDER BY id;"
    qt_sql "SELECT * FROM $indexTblName2 WHERE c MATCH '人民' ORDER BY id;"
}
