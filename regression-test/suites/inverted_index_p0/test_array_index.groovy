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


suite("test_array_index"){
    // prepare test table


    def timeout = 60000
    def delta_time = 1000
    def alter_res = "null"
    def useTime = 0

    def indexTblName = "array_test"

    sql "DROP TABLE IF EXISTS ${indexTblName}"
    // create 1 replica table
    sql """
	CREATE TABLE IF NOT EXISTS ${indexTblName}(
		`id`int(11)NULL,
		`int_array` array<int(20)> NULL,
		`c_array` array<varchar(20)> NULL,
		INDEX c_array_idx(`c_array`) USING INVERTED PROPERTIES("parser"="english") COMMENT 'c_array index',
		INDEX int_array_idx(`int_array`) USING INVERTED COMMENT 'int_array index'
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

    sql "INSERT INTO $indexTblName VALUES (1, [10,20,30], ['i','love','china']), (2, [20,30,40], ['i','love','north korea']), (3, [30,40,50], NULL);"
    sql "INSERT INTO $indexTblName VALUES (4, [40,50,60], NULL);"
    qt_sql "SELECT * FROM $indexTblName WHERE c_array MATCH 'china' ORDER BY id;"
    qt_sql "SELECT * FROM $indexTblName WHERE c_array MATCH 'love' ORDER BY id;"
    qt_sql "SELECT * FROM $indexTblName WHERE c_array MATCH 'north' ORDER BY id;"
    qt_sql "SELECT * FROM $indexTblName WHERE c_array MATCH 'korea' ORDER BY id;"
    qt_sql "SELECT * FROM $indexTblName WHERE int_array element_ge 40 ORDER BY id;"
    qt_sql "SELECT * FROM $indexTblName WHERE int_array element_le 40 ORDER BY id;"
    qt_sql "SELECT * FROM $indexTblName WHERE int_array element_gt 40 ORDER BY id;"
    qt_sql "SELECT * FROM $indexTblName WHERE int_array element_lt 40 ORDER BY id;"
    qt_sql "SELECT * FROM $indexTblName WHERE int_array element_eq 10 ORDER BY id;"
    qt_sql "SELECT * FROM $indexTblName WHERE int_array element_eq 20 ORDER BY id;"
    qt_sql "SELECT * FROM $indexTblName WHERE int_array element_eq 30 ORDER BY id;"
    qt_sql "SELECT * FROM $indexTblName WHERE int_array element_eq 40 ORDER BY id;"
    qt_sql "SELECT * FROM $indexTblName WHERE int_array element_eq 50 ORDER BY id;"
    qt_sql "SELECT * FROM $indexTblName WHERE int_array element_eq 60 ORDER BY id;"

    sql " ALTER TABLE $indexTblName drop index c_array_idx; "
    qt_sql "SELECT * FROM $indexTblName WHERE c_array MATCH 'china' ORDER BY id;"
    qt_sql "SELECT * FROM $indexTblName WHERE c_array MATCH 'love' ORDER BY id;"
    qt_sql "SELECT * FROM $indexTblName WHERE c_array MATCH 'north' ORDER BY id;"
    qt_sql "SELECT * FROM $indexTblName WHERE c_array MATCH 'korea' ORDER BY id;"
}
