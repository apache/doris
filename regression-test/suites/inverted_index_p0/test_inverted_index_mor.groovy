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


suite("test_inverted_index_mor", "p0"){
    def timeout = 60000
    def delta_time = 1000
    def alter_res = "null"
    def useTime = 0

    def indexTblName = "test_inverted_index_mor"

    sql "DROP TABLE IF EXISTS ${indexTblName}"

    sql """
	CREATE TABLE IF NOT EXISTS ${indexTblName}(
	    `k1` INT,
        `k2` INT,
        `c_int` INT,
        `c_float` FLOAT,
        `c_string` STRING,
        INDEX idx_k2(`k2`) USING INVERTED COMMENT '',
        INDEX idx_c_int(`c_int`) USING INVERTED COMMENT ''
	) ENGINE=OLAP
	UNIQUE KEY(`k1`, `k2`)
	COMMENT 'OLAP'
	DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 1
	PROPERTIES(
 	    "replication_allocation" = "tag.location.default: 1",
        "enable_unique_key_merge_on_write" = "false",
        "disable_auto_compaction" = "true"
	);
    """

    sql """ INSERT INTO $indexTblName VALUES (1, 2, 12, 1.2, '1 2'), (3, 4, 34, 3.4, '3 4'); """
    sql """ INSERT INTO $indexTblName VALUES (11, 12, 1112, 11.12, '11 22'), (13, 14, 1314, 13.14, '13 14'); """
    sql """ set enable_common_expr_pushdown = true; """
    // original data
    qt_11 """ SELECT * FROM $indexTblName ORDER BY k1,k2 """

    //
    qt_12 """ SELECT * FROM $indexTblName WHERE k2 > 2 OR c_int = 12 ORDER BY k1,k2 """

    sql """ INSERT INTO $indexTblName VALUES (1, 2, 112, 11.2, '112'); """
    qt_21 """ SELECT * FROM $indexTblName ORDER BY k1,k2 """
    qt_22 """ SELECT * FROM $indexTblName WHERE k2 > 2 OR c_int = 12 ORDER BY k1,k2 """
    qt_23 """ SELECT * FROM $indexTblName WHERE k2 > 2 OR c_int = 112 ORDER BY k1,k2 """

    // can not add INVERTED INDEX with parser
    test{
        sql """ ALTER TABLE ${indexTblName} ADD INDEX idx_c_string(`c_string`) USING INVERTED PROPERTIES("parser"="english"); """
        exception "errCode = 2, detailMessage = INVERTED index with parser can NOT be used in value columns of UNIQUE_KEYS table with merge_on_write disable. invalid index: idx_c_string"
    }

    // can add INVERTED INDEX without parser
    def success = false;
    try {
        sql """ ALTER TABLE ${indexTblName} ADD INDEX idx_c_string(`c_string`) USING INVERTED; """
        success = true
    } catch(Exception ex) {
        logger.info("ALTER TABLE ${indexTblName} ADD INDEX idx_c_string without parser exception: " + ex)
    }
    assertTrue(success)
}