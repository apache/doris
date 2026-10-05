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

import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_key_topn_merge_block_columns", "p0") {

    def dataFile1 = "test_key_topn_merge_block_columns1.csv"
    def dataFile2 = "test_key_topn_merge_block_columns2.csv"

    // define dup key table1
    def testTable = "tbl_test_key_topn_merge_block_columns"
    sql "DROP TABLE IF EXISTS ${testTable}"
    sql """
        CREATE TABLE IF NOT EXISTS ${testTable} (
            `k1` bigint(11) NULL,
            `c_int` int(11) NULL,
            INDEX idx_c_int (c_int) USING INVERTED
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`) BUCKETS 1
        PROPERTIES("replication_num" = "1", "disable_auto_compaction" = "true");
        """

    // load dataFile1 which should generate TMP columns in query
    streamLoad {
        table testTable
        file dataFile1
        time 60000

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals(10000, json.NumberTotalRows)
            assertEquals(10000, json.NumberLoadedRows)
        }
    }

    // load dataFile2 which should NOT generate TMP columns in query
    streamLoad {
        table testTable
        file dataFile2
        time 60000

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals(10000, json.NumberTotalRows)
            assertEquals(10000, json.NumberLoadedRows)
        }
    }

    // will cause be core if bug not fix by #20820
    sql "SELECT /*+ SET_VAR(topn_opt_limit_threshold=1024) */ * FROM ${testTable} WHERE c_int = 100 or k1 = 1000 ORDER BY k1 LIMIT 10"
    sql "insert into ${testTable} select * from ${testTable}"
    sql "insert into ${testTable} select * from ${testTable}"
    sql "insert into ${testTable} select * from ${testTable}"
    sql """ alter table ${testTable} ADD COLUMN (new_k1 INT DEFAULT '1', new_k2 INT DEFAULT '2');"""
    sql "select * from ${testTable} order by new_k1 limit 1"
}
