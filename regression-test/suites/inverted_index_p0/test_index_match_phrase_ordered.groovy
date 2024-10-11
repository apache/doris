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


suite("test_index_match_phrase_ordered", "nonConcurrent"){
    def indexTbName1 = "test_index_match_phrase_ordered_1"
    def indexTbName2 = "test_index_match_phrase_ordered_2"

    sql "DROP TABLE IF EXISTS ${indexTbName1}"
    sql "DROP TABLE IF EXISTS ${indexTbName2}"

    sql """
      CREATE TABLE ${indexTbName1} (
      `a` int(11) NULL COMMENT "",
      `b` string NULL COMMENT "",
      INDEX b_idx (`b`) USING INVERTED PROPERTIES("parser" = "english", "support_phrase" = "true") COMMENT ''
      ) ENGINE=OLAP
      DUPLICATE KEY(`a`)
      COMMENT "OLAP"
      DISTRIBUTED BY RANDOM BUCKETS 1
      PROPERTIES (
      "replication_allocation" = "tag.location.default: 1"
      );
    """

    sql """
      CREATE TABLE ${indexTbName2} (
        `@timestamp` int(11) NULL COMMENT "",
        `clientip` varchar(20) NULL COMMENT "",
        `request` text NULL COMMENT "",
        `status` int(11) NULL COMMENT "",
        `size` int(11) NULL COMMENT "",
        INDEX request_idx (`request`) USING INVERTED PROPERTIES("parser" = "english", "support_phrase" = "true") COMMENT ''
      ) ENGINE=OLAP
      DUPLICATE KEY(`@timestamp`)
      COMMENT "OLAP"
      DISTRIBUTED BY RANDOM BUCKETS 1
      PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "disable_auto_compaction" = "true"
      );
    """

    def load_httplogs_data = {table_name, label, read_flag, format_flag, file_name, ignore_failure=false,
                        expected_succ_rows = -1, load_to_single_tablet = 'true' ->
        
        // load the json data
        streamLoad {
            table "${table_name}"
            
            // set http request header params
            set 'label', label + "_" + UUID.randomUUID().toString()
            set 'read_json_by_line', read_flag
            set 'format', format_flag
            file file_name // import json file
            time 10000 // limit inflight 10s
            if (expected_succ_rows >= 0) {
                set 'max_filter_ratio', '1'
            }

            // if declared a check callback, the default check condition will ignore.
            // So you must check all condition
            check { result, exception, startTime, endTime ->
		        if (ignore_failure && expected_succ_rows < 0) { return }
                    if (exception != null) {
                        throw exception
                    }
                    log.info("Stream load result: ${result}".toString())
                    def json = parseJson(result)
                    assertEquals("success", json.Status.toLowerCase())
                    if (expected_succ_rows >= 0) {
                        assertEquals(json.NumberLoadedRows, expected_succ_rows)
                    } else {
                        assertEquals(json.NumberTotalRows, json.NumberLoadedRows + json.NumberUnselectedRows)
                        assertTrue(json.NumberLoadedRows > 0 && json.LoadBytes > 0)
                }
            }
        }
    }

    sql """ INSERT INTO ${indexTbName1} VALUES (1, "the quick brown fox jumped over the lazy dog"); """
    sql """ INSERT INTO ${indexTbName1} VALUES (2, "the quick brown fox jumped over the lazy dog over"); """
    sql """ INSERT INTO ${indexTbName1} VALUES (3, "the quick brown fox jumped over the lazy dog jumped"); """
    sql """ INSERT INTO ${indexTbName1} VALUES (4, "the quick brown fox jumped over the lazy dog fox"); """
    sql """ INSERT INTO ${indexTbName1} VALUES (5, "the quick brown fox jumped over the lazy dog brown"); """
    sql """ INSERT INTO ${indexTbName1} VALUES (6, "the quick brown fox jumped over the lazy dog quick"); """
    sql """ INSERT INTO ${indexTbName1} VALUES (7, "quick brown fox jumped over the lazy dog over"); """
    sql """ INSERT INTO ${indexTbName1} VALUES (8, "quick brown fox jumped over the lazy dog jumped"); """
    sql """ INSERT INTO ${indexTbName1} VALUES (9, "quick brown fox jumped over the lazy dog fox"); """
    sql """ INSERT INTO ${indexTbName1} VALUES (10, "quick brown fox jumped over the lazy dog brown"); """
    sql """ INSERT INTO ${indexTbName1} VALUES (11, "quick brown fox jumped over the lazy dog quick"); """

    try {
        load_httplogs_data.call(indexTbName2, 'test_index_match_phrase_ordered_2', 'true', 'json', 'documents-1000.json')

        sql "sync"
        sql """ set enable_common_expr_pushdown = true; """
        GetDebugPoint().enableDebugPointForAllBEs("VMatchPredicate.execute")

        qt_sql """ select count() from ${indexTbName1} where b match_phrase 'the lazy'; """
        qt_sql """ select count() from ${indexTbName1} where b match_phrase 'the lazy ~1'; """
        qt_sql """ select count() from ${indexTbName1} where b match_phrase 'the lazy ~1+'; """
        qt_sql """ select count() from ${indexTbName1} where b match_phrase 'the lazy ~1+ '; """

        qt_sql """ select count() from ${indexTbName1} where b match_phrase 'the over ~2'; """
        qt_sql """ select count() from ${indexTbName1} where b match_phrase 'the over ~2+'; """

        qt_sql """ select count() from ${indexTbName1} where b match_phrase 'the jumped ~2'; """
        qt_sql """ select count() from ${indexTbName1} where b match_phrase 'the jumped ~2+'; """

        qt_sql """ select count() from ${indexTbName1} where b match_phrase 'the fox ~2'; """
        qt_sql """ select count() from ${indexTbName1} where b match_phrase 'the fox ~2+'; """

        qt_sql """ select count() from ${indexTbName1} where b match_phrase 'the brown ~2'; """
        qt_sql """ select count() from ${indexTbName1} where b match_phrase 'the brown ~2+'; """

        qt_sql """ select count() from ${indexTbName1} where b match_phrase 'the quick ~2'; """
        qt_sql """ select count() from ${indexTbName1} where b match_phrase 'the quick ~2+'; """

        qt_sql """ select count() from ${indexTbName1} where b match_phrase 'the jumped ~3'; """
        qt_sql """ select count() from ${indexTbName1} where b match_phrase 'the jumped ~3+'; """

        qt_sql """ select count() from ${indexTbName1} where b match_phrase 'the fox ~4'; """
        qt_sql """ select count() from ${indexTbName1} where b match_phrase 'the fox ~4+'; """

        qt_sql """ select count() from ${indexTbName1} where b match_phrase 'the brown ~5'; """
        qt_sql """ select count() from ${indexTbName1} where b match_phrase 'the brown ~5+'; """

        qt_sql """ select count() from ${indexTbName1} where b match_phrase 'the quick ~6'; """
        qt_sql """ select count() from ${indexTbName1} where b match_phrase 'the quick ~6+'; """

        qt_sql """ select count() from ${indexTbName2} where request match_phrase 'english/history off.gif ~20+'; """
        qt_sql """ select count() from ${indexTbName2} where request match_phrase 'english/images off.gif ~20+'; """
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("VMatchPredicate.execute")
    }
}