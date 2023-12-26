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


suite("test_count_on_index_2", "p0"){
    def indexTbName1 = "test_count_on_index_2_index"
    def indexTbName2 = "test_count_on_index_2_no_index"
    def indexTbName3 = "test_count_on_index_2_pk"

    sql "DROP TABLE IF EXISTS ${indexTbName1}"

    sql """
      CREATE TABLE ${indexTbName1} (
      `@timestamp` int(11) NULL COMMENT "",
      `clientip` varchar(20) NULL COMMENT "",
      `request` text NULL COMMENT "",
      `status` int(11) NULL COMMENT "",
      `size` int(11) NULL COMMENT "",
      INDEX clientip_idx (`clientip`) USING INVERTED COMMENT '',
      INDEX request_idx (`request`) USING INVERTED PROPERTIES("parser" = "english", "support_phrase" = "true") COMMENT '',
      INDEX status_idx (`status`) USING INVERTED COMMENT '',
      INDEX size_idx (`size`) USING INVERTED COMMENT ''
      ) ENGINE=OLAP
      DUPLICATE KEY(`@timestamp`)
      COMMENT "OLAP"
      DISTRIBUTED BY RANDOM BUCKETS 1
      PROPERTIES (
      "replication_allocation" = "tag.location.default: 1"
      );
    """

    sql "DROP TABLE IF EXISTS ${indexTbName2}"

    sql """
      CREATE TABLE ${indexTbName2} (
      `@timestamp` int(11) NULL COMMENT "",
      `clientip` varchar(20) NULL COMMENT "",
      `request` text NULL COMMENT "",
      `status` int(11) NULL COMMENT "",
      `size` int(11) NULL COMMENT ""
      ) ENGINE=OLAP
      DUPLICATE KEY(`@timestamp`)
      COMMENT "OLAP"
      DISTRIBUTED BY RANDOM BUCKETS 1
      PROPERTIES (
      "replication_allocation" = "tag.location.default: 1"
      );
    """

    sql "DROP TABLE IF EXISTS ${indexTbName3}"

    sql """
        CREATE TABLE ${indexTbName3} (
            `a` int NULL COMMENT "",
            `b` int NULL COMMENT "",
            `c` int NULL COMMENT ""
        ) ENGINE=OLAP
        DUPLICATE KEY(`a`, `b`, `c`)
        COMMENT "OLAP"
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """ 
        INSERT INTO ${indexTbName3} VALUES
        (1, 1, 1),
        (2, 2, 2),
        (3, 3, 3),
        (4, 4, 4),
        (5, 5, 5),
        (6, 6, 6),
        (7, 7, 7),
        (8, 8, 8),
        (9, 9, 9),
        (10, 10, 10),
        (11, 11, 11),
        (12, 12, 12),
        (13, 13, 13),
        (14, 14, 14),
        (15, 15, 15),
        (16, 16, 16),
        (17, 17, 17),
        (18, 18, 18),
        (19, 19, 19),
        (20, 20, 20),
        (21, 21, 21),
        (22, 22, 22),
        (23, 23, 23),
        (24, 24, 24),
        (25, 25, 25),
        (26, 26, 26),
        (27, 27, 27),
        (28, 28, 28),
        (29, 29, 29),
        (30, 30, 30);
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

    try {
        load_httplogs_data.call(indexTbName1, indexTbName1, 'true', 'json', 'documents-1000.json')
        load_httplogs_data.call(indexTbName2, indexTbName2, 'true', 'json', 'documents-1000.json')

        sql "sync"

        qt_sql """ select count() from ${indexTbName1} where `@timestamp` >= 893964736 and `@timestamp` <= 893966453; """
        qt_sql """ select count() from ${indexTbName2} where `@timestamp` >= 893964736 and `@timestamp` <= 893966453; """

        qt_sql """ select count() from ${indexTbName1} where `@timestamp` >= 893964736 and `@timestamp` <= 893966453 and (request match 'images'); """
        qt_sql """ select count() from ${indexTbName2} where `@timestamp` >= 893964736 and `@timestamp` <= 893966453 and (request match 'images'); """

        qt_sql """ select count() from ${indexTbName1} where `@timestamp` >= 893964736 and `@timestamp` <= 893966453 and (request match 'images' and request match 'english'); """
        qt_sql """ select count() from ${indexTbName2} where `@timestamp` >= 893964736 and `@timestamp` <= 893966453 and (request match 'images' and request match 'english'); """

        qt_sql """ select count() from ${indexTbName1} where `@timestamp` >= 893964736 and `@timestamp` <= 893966453 and (request match 'images' or request match 'english'); """
        qt_sql """ select count() from ${indexTbName2} where `@timestamp` >= 893964736 and `@timestamp` <= 893966453 and (request match 'images' or request match 'english'); """

        qt_sql """ select count() from ${indexTbName1} where `@timestamp` >= 893964736 and `@timestamp` <= 893966453 and (clientip = '247.37.0.0'); """
        qt_sql """ select count() from ${indexTbName2} where `@timestamp` >= 893964736 and `@timestamp` <= 893966453 and (clientip = '247.37.0.0'); """

        qt_sql """ select count() from ${indexTbName1} where `@timestamp` >= 893964736 and `@timestamp` <= 893966453 and (clientip = '247.37.0.0' or clientip = '252.0.0.0'); """
        qt_sql """ select count() from ${indexTbName2} where `@timestamp` >= 893964736 and `@timestamp` <= 893966453 and (clientip = '247.37.0.0' or clientip = '252.0.0.0'); """

        qt_sql """ select count() from ${indexTbName1} where `@timestamp` >= 893964736 and `@timestamp` <= 893966453 and (clientip = '247.37.0.0' and request match 'hm'); """
        qt_sql """ select count() from ${indexTbName2} where `@timestamp` >= 893964736 and `@timestamp` <= 893966453 and (clientip = '247.37.0.0' and request match 'hm'); """

        qt_sql """ select count() from ${indexTbName1} where `@timestamp` >= 893964736 and `@timestamp` <= 893966453 and clientip in ('247.37.0.0', '252.0.0.0'); """
        qt_sql """ select count() from ${indexTbName2} where `@timestamp` >= 893964736 and `@timestamp` <= 893966453 and clientip in ('247.37.0.0', '252.0.0.0'); """

        qt_sql """ select count() from ${indexTbName1} where `@timestamp` >= 893964736 and `@timestamp` <= 893966453 and (status = 200); """
        qt_sql """ select count() from ${indexTbName2} where `@timestamp` >= 893964736 and `@timestamp` <= 893966453 and (status = 200); """

        qt_sql """ select count() from ${indexTbName1} where `@timestamp` >= 893964736 and `@timestamp` <= 893966453 and (status = 200 or status = 304); """
        qt_sql """ select count() from ${indexTbName2} where `@timestamp` >= 893964736 and `@timestamp` <= 893966453 and (status = 200 or status = 304); """

        qt_sql """ select count() from ${indexTbName1} where `@timestamp` >= 893964736 and `@timestamp` <= 893966453 and (clientip = '247.37.0.0' and status = 200); """
        qt_sql """ select count() from ${indexTbName2} where `@timestamp` >= 893964736 and `@timestamp` <= 893966453 and (clientip = '247.37.0.0' and status = 200); """

        qt_sql """ select count() from ${indexTbName1} where `@timestamp` >= 893964736 and `@timestamp` <= 893966453 and status in (200, 304); """
        qt_sql """ select count() from ${indexTbName2} where `@timestamp` >= 893964736 and `@timestamp` <= 893966453 and status in (200, 304); """

        qt_sql """ select count() from ${indexTbName3} where (a >= 5 and a <= 15); """
        qt_sql """ select count() from ${indexTbName3} where (a > 5 and a < 15); """
        qt_sql """ select count() from ${indexTbName3} where (a >= 7 and a <= 27); """
        qt_sql """ select count() from ${indexTbName3} where (a > 7 and a < 27); """
        qt_sql """ select count() from ${indexTbName3} where (a >= 7 and a <= 27) and (b >= 10 and b <= 20); """
        qt_sql """ select count() from ${indexTbName3} where (a >= 7 and a < 27) and (b >= 10 and b < 20); """
        qt_sql """ select count() from ${indexTbName3} where (a >= 7 and a <= 27) and (b >= 10 and b < 20) and (c >= 12 and c < 18); """
        qt_sql """ select count() from ${indexTbName3} where (a >= 2 and a < 28) and (b >= 5 and b < 20) and (c >= 8 and c < 15); """
        qt_sql """ select count() from ${indexTbName3} where (a >= 10 and a < 20) and (b >= 5 and b < 14) and (c >= 16 and c < 25); """
        qt_sql """ select count() from ${indexTbName3} where (a >= 10 and a < 20) and (b >= 5 and b < 16) and (c >= 13 and c < 25); """

    } finally {
        //try_sql("DROP TABLE IF EXISTS ${testTable}")
    }
}