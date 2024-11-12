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

suite("test_count_on_index_httplogs", "p0") {
    // define a sql table
    def testTable_dup = "httplogs_dup"
    def testTable_unique = "httplogs_unique"
    
    def create_httplogs_dup_table = {testTablex ->
        // multi-line sql
        def result = sql """
                        CREATE TABLE IF NOT EXISTS ${testTablex} (
                          `@timestamp` int(11) NULL,
                          `clientip` varchar(20) NULL,
                          `request` text NULL,
                          `status` int(11) NULL,
                          `size` int(11) NULL,
                          INDEX size_idx (`size`) USING INVERTED COMMENT '',
                          INDEX status_idx (`status`) USING INVERTED COMMENT '',
                          INDEX clientip_idx (`clientip`) USING INVERTED COMMENT '',
                          INDEX request_idx (`request`) USING INVERTED PROPERTIES("parser"="english") COMMENT ''
                        ) ENGINE=OLAP
                        DUPLICATE KEY(`@timestamp`)
                        COMMENT 'OLAP'
                        PARTITION BY RANGE(`@timestamp`)
                        (PARTITION p181998 VALUES [("-2147483648"), ("894225602")),
                        PARTITION p191998 VALUES [("894225602"), ("894830402")),
                        PARTITION p201998 VALUES [("894830402"), ("895435201")),
                        PARTITION p211998 VALUES [("895435201"), ("896040001")),
                        PARTITION p221998 VALUES [("896040001"), ("896644801")),
                        PARTITION p231998 VALUES [("896644801"), ("897249601")),
                        PARTITION p241998 VALUES [("897249601"), ("897854300")),
                        PARTITION p251998 VALUES [("897854300"), ("2147483647")))
                        DISTRIBUTED BY HASH(`@timestamp`) BUCKETS 12
                        PROPERTIES (
                        "replication_allocation" = "tag.location.default: 1",
                        "storage_format" = "V2",
                        "compression" = "ZSTD",
                        "light_schema_change" = "true",
                        "disable_auto_compaction" = "false"
                        );
                        """
    }

    def create_httplogs_unique_table = {testTablex ->
            // multi-line sql
            def result = sql """
                            CREATE TABLE IF NOT EXISTS ${testTablex} (
                              `@timestamp` int(11) NULL,
                              `clientip` varchar(20) NULL,
                              `request` text NULL,
                              `status` int(11) NULL,
                              `size` int(11) NULL,
                              INDEX size_idx (`size`) USING INVERTED COMMENT '',
                              INDEX status_idx (`status`) USING INVERTED COMMENT '',
                              INDEX clientip_idx (`clientip`) USING INVERTED COMMENT '',
                              INDEX request_idx (`request`) USING INVERTED PROPERTIES("parser"="english") COMMENT ''
                            ) ENGINE=OLAP
                            UNIQUE KEY(`@timestamp`)
                            COMMENT 'OLAP'
                            PARTITION BY RANGE(`@timestamp`)
                            (PARTITION p181998 VALUES [("-2147483648"), ("894225602")),
                            PARTITION p191998 VALUES [("894225602"), ("894830402")),
                            PARTITION p201998 VALUES [("894830402"), ("895435201")),
                            PARTITION p211998 VALUES [("895435201"), ("896040001")),
                            PARTITION p221998 VALUES [("896040001"), ("896644801")),
                            PARTITION p231998 VALUES [("896644801"), ("897249601")),
                            PARTITION p241998 VALUES [("897249601"), ("897854300")),
                            PARTITION p251998 VALUES [("897854300"), ("2147483647")))
                            DISTRIBUTED BY HASH(`@timestamp`) BUCKETS 12
                            PROPERTIES (
                            "replication_allocation" = "tag.location.default: 1",
                            "enable_unique_key_merge_on_write" = "true",
                            "storage_format" = "V2",
                            "compression" = "ZSTD",
                            "light_schema_change" = "true",
                            "disable_auto_compaction" = "false"
                            );
                            """
        }
    
    def stream_load_data = {table_name, label, read_flag, format_flag, file_name, ignore_failure=false,
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
        sql "DROP TABLE IF EXISTS ${testTable_dup}"
        sql "DROP TABLE IF EXISTS ${testTable_unique}"

        create_httplogs_dup_table.call(testTable_dup)
        create_httplogs_unique_table.call(testTable_unique)

        stream_load_data.call(testTable_dup, 'test_httplogs_load_count_on_index', 'true', 'json', 'documents-1000.json')
        stream_load_data.call(testTable_unique, 'test_httplogs_load_count_on_index', 'true', 'json', 'documents-1000.json')

        sql "sync"
        sql """ set enable_common_expr_pushdown = true """
        sql """set experimental_enable_nereids_planner=true;"""
        sql """set enable_fallback_to_original_planner=false;"""
        sql """analyze table ${testTable_dup} with sync""";
        // case1: test duplicate table
        def executeSqlWithRetry = { String sqlQuery, int maxRetries = 3, int waitSeconds = 1 ->
            def attempt = 0
            def success = false

            while (attempt < maxRetries && !success) {
                try {
                    explain {
                        // Wait for BE to report every partition's row count
                        sleep(10000)
                        sql(sqlQuery)
                        notContains("cardinality=0")
                    }
                    success = true
                } catch (Exception e) {
                    attempt++
                    log.error("Attempt ${attempt} failed: ${e.message}")
                    if (attempt < maxRetries) {
                        log.info("Retrying... (${attempt + 1}/${maxRetries}) after ${waitSeconds} second(s).")
                        sleep(waitSeconds * 1000)
                    } else {
                        log.error("All ${maxRetries} attempts failed.")
                        throw e
                    }
                }
            }
        }
        // make sure row count stats is not 0 for duplicate table
        executeSqlWithRetry("SELECT COUNT() FROM ${testTable_dup}")
        // make sure row count stats is not 0 for unique table
        sql """analyze table ${testTable_unique} with sync""";
        executeSqlWithRetry("SELECT COUNT() FROM ${testTable_unique}")

        explain {
            sql("select COUNT() from ${testTable_dup} where request match 'GET'")
            contains "pushAggOp=COUNT_ON_INDEX"
        }
        qt_sql(" select COUNT(*) from ${testTable_dup} where request match 'images' ")
        sql " set enable_count_on_index_pushdown=false; "
        qt_sql(" select COUNT(*) from ${testTable_dup} where request match 'images' ")
        sql " set enable_count_on_index_pushdown=true; "

        // case1.1: test duplicate table with null values.
        sql " insert into ${testTable_dup} values(1683964756,null,'GET /images/hm_bg.jpg HTTP/1.0 ',null,null); "
        sql """analyze table ${testTable_dup} with sync""";
        explain {
            sql("select COUNT(request) from ${testTable_dup} where request match 'GET'")
            contains "pushAggOp=COUNT_ON_INDEX"
        }
        qt_sql(" select COUNT(request) from ${testTable_dup} where request match 'images' ")
        qt_sql(" select COUNT(size) from ${testTable_dup} where request match 'images' ")
        sql " set enable_count_on_index_pushdown=false; "
        qt_sql(" select COUNT(request) from ${testTable_dup} where request match 'images' ")
        qt_sql(" select COUNT(size) from ${testTable_dup} where request match 'images' ")
        sql " set enable_count_on_index_pushdown=true; "

        // case1.2: test multiple count on different columns
        explain {
            sql(" select COUNT(size), COUNT(request) from ${testTable_dup} where request match 'GET'; ")
                contains "pushAggOp=COUNT_ON_INDEX"
            }
        qt_sql(" select COUNT(size), COUNT(request) from ${testTable_dup} where request match 'images'; ")
        sql " set enable_count_on_index_pushdown=false; "
        qt_sql(" select COUNT(size), COUNT(request) from ${testTable_dup} where request match 'images'; ")
        sql " set enable_count_on_index_pushdown=true; "

        // case1.3: test count on different column
        explain {
            sql(" select COUNT(size) from ${testTable_dup} where request match 'GET'; ")
                contains "pushAggOp=COUNT_ON_INDEX"
            }
        qt_sql(" select COUNT(size) from ${testTable_dup} where request match 'images'; ")
        sql " set enable_count_on_index_pushdown=false; "
        qt_sql(" select COUNT(size) from ${testTable_dup} where request match 'images'; ")
        sql " set enable_count_on_index_pushdown=true; "

         // case1.4: test count and sum on column
         explain {
             sql(" select COUNT(request),SUM(size) from ${testTable_dup} where request match 'GET'; ")
                 contains "pushAggOp=NONE"
             }
         qt_sql(" select COUNT(request),SUM(size) from ${testTable_dup} where request match 'GET'; ")

        // case2: test mow table
        explain {
            sql("select COUNT() from ${testTable_unique} where request match 'GET'")
            contains "pushAggOp=COUNT_ON_INDEX"
        }
        qt_sql(" select COUNT(*) from ${testTable_unique} where request match 'images' ")
        sql " set enable_count_on_index_pushdown=false; "
        qt_sql(" select COUNT(*) from ${testTable_unique} where request match 'images' ")
        sql " set enable_count_on_index_pushdown=true; "

        // case2.1: test duplicate table with null values.
        sql " insert into ${testTable_unique} values(1683964756,null,'GET /images/hm_bg.jpg HTTP/1.0 ',null,null); "
        sql """analyze table ${testTable_unique} with sync""";
        explain {
            sql("select COUNT(request) from ${testTable_unique} where request match 'GET'")
            contains "pushAggOp=COUNT_ON_INDEX"
        }
        qt_sql(" select COUNT(request) from ${testTable_unique} where request match 'images' ")
        qt_sql(" select COUNT(size) from ${testTable_unique} where request match 'images' ")
        sql " set enable_count_on_index_pushdown=false; "
        qt_sql(" select COUNT(request) from ${testTable_unique} where request match 'images' ")
        qt_sql(" select COUNT(size) from ${testTable_unique} where request match 'images' ")
        sql " set enable_count_on_index_pushdown=true; "

        // case2.2: test multiple count on different columns
        explain {
            sql(" select COUNT(size), COUNT(request) from ${testTable_unique} where request match 'GET'; ")
                contains "pushAggOp=COUNT_ON_INDEX"
            }
        qt_sql(" select COUNT(size), COUNT(request) from ${testTable_unique} where request match 'images'; ")
        sql " set enable_count_on_index_pushdown=false; "
        qt_sql(" select COUNT(size), COUNT(request) from ${testTable_unique} where request match 'images'; ")
        sql " set enable_count_on_index_pushdown=true; "

        // case2.3: test count on different column
        explain {
            sql(" select COUNT(size) from ${testTable_unique} where request match 'GET'; ")
                contains "pushAggOp=COUNT_ON_INDEX"
            }
        qt_sql(" select COUNT(size) from ${testTable_unique} where request match 'images'; ")
        sql " set enable_count_on_index_pushdown=false; "
        qt_sql(" select COUNT(size) from ${testTable_unique} where request match 'images'; ")
        sql " set enable_count_on_index_pushdown=true; "

         // case2.4: test count and sum on column
         explain {
             sql(" select COUNT(request),SUM(size) from ${testTable_unique} where request match 'GET'; ")
                 contains "pushAggOp=NONE"
             }
         qt_sql(" select COUNT(request),SUM(size) from ${testTable_unique} where request match 'GET'; ")

        // case3: test only one column table
        def tableName = 'test_count_on_index_1col'
        sql "DROP TABLE IF EXISTS ${tableName}"
        sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `key_id` varchar(20) NULL COMMENT '',
            INDEX idx_key (`key_id`) USING INVERTED PROPERTIES("parser" = "english")
        ) ENGINE=OLAP
        DUPLICATE KEY(`key_id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`key_id`) BUCKETS 3
        PROPERTIES("replication_num" = "1");
        """

        sql "INSERT INTO ${tableName} values ('dt_bjn001');"
        sql "INSERT INTO ${tableName} values ('dt_bjn002');"
        sql "INSERT INTO ${tableName} values ('dt_bjn003');"

        sql "sync"
        sql "analyze table  ${tableName} with sync;"
        explain {
            sql("select COUNT() from ${tableName} where key_id match 'bjn002'")
            contains "pushAggOp=COUNT_ON_INDEX"
        }
        qt_sql "select COUNT() from ${tableName} where key_id match 'bjn002'"

        // case4: test compound query when inverted_index_query disable
        qt_sql "SELECT  COUNT() from ${testTable_dup} where request = 'images'  or (size = 0 and status > 400)"
        qt_sql "SELECT /*+SET_VAR(enable_inverted_index_query=false) */ COUNT() from ${testTable_dup} where request = 'images'  or (size = 0 and status > 400)"

        // case5: test complex count to testify bad case
        def tableName5 = 'test_count_on_index_bad_case'
        sql "DROP TABLE IF EXISTS ${tableName5}"
        sql """
        CREATE TABLE `${tableName5}` (
          `a` DATE NOT NULL COMMENT '',
          `b` VARCHAR(4096) NULL COMMENT '',
          `c` VARCHAR(4096) NULL COMMENT '',
          `d` VARCHAR(4096) NULL COMMENT '',
          `e` VARCHAR(4096) NULL COMMENT '',
          INDEX idx_a(`a`) USING INVERTED COMMENT '',
          INDEX idx_e(`e`) USING INVERTED COMMENT ''
        ) ENGINE=OLAP
        UNIQUE KEY(`a`, `b`)
        COMMENT ''
        DISTRIBUTED BY HASH(`a`) BUCKETS 3
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
        """
        stream_load_data.call(tableName5, 'test_count_on_index_bad_case', 'true', 'json', 'count-on-index.json')
        def bad_sql = """
        SELECT
          COUNT(CASE WHEN c IN ('c1', 'c2', 'c3') AND d = 'd1' THEN b END) AS num1,
          COUNT(CASE WHEN e = 'e1' AND c IN ('c1', 'c2', 'c3') THEN b END) AS num2
        FROM ${tableName5}
        WHERE a = '2024-07-26'
          AND e = 'e1';
        """
        explain {
            sql("${bad_sql}")
                contains "pushAggOp=NONE"
        }
        qt_sql_bad "${bad_sql}"
        def bad_sql2 = """
        SELECT
            COUNT(cond1) AS num1,
            COUNT(cond2) AS num2
        FROM (
            SELECT
                CASE
                    WHEN c IN ('c1', 'c2', 'c3') AND d = 'd1' THEN b
                END AS cond1,
                CASE
                    WHEN e = 'e1' AND c IN ('c1', 'c2', 'c3') THEN b
                END AS cond2
            FROM
                ${tableName5}
            WHERE
                a = '2024-07-26'
                AND e = 'e1'
        ) AS project;
        """
        explain {
            sql("${bad_sql2}")
                contains "pushAggOp=NONE"
        }
        qt_sql_bad2 "${bad_sql2}"

        // case 6: test select count() from table where a or b;
        def tableName6 = 'test_count_where_or'
        sql "DROP TABLE IF EXISTS ${tableName6}"
        sql """
        CREATE TABLE IF NOT EXISTS ${tableName6} (
            `key_id` varchar(20) NULL COMMENT '',
            `value1` int NULL,
            `value2` bigint NULL,
            INDEX idx_key (`key_id`) USING INVERTED PROPERTIES("parser" = "english"),
            INDEX idx_v1 (`value1`) USING INVERTED,
            INDEX idx_v2 (`value2`) USING INVERTED
        ) ENGINE=OLAP
        DUPLICATE KEY(`key_id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`key_id`) BUCKETS 3
        PROPERTIES("replication_num" = "1");
        """

        sql "INSERT INTO ${tableName6} values ('dt_bjn001', 100, 200);"
        sql "INSERT INTO ${tableName6} values ('dt_bjn002', 300, 400);"
        sql "INSERT INTO ${tableName6} values ('dt_bjn003', 500, 600);"

        sql "sync"
        sql "analyze table  ${tableName6} with sync;"
        explain {
            sql("select COUNT() from ${tableName6} where value1 > 20 or value2 < 10")
            contains "pushAggOp=COUNT_ON_INDEX"
        }
        explain {
            sql("select COUNT(value1) from ${tableName6} where value1 > 20 and value2 > 5")
            contains "pushAggOp=COUNT_ON_INDEX"
        }
        explain {
            sql("select COUNT(value1) from ${tableName6} where value1 > 20 or value2 < 10")
            contains "pushAggOp=NONE"
        }

    } finally {
        //try_sql("DROP TABLE IF EXISTS ${testTable}")
    }
}
