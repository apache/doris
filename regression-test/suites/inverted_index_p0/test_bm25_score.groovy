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

suite("test_bm25_score", "p0") {
    sql "DROP TABLE IF EXISTS test_bm25_score"
     
    sql """
      CREATE TABLE test_bm25_score (
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
      "replication_allocation" = "tag.location.default: 1"
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

    try {
        load_httplogs_data.call('test_bm25_score', 'test_bm25_score', 'true', 'json', 'documents-1000.json')
        sql "sync"

        sql """ set enable_common_expr_pushdown = true; """

        def explain_result = sql """ explain verbose select *, score() as score from test_bm25_score where request match_any 'button.03.gif' order by score limit 10; """
        log.info("Explain verbose result: ${explain_result}")

        def explain_text = explain_result.toString()
        if (explain_text.contains("__DORIS_VIRTUAL_COL__1")) {
            log.info("Found __DORIS_VIRTUAL_COL__1 in explain result")
        } else {
            log.info("__DORIS_VIRTUAL_COL__1 not found in explain result")
        }

        qt_sql """ select count() from test_bm25_score where request match_any 'button.03.gif'; """
        qt_sql """ select *, score() as score from test_bm25_score where request match_any 'button.03.gif' order by score limit 10; """
        qt_sql """ select *, score() as score from test_bm25_score where request match_all 'button.03.gif' order by score limit 10; """
        qt_sql """ select *, score() as score from test_bm25_score where request match_phrase 'button.03.gif' order by score limit 10; """

        sql """ 
            (select *, score() as score from test_bm25_score where request match_any 'button.03.gif' order by score limit 5)
            union all
            (select *, score() as score from test_bm25_score where request match_any 'test' order by score limit 5);
        """

        // Test exception cases for score() function usage
        log.info("Testing exception cases for score() function...")

        test {
            sql """ select score() as score from test_bm25_score limit 10; """
            exception "score() function requires WHERE clause with MATCH function, ORDER BY and LIMIT for optimization"
        }

        test {
            sql """ select score() as score from test_bm25_score where status = 200 limit 10; """
            exception "score() function requires WHERE clause with MATCH function, ORDER BY and LIMIT for optimization"
        }

        test {
            sql """ select score() as score from test_bm25_score where request match_any 'button.03.gif' limit 10; """
            exception "score() function requires WHERE clause with MATCH function, ORDER BY and LIMIT for optimization"
        }

        test {
            sql """ select score() as score from test_bm25_score where request match_any 'button.03.gif' order by score; """
            exception "score() function requires WHERE clause with MATCH function, ORDER BY and LIMIT for optimization"
        }

        test {
            sql """ select score() as score from test_bm25_score where request match_any 'button.03.gif' order by status limit 10; """
            exception "ORDER BY expression must reference a score() function from SELECT clause for push down optimization"
        }

        test {
            sql """ select score() from test_bm25_score; """
            exception "score() function requires WHERE clause with MATCH function, ORDER BY and LIMIT for optimization"
        }

        test {
            sql """ select * from (select score() as s from test_bm25_score where status = 200) t; """
            exception "score() function requires WHERE clause with MATCH function, ORDER BY and LIMIT for optimization"
        }

        test {
            sql """ select score() as score from test_bm25_score where request = 'button.03.gif' order by score() limit 10; """
            exception "WHERE clause must contain at least one MATCH function for score() push down optimization"
        }

        test {
            sql """ select score() as score from test_bm25_score where request match 'button.03.gif'; """
            exception "score() function requires WHERE clause with MATCH function, ORDER BY and LIMIT for optimization"
        }

        test {
            sql """ 
                (select score() as score from test_bm25_score where request match_any 'button.03.gif')
                union all
                (select score() as score from test_bm25_score where request match_any 'test');
            """
            exception "score() function requires WHERE clause with MATCH function, ORDER BY and LIMIT for optimization"
        }

        test {
            sql """ select *, score() from test_bm25_score where request match_any 'button.03.gif' and score() > 0.5 order by score() limit 10; """
            exception "score() function can only be used in SELECT clause, not in WHERE clause"
        }

        test {
            sql """ select *, score() as score from test_bm25_score where request match_any 'button.03.gif' and score() > 0.5; """
            exception "score() function requires WHERE clause with MATCH function, ORDER BY and LIMIT for optimization"
        }

        log.info("All exception test cases completed successfully")
    } finally {
    }

    def timeout = 300000
    def delta_time = 1000
    def alter_res = "null"
    def useTime = 0

    def wait_for_latest_op_on_table_finish = { table_name, OpTimeout ->
        for(int t = delta_time; t <= OpTimeout; t += delta_time){
            alter_res = sql """SHOW ALTER TABLE COLUMN WHERE TableName = "${table_name}" ORDER BY CreateTime DESC LIMIT 1;"""
            alter_res = alter_res.toString()
            if(alter_res.contains("FINISHED")) {
                sleep(10000) // wait change table state to normal
                logger.info(table_name + " latest alter job finished, detail: " + alter_res)
                break
            }
            useTime = t
            sleep(delta_time)
        }
        assertTrue(useTime <= OpTimeout, "wait_for_latest_op_on_table_finish timeout")
    }

    def wait_for_last_build_index_on_table_running = { table_name, OpTimeout ->
        for(int t = delta_time; t <= OpTimeout; t += delta_time){
            alter_res = sql """SHOW BUILD INDEX WHERE TableName = "${table_name}" ORDER BY JobId """

            if (alter_res.size() == 0) {
                logger.info(table_name + " last index job finished")
                return "SKIPPED"
            }
            if (alter_res.size() > 0) {
                def last_job_state = alter_res[alter_res.size()-1][7];
                if (last_job_state == "RUNNING" || last_job_state == "FINISHED") {
                    logger.info(table_name + " last index job running, state: " + last_job_state + ", detail: " + alter_res)
                    return last_job_state;
                }
            }
            useTime = t
            sleep(delta_time)
        }
        logger.info("wait_for_last_build_index_on_table_running debug: " + alter_res)
        assertTrue(useTime <= OpTimeout, "wait_for_last_build_index_on_table_running timeout")
        return "wait_timeout"
    }
    
    if (!isCloudMode()) {
        try {
            sql "DROP TABLE IF EXISTS t1"
            sql """ create table t1(a int, b int, v variant) DISTRIBUTED BY HASH(a) buckets 1 PROPERTIES ("replication_allocation" = "tag.location.default: 1"); """
            sql """ insert into t1 values(2, 2, '{"key": "abc hhh"}'); """

            sql """ sync """
            sql """ set enable_common_expr_pushdown = true; """

            sql """ alter table t1  add index idx_v(v) USING INVERTED PROPERTIES("parser"="english", "support_phrase"=true); """
            wait_for_latest_op_on_table_finish("t1", timeout)
            qt_sql """ select *, score() from t1 where cast(v["key"] as string) match_phrase "abc"  order by score() asc limit 11; """
        } finally {
        }

        try {
            sql "DROP TABLE IF EXISTS t2"
            sql """ create table t2(a int, b int, s text) unique key(a) DISTRIBUTED BY HASH(a) buckets 1 PROPERTIES ("replication_allocation" = "tag.location.default: 1"); """
            sql """ insert into t2 values(3,3, "abc def"); """
            sql """ insert into t2 values(4,4, "hello world"); """

            sql """ sync """
            sql """ set enable_common_expr_pushdown = true; """

            sql """ alter table t2 add index idx_s(s) USING INVERTED PROPERTIES("parser"="english", "support_phrase"=true); """
            wait_for_latest_op_on_table_finish("t2", timeout)
            sql """ build index idx_s on t2; """
            wait_for_last_build_index_on_table_running("t2", timeout)

            sql """ update t2 set s = "this is a test" where a = 3; """
            qt_sql """ select * from t2; """
            qt_sql """ select *, score() from t2 where s match_any  "hello abc abc" order by score() limit 11; """
            qt_sql """ select * from t2 where s match_any  "hello abc abc"; """
        } finally {
        }
    }
}