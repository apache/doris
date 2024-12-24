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


suite("test_build_index_fault", "inverted_index, nonConcurrent,p2"){
    // prepare test table
    def timeout = 60000
    def delta_time = 1000
    def alter_res = "null"
    def useTime = 0
    
    def wait_for_latest_op_on_table_finish = { table_name, OpTimeout ->
        for(int t = delta_time; t <= OpTimeout; t += delta_time){
            alter_res = sql """SHOW ALTER TABLE COLUMN WHERE TableName = "${table_name}" ORDER BY CreateTime DESC LIMIT 1;"""
            alter_res = alter_res.toString()
            if(alter_res.contains("FINISHED")) {
                sleep(3000) // wait change table state to normal
                logger.info(table_name + " latest alter job finished, detail: " + alter_res)
                break
            }
            useTime = t
            sleep(delta_time)
        }
        assertTrue(useTime <= OpTimeout, "wait_for_latest_op_on_table_finish timeout")
    }

    def wait_for_build_index_on_partition_finish = { table_name, OpTimeout ->
        for(int t = delta_time; t <= OpTimeout; t += delta_time){
            alter_res = sql """SHOW BUILD INDEX WHERE TableName = "${table_name}";"""
            def expected_finished_num = alter_res.size();
            def finished_num = 0;
            for (int i = 0; i < expected_finished_num; i++) {
                logger.info(table_name + " build index job state: " + alter_res[i][7] + i)
                if (alter_res[i][7] == "FINISHED") {
                    ++finished_num;
                }
            }
            if (finished_num == expected_finished_num) {
                logger.info(table_name + " all build index jobs finished, detail: " + alter_res)
                break
            }
            useTime = t
            sleep(delta_time)
        }
        assertTrue(useTime <= OpTimeout, "wait_for_latest_build_index_on_partition_finish timeout")
    }

    def wait_for_last_build_index_on_table_finish = { table_name, OpTimeout ->
        for(int t = delta_time; t <= OpTimeout; t += delta_time){
            alter_res = sql """SHOW BUILD INDEX WHERE TableName = "${table_name}" ORDER BY JobId """

            if (alter_res.size() > 0) {
                def last_job_state = alter_res[alter_res.size()-1][7];
                if (last_job_state == "FINISHED" || last_job_state == "CANCELLED") {
                    logger.info(table_name + " last index job finished, state: " + last_job_state + ", detail: " + alter_res)
                    return last_job_state;
                }
            }
            useTime = t
            sleep(delta_time)
        }
        logger.info("wait_for_last_build_index_on_table_finish debug: " + alter_res)
        assertTrue(useTime <= OpTimeout, "wait_for_last_build_index_on_table_finish timeout")
        return "wait_timeout"
    }

    def wait_for_last_build_index_on_table_running = { table_name, OpTimeout ->
        for(int t = delta_time; t <= OpTimeout; t += delta_time){
            alter_res = sql """SHOW BUILD INDEX WHERE TableName = "${table_name}" ORDER BY JobId """

            if (alter_res.size() > 0) {
                def last_job_state = alter_res[alter_res.size()-1][7];
                if (last_job_state == "RUNNING") {
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

    def tableName = "hackernews_1m"

    sql "DROP TABLE IF EXISTS ${tableName}"
    // create 1 replica table
    sql """
        CREATE TABLE ${tableName} (
            `id` bigint(20) NULL,
            `deleted` tinyint(4) NULL,
            `type` text NULL,
            `author` text NULL,
            `timestamp` datetime NULL,
            `comment` text NULL,
            `dead` tinyint(4) NULL,
            `parent` bigint(20) NULL,
            `poll` bigint(20) NULL,
            `children` array<bigint(20)> NULL,
            `url` text NULL,
            `score` int(11) NULL,
            `title` text NULL,
            `parts` array<int(11)> NULL,
            `descendants` int(11) NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`id`) BUCKETS 10
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "is_being_synced" = "false",
            "storage_format" = "V2",
            "light_schema_change" = "true",
            "disable_auto_compaction" = "false",
            "enable_single_replica_compaction" = "false"
        );
    """

    // stream load data
    streamLoad {
        table "${tableName}"

        set 'compress_type', 'GZ'

        file """${getS3Url()}/regression/index/hacknernews_1m.csv.gz"""

        time 60000 // limit inflight 60s

        // stream load action will check result, include Success status, and NumberTotalRows == NumberLoadedRows

        // if declared a check callback, the default check condition will ignore.
        // So you must check all condition
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(json.NumberTotalRows, json.NumberLoadedRows)
            assertTrue(json.NumberLoadedRows > 0 && json.LoadBytes > 0)
        }
    }

    sql "sync"

    // check data
    qt_count1 """ SELECT COUNT() from ${tableName}; """

    // ADD INDEX
    sql """ ALTER TABLE ${tableName} ADD INDEX idx_comment (`comment`) USING INVERTED PROPERTIES("support_phrase" = "true", "parser" = "english", "lower_case" = "true") """

    wait_for_latest_op_on_table_finish(tableName, timeout)

    // BUILD INDEX and expect state is RUNNING
    sql """ BUILD INDEX idx_comment ON ${tableName} """
    def state = wait_for_last_build_index_on_table_running(tableName, timeout)
    def result = sql """ SHOW BUILD INDEX WHERE TableName = "${tableName}" ORDER BY JobId """
    assertEquals(result[result.size()-1][1], tableName)
    assertTrue(result[result.size()-1][3].contains("ADD INDEX"))
    assertEquals(result[result.size()-1][7], "RUNNING")

    // CANCEL BUILD INDEX and expect state is CANCELED
    sql """ CANCEL BUILD INDEX ON ${tableName} (${result[result.size()-1][0]}) """
    result = sql """ SHOW BUILD INDEX WHERE TableName = "${tableName}" ORDER BY JobId """
    assertEquals(result[result.size()-1][1], tableName)
    assertTrue(result[result.size()-1][3].contains("ADD INDEX"))
    assertEquals(result[result.size()-1][7], "CANCELLED")
    assertEquals(result[result.size()-1][8], "user cancelled")
    // check data
    qt_count2 """ SELECT COUNT() from ${tableName}; """

    // BUILD INDEX and expect state is FINISHED
    sql """ BUILD INDEX idx_comment ON ${tableName}; """
    state = wait_for_last_build_index_on_table_finish(tableName, timeout)
    assertEquals("FINISHED", state)
    // check data
    qt_count3 """ SELECT COUNT() from ${tableName}; """

    // CANCEL BUILD INDEX in FINISHED state and expect exception
    def success = false;
    try {
        sql """ CANCEL BUILD INDEX ON ${tableName}; """
        success = true
    } catch(Exception ex) {
        logger.info(" CANCEL BUILD INDEX ON ${tableName} exception: " + ex)
    }
    assertFalse(success)

    // BUILD INDEX again and expect state is FINISHED
    sql """ BUILD INDEX idx_comment ON ${tableName}; """
    state = wait_for_last_build_index_on_table_finish(tableName, timeout)
    assertEquals("FINISHED", state)
    // check data
    qt_count4 """ SELECT COUNT() from ${tableName}; """

    // BUILD INDEX with error injection
    sql """ ALTER TABLE ${tableName} ADD INDEX idx_title (`title`) USING INVERTED """
    // enable error_inject for BetaRowset link inverted index file and expect state is RUNNGING
    GetDebugPoint().enableDebugPointForAllBEs("fault_inject::BetaRowset::link_files_to::_link_inverted_index_file")
    sql """ BUILD INDEX idx_title ON ${tableName}; """
    state = wait_for_last_build_index_on_table_finish(tableName, timeout)
    assertEquals("CANCELLED", state)
    // check data
    qt_count5 """ SELECT COUNT() from ${tableName}; """

    // disable error_inject for BetaRowset link inverted index file and expect state is FINISHED
    GetDebugPoint().disableDebugPointForAllBEs("fault_inject::BetaRowset::link_files_to::_link_inverted_index_file")
    // rebuild index
    sql """ BUILD INDEX idx_title ON ${tableName}; """
    state = wait_for_last_build_index_on_table_finish(tableName, timeout)
    assertEquals("FINISHED", state)
    // check data
    qt_count6 """ SELECT COUNT() from ${tableName}; """

    // BUILD INDEX with error injection
    sql """ ALTER TABLE ${tableName} ADD INDEX idx_url (`url`) USING INVERTED """
    GetDebugPoint().enableDebugPointForAllBEs("IndexBuilder::handle_single_rowset")
    sql """ BUILD INDEX idx_url ON ${tableName}; """
    state = wait_for_last_build_index_on_table_finish(tableName, timeout)
    assertEquals("CANCELLED", state)
    // check data
    qt_count7 """ SELECT COUNT() from ${tableName}; """

    GetDebugPoint().disableDebugPointForAllBEs("IndexBuilder::handle_single_rowset")
    // rebuild index
    sql """ BUILD INDEX idx_url ON ${tableName}; """
    state = wait_for_last_build_index_on_table_finish(tableName, timeout)
    assertEquals("FINISHED", state)
    // check data
    qt_count8 """ SELECT COUNT() from ${tableName}; """
}
