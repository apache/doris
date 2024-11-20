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

suite("test_index_io_context", "nonConcurrent") {
    def tableName1 = "test_index_io_context1"
    def tableName2 = "test_index_io_context2"

    def create_table = {table_name, index_format ->
        sql """ DROP TABLE IF EXISTS ${table_name} """
        sql """
            CREATE TABLE IF NOT EXISTS ${table_name} (
                `@timestamp` int(11) NULL COMMENT "",
                `clientip` varchar(20) NULL COMMENT "",
                `request` text NULL COMMENT "",
                `status` int(11) NULL COMMENT "",
                `size` int(11) NULL COMMENT "",
                INDEX request_idx (`request`) USING INVERTED PROPERTIES("parser" = "english", "support_phrase" = "true") COMMENT ''
            )
            DISTRIBUTED BY HASH(`@timestamp`) PROPERTIES(
                "replication_num" = "1",
                "disable_auto_compaction" = "true",
                "inverted_index_storage_format" = "${index_format}"
            );
        """
    }

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
            }
        }
    }

    try {
        create_table(tableName1, "V1");
        create_table(tableName2, "V2");

        load_httplogs_data.call(tableName1, 'test_index_io_context1', 'true', 'json', 'documents-1000.json')
        load_httplogs_data.call(tableName2, 'test_index_io_context2', 'true', 'json', 'documents-1000.json')

        sql "sync"
        sql """ set enable_common_expr_pushdown = true; """

        try {
            GetDebugPoint().enableDebugPointForAllBEs("DorisFSDirectory::FSIndexInput::readInternal")

            qt_sql """ select count() from ${tableName1} where request match_any 'ticket_quest_bg2.jpg'; """
            qt_sql """ select count() from ${tableName1} where request match_any 'ticket_quest_bg2.jpg'; """
            qt_sql """ select count() from ${tableName1} where request match_any 'ticket_quest_bg2.jpg'; """
            qt_sql """ select count() from ${tableName2} where request match_any 'ticket_quest_bg2.jpg'; """
            qt_sql """ select count() from ${tableName2} where request match_any 'ticket_quest_bg2.jpg'; """
            qt_sql """ select count() from ${tableName2} where request match_any 'ticket_quest_bg2.jpg'; """
            qt_sql """ select count() from ${tableName1} where request match_all 'ticket_quest_bg2.jpg'; """
            qt_sql """ select count() from ${tableName1} where request match_all 'ticket_quest_bg2.jpg'; """
            qt_sql """ select count() from ${tableName1} where request match_all 'ticket_quest_bg2.jpg'; """
            qt_sql """ select count() from ${tableName2} where request match_all 'ticket_quest_bg2.jpg'; """
            qt_sql """ select count() from ${tableName2} where request match_all 'ticket_quest_bg2.jpg'; """
            qt_sql """ select count() from ${tableName2} where request match_all 'ticket_quest_bg2.jpg'; """
            qt_sql """ select count() from ${tableName1} where request match_phrase 'ticket_quest_bg2.jpg'; """
            qt_sql """ select count() from ${tableName1} where request match_phrase 'ticket_quest_bg2.jpg'; """
            qt_sql """ select count() from ${tableName1} where request match_phrase 'ticket_quest_bg2.jpg'; """
            qt_sql """ select count() from ${tableName2} where request match_phrase 'ticket_quest_bg2.jpg'; """
            qt_sql """ select count() from ${tableName2} where request match_phrase 'ticket_quest_bg2.jpg'; """
            qt_sql """ select count() from ${tableName2} where request match_phrase 'ticket_quest_bg2.jpg'; """
            qt_sql """ select count() from ${tableName1} where request match_phrase 'ticket_quest_bg2.jpg ~10+'; """
            qt_sql """ select count() from ${tableName1} where request match_phrase 'ticket_quest_bg2.jpg ~10+'; """
            qt_sql """ select count() from ${tableName1} where request match_phrase 'ticket_quest_bg2.jpg ~10+'; """
            qt_sql """ select count() from ${tableName2} where request match_phrase 'ticket_quest_bg2.jpg ~10+'; """
            qt_sql """ select count() from ${tableName2} where request match_phrase 'ticket_quest_bg2.jpg ~10+'; """
            qt_sql """ select count() from ${tableName2} where request match_phrase 'ticket_quest_bg2.jpg ~10+'; """
        } finally {
            GetDebugPoint().disableDebugPointForAllBEs("DorisFSDirectory::FSIndexInput::readInternal")
        }
    } finally {
    }
}