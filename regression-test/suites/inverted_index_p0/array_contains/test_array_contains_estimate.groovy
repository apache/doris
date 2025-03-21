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

suite("test_array_contains_estimate", "nonConcurrent"){
    // prepare test table
    def indexTblName = "tai_estimate"
    def dataFile = "tai_estimate.csv"

    sql """ set enable_common_expr_pushdown = true; """
    sql """ set enable_profile = true;"""
    sql """ set enable_inverted_index_query=true; """
    sql """ set inverted_index_skip_threshold = 0; """ // set skip threshold to 0

    sql "DROP TABLE IF EXISTS ${indexTblName}"
    // create 1 replica table
    sql """
	CREATE TABLE IF NOT EXISTS `${indexTblName}` (
      `apply_date` date NULL COMMENT '',
      `id` varchar(60) NOT NULL COMMENT '',
      `inventors` array<text> NULL COMMENT '',
      INDEX index_inverted_inventors(inventors) USING INVERTED  COMMENT ''
    ) ENGINE=OLAP
    DUPLICATE KEY(`apply_date`, `id`)
    COMMENT 'OLAP'
    DISTRIBUTED BY HASH(`id`) BUCKETS 1
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "is_being_synced" = "false",
    "storage_format" = "V2",
    "light_schema_change" = "true",
    "disable_auto_compaction" = "false",
    "enable_single_replica_compaction" = "false"
    );
    """

    streamLoad {
        table indexTblName

        file dataFile // import csv file
        time 10000 // limit inflight 10s
        set 'column_separator', '|'
        set 'strict_mode', 'true'

        // if declared a check callback, the default check condition will ignore.
        // So you must check all condition
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals(149, json.NumberTotalRows)
            assertEquals(149, json.NumberLoadedRows)
            assertTrue(json.LoadBytes > 0)
        }
    }

    // test array_contains estimate
    def create_sql = {
        List<String> list = new ArrayList<>()
        list.add("select count() from ${indexTblName} where  apply_date = "2024-11-18" or !array_contains(inventors, 'Amory Wang')")
        return list;
    }

    def checkpoints_name = "array_func.array_contains"
    def execute_sql = { sqlList ->
        def i = 0
        for (sqlStr in sqlList) {
            try {
                log.info("execute sql: i")
                GetDebugPoint().enableDebugPointForAllBEs(checkpoints_name, [req_id: i])
                order_qt_sql """ ${sqlStr} """
            } finally {
                GetDebugPoint().disableDebugPointForAllBEs(checkpoints_name)
            }
            ++i
        }
    }

}
