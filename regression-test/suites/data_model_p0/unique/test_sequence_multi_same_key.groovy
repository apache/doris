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

suite("test_sequence_multi_same_key") {
    def tableName = "sequence_multi_same_key"
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `user_id` bigint,
            `date` date,
            `group_id` bigint,
            `modify_date` date,
            `keyword` VARCHAR(128)
            ) ENGINE=OLAP
        UNIQUE KEY(user_id, date, group_id)
        DISTRIBUTED BY HASH (user_id) BUCKETS 1
        PROPERTIES(
                "function_column.sequence_col" = 'modify_date',
                "replication_num" = "1",
                "in_memory" = "false"
                );
    """
    // load unique key
    streamLoad {
        table "${tableName}"

        set 'column_separator', ','

        file 'sequence_multi_same_key.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(6, json.NumberTotalRows)
            assertEquals(6, json.NumberLoadedRows)
            assertEquals(0, json.NumberFilteredRows)
            assertEquals(0, json.NumberUnselectedRows)
        }
    }
    sql "sync"

    order_qt_all "SELECT * from ${tableName}"

    sql "DROP TABLE ${tableName}"
}

