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

suite("test_unique_table_new_sequence") {
    def tableName = "test_uniq_new_sequence"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
    CREATE TABLE IF NOT EXISTS ${tableName} (
      `k1` int NULL,
      `v1` tinyint NULL,
      `v2` int
    ) ENGINE=OLAP
    UNIQUE KEY(k1)
    DISTRIBUTED BY HASH(`k1`) BUCKETS 3
    PROPERTIES (
    "function_column.sequence_col" = "v2",
    "replication_allocation" = "tag.location.default: 1",
    "light_schema_change" = "true"
    );
    """
    // load unique key
    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'columns', 'k1,v1,v2'

        file 'unique_key_data1.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(3, json.NumberTotalRows)
            assertEquals(3, json.NumberLoadedRows)
            assertEquals(0, json.NumberFilteredRows)
            assertEquals(0, json.NumberUnselectedRows)
        }
    }
    sql "sync"
    order_qt_all "SELECT * from ${tableName}"

    // load unique key
    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'columns', 'k1,v1,v2'

        file 'unique_key_data2.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(3, json.NumberTotalRows)
            assertEquals(3, json.NumberLoadedRows)
            assertEquals(0, json.NumberFilteredRows)
            assertEquals(0, json.NumberUnselectedRows)
        }
    }
    sql "sync"

    order_qt_all "SELECT * from ${tableName}"

    sql "INSERT INTO ${tableName} values(15, 8, 19)"

    sql "INSERT INTO ${tableName} values(15, 9, 18)"

    sql "sync"

    order_qt_all "SELECT * from ${tableName}"

    sql "UPDATE ${tableName} SET v1 = 10 WHERE k1 = 1"

    sql "UPDATE ${tableName} SET v2 = 14 WHERE k1 = 2"

    sql "UPDATE ${tableName} SET v2 = 11 WHERE k1 = 3"

    sql "SET show_hidden_columns=true"

    sql "sync"

    order_qt_all "SELECT * from ${tableName}"

    qt_desc "desc ${tableName}"

    sql "ALTER TABLE ${tableName} RENAME COLUMN v2 vv2"

    qt_desc "desc ${tableName}"

    sql "INSERT INTO ${tableName} values(21, 8, 22)"

    sql "INSERT INTO ${tableName} values(23, 9, 24)"

    sql "sync"

    order_qt_all "SELECT * from ${tableName}"

    sql "DROP TABLE ${tableName}"
}

