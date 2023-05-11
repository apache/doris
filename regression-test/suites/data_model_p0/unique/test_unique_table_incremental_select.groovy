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

suite("test_unique_table_incremental_select") {
    def tableName = "test_unique_table_incremental_select"
    def max_version = 0
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
          `k1` int NULL,
          `v1` tinyint NULL,
          `v2` int,
          `v3` int,
          `v4` int
        ) ENGINE=OLAP
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(`k1`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """
    // load unique key
    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'columns', 'k1,v1,v2,v3,v4'

        file 'unique_key_data1.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(4, json.NumberTotalRows)
            assertEquals(4, json.NumberLoadedRows)
            assertEquals(0, json.NumberFilteredRows)
            assertEquals(0, json.NumberUnselectedRows)
        }
    }
    sql "sync"

    order_qt_all "SELECT * from ${tableName}"

    qt_inc_select "SELECT * from ${tableName} where __DORIS_VERSION_COL__ > ${max_version}"

    res = sql "SELECT max(__DORIS_VERSION_COL__) from ${tableName}"
    max_version = res[0][0]

    // load unique key
    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'columns', 'k1,v1,v2,v3,v4'

        file 'unique_key_data2.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(4, json.NumberTotalRows)
            assertEquals(4, json.NumberLoadedRows)
            assertEquals(0, json.NumberFilteredRows)
            assertEquals(0, json.NumberUnselectedRows)
        }
    }
    sql "sync"

    qt_count "SELECT COUNT(*) from ${tableName}"

    order_qt_all "SELECT * from ${tableName}"

    qt_inc_select "SELECT * from ${tableName} where __DORIS_VERSION_COL__ > ${max_version}"

    res = sql "SELECT max(__DORIS_VERSION_COL__) from ${tableName}"
    max_version = res[0][0]

    sql "INSERT INTO ${tableName} values(15, 8, 19, 20, 21)"

    sql "INSERT INTO ${tableName} values(15, 9, 20, 21, 22)"

    sql "sync"

    qt_count "SELECT COUNT(*) from ${tableName}"

    order_qt_all "SELECT * from ${tableName}"

    qt_inc_select "SELECT * from ${tableName} where __DORIS_VERSION_COL__ > ${max_version}"

    res = sql "SELECT max(__DORIS_VERSION_COL__) from ${tableName}"
    max_version = res[0][0]

    sql "INSERT INTO ${tableName} values(17, 1, 2, 3, 4), (18, 1, 2, 3, 4)"

    sql "INSERT INTO ${tableName} (k1, v1, v2, v3, v4, __DORIS_DELETE_SIGN__) values(17, 1, 2, 3, 4, 1)"

    qt_count "SELECT COUNT(*) from ${tableName}"

    order_qt_all "SELECT * from ${tableName}"

    qt_inc_select "SELECT * from ${tableName} where __DORIS_VERSION_COL__ > ${max_version}"

    sql "DROP TABLE ${tableName}"
}
