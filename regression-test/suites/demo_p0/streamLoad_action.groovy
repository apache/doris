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

suite("streamLoad_action") {

    def tableName = "test_streamload_action1"

    sql """ DROP TABLE IF EXISTS ${tableName} """

    sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                id int,
                name varchar(255)
            )
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
              "replication_num" = "1"
            ) 
        """

    streamLoad {
        // you can skip declare db, because a default db already specify in ${DORIS_HOME}/conf/regression-conf.groovy
        // db 'regression_test'
        table tableName

        // default label is UUID:
        // set 'label' UUID.randomUUID().toString()

        // default column_separator is specify in doris fe config, usually is '\t'.
        // this line change to ','
        set 'column_separator', ','

        // relate to ${DORIS_HOME}/regression-test/data/demo/streamload_input.csv.
        // also, you can stream load a http stream, e.g. http://xxx/some.csv
        file 'streamload_input.csv'

        time 10000 // limit inflight 10s

        // stream load action will check result, include Success status, and NumberTotalRows == NumberLoadedRows
    }

    def backendIps = [:]
    def backendHttpPorts = [:]
    getBackendIpHttpPort(backendIps, backendHttpPorts)
    def backendId = backendIps.keySet()[0]
    streamLoad {
        table tableName
        set 'column_separator', ','
        file 'streamload_input.csv'

        // can direct to backend, then this backend is the txn coordinator.
        directToBe  backendIps.get(backendId),  backendHttpPorts.get(backendId) as int
    }

    sql """ sync; """

    order_qt_select_1 "SELECT * FROM ${tableName}"
    sql "TRUNCATE TABLE ${tableName}"

    // stream load 100 rows
    def rowCount = 100
    // range: [0, rowCount)
    // or rangeClosed: [0, rowCount]
    def rowIt = range(0, rowCount)
            .mapToObj({i -> [i, "a_" + i]}) // change Long to List<Long, String>
            .iterator()

    streamLoad {
        table tableName
        // also, you can upload a memory iterator
        inputIterator rowIt

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
    sql """ sync; """
    order_qt_select_2 "SELECT * FROM ${tableName}"

    // to test merge sort
    sql """ DROP TABLE IF EXISTS B """
    sql """
        CREATE TABLE IF NOT EXISTS B
        (
            b_id int
        )
        DISTRIBUTED BY HASH(b_id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
    """

    sql " INSERT INTO B values (1);"
    sql """ sync; """
    qt_sql """
        SELECT subq_0.`c1` AS c1
        FROM
        (SELECT version() AS c0,
                  ref_0.`id` AS c1
        FROM test_streamload_action1 AS ref_0
        LEFT JOIN B AS ref_9 ON (ref_0.`id` = ref_9.`b_id`)
        WHERE ref_9.`b_id` IS NULL) AS subq_0
        WHERE subq_0.`c0` IS NOT NULL
        ORDER BY subq_0.`c1`, subq_0.`c0` DESC
        LIMIT 5;
    """

    def tableName2 = "test_streamload_action2"
    runStreamLoadExample(tableName2)

    sql """ DROP TABLE ${tableName} """
    sql """ DROP TABLE ${tableName2}"""

    sql """ DROP TABLE B """
}
