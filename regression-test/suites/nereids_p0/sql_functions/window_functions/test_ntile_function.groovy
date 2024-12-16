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
suite("test_ntile_function") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    def tableName = "test_ntile_function"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
            CREATE TABLE IF NOT EXISTS `${tableName}` (
            `k1` tinyint(4) NOT NULL COMMENT "",
            `k2` smallint(6) NOT NULL COMMENT "",
            `k3` smallint(6) NOT NULL COMMENT ""
            ) ENGINE=OLAP
            DUPLICATE KEY(`k1`, `k2`)
            COMMENT "OLAP"
            DISTRIBUTED BY HASH(`k1`) BUCKETS 5
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "in_memory" = "false",
            "storage_format" = "V2"
            );
        """
    streamLoad {
        table tableName

        // default label is UUID:
        // set 'label' UUID.randomUUID().toString()

        // default column_separator is specify in doris fe config, usually is '\t'.
        // this line change to ','
        set 'column_separator', '\t'

        // relate to ${DORIS_HOME}/regression-test/data/demo/streamload_input.csv.
        // also, you can stream load a http stream, e.g. http://xxx/some.csv
        file 'test_ntile_function.csv'

        time 10000 // limit inflight 10s

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

     qt_select "select k1, k2, k3, ntile(3) over (partition by k1 order by k2) as ntile from ${tableName} order by k1, k2, k3 desc;"
     qt_select "select k1, k2, k3, ntile(5) over (partition by k1 order by k2) as ntile from ${tableName} order by k1, k2, k3 desc;"
     qt_select "select k2, k1, k3, ntile(3) over (order by k2) as ntile from ${tableName} order by k2, k1, k3 desc;"
     qt_select "select k3, k2, k1, ntile(3) over (partition by k3 order by k2) as ntile from ${tableName} order by k3, k2, k1;"
     test {
         sql "select k1, k2, k3, ntile(0) over (partition by k1 order by k2) as ntile from ${tableName} order by k1, k2, k3 desc;"
         exception "The bucket parameter of NTILE must be a constant positive integer: ntile(0)"
     }
     test {
         sql "select k1, k2, k3, ntile(k1) over (partition by k1 order by k2) as ntile from ${tableName} order by k1, k2, k3 desc;"
         exception "The bucket of NTILE must be a constant value: ntile(k1)"
     }

}





