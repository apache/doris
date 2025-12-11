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

suite("test_packed_file_stream_load_case2", "p0,nonConcurrent") {
    if (!isCloudMode()) {
        log.info("skip packed_file cases in non cloud mode")
        return
    }

    final String tableName = "packed_file_case2"
    final String dataFile = "cloud_p0/packed_file/merge_file_stream_load.csv"
    final int rowsPerLoad = 200

    def createTable = {
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                `id` INT,
                `name` VARCHAR(50),
                INDEX idx_name(`name`) USING INVERTED PROPERTIES("parser" = "english")
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 20
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1"
            );
        """
    }

    def assertRowCount = { long expected ->
        def count = sql """ select count(*) from ${tableName} """
        assertEquals(expected, (count[0][0] as long))
    }

    def runLoads = { int iterations ->
        int success = 0
        for (int i = 0; i < iterations; i++) {
            streamLoad {
                table "${tableName}"
                set 'column_separator', ','
                set 'format', 'csv'
                file dataFile
                time 120000

                check { result, exception, startTime, endTime ->
                    if (exception != null) {
                        throw exception
                    }
                    log.info("Stream load result: ${result}".toString())
                    def json = parseJson(result)
                    assertEquals("success", json.Status?.toLowerCase())
                    assertEquals(json.NumberTotalRows, json.NumberLoadedRows)
                    success++
                }
            }
        }
        sql "sync"
        return success
    }

    createTable()
    def successLoads = runLoads(20)
    assertEquals(20, successLoads)
    assertRowCount(successLoads * rowsPerLoad)
    sql """ DROP TABLE IF EXISTS ${tableName} """
}
