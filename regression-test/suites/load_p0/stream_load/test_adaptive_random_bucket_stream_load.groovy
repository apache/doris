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

suite("test_adaptive_random_bucket_stream_load", "p0,nonConcurrent") {
    if (!isCloudMode()) {
        return
    }

    def tableName = "test_adaptive_random_bucket_stream_load"
    def enableAdaptiveRandomBucketConfig =
            sql """ ADMIN SHOW FRONTEND CONFIG LIKE 'enable_adaptive_random_bucket_load'; """
    String oldEnableAdaptiveRandomBucket = enableAdaptiveRandomBucketConfig[0][1]

    try {
        sql """ ADMIN SET FRONTEND CONFIG ('enable_adaptive_random_bucket_load' = 'true') """
        sql """ DROP TABLE IF EXISTS ${tableName} """
        // With a single bucket the partition has exactly one tablet, so all but one entry backend
        // do not own it and have to route the partition to the owning backend.
        sql """
            CREATE TABLE ${tableName} (
                k int NOT NULL,
                v string
            )
            DUPLICATE KEY(k)
            DISTRIBUTED BY RANDOM BUCKETS 1
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1"
            )
        """

        Map<String, String> backendIps = [:]
        Map<String, String> backendHttpPorts = [:]
        getBackendIpHttpPort(backendIps, backendHttpPorts)
        assertTrue(backendIps.size() > 1, "adaptive random bucket routing needs at least two backends")

        int rowsPerLoad = 10
        int totalRows = 0
        backendIps.each { backendId, backendIp ->
            StringBuilder data = new StringBuilder()
            for (int i = 0; i < rowsPerLoad; i++) {
                data.append("${totalRows + i},value_${totalRows + i}\n")
            }
            totalRows += rowsPerLoad
            // The entry backend is the sink backend. Only one of them owns the single tablet, the
            // others must send the partition to the owning backend instead of failing with
            // "unknown partition channel".
            streamLoad {
                table tableName
                directToBe backendIp, backendHttpPorts.get(backendId) as int
                set 'column_separator', ','
                inputText data.toString()
                time 60000

                check { result, exception, startTime, endTime ->
                    if (exception != null) {
                        throw exception
                    }
                    def json = parseJson(result)
                    assertTrue(json.Status.toString().equalsIgnoreCase("success"), "load failed: ${result}")
                    assertEquals(rowsPerLoad, json.NumberLoadedRows.toString().toInteger(), "load failed: ${result}")
                }
            }
            sql "sync"
        }

        def count = sql "SELECT count(*) FROM ${tableName}"
        assertEquals(totalRows, count[0][0] as int)
    } finally {
        sql """ ADMIN SET FRONTEND CONFIG ('enable_adaptive_random_bucket_load' = '${oldEnableAdaptiveRandomBucket}') """
    }
}
