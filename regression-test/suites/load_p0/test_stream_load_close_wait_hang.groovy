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

import org.apache.doris.regression.suite.ClusterOptions

suite("test_stream_load_close_wait_hang", "docker") {
    def options = new ClusterOptions()
    options.feNum = 1
    options.beNum = 3
    options.cloudMode = false

    // Enable single replica load in FE BE to trigger write_single_replica mode
    options.feConfigs += [
        'enable_single_replica_load=true'
    ]

    options.beConfigs += [
        'enable_debug_points=true',
        'enable_single_replica_load=true'
    ]

    docker(options) {
        def tableName = "test_close_wait_hang_table"

        // Create table with 3 replicas and RANDOM distribution
        // - replication_num=3: triggers write_single_replica mode (1 master + 2 slaves)
        // - RANDOM distribution: required by load_to_single_tablet parameter
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
            CREATE TABLE ${tableName} (
                id INT,
                name VARCHAR(100)
            )
            DUPLICATE KEY(id)
            DISTRIBUTED BY RANDOM BUCKETS 1
            PROPERTIES (
                "replication_num" = "3"
            )
        """

        // Prepare test data - write to local file
        def dataFile = new File("/tmp/test_stream_load_close_wait_hang_data.csv")
        dataFile.withWriter { writer ->
            for (int i = 1; i <= 5000; i++) {
                writer.writeLine("${i}\ttest${i}")
            }
        }
        log.info("Test data written to ${dataFile.absolutePath}")

        // Enable debug point to simulate "slave node not found" scenario
        // Effect: Sets node = nullptr to trigger the check
        // Expected behavior: Code should call cancel() and clear_in_flight() before returning
        try {
            GetDebugPoint().enableDebugPointForAllBEs("VNodeChannel.try_send_pending_block.slave_node_not_found")
            log.info("Debug point enabled: simulating slave node not found scenario")
        } catch (Exception e) {
            log.warn("Failed to enable debug point: ${e.message}")
        }

        def testStartTime = System.currentTimeMillis()
        def timed_out = false
        def load_exception = null

        // Execute stream load in separate thread to detect hangs
        // If cancel() is NOT called when slave node is missing, close_wait() will hang forever
        def load_thread = Thread.start {
            try {
                streamLoad {
                    table "${tableName}"

                    set 'column_separator', '\t'
                    set 'format', 'csv'

                    set 'load_to_single_tablet', 'true'

                    time 60000

                    file dataFile.absolutePath

                    check { result, exception, startTime, endTime ->
                        if (exception != null) {
                            load_exception = exception
                            log.error("Stream load failed with exception: ${exception.message}")
                        }
                    }
                }
            } catch (Exception e) {
                load_exception = e
                log.error("Stream load threw exception: ${e.message}")
            }
        }

        // Check if stream load completes within timeout
        // Expected: Should fail quickly (within 65s) because cancel() is called
        // Bug symptom: Hangs forever if cancel() is not called
        load_thread.join(65000)

        if (load_thread.isAlive()) {
            timed_out = true
            log.error("Stream load thread hung for too many seconds!")

            load_thread.interrupt()
            load_thread.join(5000)
        }

        def elapsed = System.currentTimeMillis() - testStartTime
        log.info("Stream load completed in ${elapsed}ms")

        try {
            GetDebugPoint().disableDebugPointForAllBEs("VNodeChannel.try_send_pending_block.slave_node_not_found")
        } catch (Exception e) {
            log.warn("Failed to disable debug point: ${e.message}")
        }

        if (timed_out) {
            throw new Exception("Stream load time out")
        }

        try {
            dataFile.delete()
            log.info("Cleaned up temp file: ${dataFile.absolutePath}")
        } catch (Exception e) {
            log.warn("Failed to delete temp file: ${e.message}")
        }
    }
}
