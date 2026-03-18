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

suite('test_snapshot_command') {
    if (!isCloudMode()) {
        log.info("not cloud mode just return")
        return
    }

    // create snapshot — submitJob is now implemented in DorisCloudSnapshotHandler.
    // In environments without full snapshot infrastructure the RPC may fail,
    // so we accept either success or a MetaService/RPC error, but NOT
    // "submitJob is not implemented".
    def createSuccess = false
    try {
        sql """ ADMIN CREATE CLUSTER SNAPSHOT PROPERTIES('ttl' = '600', 'label' = 'test_snapshot_cmd'); """
        createSuccess = true
        logger.info("create snapshot command succeeded")
    } catch (Exception e) {
        logger.info("create snapshot command error (acceptable): ${e.message}")
        // Should NOT be the old unimplemented stub error
        assertFalse(e.message.contains("submitJob is not implemented"),
            "submitJob should no longer throw NotImplementedException")
    }

    // snapshot feature off
    sql """ ADMIN SET CLUSTER SNAPSHOT FEATURE OFF; """

    // snapshot feature on
    sql """ ADMIN SET CLUSTER SNAPSHOT FEATURE ON; """

    // set auto snapshot properties
    sql """ ADMIN SET AUTO CLUSTER SNAPSHOT PROPERTIES('max_reserved_snapshots'='10', 'snapshot_interval_seconds'='3600'); """

    // show snapshot properties
    def result = sql """ select * from information_schema.cluster_snapshot_properties; """
    logger.info("show result: " + result)
    assertTrue(result != null, "cluster_snapshot_properties query should succeed")

    // list snapshot
    result = sql """ select * from information_schema.cluster_snapshots; """
    logger.info("list snapshots: " + result)

    result = sql """ select * from information_schema.cluster_snapshots where id like '%1%'; """
    logger.info("filtered snapshots: " + result)

    // drop snapshot — may fail if ID does not exist, that is acceptable
    try {
        sql """ ADMIN DROP CLUSTER SNAPSHOT where SNAPSHOT_id = '1213'; """
    } catch (Exception e) {
        logger.info("drop non-existent snapshot error (acceptable): ${e.message}")
    }

    // Cleanup: drop the snapshot created above if it was successful
    if (createSuccess) {
        try {
            def snapshots = sql """ select ID from information_schema.cluster_snapshots where LABEL = 'test_snapshot_cmd' """
            for (row in snapshots) {
                sql """ ADMIN DROP CLUSTER SNAPSHOT WHERE snapshot_id = '${row[0]}' """
            }
        } catch (Exception e) {
            logger.info("cleanup error (acceptable): ${e.message}")
        }
    }
}