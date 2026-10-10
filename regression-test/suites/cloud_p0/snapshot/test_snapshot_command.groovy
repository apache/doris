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

    // The cluster-level snapshot switch may be off by default or already enabled by another Cloud P0 suite.
    test {
        sql """ ADMIN CREATE CLUSTER SNAPSHOT PROPERTIES('ttl' = '600', 'label' = 'test_snapshot'); """
        check { result, exception, startTime, endTime ->
            def msg = exception?.toString()
            assertTrue(msg != null, "Expected snapshot creation to fail, but it succeeded")
            assertTrue(msg.contains("failed to begin snapshot, because the snapshot feature is disabled")
                            || msg.contains("submitJob is not implemented"),
                    "Unexpected snapshot creation failure: ${msg}".toString())
        }
    }

    // snapshot feature off
    test {
        sql """ ADMIN SET CLUSTER SNAPSHOT FEATURE OFF; """
        exception ""
    }
    // snapshot feature on
    test {
        sql """ ADMIN SET CLUSTER SNAPSHOT FEATURE ON; """
        exception ""
    }

    // set auto snapshot properties
    test {
        sql """ ADMIN SET AUTO CLUSTER SNAPSHOT PROPERTIES('max_reserved_snapshots'='10', 'snapshot_interval_seconds'='3600');"""
        exception ""
    }

    // show snapshot properties
    def result = sql """ select * from information_schema.cluster_snapshot_properties; """
    logger.info("show result: " + result)

    // list snapshot
    test {
        result = sql """ select * from information_schema.cluster_snapshots; """
        exception ""
    }
    test {
        result = sql """ select * from information_schema.cluster_snapshots where id like '%1%'; """
        exception ""
    }

    // drop snapshot
    test {
        sql """ ADMIN DROP CLUSTER SNAPSHOT where SNAPSHOT_id = '1213'; """
        exception ""
    }
}
