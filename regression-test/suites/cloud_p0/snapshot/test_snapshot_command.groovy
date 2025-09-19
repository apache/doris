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

    // backup snapshot
    test {
        sql """ ADMIN BACKUP CLUSTER SNAPSHOT PROPERTIES('ttl' = '600', 'label' = 'test_snapshot'); """
        exception "submitJob is not implemented"
    }

    // set snapshot properties
    test {
        sql """ ADMIN SET CLUSTER SNAPSHOT PROPERTIES('enabled'='true', 'max_reserved_snapshots'='10', 'snapshot_interval_seconds'='3600');"""
        exception ""
    }

    // show snapshot properties
    def result = sql """ ADMIN SHOW CLUSTER SNAPSHOT PROPERTIES; """
    logger.info("show result: " + result)

    // list snapshot
    test {
        sql """ ADMIN SHOW CLUSTER SNAPSHOT; """
        exception ""
    }
    test {
        sql """ ADMIN SHOW full CLUSTER SNAPSHOT; """
        exception ""
    }

    // drop snapshot
    test {
        sql """ ADMIN DROP CLUSTER SNAPSHOT where SNAPSHOT_id = '1213'; """
        exception ""
    }
}