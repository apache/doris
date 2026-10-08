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

suite("test_restricted_aggregate_versioned_state", "nonConcurrent") {
    if (isCloudMode()) {
        return
    }

    def originalVersion = sql("ADMIN SHOW FRONTEND CONFIG LIKE 'be_exec_version'")[0][1]
    try {
        sql "ADMIN SET FRONTEND CONFIG ('be_exec_version' = '15')"
        sql "SET enable_agg_state = true"
        sql "DROP TABLE IF EXISTS test_restricted_aggregate_versioned_source"
        sql """
            CREATE TABLE test_restricted_aggregate_versioned_source (
                id INT,
                x DOUBLE
            ) DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES ("replication_num" = "1")
        """
        sql "INSERT INTO test_restricted_aggregate_versioned_source VALUES (1, 1.0), (2, 2.0), (3, 3.0)"
        order_qt_direct_version_15 """
            SELECT stddev_samp(x) FROM test_restricted_aggregate_versioned_source
        """

        sql "DROP TABLE IF EXISTS test_restricted_aggregate_versioned_state"
        sql """
            CREATE TABLE test_restricted_aggregate_versioned_state (
                k INT,
                state AGG_STATE<stddev_samp(DOUBLE)> GENERIC
            ) AGGREGATE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES ("replication_num" = "1")
        """
        sql """
            INSERT INTO test_restricted_aggregate_versioned_state
            SELECT 1, stddev_samp_state(x) FROM test_restricted_aggregate_versioned_source
        """

        sql "ADMIN SET FRONTEND CONFIG ('be_exec_version' = '16')"
        order_qt_stored_version_15 """
            SELECT stddev_samp_merge(state) FROM test_restricted_aggregate_versioned_state
        """
        order_qt_direct_version_16 """
            SELECT stddev_samp(x) FROM test_restricted_aggregate_versioned_source
        """
    } finally {
        sql "ADMIN SET FRONTEND CONFIG ('be_exec_version' = '${originalVersion}')"
    }
}
