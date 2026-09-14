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

suite("test_readd_dropped_column_unique_id", "docker") {
    def options = new ClusterOptions()
    options.feConfigs += ["enable_debug_points=true"]
    options.cloudMode = false

    docker(options) {
        sql "DROP TABLE IF EXISTS test_readd_dropped_column_unique_id"
        sql """
            CREATE TABLE test_readd_dropped_column_unique_id (
                k INT NOT NULL,
                v INT NULL
            )
            DUPLICATE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "light_schema_change" = "true",
                "disable_auto_compaction" = "true"
            )
        """
        sql "INSERT INTO test_readd_dropped_column_unique_id VALUES (1, 12345)"
        sql "SYNC"

        def debugPoint = "FE.SchemaChangeHandler.updateBaseIndexSchema.forceStaleMaxColUniqueId"
        try {
            GetDebugPoint().enableDebugPointForAllFEs(
                    debugPoint, [table_name: "test_readd_dropped_column_unique_id"])
            sql "ALTER TABLE test_readd_dropped_column_unique_id DROP COLUMN v"
        } finally {
            GetDebugPoint().disableDebugPointForAllFEs(debugPoint)
        }

        sql "ALTER TABLE test_readd_dropped_column_unique_id ADD COLUMN v INT NULL"

        order_qt_old_row """
            SELECT k, v
            FROM test_readd_dropped_column_unique_id
            ORDER BY k
        """

        sql "INSERT INTO test_readd_dropped_column_unique_id VALUES (2, 67890)"
        order_qt_all_rows """
            SELECT k, v
            FROM test_readd_dropped_column_unique_id
            ORDER BY k
        """
    }
}
