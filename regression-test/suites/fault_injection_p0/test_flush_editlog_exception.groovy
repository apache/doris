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

suite("test_flush_editlog_exception", "docker") {
    def options = new ClusterOptions()
    options.cloudMode = false
    options.setFeNum(1)
    options.setBeNum(1)
    options.enableDebugPoints()

    docker(options) {

        def tableName = "test_flush_editlog_exception"
        // test txn X inverted index
        sql "DROP TABLE IF EXISTS ${tableName}"

        GetDebugPoint().enableDebugPointForAllFEs('EditLog.flushEditLog.exception')

        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                `user_id` bigint default 999,
                `group_id` bigint,
                `id` bigint,
                `vv` variant,
                INDEX idx_col1 (user_id) USING INVERTED
                ) ENGINE=OLAP
            UNIQUE KEY(user_id, group_id)
            DISTRIBUTED BY HASH (user_id) BUCKETS 1
            PROPERTIES(
                    "store_row_column" = "true",
                    "replication_num" = "1"
                    );
        """

        // Wait until SELECT returns error (i.e., Doris FE process exits due to flushEditLog exception)
        int maxWaitMs = 10000
        int intervalMs = 200
        long startTime = System.currentTimeMillis()
        boolean gotError = false
        while (true) {
            try {
                sql "SELECT * from ${tableName}"
            } catch (Exception e) {
                gotError = true
                break
            }
            if (System.currentTimeMillis() - startTime > maxWaitMs) {
                throw new IllegalStateException("Timeout waiting for SELECT to return error")
            }
            Thread.sleep(intervalMs)
        }
        assert gotError
    }
}
