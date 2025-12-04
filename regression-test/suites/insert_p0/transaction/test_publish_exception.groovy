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

suite("test_publish_exception", "nonConcurrent") {
    def tableName = "test_publish_exception"
    // test txn X inverted index
    sql "DROP TABLE IF EXISTS ${tableName}"
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
    DebugPoint.enableDebugPointForAllFEs('PublishVersionDaemon.tryFinishTxnSync.fail')
    sql "begin"
    sql """insert into ${tableName} values(1,1,5,'{"b":"b"}'),(1,1,4,'{"b":"b"}'),(1,1,3,'{"b":"b"}')"""

    test {
        sql "commit"
        exception "transaction commit successfully, BUT data will be visible later"
    }

    def result = sql "SELECT * from ${tableName}"
    assert result.size() == 0

    DebugPoint.disableDebugPointForAllFEs('PublishVersionDaemon.tryFinishTxnSync.fail')
    // Wait until the result size is 1 or timeout (10s)
    int maxWaitMs = 10000
    int intervalMs = 200
    long startTime = System.currentTimeMillis()
    while (true) {
        def result2 = sql "SELECT * from ${tableName}"
        if (result2.size() == 1) {
            break
        }
        if (System.currentTimeMillis() - startTime > maxWaitMs) {
            throw new IllegalStateException("Timeout waiting for result.size() == 1")
        }
        Thread.sleep(intervalMs)
    }
}
