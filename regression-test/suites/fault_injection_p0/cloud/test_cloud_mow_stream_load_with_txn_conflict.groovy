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

suite("test_cloud_mow_stream_load_with_txn_conflict", "nonConcurrent") {
    GetDebugPoint().clearDebugPointsForAllFEs()
    def tableName = "test_cloud_mow_stream_load_with_txn_conflict"
    try {
        // create table
        sql """ drop table if exists ${tableName}; """

        sql """
        CREATE TABLE `${tableName}` (
            `id` int(11) NOT NULL,
            `name` varchar(1100) NULL,
            `score` int(11) NULL default "-1"
        ) ENGINE=OLAP
        UNIQUE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            "enable_unique_key_merge_on_write" = "true",
            "replication_num" = "1"
        );
        """
        GetDebugPoint().enableDebugPointForAllFEs('CloudGlobalTransactionMgr.getDeleteBitmapUpdateLock.conflict', [percent: 0.4])
        streamLoad {
            table "${tableName}"

            set 'column_separator', ','
            set 'columns', 'id, name, score'
            file "test_stream_load.csv"

            time 10000 // limit inflight 10s

            check { result, exception, startTime, endTime ->
                log.info("Stream load result: ${result}")
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
            }
        }
        qt_sql """ select * from ${tableName} order by id"""
    } finally {
        GetDebugPoint().disableDebugPointForAllFEs('CloudGlobalTransactionMgr.getDeleteBitmapUpdateLock.conflict')
        sql "DROP TABLE IF EXISTS ${tableName};"
        GetDebugPoint().clearDebugPointsForAllFEs()
    }

}
