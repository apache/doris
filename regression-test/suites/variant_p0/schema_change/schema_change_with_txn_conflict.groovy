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

suite("test_schema_change_txn_conflict", "nonConcurrent") {
    if (!isCloudMode()) {
        return
    }
    def customFeConfig = [
            schema_change_max_retry_time: 10
    ]
    def tableName = "variant_txn_conflict"
    setFeConfigTemporary(customFeConfig) {
        try {
            GetDebugPoint().enableDebugPointForAllBEs("CloudSchemaChangeJob::_convert_historical_rowsets.test_conflict")
            sql "DROP TABLE IF EXISTS ${tableName}"
            sql """
                CREATE TABLE IF NOT EXISTS ${tableName} (
                    k bigint,
                    v variant
                )
                DUPLICATE KEY(`k`)
                DISTRIBUTED BY HASH(k) BUCKETS 4
                properties("replication_num" = "1");
            """
            sql """INSERT INTO ${tableName} SELECT *, '{"k1":1, "k2": "hello world", "k3" : [1234], "k4" : 1.10000, "k5" : [[123]]}' FROM numbers("number" = "1")"""
            sql """ALTER TABLE ${tableName} SET("bloom_filter_columns" = "v")"""

            waitForSchemaChangeDone {
                sql """SHOW ALTER TABLE COLUMN WHERE IndexName='${tableName}' ORDER BY createtime DESC LIMIT 1"""
                time 600
            }
            sql """insert into ${tableName} values (2, '{"a" : 12345}')"""
        } catch (Exception e) {
            GetDebugPoint().disableDebugPointForAllBEs("CloudSchemaChangeJob::_convert_historical_rowsets.test_conflict")
        } finally {
            GetDebugPoint().disableDebugPointForAllBEs("CloudSchemaChangeJob::_convert_historical_rowsets.test_conflict")
            GetDebugPoint().clearDebugPointsForAllFEs()
        }
    }
    qt_sql "select * from ${tableName} order by k limit 5"
}