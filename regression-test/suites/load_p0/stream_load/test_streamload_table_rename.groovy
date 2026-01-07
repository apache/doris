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

suite("test_streamload_table_rename", "p0,fault_injection,nonConcurrent") {
    def tableName = "test_streamload_rename_tbl"
    
    try {
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                a INT,
                b INT
            ) ENGINE=OLAP
            DUPLICATE KEY(a)
            DISTRIBUTED BY RANDOM BUCKETS 10
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1"
            );
        """
        
        GetDebugPoint().enableDebugPointForAllBEs(
            "StreamLoadExecutor.commit_txn.block"
        )
        
        def label = "test_label_${System.currentTimeMillis()}"
        def streamLoadJob = Thread.start {
            streamLoad {
                table tableName
                set 'label', label
                set 'format', 'json'
                set 'columns', 'a,b'
                set 'read_json_by_line', 'true'
                file 'data_by_line.json'
                time 300000
                
                check { result, exception, startTime, endTime ->
                    if (exception != null) {
                        throw exception
                    }
                    log.info("Stream load result: ${result}".toString())
                    def json = parseJson(result)
                    assertEquals("success", json.Status.toLowerCase())
                }
            }
        }
        
        Thread.sleep(3000)

        // rename table        
        sql """ ALTER TABLE ${tableName} RENAME ${tableName}_old """
        
        // same table
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                a INT,
                b INT
            ) ENGINE=OLAP
            DUPLICATE KEY(a)
            DISTRIBUTED BY RANDOM BUCKETS 10
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1"
            );
        """
        
        // commit
        try {
            GetDebugPoint().disableDebugPointForAllBEs(
                "StreamLoadExecutor.commit_txn.block"
            )
        } catch (Exception e) {
            log.warn("Failed to disable debug point in main flow: ${e.message}")
        }
        
        streamLoadJob.join()
        
        sql "sync"
        
        def newTableCount = sql "SELECT COUNT(*) FROM ${tableName}"
        def oldTableCount = sql "SELECT COUNT(*) FROM ${tableName}_old"
        
        log.info("New table (${tableName}) count: ${newTableCount[0][0]}")
        log.info("Old table (${tableName}_old) count: ${oldTableCount[0][0]}")
        

        assertEquals(0, newTableCount[0][0] as int, "New table should be empty")
        assertTrue(oldTableCount[0][0] as int > 0, "Old table should have data")
        
    } finally {
        try {
            GetDebugPoint().disableDebugPointForAllBEs(
                "StreamLoadExecutor.commit_txn.block"
            )
        } catch (Exception e) {
            log.warn("Failed to disable debug point (may not exist): ${e.message}")
        }
        try_sql "DROP TABLE IF EXISTS ${tableName}"
        try_sql "DROP TABLE IF EXISTS ${tableName}_old"
    }
}