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

suite("test_backup_with_ddl") {

    def tableName = "test_backup_with_ddl"
    String dbName = "test_backup_with_ddl_db"

    def insert_num = 5
     
    sql "CREATE DATABASE IF NOT EXISTS ${dbName}"
    sql "DROP TABLE IF EXISTS ${dbName}.${tableName}"
    sql """
           CREATE TABLE if NOT EXISTS ${dbName}.${tableName} 
           (
               `test` INT,
               `id` INT
           )
           ENGINE=OLAP
           UNIQUE KEY(`test`, `id`)
           DISTRIBUTED BY HASH(id) BUCKETS 1 
           PROPERTIES ( 
               "replication_allocation" = "tag.location.default: 1"
           )
        """

    for (int i = 0; i < insert_num; ++i) {
        sql """
               INSERT INTO ${dbName}.${tableName} VALUES (${i}, ${i})
            """ 
    }
    sql " sync "
    def res = sql "SELECT * FROM ${dbName}.${tableName}"
    assertEquals(res.size(), insert_num)

    result = sql_return_maparray """show tablets from ${dbName}.${tableName}"""
    assertNotNull(result)
    def tabletId = null
    for (def res1 : result) {
        tabletId = res1.TabletId
        break
    }


    logger.info("=== Test 1: drop table ===")
    def snapshotName1 = "snapshot_test_1"

    GetDebugPoint().enableDebugPointForAllBEs("SnapshotManager::make_snapshot.wait", [tablet_id:"${tabletId}"]);
    
    sql """ 
            BACKUP SNAPSHOT ${dbName}.${snapshotName1} 
            TO `__keep_on_local__` 
            ON (${tableName})
            PROPERTIES ("type" = "full")
        """

    //GetDebugPoint().enableDebugPointForAllFEs('FE.checkBuckupRunning.ignore', null)
    
    // wait backup snapshoting
    count = 10 // 20s
    for (int i = 0; i < count; ++i) {
        def records = sql_return_maparray "SHOW BACKUP FROM ${dbName}"
        int found = 0
        for (def res2 : records) {
            //logger.info("row is ${res2}")
            logger.info("row.State is ${res2.State}")

            if (res2.State .equals("SNAPSHOTING")) {
                found = 1
                break
            }
        }
        if (found == 1) {
            break
        }
        Thread.sleep(2000)
    }
    
    // truncate fail errCode = 2, detailMessage = Backup is running on this db: test_backup_with_ddl_db
    try_sql """
        truncate table ${dbName}.${tableName};
    """
    def res = sql "SELECT * FROM ${dbName}.${tableName}"
    assertEquals(res.size(), insert_num)

    syncer.waitSnapshotFinish(dbName)
  
    GetDebugPoint().disableDebugPointForAllBEs("SnapshotManager::make_snapshot.wait")
    GetDebugPoint().disableDebugPointForAllFEs('FE.checkBuckupRunning.ignore')

    sql "DROP TABLE IF EXISTS ${dbName}.${tableName}"
    sql "DROP DATABASE ${dbName} FORCE"

}

//test_create_table_exception.groovy
//disableDebugPointForAllFEs
