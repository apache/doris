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

    sql """
     ADMIN SET FRONTEND CONFIG ("catalog_trash_expire_second" = "10");
    """

    def insert_num = 5
     
    sql "DROP DATABASE IF EXISTS ${dbName}"
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
               INSERT INTO ${dbName}.${tableName} VALUES (1, ${i})
            """ 
    }
    sql " sync "
    def res = sql "SELECT * FROM ${dbName}.${tableName}"
    assertEquals(res.size(), insert_num)

    GetDebugPoint().clearDebugPointsForAllFEs()
    GetDebugPoint().clearDebugPointsForAllBEs()

    result = sql_return_maparray """show tablets from ${dbName}.${tableName}"""
    assertNotNull(result)
    def tabletId = null
    for (def res1 : result) {
        tabletId = res1.TabletId
        break
    }

    logger.info("========= Test 1: ignore checkBuckupRunning when truncate table ===")
    def snapshotName1 = "snapshot_test_1"

    GetDebugPoint().enableDebugPointForAllBEs("SnapshotManager::make_snapshot.wait", [tablet_id:"${tabletId}"]);
    
    sql """ 
            BACKUP SNAPSHOT ${dbName}.${snapshotName1} 
            TO `__keep_on_local__` 
            ON (${tableName})
            PROPERTIES ("type" = "full")
        """

    GetDebugPoint().enableDebugPointForAllFEs('FE.checkBuckupRunning.ignore', null)
    GetDebugPoint().enableDebugPointForAllFEs('FE.CatalogRecycleBin.isExpire', null)

    // wait backup snapshoting
    count = 200 // 20s
    for (int i = 0; i < count; ++i) {
        def records = sql_return_maparray "SHOW BACKUP FROM ${dbName}"
        int found = 0
        for (def res2 : records) {
            if (res2.State.equals("SNAPSHOTING")) {
                found = 1
                break
            }
        }
        if (found == 1) {
            break
        }
        Thread.sleep(100)
    }

    // truncate ok
    sql """
        truncate table ${dbName}.${tableName};
    """

    sql "sync"
    // assert truncate success
    res = sql "SELECT * FROM ${dbName}.${tableName}"
    assertEquals(res.size(), 0)

    sql """
     ADMIN CLEAN TRASH;
    """

    // wait backup canceled, failed to get tablet
    count = 1000 // 2000s
    def found = 0
    for (int i = 0; i < count; ++i) {
        def records = sql_return_maparray "SHOW BACKUP FROM ${dbName}"
        found = 0
        for (def res2 : records) {
            logger.info("row.State is ${res2.State}")
            logger.info("row.TaskErrMsg is ${res2.TaskErrMsg}")

            if (res2.State.equals("CANCELLED") && (res2.TaskErrMsg as String).contains("failed to get tablet")) {
                found = 1
                break
            }
        }
        if (found == 1) {
            break
        }
        Thread.sleep(2000)
    }

    assertEquals(found, 1)

    syncer.waitSnapshotFinish(dbName)

    GetDebugPoint().disableDebugPointForAllBEs("SnapshotManager::make_snapshot.wait")
    GetDebugPoint().disableDebugPointForAllFEs('FE.checkBuckupRunning.ignore')
    GetDebugPoint().disableDebugPointForAllFEs('FE.CatalogRecycleBin.isExpire')



    logger.info("========= Test 2: checkBuckupRunning when truncate table ===")
    def snapshotName2 = "snapshot_test_2"

    for (int i = 0; i < insert_num; ++i) {
    sql """
         INSERT INTO ${dbName}.${tableName} VALUES (${i}, ${i})
        """
    }
    sql " sync "

    result = sql_return_maparray """show tablets from ${dbName}.${tableName}"""
    assertNotNull(result)
    tabletId = null
    for (def res1 : result) {
        tabletId = res1.TabletId
        break
    }
    logger.info("========= show tablet ${tabletId}  ===")

    sql """
            BACKUP SNAPSHOT ${dbName}.${snapshotName2}
            TO `__keep_on_local__`
            ON (${tableName})
            PROPERTIES ("type" = "full")
        """

    // wait backup snapshoting
    count = 200 // 20s
    found = 0
    for (int i = 0; i < count; ++i) {
        def records = sql_return_maparray "SHOW BACKUP FROM ${dbName}"
        found = 0
        for (def res2 : records) {
            if (res2.State .equals("SNAPSHOTING")) {
                found = 1
                break
            }
        }
        if (found == 1) {
            break
        }
        Thread.sleep(100)
    }

    assertEquals(found, 1)

    
    // truncate fail errCode = 2, detailMessage = Backup is running on this db: test_backup_with_ddl_db
    try_sql """
        truncate table ${dbName}.${tableName};
    """
    // assert truncate fail
    res = sql "SELECT * FROM ${dbName}.${tableName}"
    assertEquals(res.size(), insert_num)

    syncer.waitSnapshotFinish(dbName)
  

    sql "DROP TABLE IF EXISTS ${dbName}.${tableName}"
    sql "DROP DATABASE ${dbName} FORCE"

}
