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

suite("test_backup_restore") {

    def syncer = getSyncer()
    if (!syncer.checkEnableFeatureBinlog()) {
        logger.info("fe enable_feature_binlog is false, skip case test_backup_restore")
        return
    }
    def tableName = "tbl_backup_restore"
    def test_num = 0
    def insert_num = 5

    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
           CREATE TABLE if NOT EXISTS ${tableName} 
           (
               `test` INT,
               `id` INT
           )
           ENGINE=OLAP
           UNIQUE KEY(`test`, `id`)
           DISTRIBUTED BY HASH(id) BUCKETS 1 
           PROPERTIES ( 
               "replication_allocation" = "tag.location.default: 1",
               "binlog.enable" = "true"
           )
        """

    logger.info("=== Test 1: Common backup and restore ===")
    test_num = 1
    def snapshotName = "snapshot_test_1"
    for (int i = 0; i < insert_num; ++i) {
        sql """
               INSERT INTO ${tableName} VALUES (${test_num}, ${i})
            """ 
    }
    sql " sync "
    def res = sql "SELECT * FROM ${tableName}"
    assertEquals(res.size(), insert_num)
    sql """ 
            BACKUP SNAPSHOT ${context.dbName}.${snapshotName} 
            TO `__keep_on_local__` 
            ON (${tableName})
            PROPERTIES ("type" = "full")
        """
    syncer.waitSnapshotFinish()
    assertTrue(syncer.getSnapshot("${snapshotName}", "${tableName}"))
    assertTrue(syncer.restoreSnapshot(true))
    syncer.waitTargetRestoreFinish()
    target_sql " sync "
    res = target_sql "SELECT * FROM ${tableName}"
    assertEquals(res.size(), insert_num)
}
