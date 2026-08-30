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

suite("test_colocate_mapping_constraint_ccr") {
    def syncer = getSyncer()
    if (!syncer.checkEnableFeatureBinlog()) {
        logger.info("fe enable_feature_binlog is false, skip case test_colocate_mapping_constraint_ccr")
        return
    }

    def tableName = "tbl_colocate_mapping_constraint_ccr"
    def snapshotName = "snapshot_colocate_mapping_constraint_ccr"
    def rowCount = 5

    sql "DROP TABLE IF EXISTS ${tableName}"
    target_sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE ${tableName} (
            `test` INT,
            `id` INT
        )
        ENGINE=OLAP
        UNIQUE KEY(`test`, `id`)
        DISTRIBUTED BY HASH(id) BUCKETS 4
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true"
        )
    """
    sql """
        ALTER TABLE ${tableName}
        ADD CONSTRAINT ccr_mapping
        COLOCATE MAPPING ccr_mapping_id (test) DETERMINES DISTRIBUTION KEY (id) NOT ENFORCED
    """
    for (int i = 0; i < rowCount; ++i) {
        sql "INSERT INTO ${tableName} VALUES (1, ${i})"
    }
    sql "SYNC"

    sql """
        BACKUP SNAPSHOT ${context.dbName}.${snapshotName}
        TO `__keep_on_local__`
        ON (${tableName})
        PROPERTIES ("type" = "full")
    """
    syncer.waitSnapshotFinish()
    assertTrue(syncer.getSnapshot(snapshotName, tableName))
    assertTrue(syncer.context.getSnapshotResult.isSetCommitSeq())
    syncer.context.seq = syncer.context.getSnapshotResult.getCommitSeq()
    assertTrue(syncer.restoreSnapshot(true))
    syncer.waitTargetRestoreFinish()
    target_sql "SYNC"

    assertTrue((target_sql "SHOW CONSTRAINTS FROM ${tableName}").isEmpty())
    def targetExplain = target_sql """
        EXPLAIN SELECT /*+ SET_VAR(disable_join_reorder=true,
                                  enable_colocate_mapping_constraint=true,
                                  auto_broadcast_join_threshold=-1,
                                  broadcast_row_count_limit=0) */ COUNT(*)
        FROM ${tableName} l JOIN ${tableName} r ON l.test = r.test
    """
    assertFalse(targetExplain.toString().contains("COLOCATE"))
    assertTrue(syncer.getTargetMeta(tableName))

    sql """
        ALTER TABLE ${tableName}
        DROP CONSTRAINT ccr_mapping
    """
    sql "SYNC"
    sql "INSERT INTO ${tableName} VALUES (1, ${rowCount})"
    boolean foundTableDataBinlog = false
    for (int attempt = 0; attempt < 10 && !foundTableDataBinlog; ++attempt) {
        assertTrue(syncer.getBinlog(tableName))
        def sourceTable = syncer.context.sourceTableMap.get(tableName)
        foundTableDataBinlog = syncer.context.lastBinlog.tableRecords != null
                && syncer.context.lastBinlog.tableRecords.containsKey(sourceTable.id)
    }
    assertTrue(foundTableDataBinlog)
    assertTrue(syncer.beginTxn(tableName))
    assertTrue(syncer.getBackendClients())
    assertTrue(syncer.ingestBinlog())
    assertTrue(syncer.commitTxn())
    assertTrue(syncer.checkTargetVersion())
    syncer.closeBackendClients()

    target_sql "SYNC"
    def targetRows = target_sql "SELECT * FROM ${tableName}"
    assertEquals(rowCount + 1, targetRows.size())
    def joinCount = target_sql """
        SELECT /*+ SET_VAR(disable_join_reorder=true,
                           enable_colocate_mapping_constraint=true,
                           auto_broadcast_join_threshold=-1,
                           broadcast_row_count_limit=0) */ COUNT(*)
        FROM ${tableName} l JOIN ${tableName} r ON l.test = r.test
    """
    assertEquals((rowCount + 1L) * (rowCount + 1L), joinCount[0][0] as long)
}
