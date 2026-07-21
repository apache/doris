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

package org.apache.doris.mtmv.ivm;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.Config;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.persist.DropPartitionInfo;
import org.apache.doris.persist.RecoverInfo;
import org.apache.doris.persist.ReplacePartitionOperationLog;
import org.apache.doris.persist.TruncateTableInfo;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

public class IvmBinlogBrokenTest extends TestWithFeService {

    @Override
    protected void runBeforeAll() throws Exception {
        Config.enable_table_stream = true;
    }

    @Test
    public void testTruncateMarksBinlogBroken() throws Exception {
        String db = "ivm_broken_truncate";
        createPartitionedIvmTableAndMv(db);

        executeSql("TRUNCATE TABLE ivm_base");

        Assertions.assertTrue(getMtmv(db).getIvmInfo().isBinlogBroken());
    }

    @Test
    public void testTruncatePartitionMarksBinlogBroken() throws Exception {
        String db = "ivm_broken_truncate_partition";
        createPartitionedIvmTableAndMv(db);

        executeSql("TRUNCATE TABLE ivm_base PARTITION(p202001)");

        Assertions.assertTrue(getMtmv(db).getIvmInfo().isBinlogBroken());
    }

    @Test
    public void testDropPartitionMarksBinlogBroken() throws Exception {
        String db = "ivm_broken_drop_partition";
        createPartitionedIvmTableAndMv(db);
        IvmInfo ivmInfo = getMtmv(db).getIvmInfo();

        executeSql("ALTER TABLE ivm_base DROP PARTITION p202001");

        Assertions.assertSame(ivmInfo, getMtmv(db).getIvmInfo());
        Assertions.assertTrue(getMtmv(db).getIvmInfo().isBinlogBroken());
    }

    @Test
    public void testReplacePartitionMarksBinlogBroken() throws Exception {
        String db = "ivm_broken_replace_partition";
        createPartitionedIvmTableAndMv(db);

        executeSql("ALTER TABLE ivm_base ADD TEMPORARY PARTITION tp202001 "
                + "VALUES [('2020-01-01'), ('2020-02-01'))");
        executeSql("ALTER TABLE ivm_base REPLACE PARTITION (p202001) "
                + "WITH TEMPORARY PARTITION (tp202001)");

        Assertions.assertTrue(getMtmv(db).getIvmInfo().isBinlogBroken());
    }

    @Test
    public void testRecoverPartitionMarksBinlogBroken() throws Exception {
        String db = "ivm_broken_recover_partition";
        createPartitionedIvmTableAndMv(db);
        executeSql("ALTER TABLE ivm_base DROP PARTITION p202001");
        setBinlogBroken(getMtmv(db), false);

        executeSql("RECOVER PARTITION p202001 FROM ivm_base");

        Assertions.assertTrue(getMtmv(db).getIvmInfo().isBinlogBroken());
    }

    @Test
    public void testAddPartitionDoesNotMarkBinlogBroken() throws Exception {
        String db = "ivm_broken_add_partition";
        createPartitionedIvmTableAndMv(db);

        executeSql("ALTER TABLE ivm_base ADD PARTITION p202003 "
                + "VALUES [('2020-03-01'), ('2020-04-01'))");

        Assertions.assertFalse(getMtmv(db).getIvmInfo().isBinlogBroken());
    }

    @Test
    public void testDropTempPartitionDoesNotMarkBinlogBroken() throws Exception {
        String db = "ivm_broken_drop_temp_partition";
        createPartitionedIvmTableAndMv(db);
        executeSql("ALTER TABLE ivm_base ADD TEMPORARY PARTITION tp202001 "
                + "VALUES [('2020-01-01'), ('2020-02-01'))");

        executeSql("ALTER TABLE ivm_base DROP TEMPORARY PARTITION tp202001");

        Assertions.assertFalse(getMtmv(db).getIvmInfo().isBinlogBroken());
    }

    @Test
    public void testDropMissingPartitionIfExistsDoesNotMarkBinlogBroken() throws Exception {
        String db = "ivm_broken_drop_missing_partition";
        createPartitionedIvmTableAndMv(db);

        executeSql("ALTER TABLE ivm_base DROP PARTITION IF EXISTS p_missing");

        Assertions.assertFalse(getMtmv(db).getIvmInfo().isBinlogBroken());
    }

    @Test
    public void testCompleteRefreshBaselineMarkerCanBeClearedAfterSuccess() throws Exception {
        String db = "ivm_broken_complete_refresh";
        createPartitionedIvmTableAndMv(db);
        MTMV mtmv = getMtmv(db);

        long markerGeneration = IvmRefreshManager.markIvmBaselineBroken(mtmv);
        Assertions.assertEquals(markerGeneration, IvmRefreshManager.markIvmBaselineBroken(mtmv));
        Assertions.assertTrue(mtmv.getIvmInfo().isBinlogBroken());

        Assertions.assertFalse(mtmv.markIvmBinlogBroken());
        Assertions.assertTrue(mtmv.getIvmBinlogBrokenGeneration() > markerGeneration);

        IvmPlanSignature signature = new IvmPlanSignature("current plan", "current-signature");
        JobException exception = Assertions.assertThrows(JobException.class,
                () -> IvmRefreshManager.finishIvmFullRefresh(mtmv, markerGeneration, signature));
        Assertions.assertTrue(exception.getMessage().contains("Base table metadata changed"));
        Assertions.assertTrue(mtmv.getIvmInfo().isBinlogBroken());

        IvmRefreshManager.finishIvmFullRefresh(
                mtmv, mtmv.getIvmBinlogBrokenGeneration(), signature);
        Assertions.assertFalse(mtmv.getIvmInfo().isBinlogBroken());
        Assertions.assertEquals(signature.getSha256(), mtmv.getIvmInfo().getPlanSignature());
    }

    @Test
    public void testCompleteRefreshRequiresPlanSignature() throws Exception {
        String db = "ivm_broken_complete_refresh_signature";
        createPartitionedIvmTableAndMv(db);
        MTMV mtmv = getMtmv(db);
        long markerGeneration = IvmRefreshManager.markIvmBaselineBroken(mtmv);

        Assertions.assertThrows(NullPointerException.class,
                () -> IvmRefreshManager.finishIvmFullRefresh(mtmv, markerGeneration, null));
        Assertions.assertTrue(mtmv.getIvmInfo().isBinlogBroken());
    }

    @Test
    public void testAddColumnDoesNotMarkBinlogBroken() throws Exception {
        String db = "ivm_broken_add_column";
        createPartitionedIvmTableAndMv(db);

        executeSql("ALTER TABLE ivm_base ADD COLUMN v2 INT");

        Assertions.assertFalse(getMtmv(db).getIvmInfo().isBinlogBroken());
    }

    @Test
    public void testDropColumnMarksBinlogBroken() throws Exception {
        String db = "ivm_broken_drop_column";
        createPartitionedIvmTableAndMv(db);
        executeSql("ALTER TABLE ivm_base ADD COLUMN v2 INT");

        executeSql("ALTER TABLE ivm_base DROP COLUMN v2");

        Assertions.assertTrue(getMtmv(db).getIvmInfo().isBinlogBroken());
    }

    @Test
    public void testRenameTableDoesNotMarkBinlogBroken() throws Exception {
        String db = "ivm_broken_rename_table";
        createPartitionedIvmTableAndMv(db);

        executeSql("ALTER TABLE ivm_base RENAME ivm_base_renamed");

        Assertions.assertFalse(getMtmv(db).getIvmInfo().isBinlogBroken());
    }

    @Test
    public void testReplaceTableMarksBinlogBroken() throws Exception {
        String db = "ivm_broken_replace_table";
        createPartitionedIvmTableAndMv(db);
        createTable("CREATE TABLE " + db + ".ivm_new_base (\n"
                + "  dt date NOT NULL,\n"
                + "  k1 int,\n"
                + "  v1 int\n"
                + ")\n"
                + "DUPLICATE KEY(dt, k1)\n"
                + "PARTITION BY RANGE(dt) (\n"
                + "  PARTITION p202001 VALUES [('2020-01-01'), ('2020-02-01')),\n"
                + "  PARTITION p202002 VALUES [('2020-02-01'), ('2020-03-01'))\n"
                + ")\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1', 'binlog.enable' = 'true', 'binlog.format' = 'ROW')");

        executeSql("ALTER TABLE ivm_base REPLACE WITH TABLE ivm_new_base PROPERTIES('swap' = 'false')");

        Assertions.assertTrue(getMtmv(db).getIvmInfo().isBinlogBroken());
    }

    @Test
    public void testReplaceTableSwapMarksBothSidesBinlogBroken() throws Exception {
        String db = "ivm_broken_replace_table_swap";
        createPartitionedIvmTableAndMv(db);
        createTable("CREATE TABLE " + db + ".ivm_new_base (\n"
                + "  dt date NOT NULL,\n"
                + "  k1 int,\n"
                + "  v1 int\n"
                + ")\n"
                + "DUPLICATE KEY(dt, k1)\n"
                + "PARTITION BY RANGE(dt) (\n"
                + "  PARTITION p202001 VALUES [('2020-01-01'), ('2020-02-01')),\n"
                + "  PARTITION p202002 VALUES [('2020-02-01'), ('2020-03-01'))\n"
                + ")\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1', 'binlog.enable' = 'true', 'binlog.format' = 'ROW')");
        createMvByNereids("CREATE MATERIALIZED VIEW ivm_new_mv\n"
                + "BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + "DISTRIBUTED BY RANDOM BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')\n"
                + "AS SELECT dt, k1, v1 FROM ivm_new_base");
        MTMV oldSideMtmv = getMtmv(db);
        MTMV newSideMtmv = (MTMV) getDb(db).getTableOrMetaException("ivm_new_mv");
        Assertions.assertFalse(oldSideMtmv.getIvmInfo().isBinlogBroken());
        Assertions.assertFalse(newSideMtmv.getIvmInfo().isBinlogBroken());

        executeSql("ALTER TABLE ivm_base REPLACE WITH TABLE ivm_new_base PROPERTIES('swap' = 'true')");

        Assertions.assertTrue(oldSideMtmv.getIvmInfo().isBinlogBroken());
        Assertions.assertTrue(newSideMtmv.getIvmInfo().isBinlogBroken());
    }

    @Test
    public void testReplayDropPartitionMarksBinlogBroken() throws Exception {
        String db = "ivm_broken_replay_drop_partition";
        createPartitionedIvmTableAndMv(db);
        Database database = getDb(db);
        OlapTable table = getBaseTable(db);
        Partition partition = table.getPartition("p202001");

        DropPartitionInfo info = new DropPartitionInfo(database.getId(), table.getId(), partition.getId(),
                "p202001", false, false, 0L, table.getVisibleVersion(), table.getVisibleVersionTime());
        Env.getCurrentInternalCatalog().replayDropPartition(info);

        Assertions.assertTrue(getMtmv(db).getIvmInfo().isBinlogBroken());
    }

    @Test
    public void testReplayTruncateMarksBinlogBroken() throws Exception {
        String db = "ivm_broken_replay_truncate";
        createPartitionedIvmTableAndMv(db);
        Database database = getDb(db);
        OlapTable table = getBaseTable(db);
        Partition oldPartition = table.getPartition("p202001");
        Partition newPartition = new Partition(Env.getCurrentEnv().getNextId(), oldPartition.getName(),
                new MaterializedIndex(table.getBaseIndexId(), MaterializedIndex.IndexState.NORMAL),
                oldPartition.getDistributionInfo());

        TruncateTableInfo info = new TruncateTableInfo(database.getId(), database.getFullName(), table.getId(),
                table.getName(), Collections.singletonList(newPartition), false,
                "TRUNCATE TABLE ivm_base PARTITION(p202001)", Collections.singletonList(oldPartition), true,
                Collections.emptyMap());
        Env.getCurrentInternalCatalog().replayTruncateTable(info);

        Assertions.assertTrue(getMtmv(db).getIvmInfo().isBinlogBroken());
    }

    @Test
    public void testReplayReplacePartitionMarksBinlogBroken() throws Exception {
        String db = "ivm_broken_replay_replace_partition";
        createPartitionedIvmTableAndMv(db);
        executeSql("ALTER TABLE ivm_base ADD TEMPORARY PARTITION tp202001 "
                + "VALUES [('2020-01-01'), ('2020-02-01'))");
        Database database = getDb(db);
        OlapTable table = getBaseTable(db);

        ReplacePartitionOperationLog log = new ReplacePartitionOperationLog(database.getId(), database.getFullName(),
                table.getId(), table.getName(), Collections.singletonList("p202001"),
                Collections.singletonList("tp202001"), Collections.emptyList(), false, false,
                table.getVisibleVersion(), table.getVisibleVersionTime(), false);
        Env.getCurrentEnv().replayReplaceTempPartition(log);

        Assertions.assertTrue(getMtmv(db).getIvmInfo().isBinlogBroken());
    }

    @Test
    public void testReplayRecoverPartitionMarksBinlogBroken() throws Exception {
        String db = "ivm_broken_replay_recover_partition";
        createPartitionedIvmTableAndMv(db);
        Database database = getDb(db);
        OlapTable table = getBaseTable(db);
        long partitionId = table.getPartition("p202001").getId();
        executeSql("ALTER TABLE ivm_base DROP PARTITION p202001");
        setBinlogBroken(getMtmv(db), false);

        RecoverInfo info = new RecoverInfo(database.getId(), table.getId(), partitionId, "", table.getName(),
                "", "p202001", null);
        Env.getCurrentInternalCatalog().replayRecoverPartition(info);

        Assertions.assertTrue(getMtmv(db).getIvmInfo().isBinlogBroken());
    }

    private void createPartitionedIvmTableAndMv(String db) throws Exception {
        createDatabaseAndUse(db);
        createTable("CREATE TABLE " + db + ".ivm_base (\n"
                + "  dt date NOT NULL,\n"
                + "  k1 int,\n"
                + "  v1 int\n"
                + ")\n"
                + "DUPLICATE KEY(dt, k1)\n"
                + "PARTITION BY RANGE(dt) (\n"
                + "  PARTITION p202001 VALUES [('2020-01-01'), ('2020-02-01')),\n"
                + "  PARTITION p202002 VALUES [('2020-02-01'), ('2020-03-01'))\n"
                + ")\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1', 'binlog.enable' = 'true', 'binlog.format' = 'ROW')");
        createMvByNereids("CREATE MATERIALIZED VIEW ivm_mv\n"
                + "BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + "DISTRIBUTED BY RANDOM BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')\n"
                + "AS SELECT dt, k1, v1 FROM ivm_base");
        Assertions.assertTrue(getMtmv(db).isIvm());
        Assertions.assertFalse(getMtmv(db).getIvmInfo().isBinlogBroken());
    }

    private MTMV getMtmv(String db) throws Exception {
        return (MTMV) Env.getCurrentInternalCatalog()
                .getDb(db).get()
                .getTableOrMetaException("ivm_mv");
    }

    private Database getDb(String db) {
        return Env.getCurrentInternalCatalog().getDb(db).get();
    }

    private OlapTable getBaseTable(String db) throws Exception {
        return (OlapTable) getDb(db).getTableOrMetaException("ivm_base");
    }

    private void setBinlogBroken(MTMV mtmv, boolean binlogBroken) {
        IvmInfo info = new IvmInfo(mtmv.getIvmInfo());
        info.setBinlogBroken(binlogBroken);
        Env.getCurrentEnv().alterMTMVIvmInfo(
                new TableNameInfo(mtmv.getQualifiedDbName(), mtmv.getName()), info);
    }
}
