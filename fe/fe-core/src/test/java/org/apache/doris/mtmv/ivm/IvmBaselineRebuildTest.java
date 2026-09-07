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
import org.apache.doris.catalog.stream.OlapTableStream;
import org.apache.doris.common.Config;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.job.common.TaskStatus;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.job.extensions.mtmv.MTMVTask;
import org.apache.doris.job.extensions.mtmv.MTMVTask.MTMVTaskTriggerMode;
import org.apache.doris.job.extensions.mtmv.MTMVTaskContext;
import org.apache.doris.mtmv.BaseColInfo;
import org.apache.doris.mtmv.BaseTableInfo;
import org.apache.doris.mtmv.MTMVAlterOpType;
import org.apache.doris.mtmv.MTMVPartitionInfo.MTMVPartitionType;
import org.apache.doris.mtmv.MTMVPlanUtil;
import org.apache.doris.mtmv.MTMVRefreshPartitionSnapshot;
import org.apache.doris.mtmv.MTMVRelation;
import org.apache.doris.mtmv.MTMVVersionSnapshot;
import org.apache.doris.persist.AlterMTMV;
import org.apache.doris.persist.DropPartitionInfo;
import org.apache.doris.persist.RecoverInfo;
import org.apache.doris.persist.ReplacePartitionOperationLog;
import org.apache.doris.persist.TruncateTableInfo;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

public class IvmBaselineRebuildTest extends TestWithFeService {

    @Override
    protected void runBeforeAll() throws Exception {
        Config.enable_table_stream = true;
    }

    @Test
    public void testTruncateMarksBaselineRebuild() throws Exception {
        String db = "ivm_broken_truncate";
        createPartitionedIvmTableAndMv(db);

        executeSql("TRUNCATE TABLE ivm_base");

        Assertions.assertTrue(getMtmv(db).getIvmInfo().isBaselineRebuildRequired());
    }

    @Test
    public void testTruncatePartitionMarksBaselineRebuild() throws Exception {
        String db = "ivm_broken_truncate_partition";
        createPartitionedIvmTableAndMv(db);

        executeSql("TRUNCATE TABLE ivm_base PARTITION(p202001)");

        Assertions.assertTrue(getMtmv(db).getIvmInfo().isBaselineRebuildRequired());
    }

    @Test
    public void testRepeatedBrokenEventsAdvanceSchemaChangeVersion() throws Exception {
        String db = "ivm_broken_repeated_partition_changes";
        createPartitionedIvmTableAndMv(db);
        MTMV mtmv = getMtmv(db);
        long initialSchemaChangeVersion = mtmv.getSchemaChangeVersion();

        executeSql("TRUNCATE TABLE ivm_base PARTITION(p202001)");
        Assertions.assertEquals(initialSchemaChangeVersion + 1, mtmv.getSchemaChangeVersion());

        executeSql("TRUNCATE TABLE ivm_base PARTITION(p202002)");
        Assertions.assertEquals(initialSchemaChangeVersion + 2, mtmv.getSchemaChangeVersion());
        Assertions.assertTrue(mtmv.getIvmInfo().isBaselineRebuildRequired());
    }

    @Test
    public void testDropPartitionMarksBaselineRebuild() throws Exception {
        String db = "ivm_broken_drop_partition";
        createPartitionedIvmTableAndMv(db);
        executeSql("ALTER TABLE ivm_base DROP PARTITION p202001");

        Assertions.assertTrue(getMtmv(db).getIvmInfo().isBaselineRebuildRequired());
    }

    @Test
    public void testPublishedPctPartitionUsesPartitionsBaselineRebuild() throws Exception {
        String db = "ivm_partitions_baseline_rebuild";
        createPartitionedIvmTableAndMv(db);
        MTMV mtmv = getMtmv(db);
        OlapTable baseTable = getBaseTable(db);
        publishPctPartitionSnapshot(mtmv, baseTable, "p202001");

        executeSql("ALTER TABLE ivm_base DROP PARTITION p202001");

        Assertions.assertFalse(mtmv.getIvmInfo().requiresCompleteBaselineRebuild());
        Assertions.assertEquals(Collections.singleton("mv_partition"),
                mtmv.getIvmInfo().getPendingBaselineRebuildPartitions());
    }

    @Test
    public void testMissingPctSnapshotRequiresCompleteBaselineRebuild() throws Exception {
        String db = "ivm_complete_baseline_rebuild";
        createPartitionedIvmTableAndMv(db);
        MTMV mtmv = getMtmv(db);
        mtmv.getMvPartitionInfo().setPartitionType(MTMVPartitionType.FOLLOW_BASE_TABLE);
        mtmv.getMvPartitionInfo().setPctInfos(Collections.singletonList(
                new BaseColInfo("dt", new BaseTableInfo(getBaseTable(db)))));

        executeSql("ALTER TABLE ivm_base DROP PARTITION p202001");

        Assertions.assertTrue(mtmv.getIvmInfo().requiresCompleteBaselineRebuild());
    }

    @Test
    public void testReplacePartitionMarksBaselineRebuild() throws Exception {
        String db = "ivm_broken_replace_partition";
        createPartitionedIvmTableAndMv(db);

        executeSql("ALTER TABLE ivm_base ADD TEMPORARY PARTITION tp202001 "
                + "VALUES [('2020-01-01'), ('2020-02-01'))");
        executeSql("ALTER TABLE ivm_base REPLACE PARTITION (p202001) "
                + "WITH TEMPORARY PARTITION (tp202001)");

        Assertions.assertTrue(getMtmv(db).getIvmInfo().isBaselineRebuildRequired());
    }

    @Test
    public void testRecoverPartitionMarksBaselineRebuild() throws Exception {
        String db = "ivm_broken_recover_partition";
        createPartitionedIvmTableAndMv(db);
        executeSql("ALTER TABLE ivm_base DROP PARTITION p202001");
        clearBaselineRebuild(getMtmv(db));

        executeSql("RECOVER PARTITION p202001 FROM ivm_base");

        Assertions.assertTrue(getMtmv(db).getIvmInfo().isBaselineRebuildRequired());
    }

    @Test
    public void testRecoverAndDropKeepGlobalBrokenState() throws Exception {
        String db = "ivm_broken_recover_and_drop";
        createPartitionedIvmTableAndMv(db);
        MTMV mtmv = getMtmv(db);

        executeSql("ALTER TABLE ivm_base DROP PARTITION p202001");
        Assertions.assertTrue(mtmv.getIvmInfo().isBaselineRebuildRequired());

        executeSql("RECOVER PARTITION p202001 FROM ivm_base");
        Assertions.assertTrue(mtmv.getIvmInfo().isBaselineRebuildRequired());

        executeSql("ALTER TABLE ivm_base DROP PARTITION p202002");
        Assertions.assertTrue(mtmv.getIvmInfo().isBaselineRebuildRequired());
    }

    @Test
    public void testAddPartitionDoesNotMarkBaselineRebuild() throws Exception {
        String db = "ivm_broken_add_partition";
        createPartitionedIvmTableAndMv(db);

        executeSql("ALTER TABLE ivm_base ADD PARTITION p202003 "
                + "VALUES [('2020-03-01'), ('2020-04-01'))");

        Assertions.assertFalse(getMtmv(db).getIvmInfo().isBaselineRebuildRequired());
    }

    @Test
    public void testDropTempPartitionDoesNotMarkBaselineRebuild() throws Exception {
        String db = "ivm_broken_drop_temp_partition";
        createPartitionedIvmTableAndMv(db);
        executeSql("ALTER TABLE ivm_base ADD TEMPORARY PARTITION tp202001 "
                + "VALUES [('2020-01-01'), ('2020-02-01'))");

        executeSql("ALTER TABLE ivm_base DROP TEMPORARY PARTITION tp202001");

        Assertions.assertFalse(getMtmv(db).getIvmInfo().isBaselineRebuildRequired());
    }

    @Test
    public void testDropMissingPartitionIfExistsDoesNotMarkBaselineRebuild() throws Exception {
        String db = "ivm_broken_drop_missing_partition";
        createPartitionedIvmTableAndMv(db);

        executeSql("ALTER TABLE ivm_base DROP PARTITION IF EXISTS p_missing");

        Assertions.assertFalse(getMtmv(db).getIvmInfo().isBaselineRebuildRequired());
    }

    @Test
    public void testRenameTableDoesNotMarkBaselineRebuild() throws Exception {
        String db = "ivm_broken_rename_table";
        createPartitionedIvmTableAndMv(db);

        executeSql("ALTER TABLE ivm_base RENAME ivm_base_renamed");

        Assertions.assertFalse(getMtmv(db).getIvmInfo().isBaselineRebuildRequired());
    }

    @Test
    public void testReplaceTableMarksBaselineRebuild() throws Exception {
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
                + "PROPERTIES ('replication_num' = '1', 'light_schema_change' = 'true', "
                + "'binlog.enable' = 'true', 'binlog.format' = 'ROW')");

        executeSql("ALTER TABLE ivm_base REPLACE WITH TABLE ivm_new_base PROPERTIES('swap' = 'false')");

        Assertions.assertTrue(getMtmv(db).getIvmInfo().isBaselineRebuildRequired());
    }

    @Test
    public void testReconcileIvmStreamAfterTableReplace() throws Exception {
        String db = "ivm_reconcile_replace_table";
        createPartitionedIvmTableAndMv(db);
        MTMV mtmv = getMtmv(db);
        Database database = getDb(db);
        OlapTable originalBaseTable = getBaseTable(db);
        String streamName = IvmUtil.streamName(mtmv.getId(), originalBaseTable.getFullQualifiers());
        OlapTableStream originalStream = (OlapTableStream) database.getTableOrMetaException(streamName);
        ConnectContext ctx = createDefaultCtx();
        ctx.setDatabase(db);

        reconcileIvmStreams(mtmv, ctx);
        Assertions.assertSame(originalStream, database.getTableOrMetaException(streamName));

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

        OlapTable replacedBaseTable = getBaseTable(db);
        OlapTableStream staleStream = (OlapTableStream) database.getTableOrMetaException(streamName);
        Assertions.assertFalse(IvmUtil.isIvmStreamUsable(staleStream, replacedBaseTable));

        reconcileIvmStreams(mtmv, ctx);
        OlapTableStream reconciledStream = (OlapTableStream) database.getTableOrMetaException(streamName);
        Assertions.assertNotSame(originalStream, reconciledStream);
        Assertions.assertEquals(replacedBaseTable.getId(), reconciledStream.getBaseTableNullable().getId());
    }

    private void reconcileIvmStreams(MTMV mtmv, ConnectContext ctx) throws Exception {
        MTMVPlanUtil.QueryAnalysisResult result = MTMVPlanUtil.getBaseTableFromQuery(mtmv.getQuerySql(), ctx);
        MTMVRelation relation = MTMVPlanUtil.generateMTMVRelation(
                result.getAllLevelTables(), result.getOneLevelTables());
        MTMVTask task = new MTMVTask(mtmv, relation, new MTMVTaskContext(MTMVTaskTriggerMode.MANUAL));
        Deencapsulation.invoke(task, "reconcileIvmStreams", ctx);
    }

    @Test
    public void testReplaceTableSwapMarksBothSidesBaselineRebuild() throws Exception {
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
        Assertions.assertFalse(oldSideMtmv.getIvmInfo().isBaselineRebuildRequired());
        Assertions.assertFalse(newSideMtmv.getIvmInfo().isBaselineRebuildRequired());

        executeSql("ALTER TABLE ivm_base REPLACE WITH TABLE ivm_new_base PROPERTIES('swap' = 'true')");

        Assertions.assertTrue(oldSideMtmv.getIvmInfo().isBaselineRebuildRequired());
        Assertions.assertTrue(newSideMtmv.getIvmInfo().isBaselineRebuildRequired());
    }

    @Test
    public void testReplayDropPartitionDoesNotCreateBarrier() throws Exception {
        String db = "ivm_broken_replay_drop_partition";
        createPartitionedIvmTableAndMv(db);
        Database database = getDb(db);
        OlapTable table = getBaseTable(db);
        Partition partition = table.getPartition("p202001");

        DropPartitionInfo info = new DropPartitionInfo(database.getId(), table.getId(), partition.getId(),
                "p202001", false, false, 0L, table.getVisibleVersion(), table.getVisibleVersionTime());
        Env.getCurrentInternalCatalog().replayDropPartition(info);

        Assertions.assertFalse(getMtmv(db).getIvmInfo().isBaselineRebuildRequired());
    }

    @Test
    public void testReplayTruncateDoesNotCreateBarrier() throws Exception {
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
                Collections.emptyMap(), table.getNextVersion(), System.currentTimeMillis());
        Env.getCurrentInternalCatalog().replayTruncateTable(info);

        Assertions.assertFalse(getMtmv(db).getIvmInfo().isBaselineRebuildRequired());
    }

    @Test
    public void testReplayReplacePartitionDoesNotCreateBarrier() throws Exception {
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

        Assertions.assertFalse(getMtmv(db).getIvmInfo().isBaselineRebuildRequired());
    }

    @Test
    public void testReplayRecoverPartitionDoesNotCreateBarrier() throws Exception {
        String db = "ivm_broken_replay_recover_partition";
        createPartitionedIvmTableAndMv(db);
        Database database = getDb(db);
        OlapTable table = getBaseTable(db);
        long partitionId = table.getPartition("p202001").getId();
        executeSql("ALTER TABLE ivm_base DROP PARTITION p202001");
        clearBaselineRebuild(getMtmv(db));

        RecoverInfo info = new RecoverInfo(database.getId(), table.getId(), partitionId, "", table.getName(),
                "", "p202001", null);
        Env.getCurrentInternalCatalog().replayRecoverPartition(info);

        Assertions.assertFalse(getMtmv(db).getIvmInfo().isBaselineRebuildRequired());
    }

    @Test
    public void testStaleTaskResultDoesNotMutateMtmv() throws Exception {
        String db = "ivm_stale_task_result";
        createPartitionedIvmTableAndMv(db);
        MTMV mtmv = getMtmv(db);
        long taskVersion = mtmv.getSchemaChangeVersion();
        IvmInfo pending = mtmv.getIvmInfo();
        pending.requireCompleteBaselineRebuild();
        mtmv.alterIvmInfo(pending);
        Deencapsulation.setField(mtmv, "schemaChangeVersion", taskVersion + 1);
        int historySize = mtmv.getHistoryTasks().size();

        AlterMTMV result = taskResult(mtmv, TaskStatus.FAILED, taskVersion);
        Deencapsulation.setField(result.getTask(), "refreshedIvmPlanSignature", "new_signature");
        String planSignature = mtmv.getIvmInfo().getPlanSignature();

        Assertions.assertFalse(mtmv.addTaskResult(result, false));
        Assertions.assertEquals(historySize, mtmv.getHistoryTasks().size());
        Assertions.assertTrue(mtmv.getIvmInfo().isBaselineRebuildRequired());
        Assertions.assertEquals(planSignature, mtmv.getIvmInfo().getPlanSignature());
        Assertions.assertEquals(taskVersion + 1, mtmv.getSchemaChangeVersion());
    }

    @Test
    public void testIvmRefreshStartRejectsStaleVersionOrPendingBaseline() throws Exception {
        String db = "ivm_refresh_start_validation";
        createPartitionedIvmTableAndMv(db);
        MTMV mtmv = getMtmv(db);
        long version = mtmv.getSchemaChangeVersion();

        mtmv.validateIvmRefreshStart(version);
        Assertions.assertThrows(JobException.class, () -> mtmv.validateIvmRefreshStart(version + 1));

        IvmInfo pending = mtmv.getIvmInfo();
        pending.requireCompleteBaselineRebuild();
        mtmv.alterIvmInfo(pending);
        Assertions.assertThrows(JobException.class, () -> mtmv.validateIvmRefreshStart(version));
    }

    @Test
    public void testReplayTaskResultAppliesIvmStateWithoutChangingVersion() throws Exception {
        String db = "ivm_replay_task_result";
        createPartitionedIvmTableAndMv(db);
        MTMV mtmv = getMtmv(db);
        long schemaChangeVersion = mtmv.getSchemaChangeVersion();
        AlterMTMV result = taskResult(mtmv, TaskStatus.FAILED, schemaChangeVersion);
        IvmInfo replayedInfo = mtmv.getIvmInfo();
        replayedInfo.requireCompleteBaselineRebuild();
        replayedInfo.setPlanSignature("replayed_signature");
        result.setIvmInfo(replayedInfo);

        Assertions.assertTrue(mtmv.addTaskResult(result, true));
        Assertions.assertTrue(mtmv.getIvmInfo().isBaselineRebuildRequired());
        Assertions.assertEquals("replayed_signature", mtmv.getIvmInfo().getPlanSignature());
        Assertions.assertEquals(schemaChangeVersion, mtmv.getSchemaChangeVersion());

        replayedInfo.clearBaselineRebuild();
        Assertions.assertTrue(mtmv.getIvmInfo().isBaselineRebuildRequired());
    }

    @Test
    public void testSuccessfulBaselineResultClearsPendingState() throws Exception {
        String db = "ivm_successful_baseline_result";
        createPartitionedIvmTableAndMv(db);
        MTMV mtmv = getMtmv(db);
        IvmInfo pending = mtmv.getIvmInfo();
        pending.requireCompleteBaselineRebuild();
        mtmv.alterIvmInfo(pending);
        String planSignature = mtmv.getIvmInfo().getPlanSignature();
        AlterMTMV result = taskResult(mtmv, TaskStatus.SUCCESS, mtmv.getSchemaChangeVersion());
        boolean compatibilityMode = Config.enable_check_compatibility_mode;
        try {
            Config.enable_check_compatibility_mode = true;
            Assertions.assertTrue(mtmv.addTaskResult(result, false));
        } finally {
            Config.enable_check_compatibility_mode = compatibilityMode;
        }

        Assertions.assertFalse(mtmv.getIvmInfo().isBaselineRebuildRequired());
        Assertions.assertFalse(result.getIvmInfo().isBaselineRebuildRequired());
        Assertions.assertEquals(planSignature, mtmv.getIvmInfo().getPlanSignature());
    }

    @Test
    public void testSuccessfulSignatureFallbackPublishesNewPlanSignature() throws Exception {
        String db = "ivm_successful_signature_fallback";
        createPartitionedIvmTableAndMv(db);
        MTMV mtmv = getMtmv(db);
        IvmInfo pending = mtmv.getIvmInfo();
        pending.requireCompleteBaselineRebuild();
        mtmv.alterIvmInfo(pending);
        AlterMTMV result = taskResult(mtmv, TaskStatus.SUCCESS, mtmv.getSchemaChangeVersion());
        Deencapsulation.setField(result.getTask(), "refreshedIvmPlanSignature", "new_signature");
        boolean compatibilityMode = Config.enable_check_compatibility_mode;
        try {
            Config.enable_check_compatibility_mode = true;
            Assertions.assertTrue(mtmv.addTaskResult(result, false));
        } finally {
            Config.enable_check_compatibility_mode = compatibilityMode;
        }

        Assertions.assertEquals("new_signature", mtmv.getIvmInfo().getPlanSignature());
        Assertions.assertEquals("new_signature", result.getIvmInfo().getPlanSignature());
        Assertions.assertFalse(mtmv.getIvmInfo().isBaselineRebuildRequired());
    }

    @Test
    public void testFailedBaselineResultKeepsGuard() throws Exception {
        String db = "ivm_failed_baseline_result";
        createPartitionedIvmTableAndMv(db);
        MTMV mtmv = getMtmv(db);
        IvmInfo pending = mtmv.getIvmInfo();
        pending.requireCompleteBaselineRebuild();
        mtmv.alterIvmInfo(pending);
        AlterMTMV result = taskResult(mtmv, TaskStatus.FAILED, mtmv.getSchemaChangeVersion());
        Deencapsulation.setField(result.getTask(), "refreshedIvmPlanSignature", "new_signature");
        String planSignature = mtmv.getIvmInfo().getPlanSignature();

        Assertions.assertTrue(mtmv.addTaskResult(result, false));

        Assertions.assertTrue(mtmv.getIvmInfo().isBaselineRebuildRequired());
        Assertions.assertTrue(result.getIvmInfo().isBaselineRebuildRequired());
        Assertions.assertEquals(planSignature, mtmv.getIvmInfo().getPlanSignature());
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
        Assertions.assertFalse(getMtmv(db).getIvmInfo().isBaselineRebuildRequired());
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

    private void publishPctPartitionSnapshot(MTMV mtmv, OlapTable baseTable, String partitionName) {
        BaseTableInfo baseTableInfo = new BaseTableInfo(baseTable);
        mtmv.getMvPartitionInfo().setPartitionType(MTMVPartitionType.FOLLOW_BASE_TABLE);
        mtmv.getMvPartitionInfo().setPctInfos(Collections.singletonList(new BaseColInfo("dt", baseTableInfo)));
        MTMVRefreshPartitionSnapshot snapshot = new MTMVRefreshPartitionSnapshot();
        snapshot.getPctSnapshot(baseTableInfo).put(partitionName,
                new MTMVVersionSnapshot(1L, baseTable.getPartition(partitionName).getId()));
        mtmv.getRefreshSnapshot().updateSnapshots(
                Collections.singletonMap("mv_partition", snapshot), Collections.singleton("mv_partition"));
    }

    private void clearBaselineRebuild(MTMV mtmv) {
        IvmInfo info = new IvmInfo(mtmv.getIvmInfo());
        info.clearBaselineRebuild();
        mtmv.alterIvmInfo(info);
    }

    private AlterMTMV taskResult(MTMV mtmv, TaskStatus status, long schemaChangeVersion) {
        MTMVTask task = new MTMVTask();
        task.setStatus(status);
        Deencapsulation.setField(task, "mtmvSchemaChangeVersion", schemaChangeVersion);
        AlterMTMV result = new AlterMTMV(
                new TableNameInfo(mtmv.getQualifiedDbName(), mtmv.getName()), MTMVAlterOpType.ADD_TASK);
        result.setTask(task);
        result.setRelation(mtmv.getRelation());
        result.setPartitionSnapshots(Collections.emptyMap());
        return result;
    }
}
