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

package org.apache.doris.mtmv;

import org.apache.doris.analysis.PartitionKeyDesc;
import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.RangePartitionItem;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.SinglePartitionInfo;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.job.common.IntervalUnit;
import org.apache.doris.job.common.TaskStatus;
import org.apache.doris.job.extensions.mtmv.MTMVTask;
import org.apache.doris.mtmv.MTMVRefreshEnum.BuildMode;
import org.apache.doris.mtmv.MTMVRefreshEnum.MTMVRefreshState;
import org.apache.doris.mtmv.MTMVRefreshEnum.MTMVState;
import org.apache.doris.mtmv.MTMVRefreshEnum.RefreshMethod;
import org.apache.doris.mtmv.MTMVRefreshEnum.RefreshTrigger;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.persist.AlterMTMV;
import org.apache.doris.persist.EditLog;
import org.apache.doris.persist.EditLog.EditLogItem;
import org.apache.doris.persist.OperationType;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.thrift.TStorageType;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MTMVTest {
    @Test
    public void testToInfoString() {
        String expect
                = "MTMV{refreshInfo=BUILD IMMEDIATE REFRESH COMPLETE ON SCHEDULE EVERY 2 SECOND STARTS \"ss\", "
                + "querySql='select * from xxx;', "
                + "status=MTMVStatus{state=INIT, schemaChangeDetail='null', refreshState=INIT}, "
                + "jobInfo=MTMVJobInfo{jobName='job1', "
                + "historyTasks=[MTMVTask{dbId=0, mtmvId=0, taskContext=null, "
                + "needRefreshPartitions=null, completedPartitions=null, refreshMode=null} "
                + "AbstractTask{jobId=null, taskId=1, status=null, createTimeMs=null, startTimeMs=null, "
                + "finishTimeMs=null, taskType=null, errMsg='null'}]}, mvProperties={}, "
                + "relation=MTMVRelation{baseTables=[], baseTablesOneLevel=[], baseViews=[]}, "
                + "mvPartitionInfo=MTMVPartitionInfo{partitionType=null, pctInfos=[], "
                + "partitionCol='null', expr='null'}, "
                + "refreshSnapshot=MTMVRefreshSnapshot{partitionSnapshots={}}, id=1, name='null', "
                + "qualifiedDbName='db1', comment='comment1'}";
        MTMV mtmv = new MTMV();
        mtmv.setId(1L);
        mtmv.setComment("comment1");
        mtmv.setQualifiedDbName("db1");
        mtmv.setRefreshInfo(buildMTMVRefreshInfo(mtmv));
        mtmv.setQuerySql("select * from xxx;");
        mtmv.setStatus(new MTMVStatus());
        mtmv.setJobInfo(buildMTMVJobInfo(mtmv));
        mtmv.setMvProperties(new HashMap<>());
        mtmv.setRelation(new MTMVRelation(Sets.newHashSet(), Sets.newHashSet(), Sets.newHashSet(), Sets.newHashSet(),
                Sets.newHashSet()));
        mtmv.setMvPartitionInfo(new MTMVPartitionInfo());
        mtmv.setRefreshSnapshot(new MTMVRefreshSnapshot());
        Assertions.assertEquals(expect, mtmv.toInfoString());
    }

    private MTMVRefreshInfo buildMTMVRefreshInfo(MTMV mtmv) {
        MTMVRefreshTriggerInfo info = new MTMVRefreshTriggerInfo(RefreshTrigger.SCHEDULE,
                new MTMVRefreshSchedule("ss", 2,
                        IntervalUnit.SECOND));
        MTMVRefreshInfo mtmvRefreshInfo = new MTMVRefreshInfo(BuildMode.IMMEDIATE, RefreshMethod.COMPLETE, info);
        return mtmvRefreshInfo;
    }

    private MTMVJobInfo buildMTMVJobInfo(MTMV mtmv) {
        MTMVJobInfo mtmvJobInfo = new MTMVJobInfo("job1");
        mtmvJobInfo.addHistoryTask(buildMTMVTask(mtmv));
        return mtmvJobInfo;
    }

    private MTMVTask buildMTMVTask(MTMV mtmv) {
        MTMVTask task = new MTMVTask(mtmv, null, null);
        task.setTaskId(1L);
        return task;
    }

    @Test
    public void testCalculateDoublyPartitionMappings() throws AnalysisException {
        Map<String, Set<String>> mvToBase = Maps.newHashMap();
        Map<String, String> baseToMv = Maps.newHashMap();
        Map<PartitionKeyDesc, Set<String>> relatedPartitionDescs = mockRelatedPartitionDescs();
        Map<String, PartitionItem> mvPartitionItems = mockMvPartitionItems();
        for (Entry<String, PartitionItem> entry : mvPartitionItems.entrySet()) {
            Set<String> basePartitionNames = relatedPartitionDescs.getOrDefault(entry.getValue().toPartitionKeyDesc(),
                    Sets.newHashSet());
            String mvPartitionName = entry.getKey();
            mvToBase.put(mvPartitionName, basePartitionNames);
            for (String basePartitionName : basePartitionNames) {
                baseToMv.put(basePartitionName, mvPartitionName);
            }
        }
        Assertions.assertEquals(mvToBase.get("mvp1"), Sets.newHashSet("baseP1_1", "baseP1_2"));
        Assertions.assertEquals(baseToMv.get("baseP1_1"), "mvp1");
        Assertions.assertEquals(baseToMv.get("baseP1_2"), "mvp1");
    }

    private Map<PartitionKeyDesc, Set<String>> mockRelatedPartitionDescs() throws AnalysisException {
        Map<PartitionKeyDesc, Set<String>> res = Maps.newHashMap();
        Column k1 = new Column("k1", ScalarType.createType(PrimitiveType.TINYINT), true, null, "", "key1");
        PartitionKey rangeP1Lower = PartitionKey.createPartitionKey(Lists.newArrayList(new PartitionValue("1")),
                Lists.newArrayList(k1));
        PartitionKey rangeP1Upper = PartitionKey.createPartitionKey(Lists.newArrayList(new PartitionValue("10")),
                Lists.newArrayList(k1));
        Range<PartitionKey> rangeP1 = Range.closedOpen(rangeP1Lower, rangeP1Upper);
        PartitionItem item1 = new RangePartitionItem(rangeP1);
        res.put(item1.toPartitionKeyDesc(), Sets.newHashSet("baseP1_1", "baseP1_2"));
        return res;
    }

    private Map<String, PartitionItem> mockMvPartitionItems() throws AnalysisException {
        Map<String, PartitionItem> res = Maps.newHashMap();
        Column k1 = new Column("k1", ScalarType.createType(PrimitiveType.TINYINT), true, null, "", "key1");
        PartitionKey rangeP1Lower = PartitionKey.createPartitionKey(Lists.newArrayList(new PartitionValue("1")),
                Lists.newArrayList(k1));
        PartitionKey rangeP1Upper = PartitionKey.createPartitionKey(Lists.newArrayList(new PartitionValue("10")),
                Lists.newArrayList(k1));
        Range<PartitionKey> rangeP1 = Range.closedOpen(rangeP1Lower, rangeP1Upper);
        PartitionItem item1 = new RangePartitionItem(rangeP1);
        res.put("mvp1", item1);
        return res;
    }

    @Test
    public void testGetExcludedTriggerTables() {
        Map<String, String> mvProperties = Maps.newHashMap();
        MTMV mtmv = new MTMV();
        mtmv.setMvProperties(mvProperties);

        mvProperties.put(PropertyAnalyzer.PROPERTIES_EXCLUDED_TRIGGER_TABLES, "t1");
        Set<TableNameInfo> excludedTriggerTables = mtmv.getExcludedTriggerTables();
        Assertions.assertEquals(1, excludedTriggerTables.size());
        Assertions.assertTrue(excludedTriggerTables.contains(new TableNameInfo(null, null, "t1")));

        mvProperties.put(PropertyAnalyzer.PROPERTIES_EXCLUDED_TRIGGER_TABLES, "db1.t1");
        excludedTriggerTables = mtmv.getExcludedTriggerTables();
        Assertions.assertEquals(1, excludedTriggerTables.size());
        Assertions.assertTrue(excludedTriggerTables.contains(new TableNameInfo(null, "db1", "t1")));

        mvProperties.put(PropertyAnalyzer.PROPERTIES_EXCLUDED_TRIGGER_TABLES, "ctl1.db1.t1");
        excludedTriggerTables = mtmv.getExcludedTriggerTables();
        Assertions.assertEquals(1, excludedTriggerTables.size());
        Assertions.assertTrue(excludedTriggerTables.contains(new TableNameInfo("ctl1", "db1", "t1")));

        mvProperties.put(PropertyAnalyzer.PROPERTIES_EXCLUDED_TRIGGER_TABLES, "ctl1.db1.t1,db2.t2,t3");
        excludedTriggerTables = mtmv.getExcludedTriggerTables();
        Assertions.assertEquals(3, excludedTriggerTables.size());
        Assertions.assertTrue(excludedTriggerTables.contains(new TableNameInfo("ctl1", "db1", "t1")));
        Assertions.assertTrue(excludedTriggerTables.contains(new TableNameInfo(null, "db2", "t2")));
        Assertions.assertTrue(excludedTriggerTables.contains(new TableNameInfo(null, null, "t3")));

        mvProperties.put(PropertyAnalyzer.PROPERTIES_EXCLUDED_TRIGGER_TABLES,
                " ctl1.db1.t1 , db2.t2, ,  t3  ");
        excludedTriggerTables = mtmv.getExcludedTriggerTables();
        Assertions.assertEquals(3, excludedTriggerTables.size());
        Assertions.assertTrue(excludedTriggerTables.contains(new TableNameInfo("ctl1", "db1", "t1")));
        Assertions.assertTrue(excludedTriggerTables.contains(new TableNameInfo(null, "db2", "t2")));
        Assertions.assertTrue(excludedTriggerTables.contains(new TableNameInfo(null, null, "t3")));
    }

    @Test
    public void testAlterMvPropertiesWithExcludedTriggerTablesChange() {
        Map<String, String> mvProperties = Maps.newHashMap();
        mvProperties.put(PropertyAnalyzer.PROPERTIES_EXCLUDED_TRIGGER_TABLES, "t1");
        MTMV mtmv = new MTMV();
        mtmv.setMvProperties(mvProperties);
        MTMVStatus status = new MTMVStatus(MTMVState.NORMAL, null);
        mtmv.setStatus(status);
        MTMVRefreshSnapshot refreshSnapshot = new MTMVRefreshSnapshot();
        refreshSnapshot.getPartitionSnapshots().put("p1", new MTMVRefreshPartitionSnapshot());
        mtmv.setRefreshSnapshot(refreshSnapshot);

        long oldSchemaChangeVersion = mtmv.getSchemaChangeVersion();
        Map<String, String> newProperties = Maps.newHashMap();
        newProperties.put(PropertyAnalyzer.PROPERTIES_EXCLUDED_TRIGGER_TABLES, "db1.t1");

        replayAlterMvProperties(mtmv, newProperties);

        Assertions.assertEquals(MTMVState.NORMAL, mtmv.getStatus().getState());
        Assertions.assertEquals(oldSchemaChangeVersion, mtmv.getSchemaChangeVersion());
        // Only excluded_trigger_tables changed, and nothing here brings a base table back into
        // what the MV maintains, so the change owes neither a version bump nor a snapshot
        // drop: the rows the MV holds stay valid and no rebuild is asked for.
        Assertions.assertFalse(mtmv.getRefreshSnapshot().getPartitionSnapshots().isEmpty());

        mtmv.getRefreshSnapshot().getPartitionSnapshots().put("p1", new MTMVRefreshPartitionSnapshot());
        oldSchemaChangeVersion = mtmv.getSchemaChangeVersion();
        newProperties.put(PropertyAnalyzer.PROPERTIES_EXCLUDED_TRIGGER_TABLES, "internal.db1.t1");

        replayAlterMvProperties(mtmv, newProperties);

        Assertions.assertEquals(MTMVState.NORMAL, mtmv.getStatus().getState());
        Assertions.assertEquals(oldSchemaChangeVersion, mtmv.getSchemaChangeVersion());
        // Only excluded_trigger_tables changed, and nothing here brings a base table back into
        // what the MV maintains, so the change owes neither a version bump nor a snapshot
        // drop: the rows the MV holds stay valid and no rebuild is asked for.
        Assertions.assertFalse(mtmv.getRefreshSnapshot().getPartitionSnapshots().isEmpty());
    }

    @Test
    public void testAlterMvPropertiesWithSameExcludedTriggerTables() {
        Map<String, String> mvProperties = Maps.newHashMap();
        mvProperties.put(PropertyAnalyzer.PROPERTIES_EXCLUDED_TRIGGER_TABLES, "t1,t2");
        MTMV mtmv = new MTMV();
        mtmv.setMvProperties(mvProperties);
        MTMVRefreshSnapshot refreshSnapshot = new MTMVRefreshSnapshot();
        refreshSnapshot.getPartitionSnapshots().put("p1", new MTMVRefreshPartitionSnapshot());
        mtmv.setRefreshSnapshot(refreshSnapshot);

        long oldSchemaChangeVersion = mtmv.getSchemaChangeVersion();
        Map<String, String> newProperties = Maps.newHashMap();
        newProperties.put(PropertyAnalyzer.PROPERTIES_EXCLUDED_TRIGGER_TABLES, "t2,t1");

        replayAlterMvProperties(mtmv, newProperties);

        Assertions.assertEquals(oldSchemaChangeVersion, mtmv.getSchemaChangeVersion());
        Assertions.assertFalse(mtmv.getRefreshSnapshot().getPartitionSnapshots().isEmpty());
    }

    @Test
    public void testAlterMvPropertiesWithReducedExcludedTriggerTables() {
        Map<String, String> mvProperties = Maps.newHashMap();
        mvProperties.put(PropertyAnalyzer.PROPERTIES_EXCLUDED_TRIGGER_TABLES, "t1,t2");
        MTMV mtmv = new MTMV();
        mtmv.setMvProperties(mvProperties);
        mtmv.setStatus(new MTMVStatus(MTMVState.NORMAL, null));
        MTMVRefreshSnapshot refreshSnapshot = new MTMVRefreshSnapshot();
        refreshSnapshot.getPartitionSnapshots().put("p1", new MTMVRefreshPartitionSnapshot());
        mtmv.setRefreshSnapshot(refreshSnapshot);

        long oldSchemaChangeVersion = mtmv.getSchemaChangeVersion();
        Map<String, String> newProperties = Maps.newHashMap();
        newProperties.put(PropertyAnalyzer.PROPERTIES_EXCLUDED_TRIGGER_TABLES, "t1");

        replayAlterMvProperties(mtmv, newProperties);

        Assertions.assertEquals(MTMVState.NORMAL, mtmv.getStatus().getState());
        Assertions.assertEquals(oldSchemaChangeVersion, mtmv.getSchemaChangeVersion());
        // Only excluded_trigger_tables changed, and nothing here brings a base table back into
        // what the MV maintains, so the change owes neither a version bump nor a snapshot
        // drop: the rows the MV holds stay valid and no rebuild is asked for.
        Assertions.assertFalse(mtmv.getRefreshSnapshot().getPartitionSnapshots().isEmpty());

        mtmv.getRefreshSnapshot().getPartitionSnapshots().put("p1", new MTMVRefreshPartitionSnapshot());
        oldSchemaChangeVersion = mtmv.getSchemaChangeVersion();
        newProperties.put(PropertyAnalyzer.PROPERTIES_EXCLUDED_TRIGGER_TABLES, "");

        replayAlterMvProperties(mtmv, newProperties);

        Assertions.assertEquals(MTMVState.NORMAL, mtmv.getStatus().getState());
        Assertions.assertEquals(oldSchemaChangeVersion, mtmv.getSchemaChangeVersion());
        // Only excluded_trigger_tables changed, and nothing here brings a base table back into
        // what the MV maintains, so the change owes neither a version bump nor a snapshot
        // drop: the rows the MV holds stay valid and no rebuild is asked for.
        Assertions.assertFalse(mtmv.getRefreshSnapshot().getPartitionSnapshots().isEmpty());
    }

    @Test
    public void testIncludingExcludedIvmBaseTableRequiresCompleteBaselineRebuild() {
        MTMV mtmv = new MTMV();
        mtmv.setMvProperties(new HashMap<>(
                Map.of(PropertyAnalyzer.PROPERTIES_EXCLUDED_TRIGGER_TABLES, "t1,t2")));
        BaseTableInfo includedBaseTable = new BaseTableInfo(new TableNameInfo("internal", "db1", "t2"));
        mtmv.setRelation(new MTMVRelation(Set.of(includedBaseTable), Set.of(), Set.of(), Set.of(), Set.of()));
        mtmv.setStatus(new MTMVStatus(MTMVState.NORMAL, "seed"));
        mtmv.getIvmInfo().setEnableIvm(true);

        replayAlterMvProperties(mtmv,
                Map.of(PropertyAnalyzer.PROPERTIES_EXCLUDED_TRIGGER_TABLES, "t1"));

        // The property record does not carry the invalidation: the live change journals an ALTER_STATUS
        // record ahead of it, which is what puts the MV into SCHEMA_CHANGE -- every partition, including
        // the ones it has not created yet, which no per-partition requirement can express.
        Assertions.assertEquals(MTMVState.NORMAL, mtmv.getStatus().getState());
    }

    @Test
    public void testAlterMvPropertiesWithOtherProperty() {
        Map<String, String> mvProperties = Maps.newHashMap();
        mvProperties.put(PropertyAnalyzer.PROPERTIES_EXCLUDED_TRIGGER_TABLES, "t1");
        MTMV mtmv = new MTMV();
        mtmv.setMvProperties(mvProperties);
        MTMVRefreshSnapshot refreshSnapshot = new MTMVRefreshSnapshot();
        refreshSnapshot.getPartitionSnapshots().put("p1", new MTMVRefreshPartitionSnapshot());
        mtmv.setRefreshSnapshot(refreshSnapshot);

        long oldSchemaChangeVersion = mtmv.getSchemaChangeVersion();
        Map<String, String> newProperties = Maps.newHashMap();
        newProperties.put(PropertyAnalyzer.PROPERTIES_GRACE_PERIOD, "10");

        replayAlterMvProperties(mtmv, newProperties);

        Assertions.assertEquals(oldSchemaChangeVersion, mtmv.getSchemaChangeVersion());
        Assertions.assertFalse(mtmv.getRefreshSnapshot().getPartitionSnapshots().isEmpty());
    }

    @Test
    public void testHasRefreshSnapshotAllowsIncompletePartitionSnapshot() {
        MTMV mtmv = new MTMV();
        mtmv.setBaseIndexId(1L);
        mtmv.setIndexMeta(1L, "mv", Lists.newArrayList(new Column("k1", PrimitiveType.INT, true)),
                0, 0, (short) 1, TStorageType.COLUMN, KeysType.DUP_KEYS);
        SinglePartitionInfo partitionInfo = new SinglePartitionInfo();
        mtmv.setPartitionInfo(partitionInfo);
        mtmv.addPartition(new Partition(1L, "p1", new MaterializedIndex(), null));
        mtmv.addPartition(new Partition(2L, "p2", new MaterializedIndex(), null));
        MTMVRefreshSnapshot refreshSnapshot = new MTMVRefreshSnapshot();
        refreshSnapshot.getPartitionSnapshots().put("p1", new MTMVRefreshPartitionSnapshot());
        mtmv.setRefreshSnapshot(refreshSnapshot);

        Assertions.assertTrue(mtmv.hasRefreshSnapshot());
    }

    @Test
    public void testAlterStatus() {
        MTMV mtmv = new MTMV();
        MTMVStatus status = new MTMVStatus();
        mtmv.setStatus(status);
        // test init
        Assertions.assertEquals(MTMVState.INIT, status.getState());
        Assertions.assertEquals(MTMVRefreshState.INIT, status.getRefreshState());
        // test schema change
        status.setRefreshState(MTMVRefreshState.SUCCESS);
        mtmv.alterStatus(new MTMVStatus(MTMVState.SCHEMA_CHANGE, "base table"));
        Assertions.assertEquals(MTMVState.SCHEMA_CHANGE, status.getState());
        Assertions.assertEquals(MTMVRefreshState.SUCCESS, status.getRefreshState());

        MTMVStatus alterStatus = new MTMVStatus();
        alterStatus.setState(MTMVState.SCHEMA_CHANGE);
        alterStatus.setSchemaChangeDetail("base table");
        mtmv.alterStatus(new MTMVStatus(MTMVState.SCHEMA_CHANGE, "base table"));
        Assertions.assertEquals(MTMVState.SCHEMA_CHANGE, status.getState());
        Assertions.assertEquals(MTMVRefreshState.SUCCESS, status.getRefreshState());
    }

    @Test
    public void testAlterPropertiesSubmitsJournalWhileHoldingMvLock() {
        MTMV mtmv = new MTMV();
        mtmv.setMvProperties(Maps.newHashMap());
        ReentrantReadWriteLock mvRwLock = Deencapsulation.getField(mtmv, "mvRwLock");
        Env env = Mockito.mock(Env.class);
        EditLog editLog = Mockito.mock(EditLog.class);
        EditLogItem editLogItem = Mockito.mock(EditLogItem.class);
        Mockito.when(env.getEditLog()).thenReturn(editLog);
        Mockito.when(editLog.submitEdit(Mockito.eq(OperationType.OP_ALTER_MTMV), Mockito.any(AlterMTMV.class)))
                .thenAnswer(invocation -> {
                    Assertions.assertTrue(mvRwLock.isWriteLockedByCurrentThread());
                    return editLogItem;
                });
        Mockito.when(editLogItem.await()).thenAnswer(invocation -> {
            Assertions.assertFalse(mvRwLock.isWriteLockedByCurrentThread());
            return 1L;
        });

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            AlterMTMV alterMTMV = new AlterMTMV(
                    new TableNameInfo("db", "mv"), MTMVAlterOpType.ALTER_PROPERTY);
            alterMTMV.setMvProperties(Map.of(PropertyAnalyzer.PROPERTIES_GRACE_PERIOD, "10"));
            mtmv.alterMvProperties(alterMTMV, false);
        }

        Mockito.verify(editLog).submitEdit(
                Mockito.eq(OperationType.OP_ALTER_MTMV), Mockito.any(AlterMTMV.class));
        Mockito.verify(editLogItem).await();
    }

    @Test
    public void testAddTaskResultSubmitsJournalWhileHoldingMvLock() {
        MTMV mtmv = buildSerializableMTMV();
        mtmv.getIvmInfo().setEnableIvm(true);
        ReentrantReadWriteLock mvRwLock = Deencapsulation.getField(mtmv, "mvRwLock");
        Env env = Mockito.mock(Env.class);
        EditLog editLog = Mockito.mock(EditLog.class);
        EditLogItem editLogItem = Mockito.mock(EditLogItem.class);
        MTMVService mtmvService = Mockito.mock(MTMVService.class);
        Mockito.when(env.getEditLog()).thenReturn(editLog);
        Mockito.when(env.getMtmvService()).thenReturn(mtmvService);
        Mockito.when(editLog.submitEdit(Mockito.eq(OperationType.OP_ALTER_MTMV), Mockito.any(AlterMTMV.class)))
                .thenAnswer(invocation -> {
                    Assertions.assertTrue(mvRwLock.isWriteLockedByCurrentThread());
                    return editLogItem;
                });
        Mockito.when(editLogItem.await()).thenAnswer(invocation -> {
            Assertions.assertFalse(mvRwLock.isWriteLockedByCurrentThread());
            return 1L;
        });
        MTMVRelation relation = mtmv.getRelation();
        MTMVTask task = new MTMVTask(mtmv, relation, null);
        task.setStatus(TaskStatus.FAILED);
        AlterMTMV alterMTMV = new AlterMTMV(new TableNameInfo("db1", "mv1"), MTMVAlterOpType.ADD_TASK);
        alterMTMV.setTask(task);
        alterMTMV.setRelation(relation);
        alterMTMV.setPartitionSnapshots(Map.of());

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Assertions.assertTrue(mtmv.addTaskResult(alterMTMV, false));
        }

        Mockito.verify(editLog).submitEdit(
                Mockito.eq(OperationType.OP_ALTER_MTMV), Mockito.same(alterMTMV));
        Mockito.verify(editLogItem).await();
    }

    @Test
    public void testRefreshPublishAdvancesCacheGeneration() {
        MTMVCacheManager manager = new MTMVCacheManager();
        HookedMTMV mtmv = buildHookedMTMV();
        MTMVCache refreshedGuarded = Mockito.mock(MTMVCache.class);
        MTMVCache refreshedUnguarded = Mockito.mock(MTMVCache.class);
        mtmv.refreshGuardedCache = refreshedGuarded;
        mtmv.refreshUnguardedCache = refreshedUnguarded;
        long generationBefore = Deencapsulation.getField(mtmv, "rewriteCacheGeneration");

        Env env = mockEnv(manager);
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Assertions.assertTrue(mtmv.addTaskResult(buildSuccessTaskResult(mtmv), false));
        }

        long generationAfter = Deencapsulation.getField(mtmv, "rewriteCacheGeneration");
        Assertions.assertEquals(generationBefore + 1, generationAfter);
        Assertions.assertSame(refreshedGuarded, manager.getIfPresent(mtmv.getId(), true));
        Assertions.assertSame(refreshedUnguarded, manager.getIfPresent(mtmv.getId(), false));
    }

    @Test
    public void testRefreshSkipsPlanBuildWhenCacheDisabled() {
        int originalMaxSize = Config.mtmv_cache_manage_num;
        try {
            Config.mtmv_cache_manage_num = 0;
            MTMVCacheManager manager = new MTMVCacheManager();
            HookedMTMV mtmv = buildHookedMTMV();
            mtmv.refreshGuardedCache = Mockito.mock(MTMVCache.class);
            mtmv.refreshUnguardedCache = Mockito.mock(MTMVCache.class);
            long generationBefore = Deencapsulation.getField(mtmv, "rewriteCacheGeneration");

            Env env = mockEnv(manager);
            try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
                mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
                Assertions.assertTrue(mtmv.addTaskResult(buildSuccessTaskResult(mtmv), false));
            }

            // The generation/invalidation transition still happens, but neither plan was built.
            long generationAfter = Deencapsulation.getField(mtmv, "rewriteCacheGeneration");
            Assertions.assertEquals(generationBefore + 1, generationAfter);
            Assertions.assertEquals(0, mtmv.refreshBuildCount);
            Assertions.assertNull(manager.getIfPresent(mtmv.getId(), true));
            Assertions.assertNull(manager.getIfPresent(mtmv.getId(), false));
        } finally {
            Config.mtmv_cache_manage_num = originalMaxSize;
        }
    }

    @Test
    public void testDisabledCacheReusesPlanWithinSameStatement() throws Exception {
        int originalMaxSize = Config.mtmv_cache_manage_num;
        try {
            Config.mtmv_cache_manage_num = 0;
            MTMVCacheManager manager = new MTMVCacheManager();
            Assertions.assertFalse(manager.isEnabled());

            HookedMTMV mtmv = buildHookedMTMV();
            MTMVCache plan = Mockito.mock(MTMVCache.class);
            mtmv.lazyCaches.add(plan);

            ConnectContext context = mockConnectContext();
            StatementContext statementContext = new StatementContext();
            Mockito.when(context.getStatementContext()).thenReturn(statementContext);

            Env env = mockEnv(manager);
            try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
                mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
                MTMVCache first = mtmv.getOrGenerateCache(context);
                MTMVCache second = mtmv.getOrGenerateCache(context);
                Assertions.assertSame(plan, first);
                Assertions.assertSame(first, second);
            }

            Assertions.assertEquals(1, mtmv.lazyBuildCount);
            Assertions.assertNull(manager.getIfPresent(mtmv.getId(), false));
            Assertions.assertSame(plan, statementContext.getQueryLocalMtmvCache(mtmv.getId(), false));
        } finally {
            Config.mtmv_cache_manage_num = originalMaxSize;
        }
    }

    @Test
    public void testPausedBuilderCannotRepublishPreRefreshPlan() {
        MTMVCacheManager manager = new MTMVCacheManager();
        HookedMTMV mtmv = buildHookedMTMV();
        MTMVCache prePublishPlan = Mockito.mock(MTMVCache.class);
        MTMVCache rebuiltPlan = Mockito.mock(MTMVCache.class);
        MTMVCache refreshedUnguarded = Mockito.mock(MTMVCache.class);
        mtmv.lazyCaches.add(prePublishPlan);
        mtmv.lazyCaches.add(rebuiltPlan);
        mtmv.refreshGuardedCache = Mockito.mock(MTMVCache.class);
        mtmv.refreshUnguardedCache = refreshedUnguarded;

        Env env = mockEnv(manager);
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            // The builder snapshotted the generation and is now paused outside the MV lock: the refresh
            // publishes its pair and the fresh entry is then evicted before the builder resumes.
            mtmv.duringLazyBuild = () -> {
                Assertions.assertTrue(mtmv.addTaskResult(buildSuccessTaskResult(mtmv), false));
                Assertions.assertSame(refreshedUnguarded, manager.getIfPresent(mtmv.getId(), false));
                manager.invalidate(mtmv.getId());
            };
            MTMVCache published = mtmv.getOrGenerateCache(mockConnectContext());

            Assertions.assertSame(rebuiltPlan, published);
            Assertions.assertSame(rebuiltPlan, manager.getIfPresent(mtmv.getId(), false));
            Assertions.assertNotSame(prePublishPlan, manager.getIfPresent(mtmv.getId(), false));
        }
    }

    @Test
    public void testTaskCompletionDoesNotPublishForDroppedMv() {
        MTMVCacheManager manager = new MTMVCacheManager();
        HookedMTMV mtmv = buildHookedMTMV();
        mtmv.refreshGuardedCache = Mockito.mock(MTMVCache.class);
        mtmv.refreshUnguardedCache = Mockito.mock(MTMVCache.class);

        Env env = mockEnv(manager);
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            manager.put(mtmv.getId(), true, Mockito.mock(MTMVCache.class));
            // The task builds its caches outside the MV lock; the drop lands in that window.
            mtmv.duringRefreshBuild = mtmv::markDropped;
            Assertions.assertTrue(mtmv.addTaskResult(buildSuccessTaskResult(mtmv), false));

            Assertions.assertTrue(mtmv.isDropped);
            Assertions.assertNull(manager.getIfPresent(mtmv.getId(), true));
            Assertions.assertNull(manager.getIfPresent(mtmv.getId(), false));
        }
    }

    @Test
    public void testDropStopsPausedBuilderFromPublishing() {
        MTMVCacheManager manager = new MTMVCacheManager();
        HookedMTMV mtmv = buildHookedMTMV();
        MTMVCache builtPlan = Mockito.mock(MTMVCache.class);
        mtmv.lazyCaches.add(builtPlan);

        Env env = mockEnv(manager);
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            manager.put(mtmv.getId(), true, Mockito.mock(MTMVCache.class));
            mtmv.duringLazyBuild = mtmv::markDropped;
            MTMVCache generated = mtmv.getOrGenerateCache(mockConnectContext());

            Assertions.assertSame(builtPlan, generated);
            Assertions.assertNull(manager.getIfPresent(mtmv.getId(), true));
            Assertions.assertNull(manager.getIfPresent(mtmv.getId(), false));
        }
    }

    private HookedMTMV buildHookedMTMV() {
        HookedMTMV mtmv = configureMTMV(new HookedMTMV());
        mtmv.getIvmInfo();
        return mtmv;
    }

    private Env mockEnv(MTMVCacheManager manager) {
        Env env = Mockito.mock(Env.class);
        EditLog editLog = Mockito.mock(EditLog.class);
        Mockito.when(env.getEditLog()).thenReturn(editLog);
        Mockito.when(env.getMtmvService()).thenReturn(Mockito.mock(MTMVService.class));
        Mockito.when(env.getMtmvCacheManager()).thenReturn(manager);
        Mockito.when(editLog.submitEdit(Mockito.anyShort(), Mockito.any()))
                .thenReturn(Mockito.mock(EditLogItem.class));
        return env;
    }

    private ConnectContext mockConnectContext() {
        ConnectContext context = Mockito.mock(ConnectContext.class);
        SessionVariable sessionVariable = Mockito.mock(SessionVariable.class);
        Mockito.when(context.getSessionVariable()).thenReturn(sessionVariable);
        Mockito.when(sessionVariable.getAffectQueryResultInPlanVariables()).thenReturn(Map.of());
        return context;
    }

    private AlterMTMV buildSuccessTaskResult(MTMV mtmv) {
        MTMVRelation relation = mtmv.getRelation();
        MTMVTask task = new MTMVTask(mtmv, relation, null);
        task.setStatus(TaskStatus.SUCCESS);
        AlterMTMV alterMTMV = new AlterMTMV(new TableNameInfo("db1", "mv1"), MTMVAlterOpType.ADD_TASK);
        alterMTMV.setTask(task);
        alterMTMV.setRelation(relation);
        alterMTMV.setPartitionSnapshots(Map.of());
        return alterMTMV;
    }

    /**
     * Runs a hook inside the lock-free cache build so a refresh or a drop can be interleaved with an
     * in-flight build deterministically, without threads.
     */
    private static class HookedMTMV extends MTMV {
        private final List<MTMVCache> lazyCaches = Lists.newArrayList();
        private Runnable duringRefreshBuild;
        private Runnable duringLazyBuild;
        private MTMVCache refreshGuardedCache;
        private MTMVCache refreshUnguardedCache;
        private int lazyBuildCount;
        private int refreshBuildCount;

        @Override
        protected MTMVCache createRewriteCache(ConnectContext currentContext, boolean needLock,
                boolean addSessionVarGuard) {
            // needLock is true only on the refresh path, false on the lazy query path.
            Runnable hook = needLock ? duringRefreshBuild : duringLazyBuild;
            if (needLock) {
                duringRefreshBuild = null;
            } else {
                duringLazyBuild = null;
            }
            if (hook != null) {
                hook.run();
            }
            if (needLock) {
                refreshBuildCount++;
                return addSessionVarGuard ? refreshGuardedCache : refreshUnguardedCache;
            }
            return lazyCaches.get(Math.min(lazyBuildCount++, lazyCaches.size() - 1));
        }
    }

    private void replayAlterMvProperties(MTMV mtmv, Map<String, String> properties) {
        AlterMTMV alterMTMV = new AlterMTMV(
                new TableNameInfo("db", "mv"), MTMVAlterOpType.ALTER_PROPERTY);
        alterMTMV.setMvProperties(properties);
        mtmv.alterMvProperties(alterMTMV, true);
    }

    @Test
    public void testUnknownRefreshMethodMarksSchemaChangeAfterDeserialize() {
        MTMV mtmv = buildSerializableMTMV();
        String json = GsonUtils.GSON.toJson(mtmv).replace("\"rm\":\"COMPLETE\"", "\"rm\":\"UNKNOWN\"");

        MTMV restored = GsonUtils.GSON.fromJson(json, MTMV.class);

        Assertions.assertNull(restored.getRefreshInfo().getRefreshMethod());
        Assertions.assertEquals(MTMVState.SCHEMA_CHANGE, restored.getStatus().getState());
        Assertions.assertEquals("Unknown refresh method detected during deserialization",
                restored.getStatus().getSchemaChangeDetail());
    }

    private MTMV buildSerializableMTMV() {
        return configureMTMV(new MTMV());
    }

    private <T extends MTMV> T configureMTMV(T mtmv) {
        mtmv.setId(1L);
        mtmv.setQualifiedDbName("db1");
        mtmv.setRefreshInfo(buildMTMVRefreshInfo(mtmv));
        mtmv.setQuerySql("select k1 from t1");
        mtmv.setStatus(new MTMVStatus(MTMVRefreshState.SUCCESS));
        mtmv.getStatus().setState(MTMVState.NORMAL);
        mtmv.setJobInfo(new MTMVJobInfo("job1"));
        mtmv.setMvProperties(Maps.newHashMap());
        mtmv.setRelation(new MTMVRelation(Sets.newHashSet(), Sets.newHashSet(), Sets.newHashSet(), Sets.newHashSet(),
                Sets.newHashSet()));
        mtmv.setMvPartitionInfo(new MTMVPartitionInfo());
        mtmv.setRefreshSnapshot(new MTMVRefreshSnapshot());

        List<Column> schema = Lists.newArrayList(new Column("k1", PrimitiveType.INT, true));
        mtmv.setBaseIndexId(1L);
        mtmv.setIndexMeta(1L, "mv1", schema, 0, 0, (short) 1, TStorageType.COLUMN,
                KeysType.DUP_KEYS);
        mtmv.setPartitionInfo(new SinglePartitionInfo());
        return mtmv;
    }

    @Test
    public void testGetInsertedColumnNamesIncludesAllIvmHiddenColumns() {
        MTMV mtmv = new MTMV();
        List<Column> schema = Lists.newArrayList(
                new Column(Column.IVM_ROW_ID_COL, PrimitiveType.LARGEINT, false),
                new Column(Column.IVM_HIDDEN_COLUMN_PREFIX + "SNAPSHOT_COL__", PrimitiveType.BIGINT, false),
                new Column("k1", PrimitiveType.INT, true),
                new Column("hidden", ScalarType.createType(PrimitiveType.INT), false, null,
                        false, "comment", false, Column.COLUMN_UNIQUE_ID_INIT_VALUE)
        );
        mtmv.setBaseIndexId(1L);
        mtmv.setIndexMeta(1L, "mv", schema, 0, 0, (short) 1, TStorageType.COLUMN, org.apache.doris.catalog.KeysType.DUP_KEYS);

        List<String> insertedColumnNames = mtmv.getInsertedColumnNames();

        Assertions.assertEquals(Lists.newArrayList(
                Column.IVM_ROW_ID_COL,
                Column.IVM_HIDDEN_COLUMN_PREFIX + "SNAPSHOT_COL__",
                "k1"), insertedColumnNames);
    }

    @Test
    public void testPartitionStatesSurviveImageRoundTrip() {
        MTMV mtmv = buildSerializableMTMV();
        mtmv.alterPartitionStates(Map.of("p202601", new MTMVPartitionState(3, 5)));

        MTMV restored = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(mtmv), MTMV.class);

        Map<String, MTMVPartitionState> states = restored.getPartitionStates();
        Assertions.assertEquals(Sets.newHashSet("p202601"), states.keySet());
        Assertions.assertEquals(3, states.get("p202601").getRefreshEpoch());
        Assertions.assertEquals(5, states.get("p202601").getLatestEpoch());
    }

    @Test
    public void testPartitionStatesEmptyOnImageWrittenBeforeTheFieldExisted() {
        MTMV mtmv = buildSerializableMTMV();
        mtmv.alterPartitionStates(Map.of("p202601", new MTMVPartitionState(3, 5)));
        JsonObject image = JsonParser.parseString(GsonUtils.GSON.toJson(mtmv)).getAsJsonObject();
        Assertions.assertNotNull(image.remove("pst"));

        MTMV restored = GsonUtils.GSON.fromJson(image.toString(), MTMV.class);

        // Read the field itself rather than through the getter, which copies whatever is there.
        Assertions.assertNotNull(Deencapsulation.getField(restored, "partitionStates"));
        Assertions.assertTrue(restored.getPartitionStates().isEmpty());
    }

    @Test
    public void testPartitionStatesImageThatCarriesTheFieldAsNullLoadsAsAnEmptyMap() {
        MTMV mtmv = buildSerializableMTMV();
        mtmv.alterPartitionStates(Map.of("p202601", new MTMVPartitionState(3, 5)));
        JsonObject image = JsonParser.parseString(GsonUtils.GSON.toJson(mtmv)).getAsJsonObject();
        // An MV is created with an empty map and an image that leaves the member out keeps it, so a
        // member that is there and null is the one case gsonPostProcess() has to answer for.
        image.add("pst", JsonNull.INSTANCE);

        MTMV restored = GsonUtils.GSON.fromJson(image.toString(), MTMV.class);

        Assertions.assertNotNull(Deencapsulation.getField(restored, "partitionStates"));
        Assertions.assertTrue(restored.getPartitionStates().isEmpty());
    }

    @Test
    public void testPartitionStatesGetterIsNeverNull() {
        MTMV mtmv = new MTMV();
        // Never loaded from an image and never populated: still a map, not a null.
        Assertions.assertTrue(mtmv.getPartitionStates().isEmpty());
        mtmv.alterPartitionStates(null);
        Assertions.assertTrue(mtmv.getPartitionStates().isEmpty());
    }

    @Test
    public void testPartitionStatesGetterReturnsAnUnmodifiableSnapshot() {
        MTMV mtmv = buildSerializableMTMV();
        mtmv.alterPartitionStates(Map.of("p202601", new MTMVPartitionState(3, 5)));

        Map<String, MTMVPartitionState> states = mtmv.getPartitionStates();
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> states.put("p202602", new MTMVPartitionState(0, 1)));

        // The values are copies too: changing one may not reach the state the MV owns.
        states.get("p202601").setLatestEpoch(9);
        Assertions.assertEquals(5, mtmv.getPartitionStates().get("p202601").getLatestEpoch());
    }

    @Test
    public void testAlterPartitionStatesTakesADetachedSnapshot() {
        MTMVPartitionState live = new MTMVPartitionState(0, 1);
        Map<String, MTMVPartitionState> liveStates = Maps.newLinkedHashMap();
        liveStates.put("p202601", live);

        AlterMTMV alterMTMV = new AlterMTMV(
                new TableNameInfo("db1", "mv1"), MTMVAlterOpType.ALTER_PARTITION_STATES);
        alterMTMV.setPartitionStates(liveStates);
        // A batched edit log serializes the payload after the MV lock was released, so the payload must
        // not follow the live map any further.
        live.setLatestEpoch(2);
        liveStates.remove("p202601");

        Assertions.assertEquals(1, alterMTMV.getPartitionStates().get("p202601").getLatestEpoch());
    }

    @Test
    public void testAddTaskResultReplayKeepsPartitionStatesWhenTheJournalHasNoField() {
        MTMV mtmv = buildSerializableMTMV();
        mtmv.getIvmInfo().setEnableIvm(true);
        mtmv.alterPartitionStates(Map.of("p202601", new MTMVPartitionState(3, 5)));

        // A journal written before the field existed carries no state at all: it must not clear what is
        // already there.
        runAddTaskResult(mtmv, null, true);

        Map<String, MTMVPartitionState> states = mtmv.getPartitionStates();
        Assertions.assertEquals(Sets.newHashSet("p202601"), states.keySet());
        Assertions.assertEquals(3, states.get("p202601").getRefreshEpoch());
        Assertions.assertEquals(5, states.get("p202601").getLatestEpoch());
    }

    @Test
    public void testAddTaskResultReplayAppliesPartitionStates() {
        MTMV mtmv = buildSerializableMTMV();
        mtmv.getIvmInfo().setEnableIvm(true);
        mtmv.alterPartitionStates(Map.of("p202601", new MTMVPartitionState(0, 1)));

        List<AlterMTMV> journaled = runAddTaskResult(mtmv, Map.of("p202601", new MTMVPartitionState(3, 5)), true);

        // Replay never writes a journal of its own.
        Assertions.assertTrue(journaled.isEmpty());
        MTMVPartitionState state = mtmv.getPartitionStates().get("p202601");
        Assertions.assertEquals(3, state.getRefreshEpoch());
        Assertions.assertEquals(5, state.getLatestEpoch());
    }

    @Test
    public void testDirtyPartitionsAreTheRefreshedOnesBehindTheirRequirement() {
        MTMV mtmv = Mockito.spy(buildSerializableMTMV());
        mtmv.getIvmInfo().setEnableIvm(true);
        Mockito.doReturn(Sets.newHashSet("p202601", "p202602")).when(mtmv).getPartitionNames();
        mtmv.alterPartitionStates(Maps.newHashMap(Map.of(
                "p202601", new MTMVPartitionState(1, 2),
                "p202602", new MTMVPartitionState(2, 2),
                "p202603", new MTMVPartitionState(1, 2))));

        // Only the partition that holds rows and is behind its requirement. One that reached its
        // requirement is out, and so is one the MV no longer has: a partition can be dropped while a task
        // is deciding, and its state goes with it -- until then, rebuilding it is what the stale entry
        // would ask for.
        Assertions.assertEquals(Sets.newHashSet("p202601"), mtmv.getDirtyPartitions());
    }

    @Test
    public void testTaskResultLeavesTheSnapshotOfADirtyPartitionOut() {
        MTMV mtmv = Mockito.spy(buildSerializableMTMV());
        mtmv.getIvmInfo().setEnableIvm(true);
        Mockito.doReturn(Sets.newHashSet("p202601", "p202602")).when(mtmv).getPartitionNames();
        // p202601 was invalidated while the task ran, p202602 was not.
        mtmv.alterPartitionStates(Maps.newHashMap(Map.of(
                "p202601", new MTMVPartitionState(1, 2),
                "p202602", new MTMVPartitionState(2, 2))));
        Map<String, MTMVRefreshPartitionSnapshot> snapshots = Maps.newHashMap(Map.of(
                "p202601", new MTMVRefreshPartitionSnapshot(),
                "p202602", new MTMVRefreshPartitionSnapshot()));

        runAddTaskResult(mtmv, snapshots, null, false, Map.of());

        // The invalidation dropped p202601's snapshot so that no transparent rewrite serves its rows, and
        // a result written back afterwards must not put it there again -- while the partition that was not
        // invalidated keeps the snapshot the task produced.
        Assertions.assertEquals(Sets.newHashSet("p202602"),
                mtmv.getRefreshSnapshot().getPartitionSnapshots().keySet());
    }

    @Test
    public void testReplayIgnoresTheEpochsTheTaskCaptured() {
        MTMV mtmv = buildSerializableMTMV();
        mtmv.getIvmInfo().setEnableIvm(true);
        mtmv.alterPartitionStates(Map.of("p202601", new MTMVPartitionState(3, 3)));

        // A replayed record carries the states that were decided when it was written, and the epochs a
        // task captured live in memory only. Applying them on replay would re-decide a result that is
        // already fixed, and the payload's value -- not the capture -- is what the record means.
        runAddTaskResult(mtmv, Map.of("p202601", new MTMVPartitionState(7, 9)), true, Map.of("p202601", 1L));

        Assertions.assertEquals(7, mtmv.getPartitionStates().get("p202601").getRefreshEpoch());
        Assertions.assertEquals(9, mtmv.getPartitionStates().get("p202601").getLatestEpoch());
    }

    @Test
    public void testAFreshMvHasAnEmptyStateMapAndAlignmentFillsIt() {
        MTMV mtmv = buildSerializableMTMV();
        mtmv.getIvmInfo().setEnableIvm(true);

        // The map is created with the MV, so a reader has no null case to answer; alignment is what puts
        // the MV's partitions into it.
        Assertions.assertNotNull(Deencapsulation.getField(mtmv, "partitionStates"));
        Assertions.assertTrue(mtmv.getPartitionStates().isEmpty());

        runAlignPartitionStates(mtmv, Sets.newHashSet("p202601"));

        Assertions.assertEquals(Sets.newHashSet("p202601"), mtmv.getPartitionStates().keySet());
        Assertions.assertEquals(1, mtmv.getPartitionStates().get("p202601").getLatestEpoch());
        Assertions.assertTrue(mtmv.getPartitionStates().get("p202601").isNeverRefreshed());
    }

    @Test
    public void testIvmTaskResultJournalsPartitionStates() {
        MTMV mtmv = buildSerializableMTMV();
        mtmv.getIvmInfo().setEnableIvm(true);
        mtmv.alterPartitionStates(Map.of("p202601", new MTMVPartitionState(3, 5)));

        // The task published p202601, so that is what reaches the journal: the partition at the epoch it
        // was published with, and the requirement it was read under. It carries no more than that -- see
        // testTaskResultJournalsOnlyThePartitionsItPublished.
        List<AlterMTMV> journaled = runAddTaskResult(mtmv, null, false, Map.of("p202601", 6L));

        Assertions.assertEquals(1, journaled.size());
        Map<String, MTMVPartitionState> published = journaled.get(0).getPartitionStates();
        Assertions.assertEquals(6, published.get("p202601").getRefreshEpoch());
        Assertions.assertEquals(5, published.get("p202601").getLatestEpoch());

        // The payload reaches the journal as JSON, so it has to survive that trip to be replayable.
        AlterMTMV readBack = GsonUtils.GSON.fromJson(
                GsonUtils.GSON.toJson(journaled.get(0)), AlterMTMV.class);
        Assertions.assertEquals(6, readBack.getPartitionStates().get("p202601").getRefreshEpoch());
        Assertions.assertEquals(5, readBack.getPartitionStates().get("p202601").getLatestEpoch());
    }

    @Test
    public void testNonIvmTaskResultDoesNotJournalPartitionStates() {
        MTMV mtmv = buildSerializableMTMV();
        Assertions.assertFalse(mtmv.getIvmInfo().isEnableIvm());

        List<AlterMTMV> journaled = runAddTaskResult(mtmv, null, false);

        // The payload of a non-IVM MV has to stay byte-for-byte what it was before the field existed.
        Assertions.assertEquals(1, journaled.size());
        Assertions.assertNull(journaled.get(0).getPartitionStates());
    }

    @Test
    public void testPartitionStateIsDirtyWhenItIsBehindItsRequirement() {
        Assertions.assertFalse(new MTMVPartitionState(1, 1).isDirty());
        Assertions.assertFalse(new MTMVPartitionState(4, 4).isDirty());
        Assertions.assertTrue(new MTMVPartitionState(1, 2).isDirty());
        Assertions.assertTrue(new MTMVPartitionState(5, 6).isDirty());
        // A refreshEpoch of 0 is not an exemption. It reads as "never refreshed, so no rows", and that is
        // not durable: a refresh commits the MV data transaction before its task result publishes the
        // epochs, so those rows can be there while the state still says 0. Reading the pair as clean would
        // let a later invalidation raising latestEpoch go unnoticed.
        Assertions.assertTrue(new MTMVPartitionState(0, 1).isDirty());
        Assertions.assertTrue(new MTMVPartitionState(0, 2).isDirty());
        // "Never refreshed" stays a separate fact about the past, which the escalation reads.
        Assertions.assertTrue(new MTMVPartitionState(0, 2).isNeverRefreshed());
        Assertions.assertTrue(MTMVPartitionState.initial().isNeverRefreshed());
        Assertions.assertEquals(1, MTMVPartitionState.initial().getLatestEpoch());
    }

    @Test
    public void testAlignPartitionStatesCreatesAndDropsEntries() {
        MTMV mtmv = buildSerializableMTMV();
        mtmv.getIvmInfo().setEnableIvm(true);
        mtmv.alterPartitionStates(Map.of("p202601", new MTMVPartitionState(3, 5)));

        // A partition that is already there keeps its requirement: alignment creates and destroys
        // entries, it never rewrites one.
        List<AlterMTMV> journaled = runAlignPartitionStates(mtmv, Sets.newHashSet("p202601", "p202602"));
        Assertions.assertEquals(1, journaled.size());
        Assertions.assertEquals(MTMVAlterOpType.ALTER_PARTITION_STATES, journaled.get(0).getOpType());
        Assertions.assertEquals(3, journaled.get(0).getPartitionStates().get("p202601").getRefreshEpoch());
        Assertions.assertEquals(5, journaled.get(0).getPartitionStates().get("p202601").getLatestEpoch());
        Assertions.assertEquals(0, journaled.get(0).getPartitionStates().get("p202602").getRefreshEpoch());
        Assertions.assertEquals(1, journaled.get(0).getPartitionStates().get("p202602").getLatestEpoch());
        Assertions.assertEquals(2, mtmv.getPartitionStates().size());

        // Aligning onto the same set changes nothing, so it journals nothing either.
        Assertions.assertTrue(runAlignPartitionStates(mtmv, Sets.newHashSet("p202601", "p202602")).isEmpty());

        // A partition that is gone loses its entry, which is what keeps a mark from landing on state
        // that no partition can hold rows for.
        runAlignPartitionStates(mtmv, Sets.newHashSet("p202602"));
        Assertions.assertEquals(Sets.newHashSet("p202602"), mtmv.getPartitionStates().keySet());
    }

    @Test
    public void testAlignPartitionStatesDoesNothingForANonIvmMv() {
        MTMV mtmv = buildSerializableMTMV();
        Assertions.assertFalse(mtmv.getIvmInfo().isEnableIvm());

        Assertions.assertTrue(runAlignPartitionStates(mtmv, Sets.newHashSet("p202601")).isEmpty());

        Assertions.assertTrue(mtmv.getPartitionStates().isEmpty());
    }

    @Test
    public void testTaskResultRecordsTheCapturedEpochWithoutTouchingTheRequirement() {
        MTMV mtmv = buildSerializableMTMV();
        mtmv.getIvmInfo().setEnableIvm(true);
        // The partition was read at requirement 5, and a base-table change raised it to 6 before the
        // result was written back. Writing the captured requirement back would swallow that rebuild.
        mtmv.alterPartitionStates(Map.of("p202601", new MTMVPartitionState(3, 6)));

        List<AlterMTMV> journaled = runAddTaskResult(mtmv, null, false, Map.of("p202601", 5L));

        Assertions.assertEquals(1, journaled.size());
        MTMVPartitionState journaledState = journaled.get(0).getPartitionStates().get("p202601");
        Assertions.assertEquals(5, journaledState.getRefreshEpoch());
        Assertions.assertEquals(6, journaledState.getLatestEpoch());
        Assertions.assertTrue(journaledState.isDirty());

        Assertions.assertEquals(5, mtmv.getPartitionStates().get("p202601").getRefreshEpoch());
        Assertions.assertEquals(6, mtmv.getPartitionStates().get("p202601").getLatestEpoch());
    }

    @Test
    public void testTaskResultSkipsPartitionsItDidNotCapture() {
        MTMV mtmv = buildSerializableMTMV();
        mtmv.getIvmInfo().setEnableIvm(true);
        mtmv.alterPartitionStates(Map.of("p202601", new MTMVPartitionState(3, 3)));

        // The task captured only p202602, which is not a partition of this MV any more -- and p202601,
        // which it did not capture, must keep the epoch of the data it still holds.
        runAddTaskResult(mtmv, null, false, Map.of("p202602", 9L));

        Assertions.assertEquals(Sets.newHashSet("p202601"), mtmv.getPartitionStates().keySet());
        Assertions.assertEquals(3, mtmv.getPartitionStates().get("p202601").getRefreshEpoch());
    }

    @Test
    public void testLatestEpochsOmitsPartitionsWithoutAnEntry() {
        MTMV mtmv = buildSerializableMTMV();
        mtmv.alterPartitionStates(Map.of("p202601", new MTMVPartitionState(3, 5)));

        Assertions.assertEquals(Map.of("p202601", 5L),
                mtmv.getLatestEpochs(Sets.newHashSet("p202601", "p202602")));
        Assertions.assertTrue(mtmv.getLatestEpochs(Sets.newHashSet()).isEmpty());
    }

    @Test
    public void testTaskResultJournalsOnlyThePartitionsItPublished() {
        MTMV mtmv = buildSerializableMTMV();
        mtmv.getIvmInfo().setEnableIvm(true);
        mtmv.alterPartitionStates(Map.of(
                "p202601", new MTMVPartitionState(3, 5),
                "p202602", new MTMVPartitionState(4, 4)));

        // Only p202601 was published by this task; p202602 belongs to another record.
        List<AlterMTMV> journaled = runAddTaskResult(mtmv, null, false, Map.of("p202601", 6L));

        Assertions.assertEquals(1, journaled.size());
        Map<String, MTMVPartitionState> published = journaled.get(0).getPartitionStates();
        Assertions.assertEquals(Set.of("p202601"), published.keySet());
        Assertions.assertEquals(6, published.get("p202601").getRefreshEpoch());
        // The requirement raised while the task ran rides along: a payload that dropped it would let a
        // replay restore the older one and lose the rebuild it asks for.
        Assertions.assertEquals(5, published.get("p202601").getLatestEpoch());
    }

    @Test
    public void testTaskResultReplayMergesThePartitionsItCarries() {
        MTMV mtmv = buildSerializableMTMV();
        mtmv.getIvmInfo().setEnableIvm(true);
        Map<String, MTMVPartitionState> current = new HashMap<>();
        current.put("p202601", new MTMVPartitionState(3, 5));
        current.put("p202602", new MTMVPartitionState(9, 9));
        mtmv.alterPartitionStates(current);

        // A payload carries only the partitions its task published, so a replay merges it. Assigning
        // would drop p202602, which another record -- an invalidation that ran during the task -- owns.
        runAddTaskResult(mtmv, Map.of("p202601", new MTMVPartitionState(6, 5)), true);

        Map<String, MTMVPartitionState> replayed = mtmv.getPartitionStates();
        Assertions.assertEquals(2, replayed.size());
        Assertions.assertEquals(6, replayed.get("p202601").getRefreshEpoch());
        Assertions.assertEquals(9, replayed.get("p202602").getRefreshEpoch());
    }

    @Test
    public void testMarkPartitionsForRebuildIsJournaledAndClearedByTheTaskResult() {
        MTMV mtmv = buildSerializableMTMV();
        mtmv.getIvmInfo().setEnableIvm(true);
        // The journaling path names the MV, and the fixture is built through the deserialization
        // constructor, which leaves the name unset.
        Deencapsulation.setField(mtmv, "name", "mv1");
        Map<String, MTMVPartitionState> current = new HashMap<>();
        current.put("p202601", new MTMVPartitionState(1, 1));
        current.put("p202602", new MTMVPartitionState(1, 1));
        mtmv.alterPartitionStates(current);

        List<AlterMTMV> marked = Lists.newArrayList();
        withMockedEditLog(marked, () -> mtmv.markPartitionsForRebuild(Set.of("p202601")));

        // Journaled as the full map: this is an invalidation-shaped record, not a task result.
        Assertions.assertEquals(1, marked.size());
        Assertions.assertEquals(2, marked.get(0).getPartitionStates().get("p202601").getLatestEpoch());
        Assertions.assertEquals(1, marked.get(0).getPartitionStates().get("p202602").getLatestEpoch());
        Assertions.assertTrue(mtmv.getPartitionStates().get("p202601").isDirty());
        Assertions.assertFalse(mtmv.getPartitionStates().get("p202602").isDirty());

        // Publishing the partition meets the raised requirement.
        runAddTaskResult(mtmv, null, false, Map.of("p202601", 2L));
        Assertions.assertEquals(2, mtmv.getPartitionStates().get("p202601").getRefreshEpoch());
        Assertions.assertFalse(mtmv.getPartitionStates().get("p202601").isDirty());
    }

    /**
     * Runs one ADD_TASK result through {@link MTMV#addTaskResult}, optionally carrying {@code
     * journaledStates} in its payload the way a real journal would, and returns the payloads that
     * reached the edit log -- which stays empty on the replay path.
     */
    private List<AlterMTMV> runAddTaskResult(MTMV mtmv, Map<String, MTMVPartitionState> journaledStates,
            boolean isReplay) {
        return runAddTaskResult(mtmv, journaledStates, isReplay, Map.of());
    }

    /**
     * Same, with the epochs the task captured, which a live task result turns into {@code refreshEpoch}
     * (the task carries them in memory; the journal carries the resulting states).
     */
    private List<AlterMTMV> runAddTaskResult(MTMV mtmv, Map<String, MTMVPartitionState> journaledStates,
            boolean isReplay, Map<String, Long> capturedEpochs) {
        return runAddTaskResult(mtmv, Map.of(), journaledStates, isReplay, capturedEpochs);
    }

    /**
     * Same, with the snapshots the task produced, which a live task result merges into the MV's own map
     * unless the partition it describes came out dirty.
     */
    private List<AlterMTMV> runAddTaskResult(MTMV mtmv,
            Map<String, MTMVRefreshPartitionSnapshot> partitionSnapshots,
            Map<String, MTMVPartitionState> journaledStates, boolean isReplay,
            Map<String, Long> capturedEpochs) {
        List<AlterMTMV> journaled = Lists.newArrayList();
        withMockedEditLog(journaled, () -> {
            MTMVTask task = new MTMVTask(mtmv, mtmv.getRelation(), null);
            task.setStatus(TaskStatus.FAILED);
            task.getIvmCapturedEpochs().putAll(capturedEpochs);
            AlterMTMV alterMTMV = new AlterMTMV(new TableNameInfo("db1", "mv1"), MTMVAlterOpType.ADD_TASK);
            alterMTMV.setTask(task);
            alterMTMV.setRelation(mtmv.getRelation());
            alterMTMV.setPartitionSnapshots(partitionSnapshots);
            alterMTMV.setPartitionStates(journaledStates);
            Assertions.assertTrue(mtmv.addTaskResult(alterMTMV, isReplay));
        });
        return journaled;
    }

    private List<AlterMTMV> runAlignPartitionStates(MTMV mtmv, Set<String> livePartitionNames) {
        // A journaling path names the MV, and the fixture is built through the deserialization
        // constructor, which leaves the name unset (setName() cannot be used: it rekeys the index map
        // by the current name, which is still null at that point).
        Deencapsulation.setField(mtmv, "name", "mv1");
        List<AlterMTMV> journaled = Lists.newArrayList();
        withMockedEditLog(journaled, () -> mtmv.alignPartitionStates(livePartitionNames));
        return journaled;
    }

    /** Runs {@code action} against a mocked edit log and collects the payloads it journals. */
    private void withMockedEditLog(List<AlterMTMV> journaled, Runnable action) {
        Env env = Mockito.mock(Env.class);
        EditLog editLog = Mockito.mock(EditLog.class);
        EditLogItem editLogItem = Mockito.mock(EditLogItem.class);
        Mockito.when(env.getEditLog()).thenReturn(editLog);
        Mockito.when(env.getMtmvService()).thenReturn(Mockito.mock(MTMVService.class));
        Mockito.when(editLog.submitEdit(Mockito.eq(OperationType.OP_ALTER_MTMV), Mockito.any(AlterMTMV.class)))
                .thenAnswer(invocation -> {
                    journaled.add(invocation.getArgument(1));
                    return editLogItem;
                });

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            action.run();
        }
    }
}
