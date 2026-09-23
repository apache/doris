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
import org.apache.doris.persist.AlterMTMV;
import org.apache.doris.persist.EditLog;
import org.apache.doris.persist.EditLog.EditLogItem;
import org.apache.doris.persist.OperationType;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.thrift.TStorageType;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
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
        Assertions.assertEquals(oldSchemaChangeVersion + 1, mtmv.getSchemaChangeVersion());
        Assertions.assertTrue(mtmv.getRefreshSnapshot().getPartitionSnapshots().isEmpty());

        mtmv.getRefreshSnapshot().getPartitionSnapshots().put("p1", new MTMVRefreshPartitionSnapshot());
        oldSchemaChangeVersion = mtmv.getSchemaChangeVersion();
        newProperties.put(PropertyAnalyzer.PROPERTIES_EXCLUDED_TRIGGER_TABLES, "internal.db1.t1");

        replayAlterMvProperties(mtmv, newProperties);

        Assertions.assertEquals(MTMVState.NORMAL, mtmv.getStatus().getState());
        Assertions.assertEquals(oldSchemaChangeVersion + 1, mtmv.getSchemaChangeVersion());
        Assertions.assertTrue(mtmv.getRefreshSnapshot().getPartitionSnapshots().isEmpty());
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
        Assertions.assertEquals(oldSchemaChangeVersion + 1, mtmv.getSchemaChangeVersion());
        Assertions.assertTrue(mtmv.getRefreshSnapshot().getPartitionSnapshots().isEmpty());

        mtmv.getRefreshSnapshot().getPartitionSnapshots().put("p1", new MTMVRefreshPartitionSnapshot());
        oldSchemaChangeVersion = mtmv.getSchemaChangeVersion();
        newProperties.put(PropertyAnalyzer.PROPERTIES_EXCLUDED_TRIGGER_TABLES, "");

        replayAlterMvProperties(mtmv, newProperties);

        Assertions.assertEquals(MTMVState.NORMAL, mtmv.getStatus().getState());
        Assertions.assertEquals(oldSchemaChangeVersion + 1, mtmv.getSchemaChangeVersion());
        Assertions.assertTrue(mtmv.getRefreshSnapshot().getPartitionSnapshots().isEmpty());
    }

    @Test
    public void testIncludingExcludedIvmBaseTableRequiresCompleteBaselineRebuild() {
        MTMV mtmv = new MTMV();
        mtmv.setMvProperties(new HashMap<>(
                Map.of(PropertyAnalyzer.PROPERTIES_EXCLUDED_TRIGGER_TABLES, "t1,t2")));
        BaseTableInfo includedBaseTable = new BaseTableInfo(new TableNameInfo("internal", "db1", "t2"));
        mtmv.setRelation(new MTMVRelation(Set.of(includedBaseTable), Set.of(), Set.of(), Set.of(), Set.of()));
        mtmv.getIvmInfo().setEnableIvm(true);

        replayAlterMvProperties(mtmv,
                Map.of(PropertyAnalyzer.PROPERTIES_EXCLUDED_TRIGGER_TABLES, "t1"));

        Assertions.assertTrue(mtmv.getIvmInfo().requiresCompleteBaselineRebuild());
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
        MTMV mtmv = new MTMV();
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

        // The field is gone from the image, so gsonPostProcess() is the only thing that can make it a map.
        MTMV restored = GsonUtils.GSON.fromJson(image.toString(), MTMV.class);

        // Read the field itself: the getter lazily creates the map, so it would hide a missing init.
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
    public void testIvmTaskResultJournalsPartitionStates() {
        MTMV mtmv = buildSerializableMTMV();
        mtmv.getIvmInfo().setEnableIvm(true);
        mtmv.alterPartitionStates(Map.of("p202601", new MTMVPartitionState(3, 5)));

        List<AlterMTMV> journaled = runAddTaskResult(mtmv, null, false);

        Assertions.assertEquals(1, journaled.size());
        MTMVPartitionState journaledState = journaled.get(0).getPartitionStates().get("p202601");
        Assertions.assertEquals(3, journaledState.getRefreshEpoch());
        Assertions.assertEquals(5, journaledState.getLatestEpoch());

        // The payload reaches the journal as JSON, so it has to survive that trip to be replayable.
        AlterMTMV readBack = GsonUtils.GSON.fromJson(
                GsonUtils.GSON.toJson(journaled.get(0)), AlterMTMV.class);
        Assertions.assertEquals(3, readBack.getPartitionStates().get("p202601").getRefreshEpoch());
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

    /**
     * Runs one ADD_TASK result through {@link MTMV#addTaskResult}, optionally carrying {@code
     * journaledStates} in its payload the way a real journal would, and returns the payloads that
     * reached the edit log -- which stays empty on the replay path.
     */
    private List<AlterMTMV> runAddTaskResult(MTMV mtmv, Map<String, MTMVPartitionState> journaledStates,
            boolean isReplay) {
        Env env = Mockito.mock(Env.class);
        EditLog editLog = Mockito.mock(EditLog.class);
        EditLogItem editLogItem = Mockito.mock(EditLogItem.class);
        List<AlterMTMV> journaled = Lists.newArrayList();
        Mockito.when(env.getEditLog()).thenReturn(editLog);
        Mockito.when(env.getMtmvService()).thenReturn(Mockito.mock(MTMVService.class));
        Mockito.when(editLog.submitEdit(Mockito.eq(OperationType.OP_ALTER_MTMV), Mockito.any(AlterMTMV.class)))
                .thenAnswer(invocation -> {
                    journaled.add(invocation.getArgument(1));
                    return editLogItem;
                });

        MTMVTask task = new MTMVTask(mtmv, mtmv.getRelation(), null);
        task.setStatus(TaskStatus.FAILED);
        AlterMTMV alterMTMV = new AlterMTMV(new TableNameInfo("db1", "mv1"), MTMVAlterOpType.ADD_TASK);
        alterMTMV.setTask(task);
        alterMTMV.setRelation(mtmv.getRelation());
        alterMTMV.setPartitionSnapshots(Map.of());
        alterMTMV.setPartitionStates(journaledStates);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Assertions.assertTrue(mtmv.addTaskResult(alterMTMV, isReplay));
        }
        return journaled;
    }
}
