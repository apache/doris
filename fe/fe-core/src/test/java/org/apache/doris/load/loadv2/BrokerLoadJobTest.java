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

package org.apache.doris.load.loadv2;

import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableProperty;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.Status;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.load.BrokerFileGroup;
import org.apache.doris.load.BrokerFileGroupAggInfo;
import org.apache.doris.load.BrokerFileGroupAggInfo.FileGroupAggKey;
import org.apache.doris.load.EtlStatus;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.nereids.load.NereidsBrokerFileGroup;
import org.apache.doris.nereids.load.NereidsLoadingTaskPlanner;
import org.apache.doris.persist.EditLog;
import org.apache.doris.resource.computegroup.ComputeGroup;
import org.apache.doris.resource.computegroup.ComputeGroupMgr;
import org.apache.doris.task.MasterTaskExecutor;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.transaction.GlobalTransactionMgrIface;
import org.apache.doris.transaction.TransactionState;
import org.apache.doris.transaction.TxnStateCallbackFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class BrokerLoadJobTest {

    @BeforeClass
    public static void start() {
        MetricRepo.init();
    }

    @Test
    public void testGetTableNames() throws MetaNotFoundException {
        BrokerFileGroupAggInfo fileGroupAggInfo = Mockito.mock(BrokerFileGroupAggInfo.class);
        BrokerFileGroup brokerFileGroup = Mockito.mock(BrokerFileGroup.class);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Database database = Mockito.mock(Database.class);
        Table table = Mockito.mock(Table.class);

        List<BrokerFileGroup> brokerFileGroups = Lists.newArrayList();
        brokerFileGroups.add(brokerFileGroup);
        Map<FileGroupAggKey, List<BrokerFileGroup>> aggKeyToFileGroups = Maps.newHashMap();
        FileGroupAggKey aggKey = new FileGroupAggKey(1L, null);
        aggKeyToFileGroups.put(aggKey, brokerFileGroups);
        BrokerLoadJob brokerLoadJob = new BrokerLoadJob();
        Deencapsulation.setField(brokerLoadJob, "fileGroupAggInfo", fileGroupAggInfo);
        String tableName = "table";

        try (MockedStatic<Env> envMockedStatic = Mockito.mockStatic(Env.class)) {
            envMockedStatic.when(Env::getCurrentInternalCatalog).thenReturn(catalog);

            Mockito.when(fileGroupAggInfo.getAggKeyToFileGroups()).thenReturn(aggKeyToFileGroups);
            Mockito.when(fileGroupAggInfo.getAllTableIds()).thenReturn(Sets.newHashSet(1L));
            Mockito.doReturn(Optional.of(database)).when(catalog).getDb(Mockito.anyLong());
            Mockito.doReturn(Optional.of(table)).when(database).getTable(1L);
            Mockito.when(table.getName()).thenReturn(tableName);

            Assert.assertEquals(1, brokerLoadJob.getTableNamesForShow().size());
            Assert.assertTrue(brokerLoadJob.getTableNamesForShow().contains(tableName));
        }
    }

    @Test
    public void testExecuteJob() {
        Env env = Mockito.mock(Env.class);
        MasterTaskExecutor masterTaskExecutor = Mockito.mock(MasterTaskExecutor.class);

        try (MockedStatic<Env> envMockedStatic = Mockito.mockStatic(Env.class)) {
            envMockedStatic.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(env.getPendingLoadTaskScheduler()).thenReturn(masterTaskExecutor);

            BrokerLoadJob brokerLoadJob = new BrokerLoadJob();
            brokerLoadJob.unprotectedExecuteJob();

            Map<Long, LoadTask> idToTasks = Deencapsulation.getField(brokerLoadJob, "idToTasks");
            Assert.assertEquals(1, idToTasks.size());
        }
    }

    @Test
    public void testPendingTaskOnFinishedWithJobCancelled() {
        BrokerPendingTaskAttachment attachment = Mockito.mock(BrokerPendingTaskAttachment.class);
        BrokerLoadJob brokerLoadJob = new BrokerLoadJob();
        Deencapsulation.setField(brokerLoadJob, "state", JobState.CANCELLED);
        brokerLoadJob.onTaskFinished(attachment);

        Set<Long> finishedTaskIds = Deencapsulation.getField(brokerLoadJob, "finishedTaskIds");
        Assert.assertEquals(0, finishedTaskIds.size());
    }

    @Test
    public void testPendingTaskOnFinishedWithDuplicated() {
        BrokerPendingTaskAttachment attachment = Mockito.mock(BrokerPendingTaskAttachment.class);
        BrokerLoadJob brokerLoadJob = new BrokerLoadJob();
        Deencapsulation.setField(brokerLoadJob, "state", JobState.LOADING);
        Set<Long> finishedTaskIds = Sets.newHashSet();
        long taskId = 1L;
        finishedTaskIds.add(taskId);
        Deencapsulation.setField(brokerLoadJob, "finishedTaskIds", finishedTaskIds);
        Mockito.when(attachment.getTaskId()).thenReturn(taskId);

        brokerLoadJob.onTaskFinished(attachment);
        Map<Long, LoadTask> idToTasks = Deencapsulation.getField(brokerLoadJob, "idToTasks");
        Assert.assertEquals(0, idToTasks.size());
    }

    @Test
    public void testPendingTaskOnFinished() throws Exception {
        BrokerPendingTaskAttachment attachment = Mockito.mock(BrokerPendingTaskAttachment.class);
        Env env = Mockito.mock(Env.class);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Database database = Mockito.mock(Database.class);
        BrokerFileGroupAggInfo fileGroupAggInfo = Mockito.mock(BrokerFileGroupAggInfo.class);
        BrokerFileGroup brokerFileGroup1 = Mockito.mock(BrokerFileGroup.class);
        BrokerFileGroup brokerFileGroup2 = Mockito.mock(BrokerFileGroup.class);
        BrokerFileGroup brokerFileGroup3 = Mockito.mock(BrokerFileGroup.class);
        NereidsBrokerFileGroup nereidsBfg1 = Mockito.mock(NereidsBrokerFileGroup.class);
        NereidsBrokerFileGroup nereidsBfg2 = Mockito.mock(NereidsBrokerFileGroup.class);
        NereidsBrokerFileGroup nereidsBfg3 = Mockito.mock(NereidsBrokerFileGroup.class);
        Mockito.when(brokerFileGroup1.toNereidsBrokerFileGroup()).thenReturn(nereidsBfg1);
        Mockito.when(brokerFileGroup2.toNereidsBrokerFileGroup()).thenReturn(nereidsBfg2);
        Mockito.when(brokerFileGroup3.toNereidsBrokerFileGroup()).thenReturn(nereidsBfg3);
        OlapTable olapTable = Mockito.mock(OlapTable.class);
        MasterTaskExecutor loadingTaskExecutor = Mockito.mock(MasterTaskExecutor.class);
        GlobalTransactionMgrIface globalTxnMgr = Mockito.mock(GlobalTransactionMgrIface.class);
        ProgressManager progressManager = Mockito.mock(ProgressManager.class);
        TransactionState txnState = Mockito.mock(TransactionState.class);
        ComputeGroupMgr computeGroupMgr = Mockito.mock(ComputeGroupMgr.class);
        TableProperty tableProperty = Mockito.mock(TableProperty.class);

        try (MockedStatic<Env> envMockedStatic = Mockito.mockStatic(Env.class);
                MockedConstruction<NereidsLoadingTaskPlanner> ignored =
                        Mockito.mockConstruction(NereidsLoadingTaskPlanner.class)) {
            envMockedStatic.when(Env::getCurrentEnv).thenReturn(env);
            envMockedStatic.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
            envMockedStatic.when(Env::getCurrentProgressManager).thenReturn(progressManager);
            envMockedStatic.when(Env::getCurrentGlobalTransactionMgr).thenReturn(globalTxnMgr);

            BrokerLoadJob brokerLoadJob = new BrokerLoadJob();
            Deencapsulation.setField(brokerLoadJob, "state", JobState.LOADING);
            BrokerDesc brokerDesc = Mockito.mock(BrokerDesc.class);
            Deencapsulation.setField(brokerLoadJob, "brokerDesc", brokerDesc);
            long taskId = 1L;
            long tableId1 = 1L;
            long tableId2 = 2L;
            long partitionId1 = 3L;
            long partitionId2 = 4;

            Map<FileGroupAggKey, List<BrokerFileGroup>> aggKeyToFileGroups = Maps.newHashMap();
            List<BrokerFileGroup> fileGroups1 = Lists.newArrayList();
            fileGroups1.add(brokerFileGroup1);
            FileGroupAggKey aggKey1 = new FileGroupAggKey(tableId1, null);
            aggKeyToFileGroups.put(aggKey1, fileGroups1);

            List<BrokerFileGroup> fileGroups2 = Lists.newArrayList();
            fileGroups2.add(brokerFileGroup2);
            fileGroups2.add(brokerFileGroup3);
            FileGroupAggKey aggKey2 = new FileGroupAggKey(tableId2, Lists.newArrayList(partitionId1));
            aggKeyToFileGroups.put(aggKey2, fileGroups2);
            // add another file groups with different partition id
            FileGroupAggKey aggKey3 = new FileGroupAggKey(tableId2, Lists.newArrayList(partitionId2));
            aggKeyToFileGroups.put(aggKey3, fileGroups2);

            Deencapsulation.setField(brokerLoadJob, "fileGroupAggInfo", fileGroupAggInfo);

            Mockito.when(attachment.getTaskId()).thenReturn(taskId);
            Mockito.doReturn(database).when(catalog).getDbOrMetaException(Mockito.anyLong());
            Mockito.doReturn(Lists.newArrayList()).when(database)
                    .getTablesOnIdOrderOrThrowException(Mockito.anyList());
            Mockito.when(fileGroupAggInfo.getAggKeyToFileGroups()).thenReturn(aggKeyToFileGroups);
            Mockito.when(fileGroupAggInfo.getAllTableIds()).thenReturn(Sets.newHashSet(tableId1, tableId2));
            Mockito.doReturn(olapTable).when(database).getTableNullable(Mockito.anyLong());
            Mockito.when(olapTable.isTemporary()).thenReturn(false);
            Mockito.when(olapTable.getTableProperty()).thenReturn(tableProperty);
            Mockito.when(tableProperty.getUseSchemaLightChange()).thenReturn(false);
            Mockito.when(olapTable.getIndexes()).thenReturn(null);
            List<List<TBrokerFileStatus>> fileStatuses1 =
                    Collections.singletonList(Collections.singletonList(new TBrokerFileStatus()));
            List<List<TBrokerFileStatus>> fileStatuses2 = Lists.newArrayList(
                    Collections.singletonList(new TBrokerFileStatus()),
                    Collections.singletonList(new TBrokerFileStatus()));
            Mockito.when(attachment.getFileStatusByTable(aggKey1)).thenReturn(fileStatuses1);
            Mockito.when(attachment.getFileNumByTable(aggKey1)).thenReturn(1);
            Mockito.when(attachment.getFileStatusByTable(aggKey2)).thenReturn(fileStatuses2);
            Mockito.when(attachment.getFileNumByTable(aggKey2)).thenReturn(2);
            Mockito.when(attachment.getFileStatusByTable(aggKey3)).thenReturn(fileStatuses2);
            Mockito.when(attachment.getFileNumByTable(aggKey3)).thenReturn(2);
            Mockito.when(env.getNextId()).thenReturn(1L, 2L, 3L);
            Mockito.when(env.getLoadingLoadTaskScheduler()).thenReturn(loadingTaskExecutor);
            Mockito.when(env.getComputeGroupMgr()).thenReturn(computeGroupMgr);
            Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
            EditLog editLog = Mockito.mock(EditLog.class);
            Mockito.when(env.getEditLog()).thenReturn(editLog);
            Mockito.when(computeGroupMgr.getAllBackendComputeGroup())
                    .thenReturn(new ComputeGroup("default", "default", null));
            Mockito.when(globalTxnMgr.getTransactionState(Mockito.anyLong(), Mockito.anyLong()))
                    .thenReturn(txnState);
            TxnStateCallbackFactory callbackFactory = Mockito.mock(TxnStateCallbackFactory.class);
            Mockito.when(globalTxnMgr.getCallbackFactory()).thenReturn(callbackFactory);

            brokerLoadJob.onTaskFinished(attachment);
            Set<Long> finishedTaskIds = Deencapsulation.getField(brokerLoadJob, "finishedTaskIds");
            Assert.assertEquals(1, finishedTaskIds.size());
            Assert.assertEquals(true, finishedTaskIds.contains(taskId));
            Map<Long, LoadTask> idToTasks = Deencapsulation.getField(brokerLoadJob, "idToTasks");
            Assert.assertEquals(3, idToTasks.size());
        }
    }

    @Test
    public void testLoadingTaskOnFinishedWithUnfinishedTask() {
        BrokerLoadingTaskAttachment attachment = Mockito.mock(BrokerLoadingTaskAttachment.class);
        LoadTask loadTask1 = Mockito.mock(LoadTask.class);
        LoadTask loadTask2 = Mockito.mock(LoadTask.class);

        BrokerLoadJob brokerLoadJob = new BrokerLoadJob();
        Deencapsulation.setField(brokerLoadJob, "state", JobState.LOADING);
        Map<Long, LoadTask> idToTasks = Maps.newHashMap();
        idToTasks.put(1L, loadTask1);
        idToTasks.put(2L, loadTask2);
        Deencapsulation.setField(brokerLoadJob, "idToTasks", idToTasks);

        Mockito.when(attachment.getCounter(BrokerLoadJob.DPP_NORMAL_ALL)).thenReturn("10");
        Mockito.when(attachment.getCounter(BrokerLoadJob.DPP_ABNORMAL_ALL)).thenReturn("1");
        Mockito.when(attachment.getTaskId()).thenReturn(1L);

        brokerLoadJob.onTaskFinished(attachment);
        Set<Long> finishedTaskIds = Deencapsulation.getField(brokerLoadJob, "finishedTaskIds");
        Assert.assertEquals(1, finishedTaskIds.size());
        EtlStatus loadingStatus = Deencapsulation.getField(brokerLoadJob, "loadingStatus");
        Assert.assertEquals("10", loadingStatus.getCounters().get(BrokerLoadJob.DPP_NORMAL_ALL));
        Assert.assertEquals("1", loadingStatus.getCounters().get(BrokerLoadJob.DPP_ABNORMAL_ALL));
        int progress = Deencapsulation.getField(brokerLoadJob, "progress");
        Assert.assertEquals(50, progress);
    }

    @Test
    public void testLoadingTaskOnFinishedWithErrorNum() {
        BrokerLoadingTaskAttachment attachment1 = Mockito.mock(BrokerLoadingTaskAttachment.class);
        BrokerLoadingTaskAttachment attachment2 = Mockito.mock(BrokerLoadingTaskAttachment.class);
        LoadTask loadTask1 = Mockito.mock(LoadTask.class);
        LoadTask loadTask2 = Mockito.mock(LoadTask.class);
        Env env = Mockito.mock(Env.class);
        GlobalTransactionMgrIface globalTxnMgr = Mockito.mock(GlobalTransactionMgrIface.class);
        TxnStateCallbackFactory callbackFactory = Mockito.mock(TxnStateCallbackFactory.class);
        EditLog editLog = Mockito.mock(EditLog.class);

        try (MockedStatic<Env> envMockedStatic = Mockito.mockStatic(Env.class)) {
            envMockedStatic.when(Env::getCurrentEnv).thenReturn(env);
            envMockedStatic.when(Env::getCurrentGlobalTransactionMgr).thenReturn(globalTxnMgr);

            Mockito.when(globalTxnMgr.getCallbackFactory()).thenReturn(callbackFactory);
            Mockito.when(env.getEditLog()).thenReturn(editLog);

            BrokerLoadJob brokerLoadJob = new BrokerLoadJob();
            Deencapsulation.setField(brokerLoadJob, "state", JobState.LOADING);
            Map<Long, LoadTask> idToTasks = Maps.newHashMap();
            idToTasks.put(1L, loadTask1);
            idToTasks.put(2L, loadTask2);
            Deencapsulation.setField(brokerLoadJob, "idToTasks", idToTasks);

            Mockito.when(attachment1.getCounter(BrokerLoadJob.DPP_NORMAL_ALL)).thenReturn("10");
            Mockito.when(attachment2.getCounter(BrokerLoadJob.DPP_NORMAL_ALL)).thenReturn("20");
            Mockito.when(attachment1.getCounter(BrokerLoadJob.DPP_ABNORMAL_ALL)).thenReturn("1");
            Mockito.when(attachment2.getCounter(BrokerLoadJob.DPP_ABNORMAL_ALL)).thenReturn("2");
            Mockito.when(attachment1.getTaskId()).thenReturn(1L);
            Mockito.when(attachment2.getTaskId()).thenReturn(2L);

            brokerLoadJob.onTaskFinished(attachment1);
            brokerLoadJob.onTaskFinished(attachment2);
            Set<Long> finishedTaskIds = Deencapsulation.getField(brokerLoadJob, "finishedTaskIds");
            Assert.assertEquals(2, finishedTaskIds.size());
            EtlStatus loadingStatus = Deencapsulation.getField(brokerLoadJob, "loadingStatus");
            Assert.assertEquals("30", loadingStatus.getCounters().get(BrokerLoadJob.DPP_NORMAL_ALL));
            Assert.assertEquals("3", loadingStatus.getCounters().get(BrokerLoadJob.DPP_ABNORMAL_ALL));
            int progress = Deencapsulation.getField(brokerLoadJob, "progress");
            Assert.assertEquals(99, progress);
            Assert.assertEquals(JobState.CANCELLED, Deencapsulation.getField(brokerLoadJob, "state"));
        }
    }

    @Test
    public void testLoadingTaskOnFinished() throws Exception {
        BrokerLoadingTaskAttachment attachment1 = Mockito.mock(BrokerLoadingTaskAttachment.class);
        LoadTask loadTask1 = Mockito.mock(LoadTask.class);
        Env env = Mockito.mock(Env.class);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Database database = Mockito.mock(Database.class);
        GlobalTransactionMgrIface globalTxnMgr = Mockito.mock(GlobalTransactionMgrIface.class);
        Status status = Mockito.mock(Status.class);

        try (MockedStatic<Env> envMockedStatic = Mockito.mockStatic(Env.class)) {
            envMockedStatic.when(Env::getCurrentEnv).thenReturn(env);
            envMockedStatic.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
            envMockedStatic.when(Env::getCurrentGlobalTransactionMgr).thenReturn(globalTxnMgr);

            Mockito.when(env.getEditLog()).thenReturn(Mockito.mock(EditLog.class));

            BrokerLoadJob brokerLoadJob = new BrokerLoadJob();
            Deencapsulation.setField(brokerLoadJob, "state", JobState.LOADING);
            Map<Long, LoadTask> idToTasks = Maps.newHashMap();
            idToTasks.put(1L, loadTask1);
            Deencapsulation.setField(brokerLoadJob, "idToTasks", idToTasks);

            Mockito.when(attachment1.getCounter(BrokerLoadJob.DPP_NORMAL_ALL)).thenReturn("10");
            Mockito.when(attachment1.getCounter(BrokerLoadJob.DPP_ABNORMAL_ALL)).thenReturn("0");
            Mockito.when(attachment1.getTaskId()).thenReturn(1L);
            Mockito.when(attachment1.getStatus()).thenReturn(status);
            Mockito.when(status.getErrorCode()).thenReturn(TStatusCode.OK);
            Mockito.doReturn(database).when(catalog).getDbOrMetaException(Mockito.anyLong());
            Mockito.doReturn(Lists.newArrayList()).when(database)
                    .getTablesOnIdOrderOrThrowException(Mockito.anyList());

            brokerLoadJob.onTaskFinished(attachment1);
            Set<Long> finishedTaskIds = Deencapsulation.getField(brokerLoadJob, "finishedTaskIds");
            Assert.assertEquals(1, finishedTaskIds.size());
            EtlStatus loadingStatus = Deencapsulation.getField(brokerLoadJob, "loadingStatus");
            Assert.assertEquals("10", loadingStatus.getCounters().get(BrokerLoadJob.DPP_NORMAL_ALL));
            Assert.assertEquals("0", loadingStatus.getCounters().get(BrokerLoadJob.DPP_ABNORMAL_ALL));
            int progress = Deencapsulation.getField(brokerLoadJob, "progress");
            Assert.assertEquals(99, progress);
        }
    }

    @Test
    public void testExecuteReplayOnAborted() {
        TransactionState txnState = Mockito.mock(TransactionState.class);
        LoadJobFinalOperation attachment = Mockito.mock(LoadJobFinalOperation.class);
        EtlStatus etlStatus = Mockito.mock(EtlStatus.class);

        BrokerLoadJob brokerLoadJob = new BrokerLoadJob();

        Mockito.when(txnState.getTxnCommitAttachment()).thenReturn(attachment);
        Mockito.when(attachment.getLoadingStatus()).thenReturn(etlStatus);
        Mockito.when(attachment.getProgress()).thenReturn(99);
        Mockito.when(attachment.getFinishTimestamp()).thenReturn(1L);
        Mockito.when(attachment.getJobState()).thenReturn(JobState.CANCELLED);

        brokerLoadJob.replayTxnAttachment(txnState);
        Assert.assertEquals(99, (int) Deencapsulation.getField(brokerLoadJob, "progress"));
        Assert.assertEquals(1, brokerLoadJob.getFinishTimestamp());
        Assert.assertEquals(JobState.CANCELLED, brokerLoadJob.getState());
    }


    @Test
    public void testExecuteReplayOnVisible() {
        TransactionState txnState = Mockito.mock(TransactionState.class);
        LoadJobFinalOperation attachment = Mockito.mock(LoadJobFinalOperation.class);
        EtlStatus etlStatus = Mockito.mock(EtlStatus.class);

        BrokerLoadJob brokerLoadJob = new BrokerLoadJob();

        Mockito.when(txnState.getTxnCommitAttachment()).thenReturn(attachment);
        Mockito.when(attachment.getLoadingStatus()).thenReturn(etlStatus);
        Mockito.when(attachment.getProgress()).thenReturn(99);
        Mockito.when(attachment.getFinishTimestamp()).thenReturn(1L);
        Mockito.when(attachment.getJobState()).thenReturn(JobState.LOADING);

        brokerLoadJob.replayTxnAttachment(txnState);
        Assert.assertEquals(99, (int) Deencapsulation.getField(brokerLoadJob, "progress"));
        Assert.assertEquals(1, brokerLoadJob.getFinishTimestamp());
        Assert.assertEquals(JobState.LOADING, brokerLoadJob.getState());
    }
}
