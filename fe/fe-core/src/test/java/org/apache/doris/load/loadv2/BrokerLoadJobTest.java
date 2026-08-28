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
import org.apache.doris.cloud.load.CloudBrokerLoadJob;
import org.apache.doris.common.LabelAlreadyUsedException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.load.BrokerFileGroup;
import org.apache.doris.load.BrokerFileGroupAggInfo;
import org.apache.doris.load.BrokerFileGroupAggInfo.FileGroupAggKey;
import org.apache.doris.load.EtlStatus;
import org.apache.doris.load.FailMsg;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.nereids.load.NereidsBrokerFileGroup;
import org.apache.doris.nereids.load.NereidsLoadingTaskPlanner;
import org.apache.doris.persist.EditLog;
import org.apache.doris.resource.computegroup.ComputeGroup;
import org.apache.doris.resource.computegroup.ComputeGroupMgr;
import org.apache.doris.task.MasterTaskExecutor;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.transaction.GlobalTransactionMgrIface;
import org.apache.doris.transaction.TransactionState;
import org.apache.doris.transaction.TransactionStatus;
import org.apache.doris.transaction.TxnStateCallbackFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
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
    public void testGetTableNames(@Injectable BrokerFileGroupAggInfo fileGroupAggInfo,
                                  @Injectable BrokerFileGroup brokerFileGroup, @Mocked Env env, @Mocked InternalCatalog catalog,
                                  @Injectable Database database, @Injectable Table table) throws MetaNotFoundException {
        List<BrokerFileGroup> brokerFileGroups = Lists.newArrayList();
        brokerFileGroups.add(brokerFileGroup);
        Map<FileGroupAggKey, List<BrokerFileGroup>> aggKeyToFileGroups = Maps.newHashMap();
        FileGroupAggKey aggKey = new FileGroupAggKey(1L, null);
        aggKeyToFileGroups.put(aggKey, brokerFileGroups);
        BrokerLoadJob brokerLoadJob = new BrokerLoadJob();
        Deencapsulation.setField(brokerLoadJob, "fileGroupAggInfo", fileGroupAggInfo);
        String tableName = "table";
        new Expectations() {
            {
                fileGroupAggInfo.getAggKeyToFileGroups();
                minTimes = 0;
                result = aggKeyToFileGroups;
                fileGroupAggInfo.getAllTableIds();
                minTimes = 0;
                result = Sets.newHashSet(1L);
                env.getInternalCatalog();
                minTimes = 0;
                result = catalog;
                catalog.getDb(anyLong);
                minTimes = 0;
                result = Optional.of(database);
                database.getTable(1L);
                minTimes = 0;
                result = Optional.of(table);
                table.getName();
                minTimes = 0;
                result = tableName;
            }
        };

        Assert.assertEquals(1, brokerLoadJob.getTableNamesForShow().size());
        Assert.assertTrue(brokerLoadJob.getTableNamesForShow().contains(tableName));
    }

    @Test
    public void testExecuteJob(@Mocked MasterTaskExecutor masterTaskExecutor) {
        BrokerLoadJob brokerLoadJob = new BrokerLoadJob();
        brokerLoadJob.unprotectedExecuteJob();

        Map<Long, LoadTask> idToTasks = Deencapsulation.getField(brokerLoadJob, "idToTasks");
        Assert.assertEquals(1, idToTasks.size());
    }

    @Test
    public void testPendingTaskOnFinishedWithJobCancelled(@Injectable BrokerPendingTaskAttachment attachment) {
        BrokerLoadJob brokerLoadJob = new BrokerLoadJob();
        Deencapsulation.setField(brokerLoadJob, "state", JobState.CANCELLED);
        brokerLoadJob.onTaskFinished(attachment);

        Set<Long> finishedTaskIds = Deencapsulation.getField(brokerLoadJob, "finishedTaskIds");
        Assert.assertEquals(0, finishedTaskIds.size());
    }

    @Test
    public void testPendingTaskOnFinishedWithDuplicated(@Injectable BrokerPendingTaskAttachment attachment) {
        BrokerLoadJob brokerLoadJob = new BrokerLoadJob();
        Deencapsulation.setField(brokerLoadJob, "state", JobState.LOADING);
        Set<Long> finishedTaskIds = Sets.newHashSet();
        long taskId = 1L;
        finishedTaskIds.add(taskId);
        Deencapsulation.setField(brokerLoadJob, "finishedTaskIds", finishedTaskIds);
        new Expectations() {
            {
                attachment.getTaskId();
                minTimes = 0;
                result = taskId;
            }
        };

        brokerLoadJob.onTaskFinished(attachment);
        Map<Long, LoadTask> idToTasks = Deencapsulation.getField(brokerLoadJob, "idToTasks");
        Assert.assertEquals(0, idToTasks.size());
    }

    @Test
    public void testPendingTaskOnFinished(@Injectable BrokerPendingTaskAttachment attachment, @Mocked Env env,
                                          @Mocked InternalCatalog catalog, @Injectable Database database,
                                          @Injectable BrokerFileGroupAggInfo fileGroupAggInfo, @Injectable BrokerFileGroup brokerFileGroup1,
                                          @Injectable BrokerFileGroup brokerFileGroup2, @Injectable BrokerFileGroup brokerFileGroup3,
                                          @Mocked MasterTaskExecutor masterTaskExecutor, @Injectable OlapTable olapTable,
                                          @Mocked NereidsLoadingTaskPlanner loadingTaskPlanner) {
        BrokerLoadJob brokerLoadJob = new BrokerLoadJob();
        Deencapsulation.setField(brokerLoadJob, "state", JobState.LOADING);
        long taskId = 1L;
        long tableId1 = 1L;
        long tableId2 = 2L;
        long partitionId1 = 3L;
        long partitionId2 = 4;

        Map<FileGroupAggKey, List<BrokerFileGroup>> aggKeyToFileGroups = Maps.newHashMap();
        List<BrokerFileGroup> fileGroups1 = Lists.newArrayList();
        fileGroups1.add(brokerFileGroup1);
        aggKeyToFileGroups.put(new FileGroupAggKey(tableId1, null), fileGroups1);

        List<BrokerFileGroup> fileGroups2 = Lists.newArrayList();
        fileGroups2.add(brokerFileGroup2);
        fileGroups2.add(brokerFileGroup3);
        aggKeyToFileGroups.put(new FileGroupAggKey(tableId2, Lists.newArrayList(partitionId1)), fileGroups2);
        // add another file groups with different partition id
        aggKeyToFileGroups.put(new FileGroupAggKey(tableId2, Lists.newArrayList(partitionId2)), fileGroups2);

        Deencapsulation.setField(brokerLoadJob, "fileGroupAggInfo", fileGroupAggInfo);
        new Expectations() {
            {
                attachment.getTaskId();
                minTimes = 0;
                result = taskId;
                env.getInternalCatalog();
                minTimes = 0;
                result = catalog;
                catalog.getDbNullable(anyLong);
                minTimes = 0;
                result = database;
                fileGroupAggInfo.getAggKeyToFileGroups();
                minTimes = 0;
                result = aggKeyToFileGroups;
                database.getTableNullable(anyLong);
                minTimes = 0;
                result = olapTable;
                env.getNextId();
                minTimes = 0;
                result = 1L;
                result = 2L;
                result = 3L;
            }
        };

        brokerLoadJob.onTaskFinished(attachment);
        Set<Long> finishedTaskIds = Deencapsulation.getField(brokerLoadJob, "finishedTaskIds");
        Assert.assertEquals(1, finishedTaskIds.size());
        Assert.assertEquals(true, finishedTaskIds.contains(taskId));
        Map<Long, LoadTask> idToTasks = Deencapsulation.getField(brokerLoadJob, "idToTasks");
        Assert.assertEquals(3, idToTasks.size());
    }

    @Test
    public void testLoadingTaskOnFinishedWithUnfinishedTask(@Injectable BrokerLoadingTaskAttachment attachment,
                                                            @Injectable LoadTask loadTask1,
                                                            @Injectable LoadTask loadTask2) {
        BrokerLoadJob brokerLoadJob = new BrokerLoadJob();
        Deencapsulation.setField(brokerLoadJob, "state", JobState.LOADING);
        Map<Long, LoadTask> idToTasks = Maps.newHashMap();
        idToTasks.put(1L, loadTask1);
        idToTasks.put(2L, loadTask2);
        Deencapsulation.setField(brokerLoadJob, "idToTasks", idToTasks);
        new Expectations() {
            {
                attachment.getCounter(BrokerLoadJob.DPP_NORMAL_ALL);
                minTimes = 0;
                result = 10;
                attachment.getCounter(BrokerLoadJob.DPP_ABNORMAL_ALL);
                minTimes = 0;
                result = 1;
                attachment.getTaskId();
                minTimes = 0;
                result = 1L;
            }
        };

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
    public void testLoadingTaskOnFinishedWithErrorNum(@Injectable BrokerLoadingTaskAttachment attachment1,
                                                      @Injectable BrokerLoadingTaskAttachment attachment2,
                                                      @Injectable LoadTask loadTask1,
                                                      @Injectable LoadTask loadTask2,
                                                      @Mocked Env env) {
        BrokerLoadJob brokerLoadJob = new BrokerLoadJob();
        Deencapsulation.setField(brokerLoadJob, "state", JobState.LOADING);
        Map<Long, LoadTask> idToTasks = Maps.newHashMap();
        idToTasks.put(1L, loadTask1);
        idToTasks.put(2L, loadTask2);
        Deencapsulation.setField(brokerLoadJob, "idToTasks", idToTasks);
        new Expectations() {
            {
                attachment1.getCounter(BrokerLoadJob.DPP_NORMAL_ALL);
                minTimes = 0;
                result = 10;
                attachment2.getCounter(BrokerLoadJob.DPP_NORMAL_ALL);
                minTimes = 0;
                result = 20;
                attachment1.getCounter(BrokerLoadJob.DPP_ABNORMAL_ALL);
                minTimes = 0;
                result = 1;
                attachment2.getCounter(BrokerLoadJob.DPP_ABNORMAL_ALL);
                minTimes = 0;
                result = 2;
                attachment1.getTaskId();
                minTimes = 0;
                result = 1L;
                attachment2.getTaskId();
                minTimes = 0;
                result = 2L;
            }
        };

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

    @Test
    public void testLoadingTaskOnFinished(@Injectable BrokerLoadingTaskAttachment attachment1,
                                          @Injectable LoadTask loadTask1, @Mocked Env env, @Mocked InternalCatalog catalog,
                                          @Injectable Database database) {
        BrokerLoadJob brokerLoadJob = new BrokerLoadJob();
        Deencapsulation.setField(brokerLoadJob, "state", JobState.LOADING);
        Map<Long, LoadTask> idToTasks = Maps.newHashMap();
        idToTasks.put(1L, loadTask1);
        Deencapsulation.setField(brokerLoadJob, "idToTasks", idToTasks);
        new Expectations() {
            {
                attachment1.getCounter(BrokerLoadJob.DPP_NORMAL_ALL);
                minTimes = 0;
                result = 10;
                attachment1.getCounter(BrokerLoadJob.DPP_ABNORMAL_ALL);
                minTimes = 0;
                result = 0;
                attachment1.getTaskId();
                minTimes = 0;
                result = 1L;
                env.getInternalCatalog();
                minTimes = 0;
                result = catalog;
                catalog.getDbNullable(anyLong);
                minTimes = 0;
                result = database;
            }
        };

        brokerLoadJob.onTaskFinished(attachment1);
        Set<Long> finishedTaskIds = Deencapsulation.getField(brokerLoadJob, "finishedTaskIds");
        Assert.assertEquals(1, finishedTaskIds.size());
        EtlStatus loadingStatus = Deencapsulation.getField(brokerLoadJob, "loadingStatus");
        Assert.assertEquals("10", loadingStatus.getCounters().get(BrokerLoadJob.DPP_NORMAL_ALL));
        Assert.assertEquals("0", loadingStatus.getCounters().get(BrokerLoadJob.DPP_ABNORMAL_ALL));
        int progress = Deencapsulation.getField(brokerLoadJob, "progress");
        Assert.assertEquals(99, progress);
    }

    @Test
    public void testExecuteReplayOnAborted(@Injectable TransactionState txnState,
                                           @Injectable LoadJobFinalOperation attachment,
                                           @Injectable EtlStatus etlStatus) {
        BrokerLoadJob brokerLoadJob = new BrokerLoadJob();
        new Expectations() {
            {
                txnState.getTxnCommitAttachment();
                minTimes = 0;
                result = attachment;
                attachment.getLoadingStatus();
                minTimes = 0;
                result = etlStatus;
                attachment.getProgress();
                minTimes = 0;
                result = 99;
                attachment.getFinishTimestamp();
                minTimes = 0;
                result = 1;
                attachment.getJobState();
                minTimes = 0;
                result = JobState.CANCELLED;
            }
        };
        brokerLoadJob.replayTxnAttachment(txnState);
        Assert.assertEquals(99, (int) Deencapsulation.getField(brokerLoadJob, "progress"));
        Assert.assertEquals(1, brokerLoadJob.getFinishTimestamp());
        Assert.assertEquals(JobState.CANCELLED, brokerLoadJob.getState());
    }


    @Test
    public void testExecuteReplayOnVisible(@Injectable TransactionState txnState,
                                           @Injectable LoadJobFinalOperation attachment,
                                           @Injectable EtlStatus etlStatus) {
        BrokerLoadJob brokerLoadJob = new BrokerLoadJob();
        new Expectations() {
            {
                txnState.getTxnCommitAttachment();
                minTimes = 0;
                result = attachment;
                attachment.getLoadingStatus();
                minTimes = 0;
                result = etlStatus;
                attachment.getProgress();
                minTimes = 0;
                result = 99;
                attachment.getFinishTimestamp();
                minTimes = 0;
                result = 1;
                attachment.getJobState();
                minTimes = 0;
                result = JobState.LOADING;
            }
        };
        brokerLoadJob.replayTxnAttachment(txnState);
        Assert.assertEquals(99, (int) Deencapsulation.getField(brokerLoadJob, "progress"));
        Assert.assertEquals(1, brokerLoadJob.getFinishTimestamp());
        Assert.assertEquals(JobState.LOADING, brokerLoadJob.getState());
    }

    @Test
    public void testBeginTxnReusesAlreadyBegunTxn() throws Exception {
        // A retried pending task must not begin a second txn for the same job (it would fail
        // LabelAlreadyUsedException against the job's own txn).
        GlobalTransactionMgrIface transactionMgr = Mockito.mock(GlobalTransactionMgrIface.class);
        BrokerLoadJob brokerLoadJob = new BrokerLoadJob();
        Deencapsulation.setField(brokerLoadJob, "transactionId", 12345L);

        try (MockedStatic<Env> envMockedStatic = Mockito.mockStatic(Env.class)) {
            envMockedStatic.when(Env::getCurrentGlobalTransactionMgr).thenReturn(transactionMgr);
            brokerLoadJob.beginTxn();
        }

        Assert.assertEquals(12345L, (long) Deencapsulation.getField(brokerLoadJob, "transactionId"));
        Mockito.verifyNoInteractions(transactionMgr);
    }

    @Test
    public void testBeginTxnAdoptsOwnPreparedTxn() throws Exception {
        // First attempt registered the txn but threw before transactionId was assigned
        // (e.g. edit log write failure); the retry gets LabelAlreadyUsedException for the job's
        // OWN prepared txn and must adopt it instead of cancelling the job.
        GlobalTransactionMgrIface transactionMgr = Mockito.mock(GlobalTransactionMgrIface.class);
        TransactionState preparedTxn = Mockito.mock(TransactionState.class);
        BrokerLoadJob brokerLoadJob = new BrokerLoadJob();
        Deencapsulation.setField(brokerLoadJob, "id", 1001L);
        Deencapsulation.setField(brokerLoadJob, "dbId", 1L);
        Deencapsulation.setField(brokerLoadJob, "label", "label_self_conflict");

        try (MockedStatic<Env> envMockedStatic = Mockito.mockStatic(Env.class)) {
            envMockedStatic.when(Env::getCurrentGlobalTransactionMgr).thenReturn(transactionMgr);
            Mockito.when(transactionMgr.beginTransaction(Mockito.anyLong(), Mockito.anyList(),
                    Mockito.anyString(), Mockito.any(), Mockito.any(), Mockito.any(),
                    Mockito.anyLong(), Mockito.anyLong()))
                    .thenThrow(new LabelAlreadyUsedException("label_self_conflict"));
            Mockito.when(transactionMgr.getTransactionId(Mockito.anyLong(), Mockito.anyString())).thenReturn(777L);
            Mockito.when(transactionMgr.getTransactionState(Mockito.anyLong(), Mockito.eq(777L)))
                    .thenReturn(preparedTxn);
            Mockito.when(preparedTxn.getTransactionId()).thenReturn(777L);
            Mockito.when(preparedTxn.getCallbackId()).thenReturn(1001L);
            Mockito.when(preparedTxn.getTransactionStatus()).thenReturn(TransactionStatus.PREPARE);

            brokerLoadJob.beginTxn();
        }

        Assert.assertEquals(777L, (long) Deencapsulation.getField(brokerLoadJob, "transactionId"));
    }

    @Test
    public void testBeginTxnFinishesOwnVisibleTxn() throws Exception {
        // The transaction may become visible in meta service before cloud FE persists the final
        // load-job state. After FE restart the replayed PENDING job must recover that successful
        // transaction instead of retrying beginTxn until it is cancelled by its own label.
        GlobalTransactionMgrIface transactionMgr = Mockito.mock(GlobalTransactionMgrIface.class);
        TransactionState visibleTxn = Mockito.mock(TransactionState.class);
        TxnStateCallbackFactory callbackFactory = Mockito.mock(TxnStateCallbackFactory.class);
        Env env = Mockito.mock(Env.class);
        EditLog editLog = Mockito.mock(EditLog.class);
        CloudBrokerLoadJob brokerLoadJob = new CloudBrokerLoadJob();
        Deencapsulation.setField(brokerLoadJob, "id", 1001L);
        Deencapsulation.setField(brokerLoadJob, "dbId", 1L);
        Deencapsulation.setField(brokerLoadJob, "label", "label_visible_after_restart");

        try (MockedStatic<Env> envMockedStatic = Mockito.mockStatic(Env.class)) {
            envMockedStatic.when(Env::getCurrentGlobalTransactionMgr).thenReturn(transactionMgr);
            envMockedStatic.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(env.getEditLog()).thenReturn(editLog);
            Mockito.when(transactionMgr.getCallbackFactory()).thenReturn(callbackFactory);
            Mockito.when(transactionMgr.beginTransaction(Mockito.anyLong(), Mockito.anyList(),
                    Mockito.anyString(), Mockito.any(), Mockito.any(), Mockito.any(),
                    Mockito.anyLong(), Mockito.anyLong()))
                    .thenThrow(new LabelAlreadyUsedException("label_visible_after_restart"));
            Mockito.when(transactionMgr.getTransactionId(Mockito.anyLong(), Mockito.anyString())).thenReturn(888L);
            Mockito.when(transactionMgr.getTransactionState(Mockito.anyLong(), Mockito.eq(888L)))
                    .thenReturn(visibleTxn);
            Mockito.when(visibleTxn.getTransactionId()).thenReturn(888L);
            Mockito.when(visibleTxn.getCallbackId()).thenReturn(1001L);
            Mockito.when(visibleTxn.getTransactionStatus()).thenReturn(TransactionStatus.VISIBLE);

            brokerLoadJob.beginTxn();
        }

        Assert.assertEquals(888L, (long) Deencapsulation.getField(brokerLoadJob, "transactionId"));
        Assert.assertEquals(JobState.FINISHED, brokerLoadJob.getState());
        Mockito.verify(callbackFactory).removeCallback(1001L);
        Mockito.verify(editLog).logEndLoadJob(Mockito.any(LoadJobFinalOperation.class));
    }

    @Test
    public void testBeginTxnRethrowsForeignLabelConflict() throws Exception {
        // The label belongs to some other job's txn: the original exception must propagate.
        GlobalTransactionMgrIface transactionMgr = Mockito.mock(GlobalTransactionMgrIface.class);
        TransactionState foreignTxn = Mockito.mock(TransactionState.class);
        BrokerLoadJob brokerLoadJob = new BrokerLoadJob();
        Deencapsulation.setField(brokerLoadJob, "id", 1001L);
        Deencapsulation.setField(brokerLoadJob, "dbId", 1L);
        Deencapsulation.setField(brokerLoadJob, "label", "label_foreign");

        try (MockedStatic<Env> envMockedStatic = Mockito.mockStatic(Env.class)) {
            envMockedStatic.when(Env::getCurrentGlobalTransactionMgr).thenReturn(transactionMgr);
            Mockito.when(transactionMgr.beginTransaction(Mockito.anyLong(), Mockito.anyList(),
                    Mockito.anyString(), Mockito.any(), Mockito.any(), Mockito.any(),
                    Mockito.anyLong(), Mockito.anyLong()))
                    .thenThrow(new LabelAlreadyUsedException("label_foreign"));
            Mockito.when(transactionMgr.getTransactionId(Mockito.anyLong(), Mockito.anyString())).thenReturn(888L);
            Mockito.when(transactionMgr.getTransactionState(Mockito.anyLong(), Mockito.eq(888L)))
                    .thenReturn(foreignTxn);
            Mockito.when(foreignTxn.getCallbackId()).thenReturn(9999L);

            try {
                brokerLoadJob.beginTxn();
                Assert.fail("expected LabelAlreadyUsedException");
            } catch (LabelAlreadyUsedException expected) {
                // expected
            }
        }

        Assert.assertEquals(0L, (long) Deencapsulation.getField(brokerLoadJob, "transactionId"));
    }

    @Test
    public void testPendingTaskOnFinishedWithNereidsPlanningError() throws Exception {
        // A Nereids planning error is a RuntimeException; it must cancel the job with the real
        // cause instead of escaping into the generic pending-task retry path (which used to end
        // in a misleading "Label has already been used" cancellation).
        BrokerPendingTaskAttachment attachment = Mockito.mock(BrokerPendingTaskAttachment.class);
        Env env = Mockito.mock(Env.class);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Database database = Mockito.mock(Database.class);
        BrokerFileGroupAggInfo fileGroupAggInfo = Mockito.mock(BrokerFileGroupAggInfo.class);
        BrokerFileGroup brokerFileGroup = Mockito.mock(BrokerFileGroup.class);
        NereidsBrokerFileGroup nereidsBfg = Mockito.mock(NereidsBrokerFileGroup.class);
        Mockito.when(brokerFileGroup.toNereidsBrokerFileGroup()).thenReturn(nereidsBfg);
        OlapTable olapTable = Mockito.mock(OlapTable.class);
        GlobalTransactionMgrIface globalTxnMgr = Mockito.mock(GlobalTransactionMgrIface.class);
        ProgressManager progressManager = Mockito.mock(ProgressManager.class);
        ComputeGroupMgr computeGroupMgr = Mockito.mock(ComputeGroupMgr.class);
        TableProperty tableProperty = Mockito.mock(TableProperty.class);

        try (MockedStatic<Env> envMockedStatic = Mockito.mockStatic(Env.class);
                MockedConstruction<NereidsLoadingTaskPlanner> ignored =
                        Mockito.mockConstruction(NereidsLoadingTaskPlanner.class, (mock, context) ->
                                Mockito.doThrow(new org.apache.doris.nereids.exceptions.AnalysisException(
                                        "disk /mnt/mock on backend 10001 exceed limit usage"))
                                        .when(mock).plan(Mockito.any(), Mockito.anyList(), Mockito.anyInt()))) {
            envMockedStatic.when(Env::getCurrentEnv).thenReturn(env);
            envMockedStatic.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
            envMockedStatic.when(Env::getCurrentProgressManager).thenReturn(progressManager);
            envMockedStatic.when(Env::getCurrentGlobalTransactionMgr).thenReturn(globalTxnMgr);

            BrokerLoadJob brokerLoadJob = new BrokerLoadJob();
            Deencapsulation.setField(brokerLoadJob, "state", JobState.LOADING);
            BrokerDesc brokerDesc = Mockito.mock(BrokerDesc.class);
            Deencapsulation.setField(brokerLoadJob, "brokerDesc", brokerDesc);

            Map<FileGroupAggKey, List<BrokerFileGroup>> aggKeyToFileGroups = Maps.newHashMap();
            FileGroupAggKey aggKey = new FileGroupAggKey(1L, null);
            aggKeyToFileGroups.put(aggKey, Lists.newArrayList(brokerFileGroup));
            Deencapsulation.setField(brokerLoadJob, "fileGroupAggInfo", fileGroupAggInfo);

            Mockito.when(attachment.getTaskId()).thenReturn(1L);
            Mockito.doReturn(database).when(catalog).getDbOrMetaException(Mockito.anyLong());
            Mockito.doReturn(Lists.newArrayList()).when(database)
                    .getTablesOnIdOrderOrThrowException(Mockito.anyList());
            Mockito.when(fileGroupAggInfo.getAggKeyToFileGroups()).thenReturn(aggKeyToFileGroups);
            Mockito.when(fileGroupAggInfo.getAllTableIds()).thenReturn(Sets.newHashSet(1L));
            Mockito.doReturn(olapTable).when(database).getTableNullable(Mockito.anyLong());
            Mockito.when(olapTable.isTemporary()).thenReturn(false);
            Mockito.when(olapTable.getTableProperty()).thenReturn(tableProperty);
            Mockito.when(tableProperty.getUseSchemaLightChange()).thenReturn(false);
            Mockito.when(olapTable.getIndexes()).thenReturn(null);
            Mockito.when(attachment.getFileStatusByTable(aggKey)).thenReturn(
                    Collections.singletonList(Collections.singletonList(new TBrokerFileStatus())));
            Mockito.when(attachment.getFileNumByTable(aggKey)).thenReturn(1);
            Mockito.when(env.getNextId()).thenReturn(1L);
            Mockito.when(env.getComputeGroupMgr()).thenReturn(computeGroupMgr);
            Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
            EditLog editLog = Mockito.mock(EditLog.class);
            Mockito.when(env.getEditLog()).thenReturn(editLog);
            Mockito.when(computeGroupMgr.getAllBackendComputeGroup())
                    .thenReturn(new ComputeGroup("default", "default", null));
            TxnStateCallbackFactory callbackFactory = Mockito.mock(TxnStateCallbackFactory.class);
            Mockito.when(globalTxnMgr.getCallbackFactory()).thenReturn(callbackFactory);

            brokerLoadJob.onTaskFinished(attachment);

            Assert.assertEquals(JobState.CANCELLED, brokerLoadJob.getState());
            FailMsg failMsg = Deencapsulation.getField(brokerLoadJob, "failMsg");
            Assert.assertTrue(failMsg.getMsg().contains("exceed limit usage"));
            Map<Long, LoadTask> idToTasks = Deencapsulation.getField(brokerLoadJob, "idToTasks");
            Assert.assertEquals(0, idToTasks.size());
        }
    }
}
