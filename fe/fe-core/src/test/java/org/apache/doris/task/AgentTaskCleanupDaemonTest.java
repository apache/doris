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

package org.apache.doris.task;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.Status;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.master.ReportHandler;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.persist.EditLog;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TPartitionVersionInfo;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TTaskType;
import org.apache.doris.transaction.GlobalTransactionMgrIface;
import org.apache.doris.transaction.PublishVersionDaemon;
import org.apache.doris.transaction.TransactionState;
import org.apache.doris.transaction.TransactionStatus;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class AgentTaskCleanupDaemonTest {
    private static final long BACKEND_ID = 10001L;
    private static final long HEALTHY_BACKEND_ID = 10002L;
    private static final long TRANSACTION_ID = 99L;

    private MockedStatic<Env> mockedEnv;
    private Backend backend;
    private AgentTaskCleanupDaemon daemon;
    private SystemInfoService infoService;
    private InternalCatalog catalog;
    private Database database;
    private Env env;
    private GlobalTransactionMgrIface transactionMgr;

    @BeforeEach
    public void setUp() {
        MetricRepo.init();
        AgentTaskQueue.clearAllTasks();
        backend = new Backend(BACKEND_ID, "127.0.0.1", 9050);
        backend.setAlive(false);
        Backend healthyBackend = new Backend(HEALTHY_BACKEND_ID, "127.0.0.2", 9050);
        healthyBackend.setAlive(true);
        infoService = new SystemInfoService();
        infoService.addBackend(backend);
        infoService.addBackend(healthyBackend);
        catalog = Mockito.mock(InternalCatalog.class);
        database = Mockito.spy(new Database(1L, "publish_test"));
        Mockito.when(catalog.getDbNullable(1L)).thenReturn(database);
        Mockito.when(catalog.getDbNullable(2L)).thenReturn(new Database(2L, "other_publish_test"));
        env = Mockito.mock(Env.class);
        Mockito.when(env.getEditLog()).thenReturn(Mockito.mock(EditLog.class));
        transactionMgr = Mockito.mock(GlobalTransactionMgrIface.class);
        Mockito.doCallRealMethod().when(env).eraseDatabase(Mockito.anyLong(), Mockito.anyBoolean());
        mockedEnv = mockEnv();
        daemon = new AgentTaskCleanupDaemon();
    }

    @AfterEach
    public void tearDown() {
        mockedEnv.close();
        AgentTaskQueue.clearAllTasks();
    }

    @Test
    public void testOfflineCleanupPreservesPublishTasksForResend() {
        PublishVersionTask publishTask = newPublishTask(BACKEND_ID, TRANSACTION_ID);
        PublishVersionTask anotherPublishTask = newPublishTask(BACKEND_ID, TRANSACTION_ID + 1);
        DropReplicaTask dropTask = new DropReplicaTask(BACKEND_ID, 10L, 20L, 0, false);
        DropReplicaTask healthyDropTask = new DropReplicaTask(HEALTHY_BACKEND_ID, 11L, 21L, 0, false);
        AgentTaskQueue.addTask(publishTask);
        AgentTaskQueue.addTask(anotherPublishTask);
        AgentTaskQueue.addTask(dropTask);
        AgentTaskQueue.addTask(healthyDropTask);

        for (int i = 0; i < AgentTaskCleanupDaemon.MAX_FAILURE_TIMES - 1; i++) {
            daemon.runAfterCatalogReady();
            Assertions.assertTrue(AgentTaskQueue.contains(dropTask));
            Assertions.assertFalse(dropTask.isFinished());
        }
        daemon.runAfterCatalogReady();
        Assertions.assertFalse(AgentTaskQueue.contains(dropTask));
        Assertions.assertTrue(dropTask.isFinished());
        Assertions.assertEquals(TStatusCode.ABORTED, dropTask.getErrorCode());
        Assertions.assertTrue(AgentTaskQueue.contains(healthyDropTask));
        Assertions.assertFalse(healthyDropTask.isFinished());

        // Further offline checks must not discard or finish the retained publish tasks either.
        daemon.runAfterCatalogReady();
        for (PublishVersionTask task : List.of(publishTask, anotherPublishTask)) {
            Assertions.assertSame(task, AgentTaskQueue.getTask(BACKEND_ID, TTaskType.PUBLISH_VERSION,
                    task.getSignature()));
            Assertions.assertFalse(task.isFinished());
            Assertions.assertNull(task.getErrorCode());
            Assertions.assertTrue(task.getErrorTablets().isEmpty());
        }

        backend.setAlive(true);
        daemon.runAfterCatalogReady();
        try (MockedStatic<AgentTaskExecutor> executor = Mockito.mockStatic(AgentTaskExecutor.class)) {
            Deencapsulation.invoke(ReportHandler.class, "taskReport", BACKEND_ID, Collections.emptyMap(), 0L);
            ArgumentCaptor<AgentBatchTask> batch = ArgumentCaptor.forClass(AgentBatchTask.class);
            executor.verify(() -> AgentTaskExecutor.submit(batch.capture()));
            Assertions.assertEquals(Set.of(publishTask, anotherPublishTask),
                    Set.copyOf(batch.getValue().getAllTasks()));

            executor.reset();
            Map<TTaskType, Set<Long>> runningTasks = Map.of(TTaskType.PUBLISH_VERSION,
                    Set.of(publishTask.getSignature(), anotherPublishTask.getSignature()));
            Deencapsulation.invoke(ReportHandler.class, "taskReport", BACKEND_ID, runningTasks, 2L);
            executor.verifyNoInteractions();
        }
    }

    @Test
    public void testOfflineCleanupPreservesPartialPublishResult() {
        PublishVersionTask task = newPublishTask(BACKEND_ID, TRANSACTION_ID);
        task.setSuccTablets(Map.of(10L, 3L));
        task.addErrorTablets(List.of(11L));
        task.setErrorCode(TStatusCode.RUNTIME_ERROR);
        task.setFinished(true);
        AgentTaskQueue.addTask(task);

        for (int i = 0; i < AgentTaskCleanupDaemon.MAX_FAILURE_TIMES; i++) {
            daemon.runAfterCatalogReady();
        }
        Assertions.assertSame(task, AgentTaskQueue.getTask(BACKEND_ID, TTaskType.PUBLISH_VERSION,
                task.getSignature()));
        Assertions.assertTrue(task.isFinished());
        Assertions.assertEquals(TStatusCode.RUNTIME_ERROR, task.getErrorCode());
        Assertions.assertEquals(Map.of(10L, 3L), task.getSuccTablets());
        Assertions.assertEquals(List.of(11L), task.getErrorTablets());
        Assertions.assertEquals(List.of(task), AgentTaskQueue.getDiffTasks(BACKEND_ID, Collections.emptyMap()));
    }

    @Test
    public void testVisibleTransactionRemovesRetainedOfflinePublishTask() throws Exception {
        TransactionState transaction = newTransaction();
        transaction.setTransactionStatus(TransactionStatus.COMMITTED);
        PublishVersionTask offlineTask = newPublishTask(BACKEND_ID, transaction.getTransactionId());
        PublishVersionTask healthyTask = newPublishTask(HEALTHY_BACKEND_ID, transaction.getTransactionId());
        transaction.addPublishVersionTask(BACKEND_ID, offlineTask);
        transaction.addPublishVersionTask(HEALTHY_BACKEND_ID, healthyTask);
        transaction.setSendedTask();
        AgentTaskQueue.addTask(offlineTask);
        AgentTaskQueue.addTask(healthyTask);
        for (int i = 0; i < AgentTaskCleanupDaemon.MAX_FAILURE_TIMES; i++) {
            daemon.runAfterCatalogReady();
        }
        Assertions.assertTrue(AgentTaskQueue.contains(offlineTask));
        Assertions.assertFalse(offlineTask.isFinished());
        Assertions.assertTrue(transaction.hasSendTask());

        GlobalTransactionMgrIface transactionMgr = Mockito.mock(GlobalTransactionMgrIface.class);
        Mockito.doAnswer(invocation -> {
            transaction.setTransactionStatus(TransactionStatus.VISIBLE);
            return null;
        }).when(transactionMgr).finishTransaction(Mockito.eq(transaction.getDbId()),
                Mockito.eq(transaction.getTransactionId()), Mockito.anyMap(), Mockito.anyMap());

        // Exercise the publisher's existing terminal cleanup without recovering the offline BE.
        Deencapsulation.invoke(new PublishVersionDaemon(), "tryFinishTxnSync", transaction, transactionMgr);
        Assertions.assertFalse(backend.isAlive());
        Assertions.assertEquals(TransactionStatus.VISIBLE, transaction.getTransactionStatus());
        Assertions.assertTrue(AgentTaskQueue.getTask(TTaskType.PUBLISH_VERSION).isEmpty());
        Assertions.assertTrue(transaction.getPublishVersionTasks().isEmpty());
        Assertions.assertEquals(0, AgentTaskQueue.getTaskNum());
    }

    @Test
    public void testDropBackendReclaimsReportPublishAndOtherTasks() throws Exception {
        try (MockedStatic<AgentTaskExecutor> executor = Mockito.mockStatic(AgentTaskExecutor.class)) {
            Deencapsulation.invoke(ReportHandler.class, "handleRepublishVersionInfo", republishInfo(), BACKEND_ID);
        }
        PublishVersionTask task = (PublishVersionTask) AgentTaskQueue.getTask(
                BACKEND_ID, TTaskType.PUBLISH_VERSION, TRANSACTION_ID);
        Assertions.assertNotNull(task);
        for (int i = 0; i < AgentTaskCleanupDaemon.MAX_FAILURE_TIMES; i++) {
            daemon.runAfterCatalogReady();
        }
        Assertions.assertTrue(AgentTaskQueue.contains(task));

        DropReplicaTask dropTask = new DropReplicaTask(BACKEND_ID, 10L, 20L, 0, false);
        PushTask pushTask = Mockito.mock(PushTask.class);
        Mockito.when(pushTask.getBackendId()).thenReturn(BACKEND_ID);
        Mockito.when(pushTask.getTaskType()).thenReturn(TTaskType.REALTIME_PUSH);
        AgentTaskQueue.addTask(dropTask);
        AgentTaskQueue.addTask(pushTask);
        PublishVersionTask healthyTask = newPublishTask(HEALTHY_BACKEND_ID, TRANSACTION_ID);
        AgentTaskQueue.addTask(healthyTask);

        infoService.dropBackend(BACKEND_ID);
        Assertions.assertNull(infoService.getBackend(BACKEND_ID));
        Assertions.assertFalse(AgentTaskQueue.contains(task));
        Assertions.assertTrue(task.isFinished());
        Assertions.assertEquals(TStatusCode.ABORTED, task.getErrorCode());
        Assertions.assertFalse(AgentTaskQueue.contains(dropTask));
        Assertions.assertTrue(dropTask.isFinished());
        Mockito.verify(pushTask).countDownLatchWithStatus(Mockito.eq(BACKEND_ID), Mockito.eq(0L),
                Mockito.any(Status.class));
        Mockito.verify(pushTask).setFinished(true);
        Assertions.assertTrue(AgentTaskQueue.contains(healthyTask));
        Assertions.assertEquals(1, AgentTaskQueue.getTaskNum());
    }

    @Test
    public void testReplayDropBackendReclaimsPublishTask() {
        PublishVersionTask task = newPublishTask(BACKEND_ID, TRANSACTION_ID);
        AgentTaskQueue.addTask(task);
        infoService.replayDropBackend(backend);
        Assertions.assertFalse(AgentTaskQueue.contains(task));
        Assertions.assertTrue(task.isFinished());
        Assertions.assertEquals(TStatusCode.ABORTED, task.getErrorCode());
        Assertions.assertEquals(0, AgentTaskQueue.getTaskNum());
    }

    @Test
    public void testEraseDatabaseReclaimsOnlyItsPublishTasks() {
        PublishVersionTask task = newPublishTask(BACKEND_ID, TRANSACTION_ID);
        PublishVersionTask anotherBackendTask = newPublishTask(HEALTHY_BACKEND_ID, TRANSACTION_ID);
        PublishVersionTask otherDbTask = new PublishVersionTask(BACKEND_ID, TRANSACTION_ID + 1, 2L,
                List.of(new TPartitionVersionInfo(2L, 3L, 0L)), 0L);
        DropReplicaTask dropTask = new DropReplicaTask(BACKEND_ID, 10L, 20L, 0, false);
        AgentTaskQueue.addTask(task);
        AgentTaskQueue.addTask(anotherBackendTask);
        AgentTaskQueue.addTask(otherDbTask);
        AgentTaskQueue.addTask(dropTask);

        database.markDropped();
        env.eraseDatabase(1L, false);
        Mockito.verify(transactionMgr).removeDatabaseTransactionMgr(1L);
        for (PublishVersionTask removed : List.of(task, anotherBackendTask)) {
            Assertions.assertFalse(AgentTaskQueue.contains(removed));
            Assertions.assertTrue(removed.isFinished());
            Assertions.assertEquals(TStatusCode.ABORTED, removed.getErrorCode());
        }
        Assertions.assertTrue(AgentTaskQueue.contains(otherDbTask));
        Assertions.assertFalse(otherDbTask.isFinished());
        Assertions.assertTrue(AgentTaskQueue.contains(dropTask));
        Assertions.assertFalse(dropTask.isFinished());
        Assertions.assertEquals(2, AgentTaskQueue.getTaskNum());
    }

    @Test
    public void testLateReportAfterBackendDropCannotRecreateTask() throws Exception {
        CountDownLatch reportPrepared = new CountDownLatch(1);
        CountDownLatch resumeReport = new CountDownLatch(1);
        ExecutorService worker = Executors.newSingleThreadExecutor();
        Map<Long, SetMultimap<Long, TPartitionVersionInfo>> report = republishInfo();
        try {
            Future<?> future = worker.submit(() -> {
                try (MockedStatic<Env> threadEnv = mockEnv();
                        MockedStatic<AgentTaskExecutor> executor = Mockito.mockStatic(AgentTaskExecutor.class)) {
                    reportPrepared.countDown();
                    Assertions.assertTrue(resumeReport.await(10, TimeUnit.SECONDS));
                    Deencapsulation.invoke(ReportHandler.class, "handleRepublishVersionInfo", report, BACKEND_ID);
                    ArgumentCaptor<AgentBatchTask> batch = ArgumentCaptor.forClass(AgentBatchTask.class);
                    executor.verify(() -> AgentTaskExecutor.submit(batch.capture()));
                    Assertions.assertTrue(batch.getValue().getAllTasks().isEmpty());
                }
                return null;
            });
            Assertions.assertTrue(reportPrepared.await(10, TimeUnit.SECONDS));
            infoService.dropBackend(BACKEND_ID);
            resumeReport.countDown();
            future.get(10, TimeUnit.SECONDS);
            Assertions.assertEquals(0, AgentTaskQueue.getTaskNum());
        } finally {
            resumeReport.countDown();
            worker.shutdownNow();
        }
    }

    @Test
    public void testLatePublishAfterDatabaseEraseIsRejected() {
        PublishVersionTask task = newPublishTask(BACKEND_ID, TRANSACTION_ID);
        database.markDropped();
        env.eraseDatabase(1L, false);
        Assertions.assertFalse(AgentTaskQueue.addTask(task));
        Assertions.assertTrue(task.isFinished());
        Assertions.assertEquals(TStatusCode.ABORTED, task.getErrorCode());

        Mockito.when(catalog.getDbNullable(1L)).thenReturn(null);
        Assertions.assertFalse(AgentTaskQueue.addTask(newPublishTask(BACKEND_ID, TRANSACTION_ID + 1)));
        Assertions.assertEquals(0, AgentTaskQueue.getTaskNum());
    }

    @Test
    public void testDatabaseEraseReclaimsConcurrentAdmission() throws Exception {
        CountDownLatch metadataRead = new CountDownLatch(1);
        CountDownLatch resumeAdmission = new CountDownLatch(1);
        CountDownLatch databaseDropped = new CountDownLatch(1);
        Mockito.doAnswer(invocation -> {
            boolean dropped = (boolean) invocation.callRealMethod();
            metadataRead.countDown();
            Assertions.assertTrue(resumeAdmission.await(10, TimeUnit.SECONDS));
            return dropped;
        }).when(database).isDropped();
        ExecutorService workers = Executors.newFixedThreadPool(2);
        PublishVersionTask task = newPublishTask(BACKEND_ID, TRANSACTION_ID);
        try {
            Future<Boolean> admission = workers.submit(() -> {
                try (MockedStatic<Env> threadEnv = mockEnv()) {
                    return AgentTaskQueue.addTask(task);
                }
            });
            Assertions.assertTrue(metadataRead.await(10, TimeUnit.SECONDS));
            Future<?> erasure = workers.submit(() -> {
                try (MockedStatic<Env> threadEnv = mockEnv()) {
                    database.markDropped();
                    databaseDropped.countDown();
                    env.eraseDatabase(1L, false);
                }
            });
            Assertions.assertTrue(databaseDropped.await(10, TimeUnit.SECONDS));
            resumeAdmission.countDown();
            Assertions.assertTrue(admission.get(10, TimeUnit.SECONDS));
            erasure.get(10, TimeUnit.SECONDS);
            Assertions.assertFalse(AgentTaskQueue.contains(task));
            Assertions.assertTrue(task.isFinished());
            Assertions.assertEquals(TStatusCode.ABORTED, task.getErrorCode());
            Assertions.assertEquals(0, AgentTaskQueue.getTaskNum());
        } finally {
            resumeAdmission.countDown();
            workers.shutdownNow();
        }
    }

    @Test
    public void testDuplicateRepublishDispatchesNewPartitions() {
        backend.setAlive(true);
        TPartitionVersionInfo failedPartition = new TPartitionVersionInfo(2L, 3L, 0L);
        TPartitionVersionInfo newPartition = new TPartitionVersionInfo(3L, 4L, 0L);
        PublishVersionTask task = newPublishTask(BACKEND_ID, TRANSACTION_ID);
        task.setFinished(true);
        task.setErrorCode(TStatusCode.RUNTIME_ERROR);
        task.addErrorTablets(List.of(10L));
        AgentTaskQueue.addTask(task);
        SetMultimap<Long, TPartitionVersionInfo> updates = HashMultimap.create();
        updates.put(TRANSACTION_ID, failedPartition);
        updates.put(TRANSACTION_ID, newPartition);
        try (MockedStatic<AgentTaskExecutor> executor = Mockito.mockStatic(AgentTaskExecutor.class)) {
            Deencapsulation.invoke(ReportHandler.class, "handleRepublishVersionInfo", Map.of(1L, updates), BACKEND_ID);
            ArgumentCaptor<AgentBatchTask> batch = ArgumentCaptor.forClass(AgentBatchTask.class);
            executor.verify(() -> AgentTaskExecutor.submit(batch.capture()));
            Assertions.assertEquals(1, batch.getValue().getAllTasks().size());
            PublishVersionTask dispatched = (PublishVersionTask) batch.getValue().getAllTasks().get(0);
            Assertions.assertEquals(Set.of(failedPartition, newPartition),
                    Set.copyOf(dispatched.getPartitionVersionInfos()));
            Assertions.assertSame(task, AgentTaskQueue.getTask(BACKEND_ID, TTaskType.PUBLISH_VERSION, TRANSACTION_ID));
            Assertions.assertEquals(List.of(failedPartition), task.getPartitionVersionInfos());

            executor.reset();
            Deencapsulation.invoke(ReportHandler.class, "taskReport", BACKEND_ID, Collections.emptyMap(), 0L);
            executor.verify(() -> AgentTaskExecutor.submit(batch.capture()));
            Assertions.assertEquals(List.of(task), batch.getValue().getAllTasks());
        }
    }

    @Test
    public void testRemovedBackendPublishIsTrackedWithoutDispatch() throws Exception {
        infoService.dropBackend(BACKEND_ID);
        TransactionState transaction = newTransaction();
        transaction.setTransactionStatus(TransactionStatus.COMMITTED);
        AgentBatchTask batch = new AgentBatchTask();
        Deencapsulation.invoke(new PublishVersionDaemon(), "addPublishVersionTask",
                Set.of(BACKEND_ID, HEALTHY_BACKEND_ID), TRANSACTION_ID, transaction,
                List.of(new TPartitionVersionInfo(2L, 3L, 0L)), Collections.emptyMap(), 0L, batch);
        PublishVersionTask rejected = transaction.getPublishVersionTasks().get(BACKEND_ID).get(0);
        Assertions.assertTrue(rejected.isFinished());
        Assertions.assertEquals(TStatusCode.ABORTED, rejected.getErrorCode());
        Assertions.assertTrue(rejected.getSuccTablets().isEmpty());
        Assertions.assertFalse(AgentTaskQueue.contains(rejected));
        PublishVersionTask accepted = transaction.getPublishVersionTasks().get(HEALTHY_BACKEND_ID).get(0);
        Assertions.assertEquals(List.of(accepted), batch.getAllTasks());
        Assertions.assertTrue(AgentTaskQueue.contains(accepted));
    }

    private TransactionState newTransaction() {
        return new TransactionState(1L, List.of(), TRANSACTION_ID, "publish_test", null,
                TransactionState.LoadJobSourceType.FRONTEND,
                new TransactionState.TxnCoordinator(TransactionState.TxnSourceType.FE, 0, "127.0.0.1", 0L),
                -1L, 10000L);
    }

    private MockedStatic<Env> mockEnv() {
        MockedStatic<Env> result = Mockito.mockStatic(Env.class);
        result.when(Env::getCurrentEnv).thenReturn(env);
        result.when(Env::getCurrentSystemInfo).thenReturn(infoService);
        result.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
        result.when(Env::getCurrentGlobalTransactionMgr).thenReturn(transactionMgr);
        return result;
    }

    private Map<Long, SetMultimap<Long, TPartitionVersionInfo>> republishInfo() {
        SetMultimap<Long, TPartitionVersionInfo> transactions = HashMultimap.create();
        transactions.put(TRANSACTION_ID, new TPartitionVersionInfo(2L, 3L, 0L));
        return Map.of(1L, transactions);
    }

    private PublishVersionTask newPublishTask(long backendId, long transactionId) {
        return new PublishVersionTask(backendId, transactionId, 1L,
                List.of(new TPartitionVersionInfo(2L, 3L, 0L)), 0L);
    }
}
