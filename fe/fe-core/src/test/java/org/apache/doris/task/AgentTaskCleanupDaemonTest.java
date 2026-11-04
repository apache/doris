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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.master.ReportHandler;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TPartitionVersionInfo;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TTaskType;
import org.apache.doris.transaction.GlobalTransactionMgrIface;
import org.apache.doris.transaction.PublishVersionDaemon;
import org.apache.doris.transaction.TransactionState;
import org.apache.doris.transaction.TransactionStatus;

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

public class AgentTaskCleanupDaemonTest {
    private static final long BACKEND_ID = 10001L;
    private static final long HEALTHY_BACKEND_ID = 10002L;
    private static final long TRANSACTION_ID = 99L;

    private MockedStatic<Env> mockedEnv;
    private Backend backend;
    private AgentTaskCleanupDaemon daemon;

    @BeforeEach
    public void setUp() {
        MetricRepo.init();
        AgentTaskQueue.clearAllTasks();
        backend = new Backend(BACKEND_ID, "127.0.0.1", 9050);
        backend.setAlive(false);
        Backend healthyBackend = new Backend(HEALTHY_BACKEND_ID, "127.0.0.2", 9050);
        healthyBackend.setAlive(true);
        SystemInfoService infoService = Mockito.mock(SystemInfoService.class);
        Mockito.when(infoService.getAllClusterBackends(false)).thenReturn(List.of(backend, healthyBackend));
        Mockito.when(infoService.getBackend(BACKEND_ID)).thenReturn(backend);
        mockedEnv = Mockito.mockStatic(Env.class);
        mockedEnv.when(Env::getCurrentSystemInfo).thenReturn(infoService);
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
        TransactionState transaction = new TransactionState();
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

    private PublishVersionTask newPublishTask(long backendId, long transactionId) {
        return new PublishVersionTask(backendId, transactionId, 1L,
                List.of(new TPartitionVersionInfo(2L, 3L, 0L)), 0L);
    }
}
