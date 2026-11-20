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

package org.apache.doris.transaction;

import org.apache.doris.common.Config;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.journal.JournalBatch;
import org.apache.doris.journal.JournalEntity;
import org.apache.doris.meta.MetaContext;
import org.apache.doris.persist.OperationType;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.task.PublishVersionTask;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.transaction.TransactionState.LoadJobSourceType;
import org.apache.doris.transaction.TransactionState.TxnCoordinator;
import org.apache.doris.transaction.TransactionState.TxnSourceType;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class PublishVersionDaemonTest {
    private boolean originalParallelPublish;
    private int originalPoolSize;
    private Field executorsField;
    private Object originalExecutors;
    private List<ExecutorService> createdExecutors = new ArrayList<>();
    private ExecutorService executor;
    private PublishVersionDaemon daemon;

    @BeforeEach
    public void setUp() throws Exception {
        originalParallelPublish = Config.enable_parallel_publish_version;
        originalPoolSize = Config.publish_thread_pool_num;
        executorsField = PublishVersionDaemon.class.getDeclaredField("publishExecutors");
        executorsField.setAccessible(true);
        originalExecutors = executorsField.get(null);
        Config.enable_parallel_publish_version = true;
        Config.publish_thread_pool_num = 1;
        executorsField.set(null, createdExecutors);
        daemon = new PublishVersionDaemon();
        executor = Mockito.mock(ExecutorService.class);
        executorsField.set(null, Lists.newArrayList(executor));
    }

    @AfterEach
    public void tearDown() throws Exception {
        createdExecutors.forEach(ExecutorService::shutdownNow);
        executorsField.set(null, originalExecutors);
        Config.enable_parallel_publish_version = originalParallelPublish;
        Config.publish_thread_pool_num = originalPoolSize;
        MetaContext.remove();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testDaemonDoesNotWaitForCommittingTransaction(boolean taskFinished) throws Exception {
        TransactionState state = newTransactionState();
        state.setSendedTask();
        PublishVersionTask task = newTask(state, 1L, 10L);
        task.setFinished(taskFinished);
        SystemInfoService infoService = Mockito.mock(SystemInfoService.class);
        Mockito.when(infoService.checkBackendAlive(1L)).thenReturn(true);
        GlobalTransactionMgrIface transactionMgr = Mockito.mock(GlobalTransactionMgrIface.class);
        FutureTask<Void> round = new FutureTask<>(() -> {
            tryFinish(state, infoService, transactionMgr);
            return null;
        });
        Thread thread = new Thread(round);
        try {
            // A commit exposes COMMITTED before journal durability and can retain this monitor while
            // waiting for timestamp I/O or synchronous persistence. There is no publishing ID yet.
            synchronized (state) {
                thread.start();
                round.get(5, TimeUnit.SECONDS);
                Assertions.assertTrue(state.getTableIdToTabletDeltaRows().isEmpty());
            }
        } finally {
            thread.join(TimeUnit.SECONDS.toMillis(30));
        }
        Assertions.assertFalse(thread.isAlive());
        Mockito.verify(executor, Mockito.times(taskFinished ? 1 : 0)).execute(Mockito.any(Runnable.class));
        Mockito.verifyNoInteractions(transactionMgr);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testFinishCollectsLatestTaskDeltaRows(boolean parallel) throws Exception {
        Config.enable_parallel_publish_version = parallel;
        TransactionState state = newTransactionState();
        PublishVersionTask first = newTask(state, 1L, 10L);
        PublishVersionTask second = newTask(state, 2L, 20L);
        PublishVersionTask failed = newTask(state, 3L, 100L);
        failed.addErrorTablets(Lists.newArrayList(30000L));
        newTask(state, 4L, 1000L); // An unfinished response must not contribute delta rows.
        SystemInfoService infoService = Mockito.mock(SystemInfoService.class);
        Mockito.when(infoService.checkBackendAlive(Mockito.anyLong())).thenReturn(true);
        GlobalTransactionMgrIface transactionMgr = Mockito.mock(GlobalTransactionMgrIface.class);
        AtomicReference<TransactionState> persisted = new AtomicReference<>();
        long dbId = state.getDbId();
        long transactionId = state.getTransactionId();
        Mockito.doAnswer(invocation -> {
            state.setTransactionStatus(TransactionStatus.VISIBLE);
            synchronized (state) {
                byte[] bytes = JournalBatch.serialize(OperationType.OP_UPSERT_TRANSACTION_STATE, state).getBinaryData();
                JournalEntity entity = new JournalEntity();
                entity.readFields(new DataInputStream(new ByteArrayInputStream(bytes)));
                persisted.set((TransactionState) entity.getData());
            }
            return null;
        }).when(transactionMgr).finishTransaction(Mockito.eq(dbId), Mockito.eq(transactionId),
                Mockito.anyMap(), Mockito.anyMap());
        MetaContext context = new MetaContext();
        context.setMetaVersion(FeMetaVersion.VERSION_CURRENT);
        context.setThreadLocalInfo();

        if (parallel) {
            // The timed-out attempt is queued while no response has finished.
            tryFinish(state, infoService, transactionMgr);
            Mockito.verifyNoInteractions(transactionMgr);
        }
        first.setFinished(true);
        second.setFinished(true);
        failed.setFinished(true);
        if (parallel) {
            // A later daemon round skips the queued attempt. The worker must refresh its own deltas.
            tryFinish(state, infoService, transactionMgr);
            ArgumentCaptor<Runnable> job = ArgumentCaptor.forClass(Runnable.class);
            Mockito.verify(executor).execute(job.capture());
            job.getValue().run();
        } else {
            tryFinish(state, infoService, transactionMgr);
        }
        Assertions.assertNotNull(persisted.get());
        Assertions.assertEquals(TransactionStatus.VISIBLE, persisted.get().getTransactionStatus());
        Assertions.assertEquals(Map.of(20000L, Map.of(30000L, 30L)),
                persisted.get().getTableIdToTabletDeltaRows());
        Assertions.assertTrue(state.getTableIdToTabletDeltaRows().isEmpty());
        Assertions.assertTrue(state.getPublishVersionTasks().isEmpty());
    }

    private static TransactionState newTransactionState() {
        TransactionState state = new TransactionState(1000L, Lists.newArrayList(20000L),
                3000L, "label_publish", new TUniqueId(1L, 2L), LoadJobSourceType.BACKEND_STREAMING,
                new TxnCoordinator(TxnSourceType.BE, 0, "127.0.0.1", System.currentTimeMillis()), -1L, 60000L);
        state.setTransactionStatus(TransactionStatus.COMMITTED);
        return state;
    }

    private static PublishVersionTask newTask(TransactionState state, long backendId, long rows) {
        PublishVersionTask task = new PublishVersionTask(backendId, state.getTransactionId(), state.getDbId(),
                null, System.currentTimeMillis());
        Map<Long, Map<Long, Long>> deltas = Maps.newHashMap();
        deltas.put(20000L, Map.of(30000L, rows));
        task.setTableIdTabletsDeltaRows(deltas);
        state.addPublishVersionTask(backendId, task);
        return task;
    }

    private void tryFinish(TransactionState state, SystemInfoService infoService,
            GlobalTransactionMgrIface transactionMgr) throws Exception {
        Method method = PublishVersionDaemon.class.getDeclaredMethod("tryFinishOneTxn", TransactionState.class,
                SystemInfoService.class, GlobalTransactionMgrIface.class);
        method.setAccessible(true);
        method.invoke(daemon, state, infoService, transactionMgr);
    }
}
