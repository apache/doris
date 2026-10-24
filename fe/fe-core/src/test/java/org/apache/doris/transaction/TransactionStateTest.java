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

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.load.EtlStatus;
import org.apache.doris.load.FailMsg;
import org.apache.doris.load.FailMsg.CancelType;
import org.apache.doris.load.loadv2.JobState;
import org.apache.doris.load.loadv2.LoadJobFinalOperation;
import org.apache.doris.load.routineload.RLTaskTxnCommitAttachment;
import org.apache.doris.load.routineload.kafka.KafkaProgress;
import org.apache.doris.meta.MetaContext;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TEtlState;
import org.apache.doris.thrift.TKafkaRLTaskProgress;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.transaction.TransactionState.LoadJobSourceType;
import org.apache.doris.transaction.TransactionState.TxnCoordinator;
import org.apache.doris.transaction.TransactionState.TxnSourceType;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class TransactionStateTest {

    private static String fileName = "./TransactionStateTest";
    private static String fileName2 = "./TransactionStateTest2";
    private static String fileName3 = "./TransactionStateTest3";

    @AfterEach
    public void tearDown() {
        new File(fileName).delete();
        new File(fileName2).delete();
        new File(fileName3).delete();
    }

    private void testSerDe(String fileName, TransactionState transactionState, Consumer<TransactionState> checkFun)
            throws IOException {
        MetaContext metaContext = new MetaContext();
        metaContext.setMetaVersion(FeMetaVersion.VERSION_CURRENT);
        metaContext.setThreadLocalInfo();

        // 1. Write objects to file
        File file = new File(fileName);
        file.createNewFile();
        DataOutputStream out = new DataOutputStream(new FileOutputStream(file));
        transactionState.write(out);
        out.flush();
        out.close();

        // 2. Read objects from file
        DataInputStream in = new DataInputStream(new FileInputStream(file));
        TransactionState readTransactionState = TransactionState.read(in);
        checkFun.accept(readTransactionState);
        in.close();
    }

    @Test
    public void testSerDe() throws IOException {
        UUID uuid = UUID.randomUUID();
        TransactionState transactionState = new TransactionState(1000L, Lists.newArrayList(20000L, 20001L),
                3000, "label123", new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits()),
                LoadJobSourceType.BACKEND_STREAMING,
                new TxnCoordinator(TxnSourceType.BE, 0, "127.0.0.1", System.currentTimeMillis()),
                50000L, 60 * 1000L);
        testSerDe(fileName, transactionState, readTransactionState -> {
            Assertions.assertEquals(transactionState.getCoordinator().ip, readTransactionState.getCoordinator().ip);
        });
    }

    @Test
    public void testSerDeForBatchLoad() throws IOException {
        UUID uuid = UUID.randomUUID();
        // EtlStatus
        EtlStatus etlStatus = new EtlStatus();
        etlStatus.setState(TEtlState.FINISHED);
        etlStatus.setTrackingUrl("http://123");
        // FailMsg
        FailMsg failMsg = new FailMsg();
        failMsg.setCancelType(CancelType.LOAD_RUN_FAIL);
        failMsg.setMsg("load run fail");
        // LoadJobFinalOperation
        LoadJobFinalOperation loadJobFinalOperation = new LoadJobFinalOperation(1000L, etlStatus, 0, 0, 0,
                JobState.FINISHED, failMsg);
        // TransactionState
        TransactionState transactionState = new TransactionState(1000L, Lists.newArrayList(20000L, 20001L), 3000,
                "label123", new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits()),
                LoadJobSourceType.BACKEND_STREAMING,
                new TxnCoordinator(TxnSourceType.BE, 0, "127.0.0.1", System.currentTimeMillis()),
                TransactionStatus.COMMITTED, "", 100, 50000L, loadJobFinalOperation, 100, 200, 300, 400);
        // check
        testSerDe(fileName2, transactionState, readTransactionState -> {
            Assertions.assertEquals(TransactionState.LoadJobSourceType.BATCH_LOAD_JOB,
                    readTransactionState.getTxnCommitAttachment().sourceType);
            Assertions.assertTrue(readTransactionState.getTxnCommitAttachment() instanceof LoadJobFinalOperation);
            LoadJobFinalOperation readLoadJobFinalOperation
                    = (LoadJobFinalOperation) (readTransactionState.getTxnCommitAttachment());
            Assertions.assertEquals(loadJobFinalOperation.getId(), readLoadJobFinalOperation.getId());
            EtlStatus readLoadingStatus = readLoadJobFinalOperation.getLoadingStatus();
            Assertions.assertEquals(TEtlState.FINISHED, readLoadingStatus.getState());
            Assertions.assertEquals(etlStatus.getTrackingUrl(), readLoadingStatus.getTrackingUrl());
            FailMsg readFailMsg = readLoadJobFinalOperation.getFailMsg();
            Assertions.assertEquals(failMsg.getCancelType(), readFailMsg.getCancelType());
            Assertions.assertEquals(failMsg.getMsg(), readFailMsg.getMsg());
        });
    }

    @Test
    public void testSerDeForRoutineLoad() throws IOException {
        UUID uuid = UUID.randomUUID();
        // create a RLTaskTxnCommitAttachment
        RLTaskTxnCommitAttachment attachment = new RLTaskTxnCommitAttachment();
        TKafkaRLTaskProgress tKafkaRLTaskProgress = new TKafkaRLTaskProgress();
        tKafkaRLTaskProgress.partitionCmtOffset = Maps.newHashMap();
        tKafkaRLTaskProgress.partitionCmtOffset.put(1, 100L);
        KafkaProgress kafkaProgress = new KafkaProgress(tKafkaRLTaskProgress);
        Deencapsulation.setField(attachment, "progress", kafkaProgress);
        // TransactionState
        TransactionState transactionState = new TransactionState(1000L, Lists.newArrayList(20000L, 20001L),
                3000, "label123", new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits()),
                LoadJobSourceType.BACKEND_STREAMING,
                new TxnCoordinator(TxnSourceType.BE, 0, "127.0.0.1", System.currentTimeMillis()),
                TransactionStatus.COMMITTED, "", 100, 50000L,
                attachment, 100, 200, 300, 400);
        // check
        testSerDe(fileName3, transactionState, readTransactionState -> {
            Assertions.assertEquals(TransactionState.LoadJobSourceType.ROUTINE_LOAD_TASK,
                    readTransactionState.getTxnCommitAttachment().sourceType);
            Assertions.assertTrue(readTransactionState.getTxnCommitAttachment() instanceof RLTaskTxnCommitAttachment);
            RLTaskTxnCommitAttachment readRLTaskTxnCommitAttachment
                    = (RLTaskTxnCommitAttachment) (readTransactionState.getTxnCommitAttachment());
            Assertions.assertTrue(readRLTaskTxnCommitAttachment.getProgress() instanceof KafkaProgress);
            KafkaProgress readKafkaProgress = (KafkaProgress) (readRLTaskTxnCommitAttachment.getProgress());
            Assertions.assertEquals(1, readKafkaProgress.getOffsetByPartition().size());
            Assertions.assertEquals(100L, (long) readKafkaProgress.getOffsetByPartition().getOrDefault(1, -1L));
        });
    }

    // Commit and finish serialize the state for the edit log while they hold its monitor. The writers that
    // change it later without any lock of their own (PublishVersionDaemon, the version check of finish)
    // must wait for that monitor.
    @Test
    public void testPublishUpdatesWaitForMonitor() throws Exception {
        TransactionState transactionState = newTransactionState();
        Map<Long, Map<Long, Long>> deltaRows = Maps.newHashMap();
        deltaRows.put(20000L, Maps.newHashMap());
        deltaRows.get(20000L).put(30000L, 10L);

        Thread setter = new Thread(() -> transactionState.setTableIdToTabletDeltaRows(deltaRows));
        synchronized (transactionState) {
            setter.start();
            waitUntilBlocked(setter);
            Assertions.assertTrue(transactionState.getTableIdToTabletDeltaRows().isEmpty());
        }
        setter.join(TimeUnit.SECONDS.toMillis(30));
        Assertions.assertEquals(deltaRows, transactionState.getTableIdToTabletDeltaRows());

        Thread pruner = new Thread(transactionState::pruneAfterVisible);
        synchronized (transactionState) {
            pruner.start();
            waitUntilBlocked(pruner);
            Assertions.assertFalse(transactionState.getTableIdToTabletDeltaRows().isEmpty());
        }
        pruner.join(TimeUnit.SECONDS.toMillis(30));
        Assertions.assertTrue(transactionState.getTableIdToTabletDeltaRows().isEmpty());
    }

    @Test
    public void testFinishCheckPartitionVersionWaitsForMonitor() throws Exception {
        DatabaseTransactionMgr dbTransactionMgr = new DatabaseTransactionMgr(1000L, Mockito.mock(Env.class), null);
        // The mocked database has no tables, so the check removes the commit info of a dropped table.
        Database db = Mockito.mock(Database.class);

        TransactionState transactionState = newTransactionState();
        transactionState.putIdToTableCommitInfo(20000L, new TableCommitInfo(20000L));
        Method check = DatabaseTransactionMgr.class.getDeclaredMethod("finishCheckPartitionVersion",
                TransactionState.class, Database.class, List.class);
        check.setAccessible(true);
        Thread checker = new Thread(() -> invoke(check, dbTransactionMgr, transactionState, db, new ArrayList<>()));
        synchronized (transactionState) {
            checker.start();
            waitUntilBlocked(checker);
            Assertions.assertEquals(1, transactionState.getIdToTableCommitInfos().size());
        }
        checker.join(TimeUnit.SECONDS.toMillis(30));
        Assertions.assertTrue(transactionState.getIdToTableCommitInfos().isEmpty());

        TransactionState subTxnState = newTransactionState();
        subTxnState.setSubTxnIds(Lists.newArrayList(3001L));
        subTxnState.getSubTxnIdToTableCommitInfo().put(3001L, new TableCommitInfo(20000L));
        Method subTxnCheck = DatabaseTransactionMgr.class.getDeclaredMethod(
                "finishCheckPartitionVersionWithSubTxns", TransactionState.class, Database.class);
        subTxnCheck.setAccessible(true);
        Thread subTxnChecker = new Thread(() -> invoke(subTxnCheck, dbTransactionMgr, subTxnState, db));
        synchronized (subTxnState) {
            subTxnChecker.start();
            waitUntilBlocked(subTxnChecker);
            Assertions.assertEquals(1, subTxnState.getSubTxnIds().size());
        }
        subTxnChecker.join(TimeUnit.SECONDS.toMillis(30));
        Assertions.assertTrue(subTxnState.getSubTxnIds().isEmpty());
    }

    // A publish executor finishes a transaction while it holds the monitor to serialize the state for the edit log.
    // The next round of PublishVersionDaemon must skip that transaction instead of waiting for the monitor to set
    // its delta rows again, which would hold up the publish of every other transaction.
    @Test
    public void testPublishDaemonSkipsTransactionBeingFinished() throws Exception {
        PublishVersionDaemon daemon = new PublishVersionDaemon();
        TransactionState transactionState = newTransactionState();
        Map<Long, Map<Long, Long>> deltaRows = Maps.newHashMap();
        deltaRows.put(20000L, Maps.newHashMap());
        deltaRows.get(20000L).put(30000L, 10L);
        transactionState.setTableIdToTabletDeltaRows(deltaRows);
        Set<Long> publishingTxnIds = Deencapsulation.getField(daemon, "publishingTxnIds");
        publishingTxnIds.add(transactionState.getTransactionId());
        Method tryFinish = PublishVersionDaemon.class.getDeclaredMethod("tryFinishOneTxn", TransactionState.class,
                SystemInfoService.class, GlobalTransactionMgrIface.class);
        tryFinish.setAccessible(true);

        Thread round = new Thread(() -> invoke(tryFinish, daemon, transactionState,
                Mockito.mock(SystemInfoService.class), Mockito.mock(GlobalTransactionMgrIface.class)));
        synchronized (transactionState) {
            round.start();
            long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(30);
            while (round.getState() != Thread.State.TERMINATED) {
                Assertions.assertNotEquals(Thread.State.BLOCKED, round.getState(),
                        "the daemon waited for the monitor of a transaction that is being finished");
                Assertions.assertTrue(System.currentTimeMillis() < deadline, "thread state: " + round.getState());
                Thread.sleep(10);
            }
        }
        Assertions.assertEquals(deltaRows, transactionState.getTableIdToTabletDeltaRows());
    }

    private static TransactionState newTransactionState() {
        return new TransactionState(1000L, Lists.newArrayList(20000L), 3000, "label123", new TUniqueId(1L, 2L),
                LoadJobSourceType.BACKEND_STREAMING,
                new TxnCoordinator(TxnSourceType.BE, 0, "127.0.0.1", System.currentTimeMillis()),
                -1L, 60 * 1000L);
    }

    private static void invoke(Method method, Object target, Object... args) {
        try {
            method.invoke(target, args);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    private static void waitUntilBlocked(Thread thread) throws InterruptedException {
        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(30);
        while (thread.getState() != Thread.State.BLOCKED) {
            Assertions.assertNotEquals(Thread.State.TERMINATED, thread.getState(),
                    "the update did not wait for the monitor of the transaction state");
            Assertions.assertTrue(System.currentTimeMillis() < deadline, "thread state: " + thread.getState());
            Thread.sleep(10);
        }
    }
}
