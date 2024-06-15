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

import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.load.EtlStatus;
import org.apache.doris.load.FailMsg;
import org.apache.doris.load.FailMsg.CancelType;
import org.apache.doris.load.loadv2.JobState;
import org.apache.doris.load.loadv2.LoadJobFinalOperation;
import org.apache.doris.load.routineload.KafkaProgress;
import org.apache.doris.load.routineload.RLTaskTxnCommitAttachment;
import org.apache.doris.meta.MetaContext;
import org.apache.doris.thrift.TEtlState;
import org.apache.doris.thrift.TKafkaRLTaskProgress;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.transaction.TransactionState.LoadJobSourceType;
import org.apache.doris.transaction.TransactionState.TxnCoordinator;
import org.apache.doris.transaction.TransactionState.TxnSourceType;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.UUID;
import java.util.function.Consumer;

public class TransactionStateTest {

    private static String fileName = "./TransactionStateTest";
    private static String fileName2 = "./TransactionStateTest2";
    private static String fileName3 = "./TransactionStateTest3";

    @After
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
            Assert.assertEquals(transactionState.getCoordinator().ip, readTransactionState.getCoordinator().ip);
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
            Assert.assertEquals(TransactionState.LoadJobSourceType.BATCH_LOAD_JOB,
                    readTransactionState.getTxnCommitAttachment().sourceType);
            Assert.assertTrue(readTransactionState.getTxnCommitAttachment() instanceof LoadJobFinalOperation);
            LoadJobFinalOperation readLoadJobFinalOperation
                    = (LoadJobFinalOperation) (readTransactionState.getTxnCommitAttachment());
            Assert.assertEquals(loadJobFinalOperation.getId(), readLoadJobFinalOperation.getId());
            EtlStatus readLoadingStatus = readLoadJobFinalOperation.getLoadingStatus();
            Assert.assertEquals(TEtlState.FINISHED, readLoadingStatus.getState());
            Assert.assertEquals(etlStatus.getTrackingUrl(), readLoadingStatus.getTrackingUrl());
            FailMsg readFailMsg = readLoadJobFinalOperation.getFailMsg();
            Assert.assertEquals(failMsg.getCancelType(), readFailMsg.getCancelType());
            Assert.assertEquals(failMsg.getMsg(), readFailMsg.getMsg());
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
            Assert.assertEquals(TransactionState.LoadJobSourceType.ROUTINE_LOAD_TASK,
                    readTransactionState.getTxnCommitAttachment().sourceType);
            Assert.assertTrue(readTransactionState.getTxnCommitAttachment() instanceof RLTaskTxnCommitAttachment);
            RLTaskTxnCommitAttachment readRLTaskTxnCommitAttachment
                    = (RLTaskTxnCommitAttachment) (readTransactionState.getTxnCommitAttachment());
            Assert.assertTrue(readRLTaskTxnCommitAttachment.getProgress() instanceof KafkaProgress);
            KafkaProgress readKafkaProgress = (KafkaProgress) (readRLTaskTxnCommitAttachment.getProgress());
            Assert.assertEquals(1, readKafkaProgress.getOffsetByPartition().size());
            Assert.assertEquals(100L, (long) readKafkaProgress.getOffsetByPartition().getOrDefault(1, -1L));
        });
    }
}
