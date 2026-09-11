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

import org.apache.doris.common.AnalysisException;
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
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.thrift.TEtlState;
import org.apache.doris.thrift.TKafkaRLTaskProgress;
import org.apache.doris.thrift.TOlapTableIndexSchema;
import org.apache.doris.thrift.TOlapTableSchemaParam;
import org.apache.doris.thrift.TRowBinlogWriteColumnMapping;
import org.apache.doris.thrift.TRowBinlogWriteColumnMappings;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.transaction.TransactionState.LoadJobSourceType;
import org.apache.doris.transaction.TransactionState.TxnCoordinator;
import org.apache.doris.transaction.TransactionState.TxnSourceType;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.JsonParser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;

public class TransactionStateTest {

    private static String fileName = "./TransactionStateTest";
    private static String fileName2 = "./TransactionStateTest2";
    private static String fileName3 = "./TransactionStateTest3";

    @Test
    public void testRowBinlogSnapshotsAreIndependentOfWriterAndPublishRequests() throws Exception {
        TransactionState state = new TransactionState();
        TRowBinlogWriteColumnMapping key = new TRowBinlogWriteColumnMapping(1, 11);
        TRowBinlogWriteColumnMapping value = new TRowBinlogWriteColumnMapping(2, 12)
                .setBeforeColumnUniqueId(22);
        TOlapTableIndexSchema index = new TOlapTableIndexSchema(10L, Collections.emptyList(), 1)
                .setRowBinlogId(20L).setRowBinlogNeedHistoricalValue(true)
                .setRowBinlogColumnMappings(Arrays.asList(key, value));
        TOlapTableSchemaParam schema = new TOlapTableSchemaParam().setIndexes(Collections.singletonList(index));
        state.captureRowBinlogColumnMappings(100L, schema);
        state.captureRowBinlogColumnMappings(100L, schema); // Identical retries are allowed.
        value.setCurrentColumnUniqueId(99);
        Assertions.assertThrows(AnalysisException.class, () -> state.captureRowBinlogColumnMappings(100L, schema));
        state.captureRowBinlogColumnMappings(101L, schema); // Separate subtransaction, separate schema snapshot.

        testSerDe(fileName, state, restored -> {
            Map<Long, TRowBinlogWriteColumnMappings> published = restored.getRowBinlogColumnMappings(100L);
            TRowBinlogWriteColumnMappings mapping = published.get(10L);
            Assertions.assertTrue(mapping.isSetNeedHistoricalValue());
            Assertions.assertTrue(mapping.isNeedHistoricalValue());
            Assertions.assertEquals(2, mapping.getEntriesSize());
            Assertions.assertFalse(mapping.getEntries().get(0).isSetBeforeColumnUniqueId());
            Assertions.assertEquals(12, mapping.getEntries().get(1).getCurrentColumnUniqueId());
            Assertions.assertEquals(22, mapping.getEntries().get(1).getBeforeColumnUniqueId());
            mapping.getEntries().clear();
            published.clear();
            Assertions.assertEquals(2, restored.getRowBinlogColumnMappings(100L).get(10L).getEntriesSize());
            Assertions.assertEquals(99, restored.getRowBinlogColumnMappings(101L).get(10L)
                    .getEntries().get(1).getCurrentColumnUniqueId());
            Assertions.assertTrue(restored.getRowBinlogColumnMappings(102L).isEmpty());
        });
    }

    @Test
    public void testRowBinlogSnapshotSurvivesTransactionReplay() throws IOException {
        // Key-only historical and a later subtransaction must retain distinct write-time snapshots.
        String snapshots = "{\"100\":{\"10\":{\"historical\":true,\"columns\":["
                + "{\"source\":1,\"current\":11}]}},"
                + "\"101\":{\"10\":{\"historical\":false,\"columns\":["
                + "{\"source\":1,\"current\":11},{\"source\":2,\"current\":12}]}}}";
        TransactionState state = GsonUtils.GSON.fromJson(
                "{\"txnId\":100,\"rowBinlogMappings\":" + snapshots + "}", TransactionState.class);
        testSerDe(fileName, state, restored -> Assertions.assertEquals(JsonParser.parseString(snapshots),
                JsonParser.parseString(restored.toJson()).getAsJsonObject().get("rowBinlogMappings")));
    }

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
}
