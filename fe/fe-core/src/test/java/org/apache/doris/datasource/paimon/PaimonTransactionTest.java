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

package org.apache.doris.datasource.paimon;

import org.apache.doris.common.UserException;
import org.apache.doris.thrift.TPaimonCommitMessage;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.io.DataOutputSerializer;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class PaimonTransactionTest {

    @Test
    public void testUpdateCommitMessagesSkipsInvalidAndDeduplicates() throws Exception {
        byte[] payload = serializePayload(Collections.singletonList(newCommitMessage(7)));
        PaimonTransaction transaction = new PaimonTransaction(new RecordingCommitExecutor());

        TPaimonCommitMessage emptyPayload = new TPaimonCommitMessage();
        emptyPayload.setPayload(new byte[0]);
        transaction.updateCommitMessages(Arrays.asList(
                null,
                new TPaimonCommitMessage(),
                emptyPayload,
                newThriftMessage(payload),
                newThriftMessage(Arrays.copyOf(payload, payload.length))));

        Assert.assertEquals(1, transaction.getCommitPayloadCount());
    }

    @Test
    public void testDeserializeCommitMessagePayload() throws Exception {
        byte[] payload = serializePayload(Collections.singletonList(newCommitMessage(11)));

        List<CommitMessage> messages = PaimonTransaction.deserializeCommitMessagePayload(payload);

        Assert.assertEquals(1, messages.size());
        Assert.assertEquals(11, messages.get(0).bucket());
    }

    @Test
    public void testDeserializeCommitMessagePayloadRejectsInvalidHeader() {
        IOException exception = Assert.assertThrows(IOException.class,
                () -> PaimonTransaction.deserializeCommitMessagePayload(new byte[] {'B', 'A', 'D'}));
        Assert.assertTrue(exception.getMessage().contains("header"));
    }

    @Test
    public void testDeserializeCommitMessagePayloadRejectsInvalidLength() {
        byte[] payload = new byte[] {'D', 'P', 'C', 'M', 0, 0, 0, 1, 0, 0, 0, 2, 1};

        IOException exception = Assert.assertThrows(IOException.class,
                () -> PaimonTransaction.deserializeCommitMessagePayload(payload));
        Assert.assertTrue(exception.getMessage().contains("length"));
    }

    @Test
    public void testCommitUsesDecodedMessagesAndMarksCommitted() throws Exception {
        RecordingCommitExecutor executor = new RecordingCommitExecutor();
        PaimonTransaction transaction = new PaimonTransaction(executor);
        PaimonExternalTable table = mockTable();
        transaction.beginInsert(table, Optional.empty());
        transaction.setTransactionId(42L);
        transaction.updateCommitMessages(Collections.singletonList(
                newThriftMessage(serializePayload(Collections.singletonList(newCommitMessage(3))))));

        transaction.commit();

        Assert.assertTrue(transaction.isCommitted());
        Assert.assertEquals(1, executor.commitCalls);
        Assert.assertEquals(0, executor.abortCalls);
        Assert.assertSame(table, executor.commitTable);
        Assert.assertEquals("doris_txn_42", executor.commitUser);
        Assert.assertEquals(42L, executor.transactionId);
        Assert.assertEquals(1, executor.committedMessages.size());
        Assert.assertEquals(3, executor.committedMessages.get(0).bucket());
    }

    @Test
    public void testCommitWithEmptyPayloadIsNoop() throws Exception {
        RecordingCommitExecutor executor = new RecordingCommitExecutor();
        PaimonTransaction transaction = new PaimonTransaction(executor);
        transaction.commit();

        Assert.assertFalse(transaction.isCommitted());
        Assert.assertEquals(0, executor.commitCalls);
    }

    @Test
    public void testRollbackAbortsDecodedMessages() throws Exception {
        RecordingCommitExecutor executor = new RecordingCommitExecutor();
        PaimonTransaction transaction = new PaimonTransaction(executor);
        PaimonExternalTable table = mockTable();
        transaction.beginInsert(table, Optional.empty());
        transaction.setTransactionId(43L);
        transaction.updateCommitMessages(Collections.singletonList(
                newThriftMessage(serializePayload(Collections.singletonList(newCommitMessage(5))))));

        transaction.rollback();

        Assert.assertFalse(transaction.isCommitted());
        Assert.assertEquals(0, executor.commitCalls);
        Assert.assertEquals(1, executor.abortCalls);
        Assert.assertSame(table, executor.abortTable);
        Assert.assertEquals("doris_txn_43", executor.abortUser);
        Assert.assertEquals(1, executor.abortedMessages.size());
        Assert.assertEquals(5, executor.abortedMessages.get(0).bucket());
    }

    @Test
    public void testRollbackAfterCommitIsNoop() throws Exception {
        RecordingCommitExecutor executor = new RecordingCommitExecutor();
        PaimonTransaction transaction = new PaimonTransaction(executor);
        transaction.beginInsert(mockTable(), Optional.empty());
        transaction.setTransactionId(44L);
        transaction.updateCommitMessages(Collections.singletonList(
                newThriftMessage(serializePayload(Collections.singletonList(newCommitMessage(6))))));

        transaction.commit();
        transaction.rollback();

        Assert.assertTrue(transaction.isCommitted());
        Assert.assertEquals(1, executor.commitCalls);
        Assert.assertEquals(0, executor.abortCalls);
    }

    @Test
    public void testRollbackDoesNotThrowWhenAbortFails() throws Exception {
        RecordingCommitExecutor executor = new RecordingCommitExecutor();
        executor.failAbort = true;
        PaimonTransaction transaction = new PaimonTransaction(executor);
        transaction.beginInsert(mockTable(), Optional.empty());
        transaction.setTransactionId(45L);
        transaction.updateCommitMessages(Collections.singletonList(
                newThriftMessage(serializePayload(Collections.singletonList(newCommitMessage(8))))));

        transaction.rollback();

        Assert.assertEquals(1, executor.abortCalls);
    }

    @Test
    public void testCommitWrapsExecutorFailure() throws Exception {
        RecordingCommitExecutor executor = new RecordingCommitExecutor();
        executor.failCommit = true;
        PaimonTransaction transaction = new PaimonTransaction(executor);
        transaction.beginInsert(mockTable(), Optional.empty());
        transaction.setTransactionId(46L);
        transaction.updateCommitMessages(Collections.singletonList(
                newThriftMessage(serializePayload(Collections.singletonList(newCommitMessage(9))))));

        UserException exception = Assert.assertThrows(UserException.class, transaction::commit);

        Assert.assertFalse(transaction.isCommitted());
        Assert.assertTrue(exception.getMessage().contains("Failed to commit paimon transaction"));
    }

    private static PaimonExternalTable mockTable() {
        PaimonExternalTable table = Mockito.mock(PaimonExternalTable.class);
        Mockito.when(table.getName()).thenReturn("mock_paimon_table");
        return table;
    }

    private static TPaimonCommitMessage newThriftMessage(byte[] payload) {
        TPaimonCommitMessage message = new TPaimonCommitMessage();
        message.setPayload(payload);
        return message;
    }

    private static CommitMessage newCommitMessage(int bucket) {
        return new CommitMessageImpl(BinaryRow.EMPTY_ROW, bucket, null,
                DataIncrement.emptyIncrement(), CompactIncrement.emptyIncrement());
    }

    private static byte[] serializePayload(List<CommitMessage> messages) throws IOException {
        CommitMessageSerializer serializer = new CommitMessageSerializer();
        DataOutputSerializer output = new DataOutputSerializer(1024);
        serializer.serializeList(messages, output);
        byte[] data = output.getCopyOfBuffer();
        byte[] payload = new byte[12 + data.length];
        payload[0] = 'D';
        payload[1] = 'P';
        payload[2] = 'C';
        payload[3] = 'M';
        writeInt(payload, 4, serializer.getVersion());
        writeInt(payload, 8, data.length);
        System.arraycopy(data, 0, payload, 12, data.length);
        return payload;
    }

    private static void writeInt(byte[] payload, int offset, int value) {
        payload[offset] = (byte) ((value >>> 24) & 0xFF);
        payload[offset + 1] = (byte) ((value >>> 16) & 0xFF);
        payload[offset + 2] = (byte) ((value >>> 8) & 0xFF);
        payload[offset + 3] = (byte) (value & 0xFF);
    }

    private static class RecordingCommitExecutor implements PaimonTransaction.CommitExecutor {
        private int commitCalls;
        private int abortCalls;
        private boolean failCommit;
        private boolean failAbort;
        private PaimonExternalTable commitTable;
        private PaimonExternalTable abortTable;
        private String commitUser;
        private String abortUser;
        private long transactionId;
        private List<CommitMessage> committedMessages = Collections.emptyList();
        private List<CommitMessage> abortedMessages = Collections.emptyList();

        @Override
        public void commit(PaimonExternalTable table, String commitUser, long transactionId,
                List<CommitMessage> commitMessages) throws Exception {
            commitCalls++;
            if (failCommit) {
                throw new IOException("commit failed");
            }
            this.commitTable = table;
            this.commitUser = commitUser;
            this.transactionId = transactionId;
            this.committedMessages = new ArrayList<>(commitMessages);
        }

        @Override
        public void abort(PaimonExternalTable table, String commitUser,
                List<CommitMessage> commitMessages) throws Exception {
            abortCalls++;
            if (failAbort) {
                throw new IOException("abort failed");
            }
            this.abortTable = table;
            this.abortUser = commitUser;
            this.abortedMessages = new ArrayList<>(commitMessages);
        }
    }
}
