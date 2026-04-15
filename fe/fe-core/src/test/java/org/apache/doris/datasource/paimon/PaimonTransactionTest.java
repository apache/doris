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

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.common.UserException;
import org.apache.doris.common.security.authentication.ExecutionAuthenticator;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TCommitMessage;

import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.paimon.io.DataInputView;
import org.apache.paimon.io.DataOutputViewStreamWrapper;
import org.apache.paimon.table.InnerTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.apache.paimon.table.sink.InnerTableCommit;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class PaimonTransactionTest {

    @After
    public void tearDown() {
        ConnectContext.remove();
    }

    @Test
    public void testCommitWithoutMessagesNoop(@Mocked PaimonMetadataOps ops) throws Exception {
        PaimonTransaction transaction = new PaimonTransaction(ops);
        transaction.commit();
    }

    @Test
    public void testDeserializePrefixedEmptyPayload() throws Exception {
        CommitMessageSerializer serializer = new CommitMessageSerializer();
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(output);
        serializer.serializeList(Collections.emptyList(), view);
        byte[] raw = output.toByteArray();
        ByteBuffer buffer = ByteBuffer.allocate(12 + raw.length);
        buffer.put((byte) 'D').put((byte) 'P').put((byte) 'C').put((byte) 'M');
        buffer.putInt(serializer.getVersion());
        buffer.putInt(raw.length);
        buffer.put(raw);

        List<CommitMessage> messages = deserialize(buffer.array());
        Assert.assertTrue(messages.isEmpty());
    }

    @Test
    public void testDeserializeInvalidPayloadThrows() throws Exception {
        try {
            deserialize(new byte[] {1, 2, 3, 4});
            Assert.fail("expected IOException");
        } catch (IOException e) {
            Assert.assertTrue(e.getMessage().contains("Failed to deserialize paimon commit message payload"));
        }
    }

    @Test
    public void testUpdateCommitMessagesDeduplicatesPayloads(@Mocked PaimonMetadataOps ops) throws Exception {
        PaimonTransaction transaction = new PaimonTransaction(ops);
        TCommitMessage duplicated1 = new TCommitMessage();
        duplicated1.setPayload(new byte[] {1, 2, 3});
        TCommitMessage duplicated2 = new TCommitMessage();
        duplicated2.setPayload(new byte[] {1, 2, 3});
        TCommitMessage distinct = new TCommitMessage();
        distinct.setPayload(new byte[] {4, 5, 6});
        TCommitMessage empty = new TCommitMessage();
        empty.setPayload(new byte[0]);

        transaction.updateCommitMessages(Arrays.asList(duplicated1, duplicated2, null, empty, distinct));

        List<TCommitMessage> commitMessages = getField(transaction, "commitMessages");
        Assert.assertEquals(2, commitMessages.size());
        Assert.assertArrayEquals(new byte[] {1, 2, 3}, commitMessages.get(0).getPayload());
        Assert.assertArrayEquals(new byte[] {4, 5, 6}, commitMessages.get(1).getPayload());
    }

    @Test
    public void testBeginInsertExtractsUserWithoutHost(@Mocked PaimonMetadataOps ops,
            @Mocked PaimonExternalTable table) throws Exception {
        ConnectContext ctx = new ConnectContext();
        ctx.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp("alice", "127.0.0.1"));
        ctx.setThreadLocalInfo();

        PaimonTransaction transaction = new PaimonTransaction(ops);
        transaction.beginInsert(table, Optional.empty());

        Assert.assertEquals("alice", getField(transaction, "hadoopUser"));
    }

    @Test
    public void testCommitThrowsWhenTableMissing(@Mocked PaimonMetadataOps ops) throws Exception {
        PaimonTransaction transaction = new PaimonTransaction(ops);
        transaction.setTransactionId(1001L);
        TCommitMessage message = new TCommitMessage();
        message.setPayload(new byte[] {1});
        transaction.updateCommitMessages(Collections.singletonList(message));

        try {
            transaction.commit();
            Assert.fail("expected UserException");
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("Missing paimon table for transaction"));
        }
    }

    @Test
    public void testCommitThrowsWhenTxnIdMissing(@Mocked PaimonMetadataOps ops,
            @Mocked PaimonExternalTable table) throws Exception {
        PaimonTransaction transaction = new PaimonTransaction(ops);
        transaction.beginInsert(table, Optional.empty());
        TCommitMessage message = new TCommitMessage();
        message.setPayload(new byte[] {1});
        transaction.updateCommitMessages(Collections.singletonList(message));

        try {
            transaction.commit();
            Assert.fail("expected UserException");
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("Missing transaction id for paimon commit"));
        }
    }

    @Test
    public void testCommitSucceedsAndClosesCommitter() throws Exception {
        ExternalCatalog catalog = Mockito.mock(ExternalCatalog.class);
        Mockito.when(catalog.getExecutionAuthenticator()).thenReturn(new ExecutionAuthenticator() {
        });
        PaimonMetadataOps ops = new PaimonMetadataOps(catalog, null);
        PaimonExternalTable table = Mockito.mock(PaimonExternalTable.class);
        InnerTable paimonTable = Mockito.mock(InnerTable.class);
        InnerTableCommit committer = Mockito.mock(InnerTableCommit.class);
        CommitMessage commitMessage = Mockito.mock(CommitMessage.class);
        mockCommitMessageDeserializer(commitMessage);

        Mockito.when(table.getPaimonTable(Mockito.any())).thenReturn(paimonTable);
        Mockito.when(paimonTable.newCommit("doris_txn_1002")).thenReturn(committer);

        PaimonTransaction transaction = new PaimonTransaction(ops);
        transaction.beginInsert(table, Optional.empty());
        transaction.setTransactionId(1002L);
        TCommitMessage message = new TCommitMessage();
        message.setPayload(new byte[] {1});
        transaction.updateCommitMessages(Collections.singletonList(message));

        transaction.commit();

        @SuppressWarnings("unchecked")
        ArgumentCaptor<java.util.Map<Long, List<CommitMessage>>> commitMapCaptor =
                ArgumentCaptor.forClass(java.util.Map.class);
        Mockito.verify(committer).filterAndCommit(commitMapCaptor.capture());
        Assert.assertEquals(1, commitMapCaptor.getValue().size());
        Assert.assertEquals(Collections.singletonList(commitMessage), commitMapCaptor.getValue().get(1002L));
        Mockito.verify(committer).close();
    }

    @Test
    public void testCommitWrapsCommitterFailure() throws Exception {
        ExternalCatalog catalog = Mockito.mock(ExternalCatalog.class);
        Mockito.when(catalog.getExecutionAuthenticator()).thenReturn(new ExecutionAuthenticator() {
        });
        PaimonMetadataOps ops = new PaimonMetadataOps(catalog, null);
        PaimonExternalTable table = Mockito.mock(PaimonExternalTable.class);
        InnerTable paimonTable = Mockito.mock(InnerTable.class);
        InnerTableCommit committer = Mockito.mock(InnerTableCommit.class);
        CommitMessage commitMessage = Mockito.mock(CommitMessage.class);
        mockCommitMessageDeserializer(commitMessage);

        Mockito.when(table.getPaimonTable(Mockito.any())).thenReturn(paimonTable);
        Mockito.when(paimonTable.newCommit("doris_txn_1003")).thenReturn(committer);
        Mockito.doThrow(new RuntimeException("commit failed")).when(committer).filterAndCommit(Mockito.anyMap());

        PaimonTransaction transaction = new PaimonTransaction(ops);
        transaction.beginInsert(table, Optional.empty());
        transaction.setTransactionId(1003L);
        TCommitMessage message = new TCommitMessage();
        message.setPayload(new byte[] {2});
        transaction.updateCommitMessages(Collections.singletonList(message));

        try {
            transaction.commit();
            Assert.fail("expected UserException");
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("Failed to commit paimon transaction on FE"));
            Assert.assertNotNull(e.getCause());
            Assert.assertTrue(e.getCause().getMessage().contains("commit failed"));
        }
        Mockito.verify(committer).close();
    }

    @Test
    public void testCommitRejectsNonInnerTable() throws Exception {
        ExternalCatalog catalog = Mockito.mock(ExternalCatalog.class);
        Mockito.when(catalog.getExecutionAuthenticator()).thenReturn(new ExecutionAuthenticator() {
        });
        PaimonMetadataOps ops = new PaimonMetadataOps(catalog, null);
        PaimonExternalTable table = Mockito.mock(PaimonExternalTable.class);
        Table paimonTable = Mockito.mock(Table.class);
        CommitMessage commitMessage = Mockito.mock(CommitMessage.class);
        mockCommitMessageDeserializer(commitMessage);

        Mockito.when(table.getPaimonTable(Mockito.any())).thenReturn(paimonTable);

        PaimonTransaction transaction = new PaimonTransaction(ops);
        transaction.beginInsert(table, Optional.empty());
        transaction.setTransactionId(1004L);
        TCommitMessage message = new TCommitMessage();
        message.setPayload(new byte[] {3});
        transaction.updateCommitMessages(Collections.singletonList(message));

        try {
            transaction.commit();
            Assert.fail("expected UserException");
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("Failed to commit paimon transaction on FE"));
            Assert.assertNotNull(e.getCause());
            Assert.assertTrue(e.getCause().getMessage().contains("does not support commit"));
        }
    }

    @SuppressWarnings("unchecked")
    private List<CommitMessage> deserialize(byte[] payload) throws Exception {
        Method method = PaimonTransaction.class.getDeclaredMethod("deserializeCommitMessagePayload", byte[].class);
        method.setAccessible(true);
        try {
            return (List<CommitMessage>) method.invoke(null, payload);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception) {
                throw (Exception) cause;
            }
            throw e;
        }
    }

    @SuppressWarnings("unchecked")
    private <T> T getField(Object target, String fieldName) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return (T) field.get(target);
    }

    private void mockCommitMessageDeserializer(CommitMessage commitMessage) {
        new MockUp<CommitMessageSerializer>() {
            @Mock
            public List<CommitMessage> deserializeList(int version, DataInputView input) {
                return Collections.singletonList(commitMessage);
            }
        };
    }
}
