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
import org.apache.doris.common.security.authentication.ExecutionAuthenticator;
import org.apache.doris.thrift.TPaimonCommitMessage;

import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.io.DataOutputViewStreamWrapper;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.utils.SnapshotManager;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.Callable;

public class PaimonTransactionTest {
    private static final long TRANSACTION_ID = 12345L;

    private PaimonWriteBinding binding;
    private PaimonMetadataOps ops;
    private PaimonExternalCatalog dorisCatalog;
    private ExecutionAuthenticator authenticator;
    private FileStoreTable table;
    private TableCommitImpl committer;
    private SnapshotManager snapshotManager;

    @Before
    public void setUp() throws Exception {
        binding = Mockito.mock(PaimonWriteBinding.class);
        ops = Mockito.mock(PaimonMetadataOps.class);
        dorisCatalog = Mockito.mock(PaimonExternalCatalog.class);
        authenticator = Mockito.mock(ExecutionAuthenticator.class);
        table = Mockito.mock(FileStoreTable.class);
        committer = Mockito.mock(TableCommitImpl.class);
        snapshotManager = Mockito.mock(SnapshotManager.class);

        ops.dorisCatalog = dorisCatalog;
        Mockito.when(dorisCatalog.getExecutionAuthenticator()).thenReturn(authenticator);
        Mockito.when(binding.getTable()).thenReturn(table);
        Mockito.when(binding.isOverwrite()).thenReturn(true);
        Mockito.when(binding.getStaticPartition()).thenReturn(Collections.emptyMap());
        Mockito.when(binding.tableName()).thenReturn("db.tbl");
        Mockito.when(table.newCommit(PaimonTransaction.commitUser(TRANSACTION_ID)))
                .thenReturn(committer);
        Mockito.when(table.snapshotManager()).thenReturn(snapshotManager);
        Mockito.doAnswer(invocation -> {
            Callable<?> task = invocation.getArgument(0);
            return task.call();
        }).when(authenticator).execute(ArgumentMatchers.<Callable<Object>>any());
    }

    @Test
    public void testPublishedSnapshotReconcilesAsCommitted() throws Exception {
        RuntimeException firstFailure = new RuntimeException("first atomic failure");
        RuntimeException retryFailure = new RuntimeException("retry atomic failure");
        Mockito.doThrow(firstFailure, retryFailure)
                .when(committer).filterAndCommit(ArgumentMatchers.anyMap());
        Mockito.when(snapshotManager.findSnapshotsForIdentifiers(
                PaimonTransaction.commitUser(TRANSACTION_ID),
                Collections.singletonList(TRANSACTION_ID)))
                .thenReturn(Collections.singletonList(Mockito.mock(Snapshot.class)));

        PaimonTransaction transaction = createBoundTransaction();
        transaction.commit();

        Assert.assertEquals(PaimonTransaction.CommitState.COMMITTED, transaction.getState());
        Mockito.verify(committer, Mockito.times(2)).filterAndCommit(ArgumentMatchers.anyMap());
        transaction.rollback();
        Mockito.verify(committer, Mockito.never()).abort(ArgumentMatchers.anyList());
    }

    @Test
    public void testMissingSnapshotLeavesOutcomeUnknown() throws Exception {
        Mockito.doThrow(new RuntimeException("first atomic failure"),
                        new RuntimeException("retry atomic failure"))
                .when(committer).filterAndCommit(ArgumentMatchers.anyMap());
        Mockito.when(snapshotManager.findSnapshotsForIdentifiers(
                PaimonTransaction.commitUser(TRANSACTION_ID),
                Collections.singletonList(TRANSACTION_ID))).thenReturn(Collections.emptyList());

        PaimonTransaction transaction = createBoundTransaction();
        Assert.assertThrows(UserException.class, transaction::commit);

        Assert.assertEquals(PaimonTransaction.CommitState.OUTCOME_UNKNOWN, transaction.getState());
        transaction.rollback();
        Mockito.verify(committer, Mockito.never()).abort(ArgumentMatchers.anyList());
    }

    @Test
    public void testPreCommitFailureRemainsAbortable() throws Exception {
        Mockito.when(table.newCommit(PaimonTransaction.commitUser(TRANSACTION_ID)))
                .thenThrow(new RuntimeException("pre-commit failure"))
                .thenReturn(committer);
        PaimonTransaction transaction = createBoundTransaction();
        transaction.updateCommitMessages(Collections.singletonList(commitPayload()));

        Assert.assertThrows(UserException.class, transaction::commit);
        Assert.assertEquals(PaimonTransaction.CommitState.PREPARED, transaction.getState());
        Mockito.verify(committer, Mockito.never()).filterAndCommit(ArgumentMatchers.anyMap());

        transaction.rollback();
        Mockito.verify(committer).abort(ArgumentMatchers.anyList());
    }

    @Test
    public void testCloseFailureAfterCommitDoesNotChangeOutcome() throws Exception {
        Mockito.doThrow(new RuntimeException("close failure")).when(committer).close();
        PaimonTransaction transaction = createBoundTransaction();

        transaction.commit();

        Assert.assertEquals(PaimonTransaction.CommitState.COMMITTED, transaction.getState());
        Mockito.verify(authenticator).execute(ArgumentMatchers.<Callable<Object>>any());
    }

    @Test
    public void testCommitPayloadDedupUsesExactContent() {
        byte[] first = new byte[] {0, 31};
        byte[] sameHash = new byte[] {1, 0};
        Assert.assertEquals(Arrays.hashCode(first), Arrays.hashCode(sameHash));

        PaimonTransaction transaction = createBoundTransaction();
        transaction.updateCommitMessages(Arrays.asList(
                new TPaimonCommitMessage().setPayload(first),
                new TPaimonCommitMessage().setPayload(sameHash),
                new TPaimonCommitMessage().setPayload(Arrays.copyOf(first, first.length))));

        Assert.assertEquals(2, transaction.getPayloadCount());
    }

    private PaimonTransaction createBoundTransaction() {
        PaimonTransaction transaction = new PaimonTransaction(ops, TRANSACTION_ID);
        transaction.bind(binding);
        return transaction;
    }

    private TPaimonCommitMessage commitPayload() throws Exception {
        CommitMessage message = new CommitMessageImpl(
                BinaryRow.EMPTY_ROW,
                0,
                1,
                DataIncrement.emptyIncrement(),
                CompactIncrement.emptyIncrement());
        CommitMessageSerializer serializer = new CommitMessageSerializer();
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        serializer.serializeList(Collections.singletonList(message),
                new DataOutputViewStreamWrapper(output));
        byte[] serialized = output.toByteArray();
        ByteBuffer framed = ByteBuffer.allocate(12 + serialized.length);
        framed.put(new byte[] {'D', 'P', 'C', 'M'});
        framed.putInt(serializer.getVersion());
        framed.putInt(serialized.length);
        framed.put(serialized);
        return new TPaimonCommitMessage().setPayload(framed.array());
    }
}
