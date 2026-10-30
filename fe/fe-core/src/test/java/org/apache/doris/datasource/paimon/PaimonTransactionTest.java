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
import org.apache.doris.qe.Coordinator;
import org.apache.doris.qe.QeProcessorImpl;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPaimonCommitMessage;
import org.apache.doris.thrift.TQueryOptions;
import org.apache.doris.thrift.TReportExecStatusParams;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.io.ByteStreams;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.io.DataInputDeserializer;
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
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class PaimonTransactionTest {
    private static final long TRANSACTION_ID = 12345L;
    private static final AtomicLong QUERY_SEQUENCE = new AtomicLong();

    private TUniqueId queryId;

    private PaimonWriteBinding binding;
    private PaimonMetadataOps ops;
    private PaimonExternalCatalog dorisCatalog;
    private ExecutionAuthenticator authenticator;
    private FileStoreTable table;
    private TableCommitImpl committer;
    private SnapshotManager snapshotManager;

    @Before
    public void setUp() throws Exception {
        queryId = new TUniqueId(12345, QUERY_SEQUENCE.incrementAndGet());
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
        PaimonTransaction.abortLateCommitMessages(queryId, rustPayloads());
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
        PaimonTransaction.abortLateCommitMessages(queryId, rustPayloads());
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

    @Test
    public void testRustMessagesFromMultipleBackendsCommitTogether() throws Exception {
        Mockito.when(binding.isOverwrite()).thenReturn(false);
        PaimonTransaction transaction = createBoundTransaction();
        List<TPaimonCommitMessage> payloads = rustPayloads();
        for (TPaimonCommitMessage payload : payloads) {
            transaction.updateCommitMessages(Collections.singletonList(payload));
        }
        // A repeated report must not add the same files twice.
        transaction.updateCommitMessages(payloads);
        transaction.commit();

        Assert.assertEquals(PaimonTransaction.CommitState.COMMITTED, transaction.getState());
        Mockito.verify(committer).filterAndCommit(ArgumentMatchers.argThat(commits ->
                commits.size() == 1 && commits.get(TRANSACTION_ID).size() == 2));
        transaction.rollback();
        PaimonTransaction.abortLateCommitMessages(queryId, rustPayloads());
        Mockito.verify(committer, Mockito.never()).abort(ArgumentMatchers.anyList());
    }

    @Test
    public void testPreCommitFailureAbortsRustMessagesFromBothBackends() throws Exception {
        Mockito.when(binding.isOverwrite()).thenReturn(false);
        Mockito.when(table.newCommit(PaimonTransaction.commitUser(TRANSACTION_ID)))
                .thenThrow(new RuntimeException("pre-commit failure"))
                .thenReturn(committer);
        PaimonTransaction transaction = createBoundTransaction();
        transaction.updateCommitMessages(rustPayloads());

        Assert.assertThrows(UserException.class, transaction::commit);
        Assert.assertEquals(PaimonTransaction.CommitState.PREPARED, transaction.getState());
        transaction.rollback();
        Mockito.verify(committer).abort(ArgumentMatchers.argThat(messages -> messages.size() == 2));
    }

    @Test
    public void testEmptyRollbackCleansReportAfterQueryRemoval() throws Exception {
        PaimonTransaction transaction = createBoundTransaction();
        transaction.rollback();
        Assert.assertEquals(PaimonTransaction.CommitState.ABORTED, transaction.getState());

        TReportExecStatusParams report = new TReportExecStatusParams().setQueryId(queryId)
                .setPaimonCommitMessages(rustPayloads());
        Assert.assertEquals(TStatusCode.OK, QeProcessorImpl.INSTANCE
                .reportExecStatus(report, new TNetworkAddress("127.0.0.1", 9060)).getStatus().getStatusCode());

        Mockito.verify(committer).abort(ArgumentMatchers.argThat(messages -> messages.size() == 2));
        Assert.assertEquals(0, transaction.getPayloadCount());
        // Rollback is terminal: a late commit must never publish files already deleted by abort.
        Assert.assertThrows(UserException.class, transaction::commit);
        Mockito.verify(committer, Mockito.never()).filterAndCommit(ArgumentMatchers.anyMap());
    }

    @Test
    public void testReportRacesWithTransactionRemoval() throws Exception {
        PaimonTransaction transaction = createBoundTransaction();
        Coordinator coordinator = Mockito.mock(Coordinator.class);
        Mockito.when(coordinator.getQueryOptions()).thenReturn(new TQueryOptions());
        Mockito.doAnswer(invocation -> {
            transaction.rollback();
            throw new RuntimeException("Transaction was removed during report handling");
        }).when(coordinator).updateFragmentExecStatus(ArgumentMatchers.any());
        QeProcessorImpl.INSTANCE.registerQuery(queryId, new QeProcessorImpl.QueryInfo(coordinator));
        try {
            TReportExecStatusParams report = new TReportExecStatusParams().setQueryId(queryId)
                    .setFragmentInstanceId(new TUniqueId(1, 2)).setPaimonCommitMessages(rustPayloads());
            QeProcessorImpl.INSTANCE.reportExecStatus(report, new TNetworkAddress("127.0.0.1", 9060));
            Mockito.verify(committer).abort(ArgumentMatchers.argThat(messages -> messages.size() == 2));
            Assert.assertEquals(0, transaction.getPayloadCount());
        } finally {
            QeProcessorImpl.INSTANCE.unregisterQuery(queryId);
        }
    }

    @Test
    public void testLateReportDoesNotWaitForRollbackFileIo() throws Exception {
        PaimonTransaction transaction = createBoundTransaction();
        List<TPaimonCommitMessage> payloads = rustPayloads();
        transaction.updateCommitMessages(Collections.singletonList(payloads.get(0)));
        CountDownLatch abortStarted = new CountDownLatch(1);
        CountDownLatch finishAbort = new CountDownLatch(1);
        AtomicInteger abortCalls = new AtomicInteger();
        Mockito.doAnswer(invocation -> {
            if (abortCalls.incrementAndGet() == 1) {
                abortStarted.countDown();
                Assert.assertTrue(finishAbort.await(10, TimeUnit.SECONDS));
            }
            return null;
        }).when(committer).abort(ArgumentMatchers.anyList());
        ExecutorService executor = Executors.newFixedThreadPool(2);
        Future<?> rollback = executor.submit(transaction::rollback);
        try {
            Assert.assertTrue(abortStarted.await(5, TimeUnit.SECONDS));
            Future<?> report = executor.submit(() ->
                    transaction.updateCommitMessages(Collections.singletonList(payloads.get(1))));
            report.get(5, TimeUnit.SECONDS);
        } finally {
            finishAbort.countDown();
            executor.shutdown();
        }
        rollback.get(5, TimeUnit.SECONDS);
        Mockito.verify(committer, Mockito.times(2)).abort(ArgumentMatchers.argThat(messages -> messages.size() == 1));
        Assert.assertEquals(PaimonTransaction.CommitState.ABORTED, transaction.getState());
        Assert.assertEquals(0, transaction.getPayloadCount());
    }

    @Test
    public void testCommitUserIsNamespacedByDorisCluster() {
        Assert.assertEquals(
                PaimonTransaction.commitUser(10001, TRANSACTION_ID),
                PaimonTransaction.commitUser(10001, TRANSACTION_ID));
        Assert.assertNotEquals(
                PaimonTransaction.commitUser(10001, TRANSACTION_ID),
                PaimonTransaction.commitUser(10002, TRANSACTION_ID));
    }

    private PaimonTransaction createBoundTransaction() {
        PaimonTransaction transaction = new PaimonTransaction(ops, TRANSACTION_ID);
        transaction.bind(binding);
        transaction.setQueryId(queryId);
        return transaction;
    }

    private TPaimonCommitMessage commitPayload() throws Exception {
        CommitMessage message = new CommitMessageImpl(
                BinaryRow.EMPTY_ROW,
                0,
                1,
                DataIncrement.emptyIncrement(),
                CompactIncrement.emptyIncrement());
        return commitPayload(Collections.singletonList(message));
    }

    private List<TPaimonCommitMessage> rustPayloads() throws Exception {
        byte[] bytes;
        try (InputStream input = getClass().getResourceAsStream("/paimon/rust-append-v11.base64")) {
            bytes = Base64.getMimeDecoder().decode(ByteStreams.toByteArray(input));
        }
        List<CommitMessage> messages = new CommitMessageSerializer().deserializeList(11,
                new DataInputDeserializer(bytes));
        List<TPaimonCommitMessage> payloads = new ArrayList<>();
        for (CommitMessage message : messages) {
            payloads.add(commitPayload(Collections.singletonList(message)));
        }
        return payloads;
    }

    private TPaimonCommitMessage commitPayload(List<CommitMessage> messages) throws Exception {
        CommitMessageSerializer serializer = new CommitMessageSerializer();
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        serializer.serializeList(messages,
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
