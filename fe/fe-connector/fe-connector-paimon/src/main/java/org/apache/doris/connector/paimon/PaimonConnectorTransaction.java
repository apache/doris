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

package org.apache.doris.connector.paimon;

import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.handle.ConnectorTransaction;
import org.apache.doris.thrift.TPaimonCommitMessage;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.io.DataInputDeserializer;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.apache.paimon.table.sink.InnerTableCommit;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/** FE coordinator for one Paimon write transaction. */
public class PaimonConnectorTransaction implements ConnectorTransaction {

    private static final Logger LOG = LogManager.getLogger(PaimonConnectorTransaction.class);
    private static final int COMMIT_HEADER_SIZE = 12;
    private static final byte[] COMMIT_MAGIC = new byte[] {'D', 'P', 'C', 'M'};

    enum CommitState {
        PREPARED,
        COMMITTING,
        COMMITTED,
        OUTCOME_UNKNOWN
    }

    private final long transactionId;
    private final String commitUser;
    private final PaimonCatalogOps catalogOps;
    private final ConnectorContext context;
    private final List<byte[]> commitPayloads = new ArrayList<>();
    private final Set<CommitPayloadKey> commitPayloadSet = new HashSet<>();

    private PaimonWriteBinding binding;
    private CommitState state = CommitState.PREPARED;
    private long updateCount;

    public PaimonConnectorTransaction(long transactionId, PaimonCatalogOps catalogOps,
            ConnectorContext context) {
        if (transactionId <= 0) {
            throw new IllegalArgumentException("Paimon transaction id must be positive");
        }
        this.transactionId = transactionId;
        this.catalogOps = catalogOps;
        this.context = context;
        // A random per-transaction user is stable across commit retries and cannot collide with
        // another Doris cluster writing the same Paimon warehouse.
        this.commitUser = "doris_txn_" + transactionId + "_" + UUID.randomUUID();
    }

    synchronized void bind(PaimonWriteBinding writeBinding) {
        if (writeBinding == null) {
            throw new IllegalArgumentException("Paimon write binding must not be null");
        }
        if (binding != null) {
            throw new IllegalStateException("Paimon transaction is already bound");
        }
        if (state != CommitState.PREPARED) {
            throw new IllegalStateException("Paimon transaction can only be bound while prepared");
        }
        binding = writeBinding;
    }

    String getCommitUser() {
        return commitUser;
    }

    @Override
    public long getTransactionId() {
        return transactionId;
    }

    @Override
    public void addCommitData(byte[] commitFragment) {
        TPaimonCommitMessage data = new TPaimonCommitMessage();
        try {
            new TDeserializer(new TBinaryProtocol.Factory()).deserialize(data, commitFragment);
        } catch (TException e) {
            throw new DorisConnectorException("Failed to deserialize Paimon commit data", e);
        }
        if (!data.isSetPayload() || data.getPayload() == null || data.getPayload().length == 0) {
            throw new DorisConnectorException("Paimon commit data is missing its payload");
        }
        byte[] payload = data.getPayload();
        synchronized (this) {
            if (commitPayloadSet.add(new CommitPayloadKey(payload))) {
                commitPayloads.add(payload);
                if (data.isSetRowCount()) {
                    updateCount += data.getRowCount();
                }
            }
        }
    }

    @Override
    public long getUpdateCnt() {
        synchronized (this) {
            return updateCount;
        }
    }

    @Override
    public String profileLabel() {
        return "PAIMON";
    }

    @Override
    public void commit() {
        PaimonWriteBinding writeBinding = requireBinding();
        List<byte[]> payloads = snapshotPayloads();
        if (payloads.isEmpty() && !writeBinding.isOverwrite()) {
            synchronized (this) {
                state = CommitState.COMMITTED;
            }
            return;
        }
        try {
            List<CommitMessage> messages = deserializePayloads(payloads);
            if (messages.isEmpty() && !writeBinding.isOverwrite()) {
                throw new IOException("Paimon commit messages are empty");
            }
            commitWithReconciliation(writeBinding, messages);
        } catch (Exception e) {
            throw new DorisConnectorException(
                    "Failed to commit Paimon transaction " + transactionId + ": " + e.getMessage(), e);
        }
    }

    private void commitWithReconciliation(PaimonWriteBinding writeBinding,
            List<CommitMessage> messages) throws Exception {
        Exception firstFailure;
        try {
            doCommit(writeBinding, messages);
            return;
        } catch (Exception e) {
            if (state() == CommitState.COMMITTED) {
                return;
            }
            if (state() == CommitState.PREPARED) {
                throw e;
            }
            firstFailure = e;
        }

        LOG.warn("Paimon commit outcome is unknown; retrying idempotently, txn={}, table={}",
                transactionId, writeBinding.tableName(), firstFailure);
        Exception retryFailure;
        try {
            doCommit(writeBinding, messages);
            return;
        } catch (Exception e) {
            if (state() == CommitState.COMMITTED) {
                return;
            }
            retryFailure = e;
        }

        try {
            FileStoreTable current = loadCurrentTable(writeBinding);
            if (!current.snapshotManager().findSnapshotsForIdentifiers(
                    commitUser, Collections.singletonList(transactionId)).isEmpty()) {
                markCommitted();
                return;
            }
        } catch (Exception reconciliationFailure) {
            retryFailure.addSuppressed(reconciliationFailure);
        }
        retryFailure.addSuppressed(firstFailure);
        synchronized (this) {
            state = CommitState.OUTCOME_UNKNOWN;
        }
        throw retryFailure;
    }

    private void doCommit(PaimonWriteBinding writeBinding, List<CommitMessage> messages)
            throws Exception {
        executeAuthenticated(() -> {
            FileStoreTable current = loadCurrentTableUnauthenticated(writeBinding);
            InnerTableCommit committer = current.newCommit(commitUser);
            Exception failure = null;
            try {
                if (writeBinding.isOverwrite()) {
                    committer.withOverwrite(writeBinding.getStaticPartition());
                }
                Map<Long, List<CommitMessage>> commitMap = new HashMap<>();
                commitMap.put(transactionId, messages);
                markCommitting();
                committer.filterAndCommit(commitMap);
                markCommitted();
                return null;
            } catch (Exception e) {
                failure = e;
                throw e;
            } finally {
                try {
                    committer.close();
                } catch (Exception closeFailure) {
                    if (state() == CommitState.COMMITTED) {
                        LOG.warn("Ignoring Paimon committer close failure after commit, txn={}",
                                transactionId, closeFailure);
                    } else if (failure != null) {
                        failure.addSuppressed(closeFailure);
                    } else {
                        throw closeFailure;
                    }
                }
            }
        });
    }

    @Override
    public void rollback() {
        CommitState currentState = state();
        if (currentState == CommitState.COMMITTED
                || currentState == CommitState.COMMITTING
                || currentState == CommitState.OUTCOME_UNKNOWN) {
            return;
        }
        List<byte[]> payloads = snapshotPayloads();
        if (payloads.isEmpty()) {
            return;
        }
        try {
            PaimonWriteBinding writeBinding = requireBinding();
            List<CommitMessage> messages = deserializePayloads(payloads);
            executeAuthenticated(() -> {
                // Abort the files produced against the bound table generation. Reloading the
                // current table would make a schema-generation fence block cleanup after a
                // concurrent ALTER, which is exactly when these staged files must be removed.
                InnerTableCommit committer = writeBinding.getTable().newCommit(commitUser);
                try {
                    committer.abort(messages);
                    return null;
                } finally {
                    committer.close();
                }
            });
        } catch (Exception e) {
            LOG.warn("Failed to roll back Paimon transaction {}, table={}",
                    transactionId, binding == null ? "unbound" : binding.tableName(), e);
        }
    }

    @Override
    public void close() {
        // Paimon's commit and writer objects are closed at their respective operation boundaries.
    }

    private FileStoreTable loadCurrentTable(PaimonWriteBinding writeBinding) throws Exception {
        return executeAuthenticated(() -> loadCurrentTableUnauthenticated(writeBinding));
    }

    private FileStoreTable loadCurrentTableUnauthenticated(PaimonWriteBinding writeBinding)
            throws Catalog.TableNotExistException {
        Table current = catalogOps.getTable(writeBinding.getIdentifier());
        if (!(current instanceof FileStoreTable)) {
            throw new DorisConnectorException(
                    "Paimon write target is no longer a file store table: " + writeBinding.tableName());
        }
        FileStoreTable fileStoreTable = (FileStoreTable) current;
        String currentIdentity = PaimonWritePlanProvider.writeMetadataIdentity(fileStoreTable);
        if (!writeBinding.getMetadataIdentity().equals(currentIdentity)) {
            throw new DorisConnectorException(
                    "Paimon write metadata changed after the write was bound; retry the statement");
        }
        return PaimonWriteBinding.configureTableForWrite(
                fileStoreTable, writeBinding.isOverwrite(), writeBinding.getStaticPartition());
    }

    private <T> T executeAuthenticated(java.util.concurrent.Callable<T> callable) throws Exception {
        return context == null ? callable.call() : context.executeAuthenticated(callable);
    }

    private synchronized PaimonWriteBinding requireBinding() {
        if (binding == null) {
            throw new DorisConnectorException(
                    "Missing Paimon write binding for transaction " + transactionId);
        }
        return binding;
    }

    private synchronized CommitState state() {
        return state;
    }

    private synchronized void markCommitting() {
        if (state != CommitState.PREPARED && state != CommitState.COMMITTING) {
            throw new IllegalStateException("Cannot enter Paimon commit from state " + state);
        }
        state = CommitState.COMMITTING;
    }

    private synchronized void markCommitted() {
        if (state != CommitState.COMMITTING) {
            throw new IllegalStateException("Only a committing Paimon transaction can be committed");
        }
        state = CommitState.COMMITTED;
    }

    private synchronized List<byte[]> snapshotPayloads() {
        return new ArrayList<>(commitPayloads);
    }

    static List<CommitMessage> deserializePayloads(List<byte[]> payloads) throws IOException {
        List<CommitMessage> messages = new ArrayList<>();
        for (byte[] payload : payloads) {
            messages.addAll(deserializePayload(payload));
        }
        return messages;
    }

    static List<CommitMessage> deserializePayload(byte[] payload) throws IOException {
        if (payload == null || payload.length < COMMIT_HEADER_SIZE || !hasMagic(payload)) {
            throw new IOException("Invalid Paimon commit message payload header");
        }
        int version = readInt(payload, 4);
        int length = readInt(payload, 8);
        if (length < 0 || payload.length != COMMIT_HEADER_SIZE + length) {
            throw new IOException("Invalid Paimon commit message payload length");
        }
        byte[] raw = Arrays.copyOfRange(payload, COMMIT_HEADER_SIZE, payload.length);
        List<CommitMessage> messages = new CommitMessageSerializer()
                .deserializeList(version, new DataInputDeserializer(raw));
        if (messages == null) {
            throw new IOException("Paimon commit message payload deserialized to null");
        }
        return messages;
    }

    private static boolean hasMagic(byte[] payload) {
        for (int i = 0; i < COMMIT_MAGIC.length; i++) {
            if (payload[i] != COMMIT_MAGIC[i]) {
                return false;
            }
        }
        return true;
    }

    private static int readInt(byte[] payload, int offset) {
        return ((payload[offset] & 0xff) << 24)
                | ((payload[offset + 1] & 0xff) << 16)
                | ((payload[offset + 2] & 0xff) << 8)
                | (payload[offset + 3] & 0xff);
    }

    private static final class CommitPayloadKey {
        private final byte[] payload;
        private final int hashCode;

        private CommitPayloadKey(byte[] payload) {
            this.payload = payload;
            this.hashCode = Arrays.hashCode(payload);
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof CommitPayloadKey
                    && Arrays.equals(payload, ((CommitPayloadKey) other).payload);
        }

        @Override
        public int hashCode() {
            return hashCode;
        }
    }
}
