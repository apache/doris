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
import org.apache.doris.transaction.Transaction;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.io.DataInputDeserializer;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.apache.paimon.table.sink.InnerTableCommit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Paimon transaction.
 *
 * Lifecycle:
 *   1. bind() — pin the concrete table and writer configuration
 *   2. updateCommitMessages() — called multiple times as BE reports commit data
 *   3. commit() — deserialize all CommitMessages, call StreamTableCommit.filterAndCommit()
 *   4. rollback() — abort uncommitted data files
 *
 * CommitMessage wire format:
 *   BE ← JNI ← Java: byte[] with DPCM header (magic + version + length) + Paimon
 *   CommitMessageSerializer payload
 */
public class PaimonTransaction implements Transaction {
    private static final Logger LOG = LogManager.getLogger(PaimonTransaction.class);

    enum CommitState {
        PREPARED,
        COMMITTING,
        COMMITTED,
        OUTCOME_UNKNOWN
    }

    private static final int COMMIT_HEADER_SIZE = 12;
    private static final byte[] COMMIT_MAGIC = new byte[] {'D', 'P', 'C', 'M'};

    private final PaimonMetadataOps ops;
    private final long transactionId;
    private final String commitUser;
    private PaimonWriteBinding binding;
    private CommitState state = CommitState.PREPARED;

    private final List<byte[]> commitPayloads = Lists.newArrayList();
    private final Set<CommitPayloadKey> commitPayloadSet = new HashSet<>();

    public PaimonTransaction(PaimonMetadataOps ops, long transactionId) {
        this.ops = Preconditions.checkNotNull(ops, "Paimon metadata ops must not be null");
        Preconditions.checkArgument(transactionId > 0, "Paimon transaction id must be positive");
        this.transactionId = transactionId;
        this.commitUser = commitUser(transactionId);
    }

    // ────────────────────────────────────────────────────────────
    // Transaction lifecycle
    // ────────────────────────────────────────────────────────────

    public synchronized void bind(PaimonWriteBinding binding) {
        Preconditions.checkNotNull(binding, "Paimon write binding must not be null");
        Preconditions.checkState(this.binding == null, "Paimon transaction is already bound");
        Preconditions.checkState(state == CommitState.PREPARED,
                "Paimon transaction can only be bound while prepared");
        this.binding = binding;
    }

    @Override
    public void commit() throws UserException {
        PaimonWriteBinding writeBinding = requireBinding();
        List<byte[]> rawPayloads = snapshotPayloads();
        if (rawPayloads.isEmpty() && !writeBinding.isOverwrite()) {
            LOG.info("Skip empty PaimonTransaction commit, txnId={}, table={}",
                    transactionId, tableName());
            markPreparedTransactionCommitted();
            return;
        }
        try {
            List<CommitMessage> allMessages = deserializePayloads(rawPayloads);
            LOG.info("Commit PaimonTransaction, txnId={}, table={}, payloads={}, messages={}, overwrite={}",
                    transactionId, tableName(), rawPayloads.size(), allMessages.size(),
                    writeBinding.isOverwrite());
            if (allMessages.isEmpty() && !writeBinding.isOverwrite()) {
                throw new RuntimeException(
                        "Paimon commit messages are empty, payloads=" + rawPayloads.size());
            }
            doCommitWithReconciliation(writeBinding, allMessages);
        } catch (Exception e) {
            throw new UserException("Failed to commit paimon transaction on FE", e);
        }
    }

    @Override
    public void rollback() {
        CommitState currentState = getState();
        if (currentState == CommitState.COMMITTED) {
            LOG.info("Skip rollback for committed PaimonTransaction, txnId={}, table={}",
                    transactionId, tableName());
            return;
        }
        if (currentState == CommitState.COMMITTING
                || currentState == CommitState.OUTCOME_UNKNOWN) {
            LOG.warn("Skip rollback for PaimonTransaction in state {}, txnId={}, table={}. "
                            + "Preserving data files for snapshot safety",
                    currentState, transactionId, tableName());
            return;
        }
        List<byte[]> rawPayloads = snapshotPayloads();
        if (rawPayloads.isEmpty()) {
            LOG.info("Skip empty PaimonTransaction rollback, txnId={}, table={}",
                    transactionId, tableName());
            return;
        }
        try {
            PaimonWriteBinding writeBinding = requireBinding();
            List<CommitMessage> allMessages = deserializePayloads(rawPayloads);
            if (allMessages.isEmpty()) {
                LOG.info("Skip PaimonTransaction rollback with empty decoded messages, "
                        + "txnId={}, table={}", transactionId, tableName());
                return;
            }
            LOG.info("Rollback PaimonTransaction, txnId={}, table={}, payloads={}, messages={}",
                    transactionId, tableName(), rawPayloads.size(), allMessages.size());
            doAbort(writeBinding, allMessages);
        } catch (Exception e) {
            LOG.warn("Failed to rollback PaimonTransaction, txnId={}, table={}",
                    transactionId, tableName(), e);
        }
    }

    // ────────────────────────────────────────────────────────────
    // CommitMessage collection (called from Coordinator)
    // ────────────────────────────────────────────────────────────

    public void updateCommitMessages(List<TPaimonCommitMessage> messages) {
        if (messages == null || messages.isEmpty()) {
            return;
        }
        synchronized (this) {
            for (TPaimonCommitMessage msg : messages) {
                addPayload(msg);
            }
        }
    }

    private void addPayload(TPaimonCommitMessage message) {
        if (message == null || !message.isSetPayload()) {
            return;
        }
        byte[] payload = message.getPayload();
        if (payload == null || payload.length == 0) {
            return;
        }
        // Treat the Thrift payload as immutable after report handling and adopt it directly. The
        // report is not reused, so copying it would only create a second full representation.
        if (commitPayloadSet.add(new CommitPayloadKey(payload))) {
            commitPayloads.add(payload);
        }
    }

    // ────────────────────────────────────────────────────────────
    // Transaction identity
    // ────────────────────────────────────────────────────────────

    public static String commitUser(long transactionId) {
        return "doris_txn_" + transactionId;
    }

    public String getCommitUser() {
        return commitUser;
    }

    public long getTransactionId() {
        return transactionId;
    }

    synchronized CommitState getState() {
        return state;
    }

    int getPayloadCount() {
        synchronized (this) {
            return commitPayloads.size();
        }
    }

    // ────────────────────────────────────────────────────────────
    // Internal commit logic
    // ────────────────────────────────────────────────────────────

    private void doCommit(PaimonWriteBinding writeBinding, List<CommitMessage> messages)
            throws Exception {
        ops.dorisCatalog.getExecutionAuthenticator().execute(() -> {
            InnerTableCommit committer = getCommitTable(writeBinding).newCommit(commitUser);
            Exception commitFailure = null;
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
                commitFailure = e;
                throw e;
            } finally {
                try {
                    committer.close();
                } catch (Exception closeFailure) {
                    if (getState() == CommitState.COMMITTED) {
                        LOG.warn("Ignore Paimon committer close failure after a successful commit, "
                                        + "txnId={}, table={}",
                                transactionId, tableName(), closeFailure);
                    } else if (commitFailure != null) {
                        commitFailure.addSuppressed(closeFailure);
                    } else {
                        throw closeFailure;
                    }
                }
            }
        });
    }

    private void doCommitWithReconciliation(PaimonWriteBinding writeBinding,
            List<CommitMessage> messages) throws Exception {
        Exception firstFailure;
        try {
            doCommit(writeBinding, messages);
            return;
        } catch (Exception e) {
            if (getState() == CommitState.COMMITTED) {
                LOG.warn("Paimon commit completed but post-commit processing failed, "
                                + "txnId={}, table={}",
                        transactionId, tableName(), e);
                return;
            }
            if (getState() == CommitState.PREPARED) {
                throw e;
            }
            firstFailure = e;
        }

        LOG.warn("Paimon atomic commit failed with an unknown outcome; retrying idempotently, "
                        + "txnId={}, table={}", transactionId, tableName(), firstFailure);
        Exception retryFailure;
        try {
            doCommit(writeBinding, messages);
            return;
        } catch (Exception e) {
            if (getState() == CommitState.COMMITTED) {
                LOG.warn("Paimon retry completed but post-commit processing failed, "
                                + "txnId={}, table={}",
                        transactionId, tableName(), e);
                return;
            }
            retryFailure = e;
        }

        try {
            if (ops.dorisCatalog.getExecutionAuthenticator().execute(() -> !writeBinding.getTable()
                    .snapshotManager().findSnapshotsForIdentifiers(
                            commitUser, Collections.singletonList(transactionId)).isEmpty())) {
                markCommitted();
                LOG.info("Reconciled Paimon transaction as committed, txnId={}, table={}",
                        transactionId, tableName());
                return;
            }
        } catch (Exception reconciliationFailure) {
            retryFailure.addSuppressed(reconciliationFailure);
        }
        retryFailure.addSuppressed(firstFailure);
        markOutcomeUnknown();
        throw retryFailure;
    }

    private void doAbort(PaimonWriteBinding writeBinding, List<CommitMessage> messages)
            throws Exception {
        ops.dorisCatalog.getExecutionAuthenticator().execute(() -> {
            InnerTableCommit committer = writeBinding.getTable().newCommit(commitUser);
            try {
                committer.abort(messages);
                return null;
            } finally {
                committer.close();
            }
        });
    }

    private FileStoreTable getCommitTable(PaimonWriteBinding writeBinding) {
        FileStoreTable paimonTable = writeBinding.getTable();
        if (!writeBinding.isOverwrite() || writeBinding.getStaticPartition().isEmpty()) {
            return paimonTable;
        }
        return paimonTable.copy(Collections.singletonMap(
                CoreOptions.DYNAMIC_PARTITION_OVERWRITE.key(), Boolean.FALSE.toString()));
    }

    private synchronized void markPreparedTransactionCommitted() {
        Preconditions.checkState(state == CommitState.PREPARED,
                "Only a prepared Paimon transaction can complete without a commit");
        state = CommitState.COMMITTED;
    }

    private synchronized void markCommitting() {
        Preconditions.checkState(state == CommitState.PREPARED || state == CommitState.COMMITTING,
                "Cannot enter Paimon commit from state " + state);
        state = CommitState.COMMITTING;
    }

    private synchronized void markCommitted() {
        Preconditions.checkState(state == CommitState.COMMITTING,
                "Only a committing Paimon transaction can be committed");
        state = CommitState.COMMITTED;
    }

    private synchronized void markOutcomeUnknown() {
        Preconditions.checkState(state == CommitState.COMMITTING,
                "Only a committing Paimon transaction can have an unknown outcome");
        state = CommitState.OUTCOME_UNKNOWN;
    }

    private synchronized PaimonWriteBinding requireBinding() throws UserException {
        if (binding == null) {
            throw new UserException("Missing Paimon write binding for transaction " + transactionId);
        }
        return binding;
    }

    // ────────────────────────────────────────────────────────────
    // Serialization helpers
    // ────────────────────────────────────────────────────────────

    private List<byte[]> snapshotPayloads() {
        synchronized (this) {
            return new ArrayList<>(commitPayloads);
        }
    }

    private static final class CommitPayloadKey {
        private final byte[] payload;
        private final int hashCode;

        private CommitPayloadKey(byte[] payload) {
            this.payload = payload;
            this.hashCode = Arrays.hashCode(payload);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof CommitPayloadKey)) {
                return false;
            }
            CommitPayloadKey other = (CommitPayloadKey) obj;
            return Arrays.equals(payload, other.payload);
        }

        @Override
        public int hashCode() {
            return hashCode;
        }
    }

    static List<CommitMessage> deserializePayloads(List<byte[]> payloads) throws IOException {
        List<CommitMessage> all = new ArrayList<>();
        for (byte[] payload : payloads) {
            all.addAll(deserializePayload(payload));
        }
        return all;
    }

    static List<CommitMessage> deserializePayload(byte[] payload) throws IOException {
        if (payload == null || payload.length < COMMIT_HEADER_SIZE || !hasMagic(payload)) {
            throw new IOException("Invalid paimon commit message payload header");
        }
        int version = readInt(payload, 4);
        int len = readInt(payload, 8);
        if (len < 0 || payload.length != COMMIT_HEADER_SIZE + len) {
            throw new IOException("Invalid paimon commit message payload length");
        }
        byte[] raw = new byte[len];
        System.arraycopy(payload, COMMIT_HEADER_SIZE, raw, 0, len);
        List<CommitMessage> messages =
                new CommitMessageSerializer().deserializeList(version, new DataInputDeserializer(raw));
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
        return ((payload[offset] & 0xFF) << 24)
                | ((payload[offset + 1] & 0xFF) << 16)
                | ((payload[offset + 2] & 0xFF) << 8)
                | (payload[offset + 3] & 0xFF);
    }

    private String tableName() {
        synchronized (this) {
            return binding == null ? "unbound" : binding.tableName();
        }
    }
}
