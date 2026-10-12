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

import com.google.common.base.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.io.DataInputDeserializer;
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

/** Connector-SPI transaction for one Paimon write statement. */
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
    private final ConnectorContext context;
    private final List<byte[]> commitPayloads = new ArrayList<>();
    private final Set<CommitPayloadKey> commitPayloadSet = new HashSet<>();
    private PaimonWriteBinding binding;
    private CommitState state = CommitState.PREPARED;

    PaimonConnectorTransaction(long transactionId, ConnectorContext context) {
        Preconditions.checkArgument(transactionId > 0, "Paimon transaction id must be positive");
        this.transactionId = transactionId;
        this.context = Preconditions.checkNotNull(context, "Paimon connector context must not be null");
        this.commitUser = commitUser(context.getClusterId(), transactionId);
    }

    synchronized void bind(PaimonWriteBinding writeBinding) {
        Preconditions.checkNotNull(writeBinding, "Paimon write binding must not be null");
        Preconditions.checkState(binding == null, "Paimon transaction is already bound");
        Preconditions.checkState(state == CommitState.PREPARED,
                "Paimon transaction can only be bound while prepared");
        binding = writeBinding;
    }

    String getCommitUser() {
        return commitUser;
    }

    static String commitUser(int clusterId, long transactionId) {
        return "doris_cluster_" + clusterId + "_txn_" + transactionId;
    }

    @Override
    public long getTransactionId() {
        return transactionId;
    }

    @Override
    public void addCommitData(byte[] commitFragment) {
        TPaimonCommitMessage message = new TPaimonCommitMessage();
        try {
            new TDeserializer(new TBinaryProtocol.Factory()).deserialize(message, commitFragment);
        } catch (TException e) {
            throw new DorisConnectorException("Failed to deserialize Paimon commit message", e);
        }
        Preconditions.checkState(message.isSetPayload(), "Paimon commit message payload is missing");
        byte[] payload = message.getPayload();
        Preconditions.checkState(payload.length > 0, "Paimon commit message payload is empty");
        synchronized (this) {
            if (commitPayloadSet.add(new CommitPayloadKey(payload))) {
                commitPayloads.add(payload);
            }
        }
    }

    @Override
    public void commit() {
        PaimonWriteBinding writeBinding = requireBinding();
        List<byte[]> payloads = snapshotPayloads();
        if (payloads.isEmpty() && !writeBinding.isOverwrite()) {
            markPreparedTransactionCommitted();
            return;
        }
        try {
            List<CommitMessage> messages = deserializePayloads(payloads);
            doCommitWithReconciliation(writeBinding, messages);
        } catch (Exception e) {
            throw new DorisConnectorException("Failed to commit Paimon transaction on FE", e);
        }
    }

    @Override
    public void rollback() {
        CommitState current = getState();
        if (current == CommitState.COMMITTED) {
            return;
        }
        if (current == CommitState.COMMITTING || current == CommitState.OUTCOME_UNKNOWN) {
            LOG.warn("Skip rollback for Paimon transaction in state {}, txnId={}, table={}",
                    current, transactionId, tableName());
            return;
        }
        List<byte[]> payloads = snapshotPayloads();
        if (payloads.isEmpty()) {
            return;
        }
        try {
            PaimonWriteBinding writeBinding = requireBinding();
            List<CommitMessage> messages = deserializePayloads(payloads);
            context.executeAuthenticated(() -> {
                try (InnerTableCommit committer = writeBinding.getTable().newCommit(commitUser)) {
                    committer.abort(messages);
                }
                return null;
            });
        } catch (Exception e) {
            LOG.warn("Failed to rollback Paimon transaction, txnId={}, table={}",
                    transactionId, tableName(), e);
        }
    }

    @Override
    public void close() {
        // The Paimon committer is scoped to commit/rollback and closed there.
    }

    @Override
    public String profileLabel() {
        return "PAIMON";
    }

    private void doCommitWithReconciliation(PaimonWriteBinding writeBinding,
            List<CommitMessage> messages) throws Exception {
        Exception firstFailure;
        try {
            doCommit(writeBinding, messages);
            return;
        } catch (Exception e) {
            if (getState() == CommitState.COMMITTED) {
                return;
            }
            if (getState() == CommitState.PREPARED) {
                throw e;
            }
            firstFailure = e;
        }

        Exception retryFailure;
        try {
            doCommit(writeBinding, messages);
            return;
        } catch (Exception e) {
            if (getState() == CommitState.COMMITTED) {
                return;
            }
            retryFailure = e;
        }

        try {
            boolean committed = context.executeAuthenticated(() -> !writeBinding.getTable()
                    .snapshotManager().findSnapshotsForIdentifiers(
                            commitUser, Collections.singletonList(transactionId)).isEmpty());
            if (committed) {
                markCommitted();
                return;
            }
        } catch (Exception reconciliationFailure) {
            retryFailure.addSuppressed(reconciliationFailure);
        }
        retryFailure.addSuppressed(firstFailure);
        markOutcomeUnknown();
        throw retryFailure;
    }

    private void doCommit(PaimonWriteBinding writeBinding, List<CommitMessage> messages)
            throws Exception {
        context.executeAuthenticated(() -> {
            try (InnerTableCommit committer = writeBinding.getTable().newCommit(commitUser)) {
                if (writeBinding.isOverwrite()) {
                    committer.withOverwrite(writeBinding.getStaticPartition());
                }
                Map<Long, List<CommitMessage>> commitMap = new HashMap<>();
                commitMap.put(transactionId, messages);
                markCommitting();
                committer.filterAndCommit(commitMap);
                markCommitted();
            }
            return null;
        });
    }

    private synchronized PaimonWriteBinding requireBinding() {
        Preconditions.checkState(binding != null,
                "Missing Paimon write binding for transaction " + transactionId);
        return binding;
    }

    private synchronized CommitState getState() {
        return state;
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

    private synchronized List<byte[]> snapshotPayloads() {
        return new ArrayList<>(commitPayloads);
    }

    private String tableName() {
        synchronized (this) {
            return binding == null ? "unbound" : binding.tableName();
        }
    }

    static List<CommitMessage> deserializePayloads(List<byte[]> payloads) throws IOException {
        List<CommitMessage> messages = new ArrayList<>();
        for (byte[] payload : payloads) {
            Preconditions.checkState(payload.length >= COMMIT_HEADER_SIZE && hasMagic(payload),
                    "Invalid Paimon commit message payload header");
            int version = readInt(payload, 4);
            int length = readInt(payload, 8);
            Preconditions.checkState(length >= 0 && payload.length == COMMIT_HEADER_SIZE + length,
                    "Invalid Paimon commit message payload length");
            byte[] raw = Arrays.copyOfRange(payload, COMMIT_HEADER_SIZE, payload.length);
            messages.addAll(new CommitMessageSerializer().deserializeList(
                    version, new DataInputDeserializer(raw)));
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

    private static final class CommitPayloadKey {
        private final byte[] payload;
        private final int hashCode;

        private CommitPayloadKey(byte[] payload) {
            this.payload = payload;
            this.hashCode = Arrays.hashCode(payload);
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof CommitPayloadKey
                    && Arrays.equals(payload, ((CommitPayloadKey) obj).payload);
        }

        @Override
        public int hashCode() {
            return hashCode;
        }
    }
}
