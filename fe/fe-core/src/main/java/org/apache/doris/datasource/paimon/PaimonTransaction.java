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
import org.apache.doris.datasource.mvcc.MvccUtil;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertCommandContext;
import org.apache.doris.thrift.TPaimonCommitMessage;
import org.apache.doris.transaction.Transaction;

import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.io.DataInputDeserializer;
import org.apache.paimon.table.InnerTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.apache.paimon.table.sink.StreamTableCommit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;

public class PaimonTransaction implements Transaction {
    private static final Logger LOG = LogManager.getLogger(PaimonTransaction.class);
    private static final int COMMIT_MESSAGE_HEADER_SIZE = 12;
    private static final byte[] COMMIT_MESSAGE_MAGIC = new byte[] {'D', 'P', 'C', 'M'};

    private final CommitExecutor commitExecutor;
    private PaimonExternalTable table;
    private long transactionId = -1L;
    private String commitUser = "";
    private boolean committed = false;

    private final List<byte[]> commitPayloads = Lists.newArrayList();
    private final Set<String> commitPayloadSet = new HashSet<>();

    public PaimonTransaction(PaimonMetadataOps ops) {
        this(new DefaultCommitExecutor(ops));
    }

    PaimonTransaction(CommitExecutor commitExecutor) {
        this.commitExecutor = commitExecutor;
    }

    public void updateCommitMessages(List<TPaimonCommitMessage> messages) {
        if (messages == null || messages.isEmpty()) {
            return;
        }
        synchronized (this) {
            for (TPaimonCommitMessage message : messages) {
                addCommitPayload(message);
            }
        }
    }

    public void beginInsert(PaimonExternalTable table, Optional<InsertCommandContext> insertCtx) {
        this.table = table;
    }

    public void finishInsert(PaimonExternalTable table, Optional<InsertCommandContext> insertCtx) {
    }

    @Override
    public void commit() throws UserException {
        List<byte[]> rawPayloads = snapshotCommitPayloads();
        if (rawPayloads.isEmpty()) {
            LOG.info("Skip empty PaimonTransaction commit, txnId={}, table={}", transactionId, tableName());
            return;
        }
        if (table == null) {
            throw new UserException("Missing paimon table for transaction");
        }
        if (transactionId <= 0) {
            throw new UserException("Missing transaction id for paimon commit");
        }

        try {
            List<CommitMessage> allMessages = deserializeCommitMessagePayloads(rawPayloads);
            LOG.info("Commit PaimonTransaction, txnId={}, table={}, rawPayloads={}, commitMessages={}",
                    transactionId, tableName(), rawPayloads.size(), allMessages.size());
            if (allMessages.isEmpty()) {
                throw new RuntimeException("Paimon commit messages are empty, raw payload size=" + rawPayloads.size());
            }
            commitExecutor.commit(table, commitUser, transactionId, allMessages);
            synchronized (this) {
                committed = true;
            }
        } catch (Exception e) {
            throw new UserException("Failed to commit paimon transaction on FE", e);
        }
    }

    @Override
    public void rollback() {
        if (isCommitted()) {
            LOG.info("Skip rollback for committed PaimonTransaction, txnId={}, table={}", transactionId, tableName());
            return;
        }
        List<byte[]> rawPayloads = snapshotCommitPayloads();
        if (rawPayloads.isEmpty()) {
            LOG.info("Skip empty PaimonTransaction rollback, txnId={}, table={}", transactionId, tableName());
            return;
        }
        if (table == null) {
            LOG.warn("Skip PaimonTransaction rollback because table is missing, txnId={}, rawPayloads={}",
                    transactionId, rawPayloads.size());
            return;
        }

        try {
            List<CommitMessage> allMessages = deserializeCommitMessagePayloads(rawPayloads);
            if (allMessages.isEmpty()) {
                LOG.info("Skip PaimonTransaction rollback with empty decoded messages, txnId={}, "
                        + "table={}, rawPayloads={}", transactionId, tableName(), rawPayloads.size());
                return;
            }
            LOG.info("Rollback PaimonTransaction, txnId={}, table={}, rawPayloads={}, commitMessages={}",
                    transactionId, tableName(), rawPayloads.size(), allMessages.size());
            commitExecutor.abort(table, commitUser, allMessages);
        } catch (Exception e) {
            LOG.warn("Failed to rollback PaimonTransaction, txnId={}, table={}", transactionId, tableName(), e);
        }
    }

    public long getUpdateCnt() {
        return 0L;
    }

    public void setTransactionId(long transactionId) {
        this.transactionId = transactionId;
        this.commitUser = "doris_txn_" + transactionId;
    }

    public String getCommitUser() {
        return commitUser;
    }

    boolean isCommitted() {
        synchronized (this) {
            return committed;
        }
    }

    int getCommitPayloadCount() {
        synchronized (this) {
            return commitPayloads.size();
        }
    }

    private void addCommitPayload(TPaimonCommitMessage message) {
        if (message == null || !message.isSetPayload()) {
            return;
        }
        byte[] payload = message.getPayload();
        if (payload == null || payload.length == 0) {
            return;
        }
        String key = Base64.getEncoder().encodeToString(payload);
        if (commitPayloadSet.add(key)) {
            commitPayloads.add(Arrays.copyOf(payload, payload.length));
        }
    }

    private List<byte[]> snapshotCommitPayloads() {
        synchronized (this) {
            List<byte[]> rawPayloads = new ArrayList<>(commitPayloads.size());
            for (byte[] payload : commitPayloads) {
                rawPayloads.add(Arrays.copyOf(payload, payload.length));
            }
            return rawPayloads;
        }
    }

    static List<CommitMessage> deserializeCommitMessagePayloads(List<byte[]> payloads) throws IOException {
        List<CommitMessage> allMessages = new ArrayList<>();
        for (byte[] payload : payloads) {
            allMessages.addAll(deserializeCommitMessagePayload(payload));
        }
        return allMessages;
    }

    static List<CommitMessage> deserializeCommitMessagePayload(byte[] payload) throws IOException {
        if (payload == null || payload.length < COMMIT_MESSAGE_HEADER_SIZE || !hasValidMagic(payload)) {
            throw new IOException("Invalid paimon commit message payload header");
        }

        int version = readInt(payload, 4);
        int len = readInt(payload, 8);
        if (len < 0 || payload.length != COMMIT_MESSAGE_HEADER_SIZE + len) {
            throw new IOException("Invalid paimon commit message payload length");
        }

        byte[] raw = new byte[len];
        System.arraycopy(payload, COMMIT_MESSAGE_HEADER_SIZE, raw, 0, len);
        List<CommitMessage> messages =
                new CommitMessageSerializer().deserializeList(version, new DataInputDeserializer(raw));
        if (messages == null) {
            throw new IOException("Paimon commit message payload deserialized to null");
        }
        return messages;
    }

    private static boolean hasValidMagic(byte[] payload) {
        for (int i = 0; i < COMMIT_MESSAGE_MAGIC.length; i++) {
            if (payload[i] != COMMIT_MESSAGE_MAGIC[i]) {
                return false;
            }
        }
        return true;
    }

    private static int readInt(byte[] payload, int offset) {
        return ((payload[offset] & 0xFF) << 24) | ((payload[offset + 1] & 0xFF) << 16)
                | ((payload[offset + 2] & 0xFF) << 8) | (payload[offset + 3] & 0xFF);
    }

    private String tableName() {
        return table == null ? "null" : table.getName();
    }

    interface CommitExecutor {
        void commit(PaimonExternalTable table, String commitUser, long transactionId,
                List<CommitMessage> commitMessages) throws Exception;

        void abort(PaimonExternalTable table, String commitUser, List<CommitMessage> commitMessages) throws Exception;
    }

    private static class DefaultCommitExecutor implements CommitExecutor {
        private final PaimonMetadataOps ops;

        private DefaultCommitExecutor(PaimonMetadataOps ops) {
            this.ops = ops;
        }

        @Override
        public void commit(PaimonExternalTable table, String commitUser, long transactionId,
                List<CommitMessage> commitMessages) throws Exception {
            execute(() -> {
                InnerTable paimonTable = getInnerTable(table);
                StreamTableCommit committer = paimonTable.newCommit(commitUser);
                try {
                    Map<Long, List<CommitMessage>> commitMap = new HashMap<>();
                    commitMap.put(transactionId, commitMessages);
                    committer.filterAndCommit(commitMap);
                    return null;
                } finally {
                    committer.close();
                }
            });
        }

        @Override
        public void abort(PaimonExternalTable table, String commitUser,
                List<CommitMessage> commitMessages) throws Exception {
            execute(() -> {
                InnerTable paimonTable = getInnerTable(table);
                StreamTableCommit committer = paimonTable.newCommit(commitUser);
                try {
                    committer.abort(commitMessages);
                    return null;
                } finally {
                    committer.close();
                }
            });
        }

        private <T> T execute(Callable<T> callable) throws Exception {
            ExecutionAuthenticator authenticator = ops.dorisCatalog.getExecutionAuthenticator();
            return authenticator.execute(callable);
        }

        private InnerTable getInnerTable(PaimonExternalTable table) {
            org.apache.paimon.table.Table paimonTable =
                    table.getPaimonTable(MvccUtil.getSnapshotFromContext(table));
            if (!(paimonTable instanceof InnerTable)) {
                throw new RuntimeException("Paimon table does not support commit: " + paimonTable.getClass());
            }
            return (InnerTable) paimonTable;
        }
    }

}
