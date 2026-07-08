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

/**
 * Paimon transaction.
 *
 * Lifecycle:
 *   1. beginInsert() — record target table
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

    private static final int COMMIT_HEADER_SIZE = 12;
    private static final byte[] COMMIT_MAGIC = new byte[] {'D', 'P', 'C', 'M'};

    private final PaimonMetadataOps ops;
    private PaimonExternalTable table;
    private long transactionId = -1L;
    private String commitUser = "";
    private boolean committed = false;

    private final List<byte[]> commitPayloads = Lists.newArrayList();
    private final Set<String> commitPayloadSet = new HashSet<>();

    public PaimonTransaction(PaimonMetadataOps ops) {
        this.ops = ops;
    }

    // ────────────────────────────────────────────────────────────
    // Transaction lifecycle
    // ────────────────────────────────────────────────────────────

    public void beginInsert(PaimonExternalTable table, Optional<InsertCommandContext> insertCtx) {
        this.table = table;
    }

    public void finishInsert(PaimonExternalTable table, Optional<InsertCommandContext> insertCtx) {
    }

    @Override
    public void commit() throws UserException {
        List<byte[]> rawPayloads = snapshotPayloads();
        if (rawPayloads.isEmpty()) {
            LOG.info("Skip empty PaimonTransaction commit, txnId={}, table={}",
                    transactionId, tableName());
            return;
        }
        if (table == null) {
            throw new UserException("Missing paimon table for transaction");
        }
        if (transactionId <= 0) {
            throw new UserException("Missing transaction id for paimon commit");
        }

        try {
            List<CommitMessage> allMessages = deserializePayloads(rawPayloads);
            LOG.info("Commit PaimonTransaction, txnId={}, table={}, payloads={}, messages={}",
                    transactionId, tableName(), rawPayloads.size(), allMessages.size());
            if (allMessages.isEmpty()) {
                throw new RuntimeException(
                        "Paimon commit messages are empty, payloads=" + rawPayloads.size());
            }
            doCommit(allMessages);
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
            LOG.info("Skip rollback for committed PaimonTransaction, txnId={}, table={}",
                    transactionId, tableName());
            return;
        }
        List<byte[]> rawPayloads = snapshotPayloads();
        if (rawPayloads.isEmpty()) {
            LOG.info("Skip empty PaimonTransaction rollback, txnId={}, table={}",
                    transactionId, tableName());
            return;
        }
        if (table == null) {
            LOG.warn("Skip PaimonTransaction rollback, table missing, txnId={}", transactionId);
            return;
        }
        try {
            List<CommitMessage> allMessages = deserializePayloads(rawPayloads);
            if (allMessages.isEmpty()) {
                LOG.info("Skip PaimonTransaction rollback with empty decoded messages, "
                        + "txnId={}, table={}", transactionId, tableName());
                return;
            }
            LOG.info("Rollback PaimonTransaction, txnId={}, table={}, payloads={}, messages={}",
                    transactionId, tableName(), rawPayloads.size(), allMessages.size());
            doAbort(allMessages);
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
        String key = Base64.getEncoder().encodeToString(payload);
        if (commitPayloadSet.add(key)) {
            commitPayloads.add(Arrays.copyOf(payload, payload.length));
        }
    }

    // ────────────────────────────────────────────────────────────
    // Setters
    // ────────────────────────────────────────────────────────────

    public void setTransactionId(long transactionId) {
        this.transactionId = transactionId;
        this.commitUser = "doris_txn_" + transactionId;
    }

    public String getCommitUser() {
        return commitUser;
    }

    public long getTransactionId() {
        return transactionId;
    }

    boolean isCommitted() {
        synchronized (this) {
            return committed;
        }
    }

    int getPayloadCount() {
        synchronized (this) {
            return commitPayloads.size();
        }
    }

    // ────────────────────────────────────────────────────────────
    // Internal commit logic
    // ────────────────────────────────────────────────────────────

    private void doCommit(List<CommitMessage> msgs) throws Exception {
        ops.getCatalog();
        ExecutionAuthenticator authenticator = ops.dorisCatalog.getExecutionAuthenticator();
        authenticator.execute(() -> {
            InnerTable paimonTable = getInnerTable();
            StreamTableCommit committer = paimonTable.newCommit(commitUser);
            try {
                Map<Long, List<CommitMessage>> commitMap = new HashMap<>();
                commitMap.put(transactionId, msgs);
                committer.filterAndCommit(commitMap);
                return null;
            } finally {
                committer.close();
            }
        });
    }

    private void doAbort(List<CommitMessage> msgs) throws Exception {
        ExecutionAuthenticator authenticator = ops.dorisCatalog.getExecutionAuthenticator();
        authenticator.execute(() -> {
            InnerTable paimonTable = getInnerTable();
            StreamTableCommit committer = paimonTable.newCommit(commitUser);
            try {
                committer.abort(msgs);
                return null;
            } finally {
                committer.close();
            }
        });
    }

    private InnerTable getInnerTable() {
        org.apache.paimon.table.Table paimonTable =
                table.getPaimonTable(MvccUtil.getSnapshotFromContext(table));
        if (!(paimonTable instanceof InnerTable)) {
            throw new RuntimeException(
                    "Paimon table does not support commit: " + paimonTable.getClass());
        }
        return (InnerTable) paimonTable;
    }

    // ────────────────────────────────────────────────────────────
    // Serialization helpers
    // ────────────────────────────────────────────────────────────

    private List<byte[]> snapshotPayloads() {
        synchronized (this) {
            List<byte[]> copy = new ArrayList<>(commitPayloads.size());
            for (byte[] p : commitPayloads) {
                copy.add(Arrays.copyOf(p, p.length));
            }
            return copy;
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
        return table == null ? "null" : table.getName();
    }
}
