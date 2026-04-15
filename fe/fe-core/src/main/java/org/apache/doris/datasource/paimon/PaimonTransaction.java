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
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TCommitMessage;
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
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class PaimonTransaction implements Transaction {
    private static final Logger LOG = LogManager.getLogger(PaimonTransaction.class);

    private final PaimonMetadataOps ops;
    private PaimonExternalTable table;
    private long transactionId = -1L;
    private String commitUser = "";
    private String hadoopUser = "";

    private final List<TCommitMessage> commitMessages = Lists.newArrayList();
    private final Set<String> commitPayloadSet = new HashSet<>();

    public PaimonTransaction(PaimonMetadataOps ops) {
        this.ops = ops;
    }

    public void updateCommitMessages(List<TCommitMessage> messages) {
        if (messages == null || messages.isEmpty()) {
            return;
        }
        synchronized (this) {
            for (TCommitMessage message : messages) {
                if (message == null || !message.isSetPayload()) {
                    continue;
                }
                byte[] payload = message.getPayload();
                if (payload == null || payload.length == 0) {
                    continue;
                }
                String key = Base64.getEncoder().encodeToString(payload);
                if (commitPayloadSet.add(key)) {
                    commitMessages.add(message);
                }
            }
        }
    }

    public void beginInsert(PaimonExternalTable table, Optional<InsertCommandContext> insertCtx) {
        this.table = table;
        ConnectContext ctx = ConnectContext.get();
        if (ctx == null) {
            return;
        }
        String user = ctx.getQualifiedUser();
        int atPos = user.indexOf('@');
        if (atPos > 0) {
            user = user.substring(0, atPos);
        }
        this.hadoopUser = user;
    }

    public void finishInsert(PaimonExternalTable table, Optional<InsertCommandContext> insertCtx) {
    }

    @Override
    public void commit() throws UserException {
        List<TCommitMessage> rawMessages;
        synchronized (this) {
            rawMessages = Lists.newArrayList(commitMessages);
        }
        if (rawMessages.isEmpty()) {
            return;
        }
        if (table == null) {
            throw new UserException("Missing paimon table for transaction");
        }
        if (transactionId <= 0) {
            throw new UserException("Missing transaction id for paimon commit");
        }

        try {
            ExecutionAuthenticator authenticator = ops.dorisCatalog.getExecutionAuthenticator();
            authenticator.execute(() -> {
                org.apache.paimon.table.Table paimonTable =
                        table.getPaimonTable(MvccUtil.getSnapshotFromContext(table));
                if (!(paimonTable instanceof InnerTable)) {
                    throw new RuntimeException("Paimon table does not support commit: " + paimonTable.getClass());
                }

                List<CommitMessage> allMessages = new ArrayList<>();
                for (TCommitMessage msg : rawMessages) {
                    if (msg == null || !msg.isSetPayload()) {
                        continue;
                    }
                    byte[] payload = msg.getPayload();
                    if (payload == null || payload.length == 0) {
                        continue;
                    }
                    allMessages.addAll(deserializeCommitMessagePayload(payload));
                }
                LOG.info("paimon: rawMessages size={}, allMessages size={}", rawMessages.size(), allMessages.size());
                if (allMessages.isEmpty()) {
                    throw new RuntimeException("allMessages is empty! rawMessages size=" + rawMessages.size());
                }

                StreamTableCommit committer = ((InnerTable) paimonTable).newCommit(commitUser);
                try {
                    Map<Long, List<CommitMessage>> commitMap = new HashMap<>();
                    commitMap.put(transactionId, allMessages);
                    committer.filterAndCommit(commitMap);
                    return null;
                } finally {
                    committer.close();
                }
            });
        } catch (Exception e) {
            throw new UserException("Failed to commit paimon transaction on FE", e);
        }
    }

    @Override
    public void rollback() {
        LOG.info("Rollback PaimonTransaction for table {}", table == null ? "null" : table.getName());
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

    private static List<CommitMessage> deserializeCommitMessagePayload(byte[] payload) throws IOException {
        CommitMessageSerializer serializer = new CommitMessageSerializer();
        if (payload != null && payload.length >= 12
                && payload[0] == 'D' && payload[1] == 'P' && payload[2] == 'C' && payload[3] == 'M') {
            int version = ((payload[4] & 0xFF) << 24) | ((payload[5] & 0xFF) << 16)
                    | ((payload[6] & 0xFF) << 8) | (payload[7] & 0xFF);
            int len = ((payload[8] & 0xFF) << 24) | ((payload[9] & 0xFF) << 16)
                    | ((payload[10] & 0xFF) << 8) | (payload[11] & 0xFF);
            if (len >= 0 && payload.length >= 12 + len) {
                byte[] raw = new byte[len];
                System.arraycopy(payload, 12, raw, 0, len);
                try {
                    List<CommitMessage> msgs = serializer.deserializeList(version, new DataInputDeserializer(raw));
                    if (msgs != null) {
                        return msgs;
                    } else {
                        LOG.warn("paimon: deserialized msg list is null");
                    }
                } catch (Exception e) {
                    LOG.debug("Deserialize paimon commit message failed for header version {}", version, e);
                }
            } else {
                LOG.warn("paimon: payload version or length mismatch: version={}, len={}, payload.length={}",
                        version, len, payload.length);
            }
        } else {
            LOG.warn("paimon: payload header mismatch or too short: length={}",
                    payload == null ? 0 : payload.length);
            LOG.debug("paimon: problematic payload bytes: {}",
                    payload == null ? "null" : java.util.Arrays.toString(payload));
        }

        int[] candidateVersions = new int[] {11, 10, 9, 8, 7, 6, 5, 4, 3};
        Exception last = null;
        for (int v : candidateVersions) {
            try {
                return serializer.deserializeList(v, new DataInputDeserializer(payload));
            } catch (Exception e) {
                last = e;
            }
        }
        IOException ioe =
                new IOException("Failed to deserialize paimon commit message payload for all candidate versions");
        if (last != null) {
            ioe.addSuppressed(last);
        }
        throw ioe;
    }
}
