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

package org.apache.doris.connector.maxcompute;

import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.handle.ConnectorTransaction;
import org.apache.doris.connector.api.handle.WriteBlockAllocatingConnectorTransaction;
import org.apache.doris.thrift.TMCCommitData;

import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.enviroment.EnvironmentSettings;
import com.aliyun.odps.table.write.TableBatchWriteSession;
import com.aliyun.odps.table.write.TableWriteSessionBuilder;
import com.aliyun.odps.table.write.WriterCommitMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * MaxCompute connector transaction (ports the legacy
 * {@code org.apache.doris.datasource.maxcompute.MCTransaction} write lifecycle
 * to the connector SPI).
 *
 * <p>Holds the per-statement write state: accumulated commit fragments
 * ({@link TMCCommitData}, fed back from BE via {@link #addCommitData}), the
 * block-id high-water mark, and — once the write plan (P4-T04) creates the ODPS
 * write session — the session id / target identifier / environment settings used
 * by {@link #commit()}.</p>
 *
 * <p><b>Gate-closed / dormant.</b> Nothing routes plugin-driven MaxCompute writes
 * through this class until the {@code max_compute} cutover: the executor wiring
 * ({@code beginTransaction} &rarr; {@code PluginDrivenTransactionManager.begin})
 * and {@code GlobalExternalTransactionInfoMgr} registration are deferred to that
 * step. {@link #commit()} depends on the write-session state populated by P4-T04
 * (via {@link #setWriteSession}); it is intentionally not runnable before then.</p>
 */
public class MaxComputeConnectorTransaction
        implements ConnectorTransaction, WriteBlockAllocatingConnectorTransaction {

    private static final Logger LOG = LogManager.getLogger(
            MaxComputeConnectorTransaction.class);

    /**
     * Legacy default of {@code Config.max_compute_write_max_block_count} (20000); used as the
     * fallback when the session does not carry the (tunable) value. The connector cannot import
     * fe-core {@code Config}, so the live value is threaded in through the constructor — resolved
     * from {@link org.apache.doris.connector.api.ConnectorSession#getSessionProperties()} by
     * {@code MaxComputeConnectorMetadata.resolveMaxBlockCount} (GC1 / FIX-BLOCKID-CAP-CONFIG,
     * restoring legacy fe.conf tunability and superseding the hardcoded cap in DV-011).
     */
    static final long DEFAULT_MAX_BLOCK_COUNT = 20000L;

    private final long transactionId;
    /** Upper bound on allocatable block ids; = Config.max_compute_write_max_block_count (per session). */
    private final long maxBlockCount;
    private final List<TMCCommitData> commitDataList = new ArrayList<>();
    private final AtomicLong nextBlockId = new AtomicLong(0);

    // Write-session state, populated by the write plan (P4-T04) before commit.
    private volatile String writeSessionId;
    private volatile TableIdentifier tableIdentifier;
    private volatile EnvironmentSettings settings;

    public MaxComputeConnectorTransaction(long transactionId, long maxBlockCount) {
        this.transactionId = transactionId;
        this.maxBlockCount = maxBlockCount;
    }

    /**
     * Binds the ODPS write session created by the write plan (P4-T04) so that
     * block allocation and {@link #commit()} can act on it. Resets the block-id
     * high-water mark to the start of the new session.
     */
    public void setWriteSession(String writeSessionId, TableIdentifier tableIdentifier,
            EnvironmentSettings settings) {
        this.writeSessionId = writeSessionId;
        this.tableIdentifier = tableIdentifier;
        this.settings = settings;
        this.nextBlockId.set(0);
    }

    public String getWriteSessionId() {
        return writeSessionId;
    }

    @Override
    public long getTransactionId() {
        return transactionId;
    }

    @Override
    public void addCommitData(byte[] commitFragment) {
        TMCCommitData data = new TMCCommitData();
        try {
            new TDeserializer(new TBinaryProtocol.Factory()).deserialize(data, commitFragment);
        } catch (TException e) {
            throw new DorisConnectorException("failed to deserialize MaxCompute commit data", e);
        }
        synchronized (this) {
            commitDataList.add(data);
        }
    }

    @Override
    public long allocateWriteBlockRange(String requestWriteSessionId, long count) {
        if (count <= 0) {
            throw new DorisConnectorException(
                    "MaxCompute block_id allocation length must be positive: " + count);
        }
        if (writeSessionId == null || writeSessionId.isEmpty()) {
            throw new DorisConnectorException("MaxCompute write session has not been initialized");
        }
        if (!writeSessionId.equals(requestWriteSessionId)) {
            throw new DorisConnectorException("MaxCompute write session mismatch, expected="
                    + writeSessionId + ", actual=" + requestWriteSessionId);
        }

        long start;
        long endExclusive;
        do {
            start = nextBlockId.get();
            endExclusive = start + count;
            if (endExclusive > maxBlockCount) {
                throw new DorisConnectorException("MaxCompute block_id exceeds limit, start="
                        + start + ", length=" + count + ", maxBlockCount=" + maxBlockCount);
            }
        } while (!nextBlockId.compareAndSet(start, endExclusive));

        LOG.info("Allocated MaxCompute block_id range: sessionId={}, start={}, length={}",
                writeSessionId, start, count);
        return start;
    }

    @Override
    public long getUpdateCnt() {
        return commitDataList.stream().mapToLong(TMCCommitData::getRowCount).sum();
    }

    @Override
    public String profileLabel() {
        return "MAXCOMPUTE";
    }

    @Override
    public void commit() {
        try {
            List<WriterCommitMessage> allMessages = new ArrayList<>();
            synchronized (this) {
                for (TMCCommitData data : commitDataList) {
                    if (data.isSetCommitMessage() && !data.getCommitMessage().isEmpty()) {
                        appendCommitMessages(allMessages, data.getCommitMessage());
                    }
                }
            }

            TableBatchWriteSession commitSession = new TableWriteSessionBuilder()
                    .identifier(tableIdentifier)
                    .withSessionId(writeSessionId)
                    .withSettings(settings)
                    .buildBatchWriteSession();
            commitSession.commit(allMessages.toArray(new WriterCommitMessage[0]));

            LOG.info("Committed MaxCompute write session {} with {} messages",
                    writeSessionId, allMessages.size());
        } catch (Exception e) {
            throw new DorisConnectorException(
                    "Failed to commit MaxCompute write session: " + e.getMessage(), e);
        }
    }

    @Override
    public void rollback() {
        // MaxCompute write sessions auto-expire if not committed; no explicit rollback needed.
        LOG.info("MaxCompute transaction {} rollback called; uncommitted sessions will auto-expire.",
                transactionId);
    }

    @Override
    public void close() {
        // No resources to release: the ODPS write session auto-expires if not committed.
    }

    private void appendCommitMessages(List<WriterCommitMessage> allMessages, String encodedCommitMessage)
            throws IOException, ClassNotFoundException {
        byte[] bytes = Base64.getDecoder().decode(encodedCommitMessage);
        Object payload;
        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
            payload = ois.readObject();
        }

        if (payload instanceof WriterCommitMessage) {
            allMessages.add((WriterCommitMessage) payload);
            return;
        }
        if (payload instanceof List<?>) {
            for (Object item : (List<?>) payload) {
                if (!(item instanceof WriterCommitMessage)) {
                    throw new DorisConnectorException("Unexpected MaxCompute commit payload item type: "
                            + (item == null ? "null" : item.getClass().getName()));
                }
                allMessages.add((WriterCommitMessage) item);
            }
            return;
        }
        throw new DorisConnectorException("Unexpected MaxCompute commit payload type: "
                + (payload == null ? "null" : payload.getClass().getName()));
    }
}
