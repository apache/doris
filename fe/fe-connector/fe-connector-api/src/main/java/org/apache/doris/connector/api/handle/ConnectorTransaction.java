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

package org.apache.doris.connector.api.handle;

import org.apache.doris.connector.api.pushdown.ConnectorPredicate;

import java.io.Closeable;

/**
 * A connector-managed transaction that scopes one or more write operations.
 *
 * <p>Lifecycle: the engine calls {@link #commit()} on success or
 * {@link #rollback()} on failure, then always calls {@link #close()} to
 * release resources. {@code rollback()} and {@code close()} are safe to
 * call multiple times.</p>
 *
 * <p>Extends the marker {@link ConnectorTransactionHandle} so that existing
 * APIs that traffic in opaque handles continue to work without change.</p>
 */
public interface ConnectorTransaction extends ConnectorTransactionHandle, Closeable {

    /** Stable transaction ID assigned by the connector. */
    long getTransactionId();

    /**
     * Commits all pending operations bound to this transaction.
     *
     * @throws org.apache.doris.connector.api.DorisConnectorException
     *         on conflict, IO failure, or external system error
     */
    void commit();

    /**
     * Aborts all pending operations and releases resources.
     * Safe to call multiple times; subsequent calls are no-ops.
     */
    void rollback();

    /** Called by the engine after commit OR rollback to release connections etc. */
    @Override
    void close();

    /**
     * Receives one serialized commit fragment produced by BE after writing a
     * data fragment. The connector deserializes its own Thrift payload (e.g.
     * {@code TMCCommitData} / {@code THivePartitionUpdate} / {@code TIcebergCommitData})
     * and accumulates it for {@link #commit()}.
     *
     * <p>Default is a no-op for read-only / non-writing connectors.</p>
     *
     * @param commitFragment the serialized connector-specific commit payload
     */
    default void addCommitData(byte[] commitFragment) {
        // no-op: connectors that participate in writes override this
    }

    /**
     * Whether this transaction allocates write block ranges through a write-time
     * BE&rarr;FE callback. Only connectors with a stateful write session that
     * hands out block ids (e.g. maxcompute) return {@code true}.
     */
    default boolean supportsWriteBlockAllocation() {
        return false;
    }

    /**
     * Allocates a contiguous range of write block ids for the given write
     * session, returning the first allocated id. Called from the BE&rarr;FE RPC
     * path during a write.
     *
     * <p>Only invoked when {@link #supportsWriteBlockAllocation()} returns
     * {@code true}; the default throws.</p>
     *
     * @param writeSessionId opaque connector-defined write session identifier
     * @param count          number of block ids to allocate
     * @return the first allocated block id
     */
    default long allocateWriteBlockRange(String writeSessionId, long count) {
        throw new UnsupportedOperationException("write block allocation not supported");
    }

    /** Returns the number of rows affected by the write(s) bound to this transaction. */
    default long getUpdateCnt() {
        return 0;
    }

    /**
     * Applies an optional engine-extracted, target-only write constraint used for write-time optimistic
     * conflict detection (O5-2). The engine extracts, from the analyzed DELETE/UPDATE/MERGE plan, the
     * conjuncts that reference only the target table's own columns (slot origin-table == target, excluding
     * synthetic {@code $row_id} / metadata / join columns) and hands the connector a neutral
     * {@link ConnectorPredicate} at plan time, before {@code begin}/{@code commit}.
     *
     * <p>A connector that does optimistic conflict detection converts the neutral predicate to its own
     * dialect and uses it as the conflict-detection filter when the transaction commits (ANDed with any
     * commit-time partition filter it derives itself). Connectors that do not do conflict detection — or
     * that traffic in opaque handles — ignore it. The default is a no-op.</p>
     *
     * @param targetOnlyFilter the neutral target-only predicate, or {@code null} when the plan yielded none
     */
    default void applyWriteConstraint(ConnectorPredicate targetOnlyFilter) {
        // no-op: connectors that do optimistic conflict detection override this
    }

    /**
     * A short, connector-identifying label for the query profile (cosmetic), e.g.
     * {@code "JDBC"} / {@code "MAXCOMPUTE"}. The insert executor maps this label to a
     * profile transaction type. Replaces the executor's former hard-coded connector
     * switch; the default is a generic external label.
     */
    default String profileLabel() {
        return "EXTERNAL";
    }
}
