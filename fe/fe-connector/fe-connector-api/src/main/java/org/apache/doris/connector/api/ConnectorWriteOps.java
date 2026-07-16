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

package org.apache.doris.connector.api;

import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.handle.ConnectorTransaction;
import org.apache.doris.connector.api.handle.WriteOperation;

import java.util.List;

/**
 * Write (DML) operations that a connector may support.
 *
 * <p>Every write goes through a single transaction model: the engine opens a
 * {@link ConnectorTransaction} via {@link #beginTransaction(ConnectorSession)}, the
 * connector's write plan attaches to it, BE feeds commit fragments back through
 * {@link ConnectorTransaction#addCommitData}, and the engine finally calls
 * {@code commit()} / {@code rollback()}. Connectors whose writes are auto-committed
 * by BE return a degenerate no-op transaction.</p>
 *
 * <p>All methods have default implementations (throwing / read-only), so connectors
 * only override what they support.</p>
 */
public interface ConnectorWriteOps {

    /**
     * Validates that a row-level DML {@code op} ({@link WriteOperation#DELETE} / {@link WriteOperation#UPDATE} /
     * {@link WriteOperation#MERGE}) is permitted on {@code handle} under the table's configured write mode,
     * throwing a {@link DorisConnectorException} with a connector-authored message otherwise. Called at analysis
     * time, before synthesizing the write plan, so the engine rejects an unsupported statement up front (fail
     * loud) rather than producing a broken write.
     *
     * <p>The default permits everything: connectors with no per-table mode constraint need not override. A
     * connector whose tables can be configured in a mode that forbids row-level DML (e.g. iceberg
     * copy-on-write) MUST override this and throw, so the rejection — and its message — stay in the connector
     * rather than being drafted by the engine. {@code op} values other than DELETE/UPDATE/MERGE are not
     * row-level DML and an overriding connector should treat them as a no-op.</p>
     */
    default void validateRowLevelDmlMode(ConnectorSession session, ConnectorTableHandle handle, WriteOperation op) {
        // default: no per-table mode constraint
    }

    /**
     * Validates that every column named in a static-partition spec ({@code INSERT [OVERWRITE] ... PARTITION
     * (col=val)}) is a legal static-partition target on {@code handle}, throwing a {@link DorisConnectorException}
     * with a connector-authored message otherwise. Called at analysis time, before synthesizing the write plan,
     * so the engine rejects an unknown / non-identity / unpartitioned static-partition column up front (fail
     * loud) rather than letting it slip through to physical planning.
     *
     * <p>The default accepts everything: connectors with no static-partition concept (e.g. name-mapped JDBC)
     * need not override. A connector that supports {@code PARTITION(...)} writes and can reject a column (e.g.
     * iceberg, where only identity partition fields may be targeted statically) MUST override this and throw, so
     * the rejection — and its message — stay in the connector rather than being drafted by the engine. {@code
     * staticPartitionColumnNames} is the set of column names from the PARTITION clause.</p>
     */
    default void validateStaticPartitionColumns(ConnectorSession session, ConnectorTableHandle handle,
            List<String> staticPartitionColumnNames) {
        // default: no static-partition constraint
    }

    /**
     * Validates that the dynamic partition-NAME list form ({@code INSERT [OVERWRITE] ... PARTITION (p1, p2)} — a
     * list of partition column NAMES with no values, distinct from the static {@code PARTITION(col=val)} spec) is
     * permitted on {@code handle}, throwing a {@link DorisConnectorException} with a connector-authored message
     * otherwise. Called at analysis time, before synthesizing the write plan, so the engine rejects an
     * unsupported statement up front (fail loud).
     *
     * <p>The default accepts everything: connectors that ignore the name-list form need not override. A connector
     * that must reject it (e.g. hive, where {@code INSERT ... PARTITION(p1, p2)} is unsupported) MUST override
     * this and throw, so the rejection — and its message — stay in the connector rather than being drafted by the
     * engine. {@code partitionNames} is the list of partition column names from the PARTITION clause (the engine
     * calls this only when the list is non-empty).</p>
     */
    default void validateWritePartitionNames(ConnectorSession session, ConnectorTableHandle handle,
            List<String> partitionNames) {
        // default: no partition-name-list constraint
    }

    // ──────────────────── TRANSACTION ────────────────────

    /**
     * Begins a new transaction scoped to a single SQL statement (auto-commit) or to an
     * explicit BEGIN..COMMIT block. The engine binds the returned transaction onto the
     * {@link ConnectorSession} and drives its {@code commit()} / {@code rollback()} after
     * the write completes; BE feeds commit fragments back through
     * {@link ConnectorTransaction#addCommitData}.
     *
     * <p>Every write-capable connector must return a transaction here. Connectors whose
     * writes are auto-committed by BE (e.g. jdbc) return a degenerate
     * {@link org.apache.doris.connector.api.handle.NoOpConnectorTransaction}; the default
     * throws (a connector that supports writes but does not override this is misconfigured
     * — fail loud).</p>
     */
    default ConnectorTransaction beginTransaction(ConnectorSession session) {
        throw new DorisConnectorException("Transactions not supported");
    }

    /**
     * Per-table view of {@link #beginTransaction(ConnectorSession)}: opens the transaction for the connector
     * that owns {@code handle}. The default ignores {@code handle} and returns the connector-level
     * {@link #beginTransaction(ConnectorSession)}, so every single-format connector is unaffected.
     *
     * <p>A heterogeneous gateway (one catalog serving multiple table formats) overrides this to route a foreign
     * handle to its sibling connector's transaction, so the session-bound transaction's concrete type matches
     * the per-handle-selected write plan provider. The no-arg version alone would bind the gateway's own
     * transaction and the sibling's write plan would fail to downcast it. Mirrors the per-handle
     * {@code getWritePlanProvider(handle)} seam.</p>
     */
    default ConnectorTransaction beginTransaction(ConnectorSession session, ConnectorTableHandle handle) {
        return beginTransaction(session);
    }
}
