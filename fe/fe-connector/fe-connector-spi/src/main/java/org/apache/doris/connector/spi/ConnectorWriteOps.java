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

package org.apache.doris.connector.spi;

import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.handle.ConnectorTransaction;
import org.apache.doris.connector.spi.handle.WriteOperation;
import org.apache.doris.connector.spi.pushdown.ConnectorExpression;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.BooleanSupplier;

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
     * Attempts to satisfy a {@code DELETE} entirely by dropping whole data files, without reading any rows
     * ("metadata delete"): a remote atomic commit that removes the files whose rows must all disappear. Spark,
     * StarRocks and Trino all take this path for whole-partition and whole-file predicates, and it is what makes
     * {@code DELETE FROM t} / {@code DELETE FROM t WHERE <partition>} cheap.
     *
     * <p>The engine calls this before executing the row-level (position-delete) plan of a single-table
     * {@code DELETE}; a returned value means the connector already committed the delete, so the engine skips the
     * scan and the sink entirely. Returning empty is always legal and means "not applicable": the engine then
     * falls back to the ordinary row-level delete.</p>
     *
     * <p><b>Contract on {@code predicate}.</b> The engine only calls this method when it could extract the
     * statement's predicate <i>exactly</i> (every conjunct, target table only, no partition-clause scope), so an
     * implementation may treat it as complete. That matters because a wider predicate would drop files that still
     * hold live rows. {@code Optional.empty()} means the statement has no {@code WHERE} clause at all, i.e. every
     * row of the table is deleted. An implementation MUST itself verify that the predicate only ever removes whole
     * files (e.g. by checking file statistics / partition projections) and fall back — return empty, never a
     * partial delete — when any candidate file is only partially matched.</p>
     *
     * <p>The default returns empty, so connectors without a whole-file delete path are unaffected.</p>
     *
     * @param session   the current session
     * @param handle    the target table handle
     * @param predicate the statement's complete delete predicate over the target's own columns, or empty for an
     *                  unfiltered delete (always true)
     * @param boundWriteMetadataIdentity the write-metadata generation the statement was planned against, or
     *                  {@code null} when the engine has none; a connector whose writes are fenced by a table
     *                  generation MUST reject a mismatch before committing
     * @param isCancelled checked before the remote commit; returning {@code true} must abort without committing
     *                  (the engine then reports the retained cancellation reason)
     * @return the number of deleted rows when the delete was fully executed as a metadata delete; empty to fall
     *         back to row-level DML
     */
    default OptionalLong deleteUsingMetadata(ConnectorSession session, ConnectorTableHandle handle,
            Optional<ConnectorExpression> predicate, String boundWriteMetadataIdentity,
            BooleanSupplier isCancelled) {
        return OptionalLong.empty();
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
     * {@link org.apache.doris.connector.spi.handle.NoOpConnectorTransaction}; the default
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
