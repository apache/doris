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

import org.apache.doris.connector.api.handle.ConnectorTransaction;

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

    // ──────────────────── Capability Queries ────────────────────

    /** Returns {@code true} if this connector supports INSERT operations. */
    default boolean supportsInsert() {
        return false;
    }

    /**
     * Returns {@code true} if this connector supports INSERT OVERWRITE (truncate-and-insert)
     * semantics. A connector that supports plain INSERT but not overwrite must keep this
     * {@code false} so callers reject the command up front (fail loud) instead of silently
     * degrading OVERWRITE to a plain append.
     */
    default boolean supportsInsertOverwrite() {
        return false;
    }

    /** Returns {@code true} if this connector supports DELETE operations. */
    default boolean supportsDelete() {
        return false;
    }

    /** Returns {@code true} if this connector supports MERGE (INSERT + DELETE) operations. */
    default boolean supportsMerge() {
        return false;
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
}
