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

import java.util.function.Supplier;

/**
 * A per-statement memoization arena, living exactly as long as one SQL statement (read + write).
 *
 * <p>It lets a connector load a table (and derive per-statement state) once and share that single
 * object across every resolver in the statement — read metadata, scan planning, write shaping,
 * begin-write. Reached from a connector via {@link ConnectorSession#getStatementScope()}.</p>
 *
 * <p>Neutral SPI: the engine owns the physical home (the scope is hung on the engine's per-statement
 * context) and the connector stores its own connector-typed values under string keys as opaque
 * {@code Object}s — fe-core never sees a connector type. Values are session-bound and reclaimed with
 * the statement, never promoted into any cross-session / cross-identity structure.</p>
 *
 * <p>{@link #NONE} is the off-context default (no live statement: offline planning, tests, and any
 * {@link ConnectorSession} that does not override {@link ConnectorSession#getStatementScope()}). It
 * never memoizes, so every call runs the loader — byte-identical to loading every time.</p>
 */
public interface ConnectorStatementScope {

    /**
     * Returns the value cached under {@code key}, computing it with {@code loader} on first access and
     * caching it for the rest of the statement. Within one statement the same key returns the same
     * instance to every caller; under {@link #NONE} the loader runs on every call. Once the engine has
     * closed the scope ({@link #closeAll}) the statement is over: the engine's scope throws
     * {@link java.util.concurrent.CancellationException} instead of loading, so work that outlives its
     * statement stops rather than holding a value nobody closes.
     */
    <T> T computeIfAbsent(String key, Supplier<T> loader);

    /** Returns an already loaded statement value without loading or retaining anything on a miss. */
    default <T> T getIfPresent(String key) {
        return null;
    }

    /**
     * {@link #computeIfAbsent} for a value only planning reads, such as a memo that lets the identical scans of
     * one plan share their planned ranges. The engine drops such values once planning ends ({@link #endPlanning}),
     * so nothing they reference is held while the statement executes. A call after that computes and registers
     * the value again, which a scan planned at execution time (a partition batch planned through
     * {@code ConnectorScanPlanProvider#planScanForPartitionBatch}'s default) would do: the value then stays until
     * {@link #closeAll}. The value must hold nothing to close, because it is dropped without being closed. Under
     * {@link #NONE} the loader runs on every call.
     */
    default <T> T computeIfAbsentForPlanning(String key, Supplier<T> loader) {
        return computeIfAbsent(key, loader);
    }

    /**
     * Drops every value registered through {@link #computeIfAbsentForPlanning}. The engine calls it when the
     * planning of the statement ends, whether or not the plan succeeded; values registered through
     * {@link #computeIfAbsent} stay until {@link #closeAll}. The default is a no-op.
     */
    default void endPlanning() {
    }

    /**
     * Typed convenience over {@link #computeIfAbsent} for the ONE {@link ConnectorMetadata} a statement uses per
     * {@code key}. The engine's metadata funnel builds {@code key} (as {@code "metadata:" + catalogId}, plus the
     * owning connector's label for a heterogeneous gateway) and passes a {@code factory} that calls
     * {@code Connector#getMetadata(session)}; every read / scan / DDL / MVCC resolver of the statement then shares
     * the single memoized instance and the factory runs at most once per statement. Under {@link #NONE} the
     * factory runs on every call (byte-identical to building metadata every time).
     */
    default ConnectorMetadata getOrCreateMetadata(String key, Supplier<ConnectorMetadata> factory) {
        return computeIfAbsent(key, factory);
    }

    /**
     * Deterministically closes, at statement end, every value this scope holds that is {@link AutoCloseable} (a
     * memoized {@link ConnectorMetadata} is, via {@link java.io.Closeable}). Best-effort and log-and-continue: a
     * failure closing one value does not abort the rest. MUST be idempotent (close-once) in implementations — the
     * engine fires it from more than one locus (the query-finish callback, and a reused prepared statement's
     * per-execution reset). The default is a no-op, so {@link #NONE} — which memoizes nothing — stays inert.
     */
    default void closeAll() {
    }

    /** The no-op scope: never caches; each call invokes the loader (offline / no-context / tests). */
    ConnectorStatementScope NONE = new ConnectorStatementScope() {
        @Override
        public <T> T computeIfAbsent(String key, Supplier<T> loader) {
            return loader.get();
        }
    };
}
