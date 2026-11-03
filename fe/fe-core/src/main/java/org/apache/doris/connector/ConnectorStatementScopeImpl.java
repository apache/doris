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

package org.apache.doris.connector;

import org.apache.doris.connector.spi.ConnectorStatementScope;
import org.apache.doris.datasource.plugin.CatalogStatementTransaction;

import com.google.common.base.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Statement-scoped memoization arena backing {@link ConnectorStatementScope}, hung on the per-statement
 * {@link org.apache.doris.nereids.StatementContext}.
 *
 * <p>Thread-safe by a backing {@link ConcurrentHashMap}: a scan's off-thread pumps (streaming /
 * partition-batch) reuse the single {@link org.apache.doris.connector.spi.ConnectorSession} built on the
 * request thread and so reach this same scope concurrently. {@code computeIfAbsent} gives every caller of
 * a key the same instance — required for the shared table object and for the delete supply map that scan
 * and write both mutate. Loaders MUST NOT re-enter this scope: a recursive {@code computeIfAbsent}
 * can throw {@code IllegalStateException} on a same-bin collision or silently drop entries during a
 * mid-computation resize. Connectors that need scope-backed state during scan planning nest a
 * separate connector-private memo map inside the scope, avoiding re-entry and preserving the
 * String-key SPI contract.</p>
 *
 * <p>A value registered through {@link #computeIfAbsentForPlanning} lives only until {@link #endPlanning}, which
 * {@link org.apache.doris.nereids.StatementContext#releasePlannerResources} calls once planning is over, whether or
 * not the plan succeeded.</p>
 */
public class ConnectorStatementScopeImpl implements ConnectorStatementScope {

    private static final Logger LOG = LogManager.getLogger(ConnectorStatementScopeImpl.class);

    private final ConcurrentHashMap<String, Object> cache = new ConcurrentHashMap<>();
    // Keys of the values only planning reads; endPlanning drops them.
    private final Set<String> planningKeys = ConcurrentHashMap.newKeySet();
    // Read without the lock by the off-thread pumps, which can still be loading when the statement ends.
    private volatile boolean closed;
    private boolean closeRequested;
    private int users;
    private final List<CompletableFuture<Void>> splitTasks = new ArrayList<>();

    /** Retain metadata while asynchronous split work (including read-transaction release) uses it. */
    public synchronized Runnable retain() {
        Preconditions.checkState(!closeRequested, "Cannot retain a closing connector statement scope");
        users++;
        return this::release;
    }

    public synchronized void registerSplitTask(CompletableFuture<Void> termination) {
        Preconditions.checkState(!closeRequested, "Cannot register work in a closing connector statement scope");
        splitTasks.add(termination);
    }

    /** Read transactions may be shared by all scans of the statement, so join every producer first. */
    public synchronized CompletableFuture<Void> getSplitTerminationFuture() {
        return CompletableFuture.allOf(splitTasks.toArray(new CompletableFuture<?>[0]));
    }

    private void release() {
        boolean finish;
        synchronized (this) {
            Preconditions.checkState(users > 0, "Unbalanced connector statement scope release");
            users--;
            finish = closeRequested && users == 0;
        }
        if (finish) {
            closeAll();
        }
    }

    /**
     * {@inheritDoc}
     *
     * <p>Once {@link #closeAll} disposes the scope, the loader does not run and a
     * {@link CancellationException} is thrown: a pump that outlives its statement (stopping a split producer only
     * flags it, and closing the query's task groups does not interrupt metadata IO) would otherwise load a value,
     * such as a borrowed table, that nothing closes any more. A value whose load raced the close is released here,
     * because closeAll may not have seen it.</p>
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> T computeIfAbsent(String key, Supplier<T> loader) {
        checkOpen(key);
        Object value = cache.computeIfAbsent(key, k -> {
            checkOpen(k);
            return loader.get();
        });
        if (closed) {
            // Whoever removes the entry, closeAll or this caller, releases the value, so it is released once.
            if (cache.remove(key, value)) {
                release(value);
            }
            throw closedScope(key);
        }
        return (T) value;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getIfPresent(String key) {
        return closed ? null : (T) cache.get(key);
    }

    /** Whether the scope refuses lookups; retained producers delay this transition until their cleanup exits. */
    public boolean isClosed() {
        return closed;
    }

    @Override
    public <T> T computeIfAbsentForPlanning(String key, Supplier<T> loader) {
        T value = computeIfAbsent(key, loader);
        Preconditions.checkState(!(value instanceof AutoCloseable),
                "planning value %s is dropped without being closed, so it must hold nothing to close", key);
        planningKeys.add(key);
        return value;
    }

    @Override
    public void endPlanning() {
        Iterator<String> keys = planningKeys.iterator();
        while (keys.hasNext()) {
            cache.remove(keys.next());
            keys.remove();
        }
    }

    /**
     * Tears the statement's scoped values down at statement end, in two ordered passes. Pass 1 finalizes any
     * {@link CatalogStatementTransaction} (rolling back a transaction the executor never committed — only a
     * mid-flight abort leaves one active); pass 2 closes every remaining {@link AutoCloseable} value (the
     * memoized metadata, etc.). Transactions are finalized BEFORE the metadata they were minted from is closed.
     * Idempotent: guarded by {@code closed} so a second trigger (the query-finish callback vs. a reused prepared
     * statement's per-execution reset) is a harmless no-op and no value is double-closed. Best-effort per value —
     * a failure on one does not abort the rest — mirroring the isolation of the engine's query-finish callback
     * registry. Retained split work defers disposal until it exits. Other loaders may still be running: each
     * entry is removed before its value is released, and a
     * pump that loads concurrently releases what it loaded itself (see {@link #computeIfAbsent}), so every value
     * is released exactly once and none is left behind.
     */
    @Override
    public void closeAll() {
        synchronized (this) {
            closeRequested = true;
            if (closed || users != 0) {
                return;
            }
            closed = true;
            splitTasks.clear();
        }
        // Pass 1: finalize the statement's write transaction(s) first, so a transaction aborted mid-flight is
        // rolled back / released before pass 2 closes the shared metadata instance it was minted from. On every
        // normal path the executor already finished the transaction, so this is a no-op.
        for (Map.Entry<String, Object> entry : cache.entrySet()) {
            if (entry.getValue() instanceof CatalogStatementTransaction
                    && cache.remove(entry.getKey(), entry.getValue())) {
                release(entry.getValue());
            }
        }
        // Pass 2: close every remaining AutoCloseable value once.
        for (Map.Entry<String, Object> entry : cache.entrySet()) {
            if (cache.remove(entry.getKey(), entry.getValue())) {
                release(entry.getValue());
            }
        }
    }

    private void checkOpen(String key) {
        if (closed) {
            throw closedScope(key);
        }
    }

    private static CancellationException closedScope(String key) {
        return new CancellationException("the statement has ended, its connector scope cannot load " + key);
    }

    // Finalizes a transaction and closes any other AutoCloseable value; a plain value holds nothing to release.
    private static void release(Object value) {
        if (value instanceof CatalogStatementTransaction) {
            try {
                ((CatalogStatementTransaction) value).finalizeAtStatementEnd();
            } catch (Exception e) {
                LOG.warn("failed to finalize per-statement transaction; continuing", e);
            }
        } else if (value instanceof AutoCloseable) {
            try {
                ((AutoCloseable) value).close();
            } catch (Exception e) {
                LOG.warn("failed to close per-statement scope value of type {}; continuing",
                        value.getClass().getName(), e);
            }
        }
    }
}
