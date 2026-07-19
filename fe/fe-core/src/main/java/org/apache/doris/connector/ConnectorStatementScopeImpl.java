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

import org.apache.doris.connector.api.ConnectorStatementScope;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Statement-scoped memoization arena backing {@link ConnectorStatementScope}, hung on the per-statement
 * {@link org.apache.doris.nereids.StatementContext}.
 *
 * <p>Thread-safe by a backing {@link ConcurrentHashMap}: a scan's off-thread pumps (streaming /
 * partition-batch) reuse the single {@link org.apache.doris.connector.api.ConnectorSession} built on the
 * request thread and so reach this same scope concurrently. {@code computeIfAbsent} gives every caller of
 * a key the same instance — required for the shared table object and for the delete supply map that scan
 * and write both mutate. The loaders used by connectors do not re-enter this scope, so the map's
 * single-key atomicity is safe here.</p>
 */
public class ConnectorStatementScopeImpl implements ConnectorStatementScope {

    private static final Logger LOG = LogManager.getLogger(ConnectorStatementScopeImpl.class);

    private final ConcurrentHashMap<String, Object> cache = new ConcurrentHashMap<>();
    private boolean closed;

    @Override
    @SuppressWarnings("unchecked")
    public <T> T computeIfAbsent(String key, Supplier<T> loader) {
        return (T) cache.computeIfAbsent(key, k -> loader.get());
    }

    /**
     * Closes every {@link AutoCloseable} value once, at statement end. Idempotent: guarded by {@code closed} so a
     * second trigger (the query-finish callback vs. a reused prepared statement's per-execution reset) is a
     * harmless no-op and no value is double-closed. Best-effort per value — a failure closing one does not abort
     * the rest — mirroring the isolation of the engine's query-finish callback registry. Runs after the scan
     * off-thread pumps have quiesced, so it does not race a concurrent {@code computeIfAbsent}.
     */
    @Override
    public synchronized void closeAll() {
        if (closed) {
            return;
        }
        closed = true;
        for (Object value : cache.values()) {
            if (value instanceof AutoCloseable) {
                try {
                    ((AutoCloseable) value).close();
                } catch (Exception e) {
                    LOG.warn("failed to close per-statement scope value of type {}; continuing",
                            value.getClass().getName(), e);
                }
            }
        }
        cache.clear();
    }
}
