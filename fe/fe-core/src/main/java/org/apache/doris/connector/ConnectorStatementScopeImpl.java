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

    private final ConcurrentHashMap<String, Object> cache = new ConcurrentHashMap<>();

    @Override
    @SuppressWarnings("unchecked")
    public <T> T computeIfAbsent(String key, Supplier<T> loader) {
        return (T) cache.computeIfAbsent(key, k -> loader.get());
    }
}
