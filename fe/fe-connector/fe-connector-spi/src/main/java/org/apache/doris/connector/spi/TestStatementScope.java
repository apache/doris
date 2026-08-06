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

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * TEST SUPPORT ONLY (compiled into fe-core with the api module, never used on a runtime path).
 *
 * <p>A {@link ConnectorStatementScope} that memoizes like the engine's real implementation
 * ({@code ConnectorStatementScopeImpl}: a {@link ConcurrentHashMap} backing) and additionally
 * FAILS LOUD on re-entrancy: calling {@link #computeIfAbsent} from inside a loader is the exact
 * violation the engine's scope forbids (same-map nested update is undefined behavior for a
 * {@code ConcurrentHashMap}), and connectors' own tests should catch it the moment it is
 * introduced. Replace per-connector test scopes with this class so the guard is shared.
 */
public final class TestStatementScope implements ConnectorStatementScope {

    private final ConcurrentHashMap<Object, Object> cache = new ConcurrentHashMap<>();
    private final ThreadLocal<Boolean> inLoader = ThreadLocal.withInitial(() -> Boolean.FALSE);

    @Override
    @SuppressWarnings("unchecked")
    public <T> T computeIfAbsent(Object key, Supplier<T> loader) {
        if (inLoader.get()) {
            throw new IllegalStateException(
                    "statement scope re-entered from within a loader: key=" + key
                            + " (connectors must resolve scope-backed prerequisites outside the loader)");
        }
        inLoader.set(Boolean.TRUE);
        try {
            return (T) cache.computeIfAbsent(key, k -> loader.get());
        } finally {
            inLoader.set(Boolean.FALSE);
        }
    }
}
