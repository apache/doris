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

package org.apache.doris.connector.iceberg;

import org.apache.doris.connector.api.ConnectorStatementScope;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * A memoizing {@link ConnectorStatementScope} for iceberg unit tests. The iceberg connector module does not
 * depend on fe-core, so tests cannot use the engine's {@code ConnectorStatementScopeImpl}; this is a faithful
 * copy of it. Sharing one instance across a scan session and a write session mimics a single statement's scope,
 * so a test can prove the read/scan/write resolvers collapse onto one load and that the rewritable-delete supply
 * bridges scan&rarr;write.
 */
final class TestStatementScope implements ConnectorStatementScope {

    private final ConcurrentHashMap<String, Object> cache = new ConcurrentHashMap<>();

    @Override
    @SuppressWarnings("unchecked")
    public <T> T computeIfAbsent(String key, Supplier<T> loader) {
        return (T) cache.computeIfAbsent(key, k -> loader.get());
    }
}
