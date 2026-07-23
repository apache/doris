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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * Tests for {@link ConnectorStatementScopes#resolveInStatement}: the shared per-statement resolver whose key is
 * {@code keyNamespace + ":" + catalogId + ":" + db + ":" + table + ":" + queryId}. It proves the memo collapses
 * repeat resolves within a statement, that each key axis (namespace / catalog id / db / table / queryId) isolates,
 * that a null session or {@link ConnectorStatementScope#NONE} degrades to load-every-time, and that the emitted
 * key is exactly the documented string (the byte contract every connector consumer depends on).
 */
public class ConnectorStatementScopesTest {

    @Test
    public void sameCoordinateResolvesOncePerStatement() {
        // Same (namespace, catalog, db, table, queryId) within one scope -> loader runs once, one shared instance.
        // MUTATION: not memoizing -> two loads / two instances -> red.
        RecordingMemoScope scope = new RecordingMemoScope();
        TestSession session = new TestSession(7L, "q1", scope);
        AtomicInteger loads = new AtomicInteger();
        Object a = ConnectorStatementScopes.resolveInStatement(session, "x.table", "db1", "t", () -> {
            loads.incrementAndGet();
            return new Object();
        });
        Object b = ConnectorStatementScopes.resolveInStatement(session, "x.table", "db1", "t", () -> {
            loads.incrementAndGet();
            return new Object();
        });
        Assertions.assertSame(a, b, "same coordinate -> one shared instance");
        Assertions.assertEquals(1, loads.get(), "loaded once per statement");
    }

    @Test
    public void differentQueryIdIsolatesTheLoad() {
        // A reused prepared statement runs each execution under its own queryId; the memo never crosses executions.
        RecordingMemoScope scope = new RecordingMemoScope();
        Object a = ConnectorStatementScopes.resolveInStatement(
                new TestSession(7L, "q1", scope), "x.table", "db1", "t", Object::new);
        Object b = ConnectorStatementScopes.resolveInStatement(
                new TestSession(7L, "q2", scope), "x.table", "db1", "t", Object::new);
        Assertions.assertNotSame(a, b, "different queryId -> isolated load");
    }

    @Test
    public void differentCatalogIdIsolatesTheLoad() {
        // A cross-catalog MERGE resolves the two catalogs' tables independently (the key carries the catalog id).
        RecordingMemoScope scope = new RecordingMemoScope();
        Object a = ConnectorStatementScopes.resolveInStatement(
                new TestSession(1L, "q1", scope), "x.table", "db1", "t", Object::new);
        Object b = ConnectorStatementScopes.resolveInStatement(
                new TestSession(2L, "q1", scope), "x.table", "db1", "t", Object::new);
        Assertions.assertNotSame(a, b, "different catalog id -> isolated load");
    }

    @Test
    public void differentDbOrTableIsolatesTheLoad() {
        RecordingMemoScope scope = new RecordingMemoScope();
        TestSession session = new TestSession(7L, "q1", scope);
        Object t1 = ConnectorStatementScopes.resolveInStatement(session, "x.table", "db1", "t", Object::new);
        Object t2 = ConnectorStatementScopes.resolveInStatement(session, "x.table", "db1", "u", Object::new);
        Object t3 = ConnectorStatementScopes.resolveInStatement(session, "x.table", "db2", "t", Object::new);
        Assertions.assertNotSame(t1, t2, "different table -> isolated load");
        Assertions.assertNotSame(t1, t3, "different db -> isolated load");
    }

    @Test
    public void differentNamespaceIsolatesValueTypes() {
        // The namespace guards a heterogeneous-gateway statement: two connectors sharing (db, table, catalog,
        // queryId) but storing different value types must not collide. Without namespacing, the second resolve
        // would return the first's memoized value cast to the wrong type -> ClassCastException.
        // MUTATION: dropping keyNamespace from the key -> ClassCastException here -> red.
        RecordingMemoScope scope = new RecordingMemoScope();
        TestSession session = new TestSession(7L, "q1", scope);
        String asString = ConnectorStatementScopes.resolveInStatement(
                session, "a.value", "db1", "t", () -> "stringValue");
        Integer asInt = ConnectorStatementScopes.resolveInStatement(
                session, "b.value", "db1", "t", () -> 42);
        Assertions.assertEquals("stringValue", asString, "namespace a keeps its String value");
        Assertions.assertEquals(Integer.valueOf(42), asInt, "namespace b keeps its Integer value (no collision)");
    }

    @Test
    public void nullSessionLoadsEveryTime() {
        // No session (offline / direct-construction tests): each call loads, byte-identical to loading every time.
        AtomicInteger loads = new AtomicInteger();
        ConnectorStatementScopes.resolveInStatement(null, "x.table", "db1", "t", () -> {
            loads.incrementAndGet();
            return new Object();
        });
        ConnectorStatementScopes.resolveInStatement(null, "x.table", "db1", "t", () -> {
            loads.incrementAndGet();
            return new Object();
        });
        Assertions.assertEquals(2, loads.get(), "null session -> load every time");
    }

    @Test
    public void noneScopeLoadsEveryTime() {
        // A live session whose scope is NONE (no per-statement context) also loads every time.
        TestSession session = new TestSession(7L, "q1", ConnectorStatementScope.NONE);
        AtomicInteger loads = new AtomicInteger();
        ConnectorStatementScopes.resolveInStatement(session, "x.table", "db1", "t", () -> {
            loads.incrementAndGet();
            return new Object();
        });
        ConnectorStatementScopes.resolveInStatement(session, "x.table", "db1", "t", () -> {
            loads.incrementAndGet();
            return new Object();
        });
        Assertions.assertEquals(2, loads.get(), "NONE scope -> load every time");
    }

    @Test
    public void keyIsNamespaceCatalogDbTableQueryId() {
        // The exact byte contract: keyNamespace + ":" + catalogId + ":" + db + ":" + table + ":" + queryId.
        // Every connector consumer's cross-statement isolation depends on this string; assert it verbatim.
        RecordingMemoScope scope = new RecordingMemoScope();
        ConnectorStatementScopes.resolveInStatement(
                new TestSession(7L, "q1", scope), "test.ns", "db1", "t", Object::new);
        Assertions.assertEquals("test.ns:7:db1:t:q1", scope.lastKey, "emitted key must match the documented format");
    }

    /** A statement scope that memoizes like the engine's real one and records the last key, for the assertions. */
    private static final class RecordingMemoScope implements ConnectorStatementScope {
        private final ConcurrentHashMap<String, Object> cache = new ConcurrentHashMap<>();
        private String lastKey;

        @Override
        @SuppressWarnings("unchecked")
        public <T> T computeIfAbsent(String key, Supplier<T> loader) {
            lastKey = key;
            return (T) cache.computeIfAbsent(key, k -> loader.get());
        }
    }

    /** Minimal {@link ConnectorSession} carrying a catalog id, queryId and scope for the key + memo assertions. */
    private static final class TestSession implements ConnectorSession {
        private final long catalogId;
        private final String queryId;
        private final ConnectorStatementScope scope;

        TestSession(long catalogId, String queryId, ConnectorStatementScope scope) {
            this.catalogId = catalogId;
            this.queryId = queryId;
            this.scope = scope;
        }

        @Override
        public long getCatalogId() {
            return catalogId;
        }

        @Override
        public String getQueryId() {
            return queryId;
        }

        @Override
        public String getSessionId() {
            // Deliberately != queryId so keyIsNamespaceCatalogDbTableQueryId pins the per-execution queryId
            // (cross-query isolation), not the stable per-connection sessionId; a queryId->sessionId swap in the
            // helper's key would share a table across queries of one connection and MUST turn that assertion red.
            return "session-" + queryId;
        }

        @Override
        public ConnectorStatementScope getStatementScope() {
            return scope;
        }

        @Override
        public String getUser() {
            return "u";
        }

        @Override
        public String getTimeZone() {
            return "UTC";
        }

        @Override
        public String getLocale() {
            return "en_US";
        }

        @Override
        public String getCatalogName() {
            return "c";
        }

        @Override
        public <T> T getProperty(String name, Class<T> type) {
            return null;
        }

        @Override
        public Map<String, String> getCatalogProperties() {
            return Collections.emptyMap();
        }
    }
}
