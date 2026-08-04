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

import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorStatementScope;
import org.apache.doris.thrift.TIcebergDeleteFileDesc;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * Tests for {@link IcebergStatementScope}: per-statement read and writable table memo keying, plus the
 * rewritable-delete supply map that bridges the scan&rarr;write seam.
 */
public class IcebergStatementScopeTest {

    private static final Schema SCHEMA =
            new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));

    private static Table table(String name) {
        return new FakeIcebergTable(name, SCHEMA, PartitionSpec.unpartitioned(),
                "s3://b/db1/" + name, Collections.emptyMap());
    }

    private static Table mutableTable(String name) {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("test", Collections.emptyMap());
        catalog.createNamespace(Namespace.of("db1"));
        return catalog.createTable(TableIdentifier.of("db1", name), SCHEMA);
    }

    @Test
    public void sameStatementSharesOneLoadedTable() {
        // Read metadata and scan planning resolve one frozen view through sharedTable; sharing one scope
        // collapses them onto one load and hands each the same statement instance.
        // MUTATION: not memoizing -> two loads / two instances -> red.
        ScopeSession session = new ScopeSession(7L, "q1", new TestStatementScope());
        AtomicInteger loads = new AtomicInteger();
        Table t1 = IcebergStatementScope.sharedTable(session, "db1", "t", () -> {
            loads.incrementAndGet();
            return table("t");
        });
        Table t2 = IcebergStatementScope.sharedTable(session, "db1", "t", () -> {
            loads.incrementAndGet();
            return table("t");
        });
        Assertions.assertSame(t1, t2, "same statement + table -> one shared instance");
        Assertions.assertEquals(1, loads.get(), "loaded once per statement");
    }

    @Test
    public void sameStatementSharesOneWritableTable() {
        ScopeSession session = new ScopeSession(7L, "q1", new TestStatementScope());
        AtomicInteger loads = new AtomicInteger();
        Supplier<Table> loader = () -> {
            loads.incrementAndGet();
            return table("t");
        };

        Table planned = IcebergStatementScope.sharedWritableTable(session, "db1", "t", loader);
        Table transaction = IcebergStatementScope.sharedWritableTable(session, "db1", "t", loader);

        Assertions.assertSame(planned, transaction);
        Assertions.assertEquals(1, loads.get());
    }

    @Test
    public void differentQueryIdIsolatesTheLoad() {
        // A reused prepared statement runs each EXECUTE under its own queryId, so one execution never sees
        // another's table even on the same scope object.
        TestStatementScope scope = new TestStatementScope();
        Table a = IcebergStatementScope.sharedTable(new ScopeSession(7L, "q1", scope), "db1", "t", () -> table("t"));
        Table b = IcebergStatementScope.sharedTable(new ScopeSession(7L, "q2", scope), "db1", "t", () -> table("t"));
        Assertions.assertNotSame(a, b, "different queryId -> isolated load");
    }

    @Test
    public void readGenerationSurvivesAnotherStatementMutatingCachedBaseTable() {
        Table mutable = mutableTable("t");
        Table firstStatement = IcebergStatementScope.sharedTable(
                new ScopeSession(7L, "q1", new TestStatementScope()), "db1", "t", () -> mutable);

        mutable.updateSchema().addColumn("later", Types.StringType.get()).commit();

        Assertions.assertNull(firstStatement.schema().findField("later"),
                "a statement read view must not refresh when another statement mutates the shared raw table");
        Table laterStatement = IcebergStatementScope.sharedTable(
                new ScopeSession(7L, "q2", new TestStatementScope()), "db1", "t", () -> mutable);
        Assertions.assertNotNull(laterStatement.schema().findField("later"));
    }

    @Test
    public void differentCatalogIdIsolatesTheLoad() {
        // A cross-catalog MERGE resolves the two catalogs' tables independently (the key carries the catalog id).
        TestStatementScope scope = new TestStatementScope();
        Table a = IcebergStatementScope.sharedTable(new ScopeSession(1L, "q1", scope), "db1", "t", () -> table("t"));
        Table b = IcebergStatementScope.sharedTable(new ScopeSession(2L, "q1", scope), "db1", "t", () -> table("t"));
        Assertions.assertNotSame(a, b, "different catalog id -> isolated load");
    }

    @Test
    public void underNoneScopeLoadsEveryTime() {
        // No live statement scope: each call loads (byte-identical to the pre-scope offline behavior).
        ScopeSession none = new ScopeSession(7L, "q1", ConnectorStatementScope.NONE);
        AtomicInteger loads = new AtomicInteger();
        IcebergStatementScope.sharedTable(none, "db1", "t", () -> {
            loads.incrementAndGet();
            return table("t");
        });
        IcebergStatementScope.sharedTable(none, "db1", "t", () -> {
            loads.incrementAndGet();
            return table("t");
        });
        Assertions.assertEquals(2, loads.get(), "NONE -> load every time");
    }

    @Test
    public void underNullSessionLoadsEveryTime() {
        // Offline / direct-construction (null session): sharedTable loads every time, byte-identical to the
        // pre-scope behavior. The null branch now lives in the shared helper; assert it still holds at the seam.
        AtomicInteger loads = new AtomicInteger();
        IcebergStatementScope.sharedTable(null, "db1", "t", () -> {
            loads.incrementAndGet();
            return table("t");
        });
        IcebergStatementScope.sharedTable(null, "db1", "t", () -> {
            loads.incrementAndGet();
            return table("t");
        });
        Assertions.assertEquals(2, loads.get(), "null session -> load every time");
    }

    @Test
    public void sharedTableKeyReproducesLegacyPrefixByteForByte() {
        // PARITY (PR-2): sharedTable now delegates to ConnectorStatementScopes.resolveInStatement; the memo key it
        // hands the scope MUST stay byte-identical to the pre-delegation
        // "iceberg.table:" + catalogId + ":" + db + ":" + table + ":" + queryId, or funnel hits/misses shift.
        // MUTATION: a different namespace, a dropped field, or a reordered field -> key differs -> red.
        KeyCapturingScope scope = new KeyCapturingScope();
        IcebergStatementScope.sharedTable(new ScopeSession(7L, "q1", scope), "db1", "t", () -> table("t"));
        Assertions.assertEquals("iceberg.table:7:db1:t:q1", scope.lastKey,
                "delegated key must reproduce the legacy iceberg.table prefix byte-for-byte");
    }

    @Test
    public void rewritableDeleteSupplyIsSharedPerStatementAndIsolatedPerCatalog() {
        // The scan seam and the write seam of one statement (same catalog + queryId) share ONE supply map; a
        // cross-catalog MERGE keeps each catalog's supply isolated. MUTATION: dropping the catalog id from the
        // key -> the two catalogs collide -> red.
        TestStatementScope scope = new TestStatementScope();
        Map<String, List<TIcebergDeleteFileDesc>> supplyScan =
                IcebergStatementScope.rewritableDeleteSupply(new ScopeSession(1L, "q1", scope));
        Map<String, List<TIcebergDeleteFileDesc>> supplyWrite =
                IcebergStatementScope.rewritableDeleteSupply(new ScopeSession(1L, "q1", scope));
        Assertions.assertSame(supplyScan, supplyWrite, "scan and write of one statement share one supply map");

        Map<String, List<TIcebergDeleteFileDesc>> supplyOtherCatalog =
                IcebergStatementScope.rewritableDeleteSupply(new ScopeSession(2L, "q1", scope));
        Assertions.assertNotSame(supplyScan, supplyOtherCatalog, "a different catalog (cross-catalog MERGE) is isolated");
    }

    @Test
    public void allNamespacesArePrefixedWithConnectorType() throws Exception {
        // NORM (self-extending): reflect over every "*_NAMESPACE" constant this connector declares and assert each
        // is prefixed with the connector's ConnectorProvider.getType() ("iceberg."). Source-prefixing keeps the
        // namespaces distinct across connectors on a heterogeneous gateway (no ClassCastException on the shared
        // coordinate). Reflecting means a NEW namespace is auto-covered; a forgotten prefix or a getType() drift
        // turns this red with no test upkeep.
        String prefix = new IcebergConnectorProvider().getType() + ".";
        int checked = 0;
        for (Field f : IcebergStatementScope.class.getDeclaredFields()) {
            if (Modifier.isStatic(f.getModifiers()) && f.getType() == String.class
                    && f.getName().endsWith("_NAMESPACE")) {
                f.setAccessible(true);
                String ns = (String) f.get(null);
                Assertions.assertTrue(ns.startsWith(prefix),
                        f.getName() + " (\"" + ns + "\") must be prefixed with the connector type \"" + prefix + "\"");
                checked++;
            }
        }
        Assertions.assertTrue(checked > 0, "expected at least one *_NAMESPACE constant to guard");
    }

    /** A scope that records the last key handed to {@link #computeIfAbsent}, for the byte-key parity assertion. */
    private static final class KeyCapturingScope implements ConnectorStatementScope {
        private String lastKey;

        @Override
        public <T> T computeIfAbsent(String key, Supplier<T> loader) {
            lastKey = key;
            return loader.get();
        }
    }

    /** Minimal {@link ConnectorSession} carrying a catalog id, queryId and scope for the key + memo assertions. */
    private static final class ScopeSession implements ConnectorSession {
        private final long catalogId;
        private final String queryId;
        private final ConnectorStatementScope scope;

        ScopeSession(long catalogId, String queryId, ConnectorStatementScope scope) {
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
            // Deliberately != queryId. The memo key MUST use the per-EXECUTION queryId (cross-query isolation), not
            // the stable per-connection sessionId; a queryId->sessionId swap in the key would share a table across
            // queries of one connection and MUST turn sharedTableKeyReproducesLegacyPrefixByteForByte red.
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
