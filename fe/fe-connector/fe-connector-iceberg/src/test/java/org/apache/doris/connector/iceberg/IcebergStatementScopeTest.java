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

import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorStatementScope;
import org.apache.doris.thrift.TIcebergDeleteFileDesc;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * Tests for {@link IcebergStatementScope}: the per-statement table memo keying (catalog id + db + table +
 * queryId) that lets a statement's read/scan/write share ONE loaded table, and the rewritable-delete supply
 * map keying (catalog id + queryId) that bridges the scan&rarr;write seam.
 */
public class IcebergStatementScopeTest {

    private static final Schema SCHEMA =
            new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));

    private static Table table(String name) {
        return new FakeIcebergTable(name, SCHEMA, PartitionSpec.unpartitioned(),
                "s3://b/db1/" + name, Collections.emptyMap());
    }

    @Test
    public void sameStatementSharesOneLoadedTable() {
        // The read, scan and write resolvers of one statement resolve the same table through sharedTable; sharing
        // one scope collapses them onto one load and hands each the SAME instance (read/write share).
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
    public void differentQueryIdIsolatesTheLoad() {
        // A reused prepared statement runs each EXECUTE under its own queryId, so one execution never sees
        // another's table even on the same scope object.
        TestStatementScope scope = new TestStatementScope();
        Table a = IcebergStatementScope.sharedTable(new ScopeSession(7L, "q1", scope), "db1", "t", () -> table("t"));
        Table b = IcebergStatementScope.sharedTable(new ScopeSession(7L, "q2", scope), "db1", "t", () -> table("t"));
        Assertions.assertNotSame(a, b, "different queryId -> isolated load");
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
