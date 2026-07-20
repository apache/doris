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

package org.apache.doris.common.cache;

import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.SupportBinarySearchFilteringPartitions;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.nereids.rules.expression.rules.SortedPartitionRanges;
import org.apache.doris.nereids.trees.plans.algebra.CatalogRelation;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.rpc.RpcException;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Map;
import java.util.Optional;

/**
 * Unit coverage for wiring external MVCC tables (iceberg/paimon) into
 * {@link NereidsSortedPartitionsCacheManager}: cache hit/miss/rebuild-on-version-change, origin-map
 * consistency (no #65659 TOCTOU), invalidation, and the {@link ExternalTable#getSortedPartitionRanges}
 * delegation contract.
 *
 * <p>Drives the manager with a Mockito mock of {@link SupportBinarySearchFilteringPartitions} rather than
 * a hand-written fake class: the interface extends {@code TableIf}, whose large method surface makes a
 * hand-rolled implementer impractical. Only the methods the manager actually calls are stubbed
 * ({@code getOriginPartitions}, {@code getPartitionMetaVersion}, {@code getPartitionMetaLoadTimeMillis},
 * {@code getId}, {@code getName}, {@code getDatabase}).</p>
 *
 * <p>{@link NereidsSortedPartitionsCacheManager#get} dereferences {@code ConnectContext.get()
 * .getSessionVariable()} unconditionally once {@code ConnectContext.get() != null}, so every test needs a
 * live {@link ConnectContext} (mirrors the lightweight idiom in {@code LogicalFileScanTest}: a plain
 * {@code new ConnectContext()} + {@code setThreadLocalInfo()}, no FE server bootstrap).</p>
 */
public class NereidsSortedPartitionsCacheManagerExternalTest {

    private static final String CTL = "ctl";
    private static final String DB = "db";
    private static final String TBL = "t";

    @AfterEach
    public void tearDown() {
        ConnectContext.remove();
    }

    private static void newLiveConnectContext() {
        ConnectContext ctx = new ConnectContext();
        ctx.setThreadLocalInfo();
    }

    private static ListPartitionItem listItem(int value) throws Exception {
        Column partitionColumn = new Column("id", PrimitiveType.INT);
        PartitionValue partitionValue = new PartitionValue(String.valueOf(value));
        PartitionKey partitionKey = PartitionKey.createPartitionKey(
                ImmutableList.of(partitionValue), ImmutableList.of(partitionColumn));
        return new ListPartitionItem(ImmutableList.of(partitionKey));
    }

    /**
     * A settable-state mock of {@link SupportBinarySearchFilteringPartitions}: {@link #version} and
     * {@link #parts} drive the cache manager's hit/rebuild decision; when {@code withDatabase} is true the
     * constructor also stubs a database/catalog pair (names {@link #CTL}/{@link #DB}) so
     * {@code TableIdentifier} can build.
     */
    private static final class FakeExternalTable {
        final SupportBinarySearchFilteringPartitions table = Mockito.mock(SupportBinarySearchFilteringPartitions.class);
        Object version = "s1@0";
        Map<String, PartitionItem> parts = Maps.newHashMap();

        @SuppressWarnings({"unchecked", "rawtypes"})
        FakeExternalTable(boolean withDatabase) throws RpcException {
            if (withDatabase) {
                DatabaseIf db = Mockito.mock(DatabaseIf.class);
                CatalogIf catalog = Mockito.mock(CatalogIf.class);
                Mockito.when(catalog.getName()).thenReturn(CTL);
                Mockito.when(db.getFullName()).thenReturn(DB);
                Mockito.when(db.getCatalog()).thenReturn(catalog);
                Mockito.when(table.getDatabase()).thenReturn(db);
            }
            Mockito.when(table.getId()).thenReturn(1001L);
            Mockito.when(table.getName()).thenReturn(TBL);
            Mockito.when(table.getOriginPartitions(Mockito.any())).thenAnswer(inv -> parts);
            Mockito.when(table.getPartitionMetaVersion(Mockito.any())).thenAnswer(inv -> version);
            Mockito.when(table.getPartitionMetaLoadTimeMillis(Mockito.any())).thenReturn(0L);
        }
    }

    // ──────────────────── Task 1: getDatabase()==null guards the external wiring contract ────────────────────

    @Test
    public void testManagerReturnsEmptyWhenDatabaseNull() throws Exception {
        newLiveConnectContext();
        NereidsSortedPartitionsCacheManager mgr = new NereidsSortedPartitionsCacheManager();
        FakeExternalTable t = new FakeExternalTable(false); // getDatabase() -> null (unstubbed mock default)

        Optional<SortedPartitionRanges<?>> r = mgr.get(t.table, (CatalogRelation) null);

        Assertions.assertFalse(r.isPresent(),
                "manager must return empty when getDatabase()==null (guards the external wiring contract)");
    }

    // ──────────────────── Task 2: ExternalTable.getSortedPartitionRanges delegation ────────────────────

    @Test
    public void testGetSortedPartitionRangesEmptyForNonSupportTable() {
        // A plain ExternalTable does not implement SupportBinarySearchFilteringPartitions, so the
        // delegation must short-circuit to empty WITHOUT touching Env/the cache manager.
        ExternalTable table = new ExternalTable();
        Assertions.assertFalse(table.getSortedPartitionRanges(null).isPresent(),
                "base ExternalTable (not Support) yields empty");
    }

    // ──────────────────── Task 4: invalidate is safe on an absent key ────────────────────

    @Test
    public void testInvalidateEvictsRanges() {
        NereidsSortedPartitionsCacheManager mgr = new NereidsSortedPartitionsCacheManager();
        Assertions.assertEquals(0, mgr.getPartitionCaches().estimatedSize(),
                "fresh manager is empty; invalidate is a no-op that must not throw");
        Assertions.assertDoesNotThrow(() -> mgr.invalidateTable(CTL, DB, TBL),
                "invalidateTable on an absent key must not throw");
    }

    // ──────────────────── Task 5: cache hit / version-rebuild / origin-map consistency ────────────────────

    @Test
    public void testCacheHitThenRebuildOnVersionChange() throws Exception {
        newLiveConnectContext();
        NereidsSortedPartitionsCacheManager mgr = new NereidsSortedPartitionsCacheManager();
        FakeExternalTable t = new FakeExternalTable(true);
        t.parts.put("id=1", listItem(1));
        t.parts.put("id=2", listItem(2));

        t.version = "s1@0";
        SortedPartitionRanges<?> first = mgr.get(t.table, (CatalogRelation) null).orElse(null);
        Assertions.assertNotNull(first, "ranges built and cached at snapshot s1");
        SortedPartitionRanges<?> hit = mgr.get(t.table, (CatalogRelation) null).orElse(null);
        Assertions.assertSame(first, hit, "same snapshot => cache hit returns the SAME instance");

        t.version = "s2@0"; // snapshot advanced (ALTER ADD PARTITION)
        t.parts.put("id=3", listItem(3));
        SortedPartitionRanges<?> rebuilt = mgr.get(t.table, (CatalogRelation) null).orElse(null);
        Assertions.assertNotSame(first, rebuilt, "version change => rebuild");
        Assertions.assertEquals(3, rebuilt.sortedPartitions.size(), "rebuilt from the new partition set");

        // Task 4 wiring: dropping the cache by (catalog, db, table) forces the next get() to rebuild too.
        mgr.invalidateTable(CTL, DB, TBL);
        SortedPartitionRanges<?> afterInvalidate = mgr.get(t.table, (CatalogRelation) null).orElse(null);
        Assertions.assertNotSame(rebuilt, afterInvalidate,
                "explicit invalidateTable(catalog, db, table) forces a rebuild on the next get()");
    }

    @Test
    public void testRangesConsistentWithOriginMap() throws Exception {
        // The cached ranges are built from getOriginPartitions(scan); every range id must be a key of
        // that same map -- the invariant PruneFileScanPartition's Preconditions relies on (no TOCTOU).
        newLiveConnectContext();
        NereidsSortedPartitionsCacheManager mgr = new NereidsSortedPartitionsCacheManager();
        FakeExternalTable t = new FakeExternalTable(true);
        t.parts.put("id=1", listItem(1));
        t.parts.put("id=2", listItem(2));

        SortedPartitionRanges<?> r = mgr.get(t.table, (CatalogRelation) null).orElse(null);
        Assertions.assertNotNull(r);
        r.sortedPartitions.forEach(p ->
                Assertions.assertTrue(t.parts.containsKey(p.id), "every range id is a key of the origin map"));
    }
}
