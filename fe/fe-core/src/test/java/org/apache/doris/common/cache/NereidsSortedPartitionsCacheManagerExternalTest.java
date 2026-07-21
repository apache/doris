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
import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.SupportBinarySearchFilteringPartitions;
import org.apache.doris.connector.api.mvcc.ConnectorMvccSnapshot;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.ExternalMetaCacheMgr;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.mvcc.PluginDrivenMvccExternalTable;
import org.apache.doris.datasource.mvcc.PluginDrivenMvccSnapshot;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.rules.expression.rules.SortedPartitionRanges;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.algebra.CatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan.SelectedPartitions;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.rpc.RpcException;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
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
 *
 * <p>The two tests at the bottom of this file additionally drive the REAL production wiring the
 * FakeExternalTable-based tests above bypass: {@code PluginDrivenMvccExternalTable#getOriginPartitions}
 * / {@code #getPartitionMetaVersion} / {@code #pinnedSnapshot} (via a {@code CALLS_REAL_METHODS} mock,
 * the same technique as {@code LogicalFileScanTest}), and {@code ExternalMetaCacheMgr#invalidateTable}'s
 * call into this manager (via a real {@link NereidsSortedPartitionsCacheManager} instance reached through
 * a mocked {@code Env}).</p>
 */
public class NereidsSortedPartitionsCacheManagerExternalTest {

    private static final String CTL = "ctl";
    private static final String DB = "db";
    private static final String TBL = "t";
    private static final long CATALOG_ID = 7L;

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

    // ──────────────────── Real production wiring: PluginDrivenMvccExternalTable ────────────────────
    //
    // The tests above drive the manager with a hand-stubbed SupportBinarySearchFilteringPartitions mock
    // and never touch PluginDrivenMvccExternalTable, so they miss the NEW wiring in
    // getOriginPartitions/getPartitionMetaVersion/pinnedSnapshot (PluginDrivenMvccExternalTable.java
    // around :628-651). This test drives those REAL method bodies: Mockito.CALLS_REAL_METHODS runs every
    // unstubbed method for real, so only the connector round-trip (loadSnapshot) and the identity fields
    // MvccTableInfo needs (getName/getDatabase) are stubbed -- mirroring the technique in
    // LogicalFileScanTest#computeOutputBindsThisReferencesOwnVersionNotLatest.

    @Test
    public void testPluginDrivenMvccExternalTableRealOriginPartitionsAndVersion() throws Exception {
        Map<String, PartitionItem> pinnedPartsT1 = Maps.newHashMap();
        pinnedPartsT1.put("id=1", listItem(1));
        ConnectorMvccSnapshot connectorSnapshotT1 = ConnectorMvccSnapshot.builder()
                .snapshotId(42L).schemaId(7L).build();
        PluginDrivenMvccSnapshot pinT1 = new PluginDrivenMvccSnapshot(
                connectorSnapshotT1, pinnedPartsT1, Maps.newHashMap());

        // A DIFFERENT pin for the SAME table at a second @tag reference. With two non-default versions
        // pinned and no default ("") entry, the version-BLIND lookup (StatementContext#getSnapshot(TableIf))
        // is ambiguous and gives up (see its javadoc); only the version-AWARE lookup that the
        // LogicalFileScan branch of pinnedSnapshot uses resolves the exact t1 reference. MUTATION:
        // collapsing pinnedSnapshot to the version-blind fallback makes this test observably diverge
        // (an unresolved pin sends getOrMaterialize to materializeLatest() on a field-less mock, or the
        // assertions below simply see the wrong values).
        ConnectorMvccSnapshot connectorSnapshotT2 = ConnectorMvccSnapshot.builder()
                .snapshotId(99L).schemaId(3L).build();
        PluginDrivenMvccSnapshot pinT2 = new PluginDrivenMvccSnapshot(
                connectorSnapshotT2, Maps.newHashMap(), Maps.newHashMap());

        // NOTE: table's default answer is CALLS_REAL_METHODS, so every stub below MUST use the
        // doReturn(...).when(table)... form (never when(table.foo()).thenReturn(...)) -- the latter would
        // evaluate table.foo() for REAL as part of recording the stub, exactly the pitfall Mockito spies
        // have, and several of these real bodies dereference fields this field-less mock never set.
        PluginDrivenMvccExternalTable table =
                Mockito.mock(PluginDrivenMvccExternalTable.class, Mockito.CALLS_REAL_METHODS);
        ExternalDatabase<?> database = Mockito.mock(ExternalDatabase.class);
        CatalogIf<?> catalog = Mockito.mock(CatalogIf.class);
        Mockito.doReturn(TBL).when(table).getName();
        Mockito.doReturn(database).when(table).getDatabase();
        Mockito.when(database.getFullName()).thenReturn(DB);
        Mockito.when(database.getCatalog()).thenReturn((CatalogIf) catalog);
        Mockito.when(catalog.getName()).thenReturn(CTL);
        // Not under test here (see LogicalFileScanTest precedent): bypass its real body, which needs
        // schema/catalog wiring this bare mock doesn't carry.
        Mockito.doReturn(SelectedPartitions.NOT_PRUNED).when(table).initSelectedPartitions(Mockito.any());

        TableScanParams tagT1 = new TableScanParams("tag", ImmutableMap.of(), ImmutableList.of("t1"));
        TableScanParams tagT2 = new TableScanParams("tag", ImmutableMap.of(), ImmutableList.of("t2"));
        Mockito.doReturn(pinT1).when(table).loadSnapshot(Optional.empty(), Optional.of(tagT1));
        Mockito.doReturn(pinT2).when(table).loadSnapshot(Optional.empty(), Optional.of(tagT2));

        ConnectContext ctx = new ConnectContext();
        StatementContext stmtCtx = new StatementContext(ctx, null);
        ctx.setStatementContext(stmtCtx);
        ctx.setThreadLocalInfo();
        try {
            // Pin via loadSnapshots (not a hand-rolled key) so the version key is computed by the SAME
            // function the lookup uses -- the test must not hand-roll a key and accidentally agree with
            // itself.
            stmtCtx.loadSnapshots(table, Optional.empty(), Optional.of(tagT1));
            stmtCtx.loadSnapshots(table, Optional.empty(), Optional.of(tagT2));

            LogicalFileScan scan = new LogicalFileScan(new RelationId(1), table,
                    Collections.singletonList(DB), Collections.emptyList(),
                    Optional.empty(), Optional.empty(), Optional.of(tagT1), Optional.empty());

            Map<?, PartitionItem> origin = table.getOriginPartitions(scan);
            Object version = table.getPartitionMetaVersion(scan);

            Assertions.assertEquals(pinnedPartsT1, origin,
                    "getOriginPartitions must dispatch through the LogicalFileScan branch of pinnedSnapshot "
                            + "and return exactly the t1 pin's partition map (not t2's, not latest's)");
            Assertions.assertEquals(new java.util.HashSet<>(pinnedPartsT1.keySet()), version,
                    "getPartitionMetaVersion must return the FROZEN partition NAME-SET of the t1 pin "
                            + "(a real snapshotId=42 no longer yields the <snapshotId>@<schemaId> token) -- "
                            + "resolved via the same LogicalFileScan branch, so it is t1's name set, not t2's "
                            + "(empty) or latest's");
        } finally {
            ConnectContext.remove();
        }
    }

    // ──────────────────── Cache B version is the frozen partition NAME-SET for ALL engines ─────
    //
    // getPartitionMetaVersion always returns the frozen partition NAME SET
    // (getOriginPartitions(scan).keySet(), copied) -- the SAME map the ranges are built from, so
    // version == exact content and Cache B rebuilds precisely when the partition set changes, never on a
    // stale set. This is uniform across hive (snapshotId == -1 sentinel), paimon and iceberg: the old
    // "<snapshotId>@<schemaId>" O(1) token was removed because it is unsafe wherever a connector's
    // listPartitions content is not a pure function of the snapshot id (iceberg non-RANGE: identity /
    // bucket / truncate / multi-field partitioning), whose nameToPartitionItem (Cache A) and snapshot-id
    // token (a DIFFERENT cache) expire independently. PluginDrivenMvccExternalTable no longer overrides
    // getSortedPartitionRanges (the -1 short-circuit added in 5c17b748880 was removed): every pin goes
    // through the inherited ExternalTable#getSortedPartitionRanges -> the cache manager.

    /**
     * Builds a live Env whose {@code getSortedPartitionsCacheManager()} returns {@code rangesCacheMgr},
     * mockStatic-scoped so {@code table.getSortedPartitionRanges(scan)} (now purely inherited from
     * {@link ExternalTable}, no override) can resolve {@code Env.getCurrentEnv()} on the real dispatch path.
     */
    private static MockedStatic<Env> mockEnvWithRangesCacheManager(NereidsSortedPartitionsCacheManager rangesCacheMgr) {
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getSortedPartitionsCacheManager()).thenReturn(rangesCacheMgr);
        MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class);
        envStatic.when(Env::getCurrentEnv).thenReturn(env);
        return envStatic;
    }

    /** Builds a {@code PluginDrivenMvccExternalTable} mock wired for the real getOriginPartitions/getPartitionMetaVersion/pinnedSnapshot bodies (same technique as the other real-wiring tests in this file). */
    private static PluginDrivenMvccExternalTable newRealWiringTable() {
        PluginDrivenMvccExternalTable table =
                Mockito.mock(PluginDrivenMvccExternalTable.class, Mockito.CALLS_REAL_METHODS);
        ExternalDatabase<?> database = Mockito.mock(ExternalDatabase.class);
        CatalogIf<?> catalog = Mockito.mock(CatalogIf.class);
        Mockito.doReturn(TBL).when(table).getName();
        Mockito.doReturn(database).when(table).getDatabase();
        Mockito.when(database.getFullName()).thenReturn(DB);
        Mockito.when(database.getCatalog()).thenReturn((CatalogIf) catalog);
        Mockito.when(catalog.getName()).thenReturn(CTL);
        Mockito.doReturn(SelectedPartitions.NOT_PRUNED).when(table).initSelectedPartitions(Mockito.any());
        return table;
    }

    /** Pins {@code pin} onto a FRESH ConnectContext/StatementContext ("a new query") and returns the scan built over it. */
    private static LogicalFileScan pinLatestAndBuildScan(PluginDrivenMvccExternalTable table, PluginDrivenMvccSnapshot pin) {
        ConnectContext ctx = new ConnectContext();
        StatementContext stmtCtx = new StatementContext(ctx, null);
        ctx.setStatementContext(stmtCtx);
        ctx.setThreadLocalInfo();
        // B5a implicit query-begin (latest) pin -- mirrors a plain (no @tag/@branch/time-travel) hive scan.
        Mockito.doReturn(pin).when(table).loadSnapshot(Optional.empty(), Optional.empty());
        stmtCtx.loadSnapshots(table, Optional.empty(), Optional.empty());
        return new LogicalFileScan(new RelationId(1), table,
                Collections.singletonList(DB), Collections.emptyList(),
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
    }

    private static PluginDrivenMvccSnapshot sentinelPin(Map<String, PartitionItem> parts) {
        ConnectorMvccSnapshot sentinelSnapshot = ConnectorMvccSnapshot.builder()
                .snapshotId(-1L).schemaId(0L).build();
        return new PluginDrivenMvccSnapshot(sentinelSnapshot, parts, Maps.newHashMap());
    }

    /**
     * A pin carrying a REAL (non-sentinel) connector snapshot id -- e.g. iceberg. Used to prove Cache B's
     * version token is the frozen partition NAME-SET even for {@code snapshotId != -1}: the snapshot id is
     * held CONSTANT across queries while the partition set changes, exactly the iceberg non-RANGE hazard
     * where Cache A (listPartitions) and the snapshot-id token expire independently.
     */
    private static PluginDrivenMvccSnapshot realSnapshotPin(Map<String, PartitionItem> parts) {
        ConnectorMvccSnapshot realSnapshot = ConnectorMvccSnapshot.builder()
                .snapshotId(42L).schemaId(7L).build();
        return new PluginDrivenMvccSnapshot(realSnapshot, parts, Maps.newHashMap());
    }

    @Test
    public void testGetSortedPartitionRangesPresentForSnapshotLessNonEmptyPartitions() throws Exception {
        Map<String, PartitionItem> pinnedParts = Maps.newHashMap();
        pinnedParts.put("id=1", listItem(1));
        pinnedParts.put("id=2", listItem(2));
        PluginDrivenMvccExternalTable table = newRealWiringTable();

        NereidsSortedPartitionsCacheManager rangesCacheMgr = new NereidsSortedPartitionsCacheManager();
        try (MockedStatic<Env> envStatic = mockEnvWithRangesCacheManager(rangesCacheMgr)) {
            LogicalFileScan scan = pinLatestAndBuildScan(table, sentinelPin(pinnedParts));

            Optional<SortedPartitionRanges<String>> ranges = table.getSortedPartitionRanges(scan);

            Assertions.assertTrue(ranges.isPresent(),
                    "snapshotId == -1 (hive sentinel) with a non-empty partition set must now use Cache B, "
                            + "keyed by the partition NAME-SET version token");
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    public void testGetSortedPartitionRangesRebuildsWhenSnapshotLessNameSetChanges() throws Exception {
        PluginDrivenMvccExternalTable table = newRealWiringTable();
        NereidsSortedPartitionsCacheManager rangesCacheMgr = new NereidsSortedPartitionsCacheManager();
        try (MockedStatic<Env> envStatic = mockEnvWithRangesCacheManager(rangesCacheMgr)) {
            Map<String, PartitionItem> partsAtQuery1 = Maps.newHashMap();
            partsAtQuery1.put("id=1", listItem(1));
            partsAtQuery1.put("id=2", listItem(2));
            LogicalFileScan scan1 = pinLatestAndBuildScan(table, sentinelPin(partsAtQuery1));
            SortedPartitionRanges<String> first = table.getSortedPartitionRanges(scan1).orElse(null);
            Assertions.assertNotNull(first, "ranges built and cached at the first (2-partition) name set");
            Assertions.assertEquals(2, first.sortedPartitions.size());

            // A second query ("ALTER ADD PARTITION" between queries): the pin's partition NAME SET grew.
            // The manager's Objects.equals compares the two HashSet version tokens by CONTENT, so it must
            // detect this change even though snapshotId is still the constant -1 on both pins.
            Map<String, PartitionItem> partsAtQuery2 = Maps.newHashMap();
            partsAtQuery2.put("id=1", listItem(1));
            partsAtQuery2.put("id=2", listItem(2));
            partsAtQuery2.put("id=3", listItem(3));
            LogicalFileScan scan2 = pinLatestAndBuildScan(table, sentinelPin(partsAtQuery2));
            SortedPartitionRanges<String> rebuilt = table.getSortedPartitionRanges(scan2).orElse(null);

            Assertions.assertNotNull(rebuilt, "ranges rebuilt at the second (3-partition) name set");
            Assertions.assertNotSame(first, rebuilt,
                    "name-set change at a constant snapshotId==-1 must still trigger a rebuild "
                            + "(the version token is content-derived from the partition names, not the snapshot id)");
            Assertions.assertEquals(3, rebuilt.sortedPartitions.size(), "rebuilt from the new partition set");
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    public void testGetSortedPartitionRangesRebuildsWhenRealSnapshotNameSetChanges() throws Exception {
        // Real-snapshot (snapshotId != -1) coherence: the version token is the frozen partition NAME-SET
        // for ALL engines, not just hive. This is the iceberg non-RANGE hazard -- the pin's
        // nameToPartitionItem (served by listPartitions / Cache A) can advance while the connector snapshot
        // id stays CONSTANT, because the two are backed by INDEPENDENTLY-expiring caches. Under the removed
        // "<snapshotId>@<schemaId>" token both queries would key "42@7" and serve a STALE HIT (silent
        // under-inclusive pruning); under the name-set token the grown partition set forces a rebuild.
        PluginDrivenMvccExternalTable table = newRealWiringTable();
        NereidsSortedPartitionsCacheManager rangesCacheMgr = new NereidsSortedPartitionsCacheManager();
        try (MockedStatic<Env> envStatic = mockEnvWithRangesCacheManager(rangesCacheMgr)) {
            Map<String, PartitionItem> partsAtQuery1 = Maps.newHashMap();
            partsAtQuery1.put("id=1", listItem(1));
            partsAtQuery1.put("id=2", listItem(2));
            LogicalFileScan scan1 = pinLatestAndBuildScan(table, realSnapshotPin(partsAtQuery1));
            SortedPartitionRanges<String> first = table.getSortedPartitionRanges(scan1).orElse(null);
            Assertions.assertNotNull(first, "ranges built and cached at the first (2-partition) name set");
            Assertions.assertEquals(2, first.sortedPartitions.size());

            // Same query again: IDENTICAL name set at the SAME real snapshot id => cache HIT (same instance).
            LogicalFileScan scanHit = pinLatestAndBuildScan(table, realSnapshotPin(partsAtQuery1));
            SortedPartitionRanges<String> hit = table.getSortedPartitionRanges(scanHit).orElse(null);
            Assertions.assertSame(first, hit,
                    "unchanged name set at a real snapshotId=42 => cache HIT returns the SAME instance");

            // A later query whose Cache A refreshed to include a new partition while the connector snapshot
            // id is STILL 42: the name set grew, so Cache B must rebuild even though "42@7" is unchanged.
            Map<String, PartitionItem> partsAtQuery2 = Maps.newHashMap();
            partsAtQuery2.put("id=1", listItem(1));
            partsAtQuery2.put("id=2", listItem(2));
            partsAtQuery2.put("id=3", listItem(3));
            LogicalFileScan scan2 = pinLatestAndBuildScan(table, realSnapshotPin(partsAtQuery2));
            SortedPartitionRanges<String> rebuilt = table.getSortedPartitionRanges(scan2).orElse(null);
            Assertions.assertNotNull(rebuilt, "ranges rebuilt at the second (3-partition) name set");
            Assertions.assertNotSame(first, rebuilt,
                    "name-set change at a CONSTANT real snapshotId=42 must still trigger a rebuild "
                            + "(the removed <snapshotId>@<schemaId> token would have served a stale hit)");
            Assertions.assertEquals(3, rebuilt.sortedPartitions.size(), "rebuilt from the new partition set");
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    public void testGetSortedPartitionRangesEmptyForSnapshotLessEmptyPartitions() throws Exception {
        PluginDrivenMvccExternalTable table = newRealWiringTable();
        NereidsSortedPartitionsCacheManager rangesCacheMgr = new NereidsSortedPartitionsCacheManager();
        try (MockedStatic<Env> envStatic = mockEnvWithRangesCacheManager(rangesCacheMgr)) {
            LogicalFileScan scan = pinLatestAndBuildScan(table, sentinelPin(Maps.newHashMap()));

            Optional<SortedPartitionRanges<String>> ranges = table.getSortedPartitionRanges(scan);

            Assertions.assertFalse(ranges.isPresent(),
                    "an empty partition set (snapshot-less or not) yields no ranges to cache "
                            + "(SortedPartitionRanges.build(emptyMap) == null)");
        } finally {
            ConnectContext.remove();
        }
    }

    // ──────────────────── Real production wiring: ExternalMetaCacheMgr.invalidateTable ────────────────────

    @Test
    public void testExternalMetaCacheMgrInvalidateTableDropsRangesCacheEntry() throws Exception {
        // Drives the REAL ExternalMetaCacheMgr.invalidateTable(...) (the new call at
        // ExternalMetaCacheMgr.java:217-220), not NereidsSortedPartitionsCacheManager directly, so the
        // production wiring between the two managers is exercised end-to-end.
        newLiveConnectContext();
        NereidsSortedPartitionsCacheManager rangesCacheMgr = new NereidsSortedPartitionsCacheManager();
        FakeExternalTable t = new FakeExternalTable(true);
        t.parts.put("id=1", listItem(1));
        Assertions.assertTrue(rangesCacheMgr.get(t.table, (CatalogRelation) null).isPresent(),
                "ranges built and cached before invalidation");
        Assertions.assertEquals(1, rangesCacheMgr.getPartitionCaches().estimatedSize(),
                "one entry cached before invalidation");

        long catalogId = 7L;
        Env env = Mockito.mock(Env.class);
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        CatalogIf catalog = Mockito.mock(CatalogIf.class);
        Mockito.when(catalog.getName()).thenReturn(CTL);
        Mockito.when(catalogMgr.getCatalog(catalogId)).thenReturn(catalog);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        Mockito.when(env.getSortedPartitionsCacheManager()).thenReturn(rangesCacheMgr);

        ExternalMetaCacheMgr metaCacheMgr = new ExternalMetaCacheMgr(true);
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            metaCacheMgr.invalidateTable(catalogId, DB, TBL);
        }

        Assertions.assertEquals(0, rangesCacheMgr.getPartitionCaches().estimatedSize(),
                "ExternalMetaCacheMgr.invalidateTable must also drop the "
                        + "NereidsSortedPartitionsCacheManager entry (ExternalMetaCacheMgr.java:217-220)");
    }

    // ── db/catalog-level invalidation also drops Cache B (§10 completeness) ──
    //
    // invalidateTable (above) already drops the ranges cache; invalidateDb / invalidateCatalog /
    // removeCatalog previously did NOT, so a db- or catalog-level REFRESH left STALE Cache B entries.
    // Cache B has no db/catalog-scoped eviction key, so those coarse invalidations drop ALL entries
    // (invalidateAll). Each test drives the REAL ExternalMetaCacheMgr method end-to-end.

    /**
     * Populates a live {@link NereidsSortedPartitionsCacheManager} with one entry, then runs {@code
     * invalidation} against a REAL {@link ExternalMetaCacheMgr} (with a mocked {@code Env} wiring
     * {@code getCatalogMgr}/{@code getSortedPartitionsCacheManager}) and asserts Cache B is emptied.
     */
    private void assertDropsAllRangesCache(java.util.function.Consumer<ExternalMetaCacheMgr> invalidation)
            throws Exception {
        newLiveConnectContext();
        NereidsSortedPartitionsCacheManager rangesCacheMgr = new NereidsSortedPartitionsCacheManager();
        FakeExternalTable t = new FakeExternalTable(true);
        t.parts.put("id=1", listItem(1));
        Assertions.assertTrue(rangesCacheMgr.get(t.table, (CatalogRelation) null).isPresent(),
                "ranges built and cached before invalidation");
        Assertions.assertEquals(1, rangesCacheMgr.getPartitionCaches().estimatedSize(),
                "one entry cached before invalidation");

        Env env = Mockito.mock(Env.class);
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        CatalogIf catalog = Mockito.mock(CatalogIf.class);
        Mockito.when(catalog.getName()).thenReturn(CTL);
        Mockito.when(catalogMgr.getCatalog(CATALOG_ID)).thenReturn(catalog);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        Mockito.when(env.getSortedPartitionsCacheManager()).thenReturn(rangesCacheMgr);

        ExternalMetaCacheMgr metaCacheMgr = new ExternalMetaCacheMgr(true);
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            invalidation.accept(metaCacheMgr);
        }

        Assertions.assertEquals(0, rangesCacheMgr.getPartitionCaches().estimatedSize(),
                "db/catalog-level invalidation must also drop the NereidsSortedPartitionsCacheManager entries");
    }

    @Test
    public void testInvalidateDbDropsRangesCache() throws Exception {
        assertDropsAllRangesCache(m -> m.invalidateDb(CATALOG_ID, DB));
    }

    @Test
    public void testInvalidateCatalogDropsRangesCache() throws Exception {
        assertDropsAllRangesCache(m -> m.invalidateCatalog(CATALOG_ID));
    }

    @Test
    public void testRemoveCatalogDropsRangesCache() throws Exception {
        assertDropsAllRangesCache(m -> m.removeCatalog(CATALOG_ID));
    }
}
