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

package org.apache.doris.datasource;

import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.analysis.TableSnapshot;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.RangePartitionItem;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorPartitionInfo;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorTableSchema;
import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.mvcc.ConnectorMvccPartition;
import org.apache.doris.connector.api.mvcc.ConnectorMvccPartitionView;
import org.apache.doris.connector.api.mvcc.ConnectorMvccSnapshot;
import org.apache.doris.connector.api.mvcc.ConnectorTableFreshness;
import org.apache.doris.connector.api.mvcc.ConnectorTimeTravelSpec;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.datasource.mvcc.MvccTableInfo;
import org.apache.doris.mtmv.MTMVMaxTimestampSnapshot;
import org.apache.doris.mtmv.MTMVSnapshotIdSnapshot;
import org.apache.doris.mtmv.MTMVTimestampSnapshot;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * Tests for {@link PluginDrivenMvccExternalTable}, the generic MVCC/MTMV-capable plugin table.
 *
 * <p><b>Why these matter:</b> this class is the fe-core MTMV/MvccTable bridge for snapshot-capable
 * connectors (Paimon first; later Iceberg/Hudi). It must (1) pin the REAL connector snapshot id for
 * incremental MTMV change-detection — a constant would make every refresh see "no change" or "always
 * changed"; (2) honor a supplied pin so the whole query reads ONE consistent partition set with no
 * extra connector round-trip (single-pin invariant); (3) build partition keys from the RENDERED
 * partition name the connector produced (date already a string), not a raw epoch; (4) fall back to
 * UNPARTITIONED when a partition fails to build rather than silently pruning to a partial set; and
 * (5) dispatch explicit time-travel (FOR VERSION/TIME, tag/branch/incr scan params) source-agnostically
 * into a {@link ConnectorTimeTravelSpec}, translate not-found into a user error, and pin the
 * schema-AS-OF the snapshot so reads under schema evolution see the historical columns. The class is
 * source-agnostic: it is constructed directly here against a mocked connector.</p>
 */
public class PluginDrivenMvccExternalTableTest {

    private static final long PINNED_SNAPSHOT_ID = 4242L;
    private static final long TS_2024_01_01 = 1_700_000_000_000L;
    private static final long TS_2024_02_02 = 1_800_000_000_000L;

    @AfterEach
    public void cleanup() {
        ConnectContext.remove();
    }

    // ==================== getTableSnapshot: REAL pinned id ====================

    @Test
    public void testGetTableSnapshotReturnsRealPinnedId() throws AnalysisException {
        Fixture f = Fixture.partitioned();
        MTMVSnapshotIdSnapshot snap =
                (MTMVSnapshotIdSnapshot) f.table.getTableSnapshot(Optional.empty());
        // MUTATION: returning a constant -1 (or any other id) makes this red. The pinned id is what
        // MTMV uses to decide whether the base table changed since last refresh.
        Assertions.assertEquals(PINNED_SNAPSHOT_ID, snap.getSnapshotVersion(),
                "getTableSnapshot must carry the REAL connector snapshot id");
    }

    // ==================== getPartitionSnapshot: timestamp + missing throws ====================

    @Test
    public void testGetPartitionSnapshotReturnsLastModifiedMillis() throws AnalysisException {
        Fixture f = Fixture.partitioned();
        MTMVTimestampSnapshot ts = (MTMVTimestampSnapshot) f.table.getPartitionSnapshot(
                "dt=2024-01-01", null, Optional.empty());
        // MUTATION: returning the wrong partition's millis (or 0) makes this red.
        Assertions.assertEquals(TS_2024_01_01, ts.getSnapshotVersion(),
                "partition snapshot must be that partition's lastModifiedMillis");
    }

    @Test
    public void testGetPartitionSnapshotMissingThrows() {
        Fixture f = Fixture.partitioned();
        // MUTATION: returning a default snapshot instead of throwing makes this red.
        Assertions.assertThrows(AnalysisException.class,
                () -> f.table.getPartitionSnapshot("dt=1999-12-31", null, Optional.empty()),
                "an unknown partition name must raise AnalysisException, not silently succeed");
    }

    // ==================== last-modified freshness (e.g. hive): table + partition snapshots ====================

    /**
     * Re-stubs {@code beginQuerySnapshot} so the query-begin pin advertises last-modified freshness (the flag a
     * hive connector sets). fe-core reads this off the pin to decide whether to serve MTMV freshness from the
     * on-demand SPI (hive) vs the snapshot id (paimon/iceberg).
     */
    private static void flagPinLastModified(Fixture f) {
        Mockito.when(f.metadata.beginQuerySnapshot(f.session, f.handle))
                .thenReturn(Optional.of(ConnectorMvccSnapshot.builder()
                        .snapshotId(-1L).lastModifiedFreshness(true).build()));
    }

    @Test
    public void testGetTableSnapshotLastModifiedEmitsMaxTimestampSnapshot() throws AnalysisException {
        // A last-modified connector (hive) flags its pin and reports whole-table freshness via getTableFreshness;
        // fe-core must wrap it in MTMVMaxTimestampSnapshot (byte-parity with legacy HiveDlaTable.getTableSnapshot),
        // NOT the snapshot-id token. Without this a plain-hive empty pin's snapshot id is a constant -1, so an MV
        // over a hive base table would compare equal forever and never refresh.
        Fixture f = Fixture.partitioned();
        flagPinLastModified(f);
        Mockito.when(f.metadata.getTableFreshness(Mockito.any(), Mockito.any()))
                .thenReturn(Optional.of(new ConnectorTableFreshness("dt=2024-02-02", TS_2024_02_02)));

        // MUTATION: keeping the hardcoded MTMVSnapshotIdSnapshot (ignoring getTableFreshness) makes this cast
        // throw ClassCastException -> red.
        MTMVMaxTimestampSnapshot snap =
                (MTMVMaxTimestampSnapshot) f.table.getTableSnapshot(Optional.empty());
        // MTMVMaxTimestampSnapshot.equals compares BOTH the partition name and the timestamp (the name guards
        // against dropping the partition that owns the max time). MUTATION: dropping the name or the millis makes
        // this red.
        Assertions.assertEquals(new MTMVMaxTimestampSnapshot("dt=2024-02-02", TS_2024_02_02), snap,
                "a last-modified connector's table snapshot must carry (max-partition-name, max-modify-millis)");
    }

    @Test
    public void testGetTableSnapshotContextOverloadAlsoLastModified() throws AnalysisException {
        // The MTMVRefreshContext overload (used by MTMVPartitionUtil.getTableSnapshotFromContext) must route to
        // the same freshness-aware path, not a separate hardcoded one.
        Fixture f = Fixture.partitioned();
        flagPinLastModified(f);
        Mockito.when(f.metadata.getTableFreshness(Mockito.any(), Mockito.any()))
                .thenReturn(Optional.of(new ConnectorTableFreshness("t", 4242L)));
        MTMVMaxTimestampSnapshot snap =
                (MTMVMaxTimestampSnapshot) f.table.getTableSnapshot(null, Optional.empty());
        Assertions.assertEquals(new MTMVMaxTimestampSnapshot("t", 4242L), snap);
    }

    @Test
    public void testGetPartitionSnapshotLastModifiedUsesOnDemandNotPin() throws AnalysisException {
        // A last-modified connector withholds per-partition modify time from listPartitions (names-only hot
        // path), so the pin carries the -1 UNKNOWN sentinel; getPartitionSnapshot must take the REAL time from
        // the on-demand getPartitionFreshnessMillis, not the pin.
        Fixture f = Fixture.with(Collections.singletonList(
                cpi("dt=2024-01-01", ConnectorPartitionInfo.UNKNOWN)));
        flagPinLastModified(f);
        Mockito.when(f.metadata.getPartitionFreshnessMillis(Mockito.any(), Mockito.any(),
                Mockito.eq("dt=2024-01-01"))).thenReturn(OptionalLong.of(TS_2024_01_01));

        MTMVTimestampSnapshot ts = (MTMVTimestampSnapshot) f.table.getPartitionSnapshot(
                "dt=2024-01-01", null, Optional.empty());
        // MUTATION: reading the pin value (-1) instead of the on-demand fetch makes this red (would be -1),
        // which would make every partition compare equal forever (stale MV at partition granularity).
        Assertions.assertEquals(TS_2024_01_01, ts.getSnapshotVersion(),
                "a last-modified connector's partition snapshot must use the on-demand millis, not the pin's -1");
    }

    @Test
    public void testGetPartitionSnapshotLastModifiedMissingStillThrows() {
        // Existence is validated against the materialized partition set BEFORE the on-demand fetch, so even a
        // last-modified connector raises AnalysisException for an unknown partition (parity legacy
        // HiveDlaTable.getPartitionSnapshot -> checkPartitionExists).
        Fixture f = Fixture.partitioned();
        flagPinLastModified(f);
        Mockito.when(f.metadata.getPartitionFreshnessMillis(Mockito.any(), Mockito.any(), Mockito.any()))
                .thenReturn(OptionalLong.of(TS_2024_01_01));
        Assertions.assertThrows(AnalysisException.class,
                () -> f.table.getPartitionSnapshot("dt=1999-12-31", null, Optional.empty()),
                "an unknown partition must throw even for a last-modified connector (existence checked first)");
    }

    @Test
    public void testGetPartitionSnapshotLastModifiedVanishedThrows() {
        // The partition IS in the materialized set (existence check passes) but VANISHED before the on-demand
        // fetch (a refresh-time race), so getPartitionFreshnessMillis returns empty -> fe-core must raise the
        // legacy "can not find partition", NOT emit a bogus MTMVTimestampSnapshot(0). MUTATION: falling back to
        // MTMVTimestampSnapshot(0) instead of throwing makes this red.
        Fixture f = Fixture.with(Collections.singletonList(
                cpi("dt=2024-01-01", ConnectorPartitionInfo.UNKNOWN)));
        flagPinLastModified(f);
        Mockito.when(f.metadata.getPartitionFreshnessMillis(Mockito.any(), Mockito.any(), Mockito.any()))
                .thenReturn(OptionalLong.empty());
        Assertions.assertThrows(AnalysisException.class,
                () -> f.table.getPartitionSnapshot("dt=2024-01-01", null, Optional.empty()),
                "a vanished partition (on-demand empty) must throw, not return a bogus 0 timestamp");
    }

    // ==================== snapshot-id connectors (paimon/iceberg): NO extra freshness probe ====================

    @Test
    public void testGetTableSnapshotSnapshotIdConnectorSkipsFreshnessProbe() throws AnalysisException {
        // A snapshot-id connector (paimon/iceberg) leaves the pin flag false, so getTableSnapshot must take the
        // exact pre-change path: read the snapshot id off the pin and NEVER fire the freshness probe (an extra
        // getTableHandle round-trip + a new throw surface on the live MTMV path). This guards the regression the
        // adversarial review caught.
        Fixture f = Fixture.partitioned();   // build() pin has lastModifiedFreshness=false
        MTMVSnapshotIdSnapshot snap = (MTMVSnapshotIdSnapshot) f.table.getTableSnapshot(Optional.empty());
        Assertions.assertEquals(PINNED_SNAPSHOT_ID, snap.getSnapshotVersion());
        // MUTATION: dropping the pin-flag gate (probing unconditionally) makes this verify red.
        Mockito.verify(f.metadata, Mockito.never()).getTableFreshness(Mockito.any(), Mockito.any());
    }

    @Test
    public void testGetPartitionSnapshotSnapshotIdConnectorSkipsFreshnessProbe() throws AnalysisException {
        // Same guard at partition granularity: a pin-timestamp connector (paimon) must read the pin value and
        // NEVER call getPartitionFreshnessMillis (which, per-partition in the isSyncWithPartitions loop, would be
        // an O(partitions) metadata regression).
        Fixture f = Fixture.partitioned();
        MTMVTimestampSnapshot ts = (MTMVTimestampSnapshot) f.table.getPartitionSnapshot(
                "dt=2024-01-01", null, Optional.empty());
        Assertions.assertEquals(TS_2024_01_01, ts.getSnapshotVersion());
        // MUTATION: probing unconditionally (no pin-flag gate) makes this verify red.
        Mockito.verify(f.metadata, Mockito.never())
                .getPartitionFreshnessMillis(Mockito.any(), Mockito.any(), Mockito.any());
    }

    // ==================== getNameToPartitionItems: render-from-name parity ====================

    @Test
    public void testGetNameToPartitionItemsBuildsKeyFromRenderedDateName() {
        Fixture f = Fixture.partitioned();
        Map<String, PartitionItem> items = f.table.getNameToPartitionItems(Optional.empty());

        Assertions.assertEquals(2, items.size());
        PartitionItem item = items.get("dt=2024-01-01");
        Assertions.assertTrue(item instanceof ListPartitionItem, "expected a ListPartitionItem");
        PartitionKey key = ((ListPartitionItem) item).getItems().get(0);
        // MUTATION: if the connector had returned a raw epoch "19723" and we built from that, the
        // DATEV2 key would be a different date (or fail to parse). The connector renders the date to
        // a string in getPartitionName(), so the key must be 2024-01-01.
        Assertions.assertEquals("2024-01-01", key.getKeys().get(0).getStringValue(),
                "partition key must be built from the RENDERED date name, not a raw epoch");
    }

    @Test
    public void testDefaultSentinelWithoutFlagBuildsNonNullStringKey() {
        // NO-FLAG DEFAULT path: a connector that supplies NO per-value null flags leaves every value non-null
        // (isNull=false), so a __HIVE_DEFAULT_PARTITION__ value on a VARCHAR column builds a plain StringLiteral,
        // NOT a NullLiteral. This is the unchanged default for connectors that do not opt in (hudi/maxcompute/
        // iceberg). NB: hive and paimon DO opt in now (variant B) and would supply isNull=true here — see the two
        // ...BuildsGenuineNullPartition tests below. VARCHAR keeps the sentinel parseable; a non-string column
        // without the flag throws+drops (per-partition catch) — see testDefaultSentinelWithoutFlagStillDrops.
        Fixture f = Fixture.with(Collections.singletonList(
                cpi("dt=" + TablePartitionValues.HIVE_DEFAULT_PARTITION, TS_2024_01_01)), Type.VARCHAR);
        Map<String, PartitionItem> items = f.table.getNameToPartitionItems(Optional.empty());

        Assertions.assertEquals(1, items.size());
        PartitionItem item = items.get("dt=" + TablePartitionValues.HIVE_DEFAULT_PARTITION);
        Assertions.assertTrue(item instanceof ListPartitionItem, "expected a ListPartitionItem");
        PartitionKey key = ((ListPartitionItem) item).getItems().get(0);
        // MUTATION: defaulting the absent flag to isNull=true -> the key is a NullLiteral -> red.
        Assertions.assertFalse(key.getKeys().get(0).isNullLiteral(),
                "no-flag default: a __HIVE_DEFAULT_PARTITION__ value must build a NON-null literal key (isNull=false)");
        Assertions.assertEquals(TablePartitionValues.HIVE_DEFAULT_PARTITION,
                key.getKeys().get(0).getStringValue(),
                "the no-flag partition key must carry the sentinel string verbatim (a plain StringLiteral)");
    }

    @Test
    public void testDefaultSentinelWithNullFlagOnIntColumnBuildsGenuineNullPartition() {
        // RED before the fix (fe-core hardcoded isNull=false): the sentinel on an INT column parses via
        // IntLiteral("__HIVE_DEFAULT_PARTITION__") -> NumberFormatException -> the partition is dropped -> the
        // snapshot is invalid -> the table mis-reports UNPARTITIONED (partition=0/0). With the connector-supplied
        // isNull=true flag the value builds a typed NullLiteral (no parse), so the table stays LIST-partitioned
        // with a genuine-NULL partition (legacy HiveExternalMetaCache:309 parity; hive/paimon variant B).
        Fixture f = Fixture.with(Collections.singletonList(
                cpiNull("dt=" + TablePartitionValues.HIVE_DEFAULT_PARTITION, TS_2024_01_01, true)), Type.INT);

        Assertions.assertEquals(PartitionType.LIST, f.table.getPartitionType(Optional.empty()),
                "a genuine-NULL INT partition must NOT collapse the table to UNPARTITIONED");
        Assertions.assertFalse(f.table.getPartitionColumns(Optional.empty()).isEmpty(),
                "partition columns must survive (not emptied by an invalid partition set)");
        Map<String, PartitionItem> items = f.table.getNameToPartitionItems(Optional.empty());
        Assertions.assertEquals(1, items.size(), "the null partition must be present, not dropped");
        PartitionKey key = ((ListPartitionItem) items.get(
                "dt=" + TablePartitionValues.HIVE_DEFAULT_PARTITION)).getItems().get(0);
        // MUTATION: ignoring the flag (hardcoded false) -> IntLiteral parse throws -> 0 items / UNPARTITIONED -> red.
        Assertions.assertTrue(key.getKeys().get(0).isNullLiteral(),
                "the connector-supplied NULL flag must build a typed NullLiteral for the INT column");
    }

    @Test
    public void testDefaultSentinelWithNullFlagOnDateColumnBuildsGenuineNullPartition() {
        // Second non-string family (DATEV2 also throws on the sentinel pre-fix). Same expectation as the INT case.
        Fixture f = Fixture.with(Collections.singletonList(
                cpiNull("dt=" + TablePartitionValues.HIVE_DEFAULT_PARTITION, TS_2024_01_01, true)), Type.DATEV2);

        Assertions.assertEquals(PartitionType.LIST, f.table.getPartitionType(Optional.empty()));
        Map<String, PartitionItem> items = f.table.getNameToPartitionItems(Optional.empty());
        Assertions.assertEquals(1, items.size());
        PartitionKey key = ((ListPartitionItem) items.get(
                "dt=" + TablePartitionValues.HIVE_DEFAULT_PARTITION)).getItems().get(0);
        Assertions.assertTrue(key.getKeys().get(0).isNullLiteral(),
                "the connector-supplied NULL flag must build a typed NullLiteral for the DATE column");
    }

    @Test
    public void testDefaultSentinelWithoutFlagStillDrops() {
        // Locks the fix as OPT-IN: a connector that does NOT supply the flag keeps the pre-fix behavior on a
        // non-string column — the sentinel throws on IntLiteral, the partition is dropped, the table degrades to
        // UNPARTITIONED. (Compile-independent guard: uses only the pre-existing no-flag cpi helper.)
        Fixture f = Fixture.with(Collections.singletonList(
                cpi("dt=" + TablePartitionValues.HIVE_DEFAULT_PARTITION, TS_2024_01_01)), Type.INT);

        Assertions.assertEquals(PartitionType.UNPARTITIONED, f.table.getPartitionType(Optional.empty()),
                "without the connector flag, an INT sentinel still drops the partition (UNPARTITIONED)");
        Assertions.assertTrue(f.table.getNameToPartitionItems(Optional.empty()).isEmpty());
    }

    // ==================== no-cache schema: bypass the name-keyed cache and read fresh ====================

    @Test
    public void testSchemaCacheDisabledByConnectorTtl() {
        // ttl-second <= 0 (the no-cache catalog) -> the generic name-keyed schema cache (no schemaId) must be
        // bypassed and the schema read fresh; an absent/positive override keeps the cached path.
        Connector noCache = Mockito.mock(Connector.class);
        Mockito.when(noCache.schemaCacheTtlSecondOverride()).thenReturn(OptionalLong.of(0));
        Assertions.assertTrue(PluginDrivenMvccExternalTable.schemaCacheDisabled(noCache),
                "ttl-second=0 (no-cache catalog) must disable the schema cache");

        Connector negative = Mockito.mock(Connector.class);
        Mockito.when(negative.schemaCacheTtlSecondOverride()).thenReturn(OptionalLong.of(-1));
        Assertions.assertTrue(PluginDrivenMvccExternalTable.schemaCacheDisabled(negative),
                "a negative ttl-second also disables the schema cache");

        Connector withCache = Mockito.mock(Connector.class);
        Mockito.when(withCache.schemaCacheTtlSecondOverride()).thenReturn(OptionalLong.empty());
        Assertions.assertFalse(PluginDrivenMvccExternalTable.schemaCacheDisabled(withCache),
                "an absent override (the cached catalog) keeps the schema cache");

        Connector positive = Mockito.mock(Connector.class);
        Mockito.when(positive.schemaCacheTtlSecondOverride()).thenReturn(OptionalLong.of(3600));
        Assertions.assertFalse(PluginDrivenMvccExternalTable.schemaCacheDisabled(positive),
                "a positive ttl-second keeps the schema cache");

        Assertions.assertFalse(PluginDrivenMvccExternalTable.schemaCacheDisabled(null),
                "a null connector (uninitialized) keeps the engine default");
    }

    @Test
    public void testNoCacheReadsFreshSchemaElseCached() {
        // The no-cache catalog must serve the FRESH (initSchema) schema, bypassing the cached (super) path;
        // the cached catalog serves the cached value. This restores master's meta.cache.paimon.table
        // .ttl-second=0 -> always-fresh-schema after an external ALTER (regression test_paimon_table_meta_cache
        // line 112, no-cache desc expected 3 cols but got the stale 2).
        SchemaCacheValue cached = new PluginDrivenSchemaCacheValue(
                Collections.singletonList(new Column("c", Type.INT)),
                Collections.emptyList(), Collections.emptyList());
        SchemaCacheValue fresh = new PluginDrivenSchemaCacheValue(
                Arrays.asList(new Column("c", Type.INT), new Column("c2", Type.INT)),
                Collections.emptyList(), Collections.emptyList());

        ConnectorMetadata metadata = Mockito.mock(ConnectorMetadata.class);
        ConnectorSession session = Mockito.mock(ConnectorSession.class);
        TestablePluginCatalog catalog = new TestablePluginCatalog(metadata, session);
        Connector connector = catalog.getConnector();
        ExternalDatabase<PluginDrivenExternalTable> db = mockDb("REMOTE_DB");

        PluginDrivenMvccExternalTable table =
                new PluginDrivenMvccExternalTable(1L, "tbl", "REMOTE_TBL", catalog, db) {
                    @Override
                    protected synchronized void makeSureInitialized() {
                    }

                    @Override
                    public Optional<SchemaCacheValue> initSchema() {
                        return Optional.of(fresh);
                    }

                    @Override
                    protected Optional<SchemaCacheValue> cachedSchemaCacheValue() {
                        return Optional.of(cached);
                    }
                };

        // no-cache (ttl=0): bypass the cache -> fresh
        Mockito.when(connector.schemaCacheTtlSecondOverride()).thenReturn(OptionalLong.of(0));
        Assertions.assertSame(fresh, table.getLatestSchemaCacheValue().orElse(null),
                "no-cache catalog must read the fresh schema (initSchema), not the cached value");

        // with-cache (override absent): cached
        Mockito.when(connector.schemaCacheTtlSecondOverride()).thenReturn(OptionalLong.empty());
        Assertions.assertSame(cached, table.getLatestSchemaCacheValue().orElse(null),
                "cached catalog must read the cached schema value");
    }

    // ==================== single-pin invariant: no re-query when pin supplied ====================

    @Test
    public void testSuppliedPinIsNotReQueried() throws AnalysisException {
        Fixture f = Fixture.partitioned();
        // Materialize ONCE (no pin) -> this is the single round-trip we allow.
        PluginDrivenMvccSnapshot pin =
                (PluginDrivenMvccSnapshot) f.table.loadSnapshot(Optional.empty(), Optional.empty());
        // Reset interaction counters so the verify below only counts post-pin calls.
        Mockito.clearInvocations(f.metadata);

        Optional<MvccSnapshot> pinOpt = Optional.of(pin);
        MTMVSnapshotIdSnapshot snap = (MTMVSnapshotIdSnapshot) f.table.getTableSnapshot(pinOpt);
        Map<String, PartitionItem> items = f.table.getNameToPartitionItems(pinOpt);

        Assertions.assertEquals(PINNED_SNAPSHOT_ID, snap.getSnapshotVersion());
        Assertions.assertEquals(2, items.size());
        // MUTATION: if getOrMaterialize re-listed when a pin is present, these verifies (zero calls)
        // would fail. The whole query must read the SAME materialized view passed in.
        Mockito.verify(f.metadata, Mockito.never())
                .beginQuerySnapshot(Mockito.any(), Mockito.any());
        Mockito.verify(f.metadata, Mockito.never())
                .listPartitions(Mockito.any(), Mockito.any(), Mockito.any());
    }

    // ==================== isPartitionInvalid -> UNPARTITIONED ====================

    @Test
    public void testValueCountMismatchDegradesToUnpartitioned() {
        // A value/column count mismatch is LEGITIMATE under iceberg partition spec evolution: the column
        // list comes from the CURRENT spec while a row's values come from the spec its data file was
        // written under. It must degrade to UNPARTITIONED (parity master / PaimonUtil.generatePartitionInfo)
        // rather than fail the query -- cfb0958e607 hoisted the size check out of the per-partition
        // try/catch to "fail loud", and every real-world hit turned out to be a legitimate evolution,
        // taking down 6 suites (CI 996541).
        Fixture f = Fixture.with(Arrays.asList(
                cpi("dt=2024-01-01/region=cn", TS_2024_01_01)));
        // MUTATION: hoisting the checkState back out of the per-partition try/catch makes this red.
        Assertions.assertEquals(PartitionType.UNPARTITIONED, f.table.getPartitionType(Optional.empty()),
                "a value/column count mismatch must degrade to UNPARTITIONED, not fail the query");
    }

    @Test
    public void testZeroValuesFromUnpartitionedOriginDegradeToUnpartitioned() {
        // The spec-0 shape: rows written before the table's first ADD PARTITION KEY render to an empty
        // partition name and carry ZERO values while the table now has 1 partition column. This is the
        // shape behind test_iceberg_table_cache / _partition_evolution_ddl / _partition_evolution_query_write.
        // Supplied explicitly (not via cpi()) because orderedValuesOf("") derives [""] -- size 1 -- which
        // would exercise the type-parse degrade instead of the arity one.
        Fixture f = Fixture.with(Arrays.asList(
                cpiValues("", TS_2024_01_01, Collections.emptyList())));
        // MUTATION: hoisting the checkState back out of the per-partition try/catch makes this red.
        Assertions.assertEquals(PartitionType.UNPARTITIONED, f.table.getPartitionType(Optional.empty()),
                "a zero-value partition from an unpartitioned-origin spec must degrade, not fail the query");
    }

    @Test
    public void testValidPartitionSetIsList() {
        Fixture f = Fixture.partitioned();
        Assertions.assertEquals(PartitionType.LIST, f.table.getPartitionType(Optional.empty()),
                "a fully-built partitioned table must report LIST");
    }

    @Test
    public void testDuplicateRenderedNamesCollapseAndStayValid() {
        // Two connector partitions that RENDER to the SAME partition name collapse into one entry in
        // BOTH name-keyed maps (item + lastModified). isPartitionInvalid compares those two like-keyed
        // maps (1 == 1 -> valid), matching legacy PaimonPartitionInfo which keys both maps by name.
        Fixture f = Fixture.with(Arrays.asList(
                cpi("dt=2024-01-01", TS_2024_01_01),
                cpi("dt=2024-01-01", TS_2024_02_02)));
        // MUTATION: basing the invalid check on the RAW listed count (parts.size()=2) instead of the
        // de-duplicated name-keyed size (1) makes this red — it would falsely force UNPARTITIONED and
        // drop the table's partitioning even though every listed partition built successfully.
        Assertions.assertEquals(PartitionType.LIST, f.table.getPartitionType(Optional.empty()),
                "partitions rendering to the same name must collapse, not force UNPARTITIONED");
        Assertions.assertEquals(1, f.table.getNameToPartitionItems(Optional.empty()).size(),
                "the duplicate rendered name must collapse to a single partition item");
    }

    // ==================== loadSnapshot: B5a latest materialize ====================

    @Test
    public void testLoadSnapshotEmptyMaterializes() {
        Fixture f = Fixture.partitioned();
        MvccSnapshot snap = f.table.loadSnapshot(Optional.empty(), Optional.empty());
        Assertions.assertNotNull(snap);
        Assertions.assertTrue(snap instanceof PluginDrivenMvccSnapshot);
        PluginDrivenMvccSnapshot pin = (PluginDrivenMvccSnapshot) snap;
        Assertions.assertEquals(PINNED_SNAPSHOT_ID, pin.getConnectorSnapshot().getSnapshotId());
        // B5a latest pin must NOT carry a pinned schema (callers fall back to latest) and must
        // materialize the partition maps. MUTATION: pinning a schema or dropping the partition maps
        // on the latest path makes this red.
        Assertions.assertNull(pin.getPinnedSchema(),
                "the B5a latest pin must have a null pinnedSchema (use latest schema)");
        Assertions.assertEquals(2, pin.getNameToPartitionItem().size(),
                "the latest pin must carry the materialized partition view");
    }

    @Test
    public void testLoadSnapshotNoHandleLatestDegradesToEmptyPin() {
        // No connector handle (e.g. table dropped) on the LATEST path: materializeLatest must DEGRADE
        // to a valid empty pin (snapshot id -1, empty partition maps) so downstream callers fall back
        // to UNPARTITIONED instead of NPE-ing on a null handle.
        Fixture f = Fixture.noHandle();
        PluginDrivenMvccSnapshot pin =
                (PluginDrivenMvccSnapshot) f.table.loadSnapshot(Optional.empty(), Optional.empty());
        // MUTATION: NPE-ing instead of degrading (dropping the !handleOpt.isPresent() guard) makes this
        // red; a wrong sentinel id makes the -1 assertion red.
        Assertions.assertEquals(-1L, pin.getConnectorSnapshot().getSnapshotId(),
                "the no-handle latest pin must carry the -1 snapshot sentinel");
        Assertions.assertTrue(pin.getNameToPartitionItem().isEmpty(),
                "the no-handle latest pin must have an empty partition-item map");
        Assertions.assertTrue(pin.getNameToLastModifiedMillis().isEmpty(),
                "the no-handle latest pin must have an empty last-modified map");
    }

    @Test
    public void testMaterializeLatestNullConnectorDegradesToEmptyPin() {
        // A concurrently-DROPPED catalog: onClose() nulled the (transient) connector but left objectCreated
        // true, so makeSureInitialized() does not re-create it and getConnector() returns null. A stale
        // metadata-table access (mv_infos()/jobs() scan -> isMTMVSync -> materializeLatest) must DEGRADE to a
        // valid empty pin instead of NPE-ing and aborting the whole metadata query (CI 973411 test_mysql_mtmv
        // collateral). MUTATION: dropping the null-connector guard in materializeLatest -> NPE -> red.
        ConnectorSession session = Mockito.mock(ConnectorSession.class);
        PluginDrivenExternalCatalog droppedCatalog = new TestablePluginCatalog((Connector) null, session);
        ExternalDatabase<PluginDrivenExternalTable> db = mockDb("REMOTE_DB");
        PluginDrivenMvccExternalTable table =
                new PluginDrivenMvccExternalTable(1L, "tbl", "REMOTE_TBL", droppedCatalog, db) {
                    @Override
                    protected synchronized void makeSureInitialized() {
                        // no-op: skip Env-backed catalog/db init (mirror the Fixture table)
                    }
                };

        PluginDrivenMvccSnapshot pin =
                (PluginDrivenMvccSnapshot) table.loadSnapshot(Optional.empty(), Optional.empty());

        Assertions.assertEquals(-1L, pin.getConnectorSnapshot().getSnapshotId(),
                "the null-connector (dropped-catalog) latest pin must carry the -1 snapshot sentinel");
        Assertions.assertTrue(pin.getNameToPartitionItem().isEmpty(),
                "the null-connector latest pin must have an empty partition-item map");
        Assertions.assertTrue(pin.getNameToLastModifiedMillis().isEmpty(),
                "the null-connector latest pin must have an empty last-modified map");
    }

    @Test
    public void testLoadSnapshotNoHandleTimeTravelThrows() {
        // No connector handle on a TIME-TRAVEL request: unlike the latest path it must FAIL LOUD (a
        // time-travel read against a missing table cannot degrade to "latest empty").
        Fixture f = Fixture.noHandle();
        RuntimeException e = Assertions.assertThrows(RuntimeException.class,
                () -> f.table.loadSnapshot(Optional.of(TableSnapshot.versionOf("7")), Optional.empty()));
        // MUTATION: dropping the time-travel no-handle guard (lines ~206-208) makes this red.
        Assertions.assertEquals("can not find table for time travel: REMOTE_DB.REMOTE_TBL",
                e.getMessage());
    }

    // ==================== loadSnapshot: B5b time-travel spec dispatch ====================

    @Test
    public void testForTimeAsOfDigitalMillisDispatchesTimestampDigital() {
        Fixture f = Fixture.timeTravel();
        f.table.loadSnapshot(Optional.of(TableSnapshot.timeOf("1700000000000")), Optional.empty());
        ConnectorTimeTravelSpec spec = f.captureSpec();
        // MUTATION: dispatching VERSION instead of TIME, or digital=false, makes this red — the
        // connector would parse epoch-millis as a datetime string.
        Assertions.assertEquals(ConnectorTimeTravelSpec.Kind.TIMESTAMP, spec.getKind());
        Assertions.assertTrue(spec.isDigital(), "an all-digits FOR TIME value is epoch millis");
        Assertions.assertEquals("1700000000000", spec.getStringValue());
    }

    @Test
    public void testForTimeAsOfDatetimeStringDispatchesTimestampNonDigital() {
        Fixture f = Fixture.timeTravel();
        f.table.loadSnapshot(Optional.of(TableSnapshot.timeOf("2024-01-01 00:00:00")), Optional.empty());
        ConnectorTimeTravelSpec spec = f.captureSpec();
        Assertions.assertEquals(ConnectorTimeTravelSpec.Kind.TIMESTAMP, spec.getKind());
        // MUTATION: marking a datetime string digital makes this red — the connector would treat it
        // as epoch millis instead of parsing it with the session time zone.
        Assertions.assertFalse(spec.isDigital(), "a datetime string is NOT epoch millis");
        Assertions.assertEquals("2024-01-01 00:00:00", spec.getStringValue());
    }

    @Test
    public void testForVersionAsOfDigitalDispatchesSnapshotId() {
        Fixture f = Fixture.timeTravel();
        f.table.loadSnapshot(Optional.of(TableSnapshot.versionOf("123")), Optional.empty());
        ConnectorTimeTravelSpec spec = f.captureSpec();
        Assertions.assertEquals(ConnectorTimeTravelSpec.Kind.SNAPSHOT_ID, spec.getKind());
        Assertions.assertEquals("123", spec.getStringValue());
    }

    @Test
    public void testForVersionAsOfNonDigitalDispatchesVersionRef() {
        Fixture f = Fixture.timeTravel();
        f.table.loadSnapshot(Optional.of(TableSnapshot.versionOf("my_ref")), Optional.empty());
        ConnectorTimeTravelSpec spec = f.captureSpec();
        // MUTATION: always picking SNAPSHOT_ID (ignoring the isDigitalString branch) makes this red.
        // A non-digital FOR VERSION AS OF is a source-resolved ref (VERSION_REF), NOT @tag (TAG): the
        // connector decides branch-vs-tag (iceberg accepts a branch OR a tag; paimon resolves a tag).
        // MUTATION: dispatching TAG here (the old paimon-only assumption) would re-introduce H-7 (a
        // branch ref rejected) — keep VERSION_REF so the connector owns the semantics.
        Assertions.assertEquals(ConnectorTimeTravelSpec.Kind.VERSION_REF, spec.getKind(),
                "a non-digital FOR VERSION AS OF is a source-resolved ref (VERSION_REF), not @tag");
        Assertions.assertEquals("my_ref", spec.getStringValue());
    }

    @Test
    public void testScanParamsTagDispatchesTag() {
        Fixture f = Fixture.timeTravel();
        TableScanParams params = new TableScanParams(TableScanParams.TAG,
                Collections.singletonMap(TableScanParams.PARAMS_NAME, "t1"), Collections.emptyList());
        f.table.loadSnapshot(Optional.empty(), Optional.of(params));
        ConnectorTimeTravelSpec spec = f.captureSpec();
        Assertions.assertEquals(ConnectorTimeTravelSpec.Kind.TAG, spec.getKind());
        Assertions.assertEquals("t1", spec.getStringValue());
    }

    @Test
    public void testScanParamsBranchDispatchesBranchFromListParams() {
        Fixture f = Fixture.timeTravel();
        TableScanParams params = new TableScanParams(TableScanParams.BRANCH,
                Collections.emptyMap(), Collections.singletonList("b1"));
        f.table.loadSnapshot(Optional.empty(), Optional.of(params));
        ConnectorTimeTravelSpec spec = f.captureSpec();
        // MUTATION: ignoring the listParams extraction path makes this red.
        Assertions.assertEquals(ConnectorTimeTravelSpec.Kind.BRANCH, spec.getKind());
        Assertions.assertEquals("b1", spec.getStringValue());
    }

    @Test
    public void testScanParamsIncrementalDispatchesIncrementalWithParams() {
        Fixture f = Fixture.timeTravel();
        Map<String, String> incr = new HashMap<>();
        incr.put("startSnapshotId", "1");
        incr.put("endSnapshotId", "5");
        TableScanParams params = new TableScanParams(TableScanParams.INCREMENTAL_READ,
                incr, Collections.emptyList());
        f.table.loadSnapshot(Optional.empty(), Optional.of(params));
        ConnectorTimeTravelSpec spec = f.captureSpec();
        Assertions.assertEquals(ConnectorTimeTravelSpec.Kind.INCREMENTAL, spec.getKind());
        // MUTATION: dropping the params (or passing list/empty) makes this red — the connector needs
        // the raw window arguments to validate and interpret the incremental read.
        Assertions.assertEquals(incr, spec.getIncrementalParams());
    }

    @Test
    public void testIncrementalPinListsLatestPartitionsAndUsesLatestSchema() {
        // RD-2 (B5b-4): @incr is NOT a point-in-time pin. Legacy PaimonExternalTable.getPaimonSnapshotCacheValue
        // falls through (neither tag/branch nor FOR VERSION/TIME AS OF) to getLatestSnapshotCacheValue — the
        // LATEST partition view + LATEST schema — and applies the incremental window at scan time. The bridge
        // must mirror that: POPULATE the partition maps (unlike snapshot/tag/timestamp/branch, which stay
        // EMPTY) and use the LATEST schema (pinnedSchema == null).
        Fixture f = Fixture.timeTravel();
        Map<String, String> incr = new HashMap<>();
        incr.put("startSnapshotId", "1");
        incr.put("endSnapshotId", "5");
        TableScanParams params = new TableScanParams(TableScanParams.INCREMENTAL_READ,
                incr, Collections.emptyList());

        PluginDrivenMvccSnapshot pin = (PluginDrivenMvccSnapshot) f.table.loadSnapshot(
                Optional.empty(), Optional.of(params));

        // The pin carries the connector-resolved snapshot (which holds the incremental-between scan options
        // threaded onto the handle at scan time via applySnapshot).
        Assertions.assertSame(f.resolvedSnapshot, pin.getConnectorSnapshot());

        // MUTATION: routing @incr through the EMPTY-map time-travel path (like snapshot/tag) leaves these
        // empty -> red. @incr must list the LATEST partitions (the two fixture partitions).
        Assertions.assertEquals(2, pin.getNameToPartitionItem().size(),
                "@incr must list the LATEST partitions (parity legacy getLatestSnapshotCacheValue)");
        Assertions.assertEquals(TS_2024_01_01, pin.getNameToLastModifiedMillis().get("dt=2024-01-01"));
        Assertions.assertEquals(TS_2024_02_02, pin.getNameToLastModifiedMillis().get("dt=2024-02-02"));
        Assertions.assertFalse(pin.isPartitionInvalid(),
                "a fully-built latest partition set must not be flagged invalid");
        Mockito.verify(f.metadata).listPartitions(Mockito.any(), Mockito.any(), Mockito.any());

        // @incr uses the LATEST schema, NOT an at-snapshot schema: pinnedSchema must be null so
        // getSchemaCacheValue() falls back to latest. MUTATION: resolving a schema-at-snapshot for @incr
        // (the snapshot/tag/branch path) sets a non-null pinnedSchema and invokes applySnapshot/getTableSchema
        // -> these go red.
        Assertions.assertNull(pin.getPinnedSchema(),
                "@incr reads the LATEST schema; pinnedSchema must be null");
        Mockito.verify(f.metadata, Mockito.never()).getTableSchema(Mockito.any(), Mockito.any(),
                Mockito.any(ConnectorMvccSnapshot.class));
        Mockito.verify(f.metadata, Mockito.never()).applySnapshot(Mockito.any(), Mockito.any(),
                Mockito.any());
    }

    @Test
    public void testExtractBranchOrTagNameErrors() {
        Fixture f = Fixture.timeTravel();
        // Non-empty mapParams missing the 'name' key.
        TableScanParams missingName = new TableScanParams(TableScanParams.TAG,
                Collections.singletonMap("other", "x"), Collections.emptyList());
        IllegalArgumentException e1 = Assertions.assertThrows(IllegalArgumentException.class,
                () -> f.table.loadSnapshot(Optional.empty(), Optional.of(missingName)));
        Assertions.assertEquals("must contain key 'name' in params", e1.getMessage());

        // Empty mapParams AND empty listParams.
        TableScanParams empty = new TableScanParams(TableScanParams.TAG,
                Collections.emptyMap(), Collections.emptyList());
        IllegalArgumentException e2 = Assertions.assertThrows(IllegalArgumentException.class,
                () -> f.table.loadSnapshot(Optional.empty(), Optional.of(empty)));
        Assertions.assertEquals("must contain a branch/tag name in params", e2.getMessage());
    }

    @Test
    public void testMutualExclusionBothPresentThrows() {
        Fixture f = Fixture.timeTravel();
        RuntimeException e = Assertions.assertThrows(RuntimeException.class,
                () -> f.table.loadSnapshot(Optional.of(TableSnapshot.versionOf("1")),
                        Optional.of(new TableScanParams(TableScanParams.TAG,
                                Collections.singletonMap(TableScanParams.PARAMS_NAME, "t1"),
                                Collections.emptyList()))));
        // MUTATION: silently choosing one over the other makes this red.
        Assertions.assertEquals("Can not specify scan params and table snapshot at same time.",
                e.getMessage());
    }

    // ==================== loadSnapshot: not-found translation ====================

    @Test
    public void testNotFoundTranslationSnapshotId() {
        assertNotFound(TableSnapshot.versionOf("999"), Optional.empty(),
                "can't find snapshot by id: 999");
    }

    @Test
    public void testNotFoundTranslationVersionRef() {
        // Non-numeric FOR VERSION AS OF (VERSION_REF) renders "can't find snapshot by tag" — the
        // source-agnostic wording must not claim a branch lookup a tag-only source (paimon) never did, and
        // "no such tag" is never false. Byte-identical to legacy paimon (paimon_time_travel.groovy pins it).
        // MUTATION: a default/other-kind message, or "tag or branch" (which breaks paimon parity), makes
        // this red.
        assertNotFound(TableSnapshot.versionOf("no_such_ref"), Optional.empty(),
                "can't find snapshot by tag: no_such_ref");
    }

    @Test
    public void testNotFoundTranslationScanParamTag() {
        // @tag('x') (explicit scan param, Kind.TAG) -> "can't find snapshot by tag" — covers the scan-param
        // tag path (the FOR VERSION path above is Kind.VERSION_REF; both share the TAG wording by design).
        TableScanParams params = new TableScanParams(TableScanParams.TAG,
                Collections.singletonMap(TableScanParams.PARAMS_NAME, "no_such_tag"), Collections.emptyList());
        assertNotFound(null, Optional.of(params), "can't find snapshot by tag: no_such_tag");
    }

    @Test
    public void testNotFoundTranslationBranch() {
        TableScanParams params = new TableScanParams(TableScanParams.BRANCH,
                Collections.emptyMap(), Collections.singletonList("no_such_branch"));
        assertNotFound(null, Optional.of(params), "can't find branch: no_such_branch");
    }

    @Test
    public void testNotFoundTranslationTimestamp() {
        // The TIMESTAMP branch of notFoundMessage carries a DOCUMENTED intentional divergence from
        // legacy's detailed "...the earliest snapshot's timestamp is [...]" text (the connector owns
        // the parsed millis + earliest snapshot, which fe-core cannot see). Pin its exact text.
        // MUTATION: relabeling the TIMESTAMP case to another kind's text (or the default) makes this red.
        assertNotFound(TableSnapshot.timeOf("2024-01-01 00:00:00"), Optional.empty(),
                "can't find snapshot earlier than or equal to time: 2024-01-01 00:00:00");
    }

    private void assertNotFound(TableSnapshot ts, Optional<TableScanParams> sp, String expectedMsg) {
        Fixture f = Fixture.timeTravel();
        // Connector resolves the spec to "not found".
        Mockito.when(f.metadata.resolveTimeTravel(Mockito.any(), Mockito.any(), Mockito.any()))
                .thenReturn(Optional.empty());
        Optional<TableSnapshot> tsOpt = ts == null ? Optional.empty() : Optional.of(ts);
        RuntimeException e = Assertions.assertThrows(RuntimeException.class,
                () -> f.table.loadSnapshot(tsOpt, sp));
        // MUTATION: a generic / wrong-kind message makes this red — the user error must name the
        // exact missing target.
        Assertions.assertEquals(expectedMsg, e.getMessage());
    }

    // ==================== loadSnapshot: successful time-travel pin ====================

    @Test
    public void testSuccessfulTimeTravelPinsSnapshotAndAtSnapshotSchemaNoPartitions() {
        Fixture f = Fixture.timeTravel();
        PluginDrivenMvccSnapshot pin = (PluginDrivenMvccSnapshot) f.table.loadSnapshot(
                Optional.of(TableSnapshot.versionOf("7")), Optional.empty());

        // The returned pin carries the connector-resolved snapshot.
        Assertions.assertSame(f.resolvedSnapshot, pin.getConnectorSnapshot());
        Assertions.assertEquals(Fixture.TT_SCHEMA_ID, pin.getSchemaId());
        // MUTATION: listing partitions for time-travel makes these maps non-empty (red) and the
        // verify(never) below catches the listPartitions call.
        Assertions.assertTrue(pin.getNameToPartitionItem().isEmpty(),
                "time-travel reads must NOT list partitions");
        Assertions.assertTrue(pin.getNameToLastModifiedMillis().isEmpty(),
                "time-travel reads must NOT list partitions");
        Mockito.verify(f.metadata, Mockito.never())
                .listPartitions(Mockito.any(), Mockito.any(), Mockito.any());

        // The pinned schema must be the AT-SNAPSHOT schema (column "v1"), NOT the latest fixture
        // schema (column "dt"). MUTATION: pinning the latest schema instead of the at-snapshot one
        // makes this red.
        PluginDrivenSchemaCacheValue pinned = (PluginDrivenSchemaCacheValue) pin.getPinnedSchema();
        Assertions.assertNotNull(pinned);
        Assertions.assertEquals(1, pinned.getSchema().size());
        Assertions.assertEquals("v1", pinned.getSchema().get(0).getName(),
                "the pinned schema must reflect getTableSchema(..., snapshot), not the latest schema");
    }

    @Test
    public void testBranchAppliesSnapshotBeforeResolvingSchema() {
        Fixture f = Fixture.timeTravel();
        TableScanParams params = new TableScanParams(TableScanParams.BRANCH,
                Collections.emptyMap(), Collections.singletonList("b1"));
        f.table.loadSnapshot(Optional.empty(), Optional.of(params));

        // applySnapshot was invoked, and getTableSchema(...,snapshot) was called with the handle
        // RETURNED by applySnapshot (the branch-aware handle), not the base handle. MUTATION: calling
        // getTableSchema with the base handle resolves the branch schemaId against the base table's
        // schemaManager = wrong schema, and makes this red.
        Mockito.verify(f.metadata).applySnapshot(Mockito.any(), Mockito.eq(f.handle),
                Mockito.eq(f.resolvedSnapshot));
        Mockito.verify(f.metadata).getTableSchema(Mockito.any(), Mockito.eq(f.pinnedHandle),
                Mockito.eq(f.resolvedSnapshot));
        // Make the apply-BEFORE-getTableSchema ordering explicit (not just implied by data-flow):
        // applySnapshot must thread the pin onto the handle FIRST, so the branch-aware pinnedHandle is
        // what getTableSchema resolves the schema against. MUTATION: resolving the schema before/without
        // applySnapshot (or swapping the order) makes this red.
        InOrder ord = Mockito.inOrder(f.metadata);
        ord.verify(f.metadata).applySnapshot(Mockito.any(), Mockito.eq(f.handle),
                Mockito.eq(f.resolvedSnapshot));
        ord.verify(f.metadata).getTableSchema(Mockito.any(), Mockito.eq(f.pinnedHandle),
                Mockito.eq(f.resolvedSnapshot));
    }

    // ==================== getSchemaCacheValue: schema-at-snapshot override ====================

    @Test
    public void testGetSchemaCacheValueReturnsPinnedSchemaWhenContextPinned() {
        Fixture f = Fixture.timeTravel();
        PluginDrivenSchemaCacheValue pinnedSchema = new PluginDrivenSchemaCacheValue(
                Collections.singletonList(new Column("v1", Type.INT)),
                Collections.emptyList(), Collections.emptyList());
        PluginDrivenMvccSnapshot pin = new PluginDrivenMvccSnapshot(f.resolvedSnapshot,
                Collections.emptyMap(), Collections.emptyMap(), pinnedSchema);

        withContextSnapshot(f.table, pin, () -> {
            Optional<SchemaCacheValue> got = f.table.getSchemaCacheValue();
            // MUTATION: ignoring the context pin (returning latest) makes this red.
            Assertions.assertTrue(got.isPresent());
            Assertions.assertSame(pinnedSchema, got.get(),
                    "a context pin with a pinnedSchema must yield the schema AS OF the snapshot");
        });
    }

    @Test
    public void testGetSchemaCacheValueFallsBackToLatestWhenPinHasNullSchema() {
        Fixture f = Fixture.timeTravel();
        // A B5a latest pin (pinnedSchema == null).
        PluginDrivenMvccSnapshot pin = new PluginDrivenMvccSnapshot(f.resolvedSnapshot,
                Collections.emptyMap(), Collections.emptyMap(), null);

        withContextSnapshot(f.table, pin, () -> {
            Optional<SchemaCacheValue> got = f.table.getSchemaCacheValue();
            // MUTATION: returning the (null) pinned schema instead of falling back to latest makes
            // this red; a B5a latest pin must read the latest schema.
            Assertions.assertTrue(got.isPresent());
            Assertions.assertSame(f.latestCacheValue, got.get(),
                    "a pin with a null pinnedSchema must fall back to the latest schema");
        });
    }

    @Test
    public void testGetSchemaCacheValueFallsBackToLatestWhenNoPin() {
        Fixture f = Fixture.timeTravel();
        // No ConnectContext at all -> no pin -> latest.
        Optional<SchemaCacheValue> got = f.table.getSchemaCacheValue();
        Assertions.assertTrue(got.isPresent());
        Assertions.assertSame(f.latestCacheValue, got.get(),
                "with no context pin getSchemaCacheValue must return the latest schema");
    }

    // ==================== getNewestUpdateVersionOrTime: max, bypass pin ====================

    @Test
    public void testGetNewestUpdateVersionOrTimeMaxAndBypassesPin() throws AnalysisException {
        Fixture f = Fixture.partitioned();
        // Pin a CONTEXT snapshot whose nameToLastModifiedMillis carries a max (Long.MAX_VALUE) that is
        // strictly LARGER than the fresh LATEST listing's max (TS_2024_02_02). getNewestUpdateVersionOrTime
        // takes no snapshot arg and must NOT read this pin: it calls materializeLatest() directly,
        // re-listing live.
        PluginDrivenMvccSnapshot contextPin = new PluginDrivenMvccSnapshot(
                ConnectorMvccSnapshot.builder().snapshotId(PINNED_SNAPSHOT_ID).build(),
                Collections.emptyMap(),
                Collections.singletonMap("dt=2099-12-31", Long.MAX_VALUE));

        long[] newest = new long[1];
        withContextSnapshot(f.table, contextPin, () -> {
            newest[0] = f.table.getNewestUpdateVersionOrTime();
        });

        // MUTATION: returning min instead of max makes this red. MUTATION: reading the CONTEXT pin
        // instead of re-listing would return Long.MAX_VALUE (the pinned max), not the fresh-listing max
        // — proving the pin is bypassed.
        Assertions.assertEquals(TS_2024_02_02, newest[0],
                "must return max(lastModifiedMillis) from a fresh LATEST listing, NOT the context pin's max");
        // MUTATION: reading a context pin instead of re-listing would skip this call (zero
        // interactions), making the verify red. Proves the pin is bypassed.
        Mockito.verify(f.metadata).listPartitions(Mockito.any(), Mockito.any(), Mockito.any());
    }

    @Test
    public void testGetNewestUpdateVersionOrTimeAllUnknownReturnsZeroNotSentinel() {
        // Every partition advertises UNKNOWN(-1) lastModifiedMillis (connector did not collect a
        // modified time). Legacy used Paimon's lastFileCreationTime() which has no -1 sentinel and
        // reduced to 0 when empty; the bridge must match that, not leak -1 into MTMV staleness.
        Fixture f = Fixture.with(Arrays.asList(
                cpi("dt=2024-01-01", ConnectorPartitionInfo.UNKNOWN),
                cpi("dt=2024-02-02", ConnectorPartitionInfo.UNKNOWN)));
        // MUTATION: without the `filter(v -> v >= 0)`, max() over {-1,-1} returns -1, not 0 -> red.
        Assertions.assertEquals(0L, f.table.getNewestUpdateVersionOrTime(),
                "an all-UNKNOWN table must reduce to the legacy 0, never the -1 sentinel");
    }

    @Test
    public void testGetNewestUpdateVersionOrTimeIgnoresUnknownAmongReal() throws AnalysisException {
        // A mix of a real modified time and an UNKNOWN(-1) sentinel: the sentinel must be ignored so
        // the max is the REAL value, not -1 (and not skewed by -1 participating in the reduction).
        Fixture f = Fixture.with(Arrays.asList(
                cpi("dt=2024-01-01", ConnectorPartitionInfo.UNKNOWN),
                cpi("dt=2024-02-02", TS_2024_02_02)));
        // MUTATION: the real value already wins over -1 in a plain max(), so this is a weak guard on
        // its own; the all-UNKNOWN==0 test above is the primary sentinel-leak catcher.
        Assertions.assertEquals(TS_2024_02_02, f.table.getNewestUpdateVersionOrTime(),
                "the UNKNOWN sentinel must be filtered, leaving the max of the REAL values");
    }

    // ==================== getNewestUpdateVersionOrTime: last-modified (hive) freshness ====================

    private static final long TS_TABLE_FRESH = 1_888_000_000_000L; // distinct from the partition maxes above

    @Test
    public void testGetNewestUpdateVersionLastModifiedUsesTableFreshness() {
        // A last-modified connector (hive) lists partitions names-only (all lastModifiedMillis == -1), so the
        // legacy max-over-partitions path would collapse to a CONSTANT 0 and an MV / SQL dictionary over a hive
        // base table would never auto-refresh. The pin flags last-modified freshness, so
        // getNewestUpdateVersionOrTime must return the connector's whole-table freshness millis instead.
        Fixture f = Fixture.partitioned();
        flagPinLastModified(f);
        Mockito.when(f.metadata.getTableFreshness(Mockito.any(), Mockito.any()))
                .thenReturn(Optional.of(new ConnectorTableFreshness("dt=2024-02-02", TS_TABLE_FRESH)));

        // MUTATION: taking the max-over-partitions path (ignoring the pin flag) would return the partition max
        // TS_2024_02_02, not the freshness value TS_TABLE_FRESH -> red (the values are deliberately distinct).
        Assertions.assertEquals(TS_TABLE_FRESH, f.table.getNewestUpdateVersionOrTime(),
                "a last-modified connector must surface the whole-table freshness millis, not a constant 0");
    }

    @Test
    public void testGetNewestUpdateVersionLastModifiedEmptyFreshnessReturnsZero() {
        // A dropped catalog/table, or a genuinely empty partition set, makes getTableFreshness empty; fe-core must
        // degrade to 0 (parity legacy getNewestUpdateVersionOrTime), NOT throw or leak a sentinel.
        Fixture f = Fixture.partitioned();
        flagPinLastModified(f);
        Mockito.when(f.metadata.getTableFreshness(Mockito.any(), Mockito.any()))
                .thenReturn(Optional.empty());
        // MUTATION: mapping an empty freshness to anything but 0 (e.g. throwing, or leaking -1) makes this red.
        Assertions.assertEquals(0L, f.table.getNewestUpdateVersionOrTime(),
                "an empty whole-table freshness (dropped/empty) must reduce to the legacy 0");
    }

    @Test
    public void testGetNewestUpdateVersionSnapshotIdConnectorSkipsFreshnessProbe() {
        // Byte/cost-neutrality guard: a snapshot-id connector (paimon/iceberg) leaves the pin flag false, so
        // getNewestUpdateVersionOrTime must take the EXACT pre-change max-over-partitions path and NEVER fire the
        // freshness probe (an added metadata round-trip on the live dictionary poll).
        Fixture f = Fixture.partitioned();   // build() pin has lastModifiedFreshness=false
        Assertions.assertEquals(TS_2024_02_02, f.table.getNewestUpdateVersionOrTime(),
                "a snapshot-id connector must keep the max-partition-modify path");
        // MUTATION: dropping the pin-flag gate (probing unconditionally) makes this verify red.
        Mockito.verify(f.metadata, Mockito.never()).getTableFreshness(Mockito.any(), Mockito.any());
    }

    @Test
    public void testIsPartitionColumnAllowNullTrue() {
        Assertions.assertTrue(Fixture.partitioned().table.isPartitionColumnAllowNull());
    }

    // ==================== connector range-view path (e.g. iceberg) ====================

    private static final long FRESH_555 = 555L;
    private static final long FRESH_777 = 777L;
    private static final long NEWEST_UPDATE_TIME = 1_900_000_000_000L;

    private static ConnectorMvccPartition rangePart(String name, String low, String high, long freshness) {
        return new ConnectorMvccPartition(name, Collections.singletonList(low),
                high == null ? Collections.emptyList() : Collections.singletonList(high), freshness);
    }

    private static ConnectorMvccPartitionView rangeView(ConnectorMvccPartition... parts) {
        return new ConnectorMvccPartitionView(ConnectorMvccPartitionView.Style.RANGE,
                ConnectorMvccPartitionView.Freshness.SNAPSHOT_ID, Arrays.asList(parts), NEWEST_UPDATE_TIME);
    }

    @Test
    public void testRangeViewBuildsRangePartitionTypeAndItems() {
        Fixture f = Fixture.rangeView(rangeView(
                rangePart("p20240101", "2024-01-01", "2024-01-02", FRESH_555)));

        // MUTATION: deriving LIST/UNPARTITIONED from getPartitionColumns().size() (ignoring the connector's
        // RANGE style) makes this red — a roll-up MTMV with date_trunc requires RANGE or it throws.
        Assertions.assertEquals(PartitionType.RANGE, f.table.getPartitionType(Optional.empty()),
                "the connector's RANGE style must drive getPartitionType");

        Map<String, PartitionItem> items = f.table.getNameToPartitionItems(Optional.empty());
        Assertions.assertEquals(1, items.size());
        PartitionItem item = items.get("p20240101");
        Assertions.assertTrue(item instanceof RangePartitionItem, "expected a RangePartitionItem");
        com.google.common.collect.Range<PartitionKey> range = ((RangePartitionItem) item).getItems();
        // [2024-01-01, 2024-01-02) built from the connector's pre-rendered closed/open bounds.
        Assertions.assertEquals("2024-01-01", range.lowerEndpoint().getKeys().get(0).getStringValue());
        Assertions.assertEquals("2024-01-02", range.upperEndpoint().getKeys().get(0).getStringValue());
    }

    @Test
    public void testRangeViewNullMinPartitionDerivesSuccessorUpperBound() {
        // An EMPTY upper bound denotes the NULL-min partition: fe-core derives the exclusive upper as the
        // column-type successor of the lower key (the connector cannot — it has no Doris Column). Parity with
        // master IcebergUtils.getPartitionRange's nullLowKey.successor().
        Fixture f = Fixture.rangeView(rangeView(
                rangePart("pnull", "0000-01-01", null, FRESH_777)));

        Map<String, PartitionItem> items = f.table.getNameToPartitionItems(Optional.empty());
        com.google.common.collect.Range<PartitionKey> range =
                ((RangePartitionItem) items.get("pnull")).getItems();
        Assertions.assertEquals("0000-01-01", range.lowerEndpoint().getKeys().get(0).getStringValue());
        // MUTATION: building the upper from the (empty) tuple instead of lower.successor() throws or yields the
        // wrong bound -> red. DATEV2 successor of 0000-01-01 is 0000-01-02.
        Assertions.assertEquals("0000-01-02", range.upperEndpoint().getKeys().get(0).getStringValue(),
                "the NULL-min partition's exclusive upper must be lowerKey.successor()");
    }

    @Test
    public void testRangeViewPartitionSnapshotIsSnapshotId() throws AnalysisException {
        Fixture f = Fixture.rangeView(rangeView(
                rangePart("p20240101", "2024-01-01", "2024-01-02", FRESH_555)));

        // MUTATION: wrapping the freshness value in MTMVTimestampSnapshot (ignoring the SNAPSHOT_ID freshness
        // kind) makes this ClassCastException/red — MTMV change-detection must compare snapshot ids, not millis.
        MTMVSnapshotIdSnapshot snap = (MTMVSnapshotIdSnapshot) f.table.getPartitionSnapshot(
                "p20240101", null, Optional.empty());
        Assertions.assertEquals(FRESH_555, snap.getSnapshotVersion(),
                "a snapshot-id-freshness view must pin the per-partition snapshot id");

        // Table snapshot stays the connector pin id.
        Assertions.assertEquals(PINNED_SNAPSHOT_ID,
                ((MTMVSnapshotIdSnapshot) f.table.getTableSnapshot(Optional.empty())).getSnapshotVersion());

        // An unknown partition still throws (parity with the legacy path).
        Assertions.assertThrows(AnalysisException.class,
                () -> f.table.getPartitionSnapshot("missing", null, Optional.empty()));
    }

    @Test
    public void testRangeViewNewestUpdateTimeUsesMonotonicMarkerNotSnapshotId() {
        // The dictionary auto-refresh path needs a MONOTONIC marker; the per-partition snapshot ids are not
        // monotonic. getNewestUpdateVersionOrTime must return the view's newest-update-time, NOT a max over the
        // snapshot-id freshness values.
        Fixture f = Fixture.rangeView(rangeView(
                rangePart("p20240101", "2024-01-01", "2024-01-02", FRESH_555),
                rangePart("p20240202", "2024-02-02", "2024-02-03", FRESH_777)));

        // MUTATION: returning max(freshness)=777 (the legacy max-over-the-map path) instead of the view's
        // newest-update-time makes this red — proving the view path reads newestUpdateTimeMillis.
        Assertions.assertEquals(NEWEST_UPDATE_TIME, f.table.getNewestUpdateVersionOrTime(),
                "the range-view path must answer the dictionary with the monotonic newest-update-time");
    }

    @Test
    public void testRangeViewValidRelatedTableMirrorsStyle() {
        // RANGE style => valid related table; UNPARTITIONED style (the connector's eligibility gate failed) =>
        // invalid, so MTMVTask stops the refresh loud. MUTATION: always returning true (the interface default)
        // makes the UNPARTITIONED assertion red.
        Fixture valid = Fixture.rangeView(rangeView(
                rangePart("p20240101", "2024-01-01", "2024-01-02", FRESH_555)));
        Assertions.assertTrue(valid.table.isValidRelatedTable(),
                "a RANGE range-view table is a valid related table");

        Fixture invalid = Fixture.rangeView(ConnectorMvccPartitionView.unpartitioned());
        Assertions.assertEquals(PartitionType.UNPARTITIONED,
                invalid.table.getPartitionType(Optional.empty()));
        Assertions.assertFalse(invalid.table.isValidRelatedTable(),
                "an UNPARTITIONED range-view table is NOT a valid related table (stops MTMV refresh)");
    }

    @Test
    public void testRangeViewAppliesSnapshotBeforeQueryingViewOnPinnedHandle() {
        // Snapshot-consistency: the query-begin pin must be threaded onto the handle (applySnapshot) BEFORE
        // getMvccPartitionView, and the view must be enumerated from that pinned handle — so the MTMV partition
        // set/freshness stays consistent with the data-scan pin. MUTATION: querying the view on the BASE handle
        // (or before applySnapshot) makes the InOrder / pinnedHandle verify red.
        Fixture f = Fixture.rangeView(rangeView(
                rangePart("p20240101", "2024-01-01", "2024-01-02", FRESH_555)));
        f.table.getNameToPartitionItems(Optional.empty());

        InOrder ord = Mockito.inOrder(f.metadata);
        ord.verify(f.metadata).applySnapshot(Mockito.eq(f.session), Mockito.eq(f.handle), Mockito.any());
        ord.verify(f.metadata).getMvccPartitionView(f.session, f.pinnedHandle);
        // The legacy listPartitions path must NOT run when a range view is present.
        Mockito.verify(f.metadata, Mockito.never())
                .listPartitions(Mockito.any(), Mockito.any(), Mockito.any());
    }

    @Test
    public void testAbsentRangeViewKeepsLegacyListPath() throws AnalysisException {
        // Paimon-parity guard: a connector WITHOUT a range view (getMvccPartitionView empty) keeps the legacy
        // listPartitions/LIST/timestamp path byte-unchanged. MUTATION: defaulting to a RANGE/empty view when the
        // connector returns empty would flip this to RANGE and skip listPartitions -> red.
        Fixture f = Fixture.partitioned();   // does NOT stub getMvccPartitionView -> Mockito returns empty
        // Materialize ONCE (the single allowed round-trip), then read both accessors off that pin so the
        // verify(...) counts below are unambiguous.
        Optional<MvccSnapshot> pin =
                Optional.of(f.table.loadSnapshot(Optional.empty(), Optional.empty()));
        Assertions.assertEquals(PartitionType.LIST, f.table.getPartitionType(pin),
                "an absent range view must keep the legacy LIST path");
        MTMVTimestampSnapshot ts = (MTMVTimestampSnapshot) f.table.getPartitionSnapshot(
                "dt=2024-01-01", null, pin);
        Assertions.assertEquals(TS_2024_01_01, ts.getSnapshotVersion(),
                "the legacy path must keep timestamp freshness");
        // The connector WAS consulted for a range view (and returned empty), then the legacy list path ran.
        Mockito.verify(f.metadata).getMvccPartitionView(Mockito.any(), Mockito.any());
        Mockito.verify(f.metadata).listPartitions(Mockito.any(), Mockito.any(), Mockito.any());
    }

    // ==================== fixtures / helpers ====================

    private static ConnectorPartitionInfo cpi(String name, long lastModifiedMillis) {
        return new ConnectorPartitionInfo(name, Collections.emptyMap(), Collections.emptyMap(),
                ConnectorPartitionInfo.UNKNOWN, ConnectorPartitionInfo.UNKNOWN, lastModifiedMillis,
                ConnectorPartitionInfo.UNKNOWN, orderedValuesOf(name), Collections.emptyList());
    }

    /**
     * Like {@link #cpi} but with the ordered values supplied EXPLICITLY rather than derived from the
     * rendered name — needed for the spec-evolution shapes, where a row's value count legitimately
     * differs from the current spec's field count (see
     * {@code testZeroValuesFromUnpartitionedOriginDegradeToUnpartitioned}).
     */
    private static ConnectorPartitionInfo cpiValues(String name, long lastModifiedMillis,
            List<String> orderedValues) {
        return new ConnectorPartitionInfo(name, Collections.emptyMap(), Collections.emptyMap(),
                ConnectorPartitionInfo.UNKNOWN, ConnectorPartitionInfo.UNKNOWN, lastModifiedMillis,
                ConnectorPartitionInfo.UNKNOWN, orderedValues, Collections.emptyList());
    }

    /** Like {@link #cpi} but with connector-supplied per-value SQL-NULL flags (the opt-in path). */
    private static ConnectorPartitionInfo cpiNull(String name, long lastModifiedMillis, boolean... nullFlags) {
        List<Boolean> flags = new ArrayList<>(nullFlags.length);
        for (boolean b : nullFlags) {
            flags.add(b);
        }
        return new ConnectorPartitionInfo(name, Collections.emptyMap(), Collections.emptyMap(),
                ConnectorPartitionInfo.UNKNOWN, ConnectorPartitionInfo.UNKNOWN, lastModifiedMillis,
                ConnectorPartitionInfo.UNKNOWN, orderedValuesOf(name), flags);
    }

    /**
     * One already-parsed value per name segment — what a real connector now supplies (mirrors the
     * connector-side {@code HiveWriteUtils.toPartitionValues}). fe-core no longer parses the rendered
     * name itself, so a fixture that supplies none is a mis-wired connector, not a valid input.
     */
    private static List<String> orderedValuesOf(String partitionName) {
        List<String> values = new ArrayList<>();
        for (String segment : partitionName.split("/", -1)) {
            int eq = segment.indexOf('=');
            values.add(eq < 0 ? segment : segment.substring(eq + 1));
        }
        return values;
    }

    /**
     * Runs {@code body} with {@code snapshot} pinned for {@code table} in a thread-local
     * {@link ConnectContext}'s {@link StatementContext}, then clears the thread-local.
     */
    private static void withContextSnapshot(PluginDrivenMvccExternalTable table,
            MvccSnapshot snapshot, Runnable body) {
        ConnectContext ctx = new ConnectContext();
        StatementContext stmtCtx = new StatementContext(ctx, null);
        ctx.setStatementContext(stmtCtx);
        ctx.setThreadLocalInfo();
        try {
            stmtCtx.setSnapshot(new MvccTableInfo(table), snapshot);
            body.run();
        } finally {
            ConnectContext.remove();
        }
    }

    /**
     * Wires a {@link PluginDrivenMvccExternalTable} over a mocked connector/metadata, stubbing the
     * LATEST schema cache so {@code getPartitionColumns()} returns a single DATE column {@code dt}.
     * The {@code timeTravel()} variant additionally stubs the time-travel SPI methods so
     * {@code loadSnapshot} with an explicit spec resolves to a known snapshot + at-snapshot schema.
     */
    private static final class Fixture {
        static final long TT_SCHEMA_ID = 9L;

        final PluginDrivenMvccExternalTable table;
        final ConnectorMetadata metadata;
        final ConnectorTableHandle handle;
        final ConnectorTableHandle pinnedHandle;
        final ConnectorSession session;
        final PluginDrivenSchemaCacheValue latestCacheValue;
        final ConnectorMvccSnapshot resolvedSnapshot;

        private Fixture(PluginDrivenMvccExternalTable table, ConnectorMetadata metadata,
                ConnectorTableHandle handle, ConnectorTableHandle pinnedHandle, ConnectorSession session,
                PluginDrivenSchemaCacheValue latestCacheValue, ConnectorMvccSnapshot resolvedSnapshot) {
            this.table = table;
            this.metadata = metadata;
            this.handle = handle;
            this.pinnedHandle = pinnedHandle;
            this.session = session;
            this.latestCacheValue = latestCacheValue;
            this.resolvedSnapshot = resolvedSnapshot;
        }

        /** Captures the {@link ConnectorTimeTravelSpec} passed to {@code resolveTimeTravel}. */
        ConnectorTimeTravelSpec captureSpec() {
            ArgumentCaptor<ConnectorTimeTravelSpec> captor =
                    ArgumentCaptor.forClass(ConnectorTimeTravelSpec.class);
            Mockito.verify(metadata).resolveTimeTravel(Mockito.any(), Mockito.any(), captor.capture());
            return captor.getValue();
        }

        static Fixture partitioned() {
            return with(Arrays.asList(
                    cpi("dt=2024-01-01", TS_2024_01_01),
                    cpi("dt=2024-02-02", TS_2024_02_02)));
        }

        static Fixture with(List<ConnectorPartitionInfo> partitions) {
            return build(partitions, false);
        }

        static Fixture with(List<ConnectorPartitionInfo> partitions, Type partitionColType) {
            return build(partitions, false, partitionColType);
        }

        /** Adds time-travel SPI stubs on top of the base fixture. */
        static Fixture timeTravel() {
            return build(Arrays.asList(
                    cpi("dt=2024-01-01", TS_2024_01_01),
                    cpi("dt=2024-02-02", TS_2024_02_02)), true);
        }

        /**
         * Base fixture but with {@code getTableHandle(...)} re-stubbed to {@link Optional#empty()},
         * exercising the no-handle degrade (materializeLatest empty-pin) and the time-travel no-handle
         * guard (loadSnapshot throwing).
         */
        static Fixture noHandle() {
            Fixture f = partitioned();
            Mockito.when(f.metadata.getTableHandle(f.session, "REMOTE_DB", "REMOTE_TBL"))
                    .thenReturn(Optional.empty());
            return f;
        }

        /**
         * Base fixture wired for the connector range-view path: {@code applySnapshot} threads the query-begin
         * pin onto the handle (returning the branch-aware {@code pinnedHandle}) and {@code getMvccPartitionView}
         * returns {@code view} from THAT pinned handle, so a test can assert the apply-before-view ordering and
         * the snapshot-consistent enumeration. The partition column is the default DATEV2 {@code dt}.
         */
        static Fixture rangeView(ConnectorMvccPartitionView view) {
            Fixture f = partitioned();
            Mockito.when(f.metadata.applySnapshot(Mockito.eq(f.session), Mockito.eq(f.handle), Mockito.any()))
                    .thenReturn(f.pinnedHandle);
            Mockito.when(f.metadata.getMvccPartitionView(f.session, f.pinnedHandle))
                    .thenReturn(Optional.of(view));
            return f;
        }

        private static Fixture build(List<ConnectorPartitionInfo> partitions, boolean timeTravel) {
            return build(partitions, timeTravel, Type.DATEV2);
        }

        private static Fixture build(List<ConnectorPartitionInfo> partitions, boolean timeTravel,
                Type partitionColType) {
            ConnectorMetadata metadata = Mockito.mock(ConnectorMetadata.class);
            ConnectorSession session = Mockito.mock(ConnectorSession.class);
            ConnectorTableHandle handle = Mockito.mock(ConnectorTableHandle.class);
            ConnectorTableHandle pinnedHandle = Mockito.mock(ConnectorTableHandle.class);
            TestablePluginCatalog catalog = new TestablePluginCatalog(metadata, session);
            ExternalDatabase<PluginDrivenExternalTable> db = mockDb("REMOTE_DB");

            Mockito.when(metadata.getTableHandle(session, "REMOTE_DB", "REMOTE_TBL"))
                    .thenReturn(Optional.of(handle));
            Mockito.when(metadata.beginQuerySnapshot(session, handle))
                    .thenReturn(Optional.of(
                            ConnectorMvccSnapshot.builder().snapshotId(PINNED_SNAPSHOT_ID).build()));
            Mockito.when(metadata.listPartitions(Mockito.eq(session), Mockito.eq(handle), Mockito.any()))
                    .thenReturn(partitions);
            // A Mockito mock does NOT run interface default methods (returns null for these), so mimic the SPI
            // default here: a snapshot-id connector (paimon/iceberg) surfaces no last-modified freshness. The
            // last-modified tests below re-stub these to a present value.
            Mockito.when(metadata.getTableFreshness(Mockito.any(), Mockito.any()))
                    .thenReturn(Optional.empty());
            Mockito.when(metadata.getPartitionFreshnessMillis(Mockito.any(), Mockito.any(), Mockito.any()))
                    .thenReturn(OptionalLong.empty());

            // Single partition column "dt" (DATE by default; VARCHAR variant exercises the genuine-null
            // string-key path) — the LATEST schema.
            List<Column> schema = Collections.singletonList(new Column("dt", partitionColType));
            PluginDrivenSchemaCacheValue latestCacheValue = new PluginDrivenSchemaCacheValue(
                    schema, schema, Collections.singletonList("dt"));

            ConnectorMvccSnapshot resolvedSnapshot = ConnectorMvccSnapshot.builder()
                    .snapshotId(7L).schemaId(TT_SCHEMA_ID).build();

            if (timeTravel) {
                // resolveTimeTravel succeeds; applySnapshot returns the branch-aware pinnedHandle;
                // getTableSchema(..,snapshot) returns the AT-SNAPSHOT schema (column "v1"), distinct
                // from the latest schema (column "dt"). fromRemoteColumnName is identity.
                Mockito.when(metadata.resolveTimeTravel(Mockito.eq(session), Mockito.eq(handle),
                        Mockito.any())).thenReturn(Optional.of(resolvedSnapshot));
                Mockito.when(metadata.applySnapshot(session, handle, resolvedSnapshot))
                        .thenReturn(pinnedHandle);
                ConnectorTableSchema atSchema = new ConnectorTableSchema("REMOTE_TBL",
                        Collections.singletonList(new ConnectorColumn("v1", ConnectorType.of("INT"),
                                "", true, null)),
                        "", Collections.emptyMap());
                Mockito.when(metadata.getTableSchema(Mockito.eq(session), Mockito.any(),
                        Mockito.any(ConnectorMvccSnapshot.class))).thenReturn(atSchema);
                Mockito.when(metadata.fromRemoteColumnName(Mockito.eq(session), Mockito.any(),
                        Mockito.any(), Mockito.anyString()))
                        .thenAnswer(inv -> inv.getArgument(3, String.class));
            }

            PluginDrivenMvccExternalTable table =
                    new PluginDrivenMvccExternalTable(1L, "tbl", "REMOTE_TBL", catalog, db) {
                        @Override
                        protected synchronized void makeSureInitialized() {
                            // no-op: skip Env-backed catalog/db init
                        }

                        @Override
                        protected Optional<SchemaCacheValue> getLatestSchemaCacheValue() {
                            // Bypass the live Env-backed schema cache; route the LATEST seam to the
                            // canned value so the real getSchemaCacheValue() override is exercised.
                            return Optional.of(latestCacheValue);
                        }
                    };
            return new Fixture(table, metadata, handle, pinnedHandle, session, latestCacheValue,
                    resolvedSnapshot);
        }
    }

    @SuppressWarnings("unchecked")
    private static ExternalDatabase<PluginDrivenExternalTable> mockDb(String remoteName) {
        ExternalDatabase<PluginDrivenExternalTable> db = Mockito.mock(ExternalDatabase.class);
        Mockito.when(db.getRemoteName()).thenReturn(remoteName);
        // Needed so MvccTableInfo(table) -> db.getFullName()/db.getCatalog().getName() resolve in the
        // context-pin tests.
        Mockito.when(db.getFullName()).thenReturn("test_db");
        ExternalCatalog ctl = Mockito.mock(ExternalCatalog.class);
        Mockito.when(ctl.getName()).thenReturn("test_catalog");
        Mockito.when(db.getCatalog()).thenReturn(ctl);
        return db;
    }

    /**
     * Minimal catalog returning a fixed connector/session without standing up the Doris
     * environment (mirrors PluginDrivenExternalTablePartitionTest.TestablePluginCatalog).
     */
    private static final class TestablePluginCatalog extends PluginDrivenExternalCatalog {
        private final Connector connector;
        private final ConnectorSession session;

        TestablePluginCatalog(ConnectorMetadata metadata, ConnectorSession session) {
            this(mockConnector(metadata, session), session);
        }

        private TestablePluginCatalog(Connector connector, ConnectorSession session) {
            super(1L, "test-catalog", null, makeProps(), "", connector);
            this.connector = connector;
            this.session = session;
        }

        private static Connector mockConnector(ConnectorMetadata metadata, ConnectorSession session) {
            Connector c = Mockito.mock(Connector.class);
            Mockito.when(c.getMetadata(session)).thenReturn(metadata);
            return c;
        }

        @Override
        public Connector getConnector() {
            return connector;
        }

        @Override
        public ConnectorSession buildConnectorSession() {
            return session;
        }

        @Override
        protected List<String> listDatabaseNames() {
            return Collections.emptyList();
        }

        @Override
        protected List<String> listTableNamesFromRemote(SessionContext ctx, String dbName) {
            return Collections.emptyList();
        }

        @Override
        public boolean tableExist(SessionContext ctx, String dbName, String tblName) {
            return false;
        }

        private static Map<String, String> makeProps() {
            Map<String, String> props = new HashMap<>();
            props.put("type", "mvcc-test");
            return props;
        }
    }
}
