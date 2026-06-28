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

import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorTableSchema;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.mvcc.ConnectorMvccPartition;
import org.apache.doris.connector.api.mvcc.ConnectorMvccPartitionView;
import org.apache.doris.connector.api.mvcc.ConnectorMvccSnapshot;
import org.apache.doris.connector.api.mvcc.ConnectorTimeTravelSpec;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * MVCC / time-travel tests for {@link IcebergConnectorMetadata} (T07), mirroring the paimon connector's
 * {@code PaimonConnectorMetadataMvccTest}. Uses a real {@link InMemoryCatalog} table (the
 * {@link RecordingIcebergCatalogOps} fake serves it through the seam) carrying TWO snapshots across a column
 * RENAME, plus a tag at the first snapshot and a branch at the second — so the resolution, schema-at-snapshot,
 * and ref-pinning paths are exercised against genuine iceberg metadata (no Mockito).
 */
public class IcebergConnectorMetadataMvccTest {

    private static final Schema SCHEMA_V0 = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()));

    /** A real iceberg table with two snapshots across a rename, a tag at S1, a branch at S2. */
    private static final class Fixture {
        Table table;
        long s1;
        long s2;
        long schemaIdS1;
        long schemaIdS2;
        long tsS2;
    }

    private static Fixture fixture() {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("test", Collections.emptyMap());
        catalog.createNamespace(Namespace.of("db1"));
        Table table = catalog.createTable(
                TableIdentifier.of("db1", "t1"), SCHEMA_V0, PartitionSpec.unpartitioned());

        // Snapshot S1 under schema v0 (id, name).
        table.newAppend().appendFile(dataFile("s3://b/db1/t1/f1.parquet")).commit();
        Fixture f = new Fixture();
        f.s1 = table.currentSnapshot().snapshotId();
        f.schemaIdS1 = table.currentSnapshot().schemaId();

        // Rename name -> fullname (new schema version), then snapshot S2 under it.
        table.updateSchema().renameColumn("name", "fullname").commit();
        table.newAppend().appendFile(dataFile("s3://b/db1/t1/f2.parquet")).commit();
        f.s2 = table.currentSnapshot().snapshotId();
        f.schemaIdS2 = table.currentSnapshot().schemaId();
        f.tsS2 = table.currentSnapshot().timestampMillis();

        // tag1 -> S1 (schema v0), b1 -> S2 (schema v1).
        table.manageSnapshots().createTag("tag1", f.s1).commit();
        table.manageSnapshots().createBranch("b1", f.s2).commit();

        f.table = table;
        // Schema actually evolved (the rename created a NEW schema id).
        Assertions.assertNotEquals(f.schemaIdS1, f.schemaIdS2, "the rename must create a new schema version");
        return f;
    }

    private static DataFile dataFile(String path) {
        return DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath(path).withFileSizeInBytes(100).withRecordCount(1).withFormat(FileFormat.PARQUET).build();
    }

    private static IcebergConnectorMetadata metadataFor(Table table, RecordingIcebergCatalogOps ops) {
        ops.table = table;
        return new IcebergConnectorMetadata(ops, Collections.emptyMap(), new RecordingConnectorContext());
    }

    private static ConnectorTableHandle handle() {
        return new IcebergTableHandle("db1", "t1");
    }

    private static List<String> columnNames(ConnectorTableSchema schema) {
        return schema.getColumns().stream().map(ConnectorColumn::getName).collect(Collectors.toList());
    }

    // ---------------------------------------------------------------------
    // beginQuerySnapshot
    // ---------------------------------------------------------------------

    @Test
    public void beginQuerySnapshotPinsCurrentSnapshotAndLatestSchema() {
        Fixture f = fixture();
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        Optional<ConnectorMvccSnapshot> snap = metadataFor(f.table, ops).beginQuerySnapshot(null, handle());
        // WHY: the query-begin pin is the LATEST snapshot + LATEST schema id (legacy getLatestIcebergSnapshot).
        // MUTATION: pinning currentSnapshot().schemaId() instead of table.schema().schemaId() would still be
        // schemaIdS2 here (same after the latest snapshot), so the load-bearing assertion is "current snapshot".
        Assertions.assertTrue(snap.isPresent());
        Assertions.assertEquals(f.s2, snap.get().getSnapshotId());
        Assertions.assertEquals(f.schemaIdS2, snap.get().getSchemaId());
        // The remote load goes through the seam (auth-wrapped).
        Assertions.assertTrue(ops.log.contains("loadTable:db1.t1"));
    }

    @Test
    public void beginQuerySnapshotEmptyTablePinsMinusOne() {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("test", Collections.emptyMap());
        catalog.createNamespace(Namespace.of("db1"));
        Table empty = catalog.createTable(
                TableIdentifier.of("db1", "t1"), SCHEMA_V0, PartitionSpec.unpartitioned());
        Optional<ConnectorMvccSnapshot> snap =
                metadataFor(empty, new RecordingIcebergCatalogOps()).beginQuerySnapshot(null, handle());
        // WHY: an empty table still pins (iceberg supports MVCC), at snapshot id -1 (legacy UNKNOWN_SNAPSHOT_ID).
        Assertions.assertTrue(snap.isPresent());
        Assertions.assertEquals(-1L, snap.get().getSnapshotId());
    }

    @Test
    public void beginQuerySnapshotEnabledCachePinsStableAndLoadsOnce() {
        Fixture f = fixture();
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.table = f.table;
        // An ENABLED cache (TTL 100s) injected via the 4-arg ctor — the production wiring (IcebergConnector
        // injects its per-catalog cache here). T08.
        IcebergConnectorMetadata md = new IcebergConnectorMetadata(
                ops, Collections.emptyMap(), new RecordingConnectorContext(),
                new IcebergLatestSnapshotCache(100, 1000));
        Optional<ConnectorMvccSnapshot> first = md.beginQuerySnapshot(null, handle());
        Optional<ConnectorMvccSnapshot> second = md.beginQuerySnapshot(null, handle());
        // WHY: within the TTL the second query reuses the cached pin (same snapshot + schema) WITHOUT re-loading
        // the table — the legacy with-cache catalog stability + I/O saving. MUTATION: not consulting the cache
        // (live every call) -> loadTable runs twice -> red.
        Assertions.assertEquals(f.s2, first.get().getSnapshotId());
        Assertions.assertEquals(f.s2, second.get().getSnapshotId());
        Assertions.assertEquals(f.schemaIdS2, second.get().getSchemaId());
        long loads = ops.log.stream().filter(s -> s.equals("loadTable:db1.t1")).count();
        Assertions.assertEquals(1, loads, "an enabled cache must load the table at most once within the TTL");
    }

    @Test
    public void beginQuerySnapshotDisabledCacheLoadsEveryCall() {
        Fixture f = fixture();
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        // The default 3-arg ctor wires a DISABLED cache (ttl=0) -> always live (preserves T07 semantics for the
        // direct-construction tests). MUTATION: defaulting to an enabled cache -> loads==1 -> red.
        IcebergConnectorMetadata md = metadataFor(f.table, ops);
        md.beginQuerySnapshot(null, handle());
        md.beginQuerySnapshot(null, handle());
        long loads = ops.log.stream().filter(s -> s.equals("loadTable:db1.t1")).count();
        Assertions.assertEquals(2, loads, "a disabled cache must read live (load) on every query");
    }

    // ---------------------------------------------------------------------
    // resolveTimeTravel
    // ---------------------------------------------------------------------

    @Test
    public void resolveSnapshotIdResolvesAndCarriesItsSchema() {
        Fixture f = fixture();
        Optional<ConnectorMvccSnapshot> snap = metadataFor(f.table, new RecordingIcebergCatalogOps())
                .resolveTimeTravel(null, handle(), ConnectorTimeTravelSpec.snapshotId(String.valueOf(f.s1)));
        Assertions.assertTrue(snap.isPresent());
        Assertions.assertEquals(f.s1, snap.get().getSnapshotId());
        // S1 was committed under schema v0 — its schemaId() is the OLD version, not the latest.
        Assertions.assertEquals(f.schemaIdS1, snap.get().getSchemaId());
    }

    @Test
    public void resolveSnapshotIdMissingIsEmpty() {
        Fixture f = fixture();
        // WHY: a non-existent id is "not found" (empty), which fe-core renders as the user-facing error — NOT
        // an exception (that is reserved for a malformed spec).
        Assertions.assertFalse(metadataFor(f.table, new RecordingIcebergCatalogOps())
                .resolveTimeTravel(null, handle(), ConnectorTimeTravelSpec.snapshotId("999999")).isPresent());
    }

    @Test
    public void resolveTimestampDigitalAtOrBefore() {
        Fixture f = fixture();
        // Digital epoch-millis at S2's commit time -> the at-or-before snapshot is S2.
        Optional<ConnectorMvccSnapshot> snap = metadataFor(f.table, new RecordingIcebergCatalogOps())
                .resolveTimeTravel(null, handle(),
                        ConnectorTimeTravelSpec.timestamp(String.valueOf(f.tsS2), true));
        Assertions.assertTrue(snap.isPresent());
        Assertions.assertEquals(f.s2, snap.get().getSnapshotId());
        Assertions.assertEquals(f.schemaIdS2, snap.get().getSchemaId());
    }

    @Test
    public void resolveTimestampBeforeAnySnapshotIsEmpty() {
        Fixture f = fixture();
        Assertions.assertFalse(metadataFor(f.table, new RecordingIcebergCatalogOps())
                .resolveTimeTravel(null, handle(), ConnectorTimeTravelSpec.timestamp("1", true)).isPresent(),
                "a time before any snapshot must resolve to empty (not found), not throw");
    }

    @Test
    public void resolveTagPinsByRefAndSchema() {
        Fixture f = fixture();
        Optional<ConnectorMvccSnapshot> snap = metadataFor(f.table, new RecordingIcebergCatalogOps())
                .resolveTimeTravel(null, handle(), ConnectorTimeTravelSpec.tag("tag1"));
        Assertions.assertTrue(snap.isPresent());
        Assertions.assertEquals(f.s1, snap.get().getSnapshotId());
        Assertions.assertEquals(f.schemaIdS1, snap.get().getSchemaId());
        // The ref NAME is carried so applySnapshot can scan.useRef(name) (legacy parity, not pin-by-id).
        Assertions.assertEquals("tag1", snap.get().getProperties().get(IcebergConnectorMetadata.REF_PROPERTY));
    }

    @Test
    public void resolveBranchPinsByRefAndSchema() {
        Fixture f = fixture();
        Optional<ConnectorMvccSnapshot> snap = metadataFor(f.table, new RecordingIcebergCatalogOps())
                .resolveTimeTravel(null, handle(), ConnectorTimeTravelSpec.branch("b1"));
        Assertions.assertTrue(snap.isPresent());
        Assertions.assertEquals(f.s2, snap.get().getSnapshotId());
        Assertions.assertEquals(f.schemaIdS2, snap.get().getSchemaId());
        Assertions.assertEquals("b1", snap.get().getProperties().get(IcebergConnectorMetadata.REF_PROPERTY));
    }

    @Test
    public void resolveTagRejectsABranchNameAndViceVersa() {
        Fixture f = fixture();
        IcebergConnectorMetadata md = metadataFor(f.table, new RecordingIcebergCatalogOps());
        // WHY: legacy validates the ref kind (a branch used as @tag, or a tag used as @branch, is "not found").
        Assertions.assertFalse(md.resolveTimeTravel(null, handle(),
                ConnectorTimeTravelSpec.tag("b1")).isPresent(), "a branch name must not resolve as a tag");
        Assertions.assertFalse(md.resolveTimeTravel(null, handle(),
                ConnectorTimeTravelSpec.branch("tag1")).isPresent(), "a tag name must not resolve as a branch");
    }

    @Test
    public void resolveIncrementalFailsLoud() {
        Fixture f = fixture();
        // WHY: legacy iceberg never dispatched @incr (it silently read latest); fail loud instead of a wrong
        // silent read.
        Assertions.assertThrows(DorisConnectorException.class, () ->
                metadataFor(f.table, new RecordingIcebergCatalogOps()).resolveTimeTravel(null, handle(),
                        ConnectorTimeTravelSpec.incremental(Collections.singletonMap("k", "v"))));
    }

    // ---------------------------------------------------------------------
    // applySnapshot
    // ---------------------------------------------------------------------

    @Test
    public void applySnapshotThreadsIdAndSchema() {
        Fixture f = fixture();
        ConnectorMvccSnapshot snap = ConnectorMvccSnapshot.builder().snapshotId(f.s1).schemaId(f.schemaIdS1).build();
        IcebergTableHandle pinned = (IcebergTableHandle) metadataFor(f.table, new RecordingIcebergCatalogOps())
                .applySnapshot(null, handle(), snap);
        Assertions.assertTrue(pinned.hasSnapshotPin());
        Assertions.assertEquals(f.s1, pinned.getSnapshotId());
        Assertions.assertEquals(f.schemaIdS1, pinned.getSchemaId());
        Assertions.assertNull(pinned.getRef());
    }

    @Test
    public void applySnapshotThreadsRef() {
        Fixture f = fixture();
        ConnectorMvccSnapshot snap = ConnectorMvccSnapshot.builder()
                .snapshotId(f.s1).schemaId(f.schemaIdS1).property(IcebergConnectorMetadata.REF_PROPERTY, "tag1")
                .build();
        IcebergTableHandle pinned = (IcebergTableHandle) metadataFor(f.table, new RecordingIcebergCatalogOps())
                .applySnapshot(null, handle(), snap);
        Assertions.assertEquals("tag1", pinned.getRef());
    }

    @Test
    public void applySnapshotLatestPinLeavesHandleUnchanged() {
        Fixture f = fixture();
        IcebergConnectorMetadata md = metadataFor(f.table, new RecordingIcebergCatalogOps());
        ConnectorTableHandle bare = handle();
        // null snapshot and an empty-table (-1, no ref) pin must both read latest (handle unchanged) — a
        // useSnapshot(-1) would be a non-existent snapshot.
        Assertions.assertSame(bare, md.applySnapshot(null, bare, null));
        IcebergTableHandle afterMinusOne = (IcebergTableHandle) md.applySnapshot(null, bare,
                ConnectorMvccSnapshot.builder().snapshotId(-1L).build());
        Assertions.assertFalse(afterMinusOne.hasSnapshotPin());
    }

    // ---------------------------------------------------------------------
    // getTableSchema(@snapshot)
    // ---------------------------------------------------------------------

    @Test
    public void getTableSchemaAtSnapshotReadsTheHistoricalSchema() {
        Fixture f = fixture();
        IcebergConnectorMetadata md = metadataFor(f.table, new RecordingIcebergCatalogOps());
        // schema v0 (S1) still has "name"; schema v1 (S2/latest) has "fullname".
        ConnectorTableSchema atV0 = md.getTableSchema(null, handle(),
                ConnectorMvccSnapshot.builder().snapshotId(f.s1).schemaId(f.schemaIdS1).build());
        ConnectorTableSchema atV1 = md.getTableSchema(null, handle(),
                ConnectorMvccSnapshot.builder().snapshotId(f.s2).schemaId(f.schemaIdS2).build());
        Assertions.assertEquals(java.util.Arrays.asList("id", "name"), columnNames(atV0));
        Assertions.assertEquals(java.util.Arrays.asList("id", "fullname"), columnNames(atV1));
    }

    @Test
    public void getTableSchemaNullOrUnknownSnapshotFallsBackToLatest() {
        Fixture f = fixture();
        IcebergConnectorMetadata md = metadataFor(f.table, new RecordingIcebergCatalogOps());
        // null snapshot and schemaId<0 both fall back to the latest schema (fullname).
        Assertions.assertEquals(java.util.Arrays.asList("id", "fullname"),
                columnNames(md.getTableSchema(null, handle(), null)));
        Assertions.assertEquals(java.util.Arrays.asList("id", "fullname"), columnNames(md.getTableSchema(
                null, handle(), ConnectorMvccSnapshot.builder().snapshotId(f.s2).schemaId(-1L).build())));
    }

    // ---------------------------------------------------------------------
    // T10 parity gap-fills (audit wf_9d88fe61-5c7: MVCC-1 schema-only-ALTER divergence, MVCC-2 datetime string)
    // ---------------------------------------------------------------------

    @Test
    public void beginQuerySnapshotPinsLatestSchemaAfterSchemaOnlyAlter() {
        Fixture f = fixture();
        // A schema-only ALTER with NO new append: table.schema().schemaId() advances, but currentSnapshot()
        // (and ITS schemaId) stays at S2 — a schema change never creates a new snapshot. The pin must carry the
        // LATEST table schema id (legacy getLatestIcebergSnapshot reads table.schema().schemaId()), NOT the
        // lagging current-snapshot schema id. The existing beginQuerySnapshot test cannot catch this because
        // there the two ids coincide. MUTATION: pinning currentSnapshot().schemaId() -> schemaIdS2 here -> red.
        f.table.updateSchema().addColumn("extra", Types.IntegerType.get()).commit();
        long latestSchemaId = f.table.schema().schemaId();
        long currentSnapshotSchemaId = f.table.currentSnapshot().schemaId();
        Assertions.assertNotEquals(currentSnapshotSchemaId, latestSchemaId,
                "a schema-only ALTER must advance the schema id past the current snapshot's schema id");
        Assertions.assertEquals(f.schemaIdS2, currentSnapshotSchemaId,
                "no new snapshot was committed, so the current snapshot's schema id is unchanged");

        Optional<ConnectorMvccSnapshot> snap =
                metadataFor(f.table, new RecordingIcebergCatalogOps()).beginQuerySnapshot(null, handle());
        Assertions.assertTrue(snap.isPresent());
        // The pinned snapshot did NOT advance (still S2), but the pinned SCHEMA is the latest, not S2's.
        Assertions.assertEquals(f.s2, snap.get().getSnapshotId());
        Assertions.assertEquals(latestSchemaId, snap.get().getSchemaId());
        Assertions.assertNotEquals(currentSnapshotSchemaId, snap.get().getSchemaId());
    }

    @Test
    public void resolveTimestampDatetimeStringResolvesSnapshot() {
        Fixture f = fixture();
        // The user-facing `FOR TIME AS OF '2024-01-02 12:34:56'` form: a NON-digital datetime string
        // (isDigital == false) must route through IcebergTimeUtils.datetimeToMillis(session zone) ->
        // SnapshotUtil.snapshotIdAsOfTime, distinct from the digital epoch-millis parseLong path the existing
        // test drives. A null session resolves to UTC (resolveSessionZone); zone-correctness itself is pinned by
        // IcebergTimeUtilsTest. Format one second AFTER S2 in UTC so the second-precision parse (which truncates
        // sub-second millis) still lands at-or-after S2's commit -> resolves to S2. MUTATION: routing the
        // datetime string through the digital parseLong branch -> NumberFormatException -> red; never wiring the
        // datetime branch through resolveTimeTravel -> empty/wrong snapshot -> red.
        String datetime = java.time.Instant.ofEpochMilli(f.tsS2 + 1000)
                .atZone(java.time.ZoneOffset.UTC)
                .format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        Optional<ConnectorMvccSnapshot> snap = metadataFor(f.table, new RecordingIcebergCatalogOps())
                .resolveTimeTravel(null, handle(), ConnectorTimeTravelSpec.timestamp(datetime, false));
        Assertions.assertTrue(snap.isPresent(), "datetime string at-or-after S2 must resolve");
        Assertions.assertEquals(f.s2, snap.get().getSnapshotId());
        Assertions.assertEquals(f.schemaIdS2, snap.get().getSchemaId());
    }

    // ---------------------------------------------------------------------
    // B-2: getMvccPartitionView / listPartitionNames (connector level: auth wrap + not-exist degrade)
    // The math/merge/gate parity is exhaustively covered by IcebergPartitionUtilsTest; these tests pin the
    // connector wiring (delegation, the executeAuthenticated scope, the concurrent-drop degrade).
    // ---------------------------------------------------------------------

    private static final Schema PARTITIONED_SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "ts", Types.TimestampType.withoutZone()));

    /** A real db1.t1 table partitioned by day(ts) with one data file at day=100 (1970-04-11). */
    private static Table dayPartitionedTable() {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("test", Collections.emptyMap());
        catalog.createNamespace(Namespace.of("db1"));
        PartitionSpec spec = PartitionSpec.builderFor(PARTITIONED_SCHEMA).day("ts").build();
        Table table = catalog.createTable(TableIdentifier.of("db1", "t1"), PARTITIONED_SCHEMA, spec);
        table.newAppend().appendFile(DataFiles.builder(spec)
                .withPath("s3://b/db1/t1/f1.parquet").withFileSizeInBytes(100).withRecordCount(1)
                .withPartitionPath("ts_day=1970-04-11").withFormat(FileFormat.PARQUET).build()).commit();
        return catalog.loadTable(TableIdentifier.of("db1", "t1"));
    }

    @Test
    public void getMvccPartitionViewReturnsRangeView() {
        Table table = dayPartitionedTable();
        Optional<ConnectorMvccPartitionView> view =
                metadataFor(table, new RecordingIcebergCatalogOps()).getMvccPartitionView(null, handle());
        Assertions.assertTrue(view.isPresent());
        Assertions.assertEquals(ConnectorMvccPartitionView.Style.RANGE, view.get().getStyle());
        Assertions.assertEquals(ConnectorMvccPartitionView.Freshness.SNAPSHOT_ID, view.get().getFreshness());
        List<String> names = view.get().getPartitions().stream()
                .map(ConnectorMvccPartition::getName).collect(Collectors.toList());
        Assertions.assertEquals(Collections.singletonList("ts_day=100"), names);
    }

    @Test
    public void getMvccPartitionViewFailsLoudWhenTableMissing() {
        // The MTMV partition/freshness path FAILS LOUD on a not-found base table (master parity + Rule 12): the
        // common dropped-table case is already absorbed by the generic model at handle resolution, so a not-found
        // HERE is the narrow concurrent-drop race, where masking a vanished base table as "unpartitioned/fresh"
        // would silently under-refresh the MV. MUTATION: degrading to unpartitioned() here -> no throw -> red.
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.throwNoSuchTableOnLoadTable = true;
        IcebergConnectorMetadata md =
                new IcebergConnectorMetadata(ops, Collections.emptyMap(), new RecordingConnectorContext());
        Assertions.assertThrows(RuntimeException.class, () -> md.getMvccPartitionView(null, handle()));
    }

    @Test
    public void getMvccPartitionViewRunsInsideAuthContext() {
        // failAuth throws WITHOUT invoking the task, so the remote PARTITIONS scan must sit INSIDE the wrap:
        // loadTable is never reached. Proves the Kerberos UGI scope covers the metadata read.
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.table = dayPartitionedTable();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        ctx.failAuth = true;
        IcebergConnectorMetadata md = new IcebergConnectorMetadata(ops, Collections.emptyMap(), ctx);
        Assertions.assertThrows(RuntimeException.class, () -> md.getMvccPartitionView(null, handle()));
        Assertions.assertEquals(1, ctx.authCount);
        Assertions.assertFalse(ops.log.contains("loadTable:db1.t1"), "loadTable must sit inside executeAuthenticated");
    }

    @Test
    public void listPartitionNamesReturnsRawNames() {
        Table table = dayPartitionedTable();
        List<String> names = metadataFor(table, new RecordingIcebergCatalogOps())
                .listPartitionNames(null, handle());
        Assertions.assertEquals(Collections.singletonList("ts_day=100"), names);
    }

    @Test
    public void listPartitionNamesDegradesToEmptyWhenTableMissing() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.throwNoSuchTableOnLoadTable = true;
        List<String> names =
                new IcebergConnectorMetadata(ops, Collections.emptyMap(), new RecordingConnectorContext())
                        .listPartitionNames(null, handle());
        Assertions.assertTrue(names.isEmpty());
    }

    @Test
    public void listPartitionNamesRunsInsideAuthContext() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.table = dayPartitionedTable();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        ctx.failAuth = true;
        IcebergConnectorMetadata md = new IcebergConnectorMetadata(ops, Collections.emptyMap(), ctx);
        Assertions.assertThrows(RuntimeException.class, () -> md.listPartitionNames(null, handle()));
        Assertions.assertEquals(1, ctx.authCount);
        Assertions.assertFalse(ops.log.contains("loadTable:db1.t1"), "loadTable must sit inside executeAuthenticated");
    }
}
