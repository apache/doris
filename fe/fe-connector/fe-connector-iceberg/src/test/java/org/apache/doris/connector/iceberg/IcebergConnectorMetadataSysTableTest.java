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
import org.apache.doris.connector.api.handle.ConnectorColumnHandle;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.mvcc.ConnectorMvccSnapshot;

import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/**
 * Tests for the iceberg E7 system-table capability (P6.5-T03/T04): {@code listSupportedSysTables} and
 * {@code getSysTableHandle} (T03), plus the sys-aware schema/columns reload path
 * ({@code getTableSchema}/{@code getColumnHandles} for a sys handle, T04). The scan-plane sys split path
 * lands in T05; the sys-handle identity / serialization invariants are pinned by
 * {@link IcebergTableHandleTest} (T02).
 *
 * <p>Like the other metadata tests these drive a {@link RecordingIcebergCatalogOps} fake with a
 * {@code null} real catalog, so they stay entirely offline (no live remote iceberg).
 *
 * <p>KEY iceberg-vs-paimon deviations pinned here:
 * <ul>
 *   <li>Deviation 1 (time travel): unlike paimon's {@code forSystemTable} (which clears the pin), an
 *       iceberg sys handle RETAINS the base handle's snapshot/ref/schema pin — iceberg system tables
 *       legally time-travel ({@code t$snapshots FOR VERSION/TIME AS OF ...}).</li>
 *   <li>Deviation 4 (position_deletes) is RETIRED: it used to be excluded (Q2, taken when legacy also
 *       rejected it with "not supported yet"), but upstream #65135 implemented it natively, so the
 *       connector now exposes it like any other metadata table. It is the only one whose SCAN diverges —
 *       BE reads it with a native reader instead of the JNI serialized-split path (see
 *       {@code IcebergScanPlanProvider.doPlanPositionDeletesSystemTableScan}).</li>
 *   <li>Lazy resolution: {@code getSysTableHandle} does NOT load the base table or build the
 *       metadata-table (the handle carries no SDK Table; the build happens lazily in
 *       {@code getTableSchema}/scan, mirroring legacy {@code IcebergSysExternalTable.getSysIcebergTable}).
 *       So a sys handle resolves with ZERO catalog round-trips.</li>
 * </ul>
 */
public class IcebergConnectorMetadataSysTableTest {

    private static IcebergConnectorMetadata metadataWith(RecordingIcebergCatalogOps ops) {
        return new IcebergConnectorMetadata(ops, Collections.emptyMap(), new RecordingConnectorContext());
    }

    private static IcebergConnectorMetadata metadataWith(
            RecordingIcebergCatalogOps ops, RecordingConnectorContext ctx) {
        return new IcebergConnectorMetadata(ops, Collections.emptyMap(), ctx);
    }

    private static IcebergTableHandle baseHandle() {
        return new IcebergTableHandle("db1", "t1");
    }

    /**
     * The canonical supported-sys-table list, recomputed from the SDK source of truth: every
     * {@link MetadataTableType}, lower-cased. Mirrors {@code IcebergSysTable.SUPPORTED_SYS_TABLES} so the
     * test pins "connector == the fe-core formula" without hardcoding the (SDK-version-dependent) count.
     * {@code POSITION_DELETES} is included since the native port (upstream #65135).
     */
    private static List<String> expectedSupported() {
        List<String> names = new ArrayList<>();
        for (MetadataTableType type : MetadataTableType.values()) {
            names.add(type.name().toLowerCase(Locale.ROOT));
        }
        return names;
    }

    // ---------------------------------------------------------------------
    // listSupportedSysTables
    // ---------------------------------------------------------------------

    @Test
    public void listSupportedSysTablesMirrorsMetadataTableTypes() {
        List<String> result = metadataWith(new RecordingIcebergCatalogOps())
                .listSupportedSysTables(null, baseHandle());

        // WHY: the set of selectable "$sys" tables a user sees per iceberg table IS exactly
        // MetadataTableType.values(), lower-cased (fe-core IcebergSysTable.SUPPORTED_SYS_TABLES is built
        // from that same formula). If this drifted, users could no longer reference e.g. mytable$snapshots.
        // MUTATION: returning Collections.emptyList() (the SPI default) -> red; re-adding any filter ->
        // expected (full) != actual -> red.
        Assertions.assertEquals(expectedSupported(), result,
                "must mirror MetadataTableType.values() (lower-cased), in order");
        Assertions.assertFalse(result.isEmpty(), "supported sys tables must be non-empty");
        // A representative spread of the metadata tables a user actually queries.
        Assertions.assertTrue(result.containsAll(java.util.Arrays.asList(
                        "snapshots", "history", "files", "manifests", "partitions", "refs", "entries")),
                "the supported list must include the common iceberg metadata tables");
    }

    @Test
    public void listSupportedSysTablesIncludesPositionDeletes() {
        List<String> result = metadataWith(new RecordingIcebergCatalogOps())
                .listSupportedSysTables(null, baseHandle());

        // WHY: upstream #65135 implemented $position_deletes natively, so it MUST be advertised — this is
        // the whole point of the port. While the connector excluded it (the former Q2 decision, taken when
        // legacy also rejected it), "select * from t$position_deletes" failed with "Unknown sys table" on
        // this branch while working on master = a capability regression. MUTATION: restoring the
        // `type != POSITION_DELETES` filter in listSupportedSysTables -> red.
        Assertions.assertTrue(result.contains("position_deletes"),
                "position_deletes must be advertised as a supported sys table (upstream #65135)");
    }

    @Test
    public void listSupportedSysTablesIsUnmodifiable() {
        List<String> result = metadataWith(new RecordingIcebergCatalogOps())
                .listSupportedSysTables(null, baseHandle());

        // WHY: the returned list is a defensive copy the connector hands out connector-global; a caller
        // must not be able to mutate the connector's view. MUTATION: returning a bare mutable ArrayList
        // (no Collections.unmodifiableList wrap) -> add() succeeds, no throw -> red.
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> result.add("injected"),
                "the supported-sys-table list must be unmodifiable (defensive copy)");
    }

    @Test
    public void listSupportedSysTablesIgnoresBaseHandle() {
        // WHY: the supported set is connector-global (every iceberg table exposes the same metadata
        // tables), so it must not depend on — or NPE on — the base handle. A null base handle must still
        // yield the full canonical list. MUTATION: deriving the list from the base handle (e.g. reading
        // base.getTableName()) -> NPE on a null handle -> red.
        List<String> result = metadataWith(new RecordingIcebergCatalogOps())
                .listSupportedSysTables(null, null);
        Assertions.assertEquals(expectedSupported(), result,
                "the supported list is connector-global and independent of the base handle");
    }

    // ---------------------------------------------------------------------
    // getSysTableHandle — supported names
    // ---------------------------------------------------------------------

    @Test
    public void getSysTableHandleReturnsSysHandleForSupportedName() {
        Optional<ConnectorTableHandle> opt = metadataWith(new RecordingIcebergCatalogOps())
                .getSysTableHandle(null, baseHandle(), "snapshots");

        // WHY: a supported sys name must yield a sys handle that self-describes (isSystemTable + bare sys
        // name) and carries the base table's db/table coordinates (so downstream schema/scan read the
        // right table). MUTATION: returning Optional.empty() (the SPI default) -> red.
        Assertions.assertTrue(opt.isPresent(), "a supported sys table must yield a handle");
        IcebergTableHandle handle = (IcebergTableHandle) opt.get();
        Assertions.assertTrue(handle.isSystemTable(), "the returned handle must be a sys handle");
        Assertions.assertEquals("snapshots", handle.getSysTableName());
        Assertions.assertEquals("db1", handle.getDbName());
        Assertions.assertEquals("t1", handle.getTableName());
    }

    @Test
    public void getSysTableHandleNormalizesNameToLowercase() {
        IcebergTableHandle handle = (IcebergTableHandle) metadataWith(new RecordingIcebergCatalogOps())
                .getSysTableHandle(null, baseHandle(), "SNAPSHOTS").get();

        // WHY: the STORED canonical sys name must be lower-cased so a mixed-case input and its lower-case
        // form yield the SAME handle (identical equals/hashCode/toString and the same metadata-table build
        // later). NOTE on the case-insensitive accept: legacy RESOLUTION is itself case-SENSITIVE —
        // TableIf.findSysTable does a plain Map.get against IcebergSysTable's lower-cased keyset and the
        // suffix is taken verbatim (SysTable.getTableNameWithSysTableName does not lower-case it) — so a
        // mixed-case "t$SNAPSHOTS" never resolves, and only lower-case canonical names are ever fed to
        // getSysTableHandle (PluginDrivenSysExternalTable threads the matched lower-case name). The
        // connector's equalsIgnoreCase support check is thus a harmless, production-unreachable superset;
        // MetadataTableType.from's own case-insensitivity acts at metadata-table BUILD time (resolveSysTable),
        // NOT this resolution gate. The lower-casing here is for canonical handle-identity parity. MUTATION:
        // storing sysName verbatim -> getSysTableName() == "SNAPSHOTS" and toString ends "$SNAPSHOTS" -> red.
        Assertions.assertEquals("snapshots", handle.getSysTableName(),
                "the stored sys name must be normalized to lower case");
        Assertions.assertTrue(handle.toString().endsWith("$snapshots}"),
                "toString must render the canonical lower-case suffix");
    }

    @Test
    public void getSysTableHandleRetainsSnapshotPin() {
        // A base handle carrying an explicit time-travel pin (snapshot id + ref + schema id).
        IcebergTableHandle pinnedBase = baseHandle().withSnapshot(42L, "br", 7L);

        IcebergTableHandle sysHandle = (IcebergTableHandle) metadataWith(new RecordingIcebergCatalogOps())
                .getSysTableHandle(null, pinnedBase, "snapshots").get();

        // WHY: deviation 1 — iceberg system tables legally time-travel (t$snapshots FOR VERSION/TIME AS
        // OF ...), so the sys handle MUST carry the base handle's snapshot/ref/schema pin through. This is
        // the OPPOSITE of paimon (whose forSystemTable clears the pin). Dropping the pin would silently
        // read t$snapshots at the LATEST version under a time-travel query -> a correctness regression.
        // MUTATION: building the sys handle without threading base.getSnapshotId()/getRef()/getSchemaId()
        // (e.g. forSystemTable(db, table, sys, -1, null, -1)) -> red.
        Assertions.assertTrue(sysHandle.isSystemTable());
        Assertions.assertEquals("snapshots", sysHandle.getSysTableName());
        Assertions.assertEquals(42L, sysHandle.getSnapshotId(),
                "the base snapshot-id pin must be retained on the sys handle (time travel)");
        Assertions.assertEquals("br", sysHandle.getRef(),
                "the base ref pin must be retained on the sys handle (time travel)");
        Assertions.assertEquals(7L, sysHandle.getSchemaId(),
                "the base schema-id pin must be retained on the sys handle (time travel)");
    }

    // ---------------------------------------------------------------------
    // getSysTableHandle — not exposed (empty)
    // ---------------------------------------------------------------------

    @Test
    public void getSysTableHandleResolvesPositionDeletes() {
        Optional<ConnectorTableHandle> opt = metadataWith(new RecordingIcebergCatalogOps())
                .getSysTableHandle(null, baseHandle(), "position_deletes");

        // WHY: name resolution is the gate the whole $position_deletes feature hangs off — an empty handle
        // here means fe-core renders "Unknown sys table" and the native scan path is never reached, no
        // matter how correct IcebergScanPlanProvider is. The handle must also be flagged isSystemTable so
        // planScanInternal routes it to the metadata path rather than the data-file one. MUTATION:
        // re-adding the POSITION_DELETES skip in isSupportedSysTable -> Optional.empty() -> red.
        Assertions.assertTrue(opt.isPresent(),
                "position_deletes must resolve to a sys handle (upstream #65135)");
        IcebergTableHandle sysHandle = (IcebergTableHandle) opt.get();
        Assertions.assertTrue(sysHandle.isSystemTable(), "the position_deletes handle must be a sys handle");
        Assertions.assertEquals("position_deletes", sysHandle.getSysTableName(),
                "the sys name must be the canonical lower-cased one (handle identity)");
    }

    @Test
    public void getSysTableHandleEmptyForUnknownName() {
        Optional<ConnectorTableHandle> opt = metadataWith(new RecordingIcebergCatalogOps())
                .getSysTableHandle(null, baseHandle(), "not_a_sys_table");

        // WHY: an unsupported name is "this connector does not expose that sys table" (empty), not an
        // error. MUTATION: returning a handle for any name (dropping the isSupportedSysTable guard) -> red.
        Assertions.assertFalse(opt.isPresent(), "an unknown sys name must yield Optional.empty()");
    }

    @Test
    public void getSysTableHandleNullNameReturnsEmpty() {
        Optional<ConnectorTableHandle> opt = metadataWith(new RecordingIcebergCatalogOps())
                .getSysTableHandle(null, baseHandle(), null);

        // WHY: the Javadoc contract is "or empty if not exposed" — a null sysName is simply not an
        // exposed sys table, so it must return Optional.empty(), NOT NPE on toLowerCase/equalsIgnoreCase.
        // MUTATION: removing the null-guard in isSupportedSysTable -> NPE -> the test errors (red).
        Assertions.assertFalse(opt.isPresent(), "a null sys name must yield Optional.empty()");
    }

    // ---------------------------------------------------------------------
    // lazy resolution — no catalog round-trip at handle-resolution time
    // ---------------------------------------------------------------------

    @Test
    public void getSysTableHandleDoesNotTouchCatalogSeam() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();

        Optional<ConnectorTableHandle> opt =
                metadataWith(ops, ctx).getSysTableHandle(null, baseHandle(), "snapshots");

        // WHY: unlike paimon (whose handle stashes a transient SDK Table, so it eagerly loads at
        // resolution), the iceberg handle carries NO SDK Table — the metadata-table is built lazily in
        // getTableSchema/scan (mirroring legacy IcebergSysExternalTable.getSysIcebergTable). Resolving a
        // sys handle is therefore PURE: zero catalog round-trips and no auth scope. Loading the base
        // table here would be wasted work (the result can't be stored on the handle, so it'd be rebuilt
        // downstream) — a perf regression vs legacy. MUTATION: adding an eager
        // context.executeAuthenticated(catalogOps.loadTable(...)) in getSysTableHandle -> ops.log
        // non-empty and ctx.authCount == 1 -> red.
        Assertions.assertTrue(opt.isPresent(), "precondition: snapshots is a supported sys table");
        Assertions.assertTrue(ops.log.isEmpty(),
                "getSysTableHandle must not touch the catalog seam (lazy resolution)");
        Assertions.assertEquals(0, ctx.authCount,
                "getSysTableHandle must not open an auth scope (no remote read at resolution)");
    }

    // ---------------------------------------------------------------------
    // getTableSchema / getColumnHandles for a sys handle (T04)
    // ---------------------------------------------------------------------
    //
    // These exercise the sys-aware schema/columns reload: the metadata-table is built lazily from the
    // BASE table via MetadataTableUtils.createMetadataTableInstance. createMetadataTableInstance needs a
    // real org.apache.iceberg.Table (HasTableOperations) — a FakeIcebergTable is NOT one — so the base is
    // a real InMemoryCatalog table wired through the recording seam (ops.table). The base columns
    // (id, name) are deliberately DIFFERENT from every metadata-table's columns so reading the wrong
    // schema is detectable.

    /** Base data-table schema: deliberately disjoint from every metadata-table's columns. */
    private static final Schema BASE_SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()));

    /** A real iceberg {@link Table} (a {@code BaseTable} with working {@code operations()}/{@code io()}). */
    private static Table inMemoryBaseTable() {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("test", Collections.emptyMap());
        catalog.createNamespace(Namespace.of("db1"));
        return catalog.createTable(
                TableIdentifier.of("db1", "t1"), BASE_SCHEMA, PartitionSpec.unpartitioned());
    }

    private static List<String> columnNames(ConnectorTableSchema schema) {
        List<String> names = new ArrayList<>();
        for (ConnectorColumn c : schema.getColumns()) {
            names.add(c.getName());
        }
        return names;
    }

    @Test
    public void getTableSchemaForSysHandleBuildsColumnsFromMetadataTable() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.table = inMemoryBaseTable();
        IcebergConnectorMetadata md = metadataWith(ops);

        IcebergTableHandle sysHandle = (IcebergTableHandle)
                md.getSysTableHandle(null, baseHandle(), "snapshots").get();
        List<String> names = columnNames(md.getTableSchema(null, sysHandle));

        // WHY: a sys handle's schema MUST come from the iceberg METADATA table (t$snapshots ->
        // committed_at/snapshot_id/...), not the base table — mirroring legacy
        // IcebergSysExternalTable.getOrCreateSchemaCacheValue (parseSchema of the sys table's schema).
        // Surfacing the base columns for a sys-table query would be a silent correctness bug. MUTATION:
        // dropping the isSystemTable() branch in getTableSchema -> base columns [id, name] -> red.
        Assertions.assertTrue(names.containsAll(Arrays.asList(
                        "committed_at", "snapshot_id", "parent_id", "operation", "manifest_list", "summary")),
                "sys schema must expose the snapshots metadata-table columns, got " + names);
        Assertions.assertFalse(names.contains("id"), "must NOT surface the base table's columns");
        Assertions.assertFalse(names.contains("name"), "must NOT surface the base table's columns");
    }

    @Test
    public void getTableSchemaForSysHandleUsesSysNameTypeNotHardcoded() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.table = inMemoryBaseTable();
        IcebergConnectorMetadata md = metadataWith(ops);

        IcebergTableHandle sysHandle = (IcebergTableHandle)
                md.getSysTableHandle(null, baseHandle(), "history").get();
        List<String> names = columnNames(md.getTableSchema(null, sysHandle));

        // WHY: the metadata-table TYPE must come from the handle's sys name via
        // MetadataTableType.from(getSysTableName()), NOT a hardcoded SNAPSHOTS. The history table
        // exposes made_current_at/is_current_ancestor (which the snapshots table does not), and does NOT
        // expose the snapshots-only committed_at/manifest_list. MUTATION: hardcoding
        // MetadataTableType.SNAPSHOTS -> snapshots columns (committed_at present, is_current_ancestor
        // absent) -> red.
        Assertions.assertTrue(names.containsAll(Arrays.asList(
                        "made_current_at", "snapshot_id", "parent_id", "is_current_ancestor")),
                "sys schema must expose the history metadata-table columns, got " + names);
        Assertions.assertFalse(names.contains("committed_at"),
                "history must not carry the snapshots-only columns (the sys type must thread through)");
        Assertions.assertFalse(names.contains("manifest_list"),
                "history must not carry the snapshots-only columns (the sys type must thread through)");
    }

    @Test
    public void getTableSchemaForSysHandleThreadsMappingFlags() {
        // committed_at is a TIMESTAMP-with-zone column of the snapshots metadata table, so its mapped
        // Doris type depends on enable.mapping.timestamp_tz -- a clean probe that the connector's
        // properties flags thread into the SYS-table schema parse (deviation 5).
        IcebergConnectorMetadata mdDefault = metadataWith(seamWith(inMemoryBaseTable()));

        Map<String, String> tzProps = new HashMap<>();
        tzProps.put(IcebergConnectorProperties.ENABLE_MAPPING_TIMESTAMP_TZ, "true");
        RecordingIcebergCatalogOps opsTz = seamWith(inMemoryBaseTable());
        IcebergConnectorMetadata mdTz =
                new IcebergConnectorMetadata(opsTz, tzProps, new RecordingConnectorContext());

        ConnectorColumn committedDefault = committedAtColumn(mdDefault);
        ConnectorColumn committedTz = committedAtColumn(mdTz);

        // WHY: deviation 5 -- the sys branch must reuse parseSchema so the per-catalog enable.mapping.*
        // flags (read from the connector properties) reach the metadata-table schema. With the flag ON,
        // committed_at maps to TIMESTAMPTZ; OFF (default) it does not. A sys branch that parsed the schema
        // WITHOUT threading the flags would yield identical types. MUTATION: building the sys schema from
        // a flag-less parse -> equal types -> red.
        Assertions.assertNotEquals(committedDefault.getType(), committedTz.getType(),
                "enable.mapping.timestamp_tz must change the sys-table committed_at mapping");
        Assertions.assertEquals("TIMESTAMPTZ", committedTz.getType().getTypeName(),
                "with the flag on, the sys-table committed_at must map to TIMESTAMPTZ");
    }

    private static ConnectorColumn committedAtColumn(IcebergConnectorMetadata md) {
        IcebergTableHandle sysHandle = (IcebergTableHandle)
                md.getSysTableHandle(null, baseHandle(), "snapshots").get();
        for (ConnectorColumn c : md.getTableSchema(null, sysHandle).getColumns()) {
            if (c.getName().equals("committed_at")) {
                return c;
            }
        }
        throw new AssertionError("the snapshots metadata table must expose a committed_at column");
    }

    private static RecordingIcebergCatalogOps seamWith(Table base) {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.table = base;
        return ops;
    }

    @Test
    public void getTableSchemaForSysHandleLoadsBaseInsideAuthScope() {
        RecordingIcebergCatalogOps ops = seamWith(inMemoryBaseTable());
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        IcebergConnectorMetadata md = metadataWith(ops, ctx);

        IcebergTableHandle sysHandle = (IcebergTableHandle)
                md.getSysTableHandle(null, baseHandle(), "snapshots").get();
        md.getTableSchema(null, sysHandle);

        // WHY: the metadata-table is built from the BASE table loaded via the seam with the BASE
        // coordinates (db1.t1, NOT a "$snapshots"-suffixed name), wrapped in exactly ONE auth scope (the
        // Kerberos UGI must cover the remote base load; mirrors legacy
        // IcebergSysExternalTable.getSysIcebergTable and the data-table getTableSchema). MUTATION:
        // loading by getTableName()+"$"+sys -> lastLoadTable "t1$snapshots" -> red; double-wrapping the
        // auth scope -> authCount 2 -> red.
        Assertions.assertEquals("db1", ops.lastLoadDb, "the base table must be loaded by the base db");
        Assertions.assertEquals("t1", ops.lastLoadTable,
                "the base table must be loaded by the BARE base name (not a $sys suffix)");
        Assertions.assertEquals(1, ctx.authCount, "exactly one auth scope must wrap the sys schema load");
    }

    @Test
    public void getTableSchemaForSysHandleRunsInsideAuthenticator() {
        RecordingIcebergCatalogOps ops = seamWith(inMemoryBaseTable());
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        ctx.failAuth = true; // executeAuthenticated throws WITHOUT running the wrapped task
        IcebergConnectorMetadata md = metadataWith(ops, ctx);
        IcebergTableHandle sysHandle = (IcebergTableHandle)
                md.getSysTableHandle(null, baseHandle(), "snapshots").get();

        // WHY: the remote base load (and the metadata-table build) sit INSIDE
        // context.executeAuthenticated -- when auth fails, the wrapped task must not run, so the catalog
        // seam is never touched. This proves the load is auth-scoped (Kerberos UGI parity), not a bare
        // unauthenticated catalogOps.loadTable. MUTATION: calling catalogOps.loadTable OUTSIDE
        // executeAuthenticated -> ops.log records "loadTable:db1.t1" despite failAuth -> red.
        Assertions.assertThrows(RuntimeException.class,
                () -> md.getTableSchema(null, sysHandle),
                "an auth failure must surface as a RuntimeException");
        Assertions.assertTrue(ops.log.isEmpty(),
                "the catalog seam must not be touched when the auth scope fails (load is inside auth)");
    }

    @Test
    public void getTableSchemaAtSnapshotForSysHandleUsesMetadataTableSchema() {
        RecordingIcebergCatalogOps ops = seamWith(inMemoryBaseTable());
        IcebergConnectorMetadata md = metadataWith(ops);
        IcebergTableHandle sysHandle = (IcebergTableHandle)
                md.getSysTableHandle(null, baseHandle(), "snapshots").get();

        // A pinned snapshot carrying a NON-negative schema id -- the case the data-table @snapshot path
        // would route to table.schemas().get(schemaId). An iceberg sys handle legally carries a
        // time-travel pin (deviation 1), so this overload IS reachable for a sys handle.
        ConnectorMvccSnapshot pinned =
                ConnectorMvccSnapshot.builder().snapshotId(7L).schemaId(0L).build();
        List<String> names = columnNames(md.getTableSchema(null, sysHandle, pinned));

        // WHY: an iceberg metadata table has a FIXED schema, independent of snapshot/schema-version
        // (t$snapshots always exposes committed_at/snapshot_id/...; legacy has no schema-at-snapshot for
        // sys tables). The @snapshot overload must therefore still build the metadata-table schema, NOT
        // read base.schemas().get(schemaId) (which would surface the base columns). MUTATION: dropping the
        // isSystemTable() short-circuit in the @snapshot overload -> base schema [id, name] -> red.
        Assertions.assertTrue(names.containsAll(Arrays.asList(
                        "committed_at", "snapshot_id", "manifest_list")),
                "the @snapshot sys schema must still be the metadata-table schema, got " + names);
        Assertions.assertFalse(names.contains("id"), "must not fall back to the base table schema");
    }

    @Test
    public void getColumnHandlesForSysHandleBuildsFromMetadataTable() {
        RecordingIcebergCatalogOps ops = seamWith(inMemoryBaseTable());
        IcebergConnectorMetadata md = metadataWith(ops);
        IcebergTableHandle sysHandle = (IcebergTableHandle)
                md.getSysTableHandle(null, baseHandle(), "snapshots").get();

        Map<String, ConnectorColumnHandle> handles = md.getColumnHandles(null, sysHandle);

        // WHY: the generic PluginDrivenScanNode.buildColumnHandles looks up each query slot in this map by
        // name, so a sys handle MUST expose the METADATA-table columns (t$snapshots), not the base table's
        // -- otherwise a sys-table scan could not resolve its slots (or would resolve the wrong field).
        // Pairs with the getTableSchema sys branch (same loadSysTable helper). MUTATION: dropping the
        // isSystemTable() branch in getColumnHandles -> base keys [id, name] -> red.
        Assertions.assertTrue(handles.keySet().containsAll(Arrays.asList(
                        "committed_at", "snapshot_id", "operation", "manifest_list")),
                "sys column handles must be keyed by the metadata-table column names, got " + handles.keySet());
        Assertions.assertFalse(handles.containsKey("id"), "must not expose the base table's columns");
        Assertions.assertFalse(handles.containsKey("name"), "must not expose the base table's columns");
    }

    // ---------------------------------------------------------------------
    // P6.5-T07 gap-fill
    // ---------------------------------------------------------------------

    @Test
    public void getColumnHandlesForSysHandleLoadsBaseInsideAuthScope() {
        RecordingIcebergCatalogOps ops = seamWith(inMemoryBaseTable());
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        IcebergConnectorMetadata md = metadataWith(ops, ctx);
        IcebergTableHandle sysHandle = (IcebergTableHandle)
                md.getSysTableHandle(null, baseHandle(), "snapshots").get();

        md.getColumnHandles(null, sysHandle);

        // WHY (T07 gap-fill): getColumnHandles shares loadSysTable with getTableSchema, so the BASE load
        // must sit in exactly ONE auth scope by the BARE base coordinates -- but only getTableSchema pinned
        // this. MUTATION: loading by a "$"-suffixed name -> lastLoadTable "t1$snapshots" -> red;
        // double-wrapping / no auth scope -> authCount != 1 -> red.
        Assertions.assertEquals("db1", ops.lastLoadDb);
        Assertions.assertEquals("t1", ops.lastLoadTable,
                "getColumnHandles must load the BARE base name (not a $sys suffix)");
        Assertions.assertEquals(1, ctx.authCount,
                "exactly one auth scope must wrap the sys column-handle load");
    }

    @Test
    public void getColumnHandlesForSysHandleRunsInsideAuthenticator() {
        RecordingIcebergCatalogOps ops = seamWith(inMemoryBaseTable());
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        ctx.failAuth = true; // executeAuthenticated throws WITHOUT running the wrapped task
        IcebergConnectorMetadata md = metadataWith(ops, ctx);
        IcebergTableHandle sysHandle = (IcebergTableHandle)
                md.getSysTableHandle(null, baseHandle(), "snapshots").get();

        // WHY (T07 gap-fill): like getTableSchema, the getColumnHandles base load must sit INSIDE
        // executeAuthenticated, so an auth failure leaves the catalog seam untouched. MUTATION: loading
        // OUTSIDE the auth scope -> ops.log records the load despite failAuth -> red.
        Assertions.assertThrows(RuntimeException.class, () -> md.getColumnHandles(null, sysHandle));
        Assertions.assertTrue(ops.log.isEmpty(),
                "the catalog seam must not be touched when the auth scope fails");
    }

    @Test
    public void getColumnHandlesKeysetMatchesSchemaForSysHandle() {
        RecordingIcebergCatalogOps ops = seamWith(inMemoryBaseTable());
        IcebergConnectorMetadata md = metadataWith(ops);
        IcebergTableHandle sysHandle = (IcebergTableHandle)
                md.getSysTableHandle(null, baseHandle(), "snapshots").get();

        java.util.Set<String> schemaNames =
                new java.util.HashSet<>(columnNames(md.getTableSchema(null, sysHandle)));
        java.util.Set<String> handleKeys = md.getColumnHandles(null, sysHandle).keySet();

        // WHY (T07 gap-fill, #969249): the BE scan-slot names (getTableSchema -> parseSchema) and the
        // column-handle keys (getColumnHandles) are produced by two INDEPENDENT loops over the same
        // metadata table; they match only by construction (same source + same lowercasing).
        // PluginDrivenScanNode.buildColumnHandles resolves each schema slot against the handle map by name,
        // so a drift (one path drops lowercasing or reads a different schema) leaves BE slots unresolvable
        // -- yet each method's own containsAll test still passes. MUTATION: key getColumnHandles by
        // field.name() (no lowercase) while getTableSchema keeps lowercasing -> the two sets diverge -> red.
        Assertions.assertEquals(schemaNames, handleKeys,
                "getColumnHandles keys must equal the getTableSchema column names by construction (#969249)");
    }

    @Test
    public void getSysTableHandleEmptyForBlankName() {
        // WHY (T07 gap-fill): null and unknown names each yield Optional.empty and are pinned; the
        // empty-string loop-fallthrough (isSupportedSysTable's null-guard does NOT cover "") is not.
        // Legacy TableIf.findSysTable also returns empty for an empty sys name (parity). MUTATION:
        // special-casing "" to a present handle -> isPresent() true -> red.
        Assertions.assertFalse(
                metadataWith(new RecordingIcebergCatalogOps())
                        .getSysTableHandle(null, baseHandle(), "").isPresent(),
                "an empty sys name must yield Optional.empty()");
    }
}
