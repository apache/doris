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

import org.apache.doris.connector.api.handle.ConnectorTableHandle;

import org.apache.iceberg.MetadataTableType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

/**
 * Tests for the iceberg E7 system-table capability (P6.5-T03): {@code listSupportedSysTables} and
 * {@code getSysTableHandle}. The sys-aware schema reload path ({@code getTableSchema(sysHandle)}) and
 * the scan-plane sys split path land in later tasks (T04/T05); the sys-handle identity / serialization
 * invariants are pinned by {@link IcebergTableHandleTest} (T02).
 *
 * <p>Like the other metadata tests these drive a {@link RecordingIcebergCatalogOps} fake with a
 * {@code null} real catalog, so they stay entirely offline (no live remote iceberg).
 *
 * <p>KEY iceberg-vs-paimon deviations pinned here:
 * <ul>
 *   <li>Deviation 1 (time travel): unlike paimon's {@code forSystemTable} (which clears the pin), an
 *       iceberg sys handle RETAINS the base handle's snapshot/ref/schema pin — iceberg system tables
 *       legally time-travel ({@code t$snapshots FOR VERSION/TIME AS OF ...}).</li>
 *   <li>Deviation 4 (position_deletes, Q2): {@code position_deletes} is NOT exposed, so
 *       {@code getSysTableHandle("position_deletes")} is empty (fe-core renders a generic not-found).</li>
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
     * {@link MetadataTableType} except {@code POSITION_DELETES}, lower-cased. Mirrors legacy
     * {@code IcebergSysTable.SUPPORTED_SYS_TABLES} so the test pins "connector == the legacy formula"
     * without hardcoding the (SDK-version-dependent) count.
     */
    private static List<String> expectedSupported() {
        List<String> names = new ArrayList<>();
        for (MetadataTableType type : MetadataTableType.values()) {
            if (type != MetadataTableType.POSITION_DELETES) {
                names.add(type.name().toLowerCase(Locale.ROOT));
            }
        }
        return names;
    }

    // ---------------------------------------------------------------------
    // listSupportedSysTables
    // ---------------------------------------------------------------------

    @Test
    public void listSupportedSysTablesMirrorsMetadataTableTypesMinusPositionDeletes() {
        List<String> result = metadataWith(new RecordingIcebergCatalogOps())
                .listSupportedSysTables(null, baseHandle());

        // WHY: the set of selectable "$sys" tables a user sees per iceberg table IS exactly
        // MetadataTableType.values() minus POSITION_DELETES, lower-cased (legacy
        // IcebergSysTable.SUPPORTED_SYS_TABLES is built from that same formula). If this drifted, users
        // could no longer reference e.g. mytable$snapshots. MUTATION: returning Collections.emptyList()
        // (the SPI default) -> red; dropping the position_deletes filter -> expected (filtered) != actual
        // -> red.
        Assertions.assertEquals(expectedSupported(), result,
                "must mirror MetadataTableType.values() minus POSITION_DELETES (lower-cased), in order");
        Assertions.assertFalse(result.isEmpty(), "supported sys tables must be non-empty");
        // A representative spread of the metadata tables a user actually queries.
        Assertions.assertTrue(result.containsAll(java.util.Arrays.asList(
                        "snapshots", "history", "files", "manifests", "partitions", "refs", "entries")),
                "the supported list must include the common iceberg metadata tables");
    }

    @Test
    public void listSupportedSysTablesExcludesPositionDeletes() {
        List<String> result = metadataWith(new RecordingIcebergCatalogOps())
                .listSupportedSysTables(null, baseHandle());

        // WHY: Q2 (user-signed) — position_deletes is deliberately NOT exposed, so querying
        // t$position_deletes degrades to the generic fe-core not-found path (keeping the connector pure
        // and fe-core unchanged), rather than legacy's bespoke "not supported yet" message. MUTATION:
        // including POSITION_DELETES in the list -> red.
        Assertions.assertFalse(result.contains("position_deletes"),
                "position_deletes must NOT be advertised as a supported sys table (Q2)");
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

        // WHY: the support check is case-insensitive (mirroring MetadataTableType.from / legacy
        // equalsIgnoreCase), but the STORED canonical name must be lower-cased so t$SNAPSHOTS and
        // t$snapshots are the SAME handle (identical equals/hashCode/toString and the same metadata-table
        // build later). MUTATION: storing sysName verbatim -> getSysTableName() == "SNAPSHOTS" and
        // toString ends "$SNAPSHOTS" -> red.
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
    public void getSysTableHandleEmptyForPositionDeletes() {
        Optional<ConnectorTableHandle> opt = metadataWith(new RecordingIcebergCatalogOps())
                .getSysTableHandle(null, baseHandle(), "position_deletes");

        // WHY: Q2 — position_deletes is not exposed, so resolving it must be Optional.empty() (fe-core
        // then renders the generic not-found). MUTATION: letting position_deletes through the
        // isSupportedSysTable guard -> a present handle -> red.
        Assertions.assertFalse(opt.isPresent(),
                "position_deletes must not resolve to a sys handle (Q2)");
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
}
