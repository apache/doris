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

package org.apache.doris.connector.paimon;

import org.apache.doris.connector.api.ConnectorCapability;
import org.apache.doris.connector.api.mvcc.ConnectorMvccSnapshot;
import org.apache.doris.connector.spi.ConnectorContext;

import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.OptionalLong;
import java.util.Set;

/**
 * Tests for the paimon E5 MVCC / time-travel SPI methods (P5-T20):
 * {@code beginQuerySnapshot}, {@code getSnapshotById}, {@code getSnapshotAt}, plus the
 * {@code PaimonConnector.getCapabilities()} declaration.
 *
 * <p>These drive a {@link RecordingPaimonCatalogOps} fake whose three MVCC seam methods
 * ({@code latestSnapshotId}, {@code snapshotIdAtOrBefore}, {@code snapshotExists}) return plain
 * {@code long}s / {@code boolean}s, so the metadata layer's LOGIC (sys-guard, empty-table->-1,
 * found/empty mapping) is exercised entirely offline — no real paimon {@code Snapshot} /
 * {@code SnapshotManager} is faked (those are impractical to construct without a live table).
 */
public class PaimonConnectorMetadataMvccTest {

    private static PaimonConnectorMetadata metadataWith(RecordingPaimonCatalogOps ops) {
        return new PaimonConnectorMetadata(ops, Collections.emptyMap(), new RecordingConnectorContext());
    }

    private static RowType rowType(String... columnNames) {
        RowType.Builder builder = RowType.builder();
        for (String name : columnNames) {
            builder.field(name, DataTypes.INT());
        }
        return builder.build();
    }

    /** A normal (non-system) handle with its transient Table already set (no reload needed). */
    private static PaimonTableHandle normalHandle(RecordingPaimonCatalogOps ops) {
        PaimonTableHandle handle = new PaimonTableHandle(
                "db1", "t1", Collections.emptyList(), Collections.emptyList());
        FakePaimonTable table = new FakePaimonTable(
                "t1", rowType("id"), Collections.emptyList(), Collections.emptyList());
        ops.table = table;
        handle.setPaimonTable(table);
        return handle;
    }

    /** A system handle (db1.t1$snapshots) with its transient sys Table set. */
    private static PaimonTableHandle sysHandle(RecordingPaimonCatalogOps ops) {
        PaimonTableHandle handle = PaimonTableHandle.forSystemTable("db1", "t1", "snapshots", false);
        FakePaimonTable sys = new FakePaimonTable(
                "t1$snapshots", rowType("snapshot_id"), Collections.emptyList(), Collections.emptyList());
        ops.sysTable = sys;
        handle.setPaimonTable(sys);
        return handle;
    }

    // ==================== beginQuerySnapshot ====================

    @Test
    public void beginQuerySnapshotReturnsLatestSnapshotId() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        PaimonTableHandle handle = normalHandle(ops);
        ops.latestSnapshotId = OptionalLong.of(42L);

        ConnectorMvccSnapshot snap = metadataWith(ops).beginQuerySnapshot(null, handle).get();

        // WHY: the query-begin MVCC pin must be the table's LATEST snapshot id, so all reads in the
        // query see one consistent version (legacy PaimonExternalTable used latestSnapshot().id()).
        // MUTATION: returning a constant / the wrong seam value -> id != 42 -> red.
        Assertions.assertEquals(42L, snap.getSnapshotId(),
                "the begin-query pin must carry the table's latest snapshot id");
        Assertions.assertSame(handle.getPaimonTable(), ops.lastMvccTable,
                "the live resolved Table must be passed to the latest-snapshot seam");
    }

    @Test
    public void beginQuerySnapshotEmptyTableReturnsInvalidSnapshotId() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        PaimonTableHandle handle = normalHandle(ops);
        ops.latestSnapshotId = OptionalLong.empty(); // empty table: no snapshot yet

        ConnectorMvccSnapshot snap = metadataWith(ops).beginQuerySnapshot(null, handle).get();

        // WHY: an empty paimon table (no snapshot) must STILL pin via a snapshot whose id is the
        // legacy INVALID_SNAPSHOT_ID (-1) — NOT Optional.empty(). Optional.empty() would mean "this
        // connector does not support MVCC", but paimon DOES; downstream the -1 sentinel signals
        // "read whatever is current / empty" while keeping the MvccTable wiring engaged. This mirrors
        // legacy PaimonExternalTable, which seeded latestSnapshotId = PaimonSnapshot.INVALID_SNAPSHOT_ID
        // (-1) and only overwrote it when latestSnapshot().isPresent().
        // MUTATION 1: returning Optional.empty() for an empty table -> .get() throws / assertTrue
        // below red. MUTATION 2: defaulting to 0L (or any value != -1) instead of -1 -> id != -1 red.
        Assertions.assertEquals(-1L, snap.getSnapshotId(),
                "an empty table must pin with the legacy INVALID_SNAPSHOT_ID (-1), not Optional.empty");
    }

    @Test
    public void beginQuerySnapshotSysHandleReturnsEmpty() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        PaimonTableHandle handle = sysHandle(ops);
        // Even with a non-empty latest snapshot configured, a sys handle must short-circuit to empty.
        ops.latestSnapshotId = OptionalLong.of(7L);

        Assertions.assertFalse(metadataWith(ops).beginQuerySnapshot(null, handle).isPresent(),
                "a system table must NOT expose an MVCC begin-query pin");
        // WHY: system tables (e.g. t$snapshots) are synthetic metadata views and MUST NOT participate
        // in MVCC / time-travel — pinning them to a data snapshot is meaningless and mirrors the T19
        // scan-node fail-loud guard that rejects time-travel on sys tables. MUTATION: dropping the
        // isSystemTable() guard -> the seam runs and a non-empty snapshot (id 7) is returned -> the
        // assertFalse above + the no-seam-call assertion below go red.
        Assertions.assertTrue(ops.log.isEmpty(),
                "a sys handle must short-circuit before touching the MVCC seam");
    }

    // ==================== getSnapshotById ====================

    @Test
    public void getSnapshotByIdExistsReturnsSnapshot() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        PaimonTableHandle handle = normalHandle(ops);
        ops.snapshotExists = true;

        ConnectorMvccSnapshot snap = metadataWith(ops).getSnapshotById(null, handle, 99L).get();

        // WHY: time-travel-by-id must echo the requested id when that snapshot exists. MUTATION:
        // returning empty even when the snapshot exists -> .get() throws -> red.
        Assertions.assertEquals(99L, snap.getSnapshotId(),
                "an existing snapshot id must be pinned verbatim");
    }

    @Test
    public void getSnapshotByIdNotExistsReturnsEmpty() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        PaimonTableHandle handle = normalHandle(ops);
        ops.snapshotExists = false; // SDK FileNotFoundException -> seam reports false

        // WHY: the SPI contract is "or empty if none" (ConnectorMetadata.getSnapshotById Javadoc), so
        // a missing id must degrade to Optional.empty(), NOT throw. (Legacy THREW a UserException;
        // surfacing the user-facing "not found" message is now the B5 fe-core consumer's job — a
        // documented, intentional contract difference.) MUTATION: throwing / returning a non-empty
        // snapshot for a missing id -> isPresent() true -> red.
        Assertions.assertFalse(metadataWith(ops).getSnapshotById(null, handle, 99L).isPresent(),
                "a missing snapshot id must yield Optional.empty (SPI empty-if-none)");
    }

    @Test
    public void getSnapshotByIdSysHandleReturnsEmpty() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        PaimonTableHandle handle = sysHandle(ops);
        ops.snapshotExists = true; // would yield a snapshot if the guard were missing

        Assertions.assertFalse(metadataWith(ops).getSnapshotById(null, handle, 5L).isPresent(),
                "a system table must NOT expose time-travel-by-id");
        // MUTATION: dropping the isSystemTable() guard -> snapshotExists=true yields a snapshot ->
        // red; the empty log also proves the guard short-circuits before the seam.
        Assertions.assertTrue(ops.log.isEmpty(),
                "a sys handle must short-circuit before touching the MVCC seam");
    }

    // ==================== getSnapshotAt ====================

    @Test
    public void getSnapshotAtFoundReturnsSnapshotId() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        PaimonTableHandle handle = normalHandle(ops);
        ops.snapshotIdAtOrBefore = OptionalLong.of(17L);

        ConnectorMvccSnapshot snap = metadataWith(ops).getSnapshotAt(null, handle, 1_000L).get();

        // WHY: time-travel-by-timestamp must pin the id of the latest snapshot at-or-before the
        // wall-clock time (legacy earlierOrEqualTimeMills). MUTATION: returning the timestamp instead
        // of the resolved snapshot id, or empty -> id != 17 / .get() throws -> red.
        Assertions.assertEquals(17L, snap.getSnapshotId(),
                "the timestamp must resolve to the at-or-before snapshot's id");
    }

    @Test
    public void getSnapshotAtNoneReturnsEmpty() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        PaimonTableHandle handle = normalHandle(ops);
        ops.snapshotIdAtOrBefore = OptionalLong.empty(); // no snapshot <= ts (SDK returned null)

        // WHY: when no snapshot is at-or-before the timestamp the SPI contract is empty-if-none (same
        // documented difference vs legacy, which threw with the earliest-snapshot hint). MUTATION:
        // throwing / returning a snapshot for the no-match case -> isPresent() true -> red.
        Assertions.assertFalse(metadataWith(ops).getSnapshotAt(null, handle, 1_000L).isPresent(),
                "no snapshot at-or-before the timestamp must yield Optional.empty");
    }

    @Test
    public void getSnapshotAtSysHandleReturnsEmpty() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        PaimonTableHandle handle = sysHandle(ops);
        ops.snapshotIdAtOrBefore = OptionalLong.of(3L);

        Assertions.assertFalse(metadataWith(ops).getSnapshotAt(null, handle, 1_000L).isPresent(),
                "a system table must NOT expose time-travel-by-timestamp");
        Assertions.assertTrue(ops.log.isEmpty(),
                "a sys handle must short-circuit before touching the MVCC seam");
    }

    // ==================== capabilities ====================

    @Test
    public void connectorDeclaresMvccAndTimeTravelCapabilities() {
        // PaimonConnector is unit-constructable: getCapabilities() does NOT touch the catalog (the
        // catalog is created lazily on first getMetadata/getScanPlanProvider call), so a null-config
        // connector with a recording context suffices.
        ConnectorContext ctx = new RecordingConnectorContext();
        Set<ConnectorCapability> caps = new PaimonConnector(Collections.emptyMap(), ctx).getCapabilities();

        // WHY: B5's fe-core MvccTable wiring keys off these capabilities to decide whether paimon
        // tables expose MVCC pinning and FOR TIME TRAVEL / FOR VERSION AS OF. If they were absent
        // (the inherited Connector default = emptySet), the E5 methods above would never be called.
        // MUTATION: leaving getCapabilities() unoverridden (empty set) -> both assertions red.
        Assertions.assertTrue(caps.contains(ConnectorCapability.SUPPORTS_MVCC_SNAPSHOT),
                "paimon must declare SUPPORTS_MVCC_SNAPSHOT");
        Assertions.assertTrue(caps.contains(ConnectorCapability.SUPPORTS_TIME_TRAVEL),
                "paimon must declare SUPPORTS_TIME_TRAVEL");
    }
}
