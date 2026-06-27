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
import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorTableSchema;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.mvcc.ConnectorMvccSnapshot;
import org.apache.doris.connector.api.mvcc.ConnectorTimeTravelSpec;
import org.apache.doris.connector.spi.ConnectorContext;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.DateTimeUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TimeZone;

/**
 * Tests for the paimon E5 MVCC / time-travel SPI methods:
 * {@code beginQuerySnapshot}, {@code resolveTimeTravel} (SNAPSHOT_ID / TIMESTAMP / TAG, B5b-2a;
 * INCREMENTAL/@incr, B5b-2b; BRANCH, B5b-2c), {@code getTableSchema(snapshot)} (schema-at-snapshot,
 * including branch-aware), {@code applySnapshot} (including the branch sentinel routing), plus the
 * {@code PaimonConnector.getCapabilities()} declaration. The @incr window VALIDATION rules themselves
 * live in {@link PaimonIncrementalScanParamsTest}.
 *
 * <p>These drive a {@link RecordingPaimonCatalogOps} fake whose MVCC seam methods return plain
 * {@code long}s / small structs (never a real {@code Snapshot}/{@code Tag}/{@code TableSchema}), so
 * the metadata layer's LOGIC (sys-guard, empty->-1, found/empty mapping, schemaId stamping,
 * tag-name pinning, TZ-aware timestamp parse) is exercised entirely offline.
 */
public class PaimonConnectorMetadataMvccTest {

    private static PaimonConnectorMetadata metadataWith(RecordingPaimonCatalogOps ops) {
        return new PaimonConnectorMetadata(ops, Collections.emptyMap(), new RecordingConnectorContext());
    }

    // Builds a metadata sharing an EXTERNAL PaimonSchemaAtMemo, modelling two queries (each a fresh
    // getMetadata() with its own catalog-ops) that share the connector-owned schema-at-snapshot memo.
    private static PaimonConnectorMetadata metadataWith(RecordingPaimonCatalogOps ops,
            PaimonSchemaAtMemo memo) {
        return new PaimonConnectorMetadata(
                ops, Collections.emptyMap(), new RecordingConnectorContext(), memo);
    }

    private static RowType rowType(String... columnNames) {
        RowType.Builder builder = RowType.builder();
        for (String name : columnNames) {
            builder.field(name, DataTypes.INT());
        }
        return builder.build();
    }

    /** Minimal {@link ConnectorSession} that only carries a time zone id (for the TIMESTAMP parse). */
    private static final class TzSession implements ConnectorSession {
        private final String timeZone;

        TzSession(String timeZone) {
            this.timeZone = timeZone;
        }

        @Override
        public String getQueryId() {
            return "q";
        }

        @Override
        public String getUser() {
            return "u";
        }

        @Override
        public String getTimeZone() {
            return timeZone;
        }

        @Override
        public String getLocale() {
            return "en_US";
        }

        @Override
        public long getCatalogId() {
            return 0;
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

    // ==================== resolveTimeTravel: SNAPSHOT_ID ====================

    @Test
    public void resolveSnapshotIdExistsPinsIdSchemaAndScanOption() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        PaimonTableHandle handle = normalHandle(ops);
        ops.snapshotExists = true;
        ops.snapshotSchemaId = OptionalLong.of(3L);

        ConnectorMvccSnapshot snap = metadataWith(ops)
                .resolveTimeTravel(null, handle, ConnectorTimeTravelSpec.snapshotId("99")).get();

        // WHY: FOR VERSION AS OF <id> must pin the parsed id, stamp the snapshot's schemaId (so
        // schema-at-snapshot reads pick the historical schema), and emit the scan.snapshot-id option
        // the scan path copies onto the table. MUTATION: dropping the schemaId stamp -> -1 != 3 red;
        // wrong/missing scan option -> red; not parsing the string id -> red.
        Assertions.assertEquals(99L, snap.getSnapshotId(), "the parsed snapshot id must be pinned");
        Assertions.assertEquals(3L, snap.getSchemaId(),
                "the snapshot's schemaId must be stamped for schema-at-snapshot");
        Assertions.assertEquals("99", snap.getProperties().get("scan.snapshot-id"),
                "scan.snapshot-id must be pinned to the resolved id");
    }

    @Test
    public void resolveSnapshotIdNotExistsReturnsEmpty() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        PaimonTableHandle handle = normalHandle(ops);
        ops.snapshotExists = false; // SDK FileNotFoundException -> seam reports false

        // WHY: a missing id must degrade to Optional.empty (SPI empty-if-none); the B5b-3 fe-core
        // consumer translates empty into the legacy "can't find snapshot by id" UserException.
        // MUTATION: returning a non-empty snapshot for a missing id -> isPresent() true -> red.
        Assertions.assertFalse(metadataWith(ops)
                        .resolveTimeTravel(null, handle, ConnectorTimeTravelSpec.snapshotId("99")).isPresent(),
                "a missing snapshot id must yield Optional.empty");
    }

    @Test
    public void resolveSysHandleNeverTimeTravels() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        PaimonTableHandle handle = sysHandle(ops);
        ops.snapshotExists = true; // would yield a snapshot if the guard were missing

        Assertions.assertFalse(metadataWith(ops)
                        .resolveTimeTravel(null, handle, ConnectorTimeTravelSpec.snapshotId("5")).isPresent(),
                "a system table must NOT expose time-travel");
        // WHY/MUTATION: dropping the isSystemTable() guard -> the seam runs -> non-empty -> red; the
        // empty log also proves the guard short-circuits before touching the seam.
        Assertions.assertTrue(ops.log.isEmpty(),
                "a sys handle must short-circuit before touching the MVCC seam");
    }

    // ==================== resolveTimeTravel: TIMESTAMP ====================

    @Test
    public void resolveTimestampDigitalParsesMillisVerbatim() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        PaimonTableHandle handle = normalHandle(ops);
        ops.snapshotIdAtOrBefore = OptionalLong.of(17L);
        ops.snapshotSchemaId = OptionalLong.of(2L);

        ConnectorMvccSnapshot snap = metadataWith(ops)
                .resolveTimeTravel(null, handle, ConnectorTimeTravelSpec.timestamp("1700000000000", true))
                .get();

        // WHY: a DIGITAL timestamp is epoch-millis (legacy Long.parseLong), fed straight to the
        // at-or-before lookup. MUTATION: re-parsing the digits as a datetime string / not feeding the
        // verbatim millis -> the captured arg != 1700000000000 -> red.
        Assertions.assertEquals(1_700_000_000_000L, ops.snapshotIdAtOrBeforeArg,
                "a digital timestamp must be fed to the at-or-before lookup as raw epoch-millis");
        Assertions.assertEquals(17L, snap.getSnapshotId(),
                "the at-or-before snapshot id must be pinned");
        Assertions.assertEquals(2L, snap.getSchemaId(), "the snapshot's schemaId must be stamped");
        Assertions.assertEquals("17", snap.getProperties().get("scan.snapshot-id"),
                "scan.snapshot-id must be pinned to the resolved id");
    }

    @Test
    public void resolveTimestampStringParsedWithSessionTimeZone() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        PaimonTableHandle handle = normalHandle(ops);
        ops.snapshotIdAtOrBefore = OptionalLong.of(8L);

        String literal = "2023-11-15 00:00:00";
        // Expected millis = exactly what paimon's DateTimeUtils computes for THIS literal in THIS zone
        // (the byte-parity reference: legacy parsed the same way with TimeUtils.getTimeZone()).
        long expectedShanghai = DateTimeUtils.parseTimestampData(literal, 3,
                TimeZone.getTimeZone("Asia/Shanghai")).getMillisecond();
        long expectedUtc = DateTimeUtils.parseTimestampData(literal, 3,
                TimeZone.getTimeZone("UTC")).getMillisecond();
        // Guard the test itself: the two zones must differ, else the assertion below proves nothing.
        Assertions.assertNotEquals(expectedShanghai, expectedUtc,
                "test precondition: the literal must resolve to different millis in the two zones");

        metadataWith(ops).resolveTimeTravel(new TzSession("Asia/Shanghai"), handle,
                ConnectorTimeTravelSpec.timestamp(literal, false));

        // WHY: a non-digital timestamp must be parsed with the SESSION time zone (byte-parity with
        // legacy PaimonUtil.getPaimonSnapshotByTimestamp's TimeUtils.getTimeZone()), NOT a fixed UTC.
        // MUTATION: parsing with a hardcoded UTC (or the JVM default) -> the captured millis equals
        // expectedUtc, not expectedShanghai -> red.
        Assertions.assertEquals(expectedShanghai, ops.snapshotIdAtOrBeforeArg,
                "a string timestamp must be parsed with the session time zone, not UTC");
    }

    @Test
    public void resolveTimestampStringWithGenuinelyUnknownZoneFailsLoud() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        PaimonTableHandle handle = normalHandle(ops);

        // WHY (FIX-TZ-ALIAS, no-silent-degrade invariant): after the fix the connector replicates the
        // legacy alias map (SHORT_IDS + 4 Doris overrides), so CST/PST/EST now RESOLVE (see the two
        // tests below). But an id absent from BOTH ZoneId.of's native set AND the alias map (e.g.
        // "XYZ") must still FAIL LOUD — never silently degrade to a wrong zone (a wrong zone resolves
        // the WRONG snapshot -> silently wrong rows). The fix only NARROWS the failure set to
        // genuinely-unknown ids; it must not become a silent UTC fallback.
        // MUTATION: catching and degrading to UTC -> assertThrows finds no exception -> red;
        // a raw DateTimeException leaking (no DorisConnectorException wrap) -> wrong type -> red.
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> metadataWith(ops).resolveTimeTravel(new TzSession("XYZ"), handle,
                        ConnectorTimeTravelSpec.timestamp("2023-01-01 00:00:00", false)),
                "a genuinely-unknown zone id must fail loud, not crash with a raw "
                        + "DateTimeException nor silently degrade to a wrong zone");
        Assertions.assertTrue(ex.getMessage().contains("XYZ"),
                "the error must name the offending session zone id ('XYZ')");
        Assertions.assertTrue(ex.getMessage().contains("standard")
                        && ex.getMessage().contains("zone id"),
                "the error must give actionable guidance (use a standard zone id)");
    }

    @Test
    public void resolveTimestampStringResolvesCstAliasToShanghai() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        PaimonTableHandle handle = normalHandle(ops);
        ops.snapshotIdAtOrBefore = OptionalLong.of(8L);

        String literal = "2023-11-15 00:00:00";
        long expectedShanghai = DateTimeUtils.parseTimestampData(literal, 3,
                TimeZone.getTimeZone("Asia/Shanghai")).getMillisecond();
        long expectedUtc = DateTimeUtils.parseTimestampData(literal, 3,
                TimeZone.getTimeZone("UTC")).getMillisecond();
        Assertions.assertNotEquals(expectedShanghai, expectedUtc,
                "test precondition: the literal must resolve to different millis in CST vs UTC");

        metadataWith(ops).resolveTimeTravel(new TzSession("CST"), handle,
                ConnectorTimeTravelSpec.timestamp(literal, false));

        // WHY (FIX-TZ-ALIAS): CST is Doris's DEFAULT region alias for Asia/Shanghai; legacy resolved
        // it via the alias map (the 4 overrides put CST -> Asia/Shanghai). Pinning the *Shanghai*
        // millis (not UTC, not a throw) is the byte-parity intent. Before the fix the alias-less
        // ZoneId.of threw -> FOR TIME AS OF a datetime string was broken under the DEFAULT time zone.
        // MUTATION: alias-less ZoneId.of -> throws (red); a wrong override (CST->UTC) -> captures
        // expectedUtc (red).
        Assertions.assertEquals(expectedShanghai, ops.snapshotIdAtOrBeforeArg,
                "CST must resolve to Asia/Shanghai (Doris default alias), matching legacy");
    }

    @Test
    public void resolveTimestampStringResolvesPstAndEstViaShortIds() {
        String literal = "2023-11-15 00:00:00";

        // PST resolves through ZoneId.SHORT_IDS -> America/Los_Angeles (NOT one of the 4 explicit
        // Doris overrides). This is the report-suggestion correction: a fix that inlined only the 4
        // entries would leave PST/EST THROWING. The full SHORT_IDS map is required.
        RecordingPaimonCatalogOps pstOps = new RecordingPaimonCatalogOps();
        pstOps.snapshotIdAtOrBefore = OptionalLong.of(3L);
        long expectedPst = DateTimeUtils.parseTimestampData(literal, 3,
                TimeZone.getTimeZone("America/Los_Angeles")).getMillisecond();
        metadataWith(pstOps).resolveTimeTravel(new TzSession("PST"), normalHandle(pstOps),
                ConnectorTimeTravelSpec.timestamp(literal, false));
        // WHY: PST must resolve via SHORT_IDS to America/Los_Angeles. MUTATION: dropping
        // putAll(ZoneId.SHORT_IDS) -> PST throws -> red.
        Assertions.assertEquals(expectedPst, pstOps.snapshotIdAtOrBeforeArg,
                "PST must resolve via ZoneId.SHORT_IDS to America/Los_Angeles, matching legacy");

        // EST resolves through ZoneId.SHORT_IDS -> the fixed offset "-05:00".
        RecordingPaimonCatalogOps estOps = new RecordingPaimonCatalogOps();
        estOps.snapshotIdAtOrBefore = OptionalLong.of(4L);
        long expectedEst = DateTimeUtils.parseTimestampData(literal, 3,
                TimeZone.getTimeZone(java.time.ZoneId.of("-05:00"))).getMillisecond();
        metadataWith(estOps).resolveTimeTravel(new TzSession("EST"), normalHandle(estOps),
                ConnectorTimeTravelSpec.timestamp(literal, false));
        // WHY: EST must resolve via SHORT_IDS to the -05:00 offset. MUTATION: dropping
        // putAll(ZoneId.SHORT_IDS) -> EST throws -> red.
        Assertions.assertEquals(expectedEst, estOps.snapshotIdAtOrBeforeArg,
                "EST must resolve via ZoneId.SHORT_IDS to the -05:00 offset, matching legacy");
    }

    @Test
    public void resolveTimestampDigitalUnaffectedByUnsupportedZoneAlias() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        PaimonTableHandle handle = normalHandle(ops);
        ops.snapshotIdAtOrBefore = OptionalLong.of(11L);

        // WHY: the zone-id catch must be scoped to the STRING path only. A DIGITAL timestamp is raw
        // epoch-millis and never touches ZoneId.of, so it must succeed even under a session whose
        // zone id is GENUINELY unknown (would reject a datetime string). NOTE: this uses "XYZ" (a
        // truly-unknown id) deliberately — after FIX-TZ-ALIAS "CST" now resolves, so a CST session
        // would no longer prove the bypass (it would parse fine for strings too). "XYZ" still throws
        // on the string path, so the test keeps its discriminating power. MUTATION: dropping the
        // spec.isDigital() short-circuit -> the digital value goes to parseTimestampData with zone
        // "XYZ" -> throws -> red.
        ConnectorMvccSnapshot snap = metadataWith(ops)
                .resolveTimeTravel(new TzSession("XYZ"), handle,
                        ConnectorTimeTravelSpec.timestamp("1700000000000", true))
                .get();

        Assertions.assertEquals(1_700_000_000_000L, ops.snapshotIdAtOrBeforeArg,
                "a digital timestamp must be fed verbatim even under an unknown zone id");
        Assertions.assertEquals(11L, snap.getSnapshotId(),
                "the digital timestamp path must resolve normally under an unknown-zone session (no zone needed)");
    }

    @Test
    public void resolveTimestampNoneAtOrBeforeReturnsEmpty() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        PaimonTableHandle handle = normalHandle(ops);
        ops.snapshotIdAtOrBefore = OptionalLong.empty(); // no snapshot <= ts (SDK returned null)

        // WHY: no snapshot at-or-before the timestamp must degrade to Optional.empty (empty-if-none;
        // the fe-core consumer surfaces the legacy earliest-snapshot-hint error). MUTATION: returning
        // a snapshot for the no-match case -> isPresent() true -> red.
        Assertions.assertFalse(metadataWith(ops).resolveTimeTravel(null, handle,
                        ConnectorTimeTravelSpec.timestamp("1700000000000", true)).isPresent(),
                "no snapshot at-or-before the timestamp must yield Optional.empty");
    }

    // ==================== resolveTimeTravel: TAG ====================

    @Test
    public void resolveTagFoundPinsTagNameNotId() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        PaimonTableHandle handle = normalHandle(ops);
        ops.tagSnapshot = new PaimonCatalogOps.TagSnapshot(42L, 4L);

        ConnectorMvccSnapshot snap = metadataWith(ops)
                .resolveTimeTravel(null, handle, ConnectorTimeTravelSpec.tag("release-1")).get();

        // WHY: tag time-travel must pin the tag's snapshot id + schema id, but the SCAN OPTION must be
        // scan.tag-name = the NAME (legacy PaimonExternalTable.java:137 pins the name, not the id, so a
        // later move of the tag is honored). MUTATION: pinning scan.snapshot-id (the id) instead of
        // scan.tag-name -> the tag-name assertion red; not stamping schemaId -> -1 != 4 red.
        Assertions.assertEquals(42L, snap.getSnapshotId(), "the tag's snapshot id must be pinned");
        Assertions.assertEquals(4L, snap.getSchemaId(), "the tag's schemaId must be stamped");
        Assertions.assertEquals("release-1", snap.getProperties().get("scan.tag-name"),
                "tag time-travel must pin scan.tag-name to the tag NAME (not the snapshot id)");
        Assertions.assertNull(snap.getProperties().get("scan.snapshot-id"),
                "tag time-travel must NOT pin scan.snapshot-id");
    }

    @Test
    public void resolveTagNotFoundReturnsEmpty() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        PaimonTableHandle handle = normalHandle(ops);
        ops.tagSnapshot = null; // no such tag

        // WHY: a missing tag must degrade to Optional.empty (legacy threw "can't find snapshot by
        // tag"; the fe-core consumer now surfaces that). MUTATION: returning a snapshot for an absent
        // tag -> isPresent() true -> red.
        Assertions.assertFalse(metadataWith(ops)
                        .resolveTimeTravel(null, handle, ConnectorTimeTravelSpec.tag("missing")).isPresent(),
                "a missing tag must yield Optional.empty");
    }

    // ==================== resolveTimeTravel: BRANCH ====================

    @Test
    public void resolveBranchFoundLoadsBranchTableAndPinsItsLatestSnapshot() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        PaimonTableHandle handle = normalHandle(ops); // base table is ops.table
        ops.branchExists = true;
        // The branch is a DIFFERENT table double than the base, with its own latest snapshot/schema.
        FakePaimonTable branch = new FakePaimonTable(
                "t1", rowType("id", "dt"), Collections.emptyList(), Collections.emptyList());
        ops.branchTable = branch;
        ops.latestSnapshotId = OptionalLong.of(7L);
        ops.snapshotSchemaId = OptionalLong.of(3L);

        ConnectorMvccSnapshot snap = metadataWith(ops)
                .resolveTimeTravel(null, handle, ConnectorTimeTravelSpec.branch("b1")).get();

        // WHY: @branch must pin the BRANCH's LATEST snapshot + its schemaId, carry the branch identity
        // via the CoreOptions.BRANCH sentinel (NOT scan.snapshot-id — the branch reads its own latest),
        // and validate the branch on the BASE table. Branches have no in-branch time-travel (legacy
        // reads the branch's latestSnapshot() only). MUTATION: pinning scan.snapshot-id -> the no-key
        // assertion red; validating against the branch table -> the BASE assertion red; loading the
        // base table instead of the branch -> the lastMvccTable assertion red.
        Assertions.assertEquals(7L, snap.getSnapshotId(),
                "@branch must pin the BRANCH's latest snapshot id");
        Assertions.assertEquals(3L, snap.getSchemaId(),
                "@branch must stamp the BRANCH's latest snapshot schemaId");
        Assertions.assertEquals("b1", snap.getProperties().get(CoreOptions.BRANCH.key()),
                "@branch must carry the branch name under the CoreOptions.BRANCH sentinel key");
        Assertions.assertNull(snap.getProperties().get("scan.snapshot-id"),
                "@branch must NOT pin scan.snapshot-id (the branch natively reads its own latest)");
        // The sentinel key must be the SDK key, not a drifting hardcoded string.
        Assertions.assertEquals("branch", CoreOptions.BRANCH.key(),
                "precondition: the CoreOptions.BRANCH key is 'branch'");

        // The branch was loaded via a 3-arg branch Identifier (real branch name, no system-table name).
        Assertions.assertEquals("b1", ops.lastGetTableId.getBranchName(),
                "the branch table must be loaded via a 3-arg branch Identifier");
        Assertions.assertNull(ops.lastGetTableId.getSystemTableName(),
                "a branch load must NOT carry a system-table name");
        // The latest-snapshot / schemaId lookups ran against the BRANCH table, not the base. (The last
        // seam call before this assertion is snapshotSchemaId, which captured lastMvccTable.)
        Assertions.assertSame(branch, ops.lastMvccTable,
                "latestSnapshotId/snapshotSchemaId must run against the BRANCH table");
        // branchExists validation ran against the BASE table (legacy resolvePaimonBranch).
        Assertions.assertEquals("b1", ops.lastBranchExistsArg,
                "branchExists must be asked about the requested branch name");
        // ...and validation ran against the BASE table (ops.table), NOT the freshly loaded branch.
        // MUTATION: reordering so branch validation runs after the branch load (or against the branch
        // table) -> lastBranchExistsTable would be the branch table -> both assertions red.
        Assertions.assertSame(ops.table, ops.lastBranchExistsTable,
                "branchExists must validate against the BASE table (legacy resolvePaimonBranch)");
        Assertions.assertNotSame(ops.branchTable, ops.lastBranchExistsTable,
                "branchExists must NOT validate against the freshly loaded branch table");
    }

    @Test
    public void resolveBranchNotFoundReturnsEmptyAndNeverLoadsBranch() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        PaimonTableHandle handle = normalHandle(ops);
        ops.branchExists = false; // branch does not exist on the base table

        // WHY: a missing branch must degrade to Optional.empty (empty-if-none; the B5b-3 fe-core
        // consumer translates empty into the legacy "can't find branch" UserException), consistent
        // with snapshot/tag not-found. The branch table must NEVER be loaded (no latest-snapshot work).
        // MUTATION: dropping the branchExists guard -> a snapshot is returned / the branch is loaded ->
        // both assertions red.
        Assertions.assertFalse(metadataWith(ops)
                        .resolveTimeTravel(null, handle, ConnectorTimeTravelSpec.branch("nope")).isPresent(),
                "a missing branch must yield Optional.empty");
        Assertions.assertFalse(ops.log.contains("latestSnapshotId"),
                "a missing branch must NOT load the branch table / look up its latest snapshot");
    }

    @Test
    public void resolveBranchEmptyBranchPinsInvalidSnapshotIdAndLatestSchema() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        PaimonTableHandle handle = normalHandle(ops);
        ops.branchExists = true;
        ops.branchTable = new FakePaimonTable(
                "t1", rowType("id"), Collections.emptyList(), Collections.emptyList());
        ops.latestSnapshotId = OptionalLong.empty(); // branch has no snapshot yet

        ConnectorMvccSnapshot snap = metadataWith(ops)
                .resolveTimeTravel(null, handle, ConnectorTimeTravelSpec.branch("b1")).get();

        // WHY: an EMPTY branch (no snapshot) must pin snapshotId=-1 and schemaId=-1 (latest-schema
        // fallback) WITHOUT calling snapshotSchemaId(-1), while still carrying the branch sentinel.
        // This mirrors the INCREMENTAL empty-table -1 handling (benign divergence from legacy's 0L —
        // the resulting schema is identical). MUTATION: calling snapshotSchemaId(-1) -> the log carries
        // "snapshotSchemaId:-1" -> red; not stamping -1 -> red; dropping the sentinel -> red.
        Assertions.assertEquals(-1L, snap.getSnapshotId(),
                "an empty branch must pin the INVALID_SNAPSHOT_ID (-1)");
        Assertions.assertEquals(-1L, snap.getSchemaId(),
                "an empty branch must leave schemaId=-1 (latest-schema fallback)");
        Assertions.assertEquals("b1", snap.getProperties().get(CoreOptions.BRANCH.key()),
                "an empty branch must still carry the branch sentinel");
        Assertions.assertFalse(ops.log.contains("snapshotSchemaId:-1"),
                "an empty branch must NOT resolve schemaId at the invalid snapshot id (-1)");
    }

    @Test
    public void getTableSchemaOnEmptyBranchFallsBackToBranchLatestSchema() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        PaimonTableHandle handle = normalHandle(ops); // base table is ops.table (single column "id")
        ops.branchExists = true;
        // The branch table's rowType DIFFERS from the base so the fallback can be proven to resolve
        // the BRANCH table (not the base) when feeding the schemaId=-1 empty-branch snapshot back in.
        FakePaimonTable branch = new FakePaimonTable(
                "t1", rowType("bid", "bdt"), Collections.emptyList(), Collections.emptyList());
        ops.branchTable = branch;
        ops.latestSnapshotId = OptionalLong.empty(); // branch has no snapshot yet

        PaimonConnectorMetadata metadata = metadataWith(ops);
        ConnectorMvccSnapshot snap = metadata
                .resolveTimeTravel(null, handle, ConnectorTimeTravelSpec.branch("b1")).get();
        // The resolved empty-branch snapshot carries schemaId=-1 (the documented benign divergence).
        Assertions.assertEquals(-1L, snap.getSchemaId(),
                "precondition: an empty branch resolves schemaId=-1");

        // Feed that schemaId=-1 snapshot into getTableSchema on the BRANCH handle (the B5b-3 fe-core
        // consumer does exactly this after resolveTimeTravel).
        PaimonTableHandle branchHandle = new PaimonTableHandle(
                "db1", "t1", Collections.emptyList(), Collections.emptyList()).withBranch("b1");
        ConnectorTableSchema schema = metadata.getTableSchema(null, branchHandle, snap);

        // WHY: the documented schemaId=-1 divergence is BENIGN because schemaId<0 routes to the latest
        // fallback, which resolves the BRANCH table (via the 3-arg branch Identifier) and maps the
        // BRANCH's latest rowType — identical to what legacy's branch latestSchema would yield. The
        // -1 is never passed to schemaAt/snapshotSchemaId. MUTATION: calling schemaAt(-1) (or
        // snapshotSchemaId(-1)) instead of the latest fallback -> the log carries "schemaAt:-1" /
        // "snapshotSchemaId:-1" -> red; resolving the base table instead of the branch -> columns are
        // ["id"] not ["bid","bdt"] -> red.
        Assertions.assertEquals(Arrays.asList("bid", "bdt"), columnNames(schema),
                "a schemaId=-1 empty-branch snapshot must fall back to the BRANCH table's latest schema");
        Assertions.assertFalse(ops.log.contains("schemaAt:-1"),
                "a -1 schemaId must NOT call schemaAt");
        Assertions.assertFalse(ops.log.contains("snapshotSchemaId:-1"),
                "a -1 schemaId must NOT resolve schemaId at the invalid snapshot id (-1)");
    }

    @Test
    public void resolveBranchOnSysHandleReturnsEmpty() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        PaimonTableHandle handle = sysHandle(ops);
        ops.branchExists = true; // would resolve if the sys guard were missing

        // WHY: system tables expose no time-travel (same guard as beginQuerySnapshot / the T19 scan
        // fail-loud) — the sys guard must short-circuit BEFORE branchExists is ever consulted.
        // MUTATION: dropping the isSystemTable() guard -> branchExists runs (log non-empty) and a
        // snapshot is returned -> both assertions red.
        Assertions.assertFalse(metadataWith(ops)
                        .resolveTimeTravel(null, handle, ConnectorTimeTravelSpec.branch("b1")).isPresent(),
                "a system table must NOT expose branch time-travel");
        Assertions.assertTrue(ops.log.isEmpty(),
                "a sys handle must short-circuit before touching the branch seam");
    }

    // ==================== branchExists seam: graceful non-FileStoreTable path ====================

    @Test
    public void branchExistsOnNonFileStoreTableIsGracefullyFalse() {
        // WHY: a non-FileStoreTable backend (e.g. jdbc-only) cannot have branches, so the seam must
        // return false gracefully rather than ClassCastException-ing the cast. FakePaimonTable is a
        // plain Table (NOT a FileStoreTable), so this pins the instanceof guard. MUTATION: removing
        // the instanceof guard -> the cast throws ClassCastException -> red.
        PaimonCatalogOps.CatalogBackedPaimonCatalogOps ops =
                new PaimonCatalogOps.CatalogBackedPaimonCatalogOps(null);
        FakePaimonTable notFileStore = new FakePaimonTable(
                "t1", rowType("id"), Collections.emptyList(), Collections.emptyList());

        Assertions.assertFalse(ops.branchExists(notFileStore, "b"),
                "a non-FileStoreTable backend cannot have branches -> graceful false");
    }

    // ==================== resolveTimeTravel: INCREMENTAL (@incr) ====================

    @Test
    public void resolveIncrementalPinsLatestSnapshotAndThreadsIncrementalBetween() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        PaimonTableHandle handle = normalHandle(ops);
        ops.latestSnapshotId = OptionalLong.of(9L);
        ops.snapshotSchemaId = OptionalLong.of(2L);
        Map<String, String> params = new java.util.HashMap<>();
        params.put("startSnapshotId", "1");
        params.put("endSnapshotId", "5");

        ConnectorMvccSnapshot snap = metadataWith(ops)
                .resolveTimeTravel(null, handle, ConnectorTimeTravelSpec.incremental(params)).get();

        // WHY: legacy @incr reads at the LATEST snapshot (getProcessedTable copies the incremental
        // options onto baseTable, which reads latest) and applies incremental-between at scan time, so
        // the pin must carry the latest snapshot id + its schemaId AND the incremental-between option.
        // MUTATION: not pinning latest (e.g. -1) -> id != 9 red; dropping the schemaId stamp -> -1 != 2
        // red; not producing incremental-between -> the property assertion red.
        Assertions.assertEquals(9L, snap.getSnapshotId(),
                "@incr must pin the LATEST snapshot id (legacy reads latest + applies incremental-between)");
        Assertions.assertEquals(2L, snap.getSchemaId(),
                "@incr must stamp the latest snapshot's schemaId");
        Assertions.assertEquals("1,5", snap.getProperties().get("incremental-between"),
                "@incr (both snapshot ids) must produce incremental-between=start,end");
    }

    @Test
    public void resolveIncrementalDoesNotEmitScanSnapshotId() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        PaimonTableHandle handle = normalHandle(ops);
        ops.latestSnapshotId = OptionalLong.of(9L);
        Map<String, String> params = new java.util.HashMap<>();
        params.put("startSnapshotId", "1");
        params.put("endSnapshotId", "5");

        ConnectorMvccSnapshot snap = metadataWith(ops)
                .resolveTimeTravel(null, handle, ConnectorTimeTravelSpec.incremental(params)).get();

        // WHY: @incr pins LATEST but must NOT emit scan.snapshot-id — that would conflict with
        // incremental-between (legacy nulls scan.snapshot-id for @incr, PaimonScanNode line 842/846).
        // applySnapshot threads the NON-EMPTY incremental properties verbatim and skips the
        // scan.snapshot-id fallback precisely because properties is non-empty.
        // MUTATION: also adding a scan.snapshot-id property (like SNAPSHOT_ID/TIMESTAMP do) -> the
        // assertNull below + the applySnapshot end-to-end test go red.
        Assertions.assertNull(snap.getProperties().get("scan.snapshot-id"),
                "@incr must NOT emit scan.snapshot-id (it would conflict with incremental-between)");
        // And the null-reset keys legacy seeded (scan.snapshot-id / scan.mode) must be ABSENT here,
        // NOT present-with-null. WHY (FIX-INCR-SCAN-RESET): the resolved ConnectorMvccSnapshot is the
        // shared, source-agnostic SPI type and is null-free by contract (Builder.property rejects null;
        // getProperties() is "never null"). The legacy null resets ARE required (a base table can
        // persist a stale scan.snapshot-id/scan.mode), but they are reapplied LATER at the Table.copy
        // chokepoint by PaimonIncrementalScanParams.applyResetsIfIncremental — never on this snapshot.
        // MUTATION: re-introducing the null seeds here -> containsKey true (or a build-time NPE) -> red.
        Assertions.assertFalse(snap.getProperties().containsKey("scan.snapshot-id"),
                "the resolved snapshot must stay null-free — the scan.snapshot-id reset is reapplied at copy");
        Assertions.assertFalse(snap.getProperties().containsKey("scan.mode"),
                "the resolved snapshot must stay null-free — the scan.mode reset is reapplied at copy");
    }

    @Test
    public void resolveIncrementalEmptyTableFallsBackToInvalidSnapshotIdAndLatestSchema() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        PaimonTableHandle handle = normalHandle(ops);
        ops.latestSnapshotId = OptionalLong.empty(); // empty table: no snapshot yet
        Map<String, String> params = new java.util.HashMap<>();
        params.put("startTimestamp", "100");

        ConnectorMvccSnapshot snap = metadataWith(ops)
                .resolveTimeTravel(null, handle, ConnectorTimeTravelSpec.incremental(params)).get();

        // WHY: an empty table must NOT crash the @incr resolve — it falls back to the INVALID_SNAPSHOT_ID
        // (-1) and schemaId=-1 (so getTableSchema resolves the LATEST schema, never schemaAt(-1)). The
        // incremental window is still produced (read applies it once data exists).
        // MUTATION: calling snapshotSchemaId(-1) for an empty table -> the log carries "snapshotSchemaId:-1"
        // -> red; not falling back to -1 -> red.
        Assertions.assertEquals(-1L, snap.getSnapshotId(),
                "@incr on an empty table must fall back to the INVALID_SNAPSHOT_ID (-1)");
        Assertions.assertEquals(-1L, snap.getSchemaId(),
                "@incr on an empty table must leave schemaId=-1 (latest-schema fallback)");
        Assertions.assertFalse(ops.log.contains("snapshotSchemaId:-1"),
                "an empty table must NOT resolve schemaId at the invalid snapshot id (-1)");
    }

    @Test
    public void resolveIncrementalEndToEndAppliesIncrementalOptionsIntoHandle() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        PaimonTableHandle handle = normalHandle(ops);
        ops.latestSnapshotId = OptionalLong.of(9L);
        Map<String, String> params = new java.util.HashMap<>();
        params.put("startTimestamp", "100");
        params.put("endTimestamp", "200");

        PaimonConnectorMetadata metadata = metadataWith(ops);
        ConnectorMvccSnapshot snap = metadata
                .resolveTimeTravel(null, handle, ConnectorTimeTravelSpec.incremental(params)).get();
        PaimonTableHandle pinned = (PaimonTableHandle) metadata.applySnapshot(null, handle, snap);

        // WHY: the full @incr path must end with the incremental-between-timestamp option threaded onto
        // the scan handle (NOT scan.snapshot-id), so the scan reads the incremental window. This is the
        // contract the B5b-3 fe-core consumer relies on. MUTATION: applySnapshot falling back to
        // scan.snapshot-id (because it ignored the non-empty properties) -> the timestamp assertion red
        // and the scan.snapshot-id assertion red.
        Assertions.assertEquals("100,200", pinned.getScanOptions().get("incremental-between-timestamp"),
                "applySnapshot must thread the @incr incremental-between-timestamp option into the handle");
        Assertions.assertFalse(pinned.getScanOptions().containsKey("scan.snapshot-id"),
                "an @incr pin must NOT thread scan.snapshot-id (conflicts with the incremental window)");
    }

    // ==================== getTableSchema(snapshot): schema-at-snapshot ====================

    @Test
    public void getTableSchemaAtSchemaIdResolvesHistoricalSchema() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        PaimonTableHandle handle = normalHandle(ops); // latest rowType has only "id"
        // The pinned schema has DIFFERENT columns (id, dt) and a partition key — proving the schema
        // came from schemaAt(schemaId), not the latest table.
        List<DataField> fields = rowType("id", "dt").getFields();
        ops.schemaAt = new PaimonCatalogOps.PaimonSchemaSnapshot(
                fields, Arrays.asList("dt"), Collections.emptyList());
        ConnectorMvccSnapshot snapshot = ConnectorMvccSnapshot.builder()
                .snapshotId(7L).schemaId(2L).build();

        ConnectorTableSchema schema = metadataWith(ops).getTableSchema(null, handle, snapshot);

        // WHY: under schema evolution a time-travel read must see the schema AS OF the pinned
        // schemaId (legacy initSchema(schemaId)), mapping the historical fields + partition keys.
        // MUTATION: ignoring the snapshot and returning the latest 1-column schema -> column count /
        // names / partition_columns all red.
        Assertions.assertEquals(2L, ops.lastSchemaAtArg,
                "the schema must be resolved at the snapshot's schemaId");
        Assertions.assertEquals(Arrays.asList("id", "dt"), columnNames(schema),
                "the at-snapshot schema's columns must be mapped (not the latest single-column schema)");
        Assertions.assertEquals("dt", schema.getProperties().get("partition_columns"),
                "the at-snapshot schema's partition keys must be emitted as partition_columns");
    }

    @Test
    public void getTableSchemaWithNegativeSchemaIdFallsBackToLatest() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        PaimonTableHandle handle = normalHandle(ops); // latest rowType has only "id"
        ConnectorMvccSnapshot snapshot = ConnectorMvccSnapshot.builder()
                .snapshotId(7L).build(); // schemaId defaults to -1

        ConnectorTableSchema schema = metadataWith(ops).getTableSchema(null, handle, snapshot);

        // WHY: schemaId < 0 means "unknown schema version" -> the read must fall back to the latest
        // schema, NOT call schemaAt (which would pass an invalid -1 to the SDK). MUTATION: calling
        // schemaAt(-1) instead of the latest path -> the log carries "schemaAt:-1" -> red.
        Assertions.assertEquals(Collections.singletonList("id"), columnNames(schema),
                "a -1 schemaId must fall back to the latest schema");
        Assertions.assertFalse(ops.log.contains("schemaAt:-1"),
                "a -1 schemaId must NOT call schemaAt");
    }

    private static List<String> columnNames(ConnectorTableSchema schema) {
        List<String> names = new java.util.ArrayList<>();
        for (ConnectorColumn c : schema.getColumns()) {
            names.add(c.getName());
        }
        return names;
    }

    // ============= getTableSchema(snapshot): cross-query schema-at-snapshot memo (FIX-B-MC2) =============

    @Test
    public void getTableSchemaAtSnapshotIsMemoizedAcrossQueries() {
        // Two queries = two fresh PaimonConnectorMetadata (production builds one per query via
        // getMetadata()), each with its OWN catalog-ops, sharing the connector-owned PaimonSchemaAtMemo.
        PaimonSchemaAtMemo memo = new PaimonSchemaAtMemo(10000);
        RecordingPaimonCatalogOps ops1 = new RecordingPaimonCatalogOps();
        RecordingPaimonCatalogOps ops2 = new RecordingPaimonCatalogOps();
        PaimonTableHandle handle = new PaimonTableHandle(
                "db1", "t1", Collections.emptyList(), Collections.emptyList());
        // Transient table set -> resolveTable issues no getTable; only schemaAt reaches the ops seam.
        handle.setPaimonTable(new FakePaimonTable(
                "t1", rowType("id", "dt"), Collections.emptyList(), Collections.emptyList()));
        PaimonCatalogOps.PaimonSchemaSnapshot atSchema = new PaimonCatalogOps.PaimonSchemaSnapshot(
                rowType("id", "dt").getFields(), Arrays.asList("dt"), Collections.emptyList());
        ops1.schemaAt = atSchema;
        ops2.schemaAt = atSchema;
        ConnectorMvccSnapshot snapshot = ConnectorMvccSnapshot.builder().snapshotId(7L).schemaId(2L).build();

        ConnectorTableSchema schema1 = metadataWith(ops1, memo).getTableSchema(null, handle, snapshot);
        ConnectorTableSchema schema2 = metadataWith(ops2, memo).getTableSchema(null, handle, snapshot);

        // WHY: a repeated time-travel to the same (table, schemaId) must hit the connector-level memo and
        // NOT re-read the schema file (restoring the legacy PaimonExternalMetaCache hit dropped by CACHE-P1).
        // MUTATION: drop the memo -> the second query also calls schemaAt -> ops2.log gains "schemaAt:2".
        Assertions.assertEquals(1, Collections.frequency(ops1.log, "schemaAt:2"),
                "the first query must perform the schemaAt read");
        Assertions.assertFalse(ops2.log.contains("schemaAt:2"),
                "the second query at the same schemaId must hit the memo and NOT re-read schemaAt");
        Assertions.assertEquals(columnNames(schema1), columnNames(schema2),
                "both queries must resolve the same at-snapshot schema");
    }

    @Test
    public void getTableSchemaAtSnapshotMemoIsKeyedBySchemaId() {
        PaimonSchemaAtMemo memo = new PaimonSchemaAtMemo(10000);
        RecordingPaimonCatalogOps ops1 = new RecordingPaimonCatalogOps();
        RecordingPaimonCatalogOps ops2 = new RecordingPaimonCatalogOps();
        PaimonTableHandle handle = new PaimonTableHandle(
                "db1", "t1", Collections.emptyList(), Collections.emptyList());
        handle.setPaimonTable(new FakePaimonTable(
                "t1", rowType("id"), Collections.emptyList(), Collections.emptyList()));
        ops1.schemaAt = new PaimonCatalogOps.PaimonSchemaSnapshot(
                rowType("id").getFields(), Collections.emptyList(), Collections.emptyList());
        ops2.schemaAt = new PaimonCatalogOps.PaimonSchemaSnapshot(
                rowType("id", "v2").getFields(), Collections.emptyList(), Collections.emptyList());

        metadataWith(ops1, memo).getTableSchema(null, handle,
                ConnectorMvccSnapshot.builder().snapshotId(7L).schemaId(2L).build());
        metadataWith(ops2, memo).getTableSchema(null, handle,
                ConnectorMvccSnapshot.builder().snapshotId(9L).schemaId(3L).build());

        // WHY: a DIFFERENT schemaId is a different schema version (schema evolution), so it must NOT be
        // served from schemaId=2's entry. MUTATION: drop schemaId from the key -> schemaId=3 hits
        // schemaId=2's entry -> ops2 never reads -> "schemaAt:3" absent -> red.
        Assertions.assertTrue(ops2.log.contains("schemaAt:3"),
                "a different schemaId must miss the memo and read its own schema");
    }

    @Test
    public void getTableSchemaAtSnapshotMemoIsKeyedByBranch() {
        PaimonSchemaAtMemo memo = new PaimonSchemaAtMemo(10000);
        // Query 1: BASE handle at schemaId=2 (transient table set -> no reload).
        RecordingPaimonCatalogOps baseOps = new RecordingPaimonCatalogOps();
        PaimonTableHandle baseHandle = new PaimonTableHandle(
                "db1", "t1", Collections.emptyList(), Collections.emptyList());
        baseHandle.setPaimonTable(new FakePaimonTable(
                "t1", rowType("id"), Collections.emptyList(), Collections.emptyList()));
        baseOps.schemaAt = new PaimonCatalogOps.PaimonSchemaSnapshot(
                rowType("id").getFields(), Collections.emptyList(), Collections.emptyList());
        // Query 2: BRANCH handle (db1.t1@b1) at the SAME schemaId=2; withBranch clears the transient table
        // so resolveTable reloads the branch table, whose at-schemaId schema differs (bid, bdt).
        RecordingPaimonCatalogOps branchOps = new RecordingPaimonCatalogOps();
        PaimonTableHandle branchHandle = new PaimonTableHandle(
                "db1", "t1", Collections.emptyList(), Collections.emptyList()).withBranch("b1");
        branchOps.branchTable = new FakePaimonTable(
                "t1", rowType("bid", "bdt"), Collections.emptyList(), Collections.emptyList());
        branchOps.schemaAt = new PaimonCatalogOps.PaimonSchemaSnapshot(
                rowType("bid", "bdt").getFields(), Arrays.asList("bdt"), Collections.emptyList());
        ConnectorMvccSnapshot snapshot = ConnectorMvccSnapshot.builder().snapshotId(7L).schemaId(2L).build();

        ConnectorTableSchema baseSchema =
                metadataWith(baseOps, memo).getTableSchema(null, baseHandle, snapshot);
        ConnectorTableSchema branchSchema =
                metadataWith(branchOps, memo).getTableSchema(null, branchHandle, snapshot);

        // WHY: the same schemaId on a different BRANCH is a different schema, so the branch query must miss
        // the base entry and read its own. MUTATION: drop branchName from the key -> the branch query hits
        // the base entry -> (a) branchOps never reads "schemaAt:2" AND (b) branch columns == base [id] -> red.
        Assertions.assertTrue(branchOps.log.contains("schemaAt:2"),
                "a branch handle at the same schemaId must miss the base entry and read the branch schema");
        Assertions.assertEquals(Arrays.asList("bid", "bdt"), columnNames(branchSchema),
                "the branch query must return the branch schema, not a base value cached under a branch-blind key");
        Assertions.assertEquals(Collections.singletonList("id"), columnNames(baseSchema),
                "sanity: the base query returns the base schema");
    }

    // ==================== applySnapshot ====================

    @Test
    public void applySnapshotEmptyPropsFallsBackToScanSnapshotId() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        PaimonTableHandle handle = normalHandle(ops);
        // A latest-pin snapshot (from beginQuerySnapshot) carries NO properties, only an id.
        ConnectorMvccSnapshot snapshot = ConnectorMvccSnapshot.builder().snapshotId(5L).build();

        PaimonTableHandle pinned = (PaimonTableHandle)
                metadataWith(ops).applySnapshot(null, handle, snapshot);

        // WHY: when the snapshot carries no resolved scan options (the beginQuerySnapshot latest-pin
        // path), applySnapshot must FALL BACK to scan.snapshot-id=<id> for B5a parity so the scan
        // reads at that exact version. MUTATION: dropping the empty-props fallback -> getScanOptions()
        // is empty -> red; wrong id -> value != "5" -> red.
        Assertions.assertEquals("5", pinned.getScanOptions().get("scan.snapshot-id"),
                "empty-props applySnapshot must fall back to scan.snapshot-id = the snapshot id");
        Assertions.assertTrue(handle.getScanOptions().isEmpty(),
                "applySnapshot must NOT mutate the input handle (returns a new pinned copy)");
    }

    @Test
    public void applySnapshotThreadsFullPropertiesVerbatim() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        PaimonTableHandle handle = normalHandle(ops);
        // A TAG time-travel snapshot pins scan.tag-name (NOT scan.snapshot-id) in its properties.
        ConnectorMvccSnapshot snapshot = ConnectorMvccSnapshot.builder()
                .snapshotId(42L)
                .property("scan.tag-name", "release-1")
                .build();

        PaimonTableHandle pinned = (PaimonTableHandle)
                metadataWith(ops).applySnapshot(null, handle, snapshot);

        // WHY: applySnapshot must thread the FULL resolved properties map (here scan.tag-name) so a
        // tag read pins the tag, NOT a snapshot id. MUTATION: the old id-only logic would set
        // scan.snapshot-id=42 and drop scan.tag-name -> both assertions red.
        Assertions.assertEquals("release-1", pinned.getScanOptions().get("scan.tag-name"),
                "applySnapshot must thread the resolved scan.tag-name property");
        Assertions.assertNull(pinned.getScanOptions().get("scan.snapshot-id"),
                "a tag-name pin must NOT also set scan.snapshot-id (the id is not the scan option)");
    }

    @Test
    public void applySnapshotOnSysHandleReturnsHandleUnchanged() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        PaimonTableHandle handle = sysHandle(ops);
        ConnectorMvccSnapshot snapshot = ConnectorMvccSnapshot.builder().snapshotId(5L).build();

        PaimonTableHandle result = (PaimonTableHandle)
                metadataWith(ops).applySnapshot(null, handle, snapshot);

        // WHY: system tables (e.g. t$snapshots) are synthetic metadata views with no MVCC — pinning
        // them to a data snapshot is meaningless (same guard as beginQuerySnapshot / the T19 scan
        // fail-loud). The sys handle must come back unchanged, NOT carrying scan.snapshot-id.
        // MUTATION: dropping the isSystemTable() guard -> getScanOptions() carries scan.snapshot-id
        // -> red.
        Assertions.assertSame(handle, result,
                "a sys handle must be returned unchanged (sys tables have no MVCC)");
        Assertions.assertTrue(result.getScanOptions().isEmpty(),
                "a sys handle must NOT be pinned with scan options");
    }

    @Test
    public void applySnapshotWithInvalidSnapshotIdReturnsHandleUnchanged() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        PaimonTableHandle handle = normalHandle(ops);
        // beginQuerySnapshot pins INVALID_SNAPSHOT_ID (-1) for an empty table (NOT Optional.empty),
        // and that -1 flows straight back into applySnapshot.
        ConnectorMvccSnapshot snapshot = ConnectorMvccSnapshot.builder().snapshotId(-1L).build();

        PaimonTableHandle result = (PaimonTableHandle)
                metadataWith(ops).applySnapshot(null, handle, snapshot);

        // WHY: an empty-table pin (-1) must NOT become scan.snapshot-id=-1: Table.copy(-1) resolves to
        // a non-existent snapshot in the paimon SDK (confusing "snapshot/file not found"). Legacy never
        // copied an invalid id — its empty / query-begin path reads latest WITHOUT a copy. So a -1 pin
        // must leave the handle UNCHANGED (no scan option -> reads latest).
        // MUTATION: removing the -1 guard (pinning -1) -> getScanOptions() carries scan.snapshot-id=-1
        // -> both assertions below go red.
        Assertions.assertSame(handle, result,
                "an INVALID_SNAPSHOT_ID (-1) pin must return the handle unchanged (read latest)");
        Assertions.assertTrue(result.getScanOptions().isEmpty(),
                "a -1 snapshot must NOT pin scan.snapshot-id (would hit a non-existent snapshot)");
    }

    @Test
    public void applySnapshotWithNullSnapshotReturnsHandleUnchanged() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        PaimonTableHandle handle = normalHandle(ops);

        PaimonTableHandle result = (PaimonTableHandle)
                metadataWith(ops).applySnapshot(null, handle, null);

        // WHY: a null snapshot must be tolerated (no NPE on snapshot.getSnapshotId()) and treated as
        // "no pin" — same read-latest behavior as the -1 empty-table case.
        // MUTATION: dropping the null guard -> snapshot.getSnapshotId() NPEs -> red.
        Assertions.assertSame(handle, result,
                "a null snapshot must return the handle unchanged (no pin, read latest)");
        Assertions.assertTrue(result.getScanOptions().isEmpty(),
                "a null snapshot must NOT pin scan options");
    }

    @Test
    public void applySnapshotWithBranchSentinelRoutesToWithBranch() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        PaimonTableHandle handle = normalHandle(ops); // has a transient base Table set
        ConnectorMvccSnapshot snapshot = ConnectorMvccSnapshot.builder()
                .snapshotId(7L)
                .property(CoreOptions.BRANCH.key(), "b1")
                .build();

        PaimonTableHandle pinned = (PaimonTableHandle)
                metadataWith(ops).applySnapshot(null, handle, snapshot);

        // WHY: the CoreOptions.BRANCH sentinel is a handle-IDENTITY change (a different table load),
        // NOT a scan option — applySnapshot must route it to withBranch (which clears the transient
        // base Table so resolveTable reloads the BRANCH table), detected BEFORE the generic
        // properties->withScanOptions path. MUTATION: not special-casing the sentinel -> it falls into
        // withScanOptions, so branchName stays null and scanOptions carries "branch" -> all three
        // assertions red.
        Assertions.assertEquals("b1", pinned.getBranchName(),
                "the branch sentinel must route to withBranch (handle identity), not a scan option");
        Assertions.assertTrue(pinned.getScanOptions().isEmpty(),
                "a branch pin must NOT thread the sentinel as a scan-copy option");
        Assertions.assertNull(pinned.getPaimonTable(),
                "withBranch must clear the transient base Table so the branch reloads");
    }

    @Test
    public void applySnapshotScanSnapshotIdStillRoutesToScanOptions() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        PaimonTableHandle handle = normalHandle(ops);
        // A snapshot-id/timestamp time-travel snapshot pins scan.snapshot-id (no branch sentinel).
        ConnectorMvccSnapshot snapshot = ConnectorMvccSnapshot.builder()
                .snapshotId(9L)
                .property("scan.snapshot-id", "9")
                .build();

        PaimonTableHandle pinned = (PaimonTableHandle)
                metadataWith(ops).applySnapshot(null, handle, snapshot);

        // WHY (regression): adding the branch sentinel branch to applySnapshot must NOT regress the
        // existing scan.snapshot-id path — a non-branch property map still routes to withScanOptions
        // and leaves branchName null. MUTATION: the branch detection wrongly firing for a non-branch
        // map -> branchName non-null / scanOptions empty -> both assertions red.
        Assertions.assertEquals("9", pinned.getScanOptions().get("scan.snapshot-id"),
                "a non-branch property map must still route to withScanOptions");
        Assertions.assertNull(pinned.getBranchName(),
                "a non-branch pin must leave branchName null");
        // The transient Table must be PRESERVED: withScanOptions carries it over (same table, read at
        // a version). MUTATION: a mistaken withBranch route (which clears the transient Table to force
        // a branch reload) -> pinned.getPaimonTable() null -> red.
        Assertions.assertSame(handle.getPaimonTable(), pinned.getPaimonTable(),
                "withScanOptions must preserve the transient Table (not clear it like withBranch)");
    }

    // ==================== getTableSchema(snapshot): branch-aware ====================

    @Test
    public void getTableSchemaAtSchemaIdOnBranchHandleResolvesBranchSchema() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        // A branch-aware handle with NO transient Table (forces a branch reload), built via withBranch.
        PaimonTableHandle handle = new PaimonTableHandle(
                "db1", "t1", Collections.emptyList(), Collections.emptyList()).withBranch("b1");
        // ops.table is the BASE (single column "id"); the branch table has different fields.
        ops.table = new FakePaimonTable(
                "t1", rowType("id"), Collections.emptyList(), Collections.emptyList());
        FakePaimonTable branch = new FakePaimonTable(
                "t1", rowType("bid", "bdt"), Collections.emptyList(), Collections.emptyList());
        ops.branchTable = branch;
        // The at-schemaId schema (resolved through schemaAt) carries the branch's historical fields.
        ops.schemaAt = new PaimonCatalogOps.PaimonSchemaSnapshot(
                rowType("bid", "bdt").getFields(), Arrays.asList("bdt"), Collections.emptyList());
        ConnectorMvccSnapshot snapshot = ConnectorMvccSnapshot.builder()
                .snapshotId(7L).schemaId(2L).build();

        ConnectorTableSchema schema = metadataWith(ops).getTableSchema(null, handle, snapshot);

        // WHY: getTableSchema(snapshot) with schemaId>=0 resolves schemaAt(resolveTable(handle)). For a
        // branch handle, resolveTable reloads the BRANCH table, so schemaAt runs against the branch's
        // schemaManager — branch-correct automatically (no branch logic in getTableSchema itself).
        // MUTATION: resolveTable loading the base table instead of the branch -> schemaAt ran against
        // ops.table (the base) -> the lastMvccTable assertion red.
        Assertions.assertEquals(2L, ops.lastSchemaAtArg,
                "the schema must be resolved at the snapshot's schemaId");
        Assertions.assertEquals(Arrays.asList("bid", "bdt"), columnNames(schema),
                "the at-snapshot schema's columns must come from the BRANCH schema");
        Assertions.assertSame(branch, ops.lastMvccTable,
                "schemaAt must run against the BRANCH table (resolveTable loaded the branch)");
    }

    @Test
    public void getTableSchemaLatestOnBranchHandleResolvesBranchRowType() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        PaimonTableHandle handle = new PaimonTableHandle(
                "db1", "t1", Collections.emptyList(), Collections.emptyList()).withBranch("b1");
        ops.table = new FakePaimonTable(
                "t1", rowType("id"), Collections.emptyList(), Collections.emptyList());
        FakePaimonTable branch = new FakePaimonTable(
                "t1", rowType("bid", "bdt"), Collections.emptyList(), Collections.emptyList());
        ops.branchTable = branch;
        // schemaId < 0 -> latest fallback (no schemaAt); the columns must be the BRANCH table's fields.
        ConnectorMvccSnapshot snapshot = ConnectorMvccSnapshot.builder().snapshotId(7L).build();

        ConnectorTableSchema schema = metadataWith(ops).getTableSchema(null, handle, snapshot);

        // WHY: with schemaId<0 the latest fallback resolves resolveTable(handle).rowType(); for a
        // branch handle that is the BRANCH table's rowType (proving resolveTable loaded the branch via
        // the 3-arg branch Identifier, not the base). MUTATION: resolveTable loading the base ->
        // columns are ["id"] not ["bid","bdt"] -> red; calling schemaAt(-1) -> "schemaAt:-1" in log.
        Assertions.assertEquals(Arrays.asList("bid", "bdt"), columnNames(schema),
                "the latest fallback on a branch handle must resolve the BRANCH table's rowType");
        Assertions.assertFalse(ops.log.contains("schemaAt:-1"),
                "a -1 schemaId must NOT call schemaAt");
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

    @Test
    public void connectorDeclaresColumnAutoAnalyzeButNotTopNLazyMaterialize() {
        ConnectorContext ctx = new RecordingConnectorContext();
        Set<ConnectorCapability> caps = new PaimonConnector(Collections.emptyMap(), ctx).getCapabilities();

        // WHY: paimon tables are queryable via the generic SQL-driven ExternalAnalysisTask FULL path, so the
        // flip wires them into background per-column auto-analyze (paimon was never in the legacy
        // instanceof-based whitelist). MUTATION: dropping SUPPORTS_COLUMN_AUTO_ANALYZE -> paimon stays
        // excluded from auto-analyze -> first assertion red.
        Assertions.assertTrue(caps.contains(ConnectorCapability.SUPPORTS_COLUMN_AUTO_ANALYZE),
                "paimon must declare SUPPORTS_COLUMN_AUTO_ANALYZE");
        // Paimon was NEVER eligible for Top-N lazy materialization (legacy PaimonExternalTable was never in
        // MaterializeProbeVisitor's supported set), so granting it would be a new unvalidated feature, not
        // parity. MUTATION: declaring SUPPORTS_TOPN_LAZY_MATERIALIZE on paimon -> this assertion red.
        Assertions.assertFalse(caps.contains(ConnectorCapability.SUPPORTS_TOPN_LAZY_MATERIALIZE),
                "paimon must NOT declare SUPPORTS_TOPN_LAZY_MATERIALIZE (parity: it was never eligible)");
    }

    @Test
    public void connectorDeclaresShowCreateDdlCapability() {
        ConnectorContext ctx = new RecordingConnectorContext();
        Set<ConnectorCapability> caps = new PaimonConnector(Collections.emptyMap(), ctx).getCapabilities();

        // WHY: paimon's table properties (coreOptions incl. path) are user-facing and credential-free, so
        // SHOW CREATE TABLE renders LOCATION + PROPERTIES for paimon. The capability replaces the legacy
        // paimon-only engine-name gate in Env.getDdlStmt (the credential-leak guard now keyed on a capability
        // instead of an engine string). MUTATION: dropping SUPPORTS_SHOW_CREATE_DDL -> paimon SHOW CREATE TABLE
        // regresses to a comment-only shell -> red.
        Assertions.assertTrue(caps.contains(ConnectorCapability.SUPPORTS_SHOW_CREATE_DDL),
                "paimon must declare SUPPORTS_SHOW_CREATE_DDL so SHOW CREATE TABLE keeps rendering "
                        + "LOCATION/PROPERTIES");
    }
}
