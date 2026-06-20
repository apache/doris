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

import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorTableSchema;
import org.apache.doris.connector.api.handle.ConnectorColumnHandle;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;

import org.apache.paimon.table.system.SystemTableLoader;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Tests for the paimon E7 system-table capability (P5-T17): {@code listSupportedSysTables},
 * {@code getSysTableHandle}, and the sys-aware schema/columns reload path.
 *
 * <p>Like the other metadata tests these drive a {@link RecordingPaimonCatalogOps} fake with a
 * {@code null} real catalog, so they stay entirely offline (no live remote paimon).
 */
public class PaimonConnectorMetadataSysTableTest {

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

    private static PaimonTableHandle baseHandle() {
        return new PaimonTableHandle(
                "db1", "t1", Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void listSupportedSysTablesReturnsSdkSystemTables() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();

        List<String> result = metadataWith(ops).listSupportedSysTables(null, baseHandle());

        // WHY: the set of selectable "$sys" tables a user sees per paimon table IS exactly the
        // paimon SDK's SystemTableLoader.SYSTEM_TABLES (legacy PaimonSysTable.SUPPORTED_SYS_TABLES
        // is built from that same list). If this drifted, users could no longer reference e.g.
        // mytable$snapshots. MUTATION: returning Collections.emptyList() (the SPI default) -> red.
        Assertions.assertFalse(result.isEmpty(), "supported sys tables must be non-empty");
        Assertions.assertTrue(result.contains("snapshots"),
                "the SDK system-table list must include 'snapshots'");
        Assertions.assertEquals(new ArrayList<>(SystemTableLoader.SYSTEM_TABLES), result,
                "must mirror the paimon SDK SystemTableLoader.SYSTEM_TABLES exactly");
    }

    @Test
    public void getSysTableHandleResolvesViaFourArgSysIdentifierBranchMain() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        ops.sysTable = new FakePaimonTable("t1$snapshots",
                rowType("snapshot_id", "schema_id"),
                Collections.emptyList(), Collections.emptyList());

        Optional<ConnectorTableHandle> opt =
                metadataWith(ops).getSysTableHandle(null, baseHandle(), "snapshots");

        Assertions.assertTrue(opt.isPresent(), "a supported sys table must yield a handle");
        PaimonTableHandle handle = (PaimonTableHandle) opt.get();
        // WHY: the sys table MUST be loaded through the EXISTING getTable seam using the 4-arg sys
        // Identifier (db, table, branch="main", systemTable=sysName) — that is how paimon's catalog
        // dispatches to the system table rather than the base table. The branch is hardcoded
        // "main" for legacy parity (PaimonSysExternalTable#getSysPaimonTable). MUTATION: building a
        // 2-arg Identifier.create(db,table) (dropping the sys name) -> getSystemTableName() null,
        // branch null -> red; also the wrong (base) table would be resolved.
        Assertions.assertNotNull(ops.lastGetTableId, "the getTable seam must have been called");
        Assertions.assertEquals("snapshots", ops.lastGetTableId.getSystemTableName(),
                "the seam must be called with a 4-arg sys Identifier carrying the sys-table name");
        Assertions.assertEquals("main", ops.lastGetTableId.getBranchName(),
                "the sys Identifier branch must be hardcoded 'main' (legacy parity)");
        Assertions.assertEquals("db1", ops.lastGetTableId.getDatabaseName());
        // getTableName() is the BARE base table ("t1"); getObjectName() would be "t1$snapshots".
        Assertions.assertEquals("t1", ops.lastGetTableId.getTableName());

        // WHY: the handle must self-describe as a sys table carrying the sys name and the resolved
        // sys Table; downstream schema/column reads and (T19) scan routing depend on these.
        // MUTATION: PaimonTableHandle.forSystemTable not setting sysTableName -> isSystemTable()
        // false -> red.
        Assertions.assertTrue(handle.isSystemTable(), "the returned handle must be a sys handle");
        Assertions.assertEquals("snapshots", handle.getSysTableName());
        Assertions.assertSame(ops.sysTable, handle.getPaimonTable(),
                "the resolved sys Table must be stashed as the transient ref");
    }

    @Test
    public void getSysTableHandleSnapshotsIsNotForceJni() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        ops.sysTable = new FakePaimonTable("t1$snapshots",
                rowType("snapshot_id"), Collections.emptyList(), Collections.emptyList());

        PaimonTableHandle handle = (PaimonTableHandle) metadataWith(ops)
                .getSysTableHandle(null, baseHandle(), "snapshots").get();

        // WHY: only binlog/audit_log are NAME-forced to JNI (legacy
        // PaimonScanNode.shouldForceJniForSystemTable). Metadata sys tables like 'snapshots' must
        // NOT be name-forced here — their reader is decided by split type at scan time (T19). Over-
        // forcing would push metadata tables through JNI even when a native path applies. MUTATION:
        // hardcoding forceJni=true -> red.
        Assertions.assertFalse(handle.isForceJni(),
                "metadata sys table 'snapshots' must not be name-forced to JNI");
    }

    @Test
    public void getSysTableHandleBinlogAndAuditLogAreForceJni() {
        for (String sysName : new String[] {"binlog", "audit_log"}) {
            RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
            ops.sysTable = new FakePaimonTable("t1$" + sysName,
                    rowType("c0"), Collections.emptyList(), Collections.emptyList());

            PaimonTableHandle handle = (PaimonTableHandle) metadataWith(ops)
                    .getSysTableHandle(null, baseHandle(), sysName).get();

            // WHY: binlog/audit_log are the two sys tables legacy name-forces to the JNI reader
            // (PaimonScanNode.shouldForceJniForSystemTable). The hint must travel on the handle so
            // T19 routing can honor it. MUTATION: dropping the binlog/audit_log check (forceJni
            // always false) -> red.
            Assertions.assertTrue(handle.isForceJni(),
                    sysName + " must be name-forced to JNI (legacy parity)");
        }
    }

    @Test
    public void getSysTableHandleForceJniIsCaseInsensitive() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        ops.sysTable = new FakePaimonTable("t1$BINLOG",
                rowType("c0"), Collections.emptyList(), Collections.emptyList());

        PaimonTableHandle handle = (PaimonTableHandle) metadataWith(ops)
                .getSysTableHandle(null, baseHandle(), "BINLOG").get();

        // WHY: legacy uses equalsIgnoreCase for both the supported-name check and the force-JNI
        // check, so "BINLOG"/"Binlog" must behave identically to "binlog". MUTATION: switching to
        // case-sensitive equals -> "BINLOG" treated as not-force-JNI (and could even be rejected)
        // -> red.
        Assertions.assertTrue(handle.isSystemTable(),
                "a case-different supported sys name must still resolve");
        Assertions.assertTrue(handle.isForceJni(),
                "force-JNI must match the sys name case-insensitively");
        // WHY: legacy SysTable renders the suffix as "$" + sysTableName.toLowerCase()
        // (SysTable.java:53), so the STORED handle name must be canonical lowercase — otherwise
        // t$BINLOG and t$binlog become distinct handles (distinct equals/hashCode/toString and a
        // distinct sys Identifier), splitting plan/cache identity. MUTATION: storing sysName
        // verbatim -> getSysTableName() == "BINLOG" -> red.
        Assertions.assertEquals("binlog", handle.getSysTableName(),
                "the stored sys name must be normalized to lowercase (legacy parity)");
        Assertions.assertEquals("binlog", ops.lastGetTableId.getSystemTableName(),
                "the sys Identifier must carry the lowercased canonical name");
    }

    @Test
    public void getSysTableHandleMixedCaseYieldsCanonicalLowercaseHandle() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        ops.sysTable = new FakePaimonTable("t1$SnapShots",
                rowType("snapshot_id"), Collections.emptyList(), Collections.emptyList());

        PaimonTableHandle mixed = (PaimonTableHandle) metadataWith(ops)
                .getSysTableHandle(null, baseHandle(), "SnapShots").get();

        // WHY: handle identity parity — a mixed-case input must canonicalize so that the handle
        // built from "SnapShots" is interchangeable with one built from "snapshots" (same name,
        // toString, sys Identifier). This is what prevents two cache/plan keys for one sys table.
        // MUTATION: storing sysName verbatim -> getSysTableName() == "SnapShots", toString ends in
        // "$SnapShots", Identifier carries "SnapShots" -> all three asserts red.
        Assertions.assertEquals("snapshots", mixed.getSysTableName(),
                "mixed-case input must canonicalize to lowercase");
        Assertions.assertEquals("db1.t1$snapshots", mixed.toString(),
                "toString must render the canonical lowercase suffix");
        Assertions.assertEquals("snapshots", ops.lastGetTableId.getSystemTableName(),
                "the sys Identifier must carry the lowercased canonical name");
    }

    @Test
    public void getSysTableHandleNullNameReturnsEmptyWithoutSeamCall() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();

        Optional<ConnectorTableHandle> opt =
                metadataWith(ops).getSysTableHandle(null, baseHandle(), null);

        // WHY: the Javadoc contract is "or empty if not exposed" — a null sysName is simply not an
        // exposed sys table, so it must return Optional.empty() (not NPE on toLowerCase / not a
        // remote seam call). MUTATION: removing the null-guard in isSupportedSysTable -> NPE in
        // equalsIgnoreCase or the later toLowerCase -> red (test would error instead of pass).
        Assertions.assertFalse(opt.isPresent(), "a null sys name must yield Optional.empty()");
        Assertions.assertTrue(ops.log.isEmpty(),
                "a null sys name must short-circuit before touching the seam");
    }

    @Test
    public void getSysTableHandleUnknownNameReturnsEmptyWithoutSeamCall() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();

        Optional<ConnectorTableHandle> opt =
                metadataWith(ops).getSysTableHandle(null, baseHandle(), "not_a_sys_table");

        // WHY: an unsupported name is "this connector does not expose that sys table" (empty), not
        // an error — and, per design, we must not even hit the remote seam for an unknown name (it
        // would be a wasted/failing remote call). MUTATION: removing the isSupportedSysTable guard
        // -> the seam is called -> log non-empty -> red.
        Assertions.assertFalse(opt.isPresent(), "an unknown sys name must yield Optional.empty()");
        Assertions.assertTrue(ops.log.isEmpty(),
                "an unsupported sys name must short-circuit before touching the seam");
    }

    @Test
    public void getSysTableHandleTableNotExistReturnsEmpty() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        ops.throwTableNotExist = true;

        Optional<ConnectorTableHandle> opt =
                metadataWith(ops).getSysTableHandle(null, baseHandle(), "snapshots");

        // WHY: if the base table is gone the sys table cannot exist either; this must degrade to
        // Optional.empty(), NOT propagate the checked TableNotExistException to the SPI caller.
        // MUTATION: removing the TableNotExistException catch -> the checked exception escapes ->
        // red. This branch is only forceable with a recording fake.
        Assertions.assertFalse(opt.isPresent());
    }

    @Test
    public void getTableSchemaForSysHandleBuildsColumnsFromSysRowType() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        // The BASE table has different columns than the SYS table; using the base table's rowType
        // for a sys handle would be a silent bug, so they are deliberately different here.
        ops.table = new FakePaimonTable("t1",
                rowType("id", "name"), Collections.emptyList(), Collections.emptyList());
        FakePaimonTable sys = new FakePaimonTable("t1$snapshots",
                rowType("snapshot_id", "schema_id", "commit_kind"),
                Collections.emptyList(), Collections.emptyList());

        PaimonTableHandle sysHandle = PaimonTableHandle.forSystemTable(
                "db1", "t1", "snapshots", false);
        sysHandle.setPaimonTable(sys);

        ConnectorTableSchema schema = metadataWith(ops).getTableSchema(null, sysHandle);

        // WHY: a sys handle's schema MUST come from the SYSTEM table's rowType, not the base
        // table's. MUTATION: getTableSchema hardcoding the 2-arg base Identifier / base table would
        // surface ["id","name"] -> red.
        List<String> columnNames = new ArrayList<>();
        for (ConnectorColumn c : schema.getColumns()) {
            columnNames.add(c.getName());
        }
        Assertions.assertEquals(Arrays.asList("snapshot_id", "schema_id", "commit_kind"), columnNames,
                "sys-handle schema must be built from the system table's row type");
    }

    @Test
    public void getColumnHandlesForSysHandleReloadsViaFourArgSysIdentifier() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        // Base table (would be served for a 2-arg Identifier) has DIFFERENT columns than the sys
        // table, so a wrong-Identifier reload is detectable.
        ops.table = new FakePaimonTable("t1",
                rowType("id"), Collections.emptyList(), Collections.emptyList());
        ops.sysTable = new FakePaimonTable("t1$snapshots",
                rowType("snapshot_id", "schema_id"),
                Collections.emptyList(), Collections.emptyList());

        // A deserialized sys handle: sysTableName set, transient Table lost (null).
        PaimonTableHandle sysHandle = PaimonTableHandle.forSystemTable(
                "db1", "t1", "snapshots", false);
        Assertions.assertNull(sysHandle.getPaimonTable(), "precondition: transient table is null");

        Map<String, ConnectorColumnHandle> handles =
                metadataWith(ops).getColumnHandles(null, sysHandle);

        // WHY: when the transient sys Table is lost (serialization round-trip), the reload MUST use
        // the 4-arg sys Identifier so the SYSTEM table is re-fetched, not the base table. MUTATION:
        // resolveTable always using Identifier.create(db,table) for the reload -> base columns
        // ["id"] -> red. The captured Identifier's sys name proves the correct reload path.
        Assertions.assertEquals(Arrays.asList("snapshot_id", "schema_id"),
                new ArrayList<>(handles.keySet()),
                "sys-handle columns must come from the system table's row type after reload");
        Assertions.assertNotNull(ops.lastGetTableId, "reload must have hit the seam");
        Assertions.assertEquals("snapshots", ops.lastGetTableId.getSystemTableName(),
                "the reload must use the 4-arg sys Identifier (not the 2-arg base Identifier)");
    }

    @Test
    public void sysHandleSurvivesJavaSerializationRoundTrip() throws Exception {
        // Build a sys handle WITH a transient Table set, exactly as getSysTableHandle returns it.
        FakePaimonTable sys = new FakePaimonTable("t1$binlog",
                rowType("c0"), Collections.emptyList(), Collections.emptyList());
        PaimonTableHandle original = PaimonTableHandle.forSystemTable("db1", "t1", "binlog", true);
        original.setPaimonTable(sys);

        // Real Java serialization round-trip (the FE/BE / plan-reuse wire mechanism).
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(original);
        }
        PaimonTableHandle restored;
        try (ObjectInputStream ois = new ObjectInputStream(
                new ByteArrayInputStream(baos.toByteArray()))) {
            restored = (PaimonTableHandle) ois.readObject();
        }

        // WHY: the whole sys-aware reload-fallback (FIX 1 / metadata twin) only matters BECAUSE the
        // sys identity (sysTableName, forceJni) is genuinely persisted across serialization while
        // the live Table is NOT. This test proves both halves: the non-transient fields survive
        // (so the deserialized handle still self-describes as binlog/force-JNI and can reload its
        // OWN sys Table via the 4-arg Identifier) and the transient Table is dropped (so the reload
        // path is actually exercised, never serving a stale base table). MUTATION: marking
        // sysTableName transient -> getSysTableName() null + isSystemTable() false after deserialize
        // -> red; (separately) making paimonTable non-transient -> getPaimonTable() != null -> red.
        Assertions.assertTrue(restored.isSystemTable(),
                "a deserialized sys handle must still be a sys handle (sysTableName non-transient)");
        Assertions.assertEquals("binlog", restored.getSysTableName(),
                "the (lowercased) sys name must survive serialization");
        Assertions.assertTrue(restored.isForceJni(),
                "the forceJni hint must survive serialization (non-transient)");
        Assertions.assertNull(restored.getPaimonTable(),
                "the resolved Table must be transient — dropped on deserialize, forcing a reload");
    }

    @Test
    public void sysHandleNotEqualToBaseHandle() {
        PaimonTableHandle base = baseHandle();
        PaimonTableHandle sys = PaimonTableHandle.forSystemTable("db1", "t1", "snapshots", false);

        // WHY: a sys handle (db1.t1$snapshots) and its base handle (db1.t1) are DISTINCT tables to
        // the engine; if they compared equal, plan/cache keying could collide and serve base rows
        // for a sys-table query. sysTableName is therefore part of identity. MUTATION: dropping
        // sysTableName from equals/hashCode -> base.equals(sys) true -> red.
        Assertions.assertNotEquals(base, sys, "a sys handle must not equal its base handle");
        Assertions.assertNotEquals(base.hashCode(), sys.hashCode(),
                "distinct identities should (here) hash differently");
    }
}
