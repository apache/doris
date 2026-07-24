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

import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Tests for the B5a scan-pin / partition-key-flip wiring on {@link PaimonTableHandle} and
 * {@link PaimonConnectorMetadata#getTableSchema}: the serializable {@code scanOptions} field, the
 * {@link PaimonTableHandle#withScanOptions(Map)} copy factory (identity-preserving, equals/hashCode
 * ignoring scanOptions), the Java-serialization survival of scanOptions, and the {@code
 * partition_columns} key flip the generic fe-core consumer reads.
 */
public class PaimonTableHandleScanOptionsTest {

    private static RowType rowType(String... columnNames) {
        RowType.Builder builder = RowType.builder();
        for (String name : columnNames) {
            builder.field(name, DataTypes.INT());
        }
        return builder.build();
    }

    @Test
    public void normalHandleHasEmptyScanOptions() {
        PaimonTableHandle handle = new PaimonTableHandle(
                "db1", "t1", Collections.emptyList(), Collections.emptyList());

        // WHY: a normal (un-pinned) handle must default to empty scanOptions so the scan path takes
        // the no-copy fast path. MUTATION: defaulting to null -> NPE downstream / non-empty -> the
        // un-pinned path would wrongly call Table.copy -> red.
        Assertions.assertTrue(handle.getScanOptions().isEmpty(),
                "a normal handle must carry empty scanOptions");
    }

    @Test
    public void withScanOptionsPreservesIdentityAndSetsOptions() {
        PaimonTableHandle base = new PaimonTableHandle(
                "db1", "t1",
                Arrays.asList("dt", "region"),
                Collections.singletonList("id"));
        FakePaimonTable table = new FakePaimonTable(
                "t1", rowType("id"), Collections.emptyList(), Collections.emptyList());
        base.setPaimonTable(table);

        Map<String, String> opts = Collections.singletonMap("scan.snapshot-id", "9");
        PaimonTableHandle pinned = base.withScanOptions(opts);

        // WHY: withScanOptions is a copy factory — the pinned handle is the SAME table, just read at
        // a version, so every identity field AND the transient Table must carry over unchanged while
        // only scanOptions is set. MUTATION: dropping any preserved field (e.g. partitionKeys) or
        // not setting scanOptions -> the matching assertion -> red.
        Assertions.assertEquals("db1", pinned.getDatabaseName());
        Assertions.assertEquals("t1", pinned.getTableName());
        Assertions.assertEquals(Arrays.asList("dt", "region"), pinned.getPartitionKeys(),
                "withScanOptions must preserve partitionKeys");
        Assertions.assertEquals(Collections.singletonList("id"), pinned.getPrimaryKeys(),
                "withScanOptions must preserve primaryKeys");
        Assertions.assertNull(pinned.getSysTableName(),
                "withScanOptions must preserve sysTableName (null for a normal handle)");
        Assertions.assertFalse(pinned.isForceJni(),
                "withScanOptions must preserve forceJni");
        Assertions.assertSame(table, pinned.getPaimonTable(),
                "withScanOptions must carry over the transient Table");
        Assertions.assertEquals(opts, pinned.getScanOptions(),
                "withScanOptions must set the given scanOptions");
    }

    @Test
    public void withScanOptionsPreservesSysIdentity() {
        PaimonTableHandle sys = PaimonTableHandle.forSystemTable("db1", "t1", "binlog", true);

        PaimonTableHandle pinned = sys.withScanOptions(
                Collections.singletonMap("scan.snapshot-id", "9"));

        // WHY: the copy must preserve the sys identity (sysTableName + forceJni) too — a later
        // dispatch may route through withScanOptions on any handle, and silently dropping the sys
        // identity would turn a sys handle into a base handle (wrong rows). MUTATION: omitting
        // sysTableName/forceJni from the copy ctor -> these assertions -> red.
        Assertions.assertTrue(pinned.isSystemTable(),
                "withScanOptions must preserve sys-table identity");
        Assertions.assertEquals("binlog", pinned.getSysTableName());
        Assertions.assertTrue(pinned.isForceJni(),
                "withScanOptions must preserve the forceJni hint");
    }

    @Test
    public void equalsAndHashCodeIgnoreScanOptions() {
        PaimonTableHandle base = new PaimonTableHandle(
                "db1", "t1", Collections.emptyList(), Collections.emptyList());
        PaimonTableHandle pinned = base.withScanOptions(
                Collections.singletonMap("scan.snapshot-id", "5"));

        // WHY: a snapshot-pinned handle is the SAME table read at a version, so it MUST equal/hash
        // identically to its base — otherwise plan/cache keying would treat the pinned read as a
        // different table. scanOptions therefore must NOT participate in equals/hashCode. MUTATION:
        // including scanOptions in equals/hashCode -> base.equals(pinned) false / hashes differ ->
        // red.
        Assertions.assertEquals(base, pinned,
                "a snapshot-pinned handle must equal its base handle (scanOptions ignored in equals)");
        Assertions.assertEquals(base.hashCode(), pinned.hashCode(),
                "scanOptions must not affect hashCode");
    }

    @Test
    public void scanOptionsSurviveJavaSerializationRoundTrip() throws Exception {
        PaimonTableHandle original = new PaimonTableHandle(
                "db1", "t1", Collections.emptyList(), Collections.emptyList())
                .withScanOptions(Collections.singletonMap("scan.snapshot-id", "7"));

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

        // WHY: the JNI serialized-table read happens on a DESERIALIZED handle (the transient Table
        // is dropped and reloaded on BE/plan-reuse), so the snapshot pin must survive serialization
        // — otherwise the pinned read would silently fall back to the latest version (wrong rows for
        // time-travel). scanOptions must therefore be non-transient. MUTATION: marking scanOptions
        // transient -> restored.getScanOptions() empty -> red.
        Assertions.assertEquals("7", restored.getScanOptions().get("scan.snapshot-id"),
                "scanOptions must survive serialization (non-transient) so the pinned read is preserved");
    }

    // ==================== B5b-2c: branch identity ====================

    @Test
    public void withBranchSetsBranchNameAndDoesNotCarryTransientTable() {
        PaimonTableHandle base = new PaimonTableHandle(
                "db1", "t1", Collections.emptyList(), Collections.emptyList());
        FakePaimonTable table = new FakePaimonTable(
                "t1", rowType("id"), Collections.emptyList(), Collections.emptyList());
        base.setPaimonTable(table);

        PaimonTableHandle branched = base.withBranch("b1");

        // WHY: a branch handle must record its branch name and stay a non-system handle.
        Assertions.assertEquals("b1", branched.getBranchName(),
                "withBranch must set the branch name");
        Assertions.assertFalse(branched.isSystemTable(),
                "a branch handle is NOT a system handle");
        // CRITICAL TRAP: unlike withScanOptions, withBranch must NOT carry the transient base Table
        // over — a branch is a DIFFERENT table (independent schema/snapshots). Carrying the base
        // Table would make resolveTable return the base table's rows for the branch read (silent data
        // error). MUTATION: copying this.paimonTable into the branch handle -> getPaimonTable() != null
        // -> red, so resolveTable is forced to reload the BRANCH table via the 3-arg branch Identifier.
        Assertions.assertNull(branched.getPaimonTable(),
                "withBranch must NOT carry the transient base Table (forces a branch reload)");
        // toString must render the branch suffix ("@b1") so logs / explains distinguish a branch read
        // from its base. MUTATION: dropping the branch suffix from toString -> the contains check red.
        Assertions.assertTrue(branched.toString().contains("@b1"),
                "a branch handle's toString must render the '@<branch>' suffix");
    }

    @Test
    public void withBranchPreservesOtherFields() {
        PaimonTableHandle base = new PaimonTableHandle(
                "db1", "t1",
                Arrays.asList("dt", "region"),
                Collections.singletonList("id"))
                .withScanOptions(Collections.singletonMap("scan.snapshot-id", "9"));

        PaimonTableHandle branched = base.withBranch("b1");

        // WHY: withBranch is a copy factory that changes ONLY branchName — every other field
        // (db/table/partitionKeys/primaryKeys/scanOptions) must carry over unchanged. MUTATION:
        // dropping any preserved field -> the matching assertion -> red.
        Assertions.assertEquals("db1", branched.getDatabaseName());
        Assertions.assertEquals("t1", branched.getTableName());
        Assertions.assertEquals(Arrays.asList("dt", "region"), branched.getPartitionKeys(),
                "withBranch must preserve partitionKeys");
        Assertions.assertEquals(Collections.singletonList("id"), branched.getPrimaryKeys(),
                "withBranch must preserve primaryKeys");
        Assertions.assertEquals("9", branched.getScanOptions().get("scan.snapshot-id"),
                "withBranch must preserve scanOptions");
    }

    @Test
    public void branchIsPartOfHandleIdentity() {
        PaimonTableHandle base = new PaimonTableHandle(
                "db1", "t1", Collections.emptyList(), Collections.emptyList());
        PaimonTableHandle b1 = base.withBranch("b1");
        PaimonTableHandle b2 = base.withBranch("b2");

        // WHY: a branch handle is a DIFFERENT table identity than its base and than another branch
        // (independent schema/snapshots), exactly like sysTableName — so branchName MUST participate
        // in equals/hashCode, otherwise plan/cache keying would conflate the base read with the
        // branch read (wrong rows). MUTATION: leaving branchName out of equals/hashCode -> base
        // equals b1 (and b1 equals b2) -> these assertions red.
        Assertions.assertNotEquals(base, b1,
                "a branch handle must NOT equal its base handle");
        Assertions.assertNotEquals(b1, b2,
                "two different branch handles must NOT be equal");
        Assertions.assertEquals(b1, base.withBranch("b1"),
                "two handles on the same branch must be equal");
        Assertions.assertEquals(b1.hashCode(), base.withBranch("b1").hashCode(),
                "two handles on the same branch must have equal hashCodes");
    }

    @Test
    public void scanOptionsStillIgnoredInIdentityForBranchHandle() {
        PaimonTableHandle b1 = new PaimonTableHandle(
                "db1", "t1", Collections.emptyList(), Collections.emptyList()).withBranch("b1");
        PaimonTableHandle b1Pinned = b1.withScanOptions(
                Collections.singletonMap("scan.snapshot-id", "5"));

        // WHY: scanOptions remain excluded from identity even for a branch handle — a branch read
        // pinned at a version is the SAME branch table, just read at a version. MUTATION: including
        // scanOptions in equals/hashCode -> these assertions red.
        Assertions.assertEquals(b1, b1Pinned,
                "a branch handle with vs without scanOptions must be equal (scanOptions excluded)");
        Assertions.assertEquals(b1.hashCode(), b1Pinned.hashCode(),
                "scanOptions must not affect a branch handle's hashCode");
    }

    @Test
    public void sysIdentityUnaffectedByBranchField() {
        PaimonTableHandle base = new PaimonTableHandle(
                "db1", "t1", Collections.emptyList(), Collections.emptyList());
        PaimonTableHandle sys = PaimonTableHandle.forSystemTable("db1", "t1", "snapshots", false);

        // WHY: adding branchName to identity must not regress the existing sys identity invariant —
        // a base handle (branch=null, sys=null) still must NOT equal a sys handle (branch=null,
        // sys=snapshots). MUTATION: a botched equals (e.g. comparing only branchName) -> red.
        Assertions.assertNotEquals(base, sys,
                "a base handle must still NOT equal a sys handle after adding branchName");
        Assertions.assertNull(sys.getBranchName(),
                "forSystemTable must default branchName to null");
    }

    @Test
    public void branchHandleSurvivesJavaSerializationWithTransientTableNull() throws Exception {
        PaimonTableHandle original = new PaimonTableHandle(
                "db1", "t1", Collections.emptyList(), Collections.emptyList())
                .withBranch("b1");

        // Real Java serialization round-trip (the FE/BE / plan-reuse wire mechanism). branchName is
        // non-transient and must survive so the deserialized handle still reloads the BRANCH table.
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(original);
        }
        PaimonTableHandle restored;
        try (ObjectInputStream ois = new ObjectInputStream(
                new ByteArrayInputStream(baos.toByteArray()))) {
            restored = (PaimonTableHandle) ois.readObject();
        }

        // WHY: a deserialized branch handle (transient Table dropped on the BE/plan-reuse boundary)
        // must still know it is a branch so resolveTable reloads the BRANCH table via the 3-arg branch
        // Identifier — otherwise the read would silently fall back to the base table (wrong rows).
        // MUTATION: marking branchName transient -> restored.getBranchName() null -> red.
        Assertions.assertEquals("b1", restored.getBranchName(),
                "branchName must survive serialization (non-transient) so the branch reload is preserved");
        Assertions.assertNull(restored.getPaimonTable(),
                "the transient Table must be null after deserialize (reloaded as the branch table)");
    }

    @Test
    public void getTableSchemaEmitsPartitionColumnsKeyForPartitionedHandle() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        FakePaimonTable table = new FakePaimonTable(
                "t1", rowType("id", "dt", "region"),
                Arrays.asList("dt", "region"),
                Collections.emptyList());
        ops.table = table;
        PaimonTableHandle handle = new PaimonTableHandle(
                "db1", "t1",
                Arrays.asList("dt", "region"),
                Collections.emptyList());
        handle.setPaimonTable(table);

        ConnectorTableSchema schema = new PaimonConnectorMetadata(
                ops, Collections.emptyMap(), new RecordingConnectorContext())
                .getTableSchema(null, handle);
        Map<String, String> props = schema.getProperties();

        // WHY: the generic fe-core consumer PluginDrivenExternalTable.initSchema reads the schema
        // property "partition_columns" (not "partition_keys") to learn a table is partitioned;
        // keying it under "partition_keys" left the FE treating paimon as non-partitioned. MUTATION:
        // emitting the old "partition_keys" key -> "partition_columns" absent + "partition_keys"
        // present -> both assertions red.
        Assertions.assertEquals("dt,region", props.get(ConnectorTableSchema.PARTITION_COLUMNS_KEY),
                "getTableSchema must emit partition keys under the reserved partition-columns key");
        Assertions.assertNull(props.get("partition_keys"),
                "the legacy 'partition_keys' key must no longer be emitted (FE reads partition_columns)");

        // Sanity: columns still resolved (the schema build itself is unaffected by the key flip).
        List<ConnectorColumn> columns = schema.getColumns();
        Assertions.assertEquals(3, columns.size(),
                "all columns must still be mapped from the row type");
    }
}
