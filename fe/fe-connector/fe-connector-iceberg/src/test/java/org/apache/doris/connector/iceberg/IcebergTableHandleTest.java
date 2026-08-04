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

import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * Tests for {@link IcebergTableHandle}, including the T07 MVCC / time-travel pin carriers and the
 * P6.5-T02 system-table variant ({@code sysTableName} + {@link IcebergTableHandle#forSystemTable}).
 */
public class IcebergTableHandleTest {

    @Test
    public void bareHandleHasNoPin() {
        IcebergTableHandle h = new IcebergTableHandle("db1", "t1");
        // WHY: a normal (latest) read must carry NO pin so the scan reads the current snapshot. MUTATION:
        // defaulting snapshotId to 0 (a valid id) -> hasSnapshotPin true -> red.
        Assertions.assertFalse(h.hasSnapshotPin());
        Assertions.assertEquals(-1L, h.getSnapshotId());
        Assertions.assertNull(h.getRef());
        Assertions.assertEquals(-1L, h.getSchemaId());
        Assertions.assertEquals("db1", h.getDbName());
        Assertions.assertEquals("t1", h.getTableName());
    }

    @Test
    public void withSnapshotPinsByIdAndCarriesSchemaId() {
        IcebergTableHandle pinned = new IcebergTableHandle("db1", "t1").withSnapshot(42L, null, 3L);
        Assertions.assertTrue(pinned.hasSnapshotPin());
        Assertions.assertEquals(42L, pinned.getSnapshotId());
        Assertions.assertNull(pinned.getRef());
        Assertions.assertEquals(3L, pinned.getSchemaId());
        // The coordinates survive the pin.
        Assertions.assertEquals("db1", pinned.getDbName());
        Assertions.assertEquals("t1", pinned.getTableName());
    }

    @Test
    public void withSnapshotPinsByRef() {
        IcebergTableHandle pinned = new IcebergTableHandle("db1", "t1").withSnapshot(7L, "b1", 2L);
        // WHY: a tag/branch read pins by REF (useRef), so a ref pin alone must count as a pin even if an id is
        // also present. MUTATION: hasSnapshotPin checking only snapshotId -> still true here, so also assert
        // ref-only below.
        Assertions.assertTrue(pinned.hasSnapshotPin());
        Assertions.assertEquals("b1", pinned.getRef());
    }

    @Test
    public void refOnlyPinCountsAsPin() {
        IcebergTableHandle pinned = new IcebergTableHandle("db1", "t1").withSnapshot(-1L, "tag1", 5L);
        // MUTATION: hasSnapshotPin returning snapshotId>=0 only -> false here -> red (a ref pin would be lost).
        Assertions.assertTrue(pinned.hasSnapshotPin());
        Assertions.assertEquals("tag1", pinned.getRef());
        Assertions.assertEquals(-1L, pinned.getSnapshotId());
    }

    @Test
    public void pinIsPartOfIdentity() {
        IcebergTableHandle bare = new IcebergTableHandle("db1", "t1");
        IcebergTableHandle pinned = bare.withSnapshot(42L, null, 3L);
        IcebergTableHandle samePin = new IcebergTableHandle("db1", "t1").withSnapshot(42L, null, 3L);
        // WHY: the pin is part of the handle identity (a query-begin handle and a time-travel handle for the
        // same table are different reads). MUTATION: equals/hashCode ignoring the pin -> bare.equals(pinned) -> red.
        Assertions.assertNotEquals(bare, pinned);
        Assertions.assertEquals(pinned, samePin);
        Assertions.assertEquals(pinned.hashCode(), samePin.hashCode());
        Assertions.assertEquals(bare, new IcebergTableHandle("db1", "t1"));
    }

    // ==================== P6.5-T02: system-table variant ====================

    @Test
    public void bareHandleIsNotSystemTable() {
        IcebergTableHandle h = new IcebergTableHandle("db1", "t1");
        // WHY: a normal (data) table handle must not be mistaken for a system table, or the generic
        // sys-table machinery would try to build a metadata-table for it. MUTATION: isSystemTable
        // returning true by default / sysTableName defaulting to non-null -> red.
        Assertions.assertFalse(h.isSystemTable());
        Assertions.assertNull(h.getSysTableName());
    }

    @Test
    public void forSystemTableCarriesSysNameAndCoordinates() {
        IcebergTableHandle sys = IcebergTableHandle.forSystemTable("db1", "t1", "snapshots", -1L, null, -1L);
        // WHY: the connector resolves the metadata-table from the bare sys name (no "$"), so the handle
        // must carry it through, while keeping the base table's coordinates. MUTATION: forSystemTable
        // not storing sysTableName -> isSystemTable false -> red.
        Assertions.assertTrue(sys.isSystemTable());
        Assertions.assertEquals("snapshots", sys.getSysTableName());
        Assertions.assertEquals("db1", sys.getDbName());
        Assertions.assertEquals("t1", sys.getTableName());
        // An un-pinned sys handle (latest read) carries no pin.
        Assertions.assertFalse(sys.hasSnapshotPin());
    }

    @Test
    public void forSystemTableRetainsSnapshotPin() {
        IcebergTableHandle sys = IcebergTableHandle.forSystemTable("db1", "t1", "snapshots", 42L, null, 3L);
        // WHY (deviation 1, the hard invariant of decision A): iceberg system tables legally time-travel
        // (e.g. `SELECT * FROM t$snapshots FOR VERSION AS OF 42`), so forSystemTable must RETAIN the
        // snapshot pin — unlike paimon's forSystemTable, which clears it. MUTATION: forSystemTable
        // dropping the pin (passing NO_PIN) -> snapshotId -1 / hasSnapshotPin false -> red, time-travel
        // sys-table reads would silently fall back to the latest version.
        Assertions.assertTrue(sys.hasSnapshotPin());
        Assertions.assertEquals(42L, sys.getSnapshotId());
        Assertions.assertEquals(3L, sys.getSchemaId());
    }

    @Test
    public void forSystemTableRetainsRefPin() {
        IcebergTableHandle sys = IcebergTableHandle.forSystemTable("db1", "t1", "history", -1L, "tag1", 5L);
        // WHY: a tag/branch time-travel sys read pins by REF (useRef), so a ref pin must also survive on
        // a sys handle. MUTATION: forSystemTable dropping ref -> getRef null / hasSnapshotPin false -> red.
        Assertions.assertTrue(sys.hasSnapshotPin());
        Assertions.assertEquals("tag1", sys.getRef());
        Assertions.assertEquals(-1L, sys.getSnapshotId());
    }

    @Test
    public void sysTableNameIsPartOfIdentity() {
        IcebergTableHandle base = new IcebergTableHandle("db1", "t1");
        IcebergTableHandle snapshots = IcebergTableHandle.forSystemTable("db1", "t1", "snapshots", -1L, null, -1L);
        IcebergTableHandle history = IcebergTableHandle.forSystemTable("db1", "t1", "history", -1L, null, -1L);
        IcebergTableHandle sameSnapshots =
                IcebergTableHandle.forSystemTable("db1", "t1", "snapshots", -1L, null, -1L);
        // WHY: `db.t$snapshots` is a DIFFERENT table than `db.t` and than `db.t$history` (different
        // schema/rows), so sysTableName must be part of equals/hashCode. MUTATION: equals/hashCode
        // ignoring sysTableName -> base.equals(snapshots) or snapshots.equals(history) -> red.
        Assertions.assertNotEquals(base, snapshots);
        Assertions.assertNotEquals(snapshots, history);
        Assertions.assertEquals(snapshots, sameSnapshots);
        Assertions.assertEquals(snapshots.hashCode(), sameSnapshots.hashCode());
    }

    @Test
    public void sysHandleAtDifferentVersionsAreDifferent() {
        IcebergTableHandle v1 = IcebergTableHandle.forSystemTable("db1", "t1", "snapshots", 1L, null, -1L);
        IcebergTableHandle v2 = IcebergTableHandle.forSystemTable("db1", "t1", "snapshots", 2L, null, -1L);
        // WHY: a time-travel sys read (t$snapshots FOR VERSION AS OF 1) is a different read than
        // VERSION AS OF 2, so the pin composes with sysTableName in identity (consistent with the
        // existing iceberg handle, where the pin is already part of identity — unlike paimon). MUTATION:
        // equals collapsing the pin on a sys handle -> v1.equals(v2) -> red.
        Assertions.assertNotEquals(v1, v2);
    }

    @Test
    public void withSnapshotPreservesSysTableName() {
        IcebergTableHandle sys = IcebergTableHandle.forSystemTable("db1", "t1", "snapshots", -1L, null, -1L);
        IcebergTableHandle pinned = sys.withSnapshot(99L, null, 7L);
        // WHY: withSnapshot is a copy factory used to thread a resolved time-travel pin in; it must NOT
        // silently drop sysTableName, or a sys handle would degrade into a normal data-table handle
        // (wrong schema/rows). Mirrors paimon's withScanOptions/withBranch, which preserve sysTableName.
        // MUTATION: withSnapshot rebuilding with a null sysTableName -> pinned.isSystemTable() false -> red.
        Assertions.assertTrue(pinned.isSystemTable());
        Assertions.assertEquals("snapshots", pinned.getSysTableName());
        Assertions.assertEquals(99L, pinned.getSnapshotId());
    }

    @Test
    public void sysTableNameSurvivesJavaSerializationRoundTrip() throws Exception {
        IcebergTableHandle original = IcebergTableHandle.forSystemTable("db1", "t1", "snapshots", 42L, null, 3L);

        // Real Java serialization round-trip (the FE/BE / plan-reuse wire mechanism).
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(original);
        }
        IcebergTableHandle restored;
        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()))) {
            restored = (IcebergTableHandle) ois.readObject();
        }

        // WHY: the JNI sys-table read happens on a DESERIALIZED handle, so sysTableName must be
        // non-transient — otherwise the restored handle would forget it is a sys table (and its pin),
        // silently reading the base data table at the latest version. MUTATION: marking sysTableName
        // transient -> restored.isSystemTable() false -> red.
        Assertions.assertTrue(restored.isSystemTable());
        Assertions.assertEquals("snapshots", restored.getSysTableName());
        Assertions.assertEquals(42L, restored.getSnapshotId());
        Assertions.assertEquals(original, restored);
    }

    @Test
    public void toStringIncludesSysName() {
        IcebergTableHandle sys = IcebergTableHandle.forSystemTable("db1", "t1", "snapshots", -1L, null, -1L);
        // WHY: toString is used in plan dumps / error messages; a sys handle must render its sys name so
        // a `db.t$snapshots` read is distinguishable from `db.t`. MUTATION: toString omitting sysTableName
        // -> assertion below fails.
        Assertions.assertTrue(sys.toString().contains("snapshots"),
                "toString must surface the sys-table name, was: " + sys);
    }

    @Test
    public void coordinatesArePartOfIdentity() {
        // WHY (T07 gap-fill): the handle is a plan-cache map key, so the BASE coordinates (dbName /
        // tableName) MUST participate in equals/hashCode — otherwise db1.t1$snapshots would collide with
        // db1.t2$snapshots (or db1.t1 with db2.t1), serving one table's plan for another. Every other
        // identity test fixes coords to db1/t1, so a mutation dropping dbName or tableName from
        // equals/hashCode would pass green. MUTATION: equals/hashCode omitting dbName -> the db2 asserts
        // below fail; omitting tableName -> the t2 asserts fail.
        Assertions.assertNotEquals(
                IcebergTableHandle.forSystemTable("db1", "t1", "snapshots", -1L, null, -1L),
                IcebergTableHandle.forSystemTable("db1", "t2", "snapshots", -1L, null, -1L),
                "a sys handle on a different base table must not be equal");
        Assertions.assertNotEquals(
                IcebergTableHandle.forSystemTable("db1", "t1", "snapshots", -1L, null, -1L),
                IcebergTableHandle.forSystemTable("db2", "t1", "snapshots", -1L, null, -1L),
                "a sys handle in a different base db must not be equal");
        Assertions.assertNotEquals(new IcebergTableHandle("db1", "t1"), new IcebergTableHandle("db2", "t1"),
                "a bare handle in a different db must not be equal");
        Assertions.assertNotEquals(new IcebergTableHandle("db1", "t1"), new IcebergTableHandle("db1", "t2"),
                "a bare handle on a different table must not be equal");
    }

    @Test
    public void toStringOfPinnedSysHandleRendersSeparatorAndPin() {
        IcebergTableHandle sys = IcebergTableHandle.forSystemTable("db1", "t1", "snapshots", 42L, null, 3L);
        // WHY (T07 gap-fill): the only existing toString test uses an UN-pinned sys handle and a
        // substring("snapshots") assertion, so neither the '$' separator nor the hasSnapshotPin() render
        // branch is exercised. A pinned sys handle (time-travel) must render BOTH `t1$snapshots` and the
        // pin, so a plan dump distinguishes `t1$snapshots FOR VERSION AS OF 42` from a latest read.
        // MUTATION: dropping the '$' separator -> no "t1$snapshots" -> red; dropping the pin branch on a
        // sys handle -> no "snapshotId=42" -> red.
        String s = sys.toString();
        Assertions.assertTrue(s.contains("t1$snapshots"), "must render the '$'-joined sys name, was: " + s);
        Assertions.assertTrue(s.contains("snapshotId=42"), "must render the snapshot pin, was: " + s);
    }

    // ==================== WS-REWRITE R2: rewrite_data_files per-group file scope ====================

    @Test
    public void bareHandleHasNoRewriteScope() {
        IcebergTableHandle h = new IcebergTableHandle("db1", "t1");
        // WHY: every non-rewrite scan must carry NO scope so it reads the whole (filtered) table. The scan
        // provider treats a non-null scope as "keep ONLY these files", so a default of empty (not null) would
        // make a normal scan silently return zero files. MUTATION: defaulting rewriteFileScope to an empty set
        // -> getRewriteFileScope non-null -> red.
        Assertions.assertNull(h.getRewriteFileScope());
    }

    @Test
    public void withRewriteFileScopeCarriesRawPathsAndIsPartOfIdentity() {
        IcebergTableHandle bare = new IcebergTableHandle("db1", "t1");
        IcebergTableHandle scoped = bare.withRewriteFileScope(
                ImmutableSet.of("oss://b/db/t1/f1.parquet", "oss://b/db/t1/f3.parquet"));
        // WHY: the scope is a rewrite group's bin-packed file set (raw iceberg paths); the getter must return
        // exactly those so the scan keeps only them. MUTATION: storing null/empty -> getter wrong -> red.
        Assertions.assertEquals(
                ImmutableSet.of("oss://b/db/t1/f1.parquet", "oss://b/db/t1/f3.parquet"),
                scoped.getRewriteFileScope());
        // WHY: a scoped scan is a DIFFERENT read than the full scan and than a differently-scoped scan, so the
        // scope is part of the handle identity (consistent with the snapshot pin). MUTATION: equals/hashCode
        // ignoring rewriteFileScope -> bare.equals(scoped) or the two distinct scopes equal -> red.
        Assertions.assertNotEquals(bare, scoped);
        IcebergTableHandle sameScope = new IcebergTableHandle("db1", "t1").withRewriteFileScope(
                ImmutableSet.of("oss://b/db/t1/f1.parquet", "oss://b/db/t1/f3.parquet"));
        Assertions.assertEquals(scoped, sameScope);
        Assertions.assertEquals(scoped.hashCode(), sameScope.hashCode());
        Assertions.assertNotEquals(scoped,
                new IcebergTableHandle("db1", "t1").withRewriteFileScope(
                        ImmutableSet.of("oss://b/db/t1/f1.parquet")));
    }

    @Test
    public void rewriteScopeAndSnapshotPinCompose() {
        // WHY: the rewrite driver pins the starting snapshot AND scopes the file set; applying one copy factory
        // must not drop the other carrier, else the group would scan the wrong snapshot or the wrong files.
        // MUTATION: withSnapshot rebuilding without rewriteFileScope -> scope lost -> red.
        IcebergTableHandle scopedThenPinned = new IcebergTableHandle("db1", "t1")
                .withRewriteFileScope(ImmutableSet.of("oss://b/db/t1/f1.parquet"))
                .withSnapshot(42L, null, 3L);
        Assertions.assertEquals(ImmutableSet.of("oss://b/db/t1/f1.parquet"),
                scopedThenPinned.getRewriteFileScope());
        Assertions.assertEquals(42L, scopedThenPinned.getSnapshotId());
    }

    @Test
    public void rewriteScopeSurvivesSerializationRoundTrip() throws Exception {
        IcebergTableHandle original = new IcebergTableHandle("db1", "t1")
                .withRewriteFileScope(ImmutableSet.of("oss://b/db/t1/f1.parquet", "oss://b/db/t1/f2.parquet"));
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(original);
        }
        IcebergTableHandle restored;
        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()))) {
            restored = (IcebergTableHandle) ois.readObject();
        }
        // WHY: the handle is the plan-reuse / FE-BE wire object, so the scope (an ImmutableSet field) must
        // survive serialization or a deserialized rewrite handle would forget its scope and scan the whole
        // table. MUTATION: marking rewriteFileScope transient -> restored scope null -> red.
        Assertions.assertEquals(original.getRewriteFileScope(), restored.getRewriteFileScope());
        Assertions.assertEquals(original, restored);
    }

    // ==================== M-4: Top-N lazy-materialization signal ====================

    @Test
    public void bareHandleIsNotTopnLazyMaterialize() {
        IcebergTableHandle h = new IcebergTableHandle("db1", "t1");
        // WHY: a normal read must NOT be flagged topn, or the scan provider would build the full-schema dict
        // (losing the pruned-column optimization) for every query. MUTATION: defaulting topnLazyMaterialize
        // to true -> isTopnLazyMaterialize() true -> red.
        Assertions.assertFalse(h.isTopnLazyMaterialize());
    }

    @Test
    public void withTopnLazyMaterializeSetsFlagAndIsPartOfIdentity() {
        IcebergTableHandle bare = new IcebergTableHandle("db1", "t1");
        IcebergTableHandle topn = bare.withTopnLazyMaterialize(true);
        // WHY: the flag changes the BE-facing field-id dictionary (pruned vs full), so it is part of the
        // handle identity (consistent with the snapshot pin / rewrite scope). MUTATION: withTopnLazyMaterialize
        // not storing the flag -> isTopnLazyMaterialize false -> red; equals/hashCode ignoring it ->
        // bare.equals(topn) -> red.
        Assertions.assertTrue(topn.isTopnLazyMaterialize());
        Assertions.assertNotEquals(bare, topn);
        IcebergTableHandle sameTopn = new IcebergTableHandle("db1", "t1").withTopnLazyMaterialize(true);
        Assertions.assertEquals(topn, sameTopn);
        Assertions.assertEquals(topn.hashCode(), sameTopn.hashCode());
    }

    @Test
    public void topnLazyMaterializeComposesWithPinAndScope() {
        // WHY: a time-travel + rewrite + topn scan applies all three copy factories; none must drop another
        // carrier, else the scan reads the wrong snapshot/files or loses the full-schema dict. MUTATION:
        // withSnapshot or withRewriteFileScope rebuilding without topnLazyMaterialize -> flag lost -> red.
        IcebergTableHandle h = new IcebergTableHandle("db1", "t1")
                .withTopnLazyMaterialize(true)
                .withSnapshot(42L, null, 3L)
                .withRewriteFileScope(ImmutableSet.of("oss://b/db/t1/f1.parquet"));
        Assertions.assertTrue(h.isTopnLazyMaterialize());
        Assertions.assertEquals(42L, h.getSnapshotId());
        Assertions.assertEquals(ImmutableSet.of("oss://b/db/t1/f1.parquet"), h.getRewriteFileScope());
    }

}
