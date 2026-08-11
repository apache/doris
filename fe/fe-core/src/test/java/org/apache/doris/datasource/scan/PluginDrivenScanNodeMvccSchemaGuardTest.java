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

package org.apache.doris.datasource.scan;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.SchemaCacheValue;
import org.apache.doris.datasource.mvcc.PluginDrivenMvccExternalTable;
import org.apache.doris.datasource.plugin.PluginDrivenSysExternalTable;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Unit tests for the L17 fail-loud guard {@link PluginDrivenScanNode#assertBoundColumnsResolveInPinnedSchema}:
 * a same-table multi-version reference whose FE tuple was bound at a DIFFERENT schema than the version it
 * scans must be rejected loud (BE would field-id/name-mismatch), while a reference whose bound columns all
 * resolve in its own version-aware pinned schema (incl. a field-id-stable rename or a subset projection)
 * must pass. The helper takes plain {@link Column}s + a {@link SchemaCacheValue} so it is exercised without
 * constructing a scan node.
 */
public class PluginDrivenScanNodeMvccSchemaGuardTest {

    private static Column col(String name, int uniqueId) {
        Column c = new Column(name, Type.INT);
        c.setUniqueId(uniqueId);
        return c;
    }

    private static SchemaCacheValue schema(Column... cols) {
        return new SchemaCacheValue(Arrays.asList(cols));
    }

    /** An ordinary (non-sys) table: the guard applies in full. */
    private static TableIf table() {
        TableIf t = Mockito.mock(PluginDrivenMvccExternalTable.class);
        Mockito.when(t.getName()).thenReturn("db.t");
        return t;
    }

    @Test
    public void fieldIdRenumberBetweenBoundAndScannedVersionThrows() throws UserException {
        // The tuple was bound at LATEST where column `c` has field-id 7, but THIS reference scans a pinned
        // version where `c` has field-id 5. BE matches iceberg columns by field-id, so slot field-id 7 has no
        // entry in the v-pinned dict -> crash. The guard must throw. MUTATION: dropping the guard (or matching
        // by name only) -> the renumber slips through -> red.
        List<Column> bound = Collections.singletonList(col("c", 7));
        SchemaCacheValue pinned = schema(col("c", 5));
        UserException e = Assertions.assertThrows(UserException.class,
                () -> PluginDrivenScanNode.assertBoundColumnsResolveInPinnedSchema(bound, pinned, table()));
        Assertions.assertTrue(e.getMessage().contains("multiple versions"), e.getMessage());
        Assertions.assertTrue(e.getMessage().contains("'c'"), e.getMessage());
    }

    @Test
    public void columnAddedAfterScannedVersionThrows() throws UserException {
        // The tuple (bound latest) references a column `added` (field-id 9) that does NOT exist in the pinned
        // version this reference scans -> the version's files have no such field -> the guard must throw.
        List<Column> bound = Arrays.asList(col("id", 1), col("added", 9));
        SchemaCacheValue pinned = schema(col("id", 1));   // pinned version predates `added`
        UserException e = Assertions.assertThrows(UserException.class,
                () -> PluginDrivenScanNode.assertBoundColumnsResolveInPinnedSchema(bound, pinned, table()));
        Assertions.assertTrue(e.getMessage().contains("'added'"), e.getMessage());
    }

    @Test
    public void nameMissWhenNoFieldIdThrows() throws UserException {
        // Paimon carries no top-level field-id (uniqueId == -1) so matching is by NAME: a tuple bound at
        // LATEST with column `newname` scanning a version that only has `oldname` (a paimon rename) must throw
        // (BE matches by name -> `newname` unreadable from the old files).
        List<Column> bound = Collections.singletonList(col("newname", -1));
        SchemaCacheValue pinned = schema(col("oldname", -1));
        Assertions.assertThrows(UserException.class,
                () -> PluginDrivenScanNode.assertBoundColumnsResolveInPinnedSchema(bound, pinned, table()));
    }

    @Test
    public void fieldIdStableRenameResolvesByIdNoThrow() throws UserException {
        // A rename that KEEPS the field-id is fine: BE reads iceberg field-id 5 regardless of name, so a tuple
        // bound with `newname`@5 scanning a version whose column is `oldname`@5 must NOT throw (id resolves).
        // Guards against a name-only check over-rejecting the benign rename case.
        List<Column> bound = Collections.singletonList(col("newname", 5));
        SchemaCacheValue pinned = schema(col("oldname", 5), col("other", 6));
        Assertions.assertDoesNotThrow(() ->
                PluginDrivenScanNode.assertBoundColumnsResolveInPinnedSchema(bound, pinned, table()));
    }

    @Test
    public void subsetProjectionAllResolvedNoThrow() throws UserException {
        // The tuple is a projection (subset) of the version's columns; every bound field-id is present -> ok.
        List<Column> bound = Arrays.asList(col("a", 1), col("c", 3));
        SchemaCacheValue pinned = schema(col("a", 1), col("b", 2), col("c", 3));
        Assertions.assertDoesNotThrow(() ->
                PluginDrivenScanNode.assertBoundColumnsResolveInPinnedSchema(bound, pinned, table()));
    }

    @Test
    public void nameMatchWhenNoFieldIdNoThrow() throws UserException {
        // Paimon (uniqueId == -1) matching by name: same names -> resolved -> no throw.
        List<Column> bound = Arrays.asList(col("a", -1), col("b", -1));
        SchemaCacheValue pinned = schema(col("a", -1), col("b", -1), col("c", -1));
        Assertions.assertDoesNotThrow(() ->
                PluginDrivenScanNode.assertBoundColumnsResolveInPinnedSchema(bound, pinned, table()));
    }

    @Test
    public void nullPinnedSchemaIsNoOp() throws UserException {
        // A latest / @incr / hive reference carries a null pinnedSchema -> the guard is a no-op (no
        // version-at-snapshot schema to skew against), regardless of the bound columns. (A sys-table
        // reference is excluded by TYPE, not by a null schema -- see sysTableIsExcludedNoThrow.)
        List<Column> bound = Collections.singletonList(col("anything", 42));
        Assertions.assertDoesNotThrow(() ->
                PluginDrivenScanNode.assertBoundColumnsResolveInPinnedSchema(bound, null, table()));
    }

    @Test
    public void rowIdColumnIsExcludedNoThrow() throws UserException {
        // Topn lazy materialization (LazyMaterializeTopN) injects a reader-synthesized row-id carrying
        // uniqueId = Integer.MAX_VALUE, which is BY CONSTRUCTION absent from every pinned schema. Without the
        // carve-out the guard fires on every "pinned version + order by/limit" query -- it took down
        // test_iceberg_time_travel and iceberg_branch_complex_queries (CI 996541).
        // MUTATION: dropping the GLOBAL_ROWID_COL carve-out -> red.
        List<Column> bound = Arrays.asList(col("id", 1),
                col(Column.GLOBAL_ROWID_COL + "tag_branch_table", Integer.MAX_VALUE));
        SchemaCacheValue pinned = schema(col("id", 1));
        Assertions.assertDoesNotThrow(() ->
                PluginDrivenScanNode.assertBoundColumnsResolveInPinnedSchema(bound, pinned, table()));
    }

    @Test
    public void rowIdBeforeSkewedColumnStillThrows() throws UserException {
        // The carve-out must SKIP the row-id and keep checking the rest of the tuple, not abandon the whole
        // check. MUTATION: writing the carve-out as `return` instead of `continue` -> a real skew on `added`
        // that sits AFTER the row-id slips through silently -> red.
        List<Column> bound = Arrays.asList(
                col(Column.GLOBAL_ROWID_COL + "t", Integer.MAX_VALUE),
                col("added", 9));
        SchemaCacheValue pinned = schema(col("id", 1));
        Assertions.assertThrows(UserException.class,
                () -> PluginDrivenScanNode.assertBoundColumnsResolveInPinnedSchema(bound, pinned, table()));
    }

    @Test
    public void sysTableIsExcludedNoThrow() throws UserException {
        // A sys-table scan's pin is resolved off the SOURCE table (resolveSysTableSnapshotPin), so pinnedSchema
        // carries the SOURCE's columns {id, name} while the tuple carries the SYS table's synthetic columns
        // {file_path, pos, ...}. Comparing them is a category error that can NEVER resolve -- it threw a bogus
        // "multiple versions" error (surfaced as "Failed to pin MVCC snapshot") on every iceberg sys-table time
        // travel: `select count(*) from t$position_deletes for version as of <snap>` (CI 997422,
        // test_iceberg_position_deletes_sys_table).
        // MUTATION: dropping the `table instanceof PluginDrivenSysExternalTable` exclusion -> red.
        // Two things here are LOAD-BEARING against a vacuous pass:
        //  - non-null pinnedSchema: a null one passes via the null no-op and stays green even with the
        //    exclusion reverted;
        //  - uniqueId == -1 on the bound columns: the guard keys on field-id when uniqueId >= 0, so giving the
        //    sys columns ids that happen to collide with the source's ids (file_path@1 vs id@1) would resolve
        //    by ID and never throw -- green with the exclusion reverted. Name matching is also what the real
        //    case does: CI 997422 threw on 'file_path' precisely because it resolved by NAME against the
        //    source's {id, name}.
        List<Column> bound = Arrays.asList(col("file_path", -1), col("pos", -1));
        SchemaCacheValue pinned = schema(col("id", -1), col("name", -1));
        TableIf sysTable = Mockito.mock(PluginDrivenSysExternalTable.class);
        Mockito.when(sysTable.getName()).thenReturn("db.t$position_deletes");

        Assertions.assertDoesNotThrow(() ->
                PluginDrivenScanNode.assertBoundColumnsResolveInPinnedSchema(bound, pinned, sysTable));
    }

    @Test
    public void normalMvccTableWithSameShapeStillThrows() throws UserException {
        // Locks the exclusion to the sys-table TYPE, not to the column shape: an ordinary MVCC table whose
        // tuple genuinely skews against its pinned schema must still throw. PluginDrivenSysExternalTable and
        // PluginDrivenMvccExternalTable are sibling subclasses, so the exclusion cannot swallow a real table.
        // MUTATION: widening the exclusion to PluginDrivenExternalTable (their common parent) -> red.
        List<Column> bound = Arrays.asList(col("file_path", -1), col("pos", -1));
        SchemaCacheValue pinned = schema(col("id", -1), col("name", -1));

        Assertions.assertThrows(UserException.class,
                () -> PluginDrivenScanNode.assertBoundColumnsResolveInPinnedSchema(bound, pinned, table()));
    }
}
