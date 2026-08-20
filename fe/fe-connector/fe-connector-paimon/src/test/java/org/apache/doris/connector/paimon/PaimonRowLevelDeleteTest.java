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

import org.apache.doris.connector.spi.ConnectorColumn;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.handle.WriteOperation;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Row-level DELETE tests, pinning the table-shape gate that decides HOW (and whether) a paimon table can be
 * deleted from:
 *
 * <ul>
 * <li>a <b>primary-key</b> table is always deletable (a keyed {@code RowKind.DELETE} record) and needs NO
 *     synthetic row-id column — the key is the address;</li>
 * <li>an <b>append-only</b> table is deletable only with deletion vectors, and then DOES need the row-id
 *     column (data file + ordinal) for the vector to mark;</li>
 * <li>an append-only table with neither is rejected with an actionable message — a Paimon engine
 *     limitation, so the rejection must name the fix rather than read as a Doris gap.</li>
 * </ul>
 *
 * <p>All tests run offline against the recording seam fake (null real Catalog).
 */
public class PaimonRowLevelDeleteTest {

    private static PaimonConnectorMetadata metadata(RecordingPaimonCatalogOps ops,
            RecordingConnectorContext ctx) {
        return new PaimonConnectorMetadata(ops, PaimonCatalogProperties.of(Collections.emptyMap()), ctx);
    }

    private static RowType rowType() {
        return new RowType(Arrays.asList(
                new DataField(0, "id", DataTypes.INT().notNull()),
                new DataField(1, "note", DataTypes.STRING())));
    }

    /** A handle whose backing table has the given primary keys and options. */
    private static PaimonTableHandle handle(RecordingPaimonCatalogOps ops, List<String> primaryKeys,
            Map<String, String> options) {
        PaimonTableHandle handle = new PaimonTableHandle(
                "db1", "t1", Collections.emptyList(), primaryKeys);
        FakePaimonTable table = new FakePaimonTable(
                "t1", rowType(), Collections.emptyList(), primaryKeys);
        table.setOptions(options);
        ops.table = table;
        handle.setPaimonTable(table);
        return handle;
    }

    private static Map<String, String> deletionVectorsEnabled() {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true");
        return options;
    }

    // ==================== connector-level capability ====================

    @Test
    public void connectorDeclaresTheRowLevelTrioSoTheRegistryCanRouteThem() {
        // WHY: RowLevelDmlRegistry probes the connector's declared write operations, NOT the table type.
        // Without these in the set the row-level paths are unreachable and the statements fall through to
        // the native "olapTable" rejections.
        // MUTATION: dropping any of the three flips this red.
        PaimonWritePlanProvider provider = new PaimonWritePlanProvider(
                PaimonCatalogProperties.of(Collections.emptyMap()),
                new RecordingPaimonCatalogOps(), new RecordingConnectorContext());
        Set<WriteOperation> ops = provider.supportedOperations();
        Assertions.assertTrue(ops.contains(WriteOperation.DELETE));
        Assertions.assertTrue(ops.contains(WriteOperation.UPDATE));
        Assertions.assertTrue(ops.contains(WriteOperation.MERGE));
        // The append/overwrite pair must survive the addition.
        Assertions.assertTrue(ops.contains(WriteOperation.INSERT));
        Assertions.assertTrue(ops.contains(WriteOperation.OVERWRITE));
    }

    @Test
    public void primaryKeyTablesCarryAllThreeOpsAndSoDoDeletionVectorAppendOnly() {
        // A primary-key table carries all three ops: a delete cancels against the key, and an
        // UPDATE/MERGE arrives as an operation-tagged stream the writer maps to keyed upserts/deletes.
        for (WriteOperation op : new WriteOperation[] {
                WriteOperation.DELETE, WriteOperation.UPDATE, WriteOperation.MERGE}) {
            RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
            PaimonTableHandle pkHandle = handle(ops, Collections.singletonList("id"),
                    Collections.emptyMap());
            Assertions.assertDoesNotThrow(() -> metadata(ops, new RecordingConnectorContext())
                    .validateRowLevelDmlMode(null, pkHandle, op), op + " on a PK table must be allowed");
        }

        // An unaware-bucket append-only table WITH deletion vectors now carries all three ops too. Every
        // one removes the matched rows via the deletion vector; an UPDATE/MERGE additionally appends the
        // replacement rows in the same operation-tagged write. The removal is what gates the shape, so
        // UPDATE/MERGE need nothing beyond what DELETE needs — they pass the SAME gate.
        // MUTATION: re-adding an "Only DELETE" rejection here breaks append-only UPDATE/MERGE.
        for (WriteOperation op : new WriteOperation[] {
                WriteOperation.DELETE, WriteOperation.UPDATE, WriteOperation.MERGE}) {
            RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
            PaimonTableHandle h = handle(ops, Collections.emptyList(), deletionVectorsEnabled());
            Assertions.assertDoesNotThrow(() -> metadata(ops, new RecordingConnectorContext())
                            .validateRowLevelDmlMode(null, h, op),
                    op + " on an unaware-bucket deletion-vector append-only table must be allowed");
        }
    }

    // ==================== per-table shape gate ====================

    @Test
    public void primaryKeyTableIsDeletableWithoutAnyOption() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        PaimonTableHandle handle = handle(ops, Collections.singletonList("id"), Collections.emptyMap());

        // WHY: a keyed delete needs no table option at all — the merge engine cancels the DELETE record
        // against the key. Requiring deletion vectors here would reject the common CDC-lake table.
        Assertions.assertDoesNotThrow(() ->
                metadata(ops, ctx).validateRowLevelDmlMode(null, handle, WriteOperation.DELETE));
    }

    @Test
    public void appendOnlyShapeGateAdmitsExactlyWhatTheWriterCanCarry() {
        // Shape 1: unaware-bucket + deletion vectors -> DELETE allowed. (UPDATE/MERGE are rejected for
        // every shape before the table is loaded — pinned in updateAndMergeAreRejectedForEveryTableShape.)
        RecordingPaimonCatalogOps dvOps = new RecordingPaimonCatalogOps();
        PaimonTableHandle dvHandle = handle(dvOps, Collections.emptyList(), deletionVectorsEnabled());
        Assertions.assertDoesNotThrow(() -> metadata(dvOps, new RecordingConnectorContext())
                .validateRowLevelDmlMode(null, dvHandle, WriteOperation.DELETE));

        // Shape 2: no deletion vectors -> rejected, message names the option that fixes it.
        // WHY: this is a Paimon requirement (nowhere to record the removal); the message must be
        // actionable, not read as an unexplained Doris gap.
        RecordingPaimonCatalogOps plainOps = new RecordingPaimonCatalogOps();
        PaimonTableHandle plainHandle = handle(plainOps, Collections.emptyList(), Collections.emptyMap());
        DorisConnectorException noDv = Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata(plainOps, new RecordingConnectorContext())
                        .validateRowLevelDmlMode(null, plainHandle, WriteOperation.DELETE));
        Assertions.assertTrue(noDv.getMessage().contains("db1.t1"), noDv.getMessage());
        Assertions.assertTrue(noDv.getMessage().contains(CoreOptions.DELETION_VECTORS_ENABLED.key()),
                "the rejection must name the option that enables the delete: " + noDv.getMessage());
        Assertions.assertTrue(noDv.getMessage().contains("ALTER TABLE"),
                "the rejection must show the statement that fixes it: " + noDv.getMessage());

        // Shape 3: bucketed append (pinned bucket count) -> rejected even WITH deletion vectors. The
        // vector must be filed under the file's REAL bucket, which the locator does not carry yet;
        // mis-filing under bucket 0 would corrupt reads of the other buckets.
        // MUTATION: dropping the bucket check re-introduces exactly that mis-filing.
        Map<String, String> bucketed = deletionVectorsEnabled();
        bucketed.put(CoreOptions.BUCKET.key(), "4");
        RecordingPaimonCatalogOps bucketedOps = new RecordingPaimonCatalogOps();
        PaimonTableHandle bucketedHandle = handle(bucketedOps, Collections.emptyList(), bucketed);
        DorisConnectorException pinned = Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata(bucketedOps, new RecordingConnectorContext())
                        .validateRowLevelDmlMode(null, bucketedHandle, WriteOperation.DELETE));
        Assertions.assertTrue(pinned.getMessage().contains("unaware-bucket"), pinned.getMessage());
    }

    @Test
    public void nonRowLevelOperationsSkipTheShapeGateEntirely() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        PaimonTableHandle handle = handle(ops, Collections.emptyList(), Collections.emptyMap());

        // WHY: a plain INSERT/OVERWRITE into an append-only table is perfectly legal — the gate must fire
        // ONLY for row-level DML. MUTATION: dropping the op check at the top rejects every append-only
        // INSERT, which would break the connector's existing (and most common) write path.
        Assertions.assertDoesNotThrow(() ->
                metadata(ops, ctx).validateRowLevelDmlMode(null, handle, WriteOperation.INSERT));
        Assertions.assertDoesNotThrow(() ->
                metadata(ops, ctx).validateRowLevelDmlMode(null, handle, WriteOperation.OVERWRITE));
        Assertions.assertEquals(0, ctx.authCount,
                "a skipped gate must not even load the table");
    }

    // ==================== synthetic row-id column ====================

    @Test
    public void rowIdColumnIsDeclaredForEveryTableShape() {
        // WHY: fe-core's row-level plan builders inject the locator unconditionally (the plan shape is
        // shared with iceberg, where every table declares one), so a paimon table that declared none would
        // fail at bind time with an unresolved slot. The PK writer simply ignores the locator's value.
        // MUTATION: returning empty for PK tables breaks primary-key DELETE at bind time — invisible to
        // every connector-module test, which is exactly why this declaration is pinned here.
        PaimonWritePlanProvider provider = new PaimonWritePlanProvider(
                PaimonCatalogProperties.of(Collections.emptyMap()),
                new RecordingPaimonCatalogOps(), new RecordingConnectorContext());
        for (List<String> primaryKeys : Arrays.asList(
                Collections.singletonList("id"), Collections.<String>emptyList())) {
            PaimonTableHandle handle = new PaimonTableHandle(
                    "db1", "t1", Collections.emptyList(), primaryKeys);
            List<ConnectorColumn> columns = provider.getSyntheticWriteColumns(null, handle);
            Assertions.assertEquals(1, columns.size(), "primaryKeys=" + primaryKeys);
            ConnectorColumn rowId = columns.get(0);
            Assertions.assertFalse(rowId.isVisible(),
                    "the row-id column must never surface in SELECT/SHOW");
            // The STRUCT is what a deletion vector indexes: which file, and the ordinal within it.
            Assertions.assertEquals(Arrays.asList("file_path", "row_position"),
                    rowId.getType().getFieldNames());
        }
    }

    @Test
    public void rowIdColumnNameMatchesTheFeCoreConstant() {
        // WHY: fe-core binds the synthesized DELETE plan against a literal column name it declares itself
        // (it must not depend on a connector module), while the connector declares the column under its own
        // literal. A mismatch does not fail to compile — it fails at bind time with an unresolved slot, so
        // the contract is pinned here. Keep in sync with PaimonRowLevelDmlColumns.ROWID_COL.
        PaimonWritePlanProvider provider = new PaimonWritePlanProvider(
                PaimonCatalogProperties.of(Collections.emptyMap()),
                new RecordingPaimonCatalogOps(), new RecordingConnectorContext());
        PaimonTableHandle appendHandle = new PaimonTableHandle(
                "db1", "t2", Collections.emptyList(), Collections.emptyList());
        Assertions.assertEquals("__DORIS_PAIMON_ROWID_COL__",
                provider.getSyntheticWriteColumns(null, appendHandle).get(0).getName());
    }
}
