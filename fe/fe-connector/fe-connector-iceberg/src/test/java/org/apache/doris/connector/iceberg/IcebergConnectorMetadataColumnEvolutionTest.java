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
import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.ddl.ConnectorColumnPosition;

import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Behavior tests for the B2 column-evolution overrides on {@link IcebergConnectorMetadata}, driven through the
 * {@link RecordingIcebergCatalogOps} seam + {@link RecordingConnectorContext} (no live catalog, no Mockito).
 * Asserts that each op builds the neutral column PURELY then runs the seam INSIDE the auth context, that the
 * neutral position is forwarded, and that the pre-remote parity guards (non-nullable add, aggregated/auto-inc
 * column, complex-type modify, empty reorder) fail loud BEFORE the seam runs.
 */
public class IcebergConnectorMetadataColumnEvolutionTest {

    private static final IcebergTableHandle HANDLE = new IcebergTableHandle("db1", "t1");

    private static Map<String, String> props() {
        Map<String, String> p = new HashMap<>();
        p.put(IcebergConnectorProperties.ICEBERG_CATALOG_TYPE, IcebergConnectorProperties.TYPE_REST);
        return p;
    }

    private static IcebergConnectorMetadata metadata(RecordingIcebergCatalogOps ops, RecordingConnectorContext ctx) {
        return new IcebergConnectorMetadata(ops, props(), ctx);
    }

    private static ConnectorColumn col(String name, String type) {
        return new ConnectorColumn(name, ConnectorType.of(type), "c", true, null, false);
    }

    /** A real iceberg table {@code db1.t1} whose {@code arr} column is {@code ARRAY<INT>} (the seam load the
     * nested-modify parity guard reads the current type from). */
    private static Table tableWithArrayIntColumn() {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("test", Collections.emptyMap());
        catalog.createNamespace(Namespace.of("db1"));
        Schema schema = new Schema(
                Types.NestedField.optional(1, "arr", Types.ListType.ofOptional(2, Types.IntegerType.get())));
        return catalog.createTable(TableIdentifier.of("db1", "t1"), schema);
    }

    /** A real iceberg table {@code db1.t1} whose {@code s} column is {@code STRUCT<a:INT>}. */
    private static Table tableWithStructIntColumn() {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("test", Collections.emptyMap());
        catalog.createNamespace(Namespace.of("db1"));
        Schema schema = new Schema(
                Types.NestedField.optional(1, "s", Types.StructType.of(
                        Types.NestedField.optional(2, "a", Types.IntegerType.get()))));
        return catalog.createTable(TableIdentifier.of("db1", "t1"), schema);
    }

    // ---------- addColumn ----------

    @Test
    public void testAddColumnBuildsTypeAndIsAuthWrapped() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        metadata(ops, ctx).addColumn(null, HANDLE, col("age", "INT"), ConnectorColumnPosition.after("id"));
        Assertions.assertEquals(Collections.singletonList("addColumn:db1.t1:age"), ops.log);
        Assertions.assertEquals("age", ops.lastAddColumn.getName());
        Assertions.assertEquals(Type.TypeID.INTEGER, ops.lastAddColumn.getType().typeId());
        Assertions.assertEquals("c", ops.lastAddColumn.getComment());
        Assertions.assertFalse(ops.lastAddColumnPos.isFirst());
        Assertions.assertEquals("id", ops.lastAddColumnPos.getAfterColumn());
        Assertions.assertEquals(1, ctx.authCount, "addColumn must run inside executeAuthenticated");
    }

    @Test
    public void testAddColumnNullPositionForwardedAsNull() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        metadata(ops, ctx).addColumn(null, HANDLE, col("age", "INT"), null);
        Assertions.assertNull(ops.lastAddColumnPos);
    }

    @Test
    public void testAddColumnDefaultLiteralParsed() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        ConnectorColumn withDefault = new ConnectorColumn("n", ConnectorType.of("INT"), "", true, "42", false);
        metadata(ops, ctx).addColumn(null, HANDLE, withDefault, null);
        Assertions.assertNotNull(ops.lastAddColumn.getDefaultValue());
        Assertions.assertEquals(42, ops.lastAddColumn.getDefaultValue().value());
    }

    @Test
    public void testAddNonNullableColumnFailsBeforeRemote() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        ConnectorColumn notNull = new ConnectorColumn("age", ConnectorType.of("INT"), "", false, null, false);
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata(ops, ctx).addColumn(null, HANDLE, notNull, null));
        Assertions.assertTrue(ex.getMessage().contains("non-nullable"));
        Assertions.assertTrue(ops.log.isEmpty());
        Assertions.assertEquals(0, ctx.authCount);
    }

    @Test
    public void testAddAggregatedColumnFailsBeforeRemote() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        // isKey=false, isAutoInc=false, isAggregated=true
        ConnectorColumn agg = new ConnectorColumn("s", ConnectorType.of("INT"), "", true, null, false, false, true);
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata(ops, ctx).addColumn(null, HANDLE, agg, null));
        Assertions.assertTrue(ex.getMessage().contains("aggregation"));
        Assertions.assertTrue(ops.log.isEmpty());
    }

    @Test
    public void testAddAutoIncColumnFailsBeforeRemote() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        // isKey=false, isAutoInc=true
        ConnectorColumn autoInc = new ConnectorColumn("s", ConnectorType.of("INT"), "", true, null, false, true);
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata(ops, ctx).addColumn(null, HANDLE, autoInc, null));
        Assertions.assertTrue(ex.getMessage().contains("auto incremental"));
        Assertions.assertTrue(ops.log.isEmpty());
    }

    @Test
    public void testAddColumnAuthFailureWraps() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        ctx.failAuth = true;
        Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata(ops, ctx).addColumn(null, HANDLE, col("age", "INT"), null));
        Assertions.assertTrue(ops.log.isEmpty());
    }

    // ---------- addColumns ----------

    @Test
    public void testAddColumnsBuildsAllAndIsAuthWrapped() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        metadata(ops, ctx).addColumns(null, HANDLE, Arrays.asList(col("a", "INT"), col("b", "STRING")));
        Assertions.assertEquals(Collections.singletonList("addColumns:db1.t1:2"), ops.log);
        Assertions.assertEquals(2, ops.lastAddColumns.size());
        Assertions.assertEquals("a", ops.lastAddColumns.get(0).getName());
        Assertions.assertEquals("b", ops.lastAddColumns.get(1).getName());
        Assertions.assertEquals(1, ctx.authCount);
    }

    @Test
    public void testAddColumnsRejectsNonNullableMember() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        ConnectorColumn notNull = new ConnectorColumn("b", ConnectorType.of("INT"), "", false, null, false);
        Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata(ops, ctx).addColumns(null, HANDLE, Arrays.asList(col("a", "INT"), notNull)));
        Assertions.assertTrue(ops.log.isEmpty());
    }

    // ---------- dropColumn / renameColumn ----------

    @Test
    public void testDropColumnRoutesAndIsAuthWrapped() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        metadata(ops, ctx).dropColumn(null, HANDLE, "age");
        Assertions.assertEquals(Collections.singletonList("dropColumn:db1.t1:age"), ops.log);
        Assertions.assertEquals("age", ops.lastDropColumn);
        Assertions.assertEquals(1, ctx.authCount);
    }

    @Test
    public void testRenameColumnRoutesAndIsAuthWrapped() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        metadata(ops, ctx).renameColumn(null, HANDLE, "old", "new");
        Assertions.assertEquals(Collections.singletonList("renameColumn:db1.t1:old->new"), ops.log);
        Assertions.assertEquals("old", ops.lastRenameColumnOld);
        Assertions.assertEquals("new", ops.lastRenameColumnNew);
        Assertions.assertEquals(1, ctx.authCount);
    }

    // ---------- modifyColumn ----------

    @Test
    public void testModifyScalarColumnBuildsTypeAndIsAuthWrapped() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        metadata(ops, ctx).modifyColumn(null, HANDLE, col("age", "BIGINT"), ConnectorColumnPosition.FIRST);
        Assertions.assertEquals(Collections.singletonList("modifyColumn:db1.t1:age"), ops.log);
        Assertions.assertEquals(Type.TypeID.LONG, ops.lastModifyColumn.getType().typeId());
        Assertions.assertTrue(ops.lastModifyColumnPos.isFirst());
        Assertions.assertEquals(1, ctx.authCount);
    }

    @Test
    public void testModifyScalarColumnThreadsCommentSpecifiedToSeam() {
        // #65329: the flat modify must forward the column's isCommentSpecified() flag to the seam, so an
        // omitted COMMENT preserves the existing doc (the preserve behavior itself is proven end-to-end in
        // CatalogBackedIcebergCatalogOpsColumnEvolutionTest against a real InMemoryCatalog).
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        // col(...) builds commentSpecified=false (COMMENT omitted).
        metadata(ops, ctx).modifyColumn(null, HANDLE, col("age", "BIGINT"), null);
        Assertions.assertFalse(ops.lastModifyCommentSpecified);
        // A column carrying a specified comment forwards true.
        metadata(ops, ctx).modifyColumn(null, HANDLE, col("age", "BIGINT").withSpecified(false, true), null);
        Assertions.assertTrue(ops.lastModifyCommentSpecified);
    }

    @Test
    public void testModifyComplexColumnBuildsTreeAndIsAuthWrapped() {
        // B2b: a complex modify now routes to the seam carrying the FULL new complex iceberg type
        // (built PURELY outside auth); the seam diffs it against the current schema.
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        ConnectorColumn arr = new ConnectorColumn("arr",
                ConnectorType.arrayOf(ConnectorType.of("INT")), "", true, null, false);
        metadata(ops, ctx).modifyColumn(null, HANDLE, arr, null);
        Assertions.assertEquals(Collections.singletonList("modifyColumn:db1.t1:arr"), ops.log);
        Assertions.assertEquals(Type.TypeID.LIST, ops.lastModifyColumn.getType().typeId());
        Assertions.assertEquals(Type.TypeID.INTEGER,
                ops.lastModifyColumn.getType().asListType().elementType().typeId());
        Assertions.assertEquals(1, ctx.authCount, "modifyColumn must run inside executeAuthenticated");
    }

    @Test
    public void testModifyComplexColumnNarrowToUnrepresentableRestoresLegacyMessage() {
        // ARRAY<INT> -> ARRAY<SMALLINT>: iceberg has no SMALLINT, so the eager type build throws the generic
        // "Unsupported type for Iceberg: SMALLINT". The connector must instead restore the legacy
        // "Cannot change int to smallint in nested types" (validated against the current type) so the green e2e
        // test_iceberg_schema_change_complex_types assertion survives the flip. MUTATION: dropping the upgrade
        // -> the message reverts and this goes red.
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.table = tableWithArrayIntColumn();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        ConnectorColumn arr = new ConnectorColumn("arr",
                ConnectorType.arrayOf(ConnectorType.of("SMALLINT")), "", true, null, false);
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata(ops, ctx).modifyColumn(null, HANDLE, arr, null));
        Assertions.assertEquals("Cannot change int to smallint in nested types", ex.getMessage());
        // the remote modify never ran (the build failed first); only the current type was loaded for the message.
        Assertions.assertFalse(ops.log.contains("modifyColumn:db1.t1:arr"),
                "the seam modify must not run when the nested type is unrepresentable");
    }

    @Test
    public void testModifyStructFieldNarrowToUnrepresentableRestoresLegacyMessage() {
        // STRUCT<a:INT> -> STRUCT<a:SMALLINT>: the struct branch of the walk must reach the nested int->smallint
        // leaf and restore the legacy message (covers the STRUCT path, not just LIST).
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.table = tableWithStructIntColumn();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        ConnectorColumn struct = new ConnectorColumn("s",
                ConnectorType.structOf(Collections.singletonList("a"),
                        Collections.singletonList(ConnectorType.of("SMALLINT"))), "", true, null, false);
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata(ops, ctx).modifyColumn(null, HANDLE, struct, null));
        Assertions.assertEquals("Cannot change int to smallint in nested types", ex.getMessage());
    }

    @Test
    public void testModifyScalarColumnToUnrepresentableKeepsBuildError() {
        // A TOP-LEVEL (non-nested) modify to an iceberg-unrepresentable type keeps the generic build error: the
        // nested-narrowing upgrade applies ONLY to complex types (legacy had no "in nested types" message for a
        // scalar). Proves the isComplexType early-return in upgradeNestedModifyError and that no table is loaded.
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata(ops, ctx).modifyColumn(null, HANDLE, col("c", "SMALLINT"), null));
        Assertions.assertEquals("Unsupported type for Iceberg: SMALLINT", ex.getMessage());
        Assertions.assertTrue(ops.log.isEmpty(), "a scalar build failure must not load the table");
    }

    @Test
    public void testModifyComplexColumnWithDefaultFailsBeforeRemote() {
        // Legacy parity (validateForModifyComplexColumn): a complex modify may only carry a NULL default.
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        ConnectorColumn arr = new ConnectorColumn("arr",
                ConnectorType.arrayOf(ConnectorType.of("INT")), "", true, "1", false);
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata(ops, ctx).modifyColumn(null, HANDLE, arr, null));
        Assertions.assertTrue(ex.getMessage().contains("Complex type default"));
        Assertions.assertTrue(ops.log.isEmpty());
        Assertions.assertEquals(0, ctx.authCount);
    }

    @Test
    public void testModifyAggregatedColumnFailsBeforeRemote() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        ConnectorColumn agg = new ConnectorColumn("s", ConnectorType.of("INT"), "", true, null, false, false, true);
        Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata(ops, ctx).modifyColumn(null, HANDLE, agg, null));
        Assertions.assertTrue(ops.log.isEmpty());
    }

    // ---------- reorderColumns ----------

    @Test
    public void testReorderColumnsRoutesAndIsAuthWrapped() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        metadata(ops, ctx).reorderColumns(null, HANDLE, Arrays.asList("b", "a"));
        Assertions.assertEquals(Collections.singletonList("reorderColumns:db1.t1:[b, a]"), ops.log);
        Assertions.assertEquals(Arrays.asList("b", "a"), ops.lastReorder);
        Assertions.assertEquals(1, ctx.authCount);
    }

    @Test
    public void testReorderColumnsEmptyFailsBeforeRemote() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata(ops, ctx).reorderColumns(null, HANDLE, Collections.emptyList()));
        Assertions.assertTrue(ex.getMessage().contains("empty"));
        Assertions.assertTrue(ops.log.isEmpty());
        Assertions.assertEquals(0, ctx.authCount);
    }
}
