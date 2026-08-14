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

import org.apache.doris.connector.iceberg.IcebergCatalogOps.CatalogBackedIcebergCatalogOps;
import org.apache.doris.connector.spi.ConnectorColumn;
import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.ddl.ConnectorColumnPosition;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * End-to-end seam tests for the B2 column-evolution methods on {@link CatalogBackedIcebergCatalogOps}, against a
 * REAL iceberg {@link InMemoryCatalog} (no Mockito). Proves the {@code UpdateSchema} build+commit actually
 * mutates the persisted schema (add at FIRST/AFTER/end, drop, rename, modify type/comment/nullability, reorder)
 * and that the validation guards (optional&rarr;required, missing column) fail loud.
 */
public class CatalogBackedIcebergCatalogOpsColumnEvolutionTest {

    private InMemoryCatalog catalog;
    private CatalogBackedIcebergCatalogOps ops;

    @BeforeEach
    public void setUp() {
        catalog = new InMemoryCatalog();
        catalog.initialize("test", Collections.emptyMap());
        ops = new CatalogBackedIcebergCatalogOps(catalog);
        ops.createDatabase("db1", Collections.emptyMap());
        // id BIGINT (optional), val INT (optional), name VARCHAR (optional, doc "old"), req INT (required)
        Schema schema = IcebergSchemaBuilder.buildSchema(Arrays.asList(
                new ConnectorColumn("id", ConnectorType.of("BIGINT"), "", true, null, false),
                new ConnectorColumn("val", ConnectorType.of("INT"), "", true, null, false),
                new ConnectorColumn("name", ConnectorType.of("VARCHAR", 50, 0), "old", true, null, false),
                new ConnectorColumn("req", ConnectorType.of("INT"), "", false, null, false)));
        ops.createTable("db1", "t1", schema, PartitionSpec.unpartitioned(), null,
                IcebergSchemaBuilder.buildTableProperties(Collections.emptyMap()));
    }

    @AfterEach
    public void tearDown() throws Exception {
        catalog.close();
    }

    private Schema reload() {
        return ops.loadTable("db1", "t1").schema();
    }

    private List<String> columnOrder() {
        return reload().columns().stream().map(Types.NestedField::name).collect(Collectors.toList());
    }

    private static IcebergColumnChange change(String name, Type type, String comment, boolean nullable) {
        return new IcebergColumnChange(name, type, comment, null, nullable);
    }

    // ---------- addColumn ----------

    @Test
    public void testAddColumnAtEnd() {
        ops.addColumn("db1", "t1", change("age", Types.IntegerType.get(), "c", true), null);
        Assertions.assertNotNull(reload().findField("age"));
        Assertions.assertEquals("age", columnOrder().get(columnOrder().size() - 1));
    }

    @Test
    public void testAddColumnFirst() {
        ops.addColumn("db1", "t1", change("age", Types.IntegerType.get(), "c", true),
                ConnectorColumnPosition.FIRST);
        Assertions.assertEquals("age", columnOrder().get(0));
    }

    @Test
    public void testAddColumnAfter() {
        ops.addColumn("db1", "t1", change("age", Types.IntegerType.get(), "c", true),
                ConnectorColumnPosition.after("id"));
        Assertions.assertEquals(Arrays.asList("id", "age"), columnOrder().subList(0, 2));
    }

    // ---------- addColumns ----------

    @Test
    public void testAddColumns() {
        ops.addColumns("db1", "t1", Arrays.asList(
                change("a", Types.IntegerType.get(), "c", true),
                change("b", Types.StringType.get(), "c", true)));
        Assertions.assertNotNull(reload().findField("a"));
        Assertions.assertNotNull(reload().findField("b"));
    }

    // ---------- dropColumn / renameColumn ----------

    @Test
    public void testDropColumn() {
        ops.dropColumn("db1", "t1", "val");
        Assertions.assertNull(reload().findField("val"));
    }

    @Test
    public void testDropColumnUsedByHistoricalPartitionSpecFailsLoud() {
        Table table = ops.loadTable("db1", "t1");
        table.updateSpec().addField("val").commit();
        String partitionName = table.spec().fields().get(0).name();
        DataFile dataFile = DataFiles.builder(table.spec())
                .withPath("file:/warehouse/t1/data.parquet")
                .withFileSizeInBytes(1)
                .withRecordCount(1)
                .withPartitionPath(partitionName + "=1")
                .build();
        table.newAppend().appendFile(dataFile).commit();
        table.updateSpec().removeField(partitionName).commit();

        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> ops.dropColumn("db1", "t1", "val"));
        Assertions.assertTrue(ex.getMessage().contains("used by an old partition spec"), ex.getMessage());
        Assertions.assertNotNull(reload().findField("val"));
    }

    @Test
    public void testRenameColumn() {
        ops.renameColumn("db1", "t1", "name", "full_name");
        Assertions.assertNull(reload().findField("name"));
        Assertions.assertNotNull(reload().findField("full_name"));
    }

    // ---------- case-insensitive name collision on the flat ops (#65329 flat arm) ----------

    /**
     * Creates db1.mixed with Spark-style mixed-case names ({@code Id}, {@code Label},
     * {@code Info STRUCT<Metric:INT>}) — the fixture of the regression suite
     * {@code test_iceberg_nested_schema_evolution_spark_doris_interop}. Iceberg itself matches names
     * case-SENSITIVELY, so without the connector guards a Doris user could add a second {@code id} next to
     * {@code Id} and make the table unreadable through Doris (whose column names are case-insensitive).
     */
    private void createMixedCaseTable() {
        Schema schema = new Schema(
                Types.NestedField.optional(1, "Id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "Label", Types.StringType.get()),
                Types.NestedField.optional(3, "Info", Types.StructType.of(
                        Types.NestedField.optional(4, "Metric", Types.IntegerType.get()))));
        ops.createTable("db1", "mixed", schema, PartitionSpec.unpartitioned(), null,
                IcebergSchemaBuilder.buildTableProperties(Collections.emptyMap()));
    }

    @Test
    public void testAddColumnCaseInsensitiveCollisionFailsLoud() {
        createMixedCaseTable();
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> ops.addColumn("db1", "mixed", change("id", Types.StringType.get(), "", true), null));
        Assertions.assertTrue(ex.getMessage()
                        .contains("conflicts with existing Iceberg field 'Id' (case-insensitive)"),
                ex.getMessage());
        Assertions.assertEquals(3, reload("mixed").columns().size());
    }

    @Test
    public void testAddColumnsCaseInsensitiveCollisionFailsLoud() {
        createMixedCaseTable();
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> ops.addColumns("db1", "mixed", Arrays.asList(
                        change("ok_col", Types.StringType.get(), "", true),
                        change("id", Types.StringType.get(), "", true))));
        Assertions.assertTrue(ex.getMessage()
                        .contains("conflicts with existing Iceberg field 'Id' (case-insensitive)"),
                ex.getMessage());
        // Whole batch validated before anything is staged: the valid sibling must not be committed either.
        Assertions.assertEquals(3, reload("mixed").columns().size());
    }

    @Test
    public void testAddColumnsDuplicateRequestedNamesFailLoud() {
        createMixedCaseTable();
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> ops.addColumns("db1", "mixed", Arrays.asList(
                        change("new_field", Types.StringType.get(), "", true),
                        change("NEW_FIELD", Types.StringType.get(), "", true))));
        Assertions.assertTrue(ex.getMessage()
                        .contains("conflicts with another requested column (case-insensitive)"),
                ex.getMessage());
        Assertions.assertEquals(3, reload("mixed").columns().size());
    }

    @Test
    public void testRenameColumnCaseInsensitiveCollisionFailsLoud() {
        createMixedCaseTable();
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> ops.renameColumn("db1", "mixed", "label", "id"));
        Assertions.assertTrue(ex.getMessage()
                        .contains("conflicts with existing Iceberg field 'Id' (case-insensitive)"),
                ex.getMessage());
        Assertions.assertNotNull(reload("mixed").findField("Label"));
    }

    @Test
    public void testRenameColumnResolvesSourceNameCaseInsensitively() {
        createMixedCaseTable();
        // The user types the Doris-side (case-insensitive) name; it must resolve to the iceberg field 'Label'
        // rather than hitting iceberg's case-sensitive "Cannot rename missing column".
        ops.renameColumn("db1", "mixed", "label", "full_label");
        Assertions.assertNull(reload("mixed").findField("Label"));
        Assertions.assertNotNull(reload("mixed").findField("full_label"));
    }

    @Test
    public void testRenameColumnCaseOnlySelfRenameSucceeds() {
        createMixedCaseTable();
        // 'Label' -> 'label' collides with itself; the field-identity escape in the guard must let it through,
        // otherwise a pure re-casing of a column would be impossible.
        ops.renameColumn("db1", "mixed", "label", "label");
        Assertions.assertNotNull(reload("mixed").findField("label"));
        Assertions.assertEquals(3, reload("mixed").columns().size());
    }

    @Test
    public void testRenameColumnPreservesIdentifierFieldPath() {
        // Iceberg drops an identifier-field path when the field is renamed; the flat rename must restate it
        // (legacy IcebergMetadataOps.applyRenameColumn parity, already honoured by the nested arm).
        Schema schema = new Schema(
                Arrays.asList(
                        Types.NestedField.required(1, "Key", Types.IntegerType.get()),
                        Types.NestedField.optional(2, "v", Types.StringType.get())),
                Collections.singleton(1));
        ops.createTable("db1", "ident", schema, PartitionSpec.unpartitioned(), null,
                IcebergSchemaBuilder.buildTableProperties(Collections.emptyMap()));
        ops.renameColumn("db1", "ident", "key", "renamed_key");
        Schema after = reload("ident");
        Assertions.assertEquals(Collections.singleton(after.findField("renamed_key").fieldId()),
                after.identifierFieldIds());
    }

    @Test
    public void testAddColumnKeepsWorkingWhenNoCollision() {
        createMixedCaseTable();
        ops.addColumn("db1", "mixed", change("brand_new", Types.StringType.get(), "", true), null);
        Assertions.assertNotNull(reload("mixed").findField("brand_new"));
    }

    // ---------- modifyColumn ----------

    @Test
    public void testModifyColumnWidensType() {
        ops.modifyColumn("db1", "t1", change("val", Types.LongType.get(), "c", true), true, null);
        Assertions.assertEquals(Type.TypeID.LONG, reload().findField("val").type().typeId());
    }

    @Test
    public void testModifyColumnCommentOnly() {
        // VARCHAR maps to iceberg STRING; re-sending the same type with a new doc updates only the comment.
        ops.modifyColumn("db1", "t1", change("name", Types.StringType.get(), "new comment", true), true, null);
        Assertions.assertEquals("new comment", reload().findField("name").doc());
        Assertions.assertEquals(Type.TypeID.STRING, reload().findField("name").type().typeId());
    }

    @Test
    public void testModifyColumnOmittedCommentPreservesExistingDoc() {
        // #65329 omit-preserves parity — regression for iceberg_schema_change_ddl "after_no_comment": a MODIFY
        // that carries no COMMENT (commentSpecified=false) must keep the column's current doc instead of
        // clearing it, for BOTH a same-type modify and a widening one; a specified comment still overrides.
        // name starts with doc "old", val with doc "" (see setUp); both are optional, so pass nullable=true.
        ops.modifyColumn("db1", "t1", change("name", Types.StringType.get(), "", true), false, null);
        Assertions.assertEquals("old", reload().findField("name").doc(), "same-type omit must preserve doc");

        // Give val a doc, then widen INT -> LONG omitting the comment: the doc must survive the type change.
        ops.modifyColumn("db1", "t1", change("val", Types.IntegerType.get(), "vdoc", true), true, null);
        ops.modifyColumn("db1", "t1", change("val", Types.LongType.get(), "", true), false, null);
        Types.NestedField val = reload().findField("val");
        Assertions.assertEquals(Type.TypeID.LONG, val.type().typeId());
        Assertions.assertEquals("vdoc", val.doc(), "widening omit must preserve doc");

        // When the comment IS specified (commentSpecified=true), it still overrides the existing doc.
        ops.modifyColumn("db1", "t1", change("name", Types.StringType.get(), "brand new", true), true, null);
        Assertions.assertEquals("brand new", reload().findField("name").doc());
    }

    @Test
    public void testModifyColumnRequiredToOptional() {
        Assertions.assertFalse(reload().findField("req").isOptional());
        ops.modifyColumn("db1", "t1", change("req", Types.IntegerType.get(), null, true), true, null);
        Assertions.assertTrue(reload().findField("req").isOptional());
    }

    @Test
    public void testModifyColumnRepositions() {
        ops.modifyColumn("db1", "t1", change("name", Types.StringType.get(), "c", true),
                true, ConnectorColumnPosition.FIRST);
        Assertions.assertEquals("name", columnOrder().get(0));
    }

    @Test
    public void testModifyColumnOptionalToRequiredFailsLoud() {
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> ops.modifyColumn("db1", "t1", change("id", Types.LongType.get(), null, false), true, null));
        Assertions.assertTrue(ex.getMessage().contains("not null"));
        // schema unchanged: id stays optional.
        Assertions.assertTrue(reload().findField("id").isOptional());
    }

    @Test
    public void testModifyMissingColumnFailsLoud() {
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> ops.modifyColumn("db1", "t1", change("ghost", Types.IntegerType.get(), null, true), true, null));
        Assertions.assertTrue(ex.getMessage().contains("does not exist"));
    }

    // ---------- reorderColumns ----------

    @Test
    public void testReorderColumns() {
        ops.reorderColumns("db1", "t1", Arrays.asList("name", "req", "id", "val"));
        Assertions.assertEquals(Arrays.asList("name", "req", "id", "val"), columnOrder());
    }

    // ---------- modifyColumn: complex types (B2b) — diffs the new type against the current one ----------

    private Schema reload(String table) {
        return ops.loadTable("db1", table).schema();
    }

    /** Creates db1.<table> with a single column built from {@code col}. */
    private void createTable(String table, ConnectorColumn col) {
        ops.createTable("db1", table,
                IcebergSchemaBuilder.buildSchema(Collections.singletonList(col)),
                PartitionSpec.unpartitioned(), null,
                IcebergSchemaBuilder.buildTableProperties(Collections.emptyMap()));
    }

    /** Modifies db1.<table>.<colName> to the (top-level nullable) complex {@code newType}. */
    private void modifyComplex(String table, String colName, ConnectorType newType, boolean topNullable) {
        ops.modifyColumn("db1", table,
                new IcebergColumnChange(colName, IcebergSchemaBuilder.buildColumnType(newType), null, null,
                        topNullable), true, null);
    }

    private static ConnectorType structType(List<String> names, List<ConnectorType> types,
            List<Boolean> nullable, List<String> comments) {
        return ConnectorType.structOf(names, types, nullable, comments);
    }

    @Test
    public void testModifyStructAddsNullableField() {
        createTable("s_add", new ConnectorColumn("st",
                structType(Arrays.asList("a"), Arrays.asList(ConnectorType.of("INT")),
                        Arrays.asList(true), Arrays.asList((String) null)), "", true, null, false));
        modifyComplex("s_add", "st",
                structType(Arrays.asList("a", "b"),
                        Arrays.asList(ConnectorType.of("INT"), ConnectorType.of("STRING")),
                        Arrays.asList(true, true), Arrays.asList(null, "the b")), true);
        Types.StructType st = reload("s_add").findField("st").type().asStructType();
        Assertions.assertEquals(2, st.fields().size());
        Assertions.assertEquals("b", st.fields().get(1).name());
        Assertions.assertEquals(Type.TypeID.STRING, st.fields().get(1).type().typeId());
        Assertions.assertTrue(st.fields().get(1).isOptional());
        Assertions.assertEquals("the b", st.fields().get(1).doc());
    }

    @Test
    public void testModifyStructWidensFieldTypeAndComment() {
        createTable("s_widen", new ConnectorColumn("st",
                structType(Arrays.asList("a"), Arrays.asList(ConnectorType.of("INT")),
                        Arrays.asList(true), Arrays.asList("old")), "", true, null, false));
        modifyComplex("s_widen", "st",
                structType(Arrays.asList("a"), Arrays.asList(ConnectorType.of("BIGINT")),
                        Arrays.asList(true), Arrays.asList("new")), true);
        Types.NestedField a = reload("s_widen").findField("st").type().asStructType().fields().get(0);
        Assertions.assertEquals(Type.TypeID.LONG, a.type().typeId());
        Assertions.assertEquals("new", a.doc());
    }

    @Test
    public void testModifyStructFieldWidensNotNullToNullable() {
        createTable("s_null", new ConnectorColumn("st",
                structType(Arrays.asList("a"), Arrays.asList(ConnectorType.of("INT")),
                        Arrays.asList(false), Arrays.asList((String) null)), "", true, null, false));
        Assertions.assertTrue(reload("s_null").findField("st").type().asStructType().fields().get(0).isRequired());
        modifyComplex("s_null", "st",
                structType(Arrays.asList("a"), Arrays.asList(ConnectorType.of("INT")),
                        Arrays.asList(true), Arrays.asList((String) null)), true);
        Assertions.assertTrue(reload("s_null").findField("st").type().asStructType().fields().get(0).isOptional());
    }

    @Test
    public void testModifyStructNarrowToNotNullFailsLoud() {
        createTable("s_narrow", new ConnectorColumn("st",
                structType(Arrays.asList("a"), Arrays.asList(ConnectorType.of("INT")),
                        Arrays.asList(true), Arrays.asList((String) null)), "", true, null, false));
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> modifyComplex("s_narrow", "st",
                        structType(Arrays.asList("a"), Arrays.asList(ConnectorType.of("INT")),
                                Arrays.asList(false), Arrays.asList((String) null)), true));
        Assertions.assertTrue(ex.getMessage().contains("not null"));
        Assertions.assertTrue(reload("s_narrow").findField("st").type().asStructType().fields().get(0).isOptional());
    }

    @Test
    public void testModifyStructReduceFieldsFailsLoud() {
        createTable("s_reduce", new ConnectorColumn("st",
                structType(Arrays.asList("a", "b"),
                        Arrays.asList(ConnectorType.of("INT"), ConnectorType.of("STRING")),
                        Arrays.asList(true, true), Arrays.asList(null, null)), "", true, null, false));
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> modifyComplex("s_reduce", "st",
                        structType(Arrays.asList("a"), Arrays.asList(ConnectorType.of("INT")),
                                Arrays.asList(true), Arrays.asList((String) null)), true));
        Assertions.assertTrue(ex.getMessage().contains("reduce"));
        Assertions.assertEquals(2, reload("s_reduce").findField("st").type().asStructType().fields().size());
    }

    @Test
    public void testModifyStructRenameFieldFailsLoud() {
        createTable("s_rename", new ConnectorColumn("st",
                structType(Arrays.asList("a"), Arrays.asList(ConnectorType.of("INT")),
                        Arrays.asList(true), Arrays.asList((String) null)), "", true, null, false));
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> modifyComplex("s_rename", "st",
                        structType(Arrays.asList("c"), Arrays.asList(ConnectorType.of("INT")),
                                Arrays.asList(true), Arrays.asList((String) null)), true));
        Assertions.assertTrue(ex.getMessage().contains("rename struct field"));
    }

    @Test
    public void testModifyStructNewFieldNotNullableFailsLoud() {
        createTable("s_newreq", new ConnectorColumn("st",
                structType(Arrays.asList("a"), Arrays.asList(ConnectorType.of("INT")),
                        Arrays.asList(true), Arrays.asList((String) null)), "", true, null, false));
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> modifyComplex("s_newreq", "st",
                        structType(Arrays.asList("a", "b"),
                                Arrays.asList(ConnectorType.of("INT"), ConnectorType.of("STRING")),
                                Arrays.asList(true, false), Arrays.asList(null, null)), true));
        Assertions.assertTrue(ex.getMessage().contains("must be nullable"));
    }

    @Test
    public void testModifyNestedDecimalPrecisionFailsLoud() {
        // Legacy parity: a nested primitive change is restricted to int->long / float->double / exact; a
        // DECIMAL precision change inside a struct is rejected (checkSupportSchemaChangeForNestedPrimitive).
        createTable("s_dec", new ConnectorColumn("st",
                structType(Arrays.asList("a"), Arrays.asList(ConnectorType.of("DECIMALV3", 10, 2)),
                        Arrays.asList(true), Arrays.asList((String) null)), "", true, null, false));
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> modifyComplex("s_dec", "st",
                        structType(Arrays.asList("a"), Arrays.asList(ConnectorType.of("DECIMALV3", 20, 2)),
                                Arrays.asList(true), Arrays.asList((String) null)), true));
        Assertions.assertTrue(ex.getMessage().contains("nested"));
    }

    @Test
    public void testModifyArrayElementWidens() {
        createTable("a_widen", new ConnectorColumn("arr",
                ConnectorType.arrayOf(ConnectorType.of("INT")), "", true, null, false));
        modifyComplex("a_widen", "arr", ConnectorType.arrayOf(ConnectorType.of("BIGINT")), true);
        Assertions.assertEquals(Type.TypeID.LONG,
                reload("a_widen").findField("arr").type().asListType().elementType().typeId());
    }

    @Test
    public void testModifyMapValueWidens() {
        createTable("m_widen", new ConnectorColumn("m",
                ConnectorType.mapOf(ConnectorType.of("STRING"), ConnectorType.of("INT")), "", true, null, false));
        modifyComplex("m_widen", "m",
                ConnectorType.mapOf(ConnectorType.of("STRING"), ConnectorType.of("BIGINT")), true);
        Assertions.assertEquals(Type.TypeID.LONG,
                reload("m_widen").findField("m").type().asMapType().valueType().typeId());
    }

    @Test
    public void testModifyMapKeyChangeFailsLoud() {
        createTable("m_key", new ConnectorColumn("m",
                ConnectorType.mapOf(ConnectorType.of("STRING"), ConnectorType.of("INT")), "", true, null, false));
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> modifyComplex("m_key", "m",
                        ConnectorType.mapOf(ConnectorType.of("BIGINT"), ConnectorType.of("INT")), true));
        Assertions.assertTrue(ex.getMessage().contains("MAP key"));
    }

    @Test
    public void testModifyComplexCategoryMismatchFailsLoud() {
        createTable("c_cat", new ConnectorColumn("st",
                structType(Arrays.asList("a"), Arrays.asList(ConnectorType.of("INT")),
                        Arrays.asList(true), Arrays.asList((String) null)), "", true, null, false));
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> modifyComplex("c_cat", "st", ConnectorType.arrayOf(ConnectorType.of("INT")), true));
        Assertions.assertTrue(ex.getMessage().contains("category"));
    }

    @Test
    public void testModifyComplexToPrimitiveFailsLoud() {
        // Legacy parity (regression for iceberg_schema_change_ddl "MODIFY COLUMN address STRING"): a top-level
        // STRUCT/ARRAY/MAP cannot be changed to a primitive; the seam rejects it with a clean message before
        // iceberg's updateColumn leaks a raw struct<...> type-diff error.
        createTable("c2p", new ConnectorColumn("st",
                structType(Arrays.asList("a"), Arrays.asList(ConnectorType.of("INT")),
                        Arrays.asList(true), Arrays.asList((String) null)), "", true, null, false));
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> modifyComplex("c2p", "st", ConnectorType.of("STRING"), true));
        Assertions.assertTrue(ex.getMessage().contains("Modify column type from complex to primitive is not"
                + " supported"));
        // schema unchanged: st stays a struct.
        Assertions.assertTrue(reload("c2p").findField("st").type().isStructType());
    }
}
