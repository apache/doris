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

import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.ddl.ConnectorColumnPath;
import org.apache.doris.connector.api.ddl.ConnectorColumnPosition;
import org.apache.doris.connector.iceberg.IcebergCatalogOps.CatalogBackedIcebergCatalogOps;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
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
 * End-to-end tests for {@link IcebergNestedColumnEvolution} (the #65329 dotted-path column-schema-change engine),
 * driven through the {@link CatalogBackedIcebergCatalogOps} nested seam methods
 * ({@code addNestedColumn}/{@code dropNestedColumn}/{@code renameNestedColumn}/{@code modifyNestedColumn}/
 * {@code modifyColumnComment}) against a REAL iceberg {@link InMemoryCatalog} (no Mockito) with STRUCT/ARRAY/MAP
 * columns. Proves the actual persisted schema after each build+commit, and that the validation guards fail loud.
 *
 * <p>Ported from the (deleted) fe-core {@code IcebergMetadataOpsValidationTest}, keeping its NESTED-schema
 * assertions but matched to the connector's own error-message text. Two upstream behaviors are enforced ONE LAYER
 * UP in {@code IcebergConnectorMetadata}, not the engine, so they are not exercised here: the nested {@code ADD
 * COLUMN} "must be nullable" / "DEFAULT and ON UPDATE" rejections (the engine always adds an OPTIONAL field via
 * {@code UpdateSchema.addColumn}). The "new nested struct field must be nullable" invariant is still covered here
 * via the engine-reachable complex-MODIFY append path ({@link IcebergComplexTypeDiff}).</p>
 */
public class IcebergNestedColumnEvolutionTest {

    private InMemoryCatalog catalog;
    private CatalogBackedIcebergCatalogOps ops;

    @BeforeEach
    public void setUp() {
        catalog = new InMemoryCatalog();
        catalog.initialize("test", Collections.emptyMap());
        ops = new CatalogBackedIcebergCatalogOps(catalog);
        ops.createDatabase("db1", Collections.emptyMap());
    }

    @AfterEach
    public void tearDown() throws Exception {
        catalog.close();
    }

    // ------------------------------------------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------------------------------------------

    private void createTable(String table, Schema schema) {
        ops.createTable("db1", table, schema, PartitionSpec.unpartitioned(), null,
                IcebergSchemaBuilder.buildTableProperties(Collections.emptyMap()));
    }

    private Schema reload(String table) {
        return ops.loadTable("db1", table).schema();
    }

    private static ConnectorColumnPath path(String... parts) {
        return ConnectorColumnPath.of(Arrays.asList(parts));
    }

    private static IcebergColumnChange change(String name, Type type, String comment, boolean nullable) {
        return new IcebergColumnChange(name, type, comment, null, nullable);
    }

    private static DorisConnectorException assertFailsLoud(org.junit.jupiter.api.function.Executable op,
            String expectedSubstring) {
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class, op);
        Assertions.assertTrue(ex.getMessage().contains(expectedSubstring),
                "expected message containing '" + expectedSubstring + "' but was '" + ex.getMessage() + "'");
        return ex;
    }

    // id INT (opt), s STRUCT{a INT "a doc", x INT} (opt), arr ARRAY<INT> (opt), m MAP<STRING,INT> (opt)
    private Schema flatNestedSchema() {
        return new Schema(
                Types.NestedField.optional(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "s", Types.StructType.of(
                        Types.NestedField.optional(3, "a", Types.IntegerType.get(), "a doc"),
                        Types.NestedField.optional(4, "x", Types.IntegerType.get()))),
                Types.NestedField.optional(5, "arr", Types.ListType.ofOptional(6, Types.IntegerType.get())),
                Types.NestedField.optional(7, "m", Types.MapType.ofOptional(
                        8, 9, Types.StringType.get(), Types.IntegerType.get())));
    }

    // info STRUCT{ metric INT "metric doc" (req), child STRUCT{value INT (req)} (req),
    //              big BIGINT (req), clear_me STRING "clear doc" (opt) } (req)
    private Schema requiredNestedSchema() {
        return new Schema(Types.NestedField.required(1, "info", Types.StructType.of(
                Types.NestedField.required(2, "metric", Types.IntegerType.get(), "metric doc"),
                Types.NestedField.required(3, "child", Types.StructType.of(
                        Types.NestedField.required(4, "value", Types.IntegerType.get()))),
                Types.NestedField.required(5, "big", Types.LongType.get()),
                Types.NestedField.optional(6, "clear_me", Types.StringType.get(), "clear doc"))));
    }

    // root STRUCT{ child STRUCT{ id INT (req, IDENTIFIER), value STRING (opt) } (req) } (req)
    private Schema nestedIdentifierSchema() {
        return new Schema(Collections.singletonList(
                Types.NestedField.required(1, "root", Types.StructType.of(
                        Types.NestedField.required(2, "child", Types.StructType.of(
                                Types.NestedField.required(3, "id", Types.IntegerType.get()),
                                Types.NestedField.optional(4, "value", Types.StringType.get())))))),
                Collections.singleton(3));
    }

    // ======================================================================================================
    // Group 1 — resolveColumnPath: struct field / array element / map value; reject map key / primitive / missing.
    // ======================================================================================================

    @Test
    public void testResolveNestedStructField() {
        // A dotted path descends a struct field (case-insensitive) and lands on the leaf NestedField.
        createTable("r_struct", flatNestedSchema());
        ops.modifyColumnComment("db1", "r_struct", path("s", "a"), "struct field comment");
        Types.NestedField a = reload("r_struct").findField("s").type().asStructType().field("a");
        Assertions.assertEquals("struct field comment", a.doc());
    }

    @Test
    public void testResolveArrayElement() {
        // A dotted path descends the ARRAY 'element' pseudo-field.
        createTable("r_arr", flatNestedSchema());
        ops.modifyNestedColumn("db1", "r_arr", path("arr", "element"),
                change("element", Types.LongType.get(), null, true), false, false, null);
        Assertions.assertEquals(Type.TypeID.LONG,
                reload("r_arr").findField("arr").type().asListType().elementType().typeId());
    }

    @Test
    public void testResolveMapValue() {
        // A dotted path descends the MAP 'value' pseudo-field.
        createTable("r_map", flatNestedSchema());
        ops.modifyNestedColumn("db1", "r_map", path("m", "value"),
                change("value", Types.LongType.get(), null, true), false, false, null);
        Assertions.assertEquals(Type.TypeID.LONG,
                reload("r_map").findField("m").type().asMapType().valueType().typeId());
    }

    @Test
    public void testResolveRejectsMapKey() {
        // Descending into a MAP 'key' pseudo-field is forbidden (iceberg cannot evolve a map key nested column).
        createTable("r_mapkey", flatNestedSchema());
        assertFailsLoud(() -> ops.modifyColumnComment("db1", "r_mapkey", path("m", "key"), "x"),
                "MAP key nested column");
    }

    @Test
    public void testResolveRejectsPrimitiveDescent() {
        // Descending under a primitive (top-level INT 'id') fails loud.
        createTable("r_prim", flatNestedSchema());
        assertFailsLoud(() -> ops.modifyColumnComment("db1", "r_prim", path("id", "x"), "x"),
                "Cannot resolve nested field under primitive column path");
    }

    @Test
    public void testResolveMissingPath() {
        // A path that does not exist fails loud.
        createTable("r_missing", flatNestedSchema());
        assertFailsLoud(() -> ops.modifyColumnComment("db1", "r_missing", path("s", "missing"), "x"),
                "Column path does not exist in Iceberg schema");
    }

    // ======================================================================================================
    // Group 2 — nested ADD: add a struct field; position FIRST / AFTER; case-insensitive sibling collision.
    // (The nested ADD "must be nullable" guard lives in IcebergConnectorMetadata; the engine's addColumn always
    //  adds an OPTIONAL field. The invariant is covered below via the complex-MODIFY append path.)
    // ======================================================================================================

    @Test
    public void testAddNestedStructField() {
        createTable("a_add", flatNestedSchema());
        ops.addNestedColumn("db1", "a_add", path("s", "b"),
                change("b", Types.StringType.get(), "the b", true), null);
        Types.StructType s = reload("a_add").findField("s").type().asStructType();
        Types.NestedField b = s.field("b");
        Assertions.assertNotNull(b);
        Assertions.assertEquals(Type.TypeID.STRING, b.type().typeId());
        Assertions.assertEquals("the b", b.doc());
        Assertions.assertTrue(b.isOptional());
        // Appended at the end of the parent struct by default.
        Assertions.assertEquals("b", s.fields().get(s.fields().size() - 1).name());
    }

    @Test
    public void testAddNestedFieldFirst() {
        createTable("a_first", flatNestedSchema());
        ops.addNestedColumn("db1", "a_first", path("s", "b"),
                change("b", Types.IntegerType.get(), null, true), ConnectorColumnPosition.FIRST);
        List<String> order = reload("a_first").findField("s").type().asStructType().fields()
                .stream().map(Types.NestedField::name).collect(Collectors.toList());
        Assertions.assertEquals("b", order.get(0));
    }

    @Test
    public void testAddNestedFieldAfter() {
        createTable("a_after", flatNestedSchema());
        ops.addNestedColumn("db1", "a_after", path("s", "b"),
                change("b", Types.IntegerType.get(), null, true), ConnectorColumnPosition.after("a"));
        List<String> order = reload("a_after").findField("s").type().asStructType().fields()
                .stream().map(Types.NestedField::name).collect(Collectors.toList());
        Assertions.assertEquals(Arrays.asList("a", "b"), order.subList(0, 2));
    }

    @Test
    public void testAddNestedCaseInsensitiveSiblingCollisionFailsLoud() {
        createTable("a_collide", flatNestedSchema());
        assertFailsLoud(() -> ops.addNestedColumn("db1", "a_collide", path("s", "A"),
                        change("A", Types.IntegerType.get(), null, true), null),
                "conflicts with existing Iceberg field");
        // Schema unchanged: no 'A' field was added.
        Assertions.assertEquals(2, reload("a_collide").findField("s").type().asStructType().fields().size());
    }

    @Test
    public void testModifyNestedStructAppendRequiredFieldFailsLoud() {
        // #65329 invariant: a newly appended nested struct field must be nullable. Reached here through the
        // engine-visible complex-MODIFY of the parent struct (IcebergComplexTypeDiff); the ADD-COLUMN-path
        // variant of this guard is enforced one layer up in IcebergConnectorMetadata.
        createTable("m_newreq", requiredNestedSchema());
        Type newChild = Types.StructType.of(
                Types.NestedField.required(40, "value", Types.IntegerType.get()),
                Types.NestedField.required(41, "extra", Types.IntegerType.get()));
        assertFailsLoud(() -> ops.modifyNestedColumn("db1", "m_newreq", path("info", "child"),
                        change("child", newChild, null, true), false, false, null),
                "must be nullable");
    }

    // ======================================================================================================
    // Group 3 — nested DROP / RENAME (+ identifier-field path fixup).
    // ======================================================================================================

    @Test
    public void testDropNestedStructField() {
        createTable("d_drop", flatNestedSchema());
        ops.dropNestedColumn("db1", "d_drop", path("s", "a"));
        Types.StructType s = reload("d_drop").findField("s").type().asStructType();
        Assertions.assertNull(s.field("a"));
        Assertions.assertNotNull(s.field("x"));
    }

    @Test
    public void testRenameNestedStructField() {
        createTable("d_rename", flatNestedSchema());
        ops.renameNestedColumn("db1", "d_rename", path("s", "a"), "renamed_a");
        Types.StructType s = reload("d_rename").findField("s").type().asStructType();
        Assertions.assertNull(s.field("a"));
        Assertions.assertNotNull(s.field("renamed_a"));
        Assertions.assertNotNull(s.field("x"));
    }

    @Test
    public void testRenameNestedCaseInsensitiveSiblingCollisionFailsLoud() {
        createTable("d_rcollide", flatNestedSchema());
        // Rename s.a -> X collides case-insensitively with the existing sibling s.x.
        assertFailsLoud(() -> ops.renameNestedColumn("db1", "d_rcollide", path("s", "a"), "X"),
                "conflicts with existing Iceberg field");
        Assertions.assertNotNull(reload("d_rcollide").findField("s").type().asStructType().field("a"));
    }

    @Test
    public void testRenamePreservesNestedIdentifierFieldPaths() {
        // The identifier field is the deeply-nested root.child.id (field id 3). Renaming a nested ANCESTOR must
        // preserve the identifier-field path (iceberg does not do this on its own) and keep the field id.
        createTable("d_ident", nestedIdentifierSchema());
        Assertions.assertEquals(Collections.singleton("root.child.id"),
                reload("d_ident").identifierFieldNames());

        ops.renameNestedColumn("db1", "d_ident", path("root", "child", "id"), "renamed_id");
        Assertions.assertEquals(Collections.singleton("root.child.renamed_id"),
                reload("d_ident").identifierFieldNames());

        ops.renameNestedColumn("db1", "d_ident", path("root", "child"), "renamed_child");
        Schema after = reload("d_ident");
        Assertions.assertEquals(Collections.singleton("root.renamed_child.renamed_id"),
                after.identifierFieldNames());
        Assertions.assertEquals(3, after.findField("root.renamed_child.renamed_id").fieldId());
    }

    // ======================================================================================================
    // Group 4 — nested MODIFY: primitive promotion allowed / disallowed; requiredness relaxation (explicit-only);
    // comment omit-preserve vs explicit clear; complex struct widen.
    // ======================================================================================================

    @Test
    public void testModifyNestedPrimitivePromotionAllowed() {
        createTable("m_promote", requiredNestedSchema());
        ops.modifyNestedColumn("db1", "m_promote", path("info", "metric"),
                change("metric", Types.LongType.get(), null, true), false, false, null);
        Types.NestedField metric = reload("m_promote").findField("info").type().asStructType().field("metric");
        Assertions.assertEquals(Type.TypeID.LONG, metric.type().typeId());
    }

    @Test
    public void testModifyNestedPrimitivePromotionDisallowedFailsLoud() {
        // BIGINT -> INT is not an iceberg-representable promotion.
        createTable("m_narrow", requiredNestedSchema());
        assertFailsLoud(() -> ops.modifyNestedColumn("db1", "m_narrow", path("info", "big"),
                        change("big", Types.IntegerType.get(), null, true), false, false, null),
                "Cannot change column type: info.big: long -> int");
        Assertions.assertEquals(Type.TypeID.LONG,
                reload("m_narrow").findField("info").type().asStructType().field("big").type().typeId());
    }

    @Test
    public void testModifyNestedRequirednessRelaxationExplicit() {
        // required -> optional only when nullability is EXPLICITLY specified (nullableSpecified=true).
        createTable("m_relax", requiredNestedSchema());
        Assertions.assertTrue(
                reload("m_relax").findField("info").type().asStructType().field("metric").isRequired());
        ops.modifyNestedColumn("db1", "m_relax", path("info", "metric"),
                change("metric", Types.IntegerType.get(), null, true), true, false, null);
        Assertions.assertTrue(
                reload("m_relax").findField("info").type().asStructType().field("metric").isOptional());
    }

    @Test
    public void testModifyNestedRequirednessOmitPreserves() {
        // nullableSpecified=false must NOT widen a required nested field, even while promoting its type.
        createTable("m_keepreq", requiredNestedSchema());
        ops.modifyNestedColumn("db1", "m_keepreq", path("info", "metric"),
                change("metric", Types.LongType.get(), null, true), false, false, null);
        Types.NestedField metric = reload("m_keepreq").findField("info").type().asStructType().field("metric");
        Assertions.assertTrue(metric.isRequired());
        Assertions.assertEquals(Type.TypeID.LONG, metric.type().typeId());
    }

    @Test
    public void testModifyNestedCommentOmitPreserves() {
        // commentSpecified=false must keep the field's current doc while the type is promoted.
        createTable("m_keepdoc", requiredNestedSchema());
        ops.modifyNestedColumn("db1", "m_keepdoc", path("info", "metric"),
                change("metric", Types.LongType.get(), null, true), false, false, null);
        Types.NestedField metric = reload("m_keepdoc").findField("info").type().asStructType().field("metric");
        Assertions.assertEquals("metric doc", metric.doc());
    }

    @Test
    public void testModifyNestedCommentClearExplicitEmpty() {
        // commentSpecified=true with "" clears the doc.
        createTable("m_cleardoc", requiredNestedSchema());
        ops.modifyNestedColumn("db1", "m_cleardoc", path("info", "clear_me"),
                change("clear_me", Types.StringType.get(), "", true), false, true, null);
        Types.NestedField clearMe = reload("m_cleardoc").findField("info").type().asStructType().field("clear_me");
        Assertions.assertTrue(clearMe.doc() == null || clearMe.doc().isEmpty(),
                "expected cleared doc but was '" + clearMe.doc() + "'");
    }

    @Test
    public void testModifyNestedComplexStructWidensChild() {
        // A nested field that is itself a STRUCT is diffed field-by-field (int->long promotion of its member).
        createTable("m_complex", requiredNestedSchema());
        Type newChild = Types.StructType.of(
                Types.NestedField.required(40, "value", Types.LongType.get()));
        ops.modifyNestedColumn("db1", "m_complex", path("info", "child"),
                change("child", newChild, null, true), false, false, null);
        Types.StructType child = reload("m_complex").findField("info").type().asStructType()
                .field("child").type().asStructType();
        Assertions.assertEquals(Type.TypeID.LONG, child.field("value").type().typeId());
        // Not widened to nullable (nullableSpecified=false), still required.
        Assertions.assertTrue(child.field("value").isRequired());
    }

    // info STRUCT{ payload STRUCT{ name STRING "name doc", count INT "count doc" } (opt) } (opt)
    private Schema commentStructSchema() {
        return new Schema(Types.NestedField.optional(1, "info", Types.StructType.of(
                Types.NestedField.optional(2, "payload", Types.StructType.of(
                        Types.NestedField.optional(3, "name", Types.StringType.get(), "name doc"),
                        Types.NestedField.optional(4, "count", Types.IntegerType.get(), "count doc"))))));
    }

    // A complex MODIFY carries the neutral source type (as production does via ConnectorColumnConverter) so the
    // diff can read each STRUCT field's commentSpecified: names[i]/types[i]/comments[i]/specified[i] are parallel.
    private static IcebergColumnChange structModify(String leafName, List<String> names,
            List<ConnectorType> types, List<String> comments, List<Boolean> specified) {
        List<Boolean> nullables = names.stream().map(n -> true).collect(Collectors.toList());
        ConnectorType conn = ConnectorType.structOf(names, types, nullables, comments, specified);
        // Build the iceberg new type from the SAME neutral type, exactly like IcebergConnectorMetadata.
        return new IcebergColumnChange(leafName, IcebergSchemaBuilder.buildColumnType(conn), null, null, true, conn);
    }

    @Test
    public void testModifyNestedStructSubfieldCommentOmitPreservesExplicitClears() {
        // Regression for the #65329 SPI-port gap (build 1004182): a complex MODIFY on a nested STRUCT that
        // OMITS a sub-field's COMMENT while changing its type must KEEP that sub-field's current doc, while an
        // explicit COMMENT '' on a sibling clears it. Mirrors
        // test_iceberg_nested_schema_evolution_spark_doris_interop line 102 (payload.count) + line 100 (name).
        createTable("m_doc", commentStructSchema());
        // name:STRING COMMENT '' (specified, clear) ; count:BIGINT (omitted, preserve) — count's type also changes.
        ops.modifyNestedColumn("db1", "m_doc", path("info", "payload"),
                structModify("payload",
                        Arrays.asList("name", "count"),
                        Arrays.asList(ConnectorType.of("STRING"), ConnectorType.of("BIGINT")),
                        Arrays.asList("", ""),
                        Arrays.asList(true, false)),
                false, false, null);
        Types.StructType payload = reload("m_doc").findField("info").type().asStructType()
                .field("payload").type().asStructType();
        // count: type promoted to LONG, doc PRESERVED because its COMMENT was omitted.
        Assertions.assertEquals(Type.TypeID.LONG, payload.field("count").type().typeId());
        Assertions.assertEquals("count doc", payload.field("count").doc());
        // name: doc CLEARED because COMMENT '' was explicitly specified.
        Assertions.assertTrue(payload.field("name").doc() == null || payload.field("name").doc().isEmpty(),
                "expected cleared name doc but was '" + payload.field("name").doc() + "'");
    }

    @Test
    public void testModifyNestedStructSiblingCommentOmitPreservedNoTypeChange() {
        // Sibling-clobber gap: in a complex MODIFY every sub-field must be re-listed; a sibling whose type is
        // UNCHANGED but whose COMMENT is omitted must keep its doc (must not be wiped to '').
        createTable("m_sib", commentStructSchema());
        // Both sub-fields omit COMMENT; only count's type changes (INT->BIGINT), name's type is unchanged.
        ops.modifyNestedColumn("db1", "m_sib", path("info", "payload"),
                structModify("payload",
                        Arrays.asList("name", "count"),
                        Arrays.asList(ConnectorType.of("STRING"), ConnectorType.of("BIGINT")),
                        Arrays.asList("", ""),
                        Arrays.asList(false, false)),
                false, false, null);
        Types.StructType payload = reload("m_sib").findField("info").type().asStructType()
                .field("payload").type().asStructType();
        Assertions.assertEquals("name doc", payload.field("name").doc());
        Assertions.assertEquals(Type.TypeID.LONG, payload.field("count").type().typeId());
        Assertions.assertEquals("count doc", payload.field("count").doc());
    }

    // ======================================================================================================
    // Group 5 — nested MODIFY COMMENT: set / clear; reject comment on a collection element / map value.
    // ======================================================================================================

    @Test
    public void testModifyCommentClearsNested() {
        createTable("c_clear", flatNestedSchema());
        ops.modifyColumnComment("db1", "c_clear", path("s", "a"), "");
        Types.NestedField a = reload("c_clear").findField("s").type().asStructType().field("a");
        Assertions.assertTrue(a.doc() == null || a.doc().isEmpty(),
                "expected cleared doc but was '" + a.doc() + "'");
    }

    @Test
    public void testModifyCommentRejectsArrayElement() {
        createTable("c_arr", flatNestedSchema());
        assertFailsLoud(() -> ops.modifyColumnComment("db1", "c_arr", path("arr", "element"), "x"),
                "Iceberg does not support comments on collection element or value fields: arr.element");
    }

    @Test
    public void testModifyCommentRejectsMapValue() {
        createTable("c_map", flatNestedSchema());
        assertFailsLoud(() -> ops.modifyColumnComment("db1", "c_map", path("m", "value"), "x"),
                "Iceberg does not support comments on collection element or value fields: m.value");
    }
}
