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

package org.apache.doris.datasource.iceberg;

import org.apache.doris.analysis.ColumnPath;
import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.StructField;
import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.Type;
import org.apache.doris.catalog.info.ColumnPosition;
import org.apache.doris.common.UserException;
import org.apache.doris.common.security.authentication.ExecutionAuthenticator;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalTable;

import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

public class IcebergMetadataOpsValidationTest {

    private IcebergMetadataOps ops;
    private ExternalCatalog dorisCatalog;
    private Method validateForModifyColumnMethod;
    private Method validateForModifyComplexColumnMethod;

    @Before
    public void setUp() throws Exception {
        dorisCatalog = Mockito.mock(ExternalCatalog.class);
        Catalog icebergCatalog = Mockito.mock(Catalog.class,
                Mockito.withSettings().extraInterfaces(SupportsNamespaces.class));
        Mockito.when(dorisCatalog.getExecutionAuthenticator()).thenReturn(new ExecutionAuthenticator() {
        });
        Mockito.when(dorisCatalog.getProperties()).thenReturn(Collections.emptyMap());
        Mockito.doReturn(Optional.empty()).when(dorisCatalog).getDbForReplay(Mockito.anyString());
        ops = new IcebergMetadataOps(dorisCatalog, icebergCatalog);

        validateForModifyColumnMethod = IcebergMetadataOps.class.getDeclaredMethod(
                "validateForModifyColumn", Column.class, NestedField.class);
        validateForModifyColumnMethod.setAccessible(true);
        validateForModifyComplexColumnMethod = IcebergMetadataOps.class.getDeclaredMethod(
                "validateForModifyComplexColumn", Column.class, NestedField.class);
        validateForModifyComplexColumnMethod.setAccessible(true);
    }

    @Test
    public void testValidateForModifyColumnRejectsComplexType() {
        Column column = new Column("arr_i", ArrayType.create(Type.INT, true), true);
        NestedField currentCol = Types.NestedField.required(1, "arr_i", Types.IntegerType.get());
        assertUserException(() -> invokeValidateForModifyColumn(column, currentCol),
                "Modify column type to non-primitive type is not supported");
    }

    @Test
    public void testValidateForModifyColumnRejectsNullableToNotNull() {
        Column column = new Column("int_col", Type.INT, false);
        NestedField currentCol = Types.NestedField.optional(1, "int_col", Types.IntegerType.get());
        assertUserException(() -> invokeValidateForModifyColumn(column, currentCol),
                "Can not change nullable column int_col to not null");
    }

    @Test
    public void testValidateForModifyColumnSuccess() throws Throwable {
        Column column = new Column("int_col", Type.INT, true);
        NestedField currentCol = Types.NestedField.required(1, "int_col", Types.IntegerType.get());
        invokeValidateForModifyColumn(column, currentCol);
    }

    @Test
    public void testValidateForModifyComplexColumnRejectsPrimitiveType() {
        Column column = new Column("arr_i", Type.INT, true);
        NestedField currentCol = Types.NestedField.required(1, "arr_i",
                Types.ListType.ofOptional(2, Types.IntegerType.get()));
        assertUserException(() -> invokeValidateForModifyComplexColumn(column, currentCol),
                "Modify column type to non-complex type is not supported");
    }

    @Test
    public void testValidateForModifyComplexColumnRejectsIncompatibleNestedType() {
        Column column = new Column("arr_i", ArrayType.create(Type.SMALLINT, true), true);
        NestedField currentCol = Types.NestedField.required(1, "arr_i",
                Types.ListType.ofOptional(2, Types.IntegerType.get()));
        assertUserException(() -> invokeValidateForModifyComplexColumn(column, currentCol),
                "Cannot change int to smallint in nested types");
    }

    @Test
    public void testValidateForModifyComplexColumnAllowsNestedDecimalPrecisionPromotion() throws Throwable {
        Column column = new Column("struct_col",
                new StructType(new StructField("d", ScalarType.createDecimalV3Type(10, 3))), true);
        NestedField currentCol = Types.NestedField.required(1, "struct_col",
                Types.StructType.of(Types.NestedField.optional(2, "d",
                        Types.DecimalType.of(5, 3))));
        invokeValidateForModifyComplexColumn(column, currentCol);
    }

    @Test
    public void testValidateForModifyComplexColumnRejectsNestedDecimalPrecisionNarrowing() {
        Column column = new Column("struct_col",
                new StructType(new StructField("d", ScalarType.createDecimalV3Type(5, 3))), true);
        NestedField currentCol = Types.NestedField.required(1, "struct_col",
                Types.StructType.of(Types.NestedField.optional(2, "d",
                        Types.DecimalType.of(10, 3))));
        assertUserException(() -> invokeValidateForModifyComplexColumn(column, currentCol),
                "Cannot change decimalv3(10,3) to decimalv3(5,3) in nested types");
    }

    @Test
    public void testValidateForModifyComplexColumnRejectsNestedDecimalScaleChange() {
        Column column = new Column("struct_col",
                new StructType(new StructField("d", ScalarType.createDecimalV3Type(10, 4))), true);
        NestedField currentCol = Types.NestedField.required(1, "struct_col",
                Types.StructType.of(Types.NestedField.optional(2, "d",
                        Types.DecimalType.of(5, 3))));
        assertUserException(() -> invokeValidateForModifyComplexColumn(column, currentCol),
                "Cannot change decimalv3(5,3) to decimalv3(10,4) in nested types");
    }

    @Test
    public void testValidateForModifyComplexColumnRejectsPrimitiveToComplex() {
        Column column = new Column("arr_i", ArrayType.create(Type.INT, true), true);
        NestedField currentCol = Types.NestedField.required(1, "arr_i", Types.IntegerType.get());
        assertUserException(() -> invokeValidateForModifyComplexColumn(column, currentCol),
                "Modify column type from non-complex to complex is not supported");
    }

    @Test
    public void testValidateForModifyComplexColumnRejectsDifferentComplexCategory() {
        Column column = new Column("arr_i", new MapType(Type.INT, Type.INT), true);
        NestedField currentCol = Types.NestedField.required(1, "arr_i",
                Types.ListType.ofOptional(2, Types.IntegerType.get()));
        assertUserException(() -> invokeValidateForModifyComplexColumn(column, currentCol),
                "Cannot change complex column type category");
    }

    @Test
    public void testValidateForModifyComplexColumnRejectsNullableToNotNull() {
        Column column = new Column("arr_i", ArrayType.create(Type.INT, true), false);
        NestedField currentCol = Types.NestedField.optional(1, "arr_i",
                Types.ListType.ofOptional(2, Types.IntegerType.get()));
        assertUserException(() -> invokeValidateForModifyComplexColumn(column, currentCol),
                "Cannot change nullable column arr_i to not null");
    }

    @Test
    public void testValidateForModifyComplexColumnRejectsDefaultValue() {
        Column column = new Column("arr_i", ArrayType.create(Type.INT, true),
                false, null, true, "1", "");
        NestedField currentCol = Types.NestedField.required(1, "arr_i",
                Types.ListType.ofOptional(2, Types.IntegerType.get()));
        assertUserException(() -> invokeValidateForModifyComplexColumn(column, currentCol),
                "Complex type default value only supports NULL");
    }

    @Test
    public void testValidateForModifyComplexColumnSuccess() throws Throwable {
        Column column = new Column("arr_i", ArrayType.create(Type.BIGINT, true), true);
        NestedField currentCol = Types.NestedField.required(1, "arr_i",
                Types.ListType.ofOptional(2, Types.IntegerType.get()));
        invokeValidateForModifyComplexColumn(column, currentCol);
    }

    @Test
    public void testModifyComplexColumnRejectsCaseInsensitiveStructFieldAdditions() {
        Schema schema = mixedCaseNestedSchema();
        ExternalTable dorisTable = Mockito.mock(ExternalTable.class);
        Table icebergTable = Mockito.mock(Table.class);
        UpdateSchema updateSchema = Mockito.mock(UpdateSchema.class);
        Mockito.when(icebergTable.schema()).thenReturn(schema);
        Mockito.when(icebergTable.updateSchema()).thenReturn(updateSchema);

        StructType infoType = new StructType(
                new StructField("Metric", Type.INT),
                new StructField("Label", Type.STRING),
                new StructField("metric", Type.INT));
        ArrayType eventsType = ArrayType.create(new StructType(
                new StructField("Score", Type.INT),
                new StructField("score", Type.INT)), true);
        MapType attrsType = new MapType(Type.STRING, new StructType(
                new StructField("Code", Type.INT),
                new StructField("code", Type.INT)));
        StructType duplicateNewFieldsType = new StructType(
                new StructField("Metric", Type.INT),
                new StructField("Label", Type.STRING),
                new StructField("Extra", Type.INT),
                new StructField("EXTRA", Type.STRING));

        try (MockedStatic<IcebergUtils> mockedIcebergUtils =
                Mockito.mockStatic(IcebergUtils.class, Mockito.CALLS_REAL_METHODS)) {
            mockedIcebergUtils.when(() -> IcebergUtils.getIcebergTable(dorisTable)).thenReturn(icebergTable);

            assertUserException(() -> ops.modifyColumn(
                            dorisTable, new Column("info", infoType, true), null, 1L),
                    "Added struct field 'metric' conflicts with existing field");
            assertUserException(() -> ops.modifyColumn(
                            dorisTable, new Column("events", eventsType, true), null, 1L),
                    "Added struct field 'score' conflicts with existing field");
            assertUserException(() -> ops.modifyColumn(
                            dorisTable, new Column("attrs", attrsType, true), null, 1L),
                    "Added struct field 'code' conflicts with existing field");
            assertUserException(() -> ops.modifyColumn(
                            dorisTable, new Column("info", duplicateNewFieldsType, true), null, 1L),
                    "Added struct field 'extra' conflicts with existing field");
        }

        Mockito.verifyNoInteractions(updateSchema);
    }

    @Test
    public void testResolveNestedColumnPathSupportsStructArrayElementAndMapValue() throws Throwable {
        Schema schema = nestedSchema();
        Assert.assertTrue(ops.resolveNestedColumnPath(schema, ColumnPath.fromDotName("s"), "add").isStructType());
        Assert.assertTrue(ops.resolveNestedColumnPath(schema, ColumnPath.fromDotName("arr.element"), "add")
                .isStructType());
        Assert.assertTrue(ops.resolveNestedColumnPath(schema, ColumnPath.fromDotName("m.value"), "add")
                .isStructType());
    }

    @Test
    public void testResolveNestedColumnPathUsesCaseInsensitiveCanonicalIcebergPath() throws Throwable {
        Schema schema = mixedCaseNestedSchema();
        Assert.assertEquals("Info.Metric",
                ops.getCanonicalColumnPath(schema, ColumnPath.fromDotName("info.metric"), "modify"));
        Assert.assertEquals("Events.element.Score",
                ops.getCanonicalColumnPath(schema, ColumnPath.fromDotName("events.element.score"), "modify"));
        Assert.assertEquals("Attrs.value.Code",
                ops.getCanonicalColumnPath(schema, ColumnPath.fromDotName("attrs.value.code"), "modify"));
        Assert.assertEquals("Info.Label",
                ops.getPositionReferencePath(schema, ColumnPath.fromDotName("Info.NewField"),
                        new ColumnPosition("label"), "add"));
    }

    @Test
    public void testValidateNoCaseInsensitiveSiblingCollisionRejectsAddAndRenameTargets() {
        Types.StructType parentType = mixedCaseNestedSchema().findField("Info").type().asStructType();
        ColumnPath parentPath = ColumnPath.fromDotName("Info");
        assertUserException(() -> ops.validateNoCaseInsensitiveSiblingCollision(
                        parentType, parentPath, "metric", null, "add"),
                "Cannot add nested column 'Info.metric': conflicts with existing Iceberg field 'Info.Metric'");
        assertUserException(() -> ops.validateNoCaseInsensitiveSiblingCollision(
                        parentType, parentPath, "metric", parentType.field("Label"), "rename"),
                "Cannot rename nested column 'Info.metric': conflicts with existing Iceberg field 'Info.Metric'");
    }

    @Test
    public void testValidateNoCaseInsensitiveSiblingCollisionAllowsCaseOnlyRename() throws Throwable {
        Types.StructType parentType = mixedCaseNestedSchema().findField("Info").type().asStructType();
        ops.validateNoCaseInsensitiveSiblingCollision(parentType, ColumnPath.fromDotName("Info"),
                "metric", parentType.field("Metric"), "rename");
    }

    @Test
    public void testTopLevelCaseInsensitiveCollisionsAndCaseOnlyRename() throws Throwable {
        Schema schema = new Schema(
                Types.NestedField.optional(1, "Id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "Label", Types.StringType.get()));
        ExternalTable dorisTable = Mockito.mock(ExternalTable.class);
        Table icebergTable = Mockito.mock(Table.class);
        UpdateSchema updateSchema = Mockito.mock(UpdateSchema.class);
        Mockito.when(dorisTable.getRemoteDbName()).thenReturn("db");
        Mockito.when(icebergTable.schema()).thenReturn(schema);
        Mockito.when(icebergTable.updateSchema()).thenReturn(updateSchema);

        try (MockedStatic<IcebergUtils> mockedIcebergUtils =
                Mockito.mockStatic(IcebergUtils.class, Mockito.CALLS_REAL_METHODS)) {
            mockedIcebergUtils.when(() -> IcebergUtils.getIcebergTable(dorisTable)).thenReturn(icebergTable);

            assertUserException(() -> ops.addColumn(
                            dorisTable, new Column("id", Type.STRING, true), null, 1L),
                    "Cannot add column 'id': conflicts with existing Iceberg field 'Id'");
            assertUserException(() -> ops.addColumns(dorisTable,
                            Collections.singletonList(new Column("id", Type.STRING, true)), 1L),
                    "Cannot add column 'id': conflicts with existing Iceberg field 'Id'");
            assertUserException(() -> ops.addColumns(dorisTable, Arrays.asList(
                            new Column("new_field", Type.STRING, true),
                            new Column("NEW_FIELD", Type.STRING, true)), 1L),
                    "conflicts with another requested column (case-insensitive)");
            assertUserException(() -> ops.renameColumn(dorisTable, "label", "id", 1L),
                    "Cannot rename column 'id': conflicts with existing Iceberg field 'Id'");

            ops.renameColumn(dorisTable, "id", "id", 1L);
        }

        Mockito.verify(updateSchema).renameColumn("Id", "id");
        Mockito.verify(updateSchema).commit();
    }

    @Test
    public void testModifyColumnSupportsDirectArrayElementAndMapValue() throws Throwable {
        Schema schema = primitiveContainerSchema();
        ExternalTable dorisTable = Mockito.mock(ExternalTable.class);
        Table icebergTable = Mockito.mock(Table.class);
        UpdateSchema updateSchema = Mockito.mock(UpdateSchema.class);
        Mockito.when(dorisTable.getRemoteDbName()).thenReturn("db");
        Mockito.when(icebergTable.schema()).thenReturn(schema);
        Mockito.when(icebergTable.updateSchema()).thenReturn(updateSchema);

        try (MockedStatic<IcebergUtils> mockedIcebergUtils =
                Mockito.mockStatic(IcebergUtils.class, Mockito.CALLS_REAL_METHODS)) {
            mockedIcebergUtils.when(() -> IcebergUtils.getIcebergTable(dorisTable)).thenReturn(icebergTable);

            ops.modifyColumn(dorisTable, ColumnPath.fromDotName("arr.element"),
                    new Column("element", Type.BIGINT, true), null, 1L);
            ops.modifyColumn(dorisTable, ColumnPath.fromDotName("m.value"),
                    new Column("value", Type.BIGINT, true), null, 1L);
        }

        Mockito.verify(updateSchema).updateColumn("arr.element", Types.LongType.get(), "");
        Mockito.verify(updateSchema).updateColumn("m.value", Types.LongType.get(), "");
        Mockito.verify(updateSchema).makeColumnOptional("arr.element");
        Mockito.verify(updateSchema).makeColumnOptional("m.value");
        Mockito.verify(updateSchema, Mockito.times(2)).commit();
    }

    @Test
    public void testModifyColumnCommentUsesCanonicalNestedPaths() throws Throwable {
        Schema schema = mixedCaseNestedSchema();
        ExternalTable dorisTable = Mockito.mock(ExternalTable.class);
        Table icebergTable = Mockito.mock(Table.class);
        UpdateSchema updateSchema = Mockito.mock(UpdateSchema.class);
        Mockito.when(dorisTable.getRemoteDbName()).thenReturn("db");
        Mockito.when(icebergTable.schema()).thenReturn(schema);
        Mockito.when(icebergTable.updateSchema()).thenReturn(updateSchema);

        try (MockedStatic<IcebergUtils> mockedIcebergUtils =
                Mockito.mockStatic(IcebergUtils.class, Mockito.CALLS_REAL_METHODS)) {
            mockedIcebergUtils.when(() -> IcebergUtils.getIcebergTable(dorisTable)).thenReturn(icebergTable);

            ops.modifyColumnComment(dorisTable, ColumnPath.fromDotName("info.metric"),
                    "struct comment", 1L);
            ops.modifyColumnComment(dorisTable, ColumnPath.fromDotName("events.element.score"),
                    "array element comment", 1L);
            ops.modifyColumnComment(dorisTable, ColumnPath.fromDotName("attrs.value.code"),
                    "map value comment", 1L);
        }

        Mockito.verify(updateSchema).updateColumnDoc("Info.Metric", "struct comment");
        Mockito.verify(updateSchema).updateColumnDoc("Events.element.Score", "array element comment");
        Mockito.verify(updateSchema).updateColumnDoc("Attrs.value.Code", "map value comment");
        Mockito.verify(updateSchema, Mockito.times(3)).commit();
    }

    @Test
    public void testModifyColumnCommentSupportsDirectArrayElementAndMapValue() throws Throwable {
        Schema schema = primitiveContainerSchema();
        ExternalTable dorisTable = Mockito.mock(ExternalTable.class);
        Table icebergTable = Mockito.mock(Table.class);
        UpdateSchema updateSchema = Mockito.mock(UpdateSchema.class);
        Mockito.when(dorisTable.getRemoteDbName()).thenReturn("db");
        Mockito.when(icebergTable.schema()).thenReturn(schema);
        Mockito.when(icebergTable.updateSchema()).thenReturn(updateSchema);

        try (MockedStatic<IcebergUtils> mockedIcebergUtils =
                Mockito.mockStatic(IcebergUtils.class, Mockito.CALLS_REAL_METHODS)) {
            mockedIcebergUtils.when(() -> IcebergUtils.getIcebergTable(dorisTable)).thenReturn(icebergTable);

            ops.modifyColumnComment(dorisTable, ColumnPath.fromDotName("arr.element"),
                    "array element comment", 1L);
            ops.modifyColumnComment(dorisTable, ColumnPath.fromDotName("m.value"),
                    "map value comment", 1L);
            assertUserException(() -> ops.modifyColumnComment(
                            dorisTable, ColumnPath.fromDotName("m.key"), "map key comment", 1L),
                    "Cannot modify comment MAP key nested column");
        }

        Mockito.verify(updateSchema).updateColumnDoc("arr.element", "array element comment");
        Mockito.verify(updateSchema).updateColumnDoc("m.value", "map value comment");
        Mockito.verify(updateSchema, Mockito.times(2)).commit();
    }

    @Test
    public void testResolveNestedColumnPathRejectsMapKey() {
        assertUserException(() -> ops.resolveNestedColumnPath(nestedSchema(), ColumnPath.fromDotName("m.key.x"),
                        "modify"),
                "Cannot modify MAP key nested column");
    }

    @Test
    public void testResolveNestedColumnPathRejectsPrimitiveParent() {
        assertUserException(() -> ops.resolveNestedColumnPath(nestedSchema(), ColumnPath.fromDotName("id.x"),
                        "modify"),
                "Cannot resolve nested field under primitive column path");
    }

    @Test
    public void testGetPositionReferencePathForNestedColumn() {
        Assert.assertEquals("s.a", ops.getPositionReferencePath(ColumnPath.fromDotName("s.new_col"),
                new ColumnPosition("a")));
        Assert.assertEquals("arr.element.x", ops.getPositionReferencePath(
                ColumnPath.fromDotName("arr.element.new_col"), new ColumnPosition("x")));
        Assert.assertEquals("m.value.v", ops.getPositionReferencePath(
                ColumnPath.fromDotName("m.value.new_col"), new ColumnPosition("v")));
        Assert.assertEquals("id", ops.getPositionReferencePath(ColumnPath.fromDotName("new_col"),
                new ColumnPosition("id")));
    }

    @Test
    public void testValidateNestedStructFieldSupportsStructArrayElementAndMapValueFields() throws Throwable {
        Schema schema = nestedSchema();
        Assert.assertTrue(ops.validateNestedStructField(schema, ColumnPath.fromDotName("s.a"), "drop")
                .isPrimitiveType());
        Assert.assertTrue(ops.validateNestedStructField(schema, ColumnPath.fromDotName("arr.element.x"), "drop")
                .isPrimitiveType());
        Assert.assertTrue(ops.validateNestedStructField(schema, ColumnPath.fromDotName("m.value.v"), "rename")
                .isPrimitiveType());
    }

    @Test
    public void testValidateNestedStructFieldRejectsArrayElementAndMapValuePseudoFields() {
        assertUserException(() -> ops.validateNestedStructField(nestedSchema(), ColumnPath.fromDotName("arr.element"),
                        "drop"),
                "Parent column path 'arr' is not a struct");
        assertUserException(() -> ops.validateNestedStructField(nestedSchema(), ColumnPath.fromDotName("m.value"),
                        "rename"),
                "Parent column path 'm' is not a struct");
    }

    @Test
    public void testValidateNestedStructFieldRejectsMapKeyAndMissingField() {
        assertUserException(() -> ops.validateNestedStructField(nestedSchema(), ColumnPath.fromDotName("m.key.k"),
                        "drop"),
                "Cannot drop MAP key nested column");
        assertUserException(() -> ops.validateNestedStructField(nestedSchema(), ColumnPath.fromDotName("s.missing"),
                        "rename"),
                "Column path does not exist in Iceberg schema");
    }

    private void invokeValidateForModifyColumn(Column column, NestedField currentCol) throws Throwable {
        invokeValidationMethod(validateForModifyColumnMethod, column, currentCol);
    }

    private void invokeValidateForModifyComplexColumn(Column column, NestedField currentCol) throws Throwable {
        invokeValidationMethod(validateForModifyComplexColumnMethod, column, currentCol);
    }

    private void invokeValidationMethod(Method method, Column column, NestedField currentCol) throws Throwable {
        try {
            method.invoke(ops, column, currentCol);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }

    private void assertUserException(ThrowingRunnable runnable, String expectedMessage) {
        try {
            runnable.run();
            Assert.fail("expected UserException");
        } catch (Throwable t) {
            Assert.assertTrue(t instanceof UserException);
            Assert.assertTrue(t.getMessage().contains(expectedMessage));
        }
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Throwable;
    }

    private Schema nestedSchema() {
        return new Schema(
                Types.NestedField.optional(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "s", Types.StructType.of(
                        Types.NestedField.optional(3, "a", Types.IntegerType.get()))),
                Types.NestedField.optional(4, "arr", Types.ListType.ofOptional(5,
                        Types.StructType.of(Types.NestedField.optional(6, "x", Types.IntegerType.get())))),
                Types.NestedField.optional(7, "m", Types.MapType.ofOptional(8, 9,
                        Types.StringType.get(),
                        Types.StructType.of(Types.NestedField.optional(10, "v", Types.IntegerType.get())))));
    }

    private Schema mixedCaseNestedSchema() {
        return new Schema(
                Types.NestedField.optional(1, "Id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "Info", Types.StructType.of(
                        Types.NestedField.optional(3, "Metric", Types.IntegerType.get()),
                        Types.NestedField.optional(4, "Label", Types.StringType.get()))),
                Types.NestedField.optional(5, "Events", Types.ListType.ofOptional(6,
                        Types.StructType.of(Types.NestedField.optional(7, "Score", Types.IntegerType.get())))),
                Types.NestedField.optional(8, "Attrs", Types.MapType.ofOptional(9, 10,
                        Types.StringType.get(),
                        Types.StructType.of(Types.NestedField.optional(11, "Code", Types.IntegerType.get())))));
    }

    private Schema primitiveContainerSchema() {
        return new Schema(
                Types.NestedField.optional(1, "arr",
                        Types.ListType.ofOptional(2, Types.IntegerType.get())),
                Types.NestedField.optional(3, "m", Types.MapType.ofOptional(
                        4, 5, Types.StringType.get(), Types.IntegerType.get())));
    }
}
