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
    public void testValidateForModifyColumnRejectsComplexToPrimitive() {
        Column column = new Column("struct_col", Type.INT, true);
        NestedField currentCol = Types.NestedField.required(1, "struct_col", Types.StructType.of(
                Types.NestedField.required(2, "value", Types.IntegerType.get())));
        assertUserException(() -> invokeValidateForModifyColumn(column, currentCol),
                "Modify column type from complex to primitive is not supported: struct_col");
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
    public void testRejectUnsupportedIcebergTargetTypesBeforeUpdateSchema() {
        Schema schema = requiredNestedSchema();
        ExternalTable dorisTable = Mockito.mock(ExternalTable.class);
        Table icebergTable = Mockito.mock(Table.class);
        Mockito.when(icebergTable.schema()).thenReturn(schema);

        StructType unsupportedStruct = new StructType(new StructField("value", Type.LARGEINT));
        ArrayType unsupportedArray = ArrayType.create(Type.LARGEINT, true);
        MapType unsupportedMap = new MapType(Type.STRING, Type.LARGEINT);

        try (MockedStatic<IcebergUtils> mockedIcebergUtils =
                Mockito.mockStatic(IcebergUtils.class, Mockito.CALLS_REAL_METHODS)) {
            mockedIcebergUtils.when(() -> IcebergUtils.getIcebergTable(dorisTable)).thenReturn(icebergTable);

            assertUserException(() -> ops.addColumn(dorisTable, ColumnPath.fromDotName("info.new_field"),
                            new Column("new_field", Type.LARGEINT, true), null, 1L),
                    "is not supported for Iceberg column new_field");
            assertUserException(() -> ops.modifyColumn(dorisTable, ColumnPath.fromDotName("info.metric"),
                            new Column("metric", Type.LARGEINT, true), null, 1L),
                    "is not supported for Iceberg column info.metric");
            assertUserException(() -> ops.modifyColumn(dorisTable, ColumnPath.fromDotName("info.child"),
                            new Column("child", unsupportedStruct, false), null, 1L),
                    "is not supported for Iceberg column info.child.value");
            assertUserException(() -> ops.modifyColumn(dorisTable, ColumnPath.fromDotName("info.events"),
                            new Column("events", unsupportedArray, false), null, 1L),
                    "is not supported for Iceberg column info.events.element");
            assertUserException(() -> ops.modifyColumn(dorisTable, ColumnPath.fromDotName("info.attrs"),
                            new Column("attrs", unsupportedMap, false), null, 1L),
                    "is not supported for Iceberg column info.attrs.value");
        }

        Mockito.verify(icebergTable, Mockito.never()).updateSchema();
    }

    @Test
    public void testComplexModifyPreservesRequiredNestedFields() throws Throwable {
        Schema schema = requiredNestedSchema();
        ExternalTable dorisTable = Mockito.mock(ExternalTable.class);
        Table icebergTable = Mockito.mock(Table.class);
        UpdateSchema updateSchema = Mockito.mock(UpdateSchema.class);
        Mockito.when(dorisTable.getRemoteDbName()).thenReturn("db");
        Mockito.when(icebergTable.schema()).thenReturn(schema);
        Mockito.when(icebergTable.updateSchema()).thenReturn(updateSchema);

        try (MockedStatic<IcebergUtils> mockedIcebergUtils =
                Mockito.mockStatic(IcebergUtils.class, Mockito.CALLS_REAL_METHODS)) {
            mockedIcebergUtils.when(() -> IcebergUtils.getIcebergTable(dorisTable)).thenReturn(icebergTable);

            ops.modifyColumn(dorisTable, ColumnPath.fromDotName("info.child"),
                    new Column("child", new StructType(new StructField("value", Type.BIGINT)), true), null, 1L);
            ops.modifyColumn(dorisTable, ColumnPath.fromDotName("info.events"),
                    new Column("events", ArrayType.create(Type.BIGINT, true), true), null, 1L);
            ops.modifyColumn(dorisTable, ColumnPath.fromDotName("info.attrs"),
                    new Column("attrs", new MapType(Type.STRING, Type.BIGINT), true), null, 1L);
        }

        Mockito.verify(updateSchema).updateColumn("info.child.value", Types.LongType.get(), null);
        Mockito.verify(updateSchema).updateColumn("info.events.element", Types.LongType.get(), null);
        Mockito.verify(updateSchema).updateColumn("info.attrs.value", Types.LongType.get(), null);
        Mockito.verify(updateSchema, Mockito.never()).makeColumnOptional(Mockito.anyString());
        Mockito.verify(updateSchema, Mockito.times(3)).commit();
    }

    @Test
    public void testComplexModifyPersistsDecodedStructMemberComment() throws Throwable {
        Schema schema = new Schema(Types.NestedField.optional(1, "info", Types.StructType.of(
                Types.NestedField.optional(2, "payload", Types.StructType.of(
                        Types.NestedField.optional(3, "name", Types.StringType.get(), "old comment"))))));
        ExternalTable dorisTable = Mockito.mock(ExternalTable.class);
        Table icebergTable = Mockito.mock(Table.class);
        UpdateSchema updateSchema = Mockito.mock(UpdateSchema.class);
        Mockito.when(dorisTable.getRemoteDbName()).thenReturn("db");
        Mockito.when(icebergTable.schema()).thenReturn(schema);
        Mockito.when(icebergTable.updateSchema()).thenReturn(updateSchema);

        String decodedComment = "owner's \"field\" C:\\tmp\\";
        Column column = new Column("payload", new StructType(
                new StructField("name", Type.STRING, decodedComment, true)), true);

        try (MockedStatic<IcebergUtils> mockedIcebergUtils =
                Mockito.mockStatic(IcebergUtils.class, Mockito.CALLS_REAL_METHODS)) {
            mockedIcebergUtils.when(() -> IcebergUtils.getIcebergTable(dorisTable)).thenReturn(icebergTable);

            ops.modifyColumn(dorisTable, ColumnPath.fromDotName("info.payload"), column, null, 1L);
        }

        Mockito.verify(updateSchema).updateColumnDoc("info.payload.name", decodedComment);
        Mockito.verify(updateSchema).commit();
    }

    @Test
    public void testPrimitiveModifyPreservesOmittedCommentAndClearsExplicitEmptyComment() throws Throwable {
        Schema schema = new Schema(
                Types.NestedField.optional(1, "info", Types.StructType.of(
                        Types.NestedField.optional(2, "metric", Types.IntegerType.get(), "metric doc"),
                        Types.NestedField.optional(3, "clear_me", Types.StringType.get(), "clear doc"))),
                Types.NestedField.optional(4, "top_metric", Types.IntegerType.get(), "top metric doc"));
        ExternalTable dorisTable = Mockito.mock(ExternalTable.class);
        Table icebergTable = Mockito.mock(Table.class);
        UpdateSchema updateSchema = Mockito.mock(UpdateSchema.class);
        Mockito.when(dorisTable.getRemoteDbName()).thenReturn("db");
        Mockito.when(icebergTable.schema()).thenReturn(schema);
        Mockito.when(icebergTable.updateSchema()).thenReturn(updateSchema);

        Column clearComment = new Column("clear_me", Type.STRING, true, "");
        clearComment.setCommentSpecified(true);

        try (MockedStatic<IcebergUtils> mockedIcebergUtils =
                Mockito.mockStatic(IcebergUtils.class, Mockito.CALLS_REAL_METHODS)) {
            mockedIcebergUtils.when(() -> IcebergUtils.getIcebergTable(dorisTable)).thenReturn(icebergTable);

            ops.modifyColumn(dorisTable, ColumnPath.fromDotName("info.metric"),
                    new Column("metric", Type.BIGINT, true), null, 1L);
            ops.modifyColumn(dorisTable, ColumnPath.of("top_metric"),
                    new Column("top_metric", Type.BIGINT, true), null, 1L);
            ops.modifyColumn(dorisTable, ColumnPath.fromDotName("info.clear_me"),
                    clearComment, null, 1L);
        }

        Mockito.verify(updateSchema).updateColumn("info.metric", Types.LongType.get(), "metric doc");
        Mockito.verify(updateSchema).updateColumn("top_metric", Types.LongType.get(), "top metric doc");
        Mockito.verify(updateSchema).updateColumnDoc("info.clear_me", "");
        Mockito.verify(updateSchema, Mockito.times(3)).commit();
    }

    @Test
    public void testFullStructModifyPreservesOmittedChildComments() throws Throwable {
        Schema schema = new Schema(Types.NestedField.optional(1, "info", Types.StructType.of(
                Types.NestedField.optional(2, "payload", Types.StructType.of(
                        Types.NestedField.optional(3, "metric", Types.IntegerType.get(), "metric doc"),
                        Types.NestedField.optional(4, "clear_me", Types.StringType.get(), "clear doc"),
                        Types.NestedField.optional(5, "details", Types.StructType.of(
                                Types.NestedField.optional(
                                        6, "count", Types.IntegerType.get(), "count doc")), "details doc")),
                        "payload doc"))));
        ExternalTable dorisTable = Mockito.mock(ExternalTable.class);
        Table icebergTable = Mockito.mock(Table.class);
        UpdateSchema updateSchema = Mockito.mock(UpdateSchema.class);
        Mockito.when(dorisTable.getRemoteDbName()).thenReturn("db");
        Mockito.when(icebergTable.schema()).thenReturn(schema);
        Mockito.when(icebergTable.updateSchema()).thenReturn(updateSchema);

        StructType detailsType = new StructType(
                new StructField("count", Type.BIGINT, "", true, false));
        StructType payloadType = new StructType(
                new StructField("metric", Type.BIGINT, "", true, false),
                new StructField("clear_me", Type.STRING, "", true, true),
                new StructField("details", detailsType, "", true, false));

        try (MockedStatic<IcebergUtils> mockedIcebergUtils =
                Mockito.mockStatic(IcebergUtils.class, Mockito.CALLS_REAL_METHODS)) {
            mockedIcebergUtils.when(() -> IcebergUtils.getIcebergTable(dorisTable)).thenReturn(icebergTable);

            ops.modifyColumn(dorisTable, ColumnPath.fromDotName("info.payload"),
                    new Column("payload", payloadType, true), null, 1L);
        }

        Mockito.verify(updateSchema).updateColumn(
                "info.payload.metric", Types.LongType.get(), "metric doc");
        Mockito.verify(updateSchema).updateColumnDoc("info.payload.clear_me", "");
        Mockito.verify(updateSchema).updateColumn(
                "info.payload.details.count", Types.LongType.get(), "count doc");
        Mockito.verify(updateSchema, Mockito.never()).updateColumnDoc(
                Mockito.eq("info.payload"), Mockito.nullable(String.class));
        Mockito.verify(updateSchema, Mockito.never()).updateColumnDoc(
                Mockito.eq("info.payload.details"), Mockito.nullable(String.class));
        Mockito.verify(updateSchema).commit();
    }

    @Test
    public void testPrimitiveModifyPreservesRequiredNestedField() throws Throwable {
        Schema schema = requiredNestedSchema();
        ExternalTable dorisTable = Mockito.mock(ExternalTable.class);
        Table icebergTable = Mockito.mock(Table.class);
        UpdateSchema updateSchema = Mockito.mock(UpdateSchema.class);
        Mockito.when(dorisTable.getRemoteDbName()).thenReturn("db");
        Mockito.when(icebergTable.schema()).thenReturn(schema);
        Mockito.when(icebergTable.updateSchema()).thenReturn(updateSchema);

        try (MockedStatic<IcebergUtils> mockedIcebergUtils =
                Mockito.mockStatic(IcebergUtils.class, Mockito.CALLS_REAL_METHODS)) {
            mockedIcebergUtils.when(() -> IcebergUtils.getIcebergTable(dorisTable)).thenReturn(icebergTable);

            ops.modifyColumn(dorisTable, ColumnPath.fromDotName("info.metric"),
                    new Column("metric", Type.BIGINT, true), null, 1L);
        }

        Mockito.verify(updateSchema).updateColumn("info.metric", Types.LongType.get(), null);
        Mockito.verify(updateSchema, Mockito.never()).makeColumnOptional(Mockito.anyString());
        Mockito.verify(updateSchema).commit();
    }

    @Test
    public void testTopLevelModifyPreservesRequiredMixedCaseFields() throws Throwable {
        Schema schema = new Schema(
                Types.NestedField.required(1, "Id", Types.IntegerType.get()),
                Types.NestedField.required(2, "Payload", Types.StructType.of(
                        Types.NestedField.required(3, "Value", Types.IntegerType.get()))));
        ExternalTable dorisTable = Mockito.mock(ExternalTable.class);
        Table icebergTable = Mockito.mock(Table.class);
        UpdateSchema updateSchema = Mockito.mock(UpdateSchema.class);
        Mockito.when(dorisTable.getRemoteDbName()).thenReturn("db");
        Mockito.when(icebergTable.schema()).thenReturn(schema);
        Mockito.when(icebergTable.updateSchema()).thenReturn(updateSchema);

        try (MockedStatic<IcebergUtils> mockedIcebergUtils =
                Mockito.mockStatic(IcebergUtils.class, Mockito.CALLS_REAL_METHODS)) {
            mockedIcebergUtils.when(() -> IcebergUtils.getIcebergTable(dorisTable)).thenReturn(icebergTable);

            ops.modifyColumn(dorisTable, ColumnPath.of("id"),
                    new Column("id", Type.BIGINT, true), null, 1L);
            ops.modifyColumn(dorisTable, ColumnPath.of("payload"), new Column("payload",
                    new StructType(new StructField("Value", Type.BIGINT)), true), null, 1L);
        }

        Mockito.verify(updateSchema).updateColumn("Id", Types.LongType.get(), null);
        Mockito.verify(updateSchema).updateColumn("Payload.Value", Types.LongType.get(), null);
        Mockito.verify(updateSchema, Mockito.never()).makeColumnOptional(Mockito.anyString());
        Mockito.verify(updateSchema, Mockito.times(2)).commit();
    }

    @Test
    public void testTopLevelModifyDoesNotResolveQuotedComponentAsNestedPath() {
        Schema schema = new Schema(
                Types.NestedField.optional(1, "a", Types.StructType.of(
                        Types.NestedField.optional(2, "b", Types.IntegerType.get()))),
                Types.NestedField.optional(3, "b", Types.IntegerType.get()));
        ExternalTable dorisTable = Mockito.mock(ExternalTable.class);
        Table icebergTable = Mockito.mock(Table.class);
        Mockito.when(icebergTable.schema()).thenReturn(schema);

        try (MockedStatic<IcebergUtils> mockedIcebergUtils =
                Mockito.mockStatic(IcebergUtils.class, Mockito.CALLS_REAL_METHODS)) {
            mockedIcebergUtils.when(() -> IcebergUtils.getIcebergTable(dorisTable)).thenReturn(icebergTable);

            assertUserException(() -> ops.modifyColumn(dorisTable, ColumnPath.of("a.b"),
                            new Column("a.b", Type.BIGINT, true), null, 1L),
                    "Column a.b does not exist");
        }

        Mockito.verify(icebergTable, Mockito.never()).updateSchema();
    }

    @Test
    public void testTopLevelModifyPreservesDottedTopLevelName() throws Throwable {
        Schema schema = new Schema(Types.NestedField.optional(1, "a.b", Types.IntegerType.get()));
        ExternalTable dorisTable = Mockito.mock(ExternalTable.class);
        Table icebergTable = Mockito.mock(Table.class);
        UpdateSchema updateSchema = Mockito.mock(UpdateSchema.class);
        Mockito.when(dorisTable.getRemoteDbName()).thenReturn("db");
        Mockito.when(icebergTable.schema()).thenReturn(schema);
        Mockito.when(icebergTable.updateSchema()).thenReturn(updateSchema);

        try (MockedStatic<IcebergUtils> mockedIcebergUtils =
                Mockito.mockStatic(IcebergUtils.class, Mockito.CALLS_REAL_METHODS)) {
            mockedIcebergUtils.when(() -> IcebergUtils.getIcebergTable(dorisTable)).thenReturn(icebergTable);

            ops.modifyColumn(dorisTable, ColumnPath.of("a.b"),
                    new Column("a.b", Type.BIGINT, true), null, 1L);
        }

        Mockito.verify(updateSchema).updateColumn("a.b", Types.LongType.get(), null);
        Mockito.verify(updateSchema).commit();
    }

    @Test
    public void testPrimitiveModifyPreservesActualTypeWhenMappingDisabled() throws Throwable {
        Schema schema = mappedPrimitiveSchema();
        ExternalTable dorisTable = Mockito.mock(ExternalTable.class);
        Table icebergTable = Mockito.mock(Table.class);
        UpdateSchema updateSchema = Mockito.mock(UpdateSchema.class);
        Mockito.when(dorisTable.getRemoteDbName()).thenReturn("db");
        Mockito.when(icebergTable.schema()).thenReturn(schema);
        Mockito.when(icebergTable.updateSchema()).thenReturn(updateSchema);
        Mockito.when(dorisCatalog.getEnableMappingVarbinary()).thenReturn(false);
        Mockito.when(dorisCatalog.getEnableMappingTimestampTz()).thenReturn(false);

        Column topUuid = new Column("top_uuid", Type.STRING, true);
        topUuid.setNullableSpecified(true);
        Column nestedUuid = new Column("uuid_value", Type.STRING, true);
        nestedUuid.setNullableSpecified(true);
        Column nestedTimestamp = new Column(
                "tz_value", ScalarType.createDatetimeV2Type(6), true);

        try (MockedStatic<IcebergUtils> mockedIcebergUtils =
                Mockito.mockStatic(IcebergUtils.class, Mockito.CALLS_REAL_METHODS)) {
            mockedIcebergUtils.when(() -> IcebergUtils.getIcebergTable(dorisTable)).thenReturn(icebergTable);

            ops.modifyColumn(dorisTable, ColumnPath.of("top_uuid"), topUuid, ColumnPosition.FIRST, 1L);
            ops.modifyColumn(dorisTable, ColumnPath.fromDotName("info.uuid_value"), nestedUuid,
                    new ColumnPosition("other"), 1L);
            ops.modifyColumn(dorisTable, ColumnPath.fromDotName("info.tz_value"),
                    nestedTimestamp, null, 1L);
        }

        Mockito.verify(updateSchema, Mockito.never()).updateColumnDoc(
                Mockito.anyString(), Mockito.nullable(String.class));
        Mockito.verify(updateSchema).makeColumnOptional("top_uuid");
        Mockito.verify(updateSchema).makeColumnOptional("info.uuid_value");
        Mockito.verify(updateSchema).moveFirst("top_uuid");
        Mockito.verify(updateSchema).moveAfter("info.uuid_value", "info.other");
        Mockito.verify(updateSchema, Mockito.never()).updateColumn(
                Mockito.anyString(), Mockito.any(org.apache.iceberg.types.Type.PrimitiveType.class),
                Mockito.nullable(String.class));
        Mockito.verify(updateSchema, Mockito.times(3)).commit();
    }

    @Test
    public void testPrimitiveModifyPreservesActualTypeWhenMappingEnabled() throws Throwable {
        Schema schema = mappedPrimitiveSchema();
        ExternalTable dorisTable = Mockito.mock(ExternalTable.class);
        Table icebergTable = Mockito.mock(Table.class);
        UpdateSchema updateSchema = Mockito.mock(UpdateSchema.class);
        Mockito.when(dorisTable.getRemoteDbName()).thenReturn("db");
        Mockito.when(icebergTable.schema()).thenReturn(schema);
        Mockito.when(icebergTable.updateSchema()).thenReturn(updateSchema);
        Mockito.when(dorisCatalog.getEnableMappingVarbinary()).thenReturn(true);
        Mockito.when(dorisCatalog.getEnableMappingTimestampTz()).thenReturn(true);

        try (MockedStatic<IcebergUtils> mockedIcebergUtils =
                Mockito.mockStatic(IcebergUtils.class, Mockito.CALLS_REAL_METHODS)) {
            mockedIcebergUtils.when(() -> IcebergUtils.getIcebergTable(dorisTable)).thenReturn(icebergTable);

            ops.modifyColumn(dorisTable, ColumnPath.of("top_uuid"),
                    new Column("top_uuid", ScalarType.createVarbinaryType(16), true), null, 1L);
            ops.modifyColumn(dorisTable, ColumnPath.fromDotName("info.tz_value"),
                    new Column("tz_value", ScalarType.createTimeStampTzType(6), true), null, 1L);
        }

        Mockito.verify(updateSchema, Mockito.never()).updateColumnDoc(
                Mockito.anyString(), Mockito.nullable(String.class));
        Mockito.verify(updateSchema, Mockito.never()).updateColumn(
                Mockito.anyString(), Mockito.any(org.apache.iceberg.types.Type.PrimitiveType.class),
                Mockito.nullable(String.class));
        Mockito.verify(updateSchema, Mockito.times(2)).commit();
    }

    @Test
    public void testComplexModifyIgnoresUnchangedMappedChildren() throws Throwable {
        Schema schema = mappedComplexSchema();
        ExternalTable dorisTable = Mockito.mock(ExternalTable.class);
        Table icebergTable = Mockito.mock(Table.class);
        UpdateSchema updateSchema = Mockito.mock(UpdateSchema.class);
        Mockito.when(dorisTable.getRemoteDbName()).thenReturn("db");
        Mockito.when(icebergTable.schema()).thenReturn(schema);
        Mockito.when(icebergTable.updateSchema()).thenReturn(updateSchema);
        Mockito.when(dorisCatalog.getEnableMappingVarbinary()).thenReturn(true);
        Mockito.when(dorisCatalog.getEnableMappingTimestampTz()).thenReturn(true);

        try (MockedStatic<IcebergUtils> mockedIcebergUtils =
                Mockito.mockStatic(IcebergUtils.class, Mockito.CALLS_REAL_METHODS)) {
            mockedIcebergUtils.when(() -> IcebergUtils.getIcebergTable(dorisTable)).thenReturn(icebergTable);

            ops.modifyColumn(dorisTable, ColumnPath.fromDotName("outer.payload"),
                    new Column("payload", mappedPayloadDorisType(Type.BIGINT, 8,
                            ScalarType.createVarbinaryType(4)), true), null, 1L);
        }

        Mockito.verify(updateSchema).updateColumn(
                "outer.payload.metric", Types.LongType.get(), null);
        Mockito.verify(updateSchema, Mockito.times(1)).updateColumn(
                Mockito.anyString(), Mockito.any(org.apache.iceberg.types.Type.PrimitiveType.class),
                Mockito.nullable(String.class));
        Mockito.verify(updateSchema, Mockito.never()).updateColumnDoc(
                Mockito.anyString(), Mockito.nullable(String.class));
        Mockito.verify(updateSchema).commit();
    }

    @Test
    public void testComplexModifyRejectsChangedUnsupportedMappedChildrenBeforeUpdateSchema() {
        Schema schema = mappedComplexSchema();
        ExternalTable dorisTable = Mockito.mock(ExternalTable.class);
        Table icebergTable = Mockito.mock(Table.class);
        Mockito.when(icebergTable.schema()).thenReturn(schema);
        Mockito.when(dorisCatalog.getEnableMappingVarbinary()).thenReturn(true);
        Mockito.when(dorisCatalog.getEnableMappingTimestampTz()).thenReturn(true);

        try (MockedStatic<IcebergUtils> mockedIcebergUtils =
                Mockito.mockStatic(IcebergUtils.class, Mockito.CALLS_REAL_METHODS)) {
            mockedIcebergUtils.when(() -> IcebergUtils.getIcebergTable(dorisTable)).thenReturn(icebergTable);

            assertUserException(() -> ops.modifyColumn(dorisTable, ColumnPath.fromDotName("outer.payload"),
                            new Column("payload", mappedPayloadDorisType(Type.LARGEINT, 8,
                                    ScalarType.createVarbinaryType(4)), true), null, 1L),
                    "Type largeint is not supported for Iceberg column outer.payload.metric");
            assertUserException(() -> ops.modifyColumn(dorisTable, ColumnPath.fromDotName("outer.payload"),
                            new Column("payload", mappedPayloadDorisType(Type.INT, 16,
                                    ScalarType.createVarbinaryType(4)), true), null, 1L),
                    "Type varbinary(16) is not supported for Iceberg column outer.payload.fixed_value");
            assertUserException(() -> ops.modifyColumn(dorisTable, ColumnPath.fromDotName("outer.payload"),
                            new Column("payload", mappedPayloadDorisType(Type.INT, 8,
                                    ScalarType.createVarbinaryType(8)), true), null, 1L),
                    "Cannot change MAP key type from varbinary(4) to varbinary(8)");
        }

        Mockito.verify(icebergTable, Mockito.never()).updateSchema();
    }

    @Test
    public void testLegacyModifyColumnTreatsNullabilityAsExplicit() throws Throwable {
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
        ExternalTable dorisTable = Mockito.mock(ExternalTable.class);
        Table icebergTable = Mockito.mock(Table.class);
        UpdateSchema updateSchema = Mockito.mock(UpdateSchema.class);
        Mockito.when(dorisTable.getRemoteDbName()).thenReturn("db");
        Mockito.when(icebergTable.schema()).thenReturn(schema);
        Mockito.when(icebergTable.updateSchema()).thenReturn(updateSchema);

        try (MockedStatic<IcebergUtils> mockedIcebergUtils =
                Mockito.mockStatic(IcebergUtils.class, Mockito.CALLS_REAL_METHODS)) {
            mockedIcebergUtils.when(() -> IcebergUtils.getIcebergTable(dorisTable)).thenReturn(icebergTable);

            // Iceberg schema columns are represented as keys in Doris, so the legacy API must not
            // interpret isKey as an explicit KEY clause.
            ops.modifyColumn(dorisTable,
                    new Column("id", Type.BIGINT, true, null, true, null, ""), null, 1L);
        }

        Mockito.verify(updateSchema).updateColumn("id", Types.LongType.get(), "");
        Mockito.verify(updateSchema).makeColumnOptional("id");
        Mockito.verify(updateSchema).commit();
    }

    @Test
    public void testLegacyComplexModifyRestoresRecursiveNullableSemantics() throws Throwable {
        Schema schema = requiredNestedSchema();
        ExternalTable dorisTable = Mockito.mock(ExternalTable.class);
        Table icebergTable = Mockito.mock(Table.class);
        UpdateSchema updateSchema = Mockito.mock(UpdateSchema.class);
        Mockito.when(dorisTable.getRemoteDbName()).thenReturn("db");
        Mockito.when(icebergTable.schema()).thenReturn(schema);
        Mockito.when(icebergTable.updateSchema()).thenReturn(updateSchema);

        Column column = new Column("info", new StructType(
                new StructField("metric", Type.INT),
                new StructField("child", new StructType(new StructField("value", Type.INT))),
                new StructField("events", ArrayType.create(Type.INT, true)),
                new StructField("attrs", new MapType(Type.STRING, Type.INT))), true);

        try (MockedStatic<IcebergUtils> mockedIcebergUtils =
                Mockito.mockStatic(IcebergUtils.class, Mockito.CALLS_REAL_METHODS)) {
            mockedIcebergUtils.when(() -> IcebergUtils.getIcebergTable(dorisTable)).thenReturn(icebergTable);

            ops.modifyColumn(dorisTable, column, null, 1L);
        }

        Mockito.verify(updateSchema).makeColumnOptional("info");
        Mockito.verify(updateSchema).makeColumnOptional("info.metric");
        Mockito.verify(updateSchema).makeColumnOptional("info.child.value");
        Mockito.verify(updateSchema).makeColumnOptional("info.events.element");
        Mockito.verify(updateSchema).makeColumnOptional("info.attrs.value");
        Mockito.verify(updateSchema).commit();
    }

    @Test
    public void testExplicitNullableModifyMakesRequiredFieldsOptional() throws Throwable {
        Schema schema = requiredNestedSchema();
        ExternalTable dorisTable = Mockito.mock(ExternalTable.class);
        Table icebergTable = Mockito.mock(Table.class);
        UpdateSchema updateSchema = Mockito.mock(UpdateSchema.class);
        Mockito.when(dorisTable.getRemoteDbName()).thenReturn("db");
        Mockito.when(icebergTable.schema()).thenReturn(schema);
        Mockito.when(icebergTable.updateSchema()).thenReturn(updateSchema);

        Column topLevelColumn = new Column("info", new StructType(
                new StructField("metric", Type.BIGINT),
                new StructField("child", new StructType(new StructField("value", Type.INT))),
                new StructField("events", ArrayType.create(Type.INT, true)),
                new StructField("attrs", new MapType(Type.STRING, Type.INT))), true);
        topLevelColumn.setNullableSpecified(true);
        Column nestedColumn = new Column("metric", Type.BIGINT, true);
        nestedColumn.setNullableSpecified(true);

        try (MockedStatic<IcebergUtils> mockedIcebergUtils =
                Mockito.mockStatic(IcebergUtils.class, Mockito.CALLS_REAL_METHODS)) {
            mockedIcebergUtils.when(() -> IcebergUtils.getIcebergTable(dorisTable)).thenReturn(icebergTable);

            ops.modifyColumn(dorisTable, ColumnPath.of("info"), topLevelColumn, null, 1L);
            ops.modifyColumn(dorisTable, ColumnPath.fromDotName("info.metric"), nestedColumn, null, 1L);
        }

        Mockito.verify(updateSchema).makeColumnOptional("info");
        Mockito.verify(updateSchema).makeColumnOptional("info.metric");
        Mockito.verify(updateSchema, Mockito.times(2)).commit();
    }

    @Test
    public void testNestedColumnOperationsRejectDefaultMetadata() {
        Schema schema = new Schema(Types.NestedField.optional(1, "s", Types.StructType.of(
                Types.NestedField.optional(2, "existing", Types.IntegerType.get()))));
        ExternalTable dorisTable = Mockito.mock(ExternalTable.class);
        Table icebergTable = Mockito.mock(Table.class);
        Mockito.when(icebergTable.schema()).thenReturn(schema);

        Column nestedAddDefaultColumn = new Column("new_col", Type.BIGINT, false, null, true, "7", "");
        Column nestedDefaultColumn = new Column("existing", Type.BIGINT, false, null, true, "7", "");
        Column nestedOnUpdateColumn = Mockito.spy(new Column("existing", Type.BIGINT, true));
        Mockito.doReturn(true).when(nestedOnUpdateColumn).hasOnUpdateDefaultValue();

        try (MockedStatic<IcebergUtils> mockedIcebergUtils =
                Mockito.mockStatic(IcebergUtils.class, Mockito.CALLS_REAL_METHODS)) {
            mockedIcebergUtils.when(() -> IcebergUtils.getIcebergTable(dorisTable)).thenReturn(icebergTable);

            assertUserException(() -> ops.addColumn(dorisTable, ColumnPath.fromDotName("s.new_col"),
                            nestedAddDefaultColumn, null, 1L),
                    "DEFAULT and ON UPDATE are not supported for Iceberg nested ADD COLUMN: s.new_col");
            assertUserException(() -> ops.modifyColumn(dorisTable, ColumnPath.fromDotName("s.existing"),
                            nestedDefaultColumn, null, 1L),
                    "Modifying default values is not supported for Iceberg columns: s.existing");
            assertUserException(() -> ops.modifyColumn(dorisTable, ColumnPath.fromDotName("s.existing"),
                            nestedOnUpdateColumn, null, 1L),
                    "Modifying default values is not supported for Iceberg columns: s.existing");
        }

        Mockito.verify(icebergTable, Mockito.never()).updateSchema();
    }

    @Test
    public void testTopLevelColumnOperationsRejectUnsupportedDefaultMetadata() {
        Schema schema = new Schema(Types.NestedField.optional(1, "id", Types.IntegerType.get()));
        ExternalTable dorisTable = Mockito.mock(ExternalTable.class);
        Table icebergTable = Mockito.mock(Table.class);
        Mockito.when(icebergTable.schema()).thenReturn(schema);

        Column defaultColumn = new Column("id", Type.BIGINT, false, null, true, "7", "");
        Column onUpdateColumn = Mockito.spy(new Column("id", Type.BIGINT, true));
        Column onUpdateAddColumn = Mockito.spy(new Column("new_col", Type.BIGINT, true));
        Mockito.doReturn(true).when(onUpdateColumn).hasOnUpdateDefaultValue();
        Mockito.doReturn(true).when(onUpdateAddColumn).hasOnUpdateDefaultValue();

        try (MockedStatic<IcebergUtils> mockedIcebergUtils =
                Mockito.mockStatic(IcebergUtils.class, Mockito.CALLS_REAL_METHODS)) {
            mockedIcebergUtils.when(() -> IcebergUtils.getIcebergTable(dorisTable)).thenReturn(icebergTable);

            assertUserException(() -> ops.modifyColumn(dorisTable, defaultColumn, null, 1L),
                    "Modifying default values is not supported for Iceberg columns: id");
            assertUserException(() -> ops.modifyColumn(
                            dorisTable, ColumnPath.of("id"), onUpdateColumn, null, 1L),
                    "Modifying default values is not supported for Iceberg columns: id");
            assertUserException(() -> ops.addColumn(dorisTable, onUpdateAddColumn, null, 1L),
                    "ON UPDATE is not supported for Iceberg ADD COLUMN: new_col");
            assertUserException(() -> ops.addColumns(
                            dorisTable, Collections.singletonList(onUpdateAddColumn), 1L),
                    "ON UPDATE is not supported for Iceberg ADD COLUMN: new_col");
        }

        Mockito.verify(icebergTable, Mockito.never()).updateSchema();
    }

    @Test
    public void testUnsupportedPrimitiveModifyFailsBeforeUpdateSchema() {
        Schema schema = new Schema(
                Types.NestedField.required(1, "top_long", Types.LongType.get()),
                Types.NestedField.required(2, "info", Types.StructType.of(
                        Types.NestedField.required(3, "metric", Types.LongType.get()),
                        Types.NestedField.required(4, "child", Types.StructType.of(
                                Types.NestedField.required(5, "value", Types.IntegerType.get()))))));
        ExternalTable dorisTable = Mockito.mock(ExternalTable.class);
        Table icebergTable = Mockito.mock(Table.class);
        Mockito.when(icebergTable.schema()).thenReturn(schema);

        try (MockedStatic<IcebergUtils> mockedIcebergUtils =
                Mockito.mockStatic(IcebergUtils.class, Mockito.CALLS_REAL_METHODS)) {
            mockedIcebergUtils.when(() -> IcebergUtils.getIcebergTable(dorisTable)).thenReturn(icebergTable);

            assertUserException(() -> ops.modifyColumn(
                            dorisTable, ColumnPath.of("info"), new Column("info", Type.INT, true), null, 1L),
                    "Modify column type from complex to primitive is not supported: info");
            assertUserException(() -> ops.modifyColumn(dorisTable, ColumnPath.fromDotName("info.child"),
                            new Column("child", Type.INT, true), null, 1L),
                    "Modify column type from complex to primitive is not supported: info.child");
            assertUserException(() -> ops.modifyColumn(dorisTable, ColumnPath.of("top_long"),
                            new Column("top_long", Type.INT, true), null, 1L),
                    "Cannot change column type: top_long: long -> int");
            assertUserException(() -> ops.modifyColumn(dorisTable, ColumnPath.fromDotName("info.metric"),
                            new Column("metric", Type.INT, true), null, 1L),
                    "Cannot change column type: info.metric: long -> int");
        }

        Mockito.verify(icebergTable, Mockito.never()).updateSchema();
    }

    @Test
    public void testRejectKeyAndGeneratedMetadataBeforeUpdateSchema() {
        Schema schema = new Schema(Types.NestedField.optional(1, "id", Types.IntegerType.get()));
        ExternalTable dorisTable = Mockito.mock(ExternalTable.class);
        Table icebergTable = Mockito.mock(Table.class);
        Mockito.when(icebergTable.schema()).thenReturn(schema);

        Column keyColumn = new Column("id", Type.BIGINT, true, null, true, null, "");
        Column generatedColumn = Mockito.mock(Column.class);
        Mockito.when(generatedColumn.getName()).thenReturn("id");
        Mockito.when(generatedColumn.isGeneratedColumn()).thenReturn(true);

        try (MockedStatic<IcebergUtils> mockedIcebergUtils =
                Mockito.mockStatic(IcebergUtils.class, Mockito.CALLS_REAL_METHODS)) {
            mockedIcebergUtils.when(() -> IcebergUtils.getIcebergTable(dorisTable)).thenReturn(icebergTable);

            assertUserException(() -> ops.addColumn(dorisTable, keyColumn, null, 1L),
                    "KEY is not supported for Iceberg ADD/MODIFY COLUMN");
            assertUserException(() -> ops.addColumns(dorisTable, Collections.singletonList(keyColumn), 1L),
                    "KEY is not supported for Iceberg ADD/MODIFY COLUMN");
            assertUserException(() -> ops.modifyColumn(
                            dorisTable, ColumnPath.of("id"), keyColumn, null, 1L),
                    "KEY is not supported for Iceberg ADD/MODIFY COLUMN");
            assertUserException(() -> ops.addColumns(dorisTable,
                            Collections.singletonList(generatedColumn), 1L),
                    "Generated columns are not supported for Iceberg ADD/MODIFY COLUMN");
            assertUserException(() -> ops.modifyColumn(
                            dorisTable, ColumnPath.of("id"), generatedColumn, null, 1L),
                    "Generated columns are not supported for Iceberg ADD/MODIFY COLUMN");
        }

        Mockito.verify(icebergTable, Mockito.never()).updateSchema();
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
    public void testReorderColumnsUsesCanonicalIcebergNames() throws Throwable {
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

            ops.reorderColumns(dorisTable, Arrays.asList("label", "id"), 1L);
        }

        Mockito.verify(updateSchema).moveFirst("Label");
        Mockito.verify(updateSchema).moveAfter("Id", "Label");
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

        Mockito.verify(updateSchema).updateColumn("arr.element", Types.LongType.get(), null);
        Mockito.verify(updateSchema).updateColumn("m.value", Types.LongType.get(), null);
        Mockito.verify(updateSchema, Mockito.never()).makeColumnOptional(Mockito.anyString());
        Mockito.verify(updateSchema, Mockito.times(2)).commit();
    }

    @Test
    public void testModifyColumnRejectsPositionForDirectArrayElementAndMapValue() {
        Schema schema = primitiveContainerSchema();
        ExternalTable dorisTable = Mockito.mock(ExternalTable.class);
        Table icebergTable = Mockito.mock(Table.class);
        Mockito.when(icebergTable.schema()).thenReturn(schema);

        try (MockedStatic<IcebergUtils> mockedIcebergUtils =
                Mockito.mockStatic(IcebergUtils.class, Mockito.CALLS_REAL_METHODS)) {
            mockedIcebergUtils.when(() -> IcebergUtils.getIcebergTable(dorisTable)).thenReturn(icebergTable);

            assertUserException(() -> ops.modifyColumn(dorisTable, ColumnPath.fromDotName("arr.element"),
                            new Column("element", Type.BIGINT, true), ColumnPosition.FIRST, 1L),
                    "Cannot apply column position to 'arr.element': parent column path 'arr' is not a struct");
            assertUserException(() -> ops.modifyColumn(dorisTable, ColumnPath.fromDotName("arr.element"),
                            new Column("element", Type.BIGINT, true), new ColumnPosition("element"), 1L),
                    "Cannot apply column position to 'arr.element': parent column path 'arr' is not a struct");
            assertUserException(() -> ops.modifyColumn(dorisTable, ColumnPath.fromDotName("m.value"),
                            new Column("value", Type.BIGINT, true), ColumnPosition.FIRST, 1L),
                    "Cannot apply column position to 'm.value': parent column path 'm' is not a struct");
            assertUserException(() -> ops.modifyColumn(dorisTable, ColumnPath.fromDotName("m.value"),
                            new Column("value", Type.BIGINT, true), new ColumnPosition("value"), 1L),
                    "Cannot apply column position to 'm.value': parent column path 'm' is not a struct");
        }

        Mockito.verify(icebergTable, Mockito.never()).updateSchema();
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
    public void testRejectsCommentsOnDirectArrayElementAndMapValue() {
        Schema schema = primitiveContainerSchema();
        ExternalTable dorisTable = Mockito.mock(ExternalTable.class);
        Table icebergTable = Mockito.mock(Table.class);
        Mockito.when(dorisTable.getRemoteDbName()).thenReturn("db");
        Mockito.when(icebergTable.schema()).thenReturn(schema);

        try (MockedStatic<IcebergUtils> mockedIcebergUtils =
                Mockito.mockStatic(IcebergUtils.class, Mockito.CALLS_REAL_METHODS)) {
            mockedIcebergUtils.when(() -> IcebergUtils.getIcebergTable(dorisTable)).thenReturn(icebergTable);

            assertUserException(() -> ops.modifyColumnComment(
                            dorisTable, ColumnPath.fromDotName("arr.element"), "array element comment", 1L),
                    "Iceberg does not support comments on collection element or value fields: arr.element");
            assertUserException(() -> ops.modifyColumnComment(
                            dorisTable, ColumnPath.fromDotName("m.value"), "map value comment", 1L),
                    "Iceberg does not support comments on collection element or value fields: m.value");
            assertUserException(() -> ops.modifyColumn(dorisTable, ColumnPath.fromDotName("arr.element"),
                            new Column("element", Type.BIGINT, true, "array element comment"), null, 1L),
                    "Iceberg does not support comments on collection element or value fields: arr.element");
            assertUserException(() -> ops.modifyColumn(dorisTable, ColumnPath.fromDotName("m.value"),
                            new Column("value", Type.BIGINT, true, "map value comment"), null, 1L),
                    "Iceberg does not support comments on collection element or value fields: m.value");
            Column arrayElementWithEmptyComment = new Column("element", Type.BIGINT, true, "");
            arrayElementWithEmptyComment.setCommentSpecified(true);
            assertUserException(() -> ops.modifyColumn(dorisTable, ColumnPath.fromDotName("arr.element"),
                            arrayElementWithEmptyComment, null, 1L),
                    "Iceberg does not support comments on collection element or value fields: arr.element");
            Column mapValueWithEmptyComment = new Column("value", Type.BIGINT, true, "");
            mapValueWithEmptyComment.setCommentSpecified(true);
            assertUserException(() -> ops.modifyColumn(dorisTable, ColumnPath.fromDotName("m.value"),
                            mapValueWithEmptyComment, null, 1L),
                    "Iceberg does not support comments on collection element or value fields: m.value");
            assertUserException(() -> ops.modifyColumnComment(
                            dorisTable, ColumnPath.fromDotName("m.key"), "map key comment", 1L),
                    "Cannot modify comment MAP key nested column");
        }

        Mockito.verify(icebergTable, Mockito.never()).updateSchema();
    }

    @Test
    public void testRejectsTopLevelRowLineageMutationsForV3Tables() {
        ExternalTable dorisTable = Mockito.mock(ExternalTable.class);
        Table icebergTable = Mockito.mock(Table.class);
        Mockito.when(icebergTable.properties()).thenReturn(Collections.singletonMap("format-version", "3"));

        try (MockedStatic<IcebergUtils> mockedIcebergUtils =
                Mockito.mockStatic(IcebergUtils.class, Mockito.CALLS_REAL_METHODS)) {
            mockedIcebergUtils.when(() -> IcebergUtils.getIcebergTable(dorisTable)).thenReturn(icebergTable);

            assertUserException(() -> ops.addColumn(dorisTable,
                            new Column("_row_id", Type.BIGINT, true), null, 1L),
                    "Cannot add Iceberg v3 reserved row lineage column: _row_id");
            assertUserException(() -> ops.addColumns(dorisTable, Collections.singletonList(
                            new Column("_last_updated_sequence_number", Type.BIGINT, true)), 1L),
                    "Cannot add Iceberg v3 reserved row lineage column: _last_updated_sequence_number");
            assertUserException(() -> ops.dropColumn(dorisTable, "_ROW_ID", 1L),
                    "Cannot drop Iceberg v3 reserved row lineage column: _ROW_ID");
            assertUserException(() -> ops.renameColumn(dorisTable, "_row_id", "renamed", 1L),
                    "Cannot rename Iceberg v3 reserved row lineage column: _row_id");
            assertUserException(() -> ops.renameColumn(
                            dorisTable, "id", "_last_updated_sequence_number", 1L),
                    "Cannot rename to Iceberg v3 reserved row lineage column: _last_updated_sequence_number");
            assertUserException(() -> ops.modifyColumn(dorisTable, ColumnPath.of("_row_id"),
                            new Column("_row_id", Type.BIGINT, true), null, 1L),
                    "Cannot modify Iceberg v3 reserved row lineage column: _row_id");
            assertUserException(() -> ops.modifyColumnComment(dorisTable,
                            ColumnPath.of("_last_updated_sequence_number"), "comment", 1L),
                    "Cannot modify comment for Iceberg v3 reserved row lineage column: "
                            + "_last_updated_sequence_number");
            assertUserException(() -> ops.reorderColumns(dorisTable,
                            Arrays.asList("_row_id", "id"), 1L),
                    "Cannot reorder Iceberg v3 reserved row lineage column: _row_id");
        }

        Mockito.verify(icebergTable, Mockito.never()).updateSchema();
    }

    @Test
    public void testAllowsV3NestedAndV2TopLevelRowLineageNames() throws Throwable {
        Schema v3Schema = new Schema(
                Types.NestedField.optional(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "s", Types.StructType.of(
                        Types.NestedField.optional(3, "source", Types.LongType.get()),
                        Types.NestedField.optional(4, "_row_id", Types.LongType.get()))));
        ExternalTable v3DorisTable = Mockito.mock(ExternalTable.class);
        Table v3IcebergTable = Mockito.mock(Table.class);
        UpdateSchema v3UpdateSchema = Mockito.mock(UpdateSchema.class);
        Mockito.when(v3DorisTable.getRemoteDbName()).thenReturn("db");
        Mockito.when(v3IcebergTable.properties()).thenReturn(Collections.singletonMap("format-version", "3"));
        Mockito.when(v3IcebergTable.schema()).thenReturn(v3Schema);
        Mockito.when(v3IcebergTable.updateSchema()).thenReturn(v3UpdateSchema);

        Schema v2Schema = new Schema(Types.NestedField.optional(1, "id", Types.IntegerType.get()));
        ExternalTable v2DorisTable = Mockito.mock(ExternalTable.class);
        Table v2IcebergTable = Mockito.mock(Table.class);
        UpdateSchema v2UpdateSchema = Mockito.mock(UpdateSchema.class);
        Mockito.when(v2DorisTable.getRemoteDbName()).thenReturn("db");
        Mockito.when(v2IcebergTable.properties()).thenReturn(Collections.singletonMap("format-version", "2"));
        Mockito.when(v2IcebergTable.schema()).thenReturn(v2Schema);
        Mockito.when(v2IcebergTable.updateSchema()).thenReturn(v2UpdateSchema);

        try (MockedStatic<IcebergUtils> mockedIcebergUtils =
                Mockito.mockStatic(IcebergUtils.class, Mockito.CALLS_REAL_METHODS)) {
            mockedIcebergUtils.when(() -> IcebergUtils.getIcebergTable(v3DorisTable)).thenReturn(v3IcebergTable);
            mockedIcebergUtils.when(() -> IcebergUtils.getIcebergTable(v2DorisTable)).thenReturn(v2IcebergTable);

            ops.addColumn(v3DorisTable, ColumnPath.fromDotName("s._last_updated_sequence_number"),
                    new Column("_last_updated_sequence_number", Type.BIGINT, true), null, 1L);
            ops.renameColumn(v3DorisTable, ColumnPath.fromDotName("s.source"), "_last_updated_sequence_number", 1L);
            ops.modifyColumn(v3DorisTable, ColumnPath.fromDotName("s._row_id"),
                    new Column("_row_id", Type.BIGINT, true), null, 1L);
            ops.modifyColumnComment(v3DorisTable, ColumnPath.fromDotName("s._row_id"), "comment", 1L);
            ops.dropColumn(v3DorisTable, ColumnPath.fromDotName("s._row_id"), 1L);

            ops.addColumn(v2DorisTable, new Column("_row_id", Type.BIGINT, true), null, 1L);
            ops.renameColumn(v2DorisTable, "id", "_last_updated_sequence_number", 1L);
        }

        Mockito.verify(v3UpdateSchema, Mockito.times(5)).commit();
        Mockito.verify(v2UpdateSchema, Mockito.times(2)).commit();
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
            Assert.assertTrue("expected UserException but got " + t, t instanceof UserException);
            Assert.assertTrue("expected message containing '" + expectedMessage + "' but was '"
                    + t.getMessage() + "'", t.getMessage().contains(expectedMessage));
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

    private Schema mappedPrimitiveSchema() {
        return new Schema(
                Types.NestedField.required(1, "top_uuid", Types.UUIDType.get()),
                Types.NestedField.required(2, "top_other", Types.IntegerType.get()),
                Types.NestedField.optional(3, "info", Types.StructType.of(
                        Types.NestedField.required(4, "uuid_value", Types.UUIDType.get()),
                        Types.NestedField.required(5, "tz_value", Types.TimestampType.withZone()),
                        Types.NestedField.required(6, "other", Types.IntegerType.get()))));
    }

    private Schema mappedComplexSchema() {
        return new Schema(Types.NestedField.optional(1, "outer", Types.StructType.of(
                Types.NestedField.optional(2, "payload", Types.StructType.of(
                        Types.NestedField.optional(3, "uuid_value", Types.UUIDType.get()),
                        Types.NestedField.optional(4, "binary_value", Types.BinaryType.get()),
                        Types.NestedField.optional(5, "fixed_value", Types.FixedType.ofLength(8)),
                        Types.NestedField.optional(6, "tz_value", Types.TimestampType.withZone()),
                        Types.NestedField.optional(7, "metric", Types.IntegerType.get()),
                        Types.NestedField.optional(8, "events",
                                Types.ListType.ofOptional(9, Types.UUIDType.get())),
                        Types.NestedField.optional(10, "attrs", Types.MapType.ofOptional(
                                11, 12, Types.FixedType.ofLength(4), Types.TimestampType.withZone())))))));
    }

    private StructType mappedPayloadDorisType(Type metricType, int fixedLength, Type mapKeyType) {
        return new StructType(
                new StructField("uuid_value", ScalarType.createVarbinaryType(16)),
                new StructField("binary_value",
                        ScalarType.createVarbinaryType(ScalarType.MAX_VARBINARY_LENGTH)),
                new StructField("fixed_value", ScalarType.createVarbinaryType(fixedLength)),
                new StructField("tz_value", ScalarType.createTimeStampTzType(6)),
                new StructField("metric", metricType),
                new StructField("events", ArrayType.create(
                        ScalarType.createVarbinaryType(16), true)),
                new StructField("attrs", new MapType(
                        mapKeyType, ScalarType.createTimeStampTzType(6))));
    }

    private Schema requiredNestedSchema() {
        return new Schema(Types.NestedField.required(1, "info", Types.StructType.of(
                Types.NestedField.required(2, "metric", Types.IntegerType.get()),
                Types.NestedField.required(3, "child", Types.StructType.of(
                        Types.NestedField.required(4, "value", Types.IntegerType.get()))),
                Types.NestedField.required(5, "events", Types.ListType.ofRequired(
                        6, Types.IntegerType.get())),
                Types.NestedField.required(7, "attrs", Types.MapType.ofRequired(
                        8, 9, Types.StringType.get(), Types.IntegerType.get())))));
    }
}
