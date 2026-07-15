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
import org.apache.doris.analysis.DefaultValueExprDef;
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
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
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
                    "is not supported for Iceberg column metric");
            assertUserException(() -> ops.modifyColumn(dorisTable, ColumnPath.fromDotName("info.child"),
                            new Column("child", unsupportedStruct, false), null, 1L),
                    "is not supported for Iceberg column child");
            assertUserException(() -> ops.modifyColumn(dorisTable, ColumnPath.fromDotName("info.events"),
                            new Column("events", unsupportedArray, false), null, 1L),
                    "is not supported for Iceberg column events");
            assertUserException(() -> ops.modifyColumn(dorisTable, ColumnPath.fromDotName("info.attrs"),
                            new Column("attrs", unsupportedMap, false), null, 1L),
                    "is not supported for Iceberg column attrs");
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

        Mockito.verify(updateSchema).updateColumn("info.metric", Types.LongType.get(), "");
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

        Mockito.verify(updateSchema).updateColumn("Id", Types.LongType.get(), "");
        Mockito.verify(updateSchema).updateColumn("Payload.Value", Types.LongType.get(), null);
        Mockito.verify(updateSchema, Mockito.never()).makeColumnOptional(Mockito.anyString());
        Mockito.verify(updateSchema, Mockito.times(2)).commit();
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
    public void testModifyColumnRejectsDefaultMetadata() {
        Schema schema = nestedSchema();
        ExternalTable dorisTable = Mockito.mock(ExternalTable.class);
        Table icebergTable = Mockito.mock(Table.class);
        UpdateSchema updateSchema = Mockito.mock(UpdateSchema.class);
        Mockito.when(icebergTable.schema()).thenReturn(schema);
        Mockito.when(icebergTable.updateSchema()).thenReturn(updateSchema);

        Column topLevelDefaultColumn = new Column("id", Type.BIGINT, false, null, true, "7", "");
        Column nestedDefaultColumn = new Column("a", Type.BIGINT, false, null, true, "7", "");
        Column topLevelOnUpdateColumn = Mockito.spy(new Column("id", Type.BIGINT, true));
        Column nestedOnUpdateColumn = Mockito.spy(new Column("a", Type.BIGINT, true));
        Mockito.doReturn(true).when(topLevelOnUpdateColumn).hasOnUpdateDefaultValue();
        Mockito.doReturn(true).when(nestedOnUpdateColumn).hasOnUpdateDefaultValue();

        try (MockedStatic<IcebergUtils> mockedIcebergUtils =
                Mockito.mockStatic(IcebergUtils.class, Mockito.CALLS_REAL_METHODS)) {
            mockedIcebergUtils.when(() -> IcebergUtils.getIcebergTable(dorisTable)).thenReturn(icebergTable);

            assertUserException(() -> ops.modifyColumn(dorisTable, ColumnPath.of("id"),
                            topLevelDefaultColumn, null, 1L),
                    "Modifying default values is not supported for Iceberg columns: id");
            assertUserException(() -> ops.modifyColumn(dorisTable, ColumnPath.of("id"),
                            topLevelOnUpdateColumn, null, 1L),
                    "Modifying default values is not supported for Iceberg columns: id");
            assertUserException(() -> ops.modifyColumn(dorisTable, ColumnPath.fromDotName("s.a"),
                            nestedDefaultColumn, null, 1L),
                    "Modifying default values is not supported for Iceberg columns: s.a");
            assertUserException(() -> ops.modifyColumn(dorisTable, ColumnPath.fromDotName("s.a"),
                            nestedOnUpdateColumn, null, 1L),
                    "Modifying default values is not supported for Iceberg columns: s.a");
        }

        Mockito.verifyNoInteractions(updateSchema);
    }

    @Test
    public void testAddColumnRejectsOnUpdateMetadata() {
        ExternalTable dorisTable = Mockito.mock(ExternalTable.class);
        Table icebergTable = Mockito.mock(Table.class);
        Column onUpdateColumn = Mockito.spy(new Column("new_col", Type.DATETIME, true));
        Mockito.doReturn(true).when(onUpdateColumn).hasOnUpdateDefaultValue();

        try (MockedStatic<IcebergUtils> mockedIcebergUtils =
                Mockito.mockStatic(IcebergUtils.class, Mockito.CALLS_REAL_METHODS)) {
            mockedIcebergUtils.when(() -> IcebergUtils.getIcebergTable(dorisTable)).thenReturn(icebergTable);

            assertUserException(() -> ops.addColumn(dorisTable, ColumnPath.of("new_col"),
                            onUpdateColumn, null, 1L),
                    "ON UPDATE is not supported for Iceberg ADD COLUMN: new_col");
            assertUserException(() -> ops.addColumn(dorisTable, ColumnPath.fromDotName("s.new_col"),
                            onUpdateColumn, null, 1L),
                    "ON UPDATE is not supported for Iceberg ADD COLUMN: new_col");
            assertUserException(() -> ops.addColumns(dorisTable, Collections.singletonList(onUpdateColumn), 1L),
                    "ON UPDATE is not supported for Iceberg ADD COLUMN: new_col");
        }

        Mockito.verify(icebergTable, Mockito.never()).updateSchema();
    }

    @Test
    public void testAddColumnRejectsUnsupportedDefaultsBeforeUpdateSchema() {
        ExternalTable dorisTable = Mockito.mock(ExternalTable.class);
        Table icebergTable = Mockito.mock(Table.class);
        Column currentTimestamp = Mockito.spy(new Column("event_time", Type.DATETIME, false, null, true,
                "CURRENT_TIMESTAMP", ""));
        Column currentDate = Mockito.spy(new Column("event_date", Type.DATEV2, false, null, true,
                "CURRENT_DATE", ""));
        Column complexDefault = new Column("items", ArrayType.create(Type.INT, true), false, null, true,
                "[]", "");
        Mockito.doReturn(new DefaultValueExprDef("now"))
                .when(currentTimestamp).getDefaultValueExprDef();
        Mockito.doReturn(new DefaultValueExprDef("current_date"))
                .when(currentDate).getDefaultValueExprDef();

        try (MockedStatic<IcebergUtils> mockedIcebergUtils =
                Mockito.mockStatic(IcebergUtils.class, Mockito.CALLS_REAL_METHODS)) {
            mockedIcebergUtils.when(() -> IcebergUtils.getIcebergTable(dorisTable)).thenReturn(icebergTable);

            assertUserException(() -> ops.addColumn(dorisTable, currentTimestamp, null, 1L),
                    "Dynamic default value CURRENT_TIMESTAMP is not supported for Iceberg ADD COLUMN: event_time");
            assertUserException(() -> ops.addColumn(dorisTable, ColumnPath.fromDotName("s.event_date"),
                            currentDate, null, 1L),
                    "Dynamic default value CURRENT_DATE is not supported for Iceberg ADD COLUMN: event_date");
            assertUserException(() -> ops.addColumn(dorisTable, complexDefault, null, 1L),
                    "Complex type default value only supports NULL");
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
    public void testNestedAddColumnConvertsDorisDefaultsToIcebergLiterals() throws Throwable {
        Schema schema = new Schema(Types.NestedField.optional(1, "s", Types.StructType.of(
                Types.NestedField.optional(2, "existing", Types.IntegerType.get()))));
        ExternalTable dorisTable = Mockito.mock(ExternalTable.class);
        Table icebergTable = Mockito.mock(Table.class);
        UpdateSchema updateSchema = Mockito.mock(UpdateSchema.class);
        Mockito.when(dorisTable.getRemoteDbName()).thenReturn("db");
        Mockito.when(icebergTable.schema()).thenReturn(schema);
        Mockito.when(icebergTable.updateSchema()).thenReturn(updateSchema);

        Column flag = new Column("flag", Type.BOOLEAN, false, null, true, "1", "");
        Column eventDate = new Column("event_date", Type.DATEV2, false, null, true,
                "2024-01-02", "");
        Column eventTime = new Column("event_time", ScalarType.createDatetimeV2Type(6), false, null, true,
                "2024-01-02 03:04:05.123456", "");

        try (MockedStatic<IcebergUtils> mockedIcebergUtils =
                Mockito.mockStatic(IcebergUtils.class, Mockito.CALLS_REAL_METHODS)) {
            mockedIcebergUtils.when(() -> IcebergUtils.getIcebergTable(dorisTable)).thenReturn(icebergTable);

            ops.addColumn(dorisTable, ColumnPath.fromDotName("s.flag"), flag, null, 1L);
            ops.addColumn(dorisTable, ColumnPath.fromDotName("s.event_date"), eventDate, null, 1L);
            ops.addColumn(dorisTable, ColumnPath.fromDotName("s.event_time"), eventTime, null, 1L);
        }

        int expectedDate = Math.toIntExact(LocalDate.of(2024, 1, 2).toEpochDay());
        long expectedTimestamp = ChronoUnit.MICROS.between(
                LocalDateTime.of(1970, 1, 1, 0, 0),
                LocalDateTime.of(2024, 1, 2, 3, 4, 5, 123456000));
        Mockito.verify(updateSchema).addColumn(Mockito.eq("s"), Mockito.eq("flag"),
                Mockito.eq(Types.BooleanType.get()), Mockito.eq(""),
                Mockito.<Literal<?>>argThat(literal -> Boolean.TRUE.equals(literal.value())));
        Mockito.verify(updateSchema).addColumn(Mockito.eq("s"), Mockito.eq("event_date"),
                Mockito.eq(Types.DateType.get()), Mockito.eq(""),
                Mockito.<Literal<?>>argThat(literal -> Integer.valueOf(expectedDate).equals(literal.value())));
        Mockito.verify(updateSchema).addColumn(Mockito.eq("s"), Mockito.eq("event_time"),
                Mockito.eq(Types.TimestampType.withoutZone()), Mockito.eq(""),
                Mockito.<Literal<?>>argThat(literal -> Long.valueOf(expectedTimestamp).equals(literal.value())));
        Mockito.verify(updateSchema, Mockito.times(3)).commit();
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

        Mockito.verify(updateSchema).updateColumn("arr.element", Types.LongType.get(), "");
        Mockito.verify(updateSchema).updateColumn("m.value", Types.LongType.get(), "");
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
