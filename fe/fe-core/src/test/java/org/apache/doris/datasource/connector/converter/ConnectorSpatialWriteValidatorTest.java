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

package org.apache.doris.datasource.connector.converter;

import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.StructField;
import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.Type;
import org.apache.doris.connector.spi.ConnectorColumn;
import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.CaseWhen;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.WhenClause;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Array;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Coalesce;
import org.apache.doris.nereids.trees.expressions.functions.scalar.CreateMap;
import org.apache.doris.nereids.trees.expressions.functions.scalar.CreateNamedStruct;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.functions.scalar.StGeogFromWKB;
import org.apache.doris.nereids.trees.expressions.literal.ArrayLiteral;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.MapLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StructLiteral;
import org.apache.doris.nereids.types.DataType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ConnectorSpatialWriteValidatorTest {
    @Test
    public void testImportedDefaultCrsCaseCanAcceptConstructorValues() {
        for (String crs : ImmutableList.of("ogc:crs84", "OgC:CrS84")) {
            ConnectorColumn external = new ConnectorColumn("shape", ConnectorType.of("GEOGRAPHY")
                    .withSpatialMetadata(crs, "spherical"), "", true, null);
            Column imported = ConnectorColumnConverter.convertColumn(external);
            Assertions.assertEquals("OGC:CRS84", ((ScalarType) imported.getType()).getSpatialCrs());
            ConnectorSpatialWriteValidator.validate(imported, new StGeogFromWKB(
                    new StringLiteral("0101000000000000000000F03F0000000000000040")));
        }
        Assertions.assertEquals("projjson:CustomCrs",
                ScalarType.createGeographyType("projjson:CustomCrs", "spherical").getSpatialCrs());
    }

    @Test
    public void testMatchingSpatialParametersAreAccepted() {
        validate(ScalarType.createGeometryType("EPSG:4326"),
                ScalarType.createGeometryType("EPSG:4326"));
        validate(ScalarType.createGeographyType("OGC:CRS84", "spherical"),
                ScalarType.createGeographyType("OGC:CRS84", "spherical"));
    }

    @Test
    public void testSpatialKindMismatchIsRejected() {
        assertRejected(ScalarType.createGeometryType("OGC:CRS84"),
                ScalarType.createGeographyType("OGC:CRS84", "spherical"), "type mismatch");
    }

    @Test
    public void testSpatialCrsMismatchIsRejected() {
        assertRejected(ScalarType.createGeometryType("EPSG:3857"),
                ScalarType.createGeometryType("EPSG:4326"), "CRS mismatch");
    }

    @Test
    public void testSpatialAlgorithmMismatchIsRejected() {
        assertRejected(ScalarType.createGeographyType("OGC:CRS84", "vincenty"),
                ScalarType.createGeographyType("OGC:CRS84", "spherical"), "algorithm mismatch");
    }

    @Test
    public void testNonSpatialSourceIsRejected() {
        assertRejected(Type.STRING, ScalarType.createGeometryType("OGC:CRS84"),
                "cannot convert input column");
    }

    @Test
    public void testMergeValidationInspectsSourceBeforeTargetCast() {
        ScalarType targetType = ScalarType.createGeometryType("EPSG:4326");
        DataType targetDataType = DataType.fromCatalogType(targetType);
        NamedExpression source = new Alias(new If(
                BooleanLiteral.TRUE,
                new Cast(new SlotReference("matching", targetDataType), targetDataType),
                new Cast(new SlotReference("mismatched", DataType.fromCatalogType(
                        ScalarType.createGeometryType("EPSG:3857"))), targetDataType)), "payload");
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> ConnectorSpatialWriteValidator.validate(
                        ImmutableList.of(new Column("payload", targetType)), ImmutableList.of(source)));
        Assertions.assertTrue(exception.getMessage().contains("CRS mismatch"), exception.getMessage());
        Assertions.assertTrue(exception.getMessage().contains("payload"), exception.getMessage());
    }

    @Test
    public void testNestedSpatialIdentityAndTypedNulls() {
        ScalarType geometry = ScalarType.createGeometryType("OGC:CRS84");
        ScalarType otherCrs = ScalarType.createGeometryType("EPSG:3857");
        for (Type target : ImmutableList.of(new ArrayType(geometry), new MapType(Type.STRING, geometry),
                new StructType(new StructField("geom", geometry)),
                new ArrayType(new MapType(Type.STRING, new StructType(new StructField("geom", geometry)))))) {
            Type mismatched;
            if (target instanceof ArrayType) {
                Type element = ((ArrayType) target).getItemType();
                mismatched = element instanceof MapType
                        ? new ArrayType(new MapType(Type.STRING, new StructType(new StructField("geom", otherCrs))))
                        : new ArrayType(otherCrs);
            } else if (target instanceof MapType) {
                mismatched = new MapType(Type.STRING, otherCrs);
            } else {
                mismatched = new StructType(new StructField("geom", otherCrs));
            }
            Column column = new Column("payload", target);
            DataType targetType = DataType.fromCatalogType(target);
            Assertions.assertDoesNotThrow(() -> ConnectorSpatialWriteValidator.validate(column,
                    new SlotReference("payload", targetType)));
            Assertions.assertDoesNotThrow(() -> ConnectorSpatialWriteValidator.validate(column,
                    new NullLiteral(DataType.fromCatalogType(mismatched))));
            AnalysisException error = Assertions.assertThrows(AnalysisException.class,
                    () -> ConnectorSpatialWriteValidator.validate(column,
                            new SlotReference("payload", DataType.fromCatalogType(mismatched))));
            Assertions.assertTrue(error.getMessage().contains("CRS mismatch"), error.getMessage());
        }
        DataType targetLeaf = DataType.fromCatalogType(geometry);
        Expression hiddenMismatch = new Array(new Cast(
                new SlotReference("wrong", DataType.fromCatalogType(otherCrs)), targetLeaf));
        Assertions.assertThrows(AnalysisException.class, () -> ConnectorSpatialWriteValidator.validate(
                new Column("payload", new ArrayType(geometry)), hiddenMismatch));
    }

    @Test
    public void testRequiredIcebergListRejectsNullSpatialElements() {
        ScalarType geometry = ScalarType.createGeometryType("OGC:CRS84");
        Column target = ConnectorColumnConverter.convertColumn(new ConnectorColumn("payload",
                ConnectorType.arrayOf(ConnectorType.of("GEOMETRY").withSpatialMetadata("OGC:CRS84", null), false),
                null, true, null));
        Expression nullElement = new Array(new NullLiteral(DataType.fromCatalogType(geometry)));
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> ConnectorSpatialWriteValidator.validate(target, nullElement));
        Assertions.assertTrue(exception.getMessage().contains("required array element"), exception.getMessage());
    }

    @Test
    public void testStructRequiredSpatialListsRejectNullConstructors() {
        for (String kind : ImmutableList.of("GEOMETRY", "GEOGRAPHY")) {
            ConnectorType list = spatialList(kind, false);
            Column target = importedColumn(ConnectorType.structOf(ImmutableList.of("items"),
                    ImmutableList.of(list)));
            Expression values = new CreateNamedStruct(new StringLiteral("items"),
                    new Array(new Cast(NullLiteral.INSTANCE, spatialLeaf(kind))));
            assertRequiredNullRejected(target, values, "payload.items[]");
        }
    }

    @Test
    public void testMapRequiredSpatialListsRejectNullConstructors() {
        for (String kind : ImmutableList.of("GEOMETRY", "GEOGRAPHY")) {
            Column target = importedColumn(ConnectorType.mapOf(ConnectorType.of("STRING"),
                    spatialList(kind, false)));
            Expression values = new CreateMap(new StringLiteral("ordinary-key"),
                    new Array(new Cast(NullLiteral.INSTANCE, spatialLeaf(kind))));
            assertRequiredNullRejected(target, values, "payload.value[]");
        }
    }

    @Test
    public void testNestedRequiredSpatialListsRejectFoldedNulls() {
        for (String kind : ImmutableList.of("GEOMETRY", "GEOGRAPHY")) {
            ArrayLiteral values = new ArrayLiteral(ImmutableList.of(new NullLiteral(spatialLeaf(kind))));
            Column structure = importedColumn(ConnectorType.structOf(ImmutableList.of("items"),
                    ImmutableList.of(spatialList(kind, false))));
            StructLiteral structValues = new StructLiteral(ImmutableList.of(values),
                    DataType.fromCatalogType(structure.getType()));
            assertRequiredNullRejected(structure, structValues, "payload.items[]");
            Column map = importedColumn(ConnectorType.mapOf(ConnectorType.of("STRING"), spatialList(kind, false)));
            MapLiteral mapValues = new MapLiteral(ImmutableMap.of(new StringLiteral("ordinary-key"), values));
            assertRequiredNullRejected(map, mapValues, "payload.value[]");
        }
    }

    @Test
    public void testNestedSpatialListsAcceptOptionalNullsAndRequiredValues() {
        for (String kind : ImmutableList.of("GEOMETRY", "GEOGRAPHY")) {
            for (boolean optional : ImmutableList.of(false, true)) {
                ConnectorType list = spatialList(kind, optional);
                Expression element = optional ? new Cast(NullLiteral.INSTANCE, spatialLeaf(kind))
                        : new SlotReference("valid_shape", spatialLeaf(kind));
                Column structure = importedColumn(ConnectorType.structOf(ImmutableList.of("items"),
                        ImmutableList.of(list)));
                Expression structValues = new CreateNamedStruct(new StringLiteral("items"), new Array(element));
                Assertions.assertDoesNotThrow(() -> ConnectorSpatialWriteValidator.validate(structure, structValues));
                Column map = importedColumn(ConnectorType.mapOf(ConnectorType.of("STRING"), list));
                Expression mapValues = new CreateMap(new StringLiteral("ordinary-key"), new Array(element));
                Assertions.assertDoesNotThrow(() -> ConnectorSpatialWriteValidator.validate(map, mapValues));
                Assertions.assertDoesNotThrow(() -> ConnectorSpatialWriteValidator.validate(structure,
                        new NullLiteral(DataType.fromCatalogType(structure.getType()))));
            }
        }
    }

    @Test
    public void testDeepRequiredSpatialListsAndBranchesRetainColumnMetadata() {
        for (String kind : ImmutableList.of("GEOMETRY", "GEOGRAPHY")) {
            ConnectorType structure = ConnectorType.structOf(ImmutableList.of("items"),
                    ImmutableList.of(spatialList(kind, false)));
            Column deep = importedColumn(ConnectorType.arrayOf(
                    ConnectorType.mapOf(ConnectorType.of("STRING"), structure), true));
            Expression values = new Array(new CreateMap(new StringLiteral("ordinary-key"),
                    new CreateNamedStruct(new StringLiteral("items"),
                            new Array(new Cast(NullLiteral.INSTANCE, spatialLeaf(kind))))));
            assertRequiredNullRejected(deep, values, "payload[].value.items[]");
            Column direct = importedColumn(spatialList(kind, false));
            Expression nestedCast = new Array(new Cast(new Cast(
                    new NullLiteral(spatialLeaf(kind)), spatialLeaf(kind)), spatialLeaf(kind)));
            assertRequiredNullRejected(direct, nestedCast, "payload[]");
            Expression invalid = new Array(new Cast(NullLiteral.INSTANCE, spatialLeaf(kind)));
            DataType arrayType = DataType.fromCatalogType(direct.getType());
            for (Expression wrapped : ImmutableList.of(new Alias(invalid, "wrapped"),
                    new Cast(invalid, arrayType),
                    new If(BooleanLiteral.TRUE, invalid, new NullLiteral(arrayType)),
                    new CaseWhen(ImmutableList.of(new WhenClause(BooleanLiteral.TRUE, invalid)),
                            new NullLiteral(arrayType)),
                    new Coalesce(invalid, new NullLiteral(arrayType)))) {
                assertRequiredNullRejected(direct, wrapped, "payload[]");
            }
        }
    }

    @Test
    public void testNestedOptionalSpatialListLiteralsRemainAccepted() {
        for (String kind : ImmutableList.of("GEOMETRY", "GEOGRAPHY")) {
            ArrayLiteral values = new ArrayLiteral(ImmutableList.of(new NullLiteral(spatialLeaf(kind))));
            Column structure = importedColumn(ConnectorType.structOf(ImmutableList.of("items"),
                    ImmutableList.of(spatialList(kind, true))));
            StructLiteral structValues = new StructLiteral(ImmutableList.of(values),
                    DataType.fromCatalogType(structure.getType()));
            Assertions.assertDoesNotThrow(() -> ConnectorSpatialWriteValidator.validate(structure, structValues));
            Column map = importedColumn(ConnectorType.mapOf(ConnectorType.of("STRING"), spatialList(kind, true)));
            MapLiteral mapValues = new MapLiteral(ImmutableMap.of(new StringLiteral("ordinary-key"), values));
            Assertions.assertDoesNotThrow(() -> ConnectorSpatialWriteValidator.validate(map, mapValues));
        }
    }

    private static ConnectorType spatialList(String kind, boolean optionalElements) {
        return ConnectorType.arrayOf(ConnectorType.of(kind).withSpatialMetadata("OGC:CRS84",
                kind.equals("GEOGRAPHY") ? "spherical" : null), optionalElements);
    }

    private static DataType spatialLeaf(String kind) {
        return DataType.fromCatalogType(kind.equals("GEOGRAPHY")
                ? ScalarType.createGeographyType("OGC:CRS84", "spherical")
                : ScalarType.createGeometryType("OGC:CRS84"));
    }

    private static Column importedColumn(ConnectorType type) {
        return ConnectorColumnConverter.convertColumn(new ConnectorColumn("payload", type, null, true, null));
    }

    private static void assertRequiredNullRejected(Column target, Expression source, String path) {
        AnalysisException error = Assertions.assertThrows(AnalysisException.class,
                () -> ConnectorSpatialWriteValidator.validate(target, source));
        Assertions.assertTrue(error.getMessage().contains("required array element"), error.getMessage());
        Assertions.assertTrue(error.getMessage().contains(path), error.getMessage());
    }

    @Test
    public void testTransparentWrappersDoNotHideCrsMismatch() {
        ScalarType target = ScalarType.createGeometryType("OGC:CRS84");
        DataType targetDataType = DataType.fromCatalogType(target);
        Expression mismatched = new Cast(new SlotReference("projected", DataType.fromCatalogType(
                ScalarType.createGeometryType("EPSG:3857"))), targetDataType);
        Expression matching = new SlotReference("matching", targetDataType);
        for (Expression source : ImmutableList.of(new Cast(mismatched, targetDataType),
                new CaseWhen(ImmutableList.of(new WhenClause(BooleanLiteral.TRUE, matching)), mismatched),
                new Coalesce(matching, mismatched))) {
            AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                    () -> ConnectorSpatialWriteValidator.validate(new Column("payload", target), source));
            Assertions.assertTrue(exception.getMessage().contains("CRS mismatch"), exception.getMessage());
        }
    }

    @Test
    public void testTransparentWrappersAcceptMatchingValuesAndNulls() {
        ScalarType target = ScalarType.createGeometryType("OGC:CRS84");
        DataType targetDataType = DataType.fromCatalogType(target);
        Expression matching = new SlotReference("matching", targetDataType);
        Expression castNull = new Cast(NullLiteral.INSTANCE, targetDataType);
        for (Expression source : ImmutableList.of(NullLiteral.INSTANCE, castNull,
                new Cast(new Cast(matching, targetDataType), targetDataType),
                new If(BooleanLiteral.TRUE, matching, castNull),
                new CaseWhen(ImmutableList.of(new WhenClause(BooleanLiteral.TRUE, matching)), castNull),
                new Coalesce(matching, castNull))) {
            Assertions.assertDoesNotThrow(() -> ConnectorSpatialWriteValidator.validate(
                    new Column("payload", target), source));
        }
    }

    private static void assertRejected(Type sourceType, ScalarType targetType, String message) {
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> validate(sourceType, targetType));
        Assertions.assertTrue(exception.getMessage().contains("payload"), exception.getMessage());
        Assertions.assertTrue(exception.getMessage().contains(message), exception.getMessage());
    }

    private static void validate(Type sourceType, ScalarType targetType) {
        NamedExpression source = new Alias(
                new SlotReference("source", DataType.fromCatalogType(sourceType)), "payload");
        ConnectorSpatialWriteValidator.validate(ImmutableList.of(new Column("payload", targetType)),
                ImmutableList.of(source));
    }
}
