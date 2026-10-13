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
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.CaseWhen;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.WhenClause;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Coalesce;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.types.DataType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ConnectorSpatialWriteValidatorTest {
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
    public void testNestedSpatialTargetsAreRejectedForExistingSchemas() {
        ScalarType geometry = ScalarType.createGeometryType("OGC:CRS84");
        for (Type target : ImmutableList.of(new ArrayType(geometry), new MapType(Type.STRING, geometry),
                new StructType(new StructField("geom", geometry)))) {
            AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                    () -> ConnectorSpatialWriteValidator.validate(new Column("payload", target),
                            new SlotReference("payload", DataType.fromCatalogType(target))));
            Assertions.assertTrue(exception.getMessage().contains("Nested spatial"), exception.getMessage());
        }
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
