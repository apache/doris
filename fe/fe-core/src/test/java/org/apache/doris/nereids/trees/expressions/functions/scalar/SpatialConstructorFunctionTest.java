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

package org.apache.doris.nereids.trees.expressions.functions.scalar;

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.BoundFunction;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.GeographyType;
import org.apache.doris.nereids.types.GeometryType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.VarBinaryType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class SpatialConstructorFunctionTest {
    @Test
    public void testTypedGeomFromWkbReturnsDefaultGeometry() {
        GeometryType defaultGeometry = new GeometryType("OGC:CRS84");
        Assertions.assertEquals(defaultGeometry,
                new StGeometryFromWKBTyped(new StringLiteral("0101000000000000000000F03F0000000000000040"))
                        .getDataType());
    }

    @Test
    public void testGeogFromWkbReturnsDefaultGeography() {
        GeographyType defaultGeography = new GeographyType("OGC:CRS84", "spherical");
        Assertions.assertEquals(defaultGeography,
                new StGeogFromWKB(new StringLiteral("0101000000000000000000F03F0000000000000040"))
                        .getDataType());
    }

    @Test
    public void testAsTextAcceptsGeometry() {
        GeometryType defaultGeometry = new GeometryType("OGC:CRS84");
        StAstext asText = new StAstext(new StGeometryFromWKBTyped(
                new StringLiteral("0101000000000000000000F03F0000000000000040")));
        Assertions.assertEquals(VarcharType.SYSTEM_DEFAULT,
                asText.getDataType());
        Assertions.assertEquals(defaultGeometry, asText.expectedInputTypes().get(0));
    }

    @Test
    public void testTypedAsBinaryReturnsBinaryCarrier() {
        for (DataType type : List.of(new GeometryType("EPSG:3857"),
                new GeographyType("OGC:CRS84", "vincenty"))) {
            StAsBinary function = new StAsBinary(new SlotReference("shape", type));
            Assertions.assertEquals(VarBinaryType.INSTANCE, function.getDataType());
            Assertions.assertEquals(type, function.expectedInputTypes().get(0));
        }
        Assertions.assertEquals(VarcharType.SYSTEM_DEFAULT,
                new StAsBinary(new StringLiteral("legacy geometry")).getDataType());
    }

    @Test
    public void testAsTextPreservesGeometryCrs() {
        GeometryType webMercator = new GeometryType("EPSG:3857");
        StAstext asText = new StAstext(new SlotReference("spatial", webMercator));
        Assertions.assertEquals(VarcharType.SYSTEM_DEFAULT, asText.getDataType());
        Assertions.assertEquals(webMercator, asText.expectedInputTypes().get(0));
    }

    @Test
    public void testAsTextPreservesGeographyMetadata() {
        GeographyType geography = new GeographyType("OGC:CRS84", "vincenty");
        StAstext asText = new StAstext(new SlotReference("spatial", geography));
        Assertions.assertEquals(VarcharType.SYSTEM_DEFAULT, asText.getDataType());
        Assertions.assertEquals(geography, asText.expectedInputTypes().get(0));
    }

    @Test
    public void testSpatialAccessorsPreserveParameterizedInputTypes() {
        for (DataType type : List.of(new GeometryType("OGC:CRS84"), new GeometryType("EPSG:3857"),
                new GeographyType("OGC:CRS84", "spherical"),
                new GeographyType("EPSG:3857", "spherical"),
                new GeographyType("OGC:CRS84", "vincenty"))) {
            for (boolean nullable : List.of(false, true)) {
                for (Expression argument : List.of(new SlotReference("shape", type, nullable),
                        new NullLiteral(type))) {
                    for (ScalarFunction function : List.of(new StAstext(argument), new StAswkt(argument),
                            new StAsBinary(argument), new StGeometryType(argument),
                            new StX(argument), new StY(argument))) {
                        Expression coerced = TypeCoercionUtils.processBoundFunction(function);
                        Assertions.assertSame(argument, coerced.child(0));
                        Assertions.assertEquals(type, function.expectedInputTypes().get(0));
                    }
                }
            }
        }
    }

    @Test
    public void testSpatialCalculationsRejectUnsupportedMetadata() {
        SlotReference supported = new SlotReference("geog", new GeographyType("OGC:CRS84", "spherical"));
        for (DataType type : List.of(new GeometryType("OGC:CRS84"), new GeometryType("EPSG:3857"),
                new GeographyType("EPSG:3857", "spherical"),
                new GeographyType("OGC:CRS84", "vincenty"))) {
            for (Expression argument : List.of(new SlotReference("shape", type), new NullLiteral(type))) {
                for (ScalarFunction function : List.of(new StLength(argument), new StAreaSquareMeters(argument),
                        new StAreaSquareKm(argument), new StDistance(argument, supported),
                        new StDistance(supported, argument), new StWithin(argument, NullLiteral.INSTANCE),
                        new StAzimuth(NullLiteral.INSTANCE, argument), new StContains(argument, supported),
                        new StIntersects(argument, supported), new StDisjoint(supported, argument),
                        new StTouches(argument, supported), new StEquals(argument, supported),
                        new StOverlaps(supported, argument),
                        new StDWithin(argument, supported, new IntegerLiteral(1)),
                        new StAngle(supported, argument, supported))) {
                    Assertions.assertThrows(AnalysisException.class,
                            () -> TypeCoercionUtils.processBoundFunction(function));
                }
            }
        }
    }

    @Test
    public void testSpatialAreaPreservesMetadataDuringCoercion() {
        GeographyType type = new GeographyType("OGC:CRS84", "spherical");
        for (boolean nullable : List.of(false, true)) {
            for (Expression argument : List.of(new SlotReference("shape", type, nullable),
                    new NullLiteral(type))) {
                for (ScalarFunction function : List.of(new StAreaSquareMeters(argument),
                        new StAreaSquareKm(argument))) {
                    Expression coerced = TypeCoercionUtils.processBoundFunction(function);
                    Assertions.assertSame(argument, coerced.child(0));
                    Assertions.assertEquals(type, function.expectedInputTypes().get(0));
                    Assertions.assertEquals(DoubleType.INSTANCE, coerced.getDataType());
                    Assertions.assertTrue(coerced.nullable());
                }
            }
        }
    }

    @Test
    public void testAreaRetainsLegacyStringAndNullSignatures() {
        for (Expression argument : List.of(new SlotReference("legacy", VarcharType.SYSTEM_DEFAULT),
                new StringLiteral("legacy geometry"), NullLiteral.INSTANCE)) {
            for (ScalarFunction function : List.of(new StAreaSquareMeters(argument),
                    new StAreaSquareKm(argument))) {
                Expression coerced = TypeCoercionUtils.processBoundFunction(function);
                Assertions.assertEquals(VarcharType.SYSTEM_DEFAULT, function.expectedInputTypes().get(0));
                Assertions.assertEquals(DoubleType.INSTANCE, coerced.getDataType());
                Assertions.assertTrue(coerced.nullable());
            }
        }
    }

    @Test
    public void testHistoricalWkbConstructorsKeepStringReturnTypes() {
        StringLiteral ewkb = new StringLiteral("01010000208A11000068270210774C5D40B8DECA334C3B4240");
        Assertions.assertEquals(VarcharType.SYSTEM_DEFAULT, new StGeometryFromWKB(ewkb).getDataType());
        Assertions.assertEquals(VarcharType.SYSTEM_DEFAULT, new StGeomFromWKB(ewkb).getDataType());
        StAstext text = new StAstext(new StGeometryFromWKB(ewkb));
        Assertions.assertEquals(VarcharType.SYSTEM_DEFAULT, text.expectedInputTypes().get(0));
        Assertions.assertDoesNotThrow(() -> TypeCoercionUtils.processBoundFunction(text));
    }

    @Test
    public void testSpatialBinaryFunctionsCoerceLiteralNullWithSpatialMetadata() {
        GeographyType type = new GeographyType("OGC:CRS84", "spherical");
        SlotReference slot = new SlotReference("spatial", type);
        for (int position = 0; position < 2; position++) {
            Expression left = position == 0 ? NullLiteral.INSTANCE : slot;
            Expression right = position == 1 ? NullLiteral.INSTANCE : slot;
            for (BoundFunction function : List.of(new StDistance(left, right), new StWithin(left, right),
                    new StAzimuth(left, right))) {
                Expression coerced = TypeCoercionUtils.processBoundFunction(function);
                Assertions.assertEquals(type, coerced.child(position).getDataType());
                Assertions.assertEquals(type, coerced.child(1 - position).getDataType());
            }
        }
    }

    @Test
    public void testSpatialAngleCoercesLiteralNullInEveryPosition() {
        GeographyType type = new GeographyType("OGC:CRS84", "spherical");
        SlotReference slot = new SlotReference("geography", type);
        for (int position = 0; position < 3; position++) {
            Expression[] arguments = {slot, slot, slot};
            arguments[position] = NullLiteral.INSTANCE;
            StAngle function = new StAngle(arguments[0], arguments[1], arguments[2]);
            Expression coerced = TypeCoercionUtils.processBoundFunction(function);
            for (int i = 0; i < 3; i++) {
                Assertions.assertEquals(type, coerced.child(i).getDataType());
            }
        }
    }

    @Test
    public void testNewGeographyRelationsPreserveTypesAndRejectStringCarriers() {
        GeographyType geography = new GeographyType("OGC:CRS84", "spherical");
        SlotReference value = new SlotReference("shape", geography);
        for (ScalarFunction function : List.of(new StEquals(value, value), new StOverlaps(value, value),
                new StDWithin(value, value, new IntegerLiteral(1)))) {
            function.checkLegalityBeforeTypeCoercion();
            Assertions.assertEquals(geography, function.expectedInputTypes().get(0));
            Assertions.assertEquals(geography, function.expectedInputTypes().get(1));
            Assertions.assertEquals(BooleanType.INSTANCE, function.getDataType());
        }
        Assertions.assertEquals(DoubleType.INSTANCE,
                new StDWithin(value, value,
                        new IntegerLiteral(1))
                        .expectedInputTypes().get(2));
        SlotReference text = new SlotReference("text", StringType.INSTANCE);
        for (ScalarFunction function : List.of(new StEquals(text, value), new StOverlaps(value, text),
                new StDWithin(text, value, new IntegerLiteral(1)))) {
            Assertions.assertThrows(AnalysisException.class,
                    function::checkLegalityBeforeTypeCoercion);
        }
    }

}
