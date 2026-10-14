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

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.BoundFunction;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.GeographyType;
import org.apache.doris.nereids.types.GeometryType;
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
    public void testSpatialFunctionsPreserveParameterizedInputTypes() {
        GeometryType geometry = new GeometryType("EPSG:3857");
        GeographyType geography = new GeographyType("OGC:CRS84", "vincenty");
        SlotReference geometrySlot = new SlotReference("geometry", geometry);
        SlotReference geographySlot = new SlotReference("geography", geography);

        Assertions.assertEquals(geometry, new StAswkt(geometrySlot).expectedInputTypes().get(0));
        Assertions.assertEquals(geometry, new StAsBinary(geometrySlot).expectedInputTypes().get(0));
        Assertions.assertEquals(geometry, new StGeometryType(geometrySlot).expectedInputTypes().get(0));
        Assertions.assertEquals(geometry, new StX(geometrySlot).expectedInputTypes().get(0));
        Assertions.assertEquals(geometry, new StY(geometrySlot).expectedInputTypes().get(0));
        Assertions.assertEquals(geography, new StLength(geographySlot).expectedInputTypes().get(0));

        Assertions.assertEquals(geometry,
                new StDistance(geometrySlot, geographySlot).expectedInputTypes().get(0));
        Assertions.assertEquals(geography,
                new StDistance(geometrySlot, geographySlot).expectedInputTypes().get(1));
        Assertions.assertEquals(geometry,
                new StContains(geometrySlot, geographySlot).expectedInputTypes().get(0));
        Assertions.assertEquals(geometry,
                new StIntersects(geometrySlot, geographySlot).expectedInputTypes().get(0));
        Assertions.assertEquals(geometry,
                new StDisjoint(geometrySlot, geographySlot).expectedInputTypes().get(0));
        Assertions.assertEquals(geometry,
                new StTouches(geometrySlot, geographySlot).expectedInputTypes().get(0));
        Assertions.assertEquals(geography,
                new StAzimuth(geometrySlot, geographySlot).expectedInputTypes().get(1));
        Assertions.assertEquals(geometry,
                new StWithin(geometrySlot, geographySlot).expectedInputTypes().get(0));
        StAngle angle = new StAngle(geometrySlot, geographySlot, geometrySlot);
        Assertions.assertEquals(geometry, angle.expectedInputTypes().get(0));
        Assertions.assertEquals(geography, angle.expectedInputTypes().get(1));
        Assertions.assertEquals(geometry, angle.expectedInputTypes().get(2));
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
        for (DataType type : List.of(new GeometryType("EPSG:3857"),
                new GeographyType("OGC:CRS84", "vincenty"))) {
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
    }

    @Test
    public void testSpatialAngleCoercesLiteralNullInEveryPosition() {
        GeographyType type = new GeographyType("OGC:CRS84", "vincenty");
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
}
