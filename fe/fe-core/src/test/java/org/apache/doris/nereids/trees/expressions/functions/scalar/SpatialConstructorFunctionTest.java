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

import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.types.GeographyType;
import org.apache.doris.nereids.types.GeometryType;
import org.apache.doris.nereids.types.VarcharType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SpatialConstructorFunctionTest {
    @Test
    public void testGeomFromWkbReturnsDefaultGeometry() {
        GeometryType defaultGeometry = new GeometryType("OGC:CRS84");
        Assertions.assertEquals(defaultGeometry,
                new StGeomFromWKB(new StringLiteral("0101000000000000000000F03F0000000000000040"))
                        .getDataType());
        Assertions.assertEquals(defaultGeometry,
                new StGeometryFromWKB(new StringLiteral("0101000000000000000000F03F0000000000000040"))
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
        StAstext asText = new StAstext(new StGeomFromWKB(
                new StringLiteral("0101000000000000000000F03F0000000000000040")));
        Assertions.assertEquals(VarcharType.SYSTEM_DEFAULT,
                asText.getDataType());
        Assertions.assertEquals(defaultGeometry, asText.expectedInputTypes().get(0));
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
    }
}
