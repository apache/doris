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

import org.junit.Assert;
import org.junit.Test;

public class SpatialConstructorFunctionTest {
    @Test
    public void testGeomFromWkbReturnsDefaultGeometry() {
        GeometryType defaultGeometry = new GeometryType("OGC:CRS84");
        Assert.assertEquals(defaultGeometry,
                new StGeomFromWKB(new StringLiteral("0101000000000000000000F03F0000000000000040"))
                        .getDataType());
        Assert.assertEquals(defaultGeometry,
                new StGeometryFromWKB(new StringLiteral("0101000000000000000000F03F0000000000000040"))
                        .getDataType());
    }

    @Test
    public void testGeogFromWkbReturnsDefaultGeography() {
        GeographyType defaultGeography = new GeographyType("OGC:CRS84", "spherical");
        Assert.assertEquals(defaultGeography,
                new StGeogFromWKB(new StringLiteral("0101000000000000000000F03F0000000000000040"))
                        .getDataType());
    }

    @Test
    public void testAsTextAcceptsGeometry() {
        GeometryType defaultGeometry = new GeometryType("OGC:CRS84");
        StAstext asText = new StAstext(new StGeomFromWKB(
                new StringLiteral("0101000000000000000000F03F0000000000000040")));
        Assert.assertEquals(VarcharType.SYSTEM_DEFAULT,
                asText.getDataType());
        Assert.assertEquals(defaultGeometry, asText.expectedInputTypes().get(0));
    }

    @Test
    public void testAsTextPreservesGeometryCrs() {
        GeometryType webMercator = new GeometryType("EPSG:3857");
        StAstext asText = new StAstext(new SlotReference("spatial", webMercator));
        Assert.assertEquals(VarcharType.SYSTEM_DEFAULT, asText.getDataType());
        Assert.assertEquals(webMercator, asText.expectedInputTypes().get(0));
    }

    @Test
    public void testAsTextPreservesGeographyMetadata() {
        GeographyType geography = new GeographyType("OGC:CRS84", "vincenty");
        StAstext asText = new StAstext(new SlotReference("spatial", geography));
        Assert.assertEquals(VarcharType.SYSTEM_DEFAULT, asText.getDataType());
        Assert.assertEquals(geography, asText.expectedInputTypes().get(0));
    }
}
