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

package org.apache.doris.catalog;

import org.apache.doris.thrift.TTypeDesc;

import org.junit.Assert;
import org.junit.Test;

public class SpatialTypeTest {
    @Test
    public void testRoundTrip() {
        for (ScalarType type : new ScalarType[] {
                ScalarType.createGeometryType(), ScalarType.createGeometryType("EPSG:3857"),
                ScalarType.createGeographyType(), ScalarType.createGeographyType("OGC:CRS84", "ellipsoidal")}) {
            Type restored = Type.fromThrift(type.toThrift());
            Assert.assertEquals(type, restored);
            Assert.assertEquals(type.hashCode(), restored.hashCode());
            Assert.assertEquals(type.toSql(), restored.toSql());
            Assert.assertFalse(type.getPrimitiveType().isAvailableInDdl());
        }
    }

    @Test
    public void testParametersAffectIdentity() {
        Assert.assertNotEquals(ScalarType.createGeometryType(), ScalarType.createGeometryType("EPSG:3857"));
        Assert.assertNotEquals(ScalarType.createGeometryType(), ScalarType.createGeographyType());
        Assert.assertNotEquals(ScalarType.createGeographyType(),
                ScalarType.createGeographyType("OGC:CRS84", "ellipsoidal"));
        Assert.assertEquals("GEOMETRY('OGC:CRS84')", ScalarType.createGeometryType().toSql());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRejectMissingCrs() {
        TTypeDesc thrift = ScalarType.createGeometryType().toThrift();
        thrift.getTypes().get(0).getScalarType().unsetSpatialCrs();
        Type.fromThrift(thrift);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRejectEmptyAlgorithm() {
        ScalarType.createGeographyType("OGC:CRS84", "");
    }
}
