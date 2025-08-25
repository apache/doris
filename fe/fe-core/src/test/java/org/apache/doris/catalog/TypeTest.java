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

import org.junit.Assert;
import org.junit.Test;

public class TypeTest {

    // ===================== ArrayType =====================
    @Test
    public void testArrayOfArrayExactMatch() {
        ArrayType a1 = new ArrayType(new ArrayType(Type.INT, true), true);
        ArrayType a2 = new ArrayType(new ArrayType(Type.INT, true), true);
        Assert.assertTrue(Type.matchExactType(a1, a2));

        // inner type mismatch
        ArrayType a3 = new ArrayType(new ArrayType(Type.BIGINT, true), true);
        Assert.assertFalse(Type.matchExactType(a1, a3));

        // containsNull differs -> matchesType fails
        ArrayType a4 = new ArrayType(new ArrayType(Type.INT, true), false);
        Assert.assertFalse(Type.matchExactType(a1, a4));

        // array nested decimal test
        ArrayType a5 = new ArrayType(new ArrayType(ScalarType.createDecimalV3Type(8, 2), true), true);
        ArrayType a6 = new ArrayType(new ArrayType(ScalarType.createDecimalV3Type(9, 2), true), true);
        ArrayType a7 = new ArrayType(new ArrayType(ScalarType.createDecimalV3Type(-1, -1), true), true);
        Assert.assertFalse(Type.matchExactType(a5, a6, false));
        Assert.assertFalse(Type.matchExactType(a5, a6, true));
        Assert.assertFalse(Type.matchExactType(a6, a7, false));
    }

    // ===================== MapType =====================
    @Test
    public void testMapWithNestedValueExactMatch() {
        ScalarType d10s2 = ScalarType.createDecimalV3Type(10, 2); // DECIMAL64 range
        ArrayType arrayOfD = new ArrayType(d10s2, true);
        MapType m1 = new MapType(Type.INT, arrayOfD, true, true);
        MapType m2 = new MapType(Type.INT, new ArrayType(ScalarType.createDecimalV3Type(10, 2), true), true, true);
        Assert.assertTrue(Type.matchExactType(m1, m2));

        // value decimal precision differs, same scale
        MapType m3 = new MapType(Type.INT, new ArrayType(ScalarType.createDecimalV3Type(12, 2), true), true, true);
        // ignorePrecision = false -> not match
        Assert.assertFalse(Type.matchExactType(m1, m3, false));
        Assert.assertFalse(Type.matchExactType(m1, m3, true));

        // key/value containsNull differs -> doesn't matter for matching
        MapType m4 = new MapType(Type.INT, arrayOfD, false, true);
        Assert.assertTrue(Type.matchExactType(m1, m4));
    }

    // ===================== StructType =====================
    @Test
    public void testStructWithNestedFieldsExactMatch() {
        // struct<f1:int, f2:array<int>>
        StructType s1 = new StructType(
                new StructField("f1", Type.INT, null, true),
                new StructField("f2", new ArrayType(Type.INT, true), null, true)
        );
        StructType s2 = new StructType(
                new StructField("x", Type.INT, null, true),
                new StructField("y", new ArrayType(Type.INT, true), null, true)
        );
        // names are ignored by matchExactType recursion; matchesType requires containsNull equal
        Assert.assertTrue(Type.matchExactType(s1, s2));

        // inner element type differs
        StructType s3 = new StructType(
                new StructField("f1", Type.INT, null, true),
                new StructField("f2", new ArrayType(Type.BIGINT, true), null, true)
        );
        Assert.assertFalse(Type.matchExactType(s1, s3));

        // field nullability differs -> matchesType fails upfront
        StructType s4 = new StructType(
                new StructField("f1", Type.INT, null, false),
                new StructField("f2", new ArrayType(Type.INT, true), null, true)
        );
        Assert.assertFalse(Type.matchExactType(s1, s4));
    }

    // ===================== Mixed Nesting & Precision =====================
    @Test
    public void testArrayMapStructCombinationWithPrecision() {
        // array<map<int, struct<c1:int, c2:array<decimal(10,2)>>>>
        ScalarType dec10s2 = ScalarType.createDecimalV3Type(10, 2); // DECIMAL64 range
        ArrayType innerArray = new ArrayType(dec10s2, true);
        StructType innerStruct = new StructType(
                new StructField("c1", Type.INT, null, true),
                new StructField("c2", innerArray, null, true)
        );
        MapType innerMap1 = new MapType(Type.INT, innerStruct, true, true);
        ArrayType complex1 = new ArrayType(innerMap1, true);

        // Same shape but decimal precision 12 (same DECIMAL64 group), same scale
        ScalarType dec12s2 = ScalarType.createDecimalV3Type(12, 2);
        ArrayType innerArray2 = new ArrayType(dec12s2, true);
        StructType innerStruct2 = new StructType(
                new StructField("c1", Type.INT, null, true),
                new StructField("c2", innerArray2, null, true)
        );
        MapType innerMap2 = new MapType(Type.INT, innerStruct2, true, true);
        ArrayType complex2 = new ArrayType(innerMap2, true);

        Assert.assertFalse(Type.matchExactType(complex1, complex2, false));
    }

    // ===================== Decimal/DATETIMEV2 Precision & Scale =====================
    @Test
    public void testDecimalPrecisionGroupsIgnorePrecision() {
        // DECIMAL32 group (<=9)
        ScalarType d8s2 = ScalarType.createDecimalV3Type(8, 2);
        ScalarType d9s2 = ScalarType.createDecimalV3Type(9, 2);
        Assert.assertFalse(Type.matchExactType(d8s2, d9s2, false));

        // Cross group: DECIMAL32 vs DECIMAL64 -> should be false even when ignorePrecision
        ScalarType d10s2 = ScalarType.createDecimalV3Type(10, 2);
        Assert.assertFalse(Type.matchExactType(d9s2, d10s2, true));

        // DECIMAL64 group (10..18)
        ScalarType d10s3 = ScalarType.createDecimalV3Type(10, 3);
        ScalarType d18s3 = ScalarType.createDecimalV3Type(18, 3);
        Assert.assertFalse(Type.matchExactType(d10s3, d18s3, false));

        // DECIMAL128 group (19..38)
        ScalarType d20s1 = ScalarType.createDecimalV3Type(20, 1);
        ScalarType d38s1 = ScalarType.createDecimalV3Type(38, 1);
        Assert.assertFalse(Type.matchExactType(d20s1, d38s1, false));
    }

    @Test
    public void testDatetimeV2ScaleMatching() {
        ScalarType dtv2s3 = ScalarType.createDatetimeV2Type(3);
        ScalarType dtv2s6 = ScalarType.createDatetimeV2Type(6);
        // Different scales -> no match regardless of ignorePrecision
        Assert.assertFalse(Type.matchExactType(dtv2s3, dtv2s6, false));
        Assert.assertFalse(Type.matchExactType(dtv2s3, dtv2s6, true));
        // Same scale -> match
        Assert.assertTrue(Type.matchExactType(dtv2s6, ScalarType.createDatetimeV2Type(6)));
    }
}
