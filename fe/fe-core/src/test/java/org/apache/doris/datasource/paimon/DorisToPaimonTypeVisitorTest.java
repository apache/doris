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

package org.apache.doris.datasource.paimon;

import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.datasource.DorisTypeVisitor;

import org.apache.paimon.casting.CastExecutor;
import org.apache.paimon.casting.CastExecutors;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.VarCharType;
import org.junit.Assert;
import org.junit.Test;

public class DorisToPaimonTypeVisitorTest {
    private DataType convert(Type type) {
        return DorisTypeVisitor.visit(type, new DorisToPaimonTypeVisitor());
    }

    @Test
    public void testCharacterBounds() {
        Assert.assertEquals(new CharType(3), convert(ScalarType.createCharType(3)));
        Assert.assertEquals(new VarCharType(3), convert(ScalarType.createVarcharType(3)));
        Assert.assertEquals(new VarCharType(VarCharType.MAX_LENGTH), convert(Type.STRING));
    }

    @Test
    public void testTimestampPrecision() {
        for (int precision : new int[] {0, 3, 6}) {
            Assert.assertEquals(new TimestampType(precision),
                    convert(ScalarType.createDatetimeV2Type(precision)));
        }
        Assert.assertEquals(new TimestampType(0), convert(Type.DATETIME));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testMappedHistoricalCasts() {
        CastExecutor<BinaryString, ?> varcharCast = (CastExecutor<BinaryString, ?>) CastExecutors.resolve(
                new VarCharType(10), convert(ScalarType.createVarcharType(3)));
        Assert.assertNotNull(varcharCast);
        Assert.assertEquals("abc", varcharCast.cast(BinaryString.fromString("abcdef")).toString());
        CastExecutor<BinaryString, ?> timestampCast = (CastExecutor<BinaryString, ?>) CastExecutors.resolve(
                new VarCharType(VarCharType.MAX_LENGTH), convert(ScalarType.createDatetimeV2Type(3)));
        Assert.assertNotNull(timestampCast);
        Assert.assertEquals("1970-01-01T00:00:00.042", timestampCast.cast(BinaryString.fromString("42")).toString());
    }

    @Test
    public void testNestedBoundsAndPrecision() {
        ArrayType array = new ArrayType(ScalarType.createVarcharType(3));
        Assert.assertEquals(new org.apache.paimon.types.ArrayType(new VarCharType(3)), convert(array));
        MapType map = new MapType(ScalarType.createCharType(2), ScalarType.createDatetimeV2Type(3));
        Assert.assertEquals(new org.apache.paimon.types.MapType(new CharType(false, 2), new TimestampType(3)),
                convert(map));
    }
}
