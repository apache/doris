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

package org.apache.doris.datasource.lance;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.thrift.TSearchVector;
import org.apache.doris.thrift.TVectorElementType;

import org.apache.arrow.memory.util.Float16;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Collections;

public class LanceVectorQueryTest {

    @Test
    public void testEncodesFloat32AsLittleEndianTypedVector() throws Exception {
        Field field = vectorField("embedding",
                new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), 3);
        TSearchVector vector =
                LanceVectorQuery.parseAndEncodeQueryVector(field, "[0.25, -1.5, 3]");

        Assertions.assertEquals(TVectorElementType.FLOAT32, vector.getElementType());
        Assertions.assertEquals(3, vector.getDimension());
        ByteBuffer values = vector.bufferForValues().order(ByteOrder.LITTLE_ENDIAN);
        Assertions.assertEquals(0.25F, values.getFloat());
        Assertions.assertEquals(-1.5F, values.getFloat());
        Assertions.assertEquals(3.0F, values.getFloat());
        Assertions.assertFalse(values.hasRemaining());
    }

    @Test
    public void testEncodesAllLanceCElementTypes() throws Exception {
        TSearchVector float16 = LanceVectorQuery.parseAndEncodeQueryVector(vectorField("f16",
                new ArrowType.FloatingPoint(FloatingPointPrecision.HALF), 1), "[1.5]");
        Assertions.assertEquals(TVectorElementType.FLOAT16, float16.getElementType());
        Assertions.assertEquals(1.5F, Float16.toFloat(
                float16.bufferForValues().order(ByteOrder.LITTLE_ENDIAN).getShort()));

        TSearchVector float64 = LanceVectorQuery.parseAndEncodeQueryVector(vectorField("f64",
                new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), 1), "[1.25]");
        Assertions.assertEquals(TVectorElementType.FLOAT64, float64.getElementType());
        Assertions.assertEquals(1.25,
                float64.bufferForValues().order(ByteOrder.LITTLE_ENDIAN).getDouble());

        TSearchVector uint8 = LanceVectorQuery.parseAndEncodeQueryVector(
                vectorField("u8", new ArrowType.Int(8, false), 2), "[0, 255]");
        Assertions.assertEquals(TVectorElementType.UINT8, uint8.getElementType());
        Assertions.assertArrayEquals(new byte[] {0, (byte) 255}, uint8.getValues());

        TSearchVector int8 = LanceVectorQuery.parseAndEncodeQueryVector(
                vectorField("i8", new ArrowType.Int(8, true), 2), "[-128, 127]");
        Assertions.assertEquals(TVectorElementType.INT8, int8.getElementType());
        Assertions.assertArrayEquals(new byte[] {(byte) -128, 127}, int8.getValues());
    }

    @Test
    public void testRejectsDimensionAndIntegerRangeMismatch() {
        Field float32 = vectorField("embedding",
                new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), 3);
        AnalysisException dimension = Assertions.assertThrows(
                AnalysisException.class,
                () -> LanceVectorQuery.parseAndEncodeQueryVector(float32, "[1, 2]"));
        Assertions.assertTrue(dimension.getMessage().contains("dimension"));

        Field uint8 = vectorField("embedding", new ArrowType.Int(8, false), 1);
        AnalysisException range = Assertions.assertThrows(
                AnalysisException.class,
                () -> LanceVectorQuery.parseAndEncodeQueryVector(uint8, "[256]"));
        Assertions.assertTrue(range.getMessage().contains("uint8"));
    }

    @Test
    public void testRejectsVariableAndExtensionVectors() {
        Field list = new Field("embedding", FieldType.nullable(ArrowType.List.INSTANCE),
                Collections.singletonList(Field.nullable("item",
                        new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE))));
        Assertions.assertThrows(
                AnalysisException.class,
                () -> LanceVectorQuery.parseAndEncodeQueryVector(list, "[1]"));

        Field bfloat16Item = new Field("item",
                new FieldType(true, new ArrowType.FixedSizeBinary(2), null,
                        Collections.singletonMap("ARROW:extension:name", "lance.bfloat16")),
                Collections.emptyList());
        Field bfloat16Vector = new Field("embedding",
                FieldType.nullable(new ArrowType.FixedSizeList(1)),
                Collections.singletonList(bfloat16Item));
        Assertions.assertThrows(
                AnalysisException.class,
                () -> LanceVectorQuery.parseAndEncodeQueryVector(bfloat16Vector, "[1]"));
    }

    @Test
    public void testRejectsCaseInsensitiveAmbiguousColumn() {
        Field lower = vectorField("embedding",
                new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), 1);
        Field upper = vectorField("EMBEDDING",
                new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), 1);
        Schema schema = new Schema(Arrays.asList(lower, upper));

        AnalysisException ambiguous = Assertions.assertThrows(
                AnalysisException.class,
                () -> LanceVectorQuery.findVectorColumnField(schema, "embedding"));
        Assertions.assertTrue(ambiguous.getMessage().contains("ambiguous"));
    }

    private static Field vectorField(String name, ArrowType elementType, int dimension) {
        return new Field(name, FieldType.nullable(new ArrowType.FixedSizeList(dimension)),
                Collections.singletonList(Field.nullable("item", elementType)));
    }
}
