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

package org.apache.doris.connector.lance;

import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.thrift.TSearchVector;
import org.apache.doris.thrift.TVectorElementType;
import org.apache.doris.thrift.TVectorMetric;

import org.apache.arrow.memory.util.Float16;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;

/**
 * The query vector of a vector search: the column types Lance searches, the little-endian encoding of each element
 * type, and the shape and work limits of a multi-vector query. The cases of branch-4.2's
 * VectorSearchQueryEncodingTest.
 */
class LanceVectorQueryTest {

    @Test
    void encodesFloat32AsALittleEndianTypedVector() {
        TSearchVector vector = LanceVectorQuery.parseAndEncode(vectorField("embedding",
                new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), 3), "[0.25, -1.5, 3]");

        Assertions.assertEquals(TVectorElementType.FLOAT32, vector.getElementType());
        Assertions.assertEquals(3, vector.getDimension());
        ByteBuffer values = vector.bufferForValues().order(ByteOrder.LITTLE_ENDIAN);
        Assertions.assertEquals(0.25F, values.getFloat());
        Assertions.assertEquals(-1.5F, values.getFloat());
        Assertions.assertEquals(3.0F, values.getFloat());
        Assertions.assertFalse(values.hasRemaining());
    }

    @Test
    void encodesEveryElementTypeLanceCSearches() {
        TSearchVector float16 = LanceVectorQuery.parseAndEncode(vectorField("f16",
                new ArrowType.FloatingPoint(FloatingPointPrecision.HALF), 1), "[1.5]");
        Assertions.assertEquals(TVectorElementType.FLOAT16, float16.getElementType());
        Assertions.assertEquals(1.5F, Float16.toFloat(
                float16.bufferForValues().order(ByteOrder.LITTLE_ENDIAN).getShort()));

        TSearchVector float64 = LanceVectorQuery.parseAndEncode(vectorField("f64",
                new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), 1), "[1.25]");
        Assertions.assertEquals(TVectorElementType.FLOAT64, float64.getElementType());
        Assertions.assertEquals(1.25, float64.bufferForValues().order(ByteOrder.LITTLE_ENDIAN).getDouble());

        TSearchVector uint8 = LanceVectorQuery.parseAndEncode(
                vectorField("u8", new ArrowType.Int(8, false), 2), "[0, 255]");
        Assertions.assertEquals(TVectorElementType.UINT8, uint8.getElementType());
        Assertions.assertArrayEquals(new byte[] {0, (byte) 255}, uint8.getValues());

        TSearchVector int8 = LanceVectorQuery.parseAndEncode(
                vectorField("i8", new ArrowType.Int(8, true), 2), "[-128, 127.0]");
        Assertions.assertEquals(TVectorElementType.INT8, int8.getElementType());
        Assertions.assertArrayEquals(new byte[] {(byte) -128, 127}, int8.getValues());
    }

    @Test
    void rejectsADimensionMismatchAndValuesOutsideTheElementType() {
        Field float32 = vectorField("embedding", new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), 3);
        assertRejected(float32, "[1, 2]", "dimension");
        assertRejected(float32, "[1, 2, 1e39]", "not representable as float32");
        assertRejected(float32, "[1, 2, \"3\"]", "must be a number");
        assertRejected(float32, "{\"a\": 1}", "must be a JSON array");
        assertRejected(float32, "[1, 2,", "Invalid 'query_vector' JSON");
        assertRejected(float32, "[1, 2, 3] [4]", "Invalid 'query_vector' JSON");

        Field uint8 = vectorField("embedding", new ArrowType.Int(8, false), 1);
        assertRejected(uint8, "[256]", "uint8");
        assertRejected(uint8, "[-1]", "uint8");
        // An integer element must be exactly an integer, as written: nothing is rounded into the type.
        assertRejected(uint8, "[1.5]", "uint8");
        assertRejected(uint8, "[1.00000000000000000001]", "uint8");
        assertRejected(vectorField("embedding", new ArrowType.FloatingPoint(FloatingPointPrecision.HALF), 1),
                "[100000]", "float16");
        // An exponent too large for a decimal is invalid JSON to Doris, not an exception out of the connector.
        assertRejected(float32, "[1, 2, 1e99999999999]", "Invalid 'query_vector' JSON");
    }

    @Test
    void refusesAnIntegerElementWithAHugeExponentWithoutExpandingIt() {
        // Expanding 1e500000000 into an integer takes minutes and a gigabyte; the range check comes first.
        Field int8 = vectorField("embedding", new ArrowType.Int(8, true), 1);
        Field uint8 = vectorField("embedding", new ArrowType.Int(8, false), 1);
        Assertions.assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
            assertRejected(int8, "[1e500000000]", "int8");
            assertRejected(int8, "[-1e500000000]", "int8");
            assertRejected(uint8, "[1e-500000000]", "uint8");
        });
        Assertions.assertArrayEquals(new byte[] {100},
                LanceVectorQuery.parseAndEncode(int8, "[1e2]").getValues());
        Assertions.assertArrayEquals(new byte[] {(byte) 200},
                LanceVectorQuery.parseAndEncode(uint8, "[2.000E+2]").getValues());
    }

    @Test
    void rejectsVariableLengthAndExtensionVectors() {
        Field list = new Field("embedding", FieldType.nullable(ArrowType.List.INSTANCE),
                Collections.singletonList(Field.nullable("item",
                        new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE))));
        Assertions.assertThrows(DorisConnectorException.class, () -> LanceVectorQuery.parseAndEncode(list, "[1]"));

        Field bfloat16Item = new Field("item", new FieldType(true, new ArrowType.FixedSizeBinary(2), null,
                Collections.singletonMap("ARROW:extension:name", "lance.bfloat16")), Collections.emptyList());
        Field bfloat16Vector = new Field("embedding", FieldType.nullable(new ArrowType.FixedSizeList(1)),
                Collections.singletonList(bfloat16Item));
        DorisConnectorException error = Assertions.assertThrows(DorisConnectorException.class,
                () -> LanceVectorQuery.parseAndEncode(bfloat16Vector, "[1]"));
        Assertions.assertTrue(error.getMessage().contains("must be fixed_size_list"), error.getMessage());
        Assertions.assertThrows(DorisConnectorException.class, () -> LanceVectorQuery.checkSearchable(bfloat16Vector));
    }

    @Test
    void rejectsAColumnNameThatMatchesTwoColumnsCaseInsensitively() {
        Schema schema = new Schema(Arrays.asList(
                vectorField("embedding", new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), 1),
                vectorField("EMBEDDING", new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), 1)));

        DorisConnectorException ambiguous = Assertions.assertThrows(DorisConnectorException.class,
                () -> LanceSearchArguments.requireSearchColumn(schema, "embedding", "vector"));
        Assertions.assertTrue(ambiguous.getMessage().contains("ambiguous"), ambiguous.getMessage());
        DorisConnectorException missing = Assertions.assertThrows(DorisConnectorException.class,
                () -> LanceSearchArguments.requireSearchColumn(schema, "other", "vector"));
        Assertions.assertEquals("Lance vector column 'other' does not exist", missing.getMessage());
    }

    @Test
    void encodesAMultiVectorQueryRowMajorForEveryFloatingType() {
        for (FloatingPointPrecision precision : FloatingPointPrecision.values()) {
            Field field = multiVectorField(new ArrowType.FloatingPoint(precision), 2, false, false);
            TSearchVector vector = LanceVectorQuery.parseAndEncode(field, "[[1,2],[3,4]]");
            Assertions.assertEquals(2, vector.getDimension());
            int width = precision == FloatingPointPrecision.HALF ? 2
                    : precision == FloatingPointPrecision.SINGLE ? 4 : 8;
            Assertions.assertEquals(2, vector.getNumVectors());
            Assertions.assertEquals(4 * width, vector.getValues().length);
            ByteBuffer values = vector.bufferForValues().order(ByteOrder.LITTLE_ENDIAN);
            for (int i = 1; i <= 4; i++) {
                double value = width == 2 ? Float16.toFloat(values.getShort())
                        : width == 4 ? values.getFloat() : values.getDouble();
                Assertions.assertEquals(i, value);
            }
            // The column itself reads as a nullable array of nullable arrays of the element type.
            ConnectorType outer = LanceTypeMapping.toConnectorType(field);
            Assertions.assertEquals("ARRAY", outer.getTypeName());
            Assertions.assertTrue(outer.isChildNullable(0));
            ConnectorType inner = outer.getChildren().get(0);
            Assertions.assertEquals("ARRAY", inner.getTypeName());
            Assertions.assertTrue(inner.isChildNullable(0));
            Assertions.assertEquals(precision == FloatingPointPrecision.DOUBLE ? "DOUBLE" : "FLOAT",
                    inner.getChildren().get(0).getTypeName());
        }
    }

    @Test
    void rejectsMultiVectorQueriesOfTheWrongShapeOrValues() {
        Field field = multiVectorField(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), 2, false, false);
        for (String json : Arrays.asList("[]", "[1,2]", "[[]]", "[[1,2],[3]]", "[[1,2],null]",
                "[[1,null]]", "[[1,\"x\"]]", "[[1,1e999]]", "[[[1,2]]]")) {
            Assertions.assertThrows(DorisConnectorException.class, () -> LanceVectorQuery.parseAndEncode(field, json),
                    json);
        }
    }

    @Test
    void rejectsMultiVectorColumnsWithNullableSubvectorsOrIntegerElements() {
        for (Field field : Arrays.asList(
                multiVectorField(new ArrowType.Int(8, true), 2, false, false),
                multiVectorField(new ArrowType.Int(8, false), 2, false, false),
                multiVectorField(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), 2, true, false),
                multiVectorField(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), 0, false, false))) {
            Assertions.assertThrows(DorisConnectorException.class,
                    () -> LanceVectorQuery.parseAndEncode(field, "[[1,2]]"));
            Assertions.assertThrows(DorisConnectorException.class, () -> LanceVectorQuery.checkSearchable(field));
        }
    }

    @Test
    void singleSubvectorQueryRoundTripsThroughThrift() throws Exception {
        // Lance reconstructs primitive FixedSizeList elements as nullable after persisting the schema.
        Field field = multiVectorField(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), 2, false, true);
        TSearchVector query = LanceVectorQuery.parseAndEncode(field, "[[1,2]]");
        Assertions.assertEquals(1, query.getNumVectors());
        Assertions.assertTrue(query.isSetNumVectors());
        TSearchVector copy = new TSearchVector();
        new TDeserializer().deserialize(copy, new TSerializer().serialize(query));
        Assertions.assertEquals(query, copy);
        Assertions.assertFalse(LanceVectorQuery.parseAndEncode(
                vectorField("embedding", new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), 2),
                "[1,2]").isSetNumVectors());
    }

    @Test
    void checksAMultiVectorShapeBeforeAllocatingFromTheSchemaDimension() {
        Field field = multiVectorField(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE),
                Integer.MAX_VALUE, false, true);
        assertRejected(field, "[[1,2]]", "Each query subvector");
    }

    @Test
    void boundsTheWorkOfAMultiVectorQuery() {
        Field field = multiVectorField(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), 1, false, true);
        TSearchVector query = LanceVectorQuery.parseAndEncode(field,
                "[" + String.join(",", Collections.nCopies(128, "[1]")) + "]");
        LanceVectorQuery.validateMultiVectorBudget(query, 780, 1, 1);
        Assertions.assertThrows(DorisConnectorException.class,
                () -> LanceVectorQuery.validateMultiVectorBudget(query, 781, 1, 1));
        Assertions.assertThrows(DorisConnectorException.class,
                () -> LanceVectorQuery.validateMultiVectorBudget(query, 1, 0, Integer.MAX_VALUE));
        Assertions.assertThrows(DorisConnectorException.class,
                () -> LanceVectorQuery.validateMultiVectorBudget(query, Long.MAX_VALUE, 1, 1));
        assertRejected(field, "[" + String.join(",", Collections.nCopies(129, "[1]")) + "]", "128 subvectors");
    }

    @Test
    void parsesMetricsInLanceSpellings() {
        Assertions.assertEquals(TVectorMetric.L2, LanceVectorQuery.parseMetric(" L2 "));
        Assertions.assertEquals(TVectorMetric.COSINE, LanceVectorQuery.parseMetric("cosine"));
        Assertions.assertEquals(TVectorMetric.DOT_PRODUCT, LanceVectorQuery.parseMetric("dot"));
        Assertions.assertEquals(TVectorMetric.DOT_PRODUCT, LanceVectorQuery.parseMetric("DOT_PRODUCT"));
        Assertions.assertEquals(TVectorMetric.HAMMING, LanceVectorQuery.parseMetric("hamming"));
        DorisConnectorException error = Assertions.assertThrows(DorisConnectorException.class,
                () -> LanceVectorQuery.parseMetric("euclid"));
        Assertions.assertEquals("Unsupported vector metric 'euclid': expected l2, cosine, dot, or hamming",
                error.getMessage());
        Assertions.assertEquals("dot", LanceVectorQuery.metricName(TVectorMetric.DOT_PRODUCT));
        Assertions.assertEquals("default", LanceVectorQuery.metricName(TVectorMetric.DEFAULT));
    }

    private static void assertRejected(Field field, String json, String message) {
        DorisConnectorException error = Assertions.assertThrows(DorisConnectorException.class,
                () -> LanceVectorQuery.parseAndEncode(field, json), json);
        Assertions.assertTrue(error.getMessage().contains(message), json + ": " + error.getMessage());
    }

    private static Field multiVectorField(ArrowType element, int dimension, boolean nullableVector,
            boolean nullableElement) {
        Field child = new Field("item", new FieldType(nullableVector, new ArrowType.FixedSizeList(dimension), null),
                Collections.singletonList(new Field("item", new FieldType(nullableElement, element, null),
                        Collections.emptyList())));
        return new Field("embedding", FieldType.nullable(ArrowType.List.INSTANCE), Collections.singletonList(child));
    }

    private static Field vectorField(String name, ArrowType elementType, int dimension) {
        return new Field(name, FieldType.nullable(new ArrowType.FixedSizeList(dimension)),
                Collections.singletonList(Field.nullable("item", elementType)));
    }
}
