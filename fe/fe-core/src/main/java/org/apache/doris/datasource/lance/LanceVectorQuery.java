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

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import org.apache.arrow.memory.util.Float16;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/** Validates and encodes one Lance vector-search query against its Arrow vector column. */
public final class LanceVectorQuery {
    // Keep aligned with the BE and Lance-C limits: each subvector expands into an ANN branch.
    public static final int MAX_QUERY_VECTORS = 128;
    public static final long MAX_QUERY_VECTOR_CANDIDATES = 100_000;

    private static final String ARROW_EXTENSION_NAME = "ARROW:extension:name";

    private LanceVectorQuery() {
    }

    /**
     * Find a vector column using Doris' case-insensitive identifier behavior while preserving the
     * physical Lance field name sent to the backend.
     */
    public static Field findVectorColumnField(Schema schema, String column) throws AnalysisException {
        Field match = null;
        for (Field field : schema.getFields()) {
            if (field.getName().equalsIgnoreCase(column)) {
                if (match != null) {
                    throw new AnalysisException("Lance vector column '" + column
                            + "' is ambiguous under case-insensitive matching");
                }
                match = field;
            }
        }
        if (match == null) {
            throw new AnalysisException("Lance vector column '" + column + "' does not exist");
        }
        return match;
    }

    public static void validateMultiVectorBudget(TSearchVector query, long topK, long offset, int refineFactor)
            throws AnalysisException {
        if (query.isSetNumVectors() && (query.getNumVectors() <= 0
                || query.getNumVectors() > MAX_QUERY_VECTORS || topK <= 0 || offset < 0
                || offset > MAX_QUERY_VECTOR_CANDIDATES
                || topK > MAX_QUERY_VECTOR_CANDIDATES - offset
                || refineFactor <= 0 || topK + offset > MAX_QUERY_VECTOR_CANDIDATES / refineFactor
                || query.getNumVectors() > MAX_QUERY_VECTOR_CANDIDATES / (topK + offset))) {
            throw new AnalysisException("Multi-vector query exceeds 128 subvectors or "
                    + "100000 candidate budget for num_vectors or refine_factor times (top_k + offset)");
        }
    }

    public static TSearchVector parseAndEncodeQueryVector(Field field, String json)
            throws AnalysisException {
        boolean multiVector = field.getType().getTypeID() == ArrowType.ArrowTypeID.List;
        VectorEncodingSpec encodingSpec = analyzeQueryVectorField(field);
        JsonArray values = parseQueryVector(json, field, multiVector ? -1 : encodingSpec.dimension);
        int numVectors = multiVector ? values.size() : 1;
        if (multiVector) {
            if (numVectors > MAX_QUERY_VECTORS) {
                throw new AnalysisException("Multi-vector query exceeds 128 subvectors");
            }
            // Validate shape before allocating from the schema dimension, even for a short input.
            for (JsonElement subvector : values) {
                if (!subvector.isJsonArray() || subvector.getAsJsonArray().size() != encodingSpec.dimension) {
                    throw new AnalysisException("Each query subvector must be an array of dimension "
                            + encodingSpec.dimension);
                }
            }
        }
        long bytesPerVector = (long) encodingSpec.dimension * encodingSpec.byteWidth;
        if (numVectors == 0 || numVectors > Integer.MAX_VALUE / bytesPerVector) {
            throw new AnalysisException("Query vector matrix must be non-empty and fit in a binary value");
        }
        ByteBuffer buffer = ByteBuffer.allocate((int) (numVectors * bytesPerVector)).order(ByteOrder.LITTLE_ENDIAN);
        if (multiVector) {
            for (JsonElement subvector : values) {
                encodeQueryVectorValues(subvector.getAsJsonArray(), encodingSpec, buffer);
            }
        } else {
            encodeQueryVectorValues(values, encodingSpec, buffer);
        }
        TSearchVector query = new TSearchVector()
                .setElementType(encodingSpec.elementType)
                .setDimension(encodingSpec.dimension)
                .setValues(buffer.array());
        if (multiVector) {
            query.setNumVectors(numVectors);
        }
        return query;
    }

    public static void validateVectorField(Field field) throws AnalysisException {
        analyzeQueryVectorField(field);
    }

    private static VectorEncodingSpec analyzeQueryVectorField(Field field) throws AnalysisException {
        boolean multiVector = field.getType().getTypeID() == ArrowType.ArrowTypeID.List;
        Field vectorField = field;
        if (multiVector) {
            if (hasExtension(field) || field.getDictionary() != null || field.getChildren().size() != 1) {
                throw unsupportedVectorType(field);
            }
            vectorField = field.getChildren().get(0);
            // Lance's multi-vector distance kernels do not consult inner validity bitmaps.
            if (vectorField.isNullable() || vectorField.getChildren().size() != 1) {
                throw new AnalysisException("Lance multi-vector columns require non-nullable subvectors");
            }
        }
        VectorEncodingSpec encodingSpec = analyzeVectorField(vectorField);
        if (multiVector && encodingSpec.elementType != TVectorElementType.FLOAT16
                && encodingSpec.elementType != TVectorElementType.FLOAT32
                && encodingSpec.elementType != TVectorElementType.FLOAT64) {
            throw unsupportedVectorType(field);
        }
        return encodingSpec;
    }

    private static VectorEncodingSpec analyzeVectorField(Field field) throws AnalysisException {
        if (hasExtension(field) || field.getDictionary() != null
                || field.getType().getTypeID() != ArrowType.ArrowTypeID.FixedSizeList
                || field.getChildren().size() != 1) {
            throw unsupportedVectorType(field);
        }

        int dimension = ((ArrowType.FixedSizeList) field.getType()).getListSize();
        if (dimension <= 0) {
            throw new AnalysisException("Lance vector column '" + field.getName()
                    + "' has invalid dimension " + dimension);
        }

        Field elementField = field.getChildren().get(0);
        if (hasExtension(elementField) || elementField.getDictionary() != null) {
            throw unsupportedVectorType(field);
        }
        return determineVectorEncoding(field, elementField.getType(), dimension);
    }

    private static JsonArray parseQueryVector(String json, Field field, int dimension)
            throws AnalysisException {
        try {
            JsonElement root = JsonParser.parseString(json);
            if (!root.isJsonArray()) {
                throw new AnalysisException("'query_vector' must be a JSON array");
            }
            JsonArray values = root.getAsJsonArray();
            if (dimension >= 0 && values.size() != dimension) {
                throw new AnalysisException("Query vector dimension " + values.size()
                        + " does not match Lance column '" + field.getName()
                        + "' dimension " + dimension);
            }
            return values;
        } catch (AnalysisException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new AnalysisException("Invalid 'query_vector' JSON: " + e.getMessage(), e);
        }
    }

    private static void encodeQueryVectorValues(JsonArray values,
            VectorEncodingSpec encodingSpec, ByteBuffer buffer) throws AnalysisException {
        for (int i = 0; i < values.size(); ++i) {
            JsonElement value = values.get(i);
            if (!value.isJsonPrimitive() || !value.getAsJsonPrimitive().isNumber()) {
                throw new AnalysisException("Query vector element " + i + " must be a number");
            }
            writeQueryVectorElement(
                    value.getAsJsonPrimitive(), i, encodingSpec.elementType, buffer);
        }
    }

    private static VectorEncodingSpec determineVectorEncoding(
            Field vectorField, ArrowType elementType, int dimension) throws AnalysisException {
        switch (elementType.getTypeID()) {
            case FloatingPoint:
                FloatingPointPrecision precision =
                        ((ArrowType.FloatingPoint) elementType).getPrecision();
                switch (precision) {
                    case HALF:
                        return new VectorEncodingSpec(
                                dimension, TVectorElementType.FLOAT16, Short.BYTES);
                    case SINGLE:
                        return new VectorEncodingSpec(
                                dimension, TVectorElementType.FLOAT32, Float.BYTES);
                    case DOUBLE:
                        return new VectorEncodingSpec(
                                dimension, TVectorElementType.FLOAT64, Double.BYTES);
                    default:
                        throw unsupportedVectorType(vectorField);
                }
            case Int:
                ArrowType.Int integer = (ArrowType.Int) elementType;
                if (integer.getBitWidth() == Byte.SIZE) {
                    return new VectorEncodingSpec(dimension, integer.getIsSigned()
                            ? TVectorElementType.INT8 : TVectorElementType.UINT8, Byte.BYTES);
                }
                throw unsupportedVectorType(vectorField);
            default:
                throw unsupportedVectorType(vectorField);
        }
    }

    private static void writeQueryVectorElement(JsonPrimitive value, int index,
            TVectorElementType elementType, ByteBuffer buffer) throws AnalysisException {
        try {
            switch (elementType) {
                case FLOAT16:
                    float float16Value = checkedFloat(
                            value.getAsDouble(), index, TVectorElementType.FLOAT16);
                    short float16Bits = Float16.toFloat16(float16Value);
                    if (!Float.isFinite(Float16.toFloat(float16Bits))) {
                        throw outOfRange(index, elementType);
                    }
                    buffer.putShort(float16Bits);
                    return;
                case FLOAT32:
                    buffer.putFloat(checkedFloat(
                            value.getAsDouble(), index, TVectorElementType.FLOAT32));
                    return;
                case FLOAT64:
                    double doubleValue = value.getAsDouble();
                    if (!Double.isFinite(doubleValue)) {
                        throw outOfRange(index, elementType);
                    }
                    buffer.putDouble(doubleValue);
                    return;
                case UINT8:
                    buffer.put((byte) checkedInteger(value.getAsBigDecimal(), index,
                            BigInteger.ZERO, BigInteger.valueOf(255), elementType).intValue());
                    return;
                case INT8:
                    buffer.put(checkedInteger(value.getAsBigDecimal(), index,
                            BigInteger.valueOf(Byte.MIN_VALUE), BigInteger.valueOf(Byte.MAX_VALUE),
                            elementType).byteValue());
                    return;
                default:
                    throw new AnalysisException("Unsupported query vector element type " + elementType);
            }
        } catch (AnalysisException e) {
            throw e;
        } catch (ArithmeticException | NumberFormatException e) {
            throw outOfRange(index, elementType);
        }
    }

    private static float checkedFloat(double value, int index, TVectorElementType type)
            throws AnalysisException {
        float converted = (float) value;
        if (!Double.isFinite(value) || !Float.isFinite(converted)) {
            throw outOfRange(index, type);
        }
        return converted;
    }

    private static BigInteger checkedInteger(BigDecimal value, int index, BigInteger min,
            BigInteger max, TVectorElementType type) throws AnalysisException {
        BigInteger integer = value.toBigIntegerExact();
        if (integer.compareTo(min) < 0 || integer.compareTo(max) > 0) {
            throw outOfRange(index, type);
        }
        return integer;
    }

    private static boolean hasExtension(Field field) {
        return field.getMetadata() != null
                && field.getMetadata().get(ARROW_EXTENSION_NAME) != null
                && !field.getMetadata().get(ARROW_EXTENSION_NAME).isEmpty();
    }

    private static AnalysisException unsupportedVectorType(Field field) {
        return new AnalysisException("Lance vector column '" + field.getName()
                + "' must be fixed_size_list<float16|float32|float64|uint8|int8> or "
                + "list<fixed_size_list<float16|float32|float64>>, but was "
                + field.getType());
    }

    private static AnalysisException outOfRange(int index, TVectorElementType type) {
        return new AnalysisException("Query vector element " + index
                + " is not representable as " + type.name().toLowerCase());
    }

    private static class VectorEncodingSpec {
        private final int dimension;
        private final TVectorElementType elementType;
        private final int byteWidth;

        private VectorEncodingSpec(int dimension, TVectorElementType elementType, int byteWidth) {
            this.dimension = dimension;
            this.elementType = elementType;
            this.byteWidth = byteWidth;
        }
    }
}
