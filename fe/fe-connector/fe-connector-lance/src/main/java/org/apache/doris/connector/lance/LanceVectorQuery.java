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

import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.thrift.TSearchVector;
import org.apache.doris.thrift.TVectorElementType;
import org.apache.doris.thrift.TVectorMetric;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.arrow.memory.util.Float16;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Locale;

/**
 * The query vector of a {@code vector_search}: checks that the searched column is a vector Lance can search, and
 * encodes the user's JSON array into the typed little-endian matrix the BE hands to lance-c.
 *
 * <p>A vector column is {@code fixed_size_list<float16|float32|float64|uint8|int8>}; a multi-vector column is
 * {@code list<fixed_size_list<float16|float32|float64>>} with non-nullable subvectors, queried with a matrix of
 * subvectors. Every element must be representable in the column's element type: nothing is rounded into range.
 */
final class LanceVectorQuery {
    // Keep aligned with the BE and Lance-C limits: each subvector expands into an ANN branch.
    static final int MAX_QUERY_VECTORS = 128;
    static final long MAX_QUERY_VECTOR_CANDIDATES = 100_000;

    private static final String ARROW_EXTENSION_NAME = "ARROW:extension:name";
    private static final BigDecimal UINT8_MAX = BigDecimal.valueOf(255);
    private static final BigDecimal INT8_MIN = BigDecimal.valueOf(Byte.MIN_VALUE);
    private static final BigDecimal INT8_MAX = BigDecimal.valueOf(Byte.MAX_VALUE);
    /**
     * Reads a fraction as the exact decimal written, not as a double: an integer element must be an exact integer,
     * not one a double rounds to. Text after the array is an error, not ignored.
     */
    private static final ObjectMapper JSON = new ObjectMapper()
            .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

    private LanceVectorQuery() {
    }

    /**
     * Encodes {@code json}, a JSON array of numbers (a matrix of them for a multi-vector column), for a search of
     * {@code field}.
     *
     * @throws DorisConnectorException when the column cannot be searched, or the value does not fit it
     */
    static TSearchVector parseAndEncode(Field field, String json) {
        boolean multiVector = isMultiVector(field);
        Encoding encoding = analyzeField(field);
        JsonNode values = parseArray(json, field, multiVector ? -1 : encoding.dimension);
        int numVectors = multiVector ? values.size() : 1;
        if (multiVector) {
            if (numVectors > MAX_QUERY_VECTORS) {
                throw new DorisConnectorException("Multi-vector query exceeds " + MAX_QUERY_VECTORS + " subvectors");
            }
            // Validate the shape before allocating from the schema dimension, even for a short input.
            for (JsonNode subvector : values) {
                if (!subvector.isArray() || subvector.size() != encoding.dimension) {
                    throw new DorisConnectorException("Each query subvector must be an array of dimension "
                            + encoding.dimension);
                }
            }
        }
        long bytesPerVector = (long) encoding.dimension * encoding.byteWidth;
        if (numVectors == 0 || numVectors > Integer.MAX_VALUE / bytesPerVector) {
            throw new DorisConnectorException("Query vector matrix must be non-empty and fit in a binary value");
        }
        ByteBuffer buffer = ByteBuffer.allocate((int) (numVectors * bytesPerVector)).order(ByteOrder.LITTLE_ENDIAN);
        if (multiVector) {
            for (JsonNode subvector : values) {
                encodeValues(subvector, encoding.elementType, buffer);
            }
        } else {
            encodeValues(values, encoding.elementType, buffer);
        }
        TSearchVector query = new TSearchVector()
                .setElementType(encoding.elementType)
                .setDimension(encoding.dimension)
                .setValues(buffer.array());
        if (multiVector) {
            query.setNumVectors(numVectors);
        }
        return query;
    }

    /**
     * Checks that {@code field} is a vector column Lance can search, without a query vector: what a prepared
     * statement can check before its execution supplies the vector.
     */
    static void checkSearchable(Field field) {
        analyzeField(field);
    }

    /** Whether {@code field} is a multi-vector column, a list of vectors. */
    static boolean isMultiVector(Field field) {
        return field.getType().getTypeID() == ArrowType.ArrowTypeID.List;
    }

    /**
     * Bounds the work of a multi-vector search: lance-c searches once per subvector, each for
     * {@code refine_factor * (top_k + offset)} candidates.
     */
    static void validateMultiVectorBudget(TSearchVector query, long topK, long offset, int refineFactor) {
        if (query.isSetNumVectors() && (query.getNumVectors() <= 0
                || query.getNumVectors() > MAX_QUERY_VECTORS || topK <= 0 || offset < 0
                || offset > MAX_QUERY_VECTOR_CANDIDATES
                || topK > MAX_QUERY_VECTOR_CANDIDATES - offset
                || refineFactor <= 0 || topK + offset > MAX_QUERY_VECTOR_CANDIDATES / refineFactor
                || query.getNumVectors() > MAX_QUERY_VECTOR_CANDIDATES / (topK + offset))) {
            throw new DorisConnectorException("Multi-vector query exceeds 128 subvectors or "
                    + "100000 candidate budget for num_vectors or refine_factor times (top_k + offset)");
        }
    }

    /** The {@code metric} argument: Lance's metric names, and {@code dot_product} for {@code dot}. */
    static TVectorMetric parseMetric(String value) {
        switch (value.trim().toLowerCase(Locale.ROOT)) {
            case "l2":
                return TVectorMetric.L2;
            case "cosine":
                return TVectorMetric.COSINE;
            case "dot":
            case "dot_product":
                return TVectorMetric.DOT_PRODUCT;
            case "hamming":
                return TVectorMetric.HAMMING;
            default:
                throw new DorisConnectorException("Unsupported vector metric '" + value
                        + "': expected l2, cosine, dot, or hamming");
        }
    }

    /** Lance's spelling of a metric, as EXPLAIN shows it; an index records it in upper case. */
    static String metricName(TVectorMetric metric) {
        switch (metric) {
            case L2:
                return "l2";
            case COSINE:
                return "cosine";
            case DOT_PRODUCT:
                return "dot";
            case HAMMING:
                return "hamming";
            case DEFAULT:
            default:
                return "default";
        }
    }

    private static Encoding analyzeField(Field field) {
        boolean multiVector = isMultiVector(field);
        Field vectorField = field;
        if (multiVector) {
            if (hasExtension(field) || field.getDictionary() != null || field.getChildren().size() != 1) {
                throw unsupportedVectorType(field);
            }
            vectorField = field.getChildren().get(0);
            // Lance's multi-vector distance kernels do not consult inner validity bitmaps.
            if (vectorField.isNullable() || vectorField.getChildren().size() != 1) {
                throw new DorisConnectorException("Lance multi-vector columns require non-nullable subvectors");
            }
        }
        Encoding encoding = analyzeVectorField(vectorField);
        if (multiVector && encoding.elementType != TVectorElementType.FLOAT16
                && encoding.elementType != TVectorElementType.FLOAT32
                && encoding.elementType != TVectorElementType.FLOAT64) {
            throw unsupportedVectorType(field);
        }
        return encoding;
    }

    private static Encoding analyzeVectorField(Field field) {
        if (hasExtension(field) || field.getDictionary() != null
                || field.getType().getTypeID() != ArrowType.ArrowTypeID.FixedSizeList
                || field.getChildren().size() != 1) {
            throw unsupportedVectorType(field);
        }
        int dimension = ((ArrowType.FixedSizeList) field.getType()).getListSize();
        if (dimension <= 0) {
            throw new DorisConnectorException("Lance vector column '" + field.getName()
                    + "' has invalid dimension " + dimension);
        }
        Field elementField = field.getChildren().get(0);
        if (hasExtension(elementField) || elementField.getDictionary() != null) {
            throw unsupportedVectorType(field);
        }
        return encodingOf(field, elementField.getType(), dimension);
    }

    private static Encoding encodingOf(Field vectorField, ArrowType elementType, int dimension) {
        switch (elementType.getTypeID()) {
            case FloatingPoint:
                FloatingPointPrecision precision = ((ArrowType.FloatingPoint) elementType).getPrecision();
                switch (precision) {
                    case HALF:
                        return new Encoding(dimension, TVectorElementType.FLOAT16, Short.BYTES);
                    case SINGLE:
                        return new Encoding(dimension, TVectorElementType.FLOAT32, Float.BYTES);
                    case DOUBLE:
                        return new Encoding(dimension, TVectorElementType.FLOAT64, Double.BYTES);
                    default:
                        throw unsupportedVectorType(vectorField);
                }
            case Int:
                ArrowType.Int integer = (ArrowType.Int) elementType;
                if (integer.getBitWidth() == Byte.SIZE) {
                    return new Encoding(dimension, integer.getIsSigned()
                            ? TVectorElementType.INT8 : TVectorElementType.UINT8, Byte.BYTES);
                }
                throw unsupportedVectorType(vectorField);
            default:
                throw unsupportedVectorType(vectorField);
        }
    }

    /** The JSON array, of {@code dimension} values when that is not negative. */
    private static JsonNode parseArray(String json, Field field, int dimension) {
        JsonNode values;
        try {
            values = JSON.readTree(json);
        } catch (JsonProcessingException e) {
            throw new DorisConnectorException("Invalid 'query_vector' JSON: " + e.getOriginalMessage(), e);
        } catch (NumberFormatException e) {
            // A number whose exponent does not fit a BigDecimal, such as 1e99999999999.
            throw new DorisConnectorException("Invalid 'query_vector' JSON: " + e.getMessage(), e);
        }
        if (values == null || !values.isArray()) {
            throw new DorisConnectorException("'query_vector' must be a JSON array");
        }
        if (dimension >= 0 && values.size() != dimension) {
            throw new DorisConnectorException("Query vector dimension " + values.size()
                    + " does not match Lance column '" + field.getName() + "' dimension " + dimension);
        }
        return values;
    }

    private static void encodeValues(JsonNode values, TVectorElementType elementType, ByteBuffer buffer) {
        for (int i = 0; i < values.size(); ++i) {
            JsonNode value = values.get(i);
            if (!value.isNumber()) {
                throw new DorisConnectorException("Query vector element " + i + " must be a number");
            }
            writeElement(value, i, elementType, buffer);
        }
    }

    private static void writeElement(JsonNode value, int index, TVectorElementType elementType, ByteBuffer buffer) {
        try {
            switch (elementType) {
                case FLOAT16:
                    short float16Bits = Float16.toFloat16(checkedFloat(value, index, elementType));
                    if (!Float.isFinite(Float16.toFloat(float16Bits))) {
                        throw outOfRange(index, elementType);
                    }
                    buffer.putShort(float16Bits);
                    return;
                case FLOAT32:
                    buffer.putFloat(checkedFloat(value, index, elementType));
                    return;
                case FLOAT64:
                    double doubleValue = Double.parseDouble(value.asText());
                    if (!Double.isFinite(doubleValue)) {
                        throw outOfRange(index, elementType);
                    }
                    buffer.putDouble(doubleValue);
                    return;
                case UINT8:
                    buffer.put((byte) checkedInteger(value.decimalValue(), index, BigDecimal.ZERO, UINT8_MAX,
                            elementType).intValue());
                    return;
                case INT8:
                    buffer.put(checkedInteger(value.decimalValue(), index, INT8_MIN, INT8_MAX, elementType)
                            .byteValue());
                    return;
                default:
                    throw new DorisConnectorException("Unsupported query vector element type " + elementType);
            }
        } catch (ArithmeticException | NumberFormatException e) {
            throw outOfRange(index, elementType);
        }
    }

    private static float checkedFloat(JsonNode value, int index, TVectorElementType type) {
        double parsed = Double.parseDouble(value.asText());
        float converted = (float) parsed;
        if (!Double.isFinite(parsed) || !Float.isFinite(converted)) {
            throw outOfRange(index, type);
        }
        return converted;
    }

    private static BigInteger checkedInteger(BigDecimal value, int index, BigDecimal min, BigDecimal max,
            TVectorElementType type) {
        // Checked on the decimal as written: converting first would expand an exponent such as 1e500000000 or
        // 1e-500000000 into a number of that many digits.
        if (value.compareTo(min) < 0 || value.compareTo(max) > 0 || value.stripTrailingZeros().scale() > 0) {
            throw outOfRange(index, type);
        }
        return value.toBigIntegerExact();
    }

    private static boolean hasExtension(Field field) {
        return field.getMetadata() != null
                && field.getMetadata().get(ARROW_EXTENSION_NAME) != null
                && !field.getMetadata().get(ARROW_EXTENSION_NAME).isEmpty();
    }

    private static DorisConnectorException unsupportedVectorType(Field field) {
        return new DorisConnectorException("Lance vector column '" + field.getName()
                + "' must be fixed_size_list<float16|float32|float64|uint8|int8> or "
                + "list<fixed_size_list<float16|float32|float64>>, but was " + field.getType());
    }

    private static DorisConnectorException outOfRange(int index, TVectorElementType type) {
        return new DorisConnectorException("Query vector element " + index
                + " is not representable as " + type.name().toLowerCase(Locale.ROOT));
    }

    /** How the vectors of one column are encoded: their dimension, element type and its width in bytes. */
    private static final class Encoding {
        private final int dimension;
        private final TVectorElementType elementType;
        private final int byteWidth;

        private Encoding(int dimension, TVectorElementType elementType, int byteWidth) {
            this.dimension = dimension;
            this.elementType = elementType;
            this.byteWidth = byteWidth;
        }
    }
}
