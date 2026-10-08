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

package org.apache.doris.service.arrowflight;

import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DateV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DecimalV3Literal;
import org.apache.doris.nereids.trees.expressions.literal.DoubleLiteral;
import org.apache.doris.nereids.trees.expressions.literal.FloatLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.SmallIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DateV2Type;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.FloatType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.NullType;
import org.apache.doris.nereids.types.SmallIntType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.TinyIntType;

import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Converts one parameter row into detached values consumed by Nereids placeholder analysis. */
final class FlightSqlParameters {
    private static final long MAX_PARAMETER_BYTES = 1024 * 1024;
    static final int MAX_PARAMETERS = 1024;

    private FlightSqlParameters() {
    }

    static void bind(StatementContext context, List<Literal> parameters) {
        if (parameters == null || parameters.size() != context.getPlaceholders().size()) {
            throw CallStatus.INVALID_ARGUMENT.withDescription("Bind all query parameters before execution")
                    .toRuntimeException();
        }
        for (int i = 0; i < parameters.size(); i++) {
            context.getIdToPlaceholderRealExpr().put(
                    context.getPlaceholders().get(i).getPlaceholderId(), parameters.get(i));
        }
    }

    static List<Literal> read(FlightStream stream, int parameterCount) {
        List<Literal> parameters = null;
        FlightRuntimeException failure = null;
        while (stream.next()) {
            // Consume the upload even after validation fails so clients can receive the final gRPC status.
            if (failure != null) {
                continue;
            }
            try {
                VectorSchemaRoot root = stream.getRoot();
                if (root.getFieldVectors().size() != parameterCount) {
                    throw invalid("Parameter count does not match the prepared query");
                }
                if (root.getRowCount() == 0) {
                    continue;
                }
                if (root.getRowCount() != 1 || parameters != null) {
                    throw CallStatus.UNIMPLEMENTED.withDescription("Only one parameter row per binding is supported")
                            .toRuntimeException();
                }
                parameters = convert(root);
            } catch (FlightRuntimeException e) {
                failure = e;
            } catch (RuntimeException e) {
                failure = invalid("Invalid parameter value: " + e.getMessage());
            }
        }
        if (failure != null) {
            throw failure;
        }
        if (parameters == null) {
            if (parameterCount == 0) {
                return Collections.emptyList();
            }
            throw invalid("Parameter upload contains no row");
        }
        return parameters;
    }

    static List<Literal> convert(VectorSchemaRoot root) {
        if (root.getFieldVectors().size() > MAX_PARAMETERS) {
            throw invalid("Too many query parameters (maximum 1024)");
        }
        long bytes = 0;
        List<Literal> parameters = new ArrayList<>();
        for (FieldVector vector : root.getFieldVectors()) {
            bytes += vector.getBufferSize();
            if (bytes > MAX_PARAMETER_BYTES) {
                throw invalid("Query parameters exceed the 1 MiB binding limit");
            }
            parameters.add(literal(vector));
        }
        return parameters;
    }

    private static Literal literal(FieldVector vector) {
        if (vector.getField().getDictionary() != null) {
            throw CallStatus.UNIMPLEMENTED.withDescription("Dictionary encoded parameters are not supported")
                    .toRuntimeException();
        }
        DataType type;
        switch (vector.getMinorType()) {
            case NULL:
                type = NullType.INSTANCE;
                break;
            case BIT:
                type = BooleanType.INSTANCE;
                break;
            case TINYINT:
                type = TinyIntType.INSTANCE;
                break;
            case SMALLINT:
                type = SmallIntType.INSTANCE;
                break;
            case INT:
                type = IntegerType.INSTANCE;
                break;
            case BIGINT:
                type = BigIntType.INSTANCE;
                break;
            case FLOAT4:
                type = FloatType.INSTANCE;
                break;
            case FLOAT8:
                type = DoubleType.INSTANCE;
                break;
            case VARCHAR:
                type = StringType.INSTANCE;
                break;
            case DATEDAY:
                type = DateV2Type.INSTANCE;
                break;
            case TIMESTAMPSEC:
            case TIMESTAMPMILLI:
            case TIMESTAMPMICRO:
            case TIMESTAMPNANO:
                type = DateTimeV2Type.of(6);
                break;
            case DECIMAL:
                ArrowType.Decimal decimal = (ArrowType.Decimal) vector.getField().getType();
                if (decimal.getScale() < 0 || decimal.getScale() > decimal.getPrecision()) {
                    throw invalid("Unsupported decimal parameter scale");
                }
                type = DecimalV3Type.createDecimalV3Type(decimal.getPrecision(), decimal.getScale());
                break;
            default:
                throw CallStatus.UNIMPLEMENTED.withDescription(
                        "Unsupported query parameter type: " + vector.getField().getType()).toRuntimeException();
        }
        if (vector.isNull(0)) {
            return new NullLiteral(type);
        }
        Object value = vector.getObject(0);
        switch (vector.getMinorType()) {
            case BIT: return BooleanLiteral.of((Boolean) value);
            case TINYINT: return new TinyIntLiteral(((Number) value).byteValue());
            case SMALLINT: return new SmallIntLiteral(((Number) value).shortValue());
            case INT: return new IntegerLiteral(((Number) value).intValue());
            case BIGINT: return new BigIntLiteral(((Number) value).longValue());
            case FLOAT4:
            case FLOAT8:
                double number = ((Number) value).doubleValue();
                if (!Double.isFinite(number)) {
                    throw invalid("Non-finite floating point parameters are not supported");
                }
                return type instanceof FloatType ? new FloatLiteral((float) number) : new DoubleLiteral(number);
            case VARCHAR:
                try {
                    // Reject malformed UTF-8 instead of silently replacing bytes in a bound predicate.
                    return new StringLiteral(StandardCharsets.UTF_8.newDecoder()
                            .decode(ByteBuffer.wrap(((VarCharVector) vector).get(0))).toString());
                } catch (CharacterCodingException e) {
                    throw invalid("String parameter is not valid UTF-8");
                }
            case DECIMAL: return new DecimalV3Literal((DecimalV3Type) type, (BigDecimal) value);
            case DATEDAY:
                LocalDate date = LocalDate.ofEpochDay(((DateDayVector) vector).get(0));
                checkYear(date.getYear());
                return new DateV2Literal(date.getYear(), date.getMonthValue(), date.getDayOfMonth());
            default:
                long timestamp = ((TimeStampVector) vector).get(0);
                ArrowType.Timestamp timestampType = (ArrowType.Timestamp) vector.getField().getType();
                long units;
                switch (timestampType.getUnit()) {
                    case SECOND:
                        units = 1;
                        break;
                    case MILLISECOND:
                        units = 1000;
                        break;
                    case MICROSECOND:
                        units = 1000000;
                        break;
                    case NANOSECOND:
                        units = 1000000000;
                        break;
                    default:
                        throw invalid("Unsupported timestamp unit");
                }
                long nanos = Math.floorMod(timestamp, units) * (1000000000 / units);
                if (nanos % 1000 != 0) {
                    throw invalid("Timestamp parameter exceeds microsecond precision");
                }
                LocalDateTime time = LocalDateTime.ofEpochSecond(
                        Math.floorDiv(timestamp, units), (int) nanos, ZoneOffset.UTC);
                checkYear(time.getYear());
                return new DateTimeV2Literal((DateTimeV2Type) type, time.getYear(), time.getMonthValue(),
                        time.getDayOfMonth(), time.getHour(), time.getMinute(), time.getSecond(),
                        time.getNano() / 1000);
        }
    }

    private static void checkYear(int year) {
        if (year < 0 || year > 9999) {
            throw invalid("Date parameter is outside the supported year range 0000..9999");
        }
    }

    private static FlightRuntimeException invalid(String message) {
        return CallStatus.INVALID_ARGUMENT.withDescription(message).toRuntimeException();
    }
}
