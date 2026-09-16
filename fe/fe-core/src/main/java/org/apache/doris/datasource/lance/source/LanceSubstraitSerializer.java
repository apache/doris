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

package org.apache.doris.datasource.lance.source;

import io.substrait.expression.Expression;
import io.substrait.expression.proto.ExpressionProtoConverter;
import io.substrait.extension.ExtensionCollector;
import io.substrait.proto.ExpressionReference;
import io.substrait.proto.ExtendedExpression;
import io.substrait.proto.NamedStruct;
import io.substrait.proto.Type.Nullability;
import io.substrait.relation.RelProtoConverter;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;
import java.util.Optional;

/** Encodes Lance's Substrait schema, type variations and expression envelope. */
final class LanceSubstraitSerializer {
    private static final int MAX_SUBSTRAIT_DECIMAL_PRECISION = 38;
    private final Schema schema;

    LanceSubstraitSerializer(Schema schema) {
        this.schema = schema;
    }

    // DataFusion represents Arrow unsigned integers as the corresponding Substrait signed
    // integer type class with type variation 1.
    private static final int UNSIGNED_INTEGER_TYPE_VARIATION_REFERENCE = 1;
    // DataFusion distinguishes Arrow LargeUtf8 from Utf8 with the Substrait
    // large-container type variation.
    private static final int LARGE_CONTAINER_TYPE_VARIATION_REFERENCE = 1;
    private static final String UNSUPPORTED_FIELD_PREFIX = "__unlikely_name_placeholder_doris_";

    static boolean supportsType(ArrowType type) {
        /*
         * Keep this allow-list aligned with convertLiteral(), toSubstraitType(), and
         * toSubstraitProtoType(). A type is pushed only when Doris can encode both its field and
         * literals without changing comparison or null semantics across FE -> lance-c -> DataFusion.
         *
         * Supported scalar types:
         * - Bool; signed/unsigned Int8/16/32/64; Float32/64; Utf8/LargeUtf8.
         * - Decimal128 with a valid non-negative scale and Substrait precision up to 38.
         * - Date32 (DAY) and timezone-free Timestamp in SECOND/MILLISECOND/MICROSECOND units.
         *
         * Types intentionally left as Doris residual predicates:
         * - Float16, because this converter has no lossless Substrait FP16 field/literal mapping.
         * - Decimal256, because Substrait decimal precision is limited to 38 and the 256-bit
         *   physical mapping has no end-to-end differential coverage.
         * - Date64 (MILLISECOND), because Substrait Date is expressed as days and conversion would
         *   truncate values rather than preserve Arrow semantics.
         * - Timestamp(NANOSECOND), because Doris timestamp predicates preserve at most
         *   microseconds; timezone-aware Timestamp, because Arrow instant semantics must be
         *   reconciled with Doris session-timezone semantics first.
         * - Binary variants, Time, Duration, Interval, and nested/container types, because their
         *   field/literal or nested-reference mappings have not yet been implemented and covered
         *   by differential tests.
         */
        if (type instanceof ArrowType.Int) {
            ArrowType.Int integer = (ArrowType.Int) type;
            return integer.getBitWidth() == 8 || integer.getBitWidth() == 16
                    || integer.getBitWidth() == 32 || integer.getBitWidth() == 64;
        }
        if (type instanceof ArrowType.FloatingPoint) {
            FloatingPointPrecision precision = ((ArrowType.FloatingPoint) type).getPrecision();
            return precision == FloatingPointPrecision.SINGLE || precision == FloatingPointPrecision.DOUBLE;
        }
        if (type instanceof ArrowType.Decimal) {
            ArrowType.Decimal decimal = (ArrowType.Decimal) type;
            return decimal.getBitWidth() == 128
                    && decimal.getPrecision() > 0
                    && decimal.getPrecision() <= MAX_SUBSTRAIT_DECIMAL_PRECISION
                    && decimal.getScale() >= 0
                    && decimal.getScale() <= decimal.getPrecision();
        }
        if (type instanceof ArrowType.Date) {
            return ((ArrowType.Date) type).getUnit() == DateUnit.DAY;
        }
        if (type instanceof ArrowType.Timestamp) {
            ArrowType.Timestamp timestamp = (ArrowType.Timestamp) type;
            String timezone = timestamp.getTimezone();
            return (timezone == null || timezone.isEmpty())
                    && timestampPrecision(timestamp) >= 0;
        }
        return type instanceof ArrowType.Bool
                || type instanceof ArrowType.Utf8
                || type instanceof ArrowType.LargeUtf8;
    }

    static Type fieldType(Field field) {
        TypeCreator creator = TypeCreator.of(field.isNullable());
        ArrowType type = field.getType();
        if (type instanceof ArrowType.Bool) {
            return creator.BOOLEAN;
        } else if (type instanceof ArrowType.Int) {
            switch (((ArrowType.Int) type).getBitWidth()) {
                case 8:
                    return creator.I8;
                case 16:
                    return creator.I16;
                case 32:
                    return creator.I32;
                case 64:
                    return creator.I64;
                default:
                    break;
            }
        } else if (type instanceof ArrowType.FloatingPoint) {
            FloatingPointPrecision precision = ((ArrowType.FloatingPoint) type).getPrecision();
            if (precision == FloatingPointPrecision.SINGLE) {
                return creator.FP32;
            }
            if (precision == FloatingPointPrecision.DOUBLE) {
                return creator.FP64;
            }
        } else if (type instanceof ArrowType.Decimal) {
            ArrowType.Decimal decimal = (ArrowType.Decimal) type;
            return creator.decimal(decimal.getPrecision(), decimal.getScale());
        } else if (type instanceof ArrowType.Utf8 || type instanceof ArrowType.LargeUtf8) {
            return creator.STRING;
        } else if (type instanceof ArrowType.Date) {
            return creator.DATE;
        } else if (type instanceof ArrowType.Timestamp) {
            return creator.precisionTimestamp(timestampPrecision((ArrowType.Timestamp) type));
        }
        throw new IllegalArgumentException("Unsupported Lance Substrait field type: " + type);
    }

    byte[] serialize(Expression expression) {
        ExtensionCollector extensionCollector = new ExtensionCollector();
        RelProtoConverter relConverter = new RelProtoConverter(extensionCollector);
        ExpressionProtoConverter expressionConverter =
                new ExpressionProtoConverter(extensionCollector, relConverter);
        io.substrait.proto.Expression protoExpression = expression.accept(expressionConverter);

        NamedStruct.Builder schemaBuilder = NamedStruct.newBuilder();
        io.substrait.proto.Type.Struct.Builder structBuilder = io.substrait.proto.Type.Struct.newBuilder()
                .setNullability(Nullability.NULLABILITY_REQUIRED);
        List<Field> fields = schema.getFields();
        for (int ordinal = 0; ordinal < fields.size(); ordinal++) {
            Field field = fields.get(ordinal);
            Optional<io.substrait.proto.Type> type = toSubstraitProtoType(field);
            if (type.isPresent()) {
                schemaBuilder.addNames(field.getName());
                structBuilder.addTypes(type.get());
            } else {
                // Lance removes user-defined top-level fields before handing the
                // ExtendedExpression to DataFusion and remaps field ordinals. This keeps the
                // envelope aligned with the full dataset schema when unrelated complex or
                // otherwise unsupported columns are present.
                schemaBuilder.addNames(UNSUPPORTED_FIELD_PREFIX + ordinal);
                structBuilder.addTypes(io.substrait.proto.Type.newBuilder()
                        .setUserDefined(io.substrait.proto.Type.UserDefined.newBuilder()
                                .setTypeReference(0)
                                .setNullability(nullability(field)))
                        .build());
            }
        }
        schemaBuilder.setStruct(structBuilder);

        protoExpression = applyTypeVariations(protoExpression);
        ExpressionReference expressionReference = ExpressionReference.newBuilder()
                .setExpression(protoExpression)
                .addOutputNames("filter_mask")
                .build();
        ExtendedExpression.Builder extendedExpression = ExtendedExpression.newBuilder()
                .setVersion(io.substrait.proto.Version.newBuilder()
                        .setMajorNumber(0)
                        .setMinorNumber(70)
                        .setPatchNumber(0)
                        .setProducer("doris-lance")
                        .build())
                .setBaseSchema(schemaBuilder)
                .addReferredExpr(expressionReference);
        extensionCollector.addExtensionsToExtendedExpression(extendedExpression);
        return extendedExpression.build().toByteArray();
    }

    private static Optional<io.substrait.proto.Type> toSubstraitProtoType(Field field) {
        if (!supportsType(field.getType())) {
            return Optional.empty();
        }
        ArrowType type = field.getType();
        io.substrait.proto.Type.Builder builder = io.substrait.proto.Type.newBuilder();
        if (type instanceof ArrowType.Bool) {
            return Optional.of(builder.setBool(io.substrait.proto.Type.Boolean.newBuilder()
                    .setNullability(nullability(field))).build());
        } else if (type instanceof ArrowType.Int) {
            ArrowType.Int integer = (ArrowType.Int) type;
            int typeVariationReference = integer.getIsSigned()
                    ? 0 : UNSIGNED_INTEGER_TYPE_VARIATION_REFERENCE;
            switch (integer.getBitWidth()) {
                case 8:
                    return Optional.of(builder.setI8(io.substrait.proto.Type.I8.newBuilder()
                            .setTypeVariationReference(typeVariationReference)
                            .setNullability(nullability(field))).build());
                case 16:
                    return Optional.of(builder.setI16(io.substrait.proto.Type.I16.newBuilder()
                            .setTypeVariationReference(typeVariationReference)
                            .setNullability(nullability(field))).build());
                case 32:
                    return Optional.of(builder.setI32(io.substrait.proto.Type.I32.newBuilder()
                            .setTypeVariationReference(typeVariationReference)
                            .setNullability(nullability(field))).build());
                case 64:
                    return Optional.of(builder.setI64(io.substrait.proto.Type.I64.newBuilder()
                            .setTypeVariationReference(typeVariationReference)
                            .setNullability(nullability(field))).build());
                default:
                    return Optional.empty();
            }
        } else if (type instanceof ArrowType.FloatingPoint) {
            FloatingPointPrecision precision = ((ArrowType.FloatingPoint) type).getPrecision();
            if (precision == FloatingPointPrecision.SINGLE) {
                return Optional.of(builder.setFp32(io.substrait.proto.Type.FP32.newBuilder()
                        .setNullability(nullability(field))).build());
            }
            if (precision == FloatingPointPrecision.DOUBLE) {
                return Optional.of(builder.setFp64(io.substrait.proto.Type.FP64.newBuilder()
                        .setNullability(nullability(field))).build());
            }
            return Optional.empty();
        } else if (type instanceof ArrowType.Decimal) {
            ArrowType.Decimal decimal = (ArrowType.Decimal) type;
            return Optional.of(builder.setDecimal(io.substrait.proto.Type.Decimal.newBuilder()
                    .setPrecision(decimal.getPrecision())
                    .setScale(decimal.getScale())
                    .setNullability(nullability(field))).build());
        } else if (type instanceof ArrowType.Utf8 || type instanceof ArrowType.LargeUtf8) {
            int typeVariationReference = type instanceof ArrowType.LargeUtf8
                    ? LARGE_CONTAINER_TYPE_VARIATION_REFERENCE : 0;
            return Optional.of(builder.setString(io.substrait.proto.Type.String.newBuilder()
                    .setTypeVariationReference(typeVariationReference)
                    .setNullability(nullability(field))).build());
        } else if (type instanceof ArrowType.Date) {
            return Optional.of(builder.setDate(io.substrait.proto.Type.Date.newBuilder()
                    .setNullability(nullability(field))).build());
        } else if (type instanceof ArrowType.Timestamp) {
            return Optional.of(builder.setPrecisionTimestamp(
                    io.substrait.proto.Type.PrecisionTimestamp.newBuilder()
                            .setPrecision(timestampPrecision((ArrowType.Timestamp) type))
                            .setNullability(nullability(field))).build());
        } else {
            return Optional.empty();
        }
    }

    static int timestampPrecision(ArrowType.Timestamp timestamp) {
        switch (timestamp.getUnit()) {
            case SECOND:
                return 0;
            case MILLISECOND:
                return 3;
            case MICROSECOND:
                return 6;
            default:
                return -1;
        }
    }

    private io.substrait.proto.Expression applyTypeVariations(
            io.substrait.proto.Expression expression) {
        io.substrait.proto.Expression.Builder expressionBuilder = expression.toBuilder();
        if (expression.hasScalarFunction()) {
            io.substrait.proto.Expression.ScalarFunction.Builder functionBuilder =
                    expression.getScalarFunction().toBuilder();
            for (int i = 0; i < functionBuilder.getArgumentsCount(); i++) {
                io.substrait.proto.FunctionArgument argument = functionBuilder.getArguments(i);
                if (argument.hasValue()) {
                    functionBuilder.getArgumentsBuilder(i).setValue(
                            applyTypeVariations(argument.getValue()));
                }
            }

            ArrowType argumentFieldType = null;
            for (int i = 0; i < functionBuilder.getArgumentsCount() && argumentFieldType == null; i++) {
                io.substrait.proto.FunctionArgument argument = functionBuilder.getArguments(i);
                if (argument.hasValue()) {
                    argumentFieldType = fieldType(argument.getValue());
                }
            }
            if (argumentFieldType instanceof ArrowType.Int
                    && !((ArrowType.Int) argumentFieldType).getIsSigned()) {
                int bitWidth = ((ArrowType.Int) argumentFieldType).getBitWidth();
                for (int i = 0; i < functionBuilder.getArgumentsCount(); i++) {
                    io.substrait.proto.FunctionArgument argument = functionBuilder.getArguments(i);
                    if (argument.hasValue()) {
                        functionBuilder.getArgumentsBuilder(i).setValue(
                                applyUnsignedIntegerLiteralVariation(
                                        argument.getValue(), bitWidth));
                    }
                }
            }
            if (argumentFieldType instanceof ArrowType.LargeUtf8) {
                for (int i = 0; i < functionBuilder.getArgumentsCount(); i++) {
                    io.substrait.proto.FunctionArgument argument = functionBuilder.getArguments(i);
                    if (argument.hasValue()) {
                        functionBuilder.getArgumentsBuilder(i).setValue(
                                applyLargeUtf8LiteralVariation(argument.getValue()));
                    }
                }
            }
            expressionBuilder.setScalarFunction(functionBuilder);
        } else if (expression.hasSingularOrList()) {
            io.substrait.proto.Expression.SingularOrList.Builder inBuilder =
                    expression.getSingularOrList().toBuilder();
            inBuilder.setValue(applyTypeVariations(inBuilder.getValue()));
            ArrowType valueType = fieldType(inBuilder.getValue());
            for (int i = 0; i < inBuilder.getOptionsCount(); i++) {
                io.substrait.proto.Expression option =
                        applyTypeVariations(inBuilder.getOptions(i));
                if (valueType instanceof ArrowType.Int
                        && !((ArrowType.Int) valueType).getIsSigned()) {
                    option = applyUnsignedIntegerLiteralVariation(
                            option, ((ArrowType.Int) valueType).getBitWidth());
                }
                if (valueType instanceof ArrowType.LargeUtf8) {
                    option = applyLargeUtf8LiteralVariation(option);
                }
                inBuilder.setOptions(i, option);
            }
            expressionBuilder.setSingularOrList(inBuilder);
        }
        return expressionBuilder.build();
    }

    private ArrowType fieldType(io.substrait.proto.Expression expression) {
        if (!expression.hasSelection()
                || !expression.getSelection().hasDirectReference()
                || !expression.getSelection().getDirectReference().hasStructField()) {
            return null;
        }
        int ordinal = expression.getSelection().getDirectReference().getStructField().getField();
        if (ordinal < 0 || ordinal >= schema.getFields().size()) {
            return null;
        }
        return schema.getFields().get(ordinal).getType();
    }

    private static io.substrait.proto.Expression applyUnsignedIntegerLiteralVariation(
            io.substrait.proto.Expression expression, int bitWidth) {
        if (!expression.hasLiteral()) {
            return expression;
        }
        io.substrait.proto.Expression.Literal literal = expression.getLiteral();
        boolean matchingWidth = (bitWidth == 8 && literal.hasI8())
                || (bitWidth == 16 && literal.hasI16())
                || (bitWidth == 32 && literal.hasI32())
                || (bitWidth == 64 && literal.hasI64());
        if (!matchingWidth) {
            return expression;
        }
        return expression.toBuilder()
                .setLiteral(literal.toBuilder().setTypeVariationReference(
                        UNSIGNED_INTEGER_TYPE_VARIATION_REFERENCE))
                .build();
    }

    private static io.substrait.proto.Expression applyLargeUtf8LiteralVariation(
            io.substrait.proto.Expression expression) {
        if (!expression.hasLiteral() || !expression.getLiteral().hasString()) {
            return expression;
        }
        return expression.toBuilder()
                .setLiteral(expression.getLiteral().toBuilder().setTypeVariationReference(
                        LARGE_CONTAINER_TYPE_VARIATION_REFERENCE))
                .build();
    }

    private static Nullability nullability(Field field) {
        return field.isNullable() ? Nullability.NULLABILITY_NULLABLE : Nullability.NULLABILITY_REQUIRED;
    }

}
