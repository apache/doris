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

package org.apache.doris.nereids.trees.plans.commands.info;

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.ArrayLiteral;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.MapLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StructLiteral;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;

import com.google.common.base.Preconditions;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;

/**
 * Validates a non-null ARRAY/MAP/STRUCT column default and rewrites it into a canonical literal text.
 *
 * <p>The user supplied text is parsed as a SQL literal and every nested value is cast to the declared
 * nested type, so a type mismatch is rejected at DDL time instead of when old rows are read. The
 * canonical text only contains plain nested values (unquoted numbers, double quoted strings, nested
 * brackets and NULL). BE parses the stored text in two places that must agree: the complex SerDe
 * {@code from_fe_string} used by the default value iterator and schema change, and the
 * string-to-complex cast used when INSERT fills an unmentioned column. Neither of them decodes
 * escape sequences, and the DDL parser keeps the text of a default value verbatim when SHOW CREATE
 * TABLE output is replayed, so string values containing quotes or backslashes are rejected instead
 * of stored.
 */
public class ComplexTypeDefaultValue {
    private ComplexTypeDefaultValue() {
    }

    /**
     * Validate the default literal of a complex column and return its canonical text.
     */
    public static String canonicalize(DataType type, String defaultValue) throws AnalysisException {
        Preconditions.checkArgument(type.isArrayType() || type.isMapType() || type.isStructType(),
                "%s is not a complex type", type);
        Expression expression;
        try {
            expression = new NereidsParser().parseExpression(defaultValue);
        } catch (Exception e) {
            throw literalShapeException(type);
        }
        if (!hasLiteralShape(expression, type)) {
            throw literalShapeException(type);
        }
        try {
            return render((Literal) expression, type);
        } catch (AnalysisException e) {
            throw new AnalysisException(String.format("Invalid default value '%s' for %s column: %s",
                    defaultValue, type.toSql(), e.getMessage()), e);
        }
    }

    private static boolean hasLiteralShape(Expression expression, DataType type) {
        if (type.isArrayType()) {
            return expression instanceof ArrayLiteral;
        }
        if (type.isMapType()) {
            return expression instanceof MapLiteral;
        }
        // `{}` parses as an empty map literal and means every struct field takes its own default.
        return expression instanceof StructLiteral
                || (expression instanceof MapLiteral && ((MapLiteral) expression).getValue().isEmpty());
    }

    private static AnalysisException literalShapeException(DataType type) {
        String literalKind = type.isArrayType() ? "array" : type.isMapType() ? "map" : "struct";
        return new AnalysisException(String.format("%s type column default value only supports %s literals"
                + " or DEFAULT NULL", capitalize(literalKind), literalKind));
    }

    private static String capitalize(String value) {
        return Character.toUpperCase(value.charAt(0)) + value.substring(1);
    }

    private static String render(Literal literal, DataType type) throws AnalysisException {
        if (literal instanceof NullLiteral) {
            return "NULL";
        }
        if (type.isArrayType()) {
            if (!(literal instanceof ArrayLiteral)) {
                throw new AnalysisException(literal.toSql() + " is not an array literal");
            }
            DataType itemType = ((ArrayType) type).getItemType();
            StringJoiner joiner = new StringJoiner(", ", "[", "]");
            for (Literal item : ((ArrayLiteral) literal).getValue()) {
                joiner.add(render(item, itemType));
            }
            return joiner.toString();
        }
        if (type.isMapType()) {
            if (!(literal instanceof MapLiteral)) {
                throw new AnalysisException(literal.toSql() + " is not a map literal");
            }
            MapType mapType = (MapType) type;
            // The parser keeps the last value of a repeated key, so a repeated key is stored once. Keys that
            // only collide after the cast to the key type (e.g. "01" and "1" for INT) are rejected: BE keeps
            // every entry of the stored text, while replaying it as a SQL literal would keep only the last.
            Set<String> keys = new HashSet<>();
            StringJoiner joiner = new StringJoiner(", ", "{", "}");
            for (Map.Entry<Literal, Literal> entry : ((MapLiteral) literal).getValue().entrySet()) {
                String key = render(entry.getKey(), mapType.getKeyType());
                if (!keys.add(key)) {
                    throw new AnalysisException(String.format("map key %s is repeated after casting to %s", key,
                            mapType.getKeyType().toSql()));
                }
                joiner.add(key + ":" + render(entry.getValue(), mapType.getValueType()));
            }
            return joiner.toString();
        }
        if (type.isStructType()) {
            // `{}` parses as an empty map literal and means every field takes its own default.
            if (literal instanceof MapLiteral && ((MapLiteral) literal).getValue().isEmpty()) {
                return "{}";
            }
            if (!(literal instanceof StructLiteral)) {
                throw new AnalysisException(literal.toSql() + " is not a struct literal");
            }
            List<StructField> fields = ((StructType) type).getFields();
            List<Literal> values = ((StructLiteral) literal).getValue();
            if (values.size() != fields.size()) {
                throw new AnalysisException(String.format("struct literal has %d fields but the column has %d",
                        values.size(), fields.size()));
            }
            StringJoiner joiner = new StringJoiner(", ", "{", "}");
            for (int i = 0; i < fields.size(); i++) {
                joiner.add(render(values.get(i), fields.get(i).getDataType()));
            }
            return joiner.toString();
        }
        return renderScalar((Literal) literal.checkedCastTo(type), type);
    }

    private static String renderScalar(Literal literal, DataType type) throws AnalysisException {
        if (type.isBooleanType()) {
            return ((BooleanLiteral) literal).getValue() ? "1" : "0";
        }
        if (type.isNumericType()) {
            if (type.isFloatLikeType() && !Double.isFinite(((Number) literal.getValue()).doubleValue())) {
                throw new AnalysisException(literal.toSql() + " is not a finite number");
            }
            return literal.getStringValue();
        }
        if (type.isStringLikeType() || type.isDateType() || type.isDateV2Type() || type.isDateTimeType()
                || type.isDateTimeV2Type() || type.isIPType()) {
            String value = literal.getStringValue();
            if (value.indexOf('"') >= 0 || value.indexOf('\'') >= 0 || value.indexOf('\\') >= 0) {
                throw new AnalysisException("string value " + literal.toSql()
                        + " must not contain quote or backslash");
            }
            return "\"" + value + "\"";
        }
        throw new AnalysisException(type.toSql() + " is not supported as a nested type of a default value");
    }
}
