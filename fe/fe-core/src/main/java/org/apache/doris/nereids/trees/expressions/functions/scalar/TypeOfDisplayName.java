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

package org.apache.doris.nereids.trees.expressions.functions.scalar;

import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLikeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.CharType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DecimalV2Type;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.nereids.types.TimeStampTzType;
import org.apache.doris.nereids.types.TimeV2Type;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.types.coercion.CharacterType;

import java.util.Locale;
import java.util.Optional;
import java.util.StringJoiner;

/** Utilities for Trino-compatible TYPEOF display names. */
final class TypeOfDisplayName {

    private TypeOfDisplayName() {
    }

    public static String fromExpression(Expression expression) {
        Expression candidate = unwrapStringLengthCast(expression);
        if (candidate instanceof Cast) {
            Optional<String> explicitTypeSql = ((Cast) candidate).getExplicitTypeSql();
            if (explicitTypeSql.isPresent()) {
                String typeName = fromExplicitTypeSql(explicitTypeSql.get());
                if (typeName != null) {
                    return typeName;
                }
            }
        }
        if (candidate instanceof StringLikeLiteral) {
            return formatLiteralString((StringLikeLiteral) candidate);
        }
        if (candidate instanceof IntegerLikeLiteral) {
            return formatIntegerLiteral((IntegerLikeLiteral) candidate);
        }
        if (candidate instanceof NullLiteral && candidate.getDataType().isNullType()) {
            return "unknown";
        }
        return fromDataType(candidate.getDataType());
    }

    public static String fromDataType(DataType dataType) {
        if (dataType.isNullType()) {
            return "unknown";
        }
        if (dataType.isBooleanType()) {
            return "boolean";
        }
        if (dataType.isTinyIntType()) {
            return "tinyint";
        }
        if (dataType.isSmallIntType()) {
            return "smallint";
        }
        if (dataType.isIntegerType()) {
            return "integer";
        }
        if (dataType.isBigIntType()) {
            return "bigint";
        }
        if (dataType.isLargeIntType()) {
            return "largeint";
        }
        if (dataType.isFloatType()) {
            return "real";
        }
        if (dataType.isDoubleType()) {
            return "double";
        }
        if (dataType.isDecimalV2Type()) {
            DecimalV2Type decimalV2Type = (DecimalV2Type) dataType;
            return formatDecimal(decimalV2Type.getPrecision(), decimalV2Type.getScale());
        }
        if (dataType.isDecimalV3Type()) {
            DecimalV3Type decimalV3Type = (DecimalV3Type) dataType;
            return formatDecimal(decimalV3Type.getPrecision(), decimalV3Type.getScale());
        }
        if (dataType.isDateType() || dataType.isDateV2Type()) {
            return "date";
        }
        if (dataType.isDateTimeType()) {
            return "timestamp(0)";
        }
        if (dataType.isDateTimeV2Type()) {
            return "timestamp(" + ((DateTimeV2Type) dataType).getScale() + ")";
        }
        if (dataType.isTimeType()) {
            return "time(" + ((TimeV2Type) dataType).getScale() + ")";
        }
        if (dataType.isTimeStampTzType()) {
            return "timestamp(" + ((TimeStampTzType) dataType).getScale() + ") with time zone";
        }
        if (dataType.isStringType()) {
            return "varchar";
        }
        if (dataType.isCharType()) {
            return formatCharType((CharType) dataType);
        }
        if (dataType.isVarcharType()) {
            return formatVarcharType((VarcharType) dataType);
        }
        if (dataType.isVarBinaryType()) {
            return "varbinary";
        }
        if (dataType.isArrayType()) {
            return "array(" + fromDataType(((ArrayType) dataType).getItemType()) + ")";
        }
        if (dataType.isMapType()) {
            MapType mapType = (MapType) dataType;
            return "map(" + fromDataType(mapType.getKeyType()) + ", "
                    + fromDataType(mapType.getValueType()) + ")";
        }
        if (dataType.isStructType()) {
            return formatStructType((StructType) dataType);
        }
        return normalizeFallbackType(dataType.toSql());
    }

    private static Expression unwrapStringLengthCast(Expression expression) {
        if (!(expression instanceof Substring)) {
            return expression;
        }
        Substring substring = (Substring) expression;
        if (!(substring.getSource() instanceof Cast)
                || !(substring.getPosition() instanceof IntegerLiteral)) {
            return expression;
        }
        Cast cast = (Cast) substring.getSource();
        if (!cast.isExplicitType() || !cast.getDataType().isStringLikeType()) {
            return expression;
        }
        IntegerLiteral position = (IntegerLiteral) substring.getPosition();
        if (position.getValue() != 1 || !substring.getLength().isPresent()
                || !(substring.getLength().get() instanceof IntegerLiteral)) {
            return expression;
        }
        IntegerLiteral length = (IntegerLiteral) substring.getLength().get();
        CharacterType characterType = (CharacterType) cast.getDataType();
        if (characterType.getLen() < 0 || characterType.getLen() != length.getValue()) {
            return expression;
        }
        return cast;
    }

    private static String fromExplicitTypeSql(String explicitTypeSql) {
        String normalized = explicitTypeSql.toLowerCase(Locale.ROOT).replaceAll("\\s+", "");
        if ("decimal".equals(normalized)) {
            return "decimal(38,0)";
        }
        if ("decimalv3".equals(normalized)) {
            return "decimal(38,0)";
        }
        return null;
    }

    private static String formatLiteralString(StringLikeLiteral literal) {
        if (literal.getDataType().isVarcharType()) {
            return formatVarcharType((VarcharType) literal.getDataType());
        }
        if (literal.getDataType().isCharType()) {
            return formatCharType((CharType) literal.getDataType());
        }
        return "varchar(" + literal.getValue().length() + ")";
    }

    private static String formatIntegerLiteral(IntegerLikeLiteral literal) {
        if (literal.getDataType().isBigIntType()) {
            return "bigint";
        }
        if (literal.getDataType().isLargeIntType()) {
            return "largeint";
        }
        return "integer";
    }

    private static String formatDecimal(int precision, int scale) {
        return "decimal(" + precision + "," + scale + ")";
    }

    private static String formatCharType(CharType charType) {
        return charType.getLen() < 0 ? "char" : "char(" + charType.getLen() + ")";
    }

    private static String formatVarcharType(VarcharType varcharType) {
        return varcharType.isWildcardVarchar()
                ? "varchar"
                : "varchar(" + varcharType.getLen() + ")";
    }

    private static String formatStructType(StructType structType) {
        StringJoiner joiner = new StringJoiner(", ", "row(", ")");
        for (StructField field : structType.getFields()) {
            String fieldName = field.getName().isEmpty()
                    ? StructField.DEFAULT_FIELD_NAME
                    : field.getName();
            joiner.add(fieldName + " " + fromDataType(field.getDataType()));
        }
        return joiner.toString();
    }

    private static String normalizeFallbackType(String typeSql) {
        return typeSql.toLowerCase(Locale.ROOT).replaceAll("\\s+", " ").trim();
    }
}
