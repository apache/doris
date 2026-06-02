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

package org.apache.doris.analysis;

import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.TimestampTzLiteral;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.TimeStampTzType;
import org.apache.doris.nereids.types.coercion.CharacterType;

import com.google.common.base.Preconditions;

import java.math.BigDecimal;
import java.math.RoundingMode;

public class LiteralExprUtils {

    public static LiteralExpr createLiteral(String value, Type type) throws AnalysisException {
        Preconditions.checkArgument(!type.equals(Type.INVALID));
        LiteralExpr literalExpr = null;
        switch (type.getPrimitiveType()) {
            case NULL_TYPE:
                literalExpr = new NullLiteral();
                break;
            case BOOLEAN:
                literalExpr = new BoolLiteral(value);
                break;
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
                literalExpr = new IntLiteral(value, type);
                break;
            case LARGEINT:
                literalExpr = new LargeIntLiteral(value);
                break;
            case FLOAT:
            case DOUBLE:
                literalExpr = createFloatingPointLiteral(value, type);
                break;
            case DECIMALV2:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
            case DECIMAL256:
                literalExpr = createDecimalLiteral(value, type);
                break;
            case CHAR:
            case VARCHAR:
            case HLL:
            case STRING:
                literalExpr = new StringLiteral(value);
                literalExpr.setType(type);
                break;
            case JSONB:
                literalExpr = new JsonLiteral(value);
                break;
            case DATE:
            case DATETIME:
            case DATEV2:
            case DATETIMEV2:
                literalExpr = DateLiteralUtils.createDateLiteral(value, type);
                break;
            case TIMESTAMPTZ:
                literalExpr = createTimestampTzLiteral(value, type);
                break;
            case IPV4:
                literalExpr = new IPv4Literal(value);
                break;
            case IPV6:
                literalExpr = new IPv6Literal(value);
                break;
            default:
                throw new AnalysisException("Type[" + type.toSql() + "] not supported.");
        }

        Preconditions.checkNotNull(literalExpr);
        return literalExpr;
    }

    private static LiteralExpr createFloatingPointLiteral(String value, Type type) throws AnalysisException {
        try {
            return new FloatLiteral(Double.parseDouble(value), type);
        } catch (NumberFormatException e) {
            throw new AnalysisException("Invalid floating-point literal: " + value, e);
        }
    }

    private static LiteralExpr createDecimalLiteral(String value, Type type) throws AnalysisException {
        Preconditions.checkArgument(type instanceof ScalarType);
        ScalarType scalarType = (ScalarType) type;
        BigDecimal decimalValue;
        try {
            decimalValue = new BigDecimal(value);
        } catch (NumberFormatException e) {
            throw new AnalysisException("Invalid floating-point literal: " + value, e);
        }
        decimalValue = decimalValue.setScale(scalarType.getScalarScale(), RoundingMode.HALF_UP);
        DecimalLiteral literalExpr = new DecimalLiteral(decimalValue, type);
        literalExpr.checkPrecisionAndScale(scalarType.getScalarPrecision(), scalarType.getScalarScale());
        return literalExpr;
    }

    private static LiteralExpr createTimestampTzLiteral(String value, Type type) throws AnalysisException {
        Preconditions.checkArgument(type instanceof ScalarType && type.isTimeStampTz());
        try {
            int scale = ((ScalarType) type).getScalarScale();
            return TimestampTzLiteral.fromSessionTimeZone(TimeStampTzType.of(scale), value).toLegacyLiteral();
        } catch (RuntimeException e) {
            throw new AnalysisException("Invalid TIMESTAMPTZ literal: " + value, e);
        }
    }

    public static LiteralExpr createInfinity(Type type, boolean isMax) throws AnalysisException {
        Preconditions.checkArgument(!type.equals(Type.INVALID));
        if (isMax) {
            return MaxLiteral.MAX_VALUE;
        }
        switch (type.getPrimitiveType()) {
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
                return IntLiteral.createMinValue(type);
            case LARGEINT:
                return LargeIntLiteral.createMinValue();
            case DATE:
            case DATETIME:
            case DATEV2:
            case DATETIMEV2:
            case TIMESTAMPTZ:
                return DateLiteral.createMinValue(type);
            default:
                throw new AnalysisException("Invalid data type for creating infinity: " + type);
        }
    }


    public static PlaceHolderExpr createPlaceHolderExpr(String value, Type type) throws AnalysisException {
        Preconditions.checkArgument(!type.equals(Type.INVALID));
        return new PlaceHolderExpr(LiteralExprUtils.createLiteral(value, type));
    }

    public static String normalizePartitionValueString(String value, Type type) throws AnalysisException {
        DataType dataType = DataType.fromCatalogType(type);
        if (dataType.isDateTimeType()) {
            return new DateTimeLiteral(value).checkedCastTo(dataType).toString();
        } else if (dataType.isDateTimeV2Type()) {
            return new DateTimeV2Literal(value).checkedCastTo(dataType).toString();
        } else if (dataType.isTimeStampTzType()) {
            return TimestampTzLiteral.fromSessionTimeZone((TimeStampTzType) dataType, value).checkedCastTo(dataType)
                    .toString();
        } else if (dataType.isCharType() || dataType.isVarcharType()) {
            CharacterType characterType = (CharacterType) dataType;
            if (characterType.isLengthSet() && value.length() > characterType.getLen()) {
                throw new AnalysisException(String.format(
                        "Partition value %s's length exceeds type length: %d > %d for %s",
                        value, value.length(), characterType.getLen(), dataType));
            }
        }
        return value;
    }
}
