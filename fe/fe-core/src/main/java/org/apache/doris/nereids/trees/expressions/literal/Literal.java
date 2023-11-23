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

package org.apache.doris.nereids.trees.expressions.literal;

import org.apache.doris.analysis.BoolLiteral;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.Config;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.exceptions.UnboundException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.shape.LeafExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.CharType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DecimalV2Type;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.LargeIntType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.VarcharType;

import com.google.common.collect.ImmutableList;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Locale;
import java.util.Objects;

/**
 * All data type literal expression in Nereids.
 * TODO: Increase the implementation of sub expression. such as Integer.
 */
public abstract class Literal extends Expression implements LeafExpression, Comparable<Literal> {

    protected final DataType dataType;

    /**
     * Constructor for Literal.
     *
     * @param dataType logical data type in Nereids
     */
    public Literal(DataType dataType) {
        super(ImmutableList.of());
        this.dataType = Objects.requireNonNull(dataType);
    }

    /**
     * Get literal according to value type
     */
    public static Literal of(Object value) {
        if (value == null) {
            return new NullLiteral();
        } else if (value instanceof Byte) {
            return new TinyIntLiteral((Byte) value);
        } else if (value instanceof Short) {
            return new SmallIntLiteral((Short) value);
        } else if (value instanceof Integer) {
            return new IntegerLiteral((Integer) value);
        } else if (value instanceof Long) {
            return new BigIntLiteral((Long) value);
        } else if (value instanceof BigInteger) {
            return new LargeIntLiteral((BigInteger) value);
        } else if (value instanceof Float) {
            return new FloatLiteral((Float) value);
        } else if (value instanceof Double) {
            return new DoubleLiteral((Double) value);
        } else if (value instanceof BigDecimal) {
            if (Config.enable_decimal_conversion) {
                return new DecimalV3Literal((BigDecimal) value);
            } else {
                return new DecimalLiteral((BigDecimal) value);
            }
        } else if (value instanceof Boolean) {
            return BooleanLiteral.of((Boolean) value);
        } else if (value instanceof String) {
            return new StringLiteral((String) value);
        } else {
            throw new RuntimeException();
        }
    }

    public abstract Object getValue();

    /**
     * Map literal to double, and keep "<=" order.
     * for numeric literal (int/long/double/float), directly convert to double
     * for char/varchar/string, we take first 8 chars as a int64, and convert it to double
     * for other literals, getDouble() is not used.
     * <p>
     * And hence, we could express the range of a datatype, and used in stats derive.
     * for example:
     * 'abcxxxxxxxxxxx' is between ('abb', 'zzz')
     *
     * @return double representation of literal.
     */
    public double getDouble() {
        try {
            return Double.parseDouble(getValue().toString());
        } catch (Exception e) {
            return 0.0;
        }
    }

    public String getStringValue() {
        return String.valueOf(getValue());
    }

    @Override
    public DataType getDataType() throws UnboundException {
        return dataType;
    }

    @Override
    public String toSql() {
        return toString();
    }

    @Override
    public boolean nullable() throws UnboundException {
        return this instanceof NullLiteral;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitLiteral(this, context);
    }

    /**
     * literal expr compare.
     */
    @Override
    public int compareTo(Literal other) {
        return toLegacyLiteral().compareLiteral(other.toLegacyLiteral());
    }

    /**
     * literal expr compare.
     */
    @Override
    public Expression checkedCastTo(DataType targetType) throws AnalysisException {
        if (getDataType().isNumericType()) {
            String desc = getStringValue();
            BigDecimal val = new BigDecimal(desc);
            BigDecimal maxVal = val;
            BigDecimal minVal = val;
            if (targetType.isTinyIntType()) {
                maxVal = new BigDecimal(Byte.MAX_VALUE);
                minVal = new BigDecimal(Byte.MIN_VALUE);
            } else if (targetType.isSmallIntType()) {
                maxVal = new BigDecimal(Short.MAX_VALUE);
                minVal = new BigDecimal(Short.MIN_VALUE);
            } else if (targetType.isIntegerType()) {
                maxVal = new BigDecimal(Integer.MAX_VALUE);
                minVal = new BigDecimal(Integer.MIN_VALUE);
            } else if (targetType.isBigIntType()) {
                maxVal = new BigDecimal(Long.MAX_VALUE);
                minVal = new BigDecimal(Long.MIN_VALUE);
            } else if (targetType.isLargeIntType()) {
                maxVal = new BigDecimal(LargeIntType.MAX_VALUE);
                minVal = new BigDecimal(LargeIntType.MIN_VALUE);
            } else if (targetType.isFloatType()) {
                maxVal = new BigDecimal(Float.MAX_VALUE);
                minVal = new BigDecimal(-Float.MAX_VALUE);
            } else if (targetType.isDoubleType()) {
                maxVal = new BigDecimal(Double.MAX_VALUE);
                minVal = new BigDecimal(-Double.MAX_VALUE);
            }

            if (val.compareTo(maxVal) > 0 || val.compareTo(minVal) < 0) {
                throw new AnalysisException(
                        String.format("{} can't cast to {}", desc, targetType));
            }
        }
        return uncheckedCastTo(targetType);
    }

    @Override
    protected Expression uncheckedCastTo(DataType targetType) throws AnalysisException {
        String desc = getStringValue();
        if (targetType.isBooleanType()) {
            if ("0".equals(desc) || "false".equals(desc.toLowerCase(Locale.ROOT))) {
                return Literal.of(false);
            }
            if ("1".equals(desc) || "true".equals(desc.toLowerCase(Locale.ROOT))) {
                return Literal.of(true);
            }
        }
        if (targetType.isTinyIntType()) {
            return Literal.of(Double.valueOf(desc).byteValue());
        } else if (targetType.isSmallIntType()) {
            return Literal.of(Double.valueOf(desc).shortValue());
        } else if (targetType.isIntegerType()) {
            return Literal.of(Double.valueOf(desc).intValue());
        } else if (targetType.isBigIntType()) {
            return Literal.of(Long.valueOf(desc));
        } else if (targetType.isLargeIntType()) {
            return Literal.of(new BigDecimal(desc).toBigInteger());
        } else if (targetType.isFloatType()) {
            return Literal.of(Double.valueOf(desc).floatValue());
        } else if (targetType.isDoubleType()) {
            return Literal.of(Double.parseDouble(desc));
        } else if (targetType.isCharType()) {
            if (((CharType) targetType).getLen() >= desc.length()) {
                return new CharLiteral(desc, ((CharType) targetType).getLen());
            }
        } else if (targetType.isVarcharType()) {
            return new VarcharLiteral(desc, ((VarcharType) targetType).getLen());
        } else if (targetType instanceof StringType) {
            return new StringLiteral(desc);
        } else if (targetType.isDateType()) {
            return new DateLiteral(desc);
        } else if (targetType.isDateTimeType()) {
            return new DateTimeLiteral(desc);
        } else if (targetType.isDecimalV2Type()) {
            return new DecimalLiteral((DecimalV2Type) targetType, new BigDecimal(desc));
        } else if (targetType.isDecimalV3Type()) {
            return new DecimalV3Literal((DecimalV3Type) targetType, new BigDecimal(desc));
        } else if (targetType.isDateV2Type()) {
            return new DateV2Literal(desc);
        } else if (targetType.isDateTimeV2Type()) {
            return new DateTimeV2Literal((DateTimeV2Type) targetType, desc);
        }
        throw new AnalysisException("cannot cast " + desc + " from type " + this.dataType + " to type " + targetType);
    }

    /** fromLegacyLiteral */
    public static Literal fromLegacyLiteral(LiteralExpr literalExpr, Type type) {
        DataType dataType = DataType.fromCatalogType(type);
        if (literalExpr instanceof org.apache.doris.analysis.MaxLiteral) {
            return new MaxLiteral(dataType);
        }
        String stringValue = literalExpr.getStringValue();
        if (dataType.isBooleanType()) {
            return ((BoolLiteral) literalExpr).getValue() ? BooleanLiteral.TRUE : BooleanLiteral.FALSE;
        } else if (dataType.isTinyIntType()) {
            return new TinyIntLiteral(Byte.parseByte(stringValue));
        } else if (dataType.isSmallIntType()) {
            return new SmallIntLiteral(Short.parseShort(stringValue));
        } else if (dataType.isIntegerType()) {
            return new IntegerLiteral(Integer.parseInt(stringValue));
        } else if (dataType.isBigIntType()) {
            return new BigIntLiteral(Long.parseLong(stringValue));
        } else if (dataType.isLargeIntType()) {
            return new LargeIntLiteral(new BigInteger(stringValue));
        } else if (dataType.isStringType()) {
            return new StringLiteral(stringValue);
        } else if (dataType.isCharType()) {
            return new CharLiteral(stringValue, ((CharType) dataType).getLen());
        } else if (dataType.isVarcharType()) {
            return new VarcharLiteral(stringValue, ((VarcharType) dataType).getLen());
        } else if (dataType.isFloatType()) {
            return new FloatLiteral(Float.parseFloat(stringValue));
        } else if (dataType.isDoubleType()) {
            return new DoubleLiteral(Double.parseDouble(stringValue));
        } else if (dataType.isDecimalV2Type()) {
            return new DecimalLiteral((DecimalV2Type) dataType, new BigDecimal(stringValue));
        } else if (dataType.isDecimalV3Type()) {
            return new DecimalV3Literal((DecimalV3Type) dataType, new BigDecimal(stringValue));
        } else if (dataType.isDateType()) {
            return new DateLiteral(stringValue);
        } else if (dataType.isDateV2Type()) {
            return new DateV2Literal(stringValue);
        } else if (dataType.isDateTimeType()) {
            return new DateTimeLiteral(stringValue);
        } else if (dataType.isDateTimeV2Type()) {
            return new DateTimeV2Literal(stringValue);
        } else {
            throw new AnalysisException("Unsupported convert the " + literalExpr.getType()
                    + " of legacy literal to nereids literal");
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Literal other = (Literal) o;
        return Objects.equals(getValue(), other.getValue());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(getValue());
    }

    @Override
    public String toString() {
        return String.valueOf(getValue());
    }

    public abstract LiteralExpr toLegacyLiteral();

    public boolean isStringLikeLiteral() {
        return dataType.isStringLikeType();
    }
}
