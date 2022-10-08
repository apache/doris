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

import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.exceptions.UnboundException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.shape.LeafExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.CharType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.StringType;

import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Locale;
import java.util.Objects;

/**
 * All data type literal expression in Nereids.
 * TODO: Increase the implementation of sub expression. such as Integer.
 */
public abstract class Literal extends Expression implements LeafExpression {

    private final DataType dataType;

    /**
     * Constructor for Literal.
     *
     * @param dataType logical data type in Nereids
     */
    public Literal(DataType dataType) {
        this.dataType = dataType;
    }

    /**
     * Get literal according to value type
     */
    public static Literal of(Object value) {
        if (value == null) {
            return new NullLiteral();
        } else if (value instanceof Byte) {
            return new TinyIntLiteral((byte) value);
        } else if (value instanceof Short) {
            return new SmallIntLiteral((short) value);
        } else if (value instanceof Integer) {
            return new IntegerLiteral((int) value);
        } else if (value instanceof Long) {
            return new BigIntLiteral((long) value);
        } else if (value instanceof BigInteger) {
            return new LargeIntLiteral((BigInteger) value);
        } else if (value instanceof Float) {
            return new FloatLiteral((float) value);
        } else if (value instanceof Double) {
            return new DoubleLiteral((double) value);
        } else if (value instanceof Boolean) {
            return BooleanLiteral.of((boolean) value);
        } else if (value instanceof String) {
            return new StringLiteral((String) value);
        } else {
            throw new RuntimeException();
        }
    }

    public abstract Object getValue();

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
    public int compareTo(Literal other) {
        if (isNullLiteral() && other.isNullLiteral()) {
            return 0;
        } else if (isNullLiteral() || other.isNullLiteral()) {
            return isNullLiteral() ? -1 : 1;
        }

        DataType oType = other.getDataType();
        DataType type = getDataType();
        if (!type.equals(oType)) {
            throw new RuntimeException("data type not equal!");
        } else if (type.isBooleanType()) {
            return Boolean.compare((boolean) getValue(), (boolean) other.getValue());
        } else if (type.isTinyIntType()) {
            return Byte.compare((byte) getValue(), (byte) other.getValue());
        } else if (type.isSmallIntType()) {
            return Short.compare((short) getValue(), (short) other.getValue());
        } else if (type.isIntType()) {
            return Integer.compare((int) getValue(), (int) other.getValue());
        } else if (type.isBigIntType()) {
            return Long.compare((long) getValue(), (long) other.getValue());
        } else if (type.isLargeIntType()) {
            return ((BigInteger) getValue()).compareTo((BigInteger) other.getValue());
        } else if (type.isFloatType()) {
            return Float.compare((float) getValue(), (float) other.getValue());
        } else if (type.isDoubleType()) {
            return Double.compare((double) getValue(), (double) other.getValue());
        } else if (type.isDecimalType()) {
            return Long.compare((Long) getValue(), (Long) other.getValue());
        } else if (type.isDateType()) {
            // todo process date
        } else if (type.isDecimalType()) {
            return ((BigDecimal) getValue()).compareTo((BigDecimal) other.getValue());
        } else if (type instanceof StringType) {
            return StringUtils.compare((String) getValue(), (String) other.getValue());
        }
        return -1;
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
        } else if (targetType.isIntType()) {
            return Literal.of(Double.valueOf(desc).intValue());
        } else if (targetType.isBigIntType()) {
            return Literal.of(Double.valueOf(desc).longValue());
        } else if (targetType.isLargeIntType()) {
            return Literal.of(new BigInteger(desc));
        } else if (targetType.isFloatType()) {
            return Literal.of(Float.parseFloat(desc));
        } else if (targetType.isDoubleType()) {
            return Literal.of(Double.parseDouble(desc));
        } else if (targetType.isCharType()) {
            return new CharLiteral(desc, ((CharType) targetType).getLen());
        } else if (targetType.isVarcharType()) {
            return new VarcharLiteral(desc, desc.length());
        } else if (targetType.isStringType()) {
            return Literal.of(desc);
        } else if (targetType.isDate()) {
            return new DateLiteral(desc);
        } else if (targetType.isDateTime()) {
            return new DateTimeLiteral(desc);
        } else if (targetType.isDecimalType()) {
            return new DecimalLiteral(BigDecimal.valueOf(Double.parseDouble(desc)));
        }
        throw new AnalysisException("no support cast!");
    }

    public boolean isCharacterLiteral() {
        return this instanceof StringLiteral || this instanceof CharLiteral || this instanceof VarcharLiteral;
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

}
