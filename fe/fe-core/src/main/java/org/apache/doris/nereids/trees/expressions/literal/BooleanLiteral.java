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
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.exceptions.CastException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DecimalV2Type;
import org.apache.doris.nereids.types.DecimalV3Type;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * Represents Boolean literal
 */
public class BooleanLiteral extends Literal implements ComparableLiteral {

    public static final BooleanLiteral TRUE = new BooleanLiteral(true);
    public static final BooleanLiteral FALSE = new BooleanLiteral(false);

    private final boolean value;

    private BooleanLiteral(boolean value) {
        super(BooleanType.INSTANCE);
        this.value = value;
    }

    public static BooleanLiteral of(boolean value) {
        if (value) {
            return TRUE;
        } else {
            return FALSE;
        }
    }

    public static BooleanLiteral of(Boolean value) {
        return of(value.booleanValue());
    }

    @Override
    public Boolean getValue() {
        return value;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitBooleanLiteral(this, context);
    }

    @Override
    public String toString() {
        return Boolean.toString(value).toUpperCase();
    }

    @Override
    public LiteralExpr toLegacyLiteral() {
        return new BoolLiteral(value);
    }

    @Override
    public int compareTo(ComparableLiteral other) {
        if (other instanceof BooleanLiteral) {
            return Boolean.compare(value, ((BooleanLiteral) other).value);
        }
        if (other instanceof NullLiteral) {
            return 1;
        }
        if (other instanceof MaxLiteral) {
            return -1;
        }
        throw new RuntimeException("Cannot compare two values with different data types: "
                + this + " (" + dataType + ") vs " + other + " (" + ((Literal) other).dataType + ")");
    }

    @Override
    public double getDouble() {
        if (value) {
            return 1.0;
        } else {
            return 0;
        }
    }

    @Override
    protected Expression uncheckedCastTo(DataType targetType) throws AnalysisException {
        if (this.dataType.equals(targetType)) {
            return this;
        }
        if (targetType.isFloatType()) {
            return Literal.of((float) (value ? 1 : 0));
        } else if (targetType.isDoubleType()) {
            return Literal.of((double) (value ? 1 : 0));
        } else if (targetType.isDecimalV2Type()) {
            int precision = ((DecimalV2Type) targetType).getPrecision();
            int scale = ((DecimalV2Type) targetType).getScale();
            if (precision - scale < 1) {
                throw new CastException("Precision should be larger than scale.");
            }
            return new DecimalLiteral((DecimalV2Type) targetType, new BigDecimal(value ? 1 : 0));
        } else if (targetType.isDecimalV3Type()) {
            int precision = ((DecimalV3Type) targetType).getPrecision();
            int scale = ((DecimalV3Type) targetType).getScale();
            if (precision - scale < 1) {
                throw new CastException("Precision should be larger than scale.");
            }
            return new DecimalV3Literal((DecimalV3Type) targetType, new BigDecimal(value ? 1 : 0));
        } else if (targetType.isNumericType()) {
            int value;
            if (this.equals(BooleanLiteral.TRUE)) {
                value = 1;
            } else {
                value = 0;
            }
            if (targetType.isTinyIntType()) {
                return Literal.of((byte) value);
            } else if (targetType.isSmallIntType()) {
                return Literal.of((short) value);
            } else if (targetType.isIntegerType()) {
                return Literal.of(value);
            } else if (targetType.isBigIntType()) {
                return Literal.of((long) value);
            } else if (targetType.isLargeIntType()) {
                return Literal.of(BigInteger.valueOf(value));
            }
        }
        return super.uncheckedCastTo(targetType);
    }
}
