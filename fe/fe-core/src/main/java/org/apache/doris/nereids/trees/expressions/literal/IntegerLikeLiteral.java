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

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.exceptions.CastException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.types.DataType;

import java.math.BigDecimal;
import java.math.BigInteger;

/** IntegralLiteral */
public abstract class IntegerLikeLiteral extends NumericLiteral {
    /**
     * Constructor for Literal.
     *
     * @param dataType logical data type in Nereids
     */
    public IntegerLikeLiteral(DataType dataType) {
        super(dataType);
    }

    public int getIntValue() {
        return getNumber().intValue();
    }

    public long getLongValue() {
        return getNumber().longValue();
    }

    @Override
    public BigDecimal getBigDecimalValue() {
        return new BigDecimal(getLongValue());
    }

    public abstract Number getNumber();

    @Override
    protected Expression uncheckedCastTo(DataType targetType) throws AnalysisException {
        if (this.dataType.equals(targetType)) {
            return this;
        }
        if (targetType.isDateLikeType()) {
            long value = integralValueToLong(getValue());
            if (!validCastToDate(value)) {
                throw new CastException(String.format("%s can't cast to %s in strict mode.", getValue(), targetType));
            }
            String s = getDateTimeString(value);
            return getDateLikeLiteral(s, targetType);
        } else if (targetType.isFloatType()) {
            Object value = getValue();
            return Literal.of(((Number) value).floatValue());
        } else if (targetType.isDoubleType()) {
            Object value = getValue();
            return Literal.of(((Number) value).doubleValue());
        } else if (targetType.isDecimalV2Type() || targetType.isDecimalV3Type()) {
            BigDecimal bigDecimal = getValue() instanceof BigInteger
                    ? new BigDecimal((BigInteger) getValue())
                    : new BigDecimal(((Number) getValue()).longValue());
            return getDecimalLiteral(bigDecimal, targetType);
        } else if (targetType.isBooleanType()) {
            Object value = getValue();
            if (value instanceof BigInteger) {
                if (BigInteger.ZERO.equals(value)) {
                    return BooleanLiteral.FALSE;
                } else {
                    return BooleanLiteral.TRUE;
                }
            }
            long longValue = ((Number) value).longValue();
            if (longValue == 0) {
                return BooleanLiteral.FALSE;
            } else {
                return BooleanLiteral.TRUE;
            }
        } else if (targetType.isIntegralType()) {
            // Overflow has been checked in Literal.checkedCastTo. Don't need to worry about overflow here.
            long value = getLongValue();
            if (targetType.isTinyIntType()) {
                return Literal.of((byte) value);
            } else if (targetType.isSmallIntType()) {
                return Literal.of((short) value);
            } else if (targetType.isIntegerType()) {
                return Literal.of((int) value);
            } else if (targetType.isBigIntType()) {
                return Literal.of(value);
            } else if (targetType.isLargeIntType()) {
                return Literal.of(BigInteger.valueOf(value));
            }
        }
        return super.uncheckedCastTo(targetType);
    }
}
