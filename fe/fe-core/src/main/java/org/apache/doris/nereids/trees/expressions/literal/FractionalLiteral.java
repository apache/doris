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
import java.math.MathContext;
import java.math.RoundingMode;

/**
 * float/double/decimal
 */
public abstract class FractionalLiteral extends NumericLiteral {
    /**
     * Constructor for FractionalLiteral.
     *
     * @param dataType logical data type in Nereids
     */
    public FractionalLiteral(DataType dataType) {
        super(dataType);
    }

    @Override
    protected Expression uncheckedCastTo(DataType targetType) throws AnalysisException {
        if (this.dataType.equals(targetType)) {
            return this;
        }
        if (targetType.isIntegralType()) {
            Object value = getValue();
            // finite == true means the value is neither NaN nor infinite.
            boolean isFinite = value instanceof Float && Float.isFinite((Float) value)
                    || value instanceof Double && Double.isFinite((Double) value)
                    || value instanceof BigDecimal;
            if (!isFinite) {
                throw new CastException(String.format("%s can't cast to %s in strict mode.", getValue(), targetType));
            }
            BigDecimal decimal = new BigDecimal(value.toString());
            if (numericOverflow(decimal, targetType)) {
                throw new CastException(String.format("%s can't cast to %s in strict mode.", getValue(), targetType));
            }
            BigDecimal intValue = decimal.setScale(0, RoundingMode.DOWN);
            if (targetType.isTinyIntType()) {
                return new TinyIntLiteral((byte) intValue.intValue());
            } else if (targetType.isSmallIntType()) {
                return new SmallIntLiteral((short) intValue.intValue());
            } else if (targetType.isIntegerType()) {
                return new IntegerLiteral(intValue.intValue());
            } else if (targetType.isBigIntType()) {
                return new BigIntLiteral(intValue.longValue());
            } else if (targetType.isLargeIntType()) {
                return new LargeIntLiteral(intValue.toBigInteger());
            }
        } else if (targetType.isDateLikeType()) {
            BigDecimal decimal = new BigDecimal(getValue().toString());
            long longValue = integralValueToLong(decimal.toBigInteger());
            if (!validCastToDate(longValue)) {
                throw new CastException(String.format("%s can't cast to %s in strict mode.", getValue(), targetType));
            }
            String s = getDateTimeString(longValue);
            if (decimal.stripTrailingZeros().scale() > 0) {
                s = String.format("%s.%s", s, decimal.toString().split("\\.")[1]);
            }
            return getDateLikeLiteral(s, targetType);
        } else if (targetType.isFloatType()) {
            return new FloatLiteral(((BigDecimal) getValue()).floatValue());
        } else if (targetType.isDoubleType()) {
            return new DoubleLiteral(((BigDecimal) getValue()).doubleValue());
        } else if (targetType.isDecimalV2Type() || targetType.isDecimalV3Type()) {
            return getDecimalLiteral((BigDecimal) getValue(), targetType);
        } else if (targetType.isBooleanType()) {
            Object value = getValue();
            if (value instanceof Float || value instanceof Double) {
                if (((Number) value).floatValue() == 0) {
                    return BooleanLiteral.FALSE;
                } else {
                    return BooleanLiteral.TRUE;
                }
            }
            if (value instanceof BigDecimal) {
                if (((BigDecimal) value).compareTo(BigDecimal.ZERO) == 0) {
                    return BooleanLiteral.FALSE;
                } else {
                    return BooleanLiteral.TRUE;
                }
            }
        }
        return super.uncheckedCastTo(targetType);
    }

    @Override
    public String getStringValue() {
        Object object = getValue();
        if (object instanceof BigDecimal) {
            return super.getStringValue();
        }
        double num = object instanceof Double ? (Double) object : new Double((Float) object);
        if (Double.isNaN(num)) {
            return "NaN";
        }
        if (Double.isInfinite(num)) {
            return num > 0 ? "Infinity" : "-Infinity";
        }
        if (Double.compare(num, 0.0) == 0) {
            return "0";
        }
        if (Double.compare(num, -0.0) == 0) {
            return "-0";
        }
        int precision = this instanceof DoubleLiteral ? 17 : 9;
        int expUpper = this instanceof DoubleLiteral ? 17 : 9;
        String decimalFormat = this instanceof DoubleLiteral ? "%.17f" : "%.9f";
        String sciFormat = this instanceof DoubleLiteral ? "%.16E" : "%.5E";
        MathContext mc = new MathContext(precision, RoundingMode.HALF_UP);
        BigDecimal bd = new BigDecimal(String.valueOf(num)).round(mc);
        double value = bd.doubleValue();
        int exponent = (int) Math.floor(Math.log10(Math.abs(value)));
        if (exponent < expUpper && exponent >= -4) {
            return String.format(decimalFormat, bd).replaceAll("0+$", "").replaceAll("\\.$", "");
        } else {
            return String.format(sciFormat, bd).replaceAll("(\\.\\d*?[1-9])0*E", "$1E")
                    .replaceAll("\\.0*E", "E").replaceAll("E", "e");
        }
    }
}
