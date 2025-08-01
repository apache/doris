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
import org.apache.doris.nereids.types.DateTimeType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DateType;
import org.apache.doris.nereids.types.DateV2Type;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * numeric literal
 */
public abstract class NumericLiteral extends Literal implements ComparableLiteral {
    /**
     * Constructor for NumericLiteral.
     *
     * @param dataType logical data type in Nereids
     */
    public NumericLiteral(DataType dataType) {
        super(dataType);
    }

    @Override
    public int compareTo(ComparableLiteral other) {
        if (other instanceof NumericLiteral) {
            if (this instanceof IntegerLikeLiteral && other instanceof IntegerLikeLiteral) {
                IntegerLikeLiteral thisInteger = (IntegerLikeLiteral) this;
                IntegerLikeLiteral otherInteger = (IntegerLikeLiteral) other;
                if (this instanceof LargeIntLiteral || other instanceof LargeIntLiteral) {
                    BigInteger leftValue = this instanceof LargeIntLiteral ? ((LargeIntLiteral) this).getValue()
                            : new BigInteger(String.valueOf(thisInteger.getLongValue()));
                    BigInteger rightValue = other instanceof LargeIntLiteral ? ((LargeIntLiteral) other).getValue()
                            : new BigInteger(String.valueOf(otherInteger.getLongValue()));
                    return leftValue.compareTo(rightValue);
                } else {
                    return Long.compare(((IntegerLikeLiteral) this).getLongValue(),
                            ((IntegerLikeLiteral) other).getLongValue());
                }
            }
            if (this instanceof DecimalLiteral || this instanceof DecimalV3Literal
                    || other instanceof DecimalLiteral || other instanceof DecimalV3Literal) {
                return this.getBigDecimalValue().compareTo(((NumericLiteral) other).getBigDecimalValue());
            }
            return Double.compare(this.getDouble(), ((Literal) other).getDouble());
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

    public abstract BigDecimal getBigDecimalValue();

    protected long integralValueToLong(Object value) {
        if (value instanceof BigInteger) {
            BigInteger bigInteger = (BigInteger) value;
            if (bigInteger.signum() <= 0 || bigInteger.bitLength() > 63) {
                return -1;
            }
            return bigInteger.longValue();
        }
        return ((Number) value).longValue();
    }

    protected boolean validCastToDate(long value) {
        if (value <= 0) {
            return false;
        }
        int digits = (int) (Math.floor(Math.log10(value)) + 1);
        return digits == 3 || digits == 4 || digits == 5 || digits == 6 || digits == 8 || digits == 14;
    }

    /**
     * 3-digit number (abc) => Year 2000, month 0a, day bc
     * 4-digit number (abcd) => Year 2000, month ab, day cd
     * 5-digit number (abcde) => Year 200a, month bc, day de
     * 6-digit number (abcdef, where ab ≥ 70) => Year 19ab, month cd, day ef
     * 6-digit number (abcdef, where ab < 70) => Year 20ab, month cd, day ef
     * 8-digit number (abcdefgh) => Year abcd, month ef, day gh
     * 14-digit number (abcdefghijklmn) => Year abcd, month ef, day gh, hour ij, minute kl, second mn
     */
    protected String getDateTimeString(long value) {
        int year = 0;
        int month = 0;
        int day = 0;
        int hour = 0;
        int minute = 0;
        int second = 0;
        switch ((int) (Math.floor(Math.log10(value)) + 1)) {
            case 3:
            case 4: {
                year = 2000;
                month = (int) (value / 100);
                day = (int) (value % 100);
                break;
            }
            case 5: {
                year = (int) (2000 + value / 10000);
                month = (int) ((value % 10000) / 100);
                day = (int) (value % 100);
                break;
            }
            case 6: {
                year = (int) (value / 10000 >= 70 ? 1900 + value / 10000 : 2000 + value / 10000);
                month = (int) ((value % 10000) / 100);
                day = (int) (value % 100);
                break;
            }
            case 8: {
                year = (int) (value / 10000);
                month = (int) ((value % 10000) / 100);
                day = (int) (value % 100);
                break;
            }
            case 14:
                year = (int) (value / 10000000000L);
                month = (int) ((value % 10000000000L) / 100000000);
                day = (int) ((value % 100000000) / 1000000);
                hour = (int) ((value % 1000000) / 10000);
                minute = (int) ((value % 10000) / 100);
                second = (int) (value % 100);
                break;
            default:
                throw new CastException("Unexpected value: " + (int) (Math.floor(Math.log10(value)) + 1));
        }
        return String.format("%d-%d-%d %d:%d:%d", year, month, day, hour, minute, second);
    }

    protected Expression getDateLikeLiteral(String s, DataType targetType) {
        DateTimeV2Literal l;
        try {
            l = new DateTimeV2Literal(DateTimeV2Type.MAX, s);
        } catch (AnalysisException e) {
            throw new CastException(e.getMessage(), e);
        }
        if (targetType instanceof DateType) {
            return new DateLiteral(l.getYear(), l.getMonth(), l.getDay());
        }
        if (targetType instanceof DateV2Type) {
            return new DateV2Literal(l.getYear(), l.getMonth(), l.getDay());
        }
        if (targetType instanceof DateTimeType) {
            return new DateTimeLiteral(s);
        }
        if (targetType instanceof DateTimeV2Type) {
            return new DateTimeV2Literal((DateTimeV2Type) targetType, s);
        }
        throw new AnalysisException(String.format("%s is not a DateLikeType.", targetType));
    }
}
