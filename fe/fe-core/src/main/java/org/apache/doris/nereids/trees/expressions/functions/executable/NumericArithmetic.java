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

package org.apache.doris.nereids.trees.expressions.functions.executable;

import org.apache.doris.nereids.trees.expressions.ExecFunction;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DecimalLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DecimalV3Literal;
import org.apache.doris.nereids.trees.expressions.literal.DoubleLiteral;
import org.apache.doris.nereids.trees.expressions.literal.FloatLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.LargeIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.SmallIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.DoubleType;

import org.apache.commons.math3.util.ArithmeticUtils;
import org.apache.commons.math3.util.FastMath;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;

/**
 * executable functions:
 * add, subtract, multiply, divide
 */
public class NumericArithmetic {
    /**
     * other scalar function
     */
    @ExecFunction(name = "abs")
    public static Expression abs(TinyIntLiteral literal) {
        return new SmallIntLiteral((short) Math.abs(literal.getValue()));
    }

    @ExecFunction(name = "abs")
    public static Expression abs(SmallIntLiteral literal) {
        return new IntegerLiteral(Math.abs(literal.getValue()));
    }

    @ExecFunction(name = "abs")
    public static Expression abs(IntegerLiteral literal) {
        return new BigIntLiteral(Math.abs((long) literal.getValue()));
    }

    @ExecFunction(name = "abs")
    public static Expression abs(BigIntLiteral literal) {
        return new LargeIntLiteral(BigInteger.valueOf(literal.getValue()).abs());
    }

    @ExecFunction(name = "abs")
    public static Expression abs(LargeIntLiteral literal) {
        return new LargeIntLiteral(literal.getValue().abs());
    }

    @ExecFunction(name = "abs")
    public static Expression abs(FloatLiteral literal) {
        return new FloatLiteral(Math.abs(literal.getValue()));
    }

    @ExecFunction(name = "abs")
    public static Expression abs(DoubleLiteral literal) {
        return new DoubleLiteral(Math.abs(literal.getValue()));
    }

    @ExecFunction(name = "abs")
    public static Expression abs(DecimalLiteral literal) {
        return new DecimalLiteral(literal.getValue().abs());
    }

    @ExecFunction(name = "abs")
    public static Expression abs(DecimalV3Literal literal) {
        return new DecimalV3Literal(literal.getValue().abs());
    }

    /**
     * Executable arithmetic functions add
     */
    @ExecFunction(name = "add")
    public static Expression addTinyIntTinyInt(TinyIntLiteral first, TinyIntLiteral second) {
        byte result = (byte) Math.addExact(first.getValue(), second.getValue());
        return new TinyIntLiteral(result);
    }

    @ExecFunction(name = "add")
    public static Expression addSmallIntSmallInt(SmallIntLiteral first, SmallIntLiteral second) {
        short result = (short) Math.addExact(first.getValue(), second.getValue());
        return new SmallIntLiteral(result);
    }

    @ExecFunction(name = "add")
    public static Expression addIntInt(IntegerLiteral first, IntegerLiteral second) {
        int result = Math.addExact(first.getValue(), second.getValue());
        return new IntegerLiteral(result);
    }

    @ExecFunction(name = "add")
    public static Expression addBigIntBigInt(BigIntLiteral first, BigIntLiteral second) {
        long result = Math.addExact(first.getValue(), second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "add")
    public static Expression addLargeIntLargeInt(LargeIntLiteral first, LargeIntLiteral second) {
        BigInteger result = first.getValue().add(new BigInteger(second.getValue().toString()));
        return new LargeIntLiteral(result);
    }

    @ExecFunction(name = "add")
    public static Expression addDoubleDouble(DoubleLiteral first, DoubleLiteral second) {
        double result = first.getValue() + second.getValue();
        return checkOutputBoundary(new DoubleLiteral(result));
    }

    @ExecFunction(name = "add")
    public static Expression addDecimalDecimal(DecimalLiteral first, DecimalLiteral second) {
        BigDecimal result = first.getValue().add(second.getValue());
        return new DecimalLiteral(result);
    }

    @ExecFunction(name = "add")
    public static Expression addDecimalV3DecimalV3(DecimalV3Literal first, DecimalV3Literal second) {
        BigDecimal result = first.getValue().add(second.getValue());
        return new DecimalV3Literal((DecimalV3Type) first.getDataType(), result);
    }

    /**
     * Executable arithmetic functions subtract
     */
    @ExecFunction(name = "subtract")
    public static Expression subtractTinyIntTinyInt(TinyIntLiteral first, TinyIntLiteral second) {
        byte result = (byte) Math.subtractExact(first.getValue(), second.getValue());
        return new TinyIntLiteral(result);
    }

    @ExecFunction(name = "subtract")
    public static Expression subtractSmallIntSmallInt(SmallIntLiteral first, SmallIntLiteral second) {
        short result = (short) Math.subtractExact(first.getValue(), second.getValue());
        return new SmallIntLiteral(result);
    }

    @ExecFunction(name = "subtract")
    public static Expression subtractIntInt(IntegerLiteral first, IntegerLiteral second) {
        int result = Math.subtractExact(first.getValue(), second.getValue());
        return new IntegerLiteral(result);
    }

    @ExecFunction(name = "subtract")
    public static Expression subtractBigIntBigInt(BigIntLiteral first, BigIntLiteral second) {
        long result = Math.subtractExact(first.getValue(), second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "subtract")
    public static Expression subtractLargeIntLargeInt(LargeIntLiteral first, LargeIntLiteral second) {
        BigInteger result = first.getValue().subtract(new BigInteger(second.getValue().toString()));
        return new LargeIntLiteral(result);
    }

    @ExecFunction(name = "subtract")
    public static Expression subtractDoubleDouble(DoubleLiteral first, DoubleLiteral second) {
        double result = first.getValue() - second.getValue();
        return checkOutputBoundary(new DoubleLiteral(result));
    }

    @ExecFunction(name = "subtract")
    public static Expression subtractDecimalDecimal(DecimalLiteral first, DecimalLiteral second) {
        BigDecimal result = first.getValue().subtract(second.getValue());
        return new DecimalLiteral(result);
    }

    @ExecFunction(name = "subtract")
    public static Expression subtractDecimalV3DecimalV3(DecimalV3Literal first, DecimalV3Literal second) {
        BigDecimal result = first.getValue().subtract(second.getValue());
        return new DecimalV3Literal((DecimalV3Type) first.getDataType(), result);
    }

    /**
     * Executable arithmetic functions multiply
     */
    @ExecFunction(name = "multiply")
    public static Expression multiplyTinyIntTinyInt(TinyIntLiteral first, TinyIntLiteral second) {
        byte result = (byte) Math.multiplyExact(first.getValue(), second.getValue());
        return new TinyIntLiteral(result);
    }

    @ExecFunction(name = "multiply")
    public static Expression multiplySmallIntSmallInt(SmallIntLiteral first, SmallIntLiteral second) {
        short result = (short) Math.multiplyExact(first.getValue(), second.getValue());
        return new SmallIntLiteral(result);
    }

    @ExecFunction(name = "multiply")
    public static Expression multiplyIntInt(IntegerLiteral first, IntegerLiteral second) {
        int result = Math.multiplyExact(first.getValue(), second.getValue());
        return new IntegerLiteral(result);
    }

    @ExecFunction(name = "multiply")
    public static Expression multiplyBigIntBigInt(BigIntLiteral first, BigIntLiteral second) {
        long result = Math.multiplyExact(first.getValue(), second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "multiply")
    public static Expression multiplyLargeIntLargeInt(LargeIntLiteral first, LargeIntLiteral second) {
        BigInteger result = first.getValue().multiply(new BigInteger(second.getValue().toString()));
        return new LargeIntLiteral(result);
    }

    @ExecFunction(name = "multiply")
    public static Expression multiplyDoubleDouble(DoubleLiteral first, DoubleLiteral second) {
        double result = first.getValue() * second.getValue();
        return checkOutputBoundary(new DoubleLiteral(result));
    }

    @ExecFunction(name = "multiply")
    public static Expression multiplyDecimalDecimal(DecimalLiteral first, DecimalLiteral second) {
        BigDecimal result = first.getValue().multiply(second.getValue());
        return new DecimalLiteral(result);
    }

    /**
     * decimalV3 multiply in FE
     */
    @ExecFunction(name = "multiply")
    public static Expression multiplyDecimalV3DecimalV3(DecimalV3Literal first, DecimalV3Literal second) {
        BigDecimal result = first.getValue().multiply(second.getValue());
        DecimalV3Type t1 = (DecimalV3Type) first.getDataType();
        DecimalV3Type t2 = (DecimalV3Type) second.getDataType();
        int precision = t1.getPrecision() + t2.getPrecision();
        int scale = t1.getScale() + t2.getScale();
        return new DecimalV3Literal(DecimalV3Type.createDecimalV3TypeLooseCheck(precision, scale), result);
    }

    /**
     * Executable arithmetic functions divide
     */
    @ExecFunction(name = "divide")
    public static Expression divideDouble(DoubleLiteral first, DoubleLiteral second) {
        if (second.getValue() == 0.0) {
            return new NullLiteral(first.getDataType());
        }
        double result = first.getValue() / second.getValue();
        return checkOutputBoundary(new DoubleLiteral(result));
    }

    /**
     * Executable arithmetic functions divide
     */
    @ExecFunction(name = "divide")
    public static Expression divideDecimal(DecimalLiteral first, DecimalLiteral second) {
        if (first.getValue().compareTo(BigDecimal.ZERO) == 0) {
            return new NullLiteral(first.getDataType());
        }
        BigDecimal result = first.getValue().divide(second.getValue());
        return new DecimalLiteral(result);
    }

    /**
     * decimalv3 divide in FE
     */
    @ExecFunction(name = "divide")
    public static Expression divideDecimalV3(DecimalV3Literal first, DecimalV3Literal second) {
        DecimalV3Type t1 = (DecimalV3Type) first.getDataType();
        DecimalV3Type t2 = (DecimalV3Type) second.getDataType();
        if (second.getValue().compareTo(BigDecimal.ZERO) == 0) {
            return new NullLiteral(DecimalV3Type.createDecimalV3TypeLooseCheck(
                    t1.getPrecision(), t1.getScale() - t2.getScale()));
        }
        BigDecimal result = first.getValue().divide(second.getValue());
        return new DecimalV3Literal(DecimalV3Type.createDecimalV3TypeLooseCheck(
                t1.getPrecision(), t1.getScale() - t2.getScale()), result);
    }

    /**
     * coalesce
     */
    @ExecFunction(name = "coalesce")
    public static Expression coalesce(Literal first, Literal... second) {
        if (!(first instanceof NullLiteral)) {
            return first;
        }
        for (Literal secondLiteral : second) {
            if (!(secondLiteral instanceof NullLiteral)) {
                return secondLiteral;
            }
        }
        return first;
    }

    /**
     * Method to check boundary with options for inclusive or exclusive boundaries
     */
    public static Boolean inputOutOfBound(Literal input, double lowerBound, double upperBound,
            boolean isLowerInclusive, boolean isUpperInclusive) {
        if (input instanceof DoubleLiteral) {
            double inputValue = ((DoubleLiteral) input).getValue();
            boolean lowerCheck = isLowerInclusive ? (inputValue >= lowerBound) : (inputValue > lowerBound);
            // Check upper bound
            boolean upperCheck = isUpperInclusive ? (inputValue <= upperBound) : (inputValue < upperBound);
            // Return true if both checks are satisfied
            if (!lowerCheck || !upperCheck) {
                return true;
            }
        }
        return false;
    }

    private static Expression checkOutputBoundary(Literal input) {
        if (input instanceof DoubleLiteral) {
            if (((DoubleLiteral) input).getValue().isNaN() || ((DoubleLiteral) input).getValue().isInfinite()) {
                return new NullLiteral(DoubleType.INSTANCE);
            }
        }
        return input;
    }

    private static Expression castDecimalV3Literal(DecimalV3Literal literal, int precision) {
        return new DecimalV3Literal(DecimalV3Type.createDecimalV3Type(precision, literal.getValue().scale()),
                literal.getValue());
    }

    /**
     * round
     */
    @ExecFunction(name = "round")
    public static Expression round(DecimalV3Literal first) {
        return castDecimalV3Literal(first.round(0), ((DecimalV3Type) first.getDataType()).getPrecision());
    }

    /**
     * round
     */
    @ExecFunction(name = "round")
    public static Expression round(DecimalV3Literal first, IntegerLiteral second) {
        return castDecimalV3Literal(first.round(second.getValue()),
                ((DecimalV3Type) first.getDataType()).getPrecision());
    }

    /**
     * round
     */
    @ExecFunction(name = "round")
    public static Expression round(DoubleLiteral first) {
        DecimalV3Literal middleResult = new DecimalV3Literal(new BigDecimal(Double.toString(first.getValue())));
        return new DoubleLiteral(middleResult.round(0).getDouble());
    }

    /**
     * round
     */
    @ExecFunction(name = "round")
    public static Expression round(DoubleLiteral first, IntegerLiteral second) {
        DecimalV3Literal middleResult = new DecimalV3Literal(new BigDecimal(Double.toString(first.getValue())));
        return new DoubleLiteral(middleResult.round(second.getValue()).getDouble());
    }

    /**
     * ceil
     */
    @ExecFunction(name = "ceil")
    public static Expression ceil(DecimalV3Literal first) {
        return castDecimalV3Literal(first.roundCeiling(0), ((DecimalV3Type) first.getDataType()).getPrecision());
    }

    /**
     * ceil
     */
    @ExecFunction(name = "ceil")
    public static Expression ceil(DecimalV3Literal first, IntegerLiteral second) {
        return castDecimalV3Literal(first.roundCeiling(second.getValue()),
                ((DecimalV3Type) first.getDataType()).getPrecision());
    }

    /**
     * ceil
     */
    @ExecFunction(name = "ceil")
    public static Expression ceil(DoubleLiteral first) {
        DecimalV3Literal middleResult = new DecimalV3Literal(new BigDecimal(Double.toString(first.getValue())));
        return new DoubleLiteral(middleResult.roundCeiling(0).getDouble());
    }

    /**
     * ceil
     */
    @ExecFunction(name = "ceil")
    public static Expression ceil(DoubleLiteral first, IntegerLiteral second) {
        DecimalV3Literal middleResult = new DecimalV3Literal(new BigDecimal(Double.toString(first.getValue())));
        return new DoubleLiteral(middleResult.roundCeiling(second.getValue()).getDouble());
    }

    /**
     * floor
     */
    @ExecFunction(name = "floor")
    public static Expression floor(DecimalV3Literal first) {
        return castDecimalV3Literal(first.roundFloor(0), ((DecimalV3Type) first.getDataType()).getPrecision());
    }

    /**
     * floor
     */
    @ExecFunction(name = "floor")
    public static Expression floor(DecimalV3Literal first, IntegerLiteral second) {
        return castDecimalV3Literal(first.roundFloor(second.getValue()),
                ((DecimalV3Type) first.getDataType()).getPrecision());
    }

    /**
     * floor
     */
    @ExecFunction(name = "floor")
    public static Expression floor(DoubleLiteral first) {
        DecimalV3Literal middleResult = new DecimalV3Literal(new BigDecimal(Double.toString(first.getValue())));
        return new DoubleLiteral(middleResult.roundFloor(0).getDouble());
    }

    /**
     * floor
     */
    @ExecFunction(name = "floor")
    public static Expression floor(DoubleLiteral first, IntegerLiteral second) {
        DecimalV3Literal middleResult = new DecimalV3Literal(new BigDecimal(Double.toString(first.getValue())));
        return new DoubleLiteral(middleResult.roundFloor(second.getValue()).getDouble());
    }

    /**
     * exp
     */
    @ExecFunction(name = "exp")
    public static Expression exp(DoubleLiteral first) {
        return checkOutputBoundary(new DoubleLiteral(Math.exp(first.getValue())));
    }

    /**
     * ln
     */
    @ExecFunction(name = "ln")
    public static Expression ln(DoubleLiteral first) {
        if (inputOutOfBound(first, 0.0d, Double.MAX_VALUE, false, true)) {
            return new NullLiteral(DoubleType.INSTANCE);
        }
        return checkOutputBoundary(new DoubleLiteral(Math.log(first.getValue())));
    }

    /**
     * log
     */
    @ExecFunction(name = "log")
    public static Expression log(DoubleLiteral first, DoubleLiteral second) {
        if (inputOutOfBound(first, 0.0d, Double.MAX_VALUE, false, true)
                || first.getValue().equals(1.0d)) {
            return new NullLiteral(DoubleType.INSTANCE);
        }
        return checkOutputBoundary(new DoubleLiteral(Math.log(second.getValue()) / Math.log(first.getValue())));
    }

    /**
     * log2
     */
    @ExecFunction(name = "log2")
    public static Expression log2(DoubleLiteral first) {
        if (inputOutOfBound(first, 0.0d, Double.MAX_VALUE, false, true)) {
            return new NullLiteral(DoubleType.INSTANCE);
        }
        return checkOutputBoundary(new DoubleLiteral(Math.log(first.getValue()) / Math.log(2.0)));
    }

    /**
     * log10
     */
    @ExecFunction(name = "log10")
    public static Expression log10(DoubleLiteral first) {
        if (inputOutOfBound(first, 0.0d, Double.MAX_VALUE, false, true)) {
            return new NullLiteral(DoubleType.INSTANCE);
        }
        return checkOutputBoundary(new DoubleLiteral(Math.log10(first.getValue())));
    }

    /**
     * sqrt
     */
    @ExecFunction(name = "sqrt")
    public static Expression sqrt(DoubleLiteral first) {
        if (inputOutOfBound(first, 0.0d, Double.MAX_VALUE, true, true)) {
            return new NullLiteral(DoubleType.INSTANCE);
        }
        return checkOutputBoundary(new DoubleLiteral(Math.sqrt(first.getValue())));
    }

    /**
     * power
     */
    @ExecFunction(name = "power")
    public static Expression power(DoubleLiteral first, DoubleLiteral second) {
        if (inputOutOfBound(second, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, false, false)
                || (first.getValue() < 0 && second.getValue() % 1 != 0)) {
            return new NullLiteral(DoubleType.INSTANCE);
        }
        return checkOutputBoundary(new DoubleLiteral(Math.pow(first.getValue(), second.getValue())));
    }

    /**
     * sin
     */
    @ExecFunction(name = "sin")
    public static Expression sin(DoubleLiteral first) {
        if (inputOutOfBound(first, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, false, false)) {
            return new NullLiteral(DoubleType.INSTANCE);
        }
        return checkOutputBoundary(new DoubleLiteral(Math.sin(first.getValue())));
    }

    /**
     * sinh
     */
    @ExecFunction(name = "sinh")
    public static Expression sinh(DoubleLiteral first) {
        if (inputOutOfBound(first, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, false, false)) {
            return new NullLiteral(DoubleType.INSTANCE);
        }
        return checkOutputBoundary(new DoubleLiteral(Math.sinh(first.getValue())));
    }

    /**
     * cos
     */
    @ExecFunction(name = "cos")
    public static Expression cos(DoubleLiteral first) {
        if (inputOutOfBound(first, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, false, false)) {
            return new NullLiteral(DoubleType.INSTANCE);
        }
        return checkOutputBoundary(new DoubleLiteral(Math.cos(first.getValue())));
    }

    /**
     * tan
     */
    @ExecFunction(name = "tan")
    public static Expression tan(DoubleLiteral first) {
        if (inputOutOfBound(first, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, false, false)) {
            return new NullLiteral(DoubleType.INSTANCE);
        }
        return checkOutputBoundary(new DoubleLiteral(Math.tan(first.getValue())));
    }

    /**
     * cot
     */
    @ExecFunction(name = "cot")
    public static Expression cot(DoubleLiteral first) {
        if (inputOutOfBound(first, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, false, false)) {
            return new NullLiteral(DoubleType.INSTANCE);
        }
        return checkOutputBoundary(new DoubleLiteral(1.0 / Math.tan(first.getValue())));
    }

    /**
     * cot
     */
    @ExecFunction(name = "sec")
    public static Expression sec(DoubleLiteral first) {
        if (inputOutOfBound(first, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, false, false)) {
            return new NullLiteral(DoubleType.INSTANCE);
        }
        return checkOutputBoundary(new DoubleLiteral(1.0 / Math.cos(first.getValue())));
    }

    /**
     * csc
     */
    @ExecFunction(name = "csc")
    public static Expression csc(DoubleLiteral first) {
        if (inputOutOfBound(first, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, false, false)) {
            return new NullLiteral(DoubleType.INSTANCE);
        }
        return checkOutputBoundary(new DoubleLiteral(1.0 / Math.sin(first.getValue())));
    }

    /**
     * asin
     */
    @ExecFunction(name = "asin")
    public static Expression asin(DoubleLiteral first) {
        if (inputOutOfBound(first, -1.0, 1.0, true, true)) {
            return new NullLiteral(DoubleType.INSTANCE);
        }
        return checkOutputBoundary(new DoubleLiteral(Math.asin(first.getValue())));
    }

    /**
     * acos
     */
    @ExecFunction(name = "acos")
    public static Expression acos(DoubleLiteral first) {
        if (inputOutOfBound(first, -1.0, 1.0, true, true)) {
            return new NullLiteral(DoubleType.INSTANCE);
        }
        return checkOutputBoundary(new DoubleLiteral(Math.acos(first.getValue())));
    }

    /**
     * atan
     */
    @ExecFunction(name = "atan")
    public static Expression atan(DoubleLiteral first) {
        if (inputOutOfBound(first, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, false, false)) {
            return new NullLiteral(DoubleType.INSTANCE);
        }
        return checkOutputBoundary(new DoubleLiteral(Math.atan(first.getValue())));
    }

    /**
     * asinh
     */
    @ExecFunction(name = "asinh")
    public static Expression asinh(DoubleLiteral first) {
        return checkOutputBoundary(new DoubleLiteral(FastMath.asinh(first.getValue())));
    }

    /**
     * acosh
     */
    @ExecFunction(name = "acosh")
    public static Expression acosh(DoubleLiteral first) {
        if (inputOutOfBound(first, 1.0, Double.POSITIVE_INFINITY, true, true)) {
            return new NullLiteral(DoubleType.INSTANCE);
        }
        return checkOutputBoundary(new DoubleLiteral(FastMath.acosh(first.getValue())));
    }

    /**
     * atanh
     */
    @ExecFunction(name = "atanh")
    public static Expression atanh(DoubleLiteral first) {
        if (inputOutOfBound(first, -1.0, 1.0, false, false)) {
            return new NullLiteral(DoubleType.INSTANCE);
        }
        return checkOutputBoundary(new DoubleLiteral(FastMath.atanh(first.getValue())));
    }

    /**
     * atan2
     */
    @ExecFunction(name = "atan2")
    public static Expression atan2(DoubleLiteral first, DoubleLiteral second) {
        return checkOutputBoundary(new DoubleLiteral(Math.atan2(first.getValue(), second.getValue())));
    }

    /**
     * sign
     */
    @ExecFunction(name = "sign")
    public static Expression sign(DoubleLiteral first) {
        if (first.getValue() < 0) {
            return new TinyIntLiteral((byte) -1);
        } else if (first.getValue() == 0) {
            return new TinyIntLiteral((byte) 0);
        } else {
            return new TinyIntLiteral((byte) 1);
        }
    }

    /**
     * signbit
     */
    @ExecFunction(name = "signbit")
    public static Expression signbit(DoubleLiteral first) {
        if (first.getValue() < 0) {
            return BooleanLiteral.of(true);
        } else {
            return BooleanLiteral.of(false);
        }
    }

    /**
     * even
     */
    @ExecFunction(name = "even")
    public static Expression even(DoubleLiteral first) {
        double mag = Math.abs(first.getValue());
        double evenMag = 2 * Math.ceil(mag / 2);
        double value = Math.copySign(evenMag, first.getValue());
        return checkOutputBoundary(new DoubleLiteral(value));
    }

    /**
     * gcd
     */
    @ExecFunction(name = "gcd")
    public static Expression gcd(TinyIntLiteral first, TinyIntLiteral second) {
        return new TinyIntLiteral((byte) ArithmeticUtils.gcd(first.getValue(), second.getValue()));
    }

    /**
     * gcd
     */
    @ExecFunction(name = "gcd")
    public static Expression gcd(SmallIntLiteral first, SmallIntLiteral second) {
        return new SmallIntLiteral((short) ArithmeticUtils.gcd(first.getValue(), second.getValue()));
    }

    /**
     * gcd
     */
    @ExecFunction(name = "gcd")
    public static Expression gcd(IntegerLiteral first, IntegerLiteral second) {
        return new IntegerLiteral(ArithmeticUtils.gcd(first.getValue(), second.getValue()));
    }

    /**
     * gcd
     */
    @ExecFunction(name = "gcd")
    public static Expression gcd(BigIntLiteral first, BigIntLiteral second) {
        return new BigIntLiteral(ArithmeticUtils.gcd(first.getValue(), second.getValue()));
    }

    /**
     * gcd
     */
    @ExecFunction(name = "gcd")
    public static Expression gcd(LargeIntLiteral first, LargeIntLiteral second) {
        BigInteger a = first.getValue();
        BigInteger b = second.getValue();
        return new LargeIntLiteral(a.gcd(b));
    }

    /**
     * lcm
     */
    @ExecFunction(name = "lcm")
    public static Expression lcm(TinyIntLiteral first, TinyIntLiteral second) {
        return new SmallIntLiteral((short) ArithmeticUtils.lcm(first.getValue(), second.getValue()));
    }

    /**
     * lcm
     */
    @ExecFunction(name = "lcm")
    public static Expression lcm(SmallIntLiteral first, SmallIntLiteral second) {
        return new IntegerLiteral(ArithmeticUtils.lcm(first.getValue(), second.getValue()));
    }

    /**
     * lcm
     */
    @ExecFunction(name = "lcm")
    public static Expression lcm(IntegerLiteral first, IntegerLiteral second) {
        return new BigIntLiteral(ArithmeticUtils.lcm(first.getValue(), second.getValue()));
    }

    /**
     * lcm
     */
    @ExecFunction(name = "lcm")
    public static Expression lcm(BigIntLiteral first, BigIntLiteral second) {
        BigInteger a = new BigInteger(first.getValue().toString());
        BigInteger b = new BigInteger(second.getValue().toString());
        BigInteger g = a.gcd(b);
        return abs(new LargeIntLiteral(a.multiply(b).divide(g)));
    }

    /**
     * lcm
     */
    @ExecFunction(name = "lcm")
    public static Expression lcm(LargeIntLiteral first, LargeIntLiteral second) {
        BigInteger a = first.getValue();
        BigInteger b = second.getValue();
        BigInteger g = a.gcd(b);
        return abs(new LargeIntLiteral(a.multiply(b).divide(g)));
    }

    /**
     * bit_count
     */
    @ExecFunction(name = "bit_count")
    public static Expression bitCount(TinyIntLiteral first) {
        return new TinyIntLiteral((byte) Integer.bitCount(first.getValue() & 0xFF));
    }

    /**
     * bit_count
     */
    @ExecFunction(name = "bit_count")
    public static Expression bitCount(SmallIntLiteral first) {
        return new TinyIntLiteral((byte) Integer.bitCount(first.getValue() & 0xFFFF));
    }

    /**
     * bit_count
     */
    @ExecFunction(name = "bit_count")
    public static Expression bitCount(IntegerLiteral first) {
        return new TinyIntLiteral((byte) Integer.bitCount(first.getValue()));
    }

    /**
     * bit_count
     */
    @ExecFunction(name = "bit_count")
    public static Expression bitCount(BigIntLiteral first) {
        return new TinyIntLiteral((byte) Long.bitCount(first.getValue()));
    }

    /**
     * bit_count
     */
    @ExecFunction(name = "bit_count")
    public static Expression bitCount(LargeIntLiteral first) {
        if (first.getValue().compareTo(BigInteger.ZERO) < 0) {
            return new SmallIntLiteral((short) (128 - first.getValue().bitCount()));
        } else {
            return new SmallIntLiteral((short) first.getValue().bitCount());
        }
    }

    /**
     * bit_length
     */
    @ExecFunction(name = "bit_length")
    public static Expression bitLength(VarcharLiteral first) {
        byte[] byteArray = first.getValue().getBytes(StandardCharsets.UTF_8); // Convert to bytes in UTF-8
        int byteLength = byteArray.length;
        return new IntegerLiteral(byteLength * Byte.SIZE);
    }

    /**
     * bit_length
     */
    @ExecFunction(name = "bit_length")
    public static Expression bitLength(StringLiteral first) {
        byte[] byteArray = first.getValue().getBytes(StandardCharsets.UTF_8); // Convert to bytes in UTF-8
        int byteLength = byteArray.length;
        return new IntegerLiteral(byteLength * Byte.SIZE);
    }

    /**
     * cbrt
     */
    @ExecFunction(name = "cbrt")
    public static Expression cbrt(DoubleLiteral first) {
        return checkOutputBoundary(new DoubleLiteral(Math.cbrt(first.getValue())));
    }

    /**
     * cosh
     */
    @ExecFunction(name = "cosh")
    public static Expression cosh(DoubleLiteral first) {
        return checkOutputBoundary(new DoubleLiteral(Math.cosh(first.getValue())));
    }

    /**
     * tanh
     */
    @ExecFunction(name = "tanh")
    public static Expression tanh(DoubleLiteral first) {
        return checkOutputBoundary(new DoubleLiteral(Math.tanh(first.getValue())));
    }

    /**
     * dexp
     */
    @ExecFunction(name = "dexp")
    public static Expression dexp(DoubleLiteral first) {
        double exp = Math.exp(first.getValue());
        return checkOutputBoundary(new DoubleLiteral(exp));
    }

    /**
     * dlog10
     */
    @ExecFunction(name = "dlog10")
    public static Expression dlog10(DoubleLiteral first) {
        if (inputOutOfBound(first, 0.0d, Double.MAX_VALUE, false, true)) {
            return new NullLiteral(DoubleType.INSTANCE);
        }
        return checkOutputBoundary(new DoubleLiteral(Math.log10(first.getValue())));
    }

    /**
     * dsqrt
     */
    @ExecFunction(name = "dsqrt")
    public static Expression dsqrt(DoubleLiteral first) {
        if (inputOutOfBound(first, 0.0d, Double.MAX_VALUE, false, true)) {
            return new NullLiteral(DoubleType.INSTANCE);
        }
        return checkOutputBoundary(new DoubleLiteral(Math.sqrt(first.getValue())));
    }

    /**
     * dpower
     */
    @ExecFunction(name = "dpow")
    public static Expression dpow(DoubleLiteral first, DoubleLiteral second) {
        return checkOutputBoundary(new DoubleLiteral(Math.pow(first.getValue(), second.getValue())));
    }

    /**
     * fmod
     */
    @ExecFunction(name = "fmod")
    public static Expression fmod(DoubleLiteral first, DoubleLiteral second) {
        return checkOutputBoundary(new DoubleLiteral(first.getValue() % second.getValue()));
    }

    /**
     * fmod
     */
    @ExecFunction(name = "fmod")
    public static Expression fmod(FloatLiteral first, FloatLiteral second) {
        return new FloatLiteral(first.getValue() % second.getValue());
    }

    /**
     * fpow
     */
    @ExecFunction(name = "fpow")
    public static Expression fpow(DoubleLiteral first, DoubleLiteral second) {
        return checkOutputBoundary(new DoubleLiteral(Math.pow(first.getValue(), second.getValue())));
    }

    /**
     * radians
     */
    @ExecFunction(name = "radians")
    public static Expression radians(DoubleLiteral first) {
        return checkOutputBoundary(new DoubleLiteral(Math.toRadians(first.getValue())));
    }

    /**
     * degrees
     */
    @ExecFunction(name = "degrees")
    public static Expression degrees(DoubleLiteral first) {
        return checkOutputBoundary(new DoubleLiteral(Math.toDegrees(first.getValue())));
    }

    /**
     * xor
     */
    @ExecFunction(name = "xor")
    public static Expression xor(BooleanLiteral first, BooleanLiteral second) {
        return BooleanLiteral.of(Boolean.logicalXor(first.getValue(), second.getValue()));
    }

    /**
     * pi
     */
    @ExecFunction(name = "pi")
    public static Expression pi() {
        return DoubleLiteral.of(Math.PI);
    }

    /**
     * E
     */
    @ExecFunction(name = "e")
    public static Expression mathE() {
        return DoubleLiteral.of(Math.E);
    }

    /**
     * truncate
     */
    @ExecFunction(name = "truncate")
    public static Expression truncate(DecimalV3Literal first, IntegerLiteral second) {
        if (first.getValue().compareTo(BigDecimal.ZERO) == 0) {
            return first;
        } else if (first.getValue().compareTo(BigDecimal.ZERO) < 0) {
            return castDecimalV3Literal(first.roundCeiling(second.getValue()),
                    ((DecimalV3Type) first.getDataType()).getPrecision());
        } else {
            return castDecimalV3Literal(first.roundFloor(second.getValue()),
                    ((DecimalV3Type) first.getDataType()).getPrecision());
        }
    }

    /**
     * isnan
     */
    @ExecFunction(name = "isnan")
    public static Expression isnan(DoubleLiteral first) {
        return BooleanLiteral.of(Double.isNaN(first.getValue()));
    }

    @ExecFunction(name = "isnan")
    public static Expression isnan(FloatLiteral first) {
        return BooleanLiteral.of(Float.isNaN(first.getValue()));
    }

    /**
     * isinf
     */
    @ExecFunction(name = "isinf")
    public static Expression isinf(DoubleLiteral first) {
        return BooleanLiteral.of(Double.isInfinite(first.getValue()));
    }

    @ExecFunction(name = "isinf")
    public static Expression isinf(FloatLiteral first) {
        return BooleanLiteral.of(Float.isInfinite(first.getValue()));
    }
}
