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

import org.apache.doris.nereids.exceptions.NotSupportedException;
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
        short result = (short) Math.addExact(first.getValue(), second.getValue());
        return new SmallIntLiteral(result);
    }

    @ExecFunction(name = "add")
    public static Expression addTinyIntSmallInt(TinyIntLiteral first, SmallIntLiteral second) {
        int result = Math.addExact(first.getValue(), second.getValue());
        return new IntegerLiteral(result);
    }

    @ExecFunction(name = "add")
    public static Expression addTinyIntInt(TinyIntLiteral first, IntegerLiteral second) {
        long result = Math.addExact((long) first.getValue(), (long) second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "add")
    public static Expression addTinyIntBigInt(TinyIntLiteral first, BigIntLiteral second) {
        long result = Math.addExact((long) first.getValue(), (long) second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "add")
    public static Expression addTinyIntLargeInt(TinyIntLiteral first, LargeIntLiteral second) {
        BigInteger result = second.getValue().add(new BigInteger(first.getValue().toString()));
        return new LargeIntLiteral(result);
    }

    @ExecFunction(name = "add")
    public static Expression addSmallIntTinyInt(SmallIntLiteral first, TinyIntLiteral second) {
        int result = Math.addExact(first.getValue(), second.getValue());
        return new IntegerLiteral(result);
    }

    @ExecFunction(name = "add")
    public static Expression addSmallIntSmallInt(SmallIntLiteral first, SmallIntLiteral second) {
        int result = Math.addExact(first.getValue(), second.getValue());
        return new IntegerLiteral(result);
    }

    @ExecFunction(name = "add")
    public static Expression addSmallIntInt(SmallIntLiteral first, IntegerLiteral second) {
        long result = Math.addExact((long) first.getValue(), (long) second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "add")
    public static Expression addSmallIntBigInt(SmallIntLiteral first, BigIntLiteral second) {
        long result = Math.addExact(first.getValue(), second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "add")
    public static Expression addSmallIntLargeInt(SmallIntLiteral first, LargeIntLiteral second) {
        BigInteger result = second.getValue().add(new BigInteger(first.getValue().toString()));
        return new LargeIntLiteral(result);
    }

    @ExecFunction(name = "add")
    public static Expression addIntTinyInt(IntegerLiteral first, TinyIntLiteral second) {
        long result = Math.addExact((long) first.getValue(), (long) second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "add")
    public static Expression addIntSmallInt(IntegerLiteral first, SmallIntLiteral second) {
        long result = Math.addExact((long) first.getValue(), (long) second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "add")
    public static Expression addIntInt(IntegerLiteral first, IntegerLiteral second) {
        long result = Math.addExact((long) first.getValue(), (long) second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "add")
    public static Expression addIntBigInt(IntegerLiteral first, BigIntLiteral second) {
        long result = Math.addExact(first.getValue(), second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "add")
    public static Expression addIntLargeInt(IntegerLiteral first, LargeIntLiteral second) {
        BigInteger result = second.getValue().add(new BigInteger(first.getValue().toString()));
        return new LargeIntLiteral(result);
    }

    @ExecFunction(name = "add")
    public static Expression addBigIntTinyInt(BigIntLiteral first, TinyIntLiteral second) {
        long result = Math.addExact(first.getValue(), second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "add")
    public static Expression addBigIntSmallInt(BigIntLiteral first, SmallIntLiteral second) {
        long result = Math.addExact(first.getValue(), second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "add")
    public static Expression addBigIntInt(BigIntLiteral first, IntegerLiteral second) {
        long result = Math.addExact(first.getValue(), second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "add")
    public static Expression addBigIntBigInt(BigIntLiteral first, BigIntLiteral second) {
        long result = Math.addExact(first.getValue(), second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "add")
    public static Expression addBigIntLargeInt(BigIntLiteral first, LargeIntLiteral second) {
        BigInteger result = second.getValue().add(new BigInteger(first.getValue().toString()));
        return new LargeIntLiteral(result);
    }

    @ExecFunction(name = "add")
    public static Expression addLargeIntTinyInt(LargeIntLiteral first, TinyIntLiteral second) {
        BigInteger result = first.getValue().add(new BigInteger(second.getValue().toString()));
        return new LargeIntLiteral(result);
    }

    @ExecFunction(name = "add")
    public static Expression addLargeIntSmallInt(LargeIntLiteral first, SmallIntLiteral second) {
        BigInteger result = first.getValue().add(new BigInteger(second.getValue().toString()));
        return new LargeIntLiteral(result);
    }

    @ExecFunction(name = "add")
    public static Expression addLargeIntInt(LargeIntLiteral first, IntegerLiteral second) {
        BigInteger result = first.getValue().add(new BigInteger(second.getValue().toString()));
        return new LargeIntLiteral(result);
    }

    @ExecFunction(name = "add")
    public static Expression addLargeIntBigInt(LargeIntLiteral first, BigIntLiteral second) {
        BigInteger result = first.getValue().add(new BigInteger(second.getValue().toString()));
        return new LargeIntLiteral(result);
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
        short result = (short) Math.subtractExact(first.getValue(), second.getValue());
        return new SmallIntLiteral(result);
    }

    @ExecFunction(name = "subtract")
    public static Expression subtractTinyIntSmallInt(TinyIntLiteral first, SmallIntLiteral second) {
        int result = Math.subtractExact(first.getValue(), second.getValue());
        return new IntegerLiteral(result);
    }

    @ExecFunction(name = "subtract")
    public static Expression subtractTinyIntInt(TinyIntLiteral first, IntegerLiteral second) {
        long result = Math.subtractExact((long) first.getValue(), (long) second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "subtract")
    public static Expression subtractTinyIntBigInt(TinyIntLiteral first, BigIntLiteral second) {
        long result = Math.subtractExact((long) first.getValue(), (long) second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "subtract")
    public static Expression subtractTinyIntLargeInt(TinyIntLiteral first, LargeIntLiteral second) {
        BigInteger result = second.getValue().subtract(new BigInteger(first.getValue().toString()));
        return new LargeIntLiteral(result);
    }

    @ExecFunction(name = "subtract")
    public static Expression subtractSmallIntTinyInt(SmallIntLiteral first, TinyIntLiteral second) {
        int result = Math.subtractExact(first.getValue(), second.getValue());
        return new IntegerLiteral(result);
    }

    @ExecFunction(name = "subtract")
    public static Expression subtractSmallIntSmallInt(SmallIntLiteral first, SmallIntLiteral second) {
        int result = Math.subtractExact(first.getValue(), second.getValue());
        return new IntegerLiteral(result);
    }

    @ExecFunction(name = "subtract")
    public static Expression subtractSmallIntInt(SmallIntLiteral first, IntegerLiteral second) {
        long result = Math.subtractExact((long) first.getValue(), (long) second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "subtract")
    public static Expression subtractSmallIntBigInt(SmallIntLiteral first, BigIntLiteral second) {
        long result = Math.subtractExact(first.getValue(), second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "subtract")
    public static Expression subtractSmallIntLargeInt(SmallIntLiteral first, LargeIntLiteral second) {
        BigInteger result = second.getValue().subtract(new BigInteger(first.getValue().toString()));
        return new LargeIntLiteral(result);
    }

    @ExecFunction(name = "subtract")
    public static Expression subtractIntTinyInt(IntegerLiteral first, TinyIntLiteral second) {
        long result = Math.subtractExact((long) first.getValue(), (long) second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "subtract")
    public static Expression subtractIntSmallInt(IntegerLiteral first, SmallIntLiteral second) {
        long result = Math.subtractExact((long) first.getValue(), (long) second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "subtract")
    public static Expression subtractIntInt(IntegerLiteral first, IntegerLiteral second) {
        long result = Math.subtractExact((long) first.getValue(), (long) second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "subtract")
    public static Expression subtractIntBigInt(IntegerLiteral first, BigIntLiteral second) {
        long result = Math.subtractExact(first.getValue(), second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "subtract")
    public static Expression subtractIntLargeInt(IntegerLiteral first, LargeIntLiteral second) {
        BigInteger result = second.getValue().subtract(new BigInteger(first.getValue().toString()));
        return new LargeIntLiteral(result);
    }

    @ExecFunction(name = "subtract")
    public static Expression subtractBigIntTinyInt(BigIntLiteral first, TinyIntLiteral second) {
        long result = Math.subtractExact(first.getValue(), second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "subtract")
    public static Expression subtractBigIntSmallInt(BigIntLiteral first, SmallIntLiteral second) {
        long result = Math.subtractExact(first.getValue(), second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "subtract")
    public static Expression subtractBigIntInt(BigIntLiteral first, IntegerLiteral second) {
        long result = Math.subtractExact(first.getValue(), second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "subtract")
    public static Expression subtractBigIntBigInt(BigIntLiteral first, BigIntLiteral second) {
        long result = Math.subtractExact(first.getValue(), second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "subtract")
    public static Expression subtractBigIntLargeInt(BigIntLiteral first, LargeIntLiteral second) {
        BigInteger result = second.getValue().subtract(new BigInteger(first.getValue().toString()));
        return new LargeIntLiteral(result);
    }

    @ExecFunction(name = "subtract")
    public static Expression subtractLargeIntTinyInt(LargeIntLiteral first, TinyIntLiteral second) {
        BigInteger result = first.getValue().subtract(new BigInteger(second.getValue().toString()));
        return new LargeIntLiteral(result);
    }

    @ExecFunction(name = "subtract")
    public static Expression subtractLargeIntSmallInt(LargeIntLiteral first, SmallIntLiteral second) {
        BigInteger result = first.getValue().subtract(new BigInteger(second.getValue().toString()));
        return new LargeIntLiteral(result);
    }

    @ExecFunction(name = "subtract")
    public static Expression subtractLargeIntInt(LargeIntLiteral first, IntegerLiteral second) {
        BigInteger result = first.getValue().subtract(new BigInteger(second.getValue().toString()));
        return new LargeIntLiteral(result);
    }

    @ExecFunction(name = "subtract")
    public static Expression subtractLargeIntBigInt(LargeIntLiteral first, BigIntLiteral second) {
        BigInteger result = first.getValue().subtract(new BigInteger(second.getValue().toString()));
        return new LargeIntLiteral(result);
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
        short result = (short) Math.multiplyExact(first.getValue(), second.getValue());
        return new SmallIntLiteral(result);
    }

    @ExecFunction(name = "multiply")
    public static Expression multiplyTinyIntSmallInt(TinyIntLiteral first, SmallIntLiteral second) {
        int result = Math.multiplyExact(first.getValue(), second.getValue());
        return new IntegerLiteral(result);
    }

    @ExecFunction(name = "multiply")
    public static Expression multiplyTinyIntInt(TinyIntLiteral first, IntegerLiteral second) {
        long result = Math.multiplyExact((long) first.getValue(), (long) second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "multiply")
    public static Expression multiplyTinyIntBigInt(TinyIntLiteral first, BigIntLiteral second) {
        long result = Math.multiplyExact((long) first.getValue(), (long) second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "multiply")
    public static Expression multiplyTinyIntLargeInt(TinyIntLiteral first, LargeIntLiteral second) {
        BigInteger result = second.getValue().multiply(new BigInteger(first.getValue().toString()));
        return new LargeIntLiteral(result);
    }

    @ExecFunction(name = "multiply")
    public static Expression multiplySmallIntTinyInt(SmallIntLiteral first, TinyIntLiteral second) {
        int result = Math.multiplyExact(first.getValue(), second.getValue());
        return new IntegerLiteral(result);
    }

    @ExecFunction(name = "multiply")
    public static Expression multiplySmallIntSmallInt(SmallIntLiteral first, SmallIntLiteral second) {
        int result = Math.multiplyExact(first.getValue(), second.getValue());
        return new IntegerLiteral(result);
    }

    @ExecFunction(name = "multiply")
    public static Expression multiplySmallIntInt(SmallIntLiteral first, IntegerLiteral second) {
        long result = Math.multiplyExact((long) first.getValue(), (long) second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "multiply")
    public static Expression multiplySmallIntBigInt(SmallIntLiteral first, BigIntLiteral second) {
        long result = Math.multiplyExact(first.getValue(), second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "multiply")
    public static Expression multiplySmallIntLargeInt(SmallIntLiteral first, LargeIntLiteral second) {
        BigInteger result = second.getValue().multiply(new BigInteger(first.getValue().toString()));
        return new LargeIntLiteral(result);
    }

    @ExecFunction(name = "multiply")
    public static Expression multiplyIntTinyInt(IntegerLiteral first, TinyIntLiteral second) {
        long result = Math.multiplyExact((long) first.getValue(), (long) second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "multiply")
    public static Expression multiplyIntSmallInt(IntegerLiteral first, SmallIntLiteral second) {
        long result = Math.multiplyExact((long) first.getValue(), (long) second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "multiply")
    public static Expression multiplyIntInt(IntegerLiteral first, IntegerLiteral second) {
        long result = Math.multiplyExact((long) first.getValue(), (long) second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "multiply")
    public static Expression multiplyIntBigInt(IntegerLiteral first, BigIntLiteral second) {
        long result = Math.multiplyExact(first.getValue(), second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "multiply")
    public static Expression multiplyIntLargeInt(IntegerLiteral first, LargeIntLiteral second) {
        BigInteger result = second.getValue().multiply(new BigInteger(first.getValue().toString()));
        return new LargeIntLiteral(result);
    }

    @ExecFunction(name = "multiply")
    public static Expression multiplyBigIntTinyInt(BigIntLiteral first, TinyIntLiteral second) {
        long result = Math.multiplyExact(first.getValue(), second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "multiply")
    public static Expression multiplyBigIntSmallInt(BigIntLiteral first, SmallIntLiteral second) {
        long result = Math.multiplyExact(first.getValue(), second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "multiply")
    public static Expression multiplyBigIntInt(BigIntLiteral first, IntegerLiteral second) {
        long result = Math.multiplyExact(first.getValue(), second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "multiply")
    public static Expression multiplyBigIntBigInt(BigIntLiteral first, BigIntLiteral second) {
        long result = Math.multiplyExact(first.getValue(), second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "multiply")
    public static Expression multiplyBigIntLargeInt(BigIntLiteral first, LargeIntLiteral second) {
        BigInteger result = second.getValue().multiply(new BigInteger(first.getValue().toString()));
        return new LargeIntLiteral(result);
    }

    @ExecFunction(name = "multiply")
    public static Expression multiplyLargeIntTinyInt(LargeIntLiteral first, TinyIntLiteral second) {
        BigInteger result = first.getValue().multiply(new BigInteger(second.getValue().toString()));
        return new LargeIntLiteral(result);
    }

    @ExecFunction(name = "multiply")
    public static Expression multiplyLargeIntSmallInt(LargeIntLiteral first, SmallIntLiteral second) {
        BigInteger result = first.getValue().multiply(new BigInteger(second.getValue().toString()));
        return new LargeIntLiteral(result);
    }

    @ExecFunction(name = "multiply")
    public static Expression multiplyLargeIntInt(LargeIntLiteral first, IntegerLiteral second) {
        BigInteger result = first.getValue().multiply(new BigInteger(second.getValue().toString()));
        return new LargeIntLiteral(result);
    }

    @ExecFunction(name = "multiply")
    public static Expression multiplyLargeIntBigInt(LargeIntLiteral first, BigIntLiteral second) {
        BigInteger result = first.getValue().multiply(new BigInteger(second.getValue().toString()));
        return new LargeIntLiteral(result);
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
        if (second.getValue().compareTo(BigDecimal.ZERO) == 0) {
            return new NullLiteral(first.getDataType());
        }
        DecimalV3Type t1 = (DecimalV3Type) first.getDataType();
        DecimalV3Type t2 = (DecimalV3Type) second.getDataType();
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
    public static void checkInputBoundary(Literal input, double lowerBound, double upperBound,
                                        boolean isLowerInclusive, boolean isUpperInclusive) {
        if (input instanceof DoubleLiteral) {
            double inputValue = ((DoubleLiteral) input).getValue();
            boolean lowerCheck = isLowerInclusive ? (inputValue >= lowerBound) : (inputValue > lowerBound);
            // Check upper bound
            boolean upperCheck = isUpperInclusive ? (inputValue <= upperBound) : (inputValue < upperBound);
            // Return true if both checks are satisfied
            if (!lowerCheck || !upperCheck) {
                throw new NotSupportedException("input " + input.toSql() + " is out of boundary");
            }
        }
    }

    private static Expression checkOutputBoundary(Literal input) {
        if (input instanceof DoubleLiteral) {
            if (((DoubleLiteral) input).getValue().isNaN() || ((DoubleLiteral) input).getValue().isInfinite()) {
                throw new NotSupportedException(input.toSql() + " result is invalid");
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
        checkInputBoundary(first, 0.0d, Double.MAX_VALUE, false, true);
        return checkOutputBoundary(new DoubleLiteral(Math.log(first.getValue())));
    }

    /**
     * log
     */
    @ExecFunction(name = "log")
    public static Expression log(DoubleLiteral first, DoubleLiteral second) {
        checkInputBoundary(first, 0.0d, Double.MAX_VALUE, false, true);
        return checkOutputBoundary(new DoubleLiteral(Math.log(first.getValue()) / Math.log(second.getValue())));
    }

    /**
     * log2
     */
    @ExecFunction(name = "log2")
    public static Expression log2(DoubleLiteral first) {
        checkInputBoundary(first, 0.0d, Double.MAX_VALUE, false, true);
        return checkOutputBoundary(new DoubleLiteral(Math.log(first.getValue()) / Math.log(2.0)));
    }

    /**
     * log10
     */
    @ExecFunction(name = "log10")
    public static Expression log10(DoubleLiteral first) {
        checkInputBoundary(first, 0.0d, Double.MAX_VALUE, false, true);
        return checkOutputBoundary(new DoubleLiteral(Math.log10(first.getValue())));
    }

    /**
     * sqrt
     */
    @ExecFunction(name = "sqrt")
    public static Expression sqrt(DoubleLiteral first) {
        checkInputBoundary(first, 0.0d, Double.MAX_VALUE, true, true);
        return checkOutputBoundary(new DoubleLiteral(Math.sqrt(first.getValue())));
    }

    /**
     * power
     */
    @ExecFunction(name = "power")
    public static Expression power(DoubleLiteral first, DoubleLiteral second) {
        checkInputBoundary(second, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, false, false);
        return checkOutputBoundary(new DoubleLiteral(Math.pow(first.getValue(), second.getValue())));
    }

    /**
     * sin
     */
    @ExecFunction(name = "sin")
    public static Expression sin(DoubleLiteral first) {
        checkInputBoundary(first, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, false, false);
        return checkOutputBoundary(new DoubleLiteral(Math.sin(first.getValue())));
    }

    /**
     * cos
     */
    @ExecFunction(name = "cos")
    public static Expression cos(DoubleLiteral first) {
        checkInputBoundary(first, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, false, false);
        return checkOutputBoundary(new DoubleLiteral(Math.cos(first.getValue())));
    }

    /**
     * tan
     */
    @ExecFunction(name = "tan")
    public static Expression tan(DoubleLiteral first) {
        checkInputBoundary(first, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, false, false);
        return checkOutputBoundary(new DoubleLiteral(Math.tan(first.getValue())));
    }

    /**
     * asin
     */
    @ExecFunction(name = "asin")
    public static Expression asin(DoubleLiteral first) {
        checkInputBoundary(first, -1.0, 1.0, true, true);
        return checkOutputBoundary(new DoubleLiteral(Math.asin(first.getValue())));
    }

    /**
     * acos
     */
    @ExecFunction(name = "acos")
    public static Expression acos(DoubleLiteral first) {
        checkInputBoundary(first, -1.0, 1.0, true, true);
        return checkOutputBoundary(new DoubleLiteral(Math.acos(first.getValue())));
    }

    /**
     * atan
     */
    @ExecFunction(name = "atan")
    public static Expression atan(DoubleLiteral first) {
        return checkOutputBoundary(new DoubleLiteral(Math.atan(first.getValue())));
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
        byte[] byteArray = first.getValue().getBytes(StandardCharsets.UTF_8);  // Convert to bytes in UTF-8
        int byteLength = byteArray.length;
        return new IntegerLiteral(byteLength * Byte.SIZE);
    }

    /**
     * bit_length
     */
    @ExecFunction(name = "bit_length")
    public static Expression bitLength(StringLiteral first) {
        byte[] byteArray = first.getValue().getBytes(StandardCharsets.UTF_8);  // Convert to bytes in UTF-8
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
    @ExecFunction(name = "cosh")
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
     * dlog1
     */
    @ExecFunction(name = "dlog1")
    public static Expression dlog1(DoubleLiteral first) {
        checkInputBoundary(first, 0.0d, Double.MAX_VALUE, false, true);
        return checkOutputBoundary(new DoubleLiteral(Math.log1p(first.getValue())));
    }

    /**
     * dlog10
     */
    @ExecFunction(name = "dlog10")
    public static Expression dlog10(DoubleLiteral first) {
        checkInputBoundary(first, 0.0d, Double.MAX_VALUE, false, true);
        return checkOutputBoundary(new DoubleLiteral(Math.log10(first.getValue())));
    }

    /**
     * dsqrt
     */
    @ExecFunction(name = "dsqrt")
    public static Expression dsqrt(DoubleLiteral first) {
        checkInputBoundary(first, 0.0d, Double.MAX_VALUE, false, true);
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
        return checkOutputBoundary(new DoubleLiteral(first.getValue() / second.getValue()));
    }

    /**
     * fmod
     */
    @ExecFunction(name = "fmod")
    public static Expression fmod(FloatLiteral first, FloatLiteral second) {
        return new FloatLiteral(first.getValue() / second.getValue());
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

}
