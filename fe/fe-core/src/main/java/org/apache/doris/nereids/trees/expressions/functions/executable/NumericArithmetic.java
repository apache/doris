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
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DecimalLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DecimalV3Literal;
import org.apache.doris.nereids.trees.expressions.literal.DoubleLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.LargeIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.SmallIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.types.DecimalV3Type;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * executable functions:
 * add, subtract, multiply, divide
 */
public class NumericArithmetic {
    /**
     * Executable arithmetic functions add
     */
    @ExecFunction(name = "add", argTypes = {"TINYINT", "TINYINT"}, returnType = "SMALLINT")
    public static SmallIntLiteral addTinyIntTinyInt(TinyIntLiteral first, TinyIntLiteral second) {
        short result = (short) Math.addExact(first.getValue(), second.getValue());
        return new SmallIntLiteral(result);
    }

    @ExecFunction(name = "add", argTypes = {"TINYINT", "SMALLINT"}, returnType = "INT")
    public static IntegerLiteral addTinyIntSmallInt(TinyIntLiteral first, SmallIntLiteral second) {
        int result = Math.addExact(first.getValue(), second.getValue());
        return new IntegerLiteral(result);
    }

    @ExecFunction(name = "add", argTypes = {"TINYINT", "INT"}, returnType = "BIGINT")
    public static BigIntLiteral addTinyIntInt(TinyIntLiteral first, IntegerLiteral second) {
        long result = Math.addExact((long) first.getValue(), (long) second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "add", argTypes = {"TINYINT", "BIGINT"}, returnType = "BIGINT")
    public static BigIntLiteral addTinyIntBigInt(TinyIntLiteral first, BigIntLiteral second) {
        long result = Math.addExact((long) first.getValue(), (long) second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "add", argTypes = {"TINYINT", "LARGEINT"}, returnType = "LARGEINT")
    public static LargeIntLiteral addTinyIntLargeInt(TinyIntLiteral first, LargeIntLiteral second) {
        BigInteger result = second.getValue().add(new BigInteger(first.getValue().toString()));
        return new LargeIntLiteral(result);
    }

    @ExecFunction(name = "add", argTypes = {"SMALLINT", "TINYINT"}, returnType = "INT")
    public static IntegerLiteral addSmallIntTinyInt(SmallIntLiteral first, TinyIntLiteral second) {
        int result = Math.addExact(first.getValue(), second.getValue());
        return new IntegerLiteral(result);
    }

    @ExecFunction(name = "add", argTypes = {"SMALLINT", "SMALLINT"}, returnType = "INT")
    public static IntegerLiteral addSmallIntSmallInt(SmallIntLiteral first, SmallIntLiteral second) {
        int result = Math.addExact(first.getValue(), second.getValue());
        return new IntegerLiteral(result);
    }

    @ExecFunction(name = "add", argTypes = {"SMALLINT", "INT"}, returnType = "BIGINT")
    public static BigIntLiteral addSmallIntInt(SmallIntLiteral first, IntegerLiteral second) {
        long result = Math.addExact((long) first.getValue(), (long) second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "add", argTypes = {"SMALLINT", "BIGINT"}, returnType = "BIGINT")
    public static BigIntLiteral addSmallIntBigInt(SmallIntLiteral first, BigIntLiteral second) {
        long result = Math.addExact(first.getValue(), second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "add", argTypes = {"SMALLINT", "LARGEINT"}, returnType = "LARGEINT")
    public static LargeIntLiteral addSmallIntLargeInt(SmallIntLiteral first, LargeIntLiteral second) {
        BigInteger result = second.getValue().add(new BigInteger(first.getValue().toString()));
        return new LargeIntLiteral(result);
    }

    @ExecFunction(name = "add", argTypes = {"INT", "TINYINT"}, returnType = "BIGINT")
    public static BigIntLiteral addIntTinyInt(IntegerLiteral first, TinyIntLiteral second) {
        long result = Math.addExact((long) first.getValue(), (long) second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "add", argTypes = {"INT", "SMALLINT"}, returnType = "BIGINT")
    public static BigIntLiteral addIntSmallInt(IntegerLiteral first, SmallIntLiteral second) {
        long result = Math.addExact((long) first.getValue(), (long) second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "add", argTypes = {"INT", "INT"}, returnType = "BIGINT")
    public static BigIntLiteral addIntInt(IntegerLiteral first, IntegerLiteral second) {
        long result = Math.addExact((long) first.getValue(), (long) second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "add", argTypes = {"INT", "BIGINT"}, returnType = "BIGINT")
    public static BigIntLiteral addIntBigInt(IntegerLiteral first, BigIntLiteral second) {
        long result = Math.addExact(first.getValue(), second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "add", argTypes = {"INT", "LARGEINT"}, returnType = "LARGEINT")
    public static LargeIntLiteral addIntLargeInt(IntegerLiteral first, LargeIntLiteral second) {
        BigInteger result = second.getValue().add(new BigInteger(first.getValue().toString()));
        return new LargeIntLiteral(result);
    }

    @ExecFunction(name = "add", argTypes = {"BIGINT", "TINYINT"}, returnType = "BIGINT")
    public static BigIntLiteral addBigIntTinyInt(BigIntLiteral first, TinyIntLiteral second) {
        long result = Math.addExact(first.getValue(), second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "add", argTypes = {"BIGINT", "SMALLINT"}, returnType = "BIGINT")
    public static BigIntLiteral addBigIntSmallInt(BigIntLiteral first, SmallIntLiteral second) {
        long result = Math.addExact(first.getValue(), second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "add", argTypes = {"BIGINT", "INT"}, returnType = "BIGINT")
    public static BigIntLiteral addBigIntInt(BigIntLiteral first, IntegerLiteral second) {
        long result = Math.addExact(first.getValue(), second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "add", argTypes = {"BIGINT", "BIGINT"}, returnType = "BIGINT")
    public static BigIntLiteral addBigIntBigInt(BigIntLiteral first, BigIntLiteral second) {
        long result = Math.addExact(first.getValue(), second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "add", argTypes = {"BIGINT", "LARGEINT"}, returnType = "LARGEINT")
    public static LargeIntLiteral addBigIntLargeInt(BigIntLiteral first, LargeIntLiteral second) {
        BigInteger result = second.getValue().add(new BigInteger(first.getValue().toString()));
        return new LargeIntLiteral(result);
    }

    @ExecFunction(name = "add", argTypes = {"LARGEINT", "TINYINT"}, returnType = "BIGINT")
    public static LargeIntLiteral addLargeIntTinyInt(LargeIntLiteral first, TinyIntLiteral second) {
        BigInteger result = first.getValue().add(new BigInteger(second.getValue().toString()));
        return new LargeIntLiteral(result);
    }

    @ExecFunction(name = "add", argTypes = {"LARGEINT", "SMALLINT"}, returnType = "BIGINT")
    public static LargeIntLiteral addLargeIntSmallInt(LargeIntLiteral first, SmallIntLiteral second) {
        BigInteger result = first.getValue().add(new BigInteger(second.getValue().toString()));
        return new LargeIntLiteral(result);
    }

    @ExecFunction(name = "add", argTypes = {"LARGEINT", "INT"}, returnType = "BIGINT")
    public static LargeIntLiteral addLargeIntInt(LargeIntLiteral first, IntegerLiteral second) {
        BigInteger result = first.getValue().add(new BigInteger(second.getValue().toString()));
        return new LargeIntLiteral(result);
    }

    @ExecFunction(name = "add", argTypes = {"LARGEINT", "BIGINT"}, returnType = "BIGINT")
    public static LargeIntLiteral addLargeIntBigInt(LargeIntLiteral first, BigIntLiteral second) {
        BigInteger result = first.getValue().add(new BigInteger(second.getValue().toString()));
        return new LargeIntLiteral(result);
    }

    @ExecFunction(name = "add", argTypes = {"LARGEINT", "LARGEINT"}, returnType = "LARGEINT")
    public static LargeIntLiteral addLargeIntLargeInt(LargeIntLiteral first, LargeIntLiteral second) {
        BigInteger result = first.getValue().add(new BigInteger(second.getValue().toString()));
        return new LargeIntLiteral(result);
    }

    @ExecFunction(name = "add", argTypes = {"DOUBLE", "DOUBLE"}, returnType = "DOUBLE")
    public static DoubleLiteral addDoubleDouble(DoubleLiteral first, DoubleLiteral second) {
        double result = first.getValue() + second.getValue();
        return new DoubleLiteral(result);
    }

    @ExecFunction(name = "add", argTypes = {"DECIMAL", "DECIMAL"}, returnType = "DECIMAL")
    public static DecimalLiteral addDecimalDecimal(DecimalLiteral first, DecimalLiteral second) {
        BigDecimal result = first.getValue().add(second.getValue());
        return new DecimalLiteral(result);
    }

    @ExecFunction(name = "add", argTypes = {"DECIMALV3", "DECIMALV3"}, returnType = "DECIMALV3")
    public static DecimalV3Literal addDecimalV3DecimalV3(DecimalV3Literal first, DecimalV3Literal second) {
        BigDecimal result = first.getValue().add(second.getValue());
        return new DecimalV3Literal((DecimalV3Type) first.getDataType(), result);
    }

    /**
     * Executable arithmetic functions subtract
     */
    @ExecFunction(name = "subtract", argTypes = {"TINYINT", "TINYINT"}, returnType = "SMALLINT")
    public static SmallIntLiteral subtractTinyIntTinyInt(TinyIntLiteral first, TinyIntLiteral second) {
        short result = (short) Math.subtractExact(first.getValue(), second.getValue());
        return new SmallIntLiteral(result);
    }

    @ExecFunction(name = "subtract", argTypes = {"TINYINT", "SMALLINT"}, returnType = "INT")
    public static IntegerLiteral subtractTinyIntSmallInt(TinyIntLiteral first, SmallIntLiteral second) {
        int result = Math.subtractExact(first.getValue(), second.getValue());
        return new IntegerLiteral(result);
    }

    @ExecFunction(name = "subtract", argTypes = {"TINYINT", "INT"}, returnType = "BIGINT")
    public static BigIntLiteral subtractTinyIntInt(TinyIntLiteral first, IntegerLiteral second) {
        long result = Math.subtractExact((long) first.getValue(), (long) second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "subtract", argTypes = {"TINYINT", "BIGINT"}, returnType = "BIGINT")
    public static BigIntLiteral subtractTinyIntBigInt(TinyIntLiteral first, BigIntLiteral second) {
        long result = Math.subtractExact((long) first.getValue(), (long) second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "subtract", argTypes = {"TINYINT", "LARGEINT"}, returnType = "LARGEINT")
    public static LargeIntLiteral subtractTinyIntLargeInt(TinyIntLiteral first, LargeIntLiteral second) {
        BigInteger result = second.getValue().subtract(new BigInteger(first.getValue().toString()));
        return new LargeIntLiteral(result);
    }

    @ExecFunction(name = "subtract", argTypes = {"SMALLINT", "TINYINT"}, returnType = "INT")
    public static IntegerLiteral subtractSmallIntTinyInt(SmallIntLiteral first, TinyIntLiteral second) {
        int result = Math.subtractExact(first.getValue(), second.getValue());
        return new IntegerLiteral(result);
    }

    @ExecFunction(name = "subtract", argTypes = {"SMALLINT", "SMALLINT"}, returnType = "INT")
    public static IntegerLiteral subtractSmallIntSmallInt(SmallIntLiteral first, SmallIntLiteral second) {
        int result = Math.subtractExact(first.getValue(), second.getValue());
        return new IntegerLiteral(result);
    }

    @ExecFunction(name = "subtract", argTypes = {"SMALLINT", "INT"}, returnType = "BIGINT")
    public static BigIntLiteral subtractSmallIntInt(SmallIntLiteral first, IntegerLiteral second) {
        long result = Math.subtractExact((long) first.getValue(), (long) second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "subtract", argTypes = {"SMALLINT", "BIGINT"}, returnType = "BIGINT")
    public static BigIntLiteral subtractSmallIntBigInt(SmallIntLiteral first, BigIntLiteral second) {
        long result = Math.subtractExact(first.getValue(), second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "subtract", argTypes = {"SMALLINT", "LARGEINT"}, returnType = "LARGEINT")
    public static LargeIntLiteral subtractSmallIntLargeInt(SmallIntLiteral first, LargeIntLiteral second) {
        BigInteger result = second.getValue().subtract(new BigInteger(first.getValue().toString()));
        return new LargeIntLiteral(result);
    }

    @ExecFunction(name = "subtract", argTypes = {"INT", "TINYINT"}, returnType = "BIGINT")
    public static BigIntLiteral subtractIntTinyInt(IntegerLiteral first, TinyIntLiteral second) {
        long result = Math.subtractExact((long) first.getValue(), (long) second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "subtract", argTypes = {"INT", "SMALLINT"}, returnType = "BIGINT")
    public static BigIntLiteral subtractIntSmallInt(IntegerLiteral first, SmallIntLiteral second) {
        long result = Math.subtractExact((long) first.getValue(), (long) second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "subtract", argTypes = {"INT", "INT"}, returnType = "BIGINT")
    public static BigIntLiteral subtractIntInt(IntegerLiteral first, IntegerLiteral second) {
        long result = Math.subtractExact((long) first.getValue(), (long) second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "subtract", argTypes = {"INT", "BIGINT"}, returnType = "BIGINT")
    public static BigIntLiteral subtractIntBigInt(IntegerLiteral first, BigIntLiteral second) {
        long result = Math.subtractExact(first.getValue(), second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "subtract", argTypes = {"INT", "LARGEINT"}, returnType = "LARGEINT")
    public static LargeIntLiteral subtractIntLargeInt(IntegerLiteral first, LargeIntLiteral second) {
        BigInteger result = second.getValue().subtract(new BigInteger(first.getValue().toString()));
        return new LargeIntLiteral(result);
    }

    @ExecFunction(name = "subtract", argTypes = {"BIGINT", "TINYINT"}, returnType = "BIGINT")
    public static BigIntLiteral subtractBigIntTinyInt(BigIntLiteral first, TinyIntLiteral second) {
        long result = Math.subtractExact(first.getValue(), second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "subtract", argTypes = {"BIGINT", "SMALLINT"}, returnType = "BIGINT")
    public static BigIntLiteral subtractBigIntSmallInt(BigIntLiteral first, SmallIntLiteral second) {
        long result = Math.subtractExact(first.getValue(), second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "subtract", argTypes = {"BIGINT", "INT"}, returnType = "BIGINT")
    public static BigIntLiteral subtractBigIntInt(BigIntLiteral first, IntegerLiteral second) {
        long result = Math.subtractExact(first.getValue(), second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "subtract", argTypes = {"BIGINT", "BIGINT"}, returnType = "BIGINT")
    public static BigIntLiteral subtractBigIntBigInt(BigIntLiteral first, BigIntLiteral second) {
        long result = Math.subtractExact(first.getValue(), second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "subtract", argTypes = {"BIGINT", "LARGEINT"}, returnType = "LARGEINT")
    public static LargeIntLiteral subtractBigIntLargeInt(BigIntLiteral first, LargeIntLiteral second) {
        BigInteger result = second.getValue().subtract(new BigInteger(first.getValue().toString()));
        return new LargeIntLiteral(result);
    }

    @ExecFunction(name = "subtract", argTypes = {"LARGEINT", "TINYINT"}, returnType = "BIGINT")
    public static LargeIntLiteral subtractLargeIntTinyInt(LargeIntLiteral first, TinyIntLiteral second) {
        BigInteger result = first.getValue().subtract(new BigInteger(second.getValue().toString()));
        return new LargeIntLiteral(result);
    }

    @ExecFunction(name = "subtract", argTypes = {"LARGEINT", "SMALLINT"}, returnType = "BIGINT")
    public static LargeIntLiteral subtractLargeIntSmallInt(LargeIntLiteral first, SmallIntLiteral second) {
        BigInteger result = first.getValue().subtract(new BigInteger(second.getValue().toString()));
        return new LargeIntLiteral(result);
    }

    @ExecFunction(name = "subtract", argTypes = {"LARGEINT", "INT"}, returnType = "BIGINT")
    public static LargeIntLiteral subtractLargeIntInt(LargeIntLiteral first, IntegerLiteral second) {
        BigInteger result = first.getValue().subtract(new BigInteger(second.getValue().toString()));
        return new LargeIntLiteral(result);
    }

    @ExecFunction(name = "subtract", argTypes = {"LARGEINT", "BIGINT"}, returnType = "BIGINT")
    public static LargeIntLiteral subtractLargeIntBigInt(LargeIntLiteral first, BigIntLiteral second) {
        BigInteger result = first.getValue().subtract(new BigInteger(second.getValue().toString()));
        return new LargeIntLiteral(result);
    }

    @ExecFunction(name = "subtract", argTypes = {"LARGEINT", "LARGEINT"}, returnType = "LARGEINT")
    public static LargeIntLiteral subtractLargeIntLargeInt(LargeIntLiteral first, LargeIntLiteral second) {
        BigInteger result = first.getValue().subtract(new BigInteger(second.getValue().toString()));
        return new LargeIntLiteral(result);
    }

    @ExecFunction(name = "subtract", argTypes = {"DOUBLE", "DOUBLE"}, returnType = "DOUBLE")
    public static DoubleLiteral subtractDoubleDouble(DoubleLiteral first, DoubleLiteral second) {
        double result = first.getValue() - second.getValue();
        return new DoubleLiteral(result);
    }

    @ExecFunction(name = "subtract", argTypes = {"DECIMAL", "DECIMAL"}, returnType = "DECIMAL")
    public static DecimalLiteral subtractDecimalDecimal(DecimalLiteral first, DecimalLiteral second) {
        BigDecimal result = first.getValue().subtract(second.getValue());
        return new DecimalLiteral(result);
    }

    @ExecFunction(name = "subtract", argTypes = {"DECIMALV3", "DECIMALV3"}, returnType = "DECIMALV3")
    public static DecimalV3Literal subtractDecimalV3DecimalV3(DecimalV3Literal first, DecimalV3Literal second) {
        BigDecimal result = first.getValue().subtract(second.getValue());
        return new DecimalV3Literal((DecimalV3Type) first.getDataType(), result);
    }

    /**
     * Executable arithmetic functions multiply
     */
    @ExecFunction(name = "multiply", argTypes = {"TINYINT", "TINYINT"}, returnType = "SMALLINT")
    public static SmallIntLiteral multiplyTinyIntTinyInt(TinyIntLiteral first, TinyIntLiteral second) {
        short result = (short) Math.multiplyExact(first.getValue(), second.getValue());
        return new SmallIntLiteral(result);
    }

    @ExecFunction(name = "multiply", argTypes = {"TINYINT", "SMALLINT"}, returnType = "INT")
    public static IntegerLiteral multiplyTinyIntSmallInt(TinyIntLiteral first, SmallIntLiteral second) {
        int result = Math.multiplyExact(first.getValue(), second.getValue());
        return new IntegerLiteral(result);
    }

    @ExecFunction(name = "multiply", argTypes = {"TINYINT", "INT"}, returnType = "BIGINT")
    public static BigIntLiteral multiplyTinyIntInt(TinyIntLiteral first, IntegerLiteral second) {
        long result = Math.multiplyExact((long) first.getValue(), (long) second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "multiply", argTypes = {"TINYINT", "BIGINT"}, returnType = "BIGINT")
    public static BigIntLiteral multiplyTinyIntBigInt(TinyIntLiteral first, BigIntLiteral second) {
        long result = Math.multiplyExact((long) first.getValue(), (long) second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "multiply", argTypes = {"TINYINT", "LARGEINT"}, returnType = "LARGEINT")
    public static LargeIntLiteral multiplyTinyIntLargeInt(TinyIntLiteral first, LargeIntLiteral second) {
        BigInteger result = second.getValue().multiply(new BigInteger(first.getValue().toString()));
        return new LargeIntLiteral(result);
    }

    @ExecFunction(name = "multiply", argTypes = {"SMALLINT", "TINYINT"}, returnType = "INT")
    public static IntegerLiteral multiplySmallIntTinyInt(SmallIntLiteral first, TinyIntLiteral second) {
        int result = Math.multiplyExact(first.getValue(), second.getValue());
        return new IntegerLiteral(result);
    }

    @ExecFunction(name = "multiply", argTypes = {"SMALLINT", "SMALLINT"}, returnType = "INT")
    public static IntegerLiteral multiplySmallIntSmallInt(SmallIntLiteral first, SmallIntLiteral second) {
        int result = Math.multiplyExact(first.getValue(), second.getValue());
        return new IntegerLiteral(result);
    }

    @ExecFunction(name = "multiply", argTypes = {"SMALLINT", "INT"}, returnType = "BIGINT")
    public static BigIntLiteral multiplySmallIntInt(SmallIntLiteral first, IntegerLiteral second) {
        long result = Math.multiplyExact((long) first.getValue(), (long) second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "multiply", argTypes = {"SMALLINT", "BIGINT"}, returnType = "BIGINT")
    public static BigIntLiteral multiplySmallIntBigInt(SmallIntLiteral first, BigIntLiteral second) {
        long result = Math.multiplyExact(first.getValue(), second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "multiply", argTypes = {"SMALLINT", "LARGEINT"}, returnType = "LARGEINT")
    public static LargeIntLiteral multiplySmallIntLargeInt(SmallIntLiteral first, LargeIntLiteral second) {
        BigInteger result = second.getValue().multiply(new BigInteger(first.getValue().toString()));
        return new LargeIntLiteral(result);
    }

    @ExecFunction(name = "multiply", argTypes = {"INT", "TINYINT"}, returnType = "BIGINT")
    public static BigIntLiteral multiplyIntTinyInt(IntegerLiteral first, TinyIntLiteral second) {
        long result = Math.multiplyExact((long) first.getValue(), (long) second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "multiply", argTypes = {"INT", "SMALLINT"}, returnType = "BIGINT")
    public static BigIntLiteral multiplyIntSmallInt(IntegerLiteral first, SmallIntLiteral second) {
        long result = Math.multiplyExact((long) first.getValue(), (long) second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "multiply", argTypes = {"INT", "INT"}, returnType = "BIGINT")
    public static BigIntLiteral multiplyIntInt(IntegerLiteral first, IntegerLiteral second) {
        long result = Math.multiplyExact((long) first.getValue(), (long) second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "multiply", argTypes = {"INT", "BIGINT"}, returnType = "BIGINT")
    public static BigIntLiteral multiplyIntBigInt(IntegerLiteral first, BigIntLiteral second) {
        long result = Math.multiplyExact(first.getValue(), second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "multiply", argTypes = {"INT", "LARGEINT"}, returnType = "LARGEINT")
    public static LargeIntLiteral multiplyIntLargeInt(IntegerLiteral first, LargeIntLiteral second) {
        BigInteger result = second.getValue().multiply(new BigInteger(first.getValue().toString()));
        return new LargeIntLiteral(result);
    }

    @ExecFunction(name = "multiply", argTypes = {"BIGINT", "TINYINT"}, returnType = "BIGINT")
    public static BigIntLiteral multiplyBigIntTinyInt(BigIntLiteral first, TinyIntLiteral second) {
        long result = Math.multiplyExact(first.getValue(), second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "multiply", argTypes = {"BIGINT", "SMALLINT"}, returnType = "BIGINT")
    public static BigIntLiteral multiplyBigIntSmallInt(BigIntLiteral first, SmallIntLiteral second) {
        long result = Math.multiplyExact(first.getValue(), second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "multiply", argTypes = {"BIGINT", "INT"}, returnType = "BIGINT")
    public static BigIntLiteral multiplyBigIntInt(BigIntLiteral first, IntegerLiteral second) {
        long result = Math.multiplyExact(first.getValue(), second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "multiply", argTypes = {"BIGINT", "BIGINT"}, returnType = "BIGINT")
    public static BigIntLiteral multiplyBigIntBigInt(BigIntLiteral first, BigIntLiteral second) {
        long result = Math.multiplyExact(first.getValue(), second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "multiply", argTypes = {"BIGINT", "LARGEINT"}, returnType = "LARGEINT")
    public static LargeIntLiteral multiplyBigIntLargeInt(BigIntLiteral first, LargeIntLiteral second) {
        BigInteger result = second.getValue().multiply(new BigInteger(first.getValue().toString()));
        return new LargeIntLiteral(result);
    }

    @ExecFunction(name = "multiply", argTypes = {"LARGEINT", "TINYINT"}, returnType = "BIGINT")
    public static LargeIntLiteral multiplyLargeIntTinyInt(LargeIntLiteral first, TinyIntLiteral second) {
        BigInteger result = first.getValue().multiply(new BigInteger(second.getValue().toString()));
        return new LargeIntLiteral(result);
    }

    @ExecFunction(name = "multiply", argTypes = {"LARGEINT", "SMALLINT"}, returnType = "BIGINT")
    public static LargeIntLiteral multiplyLargeIntSmallInt(LargeIntLiteral first, SmallIntLiteral second) {
        BigInteger result = first.getValue().multiply(new BigInteger(second.getValue().toString()));
        return new LargeIntLiteral(result);
    }

    @ExecFunction(name = "multiply", argTypes = {"LARGEINT", "INT"}, returnType = "BIGINT")
    public static LargeIntLiteral multiplyLargeIntInt(LargeIntLiteral first, IntegerLiteral second) {
        BigInteger result = first.getValue().multiply(new BigInteger(second.getValue().toString()));
        return new LargeIntLiteral(result);
    }

    @ExecFunction(name = "multiply", argTypes = {"LARGEINT", "BIGINT"}, returnType = "BIGINT")
    public static LargeIntLiteral multiplyLargeIntBigInt(LargeIntLiteral first, BigIntLiteral second) {
        BigInteger result = first.getValue().multiply(new BigInteger(second.getValue().toString()));
        return new LargeIntLiteral(result);
    }

    @ExecFunction(name = "multiply", argTypes = {"LARGEINT", "LARGEINT"}, returnType = "LARGEINT")
    public static LargeIntLiteral multiplyLargeIntLargeInt(LargeIntLiteral first, LargeIntLiteral second) {
        BigInteger result = first.getValue().multiply(new BigInteger(second.getValue().toString()));
        return new LargeIntLiteral(result);
    }

    @ExecFunction(name = "multiply", argTypes = {"DOUBLE", "DOUBLE"}, returnType = "DOUBLE")
    public static DoubleLiteral multiplyDoubleDouble(DoubleLiteral first, DoubleLiteral second) {
        double result = first.getValue() * second.getValue();
        return new DoubleLiteral(result);
    }

    @ExecFunction(name = "multiply", argTypes = {"DECIMAL", "DECIMAL"}, returnType = "DECIMAL")
    public static DecimalLiteral multiplyDecimalDecimal(DecimalLiteral first, DecimalLiteral second) {
        BigDecimal result = first.getValue().multiply(second.getValue());
        return new DecimalLiteral(result);
    }

    /**
     * decimalV3 multiply in FE
     */
    @ExecFunction(name = "multiply", argTypes = {"DECIMALV3", "DECIMALV3"}, returnType = "DECIMALV3")
    public static DecimalV3Literal multiplyDecimalV3DecimalV3(DecimalV3Literal first, DecimalV3Literal second) {
        BigDecimal result = first.getValue().multiply(second.getValue());
        DecimalV3Type t1 = (DecimalV3Type) first.getDataType();
        DecimalV3Type t2 = (DecimalV3Type) second.getDataType();
        int precision = t1.getPrecision() + t2.getPrecision();
        int scale = t1.getScale() + t2.getScale();
        return new DecimalV3Literal(DecimalV3Type.createDecimalV3Type(precision, scale), result);
    }

    /**
     * Executable arithmetic functions divide
     */
    @ExecFunction(name = "divide", argTypes = {"DOUBLE", "DOUBLE"}, returnType = "DOUBLE")
    public static Literal divideDouble(DoubleLiteral first, DoubleLiteral second) {
        if (second.getValue() == 0.0) {
            return new NullLiteral(first.getDataType());
        }
        double result = first.getValue() / second.getValue();
        return new DoubleLiteral(result);
    }

    @ExecFunction(name = "divide", argTypes = {"DECIMAL", "DECIMAL"}, returnType = "DECIMAL")
    public static Literal divideDecimal(DecimalLiteral first, DecimalLiteral second) {
        if (first.getValue().compareTo(BigDecimal.ZERO) == 0) {
            return new NullLiteral(first.getDataType());
        }
        BigDecimal result = first.getValue().divide(second.getValue());
        return new DecimalLiteral(result);
    }

    /**
     * decimalv3 divide in FE
     */
    @ExecFunction(name = "divide", argTypes = {"DECIMALV3", "DECIMALV3"}, returnType = "DECIMALV3")
    public static Literal divideDecimalV3(DecimalV3Literal first, DecimalV3Literal second) {
        if (first.getValue().compareTo(BigDecimal.ZERO) == 0) {
            return new NullLiteral(first.getDataType());
        }
        DecimalV3Type t1 = (DecimalV3Type) first.getDataType();
        DecimalV3Type t2 = (DecimalV3Type) second.getDataType();
        BigDecimal result = first.getValue().divide(second.getValue());
        return new DecimalV3Literal(DecimalV3Type.createDecimalV3Type(
                t1.getPrecision(), t1.getScale() - t2.getScale()), result);
    }
}
