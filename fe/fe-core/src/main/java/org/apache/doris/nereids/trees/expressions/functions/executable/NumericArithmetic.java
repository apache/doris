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
import org.apache.doris.nereids.trees.expressions.literal.DecimalLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DecimalV3Literal;
import org.apache.doris.nereids.trees.expressions.literal.DoubleLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.LargeIntLiteral;
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
    public static Expression addTinyIntTinyInt(TinyIntLiteral first, TinyIntLiteral second) {
        short result = (short) Math.addExact(first.getValue(), second.getValue());
        return new SmallIntLiteral(result);
    }

    @ExecFunction(name = "add", argTypes = {"TINYINT", "SMALLINT"}, returnType = "INT")
    public static Expression addTinyIntSmallInt(TinyIntLiteral first, SmallIntLiteral second) {
        int result = Math.addExact(first.getValue(), second.getValue());
        return new IntegerLiteral(result);
    }

    @ExecFunction(name = "add", argTypes = {"TINYINT", "INT"}, returnType = "BIGINT")
    public static Expression addTinyIntInt(TinyIntLiteral first, IntegerLiteral second) {
        long result = Math.addExact((long) first.getValue(), (long) second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "add", argTypes = {"TINYINT", "BIGINT"}, returnType = "BIGINT")
    public static Expression addTinyIntBigInt(TinyIntLiteral first, BigIntLiteral second) {
        long result = Math.addExact((long) first.getValue(), (long) second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "add", argTypes = {"TINYINT", "LARGEINT"}, returnType = "LARGEINT")
    public static Expression addTinyIntLargeInt(TinyIntLiteral first, LargeIntLiteral second) {
        BigInteger result = second.getValue().add(new BigInteger(first.getValue().toString()));
        return new LargeIntLiteral(result);
    }

    @ExecFunction(name = "add", argTypes = {"SMALLINT", "TINYINT"}, returnType = "INT")
    public static Expression addSmallIntTinyInt(SmallIntLiteral first, TinyIntLiteral second) {
        int result = Math.addExact(first.getValue(), second.getValue());
        return new IntegerLiteral(result);
    }

    @ExecFunction(name = "add", argTypes = {"SMALLINT", "SMALLINT"}, returnType = "INT")
    public static Expression addSmallIntSmallInt(SmallIntLiteral first, SmallIntLiteral second) {
        int result = Math.addExact(first.getValue(), second.getValue());
        return new IntegerLiteral(result);
    }

    @ExecFunction(name = "add", argTypes = {"SMALLINT", "INT"}, returnType = "BIGINT")
    public static Expression addSmallIntInt(SmallIntLiteral first, IntegerLiteral second) {
        long result = Math.addExact((long) first.getValue(), (long) second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "add", argTypes = {"SMALLINT", "BIGINT"}, returnType = "BIGINT")
    public static Expression addSmallIntBigInt(SmallIntLiteral first, BigIntLiteral second) {
        long result = Math.addExact(first.getValue(), second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "add", argTypes = {"SMALLINT", "LARGEINT"}, returnType = "LARGEINT")
    public static Expression addSmallIntLargeInt(SmallIntLiteral first, LargeIntLiteral second) {
        BigInteger result = second.getValue().add(new BigInteger(first.getValue().toString()));
        return new LargeIntLiteral(result);
    }

    @ExecFunction(name = "add", argTypes = {"INT", "TINYINT"}, returnType = "BIGINT")
    public static Expression addIntTinyInt(IntegerLiteral first, TinyIntLiteral second) {
        long result = Math.addExact((long) first.getValue(), (long) second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "add", argTypes = {"INT", "SMALLINT"}, returnType = "BIGINT")
    public static Expression addIntSmallInt(IntegerLiteral first, SmallIntLiteral second) {
        long result = Math.addExact((long) first.getValue(), (long) second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "add", argTypes = {"INT", "INT"}, returnType = "BIGINT")
    public static Expression addIntInt(IntegerLiteral first, IntegerLiteral second) {
        long result = Math.addExact((long) first.getValue(), (long) second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "add", argTypes = {"INT", "BIGINT"}, returnType = "BIGINT")
    public static Expression addIntBigInt(IntegerLiteral first, BigIntLiteral second) {
        long result = Math.addExact(first.getValue(), second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "add", argTypes = {"INT", "LARGEINT"}, returnType = "LARGEINT")
    public static Expression addIntLargeInt(IntegerLiteral first, LargeIntLiteral second) {
        BigInteger result = second.getValue().add(new BigInteger(first.getValue().toString()));
        return new LargeIntLiteral(result);
    }

    @ExecFunction(name = "add", argTypes = {"BIGINT", "TINYINT"}, returnType = "BIGINT")
    public static Expression addBigIntTinyInt(BigIntLiteral first, TinyIntLiteral second) {
        long result = Math.addExact(first.getValue(), second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "add", argTypes = {"BIGINT", "SMALLINT"}, returnType = "BIGINT")
    public static Expression addBigIntSmallInt(BigIntLiteral first, SmallIntLiteral second) {
        long result = Math.addExact(first.getValue(), second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "add", argTypes = {"BIGINT", "INT"}, returnType = "BIGINT")
    public static Expression addBigIntInt(BigIntLiteral first, IntegerLiteral second) {
        long result = Math.addExact(first.getValue(), second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "add", argTypes = {"BIGINT", "BIGINT"}, returnType = "BIGINT")
    public static Expression addBigIntBigInt(BigIntLiteral first, BigIntLiteral second) {
        long result = Math.addExact(first.getValue(), second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "add", argTypes = {"BIGINT", "LARGEINT"}, returnType = "LARGEINT")
    public static Expression addBigIntLargeInt(BigIntLiteral first, LargeIntLiteral second) {
        BigInteger result = second.getValue().add(new BigInteger(first.getValue().toString()));
        return new LargeIntLiteral(result);
    }

    @ExecFunction(name = "add", argTypes = {"LARGEINT", "TINYINT"}, returnType = "BIGINT")
    public static Expression addLargeIntTinyInt(LargeIntLiteral first, TinyIntLiteral second) {
        BigInteger result = first.getValue().add(new BigInteger(second.getValue().toString()));
        return new LargeIntLiteral(result);
    }

    @ExecFunction(name = "add", argTypes = {"LARGEINT", "SMALLINT"}, returnType = "BIGINT")
    public static Expression addLargeIntSmallInt(LargeIntLiteral first, SmallIntLiteral second) {
        BigInteger result = first.getValue().add(new BigInteger(second.getValue().toString()));
        return new LargeIntLiteral(result);
    }

    @ExecFunction(name = "add", argTypes = {"LARGEINT", "INT"}, returnType = "BIGINT")
    public static Expression addLargeIntInt(LargeIntLiteral first, IntegerLiteral second) {
        BigInteger result = first.getValue().add(new BigInteger(second.getValue().toString()));
        return new LargeIntLiteral(result);
    }

    @ExecFunction(name = "add", argTypes = {"LARGEINT", "BIGINT"}, returnType = "BIGINT")
    public static Expression addLargeIntBigInt(LargeIntLiteral first, BigIntLiteral second) {
        BigInteger result = first.getValue().add(new BigInteger(second.getValue().toString()));
        return new LargeIntLiteral(result);
    }

    @ExecFunction(name = "add", argTypes = {"LARGEINT", "LARGEINT"}, returnType = "LARGEINT")
    public static Expression addLargeIntLargeInt(LargeIntLiteral first, LargeIntLiteral second) {
        BigInteger result = first.getValue().add(new BigInteger(second.getValue().toString()));
        return new LargeIntLiteral(result);
    }

    @ExecFunction(name = "add", argTypes = {"DOUBLE", "DOUBLE"}, returnType = "DOUBLE")
    public static Expression addDoubleDouble(DoubleLiteral first, DoubleLiteral second) {
        double result = first.getValue() + second.getValue();
        return new DoubleLiteral(result);
    }

    @ExecFunction(name = "add", argTypes = {"DECIMAL", "DECIMAL"}, returnType = "DECIMAL")
    public static Expression addDecimalDecimal(DecimalLiteral first, DecimalLiteral second) {
        BigDecimal result = first.getValue().add(second.getValue());
        return new DecimalLiteral(result);
    }

    @ExecFunction(name = "add", argTypes = {"DECIMALV3", "DECIMALV3"}, returnType = "DECIMALV3")
    public static Expression addDecimalV3DecimalV3(DecimalV3Literal first, DecimalV3Literal second) {
        BigDecimal result = first.getValue().add(second.getValue());
        return new DecimalV3Literal((DecimalV3Type) first.getDataType(), result);
    }

    /**
     * Executable arithmetic functions subtract
     */
    @ExecFunction(name = "subtract", argTypes = {"TINYINT", "TINYINT"}, returnType = "SMALLINT")
    public static Expression subtractTinyIntTinyInt(TinyIntLiteral first, TinyIntLiteral second) {
        short result = (short) Math.subtractExact(first.getValue(), second.getValue());
        return new SmallIntLiteral(result);
    }

    @ExecFunction(name = "subtract", argTypes = {"TINYINT", "SMALLINT"}, returnType = "INT")
    public static Expression subtractTinyIntSmallInt(TinyIntLiteral first, SmallIntLiteral second) {
        int result = Math.subtractExact(first.getValue(), second.getValue());
        return new IntegerLiteral(result);
    }

    @ExecFunction(name = "subtract", argTypes = {"TINYINT", "INT"}, returnType = "BIGINT")
    public static Expression subtractTinyIntInt(TinyIntLiteral first, IntegerLiteral second) {
        long result = Math.subtractExact((long) first.getValue(), (long) second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "subtract", argTypes = {"TINYINT", "BIGINT"}, returnType = "BIGINT")
    public static Expression subtractTinyIntBigInt(TinyIntLiteral first, BigIntLiteral second) {
        long result = Math.subtractExact((long) first.getValue(), (long) second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "subtract", argTypes = {"TINYINT", "LARGEINT"}, returnType = "LARGEINT")
    public static Expression subtractTinyIntLargeInt(TinyIntLiteral first, LargeIntLiteral second) {
        BigInteger result = second.getValue().subtract(new BigInteger(first.getValue().toString()));
        return new LargeIntLiteral(result);
    }

    @ExecFunction(name = "subtract", argTypes = {"SMALLINT", "TINYINT"}, returnType = "INT")
    public static Expression subtractSmallIntTinyInt(SmallIntLiteral first, TinyIntLiteral second) {
        int result = Math.subtractExact(first.getValue(), second.getValue());
        return new IntegerLiteral(result);
    }

    @ExecFunction(name = "subtract", argTypes = {"SMALLINT", "SMALLINT"}, returnType = "INT")
    public static Expression subtractSmallIntSmallInt(SmallIntLiteral first, SmallIntLiteral second) {
        int result = Math.subtractExact(first.getValue(), second.getValue());
        return new IntegerLiteral(result);
    }

    @ExecFunction(name = "subtract", argTypes = {"SMALLINT", "INT"}, returnType = "BIGINT")
    public static Expression subtractSmallIntInt(SmallIntLiteral first, IntegerLiteral second) {
        long result = Math.subtractExact((long) first.getValue(), (long) second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "subtract", argTypes = {"SMALLINT", "BIGINT"}, returnType = "BIGINT")
    public static Expression subtractSmallIntBigInt(SmallIntLiteral first, BigIntLiteral second) {
        long result = Math.subtractExact(first.getValue(), second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "subtract", argTypes = {"SMALLINT", "LARGEINT"}, returnType = "LARGEINT")
    public static Expression subtractSmallIntLargeInt(SmallIntLiteral first, LargeIntLiteral second) {
        BigInteger result = second.getValue().subtract(new BigInteger(first.getValue().toString()));
        return new LargeIntLiteral(result);
    }

    @ExecFunction(name = "subtract", argTypes = {"INT", "TINYINT"}, returnType = "BIGINT")
    public static Expression subtractIntTinyInt(IntegerLiteral first, TinyIntLiteral second) {
        long result = Math.subtractExact((long) first.getValue(), (long) second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "subtract", argTypes = {"INT", "SMALLINT"}, returnType = "BIGINT")
    public static Expression subtractIntSmallInt(IntegerLiteral first, SmallIntLiteral second) {
        long result = Math.subtractExact((long) first.getValue(), (long) second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "subtract", argTypes = {"INT", "INT"}, returnType = "BIGINT")
    public static Expression subtractIntInt(IntegerLiteral first, IntegerLiteral second) {
        long result = Math.subtractExact((long) first.getValue(), (long) second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "subtract", argTypes = {"INT", "BIGINT"}, returnType = "BIGINT")
    public static Expression subtractIntBigInt(IntegerLiteral first, BigIntLiteral second) {
        long result = Math.subtractExact(first.getValue(), second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "subtract", argTypes = {"INT", "LARGEINT"}, returnType = "LARGEINT")
    public static Expression subtractIntLargeInt(IntegerLiteral first, LargeIntLiteral second) {
        BigInteger result = second.getValue().subtract(new BigInteger(first.getValue().toString()));
        return new LargeIntLiteral(result);
    }

    @ExecFunction(name = "subtract", argTypes = {"BIGINT", "TINYINT"}, returnType = "BIGINT")
    public static Expression subtractBigIntTinyInt(BigIntLiteral first, TinyIntLiteral second) {
        long result = Math.subtractExact(first.getValue(), second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "subtract", argTypes = {"BIGINT", "SMALLINT"}, returnType = "BIGINT")
    public static Expression subtractBigIntSmallInt(BigIntLiteral first, SmallIntLiteral second) {
        long result = Math.subtractExact(first.getValue(), second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "subtract", argTypes = {"BIGINT", "INT"}, returnType = "BIGINT")
    public static Expression subtractBigIntInt(BigIntLiteral first, IntegerLiteral second) {
        long result = Math.subtractExact(first.getValue(), second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "subtract", argTypes = {"BIGINT", "BIGINT"}, returnType = "BIGINT")
    public static Expression subtractBigIntBigInt(BigIntLiteral first, BigIntLiteral second) {
        long result = Math.subtractExact(first.getValue(), second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "subtract", argTypes = {"BIGINT", "LARGEINT"}, returnType = "LARGEINT")
    public static Expression subtractBigIntLargeInt(BigIntLiteral first, LargeIntLiteral second) {
        BigInteger result = second.getValue().subtract(new BigInteger(first.getValue().toString()));
        return new LargeIntLiteral(result);
    }

    @ExecFunction(name = "subtract", argTypes = {"LARGEINT", "TINYINT"}, returnType = "BIGINT")
    public static Expression subtractLargeIntTinyInt(LargeIntLiteral first, TinyIntLiteral second) {
        BigInteger result = first.getValue().subtract(new BigInteger(second.getValue().toString()));
        return new LargeIntLiteral(result);
    }

    @ExecFunction(name = "subtract", argTypes = {"LARGEINT", "SMALLINT"}, returnType = "BIGINT")
    public static Expression subtractLargeIntSmallInt(LargeIntLiteral first, SmallIntLiteral second) {
        BigInteger result = first.getValue().subtract(new BigInteger(second.getValue().toString()));
        return new LargeIntLiteral(result);
    }

    @ExecFunction(name = "subtract", argTypes = {"LARGEINT", "INT"}, returnType = "BIGINT")
    public static Expression subtractLargeIntInt(LargeIntLiteral first, IntegerLiteral second) {
        BigInteger result = first.getValue().subtract(new BigInteger(second.getValue().toString()));
        return new LargeIntLiteral(result);
    }

    @ExecFunction(name = "subtract", argTypes = {"LARGEINT", "BIGINT"}, returnType = "BIGINT")
    public static Expression subtractLargeIntBigInt(LargeIntLiteral first, BigIntLiteral second) {
        BigInteger result = first.getValue().subtract(new BigInteger(second.getValue().toString()));
        return new LargeIntLiteral(result);
    }

    @ExecFunction(name = "subtract", argTypes = {"LARGEINT", "LARGEINT"}, returnType = "LARGEINT")
    public static Expression subtractLargeIntLargeInt(LargeIntLiteral first, LargeIntLiteral second) {
        BigInteger result = first.getValue().subtract(new BigInteger(second.getValue().toString()));
        return new LargeIntLiteral(result);
    }

    @ExecFunction(name = "subtract", argTypes = {"DOUBLE", "DOUBLE"}, returnType = "DOUBLE")
    public static Expression subtractDoubleDouble(DoubleLiteral first, DoubleLiteral second) {
        double result = first.getValue() - second.getValue();
        return new DoubleLiteral(result);
    }

    @ExecFunction(name = "subtract", argTypes = {"DECIMAL", "DECIMAL"}, returnType = "DECIMAL")
    public static Expression subtractDecimalDecimal(DecimalLiteral first, DecimalLiteral second) {
        BigDecimal result = first.getValue().subtract(second.getValue());
        return new DecimalLiteral(result);
    }

    @ExecFunction(name = "subtract", argTypes = {"DECIMALV3", "DECIMALV3"}, returnType = "DECIMALV3")
    public static Expression subtractDecimalV3DecimalV3(DecimalV3Literal first, DecimalV3Literal second) {
        BigDecimal result = first.getValue().subtract(second.getValue());
        return new DecimalV3Literal((DecimalV3Type) first.getDataType(), result);
    }

    /**
     * Executable arithmetic functions multiply
     */
    @ExecFunction(name = "multiply", argTypes = {"TINYINT", "TINYINT"}, returnType = "SMALLINT")
    public static Expression multiplyTinyIntTinyInt(TinyIntLiteral first, TinyIntLiteral second) {
        short result = (short) Math.multiplyExact(first.getValue(), second.getValue());
        return new SmallIntLiteral(result);
    }

    @ExecFunction(name = "multiply", argTypes = {"TINYINT", "SMALLINT"}, returnType = "INT")
    public static Expression multiplyTinyIntSmallInt(TinyIntLiteral first, SmallIntLiteral second) {
        int result = Math.multiplyExact(first.getValue(), second.getValue());
        return new IntegerLiteral(result);
    }

    @ExecFunction(name = "multiply", argTypes = {"TINYINT", "INT"}, returnType = "BIGINT")
    public static Expression multiplyTinyIntInt(TinyIntLiteral first, IntegerLiteral second) {
        long result = Math.multiplyExact((long) first.getValue(), (long) second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "multiply", argTypes = {"TINYINT", "BIGINT"}, returnType = "BIGINT")
    public static Expression multiplyTinyIntBigInt(TinyIntLiteral first, BigIntLiteral second) {
        long result = Math.multiplyExact((long) first.getValue(), (long) second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "multiply", argTypes = {"TINYINT", "LARGEINT"}, returnType = "LARGEINT")
    public static Expression multiplyTinyIntLargeInt(TinyIntLiteral first, LargeIntLiteral second) {
        BigInteger result = second.getValue().multiply(new BigInteger(first.getValue().toString()));
        return new LargeIntLiteral(result);
    }

    @ExecFunction(name = "multiply", argTypes = {"SMALLINT", "TINYINT"}, returnType = "INT")
    public static Expression multiplySmallIntTinyInt(SmallIntLiteral first, TinyIntLiteral second) {
        int result = Math.multiplyExact(first.getValue(), second.getValue());
        return new IntegerLiteral(result);
    }

    @ExecFunction(name = "multiply", argTypes = {"SMALLINT", "SMALLINT"}, returnType = "INT")
    public static Expression multiplySmallIntSmallInt(SmallIntLiteral first, SmallIntLiteral second) {
        int result = Math.multiplyExact(first.getValue(), second.getValue());
        return new IntegerLiteral(result);
    }

    @ExecFunction(name = "multiply", argTypes = {"SMALLINT", "INT"}, returnType = "BIGINT")
    public static Expression multiplySmallIntInt(SmallIntLiteral first, IntegerLiteral second) {
        long result = Math.multiplyExact((long) first.getValue(), (long) second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "multiply", argTypes = {"SMALLINT", "BIGINT"}, returnType = "BIGINT")
    public static Expression multiplySmallIntBigInt(SmallIntLiteral first, BigIntLiteral second) {
        long result = Math.multiplyExact(first.getValue(), second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "multiply", argTypes = {"SMALLINT", "LARGEINT"}, returnType = "LARGEINT")
    public static Expression multiplySmallIntLargeInt(SmallIntLiteral first, LargeIntLiteral second) {
        BigInteger result = second.getValue().multiply(new BigInteger(first.getValue().toString()));
        return new LargeIntLiteral(result);
    }

    @ExecFunction(name = "multiply", argTypes = {"INT", "TINYINT"}, returnType = "BIGINT")
    public static Expression multiplyIntTinyInt(IntegerLiteral first, TinyIntLiteral second) {
        long result = Math.multiplyExact((long) first.getValue(), (long) second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "multiply", argTypes = {"INT", "SMALLINT"}, returnType = "BIGINT")
    public static Expression multiplyIntSmallInt(IntegerLiteral first, SmallIntLiteral second) {
        long result = Math.multiplyExact((long) first.getValue(), (long) second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "multiply", argTypes = {"INT", "INT"}, returnType = "BIGINT")
    public static Expression multiplyIntInt(IntegerLiteral first, IntegerLiteral second) {
        long result = Math.multiplyExact((long) first.getValue(), (long) second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "multiply", argTypes = {"INT", "BIGINT"}, returnType = "BIGINT")
    public static Expression multiplyIntBigInt(IntegerLiteral first, BigIntLiteral second) {
        long result = Math.multiplyExact(first.getValue(), second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "multiply", argTypes = {"INT", "LARGEINT"}, returnType = "LARGEINT")
    public static Expression multiplyIntLargeInt(IntegerLiteral first, LargeIntLiteral second) {
        BigInteger result = second.getValue().multiply(new BigInteger(first.getValue().toString()));
        return new LargeIntLiteral(result);
    }

    @ExecFunction(name = "multiply", argTypes = {"BIGINT", "TINYINT"}, returnType = "BIGINT")
    public static Expression multiplyBigIntTinyInt(BigIntLiteral first, TinyIntLiteral second) {
        long result = Math.multiplyExact(first.getValue(), second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "multiply", argTypes = {"BIGINT", "SMALLINT"}, returnType = "BIGINT")
    public static Expression multiplyBigIntSmallInt(BigIntLiteral first, SmallIntLiteral second) {
        long result = Math.multiplyExact(first.getValue(), second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "multiply", argTypes = {"BIGINT", "INT"}, returnType = "BIGINT")
    public static Expression multiplyBigIntInt(BigIntLiteral first, IntegerLiteral second) {
        long result = Math.multiplyExact(first.getValue(), second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "multiply", argTypes = {"BIGINT", "BIGINT"}, returnType = "BIGINT")
    public static Expression multiplyBigIntBigInt(BigIntLiteral first, BigIntLiteral second) {
        long result = Math.multiplyExact(first.getValue(), second.getValue());
        return new BigIntLiteral(result);
    }

    @ExecFunction(name = "multiply", argTypes = {"BIGINT", "LARGEINT"}, returnType = "LARGEINT")
    public static Expression multiplyBigIntLargeInt(BigIntLiteral first, LargeIntLiteral second) {
        BigInteger result = second.getValue().multiply(new BigInteger(first.getValue().toString()));
        return new LargeIntLiteral(result);
    }

    @ExecFunction(name = "multiply", argTypes = {"LARGEINT", "TINYINT"}, returnType = "BIGINT")
    public static Expression multiplyLargeIntTinyInt(LargeIntLiteral first, TinyIntLiteral second) {
        BigInteger result = first.getValue().multiply(new BigInteger(second.getValue().toString()));
        return new LargeIntLiteral(result);
    }

    @ExecFunction(name = "multiply", argTypes = {"LARGEINT", "SMALLINT"}, returnType = "BIGINT")
    public static Expression multiplyLargeIntSmallInt(LargeIntLiteral first, SmallIntLiteral second) {
        BigInteger result = first.getValue().multiply(new BigInteger(second.getValue().toString()));
        return new LargeIntLiteral(result);
    }

    @ExecFunction(name = "multiply", argTypes = {"LARGEINT", "INT"}, returnType = "BIGINT")
    public static Expression multiplyLargeIntInt(LargeIntLiteral first, IntegerLiteral second) {
        BigInteger result = first.getValue().multiply(new BigInteger(second.getValue().toString()));
        return new LargeIntLiteral(result);
    }

    @ExecFunction(name = "multiply", argTypes = {"LARGEINT", "BIGINT"}, returnType = "BIGINT")
    public static Expression multiplyLargeIntBigInt(LargeIntLiteral first, BigIntLiteral second) {
        BigInteger result = first.getValue().multiply(new BigInteger(second.getValue().toString()));
        return new LargeIntLiteral(result);
    }

    @ExecFunction(name = "multiply", argTypes = {"LARGEINT", "LARGEINT"}, returnType = "LARGEINT")
    public static Expression multiplyLargeIntLargeInt(LargeIntLiteral first, LargeIntLiteral second) {
        BigInteger result = first.getValue().multiply(new BigInteger(second.getValue().toString()));
        return new LargeIntLiteral(result);
    }

    @ExecFunction(name = "multiply", argTypes = {"DOUBLE", "DOUBLE"}, returnType = "DOUBLE")
    public static Expression multiplyDoubleDouble(DoubleLiteral first, DoubleLiteral second) {
        double result = first.getValue() * second.getValue();
        return new DoubleLiteral(result);
    }

    @ExecFunction(name = "multiply", argTypes = {"DECIMAL", "DECIMAL"}, returnType = "DECIMAL")
    public static Expression multiplyDecimalDecimal(DecimalLiteral first, DecimalLiteral second) {
        BigDecimal result = first.getValue().multiply(second.getValue());
        return new DecimalLiteral(result);
    }

    /**
     * decimalV3 multiply in FE
     */
    @ExecFunction(name = "multiply", argTypes = {"DECIMALV3", "DECIMALV3"}, returnType = "DECIMALV3")
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
    @ExecFunction(name = "divide", argTypes = {"DOUBLE", "DOUBLE"}, returnType = "DOUBLE")
    public static Expression divideDouble(DoubleLiteral first, DoubleLiteral second) {
        if (second.getValue() == 0.0) {
            return new NullLiteral(first.getDataType());
        }
        double result = first.getValue() / second.getValue();
        return new DoubleLiteral(result);
    }

    /**
     * Executable arithmetic functions divide
     */
    @ExecFunction(name = "divide", argTypes = {"DECIMAL", "DECIMAL"}, returnType = "DECIMAL")
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
    @ExecFunction(name = "divide", argTypes = {"DECIMALV3", "DECIMALV3"}, returnType = "DECIMALV3")
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
}
