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
import org.apache.doris.nereids.trees.expressions.literal.FloatLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.LargeIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.SmallIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;

import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.Random;

/**
 * functions that can be executed in FE.
 */
public class ExecutableFunctions {
    public static final ExecutableFunctions INSTANCE = new ExecutableFunctions();
    private static final Random RANDOM = new SecureRandom();

    /**
     * other scalar function
     */
    @ExecFunction(name = "abs", argTypes = {"TINYINT"}, returnType = "SMALLINT")
    public static Expression abs(TinyIntLiteral literal) {
        return new SmallIntLiteral((byte) Math.abs(literal.getValue()));
    }

    @ExecFunction(name = "abs", argTypes = {"SMALLINT"}, returnType = "INT")
    public static Expression abs(SmallIntLiteral literal) {
        return new IntegerLiteral((short) Math.abs(literal.getValue()));
    }

    @ExecFunction(name = "abs", argTypes = {"INT"}, returnType = "BIGINT")
    public static Expression abs(IntegerLiteral literal) {
        return new BigIntLiteral(Math.abs(literal.getValue()));
    }

    @ExecFunction(name = "abs", argTypes = {"BIGINT"}, returnType = "LARGEINT")
    public static Expression abs(BigIntLiteral literal) {
        return new LargeIntLiteral(new BigInteger(Long.toString(Math.abs(literal.getValue()))));
    }

    @ExecFunction(name = "abs", argTypes = {"LARGEINT"}, returnType = "LARGEINT")
    public static Expression abs(LargeIntLiteral literal) {
        return new LargeIntLiteral(literal.getValue().abs());
    }

    @ExecFunction(name = "abs", argTypes = {"FLOAT"}, returnType = "FLOAT")
    public static Expression abs(FloatLiteral literal) {
        return new FloatLiteral(Math.abs(literal.getValue()));
    }

    @ExecFunction(name = "abs", argTypes = {"DOUBLE"}, returnType = "DOUBLE")
    public static Expression abs(DoubleLiteral literal) {
        return new DoubleLiteral(Math.abs(literal.getValue()));
    }

    @ExecFunction(name = "abs", argTypes = {"DECIMAL"}, returnType = "DECIMAL")
    public static Expression abs(DecimalLiteral literal) {
        return new DecimalLiteral(literal.getValue().abs());
    }

    @ExecFunction(name = "abs", argTypes = {"DECIMALV3"}, returnType = "DECIMALV3")
    public static Expression abs(DecimalV3Literal literal) {
        return new DecimalV3Literal(literal.getValue().abs());
    }

    @ExecFunction(name = "acos", argTypes = {"DOUBLE"}, returnType = "DOUBLE")
    public static Expression acos(DoubleLiteral literal) {
        return new DoubleLiteral(Math.acos(literal.getValue()));
    }

    @ExecFunction(name = "append_trailing_if_char_absent", argTypes = {"VARCHAR", "VARCHAR"}, returnType = "VARCHAR")
    public static Expression appendTrailingIfCharAbsent(VarcharLiteral literal, VarcharLiteral chr) {
        if (literal.getValue().length() != 1) {
            return null;
        }
        return literal.getValue().endsWith(chr.getValue()) ? literal
                : new VarcharLiteral(literal.getValue() + chr.getValue());
    }

    @ExecFunction(name = "e", argTypes = {}, returnType = "DOUBLE")
    public static Expression e() { // CHECKSTYLE IGNORE THIS LINE
        return new DoubleLiteral(Math.E);
    }

    @ExecFunction(name = "p1", argTypes = {}, returnType = "DOUBLE")
    public static Expression pi() {
        return new DoubleLiteral(Math.PI);
    }
}
