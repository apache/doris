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

package org.apache.doris.nereids.trees.expressions;

import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.CharLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DecimalLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DoubleLiteral;
import org.apache.doris.nereids.trees.expressions.literal.FloatLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.LargeIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.SmallIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.trees.expressions.typecoercion.TypeCheckResult;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;

public class ExpectedInputTypesTest {
    @Test
    public void testAnd() {
        TypeCheckResult typeCheckResult;

        And secondInputIsNotBoolean = new And(new IntegerLiteral(1), BooleanLiteral.TRUE);
        typeCheckResult = secondInputIsNotBoolean.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.failed());

        And firstInputIsNotBoolean = new And(BooleanLiteral.TRUE, new IntegerLiteral(1));
        typeCheckResult = firstInputIsNotBoolean.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.failed());

        And bothInputAreBoolean = new And(BooleanLiteral.TRUE, BooleanLiteral.FALSE);
        typeCheckResult = bothInputAreBoolean.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.success());
    }

    @Test
    public void testOr() {
        TypeCheckResult typeCheckResult;

        Or secondInputIsNotBoolean = new Or(new IntegerLiteral(1), BooleanLiteral.TRUE);
        typeCheckResult = secondInputIsNotBoolean.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.failed());

        Or firstInputIsNotBoolean = new Or(BooleanLiteral.TRUE, new IntegerLiteral(1));
        typeCheckResult = firstInputIsNotBoolean.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.failed());

        Or bothInputAreBoolean = new Or(BooleanLiteral.TRUE, BooleanLiteral.FALSE);
        typeCheckResult = bothInputAreBoolean.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.success());
    }

    @Test
    public void testDivide() {
        TypeCheckResult typeCheckResult;

        Divide tinyInt = new Divide(new TinyIntLiteral((byte) 1), new TinyIntLiteral((byte) 2));
        typeCheckResult = tinyInt.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.success());

        Divide smallInt = new Divide(new SmallIntLiteral((short) 1), new SmallIntLiteral((short) 2));
        typeCheckResult = smallInt.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.success());

        Divide integer = new Divide(new IntegerLiteral(1), new IntegerLiteral(2));
        typeCheckResult = integer.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.success());

        Divide bigint = new Divide(new BigIntLiteral(1L), new BigIntLiteral(2L));
        typeCheckResult = bigint.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.success());

        Divide largeInt = new Divide(new LargeIntLiteral(new BigInteger("1")),
                new LargeIntLiteral(new BigInteger("1")));
        typeCheckResult = largeInt.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.success());

        Divide floatType = new Divide(new FloatLiteral(1.0F), new FloatLiteral(2.0F));
        typeCheckResult = floatType.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.success());

        Divide doubleType = new Divide(new DoubleLiteral(1.0), new DoubleLiteral(2.0));
        typeCheckResult = doubleType.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.success());

        Divide decimalType = new Divide(new DecimalLiteral(new BigDecimal("12.3")),
                new DecimalLiteral(new BigDecimal("12.3")));
        typeCheckResult = decimalType.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.success());

        Divide charType = new Divide(new CharLiteral("1", 1), new CharLiteral("2", 1));
        typeCheckResult = charType.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.failed());
        // this means has two type check error
        Assertions.assertEquals(4, typeCheckResult.getMessage().split(",").length);

        Divide varchar = new Divide(new VarcharLiteral("1", 10), new VarcharLiteral("2", 10));
        typeCheckResult = varchar.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.failed());
        // this means has two type check error
        Assertions.assertEquals(4, typeCheckResult.getMessage().split(",").length);

        Divide string = new Divide(new StringLiteral("1"), new StringLiteral("2"));
        typeCheckResult = string.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.failed());
        // this means has two type check error
        Assertions.assertEquals(4, typeCheckResult.getMessage().split(",").length);

        Divide bool = new Divide(BooleanLiteral.FALSE, BooleanLiteral.TRUE);
        typeCheckResult = bool.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.failed());
        // this means has two type check error
        Assertions.assertEquals(4, typeCheckResult.getMessage().split(",").length);

        Divide nullType = new Divide(NullLiteral.INSTANCE, NullLiteral.INSTANCE);
        typeCheckResult = nullType.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.failed());
        // this means has two type check error
        Assertions.assertEquals(4, typeCheckResult.getMessage().split(",").length);
    }

    @Test
    public void testComparisonPredicate() {
        TypeCheckResult typeCheckResult;

        LessThan tinyInt = new LessThan(new TinyIntLiteral((byte) 1), new TinyIntLiteral((byte) 2));
        typeCheckResult = tinyInt.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.success());

        LessThan smallInt = new LessThan(new SmallIntLiteral((short) 1), new SmallIntLiteral((short) 2));
        typeCheckResult = smallInt.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.success());

        LessThan integer = new LessThan(new IntegerLiteral(1), new IntegerLiteral(2));
        typeCheckResult = integer.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.success());

        LessThan bigint = new LessThan(new BigIntLiteral(1L), new BigIntLiteral(2L));
        typeCheckResult = bigint.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.success());

        LessThan largeInt = new LessThan(new LargeIntLiteral(new BigInteger("1")),
                new LargeIntLiteral(new BigInteger("1")));
        typeCheckResult = largeInt.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.success());

        LessThan floatType = new LessThan(new FloatLiteral(1.0F), new FloatLiteral(2.0F));
        typeCheckResult = floatType.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.success());

        LessThan doubleType = new LessThan(new DoubleLiteral(1.0), new DoubleLiteral(2.0));
        typeCheckResult = doubleType.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.success());

        LessThan decimalType = new LessThan(new DecimalLiteral(new BigDecimal("12.3")),
                new DecimalLiteral(new BigDecimal("12.3")));
        typeCheckResult = decimalType.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.success());

        LessThan charType = new LessThan(new CharLiteral("1", 1), new CharLiteral("2", 1));
        typeCheckResult = charType.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.success());

        LessThan varchar = new LessThan(new VarcharLiteral("1", 10), new VarcharLiteral("2", 10));
        typeCheckResult = varchar.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.success());

        LessThan string = new LessThan(new StringLiteral("1"), new StringLiteral("2"));
        typeCheckResult = string.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.success());

        LessThan bool = new LessThan(BooleanLiteral.FALSE, BooleanLiteral.TRUE);
        typeCheckResult = bool.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.success());

        LessThan nullType = new LessThan(NullLiteral.INSTANCE, NullLiteral.INSTANCE);
        typeCheckResult = nullType.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.success());
    }

    @Test
    public void testMultiply() {
        TypeCheckResult typeCheckResult;

        Multiply tinyInt = new Multiply(new TinyIntLiteral((byte) 1), new TinyIntLiteral((byte) 2));
        typeCheckResult = tinyInt.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.success());

        Multiply smallInt = new Multiply(new SmallIntLiteral((short) 1), new SmallIntLiteral((short) 2));
        typeCheckResult = smallInt.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.success());

        Multiply integer = new Multiply(new IntegerLiteral(1), new IntegerLiteral(2));
        typeCheckResult = integer.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.success());

        Multiply bigint = new Multiply(new BigIntLiteral(1L), new BigIntLiteral(2L));
        typeCheckResult = bigint.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.success());

        Multiply largeInt = new Multiply(new LargeIntLiteral(new BigInteger("1")),
                new LargeIntLiteral(new BigInteger("1")));
        typeCheckResult = largeInt.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.success());

        Multiply floatType = new Multiply(new FloatLiteral(1.0F), new FloatLiteral(2.0F));
        typeCheckResult = floatType.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.success());

        Multiply doubleType = new Multiply(new DoubleLiteral(1.0), new DoubleLiteral(2.0));
        typeCheckResult = doubleType.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.success());

        Multiply decimalType = new Multiply(new DecimalLiteral(new BigDecimal("12.3")),
                new DecimalLiteral(new BigDecimal("12.3")));
        typeCheckResult = decimalType.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.success());

        Multiply charType = new Multiply(new CharLiteral("1", 1), new CharLiteral("2", 1));
        typeCheckResult = charType.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.failed());
        // this means has two type check error
        Assertions.assertEquals(4, typeCheckResult.getMessage().split(",").length);

        Multiply varchar = new Multiply(new VarcharLiteral("1", 10), new VarcharLiteral("2", 10));
        typeCheckResult = varchar.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.failed());
        // this means has two type check error
        Assertions.assertEquals(4, typeCheckResult.getMessage().split(",").length);

        Multiply string = new Multiply(new StringLiteral("1"), new StringLiteral("2"));
        typeCheckResult = string.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.failed());
        // this means has two type check error
        Assertions.assertEquals(4, typeCheckResult.getMessage().split(",").length);

        Multiply bool = new Multiply(BooleanLiteral.FALSE, BooleanLiteral.TRUE);
        typeCheckResult = bool.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.failed());
        // this means has two type check error
        Assertions.assertEquals(4, typeCheckResult.getMessage().split(",").length);

        Multiply nullType = new Multiply(NullLiteral.INSTANCE, NullLiteral.INSTANCE);
        typeCheckResult = nullType.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.failed());
        // this means has two type check error
        Assertions.assertEquals(4, typeCheckResult.getMessage().split(",").length);
    }

    @Test
    public void testSubtract() {
        TypeCheckResult typeCheckResult;

        Subtract tinyInt = new Subtract(new TinyIntLiteral((byte) 1), new TinyIntLiteral((byte) 2));
        typeCheckResult = tinyInt.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.success());

        Subtract smallInt = new Subtract(new SmallIntLiteral((short) 1), new SmallIntLiteral((short) 2));
        typeCheckResult = smallInt.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.success());

        Subtract integer = new Subtract(new IntegerLiteral(1), new IntegerLiteral(2));
        typeCheckResult = integer.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.success());

        Subtract bigint = new Subtract(new BigIntLiteral(1L), new BigIntLiteral(2L));
        typeCheckResult = bigint.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.success());

        Subtract largeInt = new Subtract(new LargeIntLiteral(new BigInteger("1")),
                new LargeIntLiteral(new BigInteger("1")));
        typeCheckResult = largeInt.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.success());

        Subtract floatType = new Subtract(new FloatLiteral(1.0F), new FloatLiteral(2.0F));
        typeCheckResult = floatType.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.success());

        Subtract doubleType = new Subtract(new DoubleLiteral(1.0), new DoubleLiteral(2.0));
        typeCheckResult = doubleType.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.success());

        Subtract decimalType = new Subtract(new DecimalLiteral(new BigDecimal("12.3")),
                new DecimalLiteral(new BigDecimal("12.3")));
        typeCheckResult = decimalType.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.success());

        Subtract charType = new Subtract(new CharLiteral("1", 1), new CharLiteral("2", 1));
        typeCheckResult = charType.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.failed());
        // this means has two type check error
        Assertions.assertEquals(4, typeCheckResult.getMessage().split(",").length);

        Subtract varchar = new Subtract(new VarcharLiteral("1", 10), new VarcharLiteral("2", 10));
        typeCheckResult = varchar.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.failed());
        // this means has two type check error
        Assertions.assertEquals(4, typeCheckResult.getMessage().split(",").length);

        Subtract string = new Subtract(new StringLiteral("1"), new StringLiteral("2"));
        typeCheckResult = string.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.failed());
        // this means has two type check error
        Assertions.assertEquals(4, typeCheckResult.getMessage().split(",").length);

        Subtract bool = new Subtract(BooleanLiteral.FALSE, BooleanLiteral.TRUE);
        typeCheckResult = bool.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.failed());
        // this means has two type check error
        Assertions.assertEquals(4, typeCheckResult.getMessage().split(",").length);

        Subtract nullType = new Subtract(NullLiteral.INSTANCE, NullLiteral.INSTANCE);
        typeCheckResult = nullType.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.failed());
        // this means has two type check error
        Assertions.assertEquals(4, typeCheckResult.getMessage().split(",").length);
    }

    @Test
    public void testMod() {
        TypeCheckResult typeCheckResult;

        Mod tinyInt = new Mod(new TinyIntLiteral((byte) 1), new TinyIntLiteral((byte) 2));
        typeCheckResult = tinyInt.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.success());

        Mod smallInt = new Mod(new SmallIntLiteral((short) 1), new SmallIntLiteral((short) 2));
        typeCheckResult = smallInt.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.success());

        Mod integer = new Mod(new IntegerLiteral(1), new IntegerLiteral(2));
        typeCheckResult = integer.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.success());

        Mod bigint = new Mod(new BigIntLiteral(1L), new BigIntLiteral(2L));
        typeCheckResult = bigint.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.success());

        Mod largeInt = new Mod(new LargeIntLiteral(new BigInteger("1")),
                new LargeIntLiteral(new BigInteger("1")));
        typeCheckResult = largeInt.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.success());

        Mod floatType = new Mod(new FloatLiteral(1.0F), new FloatLiteral(2.0F));
        typeCheckResult = floatType.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.success());

        Mod doubleType = new Mod(new DoubleLiteral(1.0), new DoubleLiteral(2.0));
        typeCheckResult = doubleType.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.success());

        Mod decimalType = new Mod(new DecimalLiteral(new BigDecimal("12.3")),
                new DecimalLiteral(new BigDecimal("12.3")));
        typeCheckResult = decimalType.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.success());

        Mod charType = new Mod(new CharLiteral("1", 1), new CharLiteral("2", 1));
        typeCheckResult = charType.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.failed());
        // this means has two type check error
        Assertions.assertEquals(4, typeCheckResult.getMessage().split(",").length);

        Mod varchar = new Mod(new VarcharLiteral("1", 10), new VarcharLiteral("2", 10));
        typeCheckResult = varchar.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.failed());
        // this means has two type check error
        Assertions.assertEquals(4, typeCheckResult.getMessage().split(",").length);

        Mod string = new Mod(new StringLiteral("1"), new StringLiteral("2"));
        typeCheckResult = string.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.failed());
        // this means has two type check error
        Assertions.assertEquals(4, typeCheckResult.getMessage().split(",").length);

        Mod bool = new Mod(BooleanLiteral.FALSE, BooleanLiteral.TRUE);
        typeCheckResult = bool.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.failed());
        // this means has two type check error
        Assertions.assertEquals(4, typeCheckResult.getMessage().split(",").length);

        Mod nullType = new Mod(NullLiteral.INSTANCE, NullLiteral.INSTANCE);
        typeCheckResult = nullType.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.failed());
        // this means has two type check error
        Assertions.assertEquals(4, typeCheckResult.getMessage().split(",").length);
    }

    @Test
    public void testAdd() {
        TypeCheckResult typeCheckResult;

        Add tinyInt = new Add(new TinyIntLiteral((byte) 1), new TinyIntLiteral((byte) 2));
        typeCheckResult = tinyInt.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.success());

        Add smallInt = new Add(new SmallIntLiteral((short) 1), new SmallIntLiteral((short) 2));
        typeCheckResult = smallInt.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.success());

        Add integer = new Add(new IntegerLiteral(1), new IntegerLiteral(2));
        typeCheckResult = integer.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.success());

        Add bigint = new Add(new BigIntLiteral(1L), new BigIntLiteral(2L));
        typeCheckResult = bigint.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.success());

        Add largeInt = new Add(new LargeIntLiteral(new BigInteger("1")),
                new LargeIntLiteral(new BigInteger("1")));
        typeCheckResult = largeInt.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.success());

        Add floatType = new Add(new FloatLiteral(1.0F), new FloatLiteral(2.0F));
        typeCheckResult = floatType.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.success());

        Add doubleType = new Add(new DoubleLiteral(1.0), new DoubleLiteral(2.0));
        typeCheckResult = doubleType.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.success());

        Add decimalType = new Add(new DecimalLiteral(new BigDecimal("12.3")),
                new DecimalLiteral(new BigDecimal("12.3")));
        typeCheckResult = decimalType.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.success());

        Add charType = new Add(new CharLiteral("1", 1), new CharLiteral("2", 1));
        typeCheckResult = charType.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.failed());
        // this means has two type check error
        Assertions.assertEquals(4, typeCheckResult.getMessage().split(",").length);

        Add varchar = new Add(new VarcharLiteral("1", 10), new VarcharLiteral("2", 10));
        typeCheckResult = varchar.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.failed());
        // this means has two type check error
        Assertions.assertEquals(4, typeCheckResult.getMessage().split(",").length);

        Add string = new Add(new StringLiteral("1"), new StringLiteral("2"));
        typeCheckResult = string.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.failed());
        // this means has two type check error
        Assertions.assertEquals(4, typeCheckResult.getMessage().split(",").length);

        Add bool = new Add(BooleanLiteral.FALSE, BooleanLiteral.TRUE);
        typeCheckResult = bool.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.failed());
        // this means has two type check error
        Assertions.assertEquals(4, typeCheckResult.getMessage().split(",").length);

        Add nullType = new Add(NullLiteral.INSTANCE, NullLiteral.INSTANCE);
        typeCheckResult = nullType.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.failed());
        // this means has two type check error
        Assertions.assertEquals(4, typeCheckResult.getMessage().split(",").length);
    }

    @Test
    public void testNot() {
        TypeCheckResult typeCheckResult;

        Not childIsNotBoolean = new Not(new IntegerLiteral(1));
        typeCheckResult = childIsNotBoolean.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.failed());

        Not childIsBoolean = new Not(BooleanLiteral.TRUE);
        typeCheckResult = childIsBoolean.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.success());
    }

    @Test
    public void testWhenClause() {
        TypeCheckResult typeCheckResult;

        WhenClause firstInputIsNotBoolean = new WhenClause(new IntegerLiteral(1), BooleanLiteral.TRUE);
        typeCheckResult = firstInputIsNotBoolean.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.failed());

        WhenClause firstInputIsBoolean = new WhenClause(BooleanLiteral.TRUE, BooleanLiteral.TRUE);
        typeCheckResult = firstInputIsBoolean.checkInputDataTypes();
        Assertions.assertTrue(typeCheckResult.success());
    }
}
