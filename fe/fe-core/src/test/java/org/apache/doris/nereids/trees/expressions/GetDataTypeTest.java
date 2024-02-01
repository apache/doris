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

import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.CharLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral;
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
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.LargeIntType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;

public class GetDataTypeTest {

    NullLiteral nullLiteral = NullLiteral.INSTANCE;
    BooleanLiteral booleanLiteral = BooleanLiteral.FALSE;
    TinyIntLiteral tinyIntLiteral = new TinyIntLiteral((byte) 1);
    SmallIntLiteral smallIntLiteral = new SmallIntLiteral((short) 1);
    IntegerLiteral integerLiteral = new IntegerLiteral(1);
    BigIntLiteral bigIntLiteral = new BigIntLiteral(1L);
    LargeIntLiteral largeIntLiteral = new LargeIntLiteral(BigInteger.valueOf(1L));
    FloatLiteral floatLiteral = new FloatLiteral(1.0F);
    DoubleLiteral doubleLiteral = new DoubleLiteral(1.0);
    DecimalLiteral decimalLiteral = new DecimalLiteral(BigDecimal.ONE);
    CharLiteral charLiteral = new CharLiteral("hello", 5);
    VarcharLiteral varcharLiteral = new VarcharLiteral("hello", 5);
    StringLiteral stringLiteral = new StringLiteral("hello");
    DateLiteral dateLiteral = new DateLiteral(2022, 2, 2);
    DateTimeLiteral dateTimeLiteral = new DateTimeLiteral(2022, 2, 2, 2, 2, 2);

    @Test
    public void testSum() {
        Assertions.assertEquals(BigIntType.INSTANCE, checkAndGetDataType(new Sum(nullLiteral)));
        Assertions.assertEquals(BigIntType.INSTANCE, checkAndGetDataType(new Sum(booleanLiteral)));
        Assertions.assertEquals(BigIntType.INSTANCE, checkAndGetDataType(new Sum(tinyIntLiteral)));
        Assertions.assertEquals(BigIntType.INSTANCE, checkAndGetDataType(new Sum(smallIntLiteral)));
        Assertions.assertEquals(BigIntType.INSTANCE, checkAndGetDataType(new Sum(integerLiteral)));
        Assertions.assertEquals(BigIntType.INSTANCE, checkAndGetDataType(new Sum(bigIntLiteral)));
        Assertions.assertEquals(LargeIntType.INSTANCE, checkAndGetDataType(new Sum(largeIntLiteral)));
        Assertions.assertEquals(DoubleType.INSTANCE, checkAndGetDataType(new Sum(floatLiteral)));
        Assertions.assertEquals(DoubleType.INSTANCE, checkAndGetDataType(new Sum(doubleLiteral)));
        Assertions.assertEquals(DecimalV3Type.createDecimalV3Type(38, 0), checkAndGetDataType(new Sum(decimalLiteral)));
        Assertions.assertEquals(BigIntType.INSTANCE, checkAndGetDataType(new Sum(bigIntLiteral)));
        Assertions.assertThrows(RuntimeException.class, () -> checkAndGetDataType(new Sum(charLiteral)));
        Assertions.assertThrows(RuntimeException.class, () -> checkAndGetDataType(new Sum(varcharLiteral)));
        Assertions.assertThrows(RuntimeException.class, () -> checkAndGetDataType(new Sum(stringLiteral)));
        Assertions.assertThrows(RuntimeException.class, () -> checkAndGetDataType(new Sum(dateLiteral)));
        Assertions.assertThrows(RuntimeException.class, () -> checkAndGetDataType(new Sum(dateTimeLiteral)));
    }

    private DataType checkAndGetDataType(Expression expression) {
        expression.checkLegalityBeforeTypeCoercion();
        expression.checkLegalityAfterRewrite();
        return expression.getDataType();
    }
}
