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

import org.apache.doris.nereids.exceptions.CastException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.DecimalV2Type;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.FloatType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.LargeIntType;
import org.apache.doris.nereids.types.SmallIntType;
import org.apache.doris.nereids.types.TinyIntType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class BooleanLiteralTest {

    @Test
    public void testUncheckedCast() {
        BooleanLiteral aTrue = BooleanLiteral.TRUE;
        BooleanLiteral aFalse = BooleanLiteral.FALSE;
        Assertions.assertSame(aTrue.uncheckedCastTo(BooleanType.INSTANCE), aTrue);
        Assertions.assertSame(aFalse.uncheckedCastTo(BooleanType.INSTANCE), aFalse);

        Expression expression = aTrue.uncheckedCastTo(FloatType.INSTANCE);
        Assertions.assertInstanceOf(FloatLiteral.class, expression);
        Assertions.assertEquals(1, ((FloatLiteral) expression).getValue());
        expression = aFalse.uncheckedCastTo(FloatType.INSTANCE);
        Assertions.assertInstanceOf(FloatLiteral.class, expression);
        Assertions.assertEquals(0, ((FloatLiteral) expression).getValue());

        expression = aTrue.uncheckedCastTo(DoubleType.INSTANCE);
        Assertions.assertInstanceOf(DoubleLiteral.class, expression);
        Assertions.assertEquals(1, ((DoubleLiteral) expression).getValue());
        expression = aFalse.uncheckedCastTo(DoubleType.INSTANCE);
        Assertions.assertInstanceOf(DoubleLiteral.class, expression);
        Assertions.assertEquals(0, ((DoubleLiteral) expression).getValue());

        expression = aTrue.uncheckedCastTo(DecimalV2Type.SYSTEM_DEFAULT);
        Assertions.assertInstanceOf(DecimalLiteral.class, expression);
        Assertions.assertEquals(1, ((DecimalLiteral) expression).getValue().intValue());
        expression = aFalse.uncheckedCastTo(DecimalV2Type.SYSTEM_DEFAULT);
        Assertions.assertInstanceOf(DecimalLiteral.class, expression);
        Assertions.assertEquals(0, ((DecimalLiteral) expression).getValue().intValue());

        expression = aTrue.uncheckedCastTo(DecimalV3Type.SYSTEM_DEFAULT);
        Assertions.assertInstanceOf(DecimalV3Literal.class, expression);
        Assertions.assertEquals(1, ((DecimalV3Literal) expression).getValue().intValue());
        expression = aFalse.uncheckedCastTo(DecimalV3Type.SYSTEM_DEFAULT);
        Assertions.assertInstanceOf(DecimalV3Literal.class, expression);
        Assertions.assertEquals(0, ((DecimalV3Literal) expression).getValue().intValue());

        Assertions.assertThrows(CastException.class, () -> aTrue.uncheckedCastTo(DecimalV2Type.createDecimalV2Type(1, 1)));
        Assertions.assertThrows(CastException.class, () -> aTrue.uncheckedCastTo(DecimalV3Type.createDecimalV3Type(2, 2)));

        expression = aTrue.uncheckedCastTo(TinyIntType.INSTANCE);
        Assertions.assertInstanceOf(TinyIntLiteral.class, expression);
        Assertions.assertEquals(1, ((TinyIntLiteral) expression).getValue().intValue());
        expression = aFalse.uncheckedCastTo(TinyIntType.INSTANCE);
        Assertions.assertInstanceOf(TinyIntLiteral.class, expression);
        Assertions.assertEquals(0, ((TinyIntLiteral) expression).getValue().intValue());

        expression = aTrue.uncheckedCastTo(SmallIntType.INSTANCE);
        Assertions.assertInstanceOf(SmallIntLiteral.class, expression);
        Assertions.assertEquals(1, ((SmallIntLiteral) expression).getValue().intValue());
        expression = aFalse.uncheckedCastTo(SmallIntType.INSTANCE);
        Assertions.assertInstanceOf(SmallIntLiteral.class, expression);
        Assertions.assertEquals(0, ((SmallIntLiteral) expression).getValue().intValue());

        expression = aTrue.uncheckedCastTo(IntegerType.INSTANCE);
        Assertions.assertInstanceOf(IntegerLiteral.class, expression);
        Assertions.assertEquals(1, ((IntegerLiteral) expression).getValue().intValue());
        expression = aFalse.uncheckedCastTo(IntegerType.INSTANCE);
        Assertions.assertInstanceOf(IntegerLiteral.class, expression);
        Assertions.assertEquals(0, ((IntegerLiteral) expression).getValue().intValue());

        expression = aTrue.uncheckedCastTo(BigIntType.INSTANCE);
        Assertions.assertInstanceOf(BigIntLiteral.class, expression);
        Assertions.assertEquals(1, ((BigIntLiteral) expression).getValue().longValue());
        expression = aFalse.uncheckedCastTo(BigIntType.INSTANCE);
        Assertions.assertInstanceOf(BigIntLiteral.class, expression);
        Assertions.assertEquals(0, ((BigIntLiteral) expression).getValue().longValue());

        expression = aTrue.uncheckedCastTo(LargeIntType.INSTANCE);
        Assertions.assertInstanceOf(LargeIntLiteral.class, expression);
        Assertions.assertEquals(1, ((LargeIntLiteral) expression).getValue().intValue());
        expression = aFalse.uncheckedCastTo(LargeIntType.INSTANCE);
        Assertions.assertInstanceOf(LargeIntLiteral.class, expression);
        Assertions.assertEquals(0, ((LargeIntLiteral) expression).getValue().intValue());
    }
}
