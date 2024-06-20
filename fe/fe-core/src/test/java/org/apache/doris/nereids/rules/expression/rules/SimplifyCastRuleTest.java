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

package org.apache.doris.nereids.rules.expression.rules;

import org.apache.doris.nereids.rules.expression.ExpressionRewrite;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteTestHelper;
import org.apache.doris.nereids.rules.expression.ExpressionRuleExecutor;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.CharLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DecimalLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DecimalV3Literal;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.SmallIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DecimalV2Type;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.SmallIntType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.TinyIntType;
import org.apache.doris.nereids.types.VarcharType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

class SimplifyCastRuleTest extends ExpressionRewriteTestHelper {

    @Test
    public void testSimplify() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                ExpressionRewrite.bottomUp(SimplifyCastRule.INSTANCE))
        );
        assertRewriteAfterSimplify("CAST('1' AS STRING)", "'1'", StringType.INSTANCE);
        assertRewriteAfterSimplify("CAST('1' AS VARCHAR)", "'1'",
                VarcharType.createVarcharType(-1));
        assertRewriteAfterSimplify("CAST(1 AS DECIMAL)", "1.000000000",
                DecimalV3Type.createDecimalV3Type(38, 9));
        assertRewriteAfterSimplify("CAST(1000 AS DECIMAL)", "1000.000000000",
                DecimalV3Type.createDecimalV3Type(38, 9));
        assertRewriteAfterSimplify("CAST(1 AS DECIMALV3)", "1",
                DecimalV3Type.createDecimalV3Type(9, 0));
        assertRewriteAfterSimplify("CAST(1000 AS DECIMALV3)", "1000",
                DecimalV3Type.createDecimalV3Type(9, 0));

        Expression tinyIntLiteral = new TinyIntLiteral((byte) 12);
        // cast tinyint as tinyint
        assertRewrite(new Cast(tinyIntLiteral, TinyIntType.INSTANCE), tinyIntLiteral);
        // cast tinyint as decimalv2(3,0)
        assertRewrite(new Cast(tinyIntLiteral, DecimalV2Type.forType(TinyIntType.INSTANCE)),
                new DecimalLiteral(DecimalV2Type.forType(TinyIntType.INSTANCE), new BigDecimal(12)));

        assertRewrite(new Cast(tinyIntLiteral, DecimalV2Type.createDecimalV2Type(5, 1)),
                new DecimalLiteral(DecimalV2Type.createDecimalV2Type(5, 1),
                        new BigDecimal("12.0")));

        // cast tinyint as decimalv3(3,0)
        assertRewrite(new Cast(tinyIntLiteral, DecimalV3Type.forType(TinyIntType.INSTANCE)),
                new DecimalV3Literal(DecimalV3Type.forType(TinyIntType.INSTANCE), new BigDecimal(12)));
        // cast tinyint as decimalv3(5,1)
        assertRewrite(new Cast(tinyIntLiteral, DecimalV3Type.createDecimalV3Type(5, 1)),
                new DecimalV3Literal(DecimalV3Type.createDecimalV3Type(5, 1),
                        new BigDecimal("12.0")));

        // cast tinyint as decimalv3(2,1)
        assertRewrite(new Cast(tinyIntLiteral, DecimalV3Type.createDecimalV3Type(2, 1)),
                new Cast(tinyIntLiteral, DecimalV3Type.createDecimalV3Type(2, 1)));

        // cast tinyint as decimalv2(2,1)
        assertRewrite(new Cast(tinyIntLiteral, DecimalV2Type.createDecimalV2Type(2, 1)),
                new Cast(tinyIntLiteral, DecimalV2Type.createDecimalV2Type(2, 1)));

        Expression smallIntLiteral = new SmallIntLiteral((short) 30000);
        // cast smallint as smallint
        assertRewrite(new Cast(smallIntLiteral, SmallIntType.INSTANCE), smallIntLiteral);
        // cast smallint as decimalv2
        assertRewrite(new Cast(smallIntLiteral, DecimalV2Type.forType(SmallIntType.INSTANCE)),
                new DecimalLiteral(new BigDecimal(30000)));
        // cast smallint as decimalv3
        assertRewrite(new Cast(smallIntLiteral, DecimalV3Type.forType(SmallIntType.INSTANCE)),
                new DecimalV3Literal(new BigDecimal(30000)));

        Expression intLiteral = new IntegerLiteral(30000000);
        // cast int as int
        assertRewrite(new Cast(intLiteral, IntegerType.INSTANCE), intLiteral);
        // cast int as decimalv2
        assertRewrite(new Cast(intLiteral, DecimalV2Type.forType(IntegerType.INSTANCE)),
                new DecimalLiteral(DecimalV2Type.forType(IntegerType.INSTANCE), new BigDecimal(30000000)));
        // cast int as decimalv3
        assertRewrite(new Cast(intLiteral, DecimalV3Type.forType(IntegerType.INSTANCE)),
                new DecimalV3Literal(DecimalV3Type.forType(IntegerType.INSTANCE), new BigDecimal(30000000)));

        Expression bigIntLiteral = new BigIntLiteral(30000000000L);
        // cast bigint as bigint
        assertRewrite(new Cast(bigIntLiteral, BigIntType.INSTANCE), bigIntLiteral);
        // cast bigint as decimalv2
        assertRewrite(new Cast(bigIntLiteral, DecimalV2Type.forType(BigIntType.INSTANCE)),
                new DecimalLiteral(DecimalV2Type.forType(BigIntType.INSTANCE), new BigDecimal(30000000000L)));
        // cast bigint as decimalv3
        assertRewrite(new Cast(bigIntLiteral, DecimalV3Type.forType(BigIntType.INSTANCE)),
                new DecimalV3Literal(DecimalV3Type.forType(BigIntType.INSTANCE), new BigDecimal(30000000000L)));

        Expression varcharLiteral = new VarcharLiteral("12345");
        // cast varchar(5) as varchar(3)
        assertRewrite(new Cast(varcharLiteral, VarcharType.createVarcharType(3)),
                new VarcharLiteral("123"));
        // cast varchar(5) as varchar(10)
        assertRewrite(new Cast(varcharLiteral, VarcharType.createVarcharType(10)),
                new VarcharLiteral("12345"));
        // cast varchar(5) as string
        assertRewrite(new Cast(varcharLiteral, StringType.INSTANCE), new StringLiteral("12345"));

        Expression charLiteral = new CharLiteral("12345", 5);
        // cast char(5) as varchar(3)
        assertRewrite(new Cast(charLiteral, VarcharType.createVarcharType(3)),
                new VarcharLiteral("123"));
        // cast char(5) as varchar(10)
        assertRewrite(new Cast(charLiteral, VarcharType.createVarcharType(10)),
                new VarcharLiteral("12345"));
        // cast char(5) as string
        assertRewrite(new Cast(charLiteral, StringType.INSTANCE), new StringLiteral("12345"));

        Expression decimalV3Literal = new DecimalV3Literal(DecimalV3Type.createDecimalV3Type(3, 1),
                new BigDecimal("12.0"));
        // cast decimalv3(3,1) as decimalv3(5,1)
        assertRewrite(new Cast(decimalV3Literal, DecimalV3Type.createDecimalV3Type(5, 1)),
                new DecimalV3Literal(DecimalV3Type.createDecimalV3Type(5, 1),
                        new BigDecimal("12.0")));

        assertRewrite(new Cast(decimalV3Literal, DecimalV3Type.createDecimalV3Type(2, 1)),
                new Cast(decimalV3Literal, DecimalV3Type.createDecimalV3Type(2, 1)));

        // TODO unsupported but should?
        // cast tinyint as smallint
        assertRewrite(new Cast(tinyIntLiteral, SmallIntType.INSTANCE),
                new Cast(tinyIntLiteral, SmallIntType.INSTANCE));
        // cast tinyint as int
        assertRewrite(new Cast(tinyIntLiteral, IntegerType.INSTANCE),
                new Cast(tinyIntLiteral, IntegerType.INSTANCE));
        // cast tinyint as bigint
        assertRewrite(new Cast(tinyIntLiteral, BigIntType.INSTANCE),
                new Cast(tinyIntLiteral, BigIntType.INSTANCE));

        // unsupported
        // cast bigint as int
        assertRewrite(new Cast(bigIntLiteral, IntegerType.INSTANCE),
                new Cast(bigIntLiteral, IntegerType.INSTANCE));
        // cast bigint as smallint
        assertRewrite(new Cast(bigIntLiteral, SmallIntType.INSTANCE),
                new Cast(bigIntLiteral, SmallIntType.INSTANCE));
        // cast bigint as tinyint
        assertRewrite(new Cast(bigIntLiteral, TinyIntType.INSTANCE),
                new Cast(bigIntLiteral, TinyIntType.INSTANCE));

        // nested cast
        assertRewrite(
                new Cast(
                        new Cast(
                                new Cast(new Cast(tinyIntLiteral, TinyIntType.INSTANCE),
                                        DecimalV3Type.createDecimalV3Type(5, 1)),
                                DecimalV3Type.createDecimalV3Type(6, 1)),
                        DecimalV3Type.createDecimalV3Type(6, 1)),
                new DecimalV3Literal(DecimalV3Type.createDecimalV3Type(6, 1),
                        new BigDecimal("12.0")));
    }

    private void assertRewriteAfterSimplify(String expr, String expected, DataType expectedType) {
        Expression needRewriteExpression = PARSER.parseExpression(expr);
        Expression rewritten = executor.rewrite(needRewriteExpression, context);
        Expression expectedExpression = PARSER.parseExpression(expected);
        Assertions.assertEquals(expectedExpression.toSql(), rewritten.toSql());
        Assertions.assertEquals(expectedType, rewritten.getDataType());
    }

}
