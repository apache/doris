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

import org.apache.doris.nereids.rules.expression.ExpressionNormalization;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteTestHelper;
import org.apache.doris.nereids.rules.expression.ExpressionRuleExecutor;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Round;
import org.apache.doris.nereids.trees.expressions.literal.DecimalLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DoubleLiteral;
import org.apache.doris.nereids.trees.expressions.literal.FloatLiteral;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

public class CastDoubleToIntRewriteTest extends ExpressionRewriteTestHelper {
    @Test
    public void testRewrite() {
        ConnectContext context = new ConnectContext();
        context.getSessionVariable().setSqlDialect("presto");
        context.setThreadLocalInfo();
        executor = new ExpressionRuleExecutor(ExpressionNormalization.NORMALIZE_REWRITE_RULES);
        Assertions.assertEquals(1, new CastDoubleToIntRewrite().buildRules().size());
        assertRewriteAfterCastDoubleToInt(new Cast(new DoubleLiteral(1.0), IntegerType.INSTANCE),
                "1", IntegerType.INSTANCE);
        assertRewriteAfterCastDoubleToInt(new Cast(new DecimalLiteral(new BigDecimal("1.6")), BigIntType.INSTANCE),
                "1", BigIntType.INSTANCE);
        assertRewriteAfterCastDoubleToInt(new Cast(new FloatLiteral(1.2f), IntegerType.INSTANCE),
                "1", IntegerType.INSTANCE);
        assertRewriteAfterCastDoubleToInt(new Cast(new Round(new DecimalLiteral(new BigDecimal("1.5"))),
                IntegerType.INSTANCE), "cast(round(1.5) as INT)", IntegerType.INSTANCE);
    }

    private void assertRewriteAfterCastDoubleToInt(Expression needRewriteExpression, String expected,
                                                       DataType expectedType) {
        Expression rewritten = executor.rewrite(needRewriteExpression, context);
        Expression expectedExpression = PARSER.parseExpression(expected);
        Assertions.assertEquals(expectedExpression.toSql(), rewritten.toSql());
        Assertions.assertEquals(expectedType, rewritten.getDataType());
    }
}
