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

import org.apache.doris.nereids.rules.expression.ExpressionRewriteTestHelper;
import org.apache.doris.nereids.rules.expression.ExpressionRuleExecutor;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.VarcharType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class SimplifyCastRuleTest extends ExpressionRewriteTestHelper {

    @Test
    public void testSimplify() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(SimplifyCastRule.INSTANCE));
        assertRewriteAfterSimplify("CAST('1' AS STRING)", "'1'",
                StringType.INSTANCE);
        assertRewriteAfterSimplify("CAST('1' AS VARCHAR)", "'1'",
                VarcharType.createVarcharType(-1));
        assertRewriteAfterSimplify("CAST(1 AS DECIMAL)", "1",
                DecimalV3Type.createDecimalV3Type(9, 0));
        assertRewriteAfterSimplify("CAST(1000 AS DECIMAL)", "1000",
                DecimalV3Type.createDecimalV3Type(9, 0));
        assertRewriteAfterSimplify("CAST(1 AS DECIMALV3)", "1",
                DecimalV3Type.createDecimalV3Type(9, 0));
        assertRewriteAfterSimplify("CAST(1000 AS DECIMALV3)", "1000",
                DecimalV3Type.createDecimalV3Type(9, 0));
    }

    private void assertRewriteAfterSimplify(String expr, String expected, DataType expectedType) {
        Expression needRewriteExpression = PARSER.parseExpression(expr);
        Expression rewritten = SimplifyCastRule.INSTANCE.rewrite(needRewriteExpression, context);
        Expression expectedExpression = PARSER.parseExpression(expected);
        Assertions.assertEquals(expectedExpression.toSql(), rewritten.toSql());
        Assertions.assertEquals(expectedType, rewritten.getDataType());

    }

}
