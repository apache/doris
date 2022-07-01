// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the notICE file
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

package org.apache.doris.nereids.rules.expression.rewrite;

import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.rules.expression.rewrite.rules.NormalizeExpressionRule;
import org.apache.doris.nereids.rules.expression.rewrite.rules.SimplifyNotExprRule;
import org.apache.doris.nereids.trees.expressions.Expression;

import org.junit.Assert;
import org.junit.Test;

/**
 * all expr rewrite rule test case.
 */
public class ExpressionRewriteTest {
    private static final NereidsParser PARSER = new NereidsParser();
    private ExpressionRuleExecutor executor;

    @Test
    public void testNotRewrite() {
        executor = new ExpressionRuleExecutor(SimplifyNotExprRule.INSTANCE);

        assertRewrite("not x > y", "x <= y");
        assertRewrite("not x < y", "x >= y");
        assertRewrite("not x >= y", "x < y");
        assertRewrite("not x <= y", "x > y");
        assertRewrite("not x = y", "not x = y");
        assertRewrite("not not x > y", "x > y");
        assertRewrite("not not not x > y", "x <= y");
    }

    @Test
    public void testNormalizeExpressionRewrite() {
        executor = new ExpressionRuleExecutor(NormalizeExpressionRule.INSTANCE);

        assertRewrite("2 > x", "x < 2");
        assertRewrite("2 >= x", "x <= 2");
        assertRewrite("2 < x", "x > 2");
        assertRewrite("2 <= x", "x >= 2");
        assertRewrite("2 = x", "x = 2");
    }

    private void assertRewrite(String expression, String expected) {
        Expression needRewriteExpression = PARSER.parseExpression(expression);
        Expression expectedExpression = PARSER.parseExpression(expected);
        Expression rewrittenExpression = executor.rewrite(needRewriteExpression);
        Assert.assertEquals(expectedExpression, rewrittenExpression);
    }
}
