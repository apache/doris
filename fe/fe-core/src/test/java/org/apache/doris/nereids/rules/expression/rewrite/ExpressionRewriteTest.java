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

package org.apache.doris.nereids.rules.expression.rewrite;

import org.apache.doris.catalog.Database;
import org.apache.doris.nereids.parser.SqlParser;
import org.apache.doris.nereids.rules.expression.rewrite.rules.NormalizeExpressionRule;
import org.apache.doris.nereids.rules.expression.rewrite.rules.NotExpressionRule;
import org.apache.doris.nereids.trees.expressions.Expression;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

public class ExpressionRewriteTest {
    private static final Logger LOG = LogManager.getLogger(Database.class);

    private static final SqlParser PARSER = new SqlParser();
    private ExpressionRewriter rewriter;

    @Test
    public void testNotExpressionRewrite() {
        rewriter = new ExpressionRewriter(NotExpressionRule.INSTANCE);

        assertRewrite("NOT 'X'", "NOT 'X'");
        assertRewrite("NOT NOT 'X'", "'X'");
        assertRewrite("NOT NOT NOT 'X'", "NOT 'X'");
        assertRewrite("NOT 'X' > 'Y'", "'X' <= 'Y'");
        assertRewrite("NOT 'X' < 'Y'", "'X' >= 'Y'");
        assertRewrite("NOT 'X' >= 'Y'", "'X' < 'Y'");
        assertRewrite("NOT 'X' <= 'Y'", "'X' > 'Y'");
        assertRewrite("NOT 'X' = 'Y'", "NOT 'X' = 'Y'");
        assertRewrite("NOT NOT 'X' > 'Y'", "'X' > 'Y'");
        assertRewrite("NOT NOT NOT 'X' > 'Y'", "'X' <= 'Y'");
        assertRewrite("NOT NOT NOT 'X' >  NOT NOT 'Y'", "'X' <= 'Y'");
        assertRewrite("NOT 'X' > NOT NOT 'Y'", "'X' <= 'Y'");

    }

    @Test
    public void testNormalizeExpressionRewrite() {
        rewriter = new ExpressionRewriter(NormalizeExpressionRule.INSTANCE);

        assertRewrite("2 > 'x'", "'x' < 2");
        assertRewrite("2 >= 'x'", "'x' <= 2");
        assertRewrite("2 < 'x'", "'x' > 2");
        assertRewrite("2 <= 'x'", "'x' >= 2");
        assertRewrite("2 = 'x'", "'x' = 2");
        /*
        assertRewrite("'a' = 1", "'a' = 1");
        assertRewrite("'a' = 1 and 1 = 'a'", "'a' = 1");
        assertRewrite("'a' = 1 and 'b' > 2 and 'a' = 1", "'a' = 1 and 'b' > 2");
        assertRewrite("'a' = 1 and 'a' = 1 and 'b' > 2 and 'a' = 1 and 'a' = 1", "'a' = 1 and 'b' > 2");

        assertRewrite("'a' = 1 or 'a' = 1", "'a' = 1");
        assertRewrite("'a' = 1 or 'a' = 1 or 'b' >= 1", "'a' = 1 or 'b' >= 1");
        */
    }

    private void assertRewrite(String expression, String expected) {
        Expression expectedExpression = PARSER.createExpression(expected);
        Expression needRewriteExpression = PARSER.createExpression(expression);
        Expression rewrittenExpression = rewriter.rewrite(needRewriteExpression);
        LOG.info("original expression : {} expected expression : {} rewritten expression : {}", needRewriteExpression, expectedExpression, rewrittenExpression);
        Assert.assertEquals(expectedExpression, rewrittenExpression);
    }

}
