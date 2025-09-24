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

import org.apache.doris.nereids.analyzer.Scope;
import org.apache.doris.nereids.rules.analysis.ExpressionAnalyzer;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteTestHelper;
import org.apache.doris.nereids.rules.expression.ExpressionRuleExecutor;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import java.util.function.Function;

class ReplaceNullWithFalseForCondTest extends ExpressionRewriteTestHelper {

    @Test
    void testCaseWhen() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(ReplaceNullWithFalseForCond.INSTANCE)
        ));

        String sql = "case when null then null"
                + " when null and a = 1 and not(null) or "
                + " (case when a = 2 and null then null "
                + "       when null then not(null) "
                + "       else null or a=3"
                + "  end) "
                + " then (case when null then null else null end) "
                + " else null end";

        String expectedSql = "case when false then null"
                + " when false and a = 1 and not(null) or "
                + " (case when a = 2 and false then false "
                + "       when false then not(null) "
                + "       else false or a=3"
                + "  end) "
                + " then (case when false then null else null end) "
                + " else null end";

        assertRewrite(sql, expectedSql);

        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(new ReplaceNullWithFalseForCond(true))
        ));

        expectedSql = "case when false then false"
                + " when false and a = 1 and not(null) or "
                + " (case when a = 2 and false then false "
                + "       when false then not(null) "
                + "       else false or a=3"
                + "  end) "
                + " then (case when false then false else false end) "
                + " else false end";

        assertRewrite(sql, expectedSql);
    }

    @Test
    void testIf() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(ReplaceNullWithFalseForCond.INSTANCE)
        ));

        String sql = "if("
                + " null and not(null) and if(null and not(null), null, null),"
                + " null and not(null),"
                + " if(a = 1 and null, null, null)"
                + ")";

        String expectedSql = "if("
                + " false and not(null) and if(false and not(null), false, false),"
                + " null and not(null),"
                + " if(a = 1 and false, null, null)"
                + ")";

        assertRewrite(sql, expectedSql);

        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(new ReplaceNullWithFalseForCond(true))
        ));

        expectedSql = "if("
                + " false and not(null) and if(false and not(null), false, false),"
                + " false and not(null),"
                + " cast(if(a = 1 and false, false, false) as boolean)"
                + ")";

        assertRewrite(sql, expectedSql);
    }

    @Override
    protected void assertRewrite(String sql, String expectedSql) {
        Function<Expression, Expression> converter = expr -> new ExpressionAnalyzer(
                null, new Scope(ImmutableList.of()), null, false, false
        ) {
            // ExpressionAnalyzer will rewrite 'false and xxx' to 'false', but we want to keep the structure of the expression,
            @Override
            public Expression visitAnd(And and, ExpressionRewriteContext context) {
                return new And(
                        ExpressionUtils.extractConjunction(and)
                                .stream()
                                .map(e -> e.accept(this, context))
                                .collect(ImmutableList.toImmutableList()));
            }

            @Override
            public Expression visitOr(Or or, ExpressionRewriteContext context) {
                return new Or(
                        ExpressionUtils.extractDisjunction(or)
                                .stream()
                                .map(e -> e.accept(this, context))
                                .collect(ImmutableList.toImmutableList()));
            }
        }.analyze(expr, null);

        assertRewriteAfterConvert(sql, expectedSql, converter);
    }
}
