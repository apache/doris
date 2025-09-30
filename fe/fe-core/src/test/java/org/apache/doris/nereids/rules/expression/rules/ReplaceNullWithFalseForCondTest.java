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

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

class ReplaceNullWithFalseForCondTest extends ExpressionRewriteTestHelper {

    private final ReplaceNullWithFalseForCond replaceCaseThenInstance = new ReplaceNullWithFalseForCond() {
        @Override
        protected Expression rewrite(Expression expression) {
            return replace(expression, true);
        }
    };

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

        assertRewriteAfterTypeCoercion(sql, expectedSql);

        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(replaceCaseThenInstance)
        ));

        expectedSql = "case when false then false"
                + " when false and a = 1 and not(null) or "
                + " (case when a = 2 and false then false "
                + "       when false then not(null) "
                + "       else false or a=3"
                + "  end) "
                + " then (case when false then false else false end) "
                + " else false end";

        assertRewriteAfterTypeCoercion(sql, expectedSql);
    }

    @Test
    void testIf() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(ReplaceNullWithFalseForCond.INSTANCE)
        ));

        String sql = "if("
                + " null and not(null) and if(null and not(null), null and true, null),"
                + " null and not(null),"
                + " if(a = 1 and null, null, null)"
                + ")";

        String expectedSql = "if("
                + " false and not(null) and if(false and not(null), false and true, false),"
                + " null and not(null),"
                + " if(a = 1 and false, null, null)"
                + ")";

        assertRewrite(sql, expectedSql);

        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(replaceCaseThenInstance, SimplifyCastRule.INSTANCE)
        ));

        expectedSql = "if("
                + " false and not(null) and if(false and not(null), false and true, false),"
                + " false and not(null),"
                + " if(a = 1 and false, false, false)"
                + ")";

        assertRewriteAfterTypeCoercion(sql, expectedSql);
    }
}
