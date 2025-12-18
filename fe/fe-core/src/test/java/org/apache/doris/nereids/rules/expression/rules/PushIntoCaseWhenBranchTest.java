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

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

class PushIntoCaseWhenBranchTest extends ExpressionRewriteTestHelper {

    public PushIntoCaseWhenBranchTest() {
        setExpressionOnFilter();
    }

    @Test
    void testPushIntoCaseWhen() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(PushIntoCaseWhenBranch.INSTANCE)
        ));

        assertRewriteAfterTypeCoercion("cast(case when TA = 1 then 1 else 2 end as bigint)", "case when TA = 1 then 1 else 2 end");
        assertRewriteAfterTypeCoercion("TA > case when TB = 1 then 1 else 3 end", "TA > case when TB = 1 then 1 else 3 end");
        assertRewriteAfterTypeCoercion("2 > case when TB = 1 then 1 else 3 end", "case when TB = 1 then true else false end");
        assertRewriteAfterTypeCoercion("2 > case when TB = 1 then TC else TD end", "2 > case when TB = 1 then TC else TD end");
        assertRewriteAfterTypeCoercion("2 > case when TB = 1 then 1 else TD end", "case when TB = 1 then true else 2 > TD end");
        assertRewriteAfterTypeCoercion("2 > case when TB = 1 then TC end", "case when TB = 1 then 2 > TC else null end");
    }

    @Test
    void testPushIntoIf() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(PushIntoCaseWhenBranch.INSTANCE)
        ));

        assertRewriteAfterTypeCoercion("cast(if(TA = 1, 1, 2) as bigint)", "if(TA = 1, 1, 2)");
        assertRewriteAfterTypeCoercion("TA > if(TB = 1, 1, 3)", "TA > if(TB = 1, 1, 3)");
        assertRewriteAfterTypeCoercion("2 > if(TB = 1, 1, 3)", "if(TB = 1, true, false)");
        assertRewriteAfterTypeCoercion("10 < if(TA = 1, 1, 100) and 2 > if(TB = 1, 1, 3)",
                "if(TA = 1, false, true) and if(TB = 1, true, false)");
        assertRewriteAfterTypeCoercion("2 > if(if(TB = 1, 10, TA) > 15, 1, TC)",
                "if(if(TB = 1, false, TA > 15), true, 2 > TC)");
        assertRewriteAfterTypeCoercion("2 > if(TB = 1, TC, TD)", "2 > if(TB = 1, TC, TD)");
        assertRewriteAfterTypeCoercion("2 > if(TB = 1, 1, TD)", "if(TB = 1, true, 2 > TD)");
        assertRewriteAfterTypeCoercion("2 > if(TB = 1, TC, NULL)", "if(TB = 1, 2 > TC, null)");
    }

    @Test
    void testPushIntoNvl() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(PushIntoCaseWhenBranch.INSTANCE)
        ));
        assertRewriteAfterTypeCoercion("cast(nvl(TA, TB) as bigint)", "cast(nvl(TA, TB) as bigint)");
        assertRewriteAfterTypeCoercion("cast(nvl(TA, 1) as bigint)", "if(TA is null, 1, cast(TA as bigint))");
        assertRewriteAfterTypeCoercion("a > nvl(b, c)", "a > nvl(b, c)");
        assertRewriteAfterTypeCoercion("2 > nvl(b, c)", "2 > nvl(b, c)");
        assertRewriteAfterTypeCoercion("2 > nvl(null, c)", "if(null is null, 2 > c, null)");
        assertRewriteAfterTypeCoercion("2 > nvl(b, null)", "if(b is null, null, 2 > b)");
        assertRewriteAfterTypeCoercion("2 > nvl(a + b, null)", "if(a + b is null, null, 2 > a + b)");
        assertRewriteAfterTypeCoercion("2 > nvl(a + b + random(1, 10), null)", "2 > nvl(a + b + random(1, 10), null)");
        assertRewriteAfterTypeCoercion("2 > nvl(null, a + b + random(1, 10))", "if(null is null, 2 > a + b + random(1, 10), null)");
    }

    @Test
    void testPushIntoNullIf() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(PushIntoCaseWhenBranch.INSTANCE)
        ));
        assertRewriteAfterTypeCoercion("cast(nullif(TA, TB) as bigint)", "if(TA = TB, NULL, cast(TA as bigint))");
        assertRewriteAfterTypeCoercion("cast(nullif(TA, 1) as bigint)", "if(TA = 1, null, cast(TA as bigint))");
        assertRewriteAfterTypeCoercion("a > nullif(b, c)", "a > nullif(b, c)");
        assertRewriteAfterTypeCoercion("2 > nullif(b, c)", "if(b = c, null, 2 > b)");
        assertRewriteAfterTypeCoercion("2 > nullif(b + random(1, 10), c)", "2 > nullif(b + random(1, 10), c)");
        assertRewriteAfterTypeCoercion("2 > nullif(b, c + random(1, 10))", "if(b = c + random(1, 10), null, 2 > b)");
    }
}
