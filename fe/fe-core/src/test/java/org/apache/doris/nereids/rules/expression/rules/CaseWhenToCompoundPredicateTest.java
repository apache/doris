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

import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteTestHelper;
import org.apache.doris.nereids.rules.expression.ExpressionRuleExecutor;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;

class CaseWhenToCompoundPredicateTest extends ExpressionRewriteTestHelper {

    @Test
    void testCaseWhen() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(
                        CaseWhenToCompoundPredicate.INSTANCE
                )
        ));
        assertRewriteAfterTypeCoercion("case when a = 1 then true end", "(a = 1 <=> TRUE) or null");
        assertRewriteAfterTypeCoercion("case when a = 1 then true else null end", "(a = 1 <=> TRUE) or null");
        assertRewriteAfterTypeCoercion("case when a = 1 then true else false end", "(a = 1 <=> TRUE) or false");
        assertRewriteAfterTypeCoercion("case when a = 1 then true else true end", "(a = 1 <=> TRUE) or true");
        assertRewriteAfterTypeCoercion("case when a = 1 then true else b = 1 end", "(a = 1 <=> TRUE) or b = 1");
        assertRewriteAfterTypeCoercion("case when a = 1 then true when b = 1 then true when c = 1 then true end",
                "(a = 1 <=> TRUE) or (b = 1 <=> TRUE) or (c = 1 <=> TRUE) or null");
        assertRewriteAfterTypeCoercion("case when a = 1 then false when b = 1 then false when c = 1 then false end",
                "not(a = 1 <=> TRUE) and not (b = 1 <=> TRUE) and not(c = 1 <=> TRUE) and null");
        assertRewriteAfterTypeCoercion("case when a = 1 then true when b = 1 then false when c = 1 then true end",
                "(a = 1 <=> TRUE) or (not (b = 1 <=> TRUE) and ((c = 1 <=> TRUE) or null))");
    }

    @Test
    void testIf() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(
                        CaseWhenToCompoundPredicate.INSTANCE
                )
        ));

        assertRewriteAfterTypeCoercion("if(a = 1, true, a > b)", "(a = 1 <=> TRUE) or a > b");
        assertRewriteAfterTypeCoercion("if(a = 1, false, a > b)", "not (a = 1 <=> TRUE) and a > b");
        assertRewriteAfterTypeCoercion("if(a = 1, b = 1, true)", "if(a = 1, b = 1, true)");
        assertRewriteAfterTypeCoercion("if(a = 1, b = 1, false)", "if(a = 1, b = 1, false)");
        assertRewriteAfterTypeCoercion("if(a = 1, b = 1, null)", "if(a = 1, b = 1, null)");
    }

    @Test
    void testIfInCond() {
        LogicalFilter<?> filter = new LogicalFilter<LogicalEmptyRelation>(ImmutableSet.of(),
                new LogicalEmptyRelation(new RelationId(1), ImmutableList.of()));
        ExpressionRewriteContext oldContext = context;
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(
                        CaseWhenToCompoundPredicate.INSTANCE
                )
        ));
        try {
            context = new ExpressionRewriteContext(filter, cascadesContext);
            assertRewriteAfterTypeCoercion("if(a = 1, b = 1, true)", "not((a = 1) <=> true) or b = 1");
            assertRewriteAfterTypeCoercion("if(a = 1, b = 1, false)", "a = 1 and b = 1");
            assertRewriteAfterTypeCoercion("if(a = 1, b = 1, null)", "a = 1 and b = 1");
        } finally {
            context = oldContext;
        }
    }
}
