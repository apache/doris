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

class CondReplaceNullWithFalseTest extends ExpressionRewriteTestHelper {

    @Test
    void testInsideCondition() {
        LogicalFilter<?> filter = new LogicalFilter<LogicalEmptyRelation>(ImmutableSet.of(),
                new LogicalEmptyRelation(new RelationId(1), ImmutableList.of()));
        context = new ExpressionRewriteContext(filter, cascadesContext);
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(CondReplaceNullWithFalse.INSTANCE)
        ));

        assertRewriteAfterTypeCoercion("null", "false");
        assertRewriteAfterTypeCoercion("not(null)", "not(null)");
        assertRewriteAfterTypeCoercion("null and true", "false and true");
        assertRewriteAfterTypeCoercion("null or true", "false or true");
        assertRewriteAfterTypeCoercion("case when null and true then null and true else null end",
                "case when false and true then false and true else false end");
        assertRewriteAfterTypeCoercion("if(null and true, null and true, null and true)",
                "if(false and true, false and true, false and true)");
        assertRewriteAfterTypeCoercion("not(case when null and true then null and true else null end)",
                "not(case when false and true then null and true else null end)");
        assertRewriteAfterTypeCoercion("not(if(null and true, null and true, null and true))",
                "not(if(false and true, null and true, null and true))");

        assertRewriteAfterTypeCoercion(
                "case when null then null"
                        + " when null and a = 1 and not(null) or "
                        + " (case when a = 2 and null then null "
                        + "       when null then not(null) "
                        + "       else null or a=3"
                        + "  end) "
                        + " then (case when null then null else null end) "
                        + " else null end",

                "case when false then false"
                        + " when false and a = 1 and not(null) or "
                        + " (case when a = 2 and false then false "
                        + "       when false then not(null) "
                        + "       else false or a=3"
                        + "  end) "
                        + " then (case when false then false else false end) "
                        + " else false end"
        );

        assertRewriteAfterTypeCoercion(
                "if("
                        + " null and not(null) and if(null and not(null), null and true, null),"
                        + " null and not(null),"
                        + " if(a = 1 and null, null and true, null)"
                        + ")",

                "if("
                        + " false and not(null) and if(false and not(null), false and true, false),"
                        + " false and not(null),"
                        + " if(a = 1 and false, false and true, false)"
                        + ")"
        );
    }

    @Test
    void testNotInCondition() {
        context = new ExpressionRewriteContext(cascadesContext);
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(CondReplaceNullWithFalse.INSTANCE)
        ));

        assertRewriteAfterTypeCoercion("null", "null");
        assertRewriteAfterTypeCoercion("not(null)", "not(null)");
        assertRewriteAfterTypeCoercion("null and true", "null and true");
        assertRewriteAfterTypeCoercion("null or true", "null or true");
        assertRewriteAfterTypeCoercion("case when null and true then null and true else null end",
                "case when false and true then null and true else null end");
        assertRewriteAfterTypeCoercion("if(null and true, null and true, null and true)",
                "if(false and true, null and true, null and true)");
        assertRewriteAfterTypeCoercion("not(case when null and true then null and true else null end)",
                "not(case when false and true then null and true else null end)");
        assertRewriteAfterTypeCoercion("not(if(null and true, null and true, null and true))",
                "not(if(false and true, null and true, null and true))");
        assertRewriteAfterTypeCoercion("case when null and true then true and null end", "case when false and true then true and null end");

        assertRewriteAfterTypeCoercion(
                "case when null then null"
                        + " when null and a = 1 and not(null) or "
                        + " (case when a = 2 and null then null "
                        + "       when null then not(null) "
                        + "       else null or a=3"
                        + "  end) "
                        + " then (case when null then null else null end) "
                        + " else null end",

                "case when false then null"
                        + " when false and a = 1 and not(null) or "
                        + " (case when a = 2 and false then false "
                        + "       when false then not(null) "
                        + "       else false or a=3"
                        + "  end) "
                        + " then (case when false then null else null end) "
                        + " else null end"
        );

        assertRewriteAfterTypeCoercion(
                "if("
                        + " null and not(null) and if(null and not(null), null and true, null),"
                        + " null and not(null),"
                        + " if(a = 1 and null, null, null)"
                        + ")",

                "if("
                        + " false and not(null) and if(false and not(null), false and true, false),"
                        + " null and not(null),"
                        + " if(a = 1 and false, null, null)"
                        + ")"
        );
    }
}
