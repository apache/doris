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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.analyzer.UnboundVariable;
import org.apache.doris.nereids.analyzer.UnboundVariable.VariableType;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Variable;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

/** Tests for user variable handling in expression analysis. */
public class UserVariableAnalysisTest {

    @Test
    public void testUserVarIntegerTypeSmall() {
        ConnectContext ctx = MemoTestUtils.createConnectContext();
        // set user var @a = 1 (small int)
        ctx.setUserVar("a", new IntLiteral(1L));

        // get nereids literal via ConnectContext helper
        Literal l = ConnectContext.get().getLiteralForUserVar("a");
        Assertions.assertNotNull(l);
        Assertions.assertEquals(IntegerType.INSTANCE, l.getDataType());
    }

    @Test
    public void testUserVarIntegerTypeBig() {
        ConnectContext ctx = MemoTestUtils.createConnectContext();
        // set user var @b = Long.MAX_VALUE (bigint)
        ctx.setUserVar("b", new IntLiteral(Long.MAX_VALUE));

        Literal l = ConnectContext.get().getLiteralForUserVar("b");
        Assertions.assertNotNull(l);
        Assertions.assertEquals(BigIntType.INSTANCE, l.getDataType());
    }

    @Test
    public void testVariableRecordedAndTypeCoercion() {
        // create context and cascades context (sql cache enabled by default in test utils)
        ConnectContext ctx = MemoTestUtils.createConnectContext();
        ctx.setUserVar("uvar", new IntLiteral(42L));

        // create a comparison using an unbound user variable: @uvar = 1
        UnboundVariable unboundVar = new UnboundVariable(
                "uvar", VariableType.USER);
        IntegerLiteral right = new IntegerLiteral(1);
        ComparisonPredicate cmp = new EqualTo(unboundVar, right);

        // prepare analyzer with cascades context so sql cache context exists
        CascadesContext cascadesContext = MemoTestUtils.createCascadesContext(ctx, "select 1");
        ExpressionAnalyzer analyzer = new ExpressionAnalyzer(null,
                new org.apache.doris.nereids.analyzer.Scope(java.util.Collections.emptyList()),
                cascadesContext, false, false);
        ExpressionRewriteContext rewriteContext = new ExpressionRewriteContext(cascadesContext);

        // analyze should not throw and should handle type coercion
        Expression analyzed = analyzer.analyze(cmp, rewriteContext);
        Assertions.assertNotNull(analyzed);

        // sql cache context should have recorded the used variable
        List<Variable> used = cascadesContext.getStatementContext().getSqlCacheContext().get().getUsedVariables();
        Assertions.assertFalse(used.isEmpty());
        boolean found = used.stream().anyMatch(v -> "uvar".equals(v.getName()));
        Assertions.assertTrue(found, "user variable should be recorded in sql cache context");
    }
}
