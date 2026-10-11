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

package org.apache.doris.nereids.types;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.rules.analysis.ExpressionAnalyzer;
import org.apache.doris.nereids.rules.expression.ExpressionRewrite;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.rules.expression.ExpressionRuleExecutor;
import org.apache.doris.nereids.rules.expression.check.CheckCast;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertUtils;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.TypeCoercionUtils;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;

public class FileTypeImplicitCastTest {
    private final NereidsParser parser = new NereidsParser();
    private ConnectContext previousContext;
    private CascadesContext context;
    private ExpressionAnalyzer analyzer;

    @BeforeEach
    public void setUp() {
        previousContext = ConnectContext.get();
        context = MemoTestUtils.createCascadesContext("SELECT 1");
        // Exercise the INSERT VALUES analyzer, including its immediate FE constant folding.
        analyzer = InsertUtils.buildExprAnalyzer(context.getRewritePlan(), context);
    }

    @AfterEach
    public void tearDown() {
        if (previousContext == null) {
            ConnectContext.remove();
        } else {
            previousContext.setThreadLocalInfo();
        }
    }

    @Test
    public void testUnboundNullFileIsCheckedBeforeFolding() {
        Expression unbound = parser.parseExpression("coalesce(CAST(NULL AS FILE), CAST(NULL AS FILE))");
        Expression assignment = TypeCoercionUtils.castUnbound(unbound, StringType.INSTANCE);
        Assertions.assertThrows(AnalysisException.class, () -> analyzer.analyze(assignment));
        Assertions.assertThrows(AnalysisException.class,
                () -> analyzer.analyze(new Cast(unbound, StringType.INSTANCE, true)));
    }

    @Test
    public void testCheckCastRejectsUnsupportedTargets() {
        ExpressionRuleExecutor checks = new ExpressionRuleExecutor(Collections.singletonList(
                ExpressionRewrite.bottomUp(new CheckCast())));
        ExpressionRewriteContext rewriteContext = new ExpressionRewriteContext(context);
        Expression file = new SlotReference("f", FileType.INSTANCE);
        Assertions.assertThrows(AnalysisException.class,
                () -> checks.rewrite(new Cast(file, StringType.INSTANCE), rewriteContext));
        Assertions.assertThrows(AnalysisException.class,
                () -> checks.rewrite(new Cast(file, StringType.INSTANCE, true), rewriteContext));
    }

}
