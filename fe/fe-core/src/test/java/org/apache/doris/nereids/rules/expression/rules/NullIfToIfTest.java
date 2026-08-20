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

import org.apache.doris.nereids.rules.expression.ExpressionNormalization;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteTestHelper;
import org.apache.doris.nereids.rules.expression.ExpressionRuleExecutor;
import org.apache.doris.nereids.rules.expression.ExpressionRuleType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.stream.Collectors;

class NullIfToIfTest extends ExpressionRewriteTestHelper {

    @Test
    void testRewriteNullIfToIf() {
        executor = new ExpressionRuleExecutor(ExpressionNormalization.NORMALIZE_REWRITE_RULES);

        assertRewriteAfterTypeCoercion("nullif(a, b)", "if(a = b, null, a)");
        assertRewriteAfterTypeCoercion("nullif(a, b + 1)", "if(a = b + 1, null, a)");
    }

    @Test
    void testRewriteNullIfToIfWhenExpressionRulesDisabled() {
        executor = new ExpressionRuleExecutor(ExpressionNormalization.NORMALIZE_REWRITE_RULES);
        String disabledRules = Arrays.stream(ExpressionRuleType.values())
                .map(ExpressionRuleType::name)
                .collect(Collectors.joining(","));
        cascadesContext.getConnectContext().getSessionVariable()
                .setDisableNereidsExpressionRules(disabledRules);
        try {
            assertRewriteAfterTypeCoercion("nullif(a, b)", "if(a = b, null, a)");
        } finally {
            cascadesContext.getConnectContext().getSessionVariable()
                    .setDisableNereidsExpressionRules("");
        }
    }

    @Test
    void testFoldNullIfBeforeRewrite() {
        executor = new ExpressionRuleExecutor(ExpressionNormalization.NORMALIZE_REWRITE_RULES);

        assertRewriteAfterTypeCoercion("nullif(a, a)", "null");
        assertRewriteAfterTypeCoercion("nullif(a, null)", "a");
        assertRewriteAfterTypeCoercion("nullif(null, a)", "null");
        assertRewriteAfterTypeCoercion("nullif(1, 1)", "null");
        assertRewriteAfterTypeCoercion("nullif(1, 2)", "1");
    }
}
