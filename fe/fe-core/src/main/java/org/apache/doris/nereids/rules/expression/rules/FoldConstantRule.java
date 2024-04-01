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

import org.apache.doris.nereids.rules.expression.ExpressionBottomUpRewriter;
import org.apache.doris.nereids.rules.expression.ExpressionPatternMatcher;
import org.apache.doris.nereids.rules.expression.ExpressionPatternRuleFactory;
import org.apache.doris.nereids.rules.expression.ExpressionRewrite;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.trees.expressions.Expression;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Constant evaluation of an expression.
 */
public class FoldConstantRule implements ExpressionPatternRuleFactory {

    public static final FoldConstantRule INSTANCE = new FoldConstantRule();

    private static final ExpressionBottomUpRewriter FULL_FOLD_REWRITER = ExpressionRewrite.bottomUp(
            FoldConstantRuleOnFE.VISITOR_INSTANCE,
            FoldConstantRuleOnBE.INSTANCE
    );

    /** evaluate by pattern match */
    @Override
    public List<ExpressionPatternMatcher<? extends Expression>> buildRules() {
        return ImmutableList.<ExpressionPatternMatcher<? extends Expression>>builder()
                .addAll(FoldConstantRuleOnFE.PATTERN_MATCH_INSTANCE.buildRules())
                .addAll(FoldConstantRuleOnBE.INSTANCE.buildRules())
                .build();
    }

    /** evaluate by visitor */
    public static Expression evaluate(Expression expr, ExpressionRewriteContext ctx) {
        if (ctx.cascadesContext != null
                && ctx.cascadesContext.getConnectContext() != null
                && ctx.cascadesContext.getConnectContext().getSessionVariable().isEnableFoldConstantByBe()) {
            return FULL_FOLD_REWRITER.rewrite(expr, ctx);
        } else {
            return FoldConstantRuleOnFE.VISITOR_INSTANCE.rewrite(expr, ctx);
        }
    }
}
