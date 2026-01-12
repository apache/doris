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

package org.apache.doris.nereids.rules.expression;

import org.apache.doris.nereids.trees.expressions.Expression;

import com.google.common.collect.ImmutableList;

import java.util.List;

/** ExpressionNormalizationAndOptimization */
public class ExpressionNormalizationAndOptimization extends ExpressionRewrite {

    public static final ExpressionNormalizationAndOptimization FULL_RULE_INSTANCE
            = new ExpressionNormalizationAndOptimization(true);

    public static final ExpressionNormalizationAndOptimization NO_MIN_MAX_RANGE_INSTANCE
            = new ExpressionNormalizationAndOptimization(false);

    /** ExpressionNormalizationAndOptimization */
    public ExpressionNormalizationAndOptimization(boolean addRange) {
        super(new ExpressionRuleExecutor(buildRewriteRule(addRange)));
    }

    // The ADD_RANGE will add 'AND (slot min max range)' to the expression, later the added min max range condition
    // can be pushed down. At the beginning of rewrite phase, should add ADD_RANGE rules to infer the min max range.
    // After push down the ranges, don't optimize the expression with the add ranges.
    // So CONSTANT_PROPAGATION and PUSH_DOWN_FILTER will exclude ADD_RANGE rules.
    private static List<ExpressionRewriteRule<ExpressionRewriteContext>> buildRewriteRule(boolean addRange) {
        ImmutableList.Builder<ExpressionRewriteRule<ExpressionRewriteContext>> builder = ImmutableList.builder();
        builder.addAll(ExpressionNormalization.NORMALIZE_REWRITE_RULES)
                .addAll(ExpressionOptimization.OPTIMIZE_REWRITE_RULES);
        if (addRange) {
            builder.addAll(ExpressionOptimization.ADD_RANGE);
        }
        return builder.build();
    }

    @Override
    public Expression rewrite(Expression expression, ExpressionRewriteContext expressionRewriteContext) {
        return super.rewrite(expression, expressionRewriteContext);
    }
}
