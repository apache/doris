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

import org.apache.doris.nereids.rules.expression.rules.AddMinMax;
import org.apache.doris.nereids.rules.expression.rules.ArrayContainToArrayOverlap;
import org.apache.doris.nereids.rules.expression.rules.BetweenToEqual;
import org.apache.doris.nereids.rules.expression.rules.CaseWhenToIf;
import org.apache.doris.nereids.rules.expression.rules.DateFunctionRewrite;
import org.apache.doris.nereids.rules.expression.rules.DistinctPredicatesRule;
import org.apache.doris.nereids.rules.expression.rules.ExtractCommonFactorRule;
import org.apache.doris.nereids.rules.expression.rules.LikeToEqualRewrite;
import org.apache.doris.nereids.rules.expression.rules.NullSafeEqualToEqual;
import org.apache.doris.nereids.rules.expression.rules.SimplifyComparisonPredicate;
import org.apache.doris.nereids.rules.expression.rules.SimplifyConflictCompound;
import org.apache.doris.nereids.rules.expression.rules.SimplifyInPredicate;
import org.apache.doris.nereids.rules.expression.rules.SimplifyRange;
import org.apache.doris.nereids.rules.expression.rules.SimplifySelfComparison;
import org.apache.doris.nereids.rules.expression.rules.TopnToMax;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * optimize expression of plan rule set.
 */
public class ExpressionOptimization extends ExpressionRewrite {
    public static final List<ExpressionRewriteRule<ExpressionRewriteContext>> OPTIMIZE_REWRITE_RULES = ImmutableList.of(
            bottomUp(
                    SimplifyInPredicate.INSTANCE,

                    // comparison predicates
                    SimplifyComparisonPredicate.INSTANCE,
                    SimplifySelfComparison.INSTANCE,

                    // compound predicates
                    SimplifyRange.INSTANCE,
                    SimplifyConflictCompound.INSTANCE,
                    DistinctPredicatesRule.INSTANCE,
                    ExtractCommonFactorRule.INSTANCE,

                    DateFunctionRewrite.INSTANCE,
                    ArrayContainToArrayOverlap.INSTANCE,
                    CaseWhenToIf.INSTANCE,
                    TopnToMax.INSTANCE,
                    NullSafeEqualToEqual.INSTANCE,
                    LikeToEqualRewrite.INSTANCE,
                    BetweenToEqual.INSTANCE
            )
    );

    /**
     * don't use it with PushDownFilterThroughJoin, it may cause dead loop:
     *   LogicalFilter(origin expr)
     *      => LogicalFilter((origin expr) and (add min max range))
     *      => LogicalFilter((origin expr)) // use PushDownFilterThroughJoin
     *      => ...
     */
    public static final List<ExpressionRewriteRule<ExpressionRewriteContext>> ADD_RANGE = ImmutableList.of(
            bottomUp(
                    AddMinMax.INSTANCE
            )
    );

    private static final ExpressionRuleExecutor EXECUTOR = new ExpressionRuleExecutor(OPTIMIZE_REWRITE_RULES);

    public ExpressionOptimization() {
        super(EXECUTOR);
    }
}
