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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.functions.agg.Median;
import org.apache.doris.nereids.trees.expressions.functions.agg.Percentile;
import org.apache.doris.nereids.trees.expressions.literal.DoubleLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

import java.util.List;

/**
 * median(col) -> percentile(col, 0.5)
 */
public class MedianRewrite extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalAggregate().when(MedianRewrite::containsMediaObject).then(agg -> {
            List<NamedExpression> outputExpressions = agg.getOutputExpressions();
            Builder<NamedExpression> newOutputs
                    = ImmutableList.builderWithExpectedSize(outputExpressions.size());
            for (NamedExpression outputExpression : outputExpressions) {
                NamedExpression newOutput = (NamedExpression) outputExpression.rewriteUp(expr -> {
                    if (expr instanceof Median && expr.arity() == 1) {
                        Expression child = expr.child(0);
                        return new Percentile(child, DoubleLiteral.of(0.5));
                    }
                    return expr;
                });
                newOutputs.add(newOutput);
            }
            return agg.withAggOutput(newOutputs.build());
        }).toRule(RuleType.MEDIAN_REWRITE);
    }

    private static boolean containsMediaObject(LogicalAggregate<Plan> agg) {
        for (NamedExpression ne : agg.getOutputExpressions()) {
            boolean needRewrite = ne.anyMatch(expr -> expr instanceof Median && expr.arity() == 1);
            if (needRewrite) {
                return true;
            }
        }
        return false;
    }
}
