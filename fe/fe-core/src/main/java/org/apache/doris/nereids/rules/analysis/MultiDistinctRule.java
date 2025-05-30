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

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.RewriteRuleFactory;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.SupportMultiDistinct;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * convert count(distinct A) to multi_distinct_count(A)
 * for patterns:
 * select count(distinct A), sum(B) from T group by C
 *
 * patterns that contains only one distinct agg func, and no other agg func, are processed by 3 phase aggregate
 * select count(distinct A) from T group by B...
 * select count(distinct A) , B from T group by B...
 *
 */

public class MultiDistinctRule implements RewriteRuleFactory {

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                logicalAggregate().when(this::containsDistinctAggFunction)
                        .then(agg -> convertMultiDistinctIfNeed(agg))
                        .toRule(RuleType.MULTI_DISTINCT_RULE));
    }

    private boolean containsDistinctAggFunction(LogicalAggregate<Plan> agg) {
        for (AggregateFunction func : agg.getAggregateFunctions()) {
            if (func.isDistinct()) {
                return true;
            }
        }
        return false;
    }

    private LogicalPlan convertMultiDistinctIfNeed(LogicalAggregate<Plan> agg) {
        Set<AggregateFunction> aggFunctions = agg.getAggregateFunctions();
        int distinctCount = 0;
        boolean mustUseMultiDistinct = false;
        for (AggregateFunction func : aggFunctions) {
            if (func.isDistinct()) {
                distinctCount++;
            }
            if (func.mustUseMultiDistinctAgg()) {
                mustUseMultiDistinct = true;
                break;
            }
        }
        if (mustUseMultiDistinct || distinctCount > 1 || distinctCount == 1 && aggFunctions.size() > 1) {
            // replace func(distinct xxx) to multi_distinct_func(xxx)
            Map<AggregateFunction, AggregateFunction> replaceMap = new HashMap<>();
            for (AggregateFunction func : aggFunctions) {
                if (func.isDistinct()) {
                    // TODO convert to multi
                    Optional<AggregateFunction> multi = tryConvertToMultiDistinct(func);
                    multi.ifPresent(aggregateFunction -> replaceMap.put(func, aggregateFunction));
                }
            }

            List<NamedExpression> newAggOutputs = new ArrayList<>();
            for (NamedExpression out : agg.getOutputExpressions()) {
                Expression newOutput = ExpressionUtils.replace(out, replaceMap);
                if (!(newOutput instanceof NamedExpression)) {
                    // should not come here
                    return null;
                }
                newAggOutputs.add((NamedExpression) newOutput);
            }
            return agg.withAggOutput(newAggOutputs);
        } else {
            return null;
        }
    }

    private Optional<AggregateFunction> tryConvertToMultiDistinct(AggregateFunction function) {
        if (function instanceof SupportMultiDistinct && function.isDistinct()) {
            return Optional.of(((SupportMultiDistinct) function).convertToMultiDistinct());
        }
        return Optional.empty();
    }
}
