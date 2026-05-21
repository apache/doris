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

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.Avg;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Filter;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.PlanUtils;

import com.google.common.collect.ImmutableSet;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * InferNotNull from Agg count(distinct);
 */
public class InferAggNotNull extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalAggregate()
                .when(agg -> agg.getGroupByExpressions().size() == 0)
                .thenApply(ctx -> {
                    LogicalAggregate<Plan> agg = ctx.root;
                    Set<AggregateFunction> aggregateFunctions = agg.getAggregateFunctions();
                    Set<Expression> isNotNulls = inferCommonNotNulls(aggregateFunctions, ctx.cascadesContext);
                    Set<Expression> predicates = Collections.emptySet();
                    if ((agg.child() instanceof Filter)) {
                        predicates = ((Filter) agg.child()).getConjuncts();
                    }
                    ImmutableSet.Builder<Expression> needGenerateNotNullsBuilder = ImmutableSet.builder();
                    for (Expression isNotNull : isNotNulls) {
                        if (!predicates.contains(isNotNull)) {
                            isNotNull = ((Not) isNotNull).withGeneratedIsNotNull(true);
                            if (!predicates.contains(isNotNull)) {
                                needGenerateNotNullsBuilder.add(isNotNull);
                            }
                        }
                    }
                    Set<Expression> needGenerateNotNulls = needGenerateNotNullsBuilder.build();
                    if (needGenerateNotNulls.isEmpty()) {
                        return null;
                    }
                    return agg.withChildren(PlanUtils.filter(needGenerateNotNulls, agg.child()).get());
                }).toRule(RuleType.INFER_AGG_NOT_NULL);
    }

    private Set<Expression> inferCommonNotNulls(
            Set<AggregateFunction> aggregateFunctions, CascadesContext cascadesContext) {
        if (aggregateFunctions.isEmpty()) {
            return Collections.emptySet();
        }
        for (AggregateFunction aggregateFunction : aggregateFunctions) {
            if (!canInferFunctionNotNull(aggregateFunction)) {
                return Collections.emptySet();
            }
        }
        Set<Expression> commonNotNulls = null;
        for (AggregateFunction aggregateFunction : aggregateFunctions) {
            Set<Expression> functionNotNulls = inferFunctionNotNulls(aggregateFunction, cascadesContext);
            if (functionNotNulls.isEmpty()) {
                return Collections.emptySet();
            }
            if (commonNotNulls == null) {
                commonNotNulls = new HashSet<>(functionNotNulls);
            } else {
                commonNotNulls.retainAll(functionNotNulls);
                if (commonNotNulls.isEmpty()) {
                    return Collections.emptySet();
                }
            }
        }
        return commonNotNulls == null ? Collections.emptySet() : commonNotNulls;
    }

    private Set<Expression> inferFunctionNotNulls(
            AggregateFunction aggregateFunction, CascadesContext cascadesContext) {
        return ExpressionUtils.inferNotNull(ImmutableSet.copyOf(aggregateFunction.children()), cascadesContext);
    }

    private boolean canInferFunctionNotNull(AggregateFunction aggregateFunction) {
        return isSupportedAggregateFunction(aggregateFunction)
                && !aggregateFunction.children().isEmpty()
                && ExpressionUtils.isCheapEnoughToInferNotNull(aggregateFunction.children());
    }

    private boolean isSupportedAggregateFunction(AggregateFunction aggregateFunction) {
        return aggregateFunction instanceof Count
                || aggregateFunction instanceof Avg
                || aggregateFunction instanceof Sum
                || aggregateFunction instanceof Max
                || aggregateFunction instanceof Min;
    }

}
