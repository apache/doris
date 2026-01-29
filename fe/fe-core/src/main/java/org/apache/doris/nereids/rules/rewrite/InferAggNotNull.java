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
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.Avg;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Filter;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableSet;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * InferNotNull from Agg count(distinct);
 */
public class InferAggNotNull extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalAggregate()
                .when(agg -> agg.getGroupByExpressions().isEmpty())
                .when(agg -> agg.getAggregateFunctions().size() == 1)
                .when(agg -> {
                    Set<AggregateFunction> funcs = agg.getAggregateFunctions();
                    return funcs.stream().allMatch(f -> f instanceof Count)
                            || funcs.stream().allMatch(f -> f instanceof Avg)
                            || funcs.stream().allMatch(f -> f instanceof Sum)
                            || funcs.stream().allMatch(f -> f instanceof Max)
                            || funcs.stream().allMatch(f -> f instanceof Min);
                }).thenApply(ctx -> {
                    LogicalAggregate<Plan> agg = ctx.root;
                    Set<Expression> exprs = agg.getAggregateFunctions().stream().flatMap(f -> f.children().stream())
                            .collect(Collectors.toSet());
                    Set<Expression> isNotNulls = ExpressionUtils.inferNotNull(exprs, ctx.cascadesContext);
                    Set<Expression> predicates = Collections.emptySet();
                    if ((agg.child() instanceof Filter)) {
                        predicates = ((Filter) agg.child()).getConjuncts();
                    }
                    if (predicates.containsAll(isNotNulls)) {
                        return null;
                    }
                    ImmutableSet.Builder<Expression> newPredicateBuilder
                            = ImmutableSet.builderWithExpectedSize(predicates.size() + isNotNulls.size());
                    newPredicateBuilder.addAll(predicates);
                    newPredicateBuilder.addAll(isNotNulls);
                    Plan newFilterChild = agg.child() instanceof Filter ? agg.child().child(0) : agg.child();
                    LogicalFilter<Plan> newFilter = new LogicalFilter<>(newPredicateBuilder.build(), newFilterChild);
                    return agg.withChildren(newFilter);
                }).toRule(RuleType.INFER_AGG_NOT_NULL);
    }
}
