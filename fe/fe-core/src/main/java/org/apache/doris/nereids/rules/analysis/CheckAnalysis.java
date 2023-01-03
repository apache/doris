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

import org.apache.doris.nereids.analyzer.Unbound;
import org.apache.doris.nereids.analyzer.UnboundFunction;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.typecoercion.TypeCheckResult;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Check analysis rule to check semantic correct after analysis by Nereids.
 */
public class CheckAnalysis implements AnalysisRuleFactory {

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
            RuleType.CHECK_ANALYSIS.build(
                any().then(plan -> {
                    checkBound(plan);
                    checkExpressionInputTypes(plan);
                    return null;
                })
            ),
            RuleType.CHECK_AGGREGATE_ANALYSIS.build(
                logicalAggregate().then(agg -> {
                    checkAggregate(agg);
                    return agg;
                })
            )
        );
    }

    private void checkExpressionInputTypes(Plan plan) {
        final Optional<TypeCheckResult> firstFailed = plan.getExpressions().stream()
                .map(Expression::checkInputDataTypes)
                .filter(TypeCheckResult::failed)
                .findFirst();

        if (firstFailed.isPresent()) {
            throw new AnalysisException(firstFailed.get().getMessage());
        }
    }

    private void checkBound(Plan plan) {
        Set<Unbound> unbounds = plan.getExpressions().stream()
                .<Set<Unbound>>map(e -> e.collect(Unbound.class::isInstance))
                .flatMap(Set::stream)
                .collect(Collectors.toSet());
        if (!unbounds.isEmpty()) {
            throw new AnalysisException(String.format("unbounded object %s.",
                    StringUtils.join(unbounds.stream()
                            .map(unbound -> {
                                if (unbound instanceof UnboundSlot) {
                                    return ((UnboundSlot) unbound).toSql();
                                } else if (unbound instanceof UnboundFunction) {
                                    return ((UnboundFunction) unbound).toSql();
                                }
                                return unbound.toString();
                            })
                            .collect(Collectors.toSet()), ", ")));
        }
    }

    private void checkAggregate(LogicalAggregate<? extends Plan> aggregate) {
        Set<AggregateFunction> aggregateFunctions = aggregate.getAggregateFunctions();
        boolean distinctMultiColumns = aggregateFunctions.stream()
                .anyMatch(fun -> fun.isDistinct() && fun.arity() > 1);
        long distinctFunctionNum = aggregateFunctions.stream()
                .filter(AggregateFunction::isDistinct)
                .count();

        if (distinctMultiColumns && distinctFunctionNum > 1) {
            throw new AnalysisException(
                    "The query contains multi count distinct or sum distinct, each can't have multi columns");
        }
    }
}
