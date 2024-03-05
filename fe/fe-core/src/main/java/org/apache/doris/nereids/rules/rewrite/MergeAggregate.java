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
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.PlanUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * MergeAggregate
 */
public class MergeAggregate implements RewriteRuleFactory {
    private static final ImmutableSet<String> ALLOW_MERGE_AGGREGATE_FUNCTIONS =
            ImmutableSet.of("min", "max", "sum", "any_value");

    private Plan mergeTwoAggregate(Plan plan) {
        LogicalAggregate<Plan> outerAgg = (LogicalAggregate<Plan>) plan;
        LogicalAggregate<Plan> innerAgg = (LogicalAggregate<Plan>) outerAgg.child();

        Map<ExprId, AggregateFunction> innerAggExprIdToAggFunc = innerAgg.getOutputExpressions().stream()
                .filter(expr -> (expr instanceof Alias) && (expr.child(0) instanceof AggregateFunction))
                .collect(Collectors.toMap(NamedExpression::getExprId, value -> (AggregateFunction) value.child(0)));

        List<NamedExpression> newOutputExpressions = outerAgg.getOutputExpressions().stream()
                .map(e -> rewriteAggregateFunction(e, innerAggExprIdToAggFunc))
                .collect(Collectors.toList());
        return outerAgg.withAggOutput(newOutputExpressions).withChildren(innerAgg.children());
    }

    private Plan mergeAggProjectAgg(Plan plan) {
        LogicalAggregate<Plan> outerAgg = (LogicalAggregate<Plan>) plan;
        LogicalProject<Plan> project = (LogicalProject<Plan>) outerAgg.child();
        LogicalAggregate<Plan> innerAgg = (LogicalAggregate<Plan>) project.child();

        Map<ExprId, AggregateFunction> innerAggExprIdToAggFunc = innerAgg.getOutputExpressions().stream()
                .filter(expr -> (expr instanceof Alias) && (expr.child(0) instanceof AggregateFunction))
                .collect(Collectors.toMap(NamedExpression::getExprId, value -> (AggregateFunction) value.child(0)));

        List<NamedExpression> newOutputExpressions = outerAgg.getOutputExpressions().stream()
                .map(e -> rewriteAggregateFunction(e, innerAggExprIdToAggFunc))
                .collect(Collectors.toList());

        // replace outputExpression and groupByKeys by projections
        newOutputExpressions = PlanUtils.mergeProjections(project.getProjects(), newOutputExpressions);
        List<Expression> newGroupBy = PlanUtils.replaceExpressionByProjections(project.getProjects(),
                outerAgg.getGroupByExpressions());
        return outerAgg.withGroupByAndOutput(newGroupBy, newOutputExpressions).withChildren(innerAgg.children());
    }

    private NamedExpression rewriteAggregateFunction(NamedExpression e,
            Map<ExprId, AggregateFunction> innerAggExprIdToAggFunc) {
        return (NamedExpression) e.rewriteDownShortCircuit(expr -> {
            if (expr instanceof Alias && ((Alias) expr).child() instanceof AggregateFunction) {
                Alias alias = (Alias) expr;
                AggregateFunction aggFunc = (AggregateFunction) alias.child();
                ExprId childExprId = ((SlotReference) aggFunc.child(0)).getExprId();
                if (innerAggExprIdToAggFunc.containsKey(childExprId)) {
                    return new Alias(alias.getExprId(), innerAggExprIdToAggFunc.get(childExprId),
                            alias.getName());
                } else {
                    return expr;
                }
            } else {
                return expr;
            }
        });
    }

    private boolean canMergeAggregate(Plan plan, boolean hasProject) {
        LogicalAggregate<Plan> outerAgg = (LogicalAggregate<Plan>) plan;
        LogicalAggregate<Plan> innerAgg = hasProject ? (LogicalAggregate<Plan>) outerAgg.child().child(0)
                : (LogicalAggregate<Plan>) outerAgg.child();

        List<Expression> outerAggGroupByKeys = outerAgg.getGroupByExpressions();
        if (hasProject) {
            LogicalProject<Plan> project = (LogicalProject<Plan>) outerAgg.child();
            outerAggGroupByKeys = PlanUtils.replaceExpressionByProjections(project.getProjects(), outerAggGroupByKeys);
        }

        if (!new HashSet<>(innerAgg.getGroupByExpressions()).containsAll(outerAggGroupByKeys)) {
            return false;
        }

        Map<ExprId, AggregateFunction> innerAggExprIdToAggFunc = innerAgg.getOutputExpressions().stream()
                .filter(expr -> (expr instanceof Alias) && (expr.child(0) instanceof AggregateFunction))
                .collect(Collectors.toMap(NamedExpression::getExprId, value -> (AggregateFunction) value.child(0)));
        Set<AggregateFunction> aggregateFunctions = outerAgg.getAggregateFunctions();
        for (AggregateFunction outerFunc : aggregateFunctions) {
            if (!(ALLOW_MERGE_AGGREGATE_FUNCTIONS.contains(outerFunc.getName()))) {
                return false;
            }
            if (outerFunc.isDistinct()) {
                return false;
            }
            // not support outerAggFunc: sum(a+1),sum(a+b)
            if (!(outerFunc.child(0) instanceof SlotReference)) {
                return false;
            }
            ExprId childExprId = ((SlotReference) outerFunc.child(0)).getExprId();
            if (innerAggExprIdToAggFunc.containsKey(childExprId)) {
                AggregateFunction innerFunc = innerAggExprIdToAggFunc.get(childExprId);
                if (innerFunc.isDistinct()) {
                    return false;
                }
                // support sum(sum),min(min),max(max),sum(count),any_value(any_value)
                if (!(outerFunc.getName().equals("sum") && innerFunc.getName().equals("count"))
                        && !innerFunc.getName().equals(outerFunc.getName())) {
                    return false;
                }
            } else {
                if (!outerFunc.getName().equals("max")
                        && !outerFunc.getName().equals("min")
                        && !outerFunc.getName().equals("any_value")) {
                    return false;
                }
            }
        }
        return true;
    }

    private boolean projectHasExpression(Plan plan) {
        LogicalProject<Plan> project = (LogicalProject<Plan>) plan.child(0);
        return ExpressionUtils.anyMatch(project.getProjects(),
                expr -> !(expr instanceof SlotReference) && !(expr instanceof Alias));
    }

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                logicalAggregate(logicalAggregate()).when(plan -> canMergeAggregate(plan, false))
                        .then(this::mergeTwoAggregate)
                        .toRule(RuleType.MERGE_AGGREGATE),
                logicalAggregate(logicalProject(logicalAggregate()))
                        .when(plan -> canMergeAggregate(plan, true))
                        .whenNot(this::projectHasExpression)
                        .then(this::mergeAggProjectAgg)
                        .toRule(RuleType.MERGE_AGGREGATE));
    }
}
