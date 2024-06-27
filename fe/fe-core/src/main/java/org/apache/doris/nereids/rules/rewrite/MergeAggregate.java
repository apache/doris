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

import org.apache.doris.nereids.annotation.DependsRules;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.analysis.NormalizeAggregate;
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**MergeAggregate*/
@DependsRules({
        NormalizeAggregate.class
})
public class MergeAggregate implements RewriteRuleFactory {
    private static final ImmutableSet<String> ALLOW_MERGE_AGGREGATE_FUNCTIONS =
            ImmutableSet.of("min", "max", "sum", "any_value");

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                logicalAggregate(logicalAggregate()).when(this::canMergeAggregateWithoutProject)
                        .then(this::mergeTwoAggregate)
                        .toRule(RuleType.MERGE_AGGREGATE),
                logicalAggregate(logicalProject(logicalAggregate()))
                        .when(this::canMergeAggregateWithProject)
                        .then(this::mergeAggProjectAgg)
                        .toRule(RuleType.MERGE_AGGREGATE));
    }

    /**
     * before:
     * LogicalAggregate
     *   +--LogicalAggregate
     * after:
     * LogicalAggregate
     */
    private Plan mergeTwoAggregate(LogicalAggregate<LogicalAggregate<Plan>> outerAgg) {
        LogicalAggregate<Plan> innerAgg = outerAgg.child();
        Map<ExprId, AggregateFunction> innerAggExprIdToAggFunc = getInnerAggExprIdToAggFuncMap(innerAgg);
        List<NamedExpression> newOutputExpressions = outerAgg.getOutputExpressions().stream()
                .map(e -> rewriteAggregateFunction(e, innerAggExprIdToAggFunc))
                .collect(Collectors.toList());
        return outerAgg.withAggOutput(newOutputExpressions).withChildren(innerAgg.children());
    }

    /**
     * before:
     * LogicalAggregate (outputExpressions = [col2, sum(col1)], groupByKeys = [col2])
     *   +--LogicalProject (projects = [a as col2, col1])
     *     +--LogicalAggregate (outputExpressions = [a, b, sum(c) as col1], groupByKeys = [a,b])
     * after:
     * LogicalProject (projects = [a as col2, sum(col1) as sum(col1)]
     *   +--LogicalAggregate (outputExpression = [a, sum(c) as sum(col1)], groupByKeys = [a])
     */
    private Plan mergeAggProjectAgg(LogicalAggregate<LogicalProject<LogicalAggregate<Plan>>> outerAgg) {
        LogicalProject<LogicalAggregate<Plan>> project = outerAgg.child();
        LogicalAggregate<Plan> innerAgg = project.child();
        List<NamedExpression> outputExpressions = outerAgg.getOutputExpressions();
        List<NamedExpression> replacedOutputExpressions = PlanUtils.replaceExpressionByProjections(
                                project.getProjects(), (List) outputExpressions);
        Map<ExprId, AggregateFunction> innerAggExprIdToAggFunc = getInnerAggExprIdToAggFuncMap(innerAgg);
        // rewrite agg function. e.g. max(max)
        List<NamedExpression> replacedAggFunc = replacedOutputExpressions.stream()
                .filter(expr -> (expr instanceof Alias) && (expr.child(0) instanceof AggregateFunction))
                .map(e -> rewriteAggregateFunction(e, innerAggExprIdToAggFunc))
                .collect(Collectors.toList());
        // replace groupByKeys directly refer to the slot below the project
        List<Expression> replacedGroupBy = PlanUtils.replaceExpressionByProjections(project.getProjects(),
                outerAgg.getGroupByExpressions());
        List<NamedExpression> newOutputExpressions = ImmutableList.<NamedExpression>builder()
                .addAll(replacedGroupBy.stream().map(slot -> (NamedExpression) slot).iterator())
                .addAll(replacedAggFunc.stream().map(alias -> (NamedExpression) alias).iterator()).build();
        // construct agg
        LogicalAggregate<Plan> resAgg = outerAgg.withGroupByAndOutput(replacedGroupBy, newOutputExpressions)
                .withChildren(innerAgg.children());

        // construct upper project
        Map<ExprId, NamedExpression> exprIdToNameExpressionMap = new HashMap<>();
        for (NamedExpression pro : project.getProjects()) {
            exprIdToNameExpressionMap.put(pro.getExprId(), pro);
        }
        List<Expression> originOuterAggGroupBy = outerAgg.getGroupByExpressions();
        List<Expression> projectGroupBy = new ArrayList<>();
        for (Expression expression : originOuterAggGroupBy) {
            ExprId exprId = ((NamedExpression) expression).getExprId();
            NamedExpression namedExpression = exprIdToNameExpressionMap.get(exprId);
            projectGroupBy.add(namedExpression);
        }
        List<NamedExpression> upperProjects = ImmutableList.<NamedExpression>builder()
                .addAll(projectGroupBy.stream().map(namedExpr -> (NamedExpression) namedExpr).iterator())
                .addAll(replacedAggFunc.stream().map(expr -> ((NamedExpression) expr).toSlot()).iterator())
                .build();
        return new LogicalProject<Plan>(upperProjects, resAgg);
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

    private boolean commonCheck(LogicalAggregate<? extends Plan> outerAgg, LogicalAggregate<Plan> innerAgg,
            boolean sameGroupBy, Optional<LogicalProject> projectOptional) {
        Map<ExprId, AggregateFunction> innerAggExprIdToAggFunc = getInnerAggExprIdToAggFuncMap(innerAgg);
        Set<AggregateFunction> aggregateFunctions = outerAgg.getAggregateFunctions();
        List<AggregateFunction> replacedAggFunctions = projectOptional.map(project ->
                (List<AggregateFunction>) PlanUtils.replaceExpressionByProjections(
                projectOptional.get().getProjects(), new ArrayList<>(aggregateFunctions)))
                .orElse(new ArrayList<>(aggregateFunctions));
        for (AggregateFunction outerFunc : replacedAggFunctions) {
            if (!(ALLOW_MERGE_AGGREGATE_FUNCTIONS.contains(outerFunc.getName()))) {
                return false;
            }
            if (outerFunc.isDistinct() && !sameGroupBy) {
                return false;
            }
            // not support outerAggFunc: sum(a+1),sum(a+b)
            if (!(outerFunc.child(0) instanceof SlotReference)) {
                return false;
            }
            ExprId childExprId = ((SlotReference) outerFunc.child(0)).getExprId();
            if (innerAggExprIdToAggFunc.containsKey(childExprId)) {
                AggregateFunction innerFunc = innerAggExprIdToAggFunc.get(childExprId);
                if (innerFunc.isDistinct() && !sameGroupBy) {
                    return false;
                }
                // support sum(sum),min(min),max(max),any_value(any_value),sum(count)
                // sum(count) -> count() need outerAgg having group by keys (reason: nullable)
                if (!(outerFunc.getName().equals("sum") && innerFunc.getName().equals("count")
                        && !outerAgg.getGroupByExpressions().isEmpty())
                        && !innerFunc.getName().equals(outerFunc.getName())) {
                    return false;
                }
            } else {
                // select a, max(b), min(b), any_value(b) from (select a,b from t1 group by a, b) group by a;
                // equals select a, max(b), min(b), any_value(b) from t1 group by a;
                if (!outerFunc.getName().equals("max")
                        && !outerFunc.getName().equals("min")
                        && !outerFunc.getName().equals("any_value")) {
                    return false;
                }
            }
        }
        return true;
    }

    private boolean canMergeAggregateWithoutProject(LogicalAggregate<LogicalAggregate<Plan>> outerAgg) {
        LogicalAggregate<Plan> innerAgg = outerAgg.child();
        if (!new HashSet<>(innerAgg.getGroupByExpressions()).containsAll(outerAgg.getGroupByExpressions())) {
            return false;
        }
        boolean sameGroupBy = (innerAgg.getGroupByExpressions().size() == outerAgg.getGroupByExpressions().size());

        return commonCheck(outerAgg, innerAgg, sameGroupBy, Optional.empty());
    }

    private boolean canMergeAggregateWithProject(LogicalAggregate<LogicalProject<LogicalAggregate<Plan>>> outerAgg) {
        LogicalProject<LogicalAggregate<Plan>> project = outerAgg.child();
        LogicalAggregate<Plan> innerAgg = project.child();

        List<Expression> outerAggGroupByKeys = PlanUtils.replaceExpressionByProjections(project.getProjects(),
                outerAgg.getGroupByExpressions());
        if (!new HashSet<>(innerAgg.getGroupByExpressions()).containsAll(outerAggGroupByKeys)) {
            return false;
        }
        // project cannot have expressions like a+1
        if (ExpressionUtils.deapAnyMatch(project.getProjects(),
                expr -> !(expr instanceof SlotReference) && !(expr instanceof Alias))) {
            return false;
        }
        boolean sameGroupBy = (innerAgg.getGroupByExpressions().size() == outerAgg.getGroupByExpressions().size());
        return commonCheck(outerAgg, innerAgg, sameGroupBy, Optional.of(project));
    }

    private Map<ExprId, AggregateFunction> getInnerAggExprIdToAggFuncMap(LogicalAggregate<Plan> innerAgg) {
        return innerAgg.getOutputExpressions().stream()
                .filter(expr -> (expr instanceof Alias) && (expr.child(0) instanceof AggregateFunction))
                .collect(Collectors.toMap(NamedExpression::getExprId, value -> (AggregateFunction) value.child(0),
                        (existValue, newValue) -> existValue));
    }
}
