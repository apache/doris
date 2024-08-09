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
import org.apache.doris.nereids.rules.rewrite.NormalizeToSlot.NormalizeToSlotContext;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.Percentile;
import org.apache.doris.nereids.trees.expressions.functions.agg.PercentileArray;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ElementAt;
import org.apache.doris.nereids.trees.expressions.literal.ArrayLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**MergePercentileToArray
 * LogicalAggregate (outputExpression:[percentile(a,0.1) as c1, percentile(a,0.22) as c2])
 * ->
 * LogicalProject (projects: [element_at(percentile(a,[0.1,0.22])#1, 1) as c1,
 *      element_at(percentile(a,[0.1,0.22], 2)#1 as c2])
 *   --+LogicalAggregate(outputExpression: percentile_array(a, [0.1, 0.22]) as percentile_array(a, [0.1, 0.22])#1)
 * */
@DependsRules({
        NormalizeAggregate.class
})
public class MergePercentileToArray extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalAggregate(any())
                .then(this::doMerge)
                .toRule(RuleType.MERGE_PERCENTILE_TO_ARRAY);
    }

    // Merge percentile into percentile_array according to funcMap
    private List<AggregateFunction> getPercentileArrays(Map<DistinctAndExpr, List<AggregateFunction>> funcMap) {
        List<AggregateFunction> newPercentileArrays = Lists.newArrayList();
        for (Map.Entry<DistinctAndExpr, List<AggregateFunction>> entry : funcMap.entrySet()) {
            List<Literal> literals = new ArrayList<>();
            for (AggregateFunction aggFunc : entry.getValue()) {
                List<Expression> literal = aggFunc.child(1).collectToList(expr -> expr instanceof Literal);
                literals.add((Literal) literal.get(0));
            }
            ArrayLiteral arrayLiteral = new ArrayLiteral(literals);
            PercentileArray percentileArray = null;
            if (entry.getKey().isDistinct) {
                percentileArray = new PercentileArray(true, entry.getKey().getExpression(), new Cast(arrayLiteral,
                        ArrayType.of(DoubleType.INSTANCE)));
            } else {
                percentileArray = new PercentileArray(entry.getKey().getExpression(), new Cast(arrayLiteral,
                        ArrayType.of(DoubleType.INSTANCE)));
            }
            newPercentileArrays.add(percentileArray);
        }
        return newPercentileArrays;
    }

    // Find all the percentile functions and place them in the map
    // with the first parameter of the percentile as the key
    private Map<DistinctAndExpr, List<AggregateFunction>> collectFuncMap(LogicalAggregate<Plan> aggregate) {
        Set<AggregateFunction> aggregateFunctions = aggregate.getAggregateFunctions();
        Map<DistinctAndExpr, List<AggregateFunction>> funcMap = new HashMap<>();
        for (AggregateFunction func : aggregateFunctions) {
            if (!(func instanceof Percentile)) {
                continue;
            }
            DistinctAndExpr distictAndExpr = new DistinctAndExpr(func.child(0), func.isDistinct());
            funcMap.computeIfAbsent(distictAndExpr, k -> new ArrayList<>()).add(func);
        }
        funcMap.entrySet().removeIf(entry -> entry.getValue().size() == 1);
        return funcMap;
    }

    private Plan doMerge(LogicalAggregate<Plan> aggregate) {
        Map<DistinctAndExpr, List<AggregateFunction>> funcMap = collectFuncMap(aggregate);
        if (funcMap.isEmpty()) {
            return aggregate;
        }
        Set<AggregateFunction> canMergePercentiles = Sets.newHashSet();
        for (Map.Entry<DistinctAndExpr, List<AggregateFunction>> entry : funcMap.entrySet()) {
            canMergePercentiles.addAll(entry.getValue());
        }

        Set<AggregateFunction> aggregateFunctions = aggregate.getAggregateFunctions();
        SetView<AggregateFunction> aggFuncsNotChange = Sets.difference(aggregateFunctions, canMergePercentiles);

        // construct new Aggregate
        List<AggregateFunction> newPercentileArrays = getPercentileArrays(funcMap);
        ImmutableList.Builder<NamedExpression> normalizedAggOutputBuilder =
                ImmutableList.builderWithExpectedSize(aggregate.getGroupByExpressions().size()
                        + aggFuncsNotChange.size() + newPercentileArrays.size());
        List<NamedExpression> groupBySlots = new ArrayList<>();
        for (Expression groupBy : aggregate.getGroupByExpressions()) {
            groupBySlots.add(((NamedExpression) groupBy).toSlot());
        }
        normalizedAggOutputBuilder.addAll(groupBySlots);
        Set<Alias> existsAliases =
                ExpressionUtils.mutableCollect(aggregate.getOutputExpressions(), Alias.class::isInstance);
        NormalizeToSlotContext notChangeFuncContext = NormalizeToSlotContext.buildContext(existsAliases,
                aggFuncsNotChange);
        NormalizeToSlotContext percentileArrayContext = NormalizeToSlotContext.buildContext(new HashSet<>(),
                newPercentileArrays);
        normalizedAggOutputBuilder.addAll(notChangeFuncContext.pushDownToNamedExpression(aggFuncsNotChange));
        normalizedAggOutputBuilder.addAll(percentileArrayContext.pushDownToNamedExpression(newPercentileArrays));
        LogicalAggregate<Plan> newAggregate = aggregate.withAggOutput(normalizedAggOutputBuilder.build());

        // construct new Project
        List<Expression> notChangeForProject = notChangeFuncContext.normalizeToUseSlotRef(
                (Set<Expression>) (Set) aggFuncsNotChange);
        List<Expression> newPercentileArrayForProject = percentileArrayContext.normalizeToUseSlotRef(
                (List<Expression>) (List) newPercentileArrays);
        ImmutableList.Builder<NamedExpression> newProjectOutputExpressions = ImmutableList.builder();
        newProjectOutputExpressions.addAll((List<NamedExpression>) (List) notChangeForProject);
        Map<Expression, Alias> existsAliasMap = Maps.newHashMap();
        // existsAliasMap is used to keep upper plan refer the same expr
        for (Alias alias : existsAliases) {
            existsAliasMap.put(alias.child(), alias);
        }
        Map<DistinctAndExpr, Slot> slotMap = Maps.newHashMap();
        // slotMap is used to find the correspondence
        // between LogicalProject's element_at(percentile_array_slot_reference, i) which replaces the old percentile()
        // and the merged percentile_array() in LogicalAggregate
        for (int i = 0; i < newPercentileArrays.size(); i++) {
            DistinctAndExpr distinctAndExpr = new DistinctAndExpr(newPercentileArrays.get(i)
                    .child(0), newPercentileArrays.get(i).isDistinct());
            slotMap.put(distinctAndExpr, (Slot) newPercentileArrayForProject.get(i));
        }
        for (Map.Entry<DistinctAndExpr, List<AggregateFunction>> entry : funcMap.entrySet()) {
            for (int i = 0; i < entry.getValue().size(); i++) {
                AggregateFunction aggFunc = entry.getValue().get(i);
                Alias originAlias = existsAliasMap.get(aggFunc);
                DistinctAndExpr distinctAndExpr = new DistinctAndExpr(aggFunc.child(0), aggFunc.isDistinct());
                Alias newAlias = new Alias(originAlias.getExprId(), new ElementAt(slotMap.get(distinctAndExpr),
                        new IntegerLiteral(i + 1)), originAlias.getName());
                newProjectOutputExpressions.add(newAlias);
            }
        }
        newProjectOutputExpressions.addAll(groupBySlots);
        return new LogicalProject(newProjectOutputExpressions.build(), newAggregate);
    }

    private static class DistinctAndExpr {
        private Expression expression;
        private boolean isDistinct;

        public DistinctAndExpr(Expression expression, boolean isDistinct) {
            this.expression = expression;
            this.isDistinct = isDistinct;
        }

        public Expression getExpression() {
            return expression;
        }

        public boolean isDistinct() {
            return isDistinct;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            DistinctAndExpr a = (DistinctAndExpr) o;
            return isDistinct == a.isDistinct
                    && Objects.equals(expression, a.expression);
        }

        @Override
        public int hashCode() {
            return Objects.hash(expression, isDistinct);
        }
    }
}
