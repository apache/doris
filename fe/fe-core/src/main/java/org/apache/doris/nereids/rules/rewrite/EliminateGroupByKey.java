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

import org.apache.doris.nereids.properties.DataTrait;
import org.apache.doris.nereids.properties.FuncDeps;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.agg.AnyValue;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;


/**
 * Eliminate group by key based on fd item information.
 * such as:
 *  for a -> b, we can get:
 *          group by a, b, c  => group by a, c
 */
public class EliminateGroupByKey implements RewriteRuleFactory {

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                RuleType.ELIMINATE_GROUP_BY_KEY.build(
                        logicalProject(logicalAggregate().when(agg -> !agg.getSourceRepeat().isPresent()))
                                .then(proj -> {
                                    LogicalAggregate<? extends Plan> agg = proj.child();
                                    LogicalAggregate<Plan> newAgg = eliminateGroupByKey(agg, proj.getInputSlots());
                                    if (newAgg == null) {
                                        return null;
                                    }
                                    return proj.withChildren(newAgg);
                                })),
                RuleType.ELIMINATE_FILTER_GROUP_BY_KEY.build(
                        logicalProject(logicalFilter(logicalAggregate()
                                .when(agg -> !agg.getSourceRepeat().isPresent())))
                                .then(proj -> {
                                    LogicalAggregate<? extends Plan> agg = proj.child().child();
                                    Set<Slot> requireSlots = new HashSet<>(proj.getInputSlots());
                                    requireSlots.addAll(proj.child(0).getInputSlots());
                                    LogicalAggregate<Plan> newAgg = eliminateGroupByKey(agg, requireSlots);
                                    if (newAgg == null) {
                                        return null;
                                    }
                                    return proj.withChildren(proj.child().withChildren(newAgg));
                                })
                )
        );
    }

    LogicalAggregate<Plan> eliminateGroupByKey(LogicalAggregate<? extends Plan> agg, Set<Slot> requireOutput) {
        FindResult result = findCanBeRemovedExpressionsInternal(agg, requireOutput,
                agg.child().getLogicalProperties().getTrait());
        Set<Expression> removeExpression = result.removeExpression;
        Set<Expression> wrapWithAnyValue = result.wrapWithAnyValue;

        List<Expression> newGroupExpression = new ArrayList<>();
        for (Expression expression : agg.getGroupByExpressions()) {
            if (!removeExpression.contains(expression)
                    && !wrapWithAnyValue.contains(expression)) {
                newGroupExpression.add(expression);
            }
        }
        List<NamedExpression> newOutput = new ArrayList<>();
        for (NamedExpression expression : agg.getOutputExpressions()) {
            if (removeExpression.contains(expression)) {
                continue;
            }
            if (wrapWithAnyValue.contains(expression)) {
                // expression is FD-redundant but needed in output: wrap with any_value
                expression = new Alias(expression.getExprId(),
                        new AnyValue(expression.toSlot()), expression.getName());
            }
            newOutput.add(expression);
        }
        return agg.withGroupByAndOutput(newGroupExpression, newOutput);
    }

    /**
     * Return expressions that can be removed from both group-by and output.
     * Kept for backward compatibility with external callers (e.g. PushDownAggThroughJoinOnPkFk).
     */
    public static Set<Expression> findCanBeRemovedExpressions(LogicalAggregate<? extends Plan> agg,
            Set<Slot> requireOutput, DataTrait dataTrait) {
        FindResult result = findCanBeRemovedExpressionsInternal(agg, requireOutput, dataTrait);
        Set<Expression> all = new HashSet<>();
        all.addAll(result.removeExpression);
        all.addAll(result.wrapWithAnyValue);
        return all;
    }

    /** Result of findCanBeRemovedExpressionsInternal: two sets of expressions. */
    private static class FindResult {
        final Set<Expression> removeExpression;   // remove from group-by and output
        final Set<Expression> wrapWithAnyValue;   // remove from group-by, wrap with ANY_VALUE in output

        FindResult(Set<Expression> removeExpression, Set<Expression> wrapWithAnyValue) {
            this.removeExpression = removeExpression;
            this.wrapWithAnyValue = wrapWithAnyValue;
        }
    }

    private static FindResult findCanBeRemovedExpressionsInternal(LogicalAggregate<? extends Plan> agg,
            Set<Slot> requireOutput, DataTrait dataTrait) {
        Map<Expression, Set<Slot>> groupBySlots = new HashMap<>();
        Set<Slot> validSlots = new HashSet<>();
        for (Expression expression : agg.getGroupByExpressions()) {
            groupBySlots.put(expression, expression.getInputSlots());
            validSlots.addAll(expression.getInputSlots());
        }

        FuncDeps funcDeps = dataTrait.getAllValidFuncDeps(validSlots);
        if (funcDeps.isEmpty()) {
            LOG.warn("EliminateGroupByKey no FDs found: validSlots={}, aggChild={}",
                    validSlots, agg.child(0).getClass().getSimpleName());
            return new FindResult(new HashSet<>(), new HashSet<>());
        }
        LOG.warn("EliminateGroupByKey FDs found: validSlots={}, funcDeps size={}",
                validSlots, funcDeps.size());

        Set<Set<Slot>> minGroupBySlots = funcDeps.eliminateDeps(new HashSet<>(groupBySlots.values()), requireOutput);
        Set<Expression> removeExpression = new HashSet<>();
        Set<Expression> wrapWithAnyValue = new HashSet<>();
        for (Entry<Expression, Set<Slot>> entry : groupBySlots.entrySet()) {
            if (!minGroupBySlots.contains(entry.getValue())) {
                // FD redundant: can remove from group-by
                if (!requireOutput.containsAll(entry.getValue())) {
                    // Not needed in output either: remove completely
                    removeExpression.add(entry.getKey());
                } else {
                    // Still needed in output: remove from group-by, wrap with ANY_VALUE in output
                    wrapWithAnyValue.add(entry.getKey());
                }
            }
        }
        return new FindResult(removeExpression, wrapWithAnyValue);
    }
}
