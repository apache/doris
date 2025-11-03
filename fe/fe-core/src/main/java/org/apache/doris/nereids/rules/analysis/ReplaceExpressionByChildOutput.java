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

import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

/**
 * replace.
 */
public class ReplaceExpressionByChildOutput implements AnalysisRuleFactory {
    @Override
    public List<Rule> buildRules() {
        return ImmutableList.<Rule>builder()
                .add(RuleType.REPLACE_SORT_EXPRESSION_BY_CHILD_OUTPUT.build(
                        logicalSort(logicalProject()).then(sort -> {
                            LogicalProject<Plan> project = sort.child();
                            Map<Expression, Slot> sMap = buildOutputAliasMap(project.getProjects());
                            return replaceSortExpression(sort, sMap);
                        })
                ))
                .add(RuleType.REPLACE_SORT_EXPRESSION_BY_CHILD_OUTPUT.build(
                        logicalSort(logicalAggregate()).then(sort -> {
                            LogicalAggregate<Plan> aggregate = sort.child();
                            Map<Expression, Slot> sMap = buildOutputAliasMap(aggregate.getOutputExpressions());
                            return replaceSortExpression(sort, sMap);
                        })
                )).add(RuleType.REPLACE_SORT_EXPRESSION_BY_CHILD_OUTPUT.build(
                        logicalSort(logicalHaving(logicalAggregate())).then(sort -> {
                            LogicalAggregate<Plan> aggregate = sort.child().child();
                            Map<Expression, Slot> sMap = buildOutputAliasMap(aggregate.getOutputExpressions());
                            return replaceSortExpression(sort, sMap);
                        })
                ))
                .build();
    }

    private Map<Expression, Slot> buildOutputAliasMap(List<NamedExpression> output) {
        Map<Expression, Slot> sMap = Maps.newHashMapWithExpectedSize(output.size());
        for (NamedExpression expr : output) {
            if (expr instanceof Alias) {
                Alias alias = (Alias) expr;
                sMap.put(alias.child(), alias.toSlot());
            }
        }
        return sMap;
    }

    private LogicalPlan replaceSortExpression(LogicalSort<? extends LogicalPlan> sort, Map<Expression, Slot> sMap) {
        List<OrderKey> orderKeys = sort.getOrderKeys();

        boolean changed = false;
        ImmutableList.Builder<OrderKey> newKeys = ImmutableList.builderWithExpectedSize(orderKeys.size());
        for (OrderKey k : orderKeys) {
            Expression newExpr = ExpressionUtils.replace(k.getExpr(), sMap);
            if (newExpr != k.getExpr()) {
                changed = true;
            }
            newKeys.add(new OrderKey(newExpr, k.isAsc(), k.isNullFirst()));
        }

        return changed ? new LogicalSort<>(newKeys.build(), sort.child()) : sort;
    }
}
