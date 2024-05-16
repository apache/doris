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
import org.apache.doris.nereids.properties.FuncDeps;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;

import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


/**
 * Eliminate group by key based on fd item information.
 */
@DependsRules({EliminateGroupBy.class, ColumnPruning.class})
public class EliminateGroupByKey extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalProject(logicalAggregate()).then(proj -> {
            LogicalAggregate<? extends Plan> agg = proj.child();
            Set<Slot> groupBySlots = new HashSet<>();
            for (Expression expression : agg.getGroupByExpressions()) {
                if (expression.isSlot() && !proj.getInputSlots().contains((Slot) expression)) {
                    groupBySlots.add((Slot) expression);
                }
            }
            FuncDeps funcDeps = agg.getLogicalProperties()
                    .getFunctionalDependencies().getAllValidFuncDeps(groupBySlots);
            if (funcDeps.isEmpty()) {
                return null;
            }

            Set<Slot> minGroupBySlots = funcDeps.eliminateDeps(groupBySlots);
            Set<Slot> eliminatedSlots = Sets.difference(groupBySlots, minGroupBySlots);
            List<NamedExpression> newOutputs = new ArrayList<>();
            for (NamedExpression expression : agg.getOutputExpressions()) {
                if (!eliminatedSlots.contains(expression.toSlot())) {
                    newOutputs.add(expression);
                }
            }
            List<Expression> newGroupBy = new ArrayList<>();
            for (Expression expression : agg.getGroupByExpressions()) {
                if (!(expression.isSlot() && eliminatedSlots.contains((Slot) expression))) {
                    newGroupBy.add(expression);
                }
            }
            return agg.withGroupByAndOutput(newGroupBy, newOutputs);

        }).toRule(RuleType.ELIMINATE_GROUP_BY_KEY);
    }
}
