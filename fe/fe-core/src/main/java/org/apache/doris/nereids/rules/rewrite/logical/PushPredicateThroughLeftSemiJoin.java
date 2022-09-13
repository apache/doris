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

package org.apache.doris.nereids.rules.rewrite.logical;

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.OneRewriteRuleFactory;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.PlanUtils;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.Set;

/**
 *  Push the predicate in the LogicalFilter to the left semi join children.
 */
public class PushPredicateThroughLeftSemiJoin extends OneRewriteRuleFactory {
    /*
     * For example:
     * select a.k1,b.k1 from a left semi join b on a.k1 = b.k1 and a.k2 > 2 and b.k2 > 5 where a.k1 > 1
     * Logical plan tree:
     *                 project
     *                   |
     *                filter (a.k1 > 1 and b.k1 > 2)
     *                   |
     *                join (a.k1 = b.k1 and a.k2 > 2 and b.k2 > 5)
     *                 /   \
     *              scan  scan
     * transformed:
     *                      project
     *                        |
     *                join (a.k1 = b.k1)
     *                /                \
     * filter(a.k1 > 1 and a.k2 > 2 )   filter(b.k2 > 5)
     *             |                                    |
     *            scan                                scan
     */

    //TODO: this version only push where-predicates to left child of left-semi-join,
    // that is, only push a.k1 > 1 to scan(a).
    @Override
    public Rule build() {
        return logicalFilter(logicalProject(leftSemiLogicalJoin())).then(filter -> {
            LogicalProject<LogicalJoin<GroupPlan, GroupPlan>> project = filter.child();
            LogicalJoin<GroupPlan, GroupPlan> join = project.child();
            Expression wherePredicates = filter.getPredicates();
            Set<Slot> leftInput = join.left().getOutputSet();
            List<Expression> expressionToPush = Lists.newArrayList();
            ExpressionUtils.extractConjunction(wherePredicates)
                    .forEach(e -> {
                        if (leftInput.containsAll(e.getInputSlots())) {
                            expressionToPush.add(e);
                        }
                    });
            if (expressionToPush.isEmpty()) {
                return filter;
            }
            Plan leftChild = PlanUtils.filterOrSelf(expressionToPush, join.left());
            Plan newJoin = join.withChildren(Lists.newArrayList(leftChild, join.right()));
            return project.withChildren(Lists.newArrayList(newJoin));
        }).toRule(RuleType.PUSH_DOWN_PREDICATE_THROUGH_LEFT_SEMI_JOIN);
    }
}
