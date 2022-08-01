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

package org.apache.doris.nereids.rules.exploration.join;

import org.apache.doris.nereids.annotation.Developing;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.exploration.OneExplorationRuleFactory;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Rule for change inner join left associative to right.
 */
@Developing
public class JoinLAsscom extends OneExplorationRuleFactory {
    /*
     *      topJoin                newTopJoin
     *      /     \                 /     \
     * bottomJoin  C   -->  newBottomJoin  B
     *  /    \                  /    \
     * A      B                A      C
     */
    @Override
    public Rule build() {
        return innerLogicalJoin(innerLogicalJoin(groupPlan(), groupPlan()), groupPlan()).then(topJoin -> {
            if (!check(topJoin)) {
                return null;
            }

            LogicalJoin<GroupPlan, GroupPlan> bottomJoin = topJoin.left();

            Plan a = bottomJoin.left();
            Plan b = bottomJoin.right();
            Plan c = topJoin.right();

            Optional<Expression> optTopJoinOnClause = topJoin.getCondition();
            // inner join, onClause can't be empty().
            Preconditions.checkArgument(optTopJoinOnClause.isPresent(),
                    "bottomJoin in inner join, onClause must be present.");
            Expression topJoinOnClause = optTopJoinOnClause.get();
            Optional<Expression> optBottomJoinOnClause = bottomJoin.getCondition();
            Preconditions.checkArgument(optBottomJoinOnClause.isPresent(),
                    "bottomJoin in inner join, onClause must be present.");
            Expression bottomJoinOnClause = optBottomJoinOnClause.get();

            List<SlotReference> aOutputSlots = a.getOutput().stream().map(slot -> (SlotReference) slot)
                    .collect(Collectors.toList());
            List<SlotReference> bOutputSlots = b.getOutput().stream().map(slot -> (SlotReference) slot)
                    .collect(Collectors.toList());
            List<SlotReference> cOutputSlots = c.getOutput().stream().map(slot -> (SlotReference) slot)
                    .collect(Collectors.toList());

            // Ignore join with some OnClause like:
            // Join C = B + A for above example.
            List<Expression> topJoinOnClauseConjuncts = ExpressionUtils.extractConjunction(topJoinOnClause);
            for (Expression topJoinOnClauseConjunct : topJoinOnClauseConjuncts) {
                if (ExpressionUtils.isIntersecting(topJoinOnClauseConjunct.collect(SlotReference.class::isInstance),
                        aOutputSlots)
                        && ExpressionUtils.isIntersecting(
                        topJoinOnClauseConjunct.collect(SlotReference.class::isInstance),
                        bOutputSlots)
                        && ExpressionUtils.isIntersecting(
                        topJoinOnClauseConjunct.collect(SlotReference.class::isInstance),
                        cOutputSlots)
                ) {
                    return null;
                }
            }
            List<Expression> bottomJoinOnClauseConjuncts = ExpressionUtils.extractConjunction(bottomJoinOnClause);

            List<Expression> allOnCondition = Lists.newArrayList();
            allOnCondition.addAll(topJoinOnClauseConjuncts);
            allOnCondition.addAll(bottomJoinOnClauseConjuncts);

            List<SlotReference> newBottomJoinSlots = Lists.newArrayList();
            newBottomJoinSlots.addAll(aOutputSlots);
            newBottomJoinSlots.addAll(cOutputSlots);

            List<Expression> newBottomJoinOnCondition = Lists.newArrayList();
            List<Expression> newTopJoinOnCondition = Lists.newArrayList();
            for (Expression onCondition : allOnCondition) {
                List<SlotReference> slots = onCondition.collect(SlotReference.class::isInstance);
                if (new HashSet<>(newBottomJoinSlots).containsAll(slots)) {
                    newBottomJoinOnCondition.add(onCondition);
                } else {
                    newTopJoinOnCondition.add(onCondition);
                }
            }

            // newBottomJoinOnCondition/newTopJoinOnCondition is empty. They are cross join.
            // Example:
            // A: col1, col2. B: col2, col3. C: col3, col4
            // (A & B on A.col2=B.col2) & C on B.col3=C.col3.
            // If (A & B) & C -> (A & C) & B.
            // (A & C) will be cross join (newBottomJoinOnCondition is empty)
            if (newBottomJoinOnCondition.isEmpty() || newTopJoinOnCondition.isEmpty()) {
                return null;
            }

            // new bottom join (a, c)
            LogicalJoin newBottomJoin = new LogicalJoin(
                    bottomJoin.getJoinType(),
                    Optional.of(ExpressionUtils.and(newBottomJoinOnCondition)),
                    a, c);

            // TODO: add column map (use project)
            // SlotReference bind() may have solved this problem.
            // source: | A       | B | C      |
            // target: | A       | C      | B |

            // new top join: b
            LogicalJoin newTopJoin = new LogicalJoin(
                    topJoin.getJoinType(),
                    Optional.of(ExpressionUtils.and(newTopJoinOnCondition)),
                    newBottomJoin, b);

            return newTopJoin;
        }).toRule(RuleType.LOGICAL_JOIN_L_ASSCOM);
    }

    private boolean check(LogicalJoin topJoin) {
        if (topJoin.getJoinReorderContext().hasCommute()) {
            return false;
        }
        return true;
    }
}
