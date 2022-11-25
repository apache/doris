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
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.PlanUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Set;

/**
 * Push the predicate in the LogicalFilter to the repeat child.
 * For example:
 * Logical plan tree:
 *                 any_node
 *                   |
 *                filter (a>0 and b>0)
 *                   |
 *                repeat(a, c)
 *                   |
 *                 scan
 * transformed to:
 *                 project
 *                   |
 *              upper filter (b>0)
 *                   |
 *                repeat(a, c)
 *                   |
 *              bottom filter (a>0)
 *                   |
 *                 scan
 * Note:
 *    'a>0' could be push down, because 'a' is in group by keys;
 *    but 'b>0' could not push down, because 'b' is not in group by keys.
 *
 */

public class PushdownFilterThroughRepeat extends OneRewriteRuleFactory {

    @Override
    public Rule build() {
        return logicalFilter(logicalRepeat()).then(filter -> {
            LogicalRepeat<GroupPlan> repeat = filter.child();
            Set<Expression> commonGroupingSetExpressions = repeat.getCommonGroupingSetExpressions();
            if (commonGroupingSetExpressions.isEmpty()) {
                return filter;
            }

            List<Expression> pushedPredicates = Lists.newArrayList();
            List<Expression> notPushedPredicates = Lists.newArrayList();
            for (Expression conjunct : ExpressionUtils.extractConjunction(filter.getPredicates())) {
                Set<Slot> conjunctSlots = conjunct.getInputSlots();
                if (commonGroupingSetExpressions.containsAll(conjunctSlots)) {
                    pushedPredicates.add(conjunct);
                } else {
                    notPushedPredicates.add(conjunct);
                }
            }
            return pushDownPredicate(filter, repeat, pushedPredicates, notPushedPredicates);
        }).toRule(RuleType.PUSHDOWN_PREDICATE_THROUGH_REPEAT);
    }

    private Plan pushDownPredicate(LogicalFilter filter, LogicalRepeat repeat,
                                   List<Expression> pushedPredicates, List<Expression> notPushedPredicates) {
        if (pushedPredicates.size() == 0) {
            // nothing pushed down, just return origin plan
            return filter;
        }
        LogicalFilter bottomFilter = new LogicalFilter<>(ExpressionUtils.and(pushedPredicates),
                repeat.child(0));

        repeat = repeat.withChildren(ImmutableList.of(bottomFilter));
        return PlanUtils.filterOrSelf(notPushedPredicates, repeat);
    }
}
