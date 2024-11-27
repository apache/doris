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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;

import com.google.common.collect.Sets;

import java.util.Set;

/**
 * Push down the 'filter' into the 'window' if filter key is window partition key.
 * Logical plan tree:
 *                 any_node
 *                   |
 *                filter (a <= 100)
 *                   |
 *                window (PARTITION BY a ORDER BY b)
 *                   |
 *                 any_node
 * transformed to:
 *                 any_node
 *                   |
 *                window (PARTITION BY a ORDER BY b)
 *                   |
 *                filter (a <= 100)
 *                   |
 *                 any_node
 */

public class PushDownFilterThroughWindow extends OneRewriteRuleFactory {

    @Override
    public Rule build() {
        return logicalFilter(logicalWindow()).thenApply(ctx -> {
            LogicalFilter<LogicalWindow<Plan>> filter = ctx.root;
            LogicalWindow<Plan> window = filter.child();
            // now we only handle single slot used as partition key
            // for example:
            // select * from (select T.*, rank() over(partition by c2+c3 order by c4) rn from T) abc where c2=1;
            // c2=1 cannot be pushed down.
            Set<SlotReference> commonPartitionKeys = window.getCommonPartitionKeyFromWindowExpressions();
            Set<Expression> bottomConjuncts = Sets.newHashSet();
            Set<Expression> upperConjuncts = Sets.newHashSet();
            for (Expression expr : filter.getConjuncts()) {
                if (commonPartitionKeys.containsAll(expr.getInputSlots())) {
                    bottomConjuncts.add(expr);
                } else {
                    upperConjuncts.add(expr);
                }
            }
            if (bottomConjuncts.isEmpty()) {
                return null;
            }

            LogicalFilter<Plan> bottomFilter = new LogicalFilter<>(bottomConjuncts, window.child());
            window = (LogicalWindow<Plan>) window.withChildren(bottomFilter);
            if (upperConjuncts.isEmpty()) {
                return window;
            } else {
                LogicalFilter<Plan> upperFilter = (LogicalFilter<Plan>) filter
                        .withConjuncts(upperConjuncts).withChildren(window);
                return upperFilter;
            }
        }).toRule(RuleType.PUSH_DOWN_FILTER_THROUGH_WINDOW);
    }

}
