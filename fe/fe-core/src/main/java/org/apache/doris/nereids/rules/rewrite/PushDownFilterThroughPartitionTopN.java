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
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalPartitionTopN;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSet.Builder;

import java.util.HashSet;
import java.util.Set;

/**
 * Push down the 'filter' pass through the 'partitionTopN' if filter key is partitionTopN's partition key.
 * Logical plan tree:
 *                 any_node
 *                   |
 *                filter (a <= 100)
 *                   |
 *                partition topn (PARTITION BY a)
 *                   |
 *                 any_node
 * transformed to:
 *                 any_node
 *                   |
 *                partition topn (PARTITION BY a)
 *                   |
 *                filter (a <= 100)
 *                   |
 *                 any_node
 */

public class PushDownFilterThroughPartitionTopN extends OneRewriteRuleFactory {

    @Override
    public Rule build() {
        return logicalFilter(logicalPartitionTopN()).thenApply(ctx -> {
            LogicalFilter<LogicalPartitionTopN<Plan>> filter = ctx.root;
            LogicalPartitionTopN<Plan> partitionTopN = filter.child();
            // follow the similar checking and transformation rule as
            // PushdownFilterThroughWindow
            Builder<Expression> bottomConjunctsBuilder = ImmutableSet.builder();
            Builder<Expression> upperConjunctsBuilder = ImmutableSet.builder();
            Set<SlotReference> partitionKeySlots = new HashSet<>();
            for (Expression partitionKey : partitionTopN.getPartitionKeys()) {
                if (partitionKey instanceof SlotReference) {
                    partitionKeySlots.add((SlotReference) partitionKey);
                }
            }
            for (Expression expr : filter.getConjuncts()) {
                Set<Slot> exprInputSlots = expr.getInputSlots();
                if (partitionKeySlots.containsAll(exprInputSlots)) {
                    bottomConjunctsBuilder.add(expr);
                } else {
                    upperConjunctsBuilder.add(expr);
                }
            }
            ImmutableSet<Expression> bottomConjuncts = bottomConjunctsBuilder.build();
            ImmutableSet<Expression> upperConjuncts = upperConjunctsBuilder.build();
            if (bottomConjuncts.isEmpty()) {
                return null;
            }

            LogicalFilter<Plan> bottomFilter = new LogicalFilter<>(bottomConjuncts, partitionTopN.child());
            partitionTopN = (LogicalPartitionTopN<Plan>) partitionTopN.withChildren(bottomFilter);
            if (upperConjuncts.isEmpty()) {
                return partitionTopN;
            } else {
                return filter.withConjunctsAndChild(upperConjuncts, partitionTopN);
            }
        }).toRule(RuleType.PUSH_DOWN_FILTER_THROUGH_PARTITION_TOPN);
    }

}
