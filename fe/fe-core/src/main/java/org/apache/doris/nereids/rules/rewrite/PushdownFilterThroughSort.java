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
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.util.ExpressionUtils;

import java.util.Set;

/**
 * Push down filter through sort.
 * input:
 * filter -> sort
 * output:
 * [sort] -> filter
 * The filter can be directly push down to the sort.
 * Note when the sort key is equal to Literal, the sort can be eliminated
 */
public class PushdownFilterThroughSort extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalFilter(logicalSort()).then(filter -> {
            LogicalSort<Plan> sort = filter.child();
            if (checkSlotsConstant(sort.getInputSlots(), filter)) {
                return new LogicalFilter<>(filter.getConjuncts(), sort.child());
            }
            return sort.withChildren(new LogicalFilter<>(filter.getConjuncts(), sort.child()));
        }).toRule(RuleType.PUSHDOWN_FILTER_THROUGH_SORT);
    }

    boolean checkSlotsConstant(Set<Slot> slots, LogicalFilter<? extends Plan> filter) {
        return slots.stream().allMatch(slot -> ExpressionUtils.checkSlotConstant(slot, filter.getConjuncts()));
    }
}
