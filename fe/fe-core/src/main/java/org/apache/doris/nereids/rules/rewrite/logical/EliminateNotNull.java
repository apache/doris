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
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.PlanUtils;
import org.apache.doris.nereids.util.TypeUtils;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * EliminateNotNull.
 */
public class EliminateNotNull extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalFilter().then(filter -> {
            // 1. get all slots from `is not null`.
            // 2. infer nonNullable slots.
            // 3. remove `is not null` predicates.
            Set<Expression> removeIsNotNullPredicates = Sets.newHashSet();
            List<Slot> slotsFromIsNotNull = Lists.newArrayList();
            for (Expression predicate : filter.getConjuncts()) {
                Optional<Slot> notNullSlot = TypeUtils.isNotNull(predicate);
                if (notNullSlot.isPresent()) {
                    slotsFromIsNotNull.add(notNullSlot.get());
                } else {
                    removeIsNotNullPredicates.add(predicate);
                }
            }

            Set<Slot> nonNullableSlots = ExpressionUtils.inferNotNullSlots(removeIsNotNullPredicates);
            slotsFromIsNotNull.forEach(slot -> {
                if (slot.notNullable()) {
                    nonNullableSlots.add(slot);
                }
            });

            Set<Expression> keepIsNotNull = slotsFromIsNotNull.stream().filter(slot -> !nonNullableSlots.contains(slot))
                    .map(slot -> new Not(new IsNull(slot))).collect(Collectors.toSet());

            // merge removeIsNotNullPredicates and newIsNotNull into a new ImmutableSet
            Set<Expression> newPredicates = ImmutableSet.<Expression>builder().addAll(removeIsNotNullPredicates)
                    .addAll(keepIsNotNull).build();
            return PlanUtils.filterOrSelf(newPredicates, filter.child());
        }).toRule(RuleType.ELIMINATE_NOT_NULL);
    }
}
