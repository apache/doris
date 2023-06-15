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
import org.apache.doris.nereids.trees.expressions.BinaryArithmetic;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;

import java.util.Set;
import java.util.stream.Collectors;

/**
 * Simplify Aggregate group by Multiple to One. For example
 * <p>
 * GROUP BY ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3
 * -->
 * GROUP BY ClientIP
 */
public class SimplifyAggGroupBy extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalAggregate()
                .when(agg -> agg.getGroupByExpressions().size() > 1)
                .when(agg -> agg.getGroupByExpressions().stream().allMatch(this::isBinaryArithmeticSlot))
                .then(agg -> {
                    Set<Expression> slots = agg.getGroupByExpressions().stream()
                            .flatMap(e -> e.getInputSlots().stream()).collect(Collectors.toSet());
                    if (slots.size() != 1) {
                        return null;
                    }
                    return agg.withGroupByAndOutput(ImmutableList.copyOf(slots), agg.getOutputExpressions());
                })
                .toRule(RuleType.SIMPLIFY_AGG_GROUP_BY);
    }

    private boolean isBinaryArithmeticSlot(Expression expr) {
        if (expr instanceof Slot) {
            return true;
        }
        if (!(expr instanceof BinaryArithmetic)) {
            return false;
        }
        return ExpressionUtils.isSlotOrCastOnSlot(expr.child(0)).isPresent() && expr.child(1) instanceof Literal
                || ExpressionUtils.isSlotOrCastOnSlot(expr.child(1)).isPresent() && expr.child(0) instanceof Literal;
    }
}
