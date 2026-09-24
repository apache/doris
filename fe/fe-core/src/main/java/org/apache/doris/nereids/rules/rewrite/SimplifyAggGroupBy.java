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
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Multiply;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.Subtract;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Abs;
import org.apache.doris.nereids.trees.expressions.functions.scalar.IsInf;
import org.apache.doris.nereids.trees.expressions.functions.scalar.IsNan;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.types.DataType;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Remove deterministic grouping expressions whose inputs are already bare grouping slots.
 * <p>
 * GROUP BY ClientIP, ClientIP + 1, ClientIP + 2
 * -->
 * GROUP BY ClientIP
 *
 * <p>Retain existing bare slots so aggregate outputs can still reference them. A cast group key
 * cannot generally provide its original slot to those outputs. Never synthesize a slot from
 * derived keys, since doing so can split groups formed by non-injective expressions.</p>
 */
public class SimplifyAggGroupBy extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalAggregate()
                .when(agg -> agg.getGroupByExpressions().size() > 1)
                .then(agg -> {
                    List<Expression> simplified = simplifyGroupBy(agg.getGroupByExpressions());
                    if (simplified == null) {
                        return null;
                    }
                    return agg.withGroupByAndOutput(simplified, agg.getOutputExpressions());
                })
                .toRule(RuleType.SIMPLIFY_AGG_GROUP_BY);
    }

    @VisibleForTesting
    protected static List<Expression> simplifyGroupBy(List<Expression> groupByExpressions) {
        Set<Expression> distinctGroupBy = new LinkedHashSet<>(groupByExpressions);
        Set<Expression> determinants = distinctGroupBy.stream()
                .filter(Slot.class::isInstance).collect(ImmutableSet.toImmutableSet());
        // Keep at least one key: removing all constant keys changes the result for empty input.
        if (!determinants.isEmpty()) {
            distinctGroupBy.removeIf(expression -> !(expression instanceof Slot)
                    && !expression.containsVolatileOrNoneMovableExpression()
                    && !expression.containsNondeterministic()
                    && determinants.containsAll(expression.getInputSlots())
                    && preservesGroupingEquality(expression));
        }
        return distinctGroupBy.size() == groupByExpressions.size() ? null : ImmutableList.copyOf(distinctGroupBy);
    }

    /**
     * Doris grouping equality merges signed zeros and NaN payloads. A dependent expression
     * can be removed only if it maps each such equivalence class to one grouping result.
     * Addition, subtraction, multiplication, abs, floating casts, isnan, and isinf
     * preserve those classes. Signbit, atan2, pow, and string casts can distinguish
     * their members, so unreviewed float-dependent operations stay.
     */
    private static boolean preservesGroupingEquality(Expression expression) {
        Set<Slot> inputSlots = expression.getInputSlots();
        if (inputSlots.stream().anyMatch(slot -> !hasExactGroupingEquality(slot)
                && !slot.getDataType().isFloatLikeType())) {
            return false;
        }
        if (inputSlots.stream().noneMatch(slot -> slot.getDataType().isFloatLikeType())) {
            return true;
        }
        if (expression instanceof Slot || expression instanceof Literal) {
            return true;
        }
        if (expression instanceof Cast && !expression.getDataType().isFloatLikeType()) {
            return false;
        }
        if (!(expression instanceof Cast || expression instanceof Add || expression instanceof Subtract
                || expression instanceof Multiply || expression instanceof Abs
                || expression instanceof IsNan || expression instanceof IsInf)) {
            return false;
        }
        return expression.children().stream().allMatch(SimplifyAggGroupBy::preservesGroupingEquality);
    }

    private static boolean hasExactGroupingEquality(Slot slot) {
        DataType type = slot.getDataType();
        return type.isIntegralType() || type.isDecimalLikeType() || type.isBooleanType();
    }
}
