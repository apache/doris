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
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Project;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Convert an inner join to a left semi join when the inner join is only used as an
 * existence filter. Three conditions must be satisfied at the same time:
 *
 * 1. The right side columns of the join do not leak: every column referenced above the
 *    join comes from the left side, i.e. the right side is only used in the join
 *    conditions. (the "existence filter" property)
 * 2. All join conditions are equal conjuncts: hashJoinConjuncts is not empty and
 *    otherJoinConjuncts is empty, so the join is a pure equi-join.
 * 3. There is a deduplication guarantee above the join: the aggregate that consumes the
 *    join output is a DISTINCT-like aggregate, i.e. its group-by keys cover exactly its
 *    output columns. Otherwise, in bag semantics, the row multiplication of an inner
 *    join (a left row matching N right rows produces N copies) would change the result
 *    after the conversion, because a semi join never multiplies rows.
 *
 * Example:
 * <pre>
 *   select distinct a1.* from a1, a5
 *   where a1.lot_id = a5.lot_id and a1.ope_no = a5.ope_no and ...
 *   ======>
 *   select distinct a1.* from a1 left semi join a5
 *   on a1.lot_id = a5.lot_id and a1.ope_no = a5.ope_no and ...
 * </pre>
 *
 * The conversion avoids row multiplication (the output row count stays the left side
 * cardinality instead of being multiplied by the average number of right side matches),
 * and lets the right side be scanned/broadcast with only the join key columns.
 */
public class ConvertInnerJoinToSemiJoin implements RewriteRuleFactory {
    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                // Aggregate -> InnerJoin
                logicalAggregate(innerLogicalJoin()
                        .when(this::canConvertToSemiJoin))
                        .when(this::isDistinctLikeAggregate)
                        .thenApply(ctx -> convert(ctx.root, ctx.root.child()))
                        .toRule(RuleType.CONVERT_INNER_JOIN_TO_SEMI_JOIN),
                // Aggregate -> Project -> InnerJoin, where the project is a pure slot projection
                logicalAggregate(logicalProject(innerLogicalJoin()
                        .when(this::canConvertToSemiJoin))
                        .when(Project::isAllSlots))
                        .when(this::isDistinctLikeAggregate)
                        .thenApply(ctx -> convert(ctx.root, ctx.root.child(), ctx.root.child().child()))
                        .toRule(RuleType.CONVERT_INNER_JOIN_TO_SEMI_JOIN)
        );
    }

    /**
     * Condition 2: the join is a pure equi-join (hash conjuncts exist and no other
     * conjuncts), and it is not a mark join.
     */
    private boolean canConvertToSemiJoin(LogicalJoin<?, ?> join) {
        return !join.isMarkJoin()
                && !join.getHashJoinConjuncts().isEmpty()
                && join.getOtherJoinConjuncts().isEmpty();
    }

    /**
     * Condition 3: the aggregate is a DISTINCT-like aggregate, i.e. its group-by keys
     * cover exactly its output columns, so it collapses duplicate rows and the row
     * multiplicity change of inner-join -> semi-join does not affect the final result.
     */
    private boolean isDistinctLikeAggregate(LogicalAggregate<?> agg) {
        Set<ExprId> groupBySlotIds = agg.getGroupByExpressions().stream()
                .filter(Slot.class::isInstance)
                .map(expr -> ((Slot) expr).getExprId())
                .collect(Collectors.toSet());
        Set<ExprId> outputSlotIds = agg.getOutput().stream()
                .map(Slot::getExprId)
                .collect(Collectors.toSet());
        return groupBySlotIds.equals(outputSlotIds);
    }

    /** Aggregate -> Join */
    private Plan convert(LogicalAggregate<?> agg, LogicalJoin<?, ?> join) {
        // Condition 1: the right side columns do not leak above the join.
        if (!join.left().getOutputSet().containsAll(agg.getInputSlots())) {
            return agg;
        }
        return agg.withChildren(join.withJoinType(JoinType.LEFT_SEMI_JOIN));
    }

    /** Aggregate -> Project -> Join */
    private Plan convert(LogicalAggregate<?> agg, LogicalProject<?> project, LogicalJoin<?, ?> join) {
        // Condition 1: the right side columns do not leak above the join.
        // The project is a pure slot projection, so checking the project's input slots
        // covers every column consumed above the join.
        if (!join.left().getOutputSet().containsAll(project.getInputSlots())) {
            return agg;
        }
        return agg.withChildren(project.withChildren(join.withJoinType(JoinType.LEFT_SEMI_JOIN)));
    }
}
