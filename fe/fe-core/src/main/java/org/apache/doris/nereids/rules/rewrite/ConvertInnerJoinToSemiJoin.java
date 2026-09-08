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
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Project;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Set;

/**
 * Convert an inner join to a semi join when the inner join is only used as an
 * existence filter. The output side of the join (the side whose columns are consumed
 * above the join) becomes the probe side of the semi join: a left semi join when the
 * output side is the left child, a right semi join when it is the right child. Three
 * conditions must be satisfied at the same time:
 *
 * 1. The other side columns of the join do not leak: every column referenced above the
 *    join comes from the output side, i.e. the other side is only used in the join
 *    conditions. (the "existence filter" property)
 * 2. All join conditions are equal conjuncts: hashJoinConjuncts is not empty and
 *    otherJoinConjuncts is empty, so the join is a pure equi-join.
 * 3. There is a deduplication guarantee above the join: the aggregate that consumes the
 *    join output has no aggregate functions, i.e. every group-by expression and output
 *    expression is a slot (the DISTINCT-like aggregate check reused from
 *    Aggregate#isDistinct, also used by PushDownLimitDistinctThroughJoin and
 *    PushDownTopNDistinctThroughJoin). Otherwise, in bag semantics, the row
 *    multiplication of an inner join (a probe row matching N other-side rows produces N
 *    copies) would change the result of an aggregate function after the conversion,
 *    because a semi join never multiplies rows. Combined with condition 1, every
 *    group-by key of the aggregate is an output-side column, so the N copies of a probe
 *    row share identical group-by values and collapse to a single group exactly like
 *    the single copy a semi join keeps.
 *
 * All three conditions are enforced in the rule's match predicates, so the rule only
 * fires when the conversion actually applies.
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
 * and symmetrically, when only the right side columns are consumed above the join:
 * <pre>
 *   select distinct a5.* from a1, a5
 *   where a1.lot_id = a5.lot_id and a1.ope_no = a5.ope_no and ...
 *   ======>
 *   select distinct a5.* from a1 right semi join a5
 *   on a1.lot_id = a5.lot_id and a1.ope_no = a5.ope_no and ...
 * </pre>
 *
 * The conversion avoids row multiplication (the output row count stays the output side
 * cardinality instead of being multiplied by the average number of other side matches),
 * and lets the other side be scanned/broadcast with only the join key columns.
 */
public class ConvertInnerJoinToSemiJoin implements RewriteRuleFactory {
    @Override
    public List<Rule> buildRules() {
        ImmutableList.Builder<Rule> rules = ImmutableList.builder();
        for (boolean outputSideIsLeft : new boolean[] {true, false}) {
            JoinType semiJoinType = outputSideIsLeft ? JoinType.LEFT_SEMI_JOIN : JoinType.RIGHT_SEMI_JOIN;
            // Aggregate -> InnerJoin
            rules.add(logicalAggregate(innerLogicalJoin()
                    .when(this::canConvertToSemiJoin))
                    .when(LogicalAggregate::isDistinct)
                    .when(agg -> columnsDoNotLeak(agg.child(), outputSideIsLeft, agg.getInputSlots()))
                    .thenApply(ctx -> convert(ctx.root, ctx.root.child(), semiJoinType))
                    .toRule(RuleType.CONVERT_INNER_JOIN_TO_SEMI_JOIN));
            // Aggregate -> Project -> InnerJoin, where the project is a pure slot projection
            rules.add(logicalAggregate(logicalProject(innerLogicalJoin()
                    .when(this::canConvertToSemiJoin))
                    .when(Project::isAllSlots))
                    .when(LogicalAggregate::isDistinct)
                    .when(agg -> columnsDoNotLeak(agg.child().child(), outputSideIsLeft,
                            agg.child().getInputSlots()))
                    .thenApply(ctx -> convert(ctx.root, ctx.root.child(), ctx.root.child().child(), semiJoinType))
                    .toRule(RuleType.CONVERT_INNER_JOIN_TO_SEMI_JOIN));
        }
        return rules.build();
    }

    /**
     * Condition 2: the join is a pure equi-join (hash conjuncts exist and no other
     * conjuncts), and it is not a mark join. ASOF joins never reach this rule: the
     * pattern only matches JoinType.INNER_JOIN (ASOF joins carry ASOF_* join types),
     * and their MATCH_CONDITION (e.g. a.ts >= b.ts) is kept in otherJoinConjuncts, so
     * the empty-check fails anyway.
     */
    private boolean canConvertToSemiJoin(LogicalJoin<?, ?> join) {
        return !join.isMarkJoin()
                && !join.getHashJoinConjuncts().isEmpty()
                && join.getOtherJoinConjuncts().isEmpty();
    }

    /**
     * Condition 1: the output side of the join keeps every column consumed above the
     * join, i.e. the other side is only used in the join conditions (an existence
     * filter). The output side stays the probe side of the semi join.
     */
    private boolean columnsDoNotLeak(LogicalJoin<?, ?> join, boolean outputSideIsLeft, Set<Slot> consumedSlots) {
        return (outputSideIsLeft ? join.left() : join.right()).getOutputSet().containsAll(consumedSlots);
    }

    /** Aggregate -> Join */
    private Plan convert(LogicalAggregate<?> agg, LogicalJoin<?, ?> join, JoinType semiJoinType) {
        return agg.withChildren(join.withJoinType(semiJoinType));
    }

    /** Aggregate -> Project -> Join */
    private Plan convert(LogicalAggregate<?> agg, LogicalProject<?> project, LogicalJoin<?, ?> join,
            JoinType semiJoinType) {
        return agg.withChildren(project.withChildren(join.withJoinType(semiJoinType)));
    }
}
