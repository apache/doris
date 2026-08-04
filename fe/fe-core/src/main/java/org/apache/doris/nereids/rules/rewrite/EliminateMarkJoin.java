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
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.MarkJoinSlotReference;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Eliminate a mark join whose mark slot is only consumed, as a bare conjunct, by the filter
 * directly above it.
 *
 * A LEFT SEMI mark join outputs every left row together with a three-valued mark slot
 * (TRUE / FALSE / NULL) recording whether the semi condition matched. A filter that requires
 * the mark slot to be TRUE discards NULL rows exactly like FALSE rows, so the pair degenerates
 * to a plain LEFT SEMI JOIN over the same conjuncts:
 * <pre>
 * filter(m and rest)                     project(join output, TRUE as m)
 * +--join(LEFT SEMI, mark slot m)  =>    +--filter(rest)
 *                                           +--join(LEFT SEMI)
 * </pre>
 * The literal-TRUE alias keeps the mark slot's ExprId alive for references above the filter
 * (after the filter the mark can only be TRUE); column pruning removes it when unused.
 *
 * Besides being cheaper to execute, this matters because {@code RuntimeFilterGenerator}
 * refuses to generate runtime filters on mark joins, so a residual mark join needlessly
 * disables runtime filter pruning on the probe side scan. This shape typically arises from
 * an IN/EXISTS subquery written inside a join ON clause: the subquery is unnested into a mark
 * join before predicate push down moves the mark conjunct, so the non-mark unnesting path
 * never gets a chance to apply.
 */
public class EliminateMarkJoin extends OneRewriteRuleFactory {

    @Override
    public Rule build() {
        return logicalFilter(logicalJoin()
                .when(join -> join.isMarkJoin() && join.getJoinType() == JoinType.LEFT_SEMI_JOIN))
                .when(EliminateMarkJoin::markSlotOnlyUsedAsBareConjunct)
                .then(EliminateMarkJoin::eliminateMarkJoin)
                .toRule(RuleType.ELIMINATE_MARK_JOIN);
    }

    private static boolean markSlotOnlyUsedAsBareConjunct(LogicalFilter<LogicalJoin<Plan, Plan>> filter) {
        MarkJoinSlotReference markSlot = filter.child().getMarkJoinSlotReference().get();
        boolean hasBareMarkConjunct = false;
        for (Expression conjunct : filter.getConjuncts()) {
            if (conjunct.equals(markSlot)) {
                hasBareMarkConjunct = true;
            } else if (conjunct.getInputSlots().contains(markSlot)) {
                // the mark slot takes part in a compound expression, e.g. OR(m, x): FALSE and
                // NULL marks are distinguishable there, so the mark join must be kept
                return false;
            }
        }
        return hasBareMarkConjunct;
    }

    private static Plan eliminateMarkJoin(LogicalFilter<LogicalJoin<Plan, Plan>> filter) {
        LogicalJoin<Plan, Plan> join = filter.child();
        MarkJoinSlotReference markSlot = join.getMarkJoinSlotReference().get();
        // requiring mark = TRUE collapses the three-valued mark semantics (NULL is discarded
        // just like FALSE), so mark conjuncts can be evaluated as ordinary join conjuncts
        List<Expression> otherConjuncts = join.getMarkJoinConjuncts().isEmpty()
                ? join.getOtherJoinConjuncts()
                : ImmutableList.<Expression>builder()
                        .addAll(join.getOtherJoinConjuncts())
                        .addAll(join.getMarkJoinConjuncts())
                        .build();
        LogicalJoin<Plan, Plan> newJoin = new LogicalJoin<>(join.getJoinType(),
                join.getHashJoinConjuncts(), otherConjuncts, ExpressionUtils.EMPTY_CONDITION,
                join.getDistributeHint(), Optional.empty(), join.left(), join.right(),
                join.getJoinReorderContext());
        Set<Expression> remainingConjuncts = filter.getConjuncts().stream()
                .filter(conjunct -> !conjunct.equals(markSlot))
                .collect(ImmutableSet.toImmutableSet());
        Plan child = remainingConjuncts.isEmpty()
                ? newJoin : new LogicalFilter<>(remainingConjuncts, newJoin);
        ImmutableList.Builder<NamedExpression> projects = ImmutableList.builder();
        projects.addAll(newJoin.getOutput());
        projects.add(new Alias(markSlot.getExprId(), BooleanLiteral.TRUE, markSlot.getName()));
        return new LogicalProject<>(projects.build(), child);
    }
}
