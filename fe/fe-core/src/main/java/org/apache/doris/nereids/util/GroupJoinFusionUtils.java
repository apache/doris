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

package org.apache.doris.nereids.util;

import org.apache.doris.nereids.trees.expressions.AggregateExpression;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.EqualPredicate;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.NullSafeEqual;
import org.apache.doris.nereids.trees.expressions.OrderExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.NoneMovableFunction;
import org.apache.doris.nereids.trees.expressions.functions.Udf;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateParam;
import org.apache.doris.nereids.trees.plans.AggMode;
import org.apache.doris.nereids.trees.plans.AggPhase;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.algebra.Aggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;

import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Shared eligibility and ordering rules for GroupJoin fusion
 * (enable_group_join_fusion): fusing an INNER hash join + hash aggregation into a single
 * GroupJoin operator. Both consumers of these rules must stay in sync:
 * <ul>
 * <li>AlignGroupJoinConjunctOrder (post-processor, runs before runtime-filter generation):
 * when an eligible shape's GROUP BY merely permutes the join keys, it reorders the child
 * join's conjunct list to the group-by order (the value returned here);</li>
 * <li>PhysicalPlanTranslator.maybeTranslateToGroupJoin (fusion decision at translation):
 * fuses an eligible shape only when the join's conjuncts are already listed in exactly the
 * group-by order (i.e. the order returned here equals the join's current one), and emits
 * the conjuncts as they are.</li>
 * </ul>
 */
public final class GroupJoinFusionUtils {

    private GroupJoinFusionUtils() {}

    /**
     * Decide whether {@code aggregate} over {@code join} is eligible for GroupJoin fusion and,
     * when it is, return the conjunct order the fused operator requires: a list of the join's
     * own conjunct instances where conjunct i produces the group-by key at position i.
     * <p>
     * The fused GroupJoin operator groups rows by the shared hash key and materializes one
     * grouping-key column per equi-join conjunct: the BE writes the j-th conjunct's key into
     * the j-th output tuple slot, and the FE creates the output tuple slots from the
     * aggregate's group-by expressions in group-by order. The returned columns are therefore
     * correct iff conjunct i produces the group-by key at position i. Since GROUP BY is
     * unordered semantically, any GROUP BY that merely permutes the join keys is eligible -
     * the caller decides whether to reorder the join's conjuncts to the returned order
     * (AlignGroupJoinConjunctOrder) or to require it already (the translator).
     * <p>
     * Returns null when the shape is not eligible (not an INNER hash join, mark join,
     * broadcast join, residual/filter conjuncts, floating or complex join keys,
     * null-safe equal conjuncts, no aggregate
     * functions, aggregates with an internal ORDER BY, an aggregate
     * that is not the final one-phase node (GLOBAL + INPUT_TO_RESULT, per function and node), an
     * unsupported intermediate Project, volatile expressions, or intermediate project slots) or when the
     * group-by keys cannot be mapped one-to-one onto the conjuncts. Session-level gates
     * (enable_group_join_fusion, enable_spill) are checked by the callers, not here.
     *
     * @param project the Project between the aggregate and the join, or null when the aggregate
     *        directly consumes the join. GroupJoin can inline deterministic computed
     *        aggregate arguments and evaluate cross-side expressions on matched row pairs.
     */
    public static List<Expression> alignedConjunctsForGroupJoin(
            Aggregate<?> aggregate, PhysicalProject<?> project, PhysicalHashJoin<?, ?> join) {
        if (join.getJoinType() != JoinType.INNER_JOIN) {
            return null;
        }
        if (project != null) {
            aggregate = inlineJoinBasedProject(aggregate, project);
            if (aggregate == null) {
                return null;
            }
        }
        if (join.isMarkJoin()) {
            return null;
        }
        // The fused GroupJoin operator matches rows purely by the equi-join key and has no
        // per-pair filtering stage, so a residual non-equi ON conjunct cannot be evaluated.
        if (join.isBroadCastJoin() || !join.getOtherJoinConjuncts().isEmpty()
                || !join.getFilterConjuncts().isEmpty()) {
            return null;
        }
        List<Expression> groupByExprs = aggregate.getGroupByExpressions();
        List<Expression> hashJoinConjuncts = join.getHashJoinConjuncts();
        if (groupByExprs.isEmpty() || hashJoinConjuncts.isEmpty()
                || groupByExprs.size() != hashJoinConjuncts.size()) {
            return null;
        }
        List<AggregateExpression> aggregateExpressions = aggregate.getOutputExpressions().stream()
                .flatMap(outputExpr -> outputExpr.collect(AggregateExpression.class::isInstance).stream())
                .map(AggregateExpression.class::cast)
                .collect(Collectors.toList());
        // Pure GROUP BY and DISTINCT keep the regular HashJoin + Aggregate plan. The current
        // GroupJoin execution path is designed around maintaining at least one aggregate state.
        if (aggregateExpressions.isEmpty()) {
            return null;
        }
        // Phase gate: only the final one-phase aggregate is fusable. The fused GroupJoin node
        // materializes FINAL_RESULT and finalizes per-key aggregate state directly, so an
        // aggregate that is a partial/LOCAL buffer producer or a DISTINCT/multi-phase
        // intermediate node must stay on the ordinary HashJoinNode + AggregationNode path.
        // This is observable when two-phase aggregation is forced (agg_phase=2): the LOCAL
        // (INPUT_TO_BUFFER) phase sits directly above the join when no exchange is inserted
        // between them, and fusing it hard-codes FINAL_RESULT with finalize-on evaluators
        // while the merge-finalize aggregate above still consumes the partial buffer - BE then
        // aborts with "Aggregate function count result type check failed: Column type String
        // is not compatible with data type BIGINT". Requiring every output aggregate function's
        // own param to equal the node param also guards split shapes where the node-level label
        // alone lies (e.g. GLOBAL/INPUT_TO_RESULT node label with per-function DISTINCT_* or
        // buffer params), so the gate is per-function, not just node-level.
        if (!(aggregate instanceof PhysicalHashAggregate)) {
            return null;
        }
        AggregateParam nodeParam = ((PhysicalHashAggregate<?>) aggregate).getAggregateParam();
        if (nodeParam.aggPhase != AggPhase.GLOBAL || nodeParam.aggMode != AggMode.INPUT_TO_RESULT) {
            return null;
        }
        for (AggregateExpression aggExpr : aggregateExpressions) {
            // Foreign UDAFs rely on batched JNI/RPC updates; indexed scalar adds lose that contract.
            if (aggExpr.getFunction() instanceof Udf) {
                return null;
            }
            AggregateParam perFunctionParam = aggExpr.getAggregateParam();
            if (!perFunctionParam.equals(nodeParam)) {
                return null;
            }
        }
        // The fused operator evaluates aggregates over the probe/build rows of the join
        // children, so all input slots must be produced by those children after Project inlining.
        Set<Slot> leftOutput = join.left().getOutputSet();
        Set<Slot> rightOutput = join.right().getOutputSet();
        Set<Slot> joinChildrenOutputs = Sets.newHashSet();
        joinChildrenOutputs.addAll(leftOutput);
        joinChildrenOutputs.addAll(rightOutput);
        if (!joinChildrenOutputs.containsAll(aggregate.getInputSlots())) {
            return null;
        }
        // Order-sensitive aggregates (internal ORDER BY, e.g. GROUP_CONCAT(... ORDER BY ...))
        // are not fusable. TGroupJoinAggFunction carries no per-function
        // sort info (unlike AggregationNode's agg_sort_infos), so the translated expression's
        // ORDER BY column would be treated as an ordinary aggregate argument by the BE
        // group-join operators (they always pass an empty TSortInfo) and abort with
        // "Agg Function ... is not implemented". OrderExpression appears under an output
        // expression only inside an aggregate function's argument list.
        for (Expression outputExpr : aggregate.getOutputExpressions()) {
            if (!outputExpr.collect(OrderExpression.class::isInstance).isEmpty()) {
                return null;
            }
            if (outputExpr.containsNondeterministic()
                    || outputExpr.containsType(NoneMovableFunction.class)) {
                return null;
            }
        }
        // Each conjunct operand must live entirely on one join child, on opposite children,
        // and each group-by expression must be a bare slot of the join children.
        for (Expression groupByExpr : groupByExprs) {
            if (!(groupByExpr instanceof SlotReference)) {
                return null;
            }
            Slot groupBySlot = (SlotReference) groupByExpr;
            if (!leftOutput.contains(groupBySlot) && !rightOutput.contains(groupBySlot)) {
                return null;
            }
        }
        List<EqualPredicate> equalConjuncts = new ArrayList<>();
        for (Expression conjunct : hashJoinConjuncts) {
            // Null-safe equal (a <=> b) is not fusable: the BE group-join node rejects
            // EQ_FOR_NULL hash conjuncts (validate_group_join_node), so such joins stay on
            // the regular HashJoinNode + AggregationNode path, which preserves the
            // null-safe matching semantics.
            if (!(conjunct instanceof EqualPredicate) || conjunct instanceof NullSafeEqual) {
                return null;
            }
            EqualPredicate eq = (EqualPredicate) conjunct;
            // Normalization inside GroupJoin is too late to fix partitioning of signed zero
            // or nested floating-point values. Keep these keys on the regular Join + Agg path.
            if (eq.left().getDataType().isFloatLikeType() || eq.left().getDataType().isComplexType()
                    || eq.right().getDataType().isFloatLikeType()
                    || eq.right().getDataType().isComplexType()) {
                return null;
            }
            Set<Slot> leftSide = eq.left().getInputSlots();
            Set<Slot> rightSide = eq.right().getInputSlots();
            if (!((leftOutput.containsAll(leftSide) && rightOutput.containsAll(rightSide))
                    || (leftOutput.containsAll(rightSide) && rightOutput.containsAll(leftSide)))) {
                return null;
            }
            equalConjuncts.add(eq);
        }
        // Greedily match every group-by expression, in order, to a distinct conjunct whose join
        // key is exactly that expression; if any expression cannot be matched (or the keys are
        // duplicated ambiguously) the shape is not fusable.
        List<Expression> alignedConjuncts = new ArrayList<>(equalConjuncts.size());
        boolean[] conjunctUsed = new boolean[equalConjuncts.size()];
        for (Expression groupByExpr : groupByExprs) {
            SlotReference groupBySlot = (SlotReference) groupByExpr;
            int matched = -1;
            for (int i = 0; i < equalConjuncts.size(); i++) {
                if (conjunctUsed[i]) {
                    continue;
                }
                EqualPredicate eq = equalConjuncts.get(i);
                if (isSameSlot(eq.left(), groupBySlot) || isSameSlot(eq.right(), groupBySlot)) {
                    matched = i;
                    break;
                }
            }
            if (matched < 0) {
                return null;
            }
            conjunctUsed[matched] = true;
            alignedConjuncts.add(equalConjuncts.get(matched));
        }
        return alignedConjuncts;
    }

    /** Resolve aggregate arguments to child expressions without changing output ExprIds. */
    public static Aggregate<?> inlineJoinBasedProject(Aggregate<?> aggregate, PhysicalProject<?> project) {
        Map<Expression, Expression> replacements = new HashMap<>();
        for (NamedExpression output : project.getProjects()) {
            Expression input = output instanceof Alias ? ((Alias) output).child() : output;
            // Moving error checks or volatile expressions changes the evaluation domain.
            if (input.containsNondeterministic() || input.containsType(NoneMovableFunction.class)) {
                return null;
            }
            replacements.put(output.toSlot(), input);
        }
        // Group keys must still be raw equi-join slots. Computed grouping remains unfused.
        for (Expression key : aggregate.getGroupByExpressions()) {
            Expression input = replacements.get(key);
            if (input != null && !input.equals(key)) {
                return null;
            }
        }
        List<NamedExpression> outputs = aggregate.getOutputExpressions().stream()
                .map(output -> (NamedExpression) ExpressionUtils.replace(output, replacements))
                .collect(Collectors.toList());
        return aggregate.withAggOutput(outputs);
    }

    /** Whether two conjunct lists contain the same conjunct instances in the same order. */
    public static boolean sameConjunctOrder(List<Expression> a, List<Expression> b) {
        if (a.size() != b.size()) {
            return false;
        }
        for (int i = 0; i < a.size(); i++) {
            if (a.get(i) != b.get(i)) {
                return false;
            }
        }
        return true;
    }

    private static boolean isSameSlot(Expression expr, Slot slot) {
        return expr instanceof SlotReference
                && ((SlotReference) expr).getExprId().equals(slot.getExprId());
    }
}
