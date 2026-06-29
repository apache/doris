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

package org.apache.doris.mtmv.ivm;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.mtmv.ivm.agg.IvmAggApplyContext;
import org.apache.doris.mtmv.ivm.agg.IvmAggDeltaSlotRef;
import org.apache.doris.mtmv.ivm.agg.IvmAggExpressionBuilder;
import org.apache.doris.mtmv.ivm.agg.IvmAggFunctionRegistry;
import org.apache.doris.mtmv.ivm.agg.IvmAggMeta;
import org.apache.doris.mtmv.ivm.agg.IvmAggTarget;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.analysis.BindRelation;
import org.apache.doris.nereids.rules.exploration.join.JoinReorderContext;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Coalesce;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PreAggStatus;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Aggregate delta rewrite handler for IVM.
 *
 * <p>Non-aggregate nodes are handled by the linear and outer-join handlers. Aggregate
 * nodes return an apply plan from {@link #rewriteAggregate(LogicalAggregate,
 * IvmDeltaRewriteVisitor, IvmRefreshContext)} with an aggregate-level dml_factor.
 *
 * <p>Handles single-table aggregate MVs with count/sum/avg/min/max.
 * Min/max use an assert_true guard: if a deleted row matches the current extreme,
 * execution fails with a reason that requires full refresh recovery.
 *
 * <h3>Overall flow</h3>
 * <ol>
 *   <li><b>Delta sub-plan</b>: transforms the normalized aggregate into a signed delta aggregate
 *       where each output is weighted by {@code dml_factor} (+1 for inserts, -1 for deletes).</li>
 *   <li><b>Apply plan</b>: RIGHT JOINs the MV's current state against the delta on {@code row_id},
 *       computes new hidden states (COALESCE(old,0) + delta), derives visible values, and
 *       maps the final row state to {@code __DORIS_IVM_DML_FACTOR_COL__}.</li>
 *   <li><b>Insert command</b>: wraps the result in an {@code InsertIntoTableCommand} that writes
 *       back to the MV via MOW upsert semantics.</li>
 * </ol>
 *
 * <h3>Visitor integration</h3>
 * <p>The visitor calls {@code rewriteAggregate} as the main entry point that builds delta + apply.
 * Projects above the aggregate are then handled by the linear handler like other normalized projects.
 */
class IvmAggDeltaHandler {

    private final IvmDeltaRewriteHelper helper = IvmDeltaRewriteHelper.INSTANCE;
    private final IvmAggFunctionRegistry aggFunctionRegistry = IvmAggFunctionRegistry.INSTANCE;
    private final IvmAggExpressionBuilder aggExpressionBuilder = IvmAggExpressionBuilder.INSTANCE;

    /**
     * Intermediate result from {@link #buildDeltaSubPlan}.
     * Carries the delta aggregate project plus slot mappings needed by {@link #buildApplyPlan}.
     */
    static final class DeltaPlanParts {
        /** Top project above the delta aggregate: [row_id, group_keys, delta_agg_outputs...] */
        private final LogicalProject<?> topDeltaProject;
        /** Row-id slot from the top project (hash of group keys, or 0 for scalar). */
        private final Slot rowIdSlot;
        /** Delta group-count slot resolved from topDeltaProject output. */
        private final Slot deltaGroupCountSlot;
        /** Per-target delta slots consumed by aggregate function processors during apply. */
        private final Map<IvmAggDeltaSlotRef, Slot> applyDeltaSlots;
        /** Group key slots resolved from topDeltaProject output, keyed by column name. */
        private final Map<String, Slot> groupKeySlotsByName;

        private DeltaPlanParts(LogicalProject<?> topDeltaProject, Slot rowIdSlot, Slot deltaGroupCountSlot,
                Map<IvmAggDeltaSlotRef, Slot> applyDeltaSlots, Map<String, Slot> groupKeySlotsByName) {
            this.topDeltaProject = topDeltaProject;
            this.rowIdSlot = rowIdSlot;
            this.deltaGroupCountSlot = deltaGroupCountSlot;
            this.applyDeltaSlots = applyDeltaSlots;
            this.groupKeySlotsByName = groupKeySlotsByName;
        }
    }

    /**
     * Rewrites an aggregate subtree for IVM delta refresh.
     *
     * <p>Takes a pre-processed child where dml_factor is already injected by the visitor
     * on each branch before UNION ALL merging, and is called directly by
     * {@code IvmDeltaRewriter} after the AGG is re-attached on top of the merged child.
     */
    IvmDeltaRewriteResult rewriteAggregate(
            LogicalAggregate<? extends Plan> agg,
            IvmDeltaRewriteResult childResult,
            IvmRefreshContext context) {
        IvmNormalizeResult normalizeResult = context.getNormalizeResult();
        if (normalizeResult == null) {
            throw new AnalysisException("IVM agg delta rewrite requires normalize result");
        }
        IvmAggMeta aggMeta = normalizeResult.getAggMeta();
        if (aggMeta == null) {
            throw new AnalysisException("IVM agg delta rewrite requires aggregate metadata");
        }
        DeltaPlanParts delta = buildDeltaSubPlan(agg, childResult, aggMeta);
        LogicalProject<?> applyProject = buildApplyPlan(agg, delta, aggMeta, context);
        Slot dmlFactorSlot = helper.findSlotByName(applyProject.getOutput(), Column.IVM_DML_FACTOR_COL);
        return new IvmDeltaRewriteResult(applyProject, dmlFactorSlot);
    }

    /**
     * Builds the delta sub-plan: a signed aggregate over the base table's changes.
     *
     * <p>Input shape (from normalize):
     * <pre>
     *   Aggregate(normalized) → child subtree (with dml_factor injected)
     * </pre>
     *
     * <p>Output shape:
     * <pre>
     *   Project(row_id, group_keys, coalesced delta outputs...)
     *     └── Aggregate(delta: SUM(signed_expr), SUM(case_when_not_null), ...)
     *           └── child subtree with dml_factor
     * </pre>
     *
     * <p>The delta aggregate replaces each original agg function with signed delta expressions:
     * <ul>
     *   <li>COUNT(*): delta = SUM(dml_factor)</li>
     *   <li>COUNT(expr): delta = SUM(IF(expr IS NULL, 0, dml_factor))</li>
     *   <li>SUM(expr): delta_sum = SUM(IF(dml_factor > 0, expr, -expr)),
     *                  delta_count = SUM(IF(expr IS NULL, 0, dml_factor))</li>
     *   <li>AVG(expr): same as SUM (visible value derived later from hidden sum/count)</li>
     * </ul>
     *
     * <p>A top project wraps the aggregate to:
     * 1. Compute row_id (hash of group keys for grouped, 0 for scalar).
     * 2. Apply COALESCE to NULL-susceptible outputs (SUM may return NULL for all-NULL groups).
     */
    DeltaPlanParts buildDeltaSubPlan(LogicalAggregate<?> normalizedAgg,
            IvmDeltaRewriteResult childResult, IvmAggMeta aggMeta) {
        Plan newAggChild = childResult.plan;
        Slot dmlFactorSlot = childResult.dmlFactorSlot;

        List<NamedExpression> deltaAggOutputs = new ArrayList<>();
        int groupKeySize = aggMeta.getGroupKeySlots().size();
        for (Expression groupByExpr : normalizedAgg.getGroupByExpressions()) {
            if (!(groupByExpr instanceof NamedExpression)) {
                throw new AnalysisException("IVM agg delta rewrite requires slot-like group key, but got: "
                        + groupByExpr);
            }
            deltaAggOutputs.add((NamedExpression) groupByExpr);
        }

        Alias deltaGroupCount = new Alias(new Sum(dmlFactorSlot), Column.IVM_DELTA_GROUP_COUNT_COL);
        deltaAggOutputs.add(deltaGroupCount);

        // Dispatch each normalized aggregate target to its processor. The processor appends only the delta outputs
        // needed by that aggregate function, such as signed SUM, non-NULL COUNT, or MIN/MAX insert/delete extrema.
        for (IvmAggTarget target : aggMeta.getAggTargets()) {
            aggFunctionRegistry.appendDeltaAggregateOutputs(
                    target, dmlFactorSlot, deltaAggOutputs, aggExpressionBuilder);
        }

        LogicalAggregate<?> deltaAgg = withDeltaAggregateOutput(normalizedAgg, deltaAggOutputs, newAggChild);
        List<NamedExpression> topOutputs = new ArrayList<>();
        Alias rowIdAlias = new Alias(
                IvmUtil.buildRowIdHash(deltaAgg.getOutput().subList(0, groupKeySize)), Column.IVM_ROW_ID_COL);
        topOutputs.add(rowIdAlias);

        Set<String> zeroDefaultDeltaOutputNames = collectZeroDefaultDeltaOutputNames(aggMeta);
        for (Slot slot : deltaAgg.getOutput()) {
            if (zeroDefaultDeltaOutputNames.contains(slot.getName())) {
                topOutputs.add(new Alias(new Coalesce(slot, aggExpressionBuilder.zeroOf(slot.getDataType())),
                        slot.getName()));
            } else {
                topOutputs.add(slot);
            }
        }

        LogicalProject<?> topDeltaProject = new LogicalProject<>(ImmutableList.copyOf(topOutputs), deltaAgg);
        Map<String, Slot> outputByName = indexSlotsByName(topDeltaProject.getOutput());
        Slot deltaGroupCountSlot = outputByName.get(Column.IVM_DELTA_GROUP_COUNT_COL);
        Map<IvmAggDeltaSlotRef, Slot> applyDeltaSlots = new LinkedHashMap<>();
        for (IvmAggTarget target : aggMeta.getAggTargets()) {
            // Convert generated delta output names into stable logical keys before the apply project starts building
            // expressions. Apply expressions should depend on target ordinal + logical slot kind, not string names.
            aggFunctionRegistry.mapApplyDeltaSlots(
                    target, outputByName, applyDeltaSlots, deltaGroupCountSlot, aggExpressionBuilder);
        }
        Map<String, Slot> groupKeySlotsByName = new LinkedHashMap<>();
        for (Slot groupKey : aggMeta.getGroupKeySlots()) {
            Slot resolved = outputByName.get(groupKey.getName());
            if (resolved == null) {
                throw new AnalysisException("IVM agg delta rewrite failed to resolve delta group key slot: "
                        + groupKey.getName());
            }
            groupKeySlotsByName.put(groupKey.getName(), resolved);
        }

        return new DeltaPlanParts(topDeltaProject, outputByName.get(Column.IVM_ROW_ID_COL),
                deltaGroupCountSlot, applyDeltaSlots, groupKeySlotsByName);
    }

    /**
     * Builds the apply plan: merges delta into MV current state.
     *
     * <p>Plan shape:
     * <pre>
     *   Project(normalized aggregate outputs + __DORIS_IVM_DML_FACTOR_COL__)
     *     └── Filter(net-zero)            // grouped agg only
     *         └── RightOuterJoin(mv.row_id = delta.row_id)
     *             ├── MV current-state scan (with delete-sign filter)  [large, probe side]
     *             └── delta sub-plan                                   [small, build side]
     * </pre>
     *
     * <p>For each normalized aggregate output, computes:
     * <ul>
     *   <li>group keys: from delta side</li>
     *   <li>hidden state: COALESCE(mv_old, 0) + delta (with assert_true for non-negative counts)</li>
     *   <li>visible value: derived from new hidden state via per-function processors</li>
     * </ul>
     *
     * <p>Dml factor represents final row action rather than input delta polarity:
     * grouped agg deletes the MV row only when {@code new_group_count <= 0}; scalar agg always upserts.
     *
     * <p>Net-zero filter (grouped only): NOT(mv.row_id IS NULL AND delta_group_count <= 0)
     * prevents inserting delete-sign rows for groups that never existed in the MV.
     */
    LogicalProject<?> buildApplyPlan(LogicalAggregate<?> normalizedAgg,
            DeltaPlanParts delta, IvmAggMeta aggMeta, IvmRefreshContext ctx) {
        LogicalOlapScan rawMvScan = buildMvScan(ctx.getMtmv(), ctx);
        LogicalPlan mvPlan = BindRelation.checkAndAddDeleteSignFilter(
                rawMvScan, ctx.getConnectContext(), ctx.getMtmv());
        Slot mvRowId = helper.findSlotByName(rawMvScan.getOutput(), Column.IVM_ROW_ID_COL);
        // MV (large) on left as probe side, delta (small) on right as build side.
        LogicalJoin<Plan, Plan> join = new LogicalJoin<>(JoinType.RIGHT_OUTER_JOIN,
                ImmutableList.of(new EqualTo(mvRowId, delta.rowIdSlot)),
                mvPlan, delta.topDeltaProject, JoinReorderContext.EMPTY);
        Plan joinInput = aggMeta.isScalarAgg() ? join : buildNetZeroFilter(join, delta, mvRowId);

        Map<String, Expression> finalByColumnName = new LinkedHashMap<>();
        Expression newGroupCount = aggExpressionBuilder.assertNonNegative(
                new Add(aggExpressionBuilder.zeroIfNullMvSlot(rawMvScan, aggMeta.getGroupCountSlot().getName()),
                        deltaGroupCount(delta)),
                "negative group count");
        finalByColumnName.put(Column.IVM_ROW_ID_COL, delta.rowIdSlot);
        finalByColumnName.put(aggMeta.getGroupCountSlot().getName(), newGroupCount);
        for (Slot groupKey : aggMeta.getGroupKeySlots()) {
            finalByColumnName.put(groupKey.getName(), deltaGroupKey(delta, groupKey.getName()));
        }

        IvmAggApplyContext applyContext = new IvmAggApplyContext(
                finalByColumnName, rawMvScan, delta.applyDeltaSlots, newGroupCount, aggExpressionBuilder);
        for (IvmAggTarget target : aggMeta.getAggTargets()) {
            // The same processor that declared the target's delta outputs now merges old MV state and resolved delta
            // slots into the final visible column and any hidden state columns.
            aggFunctionRegistry.appendApplyExpressions(target, applyContext);
        }

        Expression dmlFactor = aggMeta.isScalarAgg()
                ? new TinyIntLiteral((byte) 1)
                : new If(new LessThanEqual(newGroupCount, new BigIntLiteral(0)),
                        new TinyIntLiteral((byte) -1), new TinyIntLiteral((byte) 1));

        List<NamedExpression> finalOutputs = new ArrayList<>();
        // Keep the normalized aggregate schema here. The normalize-added top project computes row-id above this
        // project, and the final sink project reorders columns by MV schema.
        for (Slot target : normalizedAgg.getOutput()) {
            Expression expr = finalByColumnName.get(target.getName());
            if (expr == null) {
                throw new AnalysisException("IVM agg delta rewrite missing output expression for column: "
                        + target.getName());
            }
            finalOutputs.add(new Alias(target.getExprId(), expr, target.getName()));
        }
        finalOutputs.add(new Alias(dmlFactor, Column.IVM_DML_FACTOR_COL));
        return new LogicalProject<>(ImmutableList.copyOf(finalOutputs), joinInput);
    }

    private LogicalFilter<Plan> buildNetZeroFilter(LogicalJoin<Plan, Plan> join, DeltaPlanParts delta, Slot mvRowId) {
        Expression filter = new Not(new And(new IsNull(mvRowId),
                new LessThanEqual(deltaGroupCount(delta), new BigIntLiteral(0))));
        return new LogicalFilter<>(ImmutableSet.of(filter), join);
    }

    private LogicalAggregate<?> withDeltaAggregateOutput(LogicalAggregate<?> normalizedAgg,
            List<NamedExpression> deltaAggOutputs, Plan newAggChild) {
        LogicalAggregate<?> newAgg = normalizedAgg.withAggOutputChild(deltaAggOutputs, newAggChild);
        if (!normalizedAgg.getSourceRepeat().isPresent()) {
            return newAgg;
        }
        Optional<LogicalRepeat<?>> sourceRepeat = newAggChild.collectFirst(LogicalRepeat.class::isInstance);
        if (!sourceRepeat.isPresent()) {
            throw new AnalysisException("IVM agg delta rewrite failed to resolve rewritten source repeat");
        }
        return newAgg.withSourceRepeat(sourceRepeat.get());
    }

    private LogicalOlapScan buildMvScan(MTMV mtmv, IvmRefreshContext ctx) {
        return new LogicalOlapScan(
                ctx.getConnectContext().getStatementContext().getNextRelationId(),
                mtmv,
                ImmutableList.of(mtmv.getQualifiedDbName()),
                ImmutableList.of(),
                mtmv.getPartitionIds(),
                mtmv.getBaseIndexId(),
                PreAggStatus.unset(),
                ImmutableList.of(),
                ImmutableList.of(),
                Optional.empty(),
                ImmutableList.of());
    }

    /** Looks up the delta_group_count slot from delta plan parts. */
    private Expression deltaGroupCount(DeltaPlanParts delta) {
        return delta.deltaGroupCountSlot;
    }

    private Expression deltaGroupKey(DeltaPlanParts delta, String name) {
        Slot slot = delta.groupKeySlotsByName.get(name);
        if (slot == null) {
            throw new AnalysisException("IVM agg delta rewrite failed to resolve delta group key: " + name);
        }
        return slot;
    }

    /**
     * Collects delta output names where NULL should be normalized to zero before apply.
     *
     * <p>Needed for arithmetic merge operands where NULL means "no delta contribution":
     * <ul>
     *   <li>Scalar aggregate group count can be NULL when base table is empty</li>
     *   <li>SUM-like signed deltas can be NULL when all input expressions are NULL</li>
     * </ul>
     */
    private Set<String> collectZeroDefaultDeltaOutputNames(IvmAggMeta aggMeta) {
        Set<String> outputNames = new LinkedHashSet<>();
        if (aggMeta.isScalarAgg()) {
            outputNames.add(Column.IVM_DELTA_GROUP_COUNT_COL);
        }
        for (IvmAggTarget target : aggMeta.getAggTargets()) {
            aggFunctionRegistry.collectZeroDefaultDeltaOutputNames(
                    target, aggMeta.isScalarAgg(), outputNames, aggExpressionBuilder);
        }
        return outputNames;
    }

    private Map<String, Slot> indexSlotsByName(List<Slot> slots) {
        Map<String, Slot> slotByName = new LinkedHashMap<>();
        for (Slot slot : slots) {
            slotByName.put(slot.getName(), slot);
        }
        return slotByName;
    }

}
