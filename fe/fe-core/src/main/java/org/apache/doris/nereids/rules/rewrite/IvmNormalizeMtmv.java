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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.Pair;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.info.TableNameInfoUtils;
import org.apache.doris.mtmv.MTMVPartitionUtil;
import org.apache.doris.mtmv.ivm.IvmAggMeta;
import org.apache.doris.mtmv.ivm.IvmAggMeta.AggTarget;
import org.apache.doris.mtmv.ivm.IvmAggMeta.AggType;
import org.apache.doris.mtmv.ivm.IvmException;
import org.apache.doris.mtmv.ivm.IvmFailureReason;
import org.apache.doris.mtmv.ivm.IvmNormalizeResult;
import org.apache.doris.mtmv.ivm.IvmUtil;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.Avg;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.functions.scalar.UuidNumeric;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapTableSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalResultSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.nereids.types.LargeIntType;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Normalizes the MV define plan for IVM at both CREATE MV and REFRESH MV time.
 *
 * <h3>Example: aggregate MV rewrite</h3>
 * <p>Given MV definition:
 * <pre>{@code
 *   SELECT sum(v1+v2), count(v3+v4), avg(v5+v6), min(v7+v8)
 *   FROM t GROUP BY k1, k2
 * }</pre>
 *
 * <p>After IvmNormalizeMtmv the plan shape is:
 * <pre>{@code
 * ResultSink [row_id, visible outputs, hidden state cols]
 *   └── Project [
 *         __DORIS_IVM_ROW_ID__  = hash(k1, k2),
 *         k1, k2,
 *         sum(v1+v2),                       -- ordinal 0 visible (SUM)
 *         count(v3+v4),                     -- ordinal 1 visible (COUNT(expr), no hidden col)
 *         avg(v5+v6),                       -- ordinal 2 visible (AVG)
 *         min(v7+v8),                       -- ordinal 3 visible (MIN)
 *         __DORIS_IVM_AGG_COUNT_COL__,      -- group COUNT(*)
 *         __DORIS_IVM_AGG_0_COUNT__,        -- SUM:   hidden COUNT(v1+v2) (no hidden SUM; visible stores it)
 *         __DORIS_IVM_AGG_2_SUM__,          -- AVG:   hidden SUM(v5+v6)
 *         __DORIS_IVM_AGG_2_COUNT__,        -- AVG:   hidden COUNT(v5+v6)
 *         __DORIS_IVM_AGG_3_COUNT__         -- MIN:   hidden COUNT(v7+v8) (no hidden MIN; visible stores it)
 *       ]
 *         └── Aggregate [GROUP BY k1, k2]
 *               outputs: [k1, k2,
 *                 sum(v1+v2), count(v3+v4), avg(v5+v6), min(v7+v8),
 *                 COUNT(*),    COUNT(v1+v2),
 *                 SUM(v5+v6), COUNT(v5+v6),
 *                 COUNT(v7+v8)]
 *               └── Scan(t) with base-table row-id
 * }</pre>
 *
 * <h3>Hidden column strategy per aggregate type</h3>
 * <ul>
 *   <li><b>COUNT(*)</b>: no hidden columns (visible = global group count)</li>
 *   <li><b>COUNT(expr)</b>: no hidden columns (visible stores the count directly)</li>
 *   <li><b>SUM</b>: hidden COUNT only (visible stores SUM; COUNT for guard)</li>
 *   <li><b>AVG</b>: hidden SUM + COUNT (visible is AVG ≠ SUM or COUNT)</li>
 *   <li><b>MIN/MAX</b>: hidden COUNT only (visible stores extremal value)</li>
 * </ul>
 *
 * <h3>Scan-level row-id injection</h3>
 * <ul>
 *   <li>MOW (UNIQUE_KEYS + merge-on-write): hash(uk columns) → deterministic
 *   <li>Excluded AGG_KEYS table: hash(agg key columns) → deterministic
 *   <li>DUP_KEYS: uuid_numeric() → non-deterministic
 *   <li>Other key types: not supported, throws.
 * </ul>
 *
 * <p>IVM row-id invariant: every row that really exists in a normalized child plan must have
 * a non-null {@code __DORIS_IVM_ROW_ID_COL__}. Outer join null-padding is the only place where
 * a child row-id slot may become NULL, and that NULL means the corresponding side has no
 * matching row. This lets left outer join compose the MV row-id as
 * {@code hash(left_row_id, right_row_id)} and use {@code right_row_id = NULL} for padded rows
 * without confusing them with real right rows.
 *
 * <h3>Supported plan nodes</h3>
 * OlapScan, filter, project, aggregate, inner/cross join, root left outer join, UNION ALL,
 * result sink, logical olap table sink.
 */
public class IvmNormalizeMtmv extends DefaultPlanRewriter<Boolean> implements CustomRewriter {

    private static final Set<Class<? extends AggregateFunction>> SUPPORTED_AGG_FUNCTIONS =
            ImmutableSet.of(Count.class, Sum.class, Avg.class, Min.class, Max.class);

    private final IvmNormalizeResult normalizeResult = new IvmNormalizeResult();
    private StatementContext statementContext;

    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        ConnectContext connectContext = jobContext.getCascadesContext().getConnectContext();
        if (connectContext == null || !connectContext.getSessionVariable().isEnableIvmNormalRewrite()) {
            return plan;
        }
        // Idempotency: if already normalized (e.g. rewritten plan re-entering), skip.
        if (jobContext.getCascadesContext().getIvmNormalizeResult().isPresent()) {
            return plan;
        }
        statementContext = jobContext.getCascadesContext().getStatementContext();
        jobContext.getCascadesContext().setIvmNormalizeResult(normalizeResult);
        Plan result = plan.accept(this, true);
        normalizeResult.setNormalizedPlan(result);
        return result;
    }

    // unsupported: any plan node not explicitly whitelisted below
    @Override
    public Plan visit(Plan plan, Boolean isFirstNonSink) {
        throw new IvmException(IvmFailureReason.PLAN_PATTERN_UNSUPPORTED, "IVM does not support plan node: "
                + plan.getClass().getSimpleName());
    }

    // whitelisted: only OlapScan — inject IVM row-id at index 0
    @Override
    public Plan visitLogicalOlapScan(LogicalOlapScan scan, Boolean isFirstNonSink) {
        OlapTable table = scan.getTable();
        Pair<Expression, Boolean> rowId = buildRowId(table, scan);
        validateBinlogEnabled(scan);
        Alias rowIdAlias = new Alias(rowId.first, Column.IVM_ROW_ID_COL);
        normalizeResult.addRowId(rowIdAlias.toSlot(), rowId.second);
        List<NamedExpression> outputs = ImmutableList.<NamedExpression>builder()
                .add(rowIdAlias)
                .addAll(scan.getOutput())
                .build();
        return new LogicalProject<>(outputs, scan);
    }

    // whitelisted: project — recurse into child, then propagate row-id if not already present
    @Override
    public Plan visitLogicalProject(LogicalProject<? extends Plan> project, Boolean isFirstNonSink) {
        Plan newChild = project.child().accept(this, isFirstNonSink);
        List<NamedExpression> newOutputs = rewriteOutputsWithIvmHiddenColumns(newChild, project.getProjects());
        if (newChild == project.child() && newOutputs.equals(project.getProjects())) {
            return project;
        }
        return project.withProjectsAndChild(newOutputs, newChild);
    }

    @Override
    public Plan visitLogicalFilter(LogicalFilter<? extends Plan> filter, Boolean isFirstNonSink) {
        Plan newChild = filter.child().accept(this, false);
        return newChild == filter.child() ? filter : filter.withChildren(ImmutableList.of(newChild));
    }

    /**
     * Handles inner join / cross join / root left outer join normalization.
     *
     * <ol>
     *   <li>Validates join type is INNER_JOIN, CROSS_JOIN, or root LEFT_OUTER_JOIN</li>
     *   <li>Normalizes both children (isFirstNonSink = false)</li>
     *   <li>Composes a single row_id = hash(left_row_id, right_row_id)</li>
     *   <li>Wraps with Project that replaces child row_id slots with the composed one</li>
     * </ol>
     *
     * <p>The composed row_id is deterministic iff both children's row_ids are deterministic.
     * Child row_id slots are removed from the output to prevent merge conflicts in
     * {@link #collectIvmHiddenSlots} when multiple {@code __DORIS_IVM_ROW_ID_COL__} exist.
     * The child entries in {@code rowIdDeterminism} are kept (not cleared) so that the
     * strategy phase can look up individual child row_id determinism.
     */
    @Override
    public Plan visitLogicalJoin(LogicalJoin<? extends Plan, ? extends Plan> join, Boolean isFirstNonSink) {
        JoinType joinType = join.getJoinType();
        if (joinType != JoinType.INNER_JOIN && joinType != JoinType.CROSS_JOIN
                && joinType != JoinType.LEFT_OUTER_JOIN) {
            throw new IvmException(IvmFailureReason.OUTER_JOIN_RETRACTION_UNSUPPORTED,
                    "IVM does not support join type: " + joinType
                            + ". Only INNER_JOIN, CROSS_JOIN and LEFT_OUTER_JOIN are supported.");
        }
        if (join.isMarkJoin()) {
            throw new IvmException(IvmFailureReason.PLAN_PATTERN_UNSUPPORTED,
                    "IVM does not support mark join (subquery with disjunction).");
        }
        if (joinType.isOuterJoin()) {
            if (!isFirstNonSink) {
                throw new IvmException(IvmFailureReason.OUTER_JOIN_RETRACTION_UNSUPPORTED,
                        "IVM OUTER JOIN must be the top-level operator "
                                + "(only sinks and projects allowed above it)");
            }
            // TODO: tighten nullable-side snapshot validation to an explicit allowlist:
            // OlapScan, Project, Filter, Inner/Cross Join, and excluded-only UNION ALL.
            checkNullableSideSnapshotSupported(join.right());
            normalizeResult.setOuterJoinMv(true);
        }

        Plan newLeft = join.left().accept(this, false);
        Plan newRight = join.right().accept(this, false);
        LogicalJoin<Plan, Plan> newJoin = (LogicalJoin<Plan, Plan>) join.withChildren(newLeft, newRight);

        // Find left and right row_id slots from children's output
        Slot leftRowIdSlot = IvmUtil.findRowIdSlot(newLeft.getOutput(), "left child of join");
        Slot rightRowIdSlot = IvmUtil.findRowIdSlot(newRight.getOutput(), "right child of join");

        // Look up each child's row_id determinism from the accumulated map
        boolean leftDet = normalizeResult.isDeterministic(leftRowIdSlot);
        boolean rightDet = normalizeResult.isDeterministic(rightRowIdSlot);
        if (joinType == JoinType.LEFT_OUTER_JOIN && !leftDet) {
            // Nullable-side deltas may emit +1/-1 pad-null repair rows keyed by
            // hash(left_row_id, NULL). A non-deterministic preserved-side row_id cannot
            // be reproduced across refreshes, so LEFT OUTER JOIN IVM cannot maintain it.
            throw new IvmException(IvmFailureReason.NON_DETERMINISTIC_ROW_ID,
                    "IVM LEFT OUTER JOIN requires deterministic row_id on preserved side");
        }

        // Compose join row_id = hash(left_row_id, right_row_id).
        // Valid child row_ids are non-null by the normalize invariant above. For LEFT OUTER JOIN,
        // a NULL right row_id can only come from join null-padding and means no matching right row.
        Expression joinRowIdExpr = IvmUtil.buildRowIdHash(ImmutableList.of(leftRowIdSlot, rightRowIdSlot));
        Alias joinRowIdAlias = new Alias(joinRowIdExpr, Column.IVM_ROW_ID_COL);

        // Build Project output: [composedRowId, joinOutput minus child row_ids]
        ImmutableList.Builder<NamedExpression> projectOutputs = ImmutableList.builder();
        projectOutputs.add(joinRowIdAlias);
        for (Slot slot : newJoin.getOutput()) {
            if (!Column.IVM_ROW_ID_COL.equals(slot.getName())) {
                projectOutputs.add(slot);
            }
        }

        // Add composed row_id to map (don't clear — child entries are kept for strategy lookup)
        normalizeResult.addRowId(joinRowIdAlias.toSlot(), leftDet && rightDet);
        return new LogicalProject<>(projectOutputs.build(), newJoin);
    }

    /**
     * Handles UNION ALL normalization.
     *
     * <p>Validates: only UNION ALL (rejects DISTINCT), no constant expression arms.
     *
     * <p>For each child arm:
     * <ol>
     *   <li>Normalizes the child (injects row_id at scan/join level)</li>
     *   <li>Wraps with a Project that computes {@code hash(arm_index, child_row_id)} as the
     *       new row_id — the arm_index literal prevents cross-arm row_id collision (e.g. self-union)</li>
     *   <li>Strips the original child row_id from the output</li>
     * </ol>
     *
     * <p>Then rebuilds the UNION with an additional union-level row_id output column prepended.
     * The union row_id is deterministic iff all arms' row_ids are deterministic.
     */
    @Override
    public Plan visitLogicalUnion(LogicalUnion union, Boolean isFirstNonSink) {
        if (union.getQualifier() != Qualifier.ALL) {
            throw new IvmException(IvmFailureReason.PLAN_PATTERN_UNSUPPORTED,
                    "IVM does not support UNION DISTINCT. Only UNION ALL is supported.");
        }
        if (!union.getConstantExprsList().isEmpty()) {
            throw new IvmException(IvmFailureReason.PLAN_PATTERN_UNSUPPORTED,
                    "IVM does not support UNION ALL with constant expressions.");
        }

        List<Plan> newChildren = new ArrayList<>();
        List<List<SlotReference>> newChildrenOutputs = new ArrayList<>();
        boolean allDet = true;

        for (int i = 0; i < union.children().size(); i++) {
            Plan normalizedChild = union.child(i).accept(this, false);
            Slot childRowId = IvmUtil.findRowIdSlot(normalizedChild.getOutput(),
                    "child " + i + " of union");
            allDet &= normalizeResult.isDeterministic(childRowId);

            // Wrap with Project: hash(arm_index, child_row_id) as row_id, plus other cols
            Expression hashExpr = IvmUtil.buildRowIdHash(
                    ImmutableList.of(new IntegerLiteral(i), childRowId));
            Alias hashAlias = new Alias(hashExpr, Column.IVM_ROW_ID_COL);

            ImmutableList.Builder<NamedExpression> projOutputs = ImmutableList.builder();
            projOutputs.add(hashAlias);
            for (Slot slot : normalizedChild.getOutput()) {
                if (!Column.IVM_ROW_ID_COL.equals(slot.getName())) {
                    projOutputs.add(slot);
                }
            }
            LogicalProject<Plan> hashedChild = new LogicalProject<>(projOutputs.build(), normalizedChild);
            newChildren.add(hashedChild);

            // Build child's regularChildrenOutputs: [hashed_row_id, ...original_mapping...]
            SlotReference hashedRowIdSlot = (SlotReference) hashedChild.getOutput().get(0);
            ImmutableList.Builder<SlotReference> childMapping = ImmutableList.builder();
            childMapping.add(hashedRowIdSlot);
            childMapping.addAll(union.getRegularChildrenOutputs().get(i));
            newChildrenOutputs.add(childMapping.build());
        }

        // Create union-level row_id output
        SlotReference unionRowId = new SlotReference(
                StatementScopeIdGenerator.newExprId(),
                Column.IVM_ROW_ID_COL, LargeIntType.INSTANCE, false, ImmutableList.of());
        normalizeResult.addRowId(unionRowId, allDet);

        // Rebuild UNION: [union_row_id, ...original_outputs...]
        ImmutableList.Builder<NamedExpression> newOutputs = ImmutableList.builder();
        newOutputs.add(unionRowId);
        newOutputs.addAll(union.getOutputs());

        return union.withNewOutputsChildrenAndConstExprsList(
                newOutputs.build(), newChildren, newChildrenOutputs, union.getConstantExprsList());
    }

    /**
     * Handles aggregate MV normalization. Post-NormalizeAggregate plan shape:
     * {@code Project(top) → Aggregate(normalized) → Project(bottom) → ... → Scan}
     *
     * <p>This method:
     * <ol>
     *   <li>Recurses into child (injects base scan row-id, unused at agg level)</li>
     *   <li>Validates all aggregate functions via {@link #checkAggFunctions}</li>
     *   <li>Adds hidden state aggregate columns to the Aggregate output</li>
     *   <li>Wraps with a Project that computes row-id = hash(group keys) or constant</li>
     *   <li>Stores {@link IvmAggMeta} in {@link IvmNormalizeResult}</li>
     * </ol>
     *
     * <p>Returns: {@code Project(ivm hidden cols + original agg outputs) → Aggregate(with hidden aggs)}
     */
    @Override
    public Plan visitLogicalAggregate(LogicalAggregate<? extends Plan> agg, Boolean isFirstNonSink) {
        if (!isFirstNonSink) {
            throw new IvmException(IvmFailureReason.AGG_UNSUPPORTED,
                    "IVM aggregate must be the top-level operator (only sinks and projects allowed above it)");
        }
        Plan newChild = agg.child().accept(this, false);

        // After NormalizeAggregate, outputs are: group-by key Slots + Alias(AggFunc)
        List<NamedExpression> origOutputs = agg.getOutputExpressions();
        List<Expression> groupByExprs = agg.getGroupByExpressions();
        boolean scalarAgg = groupByExprs.isEmpty();

        List<Alias> aggAliases = new ArrayList<>();
        for (NamedExpression output : origOutputs) {
            if (output instanceof Slot) {
                // group-by key slot — validated but not collected separately
            } else if (output instanceof Alias && ((Alias) output).child() instanceof AggregateFunction) {
                aggAliases.add((Alias) output);
            } else {
                throw new IvmException(IvmFailureReason.AGG_UNSUPPORTED,
                        "IVM: unexpected expression in normalized aggregate output: " + output);
            }
        }

        // Validate aggregate functions
        List<AggregateFunction> aggFunctions = new ArrayList<>();
        for (Alias alias : aggAliases) {
            aggFunctions.add((AggregateFunction) alias.child());
        }
        checkAggFunctions(aggFunctions);

        // Build hidden aggregate expressions and AggTarget metadata
        // __DORIS_IVM_AGG_COUNT_COL__ = COUNT(*) for group multiplicity
        Alias groupCountAlias = new Alias(new Count(), Column.IVM_AGG_COUNT_COL);

        List<NamedExpression> hiddenAggOutputs = new ArrayList<>();
        hiddenAggOutputs.add(groupCountAlias);

        List<AggTarget> aggTargets = new ArrayList<>();
        for (int i = 0; i < aggAliases.size(); i++) {
            Alias origAlias = aggAliases.get(i);
            AggregateFunction aggFunc = (AggregateFunction) origAlias.child();
            buildHiddenStateForAgg(i, aggFunc, origAlias, hiddenAggOutputs, aggTargets);
        }

        // Build new Aggregate with hidden agg outputs AFTER original outputs
        ImmutableList.Builder<NamedExpression> newAggOutputs = ImmutableList.builder();
        newAggOutputs.addAll(origOutputs);
        newAggOutputs.addAll(hiddenAggOutputs);
        LogicalAggregate<Plan> newAgg = agg.withAggOutputChild(newAggOutputs.build(), newChild);

        // Build wrapping Project that computes row-id and exposes all slots
        // Layout: [row_id, original visible outputs, hidden state outputs]
        // groupByExprs are already Slots after NormalizeAggregate
        Expression rowIdExpr = IvmUtil.buildRowIdHash(groupByExprs);
        Alias rowIdAlias = new Alias(rowIdExpr, Column.IVM_ROW_ID_COL);

        // Add agg-level row-id to IvmNormalizeResult (child entries are kept for strategy lookup)
        normalizeResult.addRowId(rowIdAlias.toSlot(), !scalarAgg);

        // Project output: row_id first, then all Aggregate output slots (original + hidden)
        ImmutableList.Builder<NamedExpression> projectOutputs = ImmutableList.builder();
        projectOutputs.add(rowIdAlias);
        for (NamedExpression aggOutput : newAgg.getOutputExpressions()) {
            projectOutputs.add(aggOutput.toSlot());
        }

        // Resolve AggTarget slots from the new Aggregate output
        List<Slot> newAggSlots = newAgg.getOutput();
        // groupCountSlot is at origOutputs.size() (first hidden output after original outputs)
        Slot groupCountSlot = newAggSlots.get(origOutputs.size());
        List<AggTarget> resolvedTargets = resolveAggTargetSlots(aggTargets, newAggSlots);

        // After NormalizeAggregate, group-by exprs are all Slots; cast directly
        List<Slot> resolvedGroupKeys = groupByExprs.stream()
                .map(expr -> (Slot) expr)
                .collect(ImmutableList.toImmutableList());

        IvmAggMeta aggMeta = new IvmAggMeta(scalarAgg, resolvedGroupKeys,
                groupCountSlot, resolvedTargets);
        normalizeResult.setAggMeta(aggMeta);

        return new LogicalProject<>(projectOutputs.build(), newAgg);
    }

    private void checkNullableSideSnapshotSupported(Plan nullableSide) {
        // Outer-join pad-null repair needs the nullable side's full pre/post snapshot.
        // For UNION ALL, the linear delta strategy prunes non-delta arms and keeps only
        // the changed arm. If such a UNION ALL contains a trigger-table OlapScan, O1 cannot
        // reconstruct the full nullable-side snapshot from the rewritten delta plan reliably.
        // Excluded trigger tables are ignored because their changes do not produce delta arms.
        if (containsUnionAllWithOlapScan(nullableSide)) {
            throw new IvmException(IvmFailureReason.SNAPSHOT_ALIGNMENT_UNSUPPORTED,
                    "IVM OUTER JOIN does not support UNION ALL with OlapScan on nullable side");
        }
    }

    private boolean containsUnionAllWithOlapScan(Plan plan) {
        if (!plan.containsType(LogicalUnion.class)) {
            return false;
        }
        return plan.<LogicalUnion>collectFirst(node -> {
            if (!(node instanceof LogicalUnion)) {
                return false;
            }
            LogicalUnion union = (LogicalUnion) node;
            if (!union.containsType(LogicalOlapScan.class)) {
                return false;
            }
            return union.getQualifier() == Qualifier.ALL
                    && union.<LogicalOlapScan>collectFirst(child -> {
                        if (!(child instanceof LogicalOlapScan)) {
                            return false;
                        }
                        LogicalOlapScan scan = (LogicalOlapScan) child;
                        return !isExcludedTriggerTable(scan);
                    }).isPresent();
        }).isPresent();
    }

    /**
     * For each user-visible aggregate, creates the hidden state columns needed for IVM delta.
     * Appends hidden Alias expressions to {@code hiddenAggOutputs} and builds an AggTarget
     * (with placeholder slots that will be resolved later from the new Aggregate output).
     */
    private void buildHiddenStateForAgg(int ordinal, AggregateFunction aggFunc, Alias origAlias,
            List<NamedExpression> hiddenAggOutputs, List<AggTarget> aggTargets) {
        AggType aggType;
        Map<AggType, Alias> hiddenAliases = new LinkedHashMap<>();

        if (aggFunc instanceof Count) {
            aggType = AggType.COUNT;
            // No hidden columns for either COUNT(*) or COUNT(expr).
            // COUNT(*) visible = global group count; COUNT(expr) visible stores count directly.
        } else if (aggFunc instanceof Sum) {
            aggType = AggType.SUM;
            // No hidden SUM column: visible column stores SUM directly.
            // Hidden COUNT is needed for the assertNonNegative guard and null-count logic.
            addHiddenAlias(hiddenAliases, ordinal, AggType.COUNT, new Count(aggFunc.child(0)));
        } else if (aggFunc instanceof Avg) {
            aggType = AggType.AVG;
            addHiddenAlias(hiddenAliases, ordinal, AggType.SUM, new Sum(aggFunc.child(0)));
            addHiddenAlias(hiddenAliases, ordinal, AggType.COUNT, new Count(aggFunc.child(0)));
        } else if (aggFunc instanceof Min) {
            aggType = AggType.MIN;
            // No hidden MIN column: the visible column already stores the extremal value.
            // Only a hidden COUNT is needed for the guard / zero-count NULL logic.
            addHiddenAlias(hiddenAliases, ordinal, AggType.COUNT, new Count(aggFunc.child(0)));
        } else if (aggFunc instanceof Max) {
            aggType = AggType.MAX;
            addHiddenAlias(hiddenAliases, ordinal, AggType.COUNT, new Count(aggFunc.child(0)));
        } else {
            throw new IvmException(IvmFailureReason.AGG_UNSUPPORTED,
                    "IVM: unsupported aggregate function: " + aggFunc.getName());
        }

        hiddenAggOutputs.addAll(hiddenAliases.values());

        // Build AggTarget with placeholder slots (to be resolved after Aggregate is rebuilt)
        ImmutableMap.Builder<AggType, Slot> placeholderHiddenSlots = ImmutableMap.builder();
        for (Map.Entry<AggType, Alias> entry : hiddenAliases.entrySet()) {
            placeholderHiddenSlots.put(entry.getKey(), entry.getValue().toSlot());
        }

        List<Expression> exprArgs = ImmutableList.of();
        if (!(aggFunc instanceof Count && ((Count) aggFunc).isCountStar())) {
            exprArgs = ImmutableList.of(aggFunc.child(0));
        }

        aggTargets.add(new AggTarget(ordinal, aggType, origAlias.toSlot(),
                placeholderHiddenSlots.build(), exprArgs));
    }

    /** Adds a single hidden alias to the map with the standard IVM column name. */
    private void addHiddenAlias(Map<AggType, Alias> hiddenAliases, int ordinal,
            AggType stateType, AggregateFunction aggFunc) {
        hiddenAliases.put(stateType, new Alias(aggFunc,
                IvmUtil.ivmAggHiddenColumnName(ordinal, stateType.name())));
    }

    /**
     * Resolves placeholder AggTarget slots to actual slots from the rebuilt Aggregate output.
     * Matching is done by column name.
     */
    private List<AggTarget> resolveAggTargetSlots(List<AggTarget> placeholderTargets,
            List<Slot> newAggSlots) {
        // Build name→slot map from the new Aggregate output
        Map<String, Slot> slotByName = new LinkedHashMap<>();
        for (Slot slot : newAggSlots) {
            slotByName.put(slot.getName(), slot);
        }

        List<AggTarget> resolved = new ArrayList<>();
        for (AggTarget target : placeholderTargets) {
            // Resolve visible slot
            Slot resolvedVisible = slotByName.get(target.getVisibleSlot().getName());
            if (resolvedVisible == null) {
                throw new IvmException(IvmFailureReason.PLAN_PATTERN_UNSUPPORTED,
                        "IVM: failed to resolve visible slot '"
                        + target.getVisibleSlot().getName() + "' from rebuilt aggregate output");
            }

            // Resolve hidden state slots
            ImmutableMap.Builder<AggType, Slot> resolvedHidden = ImmutableMap.builder();
            for (Map.Entry<AggType, Slot> entry : target.getHiddenStateSlots().entrySet()) {
                Slot resolvedSlot = slotByName.get(entry.getValue().getName());
                if (resolvedSlot == null) {
                    throw new IvmException(IvmFailureReason.PLAN_PATTERN_UNSUPPORTED,
                            "IVM: failed to resolve hidden state slot '"
                            + entry.getValue().getName() + "' from rebuilt aggregate output");
                }
                resolvedHidden.put(entry.getKey(), resolvedSlot);
            }

            resolved.add(new AggTarget(target.getOrdinal(), target.getAggType(),
                    resolvedVisible, resolvedHidden.build(), target.getExprArgs()));
        }
        return resolved;
    }

    // whitelisted: result sink — recurse into child, then prepend row-id to output exprs
    @Override
    public Plan visitLogicalResultSink(LogicalResultSink<? extends Plan> sink, Boolean isFirstNonSink) {
        Plan newChild = sink.child().accept(this, isFirstNonSink);
        List<NamedExpression> newOutputs = rewriteOutputsWithIvmHiddenColumns(newChild, sink.getOutputExprs());
        if (newChild == sink.child() && newOutputs.equals(sink.getOutputExprs())) {
            return sink;
        }
        return sink.withOutputExprs(newOutputs).withChildren(ImmutableList.of(newChild));
    }

    @Override
    public Plan visitLogicalOlapTableSink(LogicalOlapTableSink<? extends Plan> sink,
            Boolean isFirstNonSink) {
        Plan newChild = sink.child().accept(this, isFirstNonSink);
        if (newChild == sink.child()) {
            return sink;
        }
        return sink.withChildAndUpdateOutput(newChild, sink.getPartitionExprList(),
                sink.getSyncMvWhereClauses(), sink.getTargetTableSlots());
    }

    /**
     * Rewrites output expressions to include IVM hidden columns from the child.
     * Layout: [row_id, original visible outputs, other hidden cols (count, per-agg states)].
     */
    private List<NamedExpression> rewriteOutputsWithIvmHiddenColumns(
            Plan normalizedChild, List<NamedExpression> outputs) {
        Map<String, Slot> ivmHiddenSlotsByName = collectIvmHiddenSlots(normalizedChild);
        if (!ivmHiddenSlotsByName.containsKey(Column.IVM_ROW_ID_COL)) {
            throw new IvmException(IvmFailureReason.PLAN_PATTERN_UNSUPPORTED,
                    "IVM normalization error: child plan has no row-id slot after normalization");
        }

        // Separate row-id from other hidden slots
        Slot rowIdSlot = ivmHiddenSlotsByName.get(Column.IVM_ROW_ID_COL);
        Map<String, Slot> otherHiddenSlots = new LinkedHashMap<>(ivmHiddenSlotsByName);
        otherHiddenSlots.remove(Column.IVM_ROW_ID_COL);

        ImmutableList.Builder<NamedExpression> rewrittenOutputs = ImmutableList.builder();
        if (outputs.stream().noneMatch(o -> IvmUtil.isIvmHiddenColumn(o.getName()))) {
            // No hidden outputs in original list: prepend row_id, then originals, then other hidden
            rewrittenOutputs.add(rowIdSlot);
            rewrittenOutputs.addAll(outputs);
            rewrittenOutputs.addAll(otherHiddenSlots.values());
            return rewrittenOutputs.build();
        }

        // Outputs already contain some hidden columns (e.g. BindSink placeholders).
        // Replace hidden outputs in-place to preserve positions and ExprIds.
        for (NamedExpression output : outputs) {
            if (IvmUtil.isIvmHiddenColumn(output.getName())) {
                rewrittenOutputs.add(rewriteIvmHiddenOutput(output, ivmHiddenSlotsByName));
            } else {
                rewrittenOutputs.add(output);
            }
        }
        // Append any new hidden slots from child that weren't in the original outputs
        for (Map.Entry<String, Slot> entry : ivmHiddenSlotsByName.entrySet()) {
            String name = entry.getKey();
            if (outputs.stream().noneMatch(o -> name.equals(o.getName()))) {
                rewrittenOutputs.add(entry.getValue());
            }
        }
        return rewrittenOutputs.build();
    }

    private Map<String, Slot> collectIvmHiddenSlots(Plan normalizedChild) {
        return normalizedChild.getOutput().stream()
                .filter(slot -> IvmUtil.isIvmHiddenColumn(slot.getName()))
                .collect(Collectors.toMap(Slot::getName, slot -> slot, (left, right) -> left, LinkedHashMap::new));
    }

    private NamedExpression rewriteIvmHiddenOutput(NamedExpression output, Map<String, Slot> ivmHiddenSlotsByName) {
        Slot ivmHiddenSlot = ivmHiddenSlotsByName.get(output.getName());
        if (ivmHiddenSlot == null) {
            throw new IvmException(IvmFailureReason.PLAN_PATTERN_UNSUPPORTED,
                    "IVM normalization error: child plan has no hidden slot named "
                    + output.getName() + " after normalization");
        }
        if (output instanceof Slot) {
            return ivmHiddenSlot;
        }
        if (output instanceof Alias) {
            Alias alias = (Alias) output;
            return new Alias(alias.getExprId(), ImmutableList.of(ivmHiddenSlot), alias.getName(),
                    alias.getQualifier(), alias.isNameFromChild());
        }
        throw new IvmException(IvmFailureReason.PLAN_PATTERN_UNSUPPORTED,
                "IVM normalization error: unsupported hidden output expression: "
                + output.getClass().getSimpleName());
    }

    /**
     * Builds the row-id expression and returns whether it is deterministic as a pair.
     * - UNIQUE_KEYS (MOW or excluded): (buildRowIdHash(uk...), true)      — stable across refreshes
     * - DUP_KEYS: (UuidNumeric(), false)                                  — random per insert
     * - Excluded AGG_KEYS: (buildRowIdHash(agg key...), true)             — stable across refreshes
     * - Other key types: throws IvmException (unless excluded trigger table)
     */
    private Pair<Expression, Boolean> buildRowId(OlapTable table, LogicalOlapScan scan) {
        KeysType keysType = table.getKeysType();
        boolean isExcludedTriggerTable = isExcludedTriggerTable(scan);
        if (keysType == KeysType.UNIQUE_KEYS) {
            if (!table.getEnableUniqueKeyMergeOnWrite() && !isExcludedTriggerTable) {
                throw new IvmException(IvmFailureReason.PLAN_PATTERN_UNSUPPORTED,
                        "INCREMENTAL materialized view requires UNIQUE_KEYS base tables "
                                + "to enable Merge-On-Write. Table '"
                                + table.getName() + "' has MOW disabled."
                                + " If this table does not participate in incremental refresh, "
                                + "add it to 'excluded_trigger_tables'.");
            }
            return buildDeterministicRowIdFromBaseKeys(table, scan);
        }
        if (keysType == KeysType.DUP_KEYS) {
            return Pair.of(new UuidNumeric(), false);
        }
        if (keysType == KeysType.AGG_KEYS && isExcludedTriggerTable) {
            return buildDeterministicRowIdFromBaseKeys(table, scan);
        }
        throw new IvmException(IvmFailureReason.PLAN_PATTERN_UNSUPPORTED,
                "INCREMENTAL materialized view requires base tables to be "
                        + "UNIQUE_KEYS with Merge-On-Write or DUP_KEYS. Table '"
                        + table.getName() + "' is " + keysType
                        + ". If this table does not participate in incremental refresh, "
                        + "add it to 'excluded_trigger_tables'.");
    }

    private Pair<Expression, Boolean> buildDeterministicRowIdFromBaseKeys(OlapTable table, LogicalOlapScan scan) {
        Set<String> keyColNames = table.getBaseSchemaKeyColumns().stream()
                .map(Column::getName)
                .collect(Collectors.toSet());
        List<Expression> keySlots = scan.getOutput().stream()
                .filter(slot -> keyColNames.contains(slot.getName()))
                .collect(Collectors.toList());
        if (keySlots.isEmpty()) {
            throw new IvmException(IvmFailureReason.PLAN_PATTERN_UNSUPPORTED,
                    "IVM: no key columns found for "
                    + table.getKeysType() + " table: " + table.getName());
        }
        return Pair.of(IvmUtil.buildRowIdHash(keySlots), true);
    }

    private void validateBinlogEnabled(LogicalOlapScan scan) {
        if (isExcludedTriggerTable(scan)) {
            return;
        }
        OlapTable table = scan.getTable();
        if (!table.getBinlogConfig().isEnableForStreaming()) {
            throw new IvmException(IvmFailureReason.BINLOG_NOT_ENABLED,
                    "SQL can be incrementally refreshed, but row binlog is not enabled for table: "
                            + table.getName()
                            + ". Please set 'binlog.enable' = 'true' and 'binlog.format' = 'ROW'.");
        }
    }

    private boolean isExcludedTriggerTable(LogicalOlapScan scan) {
        if (statementContext == null || statementContext.getExcludedTriggerTables().isEmpty()) {
            return false;
        }
        OlapTable table = scan.getTable();
        TableNameInfo tableNameInfo = TableNameInfoUtils.fromTableOrNull(table);
        if (tableNameInfo == null) {
            List<String> qualifier = scan.getQualifier();
            String dbName = qualifier.isEmpty() ? table.getDBName() : qualifier.get(qualifier.size() - 1);
            if (dbName == null) {
                return false;
            }
            String ctlName = qualifier.size() >= 2 ? qualifier.get(qualifier.size() - 2)
                    : InternalCatalog.INTERNAL_CATALOG_NAME;
            tableNameInfo = new TableNameInfo(ctlName, dbName, table.getName());
        }
        return MTMVPartitionUtil.isTableExcluded(statementContext.getExcludedTriggerTables(), tableNameInfo);
    }

    /**
     * Validates that all aggregate functions are supported for IVM.
     *
     * <p>Rules enforced:
     * <ol>
     *   <li>Bare GROUP BY (no aggregate functions) is allowed — the group-level count
     *       hidden column alone is sufficient for incremental maintenance.</li>
     *   <li>DISTINCT aggregates are not supported.</li>
     *   <li>Only count, sum, avg, min, and max are supported.</li>
     * </ol>
     *
     * @throws IvmException if validation fails
     */
    private static void checkAggFunctions(List<AggregateFunction> aggFunctions) {
        for (AggregateFunction aggFunc : aggFunctions) {
            if (aggFunc.isDistinct()) {
                throw new IvmException(IvmFailureReason.AGG_UNSUPPORTED,
                        "Aggregate DISTINCT is not supported for IVM: " + aggFunc.toSql());
            }
            if (!SUPPORTED_AGG_FUNCTIONS.contains(aggFunc.getClass())) {
                throw new IvmException(IvmFailureReason.AGG_UNSUPPORTED,
                        "Unsupported aggregate function for IVM: " + aggFunc.getName());
            }
        }
    }
}
