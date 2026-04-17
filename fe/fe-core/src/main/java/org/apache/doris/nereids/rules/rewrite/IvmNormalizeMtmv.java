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
import org.apache.doris.info.TableNameInfoUtils;
import org.apache.doris.mtmv.MTMVPartitionUtil;
import org.apache.doris.mtmv.ivm.IvmAggMeta;
import org.apache.doris.mtmv.ivm.IvmAggMeta.AggTarget;
import org.apache.doris.mtmv.ivm.IvmAggMeta.AggType;
import org.apache.doris.mtmv.ivm.IvmNormalizeResult;
import org.apache.doris.mtmv.ivm.IvmUtil;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.Avg;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.functions.scalar.UuidNumeric;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapTableSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalResultSink;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
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
 * <h3>Supported plan nodes</h3>
 * OlapScan, filter, project, aggregate, result sink, logical olap table sink.
 * TODO: join support.
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
        throw new AnalysisException("IVM does not support plan node: "
                + plan.getClass().getSimpleName());
    }

    // whitelisted: only OlapScan — inject IVM row-id at index 0
    @Override
    public Plan visitLogicalOlapScan(LogicalOlapScan scan, Boolean isFirstNonSink) {
        OlapTable table = scan.getTable();
        Pair<Expression, Boolean> rowId = buildRowId(table, scan);
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
            throw new AnalysisException(
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
                throw new AnalysisException(
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

        // Replace base scan row-id in IvmNormalizeResult with the agg-level row-id
        normalizeResult.getRowIdDeterminism().clear();
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
            throw new AnalysisException("IVM: unsupported aggregate function: " + aggFunc.getName());
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
                throw new AnalysisException("IVM: failed to resolve visible slot '"
                        + target.getVisibleSlot().getName() + "' from rebuilt aggregate output");
            }

            // Resolve hidden state slots
            ImmutableMap.Builder<AggType, Slot> resolvedHidden = ImmutableMap.builder();
            for (Map.Entry<AggType, Slot> entry : target.getHiddenStateSlots().entrySet()) {
                Slot resolvedSlot = slotByName.get(entry.getValue().getName());
                if (resolvedSlot == null) {
                    throw new AnalysisException("IVM: failed to resolve hidden state slot '"
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
            throw new AnalysisException("IVM normalization error: child plan has no row-id slot after normalization");
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
            throw new AnalysisException("IVM normalization error: child plan has no hidden slot named "
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
        throw new AnalysisException("IVM normalization error: unsupported hidden output expression: "
                + output.getClass().getSimpleName());
    }

    /**
     * Builds the row-id expression and returns whether it is deterministic as a pair.
     * - UNIQUE_KEYS (MOW or excluded): (buildRowIdHash(uk...), true)      — stable across refreshes
     * - DUP_KEYS: (UuidNumeric(), false)                                  — random per insert
     * - Excluded AGG_KEYS: (buildRowIdHash(agg key...), true)             — stable across refreshes
     * - Other key types: throws AnalysisException (unless excluded trigger table)
     */
    private Pair<Expression, Boolean> buildRowId(OlapTable table, LogicalOlapScan scan) {
        KeysType keysType = table.getKeysType();
        boolean isExcludedTriggerTable = isExcludedTriggerTable(table);
        if (keysType == KeysType.UNIQUE_KEYS) {
            if (!table.getEnableUniqueKeyMergeOnWrite() && !isExcludedTriggerTable) {
                throw new AnalysisException(
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
        throw new AnalysisException(
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
            throw new AnalysisException("IVM: no key columns found for "
                    + table.getKeysType() + " table: " + table.getName());
        }
        return Pair.of(IvmUtil.buildRowIdHash(keySlots), true);
    }

    private boolean isExcludedTriggerTable(OlapTable table) {
        if (statementContext == null || statementContext.getExcludedTriggerTables().isEmpty()) {
            return false;
        }
        TableNameInfo tableNameInfo = TableNameInfoUtils.fromTableOrNull(table);
        if (tableNameInfo == null) {
            return false;
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
     * @throws AnalysisException if validation fails
     */
    private static void checkAggFunctions(List<AggregateFunction> aggFunctions) {
        for (AggregateFunction aggFunc : aggFunctions) {
            if (aggFunc.isDistinct()) {
                throw new AnalysisException(
                        "Aggregate DISTINCT is not supported for IVM: " + aggFunc.toSql());
            }
            if (!SUPPORTED_AGG_FUNCTIONS.contains(aggFunc.getClass())) {
                throw new AnalysisException(
                        "Unsupported aggregate function for IVM: " + aggFunc.getName());
            }
        }
    }
}
