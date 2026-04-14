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
import org.apache.doris.common.Pair;
import org.apache.doris.mtmv.ivm.IvmAggMeta;
import org.apache.doris.mtmv.ivm.IvmAggMeta.AggTarget;
import org.apache.doris.mtmv.ivm.IvmAggMeta.AggType;
import org.apache.doris.mtmv.ivm.IvmNormalizeResult;
import org.apache.doris.mtmv.ivm.IvmUtil;
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
 * - Injects __DORIS_IVM_ROW_ID_COL__ at index 0 of each OlapScan output via a wrapping LogicalProject:
 *   - MOW (UNIQUE_KEYS + merge-on-write): Alias(cast(murmur_hash3_64(uk...) as LargeInt),
 *     "__DORIS_IVM_ROW_ID_COL__")
 *     → deterministic (stable across refreshes)
 *   - DUP_KEYS: Alias(uuid_numeric(), "__DORIS_IVM_ROW_ID_COL__") → non-deterministic (random per insert)
 *   - Other key types: not supported, throws.
 * - Records (rowIdSlot → isDeterministic) in IvmNormalizeResult on CascadesContext.
 * - visitLogicalProject propagates child's row-id slot if not already in outputs.
 * - visitLogicalFilter recurses into the child and preserves filter predicates/output shape.
 * - visitLogicalResultSink recurses into the child and prepends the row-id to output exprs.
 * - Whitelists supported plan nodes; throws AnalysisException for unsupported nodes.
 * Supported: OlapScan, filter, project, result sink, logical olap table sink.
 * TODO: avg rewrite, join support.
 */
public class IvmNormalizeMtmv extends DefaultPlanRewriter<Boolean> implements CustomRewriter {

    private static final Set<Class<? extends AggregateFunction>> SUPPORTED_AGG_FUNCTIONS =
            ImmutableSet.of(Count.class, Sum.class, Avg.class, Min.class, Max.class);

    private final IvmNormalizeResult normalizeResult = new IvmNormalizeResult();

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
        List<AggTarget> resolvedTargets = resolveAggTargetSlots(aggTargets, hiddenAggOutputs, newAggSlots);

        // Resolve group key slots from the new Aggregate output by matching groupByExprs names
        List<Slot> resolvedGroupKeys = new ArrayList<>();
        for (Expression groupByExpr : groupByExprs) {
            String name = ((Slot) groupByExpr).getName();
            for (Slot newSlot : newAggSlots) {
                if (newSlot.getName().equals(name)) {
                    resolvedGroupKeys.add(newSlot);
                    break;
                }
            }
        }
        if (resolvedGroupKeys.size() != groupByExprs.size()) {
            throw new AnalysisException("IVM: failed to resolve all group-by key slots from rebuilt aggregate. "
                    + "Expected " + groupByExprs.size() + " but resolved " + resolvedGroupKeys.size());
        }

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
        Map<String, Alias> hiddenAliases = new LinkedHashMap<>();

        if (aggFunc instanceof Count) {
            Count countFunc = (Count) aggFunc;
            if (countFunc.isStar()) {
                aggType = AggType.COUNT_STAR;
                hiddenAliases.put("COUNT", new Alias(new Count(),
                        IvmUtil.ivmAggHiddenColumnName(ordinal, "COUNT")));
            } else {
                aggType = AggType.COUNT_EXPR;
                hiddenAliases.put("COUNT", new Alias(
                        new Count(aggFunc.child(0)),
                        IvmUtil.ivmAggHiddenColumnName(ordinal, "COUNT")));
            }
        } else if (aggFunc instanceof Sum) {
            aggType = AggType.SUM;
            hiddenAliases.put("SUM", new Alias(
                    new Sum(aggFunc.child(0)),
                    IvmUtil.ivmAggHiddenColumnName(ordinal, "SUM")));
            hiddenAliases.put("COUNT", new Alias(
                    new Count(aggFunc.child(0)),
                    IvmUtil.ivmAggHiddenColumnName(ordinal, "COUNT")));
        } else if (aggFunc instanceof Avg) {
            aggType = AggType.AVG;
            hiddenAliases.put("SUM", new Alias(
                    new Sum(aggFunc.child(0)),
                    IvmUtil.ivmAggHiddenColumnName(ordinal, "SUM")));
            hiddenAliases.put("COUNT", new Alias(
                    new Count(aggFunc.child(0)),
                    IvmUtil.ivmAggHiddenColumnName(ordinal, "COUNT")));
        } else if (aggFunc instanceof Min) {
            aggType = AggType.MIN;
            hiddenAliases.put("MIN", new Alias(
                    new Min(aggFunc.child(0)),
                    IvmUtil.ivmAggHiddenColumnName(ordinal, "MIN")));
            hiddenAliases.put("COUNT", new Alias(
                    new Count(aggFunc.child(0)),
                    IvmUtil.ivmAggHiddenColumnName(ordinal, "COUNT")));
        } else if (aggFunc instanceof Max) {
            aggType = AggType.MAX;
            hiddenAliases.put("MAX", new Alias(
                    new Max(aggFunc.child(0)),
                    IvmUtil.ivmAggHiddenColumnName(ordinal, "MAX")));
            hiddenAliases.put("COUNT", new Alias(
                    new Count(aggFunc.child(0)),
                    IvmUtil.ivmAggHiddenColumnName(ordinal, "COUNT")));
        } else {
            throw new AnalysisException("IVM: unsupported aggregate function: " + aggFunc.getName());
        }

        hiddenAggOutputs.addAll(hiddenAliases.values());

        // Build AggTarget with placeholder slots (to be resolved after Aggregate is rebuilt)
        ImmutableMap.Builder<String, Slot> placeholderHiddenSlots = ImmutableMap.builder();
        for (Map.Entry<String, Alias> entry : hiddenAliases.entrySet()) {
            placeholderHiddenSlots.put(entry.getKey(), entry.getValue().toSlot());
        }

        List<Expression> exprArgs = ImmutableList.of();
        if (!(aggFunc instanceof Count && ((Count) aggFunc).isStar())) {
            exprArgs = ImmutableList.of(aggFunc.child(0));
        }

        aggTargets.add(new AggTarget(ordinal, aggType, origAlias.toSlot(),
                placeholderHiddenSlots.build(), exprArgs));
    }

    /**
     * Resolves placeholder AggTarget slots to actual slots from the rebuilt Aggregate output.
     * Matching is done by column name.
     */
    private List<AggTarget> resolveAggTargetSlots(List<AggTarget> placeholderTargets,
            List<NamedExpression> hiddenAggOutputs, List<Slot> newAggSlots) {
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
            ImmutableMap.Builder<String, Slot> resolvedHidden = ImmutableMap.builder();
            for (Map.Entry<String, Slot> entry : target.getHiddenStateSlots().entrySet()) {
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

    private boolean hasIvmHiddenOutputInOutputs(List<NamedExpression> outputs) {
        return outputs.stream()
                .anyMatch(this::isIvmHiddenOutput);
    }

    private boolean isIvmHiddenOutput(NamedExpression expression) {
        return IvmUtil.isIvmHiddenColumn(expression.getName());
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
        if (!hasIvmHiddenOutputInOutputs(outputs)) {
            // No hidden outputs in original list: prepend row_id, then originals, then other hidden
            rewrittenOutputs.add(rowIdSlot);
            rewrittenOutputs.addAll(outputs);
            rewrittenOutputs.addAll(otherHiddenSlots.values());
            return rewrittenOutputs.build();
        }

        // Outputs already contain some hidden columns (e.g. BindSink placeholders).
        // Replace hidden outputs in-place to preserve positions and ExprIds.
        for (NamedExpression output : outputs) {
            if (isIvmHiddenOutput(output)) {
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
     * - MOW: (buildRowIdHash(uk...), true)  — stable across refreshes
     * - DUP_KEYS: (UuidNumeric(), false)    — random per insert
     * - Other key types: throws AnalysisException
     */
    private Pair<Expression, Boolean> buildRowId(OlapTable table, LogicalOlapScan scan) {
        KeysType keysType = table.getKeysType();
        if (keysType == KeysType.UNIQUE_KEYS && table.getEnableUniqueKeyMergeOnWrite()) {
            List<String> keyColNames = table.getBaseSchemaKeyColumns().stream()
                    .map(Column::getName)
                    .collect(Collectors.toList());
            List<Expression> keySlots = scan.getOutput().stream()
                    .filter(s -> keyColNames.contains(s.getName()))
                    .collect(Collectors.toList());
            if (keySlots.isEmpty()) {
                throw new AnalysisException("IVM: no unique key columns found for MOW table: "
                        + table.getName());
            }
            return Pair.of(IvmUtil.buildRowIdHash(keySlots), true);
        }
        if (keysType == KeysType.DUP_KEYS) {
            return Pair.of(new UuidNumeric(), false);
        }
        throw new AnalysisException("IVM does not support table key type: " + keysType
                + " for table: " + table.getName()
                + ". Only MOW (UNIQUE_KEYS with merge-on-write) and DUP_KEYS are supported.");
    }

    /**
     * Validates that all aggregate functions are supported for IVM.
     *
     * <p>Rules enforced:
     * <ol>
     *   <li>At least one aggregate function must be present (bare GROUP BY is not supported).</li>
     *   <li>DISTINCT aggregates are not supported.</li>
     *   <li>Only count, sum, and avg are supported.</li>
     * </ol>
     *
     * @throws AnalysisException if validation fails
     */
    private static void checkAggFunctions(List<AggregateFunction> aggFunctions) {
        if (aggFunctions.isEmpty()) {
            throw new AnalysisException(
                    "GROUP BY without aggregate functions is not supported for IVM");
        }
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
