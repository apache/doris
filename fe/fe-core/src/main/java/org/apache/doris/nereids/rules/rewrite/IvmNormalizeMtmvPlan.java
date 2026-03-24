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
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.ivm.IvmContext;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MurmurHash364;
import org.apache.doris.nereids.trees.expressions.functions.scalar.UuidNumeric;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapTableSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalResultSink;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.nereids.types.LargeIntType;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Normalizes the MV define plan for IVM at both CREATE MV and REFRESH MV time.
 * - Injects __DORIS_IVM_ROW_ID_COL__ at index 0 of each OlapScan output via a wrapping LogicalProject:
 *   - MOW (UNIQUE_KEYS + merge-on-write): Alias(cast(murmur_hash3_64(uk...) as LargeInt),
 *     "__DORIS_IVM_ROW_ID_COL__")
 *     → deterministic (stable across refreshes)
 *   - DUP_KEYS: Alias(uuid_numeric(), "__DORIS_IVM_ROW_ID_COL__") → non-deterministic (random per insert)
 *   - Other key types: not supported, throws.
 * - Records (rowIdSlot → isDeterministic) in IvmContext on CascadesContext.
 * - visitLogicalProject propagates child's row-id slot if not already in outputs.
 * - visitLogicalFilter recurses into the child and preserves filter predicates/output shape.
 * - visitLogicalResultSink recurses into the child and prepends the row-id to output exprs.
 * - Whitelists supported plan nodes; throws AnalysisException for unsupported nodes.
 * Supported: OlapScan, filter, project, result sink, logical olap table sink.
 * TODO: avg rewrite, join support.
 */
public class IvmNormalizeMtmvPlan extends DefaultPlanRewriter<IvmContext> implements CustomRewriter {

    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        ConnectContext connectContext = jobContext.getCascadesContext().getConnectContext();
        if (connectContext == null || !connectContext.getSessionVariable().isEnableIvmNormalRewrite()) {
            return plan;
        }
        IvmContext ivmContext = new IvmContext();
        jobContext.getCascadesContext().setIvmContext(ivmContext);
        Plan result = plan.accept(this, ivmContext);
        ivmContext.setNormalizedPlan(result);
        return result;
    }

    // unsupported: any plan node not explicitly whitelisted below
    @Override
    public Plan visit(Plan plan, IvmContext ivmContext) {
        throw new AnalysisException("IVM does not support plan node: "
                + plan.getClass().getSimpleName());
    }

    // whitelisted: only OlapScan — inject IVM row-id at index 0
    @Override
    public Plan visitLogicalOlapScan(LogicalOlapScan scan, IvmContext ivmContext) {
        OlapTable table = scan.getTable();
        Pair<Expression, Boolean> rowId = buildRowId(table, scan);
        Alias rowIdAlias = new Alias(rowId.first, Column.IVM_ROW_ID_COL);
        ivmContext.addRowId(rowIdAlias.toSlot(), rowId.second);
        List<NamedExpression> outputs = ImmutableList.<NamedExpression>builder()
                .add(rowIdAlias)
                .addAll(scan.getOutput())
                .build();
        return new LogicalProject<>(outputs, scan);
    }

    // whitelisted: project — recurse into child, then propagate row-id if not already present
    @Override
    public Plan visitLogicalProject(LogicalProject<? extends Plan> project, IvmContext ivmContext) {
        Plan newChild = project.child().accept(this, ivmContext);
        List<NamedExpression> newOutputs = rewriteOutputsWithIvmHiddenColumns(newChild, project.getProjects());
        if (newChild == project.child() && newOutputs.equals(project.getProjects())) {
            return project;
        }
        return project.withProjectsAndChild(newOutputs, newChild);
    }

    @Override
    public Plan visitLogicalFilter(LogicalFilter<? extends Plan> filter, IvmContext ivmContext) {
        Plan newChild = filter.child().accept(this, ivmContext);
        return newChild == filter.child() ? filter : filter.withChildren(ImmutableList.of(newChild));
    }

    // whitelisted: result sink — recurse into child, then prepend row-id to output exprs
    @Override
    public Plan visitLogicalResultSink(LogicalResultSink<? extends Plan> sink, IvmContext ivmContext) {
        Plan newChild = sink.child().accept(this, ivmContext);
        List<NamedExpression> newOutputs = rewriteOutputsWithIvmHiddenColumns(newChild, sink.getOutputExprs());
        if (newChild == sink.child() && newOutputs.equals(sink.getOutputExprs())) {
            return sink;
        }
        return sink.withOutputExprs(newOutputs).withChildren(ImmutableList.of(newChild));
    }

    @Override
    public Plan visitLogicalOlapTableSink(LogicalOlapTableSink<? extends Plan> sink, IvmContext ivmContext) {
        Plan newChild = sink.child().accept(this, ivmContext);
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
        return Column.isIvmHiddenColumn(expression.getName());
    }

    private List<NamedExpression> rewriteOutputsWithIvmHiddenColumns(
            Plan normalizedChild, List<NamedExpression> outputs) {
        Map<String, Slot> ivmHiddenSlotsByName = collectIvmHiddenSlots(normalizedChild);
        if (!ivmHiddenSlotsByName.containsKey(Column.IVM_ROW_ID_COL)) {
            throw new AnalysisException("IVM normalization error: child plan has no row-id slot after normalization");
        }
        ImmutableList.Builder<NamedExpression> rewrittenOutputs = ImmutableList.builder();
        if (!hasIvmHiddenOutputInOutputs(outputs)) {
            rewrittenOutputs.addAll(ivmHiddenSlotsByName.values());
            rewrittenOutputs.addAll(outputs);
            return rewrittenOutputs.build();
        }
        for (Slot ivmHiddenSlot : ivmHiddenSlotsByName.values()) {
            if (outputs.stream().noneMatch(output -> ivmHiddenSlot.getName().equals(output.getName()))) {
                rewrittenOutputs.add(ivmHiddenSlot);
            }
        }
        for (NamedExpression output : outputs) {
            if (!isIvmHiddenOutput(output)) {
                rewrittenOutputs.add(output);
                continue;
            }
            rewrittenOutputs.add(rewriteIvmHiddenOutput(output, ivmHiddenSlotsByName));
        }
        return rewrittenOutputs.build();
    }

    private Map<String, Slot> collectIvmHiddenSlots(Plan normalizedChild) {
        return normalizedChild.getOutput().stream()
                .filter(slot -> Column.isIvmHiddenColumn(slot.getName()))
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
            return Pair.of(buildRowIdHash(keySlots), true);
        }
        if (keysType == KeysType.DUP_KEYS) {
            return Pair.of(new UuidNumeric(), false);
        }
        throw new AnalysisException("IVM does not support table key type: " + keysType
                + " for table: " + table.getName()
                + ". Only MOW (UNIQUE_KEYS with merge-on-write) and DUP_KEYS are supported.");
    }

    /**
     * Builds a hash expression over the given key slots for use as a deterministic row-id.
     * Currently uses murmur_hash3_64 (64-bit) which is not collision-safe for large tables.
     * TODO: replace with a 128-bit hash once BE supports it or a Java UDF is available.
     */
    private Expression buildRowIdHash(List<Expression> keySlots) {
        Expression first = keySlots.get(0);
        Expression[] rest = keySlots.subList(1, keySlots.size()).toArray(new Expression[0]);
        return new Cast(new MurmurHash364(first, rest), LargeIntType.INSTANCE);
    }
}
