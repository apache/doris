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
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.copier.DeepCopierContext;
import org.apache.doris.nereids.trees.copier.LogicalPlanDeepCopier;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.AssertTrue;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalResultSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;

import com.google.common.collect.ImmutableList;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Shared helper methods for IVM delta rewrite handlers.
 */
class IvmDeltaRewriteHelper {
    static final IvmDeltaRewriteHelper INSTANCE = new IvmDeltaRewriteHelper();

    private static final String NON_DET_ROW_ID_MSG_PREFIX =
            "IVM fallback: delete on non-deterministic row_id in ";

    private IvmDeltaRewriteHelper() {
    }

    Plan stripResultSink(Plan plan) {
        while (plan instanceof LogicalResultSink) {
            plan = ((LogicalResultSink<?>) plan).child();
        }
        return plan;
    }

    Slot findSlotByName(List<Slot> slots, String name) {
        for (Slot slot : slots) {
            if (name.equals(slot.getName())) {
                return slot;
            }
        }
        throw new AnalysisException("IVM failed to find slot: " + name);
    }

    /**
     * Add a runtime fallback guard when a joined delta may delete rows and the snapshot side row-id is
     * non-deterministic.
     *
     * <p>The delta side itself is known to have deterministic row-ids for delete rows: base-table deletes only come
     * from MOW tables and this property is preserved while delta rows are rewritten upward. Outer join does not break
     * this assumption. Its NULL-row repair delete is derived from the normalized preserved/non-delta side row-id plus
     * NULL; LEFT/RIGHT OUTER JOIN normalization requires that side to be deterministic, and FULL OUTER JOIN
     * normalization requires both children to be deterministic. Therefore this guard only needs to check the snapshot
     * side that is joined with the delta side.
     *
     * <p>For an aggregate MV, child join row-ids are only used to compute signed aggregate input rows. The MV row-id
     * is rebuilt from group-by keys at the aggregate, so delete rows can be applied without this child join fallback
     * guard.
     */
    IvmDeltaRewriteResult addNonDetGuardForJoinDelta(LogicalJoin<Plan, Plan> join,
            IvmDeltaRewriteResult leftResult, IvmDeltaRewriteResult rightResult, IvmRefreshContext ctx) {
        boolean deltaOnLeft = leftResult.dmlFactorSlot != null;
        Slot dmlFactorSlot = deltaOnLeft ? leftResult.dmlFactorSlot : rightResult.dmlFactorSlot;
        Plan snapshotSidePlan = deltaOnLeft ? join.right() : join.left();

        if (needNonDetGuard(snapshotSidePlan, ctx)) {
            return wrapDmlFactorWithNonDetGuard(
                    new IvmDeltaRewriteResult(join, dmlFactorSlot), join.getJoinType());
        }
        return new IvmDeltaRewriteResult(join, dmlFactorSlot);
    }

    /**
     * Checks if the snapshot side's row_id slot is non-deterministic.
     * Returns true when normalizeResult or row_id slot is unavailable. Aggregate MV returns false because final
     * delete rows use aggregate group-key row-id instead of child join row-id.
     */
    boolean needNonDetGuard(Plan snapshotSidePlan, IvmRefreshContext ctx) {
        IvmNormalizeResult normalizeResult = ctx.getNormalizeResult();
        if (normalizeResult == null) {
            return true;
        }
        // Aggregate MV delete rows are applied by the aggregate output row-id, which is rebuilt from group-by keys.
        // The child join row-id is only an intermediate input for aggregate state changes, so it does not need this
        // fallback guard even when the snapshot side row-id is non-deterministic.
        if (normalizeResult.isAggMv()) {
            return false;
        }
        Slot rowIdSlot = IvmUtil.findRowIdSlotOrNull(snapshotSidePlan.getOutput());
        if (rowIdSlot == null) {
            return true;
        } else {
            return !normalizeResult.isDeterministic(rowIdSlot);
        }
    }

    /**
     * Wraps the dml_factor slot with an assert_true guard that triggers a runtime exception
     * when dml_factor < 0, causing fallback to full refresh.
     */
    IvmDeltaRewriteResult wrapDmlFactorWithNonDetGuard(IvmDeltaRewriteResult result, JoinType joinType) {
        String msg = NON_DET_ROW_ID_MSG_PREFIX + joinType;
        Expression guardedExpr = new If(
                new AssertTrue(new GreaterThanEqual(result.dmlFactorSlot,
                        new TinyIntLiteral((byte) 0)), new StringLiteral(msg)),
                result.dmlFactorSlot, new NullLiteral(result.dmlFactorSlot.getDataType()));
        Alias guardedAlias = new Alias(guardedExpr, Column.IVM_DML_FACTOR_COL);

        ImmutableList.Builder<NamedExpression> projectOutputs = ImmutableList.builder();
        for (Slot slot : result.plan.getOutput()) {
            if (Column.IVM_DML_FACTOR_COL.equals(slot.getName())) {
                projectOutputs.add(guardedAlias);
            } else {
                projectOutputs.add(slot);
            }
        }
        LogicalProject<?> guardProject = new LogicalProject<>(projectOutputs.build(), result.plan);
        Slot newDmlFactorSlot = guardProject.getOutput().stream()
                .filter(s -> Column.IVM_DML_FACTOR_COL.equals(s.getName()))
                .findFirst()
                .orElseThrow(() -> new AnalysisException("IVM: lost dml_factor after non-det guard"));
        return new IvmDeltaRewriteResult(guardProject, newDmlFactorSlot);
    }

    /**
     * Wraps the visitor-rewritten plan with a final project that maps dml_factor to delete sign.
     */
    Plan buildSinkProject(IvmDeltaRewriteResult result, IvmRefreshContext ctx) {
        List<Slot> output = result.plan.getOutput();
        List<String> insertedColumns = ctx.getMtmv().getInsertedColumnNames();
        ImmutableList.Builder<NamedExpression> sinkOutputs = ImmutableList.builderWithExpectedSize(
                insertedColumns.size() + 1);
        for (String colName : insertedColumns) {
            sinkOutputs.add(findSlotByName(output, colName));
        }
        sinkOutputs.add(new Alias(
                new If(new LessThan(result.dmlFactorSlot, new TinyIntLiteral((byte) 0)),
                        new TinyIntLiteral((byte) 1), new TinyIntLiteral((byte) 0)),
                Column.DELETE_SIGN));
        return new LogicalProject<>(sinkOutputs.build(), result.plan);
    }

    /**
     * Remap an existing source-to-output mapping through a fresh identity Project.
     */
    Pair<Plan, Map<Slot, Slot>> remapOutputs(Pair<Plan, Map<Slot, Slot>> plan) {
        return remapOutputs(plan.first, plan.second);
    }

    /**
     * Add an identity Project and return a mapping from the original source slots to the new Project outputs.
     */
    Pair<Plan, Map<Slot, Slot>> remapOutputs(Plan plan, Map<Slot, Slot> sourceToPlanOutput) {
        ImmutableList.Builder<NamedExpression> projects = ImmutableList.builderWithExpectedSize(
                plan.getOutput().size());
        Map<Slot, Slot> planOutputToAlias = new HashMap<>();
        Map<Slot, Slot> outputMapping = new HashMap<>();
        for (Slot slot : plan.getOutput()) {
            Alias alias = new Alias(slot, slot.getName());
            projects.add(alias);
            planOutputToAlias.put(slot, alias.toSlot());
        }
        for (Map.Entry<Slot, Slot> entry : sourceToPlanOutput.entrySet()) {
            outputMapping.put(entry.getKey(), planOutputToAlias.get(entry.getValue()));
        }
        LogicalProject<Plan> project = new LogicalProject<>(projects.build(), (LogicalPlan) plan);
        return Pair.of(project, outputMapping);
    }

    /**
     * Wrap an internal copy with a unique subquery alias and keep its slot mapping valid.
     */
    Pair<Plan, Map<Slot, Slot>> aliasPlan(Pair<Plan, Map<Slot, Slot>> plan, String alias) {
        Plan aliasNode = new LogicalSubQueryAlias<>(alias, plan.first);
        return Pair.of(aliasNode, remapOutputMapping(plan.second, plan.first.getOutput(), aliasNode.getOutput()));
    }

    /**
     * Build UNION ALL with synthetic output slots so the union does not reuse child ExprIds.
     */
    LogicalUnion buildUnionAll(List<Plan> children) {
        Plan first = children.get(0);
        ImmutableList.Builder<NamedExpression> outputs = ImmutableList.builder();
        for (int i = 0; i < first.getOutput().size(); i++) {
            Slot slot = first.getOutput().get(i);
            outputs.add(new SlotReference(slot.getName(), slot.getDataType(), unionOutputNullable(children, i)));
        }
        ImmutableList.Builder<List<SlotReference>> childrenOutputs = ImmutableList.builder();
        for (Plan child : children) {
            ImmutableList.Builder<SlotReference> childOutput = ImmutableList.builder();
            for (Slot slot : child.getOutput()) {
                childOutput.add((SlotReference) slot);
            }
            childrenOutputs.add(childOutput.build());
        }
        return new LogicalUnion(Qualifier.ALL, outputs.build(), childrenOutputs.build(),
                ImmutableList.of(), false, children);
    }

    /**
     * Project union outputs back to the target schema and preserve target ExprIds for downstream row-id projection.
     */
    LogicalProject<Plan> projectUnionOutputs(LogicalUnion union, List<Slot> targetOutputs) {
        if (union.getOutput().size() != targetOutputs.size()) {
            throw new AnalysisException("IVM outer join rewrite changed union output size from "
                    + targetOutputs.size() + " to " + union.getOutput().size());
        }
        ImmutableList.Builder<NamedExpression> projects = ImmutableList.builderWithExpectedSize(
                targetOutputs.size());
        for (int i = 0; i < targetOutputs.size(); i++) {
            Slot source = union.getOutput().get(i);
            Slot target = targetOutputs.get(i);
            projects.add(new Alias(target.getExprId(), source, target.getName()));
        }
        return new LogicalProject<>(projects.build(), union);
    }

    /**
     * Deep copy a plan before reusing it in another branch, and return the copied output mapping.
     */
    Pair<Plan, Map<Slot, Slot>> freshPlan(Plan plan) {
        DeepCopierContext copierContext = new DeepCopierContext();
        LogicalPlan freshPlan = LogicalPlanDeepCopier.INSTANCE.deepCopy((LogicalPlan) plan, copierContext);
        return Pair.of(freshPlan, mapCopiedOutputs(plan.getOutput(), freshPlan.getOutput(), copierContext));
    }

    private Map<Slot, Slot> mapCopiedOutputs(List<Slot> sourceOutput, List<Slot> targetOutput,
            DeepCopierContext copierContext) {
        Map<ExprId, Slot> targetOutputByExprId = new HashMap<>();
        for (Slot slot : targetOutput) {
            targetOutputByExprId.put(slot.getExprId(), slot);
        }
        Map<Slot, Slot> outputMapping = new HashMap<>();
        for (Slot sourceSlot : sourceOutput) {
            ExprId copiedExprId = copierContext.exprIdReplaceMap.get(sourceSlot.getExprId());
            Slot targetSlot = copiedExprId == null ? null : targetOutputByExprId.get(copiedExprId);
            if (targetSlot == null) {
                throw new AnalysisException("IVM outer join rewrite lost copied output slot: " + sourceSlot);
            }
            outputMapping.put(sourceSlot, targetSlot);
        }
        return outputMapping;
    }

    private Map<Slot, Slot> mapOutputs(List<Slot> sourceOutput, List<Slot> targetOutput) {
        if (sourceOutput.size() != targetOutput.size()) {
            throw new AnalysisException("IVM outer join rewrite changed output size from "
                    + sourceOutput.size() + " to " + targetOutput.size());
        }
        Map<Slot, Slot> outputMapping = new HashMap<>();
        for (int i = 0; i < sourceOutput.size(); i++) {
            outputMapping.put(sourceOutput.get(i), targetOutput.get(i));
        }
        return outputMapping;
    }

    private Map<Slot, Slot> remapOutputMapping(Map<Slot, Slot> sourceToOldOutput,
            List<Slot> oldOutput, List<Slot> newOutput) {
        Map<Slot, Slot> oldToNew = mapOutputs(oldOutput, newOutput);
        Map<Slot, Slot> sourceToNewOutput = new HashMap<>();
        for (Map.Entry<Slot, Slot> entry : sourceToOldOutput.entrySet()) {
            sourceToNewOutput.put(entry.getKey(), oldToNew.get(entry.getValue()));
        }
        return sourceToNewOutput;
    }

    private boolean unionOutputNullable(List<Plan> children, int index) {
        for (Plan child : children) {
            if (child.getOutput().get(index).nullable()) {
                return true;
            }
        }
        return false;
    }

}
