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
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapTableStreamScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;

import com.google.common.collect.ImmutableList;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Shared helper methods for IVM delta rewrite handlers.
 */
public class IvmDeltaRewriteHelper {
    public static final IvmDeltaRewriteHelper INSTANCE = new IvmDeltaRewriteHelper();

    private IvmDeltaRewriteHelper() {
    }

    Slot findSlotByName(List<Slot> slots, String name) {
        Slot slot = findSlotByNameOrNull(slots, name);
        if (slot == null) {
            throw new IvmException(IvmFailureReason.PLAN_REWRITE_FAILED,
                    "IVM failed to find slot: " + name);
        }
        return slot;
    }

    Slot findSlotByNameOrNull(List<Slot> slots, String name) {
        for (Slot slot : slots) {
            if (name.equals(slot.getName())) {
                return slot;
            }
        }
        return null;
    }

    /**
     * Adds the root-level fallback guard for a non-deterministic MV row-id. Inserts remain valid;
     * any delete delta fails so the caller can fall back to a full refresh.
     */
    IvmDeltaRewriteResult wrapDmlFactorWithRootNonDetGuard(IvmDeltaRewriteResult result) {
        String msg = IvmFailureClassifier.NON_DETERMINISTIC_ROW_ID_MSG_PREFIX;
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
                .orElseThrow(() -> new IvmException(IvmFailureReason.PLAN_REWRITE_FAILED,
                        "IVM: lost dml_factor after non-det guard"));
        Slot newSequenceSlot = findSlotByName(guardProject.getOutput(), Column.SEQUENCE_COL);
        return new IvmDeltaRewriteResult(guardProject, newDmlFactorSlot, newSequenceSlot, result.maxSeqSuffix);
    }

    Optional<IvmDeltaRewriteResult> combineDeltaResults(List<IvmDeltaRewriteResult> children,
            List<Slot> targetOutputs) {
        if (children.isEmpty()) {
            return Optional.empty();
        }
        if (children.size() == 1) {
            return Optional.of(children.get(0));
        }
        List<Plan> unionChildren = new java.util.ArrayList<>(children.size());
        long maxSeqSuffix = 0;
        for (IvmDeltaRewriteResult child : children) {
            unionChildren.add(freshDeltaResult(child).plan);
            maxSeqSuffix = Math.max(maxSeqSuffix, child.maxSeqSuffix);
        }
        LogicalUnion union = buildUnionAll(unionChildren);
        List<Slot> projectionTargets = new java.util.ArrayList<>(targetOutputs);
        projectionTargets.add(union.getOutput().get(union.getOutput().size() - 2));
        projectionTargets.add(union.getOutput().get(union.getOutput().size() - 1));
        LogicalProject<Plan> project = projectUnionOutputs(union, projectionTargets);
        Slot dmlFactorSlot = findSlotByName(project.getOutput(), Column.IVM_DML_FACTOR_COL);
        Slot sequenceSlot = findSlotByName(project.getOutput(), Column.SEQUENCE_COL);
        return Optional.of(new IvmDeltaRewriteResult(project, dmlFactorSlot, sequenceSlot, maxSeqSuffix));
    }

    IvmDeltaRewriteResult freshDeltaResult(IvmDeltaRewriteResult source) {
        Pair<Plan, Map<Slot, Slot>> copied = freshPlan(source.plan);
        return new IvmDeltaRewriteResult(copied.first, copied.second.get(source.dmlFactorSlot),
                copied.second.get(source.sequenceSlot), source.maxSeqSuffix);
    }

    /**
     * Detach the top consecutive project chain so delta rewrite can focus on the normalized query root.
     */
    public Pair<Plan, List<LogicalProject<?>>> detachAdaptProjectChain(Plan root) {
        List<LogicalProject<?>> projects = new java.util.ArrayList<>();
        Plan current = root;
        while (current instanceof LogicalProject) {
            LogicalProject<?> project = (LogicalProject<?>) current;
            projects.add(project);
            if (containsSinkHiddenColumnPlaceholder(project)) {
                return Pair.of(project.child(), projects);
            }
            current = project.child();
        }
        return Pair.of(root, ImmutableList.of());
    }

    /**
     * Finalize the rewritten delta plan into the sink query shape, then reattach the detached top projects
     * while fixing adapter placeholders such as delete_sign, sequence, and version.
     */
    public Plan finalizeQuery(Pair<Plan, List<LogicalProject<?>>> prefixChain,
            IvmDeltaRewriteResult result, IvmIncrRefreshContext ctx) {
        LogicalProject<?> finalProject = makeDeleteSignProject(result, ctx);
        List<LogicalProject<?>> projects = prefixChain.second;
        if (projects.isEmpty()) {
            return finalProject;
        }
        Plan current = attachAdapterProject(projects.get(projects.size() - 1), finalProject);
        for (int i = projects.size() - 2; i >= 0; i--) {
            current = projects.get(i).withChildren(ImmutableList.of(current));
        }
        return current;
    }

    public List<NamedExpression> rebindSinkOutputs(List<NamedExpression> outputs, List<Slot> childOutputs,
            String context) {
        Map<String, Slot> outputByName = indexOutputsByName(childOutputs);
        return outputs.stream()
                .map(outputExpr -> requireOutputSlot(outputExpr.getName(), outputByName, context))
                .collect(Collectors.toList());
    }

    private boolean isSinkHiddenColumnPlaceholder(Alias alias) {
        return IvmUtil.isCommonHiddenSlot(alias.getName()) && alias.child().isLiteral();
    }

    private boolean containsSinkHiddenColumnPlaceholder(LogicalProject<?> project) {
        return project.getProjects().stream()
                .filter(Alias.class::isInstance)
                .map(Alias.class::cast)
                .anyMatch(this::isSinkHiddenColumnPlaceholder);
    }

    private LogicalProject<?> makeDeleteSignProject(IvmDeltaRewriteResult result, IvmIncrRefreshContext ctx) {
        List<Slot> output = result.plan.getOutput();
        List<String> insertedColumns = ctx.getMtmv().getInsertedColumnNames();
        ImmutableList.Builder<NamedExpression> outputs = ImmutableList.builderWithExpectedSize(
                insertedColumns.size() + 2);
        for (String colName : insertedColumns) {
            outputs.add(findSlotByName(output, colName));
        }
        Slot sequenceSlot = findSlotByNameOrNull(output, Column.SEQUENCE_COL);
        outputs.add(sequenceSlot != null ? sequenceSlot
                : new Alias(IvmUtil.getCommonHiddenSlotDefault(Column.SEQUENCE_COL), Column.SEQUENCE_COL));
        outputs.add(new Alias(
                new If(new LessThan(result.dmlFactorSlot, new TinyIntLiteral((byte) 0)),
                        new TinyIntLiteral((byte) 1), new TinyIntLiteral((byte) 0)),
                Column.DELETE_SIGN));
        return new LogicalProject<>(outputs.build(), result.plan);
    }

    private LogicalProject<?> attachAdapterProject(LogicalProject<?> adapterProject, LogicalProject<?> finalProject) {
        Map<String, Slot> childOutputByName = indexOutputsByName(finalProject.getOutput());
        List<NamedExpression> rewrittenAdapterProjects = new java.util.ArrayList<>(adapterProject.getProjects().size());
        for (NamedExpression project : adapterProject.getProjects()) {
            if (project instanceof Alias && isSinkHiddenColumnPlaceholder((Alias) project)) {
                Alias alias = (Alias) project;
                Slot childSlot = childOutputByName.get(project.getName());
                if (childSlot != null) {
                    rewrittenAdapterProjects.add(
                            new Alias(alias.getExprId(), childSlot, alias.getName(), alias.isNameFromChild()));
                    continue;
                }
            }
            if (!(project instanceof Alias)) {
                throw new IvmException(IvmFailureReason.PLAN_REWRITE_FAILED,
                        "IVM adapter project expects alias output: " + project);
            }
            Alias alias = (Alias) project;
            Slot childSlot = childOutputByName.get(alias.getName());
            if (childSlot == null) {
                if (Column.VERSION_COL.equals(alias.getName())) {
                    rewrittenAdapterProjects.add(alias);
                    continue;
                }
                throw new IvmException(IvmFailureReason.PLAN_REWRITE_FAILED,
                        "IVM adapter project output mismatch, missing slot="
                                + alias.getName() + ", childOutputs=" + childOutputByName.keySet());
            }
            rewrittenAdapterProjects.add(
                    new Alias(alias.getExprId(), childSlot, alias.getName(), alias.isNameFromChild()));
        }
        return adapterProject.withProjectsAndChild(rewrittenAdapterProjects, finalProject);
    }

    private Map<String, Slot> indexOutputsByName(List<Slot> outputs) {
        Map<String, Slot> outputByName = new LinkedHashMap<>();
        for (Slot output : outputs) {
            outputByName.putIfAbsent(output.getName(), output);
        }
        return outputByName;
    }

    private Slot requireOutputSlot(String name, Map<String, Slot> outputByName, String context) {
        Slot slot = outputByName.get(name);
        if (slot != null) {
            return slot;
        }
        throw new IvmException(IvmFailureReason.PLAN_REWRITE_FAILED,
                "IVM incremental refresh output mismatch, missing slot="
                        + name + ", childOutputs=" + outputByName.keySet() + ", context=" + context);
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
            throw new IvmException(IvmFailureReason.PLAN_REWRITE_FAILED,
                    "IVM outer join rewrite changed union output size from "
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
                throw new IvmException(IvmFailureReason.PLAN_REWRITE_FAILED,
                        "IVM outer join rewrite lost copied output slot: " + sourceSlot);
            }
            outputMapping.put(sourceSlot, targetSlot);
        }
        return outputMapping;
    }

    private Map<Slot, Slot> mapOutputs(List<Slot> sourceOutput, List<Slot> targetOutput) {
        if (sourceOutput.size() != targetOutput.size()) {
            throw new IvmException(IvmFailureReason.PLAN_REWRITE_FAILED,
                    "IVM outer join rewrite changed output size from "
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

    // ---------------------------------------------------------------------------
    // Static helpers for IVM delta scan identification and conversion
    // ---------------------------------------------------------------------------

    /**
     * Returns true if the scan is an incremental delta scan (LogicalOlapTableStreamScan
     * with isIncremental=true).
     */
    boolean isIncrementalDeltaScan(Plan scan) {
        return scan instanceof LogicalOlapTableStreamScan
                && ((LogicalOlapTableStreamScan) scan).isIncremental();
    }

    /**
     * When a hidden column from the old plan is missing in the new plan's output,
     * choose a suitable default literal instead of NULL. For MOW tables, the
     * bound OLAP scan is naturally guarded by {@code delete_sign = 0}; using
     * NULL for the stream scan's missing delete_sign column would make that
     * filter always false. Version and sequence use their valid default value 0 when
     * absent from the stream scan. Other hidden columns still fall back to NULL.
     */
    Literal hiddenColumnFallbackLiteral(NamedExpression oldExpr) {
        String name = oldExpr.getName();
        if (IvmUtil.isCommonHiddenSlot(name)) {
            return IvmUtil.getCommonHiddenSlotDefault(name);
        }
        return new NullLiteral(oldExpr.getDataType());
    }

    /**
     * Add a project that remaps a replacement plan's output to the original OLAP scan's ExprIds and names.
     */
    LogicalPlan remapOlapScanToPlan(LogicalOlapScan oldScan, LogicalPlan newPlan) {
        Map<String, Slot> newSlotByName = new LinkedHashMap<>();
        for (Slot slot : newPlan.getOutput()) {
            newSlotByName.put(slot.getName(), slot);
        }

        ImmutableList.Builder<NamedExpression> projects =
                ImmutableList.builderWithExpectedSize(oldScan.getOutput().size());
        for (Slot oldSlot : oldScan.getOutput()) {
            Slot newSlot = newSlotByName.get(oldSlot.getName());
            if (newSlot != null) {
                if (!oldSlot.getDataType().equals(newSlot.getDataType())) {
                    throw new IvmException(IvmFailureReason.PLAN_REWRITE_FAILED,
                            "IVM: remap output type mismatch for column "
                                    + oldSlot.getName() + ": old=" + oldSlot.getDataType()
                                    + ", new=" + newSlot.getDataType());
                }
                projects.add(new Alias(oldSlot.getExprId(), newSlot, oldSlot.getName()));
            } else if (oldSlot.getName().startsWith(Column.HIDDEN_COLUMN_PREFIX)) {
                projects.add(new Alias(oldSlot.getExprId(),
                        hiddenColumnFallbackLiteral(oldSlot), oldSlot.getName()));
            } else {
                throw new IvmException(IvmFailureReason.PLAN_REWRITE_FAILED,
                        "IVM: new plan missing column "
                                + oldSlot.getName() + " when remapping scan output");
            }
        }
        return new LogicalProject<>(projects.build(), newPlan);
    }

    /**
     * Rebuild the project above an incremental stream scan after replacing that child with another plan
     * such as a pre/post snapshot scan. The remapped project preserves the original ExprIds/names while
     * rebinding placeholder hidden columns back to real snapshot outputs when they exist.
     */
    LogicalProject<?> remapStreamScanToPlan(LogicalProject<?> oldProject, LogicalPlan newPlan) {
        Map<String, Slot> newSlotByName = new LinkedHashMap<>();
        for (Slot slot : newPlan.getOutput()) {
            newSlotByName.put(slot.getName(), slot);
        }
        ImmutableList.Builder<NamedExpression> newProjects =
                ImmutableList.builderWithExpectedSize(oldProject.getProjects().size());
        for (NamedExpression expr : oldProject.getProjects()) {
            if (expr instanceof Alias) {
                Alias alias = (Alias) expr;
                Expression childExpr = alias.child();
                if (childExpr instanceof Slot) {
                    newProjects.add(remapAliasToNewChild(alias, (Slot) childExpr, newSlotByName));
                } else if (childExpr instanceof Literal
                        && childExpr.equals(hiddenColumnFallbackLiteral(alias))) {
                    Slot newSlot = newSlotByName.get(alias.getName());
                    if (newSlot == null) {
                        newProjects.add(new Alias(alias.getExprId(),
                                hiddenColumnFallbackLiteral(alias), alias.getName()));
                        continue;
                    }
                    newProjects.add(new Alias(alias.getExprId(), newSlot, alias.getName()));
                } else {
                    throw new IvmException(IvmFailureReason.PLAN_REWRITE_FAILED,
                            "IVM: unsupported alias child in snapshot remap: " + childExpr);
                }
            } else if (expr instanceof Slot) {
                Slot oldSlot = (Slot) expr;
                Slot newSlot = newSlotByName.get(oldSlot.getName());
                if (newSlot == null) {
                    if (oldSlot.getName().startsWith(Column.HIDDEN_COLUMN_PREFIX)) {
                        newProjects.add(new Alias(oldSlot.getExprId(),
                                hiddenColumnFallbackLiteral(oldSlot), oldSlot.getName()));
                        continue;
                    }
                    throw new IvmException(IvmFailureReason.PLAN_REWRITE_FAILED,
                            "IVM: new child missing passthrough slot "
                                    + oldSlot.getName() + " when remapping project child");
                }
                newProjects.add(new Alias(oldSlot.getExprId(), newSlot, oldSlot.getName()));
            } else {
                throw new IvmException(IvmFailureReason.PLAN_REWRITE_FAILED,
                        "IVM: unsupported project output in snapshot remap: " + expr);
            }
        }
        return new LogicalProject<>(newProjects.build(), newPlan);
    }

    private Alias remapAliasToNewChild(Alias alias, Slot oldChildSlot, Map<String, Slot> newSlotByName) {
        Slot newSlot = newSlotByName.get(oldChildSlot.getName());
        if (newSlot != null) {
            return new Alias(alias.getExprId(), newSlot, alias.getName());
        }
        if (oldChildSlot.getName().startsWith(Column.HIDDEN_COLUMN_PREFIX)) {
            return new Alias(alias.getExprId(), new NullLiteral(oldChildSlot.getDataType()), alias.getName());
        }
        throw new IvmException(IvmFailureReason.PLAN_REWRITE_FAILED,
                "IVM: new child missing column "
                        + oldChildSlot.getName() + " when remapping project child");
    }

}
