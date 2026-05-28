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

package org.apache.doris.nereids.processor.post.materialize;

import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.processor.post.PlanPostProcessor;
import org.apache.doris.nereids.processor.post.Validator;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.PreferPushDownProject;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.plans.AbstractPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.CatalogRelation;
import org.apache.doris.nereids.trees.plans.algebra.Relation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDistribute;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLazyMaterialize;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTVFRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTopN;
import org.apache.doris.qe.SessionVariable;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * post rule to do lazy materialize
 */
public class LazyMaterializeTopN extends PlanPostProcessor {
    private static final Logger LOG = LogManager.getLogger(LazyMaterializeTopN.class);
    private boolean hasMaterialized = false;

    @Override
    public Plan visitPhysicalTopN(PhysicalTopN topN, CascadesContext ctx) {
        try {
            Plan result = computeTopN(topN, ctx);
            if (SessionVariable.isFeDebug()) {
                Validator validator = new Validator();
                validator.processRoot(result, ctx);
            }
            return result;
        } catch (Exception e) {
            LOG.warn("lazy materialize topn failed", e);
            return topN;
        }
    }

    private Plan computeTopN(PhysicalTopN topN, CascadesContext ctx) {
        if (hasMaterialized) {
            return topN;
        }
        if (SessionVariable.getTopNLazyMaterializationThreshold() < topN.getLimit()) {
            return topN;
        }
        try {
            List<Slot> userVisibleOutput = ImmutableList.copyOf(topN.getOutput());

            // Find projects below TopN with PreferPushDownProject expressions.
            // Simplify them and expose hidden columns as lazy candidates.
            List<NamedExpression> pulledUpExprs = new ArrayList<>();
            List<Slot> lazyCandidates = new ArrayList<>();
            PhysicalProject<? extends Plan> leafProject = findLeafProject(topN);
            boolean restructured = false;

            if (leafProject != null && hasPreferPushDownProjectExprs(leafProject)) {
                // Split: remove PPD/subPath expressions from lower project
                List<NamedExpression> simplified = new ArrayList<>();
                for (NamedExpression ne : leafProject.getProjects()) {
                    if (isNestedLazyExpression(ne)) {
                        pulledUpExprs.add(ne);
                        // For PPD expressions, input slots ARE the lazy candidates
                        // (e.g., struct_col for struct_element, payload['name'] for variant)
                        lazyCandidates.addAll(ne.getInputSlots());
                    } else {
                        simplified.add(ne);
                    }
                }
                // Add lazy candidate slots to lower project so LazySlotPruning can remove them
                for (Slot slot : lazyCandidates) {
                    if (!containsSlotInExprs(simplified, slot)) {
                        simplified.add(slot);
                    }
                }
                topN = replaceLeafProject(topN, leafProject, simplified);
                restructured = true;
            }

            // Build effective probe list: TopN output (excl. pulled-up slots) + lazy candidates
            Set<ExprId> pulledUpExprIds = new HashSet<>();
            for (NamedExpression ne : pulledUpExprs) {
                pulledUpExprIds.add(ne.getExprId());
            }
            List<Slot> effectiveOutput = new ArrayList<>();
            for (Slot slot : topN.getOutput()) {
                if (!pulledUpExprIds.contains(slot.getExprId())) {
                    effectiveOutput.add(slot);
                }
            }
            if (restructured) {
                for (Slot candidate : lazyCandidates) {
                    if (!containsSlot(effectiveOutput, candidate)) {
                        effectiveOutput.add(candidate);
                    }
                }
            }

            Plan result = doComputeTopN(topN, ctx, ImmutableList.copyOf(effectiveOutput),
                    restructured ? leafProject : null, lazyCandidates);

            if (pulledUpExprs.isEmpty()) {
                result = new PhysicalProject(ImmutableList.copyOf(userVisibleOutput), null, result);
            } else {
                List<NamedExpression> outputExprs = new ArrayList<>();
                for (Slot slot : userVisibleOutput) {
                    if (!pulledUpExprIds.contains(slot.getExprId())) {
                        outputExprs.add(slot);
                    }
                }
                outputExprs.addAll(pulledUpExprs);
                result = new PhysicalProject(ImmutableList.copyOf(outputExprs), null, result);
            }
            return result;
        } catch (RuntimeException e) {
            LOG.warn("lazy materialize topn failed for plan: {}", topN.shapeInfo(), e);
            return topN;
        }
    }

    private static boolean containsSlot(List<Slot> slots, Slot slot) {
        for (Slot s : slots) {
            if (s.getExprId().equals(slot.getExprId())) {
                return true;
            }
        }
        return false;
    }

    private static boolean containsSlotInExprs(List<NamedExpression> exprs, Slot slot) {
        for (NamedExpression ne : exprs) {
            if (ne.getExprId().equals(slot.getExprId())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Create a LazySlotPruning instance that bypasses the containsAll guard.
     *
     * shouldPruneChild normally checks child.getOutput().containsAll(lazySlots)
     * to decide whether to descend. After replaceLeafProject restructures the plan
     * chain, intermediate nodes retain stale logical properties that don't include
     * the restructured columns, so containsAll returns false and the subtree is
     * skipped. We override only this guard method—all other traversal logic,
     * slot removal, and rowId injection is inherited from LazySlotPruning unchanged.
     */
    private LazySlotPruning createLazySlotPruning() {
        return new LazySlotPruning() {
            @Override
            protected boolean shouldPruneChild(Plan child, Context context) {
                return true;
            }
        };
    }

    private static boolean isNestedLazyExpression(NamedExpression ne) {
        if (!(ne instanceof Alias)) {
            return false;
        }
        if (ne.containsType(PreferPushDownProject.class)) {
            return true;
        }
        for (Slot slot : ne.getInputSlots()) {
            if (slot instanceof SlotReference && ((SlotReference) slot).hasSubColPath()) {
                return true;
            }
        }
        return false;
    }

    private static boolean hasPreferPushDownProjectExprs(PhysicalProject<? extends Plan> project) {
        for (NamedExpression ne : project.getProjects()) {
            if (isNestedLazyExpression(ne)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Walk: MERGE_SORT → Distribute → LOCAL_SORT → [Project], return the project if found.
     * This method assumes the standard OLAP single-table TopN physical plan shape.
     * If the plan shape doesn't match (e.g., no distribute, no LOCAL_SORT, or
     * extra intermediate nodes), it returns null and lazy mat is skipped gracefully.
     */
    private PhysicalProject<? extends Plan> findLeafProject(PhysicalTopN topN) {
        if (topN.children().isEmpty()) {
            return null;
        }
        Plan distribute = (Plan) topN.child(0);
        if (!(distribute instanceof PhysicalDistribute) || distribute.children().isEmpty()) {
            return null;
        }
        Plan localTopN = (Plan) distribute.child(0);
        if (!(localTopN instanceof PhysicalTopN) || localTopN.children().isEmpty()) {
            return null;
        }
        Plan belowLocal = (Plan) localTopN.child(0);
        if (!(belowLocal instanceof PhysicalProject) || belowLocal.children().isEmpty()) {
            return null;
        }
        return (PhysicalProject<? extends Plan>) belowLocal;
    }

    /** Replace the leaf project with simplified expressions, rebuilding the chain */
    private PhysicalTopN replaceLeafProject(PhysicalTopN topN,
            PhysicalProject<? extends Plan> leafProject, List<NamedExpression> simplified) {
        // If simplified projects are the same, reuse the original project.
        // Otherwise create a new one with null LogicalProperties because the old properties
        // are stale after plan restructuring (output slots have changed).
        Plan newLeaf = simplified.size() == leafProject.getProjects().size()
                && simplified.equals(leafProject.getProjects())
                ? leafProject
                : new PhysicalProject<>(ImmutableList.copyOf(simplified),
                        (LogicalProperties) null, leafProject.child(0));

        // Rebuild chain from bottom up
        Plan distribute = (Plan) topN.child(0);
        Plan localTopN = (Plan) distribute.child(0);
        Plan newLocalTopN = (Plan) localTopN.withChildren(ImmutableList.of(newLeaf));
        Plan newDistribute = (Plan) distribute.withChildren(ImmutableList.of(newLocalTopN));
        return (PhysicalTopN) topN.withChildren(ImmutableList.of(newDistribute));
    }

    private Plan doComputeTopN(PhysicalTopN topN, CascadesContext ctx, List<Slot> effectiveOutput,
            PhysicalProject<? extends Plan> leafProject, List<Slot> lazyCandidates) {
        Map<Slot, MaterializeSource> materializeMap = new HashMap<>();
        List<Slot> materializedSlots = new ArrayList<>();

        for (Slot slot : effectiveOutput) {
            Optional<MaterializeSource> source = computeMaterializeSource(topN, (SlotReference) slot,
                    leafProject, lazyCandidates);
            if (source.isPresent()) {
                SlotReference baseSlot = source.get().baseSlot;
                if (source.get().baseSlot.hasSubColPath()
                        || source.get().baseSlot.getAllAccessPaths().isPresent()) {
                    slot = baseSlot.withExprId(slot.getExprId());
                }
                materializeMap.put(slot, source.get());
            } else {
                materializedSlots.add(slot);
            }
        }

        List<Slot> lazyMaterializeSlots = filterSlotsForLazyMaterialization(materializeMap);
        if (lazyMaterializeSlots.isEmpty()) {
            return topN;
        }

        Map<Relation, List<Slot>> relationToLazySlotMap = new HashMap<>();
        for (Slot slot : lazyMaterializeSlots) {
            MaterializeSource source = materializeMap.get(slot);
            relationToLazySlotMap.computeIfAbsent(source.relation, relation -> new ArrayList<>()).add(slot);
        }

        Plan result = topN;
        BiMap<Relation, SlotReference> relationToRowId = HashBiMap.create(relationToLazySlotMap.size());
        HashSet<SlotReference> rowIdSet = new HashSet<>();
        StatementContext threadStatementContext = StatementScopeIdGenerator.getStatementContext();
        for (Relation relation : relationToLazySlotMap.keySet()) {
            if (relation instanceof CatalogRelation) {
                CatalogRelation catalogRelation = (CatalogRelation) relation;
                Column rowIdCol = new Column(Column.GLOBAL_ROWID_COL + catalogRelation.getTable().getName(),
                        Type.STRING, false, AggregateType.REPLACE, false,
                        catalogRelation.getTable().getName() + ".global_row_id", false, Integer.MAX_VALUE);
                SlotReference rowIdSlot = SlotReference.fromColumn(threadStatementContext.getNextExprId(),
                        catalogRelation.getTable(), rowIdCol, catalogRelation.getQualifier());
                result = result.accept(createLazySlotPruning(), new LazySlotPruning.Context(
                        (PhysicalCatalogRelation) relation,
                        rowIdSlot, relationToLazySlotMap.get(relation)));
                relationToRowId.put(catalogRelation, rowIdSlot);
                rowIdSet.add(rowIdSlot);
            } else if (relation instanceof PhysicalTVFRelation) {
                PhysicalTVFRelation tvfRelation = (PhysicalTVFRelation) relation;
                Column rowIdCol = new Column(Column.GLOBAL_ROWID_COL + tvfRelation.getFunction().getName(),
                        Type.STRING, false, AggregateType.REPLACE, false,
                        tvfRelation.getFunction().getName() + ".global_row_id", false, Integer.MAX_VALUE);
                SlotReference rowIdSlot = SlotReference.fromColumn(threadStatementContext.getNextExprId(),
                        tvfRelation.getFunction().getTable(), rowIdCol, ImmutableList.of());
                result = result.accept(createLazySlotPruning(), new LazySlotPruning.Context(
                        (PhysicalTVFRelation) relation,
                        rowIdSlot, relationToLazySlotMap.get(relation)));
                relationToRowId.put(tvfRelation, rowIdSlot);
                rowIdSet.add(rowIdSlot);
            } else {
                throw new RuntimeException("LazyMaterializeTopN not support this relation." + relation);
            }
        }

        List<SlotReference> materializeInput = moveRowIdsToTail(result.getOutput(), rowIdSet);

        if (materializeInput == null) {
            // Build correct materializeInput from materializedSlots + rowIds
            // (result.getOutput() may have stale logical properties)
            List<Slot> correctInput = new ArrayList<>(materializedSlots);
            for (SlotReference rowId : relationToRowId.values()) {
                correctInput.add(rowId);
            }
            result = new PhysicalLazyMaterialize(result, correctInput,
                    materializedSlots, relationToLazySlotMap, relationToRowId, materializeMap,
                    null, ((AbstractPlan) result).getStats());
            hasMaterialized = true;
        } else {
            List<Slot> reOrderedMaterializedSlots = new ArrayList<>();
            for (Slot slot : materializeInput) {
                if (rowIdSet.contains(slot)) {
                    break;
                }
                reOrderedMaterializedSlots.add(slot);
            }
            result = new PhysicalProject(materializeInput, null, result);
            result = new PhysicalLazyMaterialize(result, materializeInput,
                    reOrderedMaterializedSlots, relationToLazySlotMap, relationToRowId, materializeMap,
                    null, ((AbstractPlan) result).getStats());
            hasMaterialized = true;
        }
        return result;
    }

    private List<SlotReference> moveRowIdsToTail(List<Slot> slots, Set<SlotReference> rowIds) {
        List<SlotReference> reArrangedSlots = new ArrayList<>();
        List<SlotReference> reArrangedRowIds = new ArrayList<>();
        boolean moved = false;
        boolean meetRowId = false;
        for (Slot slot : slots) {
            if (rowIds.contains(slot)) {
                if (!reArrangedRowIds.contains(slot)) {
                    reArrangedRowIds.add((SlotReference) slot);
                }
                meetRowId = true;
            } else {
                if (meetRowId) {
                    moved = true;
                }
                reArrangedSlots.add((SlotReference) slot);
            }
        }
        if (!moved) {
            return null;
        }
        reArrangedSlots.addAll(reArrangedRowIds);
        return reArrangedSlots;
    }

    private List<Slot> filterSlotsForLazyMaterialization(Map<Slot, MaterializeSource> materializeMap) {
        return new ArrayList<>(materializeMap.keySet());
    }

    private Optional<MaterializeSource> computeMaterializeSource(PhysicalTopN topN, SlotReference slot,
            PhysicalProject<? extends Plan> leafProject, List<Slot> lazyCandidates) {
        MaterializeProbeVisitor probe = new MaterializeProbeVisitor();
        MaterializeProbeVisitor.ProbeContext context = new MaterializeProbeVisitor.ProbeContext(slot);
        Optional<MaterializeSource> source = probe.visit(topN, context);
        if (source.isPresent()) {
            return source;
        }
        // For hidden lazy candidates not reachable via TopN output, probe from below the project
        if (leafProject != null && lazyCandidates.contains(slot)) {
            return leafProject.child().accept(probe, context);
        }
        return Optional.empty();
    }

}
