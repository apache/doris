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
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.processor.post.PlanPostProcessor;
import org.apache.doris.nereids.processor.post.Validator;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.plans.AbstractPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.CatalogRelation;
import org.apache.doris.nereids.trees.plans.algebra.Relation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCatalogRelation;
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
 * Post rule to insert MaterializeNode for TopN lazy materialization.
 * Expression pull-up is handled by PullUpProjectExprUnderTopN in the logical phase.
 */
public class LazyMaterializeTopN extends PlanPostProcessor {
    private static final Logger LOG = LogManager.getLogger(LazyMaterializeTopN.class);
    private boolean hasMaterialized = false;

    @Override
    public Plan visitPhysicalTopN(PhysicalTopN<? extends Plan> topN, CascadesContext ctx) {
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

    private Plan computeTopN(PhysicalTopN<? extends Plan> topN, CascadesContext ctx) {
        if (hasMaterialized) {
            return topN;
        }
        if (SessionVariable.getTopNLazyMaterializationThreshold() < topN.getLimit()) {
            return topN;
        }
        try {
            List<Slot> userVisibleOutput = ImmutableList.copyOf(topN.getOutput());
            List<Slot> effectiveOutput = ImmutableList.copyOf(topN.getOutput());
            Plan result = doComputeTopN(topN, ctx, effectiveOutput);
            if (result == topN) {
                return topN;
            }
            result = new PhysicalProject(ImmutableList.copyOf(userVisibleOutput), null, result);
            return result;
        } catch (RuntimeException e) {
            LOG.warn("lazy materialize topn failed for plan: {}", topN.shapeInfo(), e);
            return topN;
        }
    }

    private Plan doComputeTopN(PhysicalTopN<? extends Plan> topN, CascadesContext ctx, List<Slot> effectiveOutput) {
        Map<Slot, MaterializeSource> materializeMap = new HashMap<>();
        List<Slot> materializedSlots = new ArrayList<>();
        Set<Slot> requiredMaterializedSlots = new HashSet<>();
        collectProjectExprInputSlots(topN.child(), requiredMaterializedSlots);

        /*
         * requiredMaterializedSlots only records slots consumed by Project/final-projection expressions inside the
         * TopN subtree. Other mandatory slots, such as TopN order keys or Filter predicates, are rejected by
         * MaterializeProbeVisitor while tracing each output slot from TopN down to the source relation:
         *
         *   Project(b) -> TopN(order by id) -> Filter(a > 0) -> Scan(id, a, b, c)
         *
         * For id, the probe stops at TopN because id is in TopN.getInputSlots(); for a, it stops at Filter because
         * a is in Filter.getInputSlots(). Both return Optional.empty() and are appended to materializedSlots below.
         * Therefore an empty requiredMaterializedSlots set does not mean every scan column can be delayed; it only
         * means no extra Project/final-projection input must be forced materialized by this local safety check.
         */
        for (Slot slot : effectiveOutput) {
            Optional<MaterializeSource> source = computeMaterializeSource(topN, (SlotReference) slot,
                    requiredMaterializedSlots);
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
        List<Slot> requiredOutputSlots = new ArrayList<>();
        for (Map.Entry<Slot, MaterializeSource> entry : materializeMap.entrySet()) {
            if (requiredMaterializedSlots.contains(entry.getKey())
                    || requiredMaterializedSlots.contains(entry.getValue().baseSlot)) {
                requiredOutputSlots.add(entry.getKey());
            }
        }
        for (Slot slot : requiredOutputSlots) {
            if (materializeMap.remove(slot) != null) {
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
            // TopN lazy materialization relies on BE adding a GLOBAL_ROWID_COL to the
            // tablet schema. When light_schema_change=false, the table columns have
            // col_unique_id=-1, which causes BE to skip the schema rebuild from
            // columns_desc, so the GLOBAL_ROWID_COL is never added and the scan fails.
            if (relation instanceof CatalogRelation
                    && ((CatalogRelation) relation).getTable() instanceof OlapTable
                    && !((OlapTable) ((CatalogRelation) relation).getTable()).getEnableLightSchemaChange()) {
                LOG.debug("Skip TopN lazy materialization for table {} with light_schema_change=false",
                        ((CatalogRelation) relation).getTable().getName());
                return topN;
            }
            if (relation instanceof CatalogRelation) {
                CatalogRelation catalogRelation = (CatalogRelation) relation;
                Column rowIdCol = new Column(Column.GLOBAL_ROWID_COL + catalogRelation.getTable().getName(),
                        Type.STRING, false, AggregateType.REPLACE, false,
                        catalogRelation.getTable().getName() + ".global_row_id", false, Integer.MAX_VALUE);
                SlotReference rowIdSlot = SlotReference.fromColumn(threadStatementContext.getNextExprId(),
                        catalogRelation.getTable(), rowIdCol, catalogRelation.getQualifier());
                result = result.accept(new LazySlotPruning(), new LazySlotPruning.Context(
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
                result = result.accept(new LazySlotPruning(), new LazySlotPruning.Context(
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
            // Row IDs are already at the tail in the correct order.
            // Keep materialized slots in the same order as the child tuple layout.
            List<Slot> reOrderedMaterializedSlots = new ArrayList<>();
            for (Slot slot : result.getOutput()) {
                if (rowIdSet.contains(slot)) {
                    break;
                }
                reOrderedMaterializedSlots.add(slot);
            }
            result = new PhysicalLazyMaterialize(result, result.getOutput(),
                    reOrderedMaterializedSlots, relationToLazySlotMap, relationToRowId, materializeMap,
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

    private void collectProjectExprInputSlots(Plan plan, Set<Slot> requiredMaterializedSlots) {
        if (plan instanceof PhysicalProject) {
            PhysicalProject<?> project = (PhysicalProject<?>) plan;
            for (NamedExpression projectExpr : project.getProjects()) {
                if (projectExpr instanceof SlotReference) {
                    continue;
                }
                if (projectExpr instanceof Alias && ((Alias) projectExpr).child() instanceof SlotReference) {
                    SlotReference childSlot = (SlotReference) ((Alias) projectExpr).child();
                    if (!childSlot.getOriginalColumn().isPresent()) {
                        requiredMaterializedSlots.addAll(project.getInputSlots());
                    }
                    continue;
                }
                requiredMaterializedSlots.addAll(projectExpr.getInputSlots());
            }
        } else if (plan instanceof PhysicalCatalogRelation) {
            PhysicalCatalogRelation relation = (PhysicalCatalogRelation) plan;
            if (relation.getTable() instanceof OlapTable) {
                OlapTable table = (OlapTable) relation.getTable();
                if (KeysType.UNIQUE_KEYS.equals(table.getKeysType())
                        && !table.getTableProperty().getEnableUniqueKeyMergeOnWrite()
                        || KeysType.AGG_KEYS.equals(table.getKeysType())
                        || KeysType.PRIMARY_KEYS.equals(table.getKeysType())) {
                    for (Slot slot : relation.getOutput()) {
                        SlotReference slotReference = (SlotReference) slot;
                        if (slotReference.getOriginalColumn().isPresent()
                                && slotReference.getOriginalColumn().get().isKey()) {
                            requiredMaterializedSlots.add(slotReference);
                        }
                    }
                }
            }
            for (Slot slot : plan.getOutput()) {
                if (slot instanceof SlotReference && !((SlotReference) slot).getOriginalColumn().isPresent()) {
                    requiredMaterializedSlots.addAll(plan.getOutputSet());
                    break;
                }
            }
        }
        for (Plan child : plan.children()) {
            collectProjectExprInputSlots(child, requiredMaterializedSlots);
        }
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

    private Optional<MaterializeSource> computeMaterializeSource(PhysicalTopN<? extends Plan> topN, SlotReference slot,
            Set<Slot> requiredMaterializedSlots) {
        MaterializeProbeVisitor probe = new MaterializeProbeVisitor();
        MaterializeProbeVisitor.ProbeContext context = new MaterializeProbeVisitor.ProbeContext(slot,
                requiredMaterializedSlots);
        return probe.visit(topN, context);
    }
}
