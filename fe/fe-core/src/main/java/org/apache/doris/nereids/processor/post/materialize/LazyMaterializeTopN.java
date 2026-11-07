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
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalGenerate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLazyMaterialize;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLimit;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSetOperation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTVFRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTopN;
import org.apache.doris.nereids.util.PlanUtils;
import org.apache.doris.qe.SessionVariable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
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
 * Post rule to insert MaterializeNode for TopN and join-LIMIT lazy materialization.
 * Expression pull-up is handled by PullUpProjectExprUnderTopN in the logical phase.
 */
public class LazyMaterializeTopN extends PlanPostProcessor {
    private static final Logger LOG = LogManager.getLogger(LazyMaterializeTopN.class);
    private boolean hasMaterialized = false;

    @Override
    public Plan visitPhysicalTopN(PhysicalTopN<? extends Plan> topN, CascadesContext ctx) {
        try {
            Plan result = computeBoundary(topN, topN.getLimit(),
                    SessionVariable.getTopNLazyMaterializationThreshold(), ctx, false);
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

    @Override
    public Plan visitPhysicalLimit(PhysicalLimit<? extends Plan> limit, CascadesContext ctx) {
        PhysicalLimit<? extends Plan> rewrittenLimit = (PhysicalLimit<? extends Plan>) visit(limit, ctx);
        // A local LIMIT may keep limit rows per fragment, not globally. Fetch only after the global LIMIT.
        if (!rewrittenLimit.isGlobal() || rewrittenLimit.getOffset() != 0 || rewrittenLimit.getLimit() == 0
                || ctx.getConnectContext().getSessionVariable().limitJoinLazyMaterializationThreshold <= 0) {
            return rewrittenLimit;
        }
        try {
            int threshold = ctx.getConnectContext().getSessionVariable().limitJoinLazyMaterializationThreshold;
            Plan result = computeBoundary(rewrittenLimit, rewrittenLimit.getLimit(), threshold, ctx, true);
            if (SessionVariable.isFeDebug()) {
                new Validator().processRoot(result, ctx);
            }
            return result;
        } catch (Exception e) {
            LOG.warn("lazy materialize join limit failed", e);
            return rewrittenLimit;
        }
    }

    private Plan computeBoundary(Plan boundary, long limit, int threshold, CascadesContext ctx,
            boolean joinLimit) {
        if (hasMaterialized) {
            return boundary;
        }
        if (threshold < limit || (joinLimit && !hasOnlyInnerJoins(boundary.child(0)))) {
            return boundary;
        }
        try {
            List<Slot> userVisibleOutput = ImmutableList.copyOf(boundary.getOutput());
            Plan result = doCompute(boundary, ctx, userVisibleOutput, joinLimit);
            if (result == boundary) {
                return boundary;
            }
            result = new PhysicalProject(ImmutableList.copyOf(userVisibleOutput), null, result);
            return result;
        } catch (RuntimeException e) {
            LOG.warn("lazy materialize failed for plan: {}", boundary.shapeInfo(), e);
            return boundary;
        }
    }

    private boolean hasOnlyInnerJoins(Plan plan) {
        // Leave ORDER BY to the existing TopN rewrite; this path handles a plain LIMIT only.
        if (!plan.collectToList(node -> node instanceof PhysicalTopN).isEmpty()) {
            return false;
        }
        List<AbstractPhysicalJoin> joins = plan.collectToList(node -> node instanceof AbstractPhysicalJoin);
        return !joins.isEmpty() && joins.stream().allMatch(join -> join.getJoinType().isInnerJoin());
    }

    private Plan doCompute(Plan boundary, CascadesContext ctx, List<Slot> effectiveOutput, boolean joinLimit) {
        Map<Slot, MaterializeSource> materializeMap = new HashMap<>();
        List<Slot> materializedSlots = new ArrayList<>();
        Set<Slot> requiredMaterializedSlots = new HashSet<>();
        collectProjectExprInputSlots(boundary.child(0), requiredMaterializedSlots);
        collectRequiredAliasSources(boundary, requiredMaterializedSlots);

        /*
         * requiredMaterializedSlots only records slots consumed by Project/final-projection expressions inside the
         * boundary subtree. Other mandatory slots, such as TopN order keys, join keys, or Filter predicates, are
         * rejected by MaterializeProbeVisitor while tracing each output slot down to the source relation:
         *
         *   Project(b) -> TopN(order by id) -> Filter(a > 0) -> Scan(id, a, b, c)
         *
         * For id, the probe stops at TopN because id is in TopN.getInputSlots(); for a, it stops at Filter because
         * a is in Filter.getInputSlots(). Both return Optional.empty() and are appended to materializedSlots below.
         * Therefore an empty requiredMaterializedSlots set does not mean every scan column can be delayed; it only
         * means no extra Project/final-projection input must be forced materialized by this local safety check.
         *
         * The probe only protects the slots the operators reference directly, so the columns those slots read
         * through identity aliases are protected by collectRequiredAliasSources above.
         */
        for (Slot slot : effectiveOutput) {
            Optional<MaterializeSource> source = computeMaterializeSource(boundary, (SlotReference) slot,
                    requiredMaterializedSlots);
            if (source.isPresent() && (!joinLimit || source.get().relation instanceof PhysicalOlapScan)) {
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
        // A lazy alias can share its base slot with another output that must remain materialized.
        // Keep the alias materialized too, otherwise LazySlotPruning removes the base slot needed by that output.
        List<Slot> requiredOutputSlots = collectRequiredOutputSlots(
                materializeMap, requiredMaterializedSlots, new HashSet<>(materializedSlots));
        for (Slot slot : requiredOutputSlots) {
            if (materializeMap.remove(slot) != null) {
                materializedSlots.add(slot);
            }
        }

        List<Slot> lazyMaterializeSlots = filterSlotsForLazyMaterialization(materializeMap);
        if (lazyMaterializeSlots.isEmpty()) {
            return boundary;
        }

        Map<Relation, List<Slot>> relationToLazySlotMap = new HashMap<>();
        for (Slot slot : lazyMaterializeSlots) {
            MaterializeSource source = materializeMap.get(slot);
            relationToLazySlotMap.computeIfAbsent(source.relation, relation -> new ArrayList<>()).add(slot);
        }

        Plan result = boundary;
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
                return boundary;
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

    @VisibleForTesting
    static List<Slot> collectRequiredOutputSlots(Map<Slot, MaterializeSource> materializeMap,
            Set<Slot> requiredMaterializedSlots, Set<Slot> materializedSlots) {
        List<Slot> requiredOutputSlots = new ArrayList<>();
        for (Map.Entry<Slot, MaterializeSource> entry : materializeMap.entrySet()) {
            if (requiredMaterializedSlots.contains(entry.getKey())
                    || requiredMaterializedSlots.contains(entry.getValue().baseSlot)
                    || materializedSlots.contains(entry.getValue().baseSlot)) {
                requiredOutputSlots.add(entry.getKey());
            }
        }
        return requiredOutputSlots;
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

    /**
     * Keep the columns consumed below the boundary materialized during the scan, resolved through identity
     * aliases.
     *
     * <p>{@link MaterializeProbeVisitor} only protects the slot it is tracing: a slot consumed by an
     * operator on the way from the boundary down to the relation stops the probe, but the columns an
     * identity alias reads are never resolved. For
     *
     * <pre>
     *   OuterTopN(order by z)
     *     InnerTopN(order by x)
     *       Project(lazy_col AS x, lazy_col AS y, other_col AS z)
     *         OlapScan
     * </pre>
     *
     * probing the outer output {@code y} resolves to the base column {@code lazy_col}, so {@code lazy_col}
     * is classified lazy and {@link LazySlotPruning} removes it from the scan, while {@code lazy_col AS x}
     * below the outer TopN is still read by the inner TopN. The plan then references a slot its child no
     * longer produces and the final {@link Validator} rejects it. The same happens when an identity alias
     * is consumed by a filter, a join condition or any other operator that stays below the boundary.
     *
     * <p>Therefore every slot consumed by this boundary and by the operators below it (TopN order keys, the
     * expressions of every descendant operator and the slots that are required materialized already) is
     * resolved through its identity alias chain. The only consumed slots that are not required are the ones
     * {@link MaterializeProbeVisitor#isIndexLazyFilter} keeps lazy: {@link LazySlotPruning#visitPhysicalFilter}
     * drops those from the scan's lazy slots for the same reason, so the scan keeps producing them for the
     * predicate and nothing below the TopN can be starved. Project expressions are handled by
     * {@link #collectProjectExprInputSlots}, which knows that a transparent {@code Alias(Slot)} output may
     * still be fetched lazily.
     *
     * <p>A set operation is a boundary: {@link MaterializeProbeVisitor} never reports a lazy source for a
     * slot produced by a set operation, and {@link #collectIdentityAliasMap} stops at it, so the aliases
     * below a set operation are neither resolved nor reachable. If lazy materialization is ever extended
     * through set operations, the consumed slots have to be resolved per set operation branch instead.
     */
    private void collectRequiredAliasSources(Plan boundary,
            Set<Slot> requiredMaterializedSlots) {
        Map<Slot, Slot> aliasToChild = new HashMap<>();
        collectIdentityAliasMap(boundary.child(0), aliasToChild);

        Set<Slot> consumedSlots = new HashSet<>();
        Set<Slot> indexLazySlots = new HashSet<>();
        collectConsumedSlots(boundary, consumedSlots, indexLazySlots);
        consumedSlots.addAll(requiredMaterializedSlots);
        for (Slot slot : consumedSlots) {
            if (!indexLazySlots.contains(slot)) {
                collectAliasChain(slot, aliasToChild, requiredMaterializedSlots);
            }
        }
    }

    /** Collect the slots consumed by {@code plan} itself and by every operator below it. */
    private void collectConsumedSlots(Plan plan, Set<Slot> consumedSlots, Set<Slot> indexLazySlots) {
        if (plan instanceof PhysicalSetOperation) {
            // Set operations are not materialized lazily, so nothing below them can be lazy either.
            return;
        }
        if (!(plan instanceof PhysicalProject)) {
            // Project expressions are covered by collectProjectExprInputSlots, which keeps the input of a
            // transparent Alias(Slot) lazy because that alias output may still be fetched later.
            consumedSlots.addAll(plan.getInputSlots());
        }
        if (plan instanceof PhysicalFilter
                && MaterializeProbeVisitor.isIndexLazyFilter((PhysicalFilter<?>) plan)) {
            indexLazySlots.addAll(plan.getInputSlots());
        }
        if (plan instanceof PhysicalGenerate) {
            // PhysicalGenerate.getExpressions() exposes the generators only, while the lateral conjuncts
            // are preserved by the implementation rule and executed by the generate itself. Generator
            // outputs are produced by the generate, never by the child, exactly like LogicalGenerate.
            PhysicalGenerate<?> generate = (PhysicalGenerate<?>) plan;
            consumedSlots.addAll(Sets.difference(
                    PlanUtils.fastGetInputSlots(generate.getConjuncts()),
                    new HashSet<>(generate.getGeneratorOutput())));
        }
        for (Plan child : plan.children()) {
            collectConsumedSlots(child, consumedSlots, indexLazySlots);
        }
    }

    /** Collect {@code alias slot -> child slot} for every identity Alias below the boundary. */
    private void collectIdentityAliasMap(Plan plan, Map<Slot, Slot> aliasToChild) {
        if (plan instanceof PhysicalSetOperation) {
            // Set operations are not materialized lazily, so aliases below them are never reached.
            return;
        }
        if (plan instanceof PhysicalProject) {
            for (NamedExpression project : ((PhysicalProject<?>) plan).getProjects()) {
                if (project instanceof Alias && project.child(0) instanceof Slot) {
                    aliasToChild.putIfAbsent(project.toSlot(), (Slot) project.child(0));
                }
            }
        }
        for (Plan child : plan.children()) {
            collectIdentityAliasMap(child, aliasToChild);
        }
    }

    /** Add a slot together with every slot of its alias chain, so the column the chain ends at is protected. */
    @VisibleForTesting
    static void collectAliasChain(Slot slot, Map<Slot, Slot> aliasToChild, Set<Slot> requiredMaterializedSlots) {
        Set<Slot> visited = new HashSet<>();
        Slot current = slot;
        while (current != null && visited.add(current)) {
            requiredMaterializedSlots.add(current);
            current = aliasToChild.get(current);
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

    private Optional<MaterializeSource> computeMaterializeSource(Plan boundary, SlotReference slot,
            Set<Slot> requiredMaterializedSlots) {
        MaterializeProbeVisitor probe = new MaterializeProbeVisitor();
        MaterializeProbeVisitor.ProbeContext context = new MaterializeProbeVisitor.ProbeContext(slot,
                requiredMaterializedSlots);
        return probe.visit(boundary, context);
    }
}
