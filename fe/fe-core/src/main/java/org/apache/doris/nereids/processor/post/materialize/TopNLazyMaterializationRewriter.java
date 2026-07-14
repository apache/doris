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

import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFileScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLazyMaterializeFileScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLazyMaterializeOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLazyMaterializeTVFScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTVFRelation;
import org.apache.doris.qe.SessionVariable;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** One-pass physical rewrite driven only by an immutable TopN lazy-materialization spec. */
public final class TopNLazyMaterializationRewriter {
    private final TopNLazyMaterializationSpec spec;
    private final Map<RelationId, LazySourceSpec> sourcesByRelationId;
    private final Map<RelationId, List<Slot>> baseSlotsByRelationId;

    /** Create a one-shot rewriter driven by the supplied specification. */
    public TopNLazyMaterializationRewriter(TopNLazyMaterializationSpec spec) {
        this.spec = Preconditions.checkNotNull(spec, "spec must not be null");
        this.sourcesByRelationId = ImmutableMap.copyOf(spec.getSourceByRelationId());
        Map<RelationId, List<Slot>> baseSlots = new HashMap<>();
        for (LazySourceSpec source : spec.getSources()) {
            ImmutableList.Builder<Slot> relationBaseSlots = ImmutableList.builder();
            for (DeferredColumnSpec deferred : source.getDeferredColumns()) {
                relationBaseSlots.add(deferred.getBaseSlot());
            }
            baseSlots.put(source.getRelation().getRelationId(), relationBaseSlots.build());
        }
        baseSlotsByRelationId = ImmutableMap.copyOf(baseSlots);
    }

    /** Rewrite the candidate TopN subtree exactly once. */
    public Plan rewrite(Plan plan) {
        if (plan instanceof PhysicalProject) {
            return rewriteProject((PhysicalProject<?>) plan);
        }
        if (plan instanceof PhysicalFilter) {
            return rewriteFilter((PhysicalFilter<?>) plan);
        }
        if (plan instanceof PhysicalRelation) {
            return rewriteRelation((PhysicalRelation) plan);
        }
        return rewriteChildren(plan);
    }

    private Plan rewriteProject(PhysicalProject<?> project) {
        Plan rewrittenChild = rewrite(project.child());
        List<NamedExpression> projections = new ArrayList<>();
        for (NamedExpression projection : project.getProjects()) {
            if (!hasDeferredInput(projection, rewrittenChild)) {
                projections.add(projection);
            }
        }
        Set<Slot> projectedSlots = new LinkedHashSet<>();
        for (NamedExpression projection : projections) {
            projectedSlots.add(projection.toSlot());
        }
        for (Slot outputSlot : rewrittenChild.getOutput()) {
            if (isAssignedRowId(outputSlot) && !projectedSlots.contains(outputSlot)) {
                projections.add((SlotReference) outputSlot);
            }
        }
        if (rewrittenChild == project.child() && projections.equals(project.getProjects())) {
            return project;
        }
        return project.withProjectionsAndChild(projections, rewrittenChild).resetLogicalProperties();
    }

    private Plan rewriteFilter(PhysicalFilter<?> filter) {
        if (!SessionVariable.getTopNLazyMaterializationUsingIndex()
                || !(filter.child() instanceof PhysicalOlapScan)) {
            return rewriteChildren(filter);
        }
        PhysicalOlapScan scan = (PhysicalOlapScan) filter.child();
        LazySourceSpec source = sourcesByRelationId.get(scan.getRelationId());
        if (source == null) {
            return rewriteChildren(filter);
        }

        List<Slot> lazyBaseSlots = baseSlotsByRelationId.get(scan.getRelationId());
        List<Slot> scanLazySlots = new ArrayList<>(lazyBaseSlots);
        scanLazySlots.removeAll(filter.getInputSlots());
        Plan lazyScan = new PhysicalLazyMaterializeOlapScan(
                scan, source.getScanRowIdSlot(), scanLazySlots);
        PhysicalFilter<?> rewrittenFilter = (PhysicalFilter<?>) replaceChildren(
                filter, ImmutableList.of(lazyScan));

        ImmutableList.Builder<NamedExpression> projections = ImmutableList.builder();
        for (Slot output : rewrittenFilter.getOutput()) {
            if (!lazyBaseSlots.contains(output)) {
                projections.add((SlotReference) output);
            }
        }
        List<NamedExpression> projectedOutputs = projections.build();
        return projectedOutputs.equals(rewrittenFilter.getOutput())
                ? rewrittenFilter : new PhysicalProject<>(projectedOutputs, null, rewrittenFilter);
    }

    private Plan rewriteChildren(Plan plan) {
        ImmutableList.Builder<Plan> children = ImmutableList.builderWithExpectedSize(plan.arity());
        boolean changed = false;
        for (Plan child : plan.children()) {
            Plan rewrittenChild = rewrite(child);
            children.add(rewrittenChild);
            changed |= rewrittenChild != child;
        }
        return changed ? replaceChildren(plan, children.build()) : plan;
    }

    private boolean hasDeferredInput(NamedExpression projection, Plan rewrittenChild) {
        return !rewrittenChild.getOutputSet().containsAll(projection.getInputSlots());
    }

    private Plan rewriteRelation(PhysicalRelation relation) {
        LazySourceSpec source = sourcesByRelationId.get(relation.getRelationId());
        if (source == null) {
            return relation;
        }
        List<Slot> lazyBaseSlots = baseSlotsByRelationId.get(relation.getRelationId());
        if (relation instanceof PhysicalOlapScan) {
            return new PhysicalLazyMaterializeOlapScan(
                    (PhysicalOlapScan) relation, source.getScanRowIdSlot(), lazyBaseSlots);
        }
        if (relation instanceof PhysicalFileScan) {
            return new PhysicalLazyMaterializeFileScan(
                    (PhysicalFileScan) relation, source.getScanRowIdSlot(), lazyBaseSlots);
        }
        Preconditions.checkArgument(relation instanceof PhysicalTVFRelation,
                "unsupported lazy source relation: %s", relation);
        return new PhysicalLazyMaterializeTVFScan(
                (PhysicalTVFRelation) relation, source.getScanRowIdSlot(), lazyBaseSlots);
    }

    private Plan replaceChildren(Plan plan, List<Plan> children) {
        AbstractPhysicalPlan original = (AbstractPhysicalPlan) plan;
        return ((AbstractPhysicalPlan) plan.withChildren(children))
                .copyStatsAndGroupIdFrom(original).resetLogicalProperties();
    }

    private boolean isAssignedRowId(Slot slot) {
        for (LazySourceSpec source : spec.getSources()) {
            if (source.getRowIdSlot().getExprId().equals(slot.getExprId())) {
                return true;
            }
        }
        return false;
    }
}
