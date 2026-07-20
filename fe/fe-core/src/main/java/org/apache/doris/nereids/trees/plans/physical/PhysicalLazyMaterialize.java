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

package org.apache.doris.nereids.trees.plans.physical;

import org.apache.doris.catalog.Column;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.processor.post.materialize.DeferredColumnSpec;
import org.apache.doris.nereids.processor.post.materialize.LazySourceSpec;
import org.apache.doris.nereids.processor.post.materialize.TopNLazyMaterializationSpec;
import org.apache.doris.nereids.properties.DataTrait.Builder;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.AbstractPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.algebra.Relation;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.statistics.Statistics;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/** Physical node that executes an immutable TopN lazy-materialization specification. */
public class PhysicalLazyMaterialize<CHILD_TYPE extends Plan> extends PhysicalUnary<CHILD_TYPE> {
    private final TopNLazyMaterializationSpec spec;

    public PhysicalLazyMaterialize(CHILD_TYPE child, TopNLazyMaterializationSpec spec) {
        this(child, spec, Optional.empty(), null, null, null);
    }

    public PhysicalLazyMaterialize(CHILD_TYPE child, TopNLazyMaterializationSpec spec,
            PhysicalProperties physicalProperties, Statistics statistics) {
        this(child, spec, Optional.empty(), null, physicalProperties, statistics);
    }

    private PhysicalLazyMaterialize(CHILD_TYPE child, TopNLazyMaterializationSpec spec,
            Optional<GroupExpression> groupExpression, LogicalProperties logicalProperties,
            PhysicalProperties physicalProperties, Statistics statistics) {
        super(PlanType.PHYSICAL_MATERIALIZE, groupExpression,
                logicalProperties, physicalProperties, statistics, child);
        this.spec = Preconditions.checkNotNull(spec, "spec must not be null");
    }

    public TopNLazyMaterializationSpec getSpec() {
        return spec;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalLazyMaterialize(this, context);
    }

    @Override
    public List<? extends Expression> getExpressions() {
        return spec.getMaterializedSlots();
    }

    @Override
    public List<Slot> computeOutput() {
        return spec.getMaterializeOutput();
    }

    @Override
    public PhysicalLazyMaterialize<CHILD_TYPE> withGroupExpression(Optional<GroupExpression> groupExpression) {
        return AbstractPlan.copyWithSameId(this, () -> new PhysicalLazyMaterialize<>(child(), spec,
                groupExpression, getLogicalProperties(), physicalProperties, statistics));
    }

    @Override
    public PhysicalLazyMaterialize<Plan> withGroupExprLogicalPropChildren(
            Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1, "PhysicalLazyMaterialize must have one child");
        return AbstractPlan.copyWithSameId(this, () -> new PhysicalLazyMaterialize<>(children.get(0), spec,
                groupExpression, logicalProperties.orElse(null), physicalProperties, statistics));
    }

    @Override
    public PhysicalLazyMaterialize<Plan> withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1, "PhysicalLazyMaterialize must have one child");
        return AbstractPlan.copyWithSameId(this, () -> new PhysicalLazyMaterialize<>(children.get(0), spec,
                groupExpression, getLogicalProperties(), physicalProperties, statistics));
    }

    @Override
    public PhysicalLazyMaterialize<CHILD_TYPE> withPhysicalPropertiesAndStats(
            PhysicalProperties physicalProperties, Statistics statistics) {
        return AbstractPlan.copyWithSameId(this, () -> new PhysicalLazyMaterialize<>(child(), spec,
                groupExpression, getLogicalProperties(), physicalProperties, statistics));
    }

    @Override
    public String toString() {
        return "PhysicalLazyMaterialize [Output= (" + getOutput() + "), lazySlots= ("
                + getAllLazySlots() + ")]";
    }

    @Override
    public String shapeInfo() {
        List<Slot> lazySlots = getAllLazySlots().stream()
                .sorted(Comparator.comparing(Slot::shapeInfo))
                .collect(Collectors.toList());
        return getClass().getSimpleName() + "[materializedSlots:"
                + ExpressionUtils.slotListShapeInfo(spec.getMaterializedSlots())
                + " lazySlots:" + ExpressionUtils.slotListShapeInfo(lazySlots) + "]";
    }

    public List<Relation> getRelations() {
        ImmutableList.Builder<Relation> relations = ImmutableList.builder();
        spec.getSources().forEach(source -> relations.add(source.getRelation()));
        return relations.build();
    }

    /** Existing FE-BE protocol view, deterministically derived from the immutable source specs. */
    public List<List<Column>> getLazyColumns() {
        ImmutableList.Builder<List<Column>> result = ImmutableList.builder();
        for (LazySourceSpec source : spec.getSources()) {
            ImmutableList.Builder<Column> columns = ImmutableList.builder();
            for (DeferredColumnSpec deferred : source.getDeferredColumns()) {
                for (int i = 0; i < deferred.getOutputSlots().size(); i++) {
                    columns.add(deferred.getOriginalColumn());
                }
            }
            result.add(columns.build());
        }
        return result.build();
    }

    /** Existing FE-BE protocol view, deterministically derived from the immutable source specs. */
    public List<List<Integer>> getLazySlotLocations() {
        ImmutableList.Builder<List<Integer>> result = ImmutableList.builder();
        int location = spec.getMaterializedSlots().size();
        for (LazySourceSpec source : spec.getSources()) {
            ImmutableList.Builder<Integer> locations = ImmutableList.builder();
            for (DeferredColumnSpec deferred : source.getDeferredColumns()) {
                for (int i = 0; i < deferred.getOutputSlots().size(); i++) {
                    locations.add(location++);
                }
            }
            result.add(locations.build());
        }
        return result.build();
    }

    /** Existing FE-BE protocol view, deterministically derived from the immutable source specs. */
    public List<List<Integer>> getLazyBaseColumnIndices() {
        ImmutableList.Builder<List<Integer>> result = ImmutableList.builder();
        for (LazySourceSpec source : spec.getSources()) {
            ImmutableList.Builder<Integer> indices = ImmutableList.builder();
            for (DeferredColumnSpec deferred : source.getDeferredColumns()) {
                for (int i = 0; i < deferred.getOutputSlots().size(); i++) {
                    indices.add(deferred.getBaseColumnIndex());
                }
            }
            result.add(indices.build());
        }
        return result.build();
    }

    public List<Slot> getRowIds() {
        ImmutableList.Builder<Slot> rowIds = ImmutableList.builder();
        spec.getSources().forEach(source -> rowIds.add(source.getRowIdSlot()));
        return rowIds.build();
    }

    public List<Slot> getLazySlots(Relation relation) {
        LazySourceSpec source = spec.getSourceByRelationId().get(relation.getRelationId());
        return source == null ? ImmutableList.of() : source.getLazyOutputSlots();
    }

    private List<Slot> getAllLazySlots() {
        List<Slot> lazySlots = new ArrayList<>();
        for (LazySourceSpec source : spec.getSources()) {
            lazySlots.addAll(source.getLazyOutputSlots());
        }
        return lazySlots;
    }

    @Override
    public void computeUnique(Builder builder) {
    }

    @Override
    public void computeUniform(Builder builder) {
    }

    @Override
    public void computeEqualSet(Builder builder) {
    }

    @Override
    public void computeFd(Builder builder) {
    }
}
