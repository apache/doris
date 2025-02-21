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

import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.processor.post.materialize.MaterializeSource;
import org.apache.doris.nereids.properties.DataTrait.Builder;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.statistics.Statistics;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
    lazy materialize node
 */
public class PhysicalLazyMaterialize<CHILD_TYPE extends Plan> extends PhysicalUnary<CHILD_TYPE> {

    private final List<Slot> originOutput;

    private final List<Slot> materializedSlots;

    private final Map<Slot, MaterializeSource> materializeMap;

    private final List<Slot> lazyMaterializeSlots;

    /**
     * constructor
     */
    public PhysicalLazyMaterialize(CHILD_TYPE child, List<Slot> originOutput,
            Map<Slot, MaterializeSource> materializeMap, List<Slot> lazyMaterializeSlots) {
        super(PlanType.PHYSICAL_MATERIALIZE, Optional.empty(),
                null, child);
        this.originOutput = originOutput;
        this.materializeMap = materializeMap;
        this.lazyMaterializeSlots = lazyMaterializeSlots;
        materializedSlots = originOutput.stream()
                .filter(slot -> !lazyMaterializeSlots.contains(slot))
                .collect(ImmutableList.toImmutableList());
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalLazyMaterialize(this, context);
    }

    @Override
    public List<? extends Expression> getExpressions() {
        return materializedSlots;
    }

    @Override
    public List<Slot> computeOutput() {
        return originOutput;
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return null;
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return null;
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

    @Override
    public Plan withChildren(List<Plan> children) {
        return null;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("PhysicalLazyMaterialize [Output= (")
                .append(originOutput)
                .append("), lazySlots= (")
                .append(lazyMaterializeSlots)
                .append(")]");
        return builder.toString();
    }

    @Override
    public PhysicalPlan withPhysicalPropertiesAndStats(PhysicalProperties physicalProperties, Statistics statistics) {
        return null;
    }

    @Override
    public String shapeInfo() {
        StringBuilder shapeBuilder = new StringBuilder();
        shapeBuilder.append(this.getClass().getSimpleName())
                .append("[").append("materializedSlots:")
                .append(ExpressionUtils.slotListShapeInfo(materializedSlots))
                .append("lazySlots:").append(ExpressionUtils.slotListShapeInfo(lazyMaterializeSlots))
                .append("]");
        return shapeBuilder.toString();
    }
}
