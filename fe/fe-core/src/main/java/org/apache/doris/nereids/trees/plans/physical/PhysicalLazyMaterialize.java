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
import org.apache.doris.catalog.TableIf;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.processor.post.materialize.MaterializeSource;
import org.apache.doris.nereids.properties.DataTrait.Builder;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.algebra.CatalogRelation;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.statistics.Statistics;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
    lazy materialize node
 */
public class PhysicalLazyMaterialize<CHILD_TYPE extends Plan> extends PhysicalUnary<CHILD_TYPE> {

    private final Map<CatalogRelation, List<Slot>> relationToLazySlotMap;

    private final BiMap<CatalogRelation, SlotReference> relationToRowId;

    private final Map<Slot, MaterializeSource> materializeMap;

    private final List<Slot> materializedSlots;

    private final List<Slot> materializeInput;
    private final List<Slot> materializeOutput;
    // used for BE
    private final List<Slot> rowIdList;
    private List<List<Column>> lazyColumns = new ArrayList<>();
    private List<List<Integer>> lazySlotLocations = new ArrayList<>();
    private List<List<Integer>> lazyTableIdxs = new ArrayList<>();

    private final List<CatalogRelation> relations;

    /**
     * constructor
     */
    public PhysicalLazyMaterialize(CHILD_TYPE child,
            List<Slot> materializeInput,
            List<Slot> materializedSlots,
            Map<CatalogRelation, List<Slot>> relationToLazySlotMap,
            BiMap<CatalogRelation, SlotReference> relationToRowId,
            Map<Slot, MaterializeSource> materializeMap) {
        this(child, materializeInput, materializedSlots, relationToLazySlotMap,
                relationToRowId, materializeMap, null, null);
    }

    /**
     * constructor
     */
    public PhysicalLazyMaterialize(CHILD_TYPE child,
            List<Slot> materializeInput,
            List<Slot> materializedSlots,
            Map<CatalogRelation, List<Slot>> relationToLazySlotMap,
            BiMap<CatalogRelation, SlotReference> relationToRowId,
            Map<Slot, MaterializeSource> materializeMap,
            PhysicalProperties physicalProperties, Statistics statistics) {
        super(PlanType.PHYSICAL_MATERIALIZE, Optional.empty(),
                null, physicalProperties, statistics, child);
        this.materializeInput = materializeInput;
        this.relationToLazySlotMap = relationToLazySlotMap;
        this.relationToRowId = relationToRowId;
        this.materializedSlots = ImmutableList.copyOf(materializedSlots);
        this.relations = ImmutableList.copyOf(relationToRowId.keySet());
        this.materializeMap = materializeMap;

        lazySlotLocations = new ArrayList<>();
        lazyTableIdxs = new ArrayList<>();
        lazyColumns = new ArrayList<>();

        ImmutableList.Builder<Slot> outputBuilder = ImmutableList.builder();
        outputBuilder.addAll(materializedSlots);
        int idx = materializedSlots.size();
        int loc = idx;
        ImmutableList.Builder<Slot> rowIdListBuilder = ImmutableList.builder();
        for (; idx < materializeInput.size(); idx++) {
            Slot rowId = materializeInput.get(idx);
            rowIdListBuilder.add(rowId);
            CatalogRelation rel = relationToRowId.inverse().get(rowId);
            TableIf relationTable = rel.getTable();

            List<Column> lazyColumnForRel = new ArrayList<>();
            lazyColumns.add(lazyColumnForRel);
            List<Integer> lazyIdxForRel = new ArrayList<>();
            lazyTableIdxs.add(lazyIdxForRel);

            List<Integer> lazySlotLocationForRel = new ArrayList<>();
            lazySlotLocations.add(lazySlotLocationForRel);
            for (Slot lazySlot : relationToLazySlotMap.get(rel)) {
                outputBuilder.add(lazySlot);
                lazyColumnForRel.add(((SlotReference) lazySlot).getColumn().get());
                lazyIdxForRel.add(relationTable.getBaseColumnIdxByName(lazySlot.getName()));
                lazySlotLocationForRel.add(loc);
                loc++;
            }
        }
        rowIdList = rowIdListBuilder.build();
        this.materializeOutput = outputBuilder.build();
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
        return materializeOutput;
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
        return new PhysicalLazyMaterialize<>(children.get(0),
                materializeInput, materializedSlots, relationToLazySlotMap,
                relationToRowId, materializeMap, null, null);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("PhysicalLazyMaterialize [Output= (")
                .append(getOutput()).append("), lazySlots= (");
        for (Map.Entry<CatalogRelation, List<Slot>> entry : relationToLazySlotMap.entrySet()) {
            builder.append(entry.getValue());
        }
        builder.append(")]");
        return builder.toString();
    }

    @Override
    public PhysicalPlan withPhysicalPropertiesAndStats(PhysicalProperties physicalProperties, Statistics statistics) {
        return new PhysicalLazyMaterialize(children.get(0), materializeInput, materializedSlots, relationToLazySlotMap,
                relationToRowId, materializeMap, physicalProperties, statistics);
    }

    @Override
    public String shapeInfo() {
        StringBuilder shapeBuilder = new StringBuilder();
        shapeBuilder.append(this.getClass().getSimpleName())
                .append("[").append("materializedSlots:")
                .append(ExpressionUtils.slotListShapeInfo(materializedSlots))
                .append("lazySlots: (");
        for (Map.Entry<CatalogRelation, List<Slot>> entry : relationToLazySlotMap.entrySet()) {
            shapeBuilder.append(entry.getValue());
        }
        shapeBuilder.append(")]");
        return shapeBuilder.toString();
    }

    public List<CatalogRelation> getRelations() {
        return relations;
    }

    public List<List<Column>> getLazyColumns() {
        return lazyColumns;
    }

    public List<List<Integer>> getLazySlotLocations() {
        return lazySlotLocations;
    }

    public List<List<Integer>> getlazyTableIdxs() {
        return lazyTableIdxs;
    }

    public List<Slot> getRowIds() {
        return rowIdList;
    }
}
