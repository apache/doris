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
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.processor.post.materialize.MaterializeSource;
import org.apache.doris.nereids.properties.DataTrait.Builder;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.AbstractPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.algebra.CatalogRelation;
import org.apache.doris.nereids.trees.plans.algebra.Relation;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.statistics.Statistics;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
    lazy materialize node
 */
public class PhysicalLazyMaterialize<CHILD_TYPE extends Plan> extends PhysicalUnary<CHILD_TYPE> {

    private final Map<Relation, List<Slot>> relationToLazySlotMap;

    private final BiMap<Relation, SlotReference> relationToRowId;

    private final Map<Slot, MaterializeSource> materializeMap;

    private final List<Slot> materializedSlots;

    private final List<Slot> materializeInput;
    private final List<Slot> materializeOutput;
    /**
     * The following four fields are used by BE to perform the actual lazy fetch.
     * They are indexed by relation: index i corresponds to relations.get(i).
     *
     * Example:
     *   SQL: SELECT t1.a, t1.b, t2.c, t2.d FROM t1 JOIN t2 ON ... WHERE t1.a > 5
     *   Assume t1.b and t2.d are lazily materialized (fetched after filtering).
     *
     *   materializedSlots (non-lazy, computed eagerly) = [t1.a, t2.c]
     *   Output slot order = [t1.a(0), t2.c(1), t1.b(2), t2.d(3)]
     *
     *   rowIdList         = [row_id_t1, row_id_t2]
     *                       The row-id slots passed to BE to locate the original rows.
     *
     *   relations         = [t1, t2]
     *
     *   lazyColumns       = [[Column(b)],          [Column(d)]]
     *                       For each relation, the Column objects to be lazily fetched.
     *
     *   lazyBaseColumnIndices = [[colIdxOf(b) in t1],  [colIdxOf(d) in t2]]
     *                       For each relation, the physical column index inside the table
     *                       for each lazy column (used by BE to locate the column on disk).
     *
     *   lazySlotLocations = [[2],                  [3]]
     *                       A two-level array: the outer level is indexed by relation
     *                       (same as relations / rowIdList), and the inner level lists the
     *                       output-tuple position for each lazy column of that relation.
     *                       Two levels are needed because a single relation can have
     *                       multiple lazy columns.  For example, if both t1.b and t1.e
     *                       were lazy, the entry for t1 would be [2, 4] (positions of b
     *                       and e in the output tuple), while t2 remains [3].
     *                       BE uses each position to know which output slot to fill in
     *                       after fetching the column value from disk.
     */
    private final List<Slot> rowIdList;
    private List<List<Column>> lazyColumns = new ArrayList<>();
    private List<List<Integer>> lazySlotLocations = new ArrayList<>();
    private List<List<Integer>> lazyBaseColumnIndices = new ArrayList<>();

    private final List<Relation> relations;

    /**
     * constructor
     */
    public PhysicalLazyMaterialize(CHILD_TYPE child,
            List<Slot> materializeInput,
            List<Slot> materializedSlots,
            Map<Relation, List<Slot>> relationToLazySlotMap,
            BiMap<Relation, SlotReference> relationToRowId,
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
            Map<Relation, List<Slot>> relationToLazySlotMap,
            BiMap<Relation, SlotReference> relationToRowId,
            Map<Slot, MaterializeSource> materializeMap,
            PhysicalProperties physicalProperties, Statistics statistics) {
        super(PlanType.PHYSICAL_MATERIALIZE, Optional.empty(),
                null, physicalProperties, statistics, child);
        this.materializeInput = materializeInput;
        this.relationToLazySlotMap = relationToLazySlotMap;
        this.relationToRowId = relationToRowId;
        this.materializedSlots = ImmutableList.copyOf(materializedSlots);
        this.materializeMap = materializeMap;
        lazySlotLocations = new ArrayList<>();
        lazyBaseColumnIndices = new ArrayList<>();
        lazyColumns = new ArrayList<>();

        ImmutableList.Builder<Slot> outputBuilder = ImmutableList.builder();
        outputBuilder.addAll(materializedSlots);
        int idx = materializedSlots.size();
        int loc = idx;
        ImmutableList.Builder<Slot> rowIdListBuilder = ImmutableList.builder();
        ImmutableList.Builder<Relation> relationListBuilder = ImmutableList.builder();
        for (; idx < materializeInput.size(); idx++) {
            Slot rowId = materializeInput.get(idx);
            rowIdListBuilder.add(rowId);
            Relation rel = relationToRowId.inverse().get(rowId);
            relationListBuilder.add(rel);
            TableIf relationTable;
            if (rel instanceof CatalogRelation) {
                relationTable = ((CatalogRelation) rel).getTable();
            } else if (rel instanceof PhysicalTVFRelation) {
                relationTable = ((PhysicalTVFRelation) rel).getFunction().getTable();
            } else {
                throw new AnalysisException("Unsupported relation type: " + rel);
            }

            List<Column> lazyColumnForRel = new ArrayList<>();
            lazyColumns.add(lazyColumnForRel);
            List<Integer> lazyBaseColumnIdxForRel = new ArrayList<>();
            lazyBaseColumnIndices.add(lazyBaseColumnIdxForRel);

            List<Integer> lazySlotLocationForRel = new ArrayList<>();
            lazySlotLocations.add(lazySlotLocationForRel);
            for (Slot lazySlot : relationToLazySlotMap.get(rel)) {
                // Set originalColumn on the lazy slot so that createSlotDesc can write
                // colUniqueId into the thrift SlotDescriptor — BE needs it to resolve
                // the column during remote fetch.
                Column originalColumn = materializeMap.get(lazySlot).baseSlot.getOriginalColumn().get();
                outputBuilder.add(((SlotReference) lazySlot).withColumn(originalColumn));
                lazyColumnForRel.add(originalColumn);
                lazyBaseColumnIdxForRel.add(relationTable.getBaseColumnIdxByName(lazySlot.getName()));
                lazySlotLocationForRel.add(loc);
                loc++;
            }
        }
        relations = relationListBuilder.build();
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
        return AbstractPlan.copyWithSameId(this, () -> new PhysicalLazyMaterialize<>(children.get(0),
                materializeInput, materializedSlots, relationToLazySlotMap,
                relationToRowId, materializeMap, null, null));
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("PhysicalLazyMaterialize [Output= (")
                .append(getOutput()).append("), lazySlots= (");
        for (Map.Entry<Relation, List<Slot>> entry : relationToLazySlotMap.entrySet()) {
            builder.append(entry.getValue());
        }
        builder.append(")]");
        return builder.toString();
    }

    @Override
    public PhysicalPlan withPhysicalPropertiesAndStats(PhysicalProperties physicalProperties, Statistics statistics) {
        return AbstractPlan.copyWithSameId(this, () -> new PhysicalLazyMaterialize(children.get(0),
                materializeInput, materializedSlots, relationToLazySlotMap,
                relationToRowId, materializeMap, physicalProperties, statistics));
    }

    @Override
    public String shapeInfo() {
        StringBuilder shapeBuilder = new StringBuilder();
        List<Slot> lazySlots = new ArrayList<>();
        for (List<Slot> slots : relationToLazySlotMap.values()) {
            lazySlots.addAll(slots);
        }
        lazySlots = lazySlots.stream().sorted(new Comparator<Slot>() {
            @Override
            public int compare(Slot slot, Slot t1) {
                return slot.shapeInfo().compareTo(t1.shapeInfo());
            }
        }).collect(Collectors.toList());
        shapeBuilder.append(this.getClass().getSimpleName())
                .append("[").append("materializedSlots:")
                .append(ExpressionUtils.slotListShapeInfo(materializedSlots))
                .append(" lazySlots:")
                .append(ExpressionUtils.slotListShapeInfo(lazySlots));
        shapeBuilder.append("]");
        return shapeBuilder.toString();
    }

    public List<Relation> getRelations() {
        return relations;
    }

    public List<List<Column>> getLazyColumns() {
        return lazyColumns;
    }

    public List<List<Integer>> getLazySlotLocations() {
        return lazySlotLocations;
    }

    public List<List<Integer>> getLazyBaseColumnIndices() {
        return lazyBaseColumnIndices;
    }

    public List<Slot> getRowIds() {
        return rowIdList;
    }
}
