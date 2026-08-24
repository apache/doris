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
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.nereids.hint.DistributeHint;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.memo.GroupId;
import org.apache.doris.nereids.processor.post.materialize.MaterializeSource;
import org.apache.doris.nereids.properties.DataTrait;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.AbstractPlan;
import org.apache.doris.nereids.trees.plans.DistributeType;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.LimitPhase;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PreAggStatus;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.algebra.Relation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalStorageLayerAggregate.PushDownAggOp;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.util.MutableState;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.planner.RuntimeFilterId;
import org.apache.doris.statistics.Statistics;
import org.apache.doris.thrift.TMinMaxRuntimeFilterType;
import org.apache.doris.thrift.TRuntimeFilterType;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Tests for {@link AbstractPhysicalPlan#copyPlanDetachedFromMemo(PhysicalPlan)}:
 * the copy must clear every node's group expression (breaking the reference to the memo)
 * while preserving node ids, statistics, physical properties, group ids and runtime filters.
 */
public class PhysicalPlanDetachTest {

    private static final LogicalProperties LOGICAL_PROPERTIES =
            new LogicalProperties(() -> ImmutableList.of(), () -> DataTrait.EMPTY_TRAIT);

    private static final Statistics STATS = new Statistics(100.0, ImmutableMap.of());

    /** Attach a group expression (with a real owner group) to the plan node. */
    private static <T extends AbstractPlan> T attachToGroup(T plan) {
        GroupExpression groupExpression = new GroupExpression(plan, ImmutableList.of());
        Group group = new Group(GroupId.createGenerator().getNextId(),
                groupExpression.getPlan().getLogicalProperties());
        groupExpression.setOwnerGroup(group);
        return (T) groupExpression.getPlan();
    }

    private static PhysicalOlapScan olapScan() {
        OlapTable table = PlanConstructor.newOlapTable(0, "t1", 0, KeysType.DUP_KEYS);
        List<String> qualifier = new ArrayList<>();
        qualifier.add("test");
        List<Slot> output = new ArrayList<>();
        SlotReference id = SlotReference.fromColumn(new ExprId(0), table,
                new Column("id", org.apache.doris.catalog.Type.INT), qualifier);
        output.add(id);
        LogicalProperties tableProperties = new LogicalProperties(() -> output,
                () -> DataTrait.EMPTY_TRAIT);
        PhysicalOlapScan scan = new PhysicalOlapScan(RelationId.createGenerator().getNextId(), table,
                qualifier, 0L, Collections.emptyList(), Collections.emptyList(), null,
                PreAggStatus.on(), ImmutableList.of(), Optional.empty(), tableProperties, Optional.empty(),
                ImmutableList.of(), ImmutableList.of(), ImmutableList.of(), Optional.empty(), Optional.empty(),
                ImmutableList.of(), Optional.empty());
        return (PhysicalOlapScan) attachToGroup(scan)
                .withPhysicalPropertiesAndStats(PhysicalProperties.GATHER, STATS);
    }

    private static PhysicalOneRowRelation oneRow() {
        PhysicalOneRowRelation leaf = new PhysicalOneRowRelation(
                new RelationId(1),
                ImmutableList.of(new Alias(Literal.of(1L), "a")),
                LOGICAL_PROPERTIES);
        return (PhysicalOneRowRelation) attachToGroup(leaf)
                .withPhysicalPropertiesAndStats(PhysicalProperties.GATHER, STATS);
    }

    @Test
    public void testDetachGenericTree() {
        PhysicalOneRowRelation leaf = oneRow();
        PhysicalLimit limit = new PhysicalLimit(1, 0, LimitPhase.ORIGIN, Optional.empty(),
                LOGICAL_PROPERTIES, PhysicalProperties.GATHER, STATS, leaf);
        limit = (PhysicalLimit) attachToGroup(limit)
                .withPhysicalPropertiesAndStats(PhysicalProperties.GATHER, STATS);
        List<NamedExpression> projects = ImmutableList.of(
                new SlotReference(new ExprId(0), "a", BigIntType.INSTANCE, true, Lists.newArrayList()));
        PhysicalProject root = new PhysicalProject<>(projects, Optional.empty(),
                LOGICAL_PROPERTIES, PhysicalProperties.GATHER, STATS, limit);
        root = (PhysicalProject) attachToGroup(root)
                .withPhysicalPropertiesAndStats(PhysicalProperties.GATHER, STATS);

        Assertions.assertTrue(root.getGroupExpression().isPresent());
        Assertions.assertTrue(limit.getGroupExpression().isPresent());
        Assertions.assertTrue(leaf.getGroupExpression().isPresent());

        PhysicalPlan detached = AbstractPhysicalPlan.copyPlanDetachedFromMemo(root);

        // Original tree is untouched.
        Assertions.assertTrue(root.getGroupExpression().isPresent());
        Assertions.assertTrue(limit.getGroupExpression().isPresent());
        Assertions.assertTrue(leaf.getGroupExpression().isPresent());

        // The copy has the same shape and node ids, but no group expression on any node.
        List<Plan> originalNodes = root.collectToList(p -> true);
        List<Plan> detachedNodes = detached.collectToList(p -> true);
        Assertions.assertEquals(originalNodes.size(), detachedNodes.size());
        for (int i = 0; i < originalNodes.size(); i++) {
            AbstractPlan original = (AbstractPlan) originalNodes.get(i);
            AbstractPlan copy = (AbstractPlan) detachedNodes.get(i);
            Assertions.assertEquals(original.getId(), copy.getId(),
                    "node id should be preserved");
            Assertions.assertTrue(copy.getGroupExpression().isEmpty(),
                    "group expression should be cleared on " + copy);
            Assertions.assertSame(original.getStats(), copy.getStats(),
                    "statistics should be preserved on " + copy);
            Assertions.assertEquals(original.getGroupIdAsString(), copy.getGroupIdAsString(),
                    "group id should be preserved on " + copy);
        }
        // Group ids are kept as mutable state instead of a memo reference.
        Assertions.assertTrue(detached.getMutableState(MutableState.KEY_GROUP).isPresent());
    }

    @Test
    public void testDetachStorageLayerAggregate() {
        PhysicalOlapScan scan = olapScan();
        PhysicalStorageLayerAggregate storageAgg = new PhysicalStorageLayerAggregate(scan, PushDownAggOp.COUNT);
        storageAgg = (PhysicalStorageLayerAggregate) attachToGroup(storageAgg)
                .withPhysicalPropertiesAndStats(PhysicalProperties.GATHER, STATS);
        Assertions.assertTrue(storageAgg.getRelation().getGroupExpression().isPresent());

        PhysicalPlan detached = AbstractPhysicalPlan.copyPlanDetachedFromMemo(storageAgg);

        Assertions.assertTrue(detached instanceof PhysicalStorageLayerAggregate);
        Assertions.assertTrue(detached.getGroupExpression().isEmpty());
        Assertions.assertSame(STATS, detached.getStats());
        Assertions.assertEquals(storageAgg.getGroupIdAsString(), detached.getGroupIdAsString());
        // The inner relation must be detached as well.
        PhysicalStorageLayerAggregate copy = (PhysicalStorageLayerAggregate) detached;
        Assertions.assertFalse(copy.getRelation().getGroupExpression().isPresent());
        Assertions.assertNotSame(scan, copy.getRelation());
    }

    @Test
    public void testDetachLazyMaterialize() {
        PhysicalOlapScan scan = olapScan();
        PhysicalOneRowRelation child = oneRow();

        Slot idSlot = scan.getOutput().get(0);
        SlotReference rowId = new SlotReference(new ExprId(1), "row_id", BigIntType.INSTANCE,
                true, Lists.newArrayList());
        Map<Relation, List<Slot>> relationToLazySlotMap = new HashMap<>();
        relationToLazySlotMap.put(scan, ImmutableList.of(idSlot));
        BiMap<Relation, SlotReference> relationToRowId = HashBiMap.create();
        relationToRowId.put(scan, rowId);
        Map<Slot, MaterializeSource> materializeMap = new HashMap<>();
        materializeMap.put(idSlot, new MaterializeSource(scan, (SlotReference) idSlot));

        PhysicalLazyMaterialize<Plan> lazy = new PhysicalLazyMaterialize<>(child,
                ImmutableList.of(idSlot, rowId), ImmutableList.of(idSlot),
                relationToLazySlotMap, relationToRowId, materializeMap,
                PhysicalProperties.GATHER, STATS);

        PhysicalPlan detached = AbstractPhysicalPlan.copyPlanDetachedFromMemo(lazy);

        Assertions.assertTrue(detached instanceof PhysicalLazyMaterialize);
        Assertions.assertTrue(detached.getGroupExpression().isEmpty());
        Assertions.assertSame(STATS, detached.getStats());
        // The copy must not keep the original scan nodes (and thus the memo) alive.
        PhysicalLazyMaterialize<?> copy = (PhysicalLazyMaterialize<?>) detached;
        Assertions.assertEquals(1, copy.getRelations().size());
        Assertions.assertNotSame(scan, copy.getRelations().get(0));
        Assertions.assertTrue(((Plan) copy.getRelations().get(0)).getGroupExpression().isEmpty());
    }

    @Test
    public void testDetachRuntimeFilters() {
        PhysicalOlapScan scan = olapScan();
        PhysicalOneRowRelation left = oneRow();
        PhysicalHashJoin<Plan, Plan> join = new PhysicalHashJoin<>(JoinType.INNER_JOIN,
                ImmutableList.of(), ImmutableList.of(), new DistributeHint(DistributeType.NONE),
                Optional.empty(), Optional.empty(), LOGICAL_PROPERTIES, left, scan);
        join = attachToGroup(join).withPhysicalPropertiesAndStats(PhysicalProperties.GATHER, STATS);

        SlotReference src = new SlotReference(new ExprId(0), "src", BigIntType.INSTANCE,
                true, Lists.newArrayList());
        SlotReference target = new SlotReference(new ExprId(1), "target", BigIntType.INSTANCE,
                true, Lists.newArrayList());
        RuntimeFilter rf = new RuntimeFilter(RuntimeFilterId.createGenerator().getNextId(), src,
                target, target, TRuntimeFilterType.IN_OR_BLOOM, 0, join, -1L, true,
                TMinMaxRuntimeFilterType.MIN_MAX, scan);
        scan.addAppliedRuntimeFilter(rf);
        Assertions.assertEquals(1, join.getRuntimeFilters().size());
        Assertions.assertEquals(1, scan.getAppliedRuntimeFilters().size());

        PhysicalPlan detached = AbstractPhysicalPlan.copyPlanDetachedFromMemo(join);
        PhysicalHashJoin<?, ?> joinCopy = (PhysicalHashJoin<?, ?>) detached;
        PhysicalOlapScan scanCopy = (PhysicalOlapScan) detached.child(1);

        // The cloned join keeps its runtime filter, re-pointed to the cloned nodes.
        Assertions.assertEquals(1, joinCopy.getRuntimeFilters().size());
        RuntimeFilter rfCopy = joinCopy.getRuntimeFilters().get(0);
        Assertions.assertSame(joinCopy, rfCopy.getBuilderNode());
        Assertions.assertNotSame(join, rfCopy.getBuilderNode());
        Assertions.assertSame(scanCopy, rfCopy.getTargetScan());
        Assertions.assertNotSame(scan, rfCopy.getTargetScan());
        Assertions.assertTrue(rfCopy.getTargetScan().getGroupExpression().isEmpty());
        // The cloned scan keeps the applied runtime filter (same object as on the join).
        Assertions.assertEquals(1, scanCopy.getAppliedRuntimeFilters().size());
        Assertions.assertSame(rfCopy, scanCopy.getAppliedRuntimeFilters().get(0));
        // The printed plan keeps the RF information.
        Assertions.assertTrue(joinCopy.toString().contains("RFs"));
        // The original tree is untouched.
        Assertions.assertEquals(1, join.getRuntimeFilters().size());
        Assertions.assertSame(join, join.getRuntimeFilters().get(0).getBuilderNode());
        Assertions.assertEquals(1, scan.getAppliedRuntimeFilters().size());
        Assertions.assertSame(rf, scan.getAppliedRuntimeFilters().get(0));
    }

    @Test
    public void testDetachRuntimeFiltersOnMemoFreeScan() {
        // A scan without a group expression normally gets reused as is, but if it carries an
        // applied runtime filter it must still be copied, otherwise the filter would reference
        // the original (memo-carrying) builder node through the reused node.
        PhysicalOlapScan scan = olapScan().withGroupExpression(Optional.empty())
                .withPhysicalPropertiesAndStats(PhysicalProperties.GATHER, STATS);
        Assertions.assertTrue(scan.getGroupExpression().isEmpty());
        PhysicalOneRowRelation left = oneRow();
        PhysicalHashJoin<Plan, Plan> join = new PhysicalHashJoin<>(JoinType.INNER_JOIN,
                ImmutableList.of(), ImmutableList.of(), new DistributeHint(DistributeType.NONE),
                Optional.empty(), Optional.empty(), LOGICAL_PROPERTIES, left, scan);
        join = attachToGroup(join).withPhysicalPropertiesAndStats(PhysicalProperties.GATHER, STATS);

        SlotReference src = new SlotReference(new ExprId(0), "src", BigIntType.INSTANCE,
                true, Lists.newArrayList());
        SlotReference target = new SlotReference(new ExprId(1), "target", BigIntType.INSTANCE,
                true, Lists.newArrayList());
        RuntimeFilter rf = new RuntimeFilter(RuntimeFilterId.createGenerator().getNextId(), src,
                target, target, TRuntimeFilterType.IN_OR_BLOOM, 0, join, -1L, true,
                TMinMaxRuntimeFilterType.MIN_MAX, scan);
        scan.addAppliedRuntimeFilter(rf);

        PhysicalPlan detached = AbstractPhysicalPlan.copyPlanDetachedFromMemo(join);
        PhysicalHashJoin<?, ?> joinCopy = (PhysicalHashJoin<?, ?>) detached;
        PhysicalOlapScan scanCopy = (PhysicalOlapScan) detached.child(1);

        // The memo-free scan must be copied, not reused, because of its runtime filter.
        Assertions.assertNotSame(scan, scanCopy);
        Assertions.assertTrue(scanCopy.getGroupExpression().isEmpty());
        Assertions.assertEquals(1, scanCopy.getAppliedRuntimeFilters().size());
        RuntimeFilter rfCopy = scanCopy.getAppliedRuntimeFilters().get(0);
        Assertions.assertSame(joinCopy, rfCopy.getBuilderNode());
        Assertions.assertSame(scanCopy, rfCopy.getTargetScan());
        // The original scan and join are untouched.
        Assertions.assertSame(rf, scan.getAppliedRuntimeFilters().get(0));
        Assertions.assertSame(join, join.getRuntimeFilters().get(0).getBuilderNode());
    }

    @Test
    public void testDetachNullAndMemoFreePlan() {
        Assertions.assertNull(AbstractPhysicalPlan.copyPlanDetachedFromMemo(null));
        // A plan node without group expression and without memo-referencing children is reused as is.
        PhysicalOneRowRelation leaf = new PhysicalOneRowRelation(
                new RelationId(1),
                ImmutableList.of(new Alias(Literal.of(1L), "a")),
                LOGICAL_PROPERTIES);
        Assertions.assertSame(leaf, AbstractPhysicalPlan.copyPlanDetachedFromMemo(leaf));
    }
}
