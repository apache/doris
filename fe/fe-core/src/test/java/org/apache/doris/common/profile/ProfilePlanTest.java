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

package org.apache.doris.common.profile;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.FunctionGenTable;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.IdGenerator;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.nereids.hint.DistributeHint;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.memo.GroupId;
import org.apache.doris.nereids.processor.post.materialize.MaterializeSource;
import org.apache.doris.nereids.properties.DataTrait;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.functions.table.TableValuedFunction;
import org.apache.doris.nereids.trees.plans.DistributeType;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PreAggStatus;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan.SelectedPartitions;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFileScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLazyMaterialize;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLazyMaterializeFileScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLazyMaterializeOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLazyMaterializeTVFScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlanShallowCopier;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSqlCache;
import org.apache.doris.nereids.trees.plans.physical.PhysicalStorageLayerAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalStorageLayerAggregate.PushDownAggOp;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTVFRelation;
import org.apache.doris.nereids.trees.plans.physical.RuntimeFilter;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.util.MutableState;
import org.apache.doris.planner.RuntimeFilterId;
import org.apache.doris.statistics.model.ColumnStatistic;
import org.apache.doris.statistics.model.ColumnStatisticBuilder;
import org.apache.doris.statistics.model.Statistics;
import org.apache.doris.thrift.TMinMaxRuntimeFilterType;
import org.apache.doris.thrift.TRuntimeFilterType;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.HashBiMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

class ProfilePlanTest {
    private final SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
    private final SlotReference b = new SlotReference("b", IntegerType.INSTANCE);
    private final SlotReference unused = new SlotReference("unused", IntegerType.INSTANCE);
    private final ColumnStatistic known = new ColumnStatisticBuilder(100).setNdv(10).build();

    @Test
    void testCloneDetachesMemoAndRuntimeFilterReferences() throws Exception {
        PhysicalOlapScan left = scan("db", "t", List.of(a), Map.of(a, known));
        PhysicalOlapScan right = scan("db", "u", List.of(b), Map.of(b, known));
        GroupExpression expression = new GroupExpression(left);
        new Group(GroupId.createGenerator().getNextId(), expression, left.getLogicalProperties());
        left = (PhysicalOlapScan) left.withGroupExpression(Optional.of(expression));
        left.setStatistics(new Statistics(100, Map.of(a, known)));
        PhysicalHashJoin<Plan, Plan> join = new PhysicalHashJoin<>(JoinType.INNER_JOIN,
                List.of(new EqualTo(a, b)), List.of(), new DistributeHint(DistributeType.NONE),
                Optional.empty(), null, left, right);
        join.setStatistics(new Statistics(10, Map.of(a, known, b, known)));
        left.setMutableState(MutableState.KEY_PARENT, join);
        IdGenerator<RuntimeFilterId> filterIds = RuntimeFilterId.createGenerator();
        RuntimeFilter filter = new RuntimeFilter(filterIds.getNextId(), b, a, a, TRuntimeFilterType.BLOOM,
                0, join, 10, true, TMinMaxRuntimeFilterType.MIN_MAX, left);
        left.addAppliedRuntimeFilter(filter);
        RuntimeFilter secondFilter = new RuntimeFilter(filterIds.getNextId(), a, b, b, TRuntimeFilterType.BLOOM,
                1, join, 10, true, TMinMaxRuntimeFilterType.MIN_MAX, right);
        right.addAppliedRuntimeFilter(secondFilter);

        PhysicalHashJoin<?, ?> copy = (PhysicalHashJoin<?, ?>) PhysicalPlanShallowCopier.copy(join);
        Assertions.assertEquals(join.treeString(), copy.treeString());
        Assertions.assertEquals(join.getId(), copy.getId());
        Assertions.assertSame(join.getHashJoinConjuncts().get(0), copy.getHashJoinConjuncts().get(0));
        Assertions.assertNotSame(join.getStats(), copy.getStats());
        Assertions.assertTrue(copy.child(0).getGroupExpression().isEmpty());
        Assertions.assertEquals(left.getGroupIdAsString(), copy.child(0).getGroupIdAsString());
        Assertions.assertTrue(copy.child(0).getMutableState(MutableState.KEY_PARENT).isEmpty());
        RuntimeFilter copiedFilter = copy.getRuntimeFilters().get(0);
        Assertions.assertNotSame(filter, copiedFilter);
        Assertions.assertSame(copy, copiedFilter.getBuilderNode());
        Assertions.assertSame(copy.child(0), copiedFilter.getTargetScan());
        Assertions.assertSame(copiedFilter,
                ((PhysicalOlapScan) copy.child(0)).getAppliedRuntimeFilters().get(0));
        copy.updateActualRowCount(42);
        Assertions.assertEquals(-1, join.getStats().getActualRowCount());
        assertNoMemoReferences(copy, Set.of(join, left, right));
    }

    @Test
    void testCloneEmbeddedScans() throws Exception {
        PhysicalOlapScan scan = scan("db", "t", List.of(a), Map.of(a, known));
        SlotReference rowId = new SlotReference("row_id", IntegerType.INSTANCE);
        PhysicalLazyMaterializeOlapScan lazy = new PhysicalLazyMaterializeOlapScan(scan, rowId, List.of());
        PhysicalPlan lazyCopy = PhysicalPlanShallowCopier.copy(lazy);
        Assertions.assertEquals(lazy.treeString(), lazyCopy.treeString());
        Assertions.assertNotSame(scan, ((PhysicalLazyMaterializeOlapScan) lazyCopy).getScan());
        assertNoMemoReferences(lazyCopy, Set.of(scan, lazy));

        PhysicalStorageLayerAggregate aggregate = new PhysicalStorageLayerAggregate(scan, PushDownAggOp.COUNT);
        PhysicalStorageLayerAggregate aggregateCopy = (PhysicalStorageLayerAggregate)
                PhysicalPlanShallowCopier.copy(aggregate);
        Assertions.assertEquals(aggregate.treeString(), aggregateCopy.treeString());
        Assertions.assertNotSame(scan, aggregateCopy.getRelation());
        assertNoMemoReferences(aggregateCopy, Set.of(scan, aggregate));
    }

    @Test
    void testCloneExternalAndTvfLazyScans() throws Exception {
        ExternalTable table = Mockito.mock(ExternalTable.class);
        Mockito.when(table.getName()).thenReturn("external_t");
        LogicalProperties properties = new LogicalProperties(() -> List.of(a), () -> DataTrait.EMPTY_TRAIT);
        PhysicalFileScan file = new PhysicalFileScan(StatementScopeIdGenerator.newRelationId(), table,
                List.of("catalog", "db"), null, Optional.empty(), properties, SelectedPartitions.NOT_PRUNED,
                Optional.empty(), Optional.empty(), List.of(a), Optional.empty());
        SlotReference rowId = new SlotReference("row_id", IntegerType.INSTANCE);
        PhysicalLazyMaterializeFileScan lazyFile = new PhysicalLazyMaterializeFileScan(file, rowId, List.of());
        PhysicalLazyMaterializeFileScan fileCopy =
                (PhysicalLazyMaterializeFileScan) PhysicalPlanShallowCopier.copy(lazyFile);
        Assertions.assertEquals(lazyFile.treeString(), fileCopy.treeString());
        Assertions.assertEquals(lazyFile.getId(), fileCopy.getId());
        assertNoMemoReferences(fileCopy, Set.of(file, lazyFile));

        TableValuedFunction function = Mockito.mock(TableValuedFunction.class);
        FunctionGenTable tvfTable = Mockito.mock(FunctionGenTable.class);
        Mockito.when(tvfTable.getName()).thenReturn("numbers");
        Mockito.when(function.getTable()).thenReturn(tvfTable);
        PhysicalTVFRelation tvf = new PhysicalTVFRelation(StatementScopeIdGenerator.newRelationId(), function,
                List.of(a), properties);
        PhysicalLazyMaterializeTVFScan lazyTvf = new PhysicalLazyMaterializeTVFScan(tvf, rowId, List.of());
        PhysicalLazyMaterializeTVFScan tvfCopy =
                (PhysicalLazyMaterializeTVFScan) PhysicalPlanShallowCopier.copy(lazyTvf);
        Assertions.assertEquals(lazyTvf.treeString(), tvfCopy.treeString());
        Assertions.assertEquals(lazyTvf.getId(), tvfCopy.getId());
        assertNoMemoReferences(tvfCopy, Set.of(tvf, lazyTvf));
    }

    @Test
    void testCloneLazyMaterializeRelationMaps() throws Exception {
        PhysicalOlapScan scan = scan("db", "t", List.of(a), Map.of(a, known));
        SlotReference rowId = new SlotReference("row_id", IntegerType.INSTANCE);
        SlotReference lazySlot = SlotReference.fromColumn(StatementScopeIdGenerator.newExprId(),
                scan.getTable(), new Column("a", Type.INT), List.of("db", "t"));
        PhysicalLazyMaterialize<?> materialize = new PhysicalLazyMaterialize<>(scan, List.of(rowId),
                List.of(), Map.of(scan, List.of(lazySlot)), HashBiMap.create(Map.of(scan, rowId)),
                Map.of(lazySlot, new MaterializeSource(scan, lazySlot)));
        PhysicalLazyMaterialize<?> copy = (PhysicalLazyMaterialize<?>)
                PhysicalPlanShallowCopier.copy(materialize);
        Assertions.assertEquals(materialize.treeString(), copy.treeString());
        Assertions.assertEquals(materialize.getId(), copy.getId());
        Assertions.assertSame(copy.child(), copy.getRelations().get(0));
        Assertions.assertEquals(materialize.getLazyColumns(), copy.getLazyColumns());
        Assertions.assertEquals(materialize.getLazyBaseColumnIndices(), copy.getLazyBaseColumnIndices());
        assertNoMemoReferences(copy, Set.of(scan, materialize));
    }

    @Test
    void testCloneSqlCachePlan() {
        PhysicalSqlCache cache = new PhysicalSqlCache(new TUniqueId(1, 2), List.of(), List.of(), List.of(),
                Optional.empty(), List.of(), "backend", "cached plan");
        PhysicalSqlCache copy = (PhysicalSqlCache) PhysicalPlanShallowCopier.copy(cache);
        Assertions.assertNotSame(cache, copy);
        Assertions.assertEquals(cache.getId(), copy.getId());
        Assertions.assertEquals(cache.treeString(), copy.treeString());
        Assertions.assertTrue(copy.getGroupExpression().isEmpty());
    }

    @Test
    void testKnownOperativeStatsArePrintedOnceAcrossAliases() {
        PhysicalOlapScan first = scan("db", "t", List.of(a, b),
                Map.of(a, known, b, ColumnStatistic.UNKNOWN, unused, known));
        SlotReference aliasA = new SlotReference("a", IntegerType.INSTANCE);
        SlotReference aliasB = new SlotReference("b", IntegerType.INSTANCE);
        PhysicalOlapScan second = scan("db", "t", List.of(aliasA, aliasB),
                Map.of(aliasA, known, aliasB, known));
        String expected = "db.t\n  a -> " + known + "\n  b -> " + known + "\n";
        Assertions.assertEquals(expected, Profile.collectColumnStats(List.of(first, second, first)));
        Assertions.assertEquals(expected, Profile.collectColumnStats(List.of(first, second)));
        Assertions.assertEquals("", Profile.collectColumnStats(List.of(
                scan("db", "unknown", List.of(a), Map.of(a, ColumnStatistic.UNKNOWN)),
                scan("db", "unused", List.of(), Map.of(unused, known)),
                scan("db", "missing", List.of(a), Map.of()))));
        Assertions.assertEquals("db.t\n  a -> " + known + "\nother.t\n  a -> " + known + "\n",
                Profile.collectColumnStats(List.of(first, scan("other", "t", List.of(a), Map.of(a, known)))));
    }

    @Test
    void testProfileOwnsItsPlanAndResetsOnlyItsActualRows() {
        PhysicalOlapScan scan = scan("db", "t", List.of(a), Map.of(a, known));
        Profile profile = new Profile();
        profile.setPhysicalPlan(scan);
        PhysicalPlan copy = profile.getPhysicalPlan();
        Assertions.assertNotSame(scan, copy);
        ((PhysicalOlapScan) copy).updateActualRowCount(42);
        Assertions.assertEquals(-1, scan.getStats().getActualRowCount());
        profile.clearExecutionProfiles();
        Assertions.assertEquals(-1, copy.getStats().getActualRowCount());
        profile.clearPlan();
        Assertions.assertNull(profile.getPhysicalPlan());
        profile.setPhysicalPlan(null);
        Assertions.assertNull(profile.getPhysicalPlan());
    }

    private PhysicalOlapScan scan(String database, String name, List<Slot> operativeSlots,
            Map<? extends Expression, ColumnStatistic> stats) {
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(table.getName()).thenReturn(name);
        Mockito.when(table.getNameWithFullQualifiers()).thenReturn(database + "." + name);
        LogicalProperties properties = new LogicalProperties(() -> List.of(a, b, unused),
                () -> DataTrait.EMPTY_TRAIT);
        PhysicalOlapScan scan = new PhysicalOlapScan(StatementScopeIdGenerator.newRelationId(), table,
                List.of(database), 0L, List.of(), List.of(), null, PreAggStatus.on(), List.of(a, b, unused),
                Optional.empty(), properties, Optional.empty(), operativeSlots, List.of(), List.of(),
                Optional.empty(), Optional.empty(), List.of(), Optional.empty());
        scan.setStatistics(new Statistics(100, new HashMap<>(stats)));
        return scan;
    }

    // Inspect reachability deterministically instead of relying on GC timing. Include lazy suppliers,
    // mutable states and embedded scans; catalog metadata and shared expressions are intentionally shared.
    private void assertNoMemoReferences(Object root, Set<Plan> originalPlans) throws Exception {
        Set<Object> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        ArrayDeque<Object> pending = new ArrayDeque<>();
        pending.add(root);
        while (!pending.isEmpty()) {
            Object object = pending.removeFirst();
            if (!visited.add(object)) {
                continue;
            }
            Assertions.assertFalse(object instanceof Group || object instanceof GroupExpression);
            for (Plan original : originalPlans) {
                Assertions.assertNotSame(original, object);
            }
            if (object instanceof Optional) {
                ((Optional<?>) object).ifPresent(pending::add);
            } else if (object instanceof Map) {
                ((Map<?, ?>) object).forEach((key, value) -> {
                    pending.add(key);
                    pending.add(value);
                });
            } else if (object instanceof Iterable) {
                ((Iterable<?>) object).forEach(pending::add);
            } else {
                Class<?> type = object.getClass();
                while (type != null && type.getName().startsWith("org.apache.doris.nereids")) {
                    for (Field field : type.getDeclaredFields()) {
                        if (!Modifier.isStatic(field.getModifiers())) {
                            field.setAccessible(true);
                            Object value = field.get(object);
                            if (value != null) {
                                pending.add(value);
                            }
                        }
                    }
                    type = type.getSuperclass();
                }
            }
        }
    }
}
