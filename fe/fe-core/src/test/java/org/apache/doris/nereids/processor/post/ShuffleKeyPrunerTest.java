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

package org.apache.doris.nereids.processor.post;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.properties.DistributionSpec;
import org.apache.doris.nereids.properties.DistributionSpecHash;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateParam;
import org.apache.doris.nereids.trees.plans.AggMode;
import org.apache.doris.nereids.trees.plans.AggPhase;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDistribute;
import org.apache.doris.nereids.trees.plans.physical.PhysicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Optional;

/** Unit tests for {@link ShuffleKeyPruner} helpers (join child unwrap / replace). */
class ShuffleKeyPrunerTest extends TestWithFeService {

    private SlotReference slotA;
    private PhysicalEmptyRelation empty;

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        useDatabase("test");
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        connectContext.getSessionVariable().setParallelResultSink(false);
        connectContext.getSessionVariable().enableShuffleKeyPrune = true;

        createTable("CREATE TABLE `t1` (\n"
                + "  `a` int(11) NULL,\n"
                + "  `b` int(11) NULL,\n"
                + "  `d` int(11) NULL\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`a`, `b`, `d`)\n"
                + "COMMENT 'OLAP'\n"
                + "DISTRIBUTED BY HASH(`b`) BUCKETS 3\n"
                + "PROPERTIES (\n"
                + "\"replication_allocation\" = \"tag.location.default: 1\",\n"
                + "\"in_memory\" = \"false\",\n"
                + "\"storage_format\" = \"V2\",\n"
                + "\"disable_auto_compaction\" = \"false\"\n"
                + ");");

        createTable("CREATE TABLE `t2` (\n"
                + "  `a` int(11) NULL,\n"
                + "  `b` int(11) NULL,\n"
                + "  `d` int(11) NULL\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`a`, `b`, `d`)\n"
                + "COMMENT 'OLAP'\n"
                + "DISTRIBUTED BY HASH(`b`) BUCKETS 3\n"
                + "PROPERTIES (\n"
                + "\"replication_allocation\" = \"tag.location.default: 1\",\n"
                + "\"in_memory\" = \"false\",\n"
                + "\"storage_format\" = \"V2\",\n"
                + "\"disable_auto_compaction\" = \"false\"\n"
                + ");");

        slotA = new SlotReference(new ExprId(0), "a", IntegerType.INSTANCE, true, ImmutableList.of());
        empty = new PhysicalEmptyRelation(
                connectContext.getStatementContext().getNextRelationId(),
                ImmutableList.of(slotA), null);
    }

    @Test
    void testSkewJoinHintShouldNotTriggerShuffleKeyPrune() {
        String sql = "select t1.a,t2.b from t1 join [shuffle[skew(t1.b(1,2))]]t2 "
                + "on t1.b=t2.b and t1.d=t2.d order by 1,2";
        int[] pruneOn = extractJoinShuffleKeySizes(sql, true);
        int[] pruneOff = extractJoinShuffleKeySizes(sql, false);
        connectContext.getSessionVariable().enableShuffleKeyPrune = true;

        Assertions.assertArrayEquals(pruneOff, pruneOn);
    }

    @Test
    void testDistinctSkewAggShouldNotPruneDistinctGlobalDistribute() {
        String sql = "select a,count(distinct [skew] b) from t1 group by a";
        int keySizeOn = extractDistinctGlobalChildDistributeKeySize(sql, true);
        int keySizeOff = extractDistinctGlobalChildDistributeKeySize(sql, false);
        connectContext.getSessionVariable().enableShuffleKeyPrune = true;

        Assertions.assertEquals(keySizeOff, keySizeOn);
        Assertions.assertEquals(1, keySizeOn);
    }

    private int[] extractJoinShuffleKeySizes(String sql, boolean enablePrune) {
        connectContext.getSessionVariable().enableShuffleKeyPrune = enablePrune;
        int[] sizes = new int[2];
        PlanChecker.from(connectContext).checkExplain(sql, planner -> {
            PhysicalHashJoin<? extends Plan, ? extends Plan> join = findFirstHashJoin(planner.getOptimizedPlan());
            Assertions.assertNotNull(join);
            Assertions.assertNotNull(join.getDistributeHint().getSkewInfo());

            DistributionSpecHash leftSpec = getHashSpecFromJoinChild(join.left());
            DistributionSpecHash rightSpec = getHashSpecFromJoinChild(join.right());
            sizes[0] = leftSpec.getOrderedShuffledColumns().size();
            sizes[1] = rightSpec.getOrderedShuffledColumns().size();
        });
        return sizes;
    }

    private int extractDistinctGlobalChildDistributeKeySize(String sql, boolean enablePrune) {
        connectContext.getSessionVariable().enableShuffleKeyPrune = enablePrune;
        int[] size = new int[] {-1};
        PlanChecker.from(connectContext).checkExplain(sql, planner -> {
            PhysicalHashAggregate<? extends Plan> distinctGlobal = findFirstDistinctGlobalAgg(planner.getOptimizedPlan());
            Assertions.assertNotNull(distinctGlobal);
            Assertions.assertInstanceOf(PhysicalDistribute.class, distinctGlobal.child());
            Assertions.assertInstanceOf(DistributionSpecHash.class,
                    ((PhysicalDistribute<?>) distinctGlobal.child()).getDistributionSpec());
            DistributionSpecHash spec = (DistributionSpecHash) ((PhysicalDistribute<?>) distinctGlobal.child())
                    .getDistributionSpec();
            size[0] = spec.getOrderedShuffledColumns().size();
        });
        return size[0];
    }

    private PhysicalHashAggregate<? extends Plan> findFirstDistinctGlobalAgg(Plan plan) {
        if (plan instanceof PhysicalHashAggregate) {
            PhysicalHashAggregate<? extends Plan> agg = (PhysicalHashAggregate<? extends Plan>) plan;
            if (agg.getAggPhase() == AggPhase.DISTINCT_GLOBAL) {
                return agg;
            }
        }
        for (Plan child : plan.children()) {
            PhysicalHashAggregate<? extends Plan> found = findFirstDistinctGlobalAgg(child);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    private PhysicalHashJoin<? extends Plan, ? extends Plan> findFirstHashJoin(Plan plan) {
        if (plan instanceof PhysicalHashJoin) {
            return (PhysicalHashJoin<? extends Plan, ? extends Plan>) plan;
        }
        for (Plan child : plan.children()) {
            PhysicalHashJoin<? extends Plan, ? extends Plan> found = findFirstHashJoin(child);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    private DistributionSpecHash getHashSpecFromJoinChild(Plan joinChild) {
        Optional<PhysicalDistribute<Plan>> distOpt = ShuffleKeyPruner.findHashDistributeUnderJoinChild(joinChild);
        Assertions.assertTrue(distOpt.isPresent());
        Assertions.assertInstanceOf(DistributionSpecHash.class, distOpt.get().getDistributionSpec());
        return (DistributionSpecHash) distOpt.get().getDistributionSpec();
    }

    private PhysicalDistribute<PhysicalEmptyRelation> newDistribute() {
        DistributionSpecHash spec = new DistributionSpecHash(
                ImmutableList.of(slotA.getExprId()), DistributionSpecHash.ShuffleType.REQUIRE);
        return new PhysicalDistribute<>(spec, empty);
    }

    @Test
    void testFindHashDistributeUnderJoinChild_directDistribute() {
        PhysicalDistribute<?> dist = newDistribute();
        Optional<PhysicalDistribute<Plan>> r = ShuffleKeyPruner.findHashDistributeUnderJoinChild(dist);
        Assertions.assertTrue(r.isPresent());
        Assertions.assertSame(dist, r.get());
    }

    @Test
    void testFindHashDistributeUnderJoinChild_globalAggOverDistribute() {
        PhysicalDistribute<PhysicalEmptyRelation> dist = newDistribute();
        PhysicalHashAggregate<PhysicalDistribute<PhysicalEmptyRelation>> globalAgg =
                new PhysicalHashAggregate<>(
                        Lists.newArrayList(slotA),
                        Lists.newArrayList(slotA),
                        new AggregateParam(AggPhase.GLOBAL, AggMode.BUFFER_TO_RESULT),
                        true, null, false, dist);
        Optional<PhysicalDistribute<Plan>> r = ShuffleKeyPruner.findHashDistributeUnderJoinChild(globalAgg);
        Assertions.assertTrue(r.isPresent());
        Assertions.assertSame(dist, r.get());
    }

    @Test
    void testFindHashDistributeUnderJoinChild_localAggOverDistribute_empty() {
        PhysicalDistribute<PhysicalEmptyRelation> dist = newDistribute();
        PhysicalHashAggregate<PhysicalDistribute<PhysicalEmptyRelation>> localAgg =
                new PhysicalHashAggregate<>(
                        Lists.newArrayList(slotA),
                        Lists.newArrayList(slotA),
                        new AggregateParam(AggPhase.LOCAL, AggMode.INPUT_TO_BUFFER),
                        true, null, false, dist);
        Assertions.assertFalse(ShuffleKeyPruner.findHashDistributeUnderJoinChild(localAgg).isPresent());
    }

    @Test
    void testReplaceDistributeUnderJoinChild_keepsAggWrapper() {
        PhysicalDistribute<PhysicalEmptyRelation> dist = newDistribute();
        PhysicalHashAggregate<PhysicalDistribute<PhysicalEmptyRelation>> globalAgg =
                new PhysicalHashAggregate<>(
                        Lists.newArrayList(slotA),
                        Lists.newArrayList(slotA),
                        new AggregateParam(AggPhase.GLOBAL, AggMode.BUFFER_TO_RESULT),
                        true, null, false, dist);
        PhysicalDistribute<PhysicalEmptyRelation> newDist = newDistribute();
        Plan out = ShuffleKeyPruner.replaceDistributeUnderJoinChild(globalAgg, newDist);
        Assertions.assertInstanceOf(PhysicalHashAggregate.class, out);
        Assertions.assertSame(newDist, out.child(0));
    }

    @Test
    void testFindHashDistributeUnderJoinChild_projectGlobalAggOverDistribute() {
        PhysicalDistribute<PhysicalEmptyRelation> dist = newDistribute();
        PhysicalHashAggregate<PhysicalDistribute<PhysicalEmptyRelation>> globalAgg =
                new PhysicalHashAggregate<>(
                        Lists.newArrayList(slotA),
                        Lists.newArrayList(slotA),
                        new AggregateParam(AggPhase.GLOBAL, AggMode.BUFFER_TO_RESULT),
                        true, null, false, dist);
        PhysicalProject<PhysicalHashAggregate<PhysicalDistribute<PhysicalEmptyRelation>>> project =
                new PhysicalProject<>(ImmutableList.of(slotA), null, globalAgg);
        Optional<PhysicalDistribute<Plan>> r = ShuffleKeyPruner.findHashDistributeUnderJoinChild(project);
        Assertions.assertTrue(r.isPresent());
        Assertions.assertSame(dist, r.get());
    }

    @Test
    void testFindHashDistributeUnderJoinChild_filterProjectGlobalAggOverDistribute() {
        PhysicalDistribute<PhysicalEmptyRelation> dist = newDistribute();
        PhysicalHashAggregate<PhysicalDistribute<PhysicalEmptyRelation>> globalAgg =
                new PhysicalHashAggregate<>(
                        Lists.newArrayList(slotA),
                        Lists.newArrayList(slotA),
                        new AggregateParam(AggPhase.GLOBAL, AggMode.BUFFER_TO_RESULT),
                        true, null, false, dist);
        PhysicalProject<PhysicalHashAggregate<PhysicalDistribute<PhysicalEmptyRelation>>> project =
                new PhysicalProject<>(ImmutableList.of(slotA), null, globalAgg);
        PhysicalFilter<PhysicalProject<PhysicalHashAggregate<PhysicalDistribute<PhysicalEmptyRelation>>>> filter =
                new PhysicalFilter<>(ImmutableSet.of(), null, project);
        Optional<PhysicalDistribute<Plan>> r = ShuffleKeyPruner.findHashDistributeUnderJoinChild(filter);
        Assertions.assertTrue(r.isPresent());
        Assertions.assertSame(dist, r.get());
    }

    @Test
    void testReplaceDistributeUnderJoinChild_keepsProjectWrapper() {
        PhysicalDistribute<PhysicalEmptyRelation> dist = newDistribute();
        PhysicalHashAggregate<PhysicalDistribute<PhysicalEmptyRelation>> globalAgg =
                new PhysicalHashAggregate<>(
                        Lists.newArrayList(slotA),
                        Lists.newArrayList(slotA),
                        new AggregateParam(AggPhase.GLOBAL, AggMode.BUFFER_TO_RESULT),
                        true, null, false, dist);
        PhysicalProject<PhysicalHashAggregate<PhysicalDistribute<PhysicalEmptyRelation>>> project =
                new PhysicalProject<>(ImmutableList.of(slotA), null, globalAgg);
        PhysicalDistribute<PhysicalEmptyRelation> newDist = newDistribute();
        Plan out = ShuffleKeyPruner.replaceDistributeUnderJoinChild(project, newDist);
        Assertions.assertInstanceOf(PhysicalProject.class, out);
        Assertions.assertSame(newDist, out.child(0).child(0));
    }

    @Test
    void testReplaceDistributeUnderJoinChild_keepsFilterAndProjectWrappers() {
        PhysicalDistribute<PhysicalEmptyRelation> dist = newDistribute();
        PhysicalHashAggregate<PhysicalDistribute<PhysicalEmptyRelation>> globalAgg =
                new PhysicalHashAggregate<>(
                        Lists.newArrayList(slotA),
                        Lists.newArrayList(slotA),
                        new AggregateParam(AggPhase.GLOBAL, AggMode.BUFFER_TO_RESULT),
                        true, null, false, dist);
        PhysicalProject<PhysicalHashAggregate<PhysicalDistribute<PhysicalEmptyRelation>>> project =
                new PhysicalProject<>(ImmutableList.of(slotA), null, globalAgg);
        PhysicalFilter<PhysicalProject<PhysicalHashAggregate<PhysicalDistribute<PhysicalEmptyRelation>>>> filter =
                new PhysicalFilter<>(ImmutableSet.of(), null, project);
        PhysicalDistribute<PhysicalEmptyRelation> newDist = newDistribute();
        Plan out = ShuffleKeyPruner.replaceDistributeUnderJoinChild(filter, newDist);
        Assertions.assertInstanceOf(PhysicalFilter.class, out);
        Assertions.assertInstanceOf(PhysicalProject.class, out.child(0));
        Assertions.assertSame(newDist, out.child(0).child(0).child(0));
    }

    @Test
    void testRecomputePhysicalPropertiesPostProcessorShouldRefreshWrapperDistribution() {
        SlotReference slotB = new SlotReference(new ExprId(1), "b",
                IntegerType.INSTANCE, true, ImmutableList.of());
        PhysicalEmptyRelation relation = new PhysicalEmptyRelation(
                connectContext.getStatementContext().getNextRelationId(),
                ImmutableList.of(slotA, slotB), null);
        DistributionSpecHash childSpec = new DistributionSpecHash(
                ImmutableList.of(slotA.getExprId()), DistributionSpecHash.ShuffleType.REQUIRE);
        DistributionSpecHash staleSpec = new DistributionSpecHash(
                ImmutableList.of(slotB.getExprId()), DistributionSpecHash.ShuffleType.REQUIRE);
        PhysicalDistribute<PhysicalEmptyRelation> distribute = new PhysicalDistribute<>(
                childSpec,
                Optional.empty(),
                relation.getLogicalProperties(),
                PhysicalProperties.createHash(childSpec),
                null,
                relation);
        PhysicalFilter<? extends Plan> wrapper = new PhysicalFilter<>(ImmutableSet.of(), null, distribute)
                .withPhysicalPropertiesAndStats(PhysicalProperties.createHash(staleSpec), null);

        CascadesContext cascadesContext = CascadesContext.initContext(
                new StatementContext(connectContext, new OriginStatement("", 0)),
                wrapper, PhysicalProperties.ANY);
        Plan output = wrapper.accept(RecomputePhysicalPropertiesPostProcessor.INSTANCE, cascadesContext);

        Assertions.assertInstanceOf(PhysicalFilter.class, output);
        DistributionSpec outputSpec = ((PhysicalFilter<?>) output).getPhysicalProperties().getDistributionSpec();
        Assertions.assertInstanceOf(DistributionSpecHash.class, outputSpec);
        Assertions.assertEquals(childSpec.getOrderedShuffledColumns(),
                ((DistributionSpecHash) outputSpec).getOrderedShuffledColumns());
    }
}
