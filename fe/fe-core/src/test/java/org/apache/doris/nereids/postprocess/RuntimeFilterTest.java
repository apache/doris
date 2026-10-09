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

package org.apache.doris.nereids.postprocess;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.datasets.ssb.SSBTestBase;
import org.apache.doris.nereids.datasets.ssb.SSBUtils;
import org.apache.doris.nereids.glue.translator.PhysicalPlanTranslator;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.nereids.hint.DistributeHint;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.processor.post.PlanPostProcessors;
import org.apache.doris.nereids.processor.post.RuntimeFilterContext;
import org.apache.doris.nereids.processor.post.RuntimeFilterGenerator;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.CTEId;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.NullSafeEqual;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.Subtract;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.plans.DistributeType;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCTEProducer;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRepeat;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSetOperation;
import org.apache.doris.nereids.trees.plans.physical.RuntimeFilter;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.planner.RuntimeFilterId;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.statistics.model.ColumnStatistic;
import org.apache.doris.statistics.model.ColumnStatisticBuilder;
import org.apache.doris.thrift.TMinMaxRuntimeFilterType;
import org.apache.doris.thrift.TRuntimeFilterType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class RuntimeFilterTest extends SSBTestBase {

    @Override
    public void runBeforeAll() throws Exception {
        super.runBeforeAll();
        connectContext.getSessionVariable().setRuntimeFilterMode("Global");
        connectContext.getSessionVariable().setRuntimeFilterType(8);
        connectContext.getSessionVariable().setEnableRuntimeFilterPrune(false);
        connectContext.getSessionVariable().expandRuntimeFilterByInnerJoin = false;
        connectContext.getSessionVariable().setDisableJoinReorder(true);
    }

    @Test
    public void testGenerateRuntimeFilter() {
        String sql = "SELECT * FROM lineorder JOIN customer on c_custkey = lo_custkey";
        List<RuntimeFilter> filters = getRuntimeFilters(sql).get();
        Assertions.assertEquals(1, filters.size());
        checkRuntimeFilterExprs(filters, ImmutableList.of(
                Pair.of("c_custkey", "lo_custkey")));
    }

    @Test
    public void testKeepRuntimeFiltersThatCanPruneScanRangesWhenStatsUnknown() {
        boolean oldEnableRuntimeFilterPrune = connectContext.getSessionVariable().isEnableRuntimeFilterPrune();
        boolean oldEnablePartitionPrune =
                connectContext.getSessionVariable().isEnableRuntimeFilterPartitionPrune();
        boolean oldEnableBucketPrune =
                connectContext.getSessionVariable().isEnableRuntimeFilterBucketPrune();
        int oldRuntimeFilterType = connectContext.getSessionVariable().getRuntimeFilterType();
        try {
            connectContext.getSessionVariable().setEnableRuntimeFilterPrune(true);
            connectContext.getSessionVariable().setEnableRuntimeFilterPartitionPrune(true);
            connectContext.getSessionVariable().setEnableRuntimeFilterBucketPrune(true);
            connectContext.getSessionVariable().setRuntimeFilterType(TRuntimeFilterType.IN.getValue());

            // SSB test tables have no analyzed column statistics. Scan-range pruning
            // capability must therefore keep these filters alive on the unknown-stats path.
            List<RuntimeFilter> bucketFilters = getRuntimeFilters(
                    "SELECT lo_orderkey FROM lineorder LEFT SEMI JOIN dates "
                            + "ON lo_orderkey = d_datekey").get();
            Assertions.assertEquals(1, bucketFilters.size());
            Assertions.assertTrue(bucketFilters.get(0).canPruneBuckets());

            List<RuntimeFilter> partitionFilters = getRuntimeFilters(
                    "SELECT lo_orderdate FROM lineorder LEFT SEMI JOIN dates "
                            + "ON lo_orderdate = d_datekey").get();
            Assertions.assertEquals(1, partitionFilters.size());
            Assertions.assertTrue(partitionFilters.get(0).canPrunePartitions());

            List<RuntimeFilter> nonPruningFilters = getRuntimeFilters(
                    "SELECT lo_custkey FROM lineorder LEFT SEMI JOIN dates "
                            + "ON lo_custkey = d_datekey").get();
            Assertions.assertTrue(nonPruningFilters.isEmpty());
        } finally {
            connectContext.getSessionVariable().setEnableRuntimeFilterPrune(oldEnableRuntimeFilterPrune);
            connectContext.getSessionVariable().setEnableRuntimeFilterPartitionPrune(oldEnablePartitionPrune);
            connectContext.getSessionVariable().setEnableRuntimeFilterBucketPrune(oldEnableBucketPrune);
            connectContext.getSessionVariable().setRuntimeFilterType(oldRuntimeFilterType);
        }
    }

    @Test
    public void testKeepScanRangePruningFiltersWhenStatsShowNoRowFilteringBenefit() {
        boolean oldEnableRuntimeFilterPrune = connectContext.getSessionVariable().isEnableRuntimeFilterPrune();
        boolean oldEnablePartitionPrune =
                connectContext.getSessionVariable().isEnableRuntimeFilterPartitionPrune();
        boolean oldEnableBucketPrune =
                connectContext.getSessionVariable().isEnableRuntimeFilterBucketPrune();
        int oldRuntimeFilterType = connectContext.getSessionVariable().getRuntimeFilterType();
        try {
            connectContext.getSessionVariable().setEnableRuntimeFilterPrune(true);
            connectContext.getSessionVariable().setEnableRuntimeFilterPartitionPrune(true);
            connectContext.getSessionVariable().setEnableRuntimeFilterBucketPrune(true);
            connectContext.getSessionVariable().setRuntimeFilterType(TRuntimeFilterType.IN.getValue());

            List<RuntimeFilter> bucketFilters = getRuntimeFiltersWithNonSelectiveJoinStats(
                    "SELECT lo_orderkey FROM lineorder INNER JOIN dates ON lo_orderkey = d_datekey");
            Assertions.assertEquals(1, bucketFilters.size());
            Assertions.assertTrue(bucketFilters.get(0).canPruneBuckets());

            List<RuntimeFilter> partitionFilters = getRuntimeFiltersWithNonSelectiveJoinStats(
                    "SELECT lo_orderdate FROM lineorder INNER JOIN dates ON lo_orderdate = d_datekey");
            Assertions.assertEquals(1, partitionFilters.size());
            Assertions.assertTrue(partitionFilters.get(0).canPrunePartitions());
        } finally {
            connectContext.getSessionVariable().setEnableRuntimeFilterPrune(oldEnableRuntimeFilterPrune);
            connectContext.getSessionVariable().setEnableRuntimeFilterPartitionPrune(oldEnablePartitionPrune);
            connectContext.getSessionVariable().setEnableRuntimeFilterBucketPrune(oldEnableBucketPrune);
            connectContext.getSessionVariable().setRuntimeFilterType(oldRuntimeFilterType);
        }
    }

    @Test
    public void testScanRangePruningDoesNotKeepNonPruningSiblingFilters() {
        boolean oldEnableRuntimeFilterPrune = connectContext.getSessionVariable().isEnableRuntimeFilterPrune();
        boolean oldEnablePartitionPrune =
                connectContext.getSessionVariable().isEnableRuntimeFilterPartitionPrune();
        boolean oldEnableBucketPrune =
                connectContext.getSessionVariable().isEnableRuntimeFilterBucketPrune();
        int oldRuntimeFilterType = connectContext.getSessionVariable().getRuntimeFilterType();
        boolean oldExpandRuntimeFilterByInnerJoin =
                connectContext.getSessionVariable().expandRuntimeFilterByInnerJoin;
        try {
            connectContext.getSessionVariable().setEnableRuntimeFilterPrune(true);
            connectContext.getSessionVariable().setEnableRuntimeFilterPartitionPrune(false);
            connectContext.getSessionVariable().setEnableRuntimeFilterBucketPrune(true);
            connectContext.getSessionVariable().setRuntimeFilterType(
                    TRuntimeFilterType.IN_OR_BLOOM.getValue() | TRuntimeFilterType.MIN_MAX.getValue());
            connectContext.getSessionVariable().expandRuntimeFilterByInnerJoin = true;

            List<RuntimeFilter> supplierFilters = getRuntimeFilters(
                    "SELECT * FROM lineorder JOIN part ON lo_partkey = p_partkey "
                            + "JOIN supplier ON s_suppkey = lo_partkey").get().stream()
                    .filter(filter -> filter.getSrcExpr().toSql().equals("s_suppkey"))
                    .collect(Collectors.toList());

            Assertions.assertEquals(1, supplierFilters.size());
            Assertions.assertEquals(TRuntimeFilterType.IN_OR_BLOOM, supplierFilters.get(0).getType());
            Assertions.assertEquals("p_partkey", supplierFilters.get(0).getTargetSlot().getName());
            Assertions.assertTrue(supplierFilters.get(0).canPruneBuckets());
        } finally {
            connectContext.getSessionVariable().setEnableRuntimeFilterPrune(oldEnableRuntimeFilterPrune);
            connectContext.getSessionVariable().setEnableRuntimeFilterPartitionPrune(oldEnablePartitionPrune);
            connectContext.getSessionVariable().setEnableRuntimeFilterBucketPrune(oldEnableBucketPrune);
            connectContext.getSessionVariable().setRuntimeFilterType(oldRuntimeFilterType);
            connectContext.getSessionVariable().expandRuntimeFilterByInnerJoin = oldExpandRuntimeFilterByInnerJoin;
        }
    }

    @Test
    public void testGenerateRuntimeFilterByIllegalSrcExpr() {
        String sql = "SELECT * FROM lineorder JOIN customer on c_custkey = c_custkey";
        List<RuntimeFilter> filters = getRuntimeFilters(sql).get();
        Assertions.assertEquals(0, filters.size());
    }

    @Test
    public void testComplexExpressionToRuntimeFilter() {
        String sql
                = "SELECT * FROM supplier JOIN customer on c_name = s_name and s_city = c_city and s_nation = c_nation";
        List<RuntimeFilter> filters = getRuntimeFilters(sql).get();
        Assertions.assertEquals(3, filters.size());
        checkRuntimeFilterExprs(filters, ImmutableList.of(
                Pair.of("c_name", "s_name"),
                Pair.of("c_city", "s_city"),
                Pair.of("c_nation", "s_nation")));
    }

    @Test
    public void testNestedJoinGenerateRuntimeFilter() {
        String sql = SSBUtils.Q4_4;
        List<RuntimeFilter> filters = getRuntimeFilters(sql).get();
        Assertions.assertEquals(4, filters.size());
        checkRuntimeFilterExprs(filters, ImmutableList.of(
                Pair.of("p_partkey", "lo_partkey"), Pair.of("s_suppkey", "lo_suppkey"),
                Pair.of("c_custkey", "lo_custkey"), Pair.of("d_datekey", "lo_orderdate")));
    }

    @Test
    public void testSubTreeInUnsupportedJoinType() {
        String sql = "select c_custkey"
                + " from (select lo_custkey from lineorder inner join dates on lo_orderdate = d_datekey) a"
                + " left outer join (select c_custkey from customer inner join supplier on c_custkey = s_suppkey) b"
                + " on b.c_custkey = a.lo_custkey";
        List<RuntimeFilter> filters = getRuntimeFilters(sql).get();
        Assertions.assertEquals(2, filters.size());
        checkRuntimeFilterExprs(filters, ImmutableList.of(
                Pair.of("d_datekey", "lo_orderdate"), Pair.of("s_suppkey", "c_custkey")));
    }

    @Test
    public void testPushDownEncounterUnsupportedJoinType() {
        String sql = "select c_custkey"
                + " from (select lo_custkey from lineorder left outer join dates on lo_orderdate = d_datekey) a"
                + " inner join (select c_custkey from customer inner join supplier on c_custkey = s_suppkey) b"
                + " on b.c_custkey = a.lo_custkey";
        List<RuntimeFilter> filters = getRuntimeFilters(sql).get();
        Assertions.assertEquals(2, filters.size());
        checkRuntimeFilterExprs(filters, ImmutableList.of(
                Pair.of("s_suppkey", "c_custkey"), Pair.of("c_custkey", "lo_custkey")));
    }

    @Test
    public void testDoNotPushDownNonNullPropagatingRuntimeFilterThroughOuterJoin() {
        String sql = "select * from lineorder left outer join customer on lo_custkey = c_custkey"
                + " inner join supplier on coalesce(c_custkey, 0) = s_suppkey";
        List<RuntimeFilter> filters = getRuntimeFilters(sql).get();
        Assertions.assertEquals(0, filters.size());
    }

    @Test
    public void testPushDownNullPropagatingRuntimeFilterThroughOuterJoin() {
        String sql = "select * from lineorder left outer join customer on lo_custkey = c_custkey"
                + " inner join supplier on c_custkey = s_suppkey";
        List<RuntimeFilter> filters = getRuntimeFilters(sql).get();
        Assertions.assertEquals(2, filters.size());
        checkRuntimeFilterExprs(filters, ImmutableList.of(
                Pair.of("c_custkey", "lo_custkey"),
                Pair.of("s_suppkey", "c_custkey")));
    }

    @Test
    public void testPushDownThroughAggNode() {
        String sql = "select profit"
                + " from (select lo_custkey, sum(lo_revenue - lo_supplycost) as profit from lineorder inner join dates"
                + " on lo_orderdate = d_datekey group by lo_custkey) a"
                + " inner join (select c_custkey from customer inner join supplier on c_custkey = s_suppkey) b"
                + " on b.c_custkey = a.lo_custkey";
        List<RuntimeFilter> filters = getRuntimeFilters(sql).get();
        Assertions.assertEquals(3, filters.size());
        checkRuntimeFilterExprs(filters, ImmutableList.of(
                Pair.of("c_custkey", "lo_custkey"), Pair.of("d_datekey", "lo_orderdate"),
                Pair.of("s_suppkey", "c_custkey")));
    }

    @Test
    public void testDoNotPushDownThroughAggFunction() {
        String sql = "select profit"
                + " from (select sum(c_custkey) c_custkey from customer inner join supplier"
                + " on c_custkey = s_suppkey group by s_suppkey) a"
                + " inner join (select lo_custkey, sum(lo_revenue - lo_supplycost) as profit from lineorder"
                + " inner join dates on lo_orderdate = d_datekey group by lo_custkey) b"
                + " on a.c_custkey = b.lo_custkey";
        List<RuntimeFilter> filters = getRuntimeFilters(sql).get();
        Assertions.assertEquals(2, filters.size());
        checkRuntimeFilterExprs(filters, ImmutableList.of(
                Pair.of("d_datekey", "lo_orderdate"), Pair.of("s_suppkey", "c_custkey")));
    }

    @Test
    public void testCrossJoin() {
        String sql = "select c_custkey, lo_custkey from lineorder, customer where lo_custkey = c_custkey";
        List<RuntimeFilter> filters = getRuntimeFilters(sql).get();
        Assertions.assertEquals(1, filters.size());
        checkRuntimeFilterExprs(filters, ImmutableList.of(
                Pair.of("c_custkey", "lo_custkey")));
    }

    @Test
    public void testSubQueryAlias() {
        String sql = "select c_custkey, lo_custkey from lineorder l, customer c where c.c_custkey = l.lo_custkey";
        List<RuntimeFilter> filters = getRuntimeFilters(sql).get();
        Assertions.assertEquals(1, filters.size());
        checkRuntimeFilterExprs(filters, ImmutableList.of(
                Pair.of("c_custkey", "lo_custkey")));
    }

    @Test
    public void testView() throws Exception {
        createView("create view if not exists v1 as \n"
                + "        select * \n"
                + "        from customer");
        createView("create view if not exists v2 as\n"
                + "        select *\n"
                + "        from lineorder");
        createView("create view if not exists v3 as \n"
                + "        select *\n"
                + "        from v1 join (\n"
                + "            select *\n"
                + "            from v2\n"
                + "            ) t \n"
                + "        on v1.c_custkey = t.lo_custkey");
        String sql = "select * from (\n"
                + "            select * \n"
                + "            from part p \n"
                + "            join v2 on p.p_partkey = v2.lo_partkey) t1 \n"
                + "        join (\n"
                + "            select * \n"
                + "            from supplier s \n"
                + "            join v3 on s.s_region = v3.c_region) t2 \n"
                + "        on t1.p_partkey = t2.lo_partkey\n"
                + "        order by t1.lo_custkey, t1.p_partkey, t2.s_suppkey, t2.c_custkey, t2.lo_orderkey";
        List<RuntimeFilter> filters = getRuntimeFilters(sql).get();
        Assertions.assertEquals(4, filters.size());
        checkRuntimeFilterExprs(filters, ImmutableList.of(
                Pair.of("lo_partkey", "p_partkey"), Pair.of("lo_partkey", "p_partkey"),
                Pair.of("c_region", "s_region"), Pair.of("lo_custkey", "c_custkey")));
    }

    @Test
    public void testPushDownThroughJoin() {
        String sql = "select c_custkey from (select c_custkey from (select lo_custkey from lineorder inner join dates"
                + " on lo_orderdate = d_datekey) a"
                + " inner join (select c_custkey from customer inner join supplier on c_custkey = s_suppkey) b"
                + " on b.c_custkey = a.lo_custkey) c inner join (select lo_custkey from customer inner join lineorder"
                + " on c_custkey = lo_custkey) d on c.c_custkey = d.lo_custkey";
        List<RuntimeFilter> filters = getRuntimeFilters(sql).get();
        Assertions.assertEquals(5, filters.size());
        checkRuntimeFilterExprs(filters, ImmutableList.of(
                Pair.of("lo_custkey", "c_custkey"), Pair.of("c_custkey", "lo_custkey"),
                Pair.of("d_datekey", "lo_orderdate"), Pair.of("s_suppkey", "c_custkey"),
                Pair.of("lo_custkey", "c_custkey")));

        connectContext.getSessionVariable().expandRuntimeFilterByInnerJoin = true;
        filters = getRuntimeFilters(sql).get();
        // V2-style: expanded multi-target RFs become separate single-target RFs
        // Original 5 RFs, one multi-target RF (3 targets) becomes 3 separate RFs → 5 + 2 = 7
        Assertions.assertEquals(7, filters.size());
        connectContext.getSessionVariable().expandRuntimeFilterByInnerJoin = false;

    }

    @Test
    public void testPushDownThroughUnsupportedJoinType() {
        String sql = "select c_custkey from (select c_custkey from (select lo_custkey from lineorder inner join dates"
                + " on lo_orderdate = d_datekey) a"
                + " inner join (select c_custkey from customer left outer join supplier on c_custkey = s_suppkey) b"
                + " on b.c_custkey = a.lo_custkey) c inner join (select lo_custkey from customer inner join lineorder"
                + " on c_custkey = lo_custkey) d on c.c_custkey = d.lo_custkey";
        List<RuntimeFilter> filters = getRuntimeFilters(sql).get();
        Assertions.assertEquals(4, filters.size());

        connectContext.getSessionVariable().expandRuntimeFilterByInnerJoin = true;
        filters = getRuntimeFilters(sql).get();
        // Expansion through inner joins creates additional RFs
        Assertions.assertEquals(5, filters.size());
        connectContext.getSessionVariable().expandRuntimeFilterByInnerJoin = false;

    }

    @Test
    public void testAliasCastAtLeftAndExpressionAtRight() {
        String sql = "select c_custkey from (select cast(lo_custkey as bigint) c from lineorder) a"
                + " inner join customer b on a.c = b.c_custkey + 5";
        List<RuntimeFilter> filters = getRuntimeFilters(sql).get();
        Assertions.assertEquals(1, filters.size());
        checkRuntimeFilterExprs(filters, ImmutableList.of(
                Pair.of("expr_(cast(c_custkey as BIGINT) + 5)", "lo_custkey")));
    }

    @Test
    public void testCastAtOnExpression() {
        String sql = "select * from part p, supplier s where p.p_name = s.s_name";
        List<RuntimeFilter> filters = getRuntimeFilters(sql).get();
        Assertions.assertEquals(1, filters.size());
        checkRuntimeFilterExprs(filters, ImmutableList.of(
                Pair.of("s_name", "p_name")));
    }

    @Test
    public void testExpandRfByInnerJoin() {
        String sql = "select * "
                + "from lineorder join part on lo_partkey=p_partkey "
                + "join supplier on s_suppkey=lo_partkey";
        List<RuntimeFilter> filters = getRuntimeFilters(sql).get();
        Assertions.assertEquals(2, filters.size());
        checkRuntimeFilterExprs(filters, ImmutableList.of(
                Pair.of("s_suppkey", "lo_partkey"),
                Pair.of("p_partkey", "lo_partkey")));
        connectContext.getSessionVariable().enableRuntimeFilterPrune = false;
        connectContext.getSessionVariable().expandRuntimeFilterByInnerJoin = true;
        filters = getRuntimeFilters(sql).get();
        // V2-style: expansion creates separate RFs instead of multi-target RF
        // s_suppkey→lo_partkey (original), s_suppkey→p_partkey (expanded), p_partkey→lo_partkey
        Assertions.assertEquals(3, filters.size());
        checkRuntimeFilterExprs(filters, ImmutableList.of(
                Pair.of("s_suppkey", "lo_partkey"),
                Pair.of("s_suppkey", "p_partkey"),
                Pair.of("p_partkey", "lo_partkey")));
        connectContext.getSessionVariable().expandRuntimeFilterByInnerJoin = false;

    }

    private List<RuntimeFilter> getRuntimeFiltersWithNonSelectiveJoinStats(String sql) {
        PlanChecker checker = PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .optimize();
        PhysicalPlan plan = checker.getBestPlanTree();
        PhysicalHashJoin<? extends Plan, ? extends Plan> join = findFirstHashJoin(plan);
        Assertions.assertNotNull(join);

        EqualTo equalTo = (EqualTo) join.getEqualToConjuncts().get(0);
        Slot probeSlot = equalTo.child(0).getInputSlots().iterator().next();
        Slot buildSlot = equalTo.child(1).getInputSlots().iterator().next();
        ColumnStatistic nonSelectiveStats = new ColumnStatisticBuilder(1)
                .setNdv(1)
                .setAvgSizeByte(4)
                .setNumNulls(0)
                .setMinValue(1)
                .setMaxValue(1)
                .build();
        join.left().getStats().addColumnStats(probeSlot, nonSelectiveStats);
        join.right().getStats().addColumnStats(buildSlot, nonSelectiveStats);

        plan = new PlanPostProcessors(checker.getCascadesContext()).process(plan);
        RuntimeFilterContext runtimeFilterContext = checker.getCascadesContext().getRuntimeFilterContext();
        new PhysicalPlanTranslator(new PlanTranslatorContext(checker.getCascadesContext())).translatePlan(plan);
        return runtimeFilterContext.getNereidsRuntimeFilter();
    }

    private PhysicalHashJoin<? extends Plan, ? extends Plan> findFirstHashJoin(Plan plan) {
        if (plan instanceof PhysicalHashJoin) {
            return (PhysicalHashJoin<? extends Plan, ? extends Plan>) plan;
        }
        for (Plan child : plan.children()) {
            PhysicalHashJoin<? extends Plan, ? extends Plan> join = findFirstHashJoin(child);
            if (join != null) {
                return join;
            }
        }
        return null;
    }

    private RuntimeFilterContext getRuntimeFilterContext(String sql) {
        return getRuntimeFilterContext(sql, context -> {
        });
    }

    private RuntimeFilterContext getRuntimeFilterContext(String sql, Consumer<RuntimeFilterContext> beforeTranslate) {
        PlanChecker checker = PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .optimize();
        PhysicalPlan plan = checker.getBestPlanTree();
        plan = new PlanPostProcessors(checker.getCascadesContext()).process(plan);
        RuntimeFilterContext context = checker.getCascadesContext().getRuntimeFilterContext();
        beforeTranslate.accept(context);
        new PhysicalPlanTranslator(new PlanTranslatorContext(checker.getCascadesContext())).translatePlan(plan);
        List<RuntimeFilter> filters = context.getNereidsRuntimeFilter();
        Assertions.assertTrue(filters.size() >= context.getLegacyFilters().size() + context.getTargetNullCount(),
                "nereidsRF count (" + filters.size() + ") should be >= legacyRF count ("
                        + context.getLegacyFilters().size() + ") + nullTargets ("
                        + context.getTargetNullCount() + ")");
        return context;
    }

    private Optional<List<RuntimeFilter>> getRuntimeFilters(String sql) {
        return Optional.of(getRuntimeFilterContext(sql).getNereidsRuntimeFilter());
    }

    private void checkRuntimeFilterExprs(List<RuntimeFilter> filters, List<Pair<String, String>> colNames) {
        Assertions.assertEquals(filters.size(), colNames.size());
        for (RuntimeFilter filter : filters) {
            Assertions.assertTrue(colNames.contains(Pair.of(
                    filter.getSrcExpr().toSql(),
                    filter.getTargetSlot().getName())));
        }
    }

    @Test
    public void testRuntimeFilterBlockByWindow() {
        String sql = "SELECT * FROM (select rank() over(partition by lo_partkey), lo_custkey from lineorder) t JOIN customer on lo_custkey = c_custkey";
        List<RuntimeFilter> filters = getRuntimeFilters(sql).get();
        Assertions.assertEquals(0, filters.size());
    }

    @Test
    public void testRuntimeFilterNotBlockByWindow() {
        String sql = "SELECT * FROM (select rank() over(partition by lo_custkey), lo_custkey from lineorder) t JOIN customer on lo_custkey = c_custkey";
        List<RuntimeFilter> filters = getRuntimeFilters(sql).get();
        Assertions.assertEquals(1, filters.size());
    }

    @Test
    public void testRuntimeFilterBlockByTopN() {
        String sql = "SELECT * FROM (select lo_custkey from lineorder order by lo_custkey limit 10) t JOIN customer on lo_custkey = c_custkey";
        List<RuntimeFilter> filters = getRuntimeFilters(sql).get();
        Assertions.assertEquals(0, filters.size());
    }

    @Test
    public void testRuntimeFilterShapeInfoWithoutBrackets() {
        String sql = "SELECT * FROM lineorder JOIN customer ON lo_custkey = c_custkey";
        List<RuntimeFilter> filters = getRuntimeFilters(sql).get();
        Assertions.assertEquals(1, filters.size());
        Assertions.assertFalse(filters.get(0).shapeInfo().contains("->["));
        Assertions.assertTrue(filters.get(0).shapeInfo().contains("c_custkey->lo_custkey"));
    }

    @Test
    public void testRuntimeFilterBlockByGroupingSetsPartialColumn() {
        // RF on lo_custkey should be blocked because lo_custkey is NOT in all grouping sets.
        // grouping sets ((lo_partkey), (lo_custkey, lo_partkey)) — first set lacks lo_custkey.
        // Subquery must be on LEFT (probe) side so the RF pushes through Repeat.
        String sql = "SELECT lo_custkey FROM ("
                + "  SELECT lo_custkey, lo_partkey FROM lineorder"
                + "  GROUP BY GROUPING SETS ((lo_partkey), (lo_custkey, lo_partkey))"
                + ") t INNER JOIN customer ON t.lo_custkey = c_custkey";
        List<RuntimeFilter> filters = getRuntimeFilters(sql).get();
        // No RF should push to lineorder scan because lo_custkey is not in all grouping sets
        Assertions.assertEquals(0, filters.size(),
                "RF should be blocked when probe slot is not in all grouping sets");
    }

    @Test
    public void testRuntimeFilterPushThroughGroupingSetsCommonColumn() {
        // RF on lo_partkey should push through because lo_partkey IS in all grouping sets.
        // grouping sets ((lo_partkey), (lo_custkey, lo_partkey)) — lo_partkey is common.
        // Subquery on LEFT (probe) side so RF pushes through Repeat.
        String sql = "SELECT lo_partkey FROM ("
                + "  SELECT lo_custkey, lo_partkey FROM lineorder"
                + "  GROUP BY GROUPING SETS ((lo_partkey), (lo_custkey, lo_partkey))"
                + ") t INNER JOIN part ON t.lo_partkey = p_partkey";
        List<RuntimeFilter> filters = getRuntimeFilters(sql).get();
        Assertions.assertEquals(1, filters.size(),
                "RF should push through when probe slot is in all grouping sets");
    }

    @Test
    public void testSetOperationRuntimeFilterBlockByGroupingSetsPartialColumn() {
        // SetOp RF should also block pushdown through Repeat when the target slot
        // is not present in all grouping sets.
        String sql = "SELECT c_custkey FROM customer INTERSECT SELECT lo_custkey FROM ("
                + "  SELECT lo_custkey, lo_partkey FROM lineorder"
                + "  GROUP BY GROUPING SETS ((lo_partkey), (lo_custkey, lo_partkey))"
                + ") t";
        List<RuntimeFilter> filters = getRuntimeFilters(sql).get();
        Assertions.assertEquals(0, filters.size(),
                "SetOp RF should be blocked when probe slot is not in all grouping sets");
    }

    @Test
    public void testSetOperationRuntimeFilterPushThroughGroupingSetsCommonColumn() {
        // SetOp RF should still push through Repeat for a slot that is common to all grouping sets.
        String sql = "SELECT p_partkey FROM part INTERSECT SELECT lo_partkey FROM ("
                + "  SELECT lo_custkey, lo_partkey FROM lineorder"
                + "  GROUP BY GROUPING SETS ((lo_partkey), (lo_custkey, lo_partkey))"
                + ") t";
        List<RuntimeFilter> filters = getRuntimeFilters(sql).get();
        Assertions.assertEquals(1, filters.size(),
                "SetOp RF should push through when probe slot is in all grouping sets");
    }

    @Test
    public void testSetOperationRuntimeFilterExpandThroughInnerJoin() {
        String sql = "SELECT s_suppkey FROM supplier INTERSECT "
                + "SELECT lo_suppkey FROM lineorder INNER JOIN supplier s2 ON lo_suppkey = s2.s_suppkey";
        List<RuntimeFilter> filters = getRuntimeFilters(sql).get().stream()
                .filter(rf -> rf.getBuilderNode() instanceof PhysicalSetOperation)
                .collect(Collectors.toList());
        Assertions.assertEquals(1, filters.size());
        checkRuntimeFilterExprs(filters, ImmutableList.of(
                Pair.of("s_suppkey", "lo_suppkey")));

        connectContext.getSessionVariable().expandRuntimeFilterByInnerJoin = true;
        filters = getRuntimeFilters(sql).get().stream()
                .filter(rf -> rf.getBuilderNode() instanceof PhysicalSetOperation)
                .collect(Collectors.toList());
        Assertions.assertEquals(2, filters.size());
        checkRuntimeFilterExprs(filters, ImmutableList.of(
                Pair.of("s_suppkey", "lo_suppkey"),
                Pair.of("s_suppkey", "s_suppkey")));
        connectContext.getSessionVariable().expandRuntimeFilterByInnerJoin = false;
    }

    @Test
    public void testNotGenerateRfOnDanglingSlot() {
        String sql = "select lo_custkey from lineorder union all select c_custkey from customer union all select p_partkey from part;";
        PlanChecker checker = PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .implement();
        PhysicalPlan plan = checker.getPhysicalPlan();

        /* construct plan for
         join (#18=p_partkey)
            -->join()
               -->project(null as #18, ...)
                  -->lineorder
               -->project(c_custkey#17)
                  -->customer(output: c_custkey#17, c_name#18, ...)
            -->project(p_partkey#25)
               -->part

         test purpose:
         do not generate RF by "#18=p_partkey" and apply this rf on customer
         */
        PhysicalProject<Plan> projectCustomer = (PhysicalProject<Plan>) plan.child(0).child(1);
        SlotReference cCustkey = (SlotReference) projectCustomer.getProjects().get(0);
        PhysicalProject<Plan> projectPart = (PhysicalProject<Plan>) plan.child(0).child(2);
        SlotReference pPartkey = (SlotReference) projectPart.getProjects().get(0);

        PhysicalOlapScan lo = (PhysicalOlapScan) plan.child(0).child(0).child(0);
        SlotReference loCustkey = (SlotReference) lo.getBaseOutputs().get(2);
        SlotReference loPartkey = (SlotReference) lo.getBaseOutputs().get(3);
        Alias nullAlias = new Alias(new ExprId(18), new NullLiteral(), ""); // expr#18 is used by c_name
        List<NamedExpression> projList = new ArrayList<>();
        projList.add(loCustkey);
        projList.add(loPartkey);
        projList.add(nullAlias);
        PhysicalProject projLo = new PhysicalProject(projList, null, lo);

        PhysicalHashJoin joinLoC = new PhysicalHashJoin(JoinType.INNER_JOIN,
                ImmutableList.of(new EqualTo(loCustkey, cCustkey)),
                ImmutableList.of(),
                new DistributeHint(DistributeType.NONE),
                Optional.empty(),
                null,
                projLo,
                projectCustomer
                );
        PhysicalHashJoin joinLoCP = new PhysicalHashJoin(JoinType.INNER_JOIN,
                ImmutableList.of(new EqualTo(nullAlias.toSlot(), pPartkey)),
                ImmutableList.of(),
                new DistributeHint(DistributeType.NONE),
                Optional.empty(),
                null,
                joinLoC,
                projectPart
                );
        checker.getCascadesContext().getConnectContext().getSessionVariable().enableRuntimeFilterPrune = false;
        plan = new PlanPostProcessors(checker.getCascadesContext()).process(joinLoCP);
        System.out.println(plan.treeString());
        Assertions.assertEquals(0, ((AbstractPhysicalPlan) plan.child(0).child(1).child(0))
                .getAppliedRuntimeFilters().size());
    }

    @Test
    public void testFunctionExprRejectedOnNonNumericType() {
        // substring() on varchar columns should NOT generate RF pushed to scan
        String sql = "SELECT * FROM supplier JOIN customer"
                + " on substring(s_name, 1, 2) = substring(c_name, 1, 2)";
        List<RuntimeFilter> filters = getRuntimeFilters(sql).get();
        Assertions.assertEquals(0, filters.size(),
                "substring() on varchar should not generate scan-level RF");
    }

    @Test
    public void testFunctionExprAllowedOnNumericType() {
        // abs() on numeric columns should still generate RF
        String sql = "SELECT * FROM lineorder JOIN customer"
                + " on abs(lo_custkey) = c_custkey";
        List<RuntimeFilter> filters = getRuntimeFilters(sql).get();
        Assertions.assertEquals(1, filters.size(),
                "abs() on numeric type should generate scan-level RF");
    }

    @Test
    public void testExpandRfCreatesSeparateRfsPerTarget() {
        // V2-style: expansion creates separate RF objects per target.
        // Query: supplier join (lineorder join part on lo_partkey=p_partkey) on s_suppkey=lo_partkey
        // Without expand: RF(s_suppkey → lo_partkey), RF(p_partkey → lo_partkey) = 2 RFs
        // With expand:    RF(s_suppkey → lo_partkey), RF(s_suppkey → p_partkey), RF(p_partkey → lo_partkey) = 3 RFs
        String sql = "select * "
                + "from lineorder join part on lo_partkey=p_partkey "
                + "join supplier on s_suppkey=lo_partkey";

        // Without expand: 2 RFs, each with 1 target
        connectContext.getSessionVariable().enableRuntimeFilterPrune = false;
        connectContext.getSessionVariable().expandRuntimeFilterByInnerJoin = false;
        List<RuntimeFilter> filters = getRuntimeFilters(sql).get();
        Assertions.assertEquals(2, filters.size(), "without expand: should have 2 RFs");

        // With expand: 3 separate RFs, each with 1 target
        connectContext.getSessionVariable().expandRuntimeFilterByInnerJoin = true;
        filters = getRuntimeFilters(sql).get();
        Assertions.assertEquals(3, filters.size(),
                "with expand: should have 3 separate RFs (V2-style, one per target)");
        // Verify the specific RF (src, target) pairs
        checkRuntimeFilterExprs(filters, ImmutableList.of(
                Pair.of("s_suppkey", "lo_partkey"),
                Pair.of("s_suppkey", "p_partkey"),
                Pair.of("p_partkey", "lo_partkey")));

        connectContext.getSessionVariable().expandRuntimeFilterByInnerJoin = false;
    }

    @Test
    public void testLegacyRuntimeFilterKeepsSeparateMinAndMaxForSameSource() {
        int oldType = connectContext.getSessionVariable().getRuntimeFilterType();
        connectContext.getSessionVariable().setRuntimeFilterType(TRuntimeFilterType.MIN_MAX.getValue());
        try {
            String sql = "select * from lineorder a join supplier b"
                    + " on a.lo_partkey < b.s_suppkey and a.lo_suppkey > b.s_suppkey";
            RuntimeFilterContext context = getRuntimeFilterContext(sql);
            List<String> legacyTypes = context.getLegacyFilters().stream()
                    .map(org.apache.doris.planner.RuntimeFilter::getTypeDesc)
                    .sorted()
                    .collect(Collectors.toList());
            Assertions.assertEquals(2, legacyTypes.size());
            Assertions.assertEquals(ImmutableList.of("max", "min"), legacyTypes);
        } finally {
            connectContext.getSessionVariable().setRuntimeFilterType(oldType);
        }
    }

    @Test
    public void testIgnoredRuntimeFilterIdDoesNotDropGroupedLegacyFilter() {
        String oldIgnoredIds = connectContext.getSessionVariable().ignoreRuntimeFilterIds;
        connectContext.getSessionVariable().expandRuntimeFilterByInnerJoin = true;
        try {
            String sql = "select * "
                    + "from lineorder join part on lo_partkey=p_partkey "
                    + "join supplier on s_suppkey=lo_partkey";
            RuntimeFilterContext context = getRuntimeFilterContext(sql, rfContext -> {
                List<RuntimeFilter> groupedFilters = rfContext.getNereidsRuntimeFilter().stream()
                        .filter(rf -> rf.getSrcExpr().toSql().equals("s_suppkey"))
                        .collect(Collectors.toList());
                Assertions.assertEquals(2, groupedFilters.size());
                connectContext.getSessionVariable().setIgnoreRuntimeFilterIds(
                        String.valueOf(groupedFilters.get(0).getId().asInt()));
            });
            Assertions.assertEquals(2, context.getLegacyFilters().size());
            Assertions.assertTrue(context.getLegacyFilters().stream()
                    .map(rf -> rf.getSrcExpr().toString())
                    .anyMatch("s_suppkey"::equals));
        } finally {
            connectContext.getSessionVariable().setIgnoreRuntimeFilterIds(oldIgnoredIds);
            connectContext.getSessionVariable().expandRuntimeFilterByInnerJoin = false;
        }
    }

    @Test
    public void testPushSharedCteRuntimeFiltersWhichEveryConsumerApplies() {
        CTEId cteId = new CTEId(1);
        SlotReference src = new SlotReference("src", IntegerType.INSTANCE);
        SlotReference producerPk = new SlotReference("pk", IntegerType.INSTANCE);
        SlotReference consumerPk1 = new SlotReference("pk", IntegerType.INSTANCE);
        SlotReference consumerPk2 = new SlotReference("pk", IntegerType.INSTANCE);
        PhysicalCTEConsumer consumer1 = newCteConsumer(cteId, consumerPk1, producerPk);
        PhysicalCTEConsumer consumer2 = newCteConsumer(cteId, consumerPk2, producerPk);
        Set<PhysicalCTEConsumer> consumers = ImmutableSet.of(consumer1, consumer2);

        // Both consumers apply the same filter: it can be applied once on the producer.
        List<RuntimeFilter> sameFilter = ImmutableList.of(
                newCteConsumerRuntimeFilter(consumer1, src, consumerPk1, consumerPk1),
                newCteConsumerRuntimeFilter(consumer2, src, consumerPk2, consumerPk2));
        Assertions.assertEquals(1, RuntimeFilterGenerator.selectPushableRuntimeFilters(
                sameFilter, consumers, cteId).size());

        // Both consumers apply the same pair of filters, of two different types: each filter is applied
        // by every consumer, so both are still pushed, each as its own group.
        List<RuntimeFilter> sameFilterPair = ImmutableList.of(
                newCteConsumerRuntimeFilter(consumer1, src, consumerPk1, consumerPk1,
                        TRuntimeFilterType.MIN_MAX, TMinMaxRuntimeFilterType.MIN_MAX),
                newCteConsumerRuntimeFilter(consumer1, src, consumerPk1, consumerPk1,
                        TRuntimeFilterType.IN_OR_BLOOM, TMinMaxRuntimeFilterType.MIN_MAX),
                newCteConsumerRuntimeFilter(consumer2, src, consumerPk2, consumerPk2,
                        TRuntimeFilterType.MIN_MAX, TMinMaxRuntimeFilterType.MIN_MAX),
                newCteConsumerRuntimeFilter(consumer2, src, consumerPk2, consumerPk2,
                        TRuntimeFilterType.IN_OR_BLOOM, TMinMaxRuntimeFilterType.MIN_MAX));
        Assertions.assertEquals(2, RuntimeFilterGenerator.selectPushableRuntimeFilters(
                sameFilterPair, consumers, cteId).size());

        // Only the filter that both consumers apply is pushed.
        List<RuntimeFilter> partiallySharedFilter = ImmutableList.of(
                newCteConsumerRuntimeFilter(consumer1, src, consumerPk1, consumerPk1,
                        TRuntimeFilterType.MIN_MAX, TMinMaxRuntimeFilterType.MIN_MAX),
                newCteConsumerRuntimeFilter(consumer1, src, consumerPk1, consumerPk1,
                        TRuntimeFilterType.IN_OR_BLOOM, TMinMaxRuntimeFilterType.MIN_MAX),
                newCteConsumerRuntimeFilter(consumer2, src, consumerPk2, consumerPk2,
                        TRuntimeFilterType.IN_OR_BLOOM, TMinMaxRuntimeFilterType.MIN_MAX));
        List<List<RuntimeFilter>> pushable = RuntimeFilterGenerator.selectPushableRuntimeFilters(
                partiallySharedFilter, consumers, cteId);
        Assertions.assertEquals(1, pushable.size());
        Assertions.assertEquals(2, pushable.get(0).size());

        // The filters target different expressions on the producer, so they are not the same filter and
        // neither of them is applied by both consumers.
        List<RuntimeFilter> differentTargets = ImmutableList.of(
                newCteConsumerRuntimeFilter(consumer1, src, consumerPk1,
                        new Add(consumerPk1, new IntegerLiteral(6))),
                newCteConsumerRuntimeFilter(consumer2, src, consumerPk2,
                        new Subtract(consumerPk2, new IntegerLiteral(1))));
        Assertions.assertTrue(RuntimeFilterGenerator.selectPushableRuntimeFilters(
                differentTargets, consumers, cteId).isEmpty());
    }

    @Test
    public void testDoNotPushSharedCteRuntimeFiltersWhichOtherConsumersDoNotApply() {
        CTEId cteId = new CTEId(1);
        SlotReference src = new SlotReference("src", IntegerType.INSTANCE);
        SlotReference producerK = new SlotReference("k", IntegerType.INSTANCE);
        SlotReference consumerK1 = new SlotReference("k", IntegerType.INSTANCE);
        SlotReference consumerK2 = new SlotReference("k", IntegerType.INSTANCE);
        PhysicalCTEConsumer consumer1 = newCteConsumer(cteId, consumerK1, producerK);
        PhysicalCTEConsumer consumer2 = newCteConsumer(cteId, consumerK2, producerK);
        Set<PhysicalCTEConsumer> consumers = ImmutableSet.of(consumer1, consumer2);

        // `t c1 where c1.k > b.x` produces a MIN filter, `t c2 where c2.k < b.x` produces a MAX filter.
        // They target the same producer column and share the same source expression, but each of them is
        // applied by one consumer only: pushing them into the shared producer would prune the rows that
        // the other consumer still needs.
        List<RuntimeFilter> oppositeMinMaxFilters = ImmutableList.of(
                newCteConsumerRuntimeFilter(consumer1, src, consumerK1, consumerK1,
                        TRuntimeFilterType.MIN_MAX, TMinMaxRuntimeFilterType.MIN),
                newCteConsumerRuntimeFilter(consumer2, src, consumerK2, consumerK2,
                        TRuntimeFilterType.MIN_MAX, TMinMaxRuntimeFilterType.MAX));
        Assertions.assertTrue(RuntimeFilterGenerator.selectPushableRuntimeFilters(
                oppositeMinMaxFilters, consumers, cteId).isEmpty());

        // Same direction: both consumers apply the same filter, so it is pushed.
        List<RuntimeFilter> sameDirectionFilters = ImmutableList.of(
                newCteConsumerRuntimeFilter(consumer1, src, consumerK1, consumerK1,
                        TRuntimeFilterType.MIN_MAX, TMinMaxRuntimeFilterType.MIN),
                newCteConsumerRuntimeFilter(consumer2, src, consumerK2, consumerK2,
                        TRuntimeFilterType.MIN_MAX, TMinMaxRuntimeFilterType.MIN));
        Assertions.assertEquals(1, RuntimeFilterGenerator.selectPushableRuntimeFilters(
                sameDirectionFilters, consumers, cteId).size());

        // Different filter types, each applied by one consumer only: neither is pushed.
        List<RuntimeFilter> differentTypeFilters = ImmutableList.of(
                newCteConsumerRuntimeFilter(consumer1, src, consumerK1, consumerK1,
                        TRuntimeFilterType.MIN_MAX, TMinMaxRuntimeFilterType.MIN),
                newCteConsumerRuntimeFilter(consumer2, src, consumerK2, consumerK2,
                        TRuntimeFilterType.IN_OR_BLOOM, TMinMaxRuntimeFilterType.MIN));
        Assertions.assertTrue(RuntimeFilterGenerator.selectPushableRuntimeFilters(
                differentTypeFilters, consumers, cteId).isEmpty());
    }

    @Test
    public void testDoNotPushFiltersWithDifferentNullSemantics() {
        CTEId cteId = new CTEId(1);
        SlotReference src = new SlotReference("src", IntegerType.INSTANCE);
        SlotReference producerK = new SlotReference("k", IntegerType.INSTANCE);
        SlotReference consumerK1 = new SlotReference("k", IntegerType.INSTANCE);
        SlotReference consumerK2 = new SlotReference("k", IntegerType.INSTANCE);
        PhysicalCTEConsumer consumer1 = newCteConsumer(cteId, consumerK1, producerK);
        PhysicalCTEConsumer consumer2 = newCteConsumer(cteId, consumerK2, producerK);
        Set<PhysicalCTEConsumer> consumers = ImmutableSet.of(consumer1, consumer2);

        // `c1.k <=> b.x` produces a null aware filter, `c2.k = b.x` an ordinary one. They prune different
        // rows -- the ordinary one removes the rows whose probe column is NULL, and those are exactly the
        // rows the null aware predicate matches -- so neither may replace the other on the shared producer.
        List<RuntimeFilter> differentNullSemantics = ImmutableList.of(
                newCteConsumerRuntimeFilter(consumer1, src, consumerK1, consumerK1, newHashJoinBuilder(true)),
                newCteConsumerRuntimeFilter(consumer2, src, consumerK2, consumerK2, newHashJoinBuilder(false)));
        Assertions.assertTrue(RuntimeFilterGenerator.selectPushableRuntimeFilters(
                differentNullSemantics, consumers, cteId).isEmpty());

        // The same NULL semantics on every consumer: the filter is pushed.
        List<RuntimeFilter> sameNullSemantics = ImmutableList.of(
                newCteConsumerRuntimeFilter(consumer1, src, consumerK1, consumerK1, newHashJoinBuilder(false)),
                newCteConsumerRuntimeFilter(consumer2, src, consumerK2, consumerK2, newHashJoinBuilder(false)));
        Assertions.assertEquals(1, RuntimeFilterGenerator.selectPushableRuntimeFilters(
                sameNullSemantics, consumers, cteId).size());
    }

    /**
     * The runtime filters which every consumer of a CTE applies are pushed into the producer, where one of
     * them filters the rows of all the consumers; the filters of a single consumer must stay where they are,
     * otherwise they prune the rows the other consumers still need.
     */
    @Test
    public void testPushSharedCteRuntimeFilterIntoTheProducer() {
        int oldType = connectContext.getSessionVariable().getRuntimeFilterType();
        boolean oldMaterialize = connectContext.getSessionVariable().enableCTEMaterialize;
        connectContext.getSessionVariable().setRuntimeFilterType(TRuntimeFilterType.MIN_MAX.getValue());
        connectContext.getSessionVariable().enableCTEMaterialize = true;
        try {
            // Both consumers apply the same MIN filter built from `p_partkey`, so it reaches the producer.
            PhysicalPlan plan = planAfterPostProcess(
                    "with t as (select lo_partkey as k from lineorder)"
                            + " select c2.k from t c2 cross join t c1 cross join part"
                            + " where c1.k > p_partkey and c2.k > p_partkey");
            List<RuntimeFilter> pushedIntoProducer = runtimeFiltersInsideCteProducers(plan);
            Assertions.assertFalse(pushedIntoProducer.isEmpty(),
                    "the filter which every consumer applies must be pushed into the producer");
            // the filter which was moved into the producer is no longer applied by the consumer it was built
            // for, otherwise the rows it filters would be filtered twice
            List<RuntimeFilter> onConsumers = runtimeFiltersOnCteConsumers(plan);
            Assertions.assertTrue(pushedIntoProducer.stream().noneMatch(onConsumers::contains),
                    () -> "a filter pushed into the producer must not stay on its consumer: " + onConsumers);
        } finally {
            connectContext.getSessionVariable().setRuntimeFilterType(oldType);
            connectContext.getSessionVariable().enableCTEMaterialize = oldMaterialize;
        }
    }

    @Test
    public void testDoNotPushSingleConsumerCteRuntimeFilterIntoTheProducer() {
        int oldType = connectContext.getSessionVariable().getRuntimeFilterType();
        boolean oldMaterialize = connectContext.getSessionVariable().enableCTEMaterialize;
        connectContext.getSessionVariable().setRuntimeFilterType(TRuntimeFilterType.MIN_MAX.getValue());
        connectContext.getSessionVariable().enableCTEMaterialize = true;
        try {
            // `c1.k > p_partkey` builds a MIN filter and `c2.k < p_partkey` a MAX one: each of them is
            // applied by one consumer only, so neither may be applied on the shared producer.
            List<RuntimeFilter> pushedIntoProducer = runtimeFiltersInsideCteProducers(planAfterPostProcess(
                    "with t as (select lo_partkey as k from lineorder)"
                            + " select c2.k from t c2 cross join t c1 cross join part"
                            + " where c1.k > p_partkey and c2.k < p_partkey"));
            Assertions.assertTrue(pushedIntoProducer.isEmpty(),
                    () -> "the filters of a single consumer must not reach the producer: " + pushedIntoProducer);
        } finally {
            connectContext.getSessionVariable().setRuntimeFilterType(oldType);
            connectContext.getSessionVariable().enableCTEMaterialize = oldMaterialize;
        }
    }

    /**
     * The filter created on the shared producer replaces the filters of the consumers, so it has to keep the
     * requirement of that group not to be waited for. The producer feeds every consumer, while a filter which
     * waits is built by one of them: a replacement which waits for a consumer whose own filter was
     * non-blocking closes the cycle producer -> build side of a consumer -> consumer -> producer, and the
     * query stalls until the runtime filter or the query times out.
     */
    @Test
    public void testPushSharedCteRuntimeFilterIntoTheProducerKeepsTheNonBlockingRequirement() {
        boolean oldMaterialize = connectContext.getSessionVariable().enableCTEMaterialize;
        boolean oldExpandByInnerJoin = connectContext.getSessionVariable().expandRuntimeFilterByInnerJoin;
        boolean oldDecoupled = connectContext.getSessionVariable().enableDecoupledRuntimeFilter;
        long oldMinDecoupledRows = connectContext.getSessionVariable().minDecoupledRfTargetRows;
        connectContext.getSessionVariable().enableCTEMaterialize = true;
        connectContext.getSessionVariable().expandRuntimeFilterByInnerJoin = true;
        connectContext.getSessionVariable().enableDecoupledRuntimeFilter = true;
        // the tables of the test catalog hold one row, while the decoupled filter of the plan below targets a
        // scan of it: keep the filter which describes the behavior under test rather than the pruning of
        // filters which cannot arrive in time on a tiny scan.
        connectContext.getSessionVariable().minDecoupledRfTargetRows = 0;
        try {
            // `c1.c_custkey = s_suppkey` is the condition join: its standard filter targets the CTE consumer
            // `c1` and expands to `c2`, while the reverse decoupled filter is built by the deeper join
            // `c2.c_custkey = c1.c_custkey`, whose build side carries the filter on `c_region`. The decoupled
            // filter is therefore preferred and both standard filters are marked non-blocking.
            PhysicalPlan plan = planAfterPostProcess(
                    "with t as (select c_custkey, c_region from customer)"
                            + " select c1.c_custkey from t c2 join t c1 on c2.c_custkey = c1.c_custkey"
                            + " join supplier on c1.c_custkey = s_suppkey"
                            + " where c1.c_region = 'ASIA'");
            List<RuntimeFilter> pushedIntoProducer = runtimeFiltersInsideCteProducers(plan);
            Assertions.assertFalse(pushedIntoProducer.isEmpty(),
                    () -> "the filter which every consumer applies must be pushed into the producer: "
                            + plan.treeString());
            Assertions.assertTrue(pushedIntoProducer.stream().allMatch(RuntimeFilter::isNonBlocking),
                    () -> "a filter pushed into the producer must keep the non-blocking requirement of the"
                            + " filters it replaces: " + pushedIntoProducer);
        } finally {
            connectContext.getSessionVariable().enableCTEMaterialize = oldMaterialize;
            connectContext.getSessionVariable().expandRuntimeFilterByInnerJoin = oldExpandByInnerJoin;
            connectContext.getSessionVariable().enableDecoupledRuntimeFilter = oldDecoupled;
            connectContext.getSessionVariable().minDecoupledRfTargetRows = oldMinDecoupledRows;
        }
    }

    /**
     * A node which synthesizes a value of the source column -- here the NULL the repeat adds for the grouping
     * set which does not group by it -- makes a filter built below it prune the rows the consumers above it
     * still need, so the deepest filter must not stand in for them.
     */
    @Test
    public void testDoNotPushRuntimeFilterWhichCrossesAValueSynthesizingNode() {
        boolean oldMaterialize = connectContext.getSessionVariable().enableCTEMaterialize;
        connectContext.getSessionVariable().enableCTEMaterialize = true;
        try {
            PhysicalPlan plan = planAfterPostProcess(
                    "with t as (select lo_partkey as k from lineorder)"
                            + " select c1.k from t c1 join ("
                            + "   select x from ("
                            + "     select c2.k as k2, p.p_partkey as x from t c2 join part p on c2.k <=> p.p_partkey"
                            + "   ) a group by grouping sets ((x), ())"
                            + " ) g on c1.k <=> g.x");
            Assertions.assertTrue(plan.containsType(PhysicalRepeat.class),
                    "the query must plan the grouping sets which synthesize the NULL");
            Assertions.assertTrue(!plan.<Plan>collect(PhysicalCTEProducer.class::isInstance).isEmpty(),
                    "the query must materialize the CTE");
            Assertions.assertFalse(runtimeFiltersOnCteConsumers(plan).isEmpty(),
                    "the consumers must keep the filters which may not be pushed");
            List<RuntimeFilter> pushedIntoProducer = runtimeFiltersInsideCteProducers(plan);
            Assertions.assertTrue(pushedIntoProducer.isEmpty(),
                    () -> "a filter which crosses a value synthesizing node must not reach the producer: "
                            + pushedIntoProducer);
        } finally {
            connectContext.getSessionVariable().enableCTEMaterialize = oldMaterialize;
        }
    }

    /**
     * A right outer join null extends its left child, so a filter whose source comes from that side did not
     * observe the NULL the join adds: it may not stand in for a filter which is built above the join, whose
     * build side does contain that NULL. The same join keeps the source values when the source comes from the
     * right child it preserves, and an inner join never adds a value.
     */
    @Test
    public void testSourceValueIsNotKeptWhenAnOuterJoinNullExtendsIt() {
        Plan deepestBuilder = Mockito.mock(Plan.class);
        Plan shallowestBuilder = Mockito.mock(Plan.class);
        PhysicalHashJoin<?, ?> rightOuterJoin = newMockJoin(JoinType.RIGHT_OUTER_JOIN, deepestBuilder);
        // the source comes from the left child, which the right outer join null extends
        Assertions.assertFalse(RuntimeFilterGenerator.keepsSourceValue(
                ImmutableList.of(deepestBuilder, rightOuterJoin), shallowestBuilder));
        // the same join, with the source coming from the right child it preserves
        Assertions.assertTrue(RuntimeFilterGenerator.keepsSourceValue(
                ImmutableList.of(rightOuterJoin.child(1), rightOuterJoin), shallowestBuilder));
        // an inner join restricts the rows of the source, it never adds a value to it
        Assertions.assertTrue(RuntimeFilterGenerator.keepsSourceValue(
                ImmutableList.of(deepestBuilder, newMockJoin(JoinType.INNER_JOIN, deepestBuilder)),
                shallowestBuilder));
    }

    /** A join node whose left child is the given plan. */
    private PhysicalHashJoin<?, ?> newMockJoin(JoinType joinType, Plan leftChild) {
        PhysicalHashJoin<?, ?> join = Mockito.mock(PhysicalHashJoin.class);
        Mockito.when(join.getJoinType()).thenReturn(joinType);
        Mockito.when(join.child(0)).thenReturn(leftChild);
        Mockito.when(join.child(1)).thenReturn(Mockito.mock(Plan.class));
        return join;
    }

    private PhysicalPlan planAfterPostProcess(String sql) {
        PlanChecker checker = PlanChecker.from(connectContext).analyze(sql).rewrite().optimize();
        return new PlanPostProcessors(checker.getCascadesContext()).process(checker.getBestPlanTree());
    }

    /** The runtime filters which are still applied by the consumers of a CTE. */
    private static List<RuntimeFilter> runtimeFiltersOnCteConsumers(PhysicalPlan plan) {
        List<RuntimeFilter> applied = new ArrayList<>();
        for (Plan consumer : plan.<Plan>collect(PhysicalCTEConsumer.class::isInstance)) {
            applied.addAll(((AbstractPhysicalPlan) consumer).getAppliedRuntimeFilters());
        }
        return applied;
    }

    /** The runtime filters which were installed on the relations inside the CTE producers. */
    private static List<RuntimeFilter> runtimeFiltersInsideCteProducers(PhysicalPlan plan) {
        List<RuntimeFilter> applied = new ArrayList<>();
        for (Plan producer : plan.<Plan>collect(PhysicalCTEProducer.class::isInstance)) {
            for (Plan relation : ((PhysicalCTEProducer<?>) producer).child(0)
                    .<Plan>collect(PhysicalRelation.class::isInstance)) {
                applied.addAll(((AbstractPhysicalPlan) relation).getAppliedRuntimeFilters());
            }
        }
        return applied;
    }

    /** A hash join whose only conjunct is an EQ_FOR_NULL one when nullSafeEqual is set. */
    private AbstractPhysicalPlan newHashJoinBuilder(boolean nullSafeEqual) {
        PhysicalHashJoin<?, ?> join = Mockito.mock(PhysicalHashJoin.class);
        SlotReference left = new SlotReference("k", IntegerType.INSTANCE);
        SlotReference right = new SlotReference("x", IntegerType.INSTANCE);
        Mockito.when(join.getHashJoinConjuncts()).thenReturn(ImmutableList.of(
                nullSafeEqual ? new NullSafeEqual(left, right) : new EqualTo(left, right)));
        return join;
    }

    private PhysicalCTEConsumer newCteConsumer(CTEId cteId, Slot targetSlot, Slot producerSlot) {
        PhysicalCTEConsumer consumer = Mockito.mock(PhysicalCTEConsumer.class);
        Mockito.when(consumer.getCteId()).thenReturn(cteId);
        Mockito.when(consumer.getProducerSlot(targetSlot)).thenReturn(producerSlot);
        return consumer;
    }

    private RuntimeFilter newCteConsumerRuntimeFilter(PhysicalCTEConsumer consumer, Expression src,
            Slot targetSlot, Expression targetExpression) {
        return newCteConsumerRuntimeFilter(consumer, src, targetSlot, targetExpression,
                TRuntimeFilterType.IN_OR_BLOOM, TMinMaxRuntimeFilterType.MIN_MAX);
    }

    private RuntimeFilter newCteConsumerRuntimeFilter(PhysicalCTEConsumer consumer, Expression src,
            Slot targetSlot, Expression targetExpression, TRuntimeFilterType type,
            TMinMaxRuntimeFilterType minMaxType) {
        return newCteConsumerRuntimeFilter(consumer, src, targetSlot, targetExpression,
                Mockito.mock(AbstractPhysicalPlan.class), type, minMaxType);
    }

    private RuntimeFilter newCteConsumerRuntimeFilter(PhysicalCTEConsumer consumer, Expression src,
            Slot targetSlot, Expression targetExpression, AbstractPhysicalPlan builder) {
        return newCteConsumerRuntimeFilter(consumer, src, targetSlot, targetExpression,
                builder, TRuntimeFilterType.IN_OR_BLOOM, TMinMaxRuntimeFilterType.MIN_MAX);
    }

    private RuntimeFilter newCteConsumerRuntimeFilter(PhysicalCTEConsumer consumer, Expression src,
            Slot targetSlot, Expression targetExpression, AbstractPhysicalPlan builder,
            TRuntimeFilterType type, TMinMaxRuntimeFilterType minMaxType) {
        return new RuntimeFilter(RuntimeFilterId.createGenerator().getNextId(), src, targetSlot, targetExpression,
                type, 0, builder, -1L, true, minMaxType, consumer);
    }

    @Test
    public void testRuntimeFilterBlockByRecCte() {
        String sql = new StringBuilder().append("with recursive xx as (\n").append("  select\n")
                .append("    c_custkey as c1\n").append("  from\n").append("    customer\n").append("  union\n")
                .append("  select\n").append("    xx.c1 as c1\n").append("  from\n").append("    xx\n").append(")\n")
                .append("select\n").append("    xx.c1\n").append("  from\n").append("    xx\n")
                .append("    join lineorder on lineorder.lo_custkey = xx.c1").toString();
        LogicalPlan unboundPlan = new NereidsParser().parseSingle(sql);
        StatementContext statementContext = new StatementContext(connectContext,
                new OriginStatement(sql, 0));
        NereidsPlanner planner = new NereidsPlanner(statementContext);
        planner.planWithLock(unboundPlan, PhysicalProperties.ANY,
                ExplainCommand.ExplainLevel.OPTIMIZED_PLAN);
        MemoTestUtils.initMemoAndValidState(planner.getCascadesContext());
        new PhysicalPlanTranslator(new PlanTranslatorContext(planner.getCascadesContext()))
                .translatePlan((PhysicalPlan) planner.getOptimizedPlan());
        RuntimeFilterContext context = planner.getCascadesContext().getRuntimeFilterContext();
        List<RuntimeFilter> filters = context.getNereidsRuntimeFilter();
        Assertions.assertEquals(0, filters.size());
    }

    @Test
    public void testDecoupledRuntimeFilter() {
        connectContext.getSessionVariable().enableDecoupledRuntimeFilter = true;
        // join1(c_custkey=s_suppkey) probe=join2(lo_custkey=c_custkey)
        // Standard RFs: c_custkey→lo_custkey (join2), s_suppkey→c_custkey (join1)
        // Decoupled RF: c_custkey→s_suppkey (builder=join2, pushed to supplier scan).
        // Add a visible filter on customer so the decoupled RF is not pruned by the
        // "unknown stats + no filter on builder right side" heuristic.
        String sql = "select * "
                + "from lineorder join customer on lo_custkey = c_custkey "
                + "join supplier on c_custkey = s_suppkey "
                + "where customer.c_region = 'ASIA'";

        PlanChecker checker = PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .implement();
        PhysicalPlan plan = checker.getPhysicalPlan();
        plan = new PlanPostProcessors(checker.getCascadesContext()).process(plan);
        new PhysicalPlanTranslator(new PlanTranslatorContext(checker.getCascadesContext())).translatePlan(plan);
        RuntimeFilterContext rfCtx = checker.getCascadesContext().getRuntimeFilterContext();
        List<RuntimeFilter> filters = rfCtx.getNereidsRuntimeFilter();

        List<RuntimeFilter> decoupledRFs = filters.stream()
                .filter(f -> f.getExprOrder() == -1)
                .collect(Collectors.toList());
        Assertions.assertFalse(decoupledRFs.isEmpty(),
                "Expected at least one decoupled RF with exprOrder == -1");

        RuntimeFilter decoupled = decoupledRFs.get(0);
        // Decoupled RF should target the build side of the condition join
        Assertions.assertTrue(decoupled.getTargetSlots().stream()
                        .anyMatch(s -> s.getName().equals("s_suppkey")),
                "Decoupled RF target should include s_suppkey");

        // Standard RF s_suppkey→c_custkey should still exist (not removed)
        List<RuntimeFilter> standardRFs = filters.stream()
                .filter(f -> f.getExprOrder() >= 0)
                .collect(Collectors.toList());
        Assertions.assertFalse(standardRFs.isEmpty(),
                "Standard RFs should still be present alongside non-blocking decoupled RF");

        connectContext.getSessionVariable().enableDecoupledRuntimeFilter = false;
    }

}
