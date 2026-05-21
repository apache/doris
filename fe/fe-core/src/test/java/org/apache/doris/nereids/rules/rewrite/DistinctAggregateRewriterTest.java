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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.catalog.DistributionInfo;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.nereids.rules.analysis.LogicalSubQueryAliasToLogicalProject;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.MultiDistinctCount;
import org.apache.doris.nereids.trees.expressions.functions.agg.MultiDistinctGroupConcat;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum0;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.plans.AbstractPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.ColumnStatisticBuilder;
import org.apache.doris.statistics.Statistics;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class DistinctAggregateRewriterTest extends TestWithFeService implements MemoPatternMatchSupported {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        createTable("create table test.distinct_agg_split_t(a int null, b int not null,"
                + "c varchar(10) null, d date, dt datetime)\n"
                + "distributed by hash(a) properties('replication_num' = '1');");
        createTable("CREATE TABLE IF NOT EXISTS test.sales_records\n"
                + "(\n"
                + "    record_id BIGINT,\n"
                + "    seller_id BIGINT,\n"
                + "    sale_date DATE,\n"
                + "    amount DECIMAL(18,2)\n"
                + ")\n"
                + "DUPLICATE KEY(record_id, seller_id)\n"
                + "PARTITION BY RANGE(sale_date)\n"
                + "(\n"
                + "    PARTITION p202301 VALUES LESS THAN ('2023-02-01'),\n"
                + "    PARTITION p202302 VALUES LESS THAN ('2023-03-01'),\n"
                + "    PARTITION p202303 VALUES LESS THAN ('2023-04-01')\n"
                + ")\n"
                + "DISTRIBUTED BY HASH(record_id) BUCKETS 10\n"
                + "PROPERTIES (\n"
                + "    \"replication_num\" = \"1\"\n"
                + ");");
        createTable("create table test.distinct_agg_hash_ab(a int null, b int not null, c int null, d int null)\n"
                + "distributed by hash(a, b) properties('replication_num' = '1');");
        createTable("create table test.distinct_agg_hash_abcd(a int null, b int not null, c int null, d int null)\n"
                + "distributed by hash(a, b, c, d) properties('replication_num' = '1');");
        connectContext.setDatabase("test");
        SessionVariable spySessionVariable = Mockito.spy(connectContext.getSessionVariable());
        Mockito.doReturn(24).when(spySessionVariable).getParallelExecInstanceNum(Mockito.anyString());
        connectContext.setSessionVariable(spySessionVariable);
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
    }

    private void applyMock() {
        new MockUp<DistinctAggregateRewriter>() {
            @Mock
            boolean shouldUseMultiDistinct(LogicalAggregate<? extends Plan> aggregate) {
                return false;
            }
        };
    }

    @Test
    void testSplitSingleDistinctAgg() {
        applyMock();
        PlanChecker.from(connectContext)
                .analyze("select b, count(distinct a) from test.distinct_agg_split_t group by b")
                .rewrite()
                .printlnTree()
                .matches(
                        logicalAggregate(
                                logicalAggregate()
                                        .when(agg -> agg.getGroupByExpressions().size() == 2
                                        && agg.getAggregateFunctions().isEmpty())
                        ).when(agg -> agg.getGroupByExpressions().size() == 1
                                && agg.getGroupByExpressions().get(0).toSql().equals("b")
                                && agg.getAggregateFunctions().iterator().next() instanceof Count
                        )
                );
    }

    @Test
    void testSplitSingleDistinctAggOtherFunctionCount() {
        applyMock();
        PlanChecker.from(connectContext)
                .analyze("select b, count(distinct a), count(a) from test.distinct_agg_split_t group by b")
                .rewrite()
                .printlnTree()
                .matches(
                        logicalAggregate(
                                logicalAggregate()
                                        .when(agg -> agg.getGroupByExpressions().size() == 2
                                                && agg.getAggregateFunctions().iterator().next() instanceof Count)
                        ).when(agg -> agg.getGroupByExpressions().size() == 1
                                && agg.getGroupByExpressions().get(0).toSql().equals("b")
                                && agg.getAggregateFunctions().stream().anyMatch(f -> f instanceof Sum0)
                        )
                );
    }

    @Test
    void testSplitSingleDistinctWithOtherAgg() {
        applyMock();
        PlanChecker.from(connectContext)
                .analyze("select b, count(distinct a), sum(c) from test.distinct_agg_split_t group by b")
                .rewrite()
                .printlnTree()
                .matches(
                        logicalAggregate(
                                logicalAggregate()
                                        .when(agg -> agg.getGroupByExpressions().size() == 2)
                        ).when(agg -> agg.getGroupByExpressions().size() == 1
                                && agg.getGroupByExpressions().get(0).toSql().equals("b")
                                && agg.getAggregateFunctions().stream().noneMatch(AggregateFunction::isDistinct)
                        ));
    }

    @Test
    void testNotSplitWhenNoGroupBy() {
        applyMock();
        PlanChecker.from(connectContext)
                .analyze("select count(distinct a) from test.distinct_agg_split_t")
                .rewrite()
                .printlnTree()
                .nonMatch(logicalAggregate(logicalAggregate()));
    }

    @Test
    void testSplitWhenNoGroupByHasGroupConcatDistinctOrderBy() {
        applyMock();
        PlanChecker.from(connectContext)
                .analyze("select group_concat(distinct a, '' order by b) from test.distinct_agg_split_t")
                .rewrite()
                .printlnTree()
                .matches(logicalAggregate()
                        .when(agg ->
                                agg.getAggregateFunctions().iterator().next() instanceof MultiDistinctGroupConcat));
    }

    @Test
    void testSplitWhenNoGroupByHasGroupConcatDistinct() {
        applyMock();
        PlanChecker.from(connectContext)
                .analyze("select group_concat(distinct a, '') from test.distinct_agg_split_t")
                .rewrite()
                .printlnTree()
                .nonMatch(logicalAggregate()
                        .when(agg ->
                                agg.getAggregateFunctions().iterator().next() instanceof MultiDistinctGroupConcat));
    }

    @Test
    void testMultiExprDistinct() {
        applyMock();
        PlanChecker.from(connectContext)
                .analyze("select b, sum(a), count(distinct a,c) from test.distinct_agg_split_t group by b")
                .rewrite()
                .printlnTree()
                .matches(
                        logicalAggregate(
                                logicalAggregate()
                                        .when(agg -> agg.getGroupByExpressions().size() == 3
                                                && agg.getAggregateFunctions().size() == 1)
                        ).when(agg -> agg.getGroupByExpressions().size() == 1
                                && agg.getAggregateFunctions().stream().anyMatch(
                                        f -> f instanceof Count && f.child(0) instanceof If
                                                && !f.isDistinct()))
                );
    }

    @Test
    void testNotSplitWhenNoDistinct() {
        applyMock();
        PlanChecker.from(connectContext)
                .analyze("select b, sum(a), count(c) from test.distinct_agg_split_t group by b")
                .rewrite()
                .printlnTree()
                .nonMatch(logicalAggregate(logicalAggregate()));
    }

    @Test
    void testSplitWithComplexExpression() {
        applyMock();
        PlanChecker.from(connectContext)
                .analyze("select b, count(distinct a + 1) from test.distinct_agg_split_t group by b")
                .rewrite()
                .printlnTree()
                .matches(
                        logicalAggregate(
                                logicalAggregate().when(agg -> agg.getGroupByExpressions().size() == 2)
                        ).when(agg -> agg.getGroupByExpressions().size() == 1
                                && agg.getGroupByExpressions().get(0).toSql().equals("b")));
    }

    @Test
    void testMultiDistinct() {
        connectContext.getSessionVariable().setAggPhase(2);
        PlanChecker.from(connectContext)
                .analyze("select b, count(distinct a), sum(c) from test.distinct_agg_split_t group by b")
                .rewrite()
                .printlnTree()
                .matches(
                        logicalAggregate().when(agg -> agg.getGroupByExpressions().size() == 1
                                && agg.getGroupByExpressions().get(0).toSql().equals("b")
                                && agg.getAggregateFunctions().stream().noneMatch(AggregateFunction::isDistinct)
                                && agg.getAggregateFunctions().stream().anyMatch(f -> f instanceof MultiDistinctCount)
                        ));
        connectContext.getSessionVariable().setAggPhase(0);
    }

    @Test
    void testShouldUseMultiDistinctWithoutStatsSatisfyDistribution() throws Exception {
        DistinctAggregateRewriter rewriter = DistinctAggregateRewriter.INSTANCE;
        LogicalAggregate<? extends Plan> aggregate = getLogicalAggregate(
                "select bb, count(distinct aa) from "
                        + "(select a as aa, b as bb from test.distinct_agg_split_t where b > 1) t "
                        + "group by bb"
        );
        Plan child = aggregate.child();
        Map<org.apache.doris.nereids.trees.expressions.Expression, ColumnStatistic> colStats = new HashMap<>();
        aggregate.getGroupByExpressions().forEach(expr ->
                colStats.put(expr, unknownColumnStats()));
        aggregate.getDistinctArguments().forEach(expr ->
                colStats.put(expr, unknownColumnStats()));
        ((AbstractPlan) child).setStatistics(new Statistics(10000, colStats));
        aggregate.setStatistics(new Statistics(100, ImmutableMap.of()));

        Assertions.assertFalse(rewriter.shouldUseMultiDistinct(aggregate));
    }

    @Test
    void testShouldUseMultiDistinctWithStatsSelected() throws Exception {
        DistinctAggregateRewriter rewriter = new DistinctAggregateRewriter();
        LogicalAggregate<? extends Plan> aggregate = getLogicalAggregate(
                "select b, count(distinct a) from test.distinct_agg_split_t group by b"
        );
        Plan child = aggregate.child();
        Map<org.apache.doris.nereids.trees.expressions.Expression, ColumnStatistic> colStats = new HashMap<>();
        aggregate.getGroupByExpressions().forEach(expr ->
                colStats.put(expr, buildColumnStats(240, false)));
        aggregate.getDistinctArguments().forEach(expr ->
                colStats.put(expr, buildColumnStats(10000.0, false)));
        ((AbstractPlan) child).setStatistics(new Statistics(100000, colStats));
        aggregate.setStatistics(new Statistics(240, ImmutableMap.of()));

        Assertions.assertFalse(rewriter.shouldUseMultiDistinct(aggregate));
    }

    @Test
    void testShouldUseMultiDistinctWithPartitionTable() {
        DistinctAggregateRewriter rewriter = DistinctAggregateRewriter.INSTANCE;
        LogicalAggregate<? extends Plan> aggregate = getLogicalAggregate(
                "select count(distinct record_id) from sales_records group by sale_date;"
        );
        Plan child = aggregate.child();
        Map<org.apache.doris.nereids.trees.expressions.Expression, ColumnStatistic> colStats = new HashMap<>();
        aggregate.getGroupByExpressions().forEach(expr ->
                colStats.put(expr, unknownColumnStats()));
        aggregate.getDistinctArguments().forEach(expr ->
                colStats.put(expr, unknownColumnStats()));
        ((AbstractPlan) child).setStatistics(new Statistics(10000, colStats));
        aggregate.setStatistics(new Statistics(100, ImmutableMap.of()));

        Assertions.assertTrue(rewriter.shouldUseMultiDistinct(aggregate));
    }

    @Test
    void testResolveDistinctDistributionInfoWithProjectAndFilter() throws Exception {
        LogicalAggregate<? extends Plan> aggregate = getLogicalAggregate(
                "select bb, count(distinct aa) from "
                        + "(select a as aa, b as bb from test.distinct_agg_hash_ab where c > 1) t "
                        + "group by bb"
        );

        Object info = invokeResolveDistinctDistributionInfo(aggregate);
        Assertions.assertNotNull(info);

        List<String> distinctSlotNames = getDistinctSlots(info).stream()
                .map(SlotReference::getName)
                .collect(Collectors.toList());
        Assertions.assertEquals(ImmutableList.of("a"), distinctSlotNames);

        DistributionInfo distributionInfo = getDistributionInfo(info);
        Assertions.assertTrue(distributionInfo instanceof HashDistributionInfo);
        List<String> distributionColumnNames = ((HashDistributionInfo) distributionInfo).getDistributionColumns().stream()
                .map(column -> column.getName().toLowerCase())
                .collect(Collectors.toList());
        Assertions.assertEquals(ImmutableList.of("a", "b"), distributionColumnNames);
    }

    @Test
    void testIsDistinctKeySatisfyDistributionWhenDistinctContainsDistributionColumns() throws Exception {
        LogicalAggregate<? extends Plan> aggregate = getLogicalAggregate(
                "select d, count(distinct a, b, c) from test.distinct_agg_hash_ab group by d"
        );

        Assertions.assertTrue(invokeIsDistinctKeySatisfyDistribution(aggregate));
    }

    @Test
    void testIsDistinctKeySatisfyDistributionWhenDistributionHasExtraColumns() throws Exception {
        LogicalAggregate<? extends Plan> aggregate = getLogicalAggregate(
                "select d, count(distinct a, b, c) from test.distinct_agg_hash_abcd group by d"
        );

        Assertions.assertFalse(invokeIsDistinctKeySatisfyDistribution(aggregate));
    }

    private LogicalAggregate<? extends Plan> getLogicalAggregate(String sql) {
        Plan plan = PlanChecker.from(connectContext)
                .analyze(sql)
                .applyTopDown(new LogicalSubQueryAliasToLogicalProject())
                .getPlan();
        Optional<LogicalAggregate<? extends Plan>> aggregate = findAggregate(plan);
        Assertions.assertTrue(aggregate.isPresent());
        return aggregate.get();
    }

    private Optional<LogicalAggregate<? extends Plan>> findAggregate(Plan plan) {
        if (plan instanceof LogicalAggregate) {
            return Optional.of((LogicalAggregate<? extends Plan>) plan);
        }
        for (Plan child : plan.children()) {
            Optional<LogicalAggregate<? extends Plan>> found = findAggregate(child);
            if (found.isPresent()) {
                return found;
            }
        }
        return Optional.empty();
    }

    private Object invokeResolveDistinctDistributionInfo(LogicalAggregate<? extends Plan> aggregate) throws Exception {
        Method method = DistinctAggregateRewriter.class.getDeclaredMethod(
                "resolveDistinctDistributionInfo", LogicalAggregate.class);
        method.setAccessible(true);
        return method.invoke(DistinctAggregateRewriter.INSTANCE, aggregate);
    }

    private boolean invokeIsDistinctKeySatisfyDistribution(LogicalAggregate<? extends Plan> aggregate) throws Exception {
        Method method = DistinctAggregateRewriter.class.getDeclaredMethod(
                "isDistinctKeySatisfyDistribution", LogicalAggregate.class);
        method.setAccessible(true);
        return (boolean) method.invoke(DistinctAggregateRewriter.INSTANCE, aggregate);
    }

    @SuppressWarnings("unchecked")
    private Set<SlotReference> getDistinctSlots(Object info) throws Exception {
        Field field = info.getClass().getDeclaredField("distinctSlots");
        field.setAccessible(true);
        return (Set<SlotReference>) field.get(info);
    }

    private DistributionInfo getDistributionInfo(Object info) throws Exception {
        Field field = info.getClass().getDeclaredField("distributionInfo");
        field.setAccessible(true);
        return (DistributionInfo) field.get(info);
    }

    private ColumnStatistic unknownColumnStats() {
        return buildColumnStats(0.0, true);
    }

    private ColumnStatistic buildColumnStats(double ndv, boolean isUnknown) {
        return new ColumnStatisticBuilder(1)
                .setNdv(ndv)
                .setAvgSizeByte(4)
                .setNumNulls(0)
                .setMinValue(0)
                .setMaxValue(100)
                .setIsUnknown(isUnknown)
                .setUpdatedTime("")
                .build();
    }
}
