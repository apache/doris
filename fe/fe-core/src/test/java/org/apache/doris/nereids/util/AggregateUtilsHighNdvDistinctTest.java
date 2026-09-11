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

package org.apache.doris.nereids.util;

import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.memo.GroupId;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Year;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.types.DateV2Type;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.ColumnStatisticBuilder;
import org.apache.doris.statistics.Statistics;
import org.apache.doris.statistics.StatisticsBuilder;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

class AggregateUtilsHighNdvDistinctTest {
    private static final double ROW_COUNT = 10000000;

    private final LogicalOlapScan scan = new LogicalOlapScan(
            StatementScopeIdGenerator.newRelationId(), PlanConstructor.student, ImmutableList.of(""));
    private final Slot id = scan.getOutput().get(0);
    private final Slot name = scan.getOutput().get(2);
    private final Slot age = scan.getOutput().get(3);
    private final GroupExpression scanGroupExpr = new GroupExpression(scan, ImmutableList.of());
    private final GroupPlan childGroup = new GroupPlan(
            new Group(GroupId.createGenerator().getNextId(), scanGroupExpr.getPlan().getLogicalProperties()));

    private LogicalAggregate<GroupPlan> buildAgg() {
        List<NamedExpression> outputs = Lists.newArrayList(
                new Alias(new Count(true, id), "count_distinct_id"),
                new Alias(new Count(true, name), "count_distinct_name"));
        return new LogicalAggregate<>(Lists.newArrayList(age), outputs, childGroup);
    }

    // count(distinct if(age = 1, id, null)) group by age: the distinct argument is an If, not a bare
    // slot. This is the shape hasHighNdvDistinctArgument exists for -- ExpressionEstimation.visitIf must
    // propagate id's ndv out of the then-branch, otherwise the near-unique payment_id style production
    // query would never be detected.
    private LogicalAggregate<GroupPlan> buildAggWithIf() {
        If ifExpr = new If(new EqualTo(age, new IntegerLiteral(1)), id, new NullLiteral(id.getDataType()));
        List<NamedExpression> outputs = Lists.newArrayList(
                new Alias(new Count(true, ifExpr), "count_distinct_if"),
                new Alias(new Count(true, name), "count_distinct_name"));
        return new LogicalAggregate<>(Lists.newArrayList(age), outputs, childGroup);
    }

    // count(distinct year(dt)) group by age, where dt has no statistics. year() is a determinably
    // low-ndv function (ExpressionEstimation.visitYear fabricates ndv=69, isUnknown=false), so the
    // estimate alone says "not unknown"; only the per-input-slot check sees that dt itself is
    // unanalyzed. This documents the intentional conservative fallback: an unknown base slot routes to
    // CTE even under a low-ndv wrapper, trading a costlier plan for OOM safety.
    private LogicalAggregate<GroupPlan> buildAggWithYear(Slot dateSlot) {
        List<NamedExpression> outputs = Lists.newArrayList(
                new Alias(new Count(true, new Year(dateSlot)), "count_distinct_year"),
                new Alias(new Count(true, name), "count_distinct_name"));
        return new LogicalAggregate<>(Lists.newArrayList(age), outputs, childGroup);
    }

    private ColumnStatistic ndvStat(double ndv) {
        return new ColumnStatisticBuilder(ROW_COUNT).setNdv(ndv).setAvgSizeByte(4).build();
    }

    @Test
    void nearUniqueDistinctArgumentDoesNotUseMultiDistinct() {
        Statistics childStats = new StatisticsBuilder()
                .setRowCount(ROW_COUNT)
                .putColumnStatistics(id, ndvStat(ROW_COUNT * 0.9))
                .putColumnStatistics(name, ndvStat(ROW_COUNT * 0.9))
                .putColumnStatistics(age, ndvStat(12))
                .build();
        Assertions.assertTrue(
                AggregateUtils.hasHighNdvDistinctArgument(buildAgg(), childStats, ROW_COUNT));
    }

    @Test
    void lowNdvDistinctArgumentStillUsesMultiDistinct() {
        Statistics childStats = new StatisticsBuilder()
                .setRowCount(ROW_COUNT)
                .putColumnStatistics(id, ndvStat(100))
                .putColumnStatistics(name, ndvStat(100))
                .putColumnStatistics(age, ndvStat(12))
                .build();
        Assertions.assertFalse(
                AggregateUtils.hasHighNdvDistinctArgument(buildAgg(), childStats, ROW_COUNT));
    }

    @Test
    void unknownDistinctArgumentStatsDoNotTrigger() {
        Statistics childStats = new StatisticsBuilder().setRowCount(ROW_COUNT).build();
        Assertions.assertFalse(
                AggregateUtils.hasHighNdvDistinctArgument(buildAgg(), childStats, ROW_COUNT));
    }

    @Test
    void ifWrappedNearUniqueDistinctArgumentDoesNotUseMultiDistinct() {
        Statistics childStats = new StatisticsBuilder()
                .setRowCount(ROW_COUNT)
                .putColumnStatistics(id, ndvStat(ROW_COUNT * 0.9))
                .putColumnStatistics(name, ndvStat(100))
                .putColumnStatistics(age, ndvStat(12))
                .build();
        Assertions.assertTrue(
                AggregateUtils.hasHighNdvDistinctArgument(buildAggWithIf(), childStats, ROW_COUNT));
    }

    @Test
    void ndvExactlyAtThresholdCountsAsHighNdv() {
        Statistics childStats = new StatisticsBuilder()
                .setRowCount(ROW_COUNT)
                .putColumnStatistics(id, ndvStat(ROW_COUNT * AggregateUtils.MID_CARDINALITY_THRESHOLD))
                .putColumnStatistics(name, ndvStat(100))
                .putColumnStatistics(age, ndvStat(12))
                .build();
        Assertions.assertTrue(
                AggregateUtils.hasHighNdvDistinctArgument(buildAgg(), childStats, ROW_COUNT));
    }

    @Test
    void unknownDistinctArgumentIsTreatedAsRisky() {
        // No column statistics at all: every distinct argument is unknown, so we cannot rule out a
        // near-unique argument that would OOM MultiDistinct under a low-cardinality group by. This is
        // the gap the helper closes -- an unknown distinct argument must route to CTE, matching the
        // no-group-by branch of DistinctAggStrategySelector.
        Statistics childStats = new StatisticsBuilder().setRowCount(ROW_COUNT).build();
        Assertions.assertTrue(
                AggregateUtils.hasUnknownNdvDistinctArgument(buildAgg(), childStats));
    }

    @Test
    void oneUnknownDistinctArgumentAmongKnownIsTreatedAsRisky() {
        // id has stats but name does not: a single unknown distinct argument is enough to be risky,
        // because that one argument alone could be near-unique.
        Statistics childStats = new StatisticsBuilder()
                .setRowCount(ROW_COUNT)
                .putColumnStatistics(id, ndvStat(100))
                .putColumnStatistics(age, ndvStat(12))
                .build();
        Assertions.assertTrue(
                AggregateUtils.hasUnknownNdvDistinctArgument(buildAgg(), childStats));
    }

    @Test
    void allKnownDistinctArgumentsAreNotUnknown() {
        // Both distinct arguments have statistics (regardless of ndv value), so none is unknown and
        // the group-by cardinality checks downstream are allowed to run.
        Statistics childStats = new StatisticsBuilder()
                .setRowCount(ROW_COUNT)
                .putColumnStatistics(id, ndvStat(100))
                .putColumnStatistics(name, ndvStat(ROW_COUNT * 0.9))
                .putColumnStatistics(age, ndvStat(12))
                .build();
        Assertions.assertFalse(
                AggregateUtils.hasUnknownNdvDistinctArgument(buildAgg(), childStats));
    }

    @Test
    void ifWrappedKnownDistinctArgumentIsNotUnknown() {
        // if(age = 1, id, null): ExpressionEstimation resolves through the If to id's statistics, so a
        // known column wrapped in an If is not treated as unknown.
        Statistics childStats = new StatisticsBuilder()
                .setRowCount(ROW_COUNT)
                .putColumnStatistics(id, ndvStat(100))
                .putColumnStatistics(name, ndvStat(100))
                .putColumnStatistics(age, ndvStat(12))
                .build();
        Assertions.assertFalse(
                AggregateUtils.hasUnknownNdvDistinctArgument(buildAggWithIf(), childStats));
    }

    @Test
    void ifWrappedUnknownDistinctArgumentIsTreatedAsRisky() {
        // if(age = 1, id, null) where id has no statistics. ExpressionEstimation.visitIf fabricates a
        // small non-unknown ndv, so the estimate alone would not flag this; the per-input-slot check is
        // what catches that id is unanalyzed and could be near-unique. This is the gap the slot scan
        // closes -- the production if(cond, payment_id, null) shape.
        Statistics childStats = new StatisticsBuilder()
                .setRowCount(ROW_COUNT)
                .putColumnStatistics(name, ndvStat(100))
                .putColumnStatistics(age, ndvStat(12))
                .build();
        Assertions.assertTrue(
                AggregateUtils.hasUnknownNdvDistinctArgument(buildAggWithIf(), childStats));
    }

    @Test
    void lowNdvDerivedFunctionOverUnknownSlotIsTreatedAsRisky() {
        // count(distinct year(dt)) where dt is unanalyzed. year() has an obviously bounded ndv, yet
        // because its base slot has no statistics the strategy still routes to CTE. This is intentional:
        // the per-input-slot check favors OOM safety over reusing the low-ndv estimate, so it is a
        // conservative fallback, not a regression, if a future reader expects MultiDistinct here.
        SlotReference dt = new SlotReference("dt", DateV2Type.INSTANCE);
        Statistics childStats = new StatisticsBuilder()
                .setRowCount(ROW_COUNT)
                .putColumnStatistics(name, ndvStat(100))
                .putColumnStatistics(age, ndvStat(12))
                .build();
        Assertions.assertTrue(
                AggregateUtils.hasUnknownNdvDistinctArgument(buildAggWithYear(dt), childStats));
    }
}
