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
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
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
}
