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

import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.nereids.util.TypeUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.model.ColumnStatistic;
import org.apache.doris.statistics.model.ColumnStatisticBuilder;
import org.apache.doris.thrift.TStorageType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;

class EliminateNotNullTest implements MemoPatternMatchSupported {
    private static final String SCAN_COLUMN = "nullable_col";
    private static final long SCAN_TABLE_ID = 10L;

    private final SlotReference slot = new SlotReference("nullable_col", IntegerType.INSTANCE, true);
    private final LogicalOneRowRelation relation = new LogicalOneRowRelation(new RelationId(1), ImmutableList.of(slot));

    @Test
    void testEliminateNotNullForSimplePredicate() {
        Expression simplePredicate = new EqualTo(slot, Literal.of(1));
        Expression explicitNotNull = new Not(new IsNull(slot));
        LogicalPlan plan = new LogicalPlanBuilder(relation)
                .filter(ImmutableSet.of(simplePredicate, explicitNotNull))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new EliminateNotNull())
                .matches(logicalFilter().when(filter -> filter.getConjuncts().size() == 1));
    }

    @Test
    void testKeepNotNullWhenOnlyWidePredicateCanProveIt() {
        Expression widePredicate = new EqualTo(repeatAdd(slot, 257), Literal.of(1));
        Expression explicitNotNull = new Not(new IsNull(slot));
        LogicalPlan plan = new LogicalPlanBuilder(relation)
                .filter(ImmutableSet.of(widePredicate, explicitNotNull))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new EliminateNotNull())
                .matches(logicalFilter().when(filter -> filter.getConjuncts().size() == 2));
    }

    @Test
    void testKeepGeneratedNotNullOnScanWithManyNulls() {
        LogicalOlapScan scan = newLogicalOlapScan(true);
        Slot scanSlot = scan.getOutput().get(0);
        LogicalPlan plan = new LogicalPlanBuilder(scan)
                .filter(ImmutableSet.of(new Not(new IsNull(scanSlot), true)))
                .build();
        putColumnNumNulls(2_000_000);

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new EliminateNotNull())
                .matches(logicalFilter().when(filter -> filter.getConjuncts().size() == 1
                        && isKeptIsNotNull(filter.getConjuncts().iterator().next())));
    }

    @Test
    void testEliminateGeneratedNotNullOnScanWithFewNulls() {
        LogicalOlapScan scan = newLogicalOlapScan(true);
        Slot scanSlot = scan.getOutput().get(0);
        LogicalPlan plan = new LogicalPlanBuilder(scan)
                .filter(ImmutableSet.of(new Not(new IsNull(scanSlot), true)))
                .build();
        putColumnNumNulls(1);

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new EliminateNotNull())
                .matches(logicalOlapScan());
    }

    @Test
    void testEliminateGeneratedNotNullOnScanWhenKeepingIsDisabled() {
        LogicalOlapScan scan = newLogicalOlapScan(true);
        Slot scanSlot = scan.getOutput().get(0);
        LogicalPlan plan = new LogicalPlanBuilder(scan)
                .filter(ImmutableSet.of(new Not(new IsNull(scanSlot), true)))
                .build();
        putColumnNumNulls(2_000_000);
        ConnectContext connectContext = MemoTestUtils.createConnectContext();
        connectContext.getSessionVariable().keepIsNotNullNumNullsThreshold = -1;

        PlanChecker.from(connectContext, plan)
                .applyTopDown(new EliminateNotNull())
                .matches(logicalOlapScan());
    }

    @Test
    void testEliminateGeneratedNotNullOnNotNullableScanColumn() {
        LogicalOlapScan scan = newLogicalOlapScan(false);
        Slot scanSlot = scan.getOutput().get(0);
        LogicalPlan plan = new LogicalPlanBuilder(scan)
                .filter(ImmutableSet.of(new Not(new IsNull(scanSlot), true)))
                .build();
        putColumnNumNulls(2_000_000);

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new EliminateNotNull())
                .matches(logicalOlapScan());
    }

    @Test
    void testEliminateGeneratedNotNullWhenAnotherPredicateImpliesNotNull() {
        LogicalOlapScan scan = newLogicalOlapScan(true);
        Slot scanSlot = scan.getOutput().get(0);
        Expression greaterThan = new GreaterThan(scanSlot, Literal.of(1));
        LogicalPlan plan = new LogicalPlanBuilder(scan)
                .filter(ImmutableSet.of(greaterThan, new Not(new IsNull(scanSlot), true)))
                .build();
        putColumnNumNulls(2_000_000);

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new EliminateNotNull())
                .matches(logicalFilter().when(filter -> filter.getConjuncts().size() == 1
                        && filter.getConjuncts().iterator().next() instanceof GreaterThan));
    }

    private static boolean isKeptIsNotNull(Expression expression) {
        return expression instanceof Not && !((Not) expression).isGeneratedIsNotNull()
                && TypeUtils.isNotNull(expression)
                        .map(slot -> SCAN_COLUMN.equals(slot.getName()))
                        .orElse(false);
    }

    private static LogicalOlapScan newLogicalOlapScan(boolean nullable) {
        Column column = new Column(SCAN_COLUMN, Type.INT, true, AggregateType.NONE, nullable, "0", "");
        OlapTable table = new OlapTable(SCAN_TABLE_ID, "t1", ImmutableList.of(column), KeysType.DUP_KEYS,
                new PartitionInfo(), new HashDistributionInfo(3, ImmutableList.of(column)));
        table.setIndexMeta(-1, "t1", table.getFullSchema(), 0, 0, (short) 0,
                TStorageType.COLUMN, KeysType.DUP_KEYS);
        return new LogicalOlapScan(PlanConstructor.getNextRelationId(), table, ImmutableList.of("db"));
    }

    /**
     * The table is not registered in the catalog, so the statistics are keyed by the invalid catalog id
     * and db id that the table resolves to.
     */
    private static void putColumnNumNulls(double numNulls) {
        ColumnStatistic statistic = new ColumnStatisticBuilder().setNumNulls(numNulls).build();
        Env.getCurrentEnv().getStatisticsCache()
                .updateColStatsCache(-1, -1, SCAN_TABLE_ID, -1, SCAN_COLUMN, statistic);
    }

    private Expression repeatAdd(Expression expression, int width) {
        if (width == 1) {
            return expression;
        }
        int leftWidth = width / 2;
        return new Add(repeatAdd(expression, leftWidth), repeatAdd(expression, width - leftWidth));
    }
}
