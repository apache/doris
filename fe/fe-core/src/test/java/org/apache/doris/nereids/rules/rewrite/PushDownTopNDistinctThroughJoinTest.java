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

import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PushDownTopNDistinctThroughJoinTest implements MemoPatternMatchSupported {
    private static final PushDownTopNDistinctThroughJoin RULE = new PushDownTopNDistinctThroughJoin();
    private static final LogicalOlapScan LEFT_SCAN = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
    private static final LogicalOlapScan RIGHT_SCAN = PlanConstructor.newLogicalOlapScan(1, "t2", 0);
    private ConnectContext connectContext;

    @BeforeEach
    void setUp() {
        connectContext = new ConnectContext();
        connectContext.setThreadLocalInfo();
    }

    @AfterEach
    void tearDown() {
        ConnectContext.remove();
    }

    @Test
    void pushDirectShapeWhenAccumulatedPrefixDeterminesChildOutput() {
        NamedExpression id = LEFT_SCAN.getOutput().get(0);
        LogicalPlan left = new LogicalPlanBuilder(LEFT_SCAN)
                .projectExprs(ImmutableList.of(id, LEFT_SCAN.getOutput().get(1),
                        new Alias(new Add(id, new IntegerLiteral(1)), "id_plus_one")))
                .build();
        LogicalPlan plan = new LogicalPlanBuilder(left)
                .join(RIGHT_SCAN, JoinType.LEFT_OUTER_JOIN, Pair.of(0, 0))
                .distinct(ImmutableList.of(0, 1, 2, 3, 4))
                .topN(10, 0, ImmutableList.of(0, 1, 3))
                .build();

        PlanChecker.from(connectContext, plan)
                .applyTopDown(RULE)
                .matchesFromRoot(
                        logicalTopN(
                                logicalAggregate(
                                        logicalJoin(
                                                logicalTopN(logicalAggregate(logicalProject(logicalOlapScan())))
                                                        .when(topN -> topN.getLimit() == 10),
                                                logicalOlapScan()
                                        )
                                )
                        )
                );
    }

    @Test
    void pushAllSlotsProjectShapeForNonNullUniquePrefix() {
        LogicalOlapScan uniqueScan = newUniqueScan(2, "unique_not_null", false);
        LogicalPlan join = new LogicalPlanBuilder(uniqueScan)
                .join(RIGHT_SCAN, JoinType.LEFT_OUTER_JOIN, Pair.of(0, 0))
                .build();
        LogicalPlan plan = new LogicalPlanBuilder(join)
                .projectExprs(ImmutableList.<NamedExpression>builder().addAll(join.getOutput()).build())
                .distinct(ImmutableList.of(0, 1, 2, 3))
                .topN(10, 0, ImmutableList.of(0, 2))
                .build();

        PlanChecker.from(connectContext, plan)
                .applyTopDown(RULE)
                .matchesFromRoot(
                        logicalTopN(
                                logicalProject(
                                        logicalAggregate(
                                                logicalJoin(
                                                        logicalTopN(logicalAggregate(logicalOlapScan()))
                                                                .when(topN -> topN.getLimit() == 10),
                                                        logicalOlapScan()
                                                )
                                        )
                                )
                        )
                );
    }

    @Test
    void rejectNullableUniquePrefix() {
        LogicalOlapScan nullableUniqueScan = newUniqueScan(3, "unique_nullable", true);
        LogicalPlan plan = new LogicalPlanBuilder(nullableUniqueScan)
                .join(RIGHT_SCAN, JoinType.LEFT_OUTER_JOIN, Pair.of(0, 0))
                .distinct(ImmutableList.of(0, 1, 2, 3))
                .topN(10, 0, ImmutableList.of(0, 2))
                .build();

        PlanChecker.from(connectContext, plan)
                .applyTopDown(RULE)
                .matchesFromRoot(
                        logicalTopN(
                                logicalAggregate(
                                        logicalJoin(logicalOlapScan(), logicalOlapScan())
                                )
                        )
                );
    }

    private LogicalOlapScan newUniqueScan(long tableId, String tableName, boolean keyNullable) {
        OlapTable table = PlanConstructor.newOlapTable(tableId, tableName, 0, KeysType.UNIQUE_KEYS);
        table.getFullSchema().get(0).setIsAllowNull(keyNullable);
        table.getFullSchema().get(1).setIsKey(false);
        return new LogicalOlapScan(PlanConstructor.getNextRelationId(), table, ImmutableList.of("db"));
    }
}
