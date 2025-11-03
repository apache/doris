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

package org.apache.doris.nereids.rules.exploration.join;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.rules.rewrite.PushDownAliasThroughJoin;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Substring;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Objects;

class InnerJoinLAsscomProjectTest implements MemoPatternMatchSupported {

    private final LogicalOlapScan scan1 = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
    private final LogicalOlapScan scan2 = PlanConstructor.newLogicalOlapScan(1, "t2", 0);
    private final LogicalOlapScan scan3 = PlanConstructor.newLogicalOlapScan(2, "t3", 0);

    @Test
    void testSimple() {
        /*
         * Star-Join
         * t1 -- t2
         * |
         * t3
         * <p>
         *     t1.id=t3.id               t1.id=t2.id
         *       topJoin                  newTopJoin
         *       /     \                   /     \
         *    project   t3           project    project
         * t1.id=t2.id             t1.id=t3.id    t2
         * bottomJoin       -->   newBottomJoin
         *   /    \                   /    \
         * t1      t2               t1      t3
         */
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.INNER_JOIN, Pair.of(0, 0))
                .project(ImmutableList.of(0, 1, 2))
                .join(scan3, JoinType.INNER_JOIN, Pair.of(1, 1))
                .projectAll()
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyExploration(InnerJoinLAsscomProject.INSTANCE.build())
                .printlnExploration()
                .matchesExploration(
                    logicalProject(
                        logicalJoin(
                            logicalProject(logicalJoin(
                                    logicalOlapScan().when(scan -> scan.getTable().getName().equals("t1")),
                                    logicalOlapScan().when(scan -> scan.getTable().getName().equals("t3"))
                            )),
                            logicalProject(
                                    logicalOlapScan().when(scan -> scan.getTable().getName().equals("t2"))
                            ).when(project -> project.getProjects().size() == 1)
                        )
                    )
                );
    }

    @Test
    @Disabled
    void testAlias() {
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.INNER_JOIN, Pair.of(0, 0))
                .alias(ImmutableList.of(0, 2), ImmutableList.of("t1.id", "t2.id"))
                .join(scan3, JoinType.INNER_JOIN, Pair.of(0, 0))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new PushDownAliasThroughJoin())
                .applyExploration(InnerJoinLAsscomProject.INSTANCE.build())
                .printlnExploration()
                .matchesExploration(
                        logicalJoin(
                                logicalProject(
                                        logicalJoin(
                                                logicalProject(logicalOlapScan().when(scan -> scan.getTable().getName().equals("t1"))),
                                                logicalOlapScan().when(scan -> scan.getTable().getName().equals("t3"))
                                        )
                                ).when(project -> project.getProjects().size() == 3),
                                // t1.id Add t3.id, t3.name
                                logicalProject(
                                        logicalProject(
                                                logicalOlapScan().when(scan -> scan.getTable().getName().equals("t2"))
                                        )
                                ).when(project -> project.getProjects().size() == 1)
                        )
                );
    }

    @Test
    @Disabled
    public void testHashAndOther() {
        // Alias (scan1 join scan2 on scan1.id=scan2.id and scan1.name>scan2.name);
        List<Expression> bottomHashJoinConjunct = ImmutableList.of(
                new EqualTo(scan1.getOutput().get(0), scan2.getOutput().get(0)));
        List<Expression> bottomOtherJoinConjunct = ImmutableList.of(
                new GreaterThan(scan1.getOutput().get(1), scan2.getOutput().get(1)));
        LogicalPlan bottomJoin = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.INNER_JOIN, bottomHashJoinConjunct, bottomOtherJoinConjunct)
                .alias(ImmutableList.of(0, 1, 2, 3), ImmutableList.of("t1.id", "t1.name", "t2.id", "t2.name"))
                .build();

        List<Expression> topHashJoinConjunct = ImmutableList.of(
                new EqualTo(bottomJoin.getOutput().get(0), scan3.getOutput().get(0)),
                new EqualTo(bottomJoin.getOutput().get(2), scan3.getOutput().get(0)));
        List<Expression> topOtherJoinConjunct = ImmutableList.of(
                new GreaterThan(bottomJoin.getOutput().get(1), scan3.getOutput().get(1)),
                new GreaterThan(bottomJoin.getOutput().get(3), scan3.getOutput().get(1)));
        LogicalPlan topJoin = new LogicalPlanBuilder(bottomJoin)
                .join(scan3, JoinType.INNER_JOIN, topHashJoinConjunct, topOtherJoinConjunct)
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), topJoin)
                .printlnTree()
                .applyTopDown(new PushDownAliasThroughJoin())
                .applyExploration(InnerJoinLAsscomProject.INSTANCE.build())
                .printlnExploration()
                .matchesExploration(
                        innerLogicalJoin(
                                innerLogicalJoin().when(join -> join.getHashJoinConjuncts().size() == 1
                                        && join.getOtherJoinConjuncts().size() == 1),
                                group()
                        ).when(join -> Objects.equals(join.getHashJoinConjuncts().toString(),
                                "[(t2.id#6 = id#8), (t1.id#4 = t2.id#6)]")
                                && Objects.equals(join.getOtherJoinConjuncts().toString(),
                                "[(t2.name#7 > name#9), (t1.name#5 > t2.name#7)]"))
                );
    }

    /**
     * <pre>
     * LogicalJoin ( type=INNER_JOIN, hashJoinConjuncts=[(t1.id#4 = id#8), (t2.id#6 = (t1.id#4 + id#8))], otherJoinConjuncts=[(t1.name#5 > name#9), (t2.id#6 > (t1.id#4 + id#8))] )
     * |--LogicalProject ( projects=[id#0 AS `t1.id`#4, name#1 AS `t1.name`#5, id#2 AS `t2.id`#6, name#3 AS `t2.name`#7] )
     * |  +--LogicalJoin ( type=INNER_JOIN, hashJoinConjuncts=[(id#0 = id#2)], otherJoinConjuncts=[(name#1 > name#3)] )
     * |     |--LogicalOlapScan ( qualified=db.t1, output=[id#0, name#1], candidateIndexIds=[], selectedIndexId=-1, preAgg=ON )
     * |     +--LogicalOlapScan ( qualified=db.t2, output=[id#2, name#3], candidateIndexIds=[], selectedIndexId=-1, preAgg=ON )
     * +--LogicalOlapScan ( qualified=db.t3, output=[id#8, name#9], candidateIndexIds=[], selectedIndexId=-1, preAgg=ON )
     * -----------------------------
     * LogicalJoin ( type=INNER_JOIN, hashJoinConjuncts=[(t2.id#6 = (t1.id#4 + id#8)), (t1.id#4 = t2.id#6)], otherJoinConjuncts=[(t2.id#6 > (t1.id#4 + id#8)), (t1.name#5 > t2.name#7)] )
     * |--LogicalProject ( projects=[id#0 AS `t1.id`#4, name#1 AS `t1.name`#5, id#8, name#9] )
     * |  +--LogicalJoin ( type=INNER_JOIN, hashJoinConjuncts=[(id#0 = id#8)], otherJoinConjuncts=[(name#1 > name#9)] )
     * |     |--LogicalOlapScan ( qualified=db.t1, output=[id#0, name#1], candidateIndexIds=[], selectedIndexId=-1, preAgg=ON )
     * |     +--LogicalOlapScan ( qualified=db.t3, output=[id#8, name#9], candidateIndexIds=[], selectedIndexId=-1, preAgg=ON )
     * +--LogicalProject ( projects=[id#2 AS `t2.id`#6, name#3 AS `t2.name`#7] )
     *    +--LogicalOlapScan ( qualified=db.t2, output=[id#2, name#3], candidateIndexIds=[], selectedIndexId=-1, preAgg=ON )
     * </pre>
     */
    @Test
    @Disabled
    public void testComplexConjuncts() {
        // TODO: move to sql-test
        // Alias (scan1 join scan2 on scan1.id=scan2.id and scan1.name>scan2.name);
        List<Expression> bottomHashJoinConjunct = ImmutableList.of(
                new EqualTo(scan1.getOutput().get(0), scan2.getOutput().get(0)));
        List<Expression> bottomOtherJoinConjunct = ImmutableList.of(
                new GreaterThan(scan1.getOutput().get(1), scan2.getOutput().get(1)));
        LogicalPlan bottomJoin = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.INNER_JOIN, bottomHashJoinConjunct, bottomOtherJoinConjunct)
                .alias(ImmutableList.of(0, 1, 2, 3), ImmutableList.of("t1.id", "t1.name", "t2.id", "t2.name"))
                .build();

        List<Expression> topHashJoinConjunct = ImmutableList.of(
                new EqualTo(bottomJoin.getOutput().get(0), scan3.getOutput().get(0)),
                new EqualTo(bottomJoin.getOutput().get(2),
                        new Add(bottomJoin.getOutput().get(0), scan3.getOutput().get(0))));
        List<Expression> topOtherJoinConjunct = ImmutableList.of(
                new GreaterThan(bottomJoin.getOutput().get(1), scan3.getOutput().get(1)),
                new GreaterThan(bottomJoin.getOutput().get(2),
                        new Add(bottomJoin.getOutput().get(0), scan3.getOutput().get(0))));
        LogicalPlan topJoin = new LogicalPlanBuilder(bottomJoin)
                .join(scan3, JoinType.INNER_JOIN, topHashJoinConjunct, topOtherJoinConjunct)
                .build();

        // test for no exception
        PlanChecker.from(MemoTestUtils.createConnectContext(), topJoin)
                .applyExploration(InnerJoinLAsscomProject.INSTANCE.build())
                .matchesExploration(
                        innerLogicalJoin(
                                logicalProject(
                                        innerLogicalJoin().when(
                                                join -> Objects.equals(join.getHashJoinConjuncts().toString(),
                                                        "[(id#0 = id#8)]")
                                                        && Objects.equals(join.getOtherJoinConjuncts().toString(),
                                                        "[(name#1 > name#9)]"))),
                                group()
                        ).when(join -> Objects.equals(join.getHashJoinConjuncts().toString(),
                                "[(t2.id#6 = (t1.id#4 + id#8)), (t1.id#4 = t2.id#6)]")
                                && Objects.equals(join.getOtherJoinConjuncts().toString(),
                                "[(t2.id#6 > (t1.id#4 + id#8)), (t1.name#5 > t2.name#7)]"))
                );
    }

    /**
     * <pre>
     * LogicalJoin ( type=INNER_JOIN, hashJoinConjuncts=[(t1.id#4 = id#8), (t2.id#6 = (t1.id#4 + id#8))], otherJoinConjuncts=[(t1.name#5 > name#9), ( not (substring(t1.name#5, CAST('1' AS INT), CAST('3' AS INT)) = 'abc'))] )
     * |--LogicalProject ( projects=[id#0 AS `t1.id`#4, name#1 AS `t1.name`#5, id#2 AS `t2.id`#6, name#3 AS `t2.name`#7] )
     * |  +--LogicalJoin ( type=INNER_JOIN, hashJoinConjuncts=[(id#0 = id#2)], otherJoinConjuncts=[(name#1 > name#3)] )
     * |     |--LogicalOlapScan ( qualified=db.t1, output=[id#0, name#1], candidateIndexIds=[], selectedIndexId=-1, preAgg=ON )
     * |     +--LogicalOlapScan ( qualified=db.t2, output=[id#2, name#3], candidateIndexIds=[], selectedIndexId=-1, preAgg=ON )
     * +--LogicalOlapScan ( qualified=db.t3, output=[id#8, name#9], candidateIndexIds=[], selectedIndexId=-1, preAgg=ON )
     * -----------------------------
     * LogicalJoin ( type=INNER_JOIN, hashJoinConjuncts=[(t2.id#6 = (t1.id#4 + id#8)), (t1.id#4 = t2.id#6)], otherJoinConjuncts=[(t1.name#5 > t2.name#7)] )
     * |--LogicalProject ( projects=[id#0 AS `t1.id`#4, name#1 AS `t1.name`#5, id#8, name#9] )
     * |  +--LogicalJoin ( type=INNER_JOIN, hashJoinConjuncts=[(id#0 = id#8)], otherJoinConjuncts=[(name#1 > name#9), ( not (substring(name#1, CAST('1' AS INT), CAST('3' AS INT)) = 'abc'))] )
     * |     |--LogicalOlapScan ( qualified=db.t1, output=[id#0, name#1], candidateIndexIds=[], selectedIndexId=-1, preAgg=ON )
     * |     +--LogicalOlapScan ( qualified=db.t3, output=[id#8, name#9], candidateIndexIds=[], selectedIndexId=-1, preAgg=ON )
     * +--LogicalProject ( projects=[id#2 AS `t2.id`#6, name#3 AS `t2.name`#7] )
     *    +--LogicalOlapScan ( qualified=db.t2, output=[id#2, name#3], candidateIndexIds=[], selectedIndexId=-1, preAgg=ON )
     * </pre>
     */
    @Test
    @Disabled
    public void testComplexConjunctsWithSubString() {
        // TODO: move to sql-test
        // Alias (scan1 join scan2 on scan1.id=scan2.id and scan1.name>scan2.name);
        List<Expression> bottomHashJoinConjunct = ImmutableList.of(
                new EqualTo(scan1.getOutput().get(0), scan2.getOutput().get(0)));
        List<Expression> bottomOtherJoinConjunct = ImmutableList.of(
                new GreaterThan(scan1.getOutput().get(1), scan2.getOutput().get(1)));
        LogicalPlan bottomJoin = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.INNER_JOIN, bottomHashJoinConjunct, bottomOtherJoinConjunct)
                .alias(ImmutableList.of(0, 1, 2, 3), ImmutableList.of("t1.id", "t1.name", "t2.id", "t2.name"))
                .build();

        List<Expression> topHashJoinConjunct = ImmutableList.of(
                new EqualTo(bottomJoin.getOutput().get(0), scan3.getOutput().get(0)),
                new EqualTo(bottomJoin.getOutput().get(2),
                        new Add(bottomJoin.getOutput().get(0), scan3.getOutput().get(0))));
        // substring(t3.name, 5, 10) != '123456'
        List<Expression> topOtherJoinConjunct = ImmutableList.of(
                new GreaterThan(bottomJoin.getOutput().get(1), scan3.getOutput().get(1)),
                new Not(new EqualTo(new Substring(bottomJoin.getOutput().get(1),
                        new Cast(new StringLiteral("1"), IntegerType.INSTANCE),
                        new Cast(new StringLiteral("3"), IntegerType.INSTANCE)),
                        Literal.of("abc"))));
        LogicalPlan topJoin = new LogicalPlanBuilder(bottomJoin)
                .join(scan3, JoinType.INNER_JOIN, topHashJoinConjunct, topOtherJoinConjunct)
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), topJoin)
                .applyExploration(InnerJoinLAsscomProject.INSTANCE.build())
                .matchesExploration(
                        innerLogicalJoin(
                                logicalProject(
                                        innerLogicalJoin().when(
                                                join -> Objects.equals(join.getHashJoinConjuncts().toString(),
                                                        "[(id#0 = id#8)]")
                                                        && Objects.equals(join.getOtherJoinConjuncts().toString(),
                                                        "[(name#1 > name#9), ( not (substring(name#1, cast('1' as INT), cast('3' as INT)) = 'abc'))]"))),
                                group()
                        ).when(join -> Objects.equals(join.getHashJoinConjuncts().toString(),
                                "[(t2.id#6 = (t1.id#4 + id#8)), (t1.id#4 = t2.id#6)]")
                                && Objects.equals(join.getOtherJoinConjuncts().toString(), "[(t1.name#5 > t2.name#7)]"))
                );
    }

    /**
     * <pre>
     * LogicalJoin ( type=INNER_JOIN, hashJoinConjuncts=[(t1.id#4 = id#8), ((t1.id#4 + t1.name#5) = name#9)], otherJoinConjuncts=[((t1.id#4 + t1.name#5) > name#9)] )
     * |--LogicalProject ( projects=[id#0 AS `t1.id`#4, name#1 AS `t1.name`#5, id#2 AS `t2.id`#6, name#3 AS `t2.name`#7] )
     * |  +--LogicalJoin ( type=INNER_JOIN, hashJoinConjuncts=[(id#0 = id#2)], otherJoinConjuncts=[(name#1 > name#3)] )
     * |     |--LogicalOlapScan ( qualified=db.t1, output=[id#0, name#1], candidateIndexIds=[], selectedIndexId=-1, preAgg=ON )
     * |     +--LogicalOlapScan ( qualified=db.t2, output=[id#2, name#3], candidateIndexIds=[], selectedIndexId=-1, preAgg=ON )
     * +--LogicalOlapScan ( qualified=db.t3, output=[id#8, name#9], candidateIndexIds=[], selectedIndexId=-1, preAgg=ON )
     * -----------------------------
     * LogicalJoin ( type=INNER_JOIN, hashJoinConjuncts=[(t1.id#4 = t2.id#6)], otherJoinConjuncts=[(t1.name#5 > t2.name#7)] )
     * |--LogicalProject ( projects=[id#0 AS `t1.id`#4, name#1 AS `t1.name`#5, id#8, name#9] )
     * |  +--LogicalJoin ( type=INNER_JOIN, hashJoinConjuncts=[(id#0 = id#8), ((id#0 + name#1) = name#9)], otherJoinConjuncts=[((id#0 + name#1) > name#9)] )
     * |     |--LogicalOlapScan ( qualified=db.t1, output=[id#0, name#1], candidateIndexIds=[], selectedIndexId=-1, preAgg=ON )
     * |     +--LogicalOlapScan ( qualified=db.t3, output=[id#8, name#9], candidateIndexIds=[], selectedIndexId=-1, preAgg=ON )
     * +--LogicalProject ( projects=[id#2 AS `t2.id`#6, name#3 AS `t2.name`#7] )
     *    +--LogicalOlapScan ( qualified=db.t2, output=[id#2, name#3], candidateIndexIds=[], selectedIndexId=-1, preAgg=ON )
     * </pre>
     */
    @Test
    @Disabled
    public void testComplexConjunctsAndAlias() {
        // TODO: move to sql-test
        // Alias (scan1 join scan2 on scan1.id=scan2.id and scan1.name>scan2.name);
        List<Expression> bottomHashJoinConjunct = ImmutableList.of(
                new EqualTo(scan1.getOutput().get(0), scan2.getOutput().get(0)));
        List<Expression> bottomOtherJoinConjunct = ImmutableList.of(
                new GreaterThan(scan1.getOutput().get(1), scan2.getOutput().get(1)));
        LogicalPlan bottomJoin = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.INNER_JOIN, bottomHashJoinConjunct, bottomOtherJoinConjunct)
                .alias(ImmutableList.of(0, 1, 2, 3), ImmutableList.of("t1.id", "t1.name", "t2.id", "t2.name"))
                .build();

        List<Expression> topHashJoinConjunct = ImmutableList.of(
                new EqualTo(bottomJoin.getOutput().get(0), scan3.getOutput().get(0)),
                new EqualTo(new Add(bottomJoin.getOutput().get(0), bottomJoin.getOutput().get(1)),
                        scan3.getOutput().get(1)));
        List<Expression> topOtherJoinConjunct = ImmutableList.of(
                new GreaterThan(new Add(bottomJoin.getOutput().get(0), bottomJoin.getOutput().get(1)),
                        scan3.getOutput().get(1)));
        LogicalPlan topJoin = new LogicalPlanBuilder(bottomJoin)
                .join(scan3, JoinType.INNER_JOIN, topHashJoinConjunct, topOtherJoinConjunct)
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), topJoin)
                .applyExploration(InnerJoinLAsscomProject.INSTANCE.build())
                .matchesExploration(
                        innerLogicalJoin(
                                logicalProject(
                                        innerLogicalJoin().when(
                                                join -> Objects.equals(join.getHashJoinConjuncts().toString(),
                                                        "[(id#0 = id#8), ((id#0 + name#1) = name#9)]")
                                                        && Objects.equals(join.getOtherJoinConjuncts().toString(),
                                                        "[((id#0 + name#1) > name#9)]"))),
                                group()
                        ).when(join -> Objects.equals(join.getHashJoinConjuncts().toString(), "[(t1.id#4 = t2.id#6)]")
                                && Objects.equals(join.getOtherJoinConjuncts().toString(), "[(t1.name#5 > t2.name#7)]"))
                )
                .printlnExploration();
    }
}
