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

import org.apache.doris.nereids.properties.DistributionSpecHash;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.util.MatchingUtils;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Test;

public class SaltJoinTest extends TestWithFeService implements MemoPatternMatchSupported {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        createTable("create table test.test_skew9(a int,c varchar(100), b int) distributed by hash(a) buckets 32 properties(\"replication_num\"=\"1\");");
        createTable("create table test.test_skew10(a int,c varchar(100), b int) distributed by hash(a) buckets 32 properties(\"replication_num\"=\"1\");");
        connectContext.setDatabase("test");
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        connectContext.getSessionVariable().setParallelResultSink(false);
    }

    @Test
    public void testInnerJoin() {
        PlanChecker.from(connectContext)
                .analyze("select * from test_skew9 tl inner join [shuffle[skew(tl.b(1,2))]] test_skew10 tr on tl.b = tr.b")
                .rewrite()
                .printlnTree()
                .matches(
                        logicalJoin(
                                logicalProject(
                                        logicalOlapScan()),
                                logicalProject(
                                        logicalJoin(
                                                logicalProject(
                                                        logicalGenerate(
                                                                logicalUnion())),
                                                logicalOlapScan()
                                        ).when(join -> join.getJoinType() == JoinType.RIGHT_OUTER_JOIN))
                        ).when(join -> join.getHashJoinConjuncts().size() == 2 && join.getJoinType() == JoinType.INNER_JOIN)
                );
    }

    @Test
    public void testLeftJoin() {
        PlanChecker.from(connectContext)
                .analyze("select * from test_skew9 tl left join [shuffle[skew(tl.b(1,2))]] test_skew10 tr on tl.b = tr.b;")
                .rewrite()
                .printlnTree()
                .matches(
                        logicalJoin(
                                logicalProject(
                                        logicalOlapScan()),
                                logicalProject(
                                        logicalJoin(
                                                logicalProject(
                                                        logicalGenerate(
                                                                logicalUnion())),
                                                logicalOlapScan()
                                        ).when(join -> join.getJoinType() == JoinType.RIGHT_OUTER_JOIN))
                        ).when(join -> join.getHashJoinConjuncts().size() == 2 && join.getJoinType() == JoinType.LEFT_OUTER_JOIN)
                );
    }

    @Test
    public void testRightJoin() {
        PlanChecker.from(connectContext)
                .analyze("select * from test_skew9 tl right join [shuffle[skew(tr.b(1,2))]] test_skew10 tr on tl.b = tr.b;")
                .rewrite()
                .printlnTree()
                .matches(
                        logicalJoin(
                                logicalProject(
                                        logicalJoin(
                                                logicalProject(
                                                        logicalGenerate(
                                                                logicalUnion())),
                                                logicalOlapScan()
                                        ).when(join -> join.getJoinType() == JoinType.RIGHT_OUTER_JOIN)
                                ),
                                logicalProject(
                                        logicalOlapScan())
                        ).when(join -> join.getHashJoinConjuncts().size() == 2 && join.getJoinType() == JoinType.RIGHT_OUTER_JOIN)
                );
    }

    @Test
    public void testRightJoinPhysicalPlan() {
        String sql = "select * from test_skew9 tl right join [shuffle[skew(tr.b(1,2))]] test_skew10 tr on tl.b = tr.b;";
        PlanChecker.from(connectContext).checkExplain(sql, planner -> {
            Plan plan = planner.getOptimizedPlan();
            MatchingUtils.assertMatches(plan,
                    physicalResultSink(physicalDistribute(physicalProject(
                    physicalHashJoin(
                            physicalDistribute(
                                    physicalProject(
                                            physicalHashJoin(
                                                    physicalDistribute(
                                                            physicalProject(
                                                                    physicalGenerate()
                                                            )
                                                    ),
                                                    physicalDistribute(physicalOlapScan())
                                            )
                                    )
                            ).when(dis -> dis.getDistributionSpec() instanceof DistributionSpecHash
                                    && ((DistributionSpecHash) dis.getDistributionSpec()).getOrderedShuffledColumns().size() == 2),
                            physicalDistribute(
                                    physicalProject(
                                            physicalOlapScan())
                            ).when(dis -> dis.getDistributionSpec() instanceof DistributionSpecHash
                                    && ((DistributionSpecHash) dis.getDistributionSpec()).getOrderedShuffledColumns().size() == 2)
                    ))))
            );
        });
    }

    @Test void testSkewValueIsNull() {
        PlanChecker.from(connectContext)
                .analyze("select * from test_skew9 tl inner join [shuffle[skew(tl.b(null))]] test_skew10 tr on tl.b = tr.b")
                .rewrite()
                .printlnTree()
                .matches(
                        logicalJoin(
                                logicalOlapScan(),
                                logicalOlapScan()
                        ).when(join -> join.getHashJoinConjuncts().size() == 1 && join.getJoinType() == JoinType.INNER_JOIN)
                );
    }

    @Test void testLeftJoinSkewValueIsNull() {
        PlanChecker.from(connectContext)
                .analyze("select * from test_skew9 tl left join [shuffle[skew(tl.b(null))]] test_skew10 tr on tl.b = tr.b")
                .rewrite()
                .printlnTree()
                .matches(
                        logicalJoin(
                                logicalProject(
                                        logicalOlapScan()),
                                logicalProject(
                                        logicalOlapScan()
                                )
                        ).when(join -> join.getHashJoinConjuncts().size() == 2 && join.getJoinType() == JoinType.LEFT_OUTER_JOIN)
                );
    }

    @Test void testRightJoinSkewValueIsNull() {
        PlanChecker.from(connectContext)
                .analyze("select * from test_skew9 tl right join [shuffle[skew(tr.b(null))]] test_skew10 tr on tl.b = tr.b")
                .rewrite()
                .printlnTree()
                .matches(
                        logicalJoin(
                                logicalProject(
                                                logicalOlapScan()
                                ),
                                logicalProject(
                                                logicalOlapScan())
                        ).when(join -> join.getHashJoinConjuncts().size() == 2 && join.getJoinType() == JoinType.RIGHT_OUTER_JOIN)
                );
    }

    @Test
    public void testLeading() {
        PlanChecker.from(connectContext)
                .analyze("select /*+leading(tl shuffle [skew(tr.b(1,2))] tr) */ * from test_skew9 tl right join test_skew10 tr on tl.b=tr.b;")
                .rewrite()
                .printlnTree()
                .matches(
                        logicalJoin(
                                logicalProject(
                                        logicalJoin(
                                                logicalProject(
                                                        logicalGenerate(
                                                                logicalUnion())),
                                                logicalOlapScan()
                                        ).when(join -> join.getJoinType() == JoinType.RIGHT_OUTER_JOIN)
                                ),
                                logicalProject(
                                        logicalOlapScan())
                        ).when(join -> join.getHashJoinConjuncts().size() == 2 && join.getJoinType() == JoinType.RIGHT_OUTER_JOIN)
                );
    }

    @Test
    public void testMultiLeading() {
        PlanChecker.from(connectContext)
                .analyze("select /*+leading(tl shuffle [skew(tl.c(\"abc\",\"def\"))] tr shuffle[skew(tl.c(\"abc\",\"def\"))] tt) */ * from \n"
                        + "    test_skew9 tl join test_skew10 tr on tl.c=tr.c and tl.a=tr.a inner join test_skew10 tt on tl.c = tt.c")
                .rewrite()
                .printlnTree()
                .matches(
                        logicalJoin(
                            logicalProject(
                                    logicalJoin(
                                            logicalProject(
                                                    logicalOlapScan().when(scan -> scan.getTable().getName().equals("test_skew9"))
                                            ),
                                            logicalProject(
                                                    logicalJoin(
                                                            logicalProject(
                                                                    logicalGenerate(
                                                                            logicalUnion()
                                                                    )
                                                            ),
                                                            logicalOlapScan().when(scan -> scan.getTable().getName().equals("test_skew10"))
                                                    ).when(join -> join.getJoinType() == JoinType.RIGHT_OUTER_JOIN)
                                            )
                                    ).when(join -> join.getHashJoinConjuncts().size() == 3 && join.getJoinType() == JoinType.INNER_JOIN)
                            ),
                            logicalProject(
                                    logicalJoin(
                                            logicalProject(
                                                    logicalGenerate(
                                                            logicalUnion()
                                                    )
                                            ),
                                            logicalOlapScan().when(scan -> scan.getTable().getName().equals("test_skew10"))
                                    ).when(join -> join.getJoinType() == JoinType.RIGHT_OUTER_JOIN)
                            )
                    ).when(join -> join.getHashJoinConjuncts().size() == 2 && join.getJoinType() == JoinType.INNER_JOIN));
    }

    @Test
    public void testMultiWithBracket() {
        PlanChecker.from(connectContext)
                .analyze("select /*+leading(tl shuffle [skew(tl.c(\"abc\",\"def\"))] {tr shuffle[skew(tr.c(\"abc\",\"def\"))] tt}) */ * from \n"
                        + "    test_skew9 tl join test_skew10 tr on tl.c=tr.c and tl.a=tr.a inner join test_skew10 tt on tr.c = tt.c")
                .rewrite()
                .printlnTree()
                .matches(
                        logicalJoin(
                                logicalProject(
                                        logicalOlapScan().when(scan -> scan.getTable().getName().equals("test_skew9"))
                                ),
                                logicalProject(
                                        logicalJoin(
                                                logicalProject(
                                                        logicalGenerate(
                                                                logicalUnion()
                                                        )
                                                ),
                                                logicalProject(
                                                        logicalJoin(
                                                                logicalProject(
                                                                        logicalOlapScan().when(scan -> scan.getTable().getName().equals("test_skew10"))
                                                                ),
                                                                logicalProject(
                                                                        logicalJoin(
                                                                                logicalProject(
                                                                                        logicalGenerate(
                                                                                                logicalUnion()
                                                                                        )
                                                                                ),
                                                                                logicalOlapScan().when(scan -> scan.getTable().getName().equals("test_skew10"))
                                                                        ).when(join -> join.getJoinType() == JoinType.RIGHT_OUTER_JOIN)
                                                                )
                                                        ).when(join -> join.getHashJoinConjuncts().size() == 2 && join.getJoinType() == JoinType.INNER_JOIN)
                                                )
                                        ).when(join -> join.getJoinType() == JoinType.RIGHT_OUTER_JOIN)
                                )
                        ).when(join -> join.getHashJoinConjuncts().size() == 3 && join.getJoinType() == JoinType.INNER_JOIN)
                );
    }
}
