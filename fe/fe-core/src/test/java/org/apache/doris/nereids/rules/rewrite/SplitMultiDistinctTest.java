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

import org.apache.doris.nereids.trees.expressions.NullSafeEqual;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.util.MatchingUtils;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Test;

public class SplitMultiDistinctTest extends TestWithFeService implements MemoPatternMatchSupported {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        createTable("create table test.test_distinct_multi(a int, b int, c int, d varchar(10), e date)"
                + "distributed by hash(a) properties('replication_num'='1');");
        connectContext.setDatabase("test");
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        connectContext.getSessionVariable().setEnableParallelResultSink(false);
    }

    @Test
    void multiCountWithoutGby() {
        String sql = "select count(distinct b), count(distinct a) from test_distinct_multi";
        PlanChecker.from(connectContext).checkExplain(sql, planner -> {
            Plan plan = planner.getOptimizedPlan();
            MatchingUtils.assertMatches(plan,
                    physicalCTEAnchor(
                            physicalCTEProducer(any()),
                            physicalResultSink(

                                            physicalNestedLoopJoin(
                                                    physicalProject(
                                                    physicalHashAggregate(
                                                            physicalDistribute(
                                                                    physicalHashAggregate(
                                                                            physicalHashAggregate(
                                                                                    physicalDistribute(
                                                                                            physicalHashAggregate(any()))))))),
                                                    physicalDistribute(
                                                            physicalProject(
                                                            physicalHashAggregate(
                                                                    physicalDistribute(
                                                                            physicalHashAggregate(
                                                                                    physicalHashAggregate(
                                                                                            physicalDistribute(
                                                                                                    physicalHashAggregate(any()))))))))
                                            )

                            )
                    )
            );
        });
    }

    @Test
    void multiSumWithoutGby() {
        String sql = "select sum(distinct b), sum(distinct a) from test_distinct_multi";
        PlanChecker.from(connectContext).checkExplain(sql, planner -> {
            Plan plan = planner.getOptimizedPlan();
            MatchingUtils.assertMatches(plan,
                    physicalCTEAnchor(
                            physicalCTEProducer(any()),
                            physicalResultSink(

                                            physicalNestedLoopJoin(
                                                    physicalProject(
                                                    physicalHashAggregate(
                                                            physicalDistribute(
                                                                    physicalHashAggregate(
                                                                            physicalHashAggregate(
                                                                                    physicalDistribute(
                                                                                            physicalHashAggregate(any()))))))),
                                                    physicalDistribute(
                                                            physicalProject(
                                                            physicalHashAggregate(
                                                                    physicalDistribute(
                                                                            physicalHashAggregate(
                                                                                    physicalHashAggregate(
                                                                                            physicalDistribute(
                                                                                                    physicalHashAggregate(any()))))))))
                                            )

                            )
                    )
            );
        });
    }

    @Test
    void sumCountWithoutGby() {
        String sql = "select sum(distinct b), count(distinct a) from test_distinct_multi";
        PlanChecker.from(connectContext).checkExplain(sql, planner -> {
            Plan plan = planner.getOptimizedPlan();
            MatchingUtils.assertMatches(plan,
                    physicalCTEAnchor(
                        physicalCTEProducer(any()),
                        physicalResultSink(
                             physicalNestedLoopJoin(
                                 physicalProject(
                                     physicalHashAggregate(
                                         physicalDistribute(
                                             physicalHashAggregate(
                                                 physicalHashAggregate(
                                                     physicalDistribute(
                                                         physicalHashAggregate(any()))))))),
                                     physicalDistribute(
                                         physicalProject(
                                             physicalHashAggregate(
                                                 physicalDistribute(
                                                     physicalHashAggregate(
                                                         physicalHashAggregate(
                                                             physicalDistribute(
                                                                physicalHashAggregate(any()))))))))
                                )
                        )
                    )
            );
        });
    }

    @Test
    void countMultiColumnsWithoutGby() {
        String sql = "select count(distinct b,c), count(distinct a,b) from test_distinct_multi";
        PlanChecker.from(connectContext).checkExplain(sql, planner -> {
            Plan plan = planner.getOptimizedPlan();
            MatchingUtils.assertMatches(plan,
                    physicalCTEAnchor(
                            physicalCTEProducer(any()),
                            physicalResultSink(

                                            physicalNestedLoopJoin(
                                                    physicalProject(
                                                    physicalHashAggregate(
                                                            physicalDistribute(
                                                                    physicalHashAggregate(
                                                                            physicalHashAggregate(any()))))),
                                                    physicalDistribute(
                                                            physicalProject(
                                                            physicalHashAggregate(
                                                                    physicalDistribute(
                                                                            physicalHashAggregate(
                                                                                    physicalHashAggregate(any()))))))
                                            )

                            )
                    )
            );
        });
    }

    @Test
    void countMultiColumnsWithGby() {
        String sql = "select count(distinct b,c), count(distinct a,b) from test_distinct_multi group by d";
        PlanChecker.from(connectContext).checkExplain(sql, planner -> {
            Plan plan = planner.getOptimizedPlan();
            MatchingUtils.assertMatches(plan,
                    physicalCTEAnchor(
                            physicalCTEProducer(
                                    any()),
                            physicalResultSink(
                                    physicalDistribute(
                                            physicalProject(
                                                    physicalHashJoin(
                                                            physicalProject(
                                                            physicalHashAggregate(
                                                                    physicalHashAggregate(
                                                                            physicalDistribute(any())))),
                                                            physicalProject(
                                                            physicalHashAggregate(
                                                                    physicalHashAggregate(
                                                                            physicalDistribute(any()))))
                                                    ).when(join ->
                                                        join.getJoinType() == JoinType.INNER_JOIN && join.getHashJoinConjuncts().get(0) instanceof NullSafeEqual
                                                    )
                                            )
                                    )
                            )
                    )
            );
        });
    }
}
