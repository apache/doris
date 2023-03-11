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

package org.apache.doris.nereids.rules.rewrite.logical;

import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.pattern.PatternDescriptor;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.LimitPhase;
import org.apache.doris.nereids.trees.plans.ObjectId;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.RelationUtil;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

class PushdownLimitTest extends TestWithFeService implements MemoPatternMatchSupported {
    private Plan scanScore = new LogicalOlapScan(RelationUtil.newRelationId(), PlanConstructor.score);
    private Plan scanStudent = new LogicalOlapScan(RelationUtil.newRelationId(), PlanConstructor.student);

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");

        connectContext.setDatabase("default_cluster:test");

        createTable("CREATE TABLE `t1` (\n"
                + "  `k1` int(11) NULL,\n"
                + "  `k2` int(11) NULL\n"
                + ") ENGINE=OLAP\n"
                + "COMMENT 'OLAP'\n"
                + "DISTRIBUTED BY HASH(`k1`) BUCKETS 3\n"
                + "PROPERTIES (\n"
                + "\"replication_allocation\" = \"tag.location.default: 1\",\n"
                + "\"in_memory\" = \"false\",\n"
                + "\"storage_format\" = \"V2\",\n"
                + "\"disable_auto_compaction\" = \"false\"\n"
                + ");");

        createTable("CREATE TABLE `t2` (\n"
                + "  `k1` int(11) NULL,\n"
                + "  `k2` int(11) NULL\n"
                + ") ENGINE=OLAP\n"
                + "COMMENT 'OLAP'\n"
                + "DISTRIBUTED BY HASH(`k1`) BUCKETS 3\n"
                + "PROPERTIES (\n"
                + "\"replication_allocation\" = \"tag.location.default: 1\",\n"
                + "\"in_memory\" = \"false\",\n"
                + "\"storage_format\" = \"V2\",\n"
                + "\"disable_auto_compaction\" = \"false\"\n"
                + ");");

        createTable("CREATE TABLE `t3` (\n"
                + "  `k1` int(11) NULL,\n"
                + "  `k2` int(11) NULL\n"
                + ") ENGINE=OLAP\n"
                + "COMMENT 'OLAP'\n"
                + "DISTRIBUTED BY HASH(`k1`) BUCKETS 3\n"
                + "PROPERTIES (\n"
                + "\"replication_allocation\" = \"tag.location.default: 1\",\n"
                + "\"in_memory\" = \"false\",\n"
                + "\"storage_format\" = \"V2\",\n"
                + "\"disable_auto_compaction\" = \"false\"\n"
                + ");");
    }

    @Test
    public void testPushLimitThroughLeftJoin() {
        test(JoinType.LEFT_OUTER_JOIN, true,
                logicalLimit(
                        logicalProject(
                                logicalJoin(
                                        logicalLimit(logicalOlapScan().when(s -> s.equals(scanScore))),
                                        logicalOlapScan().when(s -> s.equals(scanStudent))
                                ).when(j -> j.getJoinType() == JoinType.LEFT_OUTER_JOIN)
                        )
                )
        );
        test(JoinType.LEFT_OUTER_JOIN, false,
                logicalLimit(
                        logicalJoin(
                                logicalLimit(logicalOlapScan().when(s -> s.equals(scanScore))),
                                logicalOlapScan().when(s -> s.equals(scanStudent))
                        ).when(j -> j.getJoinType() == JoinType.LEFT_OUTER_JOIN)
                )
        );
    }

    @Test
    public void testPushLimitThroughRightJoin() {
        // after use RelationUtil to allocate relation id, the id will increase when getNextId() called.
        test(JoinType.RIGHT_OUTER_JOIN, true,
                logicalLimit(
                        logicalProject(
                                logicalJoin(
                                        logicalOlapScan().when(s -> s.equals(scanScore)),
                                        logicalLimit(logicalOlapScan().when(s -> s.equals(scanStudent)))
                                ).when(j -> j.getJoinType() == JoinType.RIGHT_OUTER_JOIN)
                        )
                )
        );
        test(JoinType.RIGHT_OUTER_JOIN, false,
                logicalLimit(
                        logicalJoin(
                                logicalOlapScan().when(s -> s.equals(scanScore)),
                                logicalLimit(logicalOlapScan().when(s -> s.equals(scanStudent)))
                        ).when(j -> j.getJoinType() == JoinType.RIGHT_OUTER_JOIN)
                )
        );
    }

    @Test
    public void testPushLimitThroughCrossJoin() {
        test(JoinType.CROSS_JOIN, true,
                logicalLimit(
                        logicalProject(
                                logicalJoin(
                                        logicalLimit(logicalOlapScan().when(s -> s.equals(scanScore))),
                                        logicalLimit(logicalOlapScan().when(s -> s.equals(scanStudent)))
                                ).when(j -> j.getJoinType() == JoinType.CROSS_JOIN)
                        )
                )
        );
        test(JoinType.CROSS_JOIN, false,
                logicalLimit(
                        logicalJoin(
                                logicalLimit(logicalOlapScan().when(s -> s.equals(scanScore))),
                                logicalLimit(logicalOlapScan().when(s -> s.equals(scanStudent)))
                        ).when(j -> j.getJoinType() == JoinType.CROSS_JOIN)
                )
        );
    }

    @Test
    public void testPushLimitThroughInnerJoin() {
        test(JoinType.INNER_JOIN, true,
                logicalLimit(
                        logicalProject(
                                logicalJoin(
                                        logicalLimit(logicalOlapScan().when(s -> s.equals(scanScore))),
                                        logicalLimit(logicalOlapScan().when(s -> s.equals(scanStudent)))
                                )
                        )
                )
        );
        test(JoinType.INNER_JOIN, false,
                logicalLimit(
                        logicalJoin(
                                logicalLimit(logicalOlapScan().when(s -> s.equals(scanScore))),
                                logicalLimit(logicalOlapScan().when(s -> s.equals(scanStudent)))
                        )
                )
        );
    }

    @Test
    public void testTranslate() {
        PlanChecker.from(connectContext).checkPlannerResult("select * from t1 left join t2 on t1.k1=t2.k1 limit 5",
                planner -> {
                    List<PlanFragment> fragments = planner.getFragments();
                    Map<String, OlapScanNode> nameToScan = fragments.stream()
                            .flatMap(fragment -> {
                                Set<OlapScanNode> scans = Sets.newHashSet();
                                fragment.getPlanRoot().collect(OlapScanNode.class, scans);
                                return scans.stream();
                            })
                            .collect(Collectors.toMap(
                                    olapScanNode -> olapScanNode.getOlapTable().getName(),
                                    Function.identity(),
                                    // plan among fragments has duplicate elements.
                                    (s1, s2) -> s1)
                            );
                    // limit is push down to left scan of `t1`.
                    Assertions.assertEquals(2, nameToScan.size());
                    Assertions.assertEquals(5, nameToScan.get("t1").getLimit());
                }
        );
    }

    @Test
    public void testLimitPushSort() {
        PlanChecker.from(connectContext)
                .analyze("select k1 from t1 order by k1 limit 1")
                .rewrite()
                .matches(logicalTopN());
    }

    @Test
    public void testLimitPushUnion() {
        PlanChecker.from(connectContext)
                .analyze("select k1 from t1 "
                        + "union all select k2 from t2 "
                        + "union all select k1 from t3 "
                        + "limit 10")
                .rewrite()
                .matches(
                        logicalUnion(
                                logicalProject(
                                        logicalOlapScan().when(scan -> "t1".equals(scan.getTable().getName()))
                                ),
                                logicalProject(
                                        logicalOlapScan().when(scan -> "t2".equals(scan.getTable().getName()))
                                ),
                                logicalLimit(
                                        logicalLimit(
                                            logicalProject(
                                                    logicalOlapScan().when(scan -> "t3".equals(scan.getTable().getName()))
                                            )
                                        )
                                )
                        )
                );
    }

    private void test(JoinType joinType, boolean hasProject, PatternDescriptor<? extends Plan> pattern) {
        Plan plan = generatePlan(joinType, hasProject);
        PlanChecker.from(MemoTestUtils.createConnectContext())
                .analyze(plan)
                .applyTopDown(new ConvertInnerOrCrossJoin())
                .applyTopDown(new PushdownLimit())
                .matchesFromRoot(pattern);
    }

    private Plan generatePlan(JoinType joinType, boolean hasProject) {
        ImmutableList<Expression> joinConditions =
                joinType == JoinType.CROSS_JOIN || joinType == JoinType.INNER_JOIN
                        ? ImmutableList.of()
                        : ImmutableList.of(new EqualTo(new UnboundSlot("sid"), new UnboundSlot("id")));

        LogicalJoin<? extends Plan, ? extends Plan> join = new LogicalJoin<>(
                joinType,
                joinConditions,
                new LogicalOlapScan(new ObjectId(0), PlanConstructor.score),
                new LogicalOlapScan(new ObjectId(1), PlanConstructor.student)
        );

        if (hasProject) {
            // return limit -> project -> join
            return new LogicalLimit<>(10, 0, LimitPhase.ORIGIN, new LogicalProject<>(
                    ImmutableList.of(new UnboundSlot("sid"), new UnboundSlot("id")),
                    join));
        } else {
            // return limit -> join
            return new LogicalLimit<>(10, 0, LimitPhase.ORIGIN, join);
        }
    }
}
