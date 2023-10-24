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

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import java.util.List;

class PushdownTopNThroughJoinTest extends TestWithFeService implements MemoPatternMatchSupported {
    private static final LogicalOlapScan scan1 = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
    private static final LogicalOlapScan scan2 = PlanConstructor.newLogicalOlapScan(1, "t2", 0);

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");

        connectContext.setDatabase("default_cluster:test");

        createTable("CREATE TABLE `t1` (\n"
                + "  `k1` int(11) NOT NULL,\n"
                + "  `k2` int(11) NOT NULL\n"
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
    }

    @Test
    void testJoin() {
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.LEFT_OUTER_JOIN, Pair.of(0, 0))
                .topN(10, 0, ImmutableList.of(0))
                .build();
        PlanChecker.from(connectContext, plan)
                .applyTopDown(new PushdownTopNThroughJoin())
                .matches(
                        logicalTopN(
                                logicalJoin(
                                        logicalTopN().when(l -> l.getLimit() == 10 && l.getOffset() == 0),
                                        logicalOlapScan()
                                )
                        )
                );
    }

    @Test
    void testProject1() {
        List<NamedExpression> projectExpres = ImmutableList.of(scan1.getOutput().get(0),
                new Cast(scan1.getOutput().get(1), VarcharType.SYSTEM_DEFAULT).alias("cast"));
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.LEFT_OUTER_JOIN, Pair.of(0, 0))
                .projectExprs(projectExpres)
                .topN(10, 0, ImmutableList.of(0))
                .build();
        PlanChecker.from(connectContext, plan)
                .applyTopDown(new PushdownTopNThroughJoin())
                .matches(
                        logicalTopN(
                                logicalProject(
                                        logicalJoin(
                                                logicalTopN().when(l -> l.getLimit() == 10 && l.getOffset() == 0),
                                                logicalOlapScan()
                                        )
                                )
                        )
                );
    }

    @Test
    void testJoinSql() {
        PlanChecker.from(connectContext)
                .analyze("select * from t1 left join t2 on t1.k1 = t2.k1 order by t1.k1 limit 10")
                .rewrite()
                .matches(
                        logicalTopN(
                                logicalProject(
                                        logicalJoin(
                                                logicalTopN().when(l -> l.getLimit() == 10 && l.getOffset() == 0),
                                                logicalOlapScan()
                                        )
                                )
                        )
                );
    }

    @Test
    void testProjectSql() {
        PlanChecker.from(connectContext)
                .analyze(
                        "select t1.k1, cast(t1.k2 as varchar) from t1 left join t2 on t1.k1 = t2.k1 order by t1.k1 limit 10")
                .rewrite()
                .matches(
                        logicalTopN(
                                logicalProject(
                                        logicalJoin(
                                                logicalTopN().when(l -> l.getLimit() == 10 && l.getOffset() == 0),
                                                logicalProject(logicalOlapScan())
                                        )
                                )
                        )
                );
    }

    @Test
    void rejectTopNUseProjectComplexExpr() {
        List<NamedExpression> projectExpres = ImmutableList.of(
                (new Add(scan1.getOutput().get(0), scan1.getOutput().get(1))).alias("add")
        );
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.LEFT_OUTER_JOIN, Pair.of(0, 0))
                .projectExprs(projectExpres)
                .topN(10, 0, ImmutableList.of(0))
                .build();
        PlanChecker.from(connectContext, plan)
                .applyTopDown(new PushdownTopNThroughJoin())
                .matches(
                        logicalJoin(
                                logicalOlapScan(),
                                logicalOlapScan()
                        )
                );
    }

    @Test
    void rejectWrongJoinType() {
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.RIGHT_OUTER_JOIN, Pair.of(0, 0))
                .topN(10, 0, ImmutableList.of(0))
                .build();
        PlanChecker.from(connectContext, plan)
                .applyTopDown(new PushdownTopNThroughJoin())
                .matches(
                        logicalJoin(
                                logicalOlapScan(),
                                logicalOlapScan()
                        )
                );
    }
}
