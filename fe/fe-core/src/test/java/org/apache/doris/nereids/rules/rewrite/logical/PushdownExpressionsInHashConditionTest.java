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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.NamedExpressionUtil;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.util.PatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import java.util.List;

public class PushdownExpressionsInHashConditionTest extends TestWithFeService implements PatternMatchSupported {
    private final List<String> testSql = ImmutableList.of(
            "SELECT * FROM T1 JOIN T2 ON T1.ID + 1 = T2.ID + 2 AND T1.ID + 1 > 2",
            "SELECT * FROM (SELECT * FROM T1) X JOIN (SELECT * FROM T2) Y ON X.ID + 1 = Y.ID + 2 AND X.ID + 1 > 2",
            "SELECT * FROM T1 JOIN (SELECT ID, SUM(SCORE) SCORE FROM T2 GROUP BY ID) T ON T1.ID + 1 = T.ID AND T.SCORE < 10",
            "SELECT * FROM T1 JOIN (SELECT ID, SUM(SCORE) SCORE FROM T2 GROUP BY ID ORDER BY ID) T ON T1.ID + 1 = T.ID AND T.SCORE < 10"
    );

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        connectContext.setDatabase("default_cluster:test");

        createTables(
                "CREATE TABLE IF NOT EXISTS T1 (\n"
                        + "    id bigint,\n"
                        + "    score bigint\n"
                        + ")\n"
                        + "DUPLICATE KEY(id)\n"
                        + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                        + "PROPERTIES (\n"
                        + "  \"replication_num\" = \"1\"\n"
                        + ")\n",
                "CREATE TABLE IF NOT EXISTS T2 (\n"
                        + "    id bigint,\n"
                        + "    score bigint\n"
                        + ")\n"
                        + "DUPLICATE KEY(id)\n"
                        + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                        + "PROPERTIES (\n"
                        + "  \"replication_num\" = \"1\"\n"
                        + ")\n"
        );
    }

    @Override
    protected void runBeforeEach() throws Exception {
        NamedExpressionUtil.clear();
    }

    @Test
    public void testGeneratePhysicalPlan() {
        List<String> testSql = ImmutableList.of(
                "SELECT * FROM T1 JOIN T2 ON T1.ID + 1 = T2.ID + 2 AND T1.ID + 1 > 2",
                "SELECT * FROM (SELECT * FROM T1) X JOIN (SELECT * FROM T2) Y ON X.ID + 1 = Y.ID + 2 AND X.ID + 1 > 2",
                "SELECT * FROM T1 JOIN (SELECT ID, SUM(SCORE) SCORE FROM T2 GROUP BY ID) T ON T1.ID + 1 = T.ID AND T.SCORE < 10",
                "SELECT * FROM T1 JOIN (SELECT ID, SUM(SCORE) SCORE FROM T2 GROUP BY ID ORDER BY ID) T ON T1.ID + 1 = T.ID AND T.SCORE < 10"
        );
        testSql.forEach(sql -> {
            try {
                PhysicalPlan plan = new NereidsPlanner(createStatementCtx(sql)).plan(
                        new NereidsParser().parseSingle(sql),
                        PhysicalProperties.ANY
                );
                System.out.println(plan.treeString());
            } catch (AnalysisException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void testSimpleCase() {
        PlanChecker.from(connectContext)
                .analyze("SELECT * FROM T1 JOIN T2 ON T1.ID + 1 = T2.ID + 2 AND T1.ID + 1 > 2")
                .applyTopDown(new FindHashConditionForJoin())
                .applyTopDown(new PushdownExpressionsInHashCondition())
                .matches(
                        logicalProject(
                                logicalJoin(
                                        logicalProject(
                                                logicalOlapScan()
                                        ),
                                        logicalProject(
                                                logicalOlapScan()
                                        )
                                )
                        )
                );
    }

    @Test
    public void testSubQueryCase() {
        PlanChecker.from(connectContext)
                .analyze(
                        "SELECT * FROM (SELECT * FROM T1) X JOIN (SELECT * FROM T2) Y ON X.ID + 1 = Y.ID + 2 AND X.ID + 1 > 2")
                .applyTopDown(new FindHashConditionForJoin())
                .applyTopDown(new PushdownExpressionsInHashCondition())
                .matches(
                        logicalProject(
                                logicalJoin(
                                        logicalProject(
                                                logicalProject(
                                                        logicalOlapScan()
                                                )
                                        ),
                                        logicalProject(
                                                logicalProject(
                                                        logicalOlapScan()
                                                )
                                        )
                                )
                        )
                );
    }

    @Test
    public void testAggNodeCase() {
        PlanChecker.from(connectContext)
                .analyze(
                        "SELECT * FROM T1 JOIN (SELECT ID, SUM(SCORE) SCORE FROM T2 GROUP BY ID) T ON T1.ID + 1 = T.ID AND T.SCORE = T1.SCORE + 10")
                .applyTopDown(new FindHashConditionForJoin())
                .applyTopDown(new PushdownExpressionsInHashCondition())
                .matches(
                        logicalProject(
                                logicalJoin(
                                        logicalProject(
                                                logicalOlapScan()
                                        ),
                                        logicalProject(
                                                logicalAggregate(
                                                        logicalOlapScan()
                                                )
                                        )
                                )
                        )
                );
    }

    @Test
    public void testSortNodeCase() {
        PlanChecker.from(connectContext)
                .analyze(
                        "SELECT * FROM T1 JOIN (SELECT ID, SUM(SCORE) SCORE FROM T2 GROUP BY ID ORDER BY ID) T ON T1.ID + 1 = T.ID AND T.SCORE = T1.SCORE + 10")
                .applyTopDown(new FindHashConditionForJoin())
                .applyTopDown(new PushdownExpressionsInHashCondition())
                .matches(
                        logicalProject(
                                logicalJoin(
                                        logicalProject(
                                                logicalOlapScan()
                                        ),
                                        logicalProject(
                                                logicalSort(
                                                        logicalAggregate(
                                                                logicalOlapScan()
                                                        )
                                                )
                                        )
                                )
                        )
                );
    }
}
