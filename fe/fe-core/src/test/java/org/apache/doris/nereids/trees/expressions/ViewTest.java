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

package org.apache.doris.nereids.trees.expressions;

import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.glue.translator.PhysicalPlanTranslator;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.analysis.LogicalSubQueryAliasToLogicalProject;
import org.apache.doris.nereids.rules.rewrite.MergeProjects;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

import java.util.List;

public class ViewTest extends TestWithFeService implements MemoPatternMatchSupported {

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        connectContext.setDatabase("test");
        createTables(
                "CREATE TABLE IF NOT EXISTS T1 (\n"
                        + "    ID1 bigint,\n"
                        + "    SCORE1 bigint\n"
                        + ")\n"
                        + "DUPLICATE KEY(ID1)\n"
                        + "DISTRIBUTED BY HASH(ID1) BUCKETS 1\n"
                        + "PROPERTIES (\n"
                        + "  \"replication_num\" = \"1\"\n"
                        + ")\n",
                "CREATE TABLE IF NOT EXISTS T2 (\n"
                        + "    ID2 bigint,\n"
                        + "    SCORE2 bigint\n"
                        + ")\n"
                        + "DUPLICATE KEY(ID2)\n"
                        + "DISTRIBUTED BY HASH(ID2) BUCKETS 1\n"
                        + "PROPERTIES (\n"
                        + "  \"replication_num\" = \"1\"\n"
                        + ")\n",
                "CREATE TABLE IF NOT EXISTS T3 (\n"
                        + "    ID3 bigint,\n"
                        + "    SCORE3 bigint\n"
                        + ")\n"
                        + "DUPLICATE KEY(ID3)\n"
                        + "DISTRIBUTED BY HASH(ID3) BUCKETS 1\n"
                        + "PROPERTIES (\n"
                        + "  \"replication_num\" = \"1\"\n"
                        + ")\n"
        );
        createView("CREATE VIEW V1 AS SELECT * FROM T1");
        createView("CREATE VIEW V2 AS SELECT * FROM T2");
        createView("CREATE VIEW V3 AS SELECT * FROM T3 JOIN (SELECT * FROM V2) T ON T3.ID3 = T.ID2");
    }

    @Override
    protected void runBeforeEach() throws Exception {
        StatementScopeIdGenerator.clear();
    }

    @Test
    public void testTranslateAllCase() throws Exception {

        List<String> testSql = Lists.newArrayList(
                "SELECT * FROM V1",
                "SELECT * FROM V2",
                "SELECT * FROM V3",
                "SELECT * FROM T1 JOIN (SELECT * FROM V1) T ON T1.ID1 = T.ID1",
                "SELECT * FROM T2 JOIN (SELECT * FROM V2) T ON T2.ID2 = T.ID2",
                "SELECT Y.ID2 FROM (SELECT * FROM V3) Y",
                "SELECT * FROM (SELECT * FROM V1 JOIN V2 ON V1.ID1 = V2.ID2) X JOIN (SELECT * FROM V1 JOIN V3 ON V1.ID1 = V3.ID2) Y ON X.ID1 = Y.ID3"
        );

        // check whether they can be translated.
        for (String sql : testSql) {
            StatementScopeIdGenerator.clear();
            System.out.println("\n\n***** " + sql + " *****\n\n");
            StatementContext statementContext = MemoTestUtils.createStatementContext(connectContext, sql);
            NereidsPlanner planner = new NereidsPlanner(statementContext);
            PhysicalPlan plan = planner.plan(
                    new NereidsParser().parseSingle(sql),
                    PhysicalProperties.ANY
            );
            // Just to check whether translate will throw exception
            new PhysicalPlanTranslator(new PlanTranslatorContext(planner.getCascadesContext())).translatePlan(plan);
        }
    }

    @Test
    public void testSimpleViewMergeProjects() {
        PlanChecker.from(connectContext)
                .analyze("SELECT * FROM V1")
                .applyTopDown(new LogicalSubQueryAliasToLogicalProject())
                .applyTopDown(new MergeProjects())
                .matches(
                      logicalProject(
                              logicalOlapScan()
                      )
                );
    }

    @Test
    public void testNestedView() {
        PlanChecker.from(connectContext)
                .analyze("SELECT *\n"
                        + "FROM (\n"
                        + "  SELECT *\n"
                        + "  FROM V1\n"
                        + "  JOIN V2\n"
                        + "  ON V1.ID1 = V2.ID2\n"
                        + ") X\n"
                        + "JOIN (\n"
                        + "  SELECT *\n"
                        + "  FROM V1\n"
                        + "  JOIN V3\n"
                        + "  ON V1.ID1 = V3.ID2\n"
                        + ") Y\n"
                        + "ON X.ID1 = Y.ID3"
                )
                .applyTopDown(new LogicalSubQueryAliasToLogicalProject())
                .applyTopDown(new MergeProjects())
                .matches(
                        logicalProject(
                                logicalJoin(
                                        logicalProject(
                                                logicalJoin(
                                                        logicalProject(
                                                                logicalOlapScan()
                                                        ),
                                                        logicalProject(
                                                                logicalOlapScan()
                                                        )
                                                )
                                        ),
                                        logicalProject(
                                                logicalJoin(
                                                        logicalProject(
                                                                logicalOlapScan()
                                                        ),
                                                        logicalProject(
                                                                logicalJoin(
                                                                        logicalOlapScan(),
                                                                        logicalProject(
                                                                                logicalOlapScan()
                                                                        )
                                                                )
                                                        )
                                                )
                                        )
                                )
                        )
                );
    }
}
