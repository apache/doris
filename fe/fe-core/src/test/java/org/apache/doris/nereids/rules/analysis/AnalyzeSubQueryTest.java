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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.glue.translator.PhysicalPlanTranslator;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.util.FieldChecker;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import java.util.List;

public class AnalyzeSubQueryTest extends TestWithFeService implements MemoPatternMatchSupported {

    private final NereidsParser parser = new NereidsParser();

    private final List<String> testSql = ImmutableList.of(
            "SELECT * FROM (SELECT * FROM T1 T) T2",
            "SELECT * FROM T1 TT1 JOIN (SELECT * FROM T2 TT2) T ON TT1.ID = T.ID",
            "SELECT * FROM T1 TT1 JOIN (SELECT TT2.ID FROM T2 TT2) T ON TT1.ID = T.ID",
            "SELECT T.ID FROM T1 T",
            "SELECT A.ID FROM T1 A, T2 B WHERE A.ID = B.ID",
            "SELECT * FROM T1 JOIN T1 T2 ON T1.ID = T2.ID"
    );

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        connectContext.setDatabase("test");

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
        StatementScopeIdGenerator.clear();
    }

    @Test
    public void testTranslateCase() throws Exception {
        for (String sql : testSql) {
            StatementScopeIdGenerator.clear();
            StatementContext statementContext = MemoTestUtils.createStatementContext(connectContext, sql);
            NereidsPlanner planner = new NereidsPlanner(statementContext);
            PhysicalPlan plan = planner.planWithLock(
                    parser.parseSingle(sql),
                    PhysicalProperties.ANY
            );
            // Just to check whether translate will throw exception
            new PhysicalPlanTranslator(new PlanTranslatorContext(planner.getCascadesContext())).translatePlan(plan);
        }
    }

    @Test
    public void testCaseSubQuery() {
        PlanChecker.from(connectContext)
                .analyze(testSql.get(0))
                .applyTopDown(new LogicalSubQueryAliasToLogicalProject())
                .matches(
                    logicalProject(
                        logicalProject(
                            logicalProject(
                                logicalProject(
                                    logicalOlapScan().when(o -> true)
                                )
                            )
                        ).when(FieldChecker.check("projects", ImmutableList.of(
                            new SlotReference(new ExprId(0), "id", BigIntType.INSTANCE, true, ImmutableList.of("T")),
                            new SlotReference(new ExprId(1), "score", BigIntType.INSTANCE, true, ImmutableList.of("T"))))
                        )
                    ).when(FieldChecker.check("projects", ImmutableList.of(
                        new SlotReference(new ExprId(0), "id", BigIntType.INSTANCE, true, ImmutableList.of("T2")),
                        new SlotReference(new ExprId(1), "score", BigIntType.INSTANCE, true, ImmutableList.of("T2"))))
                    )
                );
    }

    @Test
    public void testCaseMixed() {
        PlanChecker.from(connectContext)
                .analyze(testSql.get(1))
                .applyTopDown(new LogicalSubQueryAliasToLogicalProject())
                .matches(
                    logicalProject(
                        innerLogicalJoin(
                            logicalProject(
                                logicalOlapScan()
                            ),
                            logicalProject(
                                logicalProject(
                                    logicalProject(
                                        logicalOlapScan()
                                    )
                                )
                            ).when(FieldChecker.check("projects", ImmutableList.of(
                                new SlotReference(new ExprId(2), "id", BigIntType.INSTANCE, true, ImmutableList.of("TT2")),
                                new SlotReference(new ExprId(3), "score", BigIntType.INSTANCE, true, ImmutableList.of("TT2"))))
                            )
                        )
                        .when(FieldChecker.check("otherJoinConjuncts",
                                ImmutableList.of(new EqualTo(
                                        new SlotReference(new ExprId(0), "id", BigIntType.INSTANCE, true, ImmutableList.of("TT1")),
                                        new SlotReference(new ExprId(2), "id", BigIntType.INSTANCE, true, ImmutableList.of("T")))))
                        )
                    ).when(FieldChecker.check("projects", ImmutableList.of(
                        new SlotReference(new ExprId(0), "id", BigIntType.INSTANCE, true, ImmutableList.of("TT1")),
                        new SlotReference(new ExprId(1), "score", BigIntType.INSTANCE, true, ImmutableList.of("TT1")),
                        new SlotReference(new ExprId(2), "id", BigIntType.INSTANCE, true, ImmutableList.of("T")),
                        new SlotReference(new ExprId(3), "score", BigIntType.INSTANCE, true, ImmutableList.of("T"))))
                    )
                );
    }

    @Test
    public void testCaseJoinSameTable() {
        PlanChecker.from(connectContext)
                .analyze(testSql.get(5))
                .applyTopDown(new LogicalSubQueryAliasToLogicalProject())
                .matches(
                    logicalProject(
                        innerLogicalJoin(
                            logicalOlapScan(),
                            logicalProject(
                                logicalOlapScan()
                            )
                        )
                        .when(FieldChecker.check("otherJoinConjuncts", ImmutableList.of(new EqualTo(
                                new SlotReference(new ExprId(0), "id", BigIntType.INSTANCE, true, ImmutableList.of("test", "T1")),
                                new SlotReference(new ExprId(2), "id", BigIntType.INSTANCE, true, ImmutableList.of("T2")))))
                        )
                    ).when(FieldChecker.check("projects", ImmutableList.of(
                        new SlotReference(new ExprId(0), "id", BigIntType.INSTANCE, true, ImmutableList.of("test", "T1")),
                        new SlotReference(new ExprId(1), "score", BigIntType.INSTANCE, true, ImmutableList.of("test", "T1")),
                        new SlotReference(new ExprId(2), "id", BigIntType.INSTANCE, true, ImmutableList.of("T2")),
                        new SlotReference(new ExprId(3), "score", BigIntType.INSTANCE, true, ImmutableList.of("T2"))))
                    )
                );
    }
}
