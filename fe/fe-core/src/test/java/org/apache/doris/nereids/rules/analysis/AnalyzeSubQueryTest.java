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
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalApply;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.util.FieldChecker;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
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
                        + ")\n",
                "CREATE TABLE IF NOT EXISTS T3 (\n"
                        + "    id bigint not null,\n"
                        + "    score bigint not null\n"
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

    @Test
    public void testScalarSubquerySlotNullable() {
        List<String> nullableSqls = ImmutableList.of(
                // project list
                "select (select T3.id as k from T3 limit 1) from T1",
                "select (select T3.id as k from T3 where T3.score = T1.score limit 1) from T1",
                "select (select sum(T3.id) as k from T3) from T1",
                "select (select sum(T3.id) as k from T3 where T3.score = T1.score) from T1",
                "select (select sum(T3.id) as k from T3 group by T3.score limit 1) from T1",
                "select (select sum(T3.id) as k from T3 group by T3.score having T3.score = T1.score + 10 limit 1) from T1",
                "select (select count(T3.id) as k from T3 group by T3.score limit 1) from T1",
                "select (select count(T3.id) as k from T3 group by T3.score having T3.score = T1.score + 10 limit 1) from T1",

                // filter
                "select * from T1 where T1.id > (select T3.id as k from T3 limit 1)",
                "select * from T1 where T1.id > (select T3.id as k from T3 where T3.score = T1.score limit 1)",
                "select * from T1 where T1.id > (select sum(T3.id) as k from T3)",
                "select * from T1 where T1.id > (select sum(T3.id) as k from T3 where T3.score = T1.score)",
                "select * from T1 where T1.id > (select sum(T3.id) as k from T3 group by T3.score limit 1)",
                "select * from T1 where T1.id > (select sum(T3.id) as k from T3 group by T3.score having T3.score = T1.score + 10 limit 1)",
                "select * from T1 where T1.id > (select count(T3.id) as k from T3 group by T3.score limit 1)",
                "select * from T1 where T1.id > (select count(T3.id) as k from T3 group by T3.score having T3.score = T1.score + 10 limit 1)"
        );

        List<String> notNullableSqls = ImmutableList.of(
                // project
                "select (select count(T3.id) as k from T3) from T1",
                "select (select count(T3.id) as k from T3 where T3.score = T1.score) from T1",

                // filter
                "select * from T1 where T1.id > (select count(T3.id) as k from T3)",
                "select * from T1 where T1.id > (select count(T3.id) as k from T3 where T3.score = T1.score)"
        );

        for (String sql : nullableSqls) {
            checkScalarSubquerySlotNullable(sql, true);
        }

        for (String sql : notNullableSqls) {
            checkScalarSubquerySlotNullable(sql, false);
        }
    }

    private void checkScalarSubquerySlotNullable(String sql, boolean outputNullable) {
        Plan root = PlanChecker.from(connectContext)
                .analyze(sql)
                .getPlan();
        List<LogicalProject<?>> projectList = Lists.newArrayList();
        List<LogicalPlan> plansAboveApply = Lists.newArrayList();
        root.foreach(plan -> {
            if (plan instanceof LogicalProject && plan.child(0) instanceof LogicalApply) {
                projectList.add((LogicalProject<?>) plan);
            }
            if (!(plan instanceof LogicalApply) && plan.anyMatch(p -> p instanceof LogicalApply)) {
                plansAboveApply.add((LogicalPlan) plan);
            }
        });

        Assertions.assertEquals(1, projectList.size());
        LogicalProject<?> project = projectList.get(0);
        LogicalApply<?, ?> apply = (LogicalApply<?, ?>) project.child();

        Assertions.assertNotNull(project);
        Assertions.assertNotNull(apply);

        List<String> slotKName = ImmutableList.of("k", "any_value(k)", "ifnull(k, 0)");
        NamedExpression output = project.getProjects().stream()
                .filter(e -> slotKName.contains(e.getName()))
                .findFirst().orElse(null);
        Assertions.assertNotNull(output);
        Assertions.assertEquals(outputNullable, output.nullable());

        Slot applySubqueySlot = apply.getOutput().stream()
                .filter(e -> slotKName.contains(e.getName()))
                .findFirst().orElse(null);
        Assertions.assertNotNull(applySubqueySlot);
        if (apply.isCorrelated()) {
            // apply will change to outer join
            Assertions.assertTrue(applySubqueySlot.nullable());
        } else {
            Assertions.assertEquals(outputNullable, applySubqueySlot.nullable());
        }

        for (LogicalPlan plan : plansAboveApply) {
            Assertions.assertTrue(plan.getInputSlots().stream()
                    .filter(slot -> slot.getExprId().equals(applySubqueySlot.getExprId()))
                    .allMatch(slot -> slot.nullable() == applySubqueySlot.nullable()));

            Assertions.assertTrue(plan.getOutput().stream()
                    .filter(slot -> slot.getExprId().equals(applySubqueySlot.getExprId()))
                    .allMatch(slot -> slot.nullable() == applySubqueySlot.nullable()));
        }
    }
}
