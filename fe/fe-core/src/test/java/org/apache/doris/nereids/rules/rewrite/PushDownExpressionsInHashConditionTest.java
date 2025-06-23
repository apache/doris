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

import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.exploration.join.JoinReorderContext;
import org.apache.doris.nereids.trees.UnaryNode;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Abs;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Positive;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import java.util.List;

public class PushDownExpressionsInHashConditionTest extends TestWithFeService implements MemoPatternMatchSupported {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        connectContext.setDatabase("test");

        createTables(
                "CREATE TABLE IF NOT EXISTS T1 (\n"
                        + "    id bigint,\n"
                        + "    score bigint,\n"
                        + "    score_int int\n"
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
    public void testGeneratePhysicalPlan() {
        List<String> testSql = ImmutableList.of(
                "SELECT * FROM T1 JOIN T2 ON T1.ID + 1 = T2.ID + 2 AND T1.ID + 1 > 2",
                "SELECT * FROM (SELECT * FROM T1) X JOIN (SELECT * FROM T2) Y ON X.ID + 1 = Y.ID + 2 AND X.ID + 1 > 2",
                "SELECT * FROM T1 JOIN (SELECT ID, SUM(SCORE) SCORE FROM T2 GROUP BY ID) T ON T1.ID + 1 = T.ID AND T.SCORE < 10",
                "SELECT * FROM T1 JOIN (SELECT ID, SUM(SCORE) SCORE FROM T2 GROUP BY ID ORDER BY ID) T ON T1.ID + 1 = T.ID AND T.SCORE < 10"
        );
        testSql.forEach(sql -> new NereidsPlanner(createStatementCtx(sql)).planWithLock(
                new NereidsParser().parseSingle(sql),
                PhysicalProperties.ANY
        ));
    }

    @Test
    public void testSimpleCase() {
        PlanChecker.from(connectContext)
                .analyze("SELECT * FROM T1 JOIN T2 ON T1.ID + 1 = T2.ID + 2 AND T1.ID + 1 > 2")
                .applyTopDown(new FindHashConditionForJoin())
                .applyTopDown(new PushDownExpressionsInHashCondition())
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
                .applyTopDown(new PushDownExpressionsInHashCondition())
                .matches(
                    logicalProject(
                        logicalJoin(
                            logicalProject(
                                logicalSubQueryAlias(
                                    logicalProject(
                                        logicalOlapScan()
                                    )
                                )
                            ),
                            logicalProject(
                                    logicalSubQueryAlias(
                                    logicalProject(
                                        logicalOlapScan()
                                    )
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
                .applyTopDown(new PushDownExpressionsInHashCondition())
                .matches(
                        logicalProject(
                                logicalJoin(
                                        logicalProject(
                                                logicalOlapScan()
                                        ),
                                        logicalProject(
                                                logicalSubQueryAlias(
                                                        logicalProject(
                                                                logicalAggregate(
                                                                        logicalProject(
                                                                                logicalOlapScan()
                                                                        )))
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
                .applyTopDown(new PushDownExpressionsInHashCondition())
                .matches(
                    logicalProject(
                        logicalJoin(
                            logicalProject(
                                logicalOlapScan()
                            ),
                            logicalProject(
                                logicalSubQueryAlias(
                                    logicalSort(
                                        logicalProject(
                                            logicalAggregate(
                                                logicalProject(
                                                    logicalOlapScan()
                                                )
                                            )
                                        )
                                    )
                                )
                            )
                        )
                    )
                );
    }

    @Test
    public void testPushDownMarkConjuncts() {
        Plan left = new LogicalOneRowRelation(new RelationId(1),
                ImmutableList.of(new Alias(new ExprId(1), new IntegerLiteral(1), "a")));
        Plan right = new LogicalOneRowRelation(new RelationId(2),
                ImmutableList.of(new Alias(new ExprId(2), new IntegerLiteral(2), "b")));
        Expression sameLeft = new Abs(left.getOutput().get(0));
        Expression sameRight = new Positive(right.getOutput().get(0));
        Expression hashLeft = new Cast(sameLeft, StringType.INSTANCE);
        Expression hashRight = new Cast(sameRight, StringType.INSTANCE);
        Expression markLeft = new Cast(sameLeft, BigIntType.INSTANCE);
        Expression markRight = new Cast(sameRight, BigIntType.INSTANCE);

        LogicalJoin<?, ?> plan = new LogicalJoin<>(
                JoinType.INNER_JOIN,
                left,
                right,
                new JoinReorderContext()
        );

        Expression sameConjuncts = new EqualTo(sameLeft, sameRight);
        Expression hashConjuncts = new EqualTo(hashLeft, hashRight);
        Expression markConjuncts = new EqualTo(markLeft, markRight);
        Expression otherConjuncts = new Add(left.getOutput().get(0), new IntegerLiteral(1));

        plan = plan.withJoinConjuncts(ImmutableList.of(sameConjuncts, hashConjuncts), ImmutableList.of(otherConjuncts),
                ImmutableList.of(sameConjuncts, markConjuncts, otherConjuncts),
                new JoinReorderContext());

        PlanChecker.from(connectContext, plan).applyTopDown(new PushDownExpressionsInHashCondition())
                .matches(logicalJoin(logicalProject(logicalOneRowRelation())
                                .when(p -> p.getProjects().size() == 4
                                        && p.getProjects().stream().filter(Alias.class::isInstance)
                                        .map(Alias.class::cast).map(UnaryNode::child)
                                        .filter(sameLeft::equals).count() == 1
                                        && p.getProjects().stream().filter(Alias.class::isInstance)
                                        .map(Alias.class::cast).map(UnaryNode::child)
                                        .filter(markLeft::equals).count() == 1
                                        && p.getProjects().stream().filter(Alias.class::isInstance)
                                        .map(Alias.class::cast).map(UnaryNode::child)
                                        .filter(hashLeft::equals).count() == 1),
                        logicalProject(logicalOneRowRelation())
                                .when(p -> p.getProjects().size() == 4
                                        && p.getProjects().stream().filter(Alias.class::isInstance)
                                        .map(Alias.class::cast).map(UnaryNode::child)
                                        .filter(sameRight::equals).count() == 1
                                        && p.getProjects().stream().filter(Alias.class::isInstance)
                                        .map(Alias.class::cast).map(UnaryNode::child)
                                        .filter(markRight::equals).count() == 1
                                        && p.getProjects().stream().filter(Alias.class::isInstance)
                                        .map(Alias.class::cast).map(UnaryNode::child)
                                        .filter(hashRight::equals).count() == 1)
                        ).when(j -> j.getMarkJoinConjuncts().size() == 3
                        && j.getMarkJoinConjuncts().stream().filter(EqualTo.class::isInstance)
                                .allMatch(e -> ((EqualTo) e).left() instanceof SlotReference
                                        && ((EqualTo) e).right() instanceof SlotReference)
                        && j.getMarkJoinConjuncts().stream().filter(EqualTo.class::isInstance).count() == 2)
                        .when(j -> j.getHashJoinConjuncts().size() == 2
                        && j.getHashJoinConjuncts().stream().filter(EqualTo.class::isInstance)
                                .allMatch(e -> ((EqualTo) e).left() instanceof SlotReference
                                        && ((EqualTo) e).right() instanceof SlotReference)
                        && j.getHashJoinConjuncts().stream().filter(EqualTo.class::isInstance).count() == 2));
    }
}
