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

import org.apache.doris.common.NereidsException;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.datasets.ssb.SSBUtils;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.glue.translator.PhysicalPlanTranslator;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleSet;
import org.apache.doris.nereids.rules.implementation.AggregateStrategies;
import org.apache.doris.nereids.rules.rewrite.InApplyToJoin;
import org.apache.doris.nereids.rules.rewrite.PullUpProjectUnderApply;
import org.apache.doris.nereids.rules.rewrite.UnCorrelatedApplyFilter;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class AnalyzeCTETest extends TestWithFeService implements MemoPatternMatchSupported {

    private final NereidsParser parser = new NereidsParser();

    private final String multiCte = "WITH cte1 AS (SELECT s_suppkey FROM supplier WHERE s_suppkey < 5), "
            + "cte2 AS (SELECT s_suppkey FROM cte1 WHERE s_suppkey < 3)"
            + "SELECT * FROM cte1, cte2";

    private final String cteWithColumnAlias = "WITH cte1 (skey) AS (SELECT s_suppkey, s_nation FROM supplier WHERE s_suppkey < 5), "
            + "cte2 (sk2) AS (SELECT skey FROM cte1 WHERE skey < 3)"
            + "SELECT * FROM cte1, cte2";

    private final String cteConsumerInSubQuery = "WITH cte1 AS (SELECT * FROM supplier), "
            + "cte2 AS (SELECT * FROM supplier WHERE s_region in (\"ASIA\", \"AFRICA\"))"
            + "SELECT s_region, count(*) FROM cte1 GROUP BY s_region HAVING s_region in (SELECT s_region FROM cte2)";

    private final String cteConsumerJoin = "WITH cte1 AS (SELECT s_suppkey AS sk FROM supplier WHERE s_suppkey < 5), "
            + "cte2 AS (SELECT sk FROM cte1 WHERE sk < 3)"
            + "SELECT * FROM cte1 JOIN cte2 ON cte1.sk = cte2.sk";

    private final String cteReferToAnotherOne = "WITH V1 AS (SELECT s_suppkey FROM supplier), "
            + "V2 AS (SELECT s_suppkey FROM V1)"
            + "SELECT * FROM V2";

    private final String cteJoinSelf = "WITH cte1 AS (SELECT s_suppkey FROM supplier)"
            + "SELECT * FROM cte1 AS t1, cte1 AS t2";

    private final String cteNested = "WITH cte1 AS ("
            + "WITH cte2 AS (SELECT s_suppkey FROM supplier) SELECT * FROM cte2)"
            + " SELECT * FROM cte1";

    private final String cteInTheMiddle = "SELECT * FROM (WITH cte1 AS (SELECT s_suppkey FROM supplier)"
            + " SELECT * FROM cte1) a";

    private final String cteWithDiffRelationId = "with s as (select * from supplier) select * from s as s1, s as s2";

    private final List<String> testSqls = ImmutableList.of(
            multiCte, cteWithColumnAlias, cteConsumerInSubQuery, cteConsumerJoin, cteReferToAnotherOne, cteJoinSelf,
            cteNested, cteInTheMiddle, cteWithDiffRelationId
    );

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        useDatabase("test");
        SSBUtils.createTables(this);
        createView("CREATE VIEW V1 AS SELECT * FROM part");
        createView("CREATE VIEW V2 AS SELECT * FROM part");
    }

    @Override
    protected void runBeforeEach() throws Exception {
        StatementScopeIdGenerator.clear();
    }

    /* ********************************************************************************************
     * Test CTE
     * ******************************************************************************************** */

    @Test
    public void testTranslateCase() throws Exception {
        new MockUp<RuleSet>() {
            @Mock
            public List<Rule> getExplorationRules() {
                return Lists.newArrayList(new AggregateStrategies().buildRules());
            }
        };

        for (String sql : testSqls) {
            StatementScopeIdGenerator.clear();
            StatementContext statementContext = MemoTestUtils.createStatementContext(connectContext, sql);
            PhysicalPlan plan = new NereidsPlanner(statementContext).planWithLock(
                    parser.parseSingle(sql),
                    PhysicalProperties.ANY
            );
            // Just to check whether translate will throw exception
            new PhysicalPlanTranslator(new PlanTranslatorContext()).translatePlan(plan);
        }
    }

    @Test
    public void testCTEInHavingAndSubquery() {

        PlanChecker.from(connectContext)
                .analyze(cteConsumerInSubQuery)
                .applyBottomUp(new PullUpProjectUnderApply())
                .applyBottomUp(new UnCorrelatedApplyFilter())
                .applyBottomUp(new InApplyToJoin())
                .matches(
                        logicalFilter(
                                logicalProject(
                                        logicalJoin(
                                                logicalAggregate(),
                                                logicalProject()
                                        )
                                )

                        )
                );
    }

    @Test
    public void testCTEWithAlias() {
        PlanChecker.from(connectContext)
                .analyze(cteConsumerJoin)
                .matches(
                        logicalCTEAnchor(
                                logicalCTEProducer(),
                                logicalCTEAnchor(
                                        logicalCTEProducer(),
                                        logicalProject(
                                                logicalJoin(
                                                        logicalCTEConsumer(),
                                                        logicalCTEConsumer()
                                                )
                                        )
                                )
                        )
                );
    }

    @Test
    public void testCTEWithAnExistedTableOrViewName() {
        PlanChecker.from(connectContext)
                .analyze(cteReferToAnotherOne)
                .matches(
                        logicalCTEAnchor(
                                logicalCTEProducer(),
                                logicalCTEAnchor(
                                        logicalCTEProducer(),
                                        logicalProject(
                                                logicalCTEConsumer()
                                        )
                                )
                        )
                );

    }

    @Test
    public void testDifferenceRelationId() {
        PlanChecker.from(connectContext)
                .analyze(cteWithDiffRelationId)
                .matches(
                        logicalCTEAnchor(
                                logicalCTEProducer(),
                                logicalProject(
                                        logicalJoin(
                                                logicalSubQueryAlias(
                                                        logicalCTEConsumer()
                                                ),
                                                logicalSubQueryAlias(
                                                        logicalCTEConsumer()
                                                )
                                        )
                                )
                        )
                );
    }

    @Test
    public void testCteInTheMiddle() {
        PlanChecker.from(connectContext)
                .analyze(cteInTheMiddle)
                .matches(
                        logicalProject(
                                logicalSubQueryAlias(
                                        logicalCTEAnchor(
                                                logicalCTEProducer(),
                                                logicalProject(
                                                        logicalCTEConsumer()
                                                )
                                        )
                                )
                        )

                );
    }

    @Test
    public void testCteNested() {
        PlanChecker.from(connectContext)
                .analyze(cteNested)
                .matches(
                        logicalCTEAnchor(
                                logicalCTEProducer(
                                        logicalSubQueryAlias(
                                                logicalCTEAnchor(
                                                        logicalCTEProducer(),
                                                        logicalProject(
                                                                logicalCTEConsumer()
                                                        )
                                                )
                                        )
                                ),
                                logicalProject(
                                        logicalCTEConsumer()
                                )
                        )
                );
    }


    /* ********************************************************************************************
     * Test CTE Exceptions
     * ******************************************************************************************** */

    @Test
    public void testCTEExceptionOfDuplicatedColumnAlias() {
        String sql = "WITH cte1 (a1, A1) AS (SELECT * FROM supplier)"
                + "SELECT * FROM cte1";

        NereidsException exception = Assertions.assertThrows(NereidsException.class,
                () -> PlanChecker.from(connectContext).checkPlannerResult(sql), "Not throw expected exception.");
        Assertions.assertTrue(exception.getMessage().contains("Duplicated CTE column alias: [a1] in CTE [cte1]"));
    }

    @Test
    public void testCTEExceptionOfColumnAliasSize() {
        String sql = "WITH cte1 (a1, a2) AS "
                + "(SELECT s_suppkey FROM supplier)"
                + "SELECT * FROM cte1";

        NereidsException exception = Assertions.assertThrows(NereidsException.class,
                () -> PlanChecker.from(connectContext).checkPlannerResult(sql), "Not throw expected exception.");
        System.out.println(exception.getMessage());
        Assertions.assertTrue(exception.getMessage().contains("CTE [cte1] returns 2 columns, "
                + "but 1 labels were specified."));
    }

    @Test
    public void testCTEExceptionOfReferenceInWrongOrder() {
        String sql = "WITH cte1 AS (SELECT * FROM cte2), "
                + "cte2 AS (SELECT * FROM supplier)"
                + "SELECT * FROM cte1, cte2";

        RuntimeException exception = Assertions.assertThrows(RuntimeException.class,
                () -> PlanChecker.from(connectContext).checkPlannerResult(sql), "Not throw expected exception.");
        Assertions.assertTrue(exception.getMessage().contains("[cte2] does not exist in database"));
    }

    @Test
    public void testCTEExceptionOfErrorInUnusedCTE() {
        String sql = "WITH cte1 AS (SELECT * FROM not_existed_table)"
                + "SELECT * FROM supplier";

        RuntimeException exception = Assertions.assertThrows(RuntimeException.class,
                () -> PlanChecker.from(connectContext).checkPlannerResult(sql), "Not throw expected exception.");
        Assertions.assertTrue(exception.getMessage().contains("[not_existed_table] does not exist in database"));
    }

    @Test
    public void testCTEExceptionOfDuplicatedCTEName() {
        String sql = "WITH cte1 AS (SELECT * FROM supplier), "
                    + "cte1 AS (SELECT * FROM part)"
                    + "SELECT * FROM cte1";

        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> PlanChecker.from(connectContext).analyze(sql), "Not throw expected exception.");
        Assertions.assertTrue(exception.getMessage().contains("[cte1] cannot be used more than once"));
    }

    @Test
    public void testCTEExceptionOfRefterCTENameNotInScope() {
        String sql = "WITH cte1 AS (WITH cte2 AS (SELECT * FROM supplier) SELECT * FROM cte2)"
                + "SELECT * FROM cte2";

        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> PlanChecker.from(connectContext).analyze(sql), "Not throw expected exception.");
        Assertions.assertTrue(exception.getMessage().contains("Table [cte2] does not exist in database"));
    }
}
