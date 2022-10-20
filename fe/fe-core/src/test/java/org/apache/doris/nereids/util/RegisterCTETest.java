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

package org.apache.doris.nereids.util;

import org.apache.doris.nereids.analyzer.UnboundAlias;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.datasets.ssb.SSBUtils;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.analysis.CTEContext;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.NamedExpressionUtil;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class RegisterCTETest extends TestWithFeService implements PatternMatchSupported {

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        useDatabase("test");
        SSBUtils.createTables(this);
    }

    @Override
    protected void runBeforeEach() throws Exception {
        NamedExpressionUtil.clear();
    }

    private CTEContext getCTEContextAfterRegisterCTE(String sql) {
        return PlanChecker.from(connectContext)
                .analyze(sql)
                .getCascadesContext().getStatementContext().getCteContext();
    }

    /* ********************************************************************************************
     * Test CTE
     * ******************************************************************************************** */

    @Test
    public void testCTERegister() {
        String sql = "WITH cte1 AS (\n"
                + "  \tSELECT s_suppkey\n"
                + "  \tFROM supplier\n"
                + "  \tWHERE s_suppkey < 5\n"
                + "), cte2 AS (\n"
                + "  \tSELECT s_suppkey\n"
                + "  \tFROM cte1\n"
                + "  \tWHERE s_suppkey < 3\n"
                + ")\n"
                + "SELECT *\n"
                + "FROM cte1, cte2";
        CTEContext cteContext = getCTEContextAfterRegisterCTE(sql);

        Assertions.assertTrue(cteContext.containsCTE("cte1")
                && cteContext.containsCTE("cte2"));

        LogicalPlan cte2InitialPlan = cteContext.getInitialCTEPlan("cte2");
        PlanChecker.from(connectContext, cte2InitialPlan).matchesFromRoot(
                logicalProject(
                        logicalFilter(
                                logicalSubQueryAlias(
                                        logicalProject(
                                                logicalFilter(
                                                        unboundRelation()
                                                )
                                        )
                                )
                        )
                )
        );
    }

    @Test
    public void testCTERegisterWithColumnAlias() throws Exception {
        String sql = "  WITH cte1 (skey) AS (\n"
                + "  \tSELECT s_suppkey, s_nation\n"
                + "  \tFROM supplier\n"
                + "  \tWHERE s_suppkey < 5\n"
                + "), cte2 (sk2) AS (\n"
                + "  \tSELECT skey\n"
                + "  \tFROM cte1\n"
                + "  \tWHERE skey < 3\n"
                + ")\n"
                + "SELECT *\n"
                + "FROM cte1, cte2";
        CTEContext cteContext = getCTEContextAfterRegisterCTE(sql);

        Assertions.assertTrue(cteContext.containsCTE("cte1")
                && cteContext.containsCTE("cte2"));

        // check initial plan
        LogicalPlan cte1InitialPlan = cteContext.getInitialCTEPlan("cte1");

        List<NamedExpression> targetProjects = new ArrayList<>();
        targetProjects.add(new UnboundAlias(new UnboundSlot("s_suppkey"), "skey"));
        targetProjects.add(new UnboundSlot("s_nation"));

        PlanChecker.from(connectContext, cte1InitialPlan)
                .matches(
                    logicalProject(
                    ).when(FieldChecker.check("projects", targetProjects))
                );

        // check analyzed plan
        LogicalPlan cte1AnalyzedPlan = cteContext.getAnalyzedCTEPlan("cte1");

        targetProjects = new ArrayList<>();
        targetProjects.add(new Alias(new ExprId(7),
                new SlotReference(new ExprId(0), "s_suppkey", VarcharType.INSTANCE,
                false, ImmutableList.of("defaulst_cluster:test", "supplier")), "skey"));
        targetProjects.add(new SlotReference(new ExprId(4), "s_nation", VarcharType.INSTANCE,
                false, ImmutableList.of("defaulst_cluster:test", "supplier")));
        PlanChecker.from(connectContext, cte1AnalyzedPlan)
                .matches(
                    logicalProject(
                    ).when(FieldChecker.check("projects", targetProjects))
                );
    }

    @Test
    public void cte_test_2() {
        PlanChecker.from(connectContext)
                .checkPlannerResult("  WITH cte1 (skey, sname) AS (\n"
                    + "  \tSELECT *\n"
                    + "  \tFROM supplier\n"
                    + "  \tWHERE s_suppkey < 5\n"
                    + "), cte2 (sk2) AS (\n"
                    + "  \tSELECT skey\n"
                    + "  \tFROM cte1\n"
                    + "  \tWHERE skey < 3\n"
                    + ")\n"
                    + "SELECT *\n"
                    + "FROM cte1, cte2");
    }

    @Test
    public void cte_test_3() {
        PlanChecker.from(connectContext)
                .checkPlannerResult("  WITH cte1 AS (\n"
                    + "  \tSELECT *\n"
                    + "  \tFROM supplier\n"
                    + "  \tWHERE s_suppkey < 5\n"
                    + "), cte2 AS (\n"
                    + "  \tSELECT s_suppkey\n"
                    + "  \tFROM cte1\n"
                    + "  \tWHERE s_suppkey < 3\n"
                    + ")\n"
                    + "  SELECT *\n"
                    + "  FROM supplier\n"
                    + "  WHERE s_suppkey in (select s_suppkey from cte2)");
    }

    @Test
    public void cte_test_4() {
        PlanChecker.from(connectContext)
                .checkPlannerResult("  WITH cte1 AS (\n"
                    + "  \tSELECT *\n"
                    + "  \tFROM supplier\n"
                    + "), cte2 AS (\n"
                    + "  \tSELECT *\n"
                    + "  \tFROM supplier\n"
                    + "  \tWHERE s_region in (\"ASIA\", \"AFRICA\")\n"
                    + ")\n"
                    + "  SELECT s_region, count(*)\n"
                    + "  FROM cte1\n"
                    + "  GROUP BY s_region\n"
                    + "  HAVING s_region in (SELECT s_region FROM cte2)");
    }

    @Test
    public void cte_test_5() {
        PlanChecker.from(connectContext)
                .analyze("  WITH cte1 AS (\n"
                    + "  \tSELECT s_suppkey as sk\n"
                    + "  \tFROM supplier\n"
                    + "  \tWHERE s_suppkey < 5\n"
                    + "), cte2 AS (\n"
                    + "  \tSELECT sk\n"
                    + "  \tFROM cte1\n"
                    + "  \tWHERE sk < 3\n"
                    + ")\n"
                    + "  SELECT *\n"
                    + "  FROM cte1, cte2\n");
    }


    /* ********************************************************************************************
     * Test CTE Exceptions
     * ******************************************************************************************** */

    @Test
    public void testCTEExceptionOfDuplicatedColumnAlias() {
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class, () -> {
            PlanChecker.from(connectContext)
                    .checkPlannerResult("  WITH cte1 (skey, SKEY) AS (\n"
                        + "  \tSELECT s_suppkey, s_name\n"
                        + "  \tFROM supplier\n"
                        + "  \tWHERE s_suppkey < 5\n"
                        + ")\n"
                        + "SELECT *\n"
                        + "FROM cte1");
        }, "Not throw expected exception.");
        System.out.println(exception);
        Assertions.assertTrue(exception.getMessage().contains("Duplicated CTE column alias"));
    }

    @Test
    public void testCTEExceptionOfColumnAliasSize() {
        String sql = "WITH cte1 (a1, a2) AS "
                + "(SELECT s_suppkey FROM supplier)"
                + "SELECT * FROM cte1";

        AnalysisException exception = Assertions.assertThrows(AnalysisException.class, () -> {
            PlanChecker.from(connectContext).checkPlannerResult(sql);
        }, "Not throw expected exception.");
        System.out.println(exception);
        Assertions.assertTrue(exception.getMessage().contains("The number of column labels must be "
                    + "smaller or equal to the number of returned columns"));
    }

    @Test
    public void testCTEExceptionOfReferenceInWrongOrder() {
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            PlanChecker.from(connectContext)
                    .checkPlannerResult("  WITH cte1 AS (\n"
                        + "  \tSELECT s_suppkey\n"
                        + "  \tFROM cte2\n"
                        + "  \tWHERE s_suppkey < 5\n"
                        + "), cte2 AS (\n"
                        + "  \tSELECT s_suppkey\n"
                        + "  \tFROM supplier\n"
                        + "  \tWHERE s_suppkey < 3\n"
                        + ")\n"
                        + "SELECT *\n"
                        + "FROM cte1, cte2");
        }, "Not throw expected exception.");
        System.out.println(exception);
        Assertions.assertTrue(exception.getMessage().contains("does not exist in database"));
    }

    @Test
    public void testCTEExceptionOfErrorInUnusedCTE() {
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            PlanChecker.from(connectContext)
                    .checkPlannerResult("  WITH cte1 AS (\n"
                        + "  \tSELECT s_suppkey\n"
                        + "  \tFROM supplier\n"
                        + "  \tWHERE s_suppkey < 5\n"
                        + "), cte2 AS (\n"
                        + "  \tSELECT s_suppkey\n"
                        + "  \tFROM not_existed_table\n"
                        + "  \tWHERE s_suppkey < 3\n"
                        + ")\n"
                        + "SELECT *\n"
                        + "FROM cte1");
        }, "Not throw expected exception.");
        System.out.println(exception);
        Assertions.assertTrue(exception.getMessage().contains("does not exist in database"));
    }

    @Test
    public void testCTEExceptionOfDuplicatedCTEName() {
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class, () -> {
            PlanChecker.from(connectContext)
                    .analyze("  WITH cte1 AS (\n"
                        + "  \tSELECT s_suppkey\n"
                        + "  \tFROM supplier\n"
                        + "  \tWHERE s_suppkey < 5\n"
                        + "), cte1 AS (\n"
                        + "  \tSELECT s_suppkey\n"
                        + "  \tFROM supplier\n"
                        + "  \tWHERE s_suppkey < 3\n"
                        + ")\n"
                        + "SELECT *\n"
                        + "FROM cte1");
        }, "Not throw expected exception.");
        System.out.println(exception);
        Assertions.assertTrue(exception.getMessage().contains("cannot be used more than once"));
    }
}
