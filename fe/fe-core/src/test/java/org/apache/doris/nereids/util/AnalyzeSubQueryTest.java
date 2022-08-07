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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.analyzer.NereidsAnalyzer;
import org.apache.doris.nereids.analyzer.Unbound;
import org.apache.doris.nereids.glue.translator.PhysicalPlanTranslator;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.analysis.EliminateAliasNode;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class AnalyzeSubQueryTest extends TestWithFeService {
    private final NereidsParser parser = new NereidsParser();

    private final List<String> testSql = Lists.newArrayList(
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

    /**
     * TODO: check bound plan and expression details.
     */
    @Test
    public void testAnalyzeAllCase() {
        for (String sql : testSql) {
            System.out.println("*****\nStart test: " + sql + "\n*****\n");
            checkAnalyze(sql);
        }
    }

    @Test
    public void testAnalyze() {
        checkAnalyze(testSql.get(0));
    }

    @Test
    public void testParseAllCase() {
        for (String sql : testSql) {
            System.out.println(parser.parseSingle(sql).treeString());
        }
    }

    @Test
    public void testParse() {
        System.out.println(parser.parseSingle(testSql.get(10)).treeString());
    }

    @Test
    public void testFinalizeAnalyze() {
        finalizeAnalyze(testSql.get(0));
    }

    @Test
    public void testFinalizeAnalyzeAllCase() {
        for (String sql : testSql) {
            System.out.println("*****\nStart test: " + sql + "\n*****\n");
            finalizeAnalyze(sql);
        }
    }

    @Test
    public void testPlan() throws AnalysisException {
        testPlanCase(testSql.get(0));
    }

    @Test
    public void testPlanAllCase() throws AnalysisException {
        for (String sql : testSql) {
            System.out.println("*****\nStart test: " + sql + "\n*****\n");
            testPlanCase(sql);
        }
    }

    private void testPlanCase(String sql) throws AnalysisException {
        PhysicalPlan plan = new NereidsPlanner(connectContext).plan(
                parser.parseSingle(sql),
                new PhysicalProperties(),
                connectContext
        );
        System.out.println(plan.treeString());
        PlanFragment root = new PhysicalPlanTranslator().translatePlan(plan, new PlanTranslatorContext());
        System.out.println(root.getPlanRoot().getPlanTreeExplainStr());
    }

    private void checkAnalyze(String sql) {
        LogicalPlan analyzed = analyze(sql);
        System.out.println(analyzed.treeString());
        Assertions.assertTrue(checkBound(analyzed));
    }

    private void finalizeAnalyze(String sql) {
        LogicalPlan plan = analyze(sql);
        System.out.println(plan.treeString());
        plan = (LogicalPlan) PlanRewriter.bottomUpRewrite(plan, connectContext, new EliminateAliasNode());
        System.out.println(plan.treeString());
    }

    private LogicalPlan analyze(String sql) {
        return new NereidsAnalyzer(connectContext).analyze(sql);
    }

    /**
     * PlanNode and its expressions are all bound.
     */
    private boolean checkBound(LogicalPlan plan) {
        if (plan instanceof Unbound) {
            return false;
        }

        List<Plan> children = plan.children();
        for (Plan child : children) {
            if (!checkBound((LogicalPlan) child)) {
                return false;
            }
        }

        List<Expression> expressions = plan.getExpressions();
        return expressions.stream().allMatch(this::checkExpressionBound);
    }

    private boolean checkExpressionBound(Expression expr) {
        if (expr instanceof Unbound) {
            return false;
        }

        List<Expression> children = expr.children();
        for (Expression child : children) {
            if (!checkExpressionBound(child)) {
                return false;
            }
        }
        return true;
    }
}

