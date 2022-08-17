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

package org.apache.doris.nereids.parser;

import org.apache.doris.analysis.ExplainOptions;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.nereids.exceptions.ParseException;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand.ExplainLevel;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class NereidsParserTest {

    @Test
    public void testParseMultiple() {
        NereidsParser nereidsParser = new NereidsParser();
        String sql = "SELECT b FROM test;SELECT a FROM test;";
        List<LogicalPlan> logicalPlanList = nereidsParser.parseMultiple(sql);
        Assertions.assertEquals(2, logicalPlanList.size());
    }

    @Test
    public void testSingle() {
        NereidsParser nereidsParser = new NereidsParser();
        String sql = "SELECT * FROM test;";
        Exception exceptionOccurred = null;
        try {
            nereidsParser.parseSingle(sql);
        } catch (Exception e) {
            exceptionOccurred = e;
            e.printStackTrace();
        }
        Assertions.assertNull(exceptionOccurred);
    }

    @Test
    public void testErrorListener() {
        Exception exception = Assertions.assertThrows(ParseException.class, () -> {
            String sql = "select * from t1 where a = 1 illegal_symbol";
            NereidsParser nereidsParser = new NereidsParser();
            nereidsParser.parseSingle(sql);
        });
        Assertions.assertEquals("\nextraneous input 'illegal_symbol' expecting {<EOF>, ';'}(line 1, pos29)\n",
                exception.getMessage());
    }

    @Test
    public void testPostProcessor() {
        String sql = "select `AD``D` from t1 where a = 1";
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan logicalPlan = nereidsParser.parseSingle(sql);
        LogicalProject<Plan> logicalProject = (LogicalProject) logicalPlan;
        Assertions.assertEquals("AD`D", logicalProject.getProjects().get(0).getName());
    }

    @Test
    public void testExplainNormal() {
        String sql = "explain select `AD``D` from t1 where a = 1";
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan logicalPlan = nereidsParser.parseSingle(sql);
        Assertions.assertTrue(logicalPlan instanceof ExplainCommand);
        ExplainCommand explainCommand = (ExplainCommand) logicalPlan;
        ExplainLevel explainLevel = explainCommand.getLevel();
        Assertions.assertEquals(ExplainLevel.NORMAL, explainLevel);
        logicalPlan = explainCommand.getLogicalPlan();
        LogicalProject<Plan> logicalProject = (LogicalProject) logicalPlan;
        Assertions.assertEquals("AD`D", logicalProject.getProjects().get(0).getName());
    }

    @Test
    public void testExplainVerbose() {
        String sql = "explain verbose select `AD``D` from t1 where a = 1";
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan logicalPlan = nereidsParser.parseSingle(sql);
        ExplainCommand explainCommand = (ExplainCommand) logicalPlan;
        ExplainLevel explainLevel = explainCommand.getLevel();
        Assertions.assertEquals(ExplainLevel.VERBOSE, explainLevel);
    }

    @Test
    public void testExplainGraph() {
        String sql = "explain graph select `AD``D` from t1 where a = 1";
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan logicalPlan = nereidsParser.parseSingle(sql);
        ExplainCommand explainCommand = (ExplainCommand) logicalPlan;
        ExplainLevel explainLevel = explainCommand.getLevel();
        Assertions.assertEquals(ExplainLevel.GRAPH, explainLevel);
    }

    @Test
    public void testParseSQL() {
        String sql = "select `AD``D` from t1 where a = 1;explain graph select `AD``D` from t1 where a = 1;";
        NereidsParser nereidsParser = new NereidsParser();
        List<StatementBase> statementBases = nereidsParser.parseSQL(sql);
        Assertions.assertEquals(2, statementBases.size());
        Assertions.assertTrue(statementBases.get(0) instanceof LogicalPlanAdapter);
        Assertions.assertTrue(statementBases.get(1) instanceof LogicalPlanAdapter);
        LogicalPlan logicalPlan0 = ((LogicalPlanAdapter) statementBases.get(0)).getLogicalPlan();
        LogicalPlan logicalPlan1 = ((LogicalPlanAdapter) statementBases.get(1)).getLogicalPlan();
        Assertions.assertTrue(logicalPlan0 instanceof LogicalProject);
        Assertions.assertTrue(logicalPlan1 instanceof LogicalProject);
        Assertions.assertNull(statementBases.get(0).getExplainOptions());
        Assertions.assertNotNull(statementBases.get(1).getExplainOptions());
        ExplainOptions explainOptions = statementBases.get(1).getExplainOptions();
        Assertions.assertTrue(explainOptions.isGraph());
        Assertions.assertFalse(explainOptions.isVerbose());
    }

    @Test
    public void testParseJoin() {
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan logicalPlan;
        LogicalJoin logicalJoin;

        String innerJoin1 = "SELECT t1.a FROM t1 INNER JOIN t2 ON t1.id = t2.id;";
        logicalPlan = nereidsParser.parseSingle(innerJoin1);
        logicalJoin = (LogicalJoin) logicalPlan.child(0);
        Assertions.assertEquals(JoinType.INNER_JOIN, logicalJoin.getJoinType());

        String innerJoin2 = "SELECT t1.a FROM t1 JOIN t2 ON t1.id = t2.id;";
        logicalPlan = nereidsParser.parseSingle(innerJoin2);
        logicalJoin = (LogicalJoin) logicalPlan.child(0);
        Assertions.assertEquals(JoinType.INNER_JOIN, logicalJoin.getJoinType());

        String leftJoin1 = "SELECT t1.a FROM t1 LEFT JOIN t2 ON t1.id = t2.id;";
        logicalPlan = nereidsParser.parseSingle(leftJoin1);
        logicalJoin = (LogicalJoin) logicalPlan.child(0);
        Assertions.assertEquals(JoinType.LEFT_OUTER_JOIN, logicalJoin.getJoinType());

        String leftJoin2 = "SELECT t1.a FROM t1 LEFT OUTER JOIN t2 ON t1.id = t2.id;";
        logicalPlan = nereidsParser.parseSingle(leftJoin2);
        logicalJoin = (LogicalJoin) logicalPlan.child(0);
        Assertions.assertEquals(JoinType.LEFT_OUTER_JOIN, logicalJoin.getJoinType());

        String rightJoin1 = "SELECT t1.a FROM t1 RIGHT JOIN t2 ON t1.id = t2.id;";
        logicalPlan = nereidsParser.parseSingle(rightJoin1);
        logicalJoin = (LogicalJoin) logicalPlan.child(0);
        Assertions.assertEquals(JoinType.RIGHT_OUTER_JOIN, logicalJoin.getJoinType());

        String rightJoin2 = "SELECT t1.a FROM t1 RIGHT OUTER JOIN t2 ON t1.id = t2.id;";
        logicalPlan = nereidsParser.parseSingle(rightJoin2);
        logicalJoin = (LogicalJoin) logicalPlan.child(0);
        Assertions.assertEquals(JoinType.RIGHT_OUTER_JOIN, logicalJoin.getJoinType());

        String leftSemiJoin = "SELECT t1.a FROM t1 LEFT SEMI JOIN t2 ON t1.id = t2.id;";
        logicalPlan = nereidsParser.parseSingle(leftSemiJoin);
        logicalJoin = (LogicalJoin) logicalPlan.child(0);
        Assertions.assertEquals(JoinType.LEFT_SEMI_JOIN, logicalJoin.getJoinType());

        String rightSemiJoin = "SELECT t2.a FROM t1 RIGHT SEMI JOIN t2 ON t1.id = t2.id;";
        logicalPlan = nereidsParser.parseSingle(rightSemiJoin);
        logicalJoin = (LogicalJoin) logicalPlan.child(0);
        Assertions.assertEquals(JoinType.RIGHT_SEMI_JOIN, logicalJoin.getJoinType());

        String leftAntiJoin = "SELECT t1.a FROM t1 LEFT ANTI JOIN t2 ON t1.id = t2.id;";
        logicalPlan = nereidsParser.parseSingle(leftAntiJoin);
        logicalJoin = (LogicalJoin) logicalPlan.child(0);
        Assertions.assertEquals(JoinType.LEFT_ANTI_JOIN, logicalJoin.getJoinType());

        String righAntiJoin = "SELECT t2.a FROM t1 RIGHT ANTI JOIN t2 ON t1.id = t2.id;";
        logicalPlan = nereidsParser.parseSingle(righAntiJoin);
        logicalJoin = (LogicalJoin) logicalPlan.child(0);
        Assertions.assertEquals(JoinType.RIGHT_ANTI_JOIN, logicalJoin.getJoinType());

        String crossJoin = "SELECT t1.a FROM t1 CROSS JOIN t2;";
        logicalPlan = nereidsParser.parseSingle(crossJoin);
        logicalJoin = (LogicalJoin) logicalPlan.child(0);
        Assertions.assertEquals(JoinType.CROSS_JOIN, logicalJoin.getJoinType());
    }
}
