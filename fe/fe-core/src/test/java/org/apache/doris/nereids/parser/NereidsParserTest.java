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

import org.apache.doris.analysis.StatementBase;
import org.apache.doris.analysis.StmtType;
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.UnboundFunction;
import org.apache.doris.nereids.analyzer.UnboundOneRowRelation;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.exceptions.ParseException;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.OrderExpression;
import org.apache.doris.nereids.trees.expressions.literal.DecimalLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.trees.plans.DistributeType;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand.ExplainLevel;
import org.apache.doris.nereids.trees.plans.commands.ReplayCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTE;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.types.DateTimeType;
import org.apache.doris.nereids.types.DateType;
import org.apache.doris.nereids.types.DecimalV2Type;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.GlobalVariable;
import org.apache.doris.qe.SqlModeHelper;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class NereidsParserTest extends ParserTestBase {

    @Test
    public void testParseMultiple() {
        NereidsParser nereidsParser = new NereidsParser();
        String sql = "SELECT b FROM test;;;;SELECT a FROM test;";
        List<Pair<LogicalPlan, StatementContext>> logicalPlanList = nereidsParser.parseMultiple(sql);
        Assertions.assertEquals(2, logicalPlanList.size());
    }

    @Test
    public void testParseMultipleError() {
        NereidsParser nereidsParser = new NereidsParser();
        String sql = "SELECT b FROM test SELECT a FROM test;";
        Assertions.assertThrowsExactly(ParseException.class, () -> nereidsParser.parseMultiple(sql));
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
        parsePlan("select * from t1 where a = 1 illegal_symbol")
                .assertThrowsExactly(ParseException.class)
                .assertMessageEquals("\nextraneous input 'illegal_symbol' expecting {<EOF>, ';'}(line 1, pos 29)\n");
    }

    @Test
    public void testPostProcessor() {
        parsePlan("select `AD``D` from t1 where a = 1")
                .matches(
                        logicalProject().when(p -> "AD`D".equals(p.getProjects().get(0).getName()))
                );
    }

    @Test
    public void testParseCTE() {
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan logicalPlan;
        String cteSql1 = "with t1 as (select s_suppkey from supplier where s_suppkey < 10) select * from t1";
        logicalPlan = (LogicalPlan) nereidsParser.parseSingle(cteSql1).child(0);
        Assertions.assertEquals(PlanType.LOGICAL_CTE, logicalPlan.getType());
        Assertions.assertEquals(((LogicalCTE<?>) logicalPlan).getAliasQueries().size(), 1);

        String cteSql2 = "with t1 as (select s_suppkey from supplier), t2 as (select s_suppkey from t1) select * from t2";
        logicalPlan = (LogicalPlan) nereidsParser.parseSingle(cteSql2).child(0);
        Assertions.assertEquals(PlanType.LOGICAL_CTE, logicalPlan.getType());
        Assertions.assertEquals(((LogicalCTE<?>) logicalPlan).getAliasQueries().size(), 2);

        String cteSql3 = "with t1 (keyy, name) as (select s_suppkey, s_name from supplier) select * from t1";
        logicalPlan = (LogicalPlan) nereidsParser.parseSingle(cteSql3).child(0);
        Assertions.assertEquals(PlanType.LOGICAL_CTE, logicalPlan.getType());
        Assertions.assertEquals(((LogicalCTE<?>) logicalPlan).getAliasQueries().size(), 1);
        Optional<List<String>> columnAliases = ((LogicalCTE<?>) logicalPlan).getAliasQueries().get(0).getColumnAliases();
        Assertions.assertEquals(columnAliases.get().size(), 2);
    }

    @Test
    public void testParseWindowFunctions() {
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan logicalPlan;
        String windowSql1 = "select k1, rank() over(partition by k1 order by k1) as ranking from t1";
        logicalPlan = (LogicalPlan) nereidsParser.parseSingle(windowSql1).child(0);
        Assertions.assertEquals(PlanType.LOGICAL_PROJECT, logicalPlan.getType());
        Assertions.assertEquals(((LogicalProject<?>) logicalPlan).getProjects().size(), 2);

        String windowSql2 = "select k1, sum(k2), rank() over(partition by k1 order by k1) as ranking from t1 group by k1";
        logicalPlan = (LogicalPlan) nereidsParser.parseSingle(windowSql2).child(0);
        Assertions.assertEquals(PlanType.LOGICAL_AGGREGATE, logicalPlan.getType());
        Assertions.assertEquals(((LogicalAggregate<?>) logicalPlan).getOutputExpressions().size(), 3);

        String windowSql3 = "select rank() over from t1";
        parsePlan(windowSql3).assertThrowsExactly(ParseException.class)
                    .assertMessageContains("mismatched input 'from' expecting '('");
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
        logicalPlan = (LogicalPlan) explainCommand.getLogicalPlan().child(0);
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
    public void testExplainTree() {
        String sql = "explain tree select `AD``D` from t1 where a = 1";
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan logicalPlan = nereidsParser.parseSingle(sql);
        ExplainCommand explainCommand = (ExplainCommand) logicalPlan;
        ExplainLevel explainLevel = explainCommand.getLevel();
        Assertions.assertEquals(ExplainLevel.TREE, explainLevel);
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
    public void testPlanReplayer() {
        String sql = "plan replayer dump select `AD``D` from t1 where a = 1";
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan logicalPlan = nereidsParser.parseSingle(sql);
        ReplayCommand replayCommand = (ReplayCommand) logicalPlan;
        Assertions.assertEquals(ReplayCommand.ReplayType.DUMP, replayCommand.getReplayType());
        sql = "plan replayer play 'path'";
        logicalPlan = nereidsParser.parseSingle(sql);
        replayCommand = (ReplayCommand) logicalPlan;
        Assertions.assertEquals(ReplayCommand.ReplayType.PLAY, replayCommand.getReplayType());
        Assertions.assertEquals("path", replayCommand.getDumpFileFullPath());
    }

    @Test
    public void testParseSQL() {
        String sql = "select `AD``D` from t1 where a = 1;explain graph select `AD``D` from t1 where a = 1;";
        NereidsParser nereidsParser = new NereidsParser();
        List<StatementBase> statementBases = nereidsParser.parseSQL(sql);
        Assertions.assertEquals(2, statementBases.size());
        Assertions.assertTrue(statementBases.get(0) instanceof LogicalPlanAdapter);
        Assertions.assertTrue(statementBases.get(1) instanceof LogicalPlanAdapter);
        LogicalPlan logicalPlan0 = (LogicalPlan) ((LogicalPlanAdapter) statementBases.get(0)).getLogicalPlan().child(0);
        LogicalPlan logicalPlan1 = ((LogicalPlanAdapter) statementBases.get(1)).getLogicalPlan();
        Assertions.assertTrue(logicalPlan0 instanceof LogicalProject);
        Assertions.assertTrue(logicalPlan1 instanceof ExplainCommand);
    }

    @Test
    public void testParseJoin() {
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan logicalPlan;
        LogicalJoin logicalJoin;

        String innerJoin1 = "SELECT t1.a FROM t1 INNER JOIN t2 ON t1.id = t2.id;";
        logicalPlan = (LogicalPlan) nereidsParser.parseSingle(innerJoin1).child(0);
        logicalJoin = (LogicalJoin) logicalPlan.child(0);
        Assertions.assertEquals(JoinType.INNER_JOIN, logicalJoin.getJoinType());

        String innerJoin2 = "SELECT t1.a FROM t1 JOIN t2 ON t1.id = t2.id;";
        logicalPlan = (LogicalPlan) nereidsParser.parseSingle(innerJoin2).child(0);
        logicalJoin = (LogicalJoin) logicalPlan.child(0);
        Assertions.assertEquals(JoinType.INNER_JOIN, logicalJoin.getJoinType());

        String leftJoin1 = "SELECT t1.a FROM t1 LEFT JOIN t2 ON t1.id = t2.id;";
        logicalPlan = (LogicalPlan) nereidsParser.parseSingle(leftJoin1).child(0);
        logicalJoin = (LogicalJoin) logicalPlan.child(0);
        Assertions.assertEquals(JoinType.LEFT_OUTER_JOIN, logicalJoin.getJoinType());

        String leftJoin2 = "SELECT t1.a FROM t1 LEFT OUTER JOIN t2 ON t1.id = t2.id;";
        logicalPlan = (LogicalPlan) nereidsParser.parseSingle(leftJoin2).child(0);
        logicalJoin = (LogicalJoin) logicalPlan.child(0);
        Assertions.assertEquals(JoinType.LEFT_OUTER_JOIN, logicalJoin.getJoinType());

        String rightJoin1 = "SELECT t1.a FROM t1 RIGHT JOIN t2 ON t1.id = t2.id;";
        logicalPlan = (LogicalPlan) nereidsParser.parseSingle(rightJoin1).child(0);
        logicalJoin = (LogicalJoin) logicalPlan.child(0);
        Assertions.assertEquals(JoinType.RIGHT_OUTER_JOIN, logicalJoin.getJoinType());

        String rightJoin2 = "SELECT t1.a FROM t1 RIGHT OUTER JOIN t2 ON t1.id = t2.id;";
        logicalPlan = (LogicalPlan) nereidsParser.parseSingle(rightJoin2).child(0);
        logicalJoin = (LogicalJoin) logicalPlan.child(0);
        Assertions.assertEquals(JoinType.RIGHT_OUTER_JOIN, logicalJoin.getJoinType());

        String leftSemiJoin = "SELECT t1.a FROM t1 LEFT SEMI JOIN t2 ON t1.id = t2.id;";
        logicalPlan = (LogicalPlan) nereidsParser.parseSingle(leftSemiJoin).child(0);
        logicalJoin = (LogicalJoin) logicalPlan.child(0);
        Assertions.assertEquals(JoinType.LEFT_SEMI_JOIN, logicalJoin.getJoinType());

        String rightSemiJoin = "SELECT t2.a FROM t1 RIGHT SEMI JOIN t2 ON t1.id = t2.id;";
        logicalPlan = (LogicalPlan) nereidsParser.parseSingle(rightSemiJoin).child(0);
        logicalJoin = (LogicalJoin) logicalPlan.child(0);
        Assertions.assertEquals(JoinType.RIGHT_SEMI_JOIN, logicalJoin.getJoinType());

        String leftAntiJoin = "SELECT t1.a FROM t1 LEFT ANTI JOIN t2 ON t1.id = t2.id;";
        logicalPlan = (LogicalPlan) nereidsParser.parseSingle(leftAntiJoin).child(0);
        logicalJoin = (LogicalJoin) logicalPlan.child(0);
        Assertions.assertEquals(JoinType.LEFT_ANTI_JOIN, logicalJoin.getJoinType());

        String righAntiJoin = "SELECT t2.a FROM t1 RIGHT ANTI JOIN t2 ON t1.id = t2.id;";
        logicalPlan = (LogicalPlan) nereidsParser.parseSingle(righAntiJoin).child(0);
        logicalJoin = (LogicalJoin) logicalPlan.child(0);
        Assertions.assertEquals(JoinType.RIGHT_ANTI_JOIN, logicalJoin.getJoinType());

        String crossJoin = "SELECT t1.a FROM t1 CROSS JOIN t2;";
        logicalPlan = (LogicalPlan) nereidsParser.parseSingle(crossJoin).child(0);
        logicalJoin = (LogicalJoin) logicalPlan.child(0);
        Assertions.assertEquals(JoinType.CROSS_JOIN, logicalJoin.getJoinType());
    }

    @Test
    void parseJoinEmptyConditionError() {
        parsePlan("select * from t1 LEFT JOIN t2")
                .assertThrowsExactly(ParseException.class)
                .assertMessageEquals("\n"
                        + "on mustn't be empty except for cross/inner join(line 1, pos 17)\n"
                        + "\n"
                        + "== SQL ==\n"
                        + "select * from t1 LEFT JOIN t2\n"
                        + "-----------------^^^\n");
    }

    @Test
    public void testParseDecimal() {
        String f1 = "SELECT col1 * 0.267081789095306 FROM t";
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan logicalPlan = (LogicalPlan) nereidsParser.parseSingle(f1).child(0);
        long doubleCount = logicalPlan
                .getExpressions()
                .stream()
                .mapToLong(e -> e.<Set<DecimalLiteral>>collect(DecimalLiteral.class::isInstance).size())
                .sum();
        Assertions.assertEquals(Config.enable_decimal_conversion ? 0 : 1, doubleCount);
    }

    @Test
    public void testDatev1() {
        String dv1 = "SELECT CAST('2023-12-18' AS DATEV1)";
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan logicalPlan = (LogicalPlan) nereidsParser.parseSingle(dv1).child(0);
        Assertions.assertEquals(DateType.INSTANCE, logicalPlan.getExpressions().get(0).getDataType());
    }

    @Test
    public void testDatetimev1() {
        String dtv1 = "SELECT CAST('2023-12-18' AS DATETIMEV1)";
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan logicalPlan = (LogicalPlan) nereidsParser.parseSingle(dtv1).child(0);
        Assertions.assertEquals(DateTimeType.INSTANCE, logicalPlan.getExpressions().get(0).getDataType());

        String wrongDtv1 = "SELECT CAST('2023-12-18' AS DATETIMEV1(2))";
        Assertions.assertThrows(AnalysisException.class, () -> nereidsParser.parseSingle(wrongDtv1).child(0));

    }

    @Test
    public void testDecimalv2() {
        String decv2 = "SELECT CAST('1.234' AS decimalv2(10,5))";
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan logicalPlan = (LogicalPlan) nereidsParser.parseSingle(decv2).child(0);
        Assertions.assertTrue(logicalPlan.getExpressions().get(0).getDataType().isDecimalV2Type());
    }

    @Test
    public void parseSetOperation() {
        String union = "select * from t1 union select * from t2 union all select * from t3";
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan logicalPlan = nereidsParser.parseSingle(union);
        System.out.println(logicalPlan.treeString());

        String union1 = "select * from t1 union (select * from t2 union all select * from t3)";
        LogicalPlan logicalPlan1 = nereidsParser.parseSingle(union1);
        System.out.println(logicalPlan1.treeString());

        String union2 = "(SELECT K1, K2, K3, K4, K5, K6, K7, K8, K9, K10, K11 FROM test WHERE K1 > 0)"
                + " UNION ALL (SELECT 1, 2, 3, 4, 3.14, 'HELLO', 'WORLD', 0.0, 1.1, CAST('1989-03-21' AS DATE), CAST('1989-03-21 13:00:00' AS DATETIME))"
                + " UNION ALL (SELECT K1, K2, K3, K4, K5, K6, K7, K8, K9, K10, K11 FROM baseall WHERE K3 > 0)"
                + " ORDER BY K1, K2, K3, K4";
        LogicalPlan logicalPlan2 = nereidsParser.parseSingle(union2);
        System.out.println(logicalPlan2.treeString());

        String union3 = "select a.k1, a.k2, a.k3, b.k1, b.k2, b.k3 from test a left outer join baseall b"
                + " on a.k1 = b.k1 and a.k2 > b.k2 union (select a.k1, a.k2, a.k3, b.k1, b.k2, b.k3"
                + " from test a right outer join baseall b on a.k1 = b.k1 and a.k2 > b.k2)"
                + " order by isnull(a.k1), 1, 2, 3, 4, 5 limit 65535";
        LogicalPlan logicalPlan3 = nereidsParser.parseSingle(union3);
        System.out.println(logicalPlan3.treeString());
    }

    @Test
    public void testJoinHint() {
        // no hint
        parsePlan("select * from t1 join t2 on t1.keyy=t2.keyy")
                .matches(logicalJoin().when(j -> j.getDistributeHint().distributeType == DistributeType.NONE));

        // valid hint
        parsePlan("select * from t1 join [shuffle] t2 on t1.keyy=t2.keyy")
                .matches(logicalJoin().when(j -> j.getDistributeHint().distributeType == DistributeType.SHUFFLE_RIGHT));

        parsePlan("select * from t1 join [  shuffle ] t2 on t1.keyy=t2.keyy")
                .matches(logicalJoin().when(j -> j.getDistributeHint().distributeType == DistributeType.SHUFFLE_RIGHT));

        parsePlan("select * from t1 join [broadcast] t2 on t1.keyy=t2.keyy")
                .matches(logicalJoin().when(j -> j.getDistributeHint().distributeType == DistributeType.BROADCAST_RIGHT));

        // invalid hint position
        parsePlan("select * from [shuffle] t1 join t2 on t1.keyy=t2.keyy")
                .assertThrowsExactly(ParseException.class);

        // invalid hint content
        parsePlan("select * from t1 join [bucket] t2 on t1.keyy=t2.keyy")
                .assertThrowsExactly(ParseException.class)
                .assertMessageContains("Invalid join hint: bucket(line 1, pos 22)\n"
                        + "\n"
                        + "== SQL ==\n"
                        + "select * from t1 join [bucket] t2 on t1.keyy=t2.keyy\n"
                        + "----------------------^^^");

        // invalid multiple hints

        parsePlan("select * from t1 join [shuffle,broadcast] t2 on t1.keyy=t2.keyy")
                .assertThrowsExactly(ParseException.class);
    }

    @Test
    public void testParseCast() {
        String sql = "SELECT CAST(1 AS DECIMAL(20, 6)) FROM t";
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan logicalPlan = (LogicalPlan) nereidsParser.parseSingle(sql).child(0);
        Cast cast = (Cast) logicalPlan.getExpressions().get(0).child(0);
        if (Config.enable_decimal_conversion) {
            DecimalV3Type decimalV3Type = (DecimalV3Type) cast.getDataType();
            Assertions.assertEquals(20, decimalV3Type.getPrecision());
            Assertions.assertEquals(6, decimalV3Type.getScale());
        } else {
            DecimalV2Type decimalV2Type = (DecimalV2Type) cast.getDataType();
            Assertions.assertEquals(20, decimalV2Type.getPrecision());
            Assertions.assertEquals(6, decimalV2Type.getScale());
        }
    }

    @Test
    void testParseExprDepthWidth() {
        String sql = "SELECT 1+2 = 3 from t";
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan logicalPlan = (LogicalPlan) nereidsParser.parseSingle(sql).child(0);
        System.out.println(logicalPlan);
        // alias (1 + 2 = 3)
        Assertions.assertEquals(4, logicalPlan.getExpressions().get(0).getDepth());
        Assertions.assertEquals(3, logicalPlan.getExpressions().get(0).getWidth());
    }

    @Test
    public void testParseCollate() {
        String sql = "SELECT * FROM t1 WHERE col COLLATE utf8 = 'test'";
        NereidsParser nereidsParser = new NereidsParser();
        nereidsParser.parseSingle(sql);
    }

    @Test
    public void testParseBinaryKeyword() {
        String sql = "SELECT BINARY 'abc' FROM t";
        NereidsParser nereidsParser = new NereidsParser();
        nereidsParser.parseSingle(sql);
    }

    @Test
    public void testParseReserveKeyword() {
        // partitions and auto_increment are reserve keywords
        String sql = "SELECT BINARY 'abc' FROM information_schema.partitions order by AUTO_INCREMENT";
        NereidsParser nereidsParser = new NereidsParser();
        nereidsParser.parseSingle(sql);
    }

    @Test
    public void testParseStmtType() {
        NereidsParser nereidsParser = new NereidsParser();
        String sql = "select a from b";
        LogicalPlan plan = nereidsParser.parseSingle(sql);
        Assertions.assertEquals(plan.stmtType(), StmtType.SELECT);

        sql = "use a";
        plan = nereidsParser.parseSingle(sql);
        Assertions.assertEquals(plan.stmtType(), StmtType.USE);

        sql = "CREATE TABLE tbl (`id` INT NOT NULL) DISTRIBUTED BY HASH(`id`) BUCKETS 1";
        plan = nereidsParser.parseSingle(sql);
        Assertions.assertEquals(plan.stmtType(), StmtType.CREATE);

        sql = "update a set b =1";
        plan = nereidsParser.parseSingle(sql);
        Assertions.assertEquals(plan.stmtType(), StmtType.UPDATE);
    }

    @Test
    public void testParseUse() {
        NereidsParser nereidsParser = new NereidsParser();
        String sql = "use db";
        LogicalPlan logicalPlan = nereidsParser.parseSingle(sql);
        Assertions.assertEquals(logicalPlan.stmtType(), StmtType.USE);

        sql = "use catalog.db";
        LogicalPlan logicalPlan1 = nereidsParser.parseSingle(sql);
        Assertions.assertEquals(logicalPlan1.stmtType(), StmtType.USE);
    }

    @Test
    public void testSwitch() {
        NereidsParser nereidsParser = new NereidsParser();
        String sql = "switch catalog";
        nereidsParser.parseSingle(sql);
    }

    @Test
    public void testParseSet() {
        NereidsParser nereidsParser = new NereidsParser();
        String sql = "set a as default storage vault";
        nereidsParser.parseSingle(sql);

        sql = "set property a = b";
        nereidsParser.parseSingle(sql);

        sql = "set property for user_a a = b";
        nereidsParser.parseSingle(sql);

        sql = "set global a = 1";
        nereidsParser.parseSingle(sql);

        sql = "set local a = 1";
        nereidsParser.parseSingle(sql);

        sql = "set session a = 1";
        nereidsParser.parseSingle(sql);

        sql = "set session a = default";
        nereidsParser.parseSingle(sql);

        sql = "set @@a = 10";
        nereidsParser.parseSingle(sql);

        sql = "set char set utf8";
        nereidsParser.parseSingle(sql);

        sql = "set charset utf8";
        nereidsParser.parseSingle(sql);

        sql = "set charset default";
        nereidsParser.parseSingle(sql);

        sql = "set names  = utf8";
        nereidsParser.parseSingle(sql);

        sql = "set local transaction read only";
        nereidsParser.parseSingle(sql);

        sql = "set global transaction isolation level read committed";
        nereidsParser.parseSingle(sql);

        sql = "set global transaction isolation level read committed, read write";
        nereidsParser.parseSingle(sql);

        sql = "set global transaction read write, isolation level repeatable read";
        nereidsParser.parseSingle(sql);

        sql = "set names default collate utf_8_ci";
        nereidsParser.parseSingle(sql);

        sql = "set password for user_a = password('xyz')";
        nereidsParser.parseSingle(sql);

        sql = "set password = 'xyz'";
        nereidsParser.parseSingle(sql);

        sql = "set ldap_admin_password = password('xyz')";
        nereidsParser.parseSingle(sql);

        sql = "set v1 = 1, v2 = 2, v3 = '3'";
        nereidsParser.parseSingle(sql);

        sql = "set @@global.v1 = 1";
        nereidsParser.parseSingle(sql);
    }

    @Test
    public void testUnset() {
        NereidsParser nereidsParser = new NereidsParser();
        String sql = "unset local variable a";
        nereidsParser.parseSingle(sql);

        sql = "unset variable a";
        nereidsParser.parseSingle(sql);

        sql = "unset global variable all";
        nereidsParser.parseSingle(sql);

        sql = "unset default storage vault";
        nereidsParser.parseSingle(sql);

        sql = "unset variable all";
        nereidsParser.parseSingle(sql);
    }

    @Test
    public void testTruncateTable() {
        NereidsParser nereidsParser = new NereidsParser();
        String sql = "truncate table a";
        nereidsParser.parseSingle(sql);

        sql = "truncate table a partitions (p1, p2, p3)";
        nereidsParser.parseSingle(sql);
    }

    @Test
    public void testKill() {
        NereidsParser nereidsParser = new NereidsParser();
        String sql = "kill 1234";
        nereidsParser.parseSingle(sql);

        sql = "kill connection 1234";
        nereidsParser.parseSingle(sql);

        sql = "kill query 1234";
        nereidsParser.parseSingle(sql);

        sql = "kill query '1234'";
        nereidsParser.parseSingle(sql);
    }

    @Test
    public void testDescribe() {
        NereidsParser nereidsParser = new NereidsParser();
        String sql = "describe ctl.db.tbl";
        nereidsParser.parseSingle(sql);

        sql = "describe db.tbl partitions(p1)";
        nereidsParser.parseSingle(sql);

        sql = "describe tbl all";
        nereidsParser.parseSingle(sql);
    }

    @Test
    public void testCreateDatabase() {
        NereidsParser nereidsParser = new NereidsParser();
        String sql = "create database if not exists a properties('k'='v')";
        nereidsParser.parseSingle(sql);

        sql = "create schema ctl.db";
        nereidsParser.parseSingle(sql);
    }

    @Test
    public void testCreateCatalog() {
        NereidsParser nereidsParser = new NereidsParser();
        String sql = "create catalog if not exists ctl";
        nereidsParser.parseSingle(sql);

        sql = "create catalog ctl with resource rsc comment '' properties('k'='v')";
        nereidsParser.parseSingle(sql);
    }

    @Test
    public void testCreateFunction() {
        NereidsParser nereidsParser = new NereidsParser();
        String sql = "create session tables function func_a (int, ...) returns boolean properties('k'='v')";
        nereidsParser.parseSingle(sql);

        sql = "create local aggregate function func_a (int, ...) returns boolean intermediate varchar properties('k'='v')";
        nereidsParser.parseSingle(sql);

        sql = "create alias function func_a (int) with parameter(id) as abs(id)";
        nereidsParser.parseSingle(sql);
    }

    @Test
    public void testCreateUser() {
        NereidsParser nereidsParser = new NereidsParser();
        String sql = "create user a superuser comment 'create user'";
        nereidsParser.parseSingle(sql);
    }

    @Test
    public void testCreateRepository() {
        NereidsParser nereidsParser = new NereidsParser();
        String sql = "create repository a with S3 on location 's3://s3-repo' "
                + "properties("
                + "'AWS_ENDPOINT' = 'http://s3.us-east-1.amazonaws.com',"
                + "'AWS_ACCESS_KEY' = 'akk',"
                + "'AWS_SECRET_KEY'='skk',"
                + "'AWS_REGION' = 'us-east-1')";
        nereidsParser.parseSingle(sql);
    }

    @Test
    public void testCreateRole() {
        NereidsParser nereidsParser = new NereidsParser();
        String sql = "create role a comment 'create user'";
        nereidsParser.parseSingle(sql);
    }

    @Test
    public void testQualify() {
        NereidsParser nereidsParser = new NereidsParser();

        List<String> sqls = new ArrayList<>();
        sqls.add("select year, country, profit, row_number() over (order by year) as rk from (select * from sales) a where year >= 2000 qualify rk > 1");
        sqls.add("select year, country, profit from (select * from sales) a where year >= 2000 qualify row_number() over (order by year) > 1");
        sqls.add("select year, country, profit from (select * from sales) a where year >= 2000 qualify rank() over (order by year) > 1");
        sqls.add("select year, country, profit from (select * from sales) a where year >= 2000 qualify dense_rank() over (order by year) > 1");

        sqls.add("select country, sum(profit) as total, row_number() over (order by country) as rk from sales where year >= 2000 group by country having sum(profit) > 100 qualify rk = 1");
        sqls.add("select country, sum(profit) as total from sales where year >= 2000 group by country having sum(profit) > 100 qualify row_number() over (order by country) = 1");
        sqls.add("select country, sum(profit) as total from sales where year >= 2000 group by country having sum(profit) > 100 qualify rank() over (order by country) = 1");
        sqls.add("select country, sum(profit) as total from sales where year >= 2000 group by country having sum(profit) > 100 qualify dense_rank() over (order by country) = 1");

        sqls.add("select country, sum(profit) as total, row_number() over (order by country) as rk from sales where year >= 2000 group by country qualify rk = 1");
        sqls.add("select country, sum(profit) as total from sales where year >= 2000 group by country qualify row_number() over (order by country) = 1");
        sqls.add("select country, sum(profit) as total from sales where year >= 2000 group by country qualify rank() over (order by country) = 1");
        sqls.add("select country, sum(profit) as total from sales where year >= 2000 group by country qualify dense_rank() over (order by country) = 1");

        sqls.add("select year, country, product, profit, row_number() over (partition by year, country order by profit desc) as rk from sales where year >= 2000 qualify rk = 1 order by profit");
        sqls.add("select year, country, product, profit from sales where year >= 2000 qualify row_number() over (partition by year, country order by profit desc) = 1 order by profit");
        sqls.add("select year, country, product, profit from sales where year >= 2000 qualify rank() over (partition by year, country order by profit desc) = 1 order by profit");
        sqls.add("select year, country, product, profit from sales where year >= 2000 qualify dense_rank() over (partition by year, country order by profit desc) = 1 order by profit");

        sqls.add("select year, country, profit, row_number() over (partition by year, country order by profit desc) as rk from (select * from sales) a where year >= 2000 having profit > 200 qualify rk = 1");
        sqls.add("select year, country, profit from (select * from sales) a where year >= 2000 having profit > 200 qualify row_number() over (partition by year, country order by profit desc) = 1");
        sqls.add("select year, country, profit from (select * from sales) a where year >= 2000 having profit > 200 qualify rank() over (partition by year, country order by profit desc) = 1");
        sqls.add("select year, country, profit from (select * from sales) a where year >= 2000 having profit > 200 qualify dense_rank() over (partition by year, country order by profit desc) = 1");

        sqls.add("select distinct year, row_number() over (order by year) as rk from sales group by year qualify rk = 1");
        sqls.add("select distinct year from sales group by year qualify row_number() over (order by year) = 1");
        sqls.add("select distinct year from sales group by year qualify rank() over (order by year) = 1");
        sqls.add("select distinct year from sales group by year qualify dense_rank() over (order by year) = 1");

        sqls.add("select year, country, profit from (select year, country, profit from (select year, country, profit, row_number() over (partition by year, country order by profit desc) as rk from (select * from sales) a where year >= 2000 having profit > 200) t where rk = 1) a where year >= 2000 qualify row_number() over (order by profit) = 1");
        sqls.add("select year, country, profit from (select year, country, profit from (select * from sales) a where year >= 2000 having profit > 200 qualify row_number() over (partition by year, country order by profit desc) = 1) a qualify row_number() over (order by profit) = 1");

        for (String sql : sqls) {
            nereidsParser.parseSingle(sql);
        }
    }

    @Test
    void testQueryOrganization() {
        NereidsParser nereidsParser = new NereidsParser();
        // legacy behavior
        GlobalVariable.enable_ansi_query_organization_behavior = false;
        checkQueryTopPlanClass("SELECT 1 ORDER BY 1",
                nereidsParser, LogicalSort.class);
        checkQueryTopPlanClass("SELECT 1 LIMIT 1",
                nereidsParser, LogicalLimit.class);
        checkQueryTopPlanClass("SELECT 1 UNION ALL SELECT 1 LIMIT 1",
                nereidsParser, LogicalUnion.class);
        checkQueryTopPlanClass("(SELECT 1 UNION ALL SELECT 1) LIMIT 1",
                nereidsParser, LogicalLimit.class);
        checkQueryTopPlanClass("SELECT 1 UNION ALL (SELECT 1 LIMIT 1)",
                nereidsParser, LogicalUnion.class);
        checkQueryTopPlanClass("SELECT 1 ORDER BY 1 UNION ALL SELECT 1 LIMIT 1",
                nereidsParser, LogicalUnion.class);
        checkQueryTopPlanClass("(SELECT 1 ORDER BY 1) UNION ALL (SELECT 1 LIMIT 1)",
                nereidsParser, LogicalUnion.class);
        checkQueryTopPlanClass("(SELECT 1 ORDER BY 1) UNION ALL SELECT 1 LIMIT 1",
                nereidsParser, LogicalUnion.class);

        GlobalVariable.enable_ansi_query_organization_behavior = true;
        checkQueryTopPlanClass("SELECT 1 ORDER BY 1",
                nereidsParser, LogicalSort.class);
        checkQueryTopPlanClass("SELECT 1 LIMIT 1",
                nereidsParser, LogicalLimit.class);
        checkQueryTopPlanClass("SELECT 1 UNION ALL SELECT 1 LIMIT 1",
                nereidsParser, LogicalLimit.class);
        checkQueryTopPlanClass("(SELECT 1 UNION ALL SELECT 1) LIMIT 1",
                nereidsParser, LogicalLimit.class);
        checkQueryTopPlanClass("SELECT 1 UNION ALL (SELECT 1 LIMIT 1)",
                nereidsParser, LogicalUnion.class);
        checkQueryTopPlanClass("SELECT 1 ORDER BY 1 UNION ALL SELECT 1 LIMIT 1",
                nereidsParser, null);
        checkQueryTopPlanClass("(SELECT 1 ORDER BY 1) UNION ALL (SELECT 1 LIMIT 1)",
                nereidsParser, LogicalUnion.class);
        checkQueryTopPlanClass("(SELECT 1 ORDER BY 1) UNION ALL SELECT 1 LIMIT 1",
                nereidsParser, LogicalLimit.class);
    }

    private void checkQueryTopPlanClass(String sql, NereidsParser parser, Class<?> clazz) {
        if (clazz == null) {
            Assertions.assertThrows(ParseException.class, () -> parser.parseSingle(sql));
        } else {
            LogicalPlan logicalPlan = parser.parseSingle(sql);
            Assertions.assertInstanceOf(clazz, logicalPlan.child(0));
        }
    }

    @Test
    public void testBlockSqlAst() {
        String sql = "plan replayer dump select `AD``D` from t1 where a = 1";
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan logicalPlan = nereidsParser.parseSingle(sql);

        Config.block_sql_ast_names = "ReplayCommand";
        StmtExecutor.initBlockSqlAstNames();
        StmtExecutor stmtExecutor = new StmtExecutor(new ConnectContext(), "");
        try {
            stmtExecutor.checkSqlBlocked(logicalPlan.getClass());
            Assertions.fail();
        } catch (Exception ignore) {
            // do nothing
        }

        Config.block_sql_ast_names = "CreatePolicyCommand, ReplayCommand";
        StmtExecutor.initBlockSqlAstNames();
        try {
            stmtExecutor.checkSqlBlocked(logicalPlan.getClass());
            Assertions.fail();
        } catch (Exception ignore) {
            // do nothing
        }

        Config.block_sql_ast_names = "";
        StmtExecutor.initBlockSqlAstNames();
        try {
            stmtExecutor.checkSqlBlocked(logicalPlan.getClass());
        } catch (Exception ex) {
            Assertions.fail(ex);
        }
    }

    @Test
    public void testExpressionWithOrder() {
        NereidsParser nereidsParser = new NereidsParser();
        checkQueryTopPlanClass("SELECT a, b, sum(c) from test group by a, b DESC",
                nereidsParser, LogicalSort.class);
        checkQueryTopPlanClass("SELECT a, b, sum(c) from test group by a DESC, b",
                nereidsParser, LogicalSort.class);
        checkQueryTopPlanClass("SELECT a, b, sum(c) from test group by a ASC, b",
                nereidsParser, LogicalSort.class);
        checkQueryTopPlanClass("SELECT a, b, sum(c) from test group by a, b ASC",
                nereidsParser, LogicalSort.class);
        checkQueryTopPlanClass("SELECT a, b, sum(c) from test group by a ASC, b ASC",
                nereidsParser, LogicalSort.class);
        checkQueryTopPlanClass("SELECT a, b, sum(c) from test group by a DESC, b DESC",
                nereidsParser, LogicalSort.class);
        checkQueryTopPlanClass("SELECT a, b, sum(c) from test group by a ASC, b DESC",
                nereidsParser, LogicalSort.class);
        checkQueryTopPlanClass("SELECT a, b, sum(c) from test group by a DESC, b ASC",
                nereidsParser, LogicalSort.class);

        checkQueryTopPlanClass("SELECT a, b, sum(c) from test group by a, b DESC WITH ROLLUP",
                nereidsParser, LogicalSort.class);
        checkQueryTopPlanClass("SELECT a, b, sum(c) from test group by a DESC, b WITH ROLLUP",
                nereidsParser, LogicalSort.class);
        checkQueryTopPlanClass("SELECT a, b, sum(c) from test group by a ASC, b WITH ROLLUP",
                nereidsParser, LogicalSort.class);
        checkQueryTopPlanClass("SELECT a, b, sum(c) from test group by a, b ASC WITH ROLLUP",
                nereidsParser, LogicalSort.class);
        checkQueryTopPlanClass("SELECT a, b, sum(c) from test group by a ASC, b ASC WITH ROLLUP",
                nereidsParser, LogicalSort.class);
        checkQueryTopPlanClass("SELECT a, b, sum(c) from test group by a DESC, b DESC WITH ROLLUP",
                nereidsParser, LogicalSort.class);
        checkQueryTopPlanClass("SELECT a, b, sum(c) from test group by a ASC, b DESC WITH ROLLUP",
                nereidsParser, LogicalSort.class);
        checkQueryTopPlanClass("SELECT a, b, sum(c) from test group by a DESC, b ASC WITH ROLLUP",
                nereidsParser, LogicalSort.class);

        checkQueryTopPlanClass("SELECT a, b, sum(c) from test group by a, b",
                nereidsParser, LogicalAggregate.class);
        checkQueryTopPlanClass("SELECT a, b, sum(c) from test group by a, b WITH ROLLUP",
                nereidsParser, LogicalRepeat.class);
    }

    @Test
    public void testGroupConcat() {
        NereidsParser parser = new NereidsParser();
        List<String> validSql = Lists.newArrayList(
                "SELECT GROUP_CONCAT(ALL 'x', 'y' ORDER BY 'z' ASC, 'a' DESC, 'b')",
                "SELECT GROUP_CONCAT(ALL 'x' ORDER BY 'z' ASC, 'a' DESC, 'b' SEPARATOR 'y')"
        );

        for (String sql : validSql) {
            LogicalPlan logicalPlan = parser.parseSingle(sql);
            Assertions.assertEquals(1, ((UnboundOneRowRelation) logicalPlan.child(0)).getProjects().size(), sql);
            Expression expression = ((UnboundOneRowRelation) logicalPlan.child(0)).getProjects().get(0).child(0);
            Assertions.assertInstanceOf(UnboundFunction.class, expression);
            UnboundFunction unboundFunction = (UnboundFunction) expression;
            Assertions.assertFalse(unboundFunction.isDistinct());
            Assertions.assertEquals(5, unboundFunction.arity());
            Assertions.assertEquals("x", ((StringLikeLiteral) unboundFunction.getArgument(0)).getStringValue());
            Assertions.assertEquals("y", ((StringLikeLiteral) unboundFunction.getArgument(1)).getStringValue());
            Assertions.assertEquals("z", ((StringLikeLiteral) ((OrderExpression) unboundFunction.getArgument(2)).getOrderKey().getExpr()).getStringValue());
            Assertions.assertTrue(((OrderExpression) unboundFunction.getArgument(2)).getOrderKey().isAsc());
            Assertions.assertEquals("a", ((StringLikeLiteral) ((OrderExpression) unboundFunction.getArgument(3)).getOrderKey().getExpr()).getStringValue());
            Assertions.assertFalse(((OrderExpression) unboundFunction.getArgument(3)).getOrderKey().isAsc());
            Assertions.assertEquals("b", ((StringLikeLiteral) ((OrderExpression) unboundFunction.getArgument(4)).getOrderKey().getExpr()).getStringValue());
            Assertions.assertTrue(((OrderExpression) unboundFunction.getArgument(4)).getOrderKey().isAsc());
        }
    }

    @Test
    public void testTrim() {
        NereidsParser parser = new NereidsParser();
        String sql;
        Expression e;
        UnboundFunction unboundFunction;

        sql = "trim('1', '2')";
        e = parser.parseExpression(sql);
        Assertions.assertInstanceOf(UnboundFunction.class, e);
        unboundFunction = (UnboundFunction) e;
        Assertions.assertEquals("trim", unboundFunction.getName());
        Assertions.assertEquals("1", ((StringLikeLiteral) unboundFunction.child(0)).getStringValue());
        Assertions.assertEquals("2", ((StringLikeLiteral) unboundFunction.child(1)).getStringValue());

        sql = "trim('2' from '1')";
        e = parser.parseExpression(sql);
        Assertions.assertInstanceOf(UnboundFunction.class, e);
        unboundFunction = (UnboundFunction) e;
        Assertions.assertEquals("trim", unboundFunction.getName());
        Assertions.assertEquals("1", ((StringLikeLiteral) unboundFunction.child(0)).getStringValue());
        Assertions.assertEquals("2", ((StringLikeLiteral) unboundFunction.child(1)).getStringValue());

        sql = "trim(both '2' from '1')";
        e = parser.parseExpression(sql);
        Assertions.assertInstanceOf(UnboundFunction.class, e);
        unboundFunction = (UnboundFunction) e;
        Assertions.assertEquals("trim", unboundFunction.getName());
        Assertions.assertEquals("1", ((StringLikeLiteral) unboundFunction.child(0)).getStringValue());
        Assertions.assertEquals("2", ((StringLikeLiteral) unboundFunction.child(1)).getStringValue());

        sql = "trim(leading '2' from '1')";
        e = parser.parseExpression(sql);
        Assertions.assertInstanceOf(UnboundFunction.class, e);
        unboundFunction = (UnboundFunction) e;
        Assertions.assertEquals("ltrim", unboundFunction.getName());
        Assertions.assertEquals("1", ((StringLikeLiteral) unboundFunction.child(0)).getStringValue());
        Assertions.assertEquals("2", ((StringLikeLiteral) unboundFunction.child(1)).getStringValue());

        sql = "trim(trailing '2' from '1')";
        e = parser.parseExpression(sql);
        Assertions.assertInstanceOf(UnboundFunction.class, e);
        unboundFunction = (UnboundFunction) e;
        Assertions.assertEquals("rtrim", unboundFunction.getName());
        Assertions.assertEquals("1", ((StringLikeLiteral) unboundFunction.child(0)).getStringValue());
        Assertions.assertEquals("2", ((StringLikeLiteral) unboundFunction.child(1)).getStringValue());

        sql = "trim(both from '1')";
        e = parser.parseExpression(sql);
        Assertions.assertInstanceOf(UnboundFunction.class, e);
        unboundFunction = (UnboundFunction) e;
        Assertions.assertEquals("trim", unboundFunction.getName());
        Assertions.assertEquals(1, unboundFunction.arity());
        Assertions.assertEquals("1", ((StringLikeLiteral) unboundFunction.child(0)).getStringValue());

        sql = "trim(leading from '1')";
        e = parser.parseExpression(sql);
        Assertions.assertInstanceOf(UnboundFunction.class, e);
        unboundFunction = (UnboundFunction) e;
        Assertions.assertEquals("ltrim", unboundFunction.getName());
        Assertions.assertEquals(1, unboundFunction.arity());
        Assertions.assertEquals("1", ((StringLikeLiteral) unboundFunction.child(0)).getStringValue());

        sql = "trim(trailing from '1')";
        e = parser.parseExpression(sql);
        Assertions.assertInstanceOf(UnboundFunction.class, e);
        unboundFunction = (UnboundFunction) e;
        Assertions.assertEquals("rtrim", unboundFunction.getName());
        Assertions.assertEquals(1, unboundFunction.arity());
        Assertions.assertEquals("1", ((StringLikeLiteral) unboundFunction.child(0)).getStringValue());

        Assertions.assertThrowsExactly(ParseException.class, () -> parser.parseExpression("trim(invalid '2' from '1')"));
        Assertions.assertThrowsExactly(ParseException.class, () -> parser.parseExpression("trim(invalid '2' '1')"));
        Assertions.assertThrowsExactly(ParseException.class, () -> parser.parseExpression("trim(from '1')"));
        Assertions.assertThrowsExactly(ParseException.class, () -> parser.parseExpression("trim(both '1')"));
    }

    @Test
    public void testNoBackSlashEscapes() {
        testNoBackSlashEscapes("''", "", "");
        testNoBackSlashEscapes("\"\"", "", "");

        testNoBackSlashEscapes("''''", "'", "'");
        testNoBackSlashEscapes("\"\"\"\"", "\"", "\"");

        testNoBackSlashEscapes("\"\\\\n\"", "\\\\n", "\\n");
        testNoBackSlashEscapes("\"\\t\"", "\\t", "\t");

        testNoBackSlashEscapes("'\\'''", "\\'", null);
        testNoBackSlashEscapes("'\\''", null, "'");
        testNoBackSlashEscapes("'\\\\''", null, null);

        testNoBackSlashEscapes("'\\\"\"'", "\\\"\"", "\"\"");
        testNoBackSlashEscapes("'\\\"'", "\\\"", "\"");
        testNoBackSlashEscapes("'\\\\\"'", "\\\\\"", "\\\"");

        testNoBackSlashEscapes("\"\\''\"", "\\''", "''");
        testNoBackSlashEscapes("\"\\'\"", "\\'", "'");
        testNoBackSlashEscapes("\"\\\\'\"", "\\\\'", "\\'");

        testNoBackSlashEscapes("\"\\\"\"\"", "\\\"", null);
        testNoBackSlashEscapes("\"\\\"\"", null, "\"");
        testNoBackSlashEscapes("\"\\\\\"\"", null, null);
    }

    private void testNoBackSlashEscapes(String sql, String onResult, String offResult) {
        NereidsParser nereidsParser = new NereidsParser();

        // test on
        try (MockedStatic<SqlModeHelper> helperMockedStatic = Mockito.mockStatic(SqlModeHelper.class)) {
            helperMockedStatic.when(SqlModeHelper::hasNoBackSlashEscapes).thenReturn(true);
            if (onResult == null) {
                Assertions.assertThrowsExactly(ParseException.class, () -> nereidsParser.parseExpression(sql),
                        "should failed when NO_BACKSLASH_ESCAPES = 1: " + sql);
            } else {
                Assertions.assertEquals(onResult,
                        ((StringLikeLiteral) nereidsParser.parseExpression(sql)).getStringValue());
            }
        }

        // test off
        try (MockedStatic<SqlModeHelper> helperMockedStatic = Mockito.mockStatic(SqlModeHelper.class)) {
            helperMockedStatic.when(SqlModeHelper::hasNoBackSlashEscapes).thenReturn(false);
            if (offResult == null) {
                Assertions.assertThrowsExactly(ParseException.class, () -> nereidsParser.parseExpression(sql),
                        "should failed when NO_BACKSLASH_ESCAPES = 0: " + sql);
            } else {
                Assertions.assertEquals(offResult,
                        ((StringLikeLiteral) nereidsParser.parseExpression(sql)).getStringValue());
            }
        }
    }

    @Test
    public void testComment() {
        NereidsParser parser = new NereidsParser();

        List<String> validComments = Lists.newArrayList(
                "SELECT 1 as a /* this is comment */, 1",
                "SELECT 1 as a /* this is comment /* */ */, 1",
                "SELECT 1 as a /* this is comment /* */, 1",
                "SELECT 1 as a /* this is comment -- */, 1",
                "SELECT 1 as a -- this is comment\n, 1",
                "SELECT 1 as a, 1 -- this is comment\\n, 1"
        );

        for (String sql : validComments) {
            LogicalPlan logicalPlan = parser.parseSingle(sql);
            Assertions.assertEquals(2, ((UnboundOneRowRelation) logicalPlan.child(0)).getProjects().size(), sql);
        }

        List<String> invalidComments = Lists.newArrayList(
                "SELECT 1 as a /* this is comment */*/, 1",
                "SELECT 1 as a /* this is comment, 1"
        );

        for (String sql : invalidComments) {
            Assertions.assertThrows(ParseException.class, () -> parser.parseSingle(sql), sql);
        }
    }

    @Test
    public void testLambdaSelect() {
        parsePlan("SELECT  x -> x + 1")
                .assertThrowsExactly(ParseException.class)
                .assertMessageContains("mismatched input '->' expecting {<EOF>, ';'}");
    }

    @Test
    public void testLambdaGroupBy() {
        parsePlan("SELECT 1 from ( select 2 ) t group by x -> x + 1")
                .assertThrowsExactly(ParseException.class)
                .assertMessageContains("mismatched input '->' expecting {<EOF>, ';'}");
    }

    @Test
    public void testLambdaSort() {
        parsePlan("SELECT 1 from ( select 2 ) t order by x -> x + 1")
                .assertThrowsExactly(ParseException.class)
                .assertMessageContains("mismatched input '->' expecting {<EOF>, ';'}");
    }

    @Test
    public void testLambdaHaving() {
        parsePlan("SELECT 1 from ( select 2 ) t having x -> x + 1")
                .assertThrowsExactly(ParseException.class)
                .assertMessageContains("mismatched input '->' expecting {<EOF>, ';'}");
    }

    @Test
    public void testLambdaJoin() {
        parsePlan("SELECT 1 from ( select 2 as a1 ) t1 join ( select 2 as a2 ) as t2 on x -> x + 1 = t1.a1")
                .assertThrowsExactly(ParseException.class)
                .assertMessageContains("mismatched input '->' expecting {<EOF>, ';'}");
    }

    @Test
    public void testWarmUpSelect() {
        ConnectContext ctx = ConnectContext.get();
        ctx.getSessionVariable().setEnableFileCache(true);
        ctx.getSessionVariable().setDisableFileCache(false);
        NereidsParser nereidsParser = new NereidsParser();

        // Test basic warm up select statement
        String warmUpSql = "WARM UP SELECT * FROM test_table";
        LogicalPlan logicalPlan = nereidsParser.parseSingle(warmUpSql);
        Assertions.assertNotNull(logicalPlan);
        Assertions.assertEquals(StmtType.OTHER, logicalPlan.stmtType());

        // Test warm up select with where clause
        String warmUpSqlWithWhere = "WARM UP SELECT id, name FROM test_table WHERE id > 10";
        LogicalPlan logicalPlanWithWhere = nereidsParser.parseSingle(warmUpSqlWithWhere);
        Assertions.assertNotNull(logicalPlanWithWhere);
        Assertions.assertEquals(StmtType.OTHER, logicalPlanWithWhere.stmtType());

        // Negative cases: LIMIT, JOIN, UNION, AGGREGATE not allowed
        Assertions.assertThrows(ParseException.class, () -> nereidsParser.parseSingle("WARM UP SELECT * FROM test_table LIMIT 100"));
        Assertions.assertThrows(ParseException.class, () -> nereidsParser.parseSingle("WARM UP SELECT * FROM t1 JOIN t2 ON t1.id = t2.id"));
        Assertions.assertThrows(ParseException.class, () -> nereidsParser.parseSingle("WARM UP SELECT * FROM t1 UNION SELECT * FROM t2"));
        Assertions.assertThrows(ParseException.class, () -> nereidsParser.parseSingle("WARM UP SELECT id, COUNT(*) FROM test_table GROUP BY id"));
        Assertions.assertThrows(ParseException.class, () -> nereidsParser.parseSingle("WARM UP SELECT * FROM test_table ORDER BY id"));
    }
}
