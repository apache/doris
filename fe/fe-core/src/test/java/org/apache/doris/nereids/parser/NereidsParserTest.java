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
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.UnboundResultSink;
import org.apache.doris.nereids.exceptions.ParseException;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.literal.DecimalLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.JoinHint;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand.ExplainLevel;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTE;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.types.DecimalV2Type;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.SqlModeHelper;

import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;

public class NereidsParserTest extends ParserTestBase {

    @BeforeAll
    public static void init() {
        ConnectContext ctx = new ConnectContext();
        new MockUp<ConnectContext>() {
            @Mock
            public ConnectContext get() {
                return ctx;
            }
        };
    }

    @Test
    void testNoBackslashEscapes() {
        parseExpression("'\\b'")
                .assertEquals(new StringLiteral("\b"));
        parseExpression("'\\n'")
                .assertEquals(new StringLiteral("\n"));
        parseExpression("'\\t'")
                .assertEquals(new StringLiteral("\t"));
        parseExpression("'\\0'")
                .assertEquals(new StringLiteral("\0"));
        ConnectContext.get().getSessionVariable().setSqlMode(SqlModeHelper.MODE_NO_BACKSLASH_ESCAPES);
        parseExpression("'\\b'")
                .assertEquals(new StringLiteral("\\b"));
        parseExpression("'\\n'")
                .assertEquals(new StringLiteral("\\n"));
        parseExpression("'\\t'")
                .assertEquals(new StringLiteral("\\t"));
        parseExpression("'\\0'")
                .assertEquals(new StringLiteral("\\0"));
    }

    @Test
    public void testParseMultiple() {
        NereidsParser nereidsParser = new NereidsParser();
        String sql = "SELECT b FROM test;SELECT a FROM test;";
        List<Pair<LogicalPlan, StatementContext>> logicalPlanList = nereidsParser.parseMultiple(sql);
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
        LogicalPlan logicalPlan0 = (LogicalPlan) ((LogicalPlanAdapter) statementBases.get(0)).getLogicalPlan().child(0);
        LogicalPlan logicalPlan1 = ((LogicalPlanAdapter) statementBases.get(1)).getLogicalPlan();
        Assertions.assertTrue(logicalPlan0 instanceof LogicalProject);
        Assertions.assertTrue(logicalPlan1 instanceof ExplainCommand);
    }

    @Test
    public void testParseSQLWithDialect() {
        String sql = "select `AD``D` from t1 where a = 1;explain graph select `AD``D` from t1 where a = 1;";
        NereidsParser nereidsParser = new NereidsParser();
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setSqlDialect("trino");
        // test fall back to doris parser
        List<StatementBase> statementBases = nereidsParser.parseSQL(sql, sessionVariable);
        Assertions.assertEquals(2, statementBases.size());
        Assertions.assertTrue(statementBases.get(0) instanceof LogicalPlanAdapter);
        Assertions.assertTrue(statementBases.get(1) instanceof LogicalPlanAdapter);
        LogicalPlan logicalPlan0 = ((LogicalPlanAdapter) statementBases.get(0)).getLogicalPlan();
        LogicalPlan logicalPlan1 = ((LogicalPlanAdapter) statementBases.get(1)).getLogicalPlan();
        Assertions.assertTrue(logicalPlan0 instanceof UnboundResultSink);
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
        Assertions.assertEquals(doubleCount, Config.enable_decimal_conversion ? 0 : 1);
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
                .matches(logicalJoin().when(j -> j.getHint() == JoinHint.NONE));

        // valid hint
        parsePlan("select * from t1 join [shuffle] t2 on t1.keyy=t2.keyy")
                .matches(logicalJoin().when(j -> j.getHint() == JoinHint.SHUFFLE_RIGHT));

        parsePlan("select * from t1 join [  shuffle ] t2 on t1.keyy=t2.keyy")
                .matches(logicalJoin().when(j -> j.getHint() == JoinHint.SHUFFLE_RIGHT));

        parsePlan("select * from t1 join [broadcast] t2 on t1.keyy=t2.keyy")
                .matches(logicalJoin().when(j -> j.getHint() == JoinHint.BROADCAST_RIGHT));

        parsePlan("select * from t1 join /*+ broadcast   */ t2 on t1.keyy=t2.keyy")
                .matches(logicalJoin().when(j -> j.getHint() == JoinHint.BROADCAST_RIGHT));

        // invalid hint position
        parsePlan("select * from [shuffle] t1 join t2 on t1.keyy=t2.keyy")
                .assertThrowsExactly(ParseException.class);

        parsePlan("select * from /*+ shuffle */ t1 join t2 on t1.keyy=t2.keyy")
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
        parsePlan("select * from t1 join /*+ shuffle , broadcast */ t2 on t1.keyy=t2.keyy")
                .assertThrowsExactly(ParseException.class);

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
}
