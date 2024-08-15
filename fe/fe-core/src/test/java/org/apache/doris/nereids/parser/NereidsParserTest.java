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
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.exceptions.ParseException;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.literal.DecimalLiteral;
import org.apache.doris.nereids.trees.plans.DistributeType;
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
import org.apache.doris.nereids.types.DateTimeType;
import org.apache.doris.nereids.types.DateType;
import org.apache.doris.nereids.types.DecimalV2Type;
import org.apache.doris.nereids.types.DecimalV3Type;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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
        Assertions.assertEquals(plan.stmtType(), StmtType.OTHER);

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
        nereidsParser.parseSingle(sql);

        sql = "use catalog.db";
        nereidsParser.parseSingle(sql);
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

        sql = "describe function tvf('a' = 'b')";
        nereidsParser.parseSingle(sql);

        sql = "describe function tvf('a' = 'b') as tvf";
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
        String sql = "create repository a with S3 on location 's3://xxx' properties('k'='v')";
        nereidsParser.parseSingle(sql);
    }

    @Test
    public void testCreateRole() {
        NereidsParser nereidsParser = new NereidsParser();
        String sql = "create role a comment 'create user'";
        nereidsParser.parseSingle(sql);
    }
}
