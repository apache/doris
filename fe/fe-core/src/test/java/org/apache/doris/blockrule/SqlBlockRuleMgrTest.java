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

package org.apache.doris.blockrule;

import org.apache.doris.analysis.CreateSqlBlockRuleStmt;
import org.apache.doris.analysis.SetUserPropertyStmt;
import org.apache.doris.analysis.ShowSqlBlockRuleStmt;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.utframe.TestWithFeService;
import org.apache.doris.utframe.UtFrameUtils;

import org.apache.commons.codec.digest.DigestUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

public class SqlBlockRuleMgrTest extends TestWithFeService {

    private static final String dropSqlRule = "DROP SQL_BLOCK_RULE test_rule";

    private SqlBlockRuleMgr mgr;

    @Override
    protected void runBeforeAll() throws Exception {
        // create database
        createDatabase("test");
        MetricRepo.init();
        // create table
        createTable("create table test.table1\n" + "(k1 int, k2 int) distributed by hash(k1) buckets 1\n"
                + "properties(\"replication_num\" = \"1\");");

        createTable(
                "create table test.table2\n" + "(k1 datetime, k2 int)\n" + "ENGINE=OLAP\n" + "PARTITION BY RANGE(k1)\n"
                        + "(\n" + "PARTITION p20211213 VALUES [('2021-12-13 00:00:00'), ('2021-12-14 00:00:00')),\n"
                        + "PARTITION p20211214 VALUES [('2021-12-14 00:00:00'), ('2021-12-15 00:00:00')),\n"
                        + "PARTITION p20211215 VALUES [('2021-12-15 00:00:00'), ('2021-12-16 00:00:00')),\n"
                        + "PARTITION p20211216 VALUES [('2021-12-16 00:00:00'), ('2021-12-17 00:00:00'))\n" + ")\n"
                        + "DISTRIBUTED BY HASH(k1)\n" + "BUCKETS 10\n" + "PROPERTIES (\n"
                        + "\"replication_num\" = \"1\"\n" + ");");
        mgr = Env.getCurrentEnv().getSqlBlockRuleMgr();
    }

    @Test
    public void testUserMatchSql() throws Exception {
        String sql = "select * from table1 limit 10";
        String sqlHash = DigestUtils.md5Hex(sql);
        String sqlRule = "CREATE SQL_BLOCK_RULE test_rule PROPERTIES(\"sqlHash\"=\"" + sqlHash
                + "\", \"global\"=\"false\", \"enable\"=\"true\");";
        createSqlBlockRule(sqlRule);
        // sql block rules
        String setPropertyStr = "set property for \"root\" \"sql_block_rules\" = \"test_rule\"";
        SetUserPropertyStmt setUserPropertyStmt = (SetUserPropertyStmt) parseAndAnalyzeStmt(setPropertyStr);
        Env.getCurrentEnv().getAuth().updateUserProperty(setUserPropertyStmt);
        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class, "sql match hash sql block rule: test_rule",
                () -> Env.getCurrentEnv().getSqlBlockRuleMgr().matchSql(sql, sqlHash, "root"));
        dropSqlBlockRule(dropSqlRule);
    }

    @Test
    public void testGlobalMatchSql() throws Exception {
        String sql = "select * from test_table1 limit 10";
        String sqlHash = DigestUtils.md5Hex(sql);
        String sqlRule = "CREATE SQL_BLOCK_RULE test_rule PROPERTIES(\"sql\"=\"select \\\\* from test_table1\","
                + " \"global\"=\"true\", \"enable\"=\"true\");";
        createSqlBlockRule(sqlRule);
        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class, "sql match regex sql block rule: test_rule",
                () -> mgr.matchSql(sql, sqlHash, "test"));
        dropSqlBlockRule(dropSqlRule);
    }

    @Test
    public void testRegexMatchSql() throws Exception {
        String sql = "select * from test_table1 tt1 join test_table2 tt2 on tt1.testId=tt2.testId limit 5";
        String sqlHash = DigestUtils.md5Hex(sql);
        String sqlRule = "CREATE SQL_BLOCK_RULE test_rule PROPERTIES(\"sql\"=\".* join .*\", \"global\"=\"true\","
                + " \"enable\"=\"true\");";
        createSqlBlockRule(sqlRule);
        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class, "sql match regex sql block rule: test_rule",
                () -> mgr.matchSql(sql, sqlHash, "root"));
        dropSqlBlockRule(dropSqlRule);
    }

    @Test
    public void testHashMatchSql() throws Exception {
        String sql = "select * from test_table1 tt1 join test_table2 tt2 on tt1.testId=tt2.testId limit 5";
        String sqlHash = DigestUtils.md5Hex(sql);
        String sqlRule = "CREATE SQL_BLOCK_RULE test_rule PROPERTIES(\"sqlHash\"=\"" + sqlHash
                + "\", \"global\"=\"true\", \"enable\"=\"true\");";
        createSqlBlockRule(sqlRule);
        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class, "sql match hash sql block rule: test_rule",
                () -> mgr.matchSql(sql, sqlHash, "root"));
        dropSqlBlockRule(dropSqlRule);
    }

    @Test
    public void testLimitationsInvalid() throws Exception {
        // reach test use regression-test
        String limitRule1 = "CREATE SQL_BLOCK_RULE test_rule1 PROPERTIES(\"partition_num\"=\"-1\", \"global\"=\"true\","
                + " \"enable\"=\"true\");";

        ExceptionChecker.expectThrowsWithMsg(DdlException.class, "the value of partition_num can't be a negative",
                () -> createSqlBlockRule(limitRule1));

        // test reach tablet_num :
        String limitRule2 = "CREATE SQL_BLOCK_RULE test_rule2 PROPERTIES(\"tablet_num\"=\"-1\", \"global\"=\"true\","
                + " \"enable\"=\"true\");";

        ExceptionChecker.expectThrowsWithMsg(DdlException.class, "the value of tablet_num can't be a negative",
                () -> createSqlBlockRule(limitRule2));

        // test reach cardinality :
        String limitRule3 = "CREATE SQL_BLOCK_RULE test_rule3 PROPERTIES(\"cardinality\"=\"-1\", \"global\"=\"true\","
                + " \"enable\"=\"true\");";

        ExceptionChecker.expectThrowsWithMsg(DdlException.class, "the value of cardinality can't be a negative",
                () -> createSqlBlockRule(limitRule3));
    }

    @Test
    public void testAlterInvalid() throws Exception {
        // create : sql
        // alter : sqlHash
        // AnalysisException : Only sql or sqlHash can be configured
        String sql = "select * from test_table1 limit 10";
        String sqlHash = DigestUtils.md5Hex(sql);
        String sqlRule = "CREATE SQL_BLOCK_RULE test_rule PROPERTIES(\"sql\"=\"select \\\\* from test_table1\","
                + " \"global\"=\"false\", \"enable\"=\"true\");";
        createSqlBlockRule(sqlRule);

        String alterSqlRule = "ALTER SQL_BLOCK_RULE test_rule PROPERTIES(\"sqlHash\"=\"" + sqlHash
                + "\",\"enable\"=\"true\")";

        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class, "Only sql or sqlHash can be configured",
                () -> alterSqlBlockRule(alterSqlRule));

        // create : sql
        // alter : tabletNum
        // AnalysisException : sql/sqlHash and partition_num/tablet_num/cardinality cannot be set in one rule.
        String alterNumRule =
                "ALTER SQL_BLOCK_RULE test_rule PROPERTIES(\"partition_num\" = \"10\",\"tablet_num\"=\"300\","
                        + "\"enable\"=\"true\")";
        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class,
                "sql/sqlHash and partition_num/tablet_num/cardinality cannot be set in one rule.",
                () -> alterSqlBlockRule(alterNumRule));

        // create : cardinality
        // alter : sqlHash
        // AnalysisException : sql/sqlHash and partition_num/tablet_num/cardinality cannot be set in one rule.
        String limitRule1 = "CREATE SQL_BLOCK_RULE test_rule1 PROPERTIES(\"cardinality\"=\"10\", \"global\"=\"true\","
                + " \"enable\"=\"true\");";
        createSqlBlockRule(limitRule1);
        String alterSqlRule1 = "ALTER SQL_BLOCK_RULE test_rule1 PROPERTIES(\"sqlHash\"=\"" + sqlHash
                + "\",\"enable\"=\"true\")";
        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class,
                "sql/sqlHash and partition_num/tablet_num/cardinality cannot be set in one rule.",
                () -> alterSqlBlockRule(alterSqlRule1));
        dropSqlBlockRule("DROP SQL_BLOCK_RULE test_rule,test_rule1");
    }

    @Test
    public void testOnlyBlockQuery() throws Exception {
        String sqlRule = "CREATE SQL_BLOCK_RULE test_rule PROPERTIES(\"sql\"=\"select k1 from test_table1\","
                + " \"global\"=\"false\", \"enable\"=\"true\");";
        createSqlBlockRule(sqlRule);

        String alterSqlRule = "ALTER SQL_BLOCK_RULE test_rule PROPERTIES(\"sql\"=\"select * from test_table1\","
                + " \"enable\"=\"true\")";
        alterSqlBlockRule(alterSqlRule);


        ShowSqlBlockRuleStmt showStmt = new ShowSqlBlockRuleStmt("test_rule");

        Assertions.assertEquals(1, mgr.getSqlBlockRule(showStmt).size());
        Assertions.assertEquals("select * from test_table1", mgr.getSqlBlockRule(showStmt).get(0).getSql());
        dropSqlBlockRule(dropSqlRule);
    }

    @Test
    public void testUserPropertyInvalid() throws Exception {
        // sql block rules
        String ruleName = "test_rule_name";
        String setPropertyStr = String.format("set property for \"root\" \"sql_block_rules\" = \"%s\"", ruleName);
        SetUserPropertyStmt setUserPropertyStmt = (SetUserPropertyStmt) UtFrameUtils.parseAndAnalyzeStmt(setPropertyStr,
                connectContext);

        ExceptionChecker.expectThrowsNoException(
                () -> Env.getCurrentEnv().getAuth().updateUserProperty(setUserPropertyStmt));
    }

    @Test
    public void testAlterSqlBlock() throws Exception {
        // create : sql
        // alter : global
        String sqlRule = "CREATE SQL_BLOCK_RULE test_rule PROPERTIES(\"sql\"=\"select k1 from test_table1\","
                + " \"global\"=\"true\", \"enable\"=\"true\")";
        createSqlBlockRule(sqlRule);

        String alterSqlRule = "ALTER SQL_BLOCK_RULE test_rule PROPERTIES(\"global\"=\"false\")";
        alterSqlBlockRule(alterSqlRule);

        ShowSqlBlockRuleStmt showStmt = new ShowSqlBlockRuleStmt("test_rule");
        SqlBlockRule alteredSqlBlockRule = mgr.getSqlBlockRule(showStmt).get(0);

        Assertions.assertEquals("select k1 from test_table1", alteredSqlBlockRule.getSql());
        Assertions.assertEquals("NULL", alteredSqlBlockRule.getSqlHash());
        Assertions.assertEquals(0L, (long) alteredSqlBlockRule.getPartitionNum());
        Assertions.assertEquals(0L, (long) alteredSqlBlockRule.getTabletNum());
        Assertions.assertEquals(0L, (long) alteredSqlBlockRule.getCardinality());
        Assertions.assertEquals(false, alteredSqlBlockRule.getGlobal());
        Assertions.assertEquals(true, alteredSqlBlockRule.getEnable());

        // create : partitionNum
        // alter : tabletNum
        String sqlRule1 = "CREATE SQL_BLOCK_RULE test_rule1 PROPERTIES(\"partition_num\"=\"100\", \"global\"=\"true\","
                + " \"enable\"=\"true\");";
        createSqlBlockRule(sqlRule1);
        String alterSqlRule1 = "ALTER SQL_BLOCK_RULE test_rule1 PROPERTIES(\"tablet_num\"=\"500\")";
        alterSqlBlockRule(alterSqlRule1);

        ShowSqlBlockRuleStmt showStmt1 = new ShowSqlBlockRuleStmt("test_rule1");
        SqlBlockRule alteredSqlBlockRule1 = mgr.getSqlBlockRule(showStmt1).get(0);

        Assertions.assertEquals("NULL", alteredSqlBlockRule1.getSql());
        Assertions.assertEquals("NULL", alteredSqlBlockRule1.getSqlHash());
        Assertions.assertEquals(100L, (long) alteredSqlBlockRule1.getPartitionNum());
        Assertions.assertEquals(500L, (long) alteredSqlBlockRule1.getTabletNum());
        Assertions.assertEquals(0L, (long) alteredSqlBlockRule1.getCardinality());
        Assertions.assertEquals(true, alteredSqlBlockRule1.getGlobal());
        Assertions.assertEquals(true, alteredSqlBlockRule1.getEnable());
        dropSqlBlockRule("DROP SQL_BLOCK_RULE test_rule,test_rule1");
    }

    @Test
    public void testWriteRead() throws Exception {
        String sqlRule = "CREATE SQL_BLOCK_RULE test_rule PROPERTIES(\"sql\"=\"select k1 from test_table1\","
                + " \"global\"=\"true\", \"enable\"=\"true\")";
        CreateSqlBlockRuleStmt createSqlBlockRuleStmt = (CreateSqlBlockRuleStmt) parseAndAnalyzeStmt(sqlRule);
        SqlBlockRule sqlBlockRule = SqlBlockRule.fromCreateStmt(createSqlBlockRuleStmt);
        File file = new File("./SqlBlockRuleTest");
        file.createNewFile();
        DataOutputStream out = new DataOutputStream(new FileOutputStream(file));
        sqlBlockRule.write(out);
        out.flush();
        out.close();
        DataInputStream in = new DataInputStream(new FileInputStream(file));
        SqlBlockRule read = SqlBlockRule.read(in);
        Assertions.assertEquals(sqlBlockRule.getName(), read.getName());
        Assertions.assertEquals(sqlBlockRule.getSql(), read.getSql());
        Assertions.assertEquals(sqlBlockRule.getSqlHash(), read.getSqlHash());
        Assertions.assertEquals(sqlBlockRule.getPartitionNum(), read.getPartitionNum());
        Assertions.assertEquals(sqlBlockRule.getTabletNum(), read.getTabletNum());
        Assertions.assertEquals(sqlBlockRule.getCardinality(), read.getCardinality());
        Assertions.assertEquals(sqlBlockRule.getEnable(), read.getEnable());
        Assertions.assertEquals(sqlBlockRule.getGlobal(), read.getGlobal());
        Assertions.assertEquals(sqlBlockRule.getSqlPattern().toString(), read.getSqlPattern().toString());
        file.delete();
    }

    @Test
    public void testIfExists() throws Exception {
        String sqlRule1 = "CREATE SQL_BLOCK_RULE test_rule PROPERTIES(\"sql\"=\"select \\\\* from test_table1\","
                + " \"global\"=\"true\", \"enable\"=\"true\");";
        createSqlBlockRule(sqlRule1);
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                String.format("the sql block rule %s already create", "test_rule"), () -> createSqlBlockRule(sqlRule1));
        String sqlRule2 =
                "CREATE SQL_BLOCK_RULE if not exists test_rule PROPERTIES(\"sql\"=\"select \\\\* from test_table1\","
                        + " \"global\"=\"true\", \"enable\"=\"true\");";
        createSqlBlockRule(sqlRule2);
        dropSqlBlockRule("DROP SQL_BLOCK_RULE test_rule");
    }

    @Test
    public void testIfNotExists() throws Exception {
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                String.format("the sql block rule %s not exist", "test_rule"),
                () -> dropSqlBlockRule("DROP SQL_BLOCK_RULE test_rule"));
        dropSqlBlockRule("DROP SQL_BLOCK_RULE if exists test_rule");
    }
}
