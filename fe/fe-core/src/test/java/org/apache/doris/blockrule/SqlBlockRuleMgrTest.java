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

import org.apache.doris.analysis.AlterSqlBlockRuleStmt;
import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.CreateDbStmt;
import org.apache.doris.analysis.CreateSqlBlockRuleStmt;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.analysis.SetUserPropertyStmt;
import org.apache.doris.analysis.ShowSqlBlockRuleStmt;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.Planner;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.utframe.UtFrameUtils;

import org.apache.commons.codec.digest.DigestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.HashMap;
import java.util.Map;

public class SqlBlockRuleMgrTest {

    private static String runningDir = "fe/mocked/SqlBlockRuleMgrTest/" + UUID.randomUUID().toString() + "/";
    
    private static ConnectContext connectContext;
    
    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createDorisCluster(runningDir);
        
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        
        // create database
        String createDbStmtStr = "create database test;";
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt(createDbStmtStr, connectContext);
        Catalog.getCurrentCatalog().createDb(createDbStmt);
        
        MetricRepo.init();
        createTable("create table test.table1\n" +
                "(k1 int, k2 int) distributed by hash(k1) buckets 1\n" +
                "properties(\"replication_num\" = \"1\");");

        createTable("create table test.table2\n" +
                "(k1 datetime, k2 int)\n" +
                "ENGINE=OLAP\n" +
                "PARTITION BY RANGE(k1)\n" +
                "(\n" +
                "PARTITION p20211213 VALUES [('2021-12-13 00:00:00'), ('2021-12-14 00:00:00')),\n" +
                "PARTITION p20211214 VALUES [('2021-12-14 00:00:00'), ('2021-12-15 00:00:00')),\n" +
                "PARTITION p20211215 VALUES [('2021-12-15 00:00:00'), ('2021-12-16 00:00:00')),\n" +
                "PARTITION p20211216 VALUES [('2021-12-16 00:00:00'), ('2021-12-17 00:00:00'))\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(k1)\n" +
                "BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");");
        
    }
    
    @AfterClass
    public static void tearDown() {
        File file = new File(runningDir);
        file.delete();
    }
    
    private static void createTable(String sql) throws Exception {
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        Catalog.getCurrentCatalog().createTable(createTableStmt);
    }
    
    @Test
    public void testUserMatchSql() throws Exception {
        String sql = "select * from table1 limit 10";
        String sqlHash = DigestUtils.md5Hex(sql);
        SqlBlockRule sqlRule = new SqlBlockRule("test_rule1", null, sqlHash, 0L, 0L, 0L, false, true);
        SqlBlockRuleMgr mgr = Catalog.getCurrentCatalog().getSqlBlockRuleMgr();
        mgr.replayCreate(sqlRule);
        // sql block rules
        String setPropertyStr = "set property for \"root\" \"sql_block_rules\" = \"test_rule1\"";
        SetUserPropertyStmt setUserPropertyStmt = (SetUserPropertyStmt) UtFrameUtils.parseAndAnalyzeStmt(setPropertyStr, connectContext);
        Catalog.getCurrentCatalog().getAuth().updateUserProperty(setUserPropertyStmt);
        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class, "sql match hash sql block rule: " + sqlRule.getName(),
                () -> mgr.matchSql(sql, sqlHash, "root"));
    }
    
    @Test
    public void testGlobalMatchSql() throws AnalysisException {
        String sql = "select * from test_table1 limit 10";
        String sqlHash = DigestUtils.md5Hex(sql);
        SqlBlockRule sqlRule = new SqlBlockRule("test_rule1", null, sqlHash, 0L, 0L, 0L, true, true);
        SqlBlockRuleMgr mgr = new SqlBlockRuleMgr();
        mgr.replayCreate(sqlRule);
        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class, "sql match hash sql block rule: " + sqlRule.getName(),
                () -> mgr.matchSql(sql, sqlHash, "test"));
    }
    
    @Test
    public void testRegexMatchSql() throws AnalysisException {
        String sql = "select * from test_table1 tt1 join test_table2 tt2 on tt1.testId=tt2.testId limit 5";
        String sqlHash = DigestUtils.md5Hex(sql);
        SqlBlockRule sqlRule = new SqlBlockRule("test_rule1", ".* join .*", null, 0L, 0L, 0L, true, true);
        SqlBlockRuleMgr mgr = new SqlBlockRuleMgr();
        mgr.replayCreate(sqlRule);
        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class, "sql match regex sql block rule: " + sqlRule.getName(),
                () -> mgr.matchSql(sqlRule, sql, sqlHash));
    }
    
    @Test
    public void testHashMatchSql() throws AnalysisException {
        String sql = "select * from test_table1 tt1 join test_table2 tt2 on tt1.testId=tt2.testId limit 5";
        String sqlHash = DigestUtils.md5Hex(sql);
        System.out.println(sqlHash);
        SqlBlockRule sqlRule = new SqlBlockRule("test_rule1", null, sqlHash, 0L, 0L, 0L, true, true);
        SqlBlockRuleMgr mgr = new SqlBlockRuleMgr();
        mgr.replayCreate(sqlRule);
        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class, "sql match hash sql block rule: " + sqlRule.getName(),
                () -> mgr.matchSql(sqlRule, sql, sqlHash));
    }

    @Test
    public void testReachLimitations() throws AnalysisException, Exception {
        String sql = "select * from test.table2;";
        StmtExecutor executor = UtFrameUtils.getSqlStmtExecutor(connectContext, sql);
        Planner planner = executor.planner();
        List<ScanNode> scanNodeList = planner.getScanNodes();
        OlapScanNode olapScanNode = (OlapScanNode) scanNodeList.get(0);
        Integer selectedPartition = Deencapsulation.getField(olapScanNode, "selectedPartitionNum");
        long selectedPartitionLongValue = selectedPartition.longValue();
        long selectedTablet = Deencapsulation.getField(olapScanNode, "selectedTabletsNum");
        long cardinality = Deencapsulation.getField(olapScanNode, "cardinality");
        Assert.assertEquals(0L, selectedPartitionLongValue);
        Assert.assertEquals(0L, selectedTablet);
        Assert.assertEquals(0L, cardinality);

        SqlBlockRuleMgr mgr = Catalog.getCurrentCatalog().getSqlBlockRuleMgr();

        // test reach partition_num :
        // cuz there is no data in test.table2, so the selectedPartitionLongValue == 0;
        // set sqlBlockRule.partition_num = -1, so it can be blocked.
        SqlBlockRule sqlBlockRule = new SqlBlockRule("test_rule2", "NULL", "NULL", -1L, 0L, 0L, true, true);
        mgr.replayCreate(sqlBlockRule);
        Assert.assertEquals(true, mgr.existRule("test_rule2"));
        Assert.assertEquals("NULL", sqlBlockRule.getSql());
        Assert.assertEquals("NULL", sqlBlockRule.getSqlHash());
        Assert.assertEquals(-1L, (long) sqlBlockRule.getPartitionNum());
        Assert.assertEquals(0L, (long) sqlBlockRule.getTabletNum());
        Assert.assertEquals(0L, (long) sqlBlockRule.getCardinality());

        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class, "errCode = 2, detailMessage = sql hits sql block rule: "
                        + sqlBlockRule.getName() + ", reach partition_num : " + sqlBlockRule.getPartitionNum(),
                () -> mgr.checkLimitaions(sqlBlockRule, selectedPartitionLongValue, selectedTablet, cardinality));

        // test reach tablet_num :
        SqlBlockRule sqlBlockRule2 = new SqlBlockRule("test_rule3", "NULL", "NULL", 0L, -1L, 0L, true, true);
        mgr.replayCreate(sqlBlockRule2);
        Assert.assertEquals(true, mgr.existRule("test_rule3"));
        Assert.assertEquals("NULL", sqlBlockRule2.getSql());
        Assert.assertEquals("NULL", sqlBlockRule2.getSqlHash());
        Assert.assertEquals(0L, (long) sqlBlockRule2.getPartitionNum());
        Assert.assertEquals(-1L, (long) sqlBlockRule2.getTabletNum());
        Assert.assertEquals(0L, (long) sqlBlockRule2.getCardinality());

        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class, "errCode = 2, detailMessage = sql hits sql block rule: "
                        + sqlBlockRule2.getName() + ", reach tablet_num : " + sqlBlockRule2.getTabletNum(),
                () -> mgr.checkLimitaions(sqlBlockRule2, selectedPartitionLongValue, selectedTablet, cardinality));

        // test reach cardinality :
        SqlBlockRule sqlBlockRule3 = new SqlBlockRule("test_rule4", "NULL", "NULL", 0L, 0L, -1L, true, true);
        mgr.replayCreate(sqlBlockRule3);
        Assert.assertEquals(true, mgr.existRule("test_rule4"));
        Assert.assertEquals("NULL", sqlBlockRule3.getSql());
        Assert.assertEquals("NULL", sqlBlockRule3.getSqlHash());
        Assert.assertEquals(0L, (long) sqlBlockRule3.getPartitionNum());
        Assert.assertEquals(0L, (long) sqlBlockRule3.getTabletNum());
        Assert.assertEquals(-1L, (long) sqlBlockRule3.getCardinality());

        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class, "errCode = 2, detailMessage = sql hits sql block rule: "
                        + sqlBlockRule3.getName() + ", reach cardinality : " + sqlBlockRule3.getCardinality(),
                () -> mgr.checkLimitaions(sqlBlockRule3, selectedPartitionLongValue, selectedTablet, cardinality));
    }

    @Test
    public void testAlterInvalid() throws Exception {
        Analyzer analyzer = new Analyzer(Catalog.getCurrentCatalog(), connectContext);
        SqlBlockRuleMgr mgr = Catalog.getCurrentCatalog().getSqlBlockRuleMgr();

        // create : sql
        // alter : sqlHash
        // AnalysisException : Only sql or sqlHash can be configured
        SqlBlockRule sqlBlockRule = new SqlBlockRule("test_rule", "select \\* from test_table", "NULL", 0L, 0L, 0L, true, true);
        mgr.unprotectedAdd(sqlBlockRule);
        Assert.assertEquals(true, mgr.existRule("test_rule"));

        Map<String, String> properties = new HashMap<>();
        properties.put(CreateSqlBlockRuleStmt.SQL_HASH_PROPERTY, "xxxx");
        AlterSqlBlockRuleStmt stmt = new AlterSqlBlockRuleStmt("test_rule", properties);
        stmt.analyze(analyzer);

        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class, "Only sql or sqlHash can be configured",
                () -> mgr.alterSqlBlockRule(stmt));

        // create : sql
        // alter : tabletNum
        // AnalysisException : sql/sqlHash and partition_num/tablet_num/cardinality cannot be set in one rule.
        Map<String, String> properties2 = new HashMap<>();
        properties2.put(CreateSqlBlockRuleStmt.SCANNED_TABLET_NUM, "4");
        AlterSqlBlockRuleStmt stmt2 = new AlterSqlBlockRuleStmt("test_rule", properties2);

        stmt2.analyze(analyzer);
        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class, "sql/sqlHash and partition_num/tablet_num/cardinality cannot be set in one rule.",
                () -> mgr.alterSqlBlockRule(stmt2));

        // create : cardinality
        // alter : sqlHash
        // AnalysisException : sql/sqlHash and partition_num/tablet_num/cardinality cannot be set in one rule.
        SqlBlockRule sqlBlockRule2 = new SqlBlockRule("test_rule2", "NULL", "NULL", 0L, 0L, 10L, true, true);
        mgr.unprotectedAdd(sqlBlockRule2);
        Assert.assertEquals(true, mgr.existRule("test_rule2"));

        Map<String, String> properties3 = new HashMap<>();
        properties3.put(CreateSqlBlockRuleStmt.SQL_HASH_PROPERTY, "xxxx");
        AlterSqlBlockRuleStmt stmt3 = new AlterSqlBlockRuleStmt("test_rule2", properties3);
        stmt3.analyze(analyzer);
        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class, "sql/sqlHash and partition_num/tablet_num/cardinality cannot be set in one rule.",
                () -> mgr.alterSqlBlockRule(stmt3));
    }

    @Test
    public void testNormalCreate() throws Exception {
        String createSql = "CREATE SQL_BLOCK_RULE test_rule PROPERTIES(\"sql\"=\"select \\\\* from test_table\",\"enable\"=\"true\")";
        CreateSqlBlockRuleStmt createSqlBlockRuleStmt = (CreateSqlBlockRuleStmt) UtFrameUtils.parseAndAnalyzeStmt(createSql, connectContext);
    }

    @Test
    public void testOnlyBlockQuery() throws DdlException, UserException {
        SqlBlockRuleMgr mgr = new SqlBlockRuleMgr();
        Analyzer analyzer = new Analyzer(Catalog.getCurrentCatalog(), connectContext);

        SqlBlockRule sqlRule = new SqlBlockRule("test_rule1", "test", null, 0L, 0L, 0L, true, true);
        mgr.replayCreate(sqlRule);

        Map<String, String> properties = new HashMap<>();
        properties.put(CreateSqlBlockRuleStmt.SQL_PROPERTY, "select \\* from test_table");
        AlterSqlBlockRuleStmt stmt = new AlterSqlBlockRuleStmt("test_rule1", properties);

        stmt.analyze(analyzer);
        mgr.alterSqlBlockRule(stmt);

        ShowSqlBlockRuleStmt showStmt = new ShowSqlBlockRuleStmt("test_rule1");

        Assert.assertEquals(1, mgr.getSqlBlockRule(showStmt).size());
        Assert.assertEquals("select \\* from test_table", mgr.getSqlBlockRule(showStmt).get(0).getSql());
    }

    @Test
    public void testLimitationsInvalid() throws Exception {
        SqlBlockRuleMgr mgr = new SqlBlockRuleMgr();

        // create sql_block_rule with partition_num = -1
        // DdlException: the value of partition_num can't be a negative
        String createSql = "CREATE SQL_BLOCK_RULE test_rule PROPERTIES(\"partition_num\"=\"-1\",\"enable\"=\"true\")";
        CreateSqlBlockRuleStmt stmt = (CreateSqlBlockRuleStmt) UtFrameUtils.parseAndAnalyzeStmt(createSql, connectContext);
        ExceptionChecker.expectThrowsWithMsg(DdlException.class, "the value of partition_num can't be a negative",
                () -> mgr.createSqlBlockRule(stmt));

        // create sql_block_rule with tablet_num = -1
        // DdlException: the value of tablet_num can't be a negative
        String createSql1 = "CREATE SQL_BLOCK_RULE test_rule PROPERTIES(\"tablet_num\"=\"-1\",\"enable\"=\"true\")";
        CreateSqlBlockRuleStmt stmt1 = (CreateSqlBlockRuleStmt) UtFrameUtils.parseAndAnalyzeStmt(createSql1, connectContext);
        ExceptionChecker.expectThrowsWithMsg(DdlException.class, "the value of tablet_num can't be a negative",
                () -> mgr.createSqlBlockRule(stmt1));

        // create sql_block_rule with cardinality = -1
        // DdlException: the value of cardinality can't be a negative
        String createSql2 = "CREATE SQL_BLOCK_RULE test_rule PROPERTIES(\"cardinality\"=\"-1\",\"enable\"=\"true\")";
        CreateSqlBlockRuleStmt stmt2 = (CreateSqlBlockRuleStmt) UtFrameUtils.parseAndAnalyzeStmt(createSql2, connectContext);
        ExceptionChecker.expectThrowsWithMsg(DdlException.class, "the value of cardinality can't be a negative",
                () -> mgr.createSqlBlockRule(stmt2));
    }

    @Test
    public void testUserPropertyInvalid() throws Exception {
        // sql block rules
        String ruleName = "test_rule_name";
        String setPropertyStr = String.format("set property for \"root\" \"sql_block_rules\" = \"%s\"", ruleName);
        SetUserPropertyStmt setUserPropertyStmt = (SetUserPropertyStmt) UtFrameUtils.parseAndAnalyzeStmt(setPropertyStr, connectContext);

        ExceptionChecker.expectThrowsWithMsg(DdlException.class, String.format("the sql block rule %s not exist", ruleName),
                () -> Catalog.getCurrentCatalog().getAuth().updateUserProperty(setUserPropertyStmt));

    }

    @Test
    public void testAlterSqlBlock() throws Exception{
        Analyzer analyzer = new Analyzer(Catalog.getCurrentCatalog(), connectContext);
        SqlBlockRuleMgr mgr = Catalog.getCurrentCatalog().getSqlBlockRuleMgr();

        // create : sql
        // alter : global
        SqlBlockRule sqlBlockRule = new SqlBlockRule("test_rule", "select \\* from test_table", "NULL", 0L, 0L, 0L, true, true);
        mgr.unprotectedAdd(sqlBlockRule);
        Assert.assertEquals(true, mgr.existRule("test_rule"));

        Map<String, String> properties = new HashMap<>();
        properties.put(CreateSqlBlockRuleStmt.GLOBAL_PROPERTY, "false");
        AlterSqlBlockRuleStmt stmt = new AlterSqlBlockRuleStmt("test_rule", properties);
        stmt.analyze(analyzer);
        mgr.alterSqlBlockRule(stmt);

        ShowSqlBlockRuleStmt showStmt = new ShowSqlBlockRuleStmt("test_rule");
        SqlBlockRule alteredSqlBlockRule = mgr.getSqlBlockRule(showStmt).get(0);

        Assert.assertEquals("select \\* from test_table", alteredSqlBlockRule.getSql());
        Assert.assertEquals("NULL", alteredSqlBlockRule.getSqlHash());
        Assert.assertEquals(0L, (long)alteredSqlBlockRule.getPartitionNum());
        Assert.assertEquals(0L, (long)alteredSqlBlockRule.getTabletNum());
        Assert.assertEquals(0L, (long)alteredSqlBlockRule.getCardinality());
        Assert.assertEquals(false, alteredSqlBlockRule.getGlobal());
        Assert.assertEquals(true, alteredSqlBlockRule.getEnable());

        // create : partitionNum
        // alter : tabletNum
        SqlBlockRule sqlBlockRule2 = new SqlBlockRule("test_rule2", "NULL", "NULL", 100L, 0L, 0L, true, true);
        mgr.unprotectedAdd(sqlBlockRule2);
        Assert.assertEquals(true, mgr.existRule("test_rule2"));

        Map<String, String> properties2 = new HashMap<>();
        properties2.put(CreateSqlBlockRuleStmt.SCANNED_TABLET_NUM, "500");
        AlterSqlBlockRuleStmt stmt2 = new AlterSqlBlockRuleStmt("test_rule2", properties2);
        stmt2.analyze(analyzer);
        mgr.alterSqlBlockRule(stmt2);

        ShowSqlBlockRuleStmt showStmt2 = new ShowSqlBlockRuleStmt("test_rule2");
        SqlBlockRule alteredSqlBlockRule2 = mgr.getSqlBlockRule(showStmt2).get(0);

        Assert.assertEquals("NULL", alteredSqlBlockRule2.getSql());
        Assert.assertEquals("NULL", alteredSqlBlockRule2.getSqlHash());
        Assert.assertEquals(100L, (long)alteredSqlBlockRule2.getPartitionNum());
        Assert.assertEquals(500L, (long)alteredSqlBlockRule2.getTabletNum());
        Assert.assertEquals(0L, (long)alteredSqlBlockRule2.getCardinality());
        Assert.assertEquals(true, alteredSqlBlockRule2.getGlobal());
        Assert.assertEquals(true, alteredSqlBlockRule2.getEnable());


    }
}
