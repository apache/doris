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

import org.apache.doris.analysis.CreateDbStmt;
import org.apache.doris.analysis.CreateSqlBlockRuleStmt;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.analysis.SetUserPropertyStmt;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.UtFrameUtils;

import org.apache.commons.codec.digest.DigestUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.UUID;

public class SqlBlockRuleMgrTest {
    
    private static String runningDir = "fe/mocked/SqlBlockRuleMgrTest/" + UUID.randomUUID().toString() + "/";
    
    private static ConnectContext connectContext;
    
    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinDorisCluster(runningDir);
        
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
        SqlBlockRule sqlRule = new SqlBlockRule("test_rule1", null, sqlHash, false, true);
        SqlBlockRuleMgr mgr = new SqlBlockRuleMgr();
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
        SqlBlockRule sqlRule = new SqlBlockRule("test_rule1", null, sqlHash, true, true);
        SqlBlockRuleMgr mgr = new SqlBlockRuleMgr();
        mgr.replayCreate(sqlRule);
        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class, "sql match hash sql block rule: " + sqlRule.getName(),
                () -> mgr.matchSql(sql, sqlHash, "test"));
    }
    
    @Test
    public void testRegexMatchSql() throws AnalysisException {
        String sql = "select * from test_table1 tt1 join test_table2 tt2 on tt1.testId=tt2.testId limit 5";
        String sqlHash = DigestUtils.md5Hex(sql);
        SqlBlockRule sqlRule = new SqlBlockRule("test_rule1", ".* join .*", null, true, true);
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
        SqlBlockRule sqlRule = new SqlBlockRule("test_rule1", null, sqlHash, true, true);
        SqlBlockRuleMgr mgr = new SqlBlockRuleMgr();
        mgr.replayCreate(sqlRule);
        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class, "sql match hash sql block rule: " + sqlRule.getName(),
                () -> mgr.matchSql(sqlRule, sql, sqlHash));
    }
    
    @Test
    public void testNormalCreate() throws Exception {
        String createSql = "CREATE SQL_BLOCK_RULE test_rule PROPERTIES(\"sql\"=\"select \\\\* from test_table\",\"enable\"=\"true\")";
        CreateSqlBlockRuleStmt createSqlBlockRuleStmt = (CreateSqlBlockRuleStmt) UtFrameUtils.parseAndAnalyzeStmt(createSql, connectContext);
    }
}
