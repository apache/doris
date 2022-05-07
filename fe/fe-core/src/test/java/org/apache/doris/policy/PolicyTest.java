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

package org.apache.doris.policy;

import org.apache.doris.analysis.CreatePolicyStmt;
import org.apache.doris.analysis.CreateUserStmt;
import org.apache.doris.analysis.DropPolicyStmt;
import org.apache.doris.analysis.GrantStmt;
import org.apache.doris.analysis.ShowPolicyStmt;
import org.apache.doris.analysis.TablePattern;
import org.apache.doris.analysis.UserDesc;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.AccessPrivilege;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.utframe.DorisAssert;
import org.apache.doris.utframe.UtFrameUtils;

import com.google.common.collect.Lists;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.List;
import java.util.UUID;

public class PolicyTest {
    
    private static String runningDir = "fe/mocked/policyTest/" + UUID.randomUUID() + "/";
    
    private static ConnectContext connectContext;

    private static DorisAssert dorisAssert;
    
    @BeforeClass
    public static void beforeClass() throws Exception {
        // create cluster
        UtFrameUtils.createDorisCluster(runningDir);
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        dorisAssert = new DorisAssert(connectContext);
        // create database
        dorisAssert.withDatabase("test");
        dorisAssert.useDatabase("test");
        // create table
        dorisAssert.withTable("create table table1\n" +
                "(k1 int, k2 int) distributed by hash(k1) buckets 1\n" +
                "properties(\"replication_num\" = \"1\");");
        dorisAssert.withTable("create table table2\n" +
                "(k1 int, k2 int) distributed by hash(k1) buckets 1\n" +
                "properties(\"replication_num\" = \"1\");");
        // create user
        UserIdentity user = new UserIdentity("test_policy", "%");
        user.analyze(SystemInfoService.DEFAULT_CLUSTER);
        CreateUserStmt createUserStmt = new CreateUserStmt(new UserDesc(user));
        Catalog.getCurrentCatalog().getAuth().createUser(createUserStmt);
        List<AccessPrivilege> privileges = Lists.newArrayList(AccessPrivilege.ADMIN_PRIV);
        TablePattern tablePattern = new TablePattern("*", "*");
        tablePattern.analyze(SystemInfoService.DEFAULT_CLUSTER);
        GrantStmt grantStmt = new GrantStmt(user, null, tablePattern, privileges);
        Catalog.getCurrentCatalog().getAuth().grant(grantStmt);
        dorisAssert.useUser("test_policy");
    }
    
    @AfterClass
    public static void tearDown() {
        File file = new File(runningDir);
        file.delete();
    }
    
    @Test
    public void testNormalSql() throws Exception {
        createPolicy("CREATE ROW POLICY test_row_policy ON test.table1 AS PERMISSIVE TO test_policy USING (k1 = 1)");
        String queryStr = "EXPLAIN select * from test.table1";
        String explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
        Assert.assertTrue(explainString.contains("`k1` = 1"));
        dropPolicy("DROP ROW POLICY test_row_policy ON test.table1 FOR test_policy");
    }

    @Test
    public void testUnionSql() throws Exception {
        createPolicy("CREATE ROW POLICY test_row_policy ON test.table1 AS PERMISSIVE TO test_policy USING (k1 = 1)");
        String queryStr = "EXPLAIN select * from test.table1 union all select * from test.table1";
        String explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
        Assert.assertTrue(explainString.contains("`k1` = 1"));
        dropPolicy("DROP ROW POLICY test_row_policy ON test.table1 FOR test_policy");
    }

    @Test
    public void testInsertSelectSql() throws Exception {
        createPolicy("CREATE ROW POLICY test_row_policy ON test.table1 AS PERMISSIVE TO test_policy USING (k1 = 1)");
        String queryStr = "EXPLAIN insert into test.table1 select * from test.table1";
        String explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
        Assert.assertTrue(explainString.contains("`k1` = 1"));
        dropPolicy("DROP ROW POLICY test_row_policy ON test.table1 FOR test_policy");
    }
    
    @Test
    public void testDuplicateAddPolicy() throws Exception {
        createPolicy("CREATE ROW POLICY test_row_policy1 ON test.table1 AS PERMISSIVE TO test_policy USING (k1 = 1)");
        createPolicy("CREATE ROW POLICY IF NOT EXISTS test_row_policy1 ON test.table1 AS PERMISSIVE TO test_policy USING (k1 = 1)");
        ExceptionChecker.expectThrowsWithMsg(DdlException.class, "the policy test_row_policy1 already create",
                () -> createPolicy("CREATE ROW POLICY test_row_policy1 ON test.table1 AS PERMISSIVE TO test_policy USING (k1 = 1)"));
        dropPolicy("DROP ROW POLICY test_row_policy1 ON test.table1");
    }

    @Test
    public void testNoAuth() {
        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class, "CreatePolicyStmt command denied to user 'root'@'%' for table 'table1'",
                () -> createPolicy("CREATE ROW POLICY test_row_policy1 ON test.table1 AS PERMISSIVE TO root USING (k1 = 1)"));
    }
    
    @Test
    public void testShowPolicy() throws Exception {
        createPolicy("CREATE ROW POLICY test_row_policy1 ON test.table1 AS PERMISSIVE TO test_policy USING (k1 = 1)");
        createPolicy("CREATE ROW POLICY test_row_policy2 ON test.table1 AS PERMISSIVE TO test_policy USING (k2 = 1)");
        ShowPolicyStmt showPolicyStmt = (ShowPolicyStmt) UtFrameUtils.parseAndAnalyzeStmt("SHOW ROW POLICY", connectContext);
        int firstSize = Catalog.getCurrentCatalog().getPolicyMgr().showPolicy(showPolicyStmt).getResultRows().size();
        Assert.assertTrue(firstSize > 0);
        dropPolicy("DROP ROW POLICY test_row_policy1 ON test.table1");
        dropPolicy("DROP ROW POLICY test_row_policy2 ON test.table1");
        int secondSize = Catalog.getCurrentCatalog().getPolicyMgr().showPolicy(showPolicyStmt).getResultRows().size();
        Assert.assertEquals(2, firstSize - secondSize);
    }
    
    @Test
    public void testDropPolicy() throws Exception {
        createPolicy("CREATE ROW POLICY test_row_policy1 ON test.table1 AS PERMISSIVE TO test_policy USING (k2 = 1)");
        dropPolicy("DROP ROW POLICY test_row_policy1 ON test.table1");
        dropPolicy("DROP ROW POLICY IF EXISTS test_row_policy5 ON test.table1");
        ExceptionChecker.expectThrowsWithMsg(DdlException.class, "the policy test_row_policy1 not exist",
                () -> dropPolicy("DROP ROW POLICY test_row_policy1 ON test.table1"));
    }
    
    @Test
    public void testMergeFilter() throws Exception {
        createPolicy("CREATE ROW POLICY test_row_policy1 ON test.table1 AS RESTRICTIVE TO test_policy USING (k1 = 1)");
        createPolicy("CREATE ROW POLICY test_row_policy2 ON test.table1 AS RESTRICTIVE TO test_policy USING (k2 = 1)");
        createPolicy("CREATE ROW POLICY test_row_policy3 ON test.table1 AS PERMISSIVE TO test_policy USING (k2 = 2)");
        createPolicy("CREATE ROW POLICY test_row_policy4 ON test.table1 AS PERMISSIVE TO test_policy USING (k2 = 1)");
        String queryStr = "EXPLAIN select * from test.table1";
        String explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
        Assert.assertTrue(explainString.contains("`k1` = 1, `k2` = 1, `k2` = 2 OR `k2` = 1"));
        dropPolicy("DROP ROW POLICY test_row_policy1 ON test.table1");
        dropPolicy("DROP ROW POLICY test_row_policy2 ON test.table1");
        dropPolicy("DROP ROW POLICY test_row_policy3 ON test.table1");
        dropPolicy("DROP ROW POLICY test_row_policy4 ON test.table1");
    }
    
    @Test
    public void testComplexSql() throws Exception {
        createPolicy("CREATE ROW POLICY test_row_policy1 ON test.table1 AS RESTRICTIVE TO test_policy USING (k1 = 1)");
        createPolicy("CREATE ROW POLICY test_row_policy2 ON test.table1 AS RESTRICTIVE TO test_policy USING (k2 = 1)");
        String joinSql = "select * from table1 join table2 on table1.k1=table2.k1";
        Assert.assertTrue(UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, joinSql).contains(
                "     TABLE: table1\n" +
                        "     PREAGGREGATION: OFF. Reason: No AggregateInfo\n" +
                        "     PREDICATES: `k1` = 1, `k2` = 1"));
        String unionSql = "select * from table1 union select * from table2";
        Assert.assertTrue(UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, unionSql).contains(
                "     TABLE: table1\n" +
                        "     PREAGGREGATION: OFF. Reason: No AggregateInfo\n" +
                        "     PREDICATES: `k1` = 1, `k2` = 1"));
        String subQuerySql = "select * from table2 where k1 in (select k1 from table1)";
        Assert.assertTrue(UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, subQuerySql).contains(
                "     TABLE: table1\n" +
                        "     PREAGGREGATION: OFF. Reason: No AggregateInfo\n" +
                        "     PREDICATES: `k1` = 1, `k2` = 1"));
        String aliasSql = "select * from table1 t1 join table2 t2 on t1.k1=t2.k1";
        Assert.assertTrue(UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, aliasSql).contains(
                "     TABLE: table1\n" +
                        "     PREAGGREGATION: OFF. Reason: No AggregateInfo\n" +
                        "     PREDICATES: `k1` = 1, `k2` = 1"));
        dropPolicy("DROP ROW POLICY test_row_policy1 ON test.table1");
        dropPolicy("DROP ROW POLICY test_row_policy2 ON test.table1");
    }
    
    private static void createPolicy(String sql) throws Exception {
        CreatePolicyStmt createPolicyStmt = (CreatePolicyStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        Catalog.getCurrentCatalog().getPolicyMgr().createPolicy(createPolicyStmt);
    }
    
    private static void dropPolicy(String sql) throws Exception {
        DropPolicyStmt stmt = (DropPolicyStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        Catalog.getCurrentCatalog().getPolicyMgr().dropPolicy(stmt);
    }
}
