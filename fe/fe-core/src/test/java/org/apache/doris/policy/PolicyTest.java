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

import org.apache.doris.analysis.CreateDbStmt;
import org.apache.doris.analysis.CreatePolicyStmt;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.analysis.DropPolicyStmt;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.UtFrameUtils;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.UUID;

public class PolicyTest {

    private static String runningDir = "fe/mocked/policyTest/" + UUID.randomUUID() + "/";

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
        Catalog.getCurrentCatalog().getDb("test");

        MetricRepo.init();
        Catalog.getCurrentCatalog().changeDb(connectContext, "default_cluster:test");
        createTable("create table table1\n" +
            "(k1 int, k2 int) distributed by hash(k1) buckets 1\n" +
            "properties(\"replication_num\" = \"1\");");
        createTable("create table table2\n" +
            "(k1 int, k2 int) distributed by hash(k1) buckets 1\n" +
            "properties(\"replication_num\" = \"1\");");
    }

    @AfterClass
    public static void tearDown() {
        File file = new File(runningDir);
        file.delete();
    }

    @Test
    public void testReWriteSql() throws Exception {
        Catalog.getCurrentCatalog().changeDb(connectContext, "default_cluster:test");
        createPolicy("CREATE POLICY test_row_policy ON test.table1 AS PERMISSIVE TO root USING (k1 = 1)");
        String queryStr = "EXPLAIN select * from test.table1";
        String explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
        Assert.assertTrue(explainString.contains("`k1` = 1"));
    }

    @Test
    public void testDuplicateAddPolicy() throws Exception {
        createPolicy("CREATE POLICY test_row_policy1 ON test.table1 AS PERMISSIVE TO root USING (k1 = 1)");
        ExceptionChecker.expectThrowsWithMsg(DdlException.class, "the policy test_row_policy1 already create",
            () -> createPolicy("CREATE POLICY test_row_policy1 ON test.table1 AS PERMISSIVE TO root USING (k1 = 1)"));
    }

    @Test
    public void testShowPolicy() throws Exception {
        createPolicy("CREATE POLICY test_row_policy3 ON test.table1 AS PERMISSIVE TO root USING (k1 = 1)");
        createPolicy("CREATE POLICY test_row_policy4 ON test.table1 AS PERMISSIVE TO root USING (k2 = 1)");
        int firstSize = Catalog.getCurrentCatalog().getPolicyMgr().getDbUserPolicies(ConnectContext.get().getCurrentDbId(), "root").size();
        Assert.assertTrue(firstSize > 0);
        dropPolicy("DROP POLICY test_row_policy3 ON test.table1");
        dropPolicy("DROP POLICY test_row_policy4 ON test.table1");
        int secondSize = Catalog.getCurrentCatalog().getPolicyMgr().getDbUserPolicies(ConnectContext.get().getCurrentDbId(), "root").size();
        Assert.assertEquals(2, firstSize - secondSize);
    }

    @Test
    public void testMergeFilter() throws Exception {
        createPolicy("CREATE POLICY test_row_policy1 ON test.table2 AS PERMISSIVE TO root USING (k1 = 1)");
        createPolicy("CREATE POLICY test_row_policy2 ON test.table2 AS PERMISSIVE TO root USING (k2 = 1)");
        // filterType use last
        createPolicy("CREATE POLICY test_row_policy3 ON test.table2 AS RESTRICTIVE TO root USING (k2 = 2)");
        String queryStr = "EXPLAIN select * from test.table2";
        String explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
        Assert.assertTrue(explainString.contains("`k1` = 1, `k2` = 1, `k2` = 2"));
    }

    private static void createTable(String sql) throws Exception {
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        Catalog.getCurrentCatalog().createTable(createTableStmt);
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
