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

package org.apache.doris.planner;

import org.apache.doris.analysis.CreateDbStmt;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.UtFrameUtils;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.UUID;

public class ProjectPlannerFunctionTest {

    private static String runningDir = "fe/mocked/ProjectPlannerFunctionTest/" + UUID.randomUUID().toString() + "/";

    private static ConnectContext connectContext;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createDorisCluster(runningDir);

        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();

        // enable hash project
        Deencapsulation.setField(connectContext.getSessionVariable(), "enableProjection", true);

        // create database
        String createDbStmtStr = "create database test;";
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt(createDbStmtStr, connectContext);
        Env.getCurrentEnv().createDb(createDbStmt);
        String createTableStmtStr = "create table test.t1 (k1 int, k2 int) distributed by hash (k1) "
                + "properties(\"replication_num\" = \"1\")";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(createTableStmtStr, connectContext);
        Env.getCurrentEnv().createTable(createTableStmt);
    }

    @AfterClass
    public static void tearDown() {
        File file = new File(runningDir);
        file.delete();
    }

    // keep a.k2 after a join b
    @Test
    public void projectByAgg() throws Exception {
        String queryStr = "desc verbose select /*+ SET_VAR(enable_nereids_planner=false) */ a.k2 from test.t1 a , test.t1 b where a.k1=b.k1 group by a.k2;";
        String explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
        Assert.assertTrue(explainString.contains("output slot ids: 0"));
    }

    // keep a.k2 after a join b
    @Test
    public void projectBySort() throws Exception {
        String queryStr = "desc verbose select /*+ SET_VAR(enable_nereids_planner=false) */ a.k2 from test.t1 a , test.t1 b where a.k1=b.k1 order by a.k2;";
        String explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
        Assert.assertTrue(explainString.contains("output slot ids: 0"));
    }

    // keep a.k2 after a join c
    // keep a.k1, a.k2 after a join b
    @Test
    public void projectByJoin() throws Exception {
        String queryStr = "desc verbose select /*+ SET_VAR(enable_nereids_planner=false) */ a.k2 from test.t1 a inner join test.t1 b on a.k1=b.k1 "
                + "inner join test.t1 c on a.k1=c.k1;";
        String explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
        Assert.assertTrue(explainString.contains("output slot ids: 8"));
        Assert.assertTrue(explainString.contains("output slot ids: 4 5"));
    }

    // keep a.k2 after a join b
    @Test
    public void projectByResultExprs() throws Exception {
        String queryStr = "desc verbose select /*+ SET_VAR(enable_nereids_planner=false) */ a.k2 from test.t1 a , test.t1 b where a.k1=b.k1;";
        String explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
        Assert.assertTrue(explainString.contains("output slot ids: 0"));
    }

    // keep b.k1 after a join b
    // keep a.k2, b.k1, b.k2 after <a,b> hash table
    @Test
    public void projectHashTable() throws Exception {
        String queryStr = "desc verbose select /*+ SET_VAR(enable_nereids_planner=false) */ b.k1 from test.t1 a right join test.t1 b on a.k1=b.k1 and b.k2>1 where a.k2>1;";
        String explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
        Assert.assertTrue(explainString.contains("output slot ids: 1"));
        Assert.assertTrue(explainString.contains("hash output slot ids: 1 2 3"));
    }
}
