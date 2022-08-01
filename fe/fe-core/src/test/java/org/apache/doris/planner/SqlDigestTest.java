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
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.UtFrameUtils;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.UUID;

public class SqlDigestTest {

    private static String runningDir = "fe/mocked/SqlDigestTest/" + UUID.randomUUID().toString() + "/";

    private static ConnectContext connectContext;

    @BeforeClass
    public static void setUp() throws Exception {
        UtFrameUtils.createDorisCluster(runningDir);
        connectContext = UtFrameUtils.createDefaultCtx();

        String createDbStmtStr = "create database db1;";
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt(createDbStmtStr, connectContext);
        Env.getCurrentEnv().createDb(createDbStmt);
        // 3. create table tbl1
        String createTblStmtStr = "create table db1.tbl1(k1 varchar(32), k2 varchar(32), k3 varchar(32), k4 int) "
                + "AGGREGATE KEY(k1, k2,k3,k4) distributed by hash(k1) buckets 3 properties('replication_num' = '1');";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(createTblStmtStr, connectContext);
        Env.getCurrentEnv().createTable(createTableStmt);
    }

    @AfterClass
    public static void tearDown() {
        File file = new File(runningDir);
        file.delete();
    }

    @Test
    public void testWhere() throws Exception {
        String sql1 = "select k4 from tbl1 where k1 > 1";
        String sql2 = "select k4 from tbl1 where k1 > 5";
        String digest1 = UtFrameUtils.getStmtDigest(connectContext, sql1);
        String digest2 = UtFrameUtils.getStmtDigest(connectContext, sql2);
        Assert.assertEquals(digest1, digest2);

        sql1 = "select k4 from tbl1 where k1 like 'xxx' ";
        sql2 = "select k4 from tbl1 where k1 like 'kkskkkkkkkkk' ";
        digest1 = UtFrameUtils.getStmtDigest(connectContext, sql1);
        digest2 = UtFrameUtils.getStmtDigest(connectContext, sql2);
        Assert.assertEquals(digest1, digest2);


        sql1 = "select k4 from tb1 where k1 < 2 and k2 > -1 ";
        sql2 = "select k4 from tb1 where k1 < 1000   and k2 > 100000 ";
        digest1 = UtFrameUtils.getStmtDigest(connectContext, sql1);
        digest2 = UtFrameUtils.getStmtDigest(connectContext, sql2);
        Assert.assertEquals(digest1, digest2);

        sql1 = "select k4 from tb1 where k1 < 2 or k2 > -1 ";
        sql2 = "select k4 from tb1 where k1 < 3 or  k2 > 100000 ";
        digest1 = UtFrameUtils.getStmtDigest(connectContext, sql1);
        digest2 = UtFrameUtils.getStmtDigest(connectContext, sql2);
        Assert.assertEquals(digest1, digest2);

        sql1 = "select k4 from tb1 where not k1 < 2  ";
        sql2 = "select k4 from tb1 where not k1 < 3";
        digest1 = UtFrameUtils.getStmtDigest(connectContext, sql1);
        digest2 = UtFrameUtils.getStmtDigest(connectContext, sql2);
        Assert.assertEquals(digest1, digest2);

        sql1 = "select k4 from tb1 where not k1 < 2  ";
        sql2 = "select k4 from tb1 where not k1 > 3";
        digest1 = UtFrameUtils.getStmtDigest(connectContext, sql1);
        digest2 = UtFrameUtils.getStmtDigest(connectContext, sql2);
        Assert.assertNotEquals(digest1, digest2);
    }

    @Test
    public void testLimit() throws Exception {
        String sql1 = "select k4 from tb1 where k1 > 1 limit 1";
        String sql2 = "select k4 from tb1 where k1 > 5 limit 20";
        String digest1 = UtFrameUtils.getStmtDigest(connectContext, sql1);
        String digest2 = UtFrameUtils.getStmtDigest(connectContext, sql2);
        Assert.assertEquals(digest1, digest2);

        sql1 = "select k4 from tb1 where k1 > 1 order by k1 limit 1";
        sql2 = "select k4 from tb1 where k1 > 5 order by k1 limit 20";
        digest1 = UtFrameUtils.getStmtDigest(connectContext, sql1);
        digest2 = UtFrameUtils.getStmtDigest(connectContext, sql2);
        Assert.assertEquals(digest1, digest2);
    }

    @Test
    public void testFunction() throws Exception {
        String sql1 = "select substr(k4, 1, 2) from tb1 where k1 > 1 limit 1";
        String sql2 = "select substr(k4, 1, 5) from tb1 where k1 > 1 limit 1";
        String digest1 = UtFrameUtils.getStmtDigest(connectContext, sql1);
        String digest2 = UtFrameUtils.getStmtDigest(connectContext, sql2);
        Assert.assertEquals(digest1, digest2);
    }

    @Test
    public void testArithmetic() throws Exception {
        String sql1 = "select k1 + 1 from tb1";
        String sql2 = "select k1 + 2 from tb1";
        String digest1 = UtFrameUtils.getStmtDigest(connectContext, sql1);
        String digest2 = UtFrameUtils.getStmtDigest(connectContext, sql2);
        Assert.assertEquals(digest1, digest2);
    }

    @Test
    public void testCaseWhen() throws Exception {
        String sql1 = "select k1+20, case k2 when k3 then 1 else 0 end from tbl1 where k4 is null";
        String sql2 = "select k1+20, case k2 when k3 then 1000 else 9999999 end from tbl1 where k4 is null";
        String digest1 = UtFrameUtils.getStmtDigest(connectContext, sql1);
        String digest2 = UtFrameUtils.getStmtDigest(connectContext, sql2);
        Assert.assertEquals(digest1, digest2);
    }
}
