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
import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.FeConstants;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.UtFrameUtils;

import org.apache.commons.lang.StringUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.UUID;

public class ColocatePlanTest {
    private static final String COLOCATE_ENABLE = "colocate: true";
    private static String runningDir = "fe/mocked/DemoTest/" + UUID.randomUUID().toString() + "/";
    private static ConnectContext ctx;

    @BeforeClass
    public static void setUp() throws Exception {
        FeConstants.runningUnitTest = true;
        UtFrameUtils.createDorisCluster(runningDir, 2);
        ctx = UtFrameUtils.createDefaultCtx();
        String createDbStmtStr = "create database db1;";
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt(createDbStmtStr, ctx);
        Catalog.getCurrentCatalog().createDb(createDbStmt);
        // create table test_colocate (k1 int ,k2 int, k3 int, k4 int)
        // distributed by hash(k1, k2) buckets 10
        // properties ("replication_num" = "2");
        String createColocateTblStmtStr = "create table db1.test_colocate(k1 int, k2 int, k3 int, k4 int) "
                + "distributed by hash(k1, k2) buckets 10 properties('replication_num' = '2',"
                + "'colocate_with' = 'group1');";
        CreateTableStmt createColocateTableStmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(createColocateTblStmtStr, ctx);
        Catalog.getCurrentCatalog().createTable(createColocateTableStmt);
        String createTblStmtStr = "create table db1.test(k1 int, k2 int, k3 int, k4 int)"
                + "partition by range(k1) (partition p1 values less than (\"1\"), partition p2 values less than (\"2\"))"
                + "distributed by hash(k1, k2) buckets 10 properties('replication_num' = '2')";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(createTblStmtStr, ctx);
        Catalog.getCurrentCatalog().createTable(createTableStmt);
    }

    @AfterClass
    public static void tearDown() {
        File file = new File(runningDir);
        file.delete();
    }

    // without
    // 1. agg: group by column < distributed columns
    // 2. join: src data has been redistributed
    @Test
    public void sqlDistributedSmallerThanData1() throws Exception {
        String sql = "explain select * from (select k1 from db1.test_colocate group by k1) a , db1.test_colocate b "
                + "where a.k1=b.k1";
        String plan1 = UtFrameUtils.getSQLPlanOrErrorMsg(ctx, sql);
        Assert.assertEquals(2, StringUtils.countMatches(plan1, "AGGREGATE"));
        Assert.assertTrue(plan1.contains(DistributedPlanColocateRule.REDISTRIBUTED_SRC_DATA));
    }

    // without : join column < distributed columns;
    @Test
    public void sqlDistributedSmallerThanData2() throws Exception {
        String sql = "explain select * from (select k1 from db1.test_colocate group by k1, k2) a , db1.test_colocate b "
                + "where a.k1=b.k1";
        String plan1 = UtFrameUtils.getSQLPlanOrErrorMsg(ctx, sql);
        Assert.assertTrue(plan1.contains(DistributedPlanColocateRule.INCONSISTENT_DISTRIBUTION_OF_TABLE_AND_QUERY));
    }

    // with:
    // 1. agg columns = distributed columns
    // 2. hash columns = agg output columns = distributed columns
    @Test
    public void sqlAggAndJoinSameAsTableMeta() throws Exception {
        String sql = "explain select * from (select k1, k2 from db1.test_colocate group by k1, k2) a , db1.test_colocate b "
                + "where a.k1=b.k1 and a.k2=b.k2";
        String plan1 = UtFrameUtils.getSQLPlanOrErrorMsg(ctx, sql);
        Assert.assertEquals(1, StringUtils.countMatches(plan1, "AGGREGATE"));
        Assert.assertTrue(plan1.contains(COLOCATE_ENABLE));
    }

    // with:
    // 1. agg columns > distributed columns
    // 2. hash columns = agg output columns > distributed columns
    @Test
    public void sqlAggAndJoinMoreThanTableMeta() throws Exception {
        String sql = "explain select * from (select k1, k2, k3 from db1.test_colocate group by k1, k2, k3) a , "
                + "db1.test_colocate b where a.k1=b.k1 and a.k2=b.k2 and a.k3=b.k3";
        String plan1 = UtFrameUtils.getSQLPlanOrErrorMsg(ctx, sql);
        Assert.assertEquals(1, StringUtils.countMatches(plan1, "AGGREGATE"));
        Assert.assertTrue(plan1.contains(COLOCATE_ENABLE));
    }

    // with:
    // 1. agg columns > distributed columns
    // 2. hash columns = distributed columns
    @Test
    public void sqlAggMoreThanTableMeta() throws Exception {
        String sql = "explain select * from (select k1, k2, k3 from db1.test_colocate group by k1, k2, k3) a , "
                + "db1.test_colocate b where a.k1=b.k1 and a.k2=b.k2";
        String plan1 = UtFrameUtils.getSQLPlanOrErrorMsg(ctx, sql);
        Assert.assertEquals(1, StringUtils.countMatches(plan1, "AGGREGATE"));
        Assert.assertTrue(plan1.contains(COLOCATE_ENABLE));
    }

    // without:
    // 1. agg columns = distributed columns
    // 2. table is not in colocate group
    // 3. more then 1 instances
    // Fixed #6028
    @Test
    public void sqlAggWithNonColocateTable() throws Exception {
        String sql = "explain select k1, k2 from db1.test group by k1, k2";
        String plan1 = UtFrameUtils.getSQLPlanOrErrorMsg(ctx, sql);
        Assert.assertEquals(2, StringUtils.countMatches(plan1, "AGGREGATE"));
        Assert.assertFalse(plan1.contains(COLOCATE_ENABLE));
    }
}
