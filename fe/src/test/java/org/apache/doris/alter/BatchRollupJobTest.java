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

package org.apache.doris.alter;

import org.apache.doris.analysis.AlterTableStmt;
import org.apache.doris.analysis.CreateDbStmt;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.OlapTable.OlapTableState;
import org.apache.doris.catalog.Partition;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.UtFrameUtils;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;
import java.util.UUID;

public class BatchRollupJobTest {

    private static String runningDir = "fe/mocked/BatchRollupJobTest/" + UUID.randomUUID().toString() + "/";

    @BeforeClass
    public static void setup() throws Exception {
        UtFrameUtils.createMinDorisCluster(runningDir);
    }

    @Test
    public void test() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        // create database db1
        String createDbStmtStr = "create database db1;";
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt(createDbStmtStr, ctx);
        Catalog.getCurrentCatalog().createDb(createDbStmt);
        System.out.println(Catalog.getCurrentCatalog().getDbNames());
        // create table tbl1
        String createTblStmtStr1 = "create table db1.tbl1(k1 int, k2 int, k3 int) distributed by hash(k1) buckets 3 properties('replication_num' = '1');";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(createTblStmtStr1, ctx);
        Catalog.getCurrentCatalog().createTable(createTableStmt);

        // batch add 3 rollups
        String stmtStr = "alter table db1.tbl1 add rollup r1(k1) duplicate key(k1), r2(k1, k2) duplicate key(k1), r3(k2) duplicate key(k2);";
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseAndAnalyzeStmt(stmtStr, ctx);
        Catalog.getCurrentCatalog().getAlterInstance().processAlterTable(alterTableStmt);

        Map<Long, AlterJobV2> alterJobs = Catalog.getCurrentCatalog().getRollupHandler().getAlterJobsV2();
        Assert.assertEquals(3, alterJobs.size());

        Database db = Catalog.getCurrentCatalog().getDb("default_cluster:db1");
        Assert.assertNotNull(db);
        OlapTable tbl = (OlapTable) db.getTable("tbl1");
        Assert.assertNotNull(tbl);

        int finishedNum = 0;
        for (AlterJobV2 alterJobV2 : alterJobs.values()) {
            if (alterJobV2.getType() != AlterJobV2.JobType.ROLLUP) {
                continue;
            }
            while (!alterJobV2.getJobState().isFinalState()) {
                System.out.println(
                        "rollup job " + alterJobV2.getJobId() + " is running. state: " + alterJobV2.getJobState());
                Thread.sleep(5000);
            }
            System.out.println("rollup job " + alterJobV2.getJobId() + " is done. state: " + alterJobV2.getJobState());
            Assert.assertEquals(AlterJobV2.JobState.FINISHED, alterJobV2.getJobState());
            ++finishedNum;
            if (finishedNum == 3) {
                Thread.sleep(100);
                Assert.assertEquals(OlapTableState.NORMAL, tbl.getState());
            } else {
                Assert.assertEquals(OlapTableState.ROLLUP, tbl.getState());
            }
        }

        for (Partition partition : tbl.getPartitions()) {
            Assert.assertEquals(4, partition.getMaterializedIndices(IndexExtState.VISIBLE).size());
        }
    }
}
