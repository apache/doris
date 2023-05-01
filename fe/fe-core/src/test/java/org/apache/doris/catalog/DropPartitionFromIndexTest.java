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

package org.apache.doris.catalog;

import org.apache.doris.alter.AlterJobV2;
import org.apache.doris.analysis.AlterTableStmt;
import org.apache.doris.analysis.CreateDbStmt;
import org.apache.doris.analysis.CreateMaterializedViewStmt;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.UtFrameUtils;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.Map;
import java.util.UUID;

public class DropPartitionFromIndexTest {
    private static final String runningDir = "fe/mocked/DropPartitionFromIndex/" + UUID.randomUUID() + "/";

    private static ConnectContext connectContext;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createDorisCluster(runningDir);

        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        // create database
        String createDbStmtStr = "create database test;";
        String createTableStr = "create table test.tbl1(d1 date, k1 int, k2 bigint) duplicate key(d1, k1) "
                + "PARTITION BY RANGE(d1) (PARTITION p20210201 VALUES [('2021-02-01'), ('2021-02-02')),"
                + "PARTITION p20210202 VALUES [('2021-02-02'), ('2021-02-03')),"
                + "PARTITION p20210203 VALUES [('2021-02-03'), ('2021-02-04'))) distributed by hash(k1) "
                + "buckets 1 properties('replication_num' = '1');";
        createDb(createDbStmtStr);
        createTable(createTableStr);
    }

    @AfterClass
    public static void tearDown() {
        File file = new File(runningDir);
        file.delete();
    }

    private static void createDb(String sql) throws Exception {
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        Env.getCurrentEnv().createDb(createDbStmt);
    }

    private static void createTable(String sql) throws Exception {
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        Env.getCurrentEnv().createTable(createTableStmt);
    }

    private static void dropPartition(String sql) throws Exception {
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        Env.getCurrentEnv().alterTable(alterTableStmt);
    }

    @Test
    public void testDropPartitionFromIndex() throws Exception {
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException("default_cluster:test");
        OlapTable table = (OlapTable) db.getTableOrMetaException("tbl1", Table.TableType.OLAP);
        String createMvSql = "create materialized view test_mv as select k1,k1 + k2 from test.tbl1";
        CreateMaterializedViewStmt createMaterializedViewStmt
                = (CreateMaterializedViewStmt) UtFrameUtils.parseAndAnalyzeStmt(createMvSql, connectContext);
        Env.getCurrentEnv().createMaterializedView(createMaterializedViewStmt);
        waitSchemaChangeJobDone(true);
        String dropPartitionSql = "ALTER TABLE test.tbl1 DROP PARTITION p20210202 FROM INDEX tbl1;";
        dropPartition(dropPartitionSql);
        Partition partition = table.getPartition("p20210202");
        Assert.assertNull(partition.getBaseIndex());
        dropPartitionSql = "ALTER TABLE test.tbl1 DROP PARTITION p20210202 FROM INDEX test_mv;";
        dropPartition(dropPartitionSql);
        partition = table.getPartition("p20210202");
        Assert.assertNull(partition);
    }

    private void waitSchemaChangeJobDone(boolean rollupJob) throws Exception {
        Map<Long, AlterJobV2> alterJobs = Env.getCurrentEnv().getSchemaChangeHandler().getAlterJobsV2();
        if (rollupJob) {
            alterJobs = Env.getCurrentEnv().getMaterializedViewHandler().getAlterJobsV2();
        }
        for (AlterJobV2 alterJobV2 : alterJobs.values()) {
            while (!alterJobV2.getJobState().isFinalState()) {
                System.out.println(
                        "alter job " + alterJobV2.getJobId() + " is running. state: " + alterJobV2.getJobState());
                Thread.sleep(1000);
            }
            System.out.println(alterJobV2.getType() + " alter job " + alterJobV2.getJobId() + " is done. state: "
                    + alterJobV2.getJobState());
            Assert.assertEquals(AlterJobV2.JobState.FINISHED, alterJobV2.getJobState());
            Database db =
                    Env.getCurrentInternalCatalog().getDbOrMetaException(alterJobV2.getDbId());
            OlapTable tbl = (OlapTable) db.getTableOrMetaException(alterJobV2.getTableId());
            while (tbl.getState() != OlapTable.OlapTableState.NORMAL) {
                Thread.sleep(1000);
            }
        }
    }
}
