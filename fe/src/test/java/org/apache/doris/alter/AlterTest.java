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
import org.apache.doris.analysis.DateLiteral;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.DataProperty;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TStorageMedium;
import org.apache.doris.utframe.UtFrameUtils;
import com.google.common.collect.Lists;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class AlterTest {

    private static String runningDir = "fe/mocked/AlterTest/" + UUID.randomUUID().toString() + "/";

    private static ConnectContext connectContext;

    @BeforeClass
    public static void beforeClass() throws Exception {
        FeConstants.runningUnitTest = true;
        FeConstants.default_scheduler_interval_millisecond = 100;
        Config.dynamic_partition_enable = true;
        Config.dynamic_partition_check_interval_seconds = 1;
        UtFrameUtils.createMinDorisCluster(runningDir);

        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        // create database
        String createDbStmtStr = "create database test;";
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt(createDbStmtStr, connectContext);
        Catalog.getCurrentCatalog().createDb(createDbStmt);

        createTable("CREATE TABLE test.tbl1\n" + 
                "(\n" + 
                "    k1 date,\n" + 
                "    k2 int,\n" + 
                "    v1 int sum\n" + 
                ")\n" + 
                "PARTITION BY RANGE(k1)\n" + 
                "(\n" + 
                "    PARTITION p1 values less than('2020-02-01'),\n" + 
                "    PARTITION p2 values less than('2020-03-01')\n" + 
                ")\n" + 
                "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" + 
                "PROPERTIES('replication_num' = '1');");

        createTable("CREATE TABLE test.tbl2\n" +
                "(\n" +
                "    k1 date,\n" +
                "    v1 int sum\n" +
                ")\n" +
                "DISTRIBUTED BY HASH (k1) BUCKETS 3\n" +
                "PROPERTIES('replication_num' = '1');");

        createTable("CREATE TABLE test.tbl3\n" +
                "(\n" +
                "    k1 date,\n" +
                "    k2 int,\n" +
                "    v1 int sum\n" +
                ")\n" +
                "PARTITION BY RANGE(k1)\n" +
                "(\n" +
                "    PARTITION p1 values less than('2020-02-01'),\n" +
                "    PARTITION p2 values less than('2020-03-01')\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                "PROPERTIES('replication_num' = '1');");

        createTable("CREATE TABLE test.tbl4\n" +
                "(\n" +
                "    k1 date,\n" +
                "    k2 int,\n" +
                "    v1 int sum\n" +
                ")\n" +
                "PARTITION BY RANGE(k1)\n" +
                "(\n" +
                "    PARTITION p1 values less than('2020-02-01'),\n" +
                "    PARTITION p2 values less than('2020-03-01'),\n" +
                "    PARTITION p3 values less than('2020-04-01'),\n" +
                "    PARTITION p4 values less than('2020-05-01')\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                "PROPERTIES" +
                "(" +
                "    'replication_num' = '1',\n" +
                "    'in_memory' = 'false',\n" +
                "    'storage_medium' = 'SSD',\n" +
                "    'storage_cooldown_time' = '9999-12-31 00:00:00'\n" +
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

    private static void alterTable(String sql, boolean expectedException) throws Exception {
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        try {
            Catalog.getCurrentCatalog().alterTable(alterTableStmt);
            if (expectedException) {
                Assert.fail();
            }
        } catch (Exception e) {
            e.printStackTrace();
            if (!expectedException) {
                Assert.fail();
            }
        }
    }

    @Test
    public void testConflictAlterOperations() throws Exception {
        String stmt = "alter table test.tbl1 add partition p3 values less than('2020-04-01'), add partition p4 values less than('2020-05-01')";
        alterTable(stmt, true);

        stmt = "alter table test.tbl1 add partition p3 values less than('2020-04-01'), drop partition p4";
        alterTable(stmt, true);

        stmt = "alter table test.tbl1 drop partition p3, drop partition p4";
        alterTable(stmt, true);

        stmt = "alter table test.tbl1 drop partition p3, add column k3 int";
        alterTable(stmt, true);

        // no conflict
        stmt = "alter table test.tbl1 add column k3 int, add column k4 int";
        alterTable(stmt, false);
        waitSchemaChangeJobDone(false);
        
        stmt = "alter table test.tbl1 add rollup r1 (k1)";
        alterTable(stmt, false);
        waitSchemaChangeJobDone(true);

        stmt = "alter table test.tbl1 add rollup r2 (k1), r3 (k1)";
        alterTable(stmt, false);
        waitSchemaChangeJobDone(true);
        
        // enable dynamic partition
        // not adding the `start` property so that it won't drop the origin partition p1, p2 and p3
        stmt = "alter table test.tbl1 set (\n" + 
                "'dynamic_partition.enable' = 'true',\n" + 
                "'dynamic_partition.time_unit' = 'DAY',\n" +
                "'dynamic_partition.end' = '3',\n" + 
                "'dynamic_partition.prefix' = 'p',\n" + 
                "'dynamic_partition.buckets' = '3'\n" + 
                " );";
        alterTable(stmt, false);
        Database db = Catalog.getCurrentCatalog().getDb("default_cluster:test");
        OlapTable tbl = (OlapTable)db.getTable("tbl1");
        Assert.assertTrue(tbl.getTableProperty().getDynamicPartitionProperty().getEnable());
        Assert.assertEquals(4, tbl.getIndexIdToSchema().size());
        
        // add partition when dynamic partition is enable
        stmt = "alter table test.tbl1 add partition p3 values less than('2020-04-01') distributed by hash(k2) buckets 4 PROPERTIES ('replication_num' = '1')";
        alterTable(stmt, true);

        // add temp partition when dynamic partition is enable
        stmt = "alter table test.tbl1 add temporary partition tp3 values less than('2020-04-01') distributed by hash(k2) buckets 4 PROPERTIES ('replication_num' = '1')";
        alterTable(stmt, false);
        Assert.assertEquals(1, tbl.getTempPartitions().size());

        // disable the dynamic partition
        stmt = "alter table test.tbl1 set ('dynamic_partition.enable' = 'false')";
        alterTable(stmt, false);
        Assert.assertFalse(tbl.getTableProperty().getDynamicPartitionProperty().getEnable());
        
        // add partition when dynamic partition is disable
        stmt = "alter table test.tbl1 add partition p3 values less than('2020-04-01') distributed by hash(k2) buckets 4";
        alterTable(stmt, false);

        // set table's default replication num
        Assert.assertEquals(Short.valueOf("1"), tbl.getDefaultReplicationNum());
        stmt = "alter table test.tbl1 set ('default.replication_num' = '3');";
        alterTable(stmt, false);
        Assert.assertEquals(Short.valueOf("3"), tbl.getDefaultReplicationNum());

        // set range table's real replication num
        Partition p1 = tbl.getPartition("p1");
        Assert.assertEquals(Short.valueOf("1"), Short.valueOf(tbl.getPartitionInfo().getReplicationNum(p1.getId())));
        stmt = "alter table test.tbl1 set ('replication_num' = '3');";
        alterTable(stmt, true);
        Assert.assertEquals(Short.valueOf("1"), Short.valueOf(tbl.getPartitionInfo().getReplicationNum(p1.getId())));

        // set un-partitioned table's real replication num
        OlapTable tbl2 = (OlapTable) db.getTable("tbl2");
        Partition partition = tbl2.getPartition(tbl2.getName());
        Assert.assertEquals(Short.valueOf("1"), Short.valueOf(tbl2.getPartitionInfo().getReplicationNum(partition.getId())));
        stmt = "alter table test.tbl2 set ('replication_num' = '3');";
        alterTable(stmt, false);
        Assert.assertEquals(Short.valueOf("3"), Short.valueOf(tbl2.getPartitionInfo().getReplicationNum(partition.getId())));

        Thread.sleep(5000); // sleep to wait dynamic partition scheduler run
        // add partition without set replication num
        stmt = "alter table test.tbl1 add partition p4 values less than('2020-04-10')";
        alterTable(stmt, true);

        // add partition when dynamic partition is disable
        stmt = "alter table test.tbl1 add partition p4 values less than('2020-04-10') ('replication_num' = '1')";
        alterTable(stmt, false);
    }

    // test batch update range partitions' properties
    @Test
    public void testBatchUpdatePartitionProperties() throws Exception {
        Database db = Catalog.getCurrentCatalog().getDb("default_cluster:test");
        OlapTable tbl4 = (OlapTable) db.getTable("tbl4");
        Partition p1 = tbl4.getPartition("p1");
        Partition p2 = tbl4.getPartition("p2");
        Partition p3 = tbl4.getPartition("p3");
        Partition p4 = tbl4.getPartition("p4");

        // batch update replication_num property
        String stmt = "alter table test.tbl4 modify partition (p1, p2, p4) set ('replication_num' = '3')";
        List<Partition> partitionList = Lists.newArrayList(p1, p2, p4);
        for (Partition partition : partitionList) {
            Assert.assertEquals(Short.valueOf("1"), Short.valueOf(tbl4.getPartitionInfo().getReplicationNum(partition.getId())));
        }
        alterTable(stmt, false);
        for (Partition partition : partitionList) {
            Assert.assertEquals(Short.valueOf("3"), Short.valueOf(tbl4.getPartitionInfo().getReplicationNum(partition.getId())));
        }
        Assert.assertEquals(Short.valueOf("1"), Short.valueOf(tbl4.getPartitionInfo().getReplicationNum(p3.getId())));

        // batch update in_memory property
        stmt = "alter table test.tbl4 modify partition (p1, p2, p3) set ('in_memory' = 'true')";
        partitionList = Lists.newArrayList(p1, p2, p3);
        for (Partition partition : partitionList) {
            Assert.assertEquals(false, tbl4.getPartitionInfo().getIsInMemory(partition.getId()));
        }
        alterTable(stmt, false);
        for (Partition partition : partitionList) {
            Assert.assertEquals(true, tbl4.getPartitionInfo().getIsInMemory(partition.getId()));
        }
        Assert.assertEquals(false, tbl4.getPartitionInfo().getIsInMemory(p4.getId()));

        // batch update storage_medium and storage_cool_down properties
        stmt = "alter table test.tbl4 modify partition (p2, p3, p4) set ('storage_medium' = 'HDD')";
        DateLiteral dateLiteral = new DateLiteral("9999-12-31 00:00:00", Type.DATETIME);
        long coolDownTimeMs = dateLiteral.unixTimestamp(TimeUtils.getTimeZone());
        DataProperty oldDataProperty = new DataProperty(TStorageMedium.SSD, coolDownTimeMs);
        partitionList = Lists.newArrayList(p2, p3, p4);
        for (Partition partition : partitionList) {
            Assert.assertEquals(oldDataProperty, tbl4.getPartitionInfo().getDataProperty(partition.getId()));
        }
        alterTable(stmt, false);
        DataProperty newDataProperty = new DataProperty(TStorageMedium.HDD, DataProperty.MAX_COOLDOWN_TIME_MS);
        for (Partition partition : partitionList) {
            Assert.assertEquals(newDataProperty, tbl4.getPartitionInfo().getDataProperty(partition.getId()));
        }
        Assert.assertEquals(oldDataProperty, tbl4.getPartitionInfo().getDataProperty(p1.getId()));

        // batch update range partitions' properties with *
        stmt = "alter table test.tbl4 modify partition (*) set ('replication_num' = '1')";
        partitionList = Lists.newArrayList(p1, p2, p3, p4);
        alterTable(stmt, false);
        for (Partition partition : partitionList) {
            Assert.assertEquals(Short.valueOf("1"), Short.valueOf(tbl4.getPartitionInfo().getReplicationNum(partition.getId())));
        }
    }

    @Test
    public void testDynamicPartitionDropAndAdd() throws Exception {
        // test day range
        String stmt = "alter table test.tbl3 set (\n" +
                "'dynamic_partition.enable' = 'true',\n" +
                "'dynamic_partition.time_unit' = 'DAY',\n" +
                "'dynamic_partition.start' = '-3',\n" +
                "'dynamic_partition.end' = '3',\n" +
                "'dynamic_partition.prefix' = 'p',\n" +
                "'dynamic_partition.buckets' = '3'\n" +
                " );";
        alterTable(stmt, false);
        Thread.sleep(5000); // sleep to wait dynamic partition scheduler run

        Database db = Catalog.getCurrentCatalog().getDb("default_cluster:test");
        OlapTable tbl = (OlapTable) db.getTable("tbl3");
        Assert.assertEquals(4, tbl.getPartitionNames().size());
        Assert.assertNull(tbl.getPartition("p1"));
        Assert.assertNull(tbl.getPartition("p2"));
    }

    private void waitSchemaChangeJobDone(boolean rollupJob) throws InterruptedException {
        Map<Long, AlterJobV2> alterJobs = Catalog.getCurrentCatalog().getSchemaChangeHandler().getAlterJobsV2();
        if (rollupJob) {
            alterJobs = Catalog.getCurrentCatalog().getRollupHandler().getAlterJobsV2();
        }
        for (AlterJobV2 alterJobV2 : alterJobs.values()) {
            while (!alterJobV2.getJobState().isFinalState()) {
                System.out.println("alter job " + alterJobV2.getJobId() + " is running. state: " + alterJobV2.getJobState());
                Thread.sleep(1000);
            }
            System.out.println(alterJobV2.getType() + " alter job " + alterJobV2.getJobId() + " is done. state: " + alterJobV2.getJobState());
            Assert.assertEquals(AlterJobV2.JobState.FINISHED, alterJobV2.getJobState());
        }
    }
}
