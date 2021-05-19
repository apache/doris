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

import org.apache.doris.analysis.CreateDbStmt;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.analysis.DropDbStmt;
import org.apache.doris.catalog.ColocateTableIndex.GroupId;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.UtFrameUtils;

import com.google.common.collect.Multimap;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class ColocateTableTest {
    private static String runningDir = "fe/mocked/ColocateTableTest" + UUID.randomUUID().toString() + "/";

    private static ConnectContext connectContext;
    private static String dbName = "testDb";
    private static String fullDbName = "default_cluster:" + dbName;
    private static String tableName1 = "t1";
    private static String tableName2 = "t2";
    private static String groupName = "group1";

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createDorisCluster(runningDir);
        connectContext = UtFrameUtils.createDefaultCtx();

    }

    @AfterClass
    public static void tearDown() {
            File file = new File(runningDir);
            file.delete();
    }

    @Before
    public void createDb() throws Exception {
        String createDbStmtStr = "create database " + dbName;
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt(createDbStmtStr, connectContext);
        Catalog.getCurrentCatalog().createDb(createDbStmt);
        Catalog.getCurrentCatalog().setColocateTableIndex(new ColocateTableIndex());
    }

    @After
    public void dropDb() throws Exception {
        String dropDbStmtStr = "drop database " + dbName;
        DropDbStmt dropDbStmt = (DropDbStmt) UtFrameUtils.parseAndAnalyzeStmt(dropDbStmtStr, connectContext);
        Catalog.getCurrentCatalog().dropDb(dropDbStmt);
    }

    private static void createTable(String sql) throws Exception {
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        Catalog.getCurrentCatalog().createTable(createTableStmt);
    }

    @Test
    public void testCreateOneTable() throws Exception {
        createTable("create table " + dbName + "." + tableName1 + " (\n" +
                " `k1` int NULL COMMENT \"\",\n" +
                " `k2` varchar(10) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`, `k2`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                " \"replication_num\" = \"1\",\n" +
                " \"colocate_with\" = \"" + groupName + "\"\n" +
                ");");

        ColocateTableIndex index = Catalog.getCurrentColocateIndex();
        Database db = Catalog.getCurrentCatalog().getDb(fullDbName);
        long tableId = db.getTable(tableName1).getId();

        Assert.assertEquals(1, Deencapsulation.<Multimap<GroupId, Long>>getField(index, "group2Tables").size());
        Assert.assertEquals(1, index.getAllGroupIds().size());
        Assert.assertEquals(1, Deencapsulation.<Map<Long, GroupId>>getField(index, "table2Group").size());
        Assert.assertEquals(1, Deencapsulation.<Map<GroupId, List<List<Long>>>>getField(index, "group2BackendsPerBucketSeq").size());
        Assert.assertEquals(1, Deencapsulation.<Map<GroupId, ColocateGroupSchema>>getField(index, "group2Schema").size());
        Assert.assertEquals(0, index.getUnstableGroupIds().size());

        Assert.assertTrue(index.isColocateTable(tableId));

        Long dbId = db.getId();
        Assert.assertEquals(dbId, index.getGroup(tableId).dbId);

        GroupId groupId = index.getGroup(tableId);
        List<Long> backendIds = index.getBackendsPerBucketSeq(groupId).get(0);
        Assert.assertEquals(1, backendIds.size());

        String fullGroupName = dbId + "_" + groupName;
        Assert.assertEquals(tableId, index.getTableIdByGroup(fullGroupName));
        ColocateGroupSchema groupSchema = index.getGroupSchema(fullGroupName);
        Assert.assertNotNull(groupSchema);
        Assert.assertEquals(dbId, groupSchema.getGroupId().dbId);
        Assert.assertEquals(1, groupSchema.getBucketsNum());
        Assert.assertEquals((short) 1, groupSchema.getReplicaAlloc().getTotalReplicaNum());
    }

    @Test
    public void testCreateTwoTableWithSameGroup() throws Exception {
        createTable("create table " + dbName + "." + tableName1 + " (\n" +
                " `k1` int NULL COMMENT \"\",\n" +
                " `k2` varchar(10) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`, `k2`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                " \"replication_num\" = \"1\",\n" +
                " \"colocate_with\" = \"" + groupName + "\"\n" +
                ");");

        createTable("create table " + dbName + "." + tableName2 + " (\n" +
                " `k1` int NULL COMMENT \"\",\n" +
                " `k2` varchar(10) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`, `k2`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                " \"replication_num\" = \"1\",\n" +
                " \"colocate_with\" = \"" + groupName + "\"\n" +
                ");");

        ColocateTableIndex index = Catalog.getCurrentColocateIndex();
        Database db = Catalog.getCurrentCatalog().getDb(fullDbName);
        long firstTblId = db.getTable(tableName1).getId();
        long secondTblId = db.getTable(tableName2).getId();

        Assert.assertEquals(2, Deencapsulation.<Multimap<GroupId, Long>>getField(index, "group2Tables").size());
        Assert.assertEquals(1, index.getAllGroupIds().size());
        Assert.assertEquals(2, Deencapsulation.<Map<Long, GroupId>>getField(index, "table2Group").size());
        Assert.assertEquals(1, Deencapsulation.<Map<GroupId, List<List<Long>>>>getField(index, "group2BackendsPerBucketSeq").size());
        Assert.assertEquals(1, Deencapsulation.<Map<GroupId, ColocateGroupSchema>>getField(index, "group2Schema").size());
        Assert.assertEquals(0, index.getUnstableGroupIds().size());

        Assert.assertTrue(index.isColocateTable(firstTblId));
        Assert.assertTrue(index.isColocateTable(secondTblId));

        Assert.assertTrue(index.isSameGroup(firstTblId, secondTblId));

        // drop first
        index.removeTable(firstTblId);
        Assert.assertEquals(1, Deencapsulation.<Multimap<GroupId, Long>>getField(index, "group2Tables").size());
        Assert.assertEquals(1, index.getAllGroupIds().size());
        Assert.assertEquals(1, Deencapsulation.<Map<Long, GroupId>>getField(index, "table2Group").size());
        Assert.assertEquals(1,
                Deencapsulation.<Map<GroupId, List<List<Long>>>>getField(index, "group2BackendsPerBucketSeq").size());
        Assert.assertEquals(0, index.getUnstableGroupIds().size());

        Assert.assertFalse(index.isColocateTable(firstTblId));
        Assert.assertTrue(index.isColocateTable(secondTblId));
        Assert.assertFalse(index.isSameGroup(firstTblId, secondTblId));

        // drop second
        index.removeTable(secondTblId);
        Assert.assertEquals(0, Deencapsulation.<Multimap<GroupId, Long>>getField(index, "group2Tables").size());
        Assert.assertEquals(0, index.getAllGroupIds().size());
        Assert.assertEquals(0, Deencapsulation.<Map<Long, GroupId>>getField(index, "table2Group").size());
        Assert.assertEquals(0,
                Deencapsulation.<Map<GroupId, List<List<Long>>>>getField(index, "group2BackendsPerBucketSeq").size());
        Assert.assertEquals(0, index.getUnstableGroupIds().size());

        Assert.assertFalse(index.isColocateTable(firstTblId));
        Assert.assertFalse(index.isColocateTable(secondTblId));
    }

    @Test
    public void testBucketNum() throws Exception {
        createTable("create table " + dbName + "." + tableName1 + " (\n" +
                " `k1` int NULL COMMENT \"\",\n" +
                " `k2` varchar(10) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`, `k2`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                " \"replication_num\" = \"1\",\n" +
                " \"colocate_with\" = \"" + groupName + "\"\n" +
                ");");

        expectedEx.expect(DdlException.class);
        expectedEx.expectMessage("Colocate tables must have same bucket num: 1");
        createTable("create table " + dbName + "." + tableName2 + " (\n" +
                " `k1` int NULL COMMENT \"\",\n" +
                " `k2` varchar(10) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`, `k2`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 2\n" +
                "PROPERTIES (\n" +
                " \"replication_num\" = \"1\",\n" +
                " \"colocate_with\" = \"" + groupName + "\"\n" +
                ");");

    }

    @Test
    public void testReplicationNum() throws Exception {
        createTable("create table " + dbName + "." + tableName1 + " (\n" +
                " `k1` int NULL COMMENT \"\",\n" +
                " `k2` varchar(10) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`, `k2`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                " \"replication_num\" = \"1\",\n" +
                " \"colocate_with\" = \"" + groupName + "\"\n" +
                ");");

        expectedEx.expect(DdlException.class);
        expectedEx.expectMessage("Colocate tables must have same replication allocation: tag.location.default: 1");
        createTable("create table " + dbName + "." + tableName2 + " (\n" +
                " `k1` int NULL COMMENT \"\",\n" +
                " `k2` varchar(10) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`, `k2`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                " \"replication_num\" = \"2\",\n" +
                " \"colocate_with\" = \"" + groupName + "\"\n" +
                ");");
    }

    @Test
    public void testDistributionColumnsSize() throws Exception {
        createTable("create table " + dbName + "." + tableName1 + " (\n" +
                " `k1` int NULL COMMENT \"\",\n" +
                " `k2` varchar(10) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`, `k2`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                " \"replication_num\" = \"1\",\n" +
                " \"colocate_with\" = \"" + groupName + "\"\n" +
                ");");

        expectedEx.expect(DdlException.class);
        expectedEx.expectMessage("Colocate tables distribution columns size must be same : 2");
        createTable("create table " + dbName + "." + tableName2 + " (\n" +
                " `k1` int NULL COMMENT \"\",\n" +
                " `k2` varchar(10) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`, `k2`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                " \"replication_num\" = \"1\",\n" +
                " \"colocate_with\" = \"" + groupName + "\"\n" +
                ");");
    }

    @Test
    public void testDistributionColumnsType() throws Exception {
        createTable("create table " + dbName + "." + tableName1 + " (\n" +
                " `k1` int NULL COMMENT \"\",\n" +
                " `k2` int NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`, `k2`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                " \"replication_num\" = \"1\",\n" +
                " \"colocate_with\" = \"" + groupName + "\"\n" +
                ");");

        expectedEx.expect(DdlException.class);
        expectedEx.expectMessage("Colocate tables distribution columns must have the same data type: k2 should be INT");
        createTable("create table " + dbName + "." + tableName2 + " (\n" +
                " `k1` int NULL COMMENT \"\",\n" +
                " `k2` varchar(10) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`, `k2`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                " \"replication_num\" = \"1\",\n" +
                " \"colocate_with\" = \"" + groupName + "\"\n" +
                ");");
    }
}
