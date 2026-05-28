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

import org.apache.doris.common.Config;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.meta.MetaContext;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.AlterTableCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateDatabaseCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateTableCommand;
import org.apache.doris.nereids.trees.plans.commands.DropDatabaseCommand;
import org.apache.doris.nereids.trees.plans.commands.info.DropDatabaseInfo;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.resource.Tag;
import org.apache.doris.utframe.UtFrameUtils;

import com.google.common.collect.Multimap;
import com.google.common.collect.Table;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class TenantLevelColocateTableTest {

    private static final String runningDir = "fe/mocked/TenantLevelColocateTableTest" + UUID.randomUUID() + "/";

    private static ConnectContext connectContext;

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @BeforeClass
    public static void beforeClass() throws Exception {
        Config.allow_replica_on_same_host = true;
        UtFrameUtils.createDorisCluster(runningDir, 3);
        connectContext = UtFrameUtils.createDefaultCtx();
    }

    @AfterClass
    public static void tearDown() {
        File file = new File(runningDir);
        file.delete();
        Config.allow_replica_on_same_host = false;
    }

    @Before
    public void createDb() throws Exception {
        String createDbStmtStr = "create database testDb";
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan logicalPlan = nereidsParser.parseSingle(createDbStmtStr);
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, createDbStmtStr);
        if (logicalPlan instanceof CreateDatabaseCommand) {
            try {
                ((CreateDatabaseCommand) logicalPlan).run(connectContext, stmtExecutor);
            } catch (Throwable e) {
                e.printStackTrace();
            }
        }
        Env.getCurrentEnv().setTenantLevelColocateTableIndex(new TenantLevelColocateTableIndex());
    }

    @After
    public void dropDb() throws Exception {
        String dropDbStmtStr = "drop database testDb force";
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan logicalPlan = nereidsParser.parseSingle(dropDbStmtStr);
        DropDatabaseCommand command = (DropDatabaseCommand) logicalPlan;
        DropDatabaseInfo dropDatabaseInfo = command.getDropDatabaseInfo();
        Env.getCurrentEnv().dropDb(
                dropDatabaseInfo.getCatalogName(),
                dropDatabaseInfo.getDatabaseName(),
                dropDatabaseInfo.isIfExists(),
                dropDatabaseInfo.isForce());
    }

    private static void createTable(String sql) throws Exception {
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan parsed = nereidsParser.parseSingle(sql);
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, sql);
        if (parsed instanceof CreateTableCommand) {
            ((CreateTableCommand) parsed).run(connectContext, stmtExecutor);
        }
    }

    private static void alterTable(String sql) throws Exception {
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan parsed = nereidsParser.parseSingle(sql);
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, sql);
        if (parsed instanceof AlterTableCommand) {
            ((AlterTableCommand) parsed).run(connectContext, stmtExecutor);
        }
    }

    @Test
    public void testSerialization() throws Exception {
        createTable("create table testDb.t1 (\n"
                + " `k1` int NULL COMMENT '',\n"
                + " `k2` varchar(10) NULL COMMENT ''\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`k1`, `k2`)\n"
                + "COMMENT 'OLAP'\n"
                + "DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 5\n"
                + "PROPERTIES (\n"
                + " 'replication_num' = '1',\n"
                + " 'colocate_group' = 'tag.location.default:g1'\n"
                + ");");

        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException("testDb");
        long tableId = db.getTableOrMetaException("t1").getId();

        MetaContext metaContext = new MetaContext();
        metaContext.setMetaVersion(FeMetaVersion.VERSION_CURRENT);
        metaContext.setThreadLocalInfo();

        // 1. Write objects to file
        Path path = Paths.get("./TenantLevelColocateTableTestSerialization");
        Files.deleteIfExists(path);
        Files.createFile(path);
        DataOutputStream dos = new DataOutputStream(Files.newOutputStream(path));

        TenantLevelColocateTableIndex index1 = Env.getCurrentTenantLevelColocateIndex();
        index1.write(dos);
        dos.flush();
        dos.close();

        // 2. Read objects from file
        DataInputStream dis = new DataInputStream(Files.newInputStream(path));

        TenantLevelColocateTableIndex index2 = new TenantLevelColocateTableIndex();
        index2.read(dis);

        Assert.assertEquals(1, Deencapsulation.<Multimap<Long, Long>>getField(index2, "masterGroup2Tables").size());
        Assert.assertEquals(1, index2.getAllGroupIds().size());
        Assert.assertEquals(1, Deencapsulation.<Table<Long, Tag, Long>>getField(index2, "table2MasterGroup").size());
        Assert.assertEquals(1, Deencapsulation.<Map<Long, List<List<Long>>>>getField(index2, "group2BackendsPerBucketSeq").size());
        Assert.assertEquals(1, Deencapsulation.<Map<Long, TenantLevelColocateGroupSchema>>getField(index2, "group2Schema").size());
        Assert.assertEquals(0, index2.getUnstableMasterGroupIds().size());

        Assert.assertTrue(index2.isColocateMasterTable(tableId));
        Tag tag = Tag.DEFAULT_BACKEND_TAG;
        Assert.assertEquals(GsonUtils.GSON.toJson(index1.getGroupSchema("g1", tag)),
                GsonUtils.GSON.toJson(index2.getGroupSchema("g1", tag)));

        // 3. delete files
        dis.close();
        Files.deleteIfExists(path);
    }

    @Test
    public void testCreateTwoTable() throws Exception {
        createTable("create table testDb.t1 (\n"
                + " `k1` int NULL COMMENT '',\n"
                + " `k2` varchar(10) NULL COMMENT ''\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`k1`, `k2`)\n"
                + "COMMENT 'OLAP'\n"
                + "DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 5\n"
                + "PROPERTIES (\n"
                + " 'replication_num' = '2',\n"
                + " 'colocate_group' = 'tag.location.default:g1'\n"
                + ");");

        createTable("create table testDb.t2 (\n"
                + " `k1` int NULL COMMENT '',\n"
                + " `k2` varchar(10) NULL COMMENT ''\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`k1`, `k2`)\n"
                + "COMMENT 'OLAP'\n"
                + "DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 10\n"
                + "PROPERTIES (\n"
                + " 'replication_num' = '2',\n"
                + " 'colocate_slave' = 'tag.location.default:g1'\n"
                + ");");

        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException("testDb");
        long t1 = db.getTableOrMetaException("t1").getId();
        long t2 = db.getTableOrMetaException("t2").getId();
        TenantLevelColocateTableIndex index = Env.getCurrentTenantLevelColocateIndex();
        Assert.assertTrue(index.isColocateMasterTable(t1));
        Assert.assertTrue(index.isColocateSlaveTable(t2));

        Tag tag = Tag.DEFAULT_BACKEND_TAG;
        Long groupId = index.getMasterGroupByTable(t1, tag);
        Assert.assertNotNull(groupId);
        TenantLevelColocateGroupSchema groupSchema = index.getGroupSchema(groupId);
        Assert.assertNotNull(groupSchema);

        Assert.assertTrue(groupId.equals(index.getMasterGroupByTable(t1).get(tag).getGroupId()));
        Assert.assertTrue(groupId.equals(index.getSlaveGroupByTable(t2).get(tag).getGroupId()));

        Assert.assertEquals(3, index.getBackendsByGroup(groupId).size());
        Assert.assertEquals(3, index.getBackendsByTable(t1).size());
        Assert.assertEquals(5, index.getBackendsPerBucketSeqByGroup(groupId).size());
        Assert.assertEquals(5, index.getBackendsPerBucketSeqByTable(t1, 5).get(tag).size());
        Assert.assertEquals(10, index.getBackendsPerBucketSeqByTable(t2, 10).get(tag).size());
        Assert.assertEquals(5, index.getBackendsPerBucketSeqSet(groupId).size());
        Assert.assertEquals(10, TenantLevelColocateTableIndex.getSlaveBackendsPerBucketSeqSet(
                index.getBackendsPerBucketSeqByTable(t1, 5).get(tag), 10).size());
        Assert.assertEquals(2, index.getTabletBackendsByTable(t1, 3).size());
        Assert.assertEquals(index.getTabletBackendsByTable(t1, 3), index.getTabletBackendsByTable(t2, 8));

        alterTable("alter table testDb.t1 set('colocate_group' = '')");
        Assert.assertFalse(index.isColocateTable(t1));
        Assert.assertTrue(index.isColocateSlaveTable(t2));
        Assert.assertNotNull(index.getGroupSchema(groupId));

        alterTable("alter table testDb.t2 set('colocate_slave' = '')");
        Assert.assertFalse(index.isColocateTable(t2));
        Assert.assertNull(index.getGroupSchema(groupId));
    }
}
