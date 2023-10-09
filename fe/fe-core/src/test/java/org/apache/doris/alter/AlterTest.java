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
import org.apache.doris.analysis.CreateMaterializedViewStmt;
import org.apache.doris.analysis.CreatePolicyStmt;
import org.apache.doris.analysis.CreateResourceStmt;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.analysis.DateLiteral;
import org.apache.doris.analysis.DropResourceStmt;
import org.apache.doris.analysis.ShowCreateMaterializedViewStmt;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DataProperty;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MysqlTable;
import org.apache.doris.catalog.OdbcTable;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowExecutor;
import org.apache.doris.resource.Tag;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TStorageMedium;
import org.apache.doris.utframe.UtFrameUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
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
    private static Backend be;

    @BeforeClass
    public static void beforeClass() throws Exception {
        FeConstants.runningUnitTest = true;
        FeConstants.default_scheduler_interval_millisecond = 100;
        Config.dynamic_partition_check_interval_seconds = 1;
        Config.disable_storage_medium_check = true;
        Config.enable_storage_policy = true;
        UtFrameUtils.createDorisCluster(runningDir);

        be = Env.getCurrentSystemInfo().getIdToBackend().values().asList().get(0);

        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        // create database
        String createDbStmtStr = "create database test;";
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt(createDbStmtStr, connectContext);
        Env.getCurrentEnv().createDb(createDbStmt);

        createTable("CREATE TABLE test.tbl1\n" + "(\n" + "    k1 date,\n" + "    k2 int,\n" + "    v1 int sum\n" + ")\n"
                + "PARTITION BY RANGE(k1)\n" + "(\n" + "    PARTITION p1 values less than('2020-02-01'),\n"
                + "    PARTITION p2 values less than('2020-03-01')\n" + ")\n" + "DISTRIBUTED BY HASH(k2) BUCKETS 3\n"
                + "PROPERTIES('replication_num' = '1');");

        createTable("CREATE TABLE test.tbl2\n" + "(\n" + "    k1 date,\n" + "    v1 int sum\n" + ")\n"
                + "DISTRIBUTED BY HASH (k1) BUCKETS 3\n" + "PROPERTIES('replication_num' = '1');");

        createTable("CREATE TABLE test.tbl3\n" + "(\n" + "    k1 date,\n" + "    k2 int,\n" + "    v1 int sum\n" + ")\n"
                + "PARTITION BY RANGE(k1)\n" + "(\n" + "    PARTITION p1 values less than('2020-02-01'),\n"
                + "    PARTITION p2 values less than('2020-03-01')\n" + ")\n" + "DISTRIBUTED BY HASH(k2) BUCKETS 3\n"
                + "PROPERTIES('replication_num' = '1');");

        createTable("CREATE TABLE test.tbl4\n" + "(\n" + "    k1 date,\n" + "    k2 int,\n" + "    v1 int sum\n" + ")\n"
                + "PARTITION BY RANGE(k1)\n" + "(\n" + "    PARTITION p1 values less than('2020-02-01'),\n"
                + "    PARTITION p2 values less than('2020-03-01'),\n"
                + "    PARTITION p3 values less than('2020-04-01'),\n"
                + "    PARTITION p4 values less than('2020-05-01')\n" + ")\n" + "DISTRIBUTED BY HASH(k2) BUCKETS 3\n"
                + "PROPERTIES" + "(" + "    'replication_num' = '1',\n" + "    'in_memory' = 'false',\n"
                + "    'storage_medium' = 'SSD',\n" + "    'storage_cooldown_time' = '2999-12-31 00:00:00'\n" + ");");

        createTable("CREATE TABLE test.tbl5\n" + "(\n" + "    k1 date,\n" + "    k2 int,\n" + "    v1 int \n"
                + ") ENGINE=OLAP\n" + "UNIQUE KEY (k1,k2)\n" + "PARTITION BY RANGE(k1)\n" + "(\n"
                + "    PARTITION p1 values less than('2020-02-01'),\n"
                + "    PARTITION p2 values less than('2020-03-01')\n" + ")\n" + "DISTRIBUTED BY HASH(k2) BUCKETS 3\n"
                + "PROPERTIES('replication_num' = '1');");

        createTable("CREATE TABLE test.colocate_tbl1\n" + "(\n" + "    k1 date,\n" + "    k2 int,\n" + "    v1 int \n"
                + ") ENGINE=OLAP\n" + "UNIQUE KEY (k1,k2)\n"
                + "DISTRIBUTED BY HASH(k2) BUCKETS 3\n"
                + "PROPERTIES('replication_num' = '1', 'colocate_with' = 'group_1');");

        createTable("CREATE TABLE test.colocate_tbl2\n" + "(\n" + "    k1 date,\n" + "    k2 int,\n" + "    v1 int \n"
                + ") ENGINE=OLAP\n" + "UNIQUE KEY (k1,k2)\n" + "PARTITION BY RANGE(k1)\n" + "(\n"
                + "    PARTITION p1 values less than('2020-02-01'),\n"
                + "    PARTITION p2 values less than('2020-03-01')\n" + ")\n" + "DISTRIBUTED BY HASH(k2) BUCKETS 3\n"
                + "PROPERTIES('replication_num' = '1', 'colocate_with' = 'group_2');");

        createTable("CREATE TABLE test.colocate_tbl3 (\n"
                + "`uuid` varchar(255) NULL,\n"
                + "`action_datetime` date NULL\n"
                + ")\n"
                + "DUPLICATE KEY(uuid)\n"
                + "PARTITION BY RANGE(action_datetime)()\n"
                + "DISTRIBUTED BY HASH(uuid) BUCKETS 3\n"
                + "PROPERTIES\n"
                + "(\n"
                + "\"colocate_with\" = \"group_3\",\n"
                + "\"dynamic_partition.enable\" = \"true\",\n"
                + "\"dynamic_partition.time_unit\" = \"DAY\",\n"
                + "\"dynamic_partition.end\" = \"3\",\n"
                + "\"dynamic_partition.prefix\" = \"p\",\n"
                + "\"dynamic_partition.buckets\" = \"32\",\n"
                + "\"dynamic_partition.replication_num\" = \"1\",\n"
                + "\"dynamic_partition.create_history_partition\"=\"true\",\n"
                + "\"dynamic_partition.start\" = \"-3\"\n"
                + ");\n");

        createTable(
                "CREATE TABLE test.tbl6\n" + "(\n" + "    k1 datetime(3),\n" + "    k2 datetime(3),\n"
                        + "    v1 int \n,"
                        + "    v2 datetime(3)\n" + ") ENGINE=OLAP\n" + "UNIQUE KEY (k1,k2)\n"
                        + "PARTITION BY RANGE(k1)\n" + "(\n"
                        + "    PARTITION p1 values less than('2020-02-01 00:00:00'),\n"
                        + "    PARTITION p2 values less than('2020-03-01 00:00:00')\n" + ")\n"
                        + "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" + "PROPERTIES('replication_num' = '1','enable_unique_key_merge_on_write' = 'false');");

        createTable("create external table test.odbc_table\n" + "(  `k1` bigint(20) COMMENT \"\",\n"
                + "  `k2` datetime COMMENT \"\",\n" + "  `k3` varchar(20) COMMENT \"\",\n"
                + "  `k4` varchar(100) COMMENT \"\",\n" + "  `k5` float COMMENT \"\"\n" + ")ENGINE=ODBC\n"
                + "PROPERTIES (\n" + "\"host\" = \"127.0.0.1\",\n" + "\"port\" = \"3306\",\n" + "\"user\" = \"root\",\n"
                + "\"password\" = \"123\",\n" + "\"database\" = \"db1\",\n" + "\"table\" = \"tbl1\",\n"
                + "\"driver\" = \"Oracle Driver\",\n" + "\"odbc_type\" = \"oracle\"\n" + ");");

        // s3 resource
        createRemoteStorageResource(
                "create resource \"remote_s3\"\n" + "properties\n" + "(\n" + "   \"type\" = \"s3\", \n"
                        + "   \"AWS_ENDPOINT\" = \"bj\",\n" + "   \"AWS_REGION\" = \"bj\",\n"
                        + "   \"AWS_ROOT_PATH\" = \"/path/to/root\",\n" + "   \"AWS_ACCESS_KEY\" = \"bbb\",\n"
                        + "   \"AWS_SECRET_KEY\" = \"aaaa\",\n" + "   \"AWS_MAX_CONNECTIONS\" = \"50\",\n"
                        + "   \"AWS_REQUEST_TIMEOUT_MS\" = \"3000\",\n" + "   \"AWS_CONNECTION_TIMEOUT_MS\" = \"1000\",\n"
                        + "   \"AWS_BUCKET\" = \"test-bucket\",  \"s3_validity_check\" = \"false\"\n"
                        + ");");

        createRemoteStorageResource(
                "create resource \"remote_s3_1\"\n" + "properties\n" + "(\n" + "   \"type\" = \"s3\", \n"
                        + "   \"AWS_ENDPOINT\" = \"bj\",\n" + "   \"AWS_REGION\" = \"bj\",\n"
                        + "   \"AWS_ROOT_PATH\" = \"/path/to/root\",\n" + "   \"AWS_ACCESS_KEY\" = \"bbb\",\n"
                        + "   \"AWS_SECRET_KEY\" = \"aaaa\",\n" + "   \"AWS_MAX_CONNECTIONS\" = \"50\",\n"
                        + "   \"AWS_REQUEST_TIMEOUT_MS\" = \"3000\",\n" + "   \"AWS_CONNECTION_TIMEOUT_MS\" = \"1000\",\n"
                        + "   \"AWS_BUCKET\" = \"test-bucket\", \"s3_validity_check\" = \"false\"\n"
                        + ");");

        createRemoteStoragePolicy(
                "CREATE STORAGE POLICY testPolicy\n" + "PROPERTIES(\n" + "  \"storage_resource\" = \"remote_s3\",\n"
                        + "  \"cooldown_datetime\" = \"2100-05-10 00:00:00\"\n" + ");");

        createRemoteStoragePolicy(
                "CREATE STORAGE POLICY testPolicy2\n" + "PROPERTIES(\n" + "  \"storage_resource\" = \"remote_s3\",\n"
                        + "  \"cooldown_ttl\" = \"1\"\n" + ");");

        createRemoteStoragePolicy(
                "CREATE STORAGE POLICY testPolicyAnotherResource\n" + "PROPERTIES(\n" + "  \"storage_resource\" = \"remote_s3_1\",\n"
                        + "  \"cooldown_ttl\" = \"1\"\n" + ");");

        createTable("CREATE TABLE test.tbl_remote1\n" + "(\n" + "    k1 date,\n" + "    k2 int,\n" + "    v1 int sum\n"
                + ")\n" + "PARTITION BY RANGE(k1)\n" + "(\n" + "    PARTITION p1 values less than('2020-02-01'),\n"
                + "    PARTITION p2 values less than('2020-03-01') ('storage_policy' = 'testPolicy'),\n"
                + "    PARTITION p3 values less than('2020-04-01'),\n"
                + "    PARTITION p4 values less than('2020-05-01')\n" + ")\n" + "DISTRIBUTED BY HASH(k2) BUCKETS 3\n"
                + "PROPERTIES" + "(" + "    'replication_num' = '1',\n" + "    'in_memory' = 'false',\n"
                + "    'storage_medium' = 'SSD',\n" + "    'storage_cooldown_time' = '2100-05-09 00:00:00'\n" + ");");

        createTable("CREATE TABLE test.tbl_remote\n" + "(\n" + "    k1 date,\n" + "    k2 int,\n" + "    v1 int sum\n"
                + ")\n" + "PARTITION BY RANGE(k1)\n" + "(\n" + "    PARTITION p1 values less than('2020-02-01'),\n"
                + "    PARTITION p2 values less than('2020-03-01'),\n"
                + "    PARTITION p3 values less than('2020-04-01'),\n"
                + "    PARTITION p4 values less than('2020-05-01')\n" + ")\n" + "DISTRIBUTED BY HASH(k2) BUCKETS 3\n"
                + "PROPERTIES" + "(" + "    'replication_num' = '1',\n" + "    'in_memory' = 'false',\n"
                + "    'storage_medium' = 'SSD',\n" + "    'storage_cooldown_time' = '2100-05-09 00:00:00',\n"
                + "    'storage_policy' = 'testPolicy'\n" + ");");

        createTable("create table test.show_test (k1 int, k2 int) distributed by hash(k1) "
                + "buckets 1 properties(\"replication_num\" = \"1\");");

        createTable("create table test.unique_sequence_col (k1 int, v1 int, v2 date) ENGINE=OLAP "
                + " UNIQUE KEY(`k1`)  DISTRIBUTED BY HASH(`k1`) BUCKETS 1"
                + " PROPERTIES (\"replication_num\" = \"1\", \"function_column.sequence_col\" = \"v1\");");
    }

    @AfterClass
    public static void tearDown() {
        File file = new File(runningDir);
        file.delete();
    }

    private static void createTable(String sql) throws Exception {
        Config.enable_odbc_table = true;
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        Env.getCurrentEnv().createTable(createTableStmt);
    }

    private static void createRemoteStorageResource(String sql) throws Exception {
        CreateResourceStmt stmt = (CreateResourceStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        Env.getCurrentEnv().getResourceMgr().createResource(stmt);
    }

    private static void createRemoteStoragePolicy(String sql) throws Exception {
        CreatePolicyStmt stmt = (CreatePolicyStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        Env.getCurrentEnv().getPolicyMgr().createPolicy(stmt);
    }

    private static void alterTable(String sql, boolean expectedException) {
        try {
            AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
            Env.getCurrentEnv().alterTable(alterTableStmt);
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

    private static void createMV(String sql, boolean expectedException) {
        try {
            CreateMaterializedViewStmt createMaterializedViewStmt
                    = (CreateMaterializedViewStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
            Env.getCurrentEnv().createMaterializedView(createMaterializedViewStmt);
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

    private static void alterTableWithExceptionMsg(String sql, String msg) throws Exception {
        try {
            AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
            Env.getCurrentEnv().alterTable(alterTableStmt);
        } catch (Exception e) {
            Assert.assertEquals(msg, e.getMessage());
        }
    }

    @Test
    public void alterTableWithEnableFeature() throws Exception {
        String stmt = "alter table test.tbl5 enable feature \"SEQUENCE_LOAD\" with properties (\"function_column.sequence_type\" = \"int\") ";
        alterTable(stmt, false);

        stmt = "alter table test.tbl5 enable feature \"SEQUENCE_LOAD\" with properties (\"function_column.sequence_type\" = \"double\") ";
        alterTable(stmt, true);
    }

    @Test
    public void alterTableModifyRepliaAlloc() throws Exception {
        String[] tables = new String[]{"test.colocate_tbl1", "test.colocate_tbl2"};
        final String errChangeReplicaAlloc = "Cannot change replication allocation of colocate table";
        for (int i = 0; i < tables.length; i++) {
            String sql = "alter table " + tables[i] + " set ('default.replication_allocation' = 'tag.location.default:1')";
            AlterTableStmt alterTableStmt2 = (AlterTableStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
            ExceptionChecker.expectThrowsWithMsg(DdlException.class, errChangeReplicaAlloc,
                    () -> Env.getCurrentEnv().alterTable(alterTableStmt2));

            sql = "alter table " + tables[i] + " modify partition (*) set (\n"
                + "'replication_allocation' = 'tag.location.default:1')";
            AlterTableStmt alterTableStmt3 = (AlterTableStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
            ExceptionChecker.expectThrowsWithMsg(DdlException.class, errChangeReplicaAlloc,
                    () -> Env.getCurrentEnv().alterTable(alterTableStmt3));
        }

        String sql = "alter table test.colocate_tbl1 set ('replication_allocation' = 'tag.location.default:1')";
        AlterTableStmt alterTableStmt1 = (AlterTableStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        ExceptionChecker.expectThrowsWithMsg(DdlException.class, errChangeReplicaAlloc,
                () -> Env.getCurrentEnv().alterTable(alterTableStmt1));

        sql = "alter table test.colocate_tbl3 set (\n"
            + "'dynamic_partition.replication_allocation' = 'tag.location.default:1'\n"
            + " );";
        AlterTableStmt alterTableStmt4 = (AlterTableStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        ExceptionChecker.expectThrowsWithMsg(DdlException.class, errChangeReplicaAlloc,
                () -> Env.getCurrentEnv().alterTable(alterTableStmt4));
    }

    @Test
    public void alterTableModifyComment() throws Exception {
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException("default_cluster:test");
        Table tbl = db.getTableOrMetaException("tbl5");

        // table comment
        String stmt = "alter table test.tbl5 modify comment 'comment1'";
        alterTable(stmt, false);
        Assert.assertEquals("comment1", tbl.getComment());

        // column comment
        stmt = "alter table test.tbl5 modify column k1 comment 'k1'";
        alterTable(stmt, false);
        Assert.assertEquals("k1", tbl.getColumn("k1").getComment());

        // columns comment
        stmt = "alter table test.tbl5 modify column k1 comment 'k11', modify column v1 comment 'v11'";
        alterTable(stmt, false);
        Assert.assertEquals("k11", tbl.getColumn("k1").getComment());
        Assert.assertEquals("v11", tbl.getColumn("v1").getComment());

        // empty comment
        stmt = "alter table test.tbl5 modify comment ''";
        alterTable(stmt, false);
        Assert.assertEquals("OLAP", tbl.getComment());

        // empty column comment
        stmt = "alter table test.tbl5 modify column k1 comment '', modify column v1 comment 'v111'";
        alterTable(stmt, false);
        Assert.assertEquals("", tbl.getColumn("k1").getComment());
        Assert.assertEquals("v111", tbl.getColumn("v1").getComment());

        // unknown column
        stmt = "alter table test.tbl5 modify column x comment '', modify column v1 comment 'v111'";
        alterTable(stmt, true);
        Assert.assertEquals("", tbl.getColumn("k1").getComment());
        Assert.assertEquals("v111", tbl.getColumn("v1").getComment());

        // duplicate column
        stmt = "alter table test.tbl5 modify column k1 comment '', modify column k1 comment 'v111'";
        alterTable(stmt, true);
        Assert.assertEquals("", tbl.getColumn("k1").getComment());
        Assert.assertEquals("v111", tbl.getColumn("v1").getComment());
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
        stmt = "alter table test.tbl1 set (\n"
                + "'dynamic_partition.enable' = 'true',\n"
                + "'dynamic_partition.time_unit' = 'DAY',\n"
                + "'dynamic_partition.end' = '3',\n"
                + "'dynamic_partition.prefix' = 'p',\n"
                + "'dynamic_partition.buckets' = '3'\n"
                + " );";
        alterTable(stmt, false);
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException("default_cluster:test");
        OlapTable tbl = (OlapTable) db.getTableOrMetaException("tbl1");
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
        Assert.assertEquals((short) 1, tbl.getDefaultReplicaAllocation().getTotalReplicaNum());
        stmt = "alter table test.tbl1 set ('default.replication_num' = '3');";
        alterTable(stmt, false);
        Assert.assertEquals((short) 3, tbl.getDefaultReplicaAllocation().getTotalReplicaNum());

        // set range table's real replication num
        Partition p1 = tbl.getPartition("p1");
        Assert.assertEquals(Short.valueOf("1"),
                Short.valueOf(tbl.getPartitionInfo().getReplicaAllocation(p1.getId()).getTotalReplicaNum()));
        stmt = "alter table test.tbl1 set ('replication_num' = '3');";
        alterTable(stmt, true);
        Assert.assertEquals(Short.valueOf("1"),
                Short.valueOf(tbl.getPartitionInfo().getReplicaAllocation(p1.getId()).getTotalReplicaNum()));

        // set un-partitioned table's real replication num
        // first we need to change be's tag
        Map<String, String> originTagMap = be.getTagMap();
        Map<String, String> tagMap = Maps.newHashMap();
        tagMap.put(Tag.TYPE_LOCATION, "group1");
        be.setTagMap(tagMap);
        OlapTable tbl2 = (OlapTable) db.getTableOrMetaException("tbl2");
        Partition partition = tbl2.getPartition(tbl2.getName());
        Assert.assertEquals(Short.valueOf("1"),
                Short.valueOf(tbl2.getPartitionInfo().getReplicaAllocation(partition.getId()).getTotalReplicaNum()));
        stmt = "alter table test.tbl2 set ('replication_allocation' = 'tag.location.group1:1');";
        alterTable(stmt, false);
        Assert.assertEquals((short) 1, (short) tbl2.getPartitionInfo().getReplicaAllocation(partition.getId())
                .getReplicaNumByTag(Tag.createNotCheck(Tag.TYPE_LOCATION, "group1")));
        Assert.assertEquals((short) 1, (short) tbl2.getTableProperty().getReplicaAllocation()
                .getReplicaNumByTag(Tag.createNotCheck(Tag.TYPE_LOCATION, "group1")));
        be.setTagMap(originTagMap);

        Thread.sleep(5000); // sleep to wait dynamic partition scheduler run
        // add partition without set replication num, and default num is 3.
        stmt = "alter table test.tbl1 add partition p4 values less than('2020-04-10')";
        alterTable(stmt, true);

        // add partition when dynamic partition is disable
        stmt = "alter table test.tbl1 add partition p4 values less than('2020-04-10') ('replication_num' = '1')";
        alterTable(stmt, false);
    }

    @Test
    public void testAlterDateV2Operations() throws Exception {
        String stmt = "alter table test.tbl6 add partition p3 values less than('2020-04-01 00:00:00'),"
                + "add partition p4 values less than('2020-05-01 00:00:00')";
        alterTable(stmt, true);

        stmt = "alter table test.tbl6 add partition p3 values less than('2020-04-01 00:00:00'), drop partition p4";
        alterTable(stmt, true);

        stmt = "alter table test.tbl6 drop partition p3, drop partition p4";
        alterTable(stmt, true);

        stmt = "alter table test.tbl6 drop partition p3, add column k3 datetime(6)";
        alterTable(stmt, true);

        // no conflict
        stmt = "alter table test.tbl6 add column k3 int, add column k4 datetime(6)";
        alterTable(stmt, false);
        waitSchemaChangeJobDone(false);

        stmt = "alter table test.tbl6 add rollup r1 (k2, k1)";
        alterTable(stmt, false);
        waitSchemaChangeJobDone(true);

        stmt = "alter table test.tbl6 add rollup r2 (k2, k1), r3 (k2, k1)";
        alterTable(stmt, false);
        waitSchemaChangeJobDone(true);

        // enable dynamic partition
        // not adding the `start` property so that it won't drop the origin partition p1, p2 and p3
        stmt = "alter table test.tbl6 set (\n"
                + "'dynamic_partition.enable' = 'true',\n"
                + "'dynamic_partition.time_unit' = 'DAY',\n"
                + "'dynamic_partition.end' = '3',\n"
                + "'dynamic_partition.prefix' = 'p',\n"
                + "'dynamic_partition.buckets' = '3'\n"
                + " );";
        alterTable(stmt, false);
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException("default_cluster:test");
        OlapTable tbl = (OlapTable) db.getTableOrMetaException("tbl6");
        Assert.assertTrue(tbl.getTableProperty().getDynamicPartitionProperty().getEnable());
        Assert.assertEquals(4, tbl.getIndexIdToSchema().size());

        // add partition when dynamic partition is enable
        stmt = "alter table test.tbl6 add partition p3 values less than('2020-04-01 00:00:00') distributed by"
                + " hash(k2) buckets 4 PROPERTIES ('replication_num' = '1')";
        alterTable(stmt, true);

        // add temp partition when dynamic partition is enable
        stmt = "alter table test.tbl6 add temporary partition tp3 values less than('2020-04-01 00:00:00') distributed"
                + " by hash(k2) buckets 4 PROPERTIES ('replication_num' = '1')";
        alterTable(stmt, false);
        Assert.assertEquals(1, tbl.getTempPartitions().size());

        // disable the dynamic partition
        stmt = "alter table test.tbl6 set ('dynamic_partition.enable' = 'false')";
        alterTable(stmt, false);
        Assert.assertFalse(tbl.getTableProperty().getDynamicPartitionProperty().getEnable());

        String alterStmt = "alter table test.tbl6 set ('in_memory' = 'true')";
        String errorMsg = "errCode = 2, detailMessage = Not support set 'in_memory'='true' now!";
        alterTableWithExceptionMsg(alterStmt, errorMsg);

        // add partition when dynamic partition is disable
        stmt = "alter table test.tbl6 add partition p3 values less than('2020-04-01 00:00:00') distributed"
                + " by hash(k2) buckets 4";
        alterTable(stmt, false);

        // set table's default replication num
        Assert.assertEquals((short) 1, tbl.getDefaultReplicaAllocation().getTotalReplicaNum());
        stmt = "alter table test.tbl6 set ('default.replication_num' = '3');";
        alterTable(stmt, false);
        Assert.assertEquals((short) 3, tbl.getDefaultReplicaAllocation().getTotalReplicaNum());

        // set range table's real replication num
        Partition p1 = tbl.getPartition("p1");
        Assert.assertEquals(Short.valueOf("1"), Short.valueOf(tbl.getPartitionInfo().getReplicaAllocation(p1.getId())
                .getTotalReplicaNum()));
        stmt = "alter table test.tbl6 set ('replication_num' = '3');";
        alterTable(stmt, true);
        Assert.assertEquals(Short.valueOf("1"), Short.valueOf(tbl.getPartitionInfo().getReplicaAllocation(p1.getId())
                .getTotalReplicaNum()));
    }

    // test batch update range partitions' properties
    @Test
    public void testBatchUpdatePartitionProperties() throws Exception {
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException("default_cluster:test");
        OlapTable tbl4 = (OlapTable) db.getTableOrMetaException("tbl4");
        Partition p1 = tbl4.getPartition("p1");
        Partition p2 = tbl4.getPartition("p2");
        Partition p3 = tbl4.getPartition("p3");
        Partition p4 = tbl4.getPartition("p4");

        // batch update replication_num property
        String stmt = "alter table test.tbl4 modify partition (p1, p2, p4) set ('replication_num' = '1')";
        List<Partition> partitionList = Lists.newArrayList(p1, p2, p4);
        for (Partition partition : partitionList) {
            Assert.assertEquals(Short.valueOf("1"), Short.valueOf(tbl4.getPartitionInfo().getReplicaAllocation(partition.getId()).getTotalReplicaNum()));
        }
        alterTable(stmt, false);
        for (Partition partition : partitionList) {
            Assert.assertEquals(Short.valueOf("1"), Short.valueOf(tbl4.getPartitionInfo().getReplicaAllocation(partition.getId()).getTotalReplicaNum()));
        }
        Assert.assertEquals(Short.valueOf("1"), Short.valueOf(tbl4.getPartitionInfo().getReplicaAllocation(p3.getId()).getTotalReplicaNum()));

        // batch update in_memory property
        stmt = "alter table test.tbl4 modify partition (p1, p2, p3) set ('in_memory' = 'false')";
        partitionList = Lists.newArrayList(p1, p2, p3);
        for (Partition partition : partitionList) {
            Assert.assertEquals(false, tbl4.getPartitionInfo().getIsInMemory(partition.getId()));
        }
        alterTable(stmt, false);
        for (Partition partition : partitionList) {
            Assert.assertEquals(false, tbl4.getPartitionInfo().getIsInMemory(partition.getId()));
        }
        Assert.assertEquals(false, tbl4.getPartitionInfo().getIsInMemory(p4.getId()));

        String alterStmt = "alter table test.tbl4 modify partition (p1, p2, p3) set ('in_memory' = 'true')";
        String errorMsg = "errCode = 2, detailMessage = Not support set 'in_memory'='true' now!";
        alterTableWithExceptionMsg(alterStmt, errorMsg);

        // batch update storage_medium and storage_cooldown properties
        // alter storage_medium
        stmt = "alter table test.tbl4 modify partition (p3, p4) set ('storage_medium' = 'HDD')";
        DateLiteral dateLiteral = new DateLiteral("2999-12-31 00:00:00", Type.DATETIME);
        long cooldownTimeMs = dateLiteral.unixTimestamp(TimeUtils.getTimeZone());
        DataProperty oldDataProperty = new DataProperty(TStorageMedium.SSD, cooldownTimeMs, "");
        partitionList = Lists.newArrayList(p3, p4);
        for (Partition partition : partitionList) {
            Assert.assertEquals(oldDataProperty, tbl4.getPartitionInfo().getDataProperty(partition.getId()));
        }
        alterTable(stmt, false);
        DataProperty newDataProperty = new DataProperty(TStorageMedium.HDD, DataProperty.MAX_COOLDOWN_TIME_MS, "");
        for (Partition partition : partitionList) {
            Assert.assertEquals(newDataProperty, tbl4.getPartitionInfo().getDataProperty(partition.getId()));
        }
        Assert.assertEquals(oldDataProperty, tbl4.getPartitionInfo().getDataProperty(p1.getId()));
        Assert.assertEquals(oldDataProperty, tbl4.getPartitionInfo().getDataProperty(p2.getId()));

        // alter cooldown_time
        stmt = "alter table test.tbl4 modify partition (p1, p2) set ('storage_cooldown_time' = '2100-12-31 00:00:00')";
        alterTable(stmt, false);

        dateLiteral = new DateLiteral("2100-12-31 00:00:00", Type.DATETIME);
        cooldownTimeMs = dateLiteral.unixTimestamp(TimeUtils.getTimeZone());
        DataProperty newDataProperty1 = new DataProperty(TStorageMedium.SSD, cooldownTimeMs, "");
        partitionList = Lists.newArrayList(p1, p2);
        for (Partition partition : partitionList) {
            Assert.assertEquals(newDataProperty1, tbl4.getPartitionInfo().getDataProperty(partition.getId()));
        }
        Assert.assertEquals(newDataProperty, tbl4.getPartitionInfo().getDataProperty(p3.getId()));
        Assert.assertEquals(newDataProperty, tbl4.getPartitionInfo().getDataProperty(p4.getId()));

        // batch update range partitions' properties with *
        stmt = "alter table test.tbl4 modify partition (*) set ('replication_num' = '1')";
        partitionList = Lists.newArrayList(p1, p2, p3, p4);
        alterTable(stmt, false);
        for (Partition partition : partitionList) {
            Assert.assertEquals(Short.valueOf("1"), Short.valueOf(tbl4.getPartitionInfo().getReplicaAllocation(partition.getId()).getTotalReplicaNum()));
        }
    }

    @Test
    public void testAlterRemoteStorageTableDataProperties() throws Exception {
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException("default_cluster:test");
        OlapTable tblRemote = (OlapTable) db.getTableOrMetaException("tbl_remote");
        Partition p1 = tblRemote.getPartition("p1");
        Partition p2 = tblRemote.getPartition("p2");
        Partition p3 = tblRemote.getPartition("p3");
        Partition p4 = tblRemote.getPartition("p4");

        DateLiteral dateLiteral = new DateLiteral("2100-05-09 00:00:00", Type.DATETIME);
        long cooldownTimeMs = dateLiteral.unixTimestamp(TimeUtils.getTimeZone());
        DataProperty oldDataProperty = new DataProperty(TStorageMedium.SSD, cooldownTimeMs, "testPolicy");
        List<Partition> partitionList = Lists.newArrayList(p2, p3, p4);
        for (Partition partition : partitionList) {
            Assert.assertEquals(oldDataProperty, tblRemote.getPartitionInfo().getDataProperty(partition.getId()));
        }

        // alter cooldown_time
        String stmt = "alter table test.tbl_remote modify partition (p2, p3, p4) set ('storage_cooldown_time' = '2100-04-01 22:22:22')";
        alterTable(stmt, false);
        DateLiteral newDateLiteral = new DateLiteral("2100-04-01 22:22:22", Type.DATETIME);
        long newCooldownTimeMs = newDateLiteral.unixTimestamp(TimeUtils.getTimeZone());
        DataProperty dataProperty2 = new DataProperty(TStorageMedium.SSD, newCooldownTimeMs, "testPolicy");
        for (Partition partition : partitionList) {
            Assert.assertEquals(dataProperty2, tblRemote.getPartitionInfo().getDataProperty(partition.getId()));
        }
        Assert.assertEquals(oldDataProperty, tblRemote.getPartitionInfo().getDataProperty(p1.getId()));

        // alter storage_medium
        stmt = "alter table test.tbl_remote modify partition (p2, p3, p4) set ('storage_medium' = 'HDD')";
        alterTable(stmt, false);
        DataProperty dataProperty1 = new DataProperty(
                TStorageMedium.HDD, DataProperty.MAX_COOLDOWN_TIME_MS, "testPolicy");
        for (Partition partition : partitionList) {
            Assert.assertEquals(dataProperty1, tblRemote.getPartitionInfo().getDataProperty(partition.getId()));
        }
        Assert.assertEquals(oldDataProperty, tblRemote.getPartitionInfo().getDataProperty(p1.getId()));

        // alter remote_storage to one not exist policy
        stmt = "alter table test.tbl_remote modify partition (p2, p3, p4) set ('storage_policy' = 'testPolicy3')";
        alterTable(stmt, true);
        Assert.assertEquals(oldDataProperty, tblRemote.getPartitionInfo().getDataProperty(p1.getId()));

        // alter remote_storage to one another one which points to another resource
        stmt = "alter table test.tbl_remote modify partition (p2, p3, p4) set ('storage_policy' = 'testPolicyAnotherResource')";
        alterTable(stmt, true);
        Assert.assertEquals(oldDataProperty, tblRemote.getPartitionInfo().getDataProperty(p1.getId()));

        // alter recover to old state
        stmt = "alter table test.tbl_remote modify partition (p2, p3, p4) set ("
                + "'storage_medium' = 'SSD', "
                + "'storage_cooldown_time' = '2100-05-09 00:00:00'"
                + ")";
        alterTable(stmt, false);
        for (Partition partition : partitionList) {
            Assert.assertEquals(oldDataProperty, tblRemote.getPartitionInfo().getDataProperty(partition.getId()));
        }
        Assert.assertEquals(oldDataProperty, tblRemote.getPartitionInfo().getDataProperty(p1.getId()));

    }

    @Test
    public void testAlterRemoteStorageTableDataPropertiesPolicy() throws Exception {
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException("default_cluster:test");
        OlapTable tblRemote = (OlapTable) db.getTableOrMetaException("tbl_remote1");
        Partition p1 = tblRemote.getPartition("p1");
        Partition p2 = tblRemote.getPartition("p2");
        Assert.assertEquals(tblRemote.getPartitionInfo().getStoragePolicy(p2.getId()), "testPolicy");
        Partition p3 = tblRemote.getPartition("p3");
        Partition p4 = tblRemote.getPartition("p4");

        DateLiteral dateLiteral = new DateLiteral("2100-05-09 00:00:00", Type.DATETIME);
        long cooldownTimeMs = dateLiteral.unixTimestamp(TimeUtils.getTimeZone());
        DataProperty oldDataPropertyWithPolicy = new DataProperty(TStorageMedium.SSD, cooldownTimeMs, "testPolicy");
        List<Partition> partitionList = Lists.newArrayList(p1, p3, p4);
        for (Partition partition : partitionList) {
            Assert.assertEquals(tblRemote.getPartitionInfo().getStoragePolicy(partition.getId()), "");
        }
        Assert.assertEquals(oldDataPropertyWithPolicy, tblRemote.getPartitionInfo().getDataProperty(p2.getId()));

        // alter recover to old state
        String stmt = "alter table test.tbl_remote1 modify partition (p1, p3, p4) set ("
                + "'storage_policy' = 'testPolicy')";
        alterTable(stmt, false);
        for (Partition partition : partitionList) {
            Assert.assertEquals(tblRemote.getPartitionInfo().getStoragePolicy(partition.getId()), "testPolicy");
        }

    }

    @Test
    public void testDynamicPartitionDropAndAdd() throws Exception {
        // test day range
        String stmt = "alter table test.tbl3 set (\n"
                + "'dynamic_partition.enable' = 'true',\n"
                + "'dynamic_partition.time_unit' = 'DAY',\n"
                + "'dynamic_partition.start' = '-3',\n"
                + "'dynamic_partition.end' = '3',\n"
                + "'dynamic_partition.prefix' = 'p',\n"
                + "'dynamic_partition.buckets' = '3'\n"
                + " );";
        alterTable(stmt, false);
        Thread.sleep(5000); // sleep to wait dynamic partition scheduler run

        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException("default_cluster:test");
        OlapTable tbl = (OlapTable) db.getTableOrMetaException("tbl3");
        Assert.assertEquals(4, tbl.getPartitionNames().size());
        Assert.assertNull(tbl.getPartition("p1"));
        Assert.assertNull(tbl.getPartition("p2"));
    }

    private void waitSchemaChangeJobDone(boolean rollupJob) throws Exception {
        Map<Long, AlterJobV2> alterJobs = Env.getCurrentEnv().getSchemaChangeHandler().getAlterJobsV2();
        if (rollupJob) {
            alterJobs = Env.getCurrentEnv().getMaterializedViewHandler().getAlterJobsV2();
        }
        for (AlterJobV2 alterJobV2 : alterJobs.values()) {
            while (!alterJobV2.getJobState().isFinalState()) {
                System.out.println("alter job " + alterJobV2.getJobId() + " is running. state: " + alterJobV2.getJobState());
                Thread.sleep(1000);
            }
            System.out.println(alterJobV2.getType() + " alter job " + alterJobV2.getJobId() + " is done. state: " + alterJobV2.getJobState());
            Assert.assertEquals(AlterJobV2.JobState.FINISHED, alterJobV2.getJobState());
            Database db =
                    Env.getCurrentInternalCatalog().getDbOrMetaException(alterJobV2.getDbId());
            OlapTable tbl = (OlapTable) db.getTableOrMetaException(alterJobV2.getTableId());
            while (tbl.getState() != OlapTable.OlapTableState.NORMAL) {
                Thread.sleep(1000);
            }
        }
    }

    @Test
    public void testSetDynamicPropertiesInNormalTable() throws Exception {
        String tableName = "no_dynamic_table";
        String createOlapTblStmt = "CREATE TABLE test.`" + tableName + "` (\n"
                + "  `k1` date NULL COMMENT \"\",\n"
                + "  `k2` int NULL COMMENT \"\",\n"
                + "  `k3` smallint NULL COMMENT \"\",\n"
                + "  `v1` varchar(2048) NULL COMMENT \"\",\n"
                + "  `v2` datetime NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`k1`, `k2`, `k3`)\n"
                + "COMMENT \"OLAP\"\n"
                + "PARTITION BY RANGE (k1)\n"
                + "(\n"
                + "PARTITION p1 VALUES LESS THAN (\"2014-01-01\"),\n"
                + "PARTITION p2 VALUES LESS THAN (\"2014-06-01\"),\n"
                + "PARTITION p3 VALUES LESS THAN (\"2014-12-01\")\n"
                + ")\n"
                + "DISTRIBUTED BY HASH(`k1`) BUCKETS 32\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\"\n"
                + ");";
        createTable(createOlapTblStmt);
        String alterStmt = "alter table test." + tableName + " set (\"dynamic_partition.enable\" = \"true\");";
        String errorMsg = "errCode = 2, detailMessage = Table default_cluster:test.no_dynamic_table is not a dynamic partition table. "
                + "Use command `HELP ALTER TABLE` to see how to change a normal table to a dynamic partition table.";
        alterTableWithExceptionMsg(alterStmt, errorMsg);
        // test set dynamic properties in a no dynamic partition table
        String stmt = "alter table test." + tableName + " set (\n"
                + "'dynamic_partition.enable' = 'true',\n"
                + "'dynamic_partition.time_unit' = 'DAY',\n"
                + "'dynamic_partition.start' = '-3',\n"
                + "'dynamic_partition.end' = '3',\n"
                + "'dynamic_partition.prefix' = 'p',\n"
                + "'dynamic_partition.buckets' = '3'\n"
                + " );";
        alterTable(stmt, false);
    }

    @Test
    public void testSetDynamicPropertiesInDynamicPartitionTable() throws Exception {
        String tableName = "dynamic_table";
        String createOlapTblStmt = "CREATE TABLE test.`" + tableName + "` (\n"
                + "  `k1` date NULL COMMENT \"\",\n"
                + "  `k2` int NULL COMMENT \"\",\n"
                + "  `k3` smallint NULL COMMENT \"\",\n"
                + "  `v1` varchar(2048) NULL COMMENT \"\",\n"
                + "  `v2` datetime NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`k1`, `k2`, `k3`)\n"
                + "COMMENT \"OLAP\"\n"
                + "PARTITION BY RANGE (k1)\n"
                + "(\n"
                + "PARTITION p1 VALUES LESS THAN (\"2014-01-01\"),\n"
                + "PARTITION p2 VALUES LESS THAN (\"2014-06-01\"),\n"
                + "PARTITION p3 VALUES LESS THAN (\"2014-12-01\")\n"
                + ")\n"
                + "DISTRIBUTED BY HASH(`k1`) BUCKETS 32\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\",\n"
                + "\"dynamic_partition.enable\" = \"true\",\n"
                + "\"dynamic_partition.start\" = \"-3\",\n"
                + "\"dynamic_partition.end\" = \"3\",\n"
                + "\"dynamic_partition.time_unit\" = \"day\",\n"
                + "\"dynamic_partition.prefix\" = \"p\",\n"
                + "\"dynamic_partition.buckets\" = \"1\"\n"
                + ");";

        createTable(createOlapTblStmt);
        String alterStmt1 = "alter table test." + tableName + " set (\"dynamic_partition.enable\" = \"false\");";
        alterTable(alterStmt1, false);
        String alterStmt2 = "alter table test." + tableName + " set (\"dynamic_partition.time_unit\" = \"week\");";
        alterTable(alterStmt2, false);
        String alterStmt3 = "alter table test." + tableName + " set (\"dynamic_partition.start\" = \"-10\");";
        alterTable(alterStmt3, false);
        String alterStmt4 = "alter table test." + tableName + " set (\"dynamic_partition.end\" = \"10\");";
        alterTable(alterStmt4, false);
        String alterStmt5 = "alter table test." + tableName + " set (\"dynamic_partition.prefix\" = \"pp\");";
        alterTable(alterStmt5, false);
        String alterStmt6 = "alter table test." + tableName + " set (\"dynamic_partition.buckets\" = \"5\");";
        alterTable(alterStmt6, false);
    }

    @Test
    public void testReplaceTable() throws Exception {
        String stmt1 = "CREATE TABLE test.replace1\n"
                + "(\n"
                + "    k1 int, k2 int, k3 int sum\n"
                + ")\n"
                + "AGGREGATE KEY(k1, k2)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 10\n"
                + "rollup (\n"
                + "r1(k1),\n"
                + "r2(k2, k3)\n"
                + ")\n"
                + "PROPERTIES(\"replication_num\" = \"1\");";


        String stmt2 = "CREATE TABLE test.r1\n"
                + "(\n"
                + "    k1 int, k2 int\n"
                + ")\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 11\n"
                + "PROPERTIES(\"replication_num\" = \"1\");";

        String stmt3 = "CREATE TABLE test.replace2\n"
                + "(\n"
                + "    k1 int, k2 int\n"
                + ")\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 11\n"
                + "PROPERTIES(\"replication_num\" = \"1\");";

        String stmt4 = "CREATE TABLE test.replace3\n"
                + "(\n"
                + "    k1 int, k2 int, k3 int sum\n"
                + ")\n"
                + "PARTITION BY RANGE(k1)\n"
                + "(\n"
                + "\tPARTITION p1 values less than(\"100\"),\n"
                + "\tPARTITION p2 values less than(\"200\")\n"
                + ")\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "rollup (\n"
                + "r3(k1),\n"
                + "r4(k2, k3)\n"
                + ")\n"
                + "PROPERTIES(\"replication_num\" = \"1\");";

        createTable(stmt1);
        createTable(stmt2);
        createTable(stmt3);
        createTable(stmt4);
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException("default_cluster:test");

        // table name -> tabletIds
        Map<String, List<Long>> tblNameToTabletIds = Maps.newHashMap();
        OlapTable replace1Tbl = (OlapTable) db.getTableOrMetaException("replace1");
        OlapTable r1Tbl = (OlapTable) db.getTableOrMetaException("r1");
        OlapTable replace2Tbl = (OlapTable) db.getTableOrMetaException("replace2");
        OlapTable replace3Tbl = (OlapTable) db.getTableOrMetaException("replace3");

        tblNameToTabletIds.put("replace1", Lists.newArrayList());
        for (Partition partition : replace1Tbl.getAllPartitions()) {
            for (MaterializedIndex index : partition.getMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE)) {
                for (Tablet tablet : index.getTablets()) {
                    tblNameToTabletIds.get("replace1").add(tablet.getId());
                }
            }
        }

        tblNameToTabletIds.put("r1", Lists.newArrayList());
        for (Partition partition : r1Tbl.getAllPartitions()) {
            for (MaterializedIndex index : partition.getMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE)) {
                for (Tablet tablet : index.getTablets()) {
                    tblNameToTabletIds.get("r1").add(tablet.getId());
                }
            }
        }

        tblNameToTabletIds.put("replace2", Lists.newArrayList());
        for (Partition partition : replace2Tbl.getAllPartitions()) {
            for (MaterializedIndex index : partition.getMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE)) {
                for (Tablet tablet : index.getTablets()) {
                    tblNameToTabletIds.get("replace2").add(tablet.getId());
                }
            }
        }

        tblNameToTabletIds.put("replace3", Lists.newArrayList());
        for (Partition partition : replace3Tbl.getAllPartitions()) {
            for (MaterializedIndex index : partition.getMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE)) {
                for (Tablet tablet : index.getTablets()) {
                    tblNameToTabletIds.get("replace3").add(tablet.getId());
                }
            }
        }

        // name conflict
        String replaceStmt = "ALTER TABLE test.replace1 REPLACE WITH TABLE r1";
        alterTable(replaceStmt, true);

        // replace1 with replace2
        replaceStmt = "ALTER TABLE test.replace1 REPLACE WITH TABLE replace2";
        OlapTable replace1 = (OlapTable) db.getTableOrMetaException("replace1");
        OlapTable replace2 = (OlapTable) db.getTableOrMetaException("replace2");
        Assert.assertEquals(3, replace1.getPartition("replace1").getMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE).size());
        Assert.assertEquals(1, replace2.getPartition("replace2").getMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE).size());

        alterTable(replaceStmt, false);
        Assert.assertTrue(checkAllTabletsExists(tblNameToTabletIds.get("replace1")));
        Assert.assertTrue(checkAllTabletsExists(tblNameToTabletIds.get("replace2")));

        replace1 = (OlapTable) db.getTableOrMetaException("replace1");
        replace2 = (OlapTable) db.getTableOrMetaException("replace2");
        Assert.assertEquals(1, replace1.getPartition("replace1").getMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE).size());
        Assert.assertEquals(3, replace2.getPartition("replace2").getMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE).size());
        Assert.assertEquals("replace1", replace1.getIndexNameById(replace1.getBaseIndexId()));
        Assert.assertEquals("replace2", replace2.getIndexNameById(replace2.getBaseIndexId()));

        // replace with no swap
        replaceStmt = "ALTER TABLE test.replace1 REPLACE WITH TABLE replace2 properties('swap' = 'false')";
        alterTable(replaceStmt, false);
        replace1 = (OlapTable) db.getTableNullable("replace1");
        replace2 = (OlapTable) db.getTableNullable("replace2");
        Assert.assertNull(replace2);
        Assert.assertEquals(3, replace1.getPartition("replace1").getMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE).size());
        Assert.assertEquals("replace1", replace1.getIndexNameById(replace1.getBaseIndexId()));
        Assert.assertTrue(checkAllTabletsNotExists(tblNameToTabletIds.get("replace2")));
        Assert.assertTrue(checkAllTabletsExists(tblNameToTabletIds.get("replace1")));

        replaceStmt = "ALTER TABLE test.replace1 REPLACE WITH TABLE replace3 properties('swap' = 'true')";
        alterTable(replaceStmt, false);
        replace1 = (OlapTable) db.getTableOrMetaException("replace1");
        OlapTable replace3 = (OlapTable) db.getTableOrMetaException("replace3");
        Assert.assertEquals(3, replace1.getPartition("p1").getMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE).size());
        Assert.assertEquals(3, replace1.getPartition("p2").getMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE).size());
        Assert.assertNotNull(replace1.getIndexIdByName("r3"));
        Assert.assertNotNull(replace1.getIndexIdByName("r4"));

        Assert.assertTrue(checkAllTabletsExists(tblNameToTabletIds.get("replace1")));
        Assert.assertTrue(checkAllTabletsExists(tblNameToTabletIds.get("replace3")));

        Assert.assertEquals(3, replace3.getPartition("replace3").getMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE).size());
        Assert.assertNotNull(replace3.getIndexIdByName("r1"));
        Assert.assertNotNull(replace3.getIndexIdByName("r2"));
    }

    @Test
    public void testModifyBucketNum() throws Exception {
        String stmt = "CREATE TABLE test.bucket\n"
                + "(\n"
                + "    k1 int, k2 int, k3 int sum\n"
                + ")\n"
                + "ENGINE = OLAP\n"
                + "PARTITION BY RANGE(k1)\n"
                + "(\n"
                + "PARTITION p1 VALUES LESS THAN (\"100000\"),\n"
                + "PARTITION p2 VALUES LESS THAN (\"200000\"),\n"
                + "PARTITION p3 VALUES LESS THAN (\"300000\")\n"
                + ")\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 10\n"
                + "PROPERTIES(\"replication_num\" = \"1\");";

        createTable(stmt);
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException("default_cluster:test");

        String modifyBucketNumStmt = "ALTER TABLE test.bucket MODIFY DISTRIBUTION DISTRIBUTED BY HASH(k1) BUCKETS 1;";
        alterTable(modifyBucketNumStmt, false);
        OlapTable bucket = (OlapTable) db.getTableOrMetaException("bucket");
        Assert.assertEquals(1, bucket.getDefaultDistributionInfo().getBucketNum());

        modifyBucketNumStmt = "ALTER TABLE test.bucket MODIFY DISTRIBUTION DISTRIBUTED BY HASH(k1) BUCKETS 30;";
        alterTable(modifyBucketNumStmt, false);
        bucket = (OlapTable) db.getTableOrMetaException("bucket");
        Assert.assertEquals(30, bucket.getDefaultDistributionInfo().getBucketNum());

    }

    @Test
    public void testChangeOrder() throws Exception {
        createTable("CREATE TABLE test.change_order\n"
                + "(\n"
                + "    k1 date,\n"
                + "    k2 int,\n"
                + "    v1 int sum\n"
                + ")\n"
                + "PARTITION BY RANGE(k1)\n"
                + "(\n"
                + "    PARTITION p1 values less than('2020-02-01'),\n"
                + "    PARTITION p2 values less than('2020-03-01')\n"
                + ")\n"
                + "DISTRIBUTED BY HASH(k2) BUCKETS 3\n"
                + "PROPERTIES('replication_num' = '1');");

        String changeOrderStmt = "ALTER TABLE test.change_order ORDER BY (k2, k1, v1);;";
        alterTable(changeOrderStmt, false);
    }

    @Test
    public void testAlterUniqueTablePartitionColumn() throws Exception {
        createTable("CREATE TABLE test.unique_partition\n"
                + "(\n"
                + "    k1 date,\n"
                + "    k2 int,\n"
                + "    v1 int\n"
                + ")\n"
                + "UNIQUE KEY(k1, k2)\n"
                + "PARTITION BY RANGE(k1)\n"
                + "(\n"
                + "    PARTITION p1 values less than('2020-02-01'),\n"
                + "    PARTITION p2 values less than('2020-03-01')\n" + ")\n" + "DISTRIBUTED BY HASH(k2) BUCKETS 3\n"
                + "PROPERTIES('replication_num' = '1');");

        // partition key can not be changed.
        // this test is also for validating a bug fix about invisible columns(delete flag column)
        String changeOrderStmt = "ALTER TABLE test.unique_partition modify column k1 int key null";
        alterTable(changeOrderStmt, true);
    }

    @Test
    public void testAlterDateV2Schema() throws Exception {
        createTable("CREATE TABLE test.unique_partition_datev2\n" + "(\n" + "    k1 date,\n" + "    k2 datetime(3),\n"
                + "    k3 datetime,\n" + "    v1 date,\n" + "    v2 datetime(3),\n" + "    v3 datetime,\n" + "    v4 int\n"
                + ")\n" + "UNIQUE KEY(k1, k2, k3)\n" + "PARTITION BY RANGE(k1)\n" + "(\n" + "    PARTITION p1 values less than('2020-02-01'),\n"
                + "    PARTITION p2 values less than('2020-03-01')\n" + ")\n" + "DISTRIBUTED BY HASH(k1) BUCKETS 3\n" + "PROPERTIES('replication_num' = '1');");

        // partition key can not be changed.
        String changeOrderStmt = "ALTER TABLE test.unique_partition_datev2 modify column k1 int key null";
        alterTable(changeOrderStmt, true);
        changeOrderStmt = "ALTER TABLE test.unique_partition_datev2 modify column k2 int key null";
        alterTable(changeOrderStmt, true);
        changeOrderStmt = "ALTER TABLE test.unique_partition_datev2 modify column k3 int key null";
        alterTable(changeOrderStmt, true);

        // partition keys which are date type should be changed between each other.
        changeOrderStmt = "ALTER TABLE test.unique_partition_datev2 modify column k2 datetime key null";
        alterTable(changeOrderStmt, false);
        waitSchemaChangeJobDone(false);
        changeOrderStmt = "ALTER TABLE test.unique_partition_datev2 modify column k3 datetime(3) key null";
        alterTable(changeOrderStmt, false);
        waitSchemaChangeJobDone(false);
        // Change to another precision datetime
        changeOrderStmt = "ALTER TABLE test.unique_partition_datev2 modify column k3 datetime(6) key null";
        alterTable(changeOrderStmt, false);
        waitSchemaChangeJobDone(false);
    }

    private boolean checkAllTabletsExists(List<Long> tabletIds) {
        TabletInvertedIndex invertedIndex = Env.getCurrentEnv().getTabletInvertedIndex();
        for (long tabletId : tabletIds) {
            if (invertedIndex.getTabletMeta(tabletId) == null) {
                return false;
            }
            if (invertedIndex.getReplicasByTabletId(tabletId).isEmpty()) {
                return false;
            }
        }
        return true;
    }

    private boolean checkAllTabletsNotExists(List<Long> tabletIds) {
        TabletInvertedIndex invertedIndex = Env.getCurrentEnv().getTabletInvertedIndex();
        for (long tabletId : tabletIds) {
            if (invertedIndex.getTabletMeta(tabletId) != null) {
                return false;
            }

            if (!invertedIndex.getReplicasByTabletId(tabletId).isEmpty()) {
                return false;
            }
        }
        return true;
    }

    @Test
    public void testExternalTableAlterOperations() throws Exception {
        // external table do not support partition operation
        String stmt = "alter table test.odbc_table add partition p3 values less than('2020-04-01'), add partition p4 values less than('2020-05-01')";
        alterTable(stmt, true);

        // external table do not support rollup
        stmt = "alter table test.odbc_table add rollup r1 (k1)";
        alterTable(stmt, true);

        // external table support add column
        stmt = "alter table test.odbc_table add column k6 INT KEY after k1, add column k7 TINYINT KEY after k6";
        alterTable(stmt, false);
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException("default_cluster:test");
        Table odbcTable = db.getTableOrMetaException("odbc_table");
        Assert.assertEquals(odbcTable.getBaseSchema().size(), 7);
        Assert.assertEquals(odbcTable.getBaseSchema().get(1).getDataType(), PrimitiveType.INT);
        Assert.assertEquals(odbcTable.getBaseSchema().get(2).getDataType(), PrimitiveType.TINYINT);

        // external table support drop column
        stmt = "alter table test.odbc_table drop column k7";
        alterTable(stmt, false);
        db = Env.getCurrentInternalCatalog().getDbOrMetaException("default_cluster:test");
        odbcTable = db.getTableOrMetaException("odbc_table");
        Assert.assertEquals(odbcTable.getBaseSchema().size(), 6);

        // external table support modify column
        stmt = "alter table test.odbc_table modify column k6 bigint after k5";
        alterTable(stmt, false);
        db = Env.getCurrentInternalCatalog().getDbOrMetaException("default_cluster:test");
        odbcTable = db.getTableOrMetaException("odbc_table");
        Assert.assertEquals(odbcTable.getBaseSchema().size(), 6);
        Assert.assertEquals(odbcTable.getBaseSchema().get(5).getDataType(), PrimitiveType.BIGINT);

        // external table support reorder column
        db = Env.getCurrentInternalCatalog().getDbOrMetaException("default_cluster:test");
        odbcTable = db.getTableOrMetaException("odbc_table");
        Assert.assertEquals(odbcTable.getBaseSchema().stream()
                .map(column -> column.getName())
                .reduce("", (totalName, columnName) -> totalName + columnName), "k1k2k3k4k5k6");
        stmt = "alter table test.odbc_table order by (k6, k5, k4, k3, k2, k1)";
        alterTable(stmt, false);
        Assert.assertEquals(odbcTable.getBaseSchema().stream()
                .map(column -> column.getName())
                .reduce("", (totalName, columnName) -> totalName + columnName), "k6k5k4k3k2k1");

        // external table support drop column
        stmt = "alter table test.odbc_table drop column k6";
        alterTable(stmt, false);
        stmt = "alter table test.odbc_table drop column k5";
        alterTable(stmt, false);
        stmt = "alter table test.odbc_table drop column k4";
        alterTable(stmt, false);
        stmt = "alter table test.odbc_table drop column k3";
        alterTable(stmt, false);
        stmt = "alter table test.odbc_table drop column k2";
        alterTable(stmt, false);
        // do not allow drop last column
        Assert.assertEquals(odbcTable.getBaseSchema().size(), 1);
        stmt = "alter table test.odbc_table drop column k1";
        alterTable(stmt, true);
        Assert.assertEquals(odbcTable.getBaseSchema().size(), 1);

        // external table support rename operation
        stmt = "alter table test.odbc_table rename oracle_table";
        alterTable(stmt, false);
        db = Env.getCurrentInternalCatalog().getDbOrMetaException("default_cluster:test");
        odbcTable = db.getTableNullable("oracle_table");
        Assert.assertNotNull(odbcTable);
        odbcTable = db.getTableNullable("odbc_table");
        Assert.assertNull(odbcTable);
    }

    @Test
    public void testModifyTableEngine() throws Exception {
        String createOlapTblStmt = "CREATE TABLE test.mysql_table (\n"
                + "  `k1` date NULL COMMENT \"\",\n"
                + "  `k2` int NULL COMMENT \"\",\n"
                + "  `k3` smallint NULL COMMENT \"\",\n"
                + "  `v1` varchar(2048) NULL COMMENT \"\",\n"
                + "  `v2` datetime NULL COMMENT \"\"\n"
                + ") ENGINE=MYSQL\n"
                + "PROPERTIES (\n"
                + "\"host\" = \"172.16.0.1\",\n"
                + "\"port\" = \"3306\",\n"
                + "\"user\" = \"cmy\",\n"
                + "\"password\" = \"abc\",\n"
                + "\"database\" = \"db1\",\n"
                + "\"table\" = \"tbl1\""
                + ");";
        createTable(createOlapTblStmt);

        Database db = Env.getCurrentInternalCatalog().getDbNullable("default_cluster:test");
        MysqlTable mysqlTable = (MysqlTable) db.getTableOrMetaException("mysql_table", Table.TableType.MYSQL);

        String alterEngineStmt = "alter table test.mysql_table modify engine to odbc";
        alterTable(alterEngineStmt, true);

        alterEngineStmt = "alter table test.mysql_table modify engine to odbc properties(\"driver\" = \"MySQL\")";
        alterTable(alterEngineStmt, false);

        OdbcTable odbcTable = (OdbcTable) db.getTableNullable(mysqlTable.getId());
        Assert.assertEquals("mysql_table", odbcTable.getName());
        List<Column> schema = odbcTable.getBaseSchema();
        Assert.assertEquals(5, schema.size());
        Assert.assertEquals("172.16.0.1", odbcTable.getHost());
        Assert.assertEquals("3306", odbcTable.getPort());
        Assert.assertEquals("cmy", odbcTable.getUserName());
        Assert.assertEquals("abc", odbcTable.getPasswd());
        Assert.assertEquals("db1", odbcTable.getOdbcDatabaseName());
        Assert.assertEquals("tbl1", odbcTable.getOdbcTableName());
        Assert.assertEquals("MySQL", odbcTable.getOdbcDriver());
    }

    @Test(expected = DdlException.class)
    public void testDropInUseResource() throws Exception {
        String sql = "drop resource remote_s3";
        DropResourceStmt stmt = (DropResourceStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        Env.getCurrentEnv().getResourceMgr().dropResource(stmt);
    }

    @Test
    public void testShowMV() throws Exception {
        createMV("CREATE MATERIALIZED VIEW test_mv as select k1 from test.show_test group by k1;", false);
        waitSchemaChangeJobDone(true);

        String showMvSql = "SHOW CREATE MATERIALIZED VIEW test_mv on test.show_test;";
        ShowCreateMaterializedViewStmt showStmt = (ShowCreateMaterializedViewStmt) UtFrameUtils.parseAndAnalyzeStmt(
                showMvSql, connectContext);
        ShowExecutor executor = new ShowExecutor(connectContext, showStmt);
        Assert.assertEquals(executor.execute().getResultRows().get(0).get(2),
                "CREATE MATERIALIZED VIEW test_mv as select k1 from test.show_test group by k1;");

        showMvSql = "SHOW CREATE MATERIALIZED VIEW test_mv_empty on test.show_test;";
        showStmt = (ShowCreateMaterializedViewStmt) UtFrameUtils.parseAndAnalyzeStmt(showMvSql, connectContext);
        executor = new ShowExecutor(connectContext, showStmt);
        Assert.assertTrue(executor.execute().getResultRows().isEmpty());

        showMvSql = "SHOW CREATE MATERIALIZED VIEW test_mv on test.table1_error;";
        showStmt = (ShowCreateMaterializedViewStmt) UtFrameUtils.parseAndAnalyzeStmt(showMvSql, connectContext);
        executor = new ShowExecutor(connectContext, showStmt);
        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class, "Unknown table 'table1_error'",
                executor::execute);
    }

    @Test
    public void testModifySequenceCol() {
        String stmt = "alter table test.unique_sequence_col modify column v1 Date";
        alterTable(stmt, true);
    }
}
