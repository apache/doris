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

import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.clone.DynamicPartitionScheduler;
import org.apache.doris.clone.RebalancerTestUtil;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DynamicPartitionUtil;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.AlterTableCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateDatabaseCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateTableCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TStorageMedium;
import org.apache.doris.utframe.UtFrameUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.lang.reflect.Method;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.UUID;

public class DynamicPartitionTableTest {
    private static String runningDir = "fe/mocked/DynamicPartitionTableTest/" + UUID.randomUUID().toString() + "/";

    private static ConnectContext connectContext;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @BeforeClass
    public static void beforeClass() throws Exception {
        FeConstants.default_scheduler_interval_millisecond = 1000;
        FeConstants.runningUnitTest = true;
        Config.disable_storage_medium_check = true;
        Config.dynamic_partition_enable = false; // disable auto create dynamic partition

        UtFrameUtils.createDorisCluster(runningDir);

        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        // create database
        String createDbStmtStr = "create database test;";
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan logicalPlan = nereidsParser.parseSingle(createDbStmtStr);
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, createDbStmtStr);
        if (logicalPlan instanceof CreateDatabaseCommand) {
            ((CreateDatabaseCommand) logicalPlan).run(connectContext, stmtExecutor);
        }
    }

    @AfterClass
    public static void tearDown() {
        UtFrameUtils.cleanDorisFeDir(runningDir);
    }

    private static void changeBeDisk(TStorageMedium storageMedium) throws UserException {
        List<Backend> backends = Env.getCurrentSystemInfo().getAllBackendsByAllCluster().values().asList();
        for (Backend be : backends) {
            for (DiskInfo diskInfo : be.getDisks().values()) {
                diskInfo.setStorageMedium(storageMedium);
            }
        }
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
    public void testNormal() throws Exception {
        String createOlapTblStmt = "CREATE TABLE test.`dynamic_partition_normal` (\n"
                + "  `k1` date NULL COMMENT \"\",\n"
                + "  `k2` int NULL COMMENT \"\",\n"
                + "  `k3` smallint NULL COMMENT \"\",\n"
                + "  `v1` varchar(2048) NULL COMMENT \"\",\n" + "  `v2` datetime NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n" + "DUPLICATE KEY(`k1`, `k2`, `k3`)\n" + "COMMENT \"OLAP\"\n"
                + "PARTITION BY RANGE (k1)\n" + "(\n" + "PARTITION p1 VALUES LESS THAN (\"2014-01-01\"),\n"
                + "PARTITION p2 VALUES LESS THAN (\"2014-06-01\"),\n"
                + "PARTITION p3 VALUES LESS THAN (\"2014-12-01\")\n" + ")\n" + "DISTRIBUTED BY HASH(`k1`) BUCKETS 32\n"
                + "PROPERTIES (\n" + "\"replication_num\" = \"1\",\n" + "\"dynamic_partition.enable\" = \"true\",\n"
                + "\"dynamic_partition.start\" = \"-3\",\n" + "\"dynamic_partition.end\" = \"3\",\n"
                + "\"dynamic_partition.time_unit\" = \"day\",\n" + "\"dynamic_partition.prefix\" = \"p\",\n"
                + "\"dynamic_partition.buckets\" = \"1\"\n" + ");";
        createTable(createOlapTblStmt);
        Database db =
                Env.getCurrentInternalCatalog().getDbOrAnalysisException("test");
        OlapTable table = (OlapTable) db.getTableOrAnalysisException("dynamic_partition_normal");
        Assert.assertTrue(table.getTableProperty().getDynamicPartitionProperty().getReplicaAllocation().isNotSet());

        // test only set dynamic_partition.replication_num
        createOlapTblStmt = "CREATE TABLE test.`dynamic_partition_normal2` (\n"
                + "`uuid` varchar(255) NULL,\n"
                + "`action_datetime` date NULL\n"
                + ")\n"
                + "DUPLICATE KEY(uuid)\n"
                + "PARTITION BY RANGE(action_datetime)()\n"
                + "DISTRIBUTED BY HASH(uuid) BUCKETS 3\n"
                + "PROPERTIES\n"
                + "(\n"
                + "\"dynamic_partition.enable\" = \"true\",\n"
                + "\"dynamic_partition.time_unit\" = \"DAY\",\n"
                + "\"dynamic_partition.end\" = \"3\",\n"
                + "\"dynamic_partition.prefix\" = \"p\",\n"
                + "\"dynamic_partition.buckets\" = \"32\",\n"
                + "\"dynamic_partition.replication_num\" = \"1\",\n"
                + "\"dynamic_partition.create_history_partition\"=\"true\",\n"
                + "\"dynamic_partition.start\" = \"-3\"\n"
                + ");\n"
                + "\n";
        createTable(createOlapTblStmt);
    }

    @Test
    public void testMissPrefix() throws Exception {
        String createOlapTblStmt = "CREATE TABLE test.`dynamic_partition_prefix` (\n"
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
                + "\"dynamic_partition.buckets\" = \"1\"\n"
                + ");";
        expectedException.expect(DdlException.class);
        expectedException.expectMessage("errCode = 2, detailMessage = Must assign dynamic_partition.prefix properties");
        createTable(createOlapTblStmt);
    }

    @Test
    public void testMissTimeUnit() throws Exception {
        String createOlapTblStmt = "CREATE TABLE test.`dynamic_partition_time_unit` (\n"
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
                + "\"dynamic_partition.prefix\" = \"p\",\n"
                + "\"dynamic_partition.buckets\" = \"1\"\n"
                + ");";
        expectedException.expect(DdlException.class);
        expectedException.expectMessage("errCode = 2, detailMessage = Must assign dynamic_partition.time_unit properties");
        createTable(createOlapTblStmt);
    }

    @Test
    public void testMissStart() throws Exception {
        String createOlapTblStmt = "CREATE TABLE test.`dynamic_partition_start` (\n"
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
                + "\"dynamic_partition.end\" = \"3\",\n"
                + "\"dynamic_partition.time_unit\" = \"day\",\n"
                + "\"dynamic_partition.prefix\" = \"p\",\n"
                + "\"dynamic_partition.buckets\" = \"1\"\n"
                + ");";
        createTable(createOlapTblStmt);
    }

    @Test
    public void testMissEnd() throws Exception {
        String createOlapTblStmt = "CREATE TABLE test.`dynamic_partition_end` (\n"
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
                + "\"dynamic_partition.time_unit\" = \"day\",\n"
                + "\"dynamic_partition.prefix\" = \"p\",\n"
                + "\"dynamic_partition.buckets\" = \"1\"\n"
                + ");";
        expectedException.expect(DdlException.class);
        expectedException.expectMessage("errCode = 2, detailMessage = Must assign dynamic_partition.end properties");
        createTable(createOlapTblStmt);
    }

    @Test
    public void testMissBuckets() throws Exception {
        String createOlapTblStmt = "CREATE TABLE test.`dynamic_partition_miss_buckets` (\n"
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
                + "\"dynamic_partition.prefix\" = \"p\"\n"
                + ");";
        createTable(createOlapTblStmt);
    }

    @Test
    public void testNotAllowed() throws Exception {
        String createOlapTblStmt = "CREATE TABLE test.`dynamic_partition_not_allowed` (\n"
                + "  `k1` date NULL COMMENT \"\",\n"
                + "  `k2` int NULL COMMENT \"\",\n"
                + "  `k3` smallint NULL COMMENT \"\",\n"
                + "  `v1` varchar(2048) NULL COMMENT \"\",\n"
                + "  `v2` datetime NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`k1`, `k2`, `k3`)\n"
                + "COMMENT \"OLAP\"\n"
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
        expectedException.expect(DdlException.class);
        expectedException.expectMessage("errCode = 2, detailMessage = Only support dynamic partition properties on range partition table");
        createTable(createOlapTblStmt);
    }

    @Test
    public void testNotAllowedInMultiPartitions() throws Exception {
        String createOlapTblStmt = "CREATE TABLE test.`dynamic_partition_normal` (\n"
                + "  `k1` date NULL COMMENT \"\",\n"
                + "  `k2` int NULL COMMENT \"\",\n"
                + "  `k3` smallint NULL COMMENT \"\",\n"
                + "  `v1` varchar(2048) NULL COMMENT \"\",\n"
                + "  `v2` datetime NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`k1`, `k2`, `k3`)\n"
                + "COMMENT \"OLAP\"\n"
                + "PARTITION BY RANGE (k1, k2)\n"
                + "(\n"
                + "PARTITION p1 VALUES LESS THAN (\"2014-01-01\", \"100\"),\n"
                + "PARTITION p2 VALUES LESS THAN (\"2014-06-01\", \"200\"),\n"
                + "PARTITION p3 VALUES LESS THAN (\"2014-12-01\", \"300\")\n"
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
        expectedException.expect(DdlException.class);
        expectedException.expectMessage("errCode = 2, detailMessage = Dynamic partition only support single-column range partition");
        createTable(createOlapTblStmt);
    }

    @Test
    public void testMissTimeZone() throws Exception {
        String createOlapTblStmt = "CREATE TABLE test.`dynamic_partition_miss_time_zone` (\n"
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
                + "\"dynamic_partition.buckets\" = \"3\",\n"
                + "\"dynamic_partition.time_unit\" = \"day\",\n"
                + "\"dynamic_partition.prefix\" = \"p\"\n"
                + ");";
        createTable(createOlapTblStmt);
    }

    @Test
    public void testNormalTimeZone() throws Exception {
        String createOlapTblStmt = "CREATE TABLE test.`dynamic_partition_time_zone` (\n"
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
                + "\"dynamic_partition.buckets\" = \"3\",\n"
                + "\"dynamic_partition.time_unit\" = \"day\",\n"
                + "\"dynamic_partition.time_zone\" = \"Asia/Shanghai\",\n"
                + "\"dynamic_partition.prefix\" = \"p\"\n"
                + ");";
        createTable(createOlapTblStmt);
    }

    @Test
    public void testInvalidTimeZone() throws Exception {
        String createOlapTblStmt = "CREATE TABLE test.`dynamic_partition_invalid_time_zone` (\n"
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
                + "\"dynamic_partition.buckets\" = \"3\",\n"
                + "\"dynamic_partition.time_unit\" = \"day\",\n"
                + "\"dynamic_partition.time_zone\" = \"invalid\",\n"
                + "\"dynamic_partition.prefix\" = \"p\"\n"
                + ");";
        expectedException.expect(DdlException.class);
        expectedException.expectMessage("errCode = 2, detailMessage = Unknown or incorrect time zone: 'invalid'");
        createTable(createOlapTblStmt);
    }

    @Test
    public void testSetDynamicPartitionReplicationNum() throws Exception {
        String tableName = "dynamic_partition_replication_num";
        String createOlapTblStmt = "CREATE TABLE test.`" + tableName + "` (\n"
                + "  `k1` date NULL COMMENT \"\",\n"
                + "  `k2` int NULL COMMENT \"\",\n"
                + "  `k3` smallint NULL COMMENT \"\",\n"
                + "  `v1` varchar(2048) NULL COMMENT \"\",\n"
                + "  `v2` datetime NULL COMMENT \"\"\n" + ") ENGINE=OLAP\n" + "DUPLICATE KEY(`k1`, `k2`, `k3`)\n"
                + "COMMENT \"OLAP\"\n" + "PARTITION BY RANGE (k1)\n" + "(\n"
                + "PARTITION p1 VALUES LESS THAN (\"2014-01-01\"),\n"
                + "PARTITION p2 VALUES LESS THAN (\"2014-06-01\"),\n"
                + "PARTITION p3 VALUES LESS THAN (\"2014-12-01\")\n" + ")\n" + "DISTRIBUTED BY HASH(`k1`) BUCKETS 32\n"
                + "PROPERTIES (\n" + "\"replication_num\" = \"1\",\n" + "\"dynamic_partition.enable\" = \"true\",\n"
                + "\"dynamic_partition.start\" = \"-3\",\n" + "\"dynamic_partition.end\" = \"3\",\n"
                + "\"dynamic_partition.time_unit\" = \"day\",\n" + "\"dynamic_partition.prefix\" = \"p\",\n"
                + "\"dynamic_partition.buckets\" = \"1\",\n" + "\"dynamic_partition.replication_num\" = \"1\"\n" + ");";
        createTable(createOlapTblStmt);
        Database db =
                Env.getCurrentInternalCatalog().getDbOrAnalysisException("test");
        OlapTable table = (OlapTable) db.getTableOrAnalysisException(tableName);
        Assert.assertEquals(1,
                table.getTableProperty().getDynamicPartitionProperty().getReplicaAllocation().getTotalReplicaNum());

        String alter1 =
                "alter table test.dynamic_partition_replication_num set ('dynamic_partition.replication_num' = '0')";
        ExceptionChecker.expectThrows(AnalysisException.class, () -> alterTable(alter1));
        Assert.assertEquals(1,
                table.getTableProperty().getDynamicPartitionProperty().getReplicaAllocation().getTotalReplicaNum());
    }

    @Test
    public void testCreateDynamicPartitionImmediately() throws Exception {
        String createOlapTblStmt = "CREATE TABLE test.`empty_dynamic_partition` (\n"
                + "  `k1` date NULL COMMENT \"\",\n"
                + "  `k2` int NULL COMMENT \"\",\n"
                + "  `k3` smallint NULL COMMENT \"\",\n"
                + "  `v1` varchar(2048) NULL COMMENT \"\",\n"
                + "  `v2` datetime NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`k1`, `k2`, `k3`)\n"
                + "COMMENT \"OLAP\"\n"
                + "PARTITION BY RANGE(`k1`)\n"
                + "()\n"
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
        OlapTable emptyDynamicTable = (OlapTable) Env.getCurrentInternalCatalog()
                .getDbOrAnalysisException("test")
                .getTableOrAnalysisException("empty_dynamic_partition");
        Assert.assertTrue(emptyDynamicTable.getAllPartitions().size() == 4);

        Iterator<Partition> partitionIterator = emptyDynamicTable.getAllPartitions().iterator();
        List<String> partNames = Lists.newArrayList();
        while (partitionIterator.hasNext()) {
            String partitionName = partitionIterator.next().getName();
            partNames.add(partitionName.substring(1));
        }
        Collections.sort(partNames);

        int partitionCount = 0;
        DateTimeFormatter sdf = DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZoneId.systemDefault());
        for (String partName : partNames) {
            Date partitionDate = Date.from(
                    LocalDate.parse(partName, sdf).atStartOfDay().atZone(ZoneId.systemDefault()).toInstant());
            Date date = new Date();
            Calendar calendar = new GregorianCalendar();
            calendar.setTime(date);
            calendar.add(calendar.DATE, partitionCount);
            date = calendar.getTime();

            Assert.assertEquals(partitionDate.getYear(), date.getYear());
            Assert.assertEquals(partitionDate.getMonth(), date.getMonth());
            Assert.assertEquals(partitionDate.getDay(), date.getDay());

            partitionCount++;
        }
    }

    @Test
    public void testFillHistoryDynamicPartition() throws Exception {
        String createOlapTblStmt = "CREATE TABLE test.`histo_dynamic_partition` (\n"
                + "  `k1` date NULL COMMENT \"\",\n"
                + "  `k2` int NULL COMMENT \"\",\n"
                + "  `k3` smallint NULL COMMENT \"\",\n"
                + "  `v1` varchar(2048) NULL COMMENT \"\",\n"
                + "  `v2` datetime NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`k1`, `k2`, `k3`)\n"
                + "COMMENT \"OLAP\"\n"
                + "PARTITION BY RANGE(`k1`)\n"
                + "()\n"
                + "DISTRIBUTED BY HASH(`k1`) BUCKETS 32\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\",\n"
                + "\"dynamic_partition.enable\" = \"true\",\n"
                + "\"dynamic_partition.start\" = \"-3\",\n"
                + "\"dynamic_partition.end\" = \"3\",\n"
                + "\"dynamic_partition.create_history_partition\" = \"true\",\n"
                + "\"dynamic_partition.time_unit\" = \"day\",\n"
                + "\"dynamic_partition.prefix\" = \"p\",\n"
                + "\"dynamic_partition.buckets\" = \"1\"\n"
                + ");";
        createTable(createOlapTblStmt);
        OlapTable emptyDynamicTable = (OlapTable) Env.getCurrentInternalCatalog()
                .getDbOrAnalysisException("test")
                .getTableOrAnalysisException("histo_dynamic_partition");
        Assert.assertEquals(7, emptyDynamicTable.getAllPartitions().size());

        Iterator<Partition> partitionIterator = emptyDynamicTable.getAllPartitions().iterator();
        List<String> partNames = Lists.newArrayList();
        while (partitionIterator.hasNext()) {
            String partitionName = partitionIterator.next().getName();
            partNames.add(partitionName.substring(1));
        }
        Collections.sort(partNames);

        int partitionCount = -3;
        DateTimeFormatter sdf = DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZoneId.systemDefault());
        for (String partName : partNames) {
            Date partitionDate = Date.from(
                    LocalDate.parse(partName, sdf).atStartOfDay().atZone(ZoneId.systemDefault()).toInstant());
            Date date = new Date();
            Calendar calendar = new GregorianCalendar();
            calendar.setTime(date);
            calendar.add(calendar.DATE, partitionCount);
            date = calendar.getTime();

            Assert.assertEquals(partitionDate.getYear(), date.getYear());
            Assert.assertEquals(partitionDate.getMonth(), date.getMonth());
            Assert.assertEquals(partitionDate.getDay(), date.getDay());

            partitionCount++;
        }
    }

    @Test(expected = DdlException.class)
    public void testFillHistoryDynamicPartition2() throws Exception {
        String createOlapTblStmt = "CREATE TABLE test.`histo_dynamic_partition2` (\n"
                + "  `k1` date NULL COMMENT \"\",\n"
                + "  `k2` int NULL COMMENT \"\",\n"
                + "  `k3` smallint NULL COMMENT \"\",\n"
                + "  `v1` varchar(2048) NULL COMMENT \"\",\n"
                + "  `v2` datetime NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`k1`, `k2`, `k3`)\n"
                + "COMMENT \"OLAP\"\n"
                + "PARTITION BY RANGE(`k1`)\n"
                + "()\n"
                + "DISTRIBUTED BY HASH(`k1`) BUCKETS 32\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\",\n"
                + "\"dynamic_partition.enable\" = \"true\",\n"
                + "\"dynamic_partition.start\" = \"-3000\",\n"
                + "\"dynamic_partition.end\" = \"3\",\n"
                + "\"dynamic_partition.create_history_partition\" = \"true\",\n"
                + "\"dynamic_partition.time_unit\" = \"day\",\n"
                + "\"dynamic_partition.prefix\" = \"p\",\n"
                + "\"dynamic_partition.buckets\" = \"1\"\n"
                + ");";
        // exceed the max dynamic partition limit
        Config.max_dynamic_partition_num = 1000;
        createTable(createOlapTblStmt);
    }

    @Test
    public void testFillHistoryDynamicPartition3() throws Exception {
        String createOlapTblStmt = "CREATE TABLE test.`dynamic_partition3` (\n"
                + "  `k1` date NULL COMMENT \"\"\n"
                + ")\n"
                + "PARTITION BY RANGE (k1)\n"
                + "()\n"
                + "DISTRIBUTED BY HASH(`k1`) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\",\n"
                + "\"dynamic_partition.enable\" = \"true\",\n"
                + "\"dynamic_partition.end\" = \"3\",\n"
                + "\"dynamic_partition.time_unit\" = \"day\",\n"
                + "\"dynamic_partition.prefix\" = \"p\",\n"
                + "\"dynamic_partition.buckets\" = \"1\",\n"
                + "\"dynamic_partition.create_history_partition\" = \"true\"\n"
                + ");";
        // start and history_partition_num are not set, can not create history partition
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "Provide start or history_partition_num property when create_history_partition=true. "
                        + "Otherwise set create_history_partition=false",
                () -> createTable(createOlapTblStmt));

        String createOlapTblStmt2 = "CREATE TABLE test.`dynamic_partition3` (\n"
                + "  `k1` date NULL COMMENT \"\"\n"
                + ")\n"
                + "PARTITION BY RANGE (k1)\n"
                + "()\n"
                + "DISTRIBUTED BY HASH(`k1`) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\",\n"
                + "\"dynamic_partition.enable\" = \"true\",\n"
                + "\"dynamic_partition.end\" = \"3\",\n"
                + "\"dynamic_partition.time_unit\" = \"day\",\n"
                + "\"dynamic_partition.prefix\" = \"p\",\n"
                + "\"dynamic_partition.buckets\" = \"1\",\n"
                + "\"dynamic_partition.history_partition_num\" = \"1000\",\n"
                + "\"dynamic_partition.create_history_partition\" = \"true\"\n"
                + ");";
        // start is not set, but history_partition_num is set too large, can not create history partition
        ExceptionChecker.expectThrowsWithMsg(DdlException.class, "Too many dynamic partitions", () -> createTable(createOlapTblStmt2));

        String createOlapTblStmt3 = "CREATE TABLE test.`dynamic_partition3` (\n"
                + "  `k1` date NULL COMMENT \"\"\n"
                + ")\n"
                + "PARTITION BY RANGE (k1)\n"
                + "()\n"
                + "DISTRIBUTED BY HASH(`k1`) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\",\n"
                + "\"dynamic_partition.enable\" = \"true\",\n"
                + "\"dynamic_partition.end\" = \"3\",\n"
                + "\"dynamic_partition.time_unit\" = \"day\",\n"
                + "\"dynamic_partition.prefix\" = \"p\",\n"
                + "\"dynamic_partition.buckets\" = \"1\",\n"
                + "\"dynamic_partition.start\" = \"-1000\",\n"
                + "\"dynamic_partition.create_history_partition\" = \"true\"\n"
                + ");";
        // start is set but too small,history_partition_num is not set, can not create history partition
        ExceptionChecker.expectThrowsWithMsg(DdlException.class, "Too many dynamic partitions", () -> createTable(createOlapTblStmt3));

        String createOlapTblStmt4 =
                "CREATE TABLE test.`dynamic_partition3` (\n" + "  `k1` date NULL COMMENT \"\"\n" + ")\n"
                        + "PARTITION BY RANGE (k1)\n" + "()\n" + "DISTRIBUTED BY HASH(`k1`) BUCKETS 1\n"
                        + "PROPERTIES (\n" + "\"replication_num\" = \"1\",\n"
                        + "\"dynamic_partition.enable\" = \"true\",\n" + "\"dynamic_partition.end\" = \"3\",\n"
                        + "\"dynamic_partition.time_unit\" = \"day\",\n" + "\"dynamic_partition.prefix\" = \"p\",\n"
                        + "\"dynamic_partition.buckets\" = \"1\",\n" + "\"dynamic_partition.start\" = \"-10\",\n"
                        + "\"dynamic_partition.history_partition_num\" = \"5\",\n"
                        + "\"dynamic_partition.create_history_partition\" = \"true\"\n" + ");";
        // start and history_partition_num are set, create ok
        ExceptionChecker.expectThrowsNoException(() -> createTable(createOlapTblStmt4));
        Database db =
                Env.getCurrentInternalCatalog().getDbOrAnalysisException("test");
        OlapTable tbl = (OlapTable) db.getTableOrAnalysisException("dynamic_partition3");
        Assert.assertEquals(9, tbl.getPartitionNames().size());

        // alter dynamic partition property of table dynamic_partition3
        // start too small
        String alter1 =
                "alter table test.dynamic_partition3 set ('dynamic_partition.start' = '-1000', 'dynamic_partition.history_partition_num' = '1000')";
        ExceptionChecker.expectThrowsWithMsg(DdlException.class, "Too many dynamic partitions",
                () -> alterTable(alter1));

        // end too large
        String alter2 = "alter table test.dynamic_partition3 set ('dynamic_partition.end' = '1000')";
        ExceptionChecker.expectThrowsWithMsg(DdlException.class, "Too many dynamic partitions",
                () -> alterTable(alter2));

        // history_partition_num too large, but because start is -10, so modify ok
        String alter3 = "alter table test.dynamic_partition3 set ('dynamic_partition.history_partition_num' = '1000')";
        ExceptionChecker.expectThrowsNoException(() -> alterTable(alter3));
        Env.getCurrentEnv().getDynamicPartitionScheduler().executeDynamicPartitionFirstTime(db.getId(), tbl.getId());
        Assert.assertEquals(14, tbl.getPartitionNames().size());

        // set start and history_partition_num properly.
        String alter4 = "alter table test.dynamic_partition3 set ('dynamic_partition.history_partition_num' = '100', 'dynamic_partition.start' = '-20')";
        ExceptionChecker.expectThrowsNoException(() -> alterTable(alter4));
        Env.getCurrentEnv().getDynamicPartitionScheduler().executeDynamicPartitionFirstTime(db.getId(), tbl.getId());
        Assert.assertEquals(24, tbl.getPartitionNames().size());

        String createOlapTblStmt5 =
                "CREATE TABLE test.`dynamic_partition4` (\n" + "  `k1` datetime NULL COMMENT \"\"\n" + ")\n"
                        + "PARTITION BY RANGE (k1)\n" + "()\n" + "DISTRIBUTED BY HASH(`k1`) BUCKETS 1\n"
                        + "PROPERTIES (\n" + "\"replication_num\" = \"1\",\n"
                        + "\"dynamic_partition.enable\" = \"true\",\n" + "\"dynamic_partition.end\" = \"3\",\n"
                        + "\"dynamic_partition.time_unit\" = \"day\",\n" + "\"dynamic_partition.prefix\" = \"p\",\n"
                        + "\"dynamic_partition.buckets\" = \"1\",\n" + "\"dynamic_partition.start\" = \"-99999999\",\n"
                        + "\"dynamic_partition.history_partition_num\" = \"5\",\n"
                        + "\"dynamic_partition.create_history_partition\" = \"true\"\n" + ");";
        // start and history_partition_num are set, create ok
        ExceptionChecker.expectThrowsNoException(() -> createTable(createOlapTblStmt5));
        OlapTable tbl4 = (OlapTable) db.getTableOrAnalysisException("dynamic_partition4");
        Assert.assertEquals(9, tbl4.getPartitionNames().size());

        String alter5 = "alter table test.dynamic_partition4 set ('dynamic_partition.history_partition_num' = '3')";
        ExceptionChecker.expectThrowsNoException(() -> alterTable(alter5));
        Env.getCurrentEnv().getDynamicPartitionScheduler().executeDynamicPartitionFirstTime(db.getId(), tbl4.getId());
        Assert.assertEquals(9, tbl4.getPartitionNames().size());
        String dropPartitionErr = Env.getCurrentEnv().getDynamicPartitionScheduler()
                .getRuntimeInfo(tbl4.getId(), DynamicPartitionScheduler.DROP_PARTITION_MSG);
        Assert.assertTrue(dropPartitionErr.contains("'dynamic_partition.start' = -99999999, maybe it's too small, "
                + "can use alter table sql to increase it."));
    }

    @Test
    public void testFillHistoryDynamicPartitionWithHistoryPartitionNum() throws Exception {
        String createOlapTblStmt = "CREATE TABLE test.`history_dynamic_partition_day` (\n"
                + "  `k1` datetime NULL COMMENT \"\",\n"
                + "  `k2` int NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "PARTITION BY RANGE(`k1`)\n"
                + "()\n"
                + "DISTRIBUTED BY HASH(`k2`) BUCKETS 3\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\",\n"
                + "\"dynamic_partition.enable\" = \"true\",\n"
                + "\"dynamic_partition.end\" = \"3\",\n"
                + "\"dynamic_partition.create_history_partition\" = \"true\",\n"
                + "\"dynamic_partition.history_partition_num\" = \"10\",\n"
                + "\"dynamic_partition.time_unit\" = \"day\",\n"
                + "\"dynamic_partition.prefix\" = \"p\",\n"
                + "\"dynamic_partition.buckets\" = \"1\"\n"
                + ");";
        createTable(createOlapTblStmt);
        OlapTable emptyDynamicTable = (OlapTable) Env.getCurrentInternalCatalog()
                .getDbOrAnalysisException("test")
                .getTableOrAnalysisException("history_dynamic_partition_day");
        Map<String, String> tableProperties = emptyDynamicTable.getTableProperty().getProperties();
        Assert.assertEquals(14, emptyDynamicTable.getAllPartitions().size());
        // never delete the old partitions
        Assert.assertEquals(Integer.parseInt(tableProperties.get("dynamic_partition.start")), Integer.MIN_VALUE);
    }

    @Test
    public void testAutoPartitionRetentionCountTableRegisteredAfterSchedulerInit() throws Exception {
        String createOlapTblStmt = "CREATE TABLE test.`auto_partition_retention_init` (\n"
                + "  `k1` datetime(6) NOT NULL\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`k1`)\n"
                + "AUTO PARTITION BY RANGE (date_trunc(k1, 'day')) ()\n"
                + "DISTRIBUTED BY HASH(`k1`) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\",\n"
                + "\"partition.retention_count\" = \"3\"\n"
                + ");";
        createTable(createOlapTblStmt);

        Database db = Env.getCurrentInternalCatalog().getDbOrAnalysisException("test");
        OlapTable tbl = (OlapTable) db.getTableOrAnalysisException("auto_partition_retention_init");
        DynamicPartitionScheduler scheduler = Env.getCurrentEnv().getDynamicPartitionScheduler();

        Assert.assertTrue(scheduler.containsDynamicPartitionTable(db.getId(), tbl.getId()));

        scheduler.removeDynamicPartitionTable(db.getId(), tbl.getId());
        Assert.assertFalse(scheduler.containsDynamicPartitionTable(db.getId(), tbl.getId()));

        Method initDynamicPartitionTable = DynamicPartitionScheduler.class
                .getDeclaredMethod("initDynamicPartitionTable");
        initDynamicPartitionTable.setAccessible(true);
        initDynamicPartitionTable.invoke(scheduler);
        Assert.assertTrue(scheduler.containsDynamicPartitionTable(db.getId(), tbl.getId()));
    }

    @Test
    public void testAllTypeDynamicPartition() throws Exception {
        String createOlapTblStmt = "CREATE TABLE test.`hour_dynamic_partition` (\n"
                + "  `k1` datetime NULL COMMENT \"\",\n"
                + "  `k2` int NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "PARTITION BY RANGE(`k1`)\n"
                + "()\n"
                + "DISTRIBUTED BY HASH(`k2`) BUCKETS 3\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\",\n"
                + "\"dynamic_partition.enable\" = \"true\",\n"
                + "\"dynamic_partition.start\" = \"-3\",\n"
                + "\"dynamic_partition.end\" = \"3\",\n"
                + "\"dynamic_partition.create_history_partition\" = \"true\",\n"
                + "\"dynamic_partition.time_unit\" = \"hour\",\n"
                + "\"dynamic_partition.prefix\" = \"p\",\n"
                + "\"dynamic_partition.buckets\" = \"1\"\n"
                + ");";
        createTable(createOlapTblStmt);
        OlapTable emptyDynamicTable = (OlapTable) Env.getCurrentInternalCatalog()
                .getDbOrAnalysisException("test").getTableOrAnalysisException("hour_dynamic_partition");
        Assert.assertEquals(7, emptyDynamicTable.getAllPartitions().size());

        Iterator<Partition> partitionIterator = emptyDynamicTable.getAllPartitions().iterator();
        while (partitionIterator.hasNext()) {
            String partitionName = partitionIterator.next().getName();
            Assert.assertEquals(11, partitionName.length());
        }

        createOlapTblStmt = "CREATE TABLE test.`week_dynamic_partition` (\n"
                + "  `k1` datetime NULL COMMENT \"\",\n"
                + "  `k2` int NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "PARTITION BY RANGE(`k1`)\n"
                + "()\n"
                + "DISTRIBUTED BY HASH(`k2`) BUCKETS 3\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\",\n"
                + "\"dynamic_partition.enable\" = \"true\",\n"
                + "\"dynamic_partition.start\" = \"-3\",\n"
                + "\"dynamic_partition.end\" = \"3\",\n"
                + "\"dynamic_partition.create_history_partition\" = \"true\",\n"
                + "\"dynamic_partition.time_unit\" = \"week\",\n"
                + "\"dynamic_partition.prefix\" = \"p\",\n"
                + "\"dynamic_partition.buckets\" = \"1\"\n"
                + ");";
        createTable(createOlapTblStmt);
        emptyDynamicTable = (OlapTable) Env.getCurrentInternalCatalog()
                .getDbOrAnalysisException("test").getTableOrAnalysisException("week_dynamic_partition");
        Assert.assertEquals(7, emptyDynamicTable.getAllPartitions().size());

        partitionIterator = emptyDynamicTable.getAllPartitions().iterator();
        while (partitionIterator.hasNext()) {
            String partitionName = partitionIterator.next().getName();
            Assert.assertEquals(8, partitionName.length());
        }

        createOlapTblStmt = "CREATE TABLE test.`month_dynamic_partition` (\n"
                + "  `k1` datetime NULL COMMENT \"\",\n"
                + "  `k2` int NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "PARTITION BY RANGE(`k1`)\n"
                + "()\n"
                + "DISTRIBUTED BY HASH(`k2`) BUCKETS 3\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\",\n"
                + "\"dynamic_partition.enable\" = \"true\",\n"
                + "\"dynamic_partition.start\" = \"-3\",\n"
                + "\"dynamic_partition.end\" = \"3\",\n"
                + "\"dynamic_partition.create_history_partition\" = \"true\",\n"
                + "\"dynamic_partition.time_unit\" = \"month\",\n"
                + "\"dynamic_partition.prefix\" = \"p\",\n"
                + "\"dynamic_partition.buckets\" = \"1\"\n"
                + ");";
        createTable(createOlapTblStmt);
        emptyDynamicTable = (OlapTable) Env.getCurrentInternalCatalog()
                .getDbOrAnalysisException("test")
                .getTableOrAnalysisException("month_dynamic_partition");
        Assert.assertEquals(7, emptyDynamicTable.getAllPartitions().size());

        partitionIterator = emptyDynamicTable.getAllPartitions().iterator();
        while (partitionIterator.hasNext()) {
            String partitionName = partitionIterator.next().getName();
            Assert.assertEquals(7, partitionName.length());
        }

        createOlapTblStmt = "CREATE TABLE test.`year_dynamic_partition` (\n"
            + "  `k1` datetime NULL COMMENT \"\",\n"
            + "  `k2` int NULL COMMENT \"\"\n"
            + ") ENGINE=OLAP\n"
            + "PARTITION BY RANGE(`k1`)\n"
            + "()\n"
            + "DISTRIBUTED BY HASH(`k2`) BUCKETS 3\n"
            + "PROPERTIES (\n"
            + "\"replication_num\" = \"1\",\n"
            + "\"dynamic_partition.enable\" = \"true\",\n"
            + "\"dynamic_partition.start\" = \"-3\",\n"
            + "\"dynamic_partition.end\" = \"3\",\n"
            + "\"dynamic_partition.create_history_partition\" = \"true\",\n"
            + "\"dynamic_partition.time_unit\" = \"year\",\n"
            + "\"dynamic_partition.prefix\" = \"p\",\n"
            + "\"dynamic_partition.buckets\" = \"1\"\n"
            + ");";
        createTable(createOlapTblStmt);
        emptyDynamicTable = (OlapTable) Env.getCurrentInternalCatalog()
            .getDbOrAnalysisException("test")
            .getTableOrAnalysisException("year_dynamic_partition");
        Assert.assertEquals(7, emptyDynamicTable.getAllPartitions().size());

        partitionIterator = emptyDynamicTable.getAllPartitions().iterator();
        while (partitionIterator.hasNext()) {
            String partitionName = partitionIterator.next().getName();
            Assert.assertEquals(5, partitionName.length());
        }

        createOlapTblStmt = "CREATE TABLE test.`int_dynamic_partition_day` (\n"
                + "  `k1` int NULL COMMENT \"\",\n"
                + "  `k2` int NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "PARTITION BY RANGE(`k1`)\n"
                + "()\n"
                + "DISTRIBUTED BY HASH(`k2`) BUCKETS 3\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\",\n"
                + "\"dynamic_partition.enable\" = \"true\",\n"
                + "\"dynamic_partition.start\" = \"-3\",\n"
                + "\"dynamic_partition.end\" = \"3\",\n"
                + "\"dynamic_partition.create_history_partition\" = \"true\",\n"
                + "\"dynamic_partition.time_unit\" = \"day\",\n"
                + "\"dynamic_partition.prefix\" = \"p\",\n"
                + "\"dynamic_partition.buckets\" = \"1\"\n"
                + ");";
        createTable(createOlapTblStmt);
        emptyDynamicTable = (OlapTable) Env.getCurrentInternalCatalog()
                .getDbOrAnalysisException("test")
                .getTableOrAnalysisException("int_dynamic_partition_day");
        Assert.assertEquals(7, emptyDynamicTable.getAllPartitions().size());

        partitionIterator = emptyDynamicTable.getAllPartitions().iterator();
        while (partitionIterator.hasNext()) {
            String partitionName = partitionIterator.next().getName();
            Assert.assertEquals(9, partitionName.length());
        }

        createOlapTblStmt = "CREATE TABLE test.`int_dynamic_partition_week` (\n"
                + "  `k1` int NULL COMMENT \"\",\n"
                + "  `k2` int NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "PARTITION BY RANGE(`k1`)\n"
                + "()\n"
                + "DISTRIBUTED BY HASH(`k2`) BUCKETS 3\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\",\n"
                + "\"dynamic_partition.enable\" = \"true\",\n"
                + "\"dynamic_partition.start\" = \"-3\",\n"
                + "\"dynamic_partition.end\" = \"3\",\n"
                + "\"dynamic_partition.create_history_partition\" = \"true\",\n"
                + "\"dynamic_partition.time_unit\" = \"week\",\n"
                + "\"dynamic_partition.prefix\" = \"p\",\n"
                + "\"dynamic_partition.buckets\" = \"1\"\n"
                + ");";
        createTable(createOlapTblStmt);
        emptyDynamicTable = (OlapTable) Env.getCurrentInternalCatalog()
                .getDbOrAnalysisException("test")
                .getTableOrAnalysisException("int_dynamic_partition_week");
        Assert.assertEquals(7, emptyDynamicTable.getAllPartitions().size());

        partitionIterator = emptyDynamicTable.getAllPartitions().iterator();
        while (partitionIterator.hasNext()) {
            String partitionName = partitionIterator.next().getName();
            Assert.assertEquals(8, partitionName.length());
        }

        createOlapTblStmt = "CREATE TABLE test.`int_dynamic_partition_month` (\n"
                + "  `k1` int NULL COMMENT \"\",\n"
                + "  `k2` int NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "PARTITION BY RANGE(`k1`)\n"
                + "()\n"
                + "DISTRIBUTED BY HASH(`k2`) BUCKETS 3\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\",\n"
                + "\"dynamic_partition.enable\" = \"true\",\n"
                + "\"dynamic_partition.start\" = \"-3\",\n"
                + "\"dynamic_partition.end\" = \"3\",\n"
                + "\"dynamic_partition.create_history_partition\" = \"true\",\n"
                + "\"dynamic_partition.time_unit\" = \"month\",\n"
                + "\"dynamic_partition.prefix\" = \"p\",\n"
                + "\"dynamic_partition.buckets\" = \"1\"\n"
                + ");";
        createTable(createOlapTblStmt);
        emptyDynamicTable = (OlapTable) Env.getCurrentInternalCatalog()
                .getDbOrAnalysisException("test")
                .getTableOrAnalysisException("int_dynamic_partition_month");
        Assert.assertEquals(7, emptyDynamicTable.getAllPartitions().size());

        partitionIterator = emptyDynamicTable.getAllPartitions().iterator();
        while (partitionIterator.hasNext()) {
            String partitionName = partitionIterator.next().getName();
            Assert.assertEquals(7, partitionName.length());
        }
    }

    @Test(expected = DdlException.class)
    public void testHourDynamicPartitionWithIntType() throws Exception {
        String createOlapTblStmt = "CREATE TABLE test.`int_dynamic_partition_hour` (\n"
                + "  `k1` int NULL COMMENT \"\",\n"
                + "  `k2` int NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "PARTITION BY RANGE(`k1`)\n"
                + "()\n"
                + "DISTRIBUTED BY HASH(`k2`) BUCKETS 3\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\",\n"
                + "\"dynamic_partition.enable\" = \"true\",\n"
                + "\"dynamic_partition.start\" = \"-3\",\n"
                + "\"dynamic_partition.end\" = \"3\",\n"
                + "\"dynamic_partition.create_history_partition\" = \"true\",\n"
                + "\"dynamic_partition.time_unit\" = \"hour\",\n"
                + "\"dynamic_partition.prefix\" = \"p\",\n"
                + "\"dynamic_partition.buckets\" = \"1\"\n"
                + ");";
        createTable(createOlapTblStmt);
    }

    @Test
    public void testHotPartitionNum() throws Exception {
        changeBeDisk(TStorageMedium.SSD);

        Database testDb =
                Env.getCurrentInternalCatalog().getDbOrAnalysisException("test");
        // 1. hour
        String createOlapTblStmt = "CREATE TABLE test.`hot_partition_hour_tbl1` (\n"
                + "  `k1` datetime NULL COMMENT \"\",\n"
                + "  `k2` int NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "PARTITION BY RANGE(`k1`)\n"
                + "()\n"
                + "DISTRIBUTED BY HASH(`k2`) BUCKETS 3\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\",\n"
                + "\"dynamic_partition.enable\" = \"true\",\n"
                + "\"dynamic_partition.start\" = \"-3\",\n"
                + "\"dynamic_partition.end\" = \"3\",\n"
                + "\"dynamic_partition.create_history_partition\" = \"true\",\n"
                + "\"dynamic_partition.time_unit\" = \"hour\",\n"
                + "\"dynamic_partition.prefix\" = \"p\",\n"
                + "\"dynamic_partition.buckets\" = \"1\",\n"
                + "\"dynamic_partition.hot_partition_num\" = \"1\"\n"
                + ");";
        createTable(createOlapTblStmt);
        OlapTable tbl = (OlapTable) testDb.getTableOrAnalysisException("hot_partition_hour_tbl1");
        RangePartitionInfo partitionInfo = (RangePartitionInfo) tbl.getPartitionInfo();
        Map<Long, DataProperty> idToDataProperty = new TreeMap<>(partitionInfo.idToDataProperty);
        Assert.assertEquals(7, idToDataProperty.size());
        int count = 0;
        for (DataProperty dataProperty : idToDataProperty.values()) {
            if (count < 3) {
                Assert.assertEquals(TStorageMedium.HDD, dataProperty.getStorageMedium());
            } else {
                Assert.assertEquals(TStorageMedium.SSD, dataProperty.getStorageMedium());
            }
            ++count;
        }

        createOlapTblStmt = "CREATE TABLE test.`hot_partition_hour_tbl2` (\n"
                + "  `k1` datetime NULL COMMENT \"\",\n"
                + "  `k2` int NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "PARTITION BY RANGE(`k1`)\n"
                + "()\n"
                + "DISTRIBUTED BY HASH(`k2`) BUCKETS 3\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\",\n"
                + "\"dynamic_partition.enable\" = \"true\",\n"
                + "\"dynamic_partition.start\" = \"-3\",\n"
                + "\"dynamic_partition.end\" = \"3\",\n"
                + "\"dynamic_partition.create_history_partition\" = \"true\",\n"
                + "\"dynamic_partition.time_unit\" = \"hour\",\n"
                + "\"dynamic_partition.prefix\" = \"p\",\n"
                + "\"dynamic_partition.buckets\" = \"1\",\n"
                + "\"dynamic_partition.hot_partition_num\" = \"0\"\n"
                + ");";
        createTable(createOlapTblStmt);
        tbl = (OlapTable) testDb.getTableOrAnalysisException("hot_partition_hour_tbl2");
        partitionInfo = (RangePartitionInfo) tbl.getPartitionInfo();
        idToDataProperty = new TreeMap<>(partitionInfo.idToDataProperty);
        Assert.assertEquals(7, idToDataProperty.size());
        for (DataProperty dataProperty : idToDataProperty.values()) {
            Assert.assertEquals(TStorageMedium.HDD, dataProperty.getStorageMedium());
        }

        createOlapTblStmt = "CREATE TABLE test.`hot_partition_hour_tbl3` (\n"
                + "  `k1` datetime NULL COMMENT \"\",\n"
                + "  `k2` int NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "PARTITION BY RANGE(`k1`)\n"
                + "()\n"
                + "DISTRIBUTED BY HASH(`k2`) BUCKETS 3\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\",\n"
                + "\"dynamic_partition.enable\" = \"true\",\n"
                + "\"dynamic_partition.start\" = \"-3\",\n"
                + "\"dynamic_partition.end\" = \"3\",\n"
                + "\"dynamic_partition.create_history_partition\" = \"true\",\n"
                + "\"dynamic_partition.time_unit\" = \"hour\",\n"
                + "\"dynamic_partition.prefix\" = \"p\",\n"
                + "\"dynamic_partition.buckets\" = \"1\",\n"
                + "\"dynamic_partition.hot_partition_num\" = \"3\"\n"
                + ");";
        createTable(createOlapTblStmt);
        tbl = (OlapTable) testDb.getTableOrAnalysisException("hot_partition_hour_tbl3");
        partitionInfo = (RangePartitionInfo) tbl.getPartitionInfo();
        idToDataProperty = new TreeMap<>(partitionInfo.idToDataProperty);
        Assert.assertEquals(7, idToDataProperty.size());
        count = 0;
        for (DataProperty dataProperty : idToDataProperty.values()) {
            if (count < 1) {
                Assert.assertEquals(TStorageMedium.HDD, dataProperty.getStorageMedium());
            } else {
                Assert.assertEquals(TStorageMedium.SSD, dataProperty.getStorageMedium());
            }
            ++count;
        }

        // 2. day
        createOlapTblStmt = "CREATE TABLE test.`hot_partition_day_tbl1` (\n"
                + "  `k1` datetime NULL COMMENT \"\",\n"
                + "  `k2` int NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "PARTITION BY RANGE(`k1`)\n"
                + "()\n"
                + "DISTRIBUTED BY HASH(`k2`) BUCKETS 3\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\",\n"
                + "\"dynamic_partition.enable\" = \"true\",\n"
                + "\"dynamic_partition.start\" = \"-3\",\n"
                + "\"dynamic_partition.end\" = \"3\",\n"
                + "\"dynamic_partition.time_unit\" = \"day\",\n"
                + "\"dynamic_partition.prefix\" = \"p\",\n"
                + "\"dynamic_partition.buckets\" = \"1\",\n"
                + "\"dynamic_partition.hot_partition_num\" = \"2\"\n"
                + ");";
        createTable(createOlapTblStmt);
        tbl = (OlapTable) testDb.getTableOrAnalysisException("hot_partition_day_tbl1");
        partitionInfo = (RangePartitionInfo) tbl.getPartitionInfo();
        idToDataProperty = new TreeMap<>(partitionInfo.idToDataProperty);
        Assert.assertEquals(4, idToDataProperty.size());
        for (DataProperty dataProperty : idToDataProperty.values()) {
            Assert.assertEquals(TStorageMedium.SSD, dataProperty.getStorageMedium());
        }

        createOlapTblStmt = "CREATE TABLE test.`hot_partition_day_tbl2` (\n"
                + "  `k1` datetime NULL COMMENT \"\",\n"
                + "  `k2` int NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "PARTITION BY RANGE(`k1`)\n"
                + "()\n"
                + "DISTRIBUTED BY HASH(`k2`) BUCKETS 3\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\",\n"
                + "\"dynamic_partition.enable\" = \"true\",\n"
                + "\"dynamic_partition.start\" = \"-3\",\n"
                + "\"dynamic_partition.end\" = \"4\",\n"
                + "\"dynamic_partition.create_history_partition\" = \"true\",\n"
                + "\"dynamic_partition.time_unit\" = \"day\",\n"
                + "\"dynamic_partition.prefix\" = \"p\",\n"
                + "\"dynamic_partition.buckets\" = \"1\",\n"
                + "\"dynamic_partition.hot_partition_num\" = \"2\"\n"
                + ");";
        createTable(createOlapTblStmt);
        tbl = (OlapTable) testDb.getTableOrAnalysisException("hot_partition_day_tbl2");
        partitionInfo = (RangePartitionInfo) tbl.getPartitionInfo();
        idToDataProperty = new TreeMap<>(partitionInfo.idToDataProperty);
        Assert.assertEquals(8, idToDataProperty.size());
        count = 0;
        for (DataProperty dataProperty : idToDataProperty.values()) {
            if (count < 2) {
                Assert.assertEquals(TStorageMedium.HDD, dataProperty.getStorageMedium());
            } else {
                Assert.assertEquals(TStorageMedium.SSD, dataProperty.getStorageMedium());
            }
            ++count;
        }
        // 3. week
        createOlapTblStmt = "CREATE TABLE test.`hot_partition_week_tbl1` (\n"
                + "  `k1` datetime NULL COMMENT \"\",\n"
                + "  `k2` int NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "PARTITION BY RANGE(`k1`)\n"
                + "()\n"
                + "DISTRIBUTED BY HASH(`k2`) BUCKETS 3\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\",\n"
                + "\"dynamic_partition.enable\" = \"true\",\n"
                + "\"dynamic_partition.start\" = \"-3\",\n"
                + "\"dynamic_partition.end\" = \"4\",\n"
                + "\"dynamic_partition.create_history_partition\" = \"true\",\n"
                + "\"dynamic_partition.time_unit\" = \"week\",\n"
                + "\"dynamic_partition.prefix\" = \"p\",\n"
                + "\"dynamic_partition.buckets\" = \"1\",\n"
                + "\"dynamic_partition.hot_partition_num\" = \"1\"\n"
                + ");";
        createTable(createOlapTblStmt);
        tbl = (OlapTable) testDb.getTableOrAnalysisException("hot_partition_week_tbl1");
        partitionInfo = (RangePartitionInfo) tbl.getPartitionInfo();
        idToDataProperty = new TreeMap<>(partitionInfo.idToDataProperty);
        Assert.assertEquals(8, idToDataProperty.size());
        count = 0;
        for (DataProperty dataProperty : idToDataProperty.values()) {
            if (count < 3) {
                Assert.assertEquals(TStorageMedium.HDD, dataProperty.getStorageMedium());
            } else {
                Assert.assertEquals(TStorageMedium.SSD, dataProperty.getStorageMedium());
            }
            ++count;
        }
        // 4. month
        createOlapTblStmt = "CREATE TABLE test.`hot_partition_month_tbl1` (\n"
                + "  `k1` datetime NULL COMMENT \"\",\n"
                + "  `k2` int NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "PARTITION BY RANGE(`k1`)\n"
                + "()\n"
                + "DISTRIBUTED BY HASH(`k2`) BUCKETS 3\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\",\n"
                + "\"dynamic_partition.enable\" = \"true\",\n"
                + "\"dynamic_partition.start\" = \"-3\",\n"
                + "\"dynamic_partition.end\" = \"4\",\n"
                + "\"dynamic_partition.create_history_partition\" = \"true\",\n"
                + "\"dynamic_partition.time_unit\" = \"month\",\n"
                + "\"dynamic_partition.prefix\" = \"p\",\n"
                + "\"dynamic_partition.buckets\" = \"1\",\n"
                + "\"dynamic_partition.hot_partition_num\" = \"4\"\n"
                + ");";
        createTable(createOlapTblStmt);
        tbl = (OlapTable) testDb.getTableOrAnalysisException("hot_partition_month_tbl1");
        partitionInfo = (RangePartitionInfo) tbl.getPartitionInfo();
        idToDataProperty = new TreeMap<>(partitionInfo.idToDataProperty);
        Assert.assertEquals(8, idToDataProperty.size());
        for (DataProperty dataProperty : idToDataProperty.values()) {
            Assert.assertEquals(TStorageMedium.SSD, dataProperty.getStorageMedium());
        }
    }

    @Test(expected = DdlException.class)
    public void testHotPartitionNumAbnormalLT0() throws Exception {
        changeBeDisk(TStorageMedium.SSD);

        // dynamic_partition.hot_partition_num must larger than 0.
        String createOlapTblStmt = "CREATE TABLE test.`hot_partition_hour_tbl1_lt0` (\n"
                + "  `k1` datetime NULL COMMENT \"\",\n"
                + "  `k2` int NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "PARTITION BY RANGE(`k1`)\n"
                + "()\n"
                + "DISTRIBUTED BY HASH(`k2`) BUCKETS 3\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\",\n"
                + "\"dynamic_partition.enable\" = \"true\",\n"
                + "\"dynamic_partition.start\" = \"-3\",\n"
                + "\"dynamic_partition.end\" = \"3\",\n"
                + "\"dynamic_partition.create_history_partition\" = \"true\",\n"
                + "\"dynamic_partition.time_unit\" = \"hour\",\n"
                + "\"dynamic_partition.prefix\" = \"p\",\n"
                + "\"dynamic_partition.buckets\" = \"1\",\n"
                + "\"dynamic_partition.hot_partition_num\" = \"-1\"\n"
                + ");";
        createTable(createOlapTblStmt);
    }

    @Test(expected = DdlException.class)
    public void testHotPartitionNumAbnormalMissSSD() throws Exception {
        changeBeDisk(TStorageMedium.HDD);

        // when dynamic_partition.hot_partition_num > 0, it require ssd storage medium.
        String createOlapTblStmt = "CREATE TABLE test.`hot_partition_hour_tbl1_miss_ssd` (\n"
                + "  `k1` datetime NULL COMMENT \"\",\n"
                + "  `k2` int NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "PARTITION BY RANGE(`k1`)\n"
                + "()\n"
                + "DISTRIBUTED BY HASH(`k2`) BUCKETS 3\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\",\n"
                + "\"dynamic_partition.enable\" = \"true\",\n"
                + "\"dynamic_partition.start\" = \"-3\",\n"
                + "\"dynamic_partition.end\" = \"3\",\n"
                + "\"dynamic_partition.create_history_partition\" = \"true\",\n"
                + "\"dynamic_partition.time_unit\" = \"hour\",\n"
                + "\"dynamic_partition.prefix\" = \"p\",\n"
                + "\"dynamic_partition.buckets\" = \"1\",\n"
                + "\"dynamic_partition.hot_partition_num\" = \"1\"\n"
                + ");";
        createTable(createOlapTblStmt);
    }

    @Test
    public void testRuntimeInfo() throws Exception {
        DynamicPartitionScheduler scheduler = new DynamicPartitionScheduler("test", 10);
        long tableId = 1001;
        String key1 = "key1";
        String value1 = "value1";
        String key2 = "key2";
        String value2 = "value2";
        // add
        scheduler.createOrUpdateRuntimeInfo(tableId, key1, value1);
        scheduler.createOrUpdateRuntimeInfo(tableId, key2, value2);
        Assert.assertTrue(scheduler.getRuntimeInfo(tableId, key1) == value1);

        // modify
        String value3 = "value2";
        scheduler.createOrUpdateRuntimeInfo(tableId, key1, value3);
        Assert.assertTrue(scheduler.getRuntimeInfo(tableId, key1) == value3);

        // remove
        scheduler.removeRuntimeInfo(tableId);
        Assert.assertTrue(scheduler.getRuntimeInfo(tableId, key1) == FeConstants.null_string);
    }

    @Test
    public void testMissReservedHistoryPeriods() throws Exception {
        String createOlapTblStmt = "CREATE TABLE test.`dynamic_partition_miss_reserved_history_periods` (\n"
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
                + "\"dynamic_partition.buckets\" = \"3\",\n"
                + "\"dynamic_partition.time_unit\" = \"day\",\n"
                + "\"dynamic_partition.prefix\" = \"p\"\n"
                + ");";
        createTable(createOlapTblStmt);
        OlapTable table = (OlapTable) Env.getCurrentInternalCatalog()
                .getDbOrAnalysisException("test")
                .getTableOrAnalysisException("dynamic_partition_miss_reserved_history_periods");
        Assert.assertEquals("NULL", table.getTableProperty().getDynamicPartitionProperty().getReservedHistoryPeriods());
    }

    @Test
    public void testNormalReservedHisrotyPeriods() throws Exception {
        String createOlapTblStmt = "CREATE TABLE test.`dynamic_partition_normal_reserved_history_periods` (\n"
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
                + "PARTITION p3 VALUES LESS THAN (\"2014-12-01\"),\n"
                + "PARTITION p4 VALUES LESS THAN (\"2020-06-01\"),\n"
                + "PARTITION p5 VALUES LESS THAN (\"2020-06-20\"),\n"
                + "PARTITION p6 VALUES LESS THAN (\"2020-10-25\"),\n"
                + "PARTITION p7 VALUES LESS THAN (\"2020-11-01\"),\n"
                + "PARTITION p8 VALUES LESS THAN (\"2020-11-11\"),\n"
                + "PARTITION p9 VALUES LESS THAN (\"2020-11-21\"),\n"
                + "PARTITION p10 VALUES LESS THAN (\"2021-04-20\"),\n"
                + "PARTITION p11 VALUES LESS THAN (\"2021-05-20\")\n"
                + ")\n"
                + "DISTRIBUTED BY HASH(`k1`) BUCKETS 32\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\",\n"
                + "\"dynamic_partition.enable\" = \"true\",\n"
                + "\"dynamic_partition.start\" = \"-3\",\n"
                + "\"dynamic_partition.end\" = \"3\",\n"
                + "\"dynamic_partition.buckets\" = \"3\",\n"
                + "\"dynamic_partition.time_unit\" = \"day\",\n"
                + "\"dynamic_partition.prefix\" = \"p\",\n"
                + "\"dynamic_partition.reserved_history_periods\" = \"[2020-06-01,2020-06-20],[2020-10-25,2020-11-15],[2021-06-01,2021-06-20]\"\n"
                + ");";
        createTable(createOlapTblStmt);
        OlapTable table = (OlapTable) Env.getCurrentInternalCatalog()
                .getDbOrAnalysisException("test")
                .getTableOrAnalysisException("dynamic_partition_normal_reserved_history_periods");
        Assert.assertEquals("[2020-06-01,2020-06-20],[2020-10-25,2020-11-15],[2021-06-01,2021-06-20]", table.getTableProperty().getDynamicPartitionProperty().getReservedHistoryPeriods());
        Assert.assertEquals(table.getAllPartitions().size(), 9);

        String createOlapTblStmt2 = "CREATE TABLE test.`dynamic_partition_normal_reserved_history_periods2` (\n"
                + "  `k1` datetime NULL COMMENT \"\",\n"
                + "  `k2` int NULL COMMENT \"\",\n"
                + "  `k3` smallint NULL COMMENT \"\",\n"
                + "  `v1` varchar(2048) NULL COMMENT \"\",\n"
                + "  `v2` datetime NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`k1`, `k2`, `k3`)\n"
                + "COMMENT \"OLAP\"\n"
                + "PARTITION BY RANGE (k1)\n"
                + "(\n"
                + "PARTITION p1 VALUES LESS THAN (\"2014-01-01 00:00:00\"),\n"
                + "PARTITION p2 VALUES LESS THAN (\"2014-01-01 03:00:00\"),\n"
                + "PARTITION p3 VALUES LESS THAN (\"2014-01-01 04:00:00\"),\n"
                + "PARTITION p4 VALUES LESS THAN (\"2020-01-01 08:00:00\"),\n"
                + "PARTITION p5 VALUES LESS THAN (\"2020-06-20 00:00:00\")\n"
                + ")\n"
                + "DISTRIBUTED BY HASH(`k1`) BUCKETS 32\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\",\n"
                + "\"dynamic_partition.enable\" = \"true\",\n"
                + "\"dynamic_partition.start\" = \"-3\",\n"
                + "\"dynamic_partition.end\" = \"3\",\n"
                + "\"dynamic_partition.buckets\" = \"3\",\n"
                + "\"dynamic_partition.time_unit\" = \"hour\",\n"
                + "\"dynamic_partition.prefix\" = \"p\",\n"
                + "\"dynamic_partition.reserved_history_periods\" = \"[2014-01-01 00:00:00,2014-01-01 03:00:00]\"\n"
                + ");";
        createTable(createOlapTblStmt2);
        OlapTable table2 = (OlapTable) Env.getCurrentInternalCatalog()
                .getDbOrAnalysisException("test")
                .getTableOrAnalysisException("dynamic_partition_normal_reserved_history_periods2");
        Assert.assertEquals("[2014-01-01 00:00:00,2014-01-01 03:00:00]", table2.getTableProperty().getDynamicPartitionProperty().getReservedHistoryPeriods());
        Assert.assertEquals(table2.getAllPartitions().size(), 6);

        String createOlapTblStmt3 = "CREATE TABLE test.`dynamic_partition_normal_reserved_history_periods3` (\n"
                + "  `k1` int NULL COMMENT \"\",\n"
                + "  `k2` int NULL COMMENT \"\",\n"
                + "  `k3` smallint NULL COMMENT \"\",\n"
                + "  `v1` varchar(2048) NULL COMMENT \"\",\n"
                + "  `v2` datetime NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`k1`, `k2`, `k3`)\n"
                + "COMMENT \"OLAP\"\n"
                + "PARTITION BY RANGE (k1)\n"
                + "(\n"
                + "PARTITION p202127 VALUES [(\"20200527\"), (\"20200628\"))\n"
                + ")\n"
                + "DISTRIBUTED BY HASH(`k2`) BUCKETS 32\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\",\n"
                + "\"dynamic_partition.enable\" = \"true\",\n"
                + "\"dynamic_partition.start\" = \"-3\",\n"
                + "\"dynamic_partition.end\" = \"3\",\n"
                + "\"dynamic_partition.buckets\" = \"1\",\n"
                + "\"dynamic_partition.time_unit\" = \"day\",\n"
                + "\"dynamic_partition.prefix\" = \"p\",\n"
                + "\"dynamic_partition.reserved_history_periods\" = \"[2020-06-01,2020-06-30]\"\n"
                + ");";
        createTable(createOlapTblStmt3);
        OlapTable table3 = (OlapTable) Env.getCurrentInternalCatalog()
                .getDbOrAnalysisException("test")
                .getTableOrAnalysisException("dynamic_partition_normal_reserved_history_periods3");
        Assert.assertEquals("[2020-06-01,2020-06-30]", table3.getTableProperty().getDynamicPartitionProperty().getReservedHistoryPeriods());
        Assert.assertEquals(table3.getAllPartitions().size(), 5);
    }

    @Test
    public void testInvalidReservedHistoryPeriods() throws Exception {
        String createOlapTblStmt1 = "CREATE TABLE test.`dynamic_partition_invalid_reserved_history_periods1` (\n"
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
                + "\"dynamic_partition.buckets\" = \"3\",\n"
                + "\"dynamic_partition.time_unit\" = \"day\",\n"
                + "\"dynamic_partition.prefix\" = \"p\",\n"
                + "\"dynamic_partition.reserved_history_periods\" = \"[20210101,2021-10-10]\"\n"
                + ");";
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "errCode = 2, detailMessage = Invalid \" dynamic_partition.reserved_history_periods \" value [20210101,2021-10-10]. "
                        + "It must be like \"[yyyy-MM-dd,yyyy-MM-dd],[...,...]\" while time_unit is DAY/WEEK/MONTH or "
                        + "\"[yyyy-MM-dd HH:mm:ss,yyyy-MM-dd HH:mm:ss],[...,...]\" while time_unit is HOUR.",
                () -> createTable(createOlapTblStmt1));

        String createOlapTblStmt2 = "CREATE TABLE test.`dynamic_partition_invalid_reserved_history_periods2` (\n"
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
                + "\"dynamic_partition.buckets\" = \"3\",\n"
                + "\"dynamic_partition.time_unit\" = \"day\",\n"
                + "\"dynamic_partition.prefix\" = \"p\",\n"
                + "\"dynamic_partition.reserved_history_periods\" = \"[0000-00-00,2021-10-10]\"\n"
                + ");";
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "errCode = 2, detailMessage = Invalid dynamic_partition.reserved_history_periods value. "
                        + "It must be like "
                        + "\"[yyyy-MM-dd,yyyy-MM-dd],[...,...]\" while time_unit is DAY/WEEK/MONTH or "
                        + "\"[yyyy-MM-dd HH:mm:ss,yyyy-MM-dd HH:mm:ss],[...,...]\" while time_unit is HOUR.",
                () -> createTable(createOlapTblStmt2));
    }

    @Test
    public void testReservedHistoryPeriodsValidate() throws Exception {
        String createOlapTblStmt1 = "CREATE TABLE test.`dynamic_partition_reserved_history_periods_validate1` (\n"
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
                + "\"dynamic_partition.buckets\" = \"3\",\n"
                + "\"dynamic_partition.time_unit\" = \"day\",\n"
                + "\"dynamic_partition.prefix\" = \"p\",\n"
                + "\"dynamic_partition.reserved_history_periods\" = \"[2021-01-01,]\"\n"
                + ");";
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "errCode = 2, detailMessage = Invalid \" dynamic_partition.reserved_history_periods \" value [2021-01-01,]. "
                        + "It must be like "
                        + "\"[yyyy-MM-dd,yyyy-MM-dd],[...,...]\" while time_unit is DAY/WEEK/MONTH "
                        + "or \"[yyyy-MM-dd HH:mm:ss,yyyy-MM-dd HH:mm:ss],[...,...]\" while time_unit is HOUR.",
                () -> createTable(createOlapTblStmt1));

        String createOlapTblStmt2 = "CREATE TABLE test.`dynamic_partition_reserved_history_periods_validate2` (\n"
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
                + "\"dynamic_partition.buckets\" = \"3\",\n"
                + "\"dynamic_partition.time_unit\" = \"day\",\n"
                + "\"dynamic_partition.prefix\" = \"p\",\n"
                + "\"dynamic_partition.reserved_history_periods\" = \"[,2021-01-01]\"\n"
                + ");";
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "errCode = 2, detailMessage = Invalid \" dynamic_partition.reserved_history_periods \" value [,2021-01-01]. "
                        + "It must be like "
                        + "\"[yyyy-MM-dd,yyyy-MM-dd],[...,...]\" while time_unit is DAY/WEEK/MONTH or "
                        + "\"[yyyy-MM-dd HH:mm:ss,yyyy-MM-dd HH:mm:ss],[...,...]\" while time_unit is HOUR.",
                () -> createTable(createOlapTblStmt2));

        String createOlapTblStmt3 = "CREATE TABLE test.`dynamic_partition_reserved_history_periods_validate3` (\n"
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
                + "\"dynamic_partition.buckets\" = \"3\",\n"
                + "\"dynamic_partition.time_unit\" = \"day\",\n"
                + "\"dynamic_partition.prefix\" = \"p\",\n"
                + "\"dynamic_partition.reserved_history_periods\" = \"[2020-01-01,2020-03-01],[2021-10-01,2021-09-01]\"\n"
                + ");";
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "errCode = 2, detailMessage = The first date is larger than the second date, [2021-10-01,2021-09-01] is invalid.",
                () -> createTable(createOlapTblStmt3));

        String createOlapTblStmt4 = "CREATE TABLE test.`dynamic_partition_reserved_history_periods_validate4` (\n"
                + "  `k1` datetime NULL COMMENT \"\",\n"
                + "  `k2` int NULL COMMENT \"\",\n"
                + "  `k3` smallint NULL COMMENT \"\",\n"
                + "  `v1` varchar(2048) NULL COMMENT \"\",\n"
                + "  `v2` datetime NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`k1`, `k2`, `k3`)\n"
                + "COMMENT \"OLAP\"\n"
                + "PARTITION BY RANGE (k1)\n"
                + "(\n"
                + "PARTITION p1 VALUES LESS THAN (\"2014-01-01 00:00:00\"),\n"
                + "PARTITION p2 VALUES LESS THAN (\"2014-06-01 00:00:00\"),\n"
                + "PARTITION p3 VALUES LESS THAN (\"2014-12-01 00:00:00\")\n"
                + ")\n"
                + "DISTRIBUTED BY HASH(`k1`) BUCKETS 32\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\",\n"
                + "\"dynamic_partition.enable\" = \"true\",\n"
                + "\"dynamic_partition.start\" = \"-3\",\n"
                + "\"dynamic_partition.end\" = \"3\",\n"
                + "\"dynamic_partition.buckets\" = \"3\",\n"
                + "\"dynamic_partition.time_unit\" = \"hour\",\n"
                + "\"dynamic_partition.prefix\" = \"p\",\n"
                + "\"dynamic_partition.reserved_history_periods\" = \"[2020-01-01,2020-03-01]\"\n"
                + ");";
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "errCode = 2, detailMessage = Invalid \" dynamic_partition.reserved_history_periods \""
                        + " value [2020-01-01,2020-03-01]. "
                        + "It must be like "
                        + "\"[yyyy-MM-dd,yyyy-MM-dd],[...,...]\" while time_unit is DAY/WEEK/MONTH "
                        + "or \"[yyyy-MM-dd HH:mm:ss,yyyy-MM-dd HH:mm:ss],[...,...]\" while time_unit is HOUR.",
                () -> createTable(createOlapTblStmt4));
    }

    @Test
    public void testNoPartition() throws AnalysisException {
        String createOlapTblStmt = "CREATE TABLE test.`no_partition` (\n"
                + "  `k1` datetime NULL COMMENT \"\",\n"
                + "  `k2` int NULL COMMENT \"\",\n"
                + "  `k3` smallint NULL COMMENT \"\",\n"
                + "  `v1` varchar(2048) NULL COMMENT \"\",\n"
                + "  `v2` datetime NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`k1`, `k2`, `k3`)\n"
                + "COMMENT \"OLAP\"\n"
                + "PARTITION BY RANGE (k1)()\n"
                + "DISTRIBUTED BY HASH(`k1`) BUCKETS 32\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\"\n"
                + ");";
        ExceptionChecker.expectThrowsNoException(() -> createTable(createOlapTblStmt));
        OlapTable table = (OlapTable) Env.getCurrentInternalCatalog()
                .getDbOrAnalysisException("test")
                .getTableOrAnalysisException("no_partition");
        Collection<Partition> partitions = table.getPartitions();
        Assert.assertTrue(partitions.isEmpty());
        OlapTable copiedTable = table.selectiveCopy(Collections.emptyList(), IndexExtState.VISIBLE, true);
        partitions = copiedTable.getPartitions();
        Assert.assertTrue(partitions.isEmpty());
    }

    @Test
    public void testRejectDynamicPartitionStorageMediumOnNonDynamicTable() throws Exception {
        String createOlapTblStmt = "CREATE TABLE test.`non_dynamic_storage_medium` (\n"
                + "  `k1` date NULL COMMENT \"\",\n"
                + "  `v1` int NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`k1`)\n"
                + "PARTITION BY RANGE(`k1`)\n"
                + "(\n"
                + "PARTITION p1 VALUES [('2026-05-26'), ('2026-05-27')),\n"
                + "PARTITION p2 VALUES [('2026-05-27'), ('2026-05-28'))\n"
                + ")\n"
                + "DISTRIBUTED BY HASH(`k1`) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\"\n"
                + ");";
        createTable(createOlapTblStmt);

        String alterStmt = "ALTER TABLE test.`non_dynamic_storage_medium` "
                + "SET (\"dynamic_partition.storage_medium\" = \"hdd\")";
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "is not a dynamic partition table",
                () -> alterTable(alterStmt));

        Database db = Env.getCurrentInternalCatalog().getDbOrAnalysisException("test");
        OlapTable table = (OlapTable) db.getTableOrAnalysisException("non_dynamic_storage_medium");
        Assert.assertFalse(table.dynamicPartitionExists());
        Assert.assertFalse(table.getTableProperty().getProperties()
                .containsKey(DynamicPartitionProperty.STORAGE_MEDIUM));
        Assert.assertNotNull(table.selectiveCopy(null, IndexExtState.VISIBLE, true));
    }

    @Test
    public void testRejectDynamicPartitionStoragePolicyOnNonDynamicTable() throws Exception {
        String createOlapTblStmt = "CREATE TABLE test.`non_dynamic_storage_policy` (\n"
                + "  `k1` date NULL COMMENT \"\",\n"
                + "  `v1` int NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`k1`)\n"
                + "PARTITION BY RANGE(`k1`)\n"
                + "(\n"
                + "PARTITION p1 VALUES [('2026-05-26'), ('2026-05-27')),\n"
                + "PARTITION p2 VALUES [('2026-05-27'), ('2026-05-28'))\n"
                + ")\n"
                + "DISTRIBUTED BY HASH(`k1`) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\"\n"
                + ");";
        createTable(createOlapTblStmt);

        String alterStmt = "ALTER TABLE test.`non_dynamic_storage_policy` "
                + "SET (\"dynamic_partition.storage_policy\" = \"test_policy\")";
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "is not a dynamic partition table",
                () -> alterTable(alterStmt));

        Database db = Env.getCurrentInternalCatalog().getDbOrAnalysisException("test");
        OlapTable table = (OlapTable) db.getTableOrAnalysisException("non_dynamic_storage_policy");
        Assert.assertFalse(table.dynamicPartitionExists());
        Assert.assertFalse(table.getTableProperty().getProperties()
                .containsKey(DynamicPartitionProperty.STORAGE_POLICY));
        Assert.assertNotNull(table.selectiveCopy(null, IndexExtState.VISIBLE, true));
    }

    @Test
    public void testAlterDynamicPartitionStorageMediumOnDynamicTable() throws Exception {
        String createOlapTblStmt = "CREATE TABLE test.`dynamic_storage_medium` (\n"
                + "  `k1` date NULL COMMENT \"\",\n"
                + "  `v1` int NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`k1`)\n"
                + "PARTITION BY RANGE(`k1`)()\n"
                + "DISTRIBUTED BY HASH(`k1`) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\",\n"
                + "\"dynamic_partition.enable\" = \"true\",\n"
                + "\"dynamic_partition.time_unit\" = \"DAY\",\n"
                + "\"dynamic_partition.start\" = \"-1\",\n"
                + "\"dynamic_partition.end\" = \"3\",\n"
                + "\"dynamic_partition.prefix\" = \"p\",\n"
                + "\"dynamic_partition.buckets\" = \"1\"\n"
                + ");";
        createTable(createOlapTblStmt);

        String alterStmt = "ALTER TABLE test.`dynamic_storage_medium` "
                + "SET (\"dynamic_partition.storage_medium\" = \"hdd\")";
        ExceptionChecker.expectThrowsNoException(() -> alterTable(alterStmt));

        Database db = Env.getCurrentInternalCatalog().getDbOrAnalysisException("test");
        OlapTable table = (OlapTable) db.getTableOrAnalysisException("dynamic_storage_medium");
        Assert.assertTrue(table.dynamicPartitionExists());
        Assert.assertEquals("hdd", table.getTableProperty().getDynamicPartitionProperty().getStorageMedium());
        Assert.assertEquals(3, table.getTableProperty().getDynamicPartitionProperty().getEnd());
        Assert.assertEquals(1, table.getTableProperty().getDynamicPartitionProperty().getBuckets());
    }

    @Test
    public void testHourUnitWithDateType() throws AnalysisException {
        String createOlapTblStmt = "CREATE TABLE if not exists test.hour_with_date1 (\n"
                + "  `days` DATEV2 NOT NULL,\n"
                + "  `hours` char(2) NOT NULL,\n"
                + "  `positionID` char(20)\n"
                + "  )\n"
                + "UNIQUE KEY(`days`,`hours`,`positionID`)\n"
                + "PARTITION BY RANGE(`days`) ()\n"
                + "DISTRIBUTED BY HASH(`positionID`) BUCKETS AUTO\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\",\n"
                + "\"compression\" = \"zstd\",\n"
                + "\"enable_unique_key_merge_on_write\" = \"true\",\n"
                + "\"light_schema_change\" = \"true\",\n"
                + "\"dynamic_partition.enable\" = \"true\",\n"
                + "\"dynamic_partition.time_zone\" = \"+00:00\",\n"
                + "\"dynamic_partition.time_unit\" = \"HOUR\",\n"
                + "\"dynamic_partition.start\" = \"-24\",\n"
                + "\"dynamic_partition.end\" = \"24\",\n"
                + "\"dynamic_partition.prefix\" = \"p\",\n"
                + "\"dynamic_partition.buckets\" = \"2\",\n"
                + "\"dynamic_partition.create_history_partition\" = \"true\"\n"
                + ");";
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "could not be HOUR when type of partition column days is DATE or DATEV2",
                () -> createTable(createOlapTblStmt));

        String createOlapTblStmt2 = "CREATE TABLE if not exists test.hour_with_date2 (\n"
                + "  `days` DATETIMEV2 NOT NULL,\n"
                + "  `hours` char(2) NOT NULL,\n"
                + "  `positionID` char(20)\n"
                + "  )\n"
                + "UNIQUE KEY(`days`,`hours`,`positionID`)\n"
                + "PARTITION BY RANGE(`days`) ()\n"
                + "DISTRIBUTED BY HASH(`positionID`) BUCKETS AUTO\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\",\n"
                + "\"compression\" = \"zstd\",\n"
                + "\"enable_unique_key_merge_on_write\" = \"true\",\n"
                + "\"light_schema_change\" = \"true\",\n"
                + "\"dynamic_partition.enable\" = \"true\",\n"
                + "\"dynamic_partition.time_zone\" = \"+00:00\",\n"
                + "\"dynamic_partition.time_unit\" = \"HOUR\",\n"
                + "\"dynamic_partition.start\" = \"-24\",\n"
                + "\"dynamic_partition.end\" = \"24\",\n"
                + "\"dynamic_partition.prefix\" = \"p\",\n"
                + "\"dynamic_partition.buckets\" = \"2\",\n"
                + "\"dynamic_partition.create_history_partition\" = \"true\"\n"
                + ");";
        ExceptionChecker.expectThrowsNoException(() -> createTable(createOlapTblStmt2));

        connectContext.getSessionVariable().setTimeZone("Asia/Tokyo");
        String createOlapTblStmt3 = "CREATE TABLE if not exists test.hour_with_date3 (\n"
                + "  `days` DATETIMEV2 NOT NULL,\n"
                + "  `hours` char(2) NOT NULL,\n"
                + "  `positionID` char(20)\n"
                + "  )\n"
                + "UNIQUE KEY(`days`,`hours`,`positionID`)\n"
                + "PARTITION BY RANGE(`days`) ()\n"
                + "DISTRIBUTED BY HASH(`positionID`) BUCKETS AUTO\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\",\n"
                + "\"compression\" = \"zstd\",\n"
                + "\"enable_unique_key_merge_on_write\" = \"true\",\n"
                + "\"light_schema_change\" = \"true\",\n"
                + "\"dynamic_partition.enable\" = \"true\",\n"
                + "\"dynamic_partition.time_unit\" = \"HOUR\",\n"
                + "\"dynamic_partition.start\" = \"-24\",\n"
                + "\"dynamic_partition.end\" = \"24\",\n"
                + "\"dynamic_partition.prefix\" = \"p\",\n"
                + "\"dynamic_partition.buckets\" = \"2\",\n"
                + "\"dynamic_partition.hot_partition_num\" = \"0\",\n"
                + "\"dynamic_partition.storage_medium\" = \"HDD\", \n"
                + "\"dynamic_partition.create_history_partition\" = \"true\"\n"
                + ");";
        ExceptionChecker.expectThrowsNoException(() -> createTable(createOlapTblStmt3));
    }

    @Test
    public void testAutoBuckets() throws Exception {
        String createOlapTblStmt = " CREATE TABLE test.test_autobucket_dynamic_partition \n"
                + " (k1 DATETIME)\n"
                + " PARTITION BY RANGE (k1) () DISTRIBUTED BY HASH (k1) BUCKETS AUTO\n"
                + " PROPERTIES (\n"
                + " \"dynamic_partition.enable\" = \"true\",\n"
                + " \"dynamic_partition.time_unit\" = \"YEAR\",\n"
                + " \"dynamic_partition.start\" = \"-50\",\n"
                + " \"dynamic_partition.create_history_partition\" = \"true\",\n"
                + " \"dynamic_partition.end\" = \"1\",\n"
                + " \"dynamic_partition.prefix\" = \"p\",\n"
                + " \"replication_allocation\" = \"tag.location.default: 1\"\n"
                + ")";
        ExceptionChecker.expectThrowsNoException(() -> createTable(createOlapTblStmt));
        Database db = Env.getCurrentInternalCatalog().getDbOrAnalysisException("test");
        OlapTable table = (OlapTable) db.getTableOrAnalysisException("test_autobucket_dynamic_partition");
        List<Partition> partitions = Lists.newArrayList(table.getAllPartitions());
        Assert.assertEquals(52, partitions.size());
        for (Partition partition : partitions) {
            Assert.assertEquals(FeConstants.default_bucket_num, partition.getDistributionInfo().getBucketNum());
            partition.setVisibleVersionAndTime(2L, System.currentTimeMillis());
        }
        RebalancerTestUtil.updateReplicaDataSize(1, 1, 1);

        Config.autobucket_out_of_bounds_percent_threshold = 0.99;
        String alterStmt1 =
                "alter table test.test_autobucket_dynamic_partition set ('dynamic_partition.end' = '2')";
        ExceptionChecker.expectThrowsNoException(() -> alterTable(alterStmt1));
        List<Pair<Long, Long>> tempDynamicPartitionTableInfo = Lists.newArrayList(Pair.of(db.getId(), table.getId()));
        Env.getCurrentEnv().getDynamicPartitionScheduler().executeDynamicPartition(tempDynamicPartitionTableInfo, false);

        partitions = Lists.newArrayList(table.getAllPartitions());
        partitions.sort(Comparator.comparing(Partition::getId));
        Assert.assertEquals(53, partitions.size());
        Assert.assertEquals(3, partitions.get(partitions.size() - 1).getDistributionInfo().getBucketNum());
        Config.autobucket_out_of_bounds_percent_threshold = 0.5;

        table.readLock();
        try {
            // first 40 partitions with size 0,  then 13 partitions with size 100GB(10GB * 10 buckets)
            for (int i = 0; i < 52; i++) {
                Partition partition = partitions.get(i);
                partition.updateVisibleVersion(2L);
                for (MaterializedIndex idx : partition.getMaterializedIndices(
                        MaterializedIndex.IndexExtState.VISIBLE)) {
                    Assert.assertEquals(10, idx.getTablets().size());
                    for (Tablet tablet : idx.getTablets()) {
                        for (Replica replica : tablet.getReplicas()) {
                            replica.updateVersion(2L);
                            replica.setDataSize(i < 40 ? 0L : 10L << 30);
                            replica.setRowCount(1000L);
                        }
                    }
                }
                if (i >= 40) {
                    // first 52 partitions are 10 buckets(FeConstants.default_bucket_num)
                    Assert.assertEquals(10 * (10L << 30), partition.getAllDataSize(true));
                }
            }
        } finally {
            table.readUnlock();
        }

        String alterStmt2 =
                "alter table test.test_autobucket_dynamic_partition set ('dynamic_partition.end' = '3')";
        ExceptionChecker.expectThrowsNoException(() -> alterTable(alterStmt2));
        Env.getCurrentEnv().getDynamicPartitionScheduler().executeDynamicPartition(tempDynamicPartitionTableInfo, false);

        partitions = Lists.newArrayList(table.getAllPartitions());
        partitions.sort(Comparator.comparing(Partition::getId));
        Assert.assertEquals(54, partitions.size());
        // 100GB total, 5GB per bucket, should 20 buckets.
        Assert.assertEquals(20, partitions.get(partitions.size() - 1).getDistributionInfo().getBucketNum());

        // mock partition size eq 0, use back-to-back logic
        table.readLock();
        try {
            // when fe restart, when stat thread not get replica size from be/ms, replica size eq 0
            for (int i = 0; i < 54; i++) {
                Partition partition = partitions.get(i);
                partition.updateVisibleVersion(2L);
                for (MaterializedIndex idx : partition.getMaterializedIndices(
                        MaterializedIndex.IndexExtState.VISIBLE)) {
                    if (i < 52) {
                        Assert.assertEquals(10, idx.getTablets().size());
                    } else if (i == 52) {
                        Assert.assertEquals(3, idx.getTablets().size());
                    } else if (i == 53) {
                        Assert.assertEquals(20, idx.getTablets().size());
                    }
                    for (Tablet tablet : idx.getTablets()) {
                        for (Replica replica : tablet.getReplicas()) {
                            replica.updateVersion(3L);
                            // mock replica size eq 0
                            replica.setDataSize(0L);
                            replica.setRowCount(0L);
                        }
                    }
                }
                Assert.assertEquals(0, partition.getAllDataSize(true));
            }
        } finally {
            table.readUnlock();
        }

        String alterStmt3 = "alter table test.test_autobucket_dynamic_partition set ('dynamic_partition.end' = '4')";
        ExceptionChecker.expectThrowsNoException(() -> alterTable(alterStmt3));
        // 54th previous partition size set 53, check back to back logic work
        partitions.get(53).getDistributionInfo().setBucketNum(53);
        Env.getCurrentEnv().getDynamicPartitionScheduler().executeDynamicPartition(tempDynamicPartitionTableInfo, false);

        partitions = Lists.newArrayList(table.getAllPartitions());
        partitions.sort(Comparator.comparing(Partition::getId));
        Assert.assertEquals(55, partitions.size());
        // due to partition size eq 0, use previous partition's(54th) bucket num
        Assert.assertEquals(53, partitions.get(partitions.size() - 1).getDistributionInfo().getBucketNum());
    }

    @Test
    public void testTimeStampTzDynamicPartition() throws Exception {
        // Set session timezone different from UTC to verify
        // the scheduler uses UTC for both naming and partition boundaries.
        String originalTimeZone = connectContext.getSessionVariable().getTimeZone();
        try {
            connectContext.getSessionVariable().setTimeZone("America/Chicago");

            String createOlapTblStmt = "CREATE TABLE test.`timestamptz_dynamic_partition` (\n"
                    + "  `k1` TIMESTAMPTZ NULL COMMENT \"\",\n"
                    + "  `k2` int NULL COMMENT \"\"\n"
                    + ") ENGINE=OLAP\n"
                    + "DUPLICATE KEY(`k1`, `k2`)\n"
                    + "PARTITION BY RANGE(`k1`)\n"
                    + "()\n"
                    + "DISTRIBUTED BY HASH(`k2`) BUCKETS 3\n"
                    + "PROPERTIES (\n"
                    + "\"replication_num\" = \"1\",\n"
                    + "\"dynamic_partition.enable\" = \"true\",\n"
                    + "\"dynamic_partition.start\" = \"-3\",\n"
                    + "\"dynamic_partition.end\" = \"3\",\n"
                    + "\"dynamic_partition.create_history_partition\" = \"true\",\n"
                    + "\"dynamic_partition.time_unit\" = \"day\",\n"
                    + "\"dynamic_partition.prefix\" = \"p\",\n"
                    + "\"dynamic_partition.buckets\" = \"1\",\n"
                    + "\"dynamic_partition.time_zone\" = \"Asia/Shanghai\"\n"
                    + ");";
            createTable(createOlapTblStmt);

            Database db = Env.getCurrentInternalCatalog().getDbOrAnalysisException("test");
            OlapTable table = (OlapTable) db.getTableOrAnalysisException("timestamptz_dynamic_partition");
            Assert.assertTrue(table.dynamicPartitionExists());

            // Execute dynamic partition scheduling
            Env.getCurrentEnv().getDynamicPartitionScheduler()
                    .executeDynamicPartitionFirstTime(db.getId(), table.getId());

            // Verify total partitions (7 = start(-3) to end(3), inclusive)
            int partitionCount = table.getPartitionNames().size();
            Assert.assertEquals(7, partitionCount);

            // Verify partition names use configured timezone, boundaries are UTC
            RangePartitionInfo partitionInfo = (RangePartitionInfo) table.getPartitionInfo();
            for (Map.Entry<Long, PartitionItem> entry : partitionInfo.getIdToItem(false).entrySet()) {
                RangePartitionItem item = (RangePartitionItem) entry.getValue();

                // Verify the partition name is clean
                String partitionName = table.getPartition(entry.getKey()).getName();
                Assert.assertTrue("Partition name should start with 'p': " + partitionName,
                        partitionName.startsWith("p"));
                Assert.assertEquals("Partition name should be exactly 9 chars (p + yyyyMMdd): " + partitionName,
                        9, partitionName.length());

                // Verify the range endpoints are valid and correctly ordered
                Range<PartitionKey> range = item.getItems();
                PartitionKey lower = range.lowerEndpoint();
                PartitionKey upper = range.upperEndpoint();
                Assert.assertTrue("lower must be < upper: " + range,
                        lower.compareTo(upper) < 0);

                // Verify partition keys are UTC timestamps (with +00:00 suffix)
                List<LiteralExpr> lowerKeys = lower.getKeys();
                Assert.assertEquals(1, lowerKeys.size());
                String lowerStr = lowerKeys.get(0).getStringValue();
                Assert.assertTrue("Lower key must be UTC with +00:00 suffix: " + lowerStr,
                        lowerStr.contains("+00:00"));

                List<LiteralExpr> upperKeys = upper.getKeys();
                Assert.assertEquals(1, upperKeys.size());
                String upperStr = upperKeys.get(0).getStringValue();
                Assert.assertTrue("Upper key must be UTC with +00:00 suffix: " + upperStr,
                        upperStr.contains("+00:00"));

                // Partition boundaries must be at UTC midnight (hour=00)
                // regardless of time_zone.
                String lowerHour = lowerStr.substring(11, 13);
                Assert.assertEquals("Lower bound must be UTC midnight (00): " + lowerStr,
                        "00", lowerHour);
                String upperHour = upperStr.substring(11, 13);
                Assert.assertEquals("Upper bound must be UTC midnight (00): " + upperStr,
                        "00", upperHour);
            }

            // Identify the current partition (idx=0) by its stored range
            // rather than sampling ZonedDateTime.now() after scheduling,
            // which can tick and produce a name that happens to match an
            // adjacent historical partition even when idx=0 is misnamed.
            List<Map.Entry<Long, PartitionItem>> sorted = Lists.newArrayList(
                    partitionInfo.getIdToItem(false).entrySet());
            sorted.sort((a, b) -> {
                RangePartitionItem ai = (RangePartitionItem) a.getValue();
                RangePartitionItem bi = (RangePartitionItem) b.getValue();
                return ai.getItems().lowerEndpoint().compareTo(bi.getItems().lowerEndpoint());
            });
            Assert.assertEquals(7, sorted.size());
            // idx=0 is the 4th partition (index 3) for start=-3,end=3.
            RangePartitionItem currentItem = (RangePartitionItem) sorted.get(3).getValue();
            String currentLowerStr = currentItem.getItems().lowerEndpoint().getKeys().get(0)
                    .getStringValue();
            ZonedDateTime currentLower = ZonedDateTime.parse(currentLowerStr,
                    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssXXX"));
            String expectedCurrentName = "p"
                    + DateTimeFormatter.ofPattern("yyyyMMdd").format(currentLower);
            String actualCurrentName = table.getPartition(sorted.get(3).getKey()).getName();
            Assert.assertEquals("Current partition (idx=0) name must match its UTC lower bound",
                    expectedCurrentName, actualCurrentName);
            Assert.assertEquals("Current partition lower bound must be UTC midnight",
                    "00", currentLowerStr.substring(11, 13));

            for (Partition partition : table.getPartitions()) {
                RangePartitionItem item = (RangePartitionItem) partitionInfo.getItem(partition.getId());
                Assert.assertNotNull("Each partition should have a range item", item);
            }
        } finally {
            connectContext.getSessionVariable().setTimeZone(originalTimeZone);
        }
    }

    @Test
    public void testTimeStampTzDynamicPartitionWeekUnit() throws Exception {
        String originalTimeZone = connectContext.getSessionVariable().getTimeZone();
        try {
            // Session TZ different from UTC to verify the scheduler
            // uses UTC for both naming and boundaries.
            connectContext.getSessionVariable().setTimeZone("Europe/London");

            String createOlapTblStmt = "CREATE TABLE test.`timestamptz_dynamic_week` (\n"
                    + "  `k1` TIMESTAMPTZ NULL COMMENT \"\",\n"
                    + "  `k2` int NULL COMMENT \"\"\n"
                    + ") ENGINE=OLAP\n"
                    + "DUPLICATE KEY(`k1`, `k2`)\n"
                    + "PARTITION BY RANGE(`k1`)\n"
                    + "()\n"
                    + "DISTRIBUTED BY HASH(`k2`) BUCKETS 3\n"
                    + "PROPERTIES (\n"
                    + "\"replication_num\" = \"1\",\n"
                    + "\"dynamic_partition.enable\" = \"true\",\n"
                    + "\"dynamic_partition.start\" = \"-3\",\n"
                    + "\"dynamic_partition.end\" = \"3\",\n"
                    + "\"dynamic_partition.create_history_partition\" = \"true\",\n"
                    + "\"dynamic_partition.time_unit\" = \"week\",\n"
                    + "\"dynamic_partition.prefix\" = \"p\",\n"
                    + "\"dynamic_partition.buckets\" = \"1\",\n"
                    + "\"dynamic_partition.time_zone\" = \"Asia/Tokyo\"\n"
                    + ");";
            createTable(createOlapTblStmt);

            Database db = Env.getCurrentInternalCatalog().getDbOrAnalysisException("test");
            OlapTable table = (OlapTable) db.getTableOrAnalysisException("timestamptz_dynamic_week");
            Assert.assertTrue(table.dynamicPartitionExists());

            Env.getCurrentEnv().getDynamicPartitionScheduler()
                    .executeDynamicPartitionFirstTime(db.getId(), table.getId());

            int partitionCount = table.getPartitionNames().size();
            Assert.assertEquals(7, partitionCount);

            // Verify partition boundaries are UTC midnight and names use
            // configured timezone (Asia/Tokyo).
            RangePartitionInfo partitionInfo = (RangePartitionInfo) table.getPartitionInfo();
            for (Map.Entry<Long, PartitionItem> entry : partitionInfo.getIdToItem(false).entrySet()) {
                RangePartitionItem item = (RangePartitionItem) entry.getValue();
                String partitionName = table.getPartition(entry.getKey()).getName();
                Assert.assertTrue("Partition name should start with 'p': " + partitionName,
                        partitionName.startsWith("p"));
                // Week partition name should be like "p2026_26" (year_week)
                Assert.assertFalse("Partition name must not contain timezone: " + partitionName,
                        partitionName.contains("Asia") || partitionName.contains("Tokyo"));

                // Verify range validity
                Range<PartitionKey> range = item.getItems();
                Assert.assertTrue("lower must be < upper",
                        range.lowerEndpoint().compareTo(range.upperEndpoint()) < 0);

                // Partition boundaries must be at UTC midnight (hour=00)
                // regardless of time_zone.
                List<LiteralExpr> lowerKeys = range.lowerEndpoint().getKeys();
                Assert.assertEquals(1, lowerKeys.size());
                String lowerStr = lowerKeys.get(0).getStringValue();
                Assert.assertTrue("Lower key must be UTC with +00:00 suffix: " + lowerStr,
                        lowerStr.contains("+00:00"));

                List<LiteralExpr> upperKeys = range.upperEndpoint().getKeys();
                Assert.assertEquals(1, upperKeys.size());
                String upperStr = upperKeys.get(0).getStringValue();
                Assert.assertTrue("Upper key must be UTC with +00:00 suffix: " + upperStr,
                        upperStr.contains("+00:00"));

                // UTC midnight (00:00), regardless of time_zone.
                String lowerHour = lowerStr.substring(11, 13);
                Assert.assertEquals("Lower bound must be UTC midnight (00): " + lowerStr,
                        "00", lowerHour);
                String upperHour = upperStr.substring(11, 13);
                Assert.assertEquals("Upper bound must be UTC midnight (00): " + upperStr,
                        "00", upperHour);
            }

            // Identify the current partition (idx=0) by its stored range.
            List<Map.Entry<Long, PartitionItem>> sorted = Lists.newArrayList(
                    partitionInfo.getIdToItem(false).entrySet());
            sorted.sort((a, b) -> {
                RangePartitionItem ai = (RangePartitionItem) a.getValue();
                RangePartitionItem bi = (RangePartitionItem) b.getValue();
                return ai.getItems().lowerEndpoint().compareTo(bi.getItems().lowerEndpoint());
            });
            Assert.assertEquals(7, sorted.size());
            RangePartitionItem currentItem = (RangePartitionItem) sorted.get(3).getValue();
            String currentLowerStr = currentItem.getItems().lowerEndpoint().getKeys().get(0)
                    .getStringValue();
            ZonedDateTime currentLower = ZonedDateTime.parse(currentLowerStr,
                    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssXXX"));
            // Compute the expected week name from the stored lower bound
            // using the same utility path the scheduler uses.
            DynamicPartitionProperty prop = table.getTableProperty().getDynamicPartitionProperty();
            String partFormat = DynamicPartitionUtil.getPartitionFormat(
                    ((RangePartitionInfo) table.getPartitionInfo()).getPartitionColumns().get(0));
            TimeZone utcTz = TimeZone.getTimeZone("UTC");
            String border = DynamicPartitionUtil.getPartitionRangeString(
                    prop, currentLower, 0, partFormat);
            String expectedWeekName = "p" + DynamicPartitionUtil.getFormattedPartitionName(
                    utcTz, border, prop.getTimeUnit());
            String actualCurrentName = table.getPartition(sorted.get(3).getKey()).getName();
            Assert.assertEquals("Current partition (idx=0) week name must match its UTC lower bound",
                    expectedWeekName, actualCurrentName);
        } finally {
            connectContext.getSessionVariable().setTimeZone(originalTimeZone);
        }
    }

    @Test
    public void testTimeStampTzDynamicPartitionHourUnit() throws Exception {
        String originalTimeZone = connectContext.getSessionVariable().getTimeZone();
        try {
            // Session TZ different from UTC to verify the scheduler
            // uses UTC for both naming and boundaries.  Use Asia/Kathmandu
            // (UTC+05:45) — a fractional-hour offset — so that a configured-
            // timezone midnight floor would produce non-zero minutes (e.g.
            // 2026-07-20 00:00:00+05:45 → 2026-07-19 18:15:00+00:00).
            // Asserting that every bound has minute=second=00 proves the
            // boundaries are generated at whole UTC hours, not merely
            // integral-hour-shifted configured-timezone midnights.
            connectContext.getSessionVariable().setTimeZone("America/Chicago");

            String createOlapTblStmt = "CREATE TABLE test.`timestamptz_dynamic_hour` (\n"
                    + "  `k1` TIMESTAMPTZ NULL COMMENT \"\",\n"
                    + "  `k2` int NULL COMMENT \"\"\n"
                    + ") ENGINE=OLAP\n"
                    + "DUPLICATE KEY(`k1`, `k2`)\n"
                    + "PARTITION BY RANGE(`k1`)\n"
                    + "()\n"
                    + "DISTRIBUTED BY HASH(`k2`) BUCKETS 3\n"
                    + "PROPERTIES (\n"
                    + "\"replication_num\" = \"1\",\n"
                    + "\"dynamic_partition.enable\" = \"true\",\n"
                    + "\"dynamic_partition.start\" = \"-3\",\n"
                    + "\"dynamic_partition.end\" = \"3\",\n"
                    + "\"dynamic_partition.create_history_partition\" = \"true\",\n"
                    + "\"dynamic_partition.time_unit\" = \"hour\",\n"
                    + "\"dynamic_partition.prefix\" = \"p\",\n"
                    + "\"dynamic_partition.buckets\" = \"1\",\n"
                    + "\"dynamic_partition.time_zone\" = \"Asia/Kathmandu\"\n"
                    + ");";
            createTable(createOlapTblStmt);

            Database db = Env.getCurrentInternalCatalog().getDbOrAnalysisException("test");
            OlapTable table = (OlapTable) db.getTableOrAnalysisException("timestamptz_dynamic_hour");
            Assert.assertTrue(table.dynamicPartitionExists());

            Env.getCurrentEnv().getDynamicPartitionScheduler()
                    .executeDynamicPartitionFirstTime(db.getId(), table.getId());

            int partitionCount = table.getPartitionNames().size();
            Assert.assertEquals(7, partitionCount);

            // Hour partition boundaries must be at whole UTC hours
            // regardless of time_zone. Names use configured timezone.
            RangePartitionInfo partitionInfo = (RangePartitionInfo) table.getPartitionInfo();
            List<Map.Entry<Long, PartitionItem>> sortedEntries = Lists.newArrayList(
                    partitionInfo.getIdToItem(false).entrySet());
            sortedEntries.sort((a, b) -> {
                RangePartitionItem ai = (RangePartitionItem) a.getValue();
                RangePartitionItem bi = (RangePartitionItem) b.getValue();
                return ai.getItems().lowerEndpoint().compareTo(bi.getItems().lowerEndpoint());
            });

            ZonedDateTime prevLower = null;
            ZonedDateTime prevUpper = null;
            for (Map.Entry<Long, PartitionItem> entry : sortedEntries) {
                RangePartitionItem item = (RangePartitionItem) entry.getValue();
                String partitionName = table.getPartition(entry.getKey()).getName();
                Assert.assertTrue("Partition name should start with 'p': " + partitionName,
                        partitionName.startsWith("p"));
                // Hour partition names: p + yyyyMMddHH → length 11 (p + 10 digits)
                Assert.assertEquals("Hour partition name length: " + partitionName,
                        11, partitionName.length());

                // Verify range validity
                Range<PartitionKey> range = item.getItems();
                Assert.assertTrue("lower must be < upper",
                        range.lowerEndpoint().compareTo(range.upperEndpoint()) < 0);

                List<LiteralExpr> lowerKeys = range.lowerEndpoint().getKeys();
                Assert.assertEquals(1, lowerKeys.size());
                String lowerStr = lowerKeys.get(0).getStringValue();
                Assert.assertTrue("Lower key must have +00:00 suffix: " + lowerStr,
                        lowerStr.contains("+00:00"));

                List<LiteralExpr> upperKeys = range.upperEndpoint().getKeys();
                Assert.assertEquals(1, upperKeys.size());
                String upperStr = upperKeys.get(0).getStringValue();
                Assert.assertTrue("Upper key must have +00:00 suffix: " + upperStr,
                        upperStr.contains("+00:00"));

                // Partition boundaries must be at whole UTC hours (minute=second=00).
                // A configured timezone with a fractional offset (Asia/Kathmandu,
                // UTC+05:45) would produce boundaries with non-zero minutes if the
                // old configured-timezone flooring were used instead of UTC-first.
                String lowerMinutes = lowerStr.substring(14, 16);
                String lowerSeconds = lowerStr.substring(17, 19);
                Assert.assertEquals("Lower bound minutes must be 00: " + lowerStr,
                        "00", lowerMinutes);
                Assert.assertEquals("Lower bound seconds must be 00: " + lowerStr,
                        "00", lowerSeconds);
                String upperMinutes = upperStr.substring(14, 16);
                String upperSeconds = upperStr.substring(17, 19);
                Assert.assertEquals("Upper bound minutes must be 00: " + upperStr,
                        "00", upperMinutes);
                Assert.assertEquals("Upper bound seconds must be 00: " + upperStr,
                        "00", upperSeconds);

                // Verify adjacency using full timestamps (handles midnight crossing)
                if (prevLower != null) {
                    ZonedDateTime expectedNext = prevLower.plusHours(1);
                    ZonedDateTime actual = ZonedDateTime.parse(lowerStr,
                            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssXXX"));
                    Assert.assertEquals("Adjacent partitions' lower bounds must differ by 1 hour",
                            expectedNext, actual);
                    ZonedDateTime expectedUpper = prevUpper.plusHours(1);
                    ZonedDateTime actualUpper = ZonedDateTime.parse(upperStr,
                            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssXXX"));
                    Assert.assertEquals("Adjacent partitions' upper bounds must differ by 1 hour",
                            expectedUpper, actualUpper);
                }
                prevLower = ZonedDateTime.parse(lowerStr,
                        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssXXX"));
                prevUpper = ZonedDateTime.parse(upperStr,
                        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssXXX"));
            }

            // Identify the current partition (idx=0) by its stored range.
            // start=-3,end=3 → idx=0 is at index 3.
            RangePartitionItem currentItem = (RangePartitionItem) sortedEntries.get(3).getValue();
            String currentLowerStr = currentItem.getItems().lowerEndpoint().getKeys().get(0)
                    .getStringValue();
            ZonedDateTime currentLower = ZonedDateTime.parse(currentLowerStr,
                    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssXXX"));
            String expectedCurrentName = "p"
                    + DateTimeFormatter.ofPattern("yyyyMMddHH").format(currentLower);
            String actualCurrentName = table.getPartition(sortedEntries.get(3).getKey()).getName();
            Assert.assertEquals("Current partition (idx=0) hour name must match its UTC lower bound",
                    expectedCurrentName, actualCurrentName);
            // With a fractional-offset timezone, only the UTC-first approach
            // guarantees minute=second=00 on every bound.
            Assert.assertEquals("Current partition lower bound must end :00:00: " + currentLowerStr,
                    "00", currentLowerStr.substring(14, 16)); // minutes
            Assert.assertEquals("Current partition lower bound must end :00:00: " + currentLowerStr,
                    "00", currentLowerStr.substring(17, 19)); // seconds
        } finally {
            connectContext.getSessionVariable().setTimeZone(originalTimeZone);
        }
    }

    @Test
    public void testTimeStampTzDynamicPartitionDropCutoffAligned() throws Exception {
        // With time_zone = "Asia/Shanghai", the old drop-path code computed
        // the reserved range lower bound at the configured timezone's midnight
        // (16:00 UTC) instead of UTC midnight (00:00). This caused partitions
        // just before UTC midnight to intersect the shifted reserved range and
        // not be dropped. Verify the drop cutoff now aligns with the add path.
        String originalTimeZone = connectContext.getSessionVariable().getTimeZone();
        try {
            connectContext.getSessionVariable().setTimeZone("America/Chicago");

            String createOlapTblStmt = "CREATE TABLE test.`tstz_drop_cutoff` (\n"
                    + "  `k1` TIMESTAMPTZ NULL COMMENT \"\",\n"
                    + "  `k2` int NULL COMMENT \"\"\n"
                    + ") ENGINE=OLAP\n"
                    + "DUPLICATE KEY(`k1`, `k2`)\n"
                    + "PARTITION BY RANGE(`k1`)\n"
                    + "()\n"
                    + "DISTRIBUTED BY HASH(`k2`) BUCKETS 3\n"
                    + "PROPERTIES (\n"
                    + "\"replication_num\" = \"1\",\n"
                    + "\"dynamic_partition.enable\" = \"true\",\n"
                    + "\"dynamic_partition.start\" = \"-1\",\n"
                    + "\"dynamic_partition.end\" = \"1\",\n"
                    + "\"dynamic_partition.create_history_partition\" = \"false\",\n"
                    + "\"dynamic_partition.time_unit\" = \"day\",\n"
                    + "\"dynamic_partition.prefix\" = \"p\",\n"
                    + "\"dynamic_partition.buckets\" = \"1\",\n"
                    + "\"dynamic_partition.time_zone\" = \"Asia/Shanghai\"\n"
                    + ");";
            createTable(createOlapTblStmt);

            Database db = Env.getCurrentInternalCatalog().getDbOrAnalysisException("test");
            OlapTable table = (OlapTable) db.getTableOrAnalysisException("tstz_drop_cutoff");

            // Inject a fixed clock so the test is deterministic regardless of
            // wall-clock time.  Choose 2026-07-21 08:00:00Z when Asia/Shanghai
            // is at 16:00 (same calendar day as UTC).  At this instant:
            //
            //   UTC now = 2026-07-21 08:00Z
            //   start=-1 reserved lower (new, UTC)   = 2026-07-20 00:00Z
            //   start=-1 reserved lower (old, +08:00) = 2026-07-19 16:00Z
            //
            //   p_old = [2026-07-19 00:00Z, 2026-07-20 00:00Z)
            //
            //   New code:  reserved [2026-07-20 00:00Z, ∞) → no intersect → DROP
            //   Old code:  reserved [2026-07-19 16:00Z, ∞) → intersect  → KEEP
            //
            // The assertion below therefore always fails on the old
            // implementation, not just for 16 hours of the day.
            ZonedDateTime fixedNow = ZonedDateTime.of(
                    2026, 7, 21, 8, 0, 0, 0, ZoneOffset.UTC);
            DynamicPartitionScheduler.testNow = fixedNow;

            DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            String oldLower = fixedNow.minusDays(2).withHour(0).withMinute(0)
                    .withSecond(0).withNano(0).format(fmt) + "+00:00";
            String oldUpper = fixedNow.minusDays(1).withHour(0).withMinute(0)
                    .withSecond(0).withNano(0).format(fmt) + "+00:00";
            alterTable("ALTER TABLE test.tstz_drop_cutoff SET "
                    + "('dynamic_partition.enable' = 'false')");
            alterTable("ALTER TABLE test.tstz_drop_cutoff ADD PARTITION p_old VALUES "
                    + "[('" + oldLower + "'), ('" + oldUpper + "'))");
            Assert.assertTrue("p_old should be added", table.getPartitionNames().contains("p_old"));
            alterTable("ALTER TABLE test.tstz_drop_cutoff SET "
                    + "('dynamic_partition.enable' = 'true')");

            Env.getCurrentEnv().getDynamicPartitionScheduler()
                    .executeDynamicPartitionFirstTime(db.getId(), table.getId());

            // p_old must be dropped by the start=-1 cutoff.
            // With the fix, the cutoff is at UTC midnight of the previous day,
            // and p_old (two days ago) is entirely before it.
            Assert.assertFalse("p_old should be dropped — drop cutoff must be at UTC midnight,"
                    + " not timezone-offset midnight",
                    table.getPartitionNames().contains("p_old"));

            // idx=0 (today) and idx=1 (tomorrow) should remain (start=-1, end=1,
            // create_history_partition=false only creates idx>=0)
            Assert.assertEquals("Should have idx=0 and idx=1 partitions after drop",
                    2, table.getPartitionNames().size());

            // Verify remaining partitions have UTC midnight boundaries
            RangePartitionInfo partitionInfo = (RangePartitionInfo) table.getPartitionInfo();
            for (Map.Entry<Long, PartitionItem> entry : partitionInfo.getIdToItem(false).entrySet()) {
                RangePartitionItem item = (RangePartitionItem) entry.getValue();
                List<LiteralExpr> lowerKeys = item.getItems().lowerEndpoint().getKeys();
                String lowerStr = lowerKeys.get(0).getStringValue();
                Assert.assertTrue("Lower key must be UTC: " + lowerStr,
                        lowerStr.contains("+00:00"));
                Assert.assertEquals("Boundary must be at UTC midnight: " + lowerStr,
                        "00", lowerStr.substring(11, 13));
            }
        } finally {
            DynamicPartitionScheduler.testNow = null;
            connectContext.getSessionVariable().setTimeZone(originalTimeZone);
        }
    }

    @Test
    public void testAutoPartitionRetentionTimestampTzCutoffNormalized() throws Exception {
        // The bug occurs on the scheduler thread where there is no ConnectContext:
        // DateUtils.getTimeZone() falls back to JVM default (e.g. Asia/Shanghai)
        // while TimestampTzLiteral.fromSessionTimeZone() falls back to UTC.
        // This creates a mismatch where currentTimeStr is formatted in the JVM
        // timezone but parsed as UTC, shifting the cutoff.
        String originalSessionTz = connectContext.getSessionVariable().getTimeZone();
        ConnectContext savedCtx = ConnectContext.get();
        TimeZone originalJvmTz = TimeZone.getDefault();
        try {
            // Create table and add partitions — use session TZ for table ops.
            connectContext.getSessionVariable().setTimeZone("Asia/Shanghai");

            String createSql = "CREATE TABLE test.`auto_retention_tstz` (\n"
                    + "  `k1` TIMESTAMPTZ NOT NULL\n"
                    + ") ENGINE=OLAP\n"
                    + "DUPLICATE KEY(`k1`)\n"
                    + "AUTO PARTITION BY RANGE (date_trunc(k1, 'day')) ()\n"
                    + "DISTRIBUTED BY HASH(`k1`) BUCKETS 1\n"
                    + "PROPERTIES (\n"
                    + "\"replication_num\" = \"1\",\n"
                    + "\"partition.retention_count\" = \"1\"\n"
                    + ");";
            createTable(createSql);

            Database db = Env.getCurrentInternalCatalog().getDbOrAnalysisException("test");
            OlapTable tbl = (OlapTable) db.getTableOrAnalysisException("auto_retention_tstz");

            // Definitively historical partition (year 2000) — always history.
            alterTable("ALTER TABLE test.auto_retention_tstz ADD PARTITION p_old VALUES "
                    + "[('2000-01-01 00:00:00+00:00'), ('2000-01-02 00:00:00+00:00'))");
            // Another historical partition (year 2020) — always history.
            alterTable("ALTER TABLE test.auto_retention_tstz ADD PARTITION p_mid VALUES "
                    + "[('2020-01-01 00:00:00+00:00'), ('2020-01-02 00:00:00+00:00'))");

            // Recent partition: upper bound is 4h in the future (UTC).
            DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            ZonedDateTime utcNow = ZonedDateTime.now(ZoneOffset.UTC);
            String recentLower = utcNow.minusHours(1).format(fmt) + "+00:00";
            String recentUpper = utcNow.plusHours(4).format(fmt) + "+00:00";
            alterTable("ALTER TABLE test.auto_retention_tstz ADD PARTITION p_recent VALUES "
                    + "[('" + recentLower + "'), ('" + recentUpper + "'))");
            Assert.assertEquals(3, tbl.getPartitionNames().size());

            // Simulate the scheduler thread: remove ConnectContext and set
            // JVM default to a non-UTC zone. DateUtils.getTimeZone() now
            // returns Asia/Shanghai while TimestampTzLiteral parsing defaults
            // to UTC — the exact mismatch this fix protects against.
            ConnectContext.remove();
            TimeZone.setDefault(TimeZone.getTimeZone("Asia/Shanghai"));
            try {
                Env.getCurrentEnv().getDynamicPartitionScheduler()
                        .executeDynamicPartitionFirstTime(db.getId(), tbl.getId());
            } finally {
                TimeZone.setDefault(originalJvmTz);
                if (savedCtx != null) {
                    savedCtx.setThreadLocalInfo();
                }
            }

            // With the fix, cutoff is UTC-normalized: p_recent (upper bound
            // ahead of UTC now) is not history → kept.
            // p_old (2000, oldest history) → dropped by retention_count=1.
            // p_mid (2020, latest history) → kept.
            // Total: 2 partitions survive.
            Assert.assertEquals("After retention, 2 partitions should remain", 2,
                    tbl.getPartitionNames().size());
            Assert.assertTrue("p_mid should be kept as the latest history partition",
                    tbl.getPartitionNames().contains("p_mid"));
            Assert.assertTrue("p_recent should survive (not history)",
                    tbl.getPartitionNames().contains("p_recent"));
        } finally {
            TimeZone.setDefault(originalJvmTz);
            connectContext.getSessionVariable().setTimeZone(originalSessionTz);
        }
    }

    @Test
    public void testTimeStampTzGetHistoricalPartitionsRangeBased() throws Exception {
        // Verify that getHistoricalPartitions correctly identifies the current
        // partition by range lower bound (currentUtcBorder) rather than by name.
        // When the configured timezone (e.g. Asia/Shanghai) is a day ahead of UTC,
        // the scheduler computes nowPartitionName from the TZ date (e.g. "p20260707")
        // while currentUtcBorder is from the UTC date ("2026-07-06 00:00:00+00:00").
        // Name-based comparison would fail to exclude the real current partition
        // (p20260706), treating it as historical. Range-based comparison correctly
        // excludes it by matching the stored partition lower bound.

        String createSql = "CREATE TABLE test.`tstz_hist_parts` (\n"
                + "  `k1` TIMESTAMPTZ NULL COMMENT \"\",\n"
                + "  `k2` int NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`k1`, `k2`)\n"
                + "PARTITION BY RANGE(`k1`)\n"
                + "()\n"
                + "DISTRIBUTED BY HASH(`k2`) BUCKETS 3\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\",\n"
                + "\"dynamic_partition.enable\" = \"true\",\n"
                + "\"dynamic_partition.start\" = \"-3\",\n"
                + "\"dynamic_partition.end\" = \"3\",\n"
                + "\"dynamic_partition.create_history_partition\" = \"true\",\n"
                + "\"dynamic_partition.time_unit\" = \"day\",\n"
                + "\"dynamic_partition.prefix\" = \"p\",\n"
                + "\"dynamic_partition.buckets\" = \"1\",\n"
                + "\"dynamic_partition.time_zone\" = \"Asia/Shanghai\"\n"
                + ");";
        createTable(createSql);

        Database db = Env.getCurrentInternalCatalog().getDbOrAnalysisException("test");
        OlapTable table = (OlapTable) db.getTableOrAnalysisException("tstz_hist_parts");

        Env.getCurrentEnv().getDynamicPartitionScheduler()
                .executeDynamicPartitionFirstTime(db.getId(), table.getId());

        int totalPartitions = table.getPartitionNames().size();
        Assert.assertEquals(7, totalPartitions);

        RangePartitionInfo info = (RangePartitionInfo) table.getPartitionInfo();
        DynamicPartitionProperty prop = table.getTableProperty().getDynamicPartitionProperty();
        String partitionFormat = DynamicPartitionUtil.getPartitionFormat(
                info.getPartitionColumns().get(0));
        ZonedDateTime utcNow = ZonedDateTime.now(ZoneOffset.UTC);
        // getPartitionRangeString appends +00:00 when current is UTC-based.
        String currentUtcBorder = DynamicPartitionUtil.getPartitionRangeString(prop, utcNow, 0, partitionFormat);

        // Use a deliberately mismatched nowPartitionName to simulate the scenario
        // where the configured TZ and UTC disagree on the calendar date.
        // Name-based comparison would NOT exclude any partition with this name.
        String wrongNowPartitionName = "p_nonexistent";

        // 1. Without currentUtcBorder: name-based cannot identify the current partition.
        List<Partition> historicalNoUtc = DynamicPartitionScheduler.getHistoricalPartitions(
                table, wrongNowPartitionName, null);
        boolean currentFoundByName = false;
        for (Partition p : historicalNoUtc) {
            RangePartitionItem item = (RangePartitionItem) info.getItem(p.getId());
            String lowerBound = item.getItems().lowerEndpoint().getKeys().get(0).getStringValue();
            if (lowerBound.equals(currentUtcBorder)) {
                currentFoundByName = true;
                break;
            }
        }
        Assert.assertTrue("Name-based should fail to exclude current partition: "
                + "nowPartitionName='" + wrongNowPartitionName + "' does not match any partition",
                currentFoundByName);

        // 2. With currentUtcBorder: range-based correctly excludes the current partition.
        List<Partition> historicalWithUtc = DynamicPartitionScheduler.getHistoricalPartitions(
                table, wrongNowPartitionName, currentUtcBorder);
        boolean currentFoundByRange = false;
        for (Partition p : historicalWithUtc) {
            RangePartitionItem item = (RangePartitionItem) info.getItem(p.getId());
            String lowerBound = item.getItems().lowerEndpoint().getKeys().get(0).getStringValue();
            if (lowerBound.equals(currentUtcBorder)) {
                currentFoundByRange = true;
                break;
            }
        }
        Assert.assertFalse("Range-based should correctly exclude the current partition",
                currentFoundByRange);
        Assert.assertEquals("Should exclude exactly the current partition",
                totalPartitions - 1, historicalWithUtc.size());
    }

    @Test
    public void testTimeStampTzDynamicPartitionMonthUnit() throws Exception {
        // Verify TIMESTAMPTZ dynamic partition with MONTH unit:
        // boundaries are at UTC midnight (day=01 00:00:00+00:00),
        // names use the configured timezone's calendar month.
        String originalTimeZone = connectContext.getSessionVariable().getTimeZone();
        try {
            connectContext.getSessionVariable().setTimeZone("America/Chicago");

            String createOlapTblStmt = "CREATE TABLE test.`timestamptz_dynamic_month` (\n"
                    + "  `k1` TIMESTAMPTZ NULL COMMENT \"\",\n"
                    + "  `k2` int NULL COMMENT \"\"\n"
                    + ") ENGINE=OLAP\n"
                    + "DUPLICATE KEY(`k1`, `k2`)\n"
                    + "PARTITION BY RANGE(`k1`)\n"
                    + "()\n"
                    + "DISTRIBUTED BY HASH(`k2`) BUCKETS 3\n"
                    + "PROPERTIES (\n"
                    + "\"replication_num\" = \"1\",\n"
                    + "\"dynamic_partition.enable\" = \"true\",\n"
                    + "\"dynamic_partition.start\" = \"-3\",\n"
                    + "\"dynamic_partition.end\" = \"3\",\n"
                    + "\"dynamic_partition.create_history_partition\" = \"true\",\n"
                    + "\"dynamic_partition.time_unit\" = \"month\",\n"
                    + "\"dynamic_partition.prefix\" = \"p\",\n"
                    + "\"dynamic_partition.buckets\" = \"1\",\n"
                    + "\"dynamic_partition.time_zone\" = \"Asia/Shanghai\"\n"
                    + ");";
            createTable(createOlapTblStmt);

            Database db = Env.getCurrentInternalCatalog().getDbOrAnalysisException("test");
            OlapTable table = (OlapTable) db.getTableOrAnalysisException("timestamptz_dynamic_month");
            Assert.assertTrue(table.dynamicPartitionExists());

            Env.getCurrentEnv().getDynamicPartitionScheduler()
                    .executeDynamicPartitionFirstTime(db.getId(), table.getId());

            int partitionCount = table.getPartitionNames().size();
            Assert.assertEquals(7, partitionCount);

            // Verify partition boundaries are UTC midnight and names use
            // configured timezone (Asia/Shanghai).
            RangePartitionInfo partitionInfo = (RangePartitionInfo) table.getPartitionInfo();
            for (Map.Entry<Long, PartitionItem> entry : partitionInfo.getIdToItem(false).entrySet()) {
                RangePartitionItem item = (RangePartitionItem) entry.getValue();
                String partitionName = table.getPartition(entry.getKey()).getName();
                Assert.assertTrue("Partition name should start with 'p': " + partitionName,
                        partitionName.startsWith("p"));
                // Month partition names: p + yyyyMM → length 7
                Assert.assertEquals("Month partition name length: " + partitionName,
                        7, partitionName.length());

                // Verify range validity
                Range<PartitionKey> range = item.getItems();
                Assert.assertTrue("lower must be < upper",
                        range.lowerEndpoint().compareTo(range.upperEndpoint()) < 0);

                // Partition boundaries must be UTC timestamps with +00:00 suffix.
                List<LiteralExpr> lowerKeys = range.lowerEndpoint().getKeys();
                Assert.assertEquals(1, lowerKeys.size());
                String lowerStr = lowerKeys.get(0).getStringValue();
                Assert.assertTrue("Lower key must be UTC with +00:00 suffix: " + lowerStr,
                        lowerStr.contains("+00:00"));

                List<LiteralExpr> upperKeys = range.upperEndpoint().getKeys();
                Assert.assertEquals(1, upperKeys.size());
                String upperStr = upperKeys.get(0).getStringValue();
                Assert.assertTrue("Upper key must be UTC with +00:00 suffix: " + upperStr,
                        upperStr.contains("+00:00"));

                // Partition boundaries must be at UTC midnight (hour=00)
                // regardless of time_zone. Day should be 01 (first of month).
                String lowerDay = lowerStr.substring(8, 10);
                Assert.assertEquals("Lower bound must be day 01 for month unit: " + lowerStr,
                        "01", lowerDay);
                String lowerHour = lowerStr.substring(11, 13);
                Assert.assertEquals("Lower bound must be UTC midnight (00): " + lowerStr,
                        "00", lowerHour);
                String upperHour = upperStr.substring(11, 13);
                Assert.assertEquals("Upper bound must be UTC midnight (00): " + upperStr,
                        "00", upperHour);
            }

            // Identify the current partition (idx=0) by its stored range.
            List<Map.Entry<Long, PartitionItem>> sorted = Lists.newArrayList(
                    partitionInfo.getIdToItem(false).entrySet());
            sorted.sort((a, b) -> {
                RangePartitionItem ai = (RangePartitionItem) a.getValue();
                RangePartitionItem bi = (RangePartitionItem) b.getValue();
                return ai.getItems().lowerEndpoint().compareTo(bi.getItems().lowerEndpoint());
            });
            Assert.assertEquals(7, sorted.size());
            RangePartitionItem currentItem = (RangePartitionItem) sorted.get(3).getValue();
            String currentLowerStr = currentItem.getItems().lowerEndpoint().getKeys().get(0)
                    .getStringValue();
            ZonedDateTime currentLower = ZonedDateTime.parse(currentLowerStr,
                    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssXXX"));
            String expectedCurrentName = "p"
                    + DateTimeFormatter.ofPattern("yyyyMM").format(currentLower);
            String actualCurrentName = table.getPartition(sorted.get(3).getKey()).getName();
            Assert.assertEquals("Current partition (idx=0) month name must match its UTC lower bound",
                    expectedCurrentName, actualCurrentName);
            Assert.assertEquals("Current partition lower bound must be UTC midnight",
                    "00", currentLowerStr.substring(11, 13));
            Assert.assertEquals("Current partition lower bound day must be 01",
                    "01", currentLowerStr.substring(8, 10));
        } finally {
            connectContext.getSessionVariable().setTimeZone(originalTimeZone);
        }
    }

    @Test
    public void testTimeStampTzDynamicPartitionYearUnit() throws Exception {
        // Verify TIMESTAMPTZ dynamic partition with YEAR unit:
        // boundaries are at UTC midnight (month=01, day=01 00:00:00+00:00),
        // names use the configured timezone's calendar year.
        String originalTimeZone = connectContext.getSessionVariable().getTimeZone();
        try {
            connectContext.getSessionVariable().setTimeZone("America/Chicago");

            String createOlapTblStmt = "CREATE TABLE test.`timestamptz_dynamic_year` (\n"
                    + "  `k1` TIMESTAMPTZ NULL COMMENT \"\",\n"
                    + "  `k2` int NULL COMMENT \"\"\n"
                    + ") ENGINE=OLAP\n"
                    + "DUPLICATE KEY(`k1`, `k2`)\n"
                    + "PARTITION BY RANGE(`k1`)\n"
                    + "()\n"
                    + "DISTRIBUTED BY HASH(`k2`) BUCKETS 3\n"
                    + "PROPERTIES (\n"
                    + "\"replication_num\" = \"1\",\n"
                    + "\"dynamic_partition.enable\" = \"true\",\n"
                    + "\"dynamic_partition.start\" = \"-3\",\n"
                    + "\"dynamic_partition.end\" = \"3\",\n"
                    + "\"dynamic_partition.create_history_partition\" = \"true\",\n"
                    + "\"dynamic_partition.time_unit\" = \"year\",\n"
                    + "\"dynamic_partition.prefix\" = \"p\",\n"
                    + "\"dynamic_partition.buckets\" = \"1\",\n"
                    + "\"dynamic_partition.time_zone\" = \"Asia/Shanghai\"\n"
                    + ");";
            createTable(createOlapTblStmt);

            Database db = Env.getCurrentInternalCatalog().getDbOrAnalysisException("test");
            OlapTable table = (OlapTable) db.getTableOrAnalysisException("timestamptz_dynamic_year");
            Assert.assertTrue(table.dynamicPartitionExists());

            Env.getCurrentEnv().getDynamicPartitionScheduler()
                    .executeDynamicPartitionFirstTime(db.getId(), table.getId());

            int partitionCount = table.getPartitionNames().size();
            Assert.assertEquals(7, partitionCount);

            // Verify partition boundaries are UTC midnight and names use
            // configured timezone (Asia/Shanghai).
            RangePartitionInfo partitionInfo = (RangePartitionInfo) table.getPartitionInfo();
            for (Map.Entry<Long, PartitionItem> entry : partitionInfo.getIdToItem(false).entrySet()) {
                RangePartitionItem item = (RangePartitionItem) entry.getValue();
                String partitionName = table.getPartition(entry.getKey()).getName();
                Assert.assertTrue("Partition name should start with 'p': " + partitionName,
                        partitionName.startsWith("p"));
                // Year partition names: p + yyyy → length 5
                Assert.assertEquals("Year partition name length: " + partitionName,
                        5, partitionName.length());

                // Verify range validity
                Range<PartitionKey> range = item.getItems();
                Assert.assertTrue("lower must be < upper",
                        range.lowerEndpoint().compareTo(range.upperEndpoint()) < 0);

                // Partition boundaries must be UTC timestamps with +00:00 suffix.
                List<LiteralExpr> lowerKeys = range.lowerEndpoint().getKeys();
                Assert.assertEquals(1, lowerKeys.size());
                String lowerStr = lowerKeys.get(0).getStringValue();
                Assert.assertTrue("Lower key must be UTC with +00:00 suffix: " + lowerStr,
                        lowerStr.contains("+00:00"));

                List<LiteralExpr> upperKeys = range.upperEndpoint().getKeys();
                Assert.assertEquals(1, upperKeys.size());
                String upperStr = upperKeys.get(0).getStringValue();
                Assert.assertTrue("Upper key must be UTC with +00:00 suffix: " + upperStr,
                        upperStr.contains("+00:00"));

                // Partition boundaries must be at UTC midnight (hour=00)
                // regardless of time_zone. Month should be 01, day should be 01.
                String lowerMonth = lowerStr.substring(5, 7);
                Assert.assertEquals("Lower bound must be month 01 for year unit: " + lowerStr,
                        "01", lowerMonth);
                String lowerDay = lowerStr.substring(8, 10);
                Assert.assertEquals("Lower bound must be day 01 for year unit: " + lowerStr,
                        "01", lowerDay);
                String lowerHour = lowerStr.substring(11, 13);
                Assert.assertEquals("Lower bound must be UTC midnight (00): " + lowerStr,
                        "00", lowerHour);
                String upperHour = upperStr.substring(11, 13);
                Assert.assertEquals("Upper bound must be UTC midnight (00): " + upperStr,
                        "00", upperHour);
            }

            // Identify the current partition (idx=0) by its stored range.
            List<Map.Entry<Long, PartitionItem>> sorted = Lists.newArrayList(
                    partitionInfo.getIdToItem(false).entrySet());
            sorted.sort((a, b) -> {
                RangePartitionItem ai = (RangePartitionItem) a.getValue();
                RangePartitionItem bi = (RangePartitionItem) b.getValue();
                return ai.getItems().lowerEndpoint().compareTo(bi.getItems().lowerEndpoint());
            });
            Assert.assertEquals(7, sorted.size());
            RangePartitionItem currentItem = (RangePartitionItem) sorted.get(3).getValue();
            String currentLowerStr = currentItem.getItems().lowerEndpoint().getKeys().get(0)
                    .getStringValue();
            ZonedDateTime currentLower = ZonedDateTime.parse(currentLowerStr,
                    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssXXX"));
            String expectedCurrentName = "p"
                    + DateTimeFormatter.ofPattern("yyyy").format(currentLower);
            String actualCurrentName = table.getPartition(sorted.get(3).getKey()).getName();
            Assert.assertEquals("Current partition (idx=0) year name must match its UTC lower bound",
                    expectedCurrentName, actualCurrentName);
            Assert.assertEquals("Current partition lower bound must be UTC midnight",
                    "00", currentLowerStr.substring(11, 13));
            Assert.assertEquals("Current partition lower bound month must be 01",
                    "01", currentLowerStr.substring(5, 7));
            Assert.assertEquals("Current partition lower bound day must be 01",
                    "01", currentLowerStr.substring(8, 10));
        } finally {
            connectContext.getSessionVariable().setTimeZone(originalTimeZone);
        }
    }

    @Test
    public void testTimeStampTzReservedHistoryPeriodsUtcAligned() throws Exception {
        // Verify that reserved_history_periods uses UTC-midnight boundaries
        // (via borderTimeZone from getDropPartitionOpForDynamic) consistently
        // with the main drop cutoff and add-partition boundaries.
        // Without the fix, getClosedRange() hardcoded the configured timezone
        // (e.g. Asia/Shanghai) for convertToUtcTimestamp(), shifting reserved
        // ranges by the UTC offset and causing mismatches with UTC-aligned
        // partition boundaries.
        String originalTimeZone = connectContext.getSessionVariable().getTimeZone();
        try {
            connectContext.getSessionVariable().setTimeZone("America/Chicago");

            String createSql = "CREATE TABLE test.`tstz_reserved_hist` (\n"
                    + "  `k1` TIMESTAMPTZ NULL COMMENT \"\",\n"
                    + "  `k2` int NULL COMMENT \"\"\n"
                    + ") ENGINE=OLAP\n"
                    + "DUPLICATE KEY(`k1`, `k2`)\n"
                    + "PARTITION BY RANGE(`k1`)\n"
                    + "()\n"
                    + "DISTRIBUTED BY HASH(`k2`) BUCKETS 3\n"
                    + "PROPERTIES (\n"
                    + "\"replication_num\" = \"1\",\n"
                    + "\"dynamic_partition.enable\" = \"true\",\n"
                    + "\"dynamic_partition.start\" = \"-3\",\n"
                    + "\"dynamic_partition.end\" = \"3\",\n"
                    + "\"dynamic_partition.create_history_partition\" = \"false\",\n"
                    + "\"dynamic_partition.time_unit\" = \"day\",\n"
                    + "\"dynamic_partition.prefix\" = \"p\",\n"
                    + "\"dynamic_partition.buckets\" = \"1\",\n"
                    + "\"dynamic_partition.time_zone\" = \"Asia/Shanghai\",\n"
                    + "\"dynamic_partition.reserved_history_periods\" = \"[2019-06-01,2020-08-01]\"\n"
                    + ");";
            createTable(createSql);

            Database db = Env.getCurrentInternalCatalog().getDbOrAnalysisException("test");
            OlapTable table = (OlapTable) db.getTableOrAnalysisException("tstz_reserved_hist");

            // Add partitions manually — must temporarily disable dynamic partition.
            alterTable("ALTER TABLE test.tstz_reserved_hist SET "
                    + "('dynamic_partition.enable' = 'false')");
            // p_202001: well inside both the fixed UTC range and the old
            // Asia/Shanghai-shifted range — weak test, passes either way.
            alterTable("ALTER TABLE test.tstz_reserved_hist ADD PARTITION p_202001 VALUES "
                    + "[('2020-01-01 00:00:00+00:00'), ('2020-02-01 00:00:00+00:00'))");
            // p_old: before both ranges — dropped by the start=-3 cutoff
            // regardless of the reserved-period interpretation.
            alterTable("ALTER TABLE test.tstz_reserved_hist ADD PARTITION p_old VALUES "
                    + "[('2018-01-01 00:00:00+00:00'), ('2018-02-01 00:00:00+00:00'))");

            // p_boundary: discriminating partition. Its lower bound
            // (2020-07-31 17:00 UTC) is AFTER the old shifted upper bound
            // (~2020-07-31 16:00 UTC) but BEFORE the corrected UTC upper
            // bound (2020-08-01 00:00 UTC). Only the fix keeps it.
            alterTable("ALTER TABLE test.tstz_reserved_hist ADD PARTITION p_boundary VALUES "
                    + "[('2020-07-31 17:00:00+00:00'), ('2020-07-31 18:00:00+00:00'))");

            Assert.assertTrue("p_202001 should exist before scheduling",
                    table.getPartitionNames().contains("p_202001"));
            Assert.assertTrue("p_old should exist before scheduling",
                    table.getPartitionNames().contains("p_old"));
            Assert.assertTrue("p_boundary should exist before scheduling",
                    table.getPartitionNames().contains("p_boundary"));

            // Re-enable dynamic partition and run the scheduler.
            alterTable("ALTER TABLE test.tstz_reserved_hist SET "
                    + "('dynamic_partition.enable' = 'true')");

            Env.getCurrentEnv().getDynamicPartitionScheduler()
                    .executeDynamicPartitionFirstTime(db.getId(), table.getId());

            // p_202001 falls within the reserved period in both old and new
            // interpretations — kept regardless.
            Assert.assertTrue("p_202001 should be kept",
                    table.getPartitionNames().contains("p_202001"));
            // p_old is before the start=-3 cutoff and outside the reserved
            // period — dropped regardless.
            Assert.assertFalse("p_old should be dropped",
                    table.getPartitionNames().contains("p_old"));
            // p_boundary is the discriminating case: only kept when the
            // reserved period is interpreted in UTC rather than shifted by
            // the configured timezone (Asia/Shanghai, UTC+8).
            Assert.assertTrue("p_boundary should be kept — reserved period is UTC-aligned,"
                    + " not shifted by the configured timezone",
                    table.getPartitionNames().contains("p_boundary"));
        } finally {
            connectContext.getSessionVariable().setTimeZone(originalTimeZone);
        }
    }

    @Test
    public void testTimeStampTzHotPartitionCooldownUtcAligned() throws Exception {
        // Verify that hot-partition cooldown times are UTC-midnight aligned
        // and not shifted by the configured timezone (America/Chicago, UTC-5).
        // Without the fix, setStorageMediumProperty() used nowTz (configured
        // timezone) which could place the cooldown boundary on a different
        // calendar day than the partition's UTC range.
        String originalTimeZone = connectContext.getSessionVariable().getTimeZone();
        try {
            connectContext.getSessionVariable().setTimeZone("Asia/Shanghai");
            changeBeDisk(TStorageMedium.SSD);

            String createSql = "CREATE TABLE test.`tstz_hot_part_cooldown` (\n"
                    + "  `k1` TIMESTAMPTZ NULL COMMENT \"\",\n"
                    + "  `k2` int NULL COMMENT \"\"\n"
                    + ") ENGINE=OLAP\n"
                    + "DUPLICATE KEY(`k1`, `k2`)\n"
                    + "PARTITION BY RANGE(`k1`)\n"
                    + "()\n"
                    + "DISTRIBUTED BY HASH(`k2`) BUCKETS 3\n"
                    + "PROPERTIES (\n"
                    + "\"replication_num\" = \"1\",\n"
                    + "\"dynamic_partition.enable\" = \"true\",\n"
                    + "\"dynamic_partition.start\" = \"-3\",\n"
                    + "\"dynamic_partition.end\" = \"3\",\n"
                    + "\"dynamic_partition.create_history_partition\" = \"true\",\n"
                    + "\"dynamic_partition.time_unit\" = \"day\",\n"
                    + "\"dynamic_partition.prefix\" = \"p\",\n"
                    + "\"dynamic_partition.buckets\" = \"1\",\n"
                    + "\"dynamic_partition.time_zone\" = \"America/Chicago\",\n"
                    + "\"dynamic_partition.hot_partition_num\" = \"1\"\n"
                    + ");";
            createTable(createSql);

            Database db = Env.getCurrentInternalCatalog().getDbOrAnalysisException("test");
            OlapTable table = (OlapTable) db.getTableOrAnalysisException("tstz_hot_part_cooldown");

            Env.getCurrentEnv().getDynamicPartitionScheduler()
                    .executeDynamicPartitionFirstTime(db.getId(), table.getId());

            RangePartitionInfo partitionInfo = (RangePartitionInfo) table.getPartitionInfo();
            // Sort partitions by lower bound.
            List<Map.Entry<Long, PartitionItem>> sortedEntries = Lists.newArrayList(
                    partitionInfo.getIdToItem(false).entrySet());
            sortedEntries.sort((a, b) -> {
                RangePartitionItem ai = (RangePartitionItem) a.getValue();
                RangePartitionItem bi = (RangePartitionItem) b.getValue();
                return ai.getItems().lowerEndpoint().compareTo(bi.getItems().lowerEndpoint());
            });

            Assert.assertEquals(7, sortedEntries.size());

            // Partitions idx=-3,-2,-1 are before the hot range → HDD.
            // Partitions idx=0..3 (4 partitions) are within hot_partition_num=1:
            //   - idx=0: hot (offset + hotPartitionNum = 0+1 = 1 > 0) → SSD
            //   - idx=1,2,3: hot (offset + hotPartitionNum > 0) → SSD
            // Because hot_partition_num=1 means only the current partition is hot,
            // but the logic counts from idx + hotPartitionNum > 0.
            // Actually: the condition is `offset + hotPartitionNum <= 0` → HDD.
            // So idx=-3,-2,-1: -3+1=-2≤0 HDD, -2+1=-1≤0 HDD, -1+1=0≤0 HDD.
            // idx=0,1,2,3: 0+1=1>0 SSD, etc.
            // idx=-3,-2,-1 are HDD, idx=0,1,2,3 are SSD.

            for (int i = 0; i < sortedEntries.size(); i++) {
                Map.Entry<Long, PartitionItem> entry = sortedEntries.get(i);
                RangePartitionItem item = (RangePartitionItem) entry.getValue();
                DataProperty dp = partitionInfo.getDataProperty(entry.getKey());

                if (i < 3) {
                    // Historical partitions: HDD
                    Assert.assertEquals("Historical partition should be HDD: idx=" + (i - 3),
                            TStorageMedium.HDD, dp.getStorageMedium());
                } else {
                    // Hot partitions: SSD
                    Assert.assertEquals("Hot partition should be SSD: idx=" + (i - 3),
                            TStorageMedium.SSD, dp.getStorageMedium());

                    // Every hot partition must have a finite cooldown equal to
                    // that partition's upper endpoint (offset + hotPartitionNum).
                    // Assert it is NOT the MAX fallback, which would indicate
                    // the TIMESTAMPTZ lifecycle string was rejected during parse.
                    Assert.assertNotEquals("Hot partition must have a finite cooldown: idx=" + (i - 3),
                            DataProperty.MAX_COOLDOWN_TIME_MS, dp.getCooldownTimeMs());

                    ZonedDateTime cooldownUtc = ZonedDateTime.ofInstant(
                            java.time.Instant.ofEpochMilli(dp.getCooldownTimeMs()),
                            ZoneOffset.UTC);

                    // Parse the partition's upper bound as a UTC instant.
                    String upperStr = item.getItems().upperEndpoint().getKeys().get(0).getStringValue();
                    ZonedDateTime upperUtc = ZonedDateTime.parse(upperStr,
                            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssXXX"));

                    Assert.assertEquals("Cooldown must equal the partition upper bound ("
                            + upperUtc + "): idx=" + (i - 3),
                            upperUtc.toInstant(), cooldownUtc.toInstant());
                }

                // Verify partition boundaries are UTC midnight.
                List<LiteralExpr> lowerKeys = item.getItems().lowerEndpoint().getKeys();
                String lowerStr = lowerKeys.get(0).getStringValue();
                Assert.assertTrue("Lower key must be UTC: " + lowerStr,
                        lowerStr.contains("+00:00"));
                Assert.assertEquals("Lower bound must be at UTC midnight: " + lowerStr,
                        "00", lowerStr.substring(11, 13));
            }
        } finally {
            connectContext.getSessionVariable().setTimeZone(originalTimeZone);
        }
    }

    @Test
    public void testTimeStampTzHotPartitionCooldownDstMonth() throws Exception {
        // Regression: When the session timezone has DST (e.g. America/Chicago,
        // UTC-5 in summer, UTC-6 in winter), PropertyAnalyzer.analyzeDataProperty()
        // parses cooldown values as DATETIME via DateLiteralUtils.createDateLiteral()
        // which applies the current Instant's offset during the initial shift
        // while using the target date's offset in unixTimestamp(). With the old
        // convertToUtcTimestamp path (which appended "+00:00"), a cooldown
        // boundary in a different DST period than NOW would shift by one hour.
        // The fix stores the cooldown with an explicit +00:00 suffix, and
        // PropertyAnalyzer now parses timezone-suffixed values directly as
        // instants. This test uses MONTH unit with America/Chicago (DST) for
        // both session and partition timezones so the cooldown boundary is
        // likely to fall in a different DST period than the current date.
        String originalTimeZone = connectContext.getSessionVariable().getTimeZone();
        try {
            connectContext.getSessionVariable().setTimeZone("America/Chicago");
            changeBeDisk(TStorageMedium.SSD);

            String createSql = "CREATE TABLE test.`tstz_cooldown_dst_month` (\n"
                    + "  `k1` TIMESTAMPTZ NULL COMMENT \"\",\n"
                    + "  `k2` int NULL COMMENT \"\"\n"
                    + ") ENGINE=OLAP\n"
                    + "DUPLICATE KEY(`k1`, `k2`)\n"
                    + "PARTITION BY RANGE(`k1`)\n"
                    + "()\n"
                    + "DISTRIBUTED BY HASH(`k2`) BUCKETS 3\n"
                    + "PROPERTIES (\n"
                    + "\"replication_num\" = \"1\",\n"
                    + "\"dynamic_partition.enable\" = \"true\",\n"
                    + "\"dynamic_partition.start\" = \"-3\",\n"
                    + "\"dynamic_partition.end\" = \"3\",\n"
                    + "\"dynamic_partition.create_history_partition\" = \"true\",\n"
                    + "\"dynamic_partition.time_unit\" = \"month\",\n"
                    + "\"dynamic_partition.prefix\" = \"p\",\n"
                    + "\"dynamic_partition.buckets\" = \"1\",\n"
                    + "\"dynamic_partition.time_zone\" = \"America/Chicago\",\n"
                    + "\"dynamic_partition.hot_partition_num\" = \"6\"\n"
                    + ");";
            createTable(createSql);

            Database db = Env.getCurrentInternalCatalog().getDbOrAnalysisException("test");
            OlapTable table = (OlapTable) db.getTableOrAnalysisException("tstz_cooldown_dst_month");

            Env.getCurrentEnv().getDynamicPartitionScheduler()
                    .executeDynamicPartitionFirstTime(db.getId(), table.getId());

            RangePartitionInfo partitionInfo = (RangePartitionInfo) table.getPartitionInfo();
            List<Map.Entry<Long, PartitionItem>> sortedEntries = Lists.newArrayList(
                    partitionInfo.getIdToItem(false).entrySet());
            sortedEntries.sort((a, b) -> {
                RangePartitionItem ai = (RangePartitionItem) a.getValue();
                RangePartitionItem bi = (RangePartitionItem) b.getValue();
                return ai.getItems().lowerEndpoint().compareTo(bi.getItems().lowerEndpoint());
            });

            Assert.assertEquals(7, sortedEntries.size());

            // idx=-3,-2,-1: offset+6=3,2,1 >0 → SSD (hot)
            // idx=0..3: offset+6=6,7,8,9 >0 → SSD (hot)
            // All partitions should be SSD since hot_partition_num covers all.

            // Derive the expected cooldown from the current partition's
            // stored lower bound rather than sampling ZonedDateTime.now(),
            // which can differ from the clock captured by the scheduler
            // and cause spurious failures across UTC month boundaries.
            RangePartitionItem currentItem = (RangePartitionItem) sortedEntries.get(3).getValue();
            ZonedDateTime currentLower = ZonedDateTime.parse(
                    currentItem.getItems().lowerEndpoint().getKeys().get(0).getStringValue(),
                    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssXXX"));

            for (int i = 0; i < sortedEntries.size(); i++) {
                int idx = i - 3; // idx=-3,-2,-1,0,1,2,3
                Map.Entry<Long, PartitionItem> entry = sortedEntries.get(i);
                RangePartitionItem item = (RangePartitionItem) entry.getValue();
                DataProperty dp = partitionInfo.getDataProperty(entry.getKey());

                Assert.assertEquals("All partitions should be SSD with hot_partition_num=6",
                        TStorageMedium.SSD, dp.getStorageMedium());
                Assert.assertNotEquals("Hot partition must have a finite cooldown",
                        DataProperty.MAX_COOLDOWN_TIME_MS, dp.getCooldownTimeMs());

                // Cooldown is the lower bound of the (idx + hotPartitionNum)-th
                // partition. Derive it from the current partition's lower bound
                // (which shares the scheduler's clock) to avoid race conditions.
                ZonedDateTime cooldownUtc = ZonedDateTime.ofInstant(
                        java.time.Instant.ofEpochMilli(dp.getCooldownTimeMs()),
                        ZoneOffset.UTC);
                ZonedDateTime expectedUtc = currentLower.plusMonths(idx + 6);
                Assert.assertEquals("Cooldown must equal lower bound of partition at offset "
                        + (idx + 6) + " (" + expectedUtc + "): idx=" + idx,
                        expectedUtc.toInstant(), cooldownUtc.toInstant());

                // Verify partition boundaries are UTC midnight, day=01.
                List<LiteralExpr> lowerKeys = item.getItems().lowerEndpoint().getKeys();
                String lowerStr = lowerKeys.get(0).getStringValue();
                Assert.assertTrue("Lower key must be UTC: " + lowerStr,
                        lowerStr.contains("+00:00"));
                Assert.assertEquals("Lower bound must be at UTC midnight: " + lowerStr,
                        "00", lowerStr.substring(11, 13));
                // Month boundaries must fall on the first day of the month.
                Assert.assertEquals("Month lower bound day must be 01: " + lowerStr,
                        "01", lowerStr.substring(8, 10));
            }
        } finally {
            connectContext.getSessionVariable().setTimeZone(originalTimeZone);
        }
    }

    @Test
    public void testTimeStampTzHotPartitionCooldownDstFallback() throws Exception {
        // DST fall-back regression: on 2026-11-01 at 2:00 AM CDT, America/Chicago
        // clocks fall back to 1:00 AM CST. The hour 01:00–02:00 occurs twice,
        // so a wall-clock string "01:00:00" without offset is ambiguous and Java
        // resolves it to the earlier offset (06:00 UTC instead of 07:00 UTC).
        // The fix stores the cooldown as an unambiguous UTC timestamp with +00:00
        // suffix (e.g. "2026-11-01 07:00:00+00:00"), and PropertyAnalyzer now
        // parses timezone-suffixed values directly as instants, bypassing the
        // broken DATETIME path. This test uses HOUR unit with America/Chicago
        // to ensure cooldown timestamps remain correct regardless of DST status.
        String originalTimeZone = connectContext.getSessionVariable().getTimeZone();
        try {
            connectContext.getSessionVariable().setTimeZone("America/Chicago");
            changeBeDisk(TStorageMedium.SSD);

            String createSql = "CREATE TABLE test.`tstz_cooldown_dst_fallback` (\n"
                    + "  `k1` TIMESTAMPTZ NULL COMMENT \"\",\n"
                    + "  `k2` int NULL COMMENT \"\"\n"
                    + ") ENGINE=OLAP\n"
                    + "DUPLICATE KEY(`k1`, `k2`)\n"
                    + "PARTITION BY RANGE(`k1`)\n"
                    + "()\n"
                    + "DISTRIBUTED BY HASH(`k2`) BUCKETS 3\n"
                    + "PROPERTIES (\n"
                    + "\"replication_num\" = \"1\",\n"
                    + "\"dynamic_partition.enable\" = \"true\",\n"
                    + "\"dynamic_partition.start\" = \"-3\",\n"
                    + "\"dynamic_partition.end\" = \"3\",\n"
                    + "\"dynamic_partition.create_history_partition\" = \"true\",\n"
                    + "\"dynamic_partition.time_unit\" = \"hour\",\n"
                    + "\"dynamic_partition.prefix\" = \"p\",\n"
                    + "\"dynamic_partition.buckets\" = \"1\",\n"
                    + "\"dynamic_partition.time_zone\" = \"America/Chicago\",\n"
                    + "\"dynamic_partition.hot_partition_num\" = \"1\"\n"
                    + ");";
            createTable(createSql);

            Database db = Env.getCurrentInternalCatalog().getDbOrAnalysisException("test");
            OlapTable table = (OlapTable) db.getTableOrAnalysisException("tstz_cooldown_dst_fallback");

            Env.getCurrentEnv().getDynamicPartitionScheduler()
                    .executeDynamicPartitionFirstTime(db.getId(), table.getId());

            RangePartitionInfo partitionInfo = (RangePartitionInfo) table.getPartitionInfo();
            List<Map.Entry<Long, PartitionItem>> sortedEntries = Lists.newArrayList(
                    partitionInfo.getIdToItem(false).entrySet());
            sortedEntries.sort((a, b) -> {
                RangePartitionItem ai = (RangePartitionItem) a.getValue();
                RangePartitionItem bi = (RangePartitionItem) b.getValue();
                return ai.getItems().lowerEndpoint().compareTo(bi.getItems().lowerEndpoint());
            });

            Assert.assertEquals(7, sortedEntries.size());

            // Derive the expected cooldown from the current partition's
            // stored lower bound rather than sampling ZonedDateTime.now(),
            // which can differ from the clock captured by the scheduler
            // and cause spurious failures across UTC hour boundaries.
            RangePartitionItem currentItem = (RangePartitionItem) sortedEntries.get(3).getValue();
            ZonedDateTime currentLower = ZonedDateTime.parse(
                    currentItem.getItems().lowerEndpoint().getKeys().get(0).getStringValue(),
                    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssXXX"));

            for (int i = 0; i < sortedEntries.size(); i++) {
                int idx = i - 3;
                Map.Entry<Long, PartitionItem> entry = sortedEntries.get(i);
                RangePartitionItem item = (RangePartitionItem) entry.getValue();
                DataProperty dp = partitionInfo.getDataProperty(entry.getKey());

                // idx=-3,-2,-1: offset+1 ≤ 0 → HDD (MAX_COOLDOWN_TIME_MS)
                // idx=0,1,2,3: offset+1 > 0 → SSD (finite cooldown)
                if (idx + 1 <= 0) {
                    Assert.assertEquals("Historical partition should be HDD: idx=" + idx,
                            TStorageMedium.HDD, dp.getStorageMedium());
                    continue;
                }

                Assert.assertEquals("Hot partition should be SSD: idx=" + idx,
                        TStorageMedium.SSD, dp.getStorageMedium());
                Assert.assertNotEquals("Hot partition must have a finite cooldown: idx=" + idx,
                        DataProperty.MAX_COOLDOWN_TIME_MS, dp.getCooldownTimeMs());

                // Cooldown is the lower bound of the (idx + hotPartitionNum)-th
                // partition. Derive it from the current partition's lower bound
                // (which shares the scheduler's clock) to avoid race conditions.
                ZonedDateTime cooldownUtc = ZonedDateTime.ofInstant(
                        java.time.Instant.ofEpochMilli(dp.getCooldownTimeMs()),
                        ZoneOffset.UTC);
                ZonedDateTime expectedUtc = currentLower.plusHours(idx + 1);
                Assert.assertEquals("Cooldown must equal lower bound of partition at offset "
                        + (idx + 1) + " (" + expectedUtc + "): idx=" + idx,
                        expectedUtc.toInstant(), cooldownUtc.toInstant());

                // Hour boundaries must be at whole UTC hours.
                List<LiteralExpr> lowerKeys = item.getItems().lowerEndpoint().getKeys();
                String lowerStr = lowerKeys.get(0).getStringValue();
                Assert.assertTrue("Lower key must be UTC: " + lowerStr,
                        lowerStr.contains("+00:00"));
                int lowerHour = Integer.parseInt(lowerStr.substring(11, 13));
                Assert.assertTrue("Lower bound hour must be 0-23: " + lowerStr,
                        lowerHour >= 0 && lowerHour <= 23);
            }
        } finally {
            connectContext.getSessionVariable().setTimeZone(originalTimeZone);
        }
    }

    @Test
    public void testTimeStampTzHotPartitionCooldownFractionalSeconds() throws Exception {
        // Regression: convertToUtcTimestamp() preserves the column's scale
        // (0–6) via TimeStampTzType.of(), so cooldown strings may carry
        // fractional seconds (e.g. "2027-01-01 00:00:00.000000+00:00" for
        // TIMESTAMPTZ(6)). The old PropertyAnalyzer branch used a fixed-
        // position offset check and a "yyyy-MM-dd HH:mm:ssXXX" formatter,
        // both of which silently rejected fractional-second values and fell
        // back to MAX_COOLDOWN_TIME_MS. The fix uses a regex offset detector
        // and a DateTimeFormatterBuilder with appendFraction(0,6,true) so
        // values with any scale 0–6 are parsed as instants.
        // This test exercises precisions 0, 3, and 6.
        String originalTimeZone = connectContext.getSessionVariable().getTimeZone();
        int[] precisions = {0, 3, 6};
        try {
            connectContext.getSessionVariable().setTimeZone("Asia/Shanghai");
            changeBeDisk(TStorageMedium.SSD);

            for (int precision : precisions) {
                String tableName = "tstz_cooldown_frac_" + precision;
                String createSql = "CREATE TABLE test.`" + tableName + "` (\n"
                        + "  `k1` TIMESTAMPTZ(" + precision + ") NULL COMMENT \"\",\n"
                        + "  `k2` int NULL COMMENT \"\"\n"
                        + ") ENGINE=OLAP\n"
                        + "DUPLICATE KEY(`k1`, `k2`)\n"
                        + "PARTITION BY RANGE(`k1`)\n"
                        + "()\n"
                        + "DISTRIBUTED BY HASH(`k2`) BUCKETS 3\n"
                        + "PROPERTIES (\n"
                        + "\"replication_num\" = \"1\",\n"
                        + "\"dynamic_partition.enable\" = \"true\",\n"
                        + "\"dynamic_partition.start\" = \"-3\",\n"
                        + "\"dynamic_partition.end\" = \"3\",\n"
                        + "\"dynamic_partition.create_history_partition\" = \"true\",\n"
                        + "\"dynamic_partition.time_unit\" = \"day\",\n"
                        + "\"dynamic_partition.prefix\" = \"p\",\n"
                        + "\"dynamic_partition.buckets\" = \"1\",\n"
                        + "\"dynamic_partition.time_zone\" = \"America/Chicago\",\n"
                        + "\"dynamic_partition.hot_partition_num\" = \"1\"\n"
                        + ");";
                createTable(createSql);

                Database db = Env.getCurrentInternalCatalog().getDbOrAnalysisException("test");
                OlapTable table = (OlapTable) db.getTableOrAnalysisException(tableName);

                Env.getCurrentEnv().getDynamicPartitionScheduler()
                        .executeDynamicPartitionFirstTime(db.getId(), table.getId());

                RangePartitionInfo partitionInfo = (RangePartitionInfo) table.getPartitionInfo();
                List<Map.Entry<Long, PartitionItem>> sortedEntries = Lists.newArrayList(
                        partitionInfo.getIdToItem(false).entrySet());
                sortedEntries.sort((a, b) -> {
                    RangePartitionItem ai = (RangePartitionItem) a.getValue();
                    RangePartitionItem bi = (RangePartitionItem) b.getValue();
                    return ai.getItems().lowerEndpoint().compareTo(bi.getItems().lowerEndpoint());
                });

                Assert.assertEquals("TIMESTAMPTZ(" + precision + "): partition count",
                        7, sortedEntries.size());

                // Hot partitions must have finite cooldown, not
                // MAX_COOLDOWN_TIME_MS (which would mean the cooldown
                // string was rejected by PropertyAnalyzer).
                java.time.format.DateTimeFormatterBuilder fb =
                        new java.time.format.DateTimeFormatterBuilder()
                                .appendPattern("yyyy-MM-dd HH:mm:ss")
                                .appendFraction(java.time.temporal.ChronoField.NANO_OF_SECOND, 0, 6, true)
                                .appendOffset("+HH:MM", "+00:00");
                java.time.format.DateTimeFormatter boundFmt = fb.toFormatter();
                for (int i = 0; i < sortedEntries.size(); i++) {
                    int idx = i - 3;
                    Map.Entry<Long, PartitionItem> entry = sortedEntries.get(i);
                    RangePartitionItem item = (RangePartitionItem) entry.getValue();
                    DataProperty dp = partitionInfo.getDataProperty(entry.getKey());

                    if (i < 3) {
                        Assert.assertEquals("TIMESTAMPTZ(" + precision
                                + ") historical partition should be HDD: idx=" + idx,
                                TStorageMedium.HDD, dp.getStorageMedium());
                    } else {
                        Assert.assertEquals("TIMESTAMPTZ(" + precision
                                + ") hot partition should be SSD: idx=" + idx,
                                TStorageMedium.SSD, dp.getStorageMedium());
                        Assert.assertNotEquals("TIMESTAMPTZ(" + precision
                                + ") cooldown must be finite (not MAX): idx=" + idx,
                                DataProperty.MAX_COOLDOWN_TIME_MS, dp.getCooldownTimeMs());

                        ZonedDateTime cooldownUtc = ZonedDateTime.ofInstant(
                                java.time.Instant.ofEpochMilli(dp.getCooldownTimeMs()),
                                ZoneOffset.UTC);
                        String upperStr = item.getItems().upperEndpoint().getKeys().get(0)
                                .getStringValue();
                        ZonedDateTime upperUtc = ZonedDateTime.parse(upperStr, boundFmt);
                        Assert.assertEquals("TIMESTAMPTZ(" + precision
                                + ") cooldown must equal partition upper bound: idx=" + idx,
                                upperUtc.toInstant(), cooldownUtc.toInstant());
                    }
                }
            }
        } finally {
            connectContext.getSessionVariable().setTimeZone(originalTimeZone);
        }
    }

    @Test
    public void testTimeStampTzGetHistoricalPartitionsScaledColumn() throws Exception {
        // TIMESTAMPTZ(6) stores lower keys with ".000000+00:00" suffix while
        // currentUtcBorder uses "+00:00" (no fractional part).  The old string
        // comparison failed on this scale difference.  Fix: compare by
        // PartitionKey.compareTo instead.
        String originalTimeZone = connectContext.getSessionVariable().getTimeZone();
        try {
            connectContext.getSessionVariable().setTimeZone("America/Chicago");

            String createSql = "CREATE TABLE test.`tstz_hist_scaled` (\n"
                    + "  `k1` TIMESTAMPTZ(6) NULL COMMENT \"\",\n"
                    + "  `k2` int NULL COMMENT \"\"\n"
                    + ") ENGINE=OLAP\n"
                    + "DUPLICATE KEY(`k1`, `k2`)\n"
                    + "PARTITION BY RANGE(`k1`)\n"
                    + "()\n"
                    + "DISTRIBUTED BY HASH(`k2`) BUCKETS 3\n"
                    + "PROPERTIES (\n"
                    + "\"replication_num\" = \"1\",\n"
                    + "\"dynamic_partition.enable\" = \"true\",\n"
                    + "\"dynamic_partition.start\" = \"-3\",\n"
                    + "\"dynamic_partition.end\" = \"3\",\n"
                    + "\"dynamic_partition.create_history_partition\" = \"true\",\n"
                    + "\"dynamic_partition.time_unit\" = \"day\",\n"
                    + "\"dynamic_partition.prefix\" = \"p\",\n"
                    + "\"dynamic_partition.buckets\" = \"1\",\n"
                    + "\"dynamic_partition.time_zone\" = \"America/Chicago\"\n"
                    + ");";
            createTable(createSql);

            Database db = Env.getCurrentInternalCatalog().getDbOrAnalysisException("test");
            OlapTable table = (OlapTable) db.getTableOrAnalysisException("tstz_hist_scaled");

            Env.getCurrentEnv().getDynamicPartitionScheduler()
                    .executeDynamicPartitionFirstTime(db.getId(), table.getId());

            int totalPartitions = table.getPartitionNames().size();
            Assert.assertEquals(7, totalPartitions);

            RangePartitionInfo info = (RangePartitionInfo) table.getPartitionInfo();
            // Verify that stored lower keys actually have .000000 fractional part.
            for (PartitionItem item : info.getIdToItem(false).values()) {
                RangePartitionItem rItem = (RangePartitionItem) item;
                String lowerStr = rItem.getItems().lowerEndpoint().getKeys().get(0).getStringValue();
                Assert.assertTrue("TIMESTAMPTZ(6) lower key must include .000000: " + lowerStr,
                        lowerStr.contains(".000000"));
            }

            // Compute currentUtcBorder (no fractional seconds).
            DynamicPartitionProperty prop = table.getTableProperty().getDynamicPartitionProperty();
            String partitionFormat = DynamicPartitionUtil.getPartitionFormat(
                    info.getPartitionColumns().get(0));
            String currentUtcBorder = DynamicPartitionUtil.getPartitionRangeString(
                    prop, ZonedDateTime.now(ZoneOffset.UTC), 0, partitionFormat);
            Assert.assertFalse("currentUtcBorder should NOT contain fractional seconds: "
                    + currentUtcBorder, currentUtcBorder.contains("."));

            // Without currentUtcBorder: name-based cannot identify the current
            // partition when nowPartitionName does not match.
            String wrongNowPartitionName = "p_nonexistent";
            List<Partition> historicalNoUtc = DynamicPartitionScheduler.getHistoricalPartitions(
                    table, wrongNowPartitionName, null);
            boolean currentFoundByName = false;
            for (Partition p : historicalNoUtc) {
                RangePartitionItem item = (RangePartitionItem) info.getItem(p.getId());
                String lowerBound = item.getItems().lowerEndpoint().getKeys().get(0).getStringValue();
                if (lowerBound.contains(currentUtcBorder.replace("+00:00", ""))) {
                    currentFoundByName = true;
                    break;
                }
            }
            Assert.assertTrue("Scaled TIMESTAMPTZ(6): name-based should fail to exclude current partition",
                    currentFoundByName);

            // With currentUtcBorder: range-based correctly excludes the current
            // partition even though the string representations differ (.000000).
            List<Partition> historicalWithUtc = DynamicPartitionScheduler.getHistoricalPartitions(
                    table, wrongNowPartitionName, currentUtcBorder);
            boolean currentFoundByRange = false;
            for (Partition p : historicalWithUtc) {
                RangePartitionItem item = (RangePartitionItem) info.getItem(p.getId());
                String lowerBound = item.getItems().lowerEndpoint().getKeys().get(0).getStringValue();
                if (lowerBound.contains(currentUtcBorder.replace("+00:00", ""))) {
                    currentFoundByRange = true;
                    break;
                }
            }
            Assert.assertFalse("Scaled TIMESTAMPTZ(6): range-based must exclude current partition",
                    currentFoundByRange);
            Assert.assertEquals("Scaled TIMESTAMPTZ(6): exclude exactly one partition",
                    totalPartitions - 1, historicalWithUtc.size());
        } finally {
            connectContext.getSessionVariable().setTimeZone(originalTimeZone);
        }
    }

    @Test
    public void testTimeStampTzGetHistoricalPartitionsOldPrefix() throws Exception {
        // When dynamic_partition.prefix is changed after initial partition
        // creation, existing partitions keep their old names (e.g. "p20260720")
        // while the new nowPartitionName uses the new prefix ("q20260720").
        // Name-based comparison fails to identify the current partition;
        // range-based comparison (currentUtcBorder) must still work.
        String originalTimeZone = connectContext.getSessionVariable().getTimeZone();
        try {
            connectContext.getSessionVariable().setTimeZone("Asia/Shanghai");

            String createSql = "CREATE TABLE test.`tstz_old_prefix` (\n"
                    + "  `k1` TIMESTAMPTZ NULL COMMENT \"\",\n"
                    + "  `k2` int NULL COMMENT \"\"\n"
                    + ") ENGINE=OLAP\n"
                    + "DUPLICATE KEY(`k1`, `k2`)\n"
                    + "PARTITION BY RANGE(`k1`)\n"
                    + "()\n"
                    + "DISTRIBUTED BY HASH(`k2`) BUCKETS 3\n"
                    + "PROPERTIES (\n"
                    + "\"replication_num\" = \"1\",\n"
                    + "\"dynamic_partition.enable\" = \"true\",\n"
                    + "\"dynamic_partition.start\" = \"-3\",\n"
                    + "\"dynamic_partition.end\" = \"3\",\n"
                    + "\"dynamic_partition.create_history_partition\" = \"true\",\n"
                    + "\"dynamic_partition.time_unit\" = \"day\",\n"
                    + "\"dynamic_partition.prefix\" = \"p\",\n"
                    + "\"dynamic_partition.buckets\" = \"1\",\n"
                    + "\"dynamic_partition.time_zone\" = \"Asia/Shanghai\"\n"
                    + ");";
            createTable(createSql);

            Database db = Env.getCurrentInternalCatalog().getDbOrAnalysisException("test");
            OlapTable table = (OlapTable) db.getTableOrAnalysisException("tstz_old_prefix");

            Env.getCurrentEnv().getDynamicPartitionScheduler()
                    .executeDynamicPartitionFirstTime(db.getId(), table.getId());

            int totalPartitions = table.getPartitionNames().size();
            Assert.assertEquals("Initial partitions should be 7", 7, totalPartitions);

            // Verify all existing partitions use the old prefix "p".
            for (String name : table.getPartitionNames()) {
                Assert.assertTrue("Existing partitions must use old prefix: " + name,
                        name.startsWith("p"));
            }

            // Now change the prefix from "p" to "q" via table properties.
            HashMap<String, String> newPrefixProps = new HashMap<>();
            newPrefixProps.put("dynamic_partition.prefix", "q");
            table.getTableProperty().modifyTableProperties(newPrefixProps);
            table.getTableProperty().buildDynamicProperty();

            // Re-read the dynamic property to get the updated prefix.
            DynamicPartitionProperty updatedProp = table.getTableProperty().getDynamicPartitionProperty();
            Assert.assertEquals("Prefix should now be 'q'", "q", updatedProp.getPrefix());

            // Compute currentUtcBorder from the scheduler's logic.
            RangePartitionInfo info = (RangePartitionInfo) table.getPartitionInfo();
            String partitionFormat = DynamicPartitionUtil.getPartitionFormat(
                    info.getPartitionColumns().get(0));
            String currentUtcBorder = DynamicPartitionUtil.getPartitionRangeString(
                    updatedProp, ZonedDateTime.now(ZoneOffset.UTC), 0, partitionFormat);

            // nowPartitionName uses the new prefix "q" — no existing partition
            // matches it, so name-based comparison would not exclude anything.
            String newPrefixNowName = "q_nonexistent";

            // 1. Without currentUtcBorder: name-based fails to exclude the
            //    current partition because the name "q_nonexistent" matches nothing.
            List<Partition> historicalNoUtc = DynamicPartitionScheduler.getHistoricalPartitions(
                    table, newPrefixNowName, null);
            boolean currentFoundByName = false;
            for (Partition p : historicalNoUtc) {
                RangePartitionItem item = (RangePartitionItem) info.getItem(p.getId());
                String lowerBound = item.getItems().lowerEndpoint().getKeys().get(0).getStringValue();
                if (lowerBound.contains(currentUtcBorder.replace("+00:00", ""))) {
                    currentFoundByName = true;
                    break;
                }
            }
            Assert.assertTrue("Old prefix: name-based should fail to exclude current partition",
                    currentFoundByName);

            // 2. With currentUtcBorder: range-based correctly excludes the
            //    current partition despite the name mismatch.
            List<Partition> historicalWithUtc = DynamicPartitionScheduler.getHistoricalPartitions(
                    table, newPrefixNowName, currentUtcBorder);
            boolean currentFoundByRange = false;
            for (Partition p : historicalWithUtc) {
                RangePartitionItem item = (RangePartitionItem) info.getItem(p.getId());
                String lowerBound = item.getItems().lowerEndpoint().getKeys().get(0).getStringValue();
                if (lowerBound.contains(currentUtcBorder.replace("+00:00", ""))) {
                    currentFoundByRange = true;
                    break;
                }
            }
            Assert.assertFalse("Old prefix: range-based must exclude current partition",
                    currentFoundByRange);
            Assert.assertEquals("Old prefix: exclude exactly one partition",
                    totalPartitions - 1, historicalWithUtc.size());
        } finally {
            connectContext.getSessionVariable().setTimeZone(originalTimeZone);
        }
    }

    @Test
    public void testTimeStampTzGetHistoricalPartitionsNoncanonicalRange() throws Exception {
        // A pre-fix partition created under Asia/Shanghai has a lower bound
        // at 16:00Z (Shanghai midnight = UTC 16:00 previous day).  The new
        // scheduler captures currentUtcBorder at UTC midnight (00:00Z) which
        // falls INSIDE such a partition but does NOT equal its lower endpoint.
        // Exact lower-bound equality would miss this partition, leaving it
        // in the historical list and corrupting auto-bucket calculations.
        // Range containment (lower <= rangeKey < upper) correctly excludes it.
        String originalTimeZone = connectContext.getSessionVariable().getTimeZone();
        try {
            connectContext.getSessionVariable().setTimeZone("America/Chicago");

            // Create table without dynamic partition — we add ranges manually.
            String createSql = "CREATE TABLE test.`tstz_noncanonical` (\n"
                    + "  `k1` TIMESTAMPTZ NULL COMMENT \"\",\n"
                    + "  `k2` int NULL COMMENT \"\"\n"
                    + ") ENGINE=OLAP\n"
                    + "DUPLICATE KEY(`k1`, `k2`)\n"
                    + "PARTITION BY RANGE(`k1`)\n"
                    + "()\n"
                    + "DISTRIBUTED BY HASH(`k2`) BUCKETS 3\n"
                    + "PROPERTIES (\n"
                    + "\"replication_num\" = \"1\"\n"
                    + ");";
            createTable(createSql);

            Database db = Env.getCurrentInternalCatalog().getDbOrAnalysisException("test");
            OlapTable table = (OlapTable) db.getTableOrAnalysisException("tstz_noncanonical");

            DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);

            // Non-canonical partition: lower = yesterday 16:00Z, upper = today 16:00Z.
            // currentUtcBorder (today 00:00Z) is inside [yesterday 16:00Z, today 16:00Z)
            // but does NOT equal the lower bound (16:00Z).
            ZonedDateTime yesterday16Z = now.withHour(16).withMinute(0).withSecond(0).withNano(0);
            if (now.getHour() < 16) {
                yesterday16Z = yesterday16Z.minusDays(1);
            }
            ZonedDateTime today16Z = yesterday16Z.plusDays(1);
            String oldLower = fmt.format(yesterday16Z) + "+00:00";
            String oldUpper = fmt.format(today16Z) + "+00:00";
            alterTable("ALTER TABLE test.tstz_noncanonical ADD PARTITION p_legacy VALUES "
                    + "[('" + oldLower + "'), ('" + oldUpper + "'))");

            // Historical non-overlapping partition (far in the past).
            alterTable("ALTER TABLE test.tstz_noncanonical ADD PARTITION p_hist VALUES "
                    + "[('2020-01-01 00:00:00+00:00'), ('2020-01-02 00:00:00+00:00'))");

            Assert.assertEquals(2, table.getPartitionNames().size());

            // currentUtcBorder = today's UTC midnight → inside p_legacy.
            String currentUtcBorder = fmt.format(now.withHour(0).withMinute(0).withSecond(0).withNano(0)) + "+00:00";

            // Sanity check: currentUtcBorder is inside p_legacy.
            RangePartitionInfo info = (RangePartitionInfo) table.getPartitionInfo();
            for (Map.Entry<Long, PartitionItem> entry : info.getIdToItem(false).entrySet()) {
                RangePartitionItem item = (RangePartitionItem) entry.getValue();
                String lowerStr = item.getItems().lowerEndpoint().getKeys().get(0).getStringValue();
                String upperStr = item.getItems().upperEndpoint().getKeys().get(0).getStringValue();
                if ("p_legacy".equals(table.getPartition(entry.getKey()).getName())) {
                    Assert.assertTrue("00:00Z must be inside [" + lowerStr + ", " + upperStr + ")",
                            currentUtcBorder.compareTo(lowerStr) >= 0
                            && currentUtcBorder.compareTo(upperStr) < 0);
                    Assert.assertFalse("00:00Z must NOT equal " + lowerStr,
                            currentUtcBorder.equals(lowerStr));
                }
            }

            String wrongNowPartitionName = "p_nonexistent";

            // 1. Without currentUtcBorder: name-based fallback returns both.
            List<Partition> historicalNoUtc = DynamicPartitionScheduler.getHistoricalPartitions(
                    table, wrongNowPartitionName, null);
            Assert.assertEquals("Name-based fallback returns all partitions",
                    2, historicalNoUtc.size());

            // 2. With currentUtcBorder: range containment excludes p_legacy
            //    because 00:00Z ∈ [yesterday 16:00Z, today 16:00Z).
            //    p_legacy's lower bound (16:00Z) ≠ 00:00Z, so exact equality
            //    would NOT find it — only containment does.
            List<Partition> historicalWithUtc = DynamicPartitionScheduler.getHistoricalPartitions(
                    table, wrongNowPartitionName, currentUtcBorder);
            Assert.assertEquals("Range containment excludes exactly one partition",
                    1, historicalWithUtc.size());
            Assert.assertFalse("p_legacy must be excluded by range containment",
                    "p_legacy".equals(historicalWithUtc.get(0).getName()));
            Assert.assertEquals("p_hist should survive",
                    "p_hist", historicalWithUtc.get(0).getName());
        } finally {
            connectContext.getSessionVariable().setTimeZone(originalTimeZone);
        }
    }
}
