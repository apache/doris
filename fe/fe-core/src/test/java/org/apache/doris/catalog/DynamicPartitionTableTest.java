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

import org.apache.doris.analysis.AlterTableStmt;
import org.apache.doris.analysis.CreateDbStmt;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.clone.DynamicPartitionScheduler;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.common.FeConstants;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TStorageMedium;
import org.apache.doris.utframe.UtFrameUtils;

import com.clearspring.analytics.util.Lists;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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

        UtFrameUtils.createDorisCluster(runningDir);

        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        // create database
        String createDbStmtStr = "create database test;";
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt(createDbStmtStr, connectContext);
        Catalog.getCurrentCatalog().createDb(createDbStmt);
    }

    @AfterClass
    public static void TearDown() {
        UtFrameUtils.cleanDorisFeDir(runningDir);
    }

    private static void createTable(String sql) throws Exception {
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        Catalog.getCurrentCatalog().createTable(createTableStmt);
    }

    private static void alterTable(String sql) throws Exception {
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        Catalog.getCurrentCatalog().alterTable(alterTableStmt);
    }

    @Test
    public void testNormal() throws Exception {
        String createOlapTblStmt = "CREATE TABLE test.`dynamic_partition_normal` (\n" +
                "  `k1` date NULL COMMENT \"\",\n" +
                "  `k2` int NULL COMMENT \"\",\n" +
                "  `k3` smallint NULL COMMENT \"\",\n" +
                "  `v1` varchar(2048) NULL COMMENT \"\",\n" +
                "  `v2` datetime NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`, `k2`, `k3`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE (k1)\n" +
                "(\n" +
                "PARTITION p1 VALUES LESS THAN (\"2014-01-01\"),\n" +
                "PARTITION p2 VALUES LESS THAN (\"2014-06-01\"),\n" +
                "PARTITION p3 VALUES LESS THAN (\"2014-12-01\")\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 32\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"dynamic_partition.enable\" = \"true\",\n" +
                "\"dynamic_partition.start\" = \"-3\",\n" +
                "\"dynamic_partition.end\" = \"3\",\n" +
                "\"dynamic_partition.time_unit\" = \"day\",\n" +
                "\"dynamic_partition.prefix\" = \"p\",\n" +
                "\"dynamic_partition.buckets\" = \"1\"\n" +
                ");";
        createTable(createOlapTblStmt);
        Database db = Catalog.getCurrentCatalog().getDbOrAnalysisException("default_cluster:test");
        OlapTable table = (OlapTable) db.getTableOrAnalysisException("dynamic_partition_normal");
        Assert.assertTrue(table.getTableProperty().getDynamicPartitionProperty().getReplicaAllocation().isNotSet());
    }

    @Test
    public void testMissPrefix() throws Exception {
        String createOlapTblStmt = "CREATE TABLE test.`dynamic_partition_prefix` (\n" +
                "  `k1` date NULL COMMENT \"\",\n" +
                "  `k2` int NULL COMMENT \"\",\n" +
                "  `k3` smallint NULL COMMENT \"\",\n" +
                "  `v1` varchar(2048) NULL COMMENT \"\",\n" +
                "  `v2` datetime NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`, `k2`, `k3`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE (k1)\n" +
                "(\n" +
                "PARTITION p1 VALUES LESS THAN (\"2014-01-01\"),\n" +
                "PARTITION p2 VALUES LESS THAN (\"2014-06-01\"),\n" +
                "PARTITION p3 VALUES LESS THAN (\"2014-12-01\")\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 32\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"dynamic_partition.enable\" = \"true\",\n" +
                "\"dynamic_partition.start\" = \"-3\",\n" +
                "\"dynamic_partition.end\" = \"3\",\n" +
                "\"dynamic_partition.time_unit\" = \"day\",\n" +
                "\"dynamic_partition.buckets\" = \"1\"\n" +
                ");";
        expectedException.expect(DdlException.class);
        expectedException.expectMessage("errCode = 2, detailMessage = Must assign dynamic_partition.prefix properties");
        createTable(createOlapTblStmt);
    }

    @Test
    public void testMissTimeUnit() throws Exception {
        String createOlapTblStmt = "CREATE TABLE test.`dynamic_partition_time_unit` (\n" +
                "  `k1` date NULL COMMENT \"\",\n" +
                "  `k2` int NULL COMMENT \"\",\n" +
                "  `k3` smallint NULL COMMENT \"\",\n" +
                "  `v1` varchar(2048) NULL COMMENT \"\",\n" +
                "  `v2` datetime NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`, `k2`, `k3`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE (k1)\n" +
                "(\n" +
                "PARTITION p1 VALUES LESS THAN (\"2014-01-01\"),\n" +
                "PARTITION p2 VALUES LESS THAN (\"2014-06-01\"),\n" +
                "PARTITION p3 VALUES LESS THAN (\"2014-12-01\")\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 32\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"dynamic_partition.enable\" = \"true\",\n" +
                "\"dynamic_partition.start\" = \"-3\",\n" +
                "\"dynamic_partition.end\" = \"3\",\n" +
                "\"dynamic_partition.prefix\" = \"p\",\n" +
                "\"dynamic_partition.buckets\" = \"1\"\n" +
                ");";
        expectedException.expect(DdlException.class);
        expectedException.expectMessage("errCode = 2, detailMessage = Must assign dynamic_partition.time_unit properties");
        createTable(createOlapTblStmt);
    }

    @Test
    public void testMissStart() throws Exception {
        String createOlapTblStmt = "CREATE TABLE test.`dynamic_partition_start` (\n" +
                "  `k1` date NULL COMMENT \"\",\n" +
                "  `k2` int NULL COMMENT \"\",\n" +
                "  `k3` smallint NULL COMMENT \"\",\n" +
                "  `v1` varchar(2048) NULL COMMENT \"\",\n" +
                "  `v2` datetime NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`, `k2`, `k3`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE (k1)\n" +
                "(\n" +
                "PARTITION p1 VALUES LESS THAN (\"2014-01-01\"),\n" +
                "PARTITION p2 VALUES LESS THAN (\"2014-06-01\"),\n" +
                "PARTITION p3 VALUES LESS THAN (\"2014-12-01\")\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 32\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"dynamic_partition.enable\" = \"true\",\n" +
                "\"dynamic_partition.end\" = \"3\",\n" +
                "\"dynamic_partition.time_unit\" = \"day\",\n" +
                "\"dynamic_partition.prefix\" = \"p\",\n" +
                "\"dynamic_partition.buckets\" = \"1\"\n" +
                ");";
        createTable(createOlapTblStmt);
    }

    @Test
    public void testMissEnd() throws Exception {
        String createOlapTblStmt = "CREATE TABLE test.`dynamic_partition_end` (\n" +
                "  `k1` date NULL COMMENT \"\",\n" +
                "  `k2` int NULL COMMENT \"\",\n" +
                "  `k3` smallint NULL COMMENT \"\",\n" +
                "  `v1` varchar(2048) NULL COMMENT \"\",\n" +
                "  `v2` datetime NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`, `k2`, `k3`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE (k1)\n" +
                "(\n" +
                "PARTITION p1 VALUES LESS THAN (\"2014-01-01\"),\n" +
                "PARTITION p2 VALUES LESS THAN (\"2014-06-01\"),\n" +
                "PARTITION p3 VALUES LESS THAN (\"2014-12-01\")\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 32\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"dynamic_partition.enable\" = \"true\",\n" +
                "\"dynamic_partition.start\" = \"-3\",\n" +
                "\"dynamic_partition.time_unit\" = \"day\",\n" +
                "\"dynamic_partition.prefix\" = \"p\",\n" +
                "\"dynamic_partition.buckets\" = \"1\"\n" +
                ");";
        expectedException.expect(DdlException.class);
        expectedException.expectMessage("errCode = 2, detailMessage = Must assign dynamic_partition.end properties");
        createTable(createOlapTblStmt);
    }

    @Test
    public void testMissBuckets() throws Exception {
        String createOlapTblStmt = "CREATE TABLE test.`dynamic_partition_buckets` (\n" +
                "  `k1` date NULL COMMENT \"\",\n" +
                "  `k2` int NULL COMMENT \"\",\n" +
                "  `k3` smallint NULL COMMENT \"\",\n" +
                "  `v1` varchar(2048) NULL COMMENT \"\",\n" +
                "  `v2` datetime NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`, `k2`, `k3`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE (k1)\n" +
                "(\n" +
                "PARTITION p1 VALUES LESS THAN (\"2014-01-01\"),\n" +
                "PARTITION p2 VALUES LESS THAN (\"2014-06-01\"),\n" +
                "PARTITION p3 VALUES LESS THAN (\"2014-12-01\")\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 32\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"dynamic_partition.enable\" = \"true\",\n" +
                "\"dynamic_partition.start\" = \"-3\",\n" +
                "\"dynamic_partition.end\" = \"3\",\n" +
                "\"dynamic_partition.time_unit\" = \"day\",\n" +
                "\"dynamic_partition.prefix\" = \"p\"\n" +
                ");";
        expectedException.expect(DdlException.class);
        expectedException.expectMessage("errCode = 2, detailMessage = Must assign dynamic_partition.buckets properties");
        createTable(createOlapTblStmt);
    }

    @Test
    public void testNotAllowed() throws Exception {
        String createOlapTblStmt = "CREATE TABLE test.`dynamic_partition_buckets` (\n" +
                "  `k1` date NULL COMMENT \"\",\n" +
                "  `k2` int NULL COMMENT \"\",\n" +
                "  `k3` smallint NULL COMMENT \"\",\n" +
                "  `v1` varchar(2048) NULL COMMENT \"\",\n" +
                "  `v2` datetime NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`, `k2`, `k3`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 32\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"dynamic_partition.enable\" = \"true\",\n" +
                "\"dynamic_partition.start\" = \"-3\",\n" +
                "\"dynamic_partition.end\" = \"3\",\n" +
                "\"dynamic_partition.time_unit\" = \"day\",\n" +
                "\"dynamic_partition.prefix\" = \"p\",\n" +
                "\"dynamic_partition.buckets\" = \"1\"\n" +
                ");";
        expectedException.expect(DdlException.class);
        expectedException.expectMessage("errCode = 2, detailMessage = Only support dynamic partition properties on range partition table");
        createTable(createOlapTblStmt);
    }

    @Test
    public void testNotAllowedInMultiPartitions() throws Exception {
        String createOlapTblStmt = "CREATE TABLE test.`dynamic_partition_normal` (\n" +
                "  `k1` date NULL COMMENT \"\",\n" +
                "  `k2` int NULL COMMENT \"\",\n" +
                "  `k3` smallint NULL COMMENT \"\",\n" +
                "  `v1` varchar(2048) NULL COMMENT \"\",\n" +
                "  `v2` datetime NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`, `k2`, `k3`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE (k1, k2)\n" +
                "(\n" +
                "PARTITION p1 VALUES LESS THAN (\"2014-01-01\", \"100\"),\n" +
                "PARTITION p2 VALUES LESS THAN (\"2014-06-01\", \"200\"),\n" +
                "PARTITION p3 VALUES LESS THAN (\"2014-12-01\", \"300\")\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 32\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"dynamic_partition.enable\" = \"true\",\n" +
                "\"dynamic_partition.start\" = \"-3\",\n" +
                "\"dynamic_partition.end\" = \"3\",\n" +
                "\"dynamic_partition.time_unit\" = \"day\",\n" +
                "\"dynamic_partition.prefix\" = \"p\",\n" +
                "\"dynamic_partition.buckets\" = \"1\"\n" +
                ");";
        expectedException.expect(DdlException.class);
        expectedException.expectMessage("errCode = 2, detailMessage = Dynamic partition only support single-column range partition");
        createTable(createOlapTblStmt);
    }

    @Test
    public void testMissTimeZone() throws Exception {
        String createOlapTblStmt = "CREATE TABLE test.`dynamic_partition_miss_time_zone` (\n" +
                "  `k1` date NULL COMMENT \"\",\n" +
                "  `k2` int NULL COMMENT \"\",\n" +
                "  `k3` smallint NULL COMMENT \"\",\n" +
                "  `v1` varchar(2048) NULL COMMENT \"\",\n" +
                "  `v2` datetime NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`, `k2`, `k3`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE (k1)\n" +
                "(\n" +
                "PARTITION p1 VALUES LESS THAN (\"2014-01-01\"),\n" +
                "PARTITION p2 VALUES LESS THAN (\"2014-06-01\"),\n" +
                "PARTITION p3 VALUES LESS THAN (\"2014-12-01\")\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 32\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"dynamic_partition.enable\" = \"true\",\n" +
                "\"dynamic_partition.start\" = \"-3\",\n" +
                "\"dynamic_partition.end\" = \"3\",\n" +
                "\"dynamic_partition.buckets\" = \"3\",\n" +
                "\"dynamic_partition.time_unit\" = \"day\",\n" +
                "\"dynamic_partition.prefix\" = \"p\"\n" +
                ");";
        createTable(createOlapTblStmt);
    }

    @Test
    public void testNormalTimeZone() throws Exception {
        String createOlapTblStmt = "CREATE TABLE test.`dynamic_partition_time_zone` (\n" +
                "  `k1` date NULL COMMENT \"\",\n" +
                "  `k2` int NULL COMMENT \"\",\n" +
                "  `k3` smallint NULL COMMENT \"\",\n" +
                "  `v1` varchar(2048) NULL COMMENT \"\",\n" +
                "  `v2` datetime NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`, `k2`, `k3`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE (k1)\n" +
                "(\n" +
                "PARTITION p1 VALUES LESS THAN (\"2014-01-01\"),\n" +
                "PARTITION p2 VALUES LESS THAN (\"2014-06-01\"),\n" +
                "PARTITION p3 VALUES LESS THAN (\"2014-12-01\")\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 32\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"dynamic_partition.enable\" = \"true\",\n" +
                "\"dynamic_partition.start\" = \"-3\",\n" +
                "\"dynamic_partition.end\" = \"3\",\n" +
                "\"dynamic_partition.buckets\" = \"3\",\n" +
                "\"dynamic_partition.time_unit\" = \"day\",\n" +
                "\"dynamic_partition.time_zone\" = \"Asia/Shanghai\",\n" +
                "\"dynamic_partition.prefix\" = \"p\"\n" +
                ");";
        createTable(createOlapTblStmt);
    }

    @Test
    public void testInvalidTimeZone() throws Exception {
        String createOlapTblStmt = "CREATE TABLE test.`dynamic_partition_invalid_time_zone` (\n" +
                "  `k1` date NULL COMMENT \"\",\n" +
                "  `k2` int NULL COMMENT \"\",\n" +
                "  `k3` smallint NULL COMMENT \"\",\n" +
                "  `v1` varchar(2048) NULL COMMENT \"\",\n" +
                "  `v2` datetime NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`, `k2`, `k3`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE (k1)\n" +
                "(\n" +
                "PARTITION p1 VALUES LESS THAN (\"2014-01-01\"),\n" +
                "PARTITION p2 VALUES LESS THAN (\"2014-06-01\"),\n" +
                "PARTITION p3 VALUES LESS THAN (\"2014-12-01\")\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 32\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"dynamic_partition.enable\" = \"true\",\n" +
                "\"dynamic_partition.start\" = \"-3\",\n" +
                "\"dynamic_partition.end\" = \"3\",\n" +
                "\"dynamic_partition.buckets\" = \"3\",\n" +
                "\"dynamic_partition.time_unit\" = \"day\",\n" +
                "\"dynamic_partition.time_zone\" = \"invalid\",\n" +
                "\"dynamic_partition.prefix\" = \"p\"\n" +
                ");";
        expectedException.expect(DdlException.class);
        expectedException.expectMessage("errCode = 2, detailMessage = Unknown or incorrect time zone: 'invalid'");
        createTable(createOlapTblStmt);
    }

    @Test
    public void testSetDynamicPartitionReplicationNum() throws Exception {
        String tableName = "dynamic_partition_replication_num";
        String createOlapTblStmt = "CREATE TABLE test.`" + tableName + "` (\n" +
                "  `k1` date NULL COMMENT \"\",\n" +
                "  `k2` int NULL COMMENT \"\",\n" +
                "  `k3` smallint NULL COMMENT \"\",\n" +
                "  `v1` varchar(2048) NULL COMMENT \"\",\n" +
                "  `v2` datetime NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`, `k2`, `k3`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE (k1)\n" +
                "(\n" +
                "PARTITION p1 VALUES LESS THAN (\"2014-01-01\"),\n" +
                "PARTITION p2 VALUES LESS THAN (\"2014-06-01\"),\n" +
                "PARTITION p3 VALUES LESS THAN (\"2014-12-01\")\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 32\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"dynamic_partition.enable\" = \"true\",\n" +
                "\"dynamic_partition.start\" = \"-3\",\n" +
                "\"dynamic_partition.end\" = \"3\",\n" +
                "\"dynamic_partition.time_unit\" = \"day\",\n" +
                "\"dynamic_partition.prefix\" = \"p\",\n" +
                "\"dynamic_partition.buckets\" = \"1\",\n" +
                "\"dynamic_partition.replication_num\" = \"2\"\n" +
                ");";
        createTable(createOlapTblStmt);
        Database db = Catalog.getCurrentCatalog().getDbOrAnalysisException("default_cluster:test");
        OlapTable table = (OlapTable) db.getTableOrAnalysisException(tableName);
        Assert.assertEquals(2, table.getTableProperty().getDynamicPartitionProperty().getReplicaAllocation().getTotalReplicaNum());

        String alter1 = "alter table test.dynamic_partition_replication_num set ('dynamic_partition.replication_num' = '1')";
        ExceptionChecker.expectThrowsNoException(() -> alterTable(alter1));
        Assert.assertEquals(1, table.getTableProperty().getDynamicPartitionProperty().getReplicaAllocation().getTotalReplicaNum());

        String alter2 = "alter table test.dynamic_partition_replication_num set ('dynamic_partition.replication_num' = '0')";
        ExceptionChecker.expectThrows(AnalysisException.class, () -> alterTable(alter2));
        Assert.assertEquals(1, table.getTableProperty().getDynamicPartitionProperty().getReplicaAllocation().getTotalReplicaNum());
    }

    @Test
    public void testCreateDynamicPartitionImmediately() throws Exception {
        String createOlapTblStmt = "CREATE TABLE test.`empty_dynamic_partition` (\n" +
                "  `k1` date NULL COMMENT \"\",\n" +
                "  `k2` int NULL COMMENT \"\",\n" +
                "  `k3` smallint NULL COMMENT \"\",\n" +
                "  `v1` varchar(2048) NULL COMMENT \"\",\n" +
                "  `v2` datetime NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`, `k2`, `k3`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE(`k1`)\n" +
                "()\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 32\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"dynamic_partition.enable\" = \"true\",\n" +
                "\"dynamic_partition.start\" = \"-3\",\n" +
                "\"dynamic_partition.end\" = \"3\",\n" +
                "\"dynamic_partition.time_unit\" = \"day\",\n" +
                "\"dynamic_partition.prefix\" = \"p\",\n" +
                "\"dynamic_partition.buckets\" = \"1\"\n" +
                ");";
        createTable(createOlapTblStmt);
        OlapTable emptyDynamicTable = (OlapTable) Catalog.getCurrentCatalog().getDbOrAnalysisException("default_cluster:test").getTableOrAnalysisException("empty_dynamic_partition");
        Assert.assertTrue(emptyDynamicTable.getAllPartitions().size() == 4);

        Iterator<Partition> partitionIterator = emptyDynamicTable.getAllPartitions().iterator();
        List<String> partNames = Lists.newArrayList();
        while (partitionIterator.hasNext()) {
            String partitionName = partitionIterator.next().getName();
            partNames.add(partitionName.substring(1));
        }
        Collections.sort(partNames);

        int partitionCount = 0;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        for (String partName : partNames) {
            Date partitionDate = sdf.parse(partName);
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
        String createOlapTblStmt = "CREATE TABLE test.`histo_dynamic_partition` (\n" +
                "  `k1` date NULL COMMENT \"\",\n" +
                "  `k2` int NULL COMMENT \"\",\n" +
                "  `k3` smallint NULL COMMENT \"\",\n" +
                "  `v1` varchar(2048) NULL COMMENT \"\",\n" +
                "  `v2` datetime NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`, `k2`, `k3`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE(`k1`)\n" +
                "()\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 32\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"dynamic_partition.enable\" = \"true\",\n" +
                "\"dynamic_partition.start\" = \"-3\",\n" +
                "\"dynamic_partition.end\" = \"3\",\n" +
                "\"dynamic_partition.create_history_partition\" = \"true\",\n" +
                "\"dynamic_partition.time_unit\" = \"day\",\n" +
                "\"dynamic_partition.prefix\" = \"p\",\n" +
                "\"dynamic_partition.buckets\" = \"1\"\n" +
                ");";
        createTable(createOlapTblStmt);
        OlapTable emptyDynamicTable = (OlapTable) Catalog.getCurrentCatalog().getDbOrAnalysisException("default_cluster:test").getTableOrAnalysisException("histo_dynamic_partition");
        Assert.assertEquals(7, emptyDynamicTable.getAllPartitions().size());

        Iterator<Partition> partitionIterator = emptyDynamicTable.getAllPartitions().iterator();
        List<String> partNames = Lists.newArrayList();
        while (partitionIterator.hasNext()) {
            String partitionName = partitionIterator.next().getName();
            partNames.add(partitionName.substring(1));
        }
        Collections.sort(partNames);

        int partitionCount = -3;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        for (String partName : partNames) {
            Date partitionDate = sdf.parse(partName);
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
        String createOlapTblStmt = "CREATE TABLE test.`histo_dynamic_partition2` (\n" +
                "  `k1` date NULL COMMENT \"\",\n" +
                "  `k2` int NULL COMMENT \"\",\n" +
                "  `k3` smallint NULL COMMENT \"\",\n" +
                "  `v1` varchar(2048) NULL COMMENT \"\",\n" +
                "  `v2` datetime NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`, `k2`, `k3`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE(`k1`)\n" +
                "()\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 32\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"dynamic_partition.enable\" = \"true\",\n" +
                "\"dynamic_partition.start\" = \"-3000\",\n" +
                "\"dynamic_partition.end\" = \"3\",\n" +
                "\"dynamic_partition.create_history_partition\" = \"true\",\n" +
                "\"dynamic_partition.time_unit\" = \"day\",\n" +
                "\"dynamic_partition.prefix\" = \"p\",\n" +
                "\"dynamic_partition.buckets\" = \"1\"\n" +
                ");";
        // exceed the max dynamic partition limit
        Config.max_dynamic_partition_num = 1000;
        createTable(createOlapTblStmt);
    }

    @Test
    public void testFillHistoryDynamicPartition3() throws Exception {
        String createOlapTblStmt = "CREATE TABLE test.`dynamic_partition3` (\n" +
                "  `k1` date NULL COMMENT \"\"\n" +
                ")\n" +
                "PARTITION BY RANGE (k1)\n" +
                "()\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"dynamic_partition.enable\" = \"true\",\n" +
                "\"dynamic_partition.end\" = \"3\",\n" +
                "\"dynamic_partition.time_unit\" = \"day\",\n" +
                "\"dynamic_partition.prefix\" = \"p\",\n" +
                "\"dynamic_partition.buckets\" = \"1\",\n" +
                "\"dynamic_partition.create_history_partition\" = \"true\"\n" +
                ");";
        // start and history_partition_num are not set, can not create history partition
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "Provide start or history_partition_num property when creating history partition",
                () -> createTable(createOlapTblStmt));

        String createOlapTblStmt2 = "CREATE TABLE test.`dynamic_partition3` (\n" +
                "  `k1` date NULL COMMENT \"\"\n" +
                ")\n" +
                "PARTITION BY RANGE (k1)\n" +
                "()\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"dynamic_partition.enable\" = \"true\",\n" +
                "\"dynamic_partition.end\" = \"3\",\n" +
                "\"dynamic_partition.time_unit\" = \"day\",\n" +
                "\"dynamic_partition.prefix\" = \"p\",\n" +
                "\"dynamic_partition.buckets\" = \"1\",\n" +
                "\"dynamic_partition.history_partition_num\" = \"1000\",\n" +
                "\"dynamic_partition.create_history_partition\" = \"true\"\n" +
                ");";
        // start is not set, but history_partition_num is set too large, can not create history partition
        ExceptionChecker.expectThrowsWithMsg(DdlException.class, "Too many dynamic partitions", () -> createTable(createOlapTblStmt2));

        String createOlapTblStmt3 = "CREATE TABLE test.`dynamic_partition3` (\n" +
                "  `k1` date NULL COMMENT \"\"\n" +
                ")\n" +
                "PARTITION BY RANGE (k1)\n" +
                "()\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"dynamic_partition.enable\" = \"true\",\n" +
                "\"dynamic_partition.end\" = \"3\",\n" +
                "\"dynamic_partition.time_unit\" = \"day\",\n" +
                "\"dynamic_partition.prefix\" = \"p\",\n" +
                "\"dynamic_partition.buckets\" = \"1\",\n" +
                "\"dynamic_partition.start\" = \"-1000\",\n" +
                "\"dynamic_partition.create_history_partition\" = \"true\"\n" +
                ");";
        // start is set but too small,history_partition_num is not set, can not create history partition
        ExceptionChecker.expectThrowsWithMsg(DdlException.class, "Too many dynamic partitions", () -> createTable(createOlapTblStmt3));

        String createOlapTblStmt4 = "CREATE TABLE test.`dynamic_partition3` (\n" +
                "  `k1` date NULL COMMENT \"\"\n" +
                ")\n" +
                "PARTITION BY RANGE (k1)\n" +
                "()\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"dynamic_partition.enable\" = \"true\",\n" +
                "\"dynamic_partition.end\" = \"3\",\n" +
                "\"dynamic_partition.time_unit\" = \"day\",\n" +
                "\"dynamic_partition.prefix\" = \"p\",\n" +
                "\"dynamic_partition.buckets\" = \"1\",\n" +
                "\"dynamic_partition.start\" = \"-10\",\n" +
                "\"dynamic_partition.history_partition_num\" = \"5\",\n" +
                "\"dynamic_partition.create_history_partition\" = \"true\"\n" +
                ");";
        // start and history_partition_num are set, create ok
        ExceptionChecker.expectThrowsNoException(() -> createTable(createOlapTblStmt4));
        Database db = Catalog.getCurrentCatalog().getDbOrAnalysisException("default_cluster:test");
        OlapTable tbl = (OlapTable) db.getTableOrAnalysisException("dynamic_partition3");
        Assert.assertEquals(9, tbl.getPartitionNames().size());

        // alter dynamic partition property of table dynamic_partition3
        // start too small
        String alter1 = "alter table test.dynamic_partition3 set ('dynamic_partition.start' = '-1000', 'dynamic_partition.history_partition_num' = '1000')";
        ExceptionChecker.expectThrowsWithMsg(DdlException.class, "Too many dynamic partitions", () -> alterTable(alter1));

        // end too large
        String alter2 = "alter table test.dynamic_partition3 set ('dynamic_partition.end' = '1000')";
        ExceptionChecker.expectThrowsWithMsg(DdlException.class, "Too many dynamic partitions", () -> alterTable(alter2));

        // history_partition_num too large, but because start is -10, so modify ok
        String alter3 = "alter table test.dynamic_partition3 set ('dynamic_partition.history_partition_num' = '1000')";
        ExceptionChecker.expectThrowsNoException(() -> alterTable(alter3));
        Assert.assertEquals(14, tbl.getPartitionNames().size());

        // set start and history_partition_num properly.
        String alter4 = "alter table test.dynamic_partition3 set ('dynamic_partition.history_partition_num' = '100', 'dynamic_partition.start' = '-20')";
        ExceptionChecker.expectThrowsNoException(() -> alterTable(alter4));
        Assert.assertEquals(24, tbl.getPartitionNames().size());
    }

    @Test
    public void testFillHistoryDynamicPartitionWithHistoryPartitionNum() throws Exception {
        String createOlapTblStmt = "CREATE TABLE test.`history_dynamic_partition_day` (\n" +
                "  `k1` datetime NULL COMMENT \"\",\n" +
                "  `k2` int NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "PARTITION BY RANGE(`k1`)\n" +
                "()\n" +
                "DISTRIBUTED BY HASH(`k2`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"dynamic_partition.enable\" = \"true\",\n" +
                "\"dynamic_partition.end\" = \"3\",\n" +
                "\"dynamic_partition.create_history_partition\" = \"true\",\n" +
                "\"dynamic_partition.history_partition_num\" = \"10\",\n" +
                "\"dynamic_partition.time_unit\" = \"day\",\n" +
                "\"dynamic_partition.prefix\" = \"p\",\n" +
                "\"dynamic_partition.buckets\" = \"1\"\n" +
                ");";
        createTable(createOlapTblStmt);
        OlapTable emptyDynamicTable = (OlapTable) Catalog.getCurrentCatalog().getDbOrAnalysisException("default_cluster:test").getTableOrAnalysisException("history_dynamic_partition_day");
        Map<String, String> tableProperties = emptyDynamicTable.getTableProperty().getProperties();
        Assert.assertEquals(14, emptyDynamicTable.getAllPartitions().size());
        // never delete the old partitions
        Assert.assertEquals(Integer.parseInt(tableProperties.get("dynamic_partition.start")), Integer.MIN_VALUE);
    }

    @Test
    public void testAllTypeDynamicPartition() throws Exception {
        String createOlapTblStmt = "CREATE TABLE test.`hour_dynamic_partition` (\n" +
                "  `k1` datetime NULL COMMENT \"\",\n" +
                "  `k2` int NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "PARTITION BY RANGE(`k1`)\n" +
                "()\n" +
                "DISTRIBUTED BY HASH(`k2`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"dynamic_partition.enable\" = \"true\",\n" +
                "\"dynamic_partition.start\" = \"-3\",\n" +
                "\"dynamic_partition.end\" = \"3\",\n" +
                "\"dynamic_partition.create_history_partition\" = \"true\",\n" +
                "\"dynamic_partition.time_unit\" = \"hour\",\n" +
                "\"dynamic_partition.prefix\" = \"p\",\n" +
                "\"dynamic_partition.buckets\" = \"1\"\n" +
                ");";
        createTable(createOlapTblStmt);
        OlapTable emptyDynamicTable = (OlapTable) Catalog.getCurrentCatalog().getDbOrAnalysisException("default_cluster:test").getTableOrAnalysisException("hour_dynamic_partition");
        Assert.assertEquals(7, emptyDynamicTable.getAllPartitions().size());

        Iterator<Partition> partitionIterator = emptyDynamicTable.getAllPartitions().iterator();
        while (partitionIterator.hasNext()) {
            String partitionName = partitionIterator.next().getName();
            Assert.assertEquals(11, partitionName.length());
        }

        createOlapTblStmt = "CREATE TABLE test.`week_dynamic_partition` (\n" +
                "  `k1` datetime NULL COMMENT \"\",\n" +
                "  `k2` int NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "PARTITION BY RANGE(`k1`)\n" +
                "()\n" +
                "DISTRIBUTED BY HASH(`k2`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"dynamic_partition.enable\" = \"true\",\n" +
                "\"dynamic_partition.start\" = \"-3\",\n" +
                "\"dynamic_partition.end\" = \"3\",\n" +
                "\"dynamic_partition.create_history_partition\" = \"true\",\n" +
                "\"dynamic_partition.time_unit\" = \"week\",\n" +
                "\"dynamic_partition.prefix\" = \"p\",\n" +
                "\"dynamic_partition.buckets\" = \"1\"\n" +
                ");";
        createTable(createOlapTblStmt);
        emptyDynamicTable = (OlapTable) Catalog.getCurrentCatalog().getDbOrAnalysisException("default_cluster:test").getTableOrAnalysisException("week_dynamic_partition");
        Assert.assertEquals(7, emptyDynamicTable.getAllPartitions().size());

        partitionIterator = emptyDynamicTable.getAllPartitions().iterator();
        while (partitionIterator.hasNext()) {
            String partitionName = partitionIterator.next().getName();
            Assert.assertEquals(8, partitionName.length());
        }

        createOlapTblStmt = "CREATE TABLE test.`month_dynamic_partition` (\n" +
                "  `k1` datetime NULL COMMENT \"\",\n" +
                "  `k2` int NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "PARTITION BY RANGE(`k1`)\n" +
                "()\n" +
                "DISTRIBUTED BY HASH(`k2`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"dynamic_partition.enable\" = \"true\",\n" +
                "\"dynamic_partition.start\" = \"-3\",\n" +
                "\"dynamic_partition.end\" = \"3\",\n" +
                "\"dynamic_partition.create_history_partition\" = \"true\",\n" +
                "\"dynamic_partition.time_unit\" = \"month\",\n" +
                "\"dynamic_partition.prefix\" = \"p\",\n" +
                "\"dynamic_partition.buckets\" = \"1\"\n" +
                ");";
        createTable(createOlapTblStmt);
        emptyDynamicTable = (OlapTable) Catalog.getCurrentCatalog().getDbOrAnalysisException("default_cluster:test").getTableOrAnalysisException("month_dynamic_partition");
        Assert.assertEquals(7, emptyDynamicTable.getAllPartitions().size());

        partitionIterator = emptyDynamicTable.getAllPartitions().iterator();
        while (partitionIterator.hasNext()) {
            String partitionName = partitionIterator.next().getName();
            Assert.assertEquals(7, partitionName.length());
        }

        createOlapTblStmt = "CREATE TABLE test.`int_dynamic_partition_day` (\n" +
                "  `k1` int NULL COMMENT \"\",\n" +
                "  `k2` int NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "PARTITION BY RANGE(`k1`)\n" +
                "()\n" +
                "DISTRIBUTED BY HASH(`k2`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"dynamic_partition.enable\" = \"true\",\n" +
                "\"dynamic_partition.start\" = \"-3\",\n" +
                "\"dynamic_partition.end\" = \"3\",\n" +
                "\"dynamic_partition.create_history_partition\" = \"true\",\n" +
                "\"dynamic_partition.time_unit\" = \"day\",\n" +
                "\"dynamic_partition.prefix\" = \"p\",\n" +
                "\"dynamic_partition.buckets\" = \"1\"\n" +
                ");";
        createTable(createOlapTblStmt);
        emptyDynamicTable = (OlapTable) Catalog.getCurrentCatalog().getDbOrAnalysisException("default_cluster:test").getTableOrAnalysisException("int_dynamic_partition_day");
        Assert.assertEquals(7, emptyDynamicTable.getAllPartitions().size());

        partitionIterator = emptyDynamicTable.getAllPartitions().iterator();
        while (partitionIterator.hasNext()) {
            String partitionName = partitionIterator.next().getName();
            Assert.assertEquals(9, partitionName.length());
        }

        createOlapTblStmt = "CREATE TABLE test.`int_dynamic_partition_week` (\n" +
                "  `k1` int NULL COMMENT \"\",\n" +
                "  `k2` int NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "PARTITION BY RANGE(`k1`)\n" +
                "()\n" +
                "DISTRIBUTED BY HASH(`k2`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"dynamic_partition.enable\" = \"true\",\n" +
                "\"dynamic_partition.start\" = \"-3\",\n" +
                "\"dynamic_partition.end\" = \"3\",\n" +
                "\"dynamic_partition.create_history_partition\" = \"true\",\n" +
                "\"dynamic_partition.time_unit\" = \"week\",\n" +
                "\"dynamic_partition.prefix\" = \"p\",\n" +
                "\"dynamic_partition.buckets\" = \"1\"\n" +
                ");";
        createTable(createOlapTblStmt);
        emptyDynamicTable = (OlapTable) Catalog.getCurrentCatalog().getDbOrAnalysisException("default_cluster:test").getTableOrAnalysisException("int_dynamic_partition_week");
        Assert.assertEquals(7, emptyDynamicTable.getAllPartitions().size());

        partitionIterator = emptyDynamicTable.getAllPartitions().iterator();
        while (partitionIterator.hasNext()) {
            String partitionName = partitionIterator.next().getName();
            Assert.assertEquals(8, partitionName.length());
        }

        createOlapTblStmt = "CREATE TABLE test.`int_dynamic_partition_month` (\n" +
                "  `k1` int NULL COMMENT \"\",\n" +
                "  `k2` int NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "PARTITION BY RANGE(`k1`)\n" +
                "()\n" +
                "DISTRIBUTED BY HASH(`k2`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"dynamic_partition.enable\" = \"true\",\n" +
                "\"dynamic_partition.start\" = \"-3\",\n" +
                "\"dynamic_partition.end\" = \"3\",\n" +
                "\"dynamic_partition.create_history_partition\" = \"true\",\n" +
                "\"dynamic_partition.time_unit\" = \"month\",\n" +
                "\"dynamic_partition.prefix\" = \"p\",\n" +
                "\"dynamic_partition.buckets\" = \"1\"\n" +
                ");";
        createTable(createOlapTblStmt);
        emptyDynamicTable = (OlapTable) Catalog.getCurrentCatalog().getDbOrAnalysisException("default_cluster:test").getTableOrAnalysisException("int_dynamic_partition_month");
        Assert.assertEquals(7, emptyDynamicTable.getAllPartitions().size());

        partitionIterator = emptyDynamicTable.getAllPartitions().iterator();
        while (partitionIterator.hasNext()) {
            String partitionName = partitionIterator.next().getName();
            Assert.assertEquals(7, partitionName.length());
        }
    }

    @Test(expected = DdlException.class)
    public void testHourDynamicPartitionWithIntType() throws Exception {
        String createOlapTblStmt = "CREATE TABLE test.`int_dynamic_partition_hour` (\n" +
                "  `k1` int NULL COMMENT \"\",\n" +
                "  `k2` int NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "PARTITION BY RANGE(`k1`)\n" +
                "()\n" +
                "DISTRIBUTED BY HASH(`k2`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"dynamic_partition.enable\" = \"true\",\n" +
                "\"dynamic_partition.start\" = \"-3\",\n" +
                "\"dynamic_partition.end\" = \"3\",\n" +
                "\"dynamic_partition.create_history_partition\" = \"true\",\n" +
                "\"dynamic_partition.time_unit\" = \"hour\",\n" +
                "\"dynamic_partition.prefix\" = \"p\",\n" +
                "\"dynamic_partition.buckets\" = \"1\"\n" +
                ");";
        createTable(createOlapTblStmt);
    }

    @Test
    public void testHotPartitionNum() throws Exception {
        Database testDb = Catalog.getCurrentCatalog().getDbOrAnalysisException("default_cluster:test");
        // 1. hour
        String createOlapTblStmt = "CREATE TABLE test.`hot_partition_hour_tbl1` (\n" +
                "  `k1` datetime NULL COMMENT \"\",\n" +
                "  `k2` int NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "PARTITION BY RANGE(`k1`)\n" +
                "()\n" +
                "DISTRIBUTED BY HASH(`k2`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"dynamic_partition.enable\" = \"true\",\n" +
                "\"dynamic_partition.start\" = \"-3\",\n" +
                "\"dynamic_partition.end\" = \"3\",\n" +
                "\"dynamic_partition.create_history_partition\" = \"true\",\n" +
                "\"dynamic_partition.time_unit\" = \"hour\",\n" +
                "\"dynamic_partition.prefix\" = \"p\",\n" +
                "\"dynamic_partition.buckets\" = \"1\",\n" +
                "\"dynamic_partition.hot_partition_num\" = \"1\"\n" +
                ");";
        createTable(createOlapTblStmt);
        OlapTable tbl = (OlapTable)testDb.getTableOrAnalysisException("hot_partition_hour_tbl1");
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

        createOlapTblStmt = "CREATE TABLE test.`hot_partition_hour_tbl2` (\n" +
                "  `k1` datetime NULL COMMENT \"\",\n" +
                "  `k2` int NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "PARTITION BY RANGE(`k1`)\n" +
                "()\n" +
                "DISTRIBUTED BY HASH(`k2`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"dynamic_partition.enable\" = \"true\",\n" +
                "\"dynamic_partition.start\" = \"-3\",\n" +
                "\"dynamic_partition.end\" = \"3\",\n" +
                "\"dynamic_partition.create_history_partition\" = \"true\",\n" +
                "\"dynamic_partition.time_unit\" = \"hour\",\n" +
                "\"dynamic_partition.prefix\" = \"p\",\n" +
                "\"dynamic_partition.buckets\" = \"1\",\n" +
                "\"dynamic_partition.hot_partition_num\" = \"0\"\n" +
                ");";
        createTable(createOlapTblStmt);
        tbl = (OlapTable)testDb.getTableOrAnalysisException("hot_partition_hour_tbl2");
        partitionInfo = (RangePartitionInfo) tbl.getPartitionInfo();
        idToDataProperty = new TreeMap<>(partitionInfo.idToDataProperty);
        Assert.assertEquals(7, idToDataProperty.size());
        for (DataProperty dataProperty : idToDataProperty.values()) {
            Assert.assertEquals(TStorageMedium.HDD, dataProperty.getStorageMedium());
        }

        createOlapTblStmt = "CREATE TABLE test.`hot_partition_hour_tbl3` (\n" +
                "  `k1` datetime NULL COMMENT \"\",\n" +
                "  `k2` int NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "PARTITION BY RANGE(`k1`)\n" +
                "()\n" +
                "DISTRIBUTED BY HASH(`k2`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"dynamic_partition.enable\" = \"true\",\n" +
                "\"dynamic_partition.start\" = \"-3\",\n" +
                "\"dynamic_partition.end\" = \"3\",\n" +
                "\"dynamic_partition.create_history_partition\" = \"true\",\n" +
                "\"dynamic_partition.time_unit\" = \"hour\",\n" +
                "\"dynamic_partition.prefix\" = \"p\",\n" +
                "\"dynamic_partition.buckets\" = \"1\",\n" +
                "\"dynamic_partition.hot_partition_num\" = \"3\"\n" +
                ");";
        createTable(createOlapTblStmt);
        tbl = (OlapTable)testDb.getTableOrAnalysisException("hot_partition_hour_tbl3");
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
        createOlapTblStmt = "CREATE TABLE test.`hot_partition_day_tbl1` (\n" +
                "  `k1` datetime NULL COMMENT \"\",\n" +
                "  `k2` int NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "PARTITION BY RANGE(`k1`)\n" +
                "()\n" +
                "DISTRIBUTED BY HASH(`k2`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"dynamic_partition.enable\" = \"true\",\n" +
                "\"dynamic_partition.start\" = \"-3\",\n" +
                "\"dynamic_partition.end\" = \"3\",\n" +
                "\"dynamic_partition.time_unit\" = \"day\",\n" +
                "\"dynamic_partition.prefix\" = \"p\",\n" +
                "\"dynamic_partition.buckets\" = \"1\",\n" +
                "\"dynamic_partition.hot_partition_num\" = \"2\"\n" +
                ");";
        createTable(createOlapTblStmt);
        tbl = (OlapTable)testDb.getTableOrAnalysisException("hot_partition_day_tbl1");
        partitionInfo = (RangePartitionInfo) tbl.getPartitionInfo();
        idToDataProperty = new TreeMap<>(partitionInfo.idToDataProperty);
        Assert.assertEquals(4, idToDataProperty.size());
        for (DataProperty dataProperty : idToDataProperty.values()) {
            Assert.assertEquals(TStorageMedium.SSD, dataProperty.getStorageMedium());
        }

        createOlapTblStmt = "CREATE TABLE test.`hot_partition_day_tbl2` (\n" +
                "  `k1` datetime NULL COMMENT \"\",\n" +
                "  `k2` int NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "PARTITION BY RANGE(`k1`)\n" +
                "()\n" +
                "DISTRIBUTED BY HASH(`k2`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"dynamic_partition.enable\" = \"true\",\n" +
                "\"dynamic_partition.start\" = \"-3\",\n" +
                "\"dynamic_partition.end\" = \"4\",\n" +
                "\"dynamic_partition.create_history_partition\" = \"true\",\n" +
                "\"dynamic_partition.time_unit\" = \"day\",\n" +
                "\"dynamic_partition.prefix\" = \"p\",\n" +
                "\"dynamic_partition.buckets\" = \"1\",\n" +
                "\"dynamic_partition.hot_partition_num\" = \"2\"\n" +
                ");";
        createTable(createOlapTblStmt);
        tbl = (OlapTable)testDb.getTableOrAnalysisException("hot_partition_day_tbl2");
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
        createOlapTblStmt = "CREATE TABLE test.`hot_partition_week_tbl1` (\n" +
                "  `k1` datetime NULL COMMENT \"\",\n" +
                "  `k2` int NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "PARTITION BY RANGE(`k1`)\n" +
                "()\n" +
                "DISTRIBUTED BY HASH(`k2`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"dynamic_partition.enable\" = \"true\",\n" +
                "\"dynamic_partition.start\" = \"-3\",\n" +
                "\"dynamic_partition.end\" = \"4\",\n" +
                "\"dynamic_partition.create_history_partition\" = \"true\",\n" +
                "\"dynamic_partition.time_unit\" = \"week\",\n" +
                "\"dynamic_partition.prefix\" = \"p\",\n" +
                "\"dynamic_partition.buckets\" = \"1\",\n" +
                "\"dynamic_partition.hot_partition_num\" = \"1\"\n" +
                ");";
        createTable(createOlapTblStmt);
        tbl = (OlapTable)testDb.getTableOrAnalysisException("hot_partition_week_tbl1");
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
        createOlapTblStmt = "CREATE TABLE test.`hot_partition_month_tbl1` (\n" +
                "  `k1` datetime NULL COMMENT \"\",\n" +
                "  `k2` int NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "PARTITION BY RANGE(`k1`)\n" +
                "()\n" +
                "DISTRIBUTED BY HASH(`k2`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"dynamic_partition.enable\" = \"true\",\n" +
                "\"dynamic_partition.start\" = \"-3\",\n" +
                "\"dynamic_partition.end\" = \"4\",\n" +
                "\"dynamic_partition.create_history_partition\" = \"true\",\n" +
                "\"dynamic_partition.time_unit\" = \"month\",\n" +
                "\"dynamic_partition.prefix\" = \"p\",\n" +
                "\"dynamic_partition.buckets\" = \"1\",\n" +
                "\"dynamic_partition.hot_partition_num\" = \"4\"\n" +
                ");";
        createTable(createOlapTblStmt);
        tbl = (OlapTable)testDb.getTableOrAnalysisException("hot_partition_month_tbl1");
        partitionInfo = (RangePartitionInfo) tbl.getPartitionInfo();
        idToDataProperty = new TreeMap<>(partitionInfo.idToDataProperty);
        Assert.assertEquals(8, idToDataProperty.size());
        for (DataProperty dataProperty : idToDataProperty.values()) {
            Assert.assertEquals(TStorageMedium.SSD, dataProperty.getStorageMedium());
        }
    }

    @Test(expected = DdlException.class)
    public void testHotPartitionNumAbnormal() throws Exception {
        // dynamic_partition.hot_partition_num must larger than 0.
        String createOlapTblStmt = "CREATE TABLE test.`hot_partition_hour_tbl1` (\n" +
                "  `k1` datetime NULL COMMENT \"\",\n" +
                "  `k2` int NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "PARTITION BY RANGE(`k1`)\n" +
                "()\n" +
                "DISTRIBUTED BY HASH(`k2`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"dynamic_partition.enable\" = \"true\",\n" +
                "\"dynamic_partition.start\" = \"-3\",\n" +
                "\"dynamic_partition.end\" = \"3\",\n" +
                "\"dynamic_partition.create_history_partition\" = \"true\",\n" +
                "\"dynamic_partition.time_unit\" = \"hour\",\n" +
                "\"dynamic_partition.prefix\" = \"p\",\n" +
                "\"dynamic_partition.buckets\" = \"1\",\n" +
                "\"dynamic_partition.hot_partition_num\" = \"-1\"\n" +
                ");";
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
        String createOlapTblStmt = "CREATE TABLE test.`dynamic_partition_miss_reserved_history_periods` (\n" +
                "  `k1` date NULL COMMENT \"\",\n" +
                "  `k2` int NULL COMMENT \"\",\n" +
                "  `k3` smallint NULL COMMENT \"\",\n" +
                "  `v1` varchar(2048) NULL COMMENT \"\",\n" +
                "  `v2` datetime NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`, `k2`, `k3`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE (k1)\n" +
                "(\n" +
                "PARTITION p1 VALUES LESS THAN (\"2014-01-01\"),\n" +
                "PARTITION p2 VALUES LESS THAN (\"2014-06-01\"),\n" +
                "PARTITION p3 VALUES LESS THAN (\"2014-12-01\")\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 32\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"dynamic_partition.enable\" = \"true\",\n" +
                "\"dynamic_partition.start\" = \"-3\",\n" +
                "\"dynamic_partition.end\" = \"3\",\n" +
                "\"dynamic_partition.buckets\" = \"3\",\n" +
                "\"dynamic_partition.time_unit\" = \"day\",\n" +
                "\"dynamic_partition.prefix\" = \"p\"\n" +
                ");";
        createTable(createOlapTblStmt);
        OlapTable table = (OlapTable) Catalog.getCurrentCatalog().getDbOrAnalysisException("default_cluster:test").getTableOrAnalysisException("dynamic_partition_miss_reserved_history_periods");
        Assert.assertEquals("NULL", table.getTableProperty().getDynamicPartitionProperty().getReservedHistoryPeriods());
    }

    @Test
    public void testNormalReservedHisrotyPeriods() throws Exception {
        String createOlapTblStmt = "CREATE TABLE test.`dynamic_partition_normal_reserved_history_periods` (\n" +
                "  `k1` date NULL COMMENT \"\",\n" +
                "  `k2` int NULL COMMENT \"\",\n" +
                "  `k3` smallint NULL COMMENT \"\",\n" +
                "  `v1` varchar(2048) NULL COMMENT \"\",\n" +
                "  `v2` datetime NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`, `k2`, `k3`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE (k1)\n" +
                "(\n" +
                "PARTITION p1 VALUES LESS THAN (\"2014-01-01\"),\n" +
                "PARTITION p2 VALUES LESS THAN (\"2014-06-01\"),\n" +
                "PARTITION p3 VALUES LESS THAN (\"2014-12-01\"),\n" +
                "PARTITION p4 VALUES LESS THAN (\"2020-06-01\"),\n" +
                "PARTITION p5 VALUES LESS THAN (\"2020-06-20\"),\n" +
                "PARTITION p6 VALUES LESS THAN (\"2020-10-25\"),\n" +
                "PARTITION p7 VALUES LESS THAN (\"2020-11-01\"),\n" +
                "PARTITION p8 VALUES LESS THAN (\"2020-11-11\"),\n" +
                "PARTITION p9 VALUES LESS THAN (\"2020-11-21\"),\n" +
                "PARTITION p10 VALUES LESS THAN (\"2021-04-20\"),\n" +
                "PARTITION p11 VALUES LESS THAN (\"2021-05-20\")\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 32\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"dynamic_partition.enable\" = \"true\",\n" +
                "\"dynamic_partition.start\" = \"-3\",\n" +
                "\"dynamic_partition.end\" = \"3\",\n" +
                "\"dynamic_partition.buckets\" = \"3\",\n" +
                "\"dynamic_partition.time_unit\" = \"day\",\n" +
                "\"dynamic_partition.prefix\" = \"p\",\n" +
                "\"dynamic_partition.reserved_history_periods\" = \"[2020-06-01,2020-06-20],[2020-10-25,2020-11-15],[2021-06-01,2021-06-20]\"\n" +
                ");";
        createTable(createOlapTblStmt);
        OlapTable table = (OlapTable) Catalog.getCurrentCatalog().getDbOrAnalysisException("default_cluster:test").getTableOrAnalysisException("dynamic_partition_normal_reserved_history_periods");
        Assert.assertEquals("[2020-06-01,2020-06-20],[2020-10-25,2020-11-15],[2021-06-01,2021-06-20]", table.getTableProperty().getDynamicPartitionProperty().getReservedHistoryPeriods());
        Assert.assertEquals(table.getAllPartitions().size(), 9);

        String createOlapTblStmt2 = "CREATE TABLE test.`dynamic_partition_normal_reserved_history_periods2` (\n" +
                "  `k1` datetime NULL COMMENT \"\",\n" +
                "  `k2` int NULL COMMENT \"\",\n" +
                "  `k3` smallint NULL COMMENT \"\",\n" +
                "  `v1` varchar(2048) NULL COMMENT \"\",\n" +
                "  `v2` datetime NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`, `k2`, `k3`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE (k1)\n" +
                "(\n" +
                "PARTITION p1 VALUES LESS THAN (\"2014-01-01 00:00:00\"),\n" +
                "PARTITION p2 VALUES LESS THAN (\"2014-01-01 03:00:00\"),\n" +
                "PARTITION p3 VALUES LESS THAN (\"2014-01-01 04:00:00\"),\n" +
                "PARTITION p4 VALUES LESS THAN (\"2020-01-01 08:00:00\"),\n" +
                "PARTITION p5 VALUES LESS THAN (\"2020-06-20 00:00:00\")\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 32\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"dynamic_partition.enable\" = \"true\",\n" +
                "\"dynamic_partition.start\" = \"-3\",\n" +
                "\"dynamic_partition.end\" = \"3\",\n" +
                "\"dynamic_partition.buckets\" = \"3\",\n" +
                "\"dynamic_partition.time_unit\" = \"hour\",\n" +
                "\"dynamic_partition.prefix\" = \"p\",\n" +
                "\"dynamic_partition.reserved_history_periods\" = \"[2014-01-01 00:00:00,2014-01-01 03:00:00]\"\n" +
                ");";
        createTable(createOlapTblStmt2);
        OlapTable table2 = (OlapTable) Catalog.getCurrentCatalog().getDbOrAnalysisException("default_cluster:test").getTableOrAnalysisException("dynamic_partition_normal_reserved_history_periods2");
        Assert.assertEquals("[2014-01-01 00:00:00,2014-01-01 03:00:00]", table2.getTableProperty().getDynamicPartitionProperty().getReservedHistoryPeriods());
        Assert.assertEquals(table2.getAllPartitions().size(), 6);

        String createOlapTblStmt3 = "CREATE TABLE test.`dynamic_partition_normal_reserved_history_periods3` (\n" +
                "  `k1` int NULL COMMENT \"\",\n" +
                "  `k2` int NULL COMMENT \"\",\n" +
                "  `k3` smallint NULL COMMENT \"\",\n" +
                "  `v1` varchar(2048) NULL COMMENT \"\",\n" +
                "  `v2` datetime NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`, `k2`, `k3`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE (k1)\n" +
                "(\n" +
                "PARTITION p202127 VALUES [(\"20200527\"), (\"20200628\"))\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`k2`) BUCKETS 32\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"dynamic_partition.enable\" = \"true\",\n" +
                "\"dynamic_partition.start\" = \"-3\",\n" +
                "\"dynamic_partition.end\" = \"3\",\n" +
                "\"dynamic_partition.buckets\" = \"1\",\n" +
                "\"dynamic_partition.time_unit\" = \"day\",\n" +
                "\"dynamic_partition.prefix\" = \"p\",\n" +
                "\"dynamic_partition.reserved_history_periods\" = \"[2020-06-01,2020-06-30]\"\n" +
                ");";
        createTable(createOlapTblStmt3);
        OlapTable table3 = (OlapTable) Catalog.getCurrentCatalog().getDbOrAnalysisException("default_cluster:test").getTableOrAnalysisException("dynamic_partition_normal_reserved_history_periods3");
        Assert.assertEquals("[2020-06-01,2020-06-30]", table3.getTableProperty().getDynamicPartitionProperty().getReservedHistoryPeriods());
        Assert.assertEquals(table3.getAllPartitions().size(), 5);
    }

    @Test
    public void testInvalidReservedHistoryPeriods() throws Exception {
        String createOlapTblStmt1 = "CREATE TABLE test.`dynamic_partition_invalid_reserved_history_periods1` (\n" +
                "  `k1` date NULL COMMENT \"\",\n" +
                "  `k2` int NULL COMMENT \"\",\n" +
                "  `k3` smallint NULL COMMENT \"\",\n" +
                "  `v1` varchar(2048) NULL COMMENT \"\",\n" +
                "  `v2` datetime NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`, `k2`, `k3`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE (k1)\n" +
                "(\n" +
                 "PARTITION p1 VALUES LESS THAN (\"2014-01-01\"),\n" +
                "PARTITION p2 VALUES LESS THAN (\"2014-06-01\"),\n" +
                "PARTITION p3 VALUES LESS THAN (\"2014-12-01\")\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 32\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"dynamic_partition.enable\" = \"true\",\n" +
                "\"dynamic_partition.start\" = \"-3\",\n" +
                "\"dynamic_partition.end\" = \"3\",\n" +
                "\"dynamic_partition.buckets\" = \"3\",\n" +
                "\"dynamic_partition.time_unit\" = \"day\",\n" +
                "\"dynamic_partition.prefix\" = \"p\",\n" +
                "\"dynamic_partition.reserved_history_periods\" = \"[20210101,2021-10-10]\"\n" +
                ");";
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "errCode = 2, detailMessage = Invalid \" dynamic_partition.reserved_history_periods \" value [20210101,2021-10-10]. " +
                        "It must be like \"[yyyy-MM-dd,yyyy-MM-dd],[...,...]\" while time_unit is DAY/WEEK/MONTH or " +
                        "\"[yyyy-MM-dd HH:mm:ss,yyyy-MM-dd HH:mm:ss],[...,...]\" while time_unit is HOUR.",
                () -> createTable(createOlapTblStmt1));

        String createOlapTblStmt2 = "CREATE TABLE test.`dynamic_partition_invalid_reserved_history_periods2` (\n" +
                "  `k1` date NULL COMMENT \"\",\n" +
                "  `k2` int NULL COMMENT \"\",\n" +
                "  `k3` smallint NULL COMMENT \"\",\n" +
                "  `v1` varchar(2048) NULL COMMENT \"\",\n" +
                "  `v2` datetime NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`, `k2`, `k3`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE (k1)\n" +
                "(\n" +
                "PARTITION p1 VALUES LESS THAN (\"2014-01-01\"),\n" +
                "PARTITION p2 VALUES LESS THAN (\"2014-06-01\"),\n" +
                "PARTITION p3 VALUES LESS THAN (\"2014-12-01\")\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 32\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"dynamic_partition.enable\" = \"true\",\n" +
                "\"dynamic_partition.start\" = \"-3\",\n" +
                "\"dynamic_partition.end\" = \"3\",\n" +
                "\"dynamic_partition.buckets\" = \"3\",\n" +
                "\"dynamic_partition.time_unit\" = \"day\",\n" +
                "\"dynamic_partition.prefix\" = \"p\",\n" +
                "\"dynamic_partition.reserved_history_periods\" = \"[0000-00-00,2021-10-10]\"\n" +
                ");";
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "errCode = 2, detailMessage = Invalid dynamic_partition.reserved_history_periods value. " +
                        "It must be correct DATE value " +
                        "\"[yyyy-MM-dd,yyyy-MM-dd],[...,...]\" while time_unit is DAY/WEEK/MONTH or " +
                        "\"[yyyy-MM-dd HH:mm:ss,yyyy-MM-dd HH:mm:ss],[...,...]\" while time_unit is HOUR.",
                () -> createTable(createOlapTblStmt2));
    }

    @Test
    public void testReservedHistoryPeriodsValidate() throws Exception {
        String createOlapTblStmt1 = "CREATE TABLE test.`dynamic_partition_reserved_history_periods_validate1` (\n" +
                "  `k1` date NULL COMMENT \"\",\n" +
                "  `k2` int NULL COMMENT \"\",\n" +
                "  `k3` smallint NULL COMMENT \"\",\n" +
                "  `v1` varchar(2048) NULL COMMENT \"\",\n" +
                "  `v2` datetime NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`, `k2`, `k3`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE (k1)\n" +
                "(\n" +
                "PARTITION p1 VALUES LESS THAN (\"2014-01-01\"),\n" +
                "PARTITION p2 VALUES LESS THAN (\"2014-06-01\"),\n" +
                "PARTITION p3 VALUES LESS THAN (\"2014-12-01\")\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 32\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"dynamic_partition.enable\" = \"true\",\n" +
                "\"dynamic_partition.start\" = \"-3\",\n" +
                "\"dynamic_partition.end\" = \"3\",\n" +
                "\"dynamic_partition.buckets\" = \"3\",\n" +
                "\"dynamic_partition.time_unit\" = \"day\",\n" +
                "\"dynamic_partition.prefix\" = \"p\",\n" +
                "\"dynamic_partition.reserved_history_periods\" = \"[2021-01-01,]\"\n" +
                ");";
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "errCode = 2, detailMessage = Invalid \" dynamic_partition.reserved_history_periods \" value [2021-01-01,]. " +
                        "It must be like " +
                        "\"[yyyy-MM-dd,yyyy-MM-dd],[...,...]\" while time_unit is DAY/WEEK/MONTH " +
                        "or \"[yyyy-MM-dd HH:mm:ss,yyyy-MM-dd HH:mm:ss],[...,...]\" while time_unit is HOUR.",
                () -> createTable(createOlapTblStmt1));

        String createOlapTblStmt2 = "CREATE TABLE test.`dynamic_partition_reserved_history_periods_validate2` (\n" +
                "  `k1` date NULL COMMENT \"\",\n" +
                "  `k2` int NULL COMMENT \"\",\n" +
                "  `k3` smallint NULL COMMENT \"\",\n" +
                "  `v1` varchar(2048) NULL COMMENT \"\",\n" +
                "  `v2` datetime NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`, `k2`, `k3`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE (k1)\n" +
                "(\n" +
                "PARTITION p1 VALUES LESS THAN (\"2014-01-01\"),\n" +
                "PARTITION p2 VALUES LESS THAN (\"2014-06-01\"),\n" +
                "PARTITION p3 VALUES LESS THAN (\"2014-12-01\")\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 32\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"dynamic_partition.enable\" = \"true\",\n" +
                "\"dynamic_partition.start\" = \"-3\",\n" +
                "\"dynamic_partition.end\" = \"3\",\n" +
                "\"dynamic_partition.buckets\" = \"3\",\n" +
                "\"dynamic_partition.time_unit\" = \"day\",\n" +
                "\"dynamic_partition.prefix\" = \"p\",\n" +
                "\"dynamic_partition.reserved_history_periods\" = \"[,2021-01-01]\"\n" +
                ");";
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "errCode = 2, detailMessage = Invalid \" dynamic_partition.reserved_history_periods \" value [,2021-01-01]. " +
                        "It must be like " +
                        "\"[yyyy-MM-dd,yyyy-MM-dd],[...,...]\" while time_unit is DAY/WEEK/MONTH or " +
                        "\"[yyyy-MM-dd HH:mm:ss,yyyy-MM-dd HH:mm:ss],[...,...]\" while time_unit is HOUR.",
                () -> createTable(createOlapTblStmt2));

    String createOlapTblStmt3 = "CREATE TABLE test.`dynamic_partition_reserved_history_periods_validate3` (\n" +
                "  `k1` date NULL COMMENT \"\",\n" +
                "  `k2` int NULL COMMENT \"\",\n" +
                "  `k3` smallint NULL COMMENT \"\",\n" +
                "  `v1` varchar(2048) NULL COMMENT \"\",\n" +
                "  `v2` datetime NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`, `k2`, `k3`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE (k1)\n" +
                "(\n" +
                "PARTITION p1 VALUES LESS THAN (\"2014-01-01\"),\n" +
                "PARTITION p2 VALUES LESS THAN (\"2014-06-01\"),\n" +
                "PARTITION p3 VALUES LESS THAN (\"2014-12-01\")\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 32\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"dynamic_partition.enable\" = \"true\",\n" +
                "\"dynamic_partition.start\" = \"-3\",\n" +
                "\"dynamic_partition.end\" = \"3\",\n" +
                "\"dynamic_partition.buckets\" = \"3\",\n" +
                "\"dynamic_partition.time_unit\" = \"day\",\n" +
                "\"dynamic_partition.prefix\" = \"p\",\n" +
                "\"dynamic_partition.reserved_history_periods\" = \"[2020-01-01,2020-03-01],[2021-10-01,2021-09-01]\"\n" +
                ");";
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "errCode = 2, detailMessage = The first date is larger than the second date, [2021-10-01,2021-09-01] is invalid.",
                () -> createTable(createOlapTblStmt3));

    String createOlapTblStmt4 = "CREATE TABLE test.`dynamic_partition_reserved_history_periods_validate4` (\n" +
            "  `k1` datetime NULL COMMENT \"\",\n" +
            "  `k2` int NULL COMMENT \"\",\n" +
            "  `k3` smallint NULL COMMENT \"\",\n" +
            "  `v1` varchar(2048) NULL COMMENT \"\",\n" +
            "  `v2` datetime NULL COMMENT \"\"\n" +
            ") ENGINE=OLAP\n" +
            "DUPLICATE KEY(`k1`, `k2`, `k3`)\n" +
            "COMMENT \"OLAP\"\n" +
            "PARTITION BY RANGE (k1)\n" +
            "(\n" +
            "PARTITION p1 VALUES LESS THAN (\"2014-01-01 00:00:00\"),\n" +
            "PARTITION p2 VALUES LESS THAN (\"2014-06-01 00:00:00\"),\n" +
            "PARTITION p3 VALUES LESS THAN (\"2014-12-01 00:00:00\")\n" +
            ")\n" +
            "DISTRIBUTED BY HASH(`k1`) BUCKETS 32\n" +
            "PROPERTIES (\n" +
            "\"replication_num\" = \"1\",\n" +
            "\"dynamic_partition.enable\" = \"true\",\n" +
            "\"dynamic_partition.start\" = \"-3\",\n" +
            "\"dynamic_partition.end\" = \"3\",\n" +
            "\"dynamic_partition.buckets\" = \"3\",\n" +
            "\"dynamic_partition.time_unit\" = \"hour\",\n" +
            "\"dynamic_partition.prefix\" = \"p\",\n" +
            "\"dynamic_partition.reserved_history_periods\" = \"[2020-01-01,2020-03-01]\"\n" +
            ");";
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
            "errCode = 2, detailMessage = Invalid \" dynamic_partition.reserved_history_periods \" value [2020-01-01,2020-03-01]. " +
                    "It must be like " +
                    "\"[yyyy-MM-dd,yyyy-MM-dd],[...,...]\" while time_unit is DAY/WEEK/MONTH " +
                    "or \"[yyyy-MM-dd HH:mm:ss,yyyy-MM-dd HH:mm:ss],[...,...]\" while time_unit is HOUR.",
                () -> createTable(createOlapTblStmt4));
    }
}
