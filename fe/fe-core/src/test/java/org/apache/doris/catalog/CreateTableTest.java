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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ConfigBase;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.UtFrameUtils;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public class CreateTableTest {
    private static String runningDir = "fe/mocked/CreateTableTest2/" + UUID.randomUUID().toString() + "/";

    private static ConnectContext connectContext;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createDorisCluster(runningDir);

        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        // create database
        String createDbStmtStr = "create database test;";
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt(createDbStmtStr, connectContext);
        Catalog.getCurrentCatalog().createDb(createDbStmt);
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

    @Test
    public void testDuplicateCreateTable() throws Exception{
        // test
        Catalog catalog = Catalog.getCurrentCatalog();
        String sql = "create table if not exists test.tbl1_colocate\n" + "(k1 int, k2 int)\n" + "duplicate key(k1)\n"
                + "distributed by hash(k2) buckets 1\n" + "properties('replication_num' = '1','colocate_with'='test'); ";
        createTable(sql);
        Set<Long> tabletIdSetAfterCreateFirstTable = catalog.getTabletInvertedIndex().getReplicaMetaTable().rowKeySet();
        Set<TabletMeta> tabletMetaSetBeforeCreateFirstTable = new HashSet<>();
        catalog.getTabletInvertedIndex().getTabletMetaTable().values().forEach(tabletMeta -> {tabletMetaSetBeforeCreateFirstTable.add(tabletMeta);});
        Set<Long> colocateTableIdBeforeCreateFirstTable = catalog.getColocateTableIndex().getTable2Group().keySet();
        Assert.assertTrue(colocateTableIdBeforeCreateFirstTable.size() > 0);
        Assert.assertTrue(tabletIdSetAfterCreateFirstTable.size() > 0);

        createTable(sql);
        // check whether tablet is cleared after duplicate create table
        Set<Long> tabletIdSetAfterDuplicateCreateTable1 = catalog.getTabletInvertedIndex().getReplicaMetaTable().rowKeySet();
        Set<Long> tabletIdSetAfterDuplicateCreateTable2 = catalog.getTabletInvertedIndex().getBackingReplicaMetaTable().columnKeySet();
        Set<Long> tabletIdSetAfterDuplicateCreateTable3 = catalog.getTabletInvertedIndex().getTabletMetaMap().keySet();
        Set<TabletMeta> tabletIdSetAfterDuplicateCreateTable4 = new HashSet<>();
        catalog.getTabletInvertedIndex().getTabletMetaTable().values().forEach(tabletMeta -> {tabletIdSetAfterDuplicateCreateTable4.add(tabletMeta);});

        Assert.assertTrue(tabletIdSetAfterCreateFirstTable.equals(tabletIdSetAfterDuplicateCreateTable1));
        Assert.assertTrue(tabletIdSetAfterCreateFirstTable.equals(tabletIdSetAfterDuplicateCreateTable2));
        Assert.assertTrue(tabletIdSetAfterCreateFirstTable.equals(tabletIdSetAfterDuplicateCreateTable3));
        Assert.assertTrue(tabletMetaSetBeforeCreateFirstTable.equals(tabletIdSetAfterDuplicateCreateTable4));

        // check whether table id is cleared from colocate group after duplicate create table
        Set<Long> colocateTableIdAfterCreateFirstTable = catalog.getColocateTableIndex().getTable2Group().keySet();
        Assert.assertTrue(colocateTableIdBeforeCreateFirstTable.equals(colocateTableIdAfterCreateFirstTable));
    }

    @Test
    public void testNormal() throws DdlException {
        ExceptionChecker.expectThrowsNoException(
                () -> createTable("create table test.tbl1\n" + "(k1 int, k2 int)\n" + "duplicate key(k1)\n"
                        + "distributed by hash(k2) buckets 1\n" + "properties('replication_num' = '1'); "));

        ExceptionChecker.expectThrowsNoException(() -> createTable("create table test.tbl2\n" + "(k1 int, k2 int)\n"
                + "duplicate key(k1)\n" + "partition by range(k2)\n" + "(partition p1 values less than(\"10\"))\n"
                + "distributed by hash(k2) buckets 1\n" + "properties('replication_num' = '1'); "));

        ExceptionChecker.expectThrowsNoException(
                () -> createTable("create table test.tbl3\n" + "(k1 varchar(40), k2 int)\n" + "duplicate key(k1)\n"
                        + "partition by range(k2)\n" + "(partition p1 values less than(\"10\"))\n"
                        + "distributed by hash(k2) buckets 1\n" + "properties('replication_num' = '1');"));

        ExceptionChecker.expectThrowsNoException(
                () -> createTable("create table test.tbl4\n" + "(k1 varchar(40), k2 int, v1 int sum)\n"
                        + "partition by range(k2)\n" + "(partition p1 values less than(\"10\"))\n"
                        + "distributed by hash(k1) buckets 1\n" + "properties('replication_num' = '1');"));

        ExceptionChecker.expectThrowsNoException(() -> createTable(
                "create table test.tbl5\n" + "(k1 varchar(40), k2 int, v1 int sum)\n" + "aggregate key(k1,k2)\n"
                        + "partition by range(k2)\n" + "(partition p1 values less than(\"10\"))\n"
                        + "distributed by hash(k1) buckets 1\n" + "properties('replication_num' = '1');"));

        ExceptionChecker.expectThrowsNoException(() -> createTable(
                "create table test.tbl6\n" + "(k1 varchar(40), k2 int, k3 int)\n" + "duplicate key(k1, k2, k3)\n"
                        + "partition by range(k2)\n" + "(partition p1 values less than(\"10\"))\n"
                        + "distributed by hash(k1) buckets 1\n" + "properties('replication_num' = '1');"));

        ExceptionChecker
                .expectThrowsNoException(() -> createTable("create table test.tbl7\n" + "(k1 varchar(40), k2 int)\n"
                        + "partition by range(k2)\n" + "(partition p1 values less than(\"10\"))\n"
                        + "distributed by hash(k2) buckets 1\n" + "properties('replication_num' = '1');"));

        ConfigBase.setMutableConfig("enable_strict_storage_medium_check", "false");
        ExceptionChecker
                .expectThrowsNoException(() -> createTable("create table test.tb7(key1 int, key2 varchar(10)) \n"
                        + "distributed by hash(key1) buckets 1 properties('replication_num' = '1', 'storage_medium' = 'ssd');"));

        ExceptionChecker
                .expectThrowsNoException(() -> createTable("create table test.tbl8\n" + "(k1 varchar(40), k2 int, v1 int)\n"
                        + "unique key(k1, k2)\n"
                        + "partition by range(k2)\n" + "(partition p1 values less than(\"10\"))\n"
                        + "distributed by hash(k2) buckets 1\n" + "properties('replication_num' = '1',\n"
                        + "'function_column.sequence_type' = 'int');"));

        /**
         * create table with list partition
         */
        // single partition column with single key
        ExceptionChecker
                .expectThrowsNoException(() -> createTable("create table test.tbl9\n"
                        + "(k1 int not null, k2 varchar(128), k3 int, v1 int, v2 int)\n"
                        + "partition by list(k1)\n"
                        + "(\n"
                        + "partition p1 values in (\"1\"),\n"
                        + "partition p2 values in (\"2\")\n"
                        + ")\n"
                        + "distributed by hash(k2) buckets 1\n"
                        + "properties('replication_num' = '1');"));

        // single partition column with multi keys
        ExceptionChecker
                .expectThrowsNoException(() -> createTable("create table test.tbl10\n"
                        + "(k1 int not null, k2 varchar(128), k3 int, v1 int, v2 int)\n"
                        + "partition by list(k1)\n"
                        + "(\n"
                        + "partition p1 values in (\"1\", \"3\", \"5\"),\n"
                        + "partition p2 values in (\"2\", \"4\", \"6\"),\n"
                        + "partition p3 values in (\"7\", \"8\")\n"
                        + ")\n"
                        + "distributed by hash(k2) buckets 1\n"
                        + "properties('replication_num' = '1');"));

        // multi partition columns with single key
        ExceptionChecker
                .expectThrowsNoException(() -> createTable("create table test.tbl11\n"
                        + "(k1 int not null, k2 varchar(128) not null, k3 int, v1 int, v2 int)\n"
                        + "partition by list(k1, k2)\n"
                        + "(\n"
                        + "partition p1 values in ((\"1\", \"beijing\")),\n"
                        + "partition p2 values in ((\"2\", \"beijing\"))\n"
                        + ")\n"
                        + "distributed by hash(k2) buckets 1\n"
                        + "properties('replication_num' = '1');"));

        // multi partition columns with multi keys
        ExceptionChecker
                .expectThrowsNoException(() -> createTable("create table test.tbl12\n"
                        + "(k1 int not null, k2 varchar(128) not null, k3 int, v1 int, v2 int)\n"
                        + "partition by list(k1, k2)\n"
                        + "(\n"
                        + "partition p1 values in ((\"1\", \"beijing\"), (\"1\", \"shanghai\")),\n"
                        + "partition p2 values in ((\"2\", \"beijing\"), (\"2\", \"shanghai\")),\n"
                        + "partition p3 values in ((\"3\", \"tianjin\"))\n"
                        + ")\n"
                        + "distributed by hash(k2) buckets 1\n"
                        + "properties('replication_num' = '1');"));

        Database db = Catalog.getCurrentCatalog().getDb("default_cluster:test");
        OlapTable tbl6 = (OlapTable) db.getTable("tbl6");
        Assert.assertTrue(tbl6.getColumn("k1").isKey());
        Assert.assertTrue(tbl6.getColumn("k2").isKey());
        Assert.assertTrue(tbl6.getColumn("k3").isKey());

        OlapTable tbl7 = (OlapTable) db.getTable("tbl7");
        Assert.assertTrue(tbl7.getColumn("k1").isKey());
        Assert.assertFalse(tbl7.getColumn("k2").isKey());
        Assert.assertTrue(tbl7.getColumn("k2").getAggregationType() == AggregateType.NONE);

        OlapTable tbl8 = (OlapTable) db.getTable("tbl8");
        Assert.assertTrue(tbl8.getColumn("k1").isKey());
        Assert.assertTrue(tbl8.getColumn("k2").isKey());
        Assert.assertFalse(tbl8.getColumn("v1").isKey());
        Assert.assertTrue(tbl8.getColumn(Column.SEQUENCE_COL).getAggregationType() == AggregateType.REPLACE);
    }

    @Test
    public void testAbnormal() throws DdlException {
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "Floating point type column can not be distribution column",
                () -> createTable("create table test.atbl1\n" + "(k1 int, k2 float)\n" + "duplicate key(k1)\n"
                        + "distributed by hash(k2) buckets 1\n" + "properties('replication_num' = '1'); "));

        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class,
                "Floating point type column can not be partition column",
                () -> createTable("create table test.atbl3\n" + "(k1 int, k2 int, k3 float)\n" + "duplicate key(k1)\n"
                        + "partition by range(k3)\n" + "(partition p1 values less than(\"10\"))\n"
                        + "distributed by hash(k2) buckets 1\n" + "properties('replication_num' = '1'); "));

        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "Varchar should not in the middle of short keys",
                () -> createTable("create table test.atbl3\n" + "(k1 varchar(40), k2 int, k3 int)\n"
                        + "duplicate key(k1, k2, k3)\n" + "distributed by hash(k1) buckets 1\n"
                        + "properties('replication_num' = '1', 'short_key' = '3');"));

        ExceptionChecker.expectThrowsWithMsg(DdlException.class, "Short key is too large. should less than: 3",
                () -> createTable("create table test.atbl4\n" + "(k1 int, k2 int, k3 int)\n"
                        + "duplicate key(k1, k2, k3)\n" + "distributed by hash(k1) buckets 1\n"
                        + "properties('replication_num' = '1', 'short_key' = '4');"));

        ExceptionChecker
                .expectThrowsWithMsg(DdlException.class, "Failed to find enough host with storage medium and " +
                                "tag(NaN/{\"location\" : \"default\"}) in all backends. need: 3",
                        () -> createTable("create table test.atbl5\n" + "(k1 int, k2 int, k3 int)\n"
                                + "duplicate key(k1, k2, k3)\n" + "distributed by hash(k1) buckets 1\n"
                                + "properties('replication_num' = '3');"));

        ExceptionChecker.expectThrowsNoException(
                () -> createTable("create table test.atbl6\n" + "(k1 int, k2 int)\n" + "duplicate key(k1)\n"
                        + "distributed by hash(k2) buckets 1\n" + "properties('replication_num' = '1'); "));

        ExceptionChecker
                .expectThrowsWithMsg(DdlException.class, "Table 'atbl6' already exists",
                        () -> createTable("create table test.atbl6\n" + "(k1 int, k2 int, k3 int)\n"
                                + "duplicate key(k1, k2, k3)\n" + "distributed by hash(k1) buckets 1\n"
                                + "properties('replication_num' = '1');"));

        ConfigBase.setMutableConfig("enable_strict_storage_medium_check", "true");
        ExceptionChecker
                .expectThrowsWithMsg(DdlException.class, "Failed to find enough host with storage medium and " +
                                "tag(SSD/{\"location\" : \"default\"}) in all backends. need: 1",
                        () -> createTable("create table test.tb7(key1 int, key2 varchar(10)) distributed by hash(key1) \n"
                                + "buckets 1 properties('replication_num' = '1', 'storage_medium' = 'ssd');"));

        ExceptionChecker
                .expectThrowsWithMsg(DdlException.class, "sequence column only support UNIQUE_KEYS",
                        () -> createTable("create table test.atbl8\n" + "(k1 varchar(40), k2 int, v1 int sum)\n"
                        + "aggregate key(k1, k2)\n"
                        + "partition by range(k2)\n" + "(partition p1 values less than(\"10\"))\n"
                        + "distributed by hash(k2) buckets 1\n" + "properties('replication_num' = '1',\n"
                        + "'function_column.sequence_type' = 'int');"));

        ExceptionChecker
                .expectThrowsWithMsg(DdlException.class, "sequence type only support integer types and date types",
                        () -> createTable("create table test.atbl8\n" + "(k1 varchar(40), k2 int, v1 int)\n"
                                + "unique key(k1, k2)\n"
                                + "partition by range(k2)\n" + "(partition p1 values less than(\"10\"))\n"
                                + "distributed by hash(k2) buckets 1\n" + "properties('replication_num' = '1',\n"
                                + "'function_column.sequence_type' = 'double');"));

        /**
         * create table with list partition
         */
        // single partition column with single key
        ExceptionChecker
                .expectThrowsWithMsg(AnalysisException.class, "Syntax error", () -> createTable("create table test.tbl9\n"
                        + "(k1 int not null, k2 varchar(128), k3 int, v1 int, v2 int)\n"
                        + "partition by list(k1)\n"
                        + "(\n"
                        + "partition p1 values in (\"1\"),\n"
                        + "partition p2 values in ()\n"
                        + ")\n"
                        + "distributed by hash(k2) buckets 1\n"
                        + "properties('replication_num' = '1');"));

        // single partition column with multi keys
        ExceptionChecker
                .expectThrowsWithMsg(IllegalArgumentException.class, "partition key desc list size[2] is not equal to partition column size[1]",
                        () -> createTable("create table test.tbl10\n"
                                + "(k1 int not null, k2 varchar(128), k3 int, v1 int, v2 int)\n"
                                + "partition by list(k1)\n"
                                + "(\n"
                                + "partition p1 values in (\"1\", \"3\", \"5\"),\n"
                                + "partition p2 values in (\"2\", \"4\", \"6\"),\n"
                                + "partition p3 values in ((\"7\", \"8\"))\n"
                                + ")\n"
                                + "distributed by hash(k2) buckets 1\n"
                                + "properties('replication_num' = '1');"));

        // multi partition columns with single key
        ExceptionChecker
                .expectThrowsWithMsg(IllegalArgumentException.class, "partition key desc list size[1] is not equal to partition column size[2]",
                        () -> createTable("create table test.tbl11\n"
                                + "(k1 int not null, k2 varchar(128) not null, k3 int, v1 int, v2 int)\n"
                                + "partition by list(k1, k2)\n"
                                + "(\n"
                                + "partition p1 values in ((\"1\", \"beijing\")),\n"
                                + "partition p2 values in (\"2\", \"beijing\")\n"
                                + ")\n"
                                + "distributed by hash(k2) buckets 1\n"
                                + "properties('replication_num' = '1');"));

        // multi partition columns with multi keys
        ExceptionChecker
                .expectThrowsWithMsg(IllegalArgumentException.class, "partition key desc list size[3] is not equal to partition column size[2]",
                        () -> createTable("create table test.tbl12\n"
                                + "(k1 int not null, k2 varchar(128) not null, k3 int, v1 int, v2 int)\n"
                                + "partition by list(k1, k2)\n"
                                + "(\n"
                                + "partition p1 values in ((\"1\", \"beijing\"), (\"1\", \"shanghai\")),\n"
                                + "partition p2 values in ((\"2\", \"beijing\"), (\"2\", \"shanghai\")),\n"
                                + "partition p3 values in ((\"3\", \"tianjin\", \"3\"))\n"
                                + ")\n"
                                + "distributed by hash(k2) buckets 1\n"
                                + "properties('replication_num' = '1');"));

        // multi partition columns with multi keys
        ExceptionChecker
                .expectThrowsWithMsg(AnalysisException.class, "Syntax error",
                        () -> createTable("create table test.tbl13\n"
                                + "(k1 int not null, k2 varchar(128) not null, k3 int, v1 int, v2 int)\n"
                                + "partition by list(k1, k2)\n"
                                + "(\n"
                                + "partition p1 values in ((\"1\", \"beijing\"), (\"1\", \"shanghai\")),\n"
                                + "partition p2 values in ((\"2\", \"beijing\"), (\"2\", \"shanghai\")),\n"
                                + "partition p3 values in ()\n"
                                + ")\n"
                                + "distributed by hash(k2) buckets 1\n"
                                + "properties('replication_num' = '1');"));

        /**
         * create table with both list and range partition
         */
        // list contain less than
        ExceptionChecker
                .expectThrowsWithMsg(AnalysisException.class, "You can only use in values to create list partitions",
                        () -> createTable("CREATE TABLE test.tbl14 (\n" +
                                "    k1 int not null, k2 varchar(128), k3 int, v1 int, v2 int\n" +
                                ")\n" +
                                "PARTITION BY LIST(k1)\n" +
                                "(\n" +
                                "    PARTITION p1 VALUES less than (\"1\"),\n" +
                                "    PARTITION p2 VALUES less than (\"2\"),\n" +
                                "    partition p3 values less than (\"5\")\n" +
                                ")DISTRIBUTED BY HASH(k2) BUCKETS 10\n" +
                                "PROPERTIES(\"replication_num\" = \"1\");"));

        // range contain in
        ExceptionChecker
                .expectThrowsWithMsg(AnalysisException.class, "You can only use fixed or less than values to create range partitions",
                        () -> createTable("CREATE TABLE test.tbl15 (\n" +
                                "    k1 int, k2 varchar(128), k3 int, v1 int, v2 int\n" +
                                ")\n" +
                                "PARTITION BY range(k1)\n" +
                                "(\n" +
                                "    PARTITION p1 VALUES in (\"1\"),\n" +
                                "    PARTITION p2 VALUES in (\"2\"),\n" +
                                "    partition p3 values in (\"5\")\n" +
                                ")DISTRIBUTED BY HASH(k2) BUCKETS 10\n" +
                                "PROPERTIES(\"replication_num\" = \"1\");"));

        // list contain both
        ExceptionChecker
                .expectThrowsWithMsg(AnalysisException.class, "You can only use in values to create list partitions",
                        () -> createTable("CREATE TABLE test.tbl15 (\n" +
                                "    k1 int not null, k2 varchar(128), k3 int, v1 int, v2 int\n" +
                                ")\n" +
                                "PARTITION BY LIST(k1)\n" +
                                "(\n" +
                                "    PARTITION p1 VALUES in (\"1\"),\n" +
                                "    PARTITION p2 VALUES in (\"2\"),\n" +
                                "    partition p3 values less than (\"5\")\n" +
                                ")DISTRIBUTED BY HASH(k2) BUCKETS 10\n" +
                                "PROPERTIES(\"replication_num\" = \"1\");"));

        // range contain both
        ExceptionChecker
                .expectThrowsWithMsg(AnalysisException.class, "You can only use fixed or less than values to create range partitions",
                        () -> createTable("CREATE TABLE test.tbl16 (\n" +
                                "    k1 int, k2 varchar(128), k3 int, v1 int, v2 int\n" +
                                ")\n" +
                                "PARTITION BY RANGE(k1)\n" +
                                "(\n" +
                                "    PARTITION p1 VALUES less than (\"1\"),\n" +
                                "    PARTITION p2 VALUES less than (\"2\"),\n" +
                                "    partition p3 values in (\"5\")\n" +
                                ")DISTRIBUTED BY HASH(k2) BUCKETS 10\n" +
                                "PROPERTIES(\"replication_num\" = \"1\");"));

        // range: partition content != partition key type
        ExceptionChecker
                .expectThrowsWithMsg(DdlException.class, "Invalid number format: beijing",
                        () -> createTable("CREATE TABLE test.tbl17 (\n" +
                                "    k1 int, k2 varchar(128), k3 int, v1 int, v2 int\n" +
                                ")\n" +
                                "PARTITION BY range(k1)\n" +
                                "(\n" +
                                "    PARTITION p1 VALUES less than (\"beijing\"),\n" +
                                "    PARTITION p2 VALUES less than (\"shanghai\"),\n" +
                                "    partition p3 values less than (\"tianjin\")\n" +
                                ")DISTRIBUTED BY HASH(k2) BUCKETS 10\n" +
                                "PROPERTIES(\"replication_num\" = \"1\");"));

        // list: partition content != partition key type
        ExceptionChecker
                .expectThrowsWithMsg(DdlException.class, "Invalid number format: beijing",
                        () -> createTable("CREATE TABLE test.tbl18 (\n" +
                                "    k1 int not null, k2 varchar(128), k3 int, v1 int, v2 int\n" +
                                ")\n" +
                                "PARTITION BY list(k1)\n" +
                                "(\n" +
                                "    PARTITION p1 VALUES in (\"beijing\"),\n" +
                                "    PARTITION p2 VALUES in (\"shanghai\"),\n" +
                                "    partition p3 values in (\"tianjin\")\n" +
                                ")DISTRIBUTED BY HASH(k2) BUCKETS 10\n" +
                                "PROPERTIES(\"replication_num\" = \"1\");"));

        /**
         * dynamic partition table
         */
        // list partition with dynamic properties
        ExceptionChecker
                .expectThrowsWithMsg(DdlException.class, "Only support dynamic partition properties on range partition table",
                        () -> createTable("CREATE TABLE test.tbl19\n" +
                                "(\n" +
                                "    k1 DATE not null\n" +
                                ")\n" +
                                "PARTITION BY LIST(k1) ()\n" +
                                "DISTRIBUTED BY HASH(k1)\n" +
                                "PROPERTIES\n" +
                                "(\n" +
                                "    \"dynamic_partition.enable\" = \"true\",\n" +
                                "    \"dynamic_partition.time_unit\" = \"MONTH\",\n" +
                                "    \"dynamic_partition.end\" = \"2\",\n" +
                                "    \"dynamic_partition.prefix\" = \"p\",\n" +
                                "    \"dynamic_partition.buckets\" = \"8\",\n" +
                                "    \"dynamic_partition.start_day_of_month\" = \"3\"\n" +
                                ");\n"));

        // no partition table with dynamic properties
        ExceptionChecker
                .expectThrowsWithMsg(DdlException.class, "Only support dynamic partition properties on range partition table",
                        () -> createTable("CREATE TABLE test.tbl20\n" +
                                "(\n" +
                                "    k1 DATE\n" +
                                ")\n" +
                                "DISTRIBUTED BY HASH(k1)\n" +
                                "PROPERTIES\n" +
                                "(\n" +
                                "    \"dynamic_partition.enable\" = \"true\",\n" +
                                "    \"dynamic_partition.time_unit\" = \"MONTH\",\n" +
                                "    \"dynamic_partition.end\" = \"2\",\n" +
                                "    \"dynamic_partition.prefix\" = \"p\",\n" +
                                "    \"dynamic_partition.buckets\" = \"8\",\n" +
                                "    \"dynamic_partition.start_day_of_month\" = \"3\"\n" +
                                ");"));

    }
}
