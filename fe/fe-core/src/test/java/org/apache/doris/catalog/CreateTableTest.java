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
        UtFrameUtils.createMinDorisCluster(runningDir);

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
                .expectThrowsWithMsg(DdlException.class, "Failed to find enough host in all backends. need: 3",
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
                .expectThrowsWithMsg(DdlException.class, "Failed to find enough host with storage medium is SSD in all backends. need: 1",
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
    }
}
