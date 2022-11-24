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
import org.apache.doris.common.Config;
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
        Config.disable_storage_medium_check = true;
        UtFrameUtils.createDorisCluster(runningDir);

        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        // create database
        String createDbStmtStr = "create database test;";
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt(createDbStmtStr, connectContext);
        Env.getCurrentEnv().createDb(createDbStmt);
    }

    @AfterClass
    public static void tearDown() {
        File file = new File(runningDir);
        file.delete();
    }

    private static void createTable(String sql) throws Exception {
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        Env.getCurrentEnv().createTable(createTableStmt);
    }

    @Test
    public void testDuplicateCreateTable() throws Exception {
        // test
        Env env = Env.getCurrentEnv();
        String sql = "create table if not exists test.tbl1_colocate\n" + "(k1 int, k2 int)\n" + "duplicate key(k1)\n"
                + "distributed by hash(k2) buckets 1\n" + "properties('replication_num' = '1','colocate_with'='test'); ";
        createTable(sql);
        Set<Long> tabletIdSetAfterCreateFirstTable = env.getTabletInvertedIndex().getReplicaMetaTable().rowKeySet();
        Set<TabletMeta> tabletMetaSetBeforeCreateFirstTable =
                new HashSet<>(env.getTabletInvertedIndex().getTabletMetaTable().values());
        Set<Long> colocateTableIdBeforeCreateFirstTable = env.getColocateTableIndex().getTable2Group().keySet();
        Assert.assertTrue(colocateTableIdBeforeCreateFirstTable.size() > 0);
        Assert.assertTrue(tabletIdSetAfterCreateFirstTable.size() > 0);

        createTable(sql);
        // check whether tablet is cleared after duplicate create table
        Set<Long> tabletIdSetAfterDuplicateCreateTable1 = env.getTabletInvertedIndex().getReplicaMetaTable().rowKeySet();
        Set<Long> tabletIdSetAfterDuplicateCreateTable2 = env.getTabletInvertedIndex().getBackingReplicaMetaTable().columnKeySet();
        Set<Long> tabletIdSetAfterDuplicateCreateTable3 = env.getTabletInvertedIndex().getTabletMetaMap().keySet();
        Set<TabletMeta> tabletIdSetAfterDuplicateCreateTable4 =
                new HashSet<>(env.getTabletInvertedIndex().getTabletMetaTable().values());

        Assert.assertTrue(tabletIdSetAfterCreateFirstTable.equals(tabletIdSetAfterDuplicateCreateTable1));
        Assert.assertTrue(tabletIdSetAfterCreateFirstTable.equals(tabletIdSetAfterDuplicateCreateTable2));
        Assert.assertTrue(tabletIdSetAfterCreateFirstTable.equals(tabletIdSetAfterDuplicateCreateTable3));
        Assert.assertTrue(tabletMetaSetBeforeCreateFirstTable.equals(tabletIdSetAfterDuplicateCreateTable4));

        // check whether table id is cleared from colocate group after duplicate create table
        Set<Long> colocateTableIdAfterCreateFirstTable = env.getColocateTableIndex().getTable2Group().keySet();
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

        ConfigBase.setMutableConfig("disable_storage_medium_check", "true");
        ExceptionChecker
                .expectThrowsNoException(() -> createTable("create table test.tb7(key1 int, key2 varchar(10)) \n"
                        + "distributed by hash(key1) buckets 1 properties('replication_num' = '1', 'storage_medium' = 'ssd');"));

        ExceptionChecker
                .expectThrowsNoException(() -> createTable("create table test.compression1(key1 int, key2 varchar(10)) \n"
                        + "distributed by hash(key1) buckets 1 \n"
                        + "properties('replication_num' = '1', 'compression' = 'lz4f');"));

        ExceptionChecker
                .expectThrowsNoException(() -> createTable("create table test.compression2(key1 int, key2 varchar(10)) \n"
                        + "distributed by hash(key1) buckets 1 \n"
                        + "properties('replication_num' = '1', 'compression' = 'snappy');"));

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

        // table with sequence col
        ExceptionChecker
                .expectThrowsNoException(() -> createTable("create table test.tbl13\n"
                        + "(k1 varchar(40), k2 int, v1 int)\n"
                        + "unique key(k1, k2)\n"
                        + "partition by range(k2)\n" + "(partition p1 values less than(\"10\"))\n"
                        + "distributed by hash(k2) buckets 1\n" + "properties('replication_num' = '1',\n"
                        + "'function_column.sequence_col' = 'v1');"));

        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException("default_cluster:test");
        OlapTable tbl6 = (OlapTable) db.getTableOrDdlException("tbl6");
        Assert.assertTrue(tbl6.getColumn("k1").isKey());
        Assert.assertTrue(tbl6.getColumn("k2").isKey());
        Assert.assertTrue(tbl6.getColumn("k3").isKey());

        OlapTable tbl7 = (OlapTable) db.getTableOrDdlException("tbl7");
        Assert.assertTrue(tbl7.getColumn("k1").isKey());
        Assert.assertFalse(tbl7.getColumn("k2").isKey());
        Assert.assertTrue(tbl7.getColumn("k2").getAggregationType() == AggregateType.NONE);

        OlapTable tbl8 = (OlapTable) db.getTableOrDdlException("tbl8");
        Assert.assertTrue(tbl8.getColumn("k1").isKey());
        Assert.assertTrue(tbl8.getColumn("k2").isKey());
        Assert.assertFalse(tbl8.getColumn("v1").isKey());
        Assert.assertTrue(tbl8.getColumn(Column.SEQUENCE_COL).getAggregationType() == AggregateType.REPLACE);

        OlapTable tbl13 = (OlapTable) db.getTableOrDdlException("tbl13");
        Assert.assertTrue(tbl13.getColumn(Column.SEQUENCE_COL).getAggregationType() == AggregateType.REPLACE);
        Assert.assertTrue(tbl13.getColumn(Column.SEQUENCE_COL).getType() == Type.INT);
        Assert.assertEquals(tbl13.getSequenceMapCol(), "v1");
    }

    @Test
    public void testAbnormal() throws DdlException {
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "Floating point type should not be used in distribution column",
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
                .expectThrowsWithMsg(DdlException.class, "Failed to find 3 backends for policy",
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

        ConfigBase.setMutableConfig("disable_storage_medium_check", "false");
        ExceptionChecker
                .expectThrowsWithMsg(DdlException.class, " Failed to find 1 backends for policy:",
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

        ExceptionChecker
                .expectThrowsWithMsg(DdlException.class, "The sequence_col and sequence_type cannot be set at the same time",
                        () -> createTable("create table test.atbl8\n" + "(k1 varchar(40), k2 int, v1 int)\n"
                                + "unique key(k1, k2)\n"
                                + "partition by range(k2)\n" + "(partition p1 values less than(\"10\"))\n"
                                + "distributed by hash(k2) buckets 1\n" + "properties('replication_num' = '1',\n"
                                + "'function_column.sequence_type' = 'int', 'function_column.sequence_col' = 'v1');"));

        ExceptionChecker
                .expectThrowsWithMsg(DdlException.class, "The specified sequence column[v3] not exists",
                        () -> createTable("create table test.atbl8\n" + "(k1 varchar(40), k2 int, v1 int)\n"
                                + "unique key(k1, k2)\n"
                                + "partition by range(k2)\n" + "(partition p1 values less than(\"10\"))\n"
                                + "distributed by hash(k2) buckets 1\n" + "properties('replication_num' = '1',\n"
                                + "'function_column.sequence_col' = 'v3');"));

        ExceptionChecker
                .expectThrowsWithMsg(DdlException.class, "Sequence type only support integer types and date types",
                        () -> createTable("create table test.atbl8\n" + "(k1 varchar(40), k2 int, v1 int)\n"
                                + "unique key(k1, k2)\n"
                                + "partition by range(k2)\n" + "(partition p1 values less than(\"10\"))\n"
                                + "distributed by hash(k2) buckets 1\n" + "properties('replication_num' = '1',\n"
                                + "'function_column.sequence_col' = 'k1');"));

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
                        () -> createTable("CREATE TABLE test.tbl14 (\n"
                                + "    k1 int not null, k2 varchar(128), k3 int, v1 int, v2 int\n"
                                + ")\n"
                                + "PARTITION BY LIST(k1)\n"
                                + "(\n"
                                + "    PARTITION p1 VALUES less than (\"1\"),\n"
                                + "    PARTITION p2 VALUES less than (\"2\"),\n"
                                + "    partition p3 values less than (\"5\")\n"
                                + ")DISTRIBUTED BY HASH(k2) BUCKETS 10\n"
                                + "PROPERTIES(\"replication_num\" = \"1\");"));

        // range contain in
        ExceptionChecker
                .expectThrowsWithMsg(AnalysisException.class, "You can only use fixed or less than values to create range partitions",
                        () -> createTable("CREATE TABLE test.tbl15 (\n"
                                + "    k1 int, k2 varchar(128), k3 int, v1 int, v2 int\n"
                                + ")\n"
                                + "PARTITION BY range(k1)\n"
                                + "(\n"
                                + "    PARTITION p1 VALUES in (\"1\"),\n"
                                + "    PARTITION p2 VALUES in (\"2\"),\n"
                                + "    partition p3 values in (\"5\")\n"
                                + ")DISTRIBUTED BY HASH(k2) BUCKETS 10\n"
                                + "PROPERTIES(\"replication_num\" = \"1\");"));

        // list contain both
        ExceptionChecker
                .expectThrowsWithMsg(AnalysisException.class, "You can only use in values to create list partitions",
                        () -> createTable("CREATE TABLE test.tbl15 (\n"
                                + "    k1 int not null, k2 varchar(128), k3 int, v1 int, v2 int\n"
                                + ")\n"
                                + "PARTITION BY LIST(k1)\n"
                                + "(\n"
                                + "    PARTITION p1 VALUES in (\"1\"),\n"
                                + "    PARTITION p2 VALUES in (\"2\"),\n"
                                + "    partition p3 values less than (\"5\")\n"
                                + ")DISTRIBUTED BY HASH(k2) BUCKETS 10\n"
                                + "PROPERTIES(\"replication_num\" = \"1\");"));

        // range contain both
        ExceptionChecker
                .expectThrowsWithMsg(AnalysisException.class, "You can only use fixed or less than values to create range partitions",
                        () -> createTable("CREATE TABLE test.tbl16 (\n"
                                + "    k1 int, k2 varchar(128), k3 int, v1 int, v2 int\n"
                                + ")\n"
                                + "PARTITION BY RANGE(k1)\n"
                                + "(\n"
                                + "    PARTITION p1 VALUES less than (\"1\"),\n"
                                + "    PARTITION p2 VALUES less than (\"2\"),\n"
                                + "    partition p3 values in (\"5\")\n"
                                + ")DISTRIBUTED BY HASH(k2) BUCKETS 10\n"
                                + "PROPERTIES(\"replication_num\" = \"1\");"));

        // range: partition content != partition key type
        ExceptionChecker
                .expectThrowsWithMsg(DdlException.class, "Invalid number format: beijing",
                        () -> createTable("CREATE TABLE test.tbl17 (\n"
                                + "    k1 int, k2 varchar(128), k3 int, v1 int, v2 int\n"
                                + ")\n"
                                + "PARTITION BY range(k1)\n"
                                + "(\n"
                                + "    PARTITION p1 VALUES less than (\"beijing\"),\n"
                                + "    PARTITION p2 VALUES less than (\"shanghai\"),\n"
                                + "    partition p3 values less than (\"tianjin\")\n"
                                + ")DISTRIBUTED BY HASH(k2) BUCKETS 10\n"
                                + "PROPERTIES(\"replication_num\" = \"1\");"));

        // list: partition content != partition key type
        ExceptionChecker
                .expectThrowsWithMsg(DdlException.class, "Invalid number format: beijing",
                        () -> createTable("CREATE TABLE test.tbl18 (\n"
                                + "    k1 int not null, k2 varchar(128), k3 int, v1 int, v2 int\n"
                                + ")\n"
                                + "PARTITION BY list(k1)\n"
                                + "(\n"
                                + "    PARTITION p1 VALUES in (\"beijing\"),\n"
                                + "    PARTITION p2 VALUES in (\"shanghai\"),\n"
                                + "    partition p3 values in (\"tianjin\")\n"
                                + ")DISTRIBUTED BY HASH(k2) BUCKETS 10\n"
                                + "PROPERTIES(\"replication_num\" = \"1\");"));

        /**
         * dynamic partition table
         */
        // list partition with dynamic properties
        ExceptionChecker
                .expectThrowsWithMsg(DdlException.class, "Only support dynamic partition properties on range partition table",
                        () -> createTable("CREATE TABLE test.tbl19\n"
                                + "(\n"
                                + "    k1 DATE not null\n"
                                + ")\n"
                                + "PARTITION BY LIST(k1) ()\n"
                                + "DISTRIBUTED BY HASH(k1)\n"
                                + "PROPERTIES\n"
                                + "(\n"
                                + "    \"dynamic_partition.enable\" = \"true\",\n"
                                + "    \"dynamic_partition.time_unit\" = \"MONTH\",\n"
                                + "    \"dynamic_partition.end\" = \"2\",\n"
                                + "    \"dynamic_partition.prefix\" = \"p\",\n"
                                + "    \"dynamic_partition.buckets\" = \"8\",\n"
                                + "    \"dynamic_partition.start_day_of_month\" = \"3\"\n"
                                + ");\n"));

        // no partition table with dynamic properties
        ExceptionChecker
                .expectThrowsWithMsg(DdlException.class, "Only support dynamic partition properties on range partition table",
                        () -> createTable("CREATE TABLE test.tbl20\n"
                                + "(\n"
                                + "    k1 DATE\n"
                                + ")\n"
                                + "DISTRIBUTED BY HASH(k1)\n"
                                + "PROPERTIES\n"
                                + "(\n"
                                + "    \"dynamic_partition.enable\" = \"true\",\n"
                                + "    \"dynamic_partition.time_unit\" = \"MONTH\",\n"
                                + "    \"dynamic_partition.end\" = \"2\",\n"
                                + "    \"dynamic_partition.prefix\" = \"p\",\n"
                                + "    \"dynamic_partition.buckets\" = \"8\",\n"
                                + "    \"dynamic_partition.start_day_of_month\" = \"3\"\n"
                                + ");"));

        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class,
                "Create unique keys table should not contain random distribution desc",
                () -> createTable("CREATE TABLE test.tbl21\n"
                            + "(\n"
                            + "  `k1` bigint(20) NULL COMMENT \"\",\n"
                            + "  `k2` largeint(40) NULL COMMENT \"\",\n"
                            + "  `v1` varchar(204) NULL COMMENT \"\",\n"
                            + "  `v2` smallint(6) NULL DEFAULT \"10\" COMMENT \"\"\n"
                            + ") ENGINE=OLAP\n"
                            + "UNIQUE KEY(`k1`, `k2`)\n"
                            + "DISTRIBUTED BY RANDOM BUCKETS 32\n"
                            + "PROPERTIES (\n"
                            + "\"replication_allocation\" = \"tag.location.default: 1\"\n"
                            + ");"));

        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class,
                "Create aggregate keys table with value columns of which aggregate type"
                    + " is REPLACE should not contain random distribution desc",
                () -> createTable("CREATE TABLE test.tbl22\n"
                    + "(\n"
                    + "  `k1` bigint(20) NULL COMMENT \"\",\n"
                    + "  `k2` largeint(40) NULL COMMENT \"\",\n"
                    + "  `v1` bigint(20) REPLACE NULL COMMENT \"\",\n"
                    + "  `v2` smallint(6) REPLACE_IF_NOT_NULL NULL DEFAULT \"10\" COMMENT \"\"\n"
                    + ") ENGINE=OLAP\n"
                    + "AGGREGATE KEY(`k1`, `k2`)\n"
                    + "DISTRIBUTED BY RANDOM BUCKETS 32\n"
                    + "PROPERTIES (\n"
                    + "\"replication_allocation\" = \"tag.location.default: 1\"\n"
                    + ");"));
    }

    @Test
    public void testZOrderTable() {
        // create lexically sort table
        ExceptionChecker.expectThrowsNoException(() -> createTable(
                "create table test.zorder_tbl1\n" + "(k1 varchar(40), k2 int, k3 int)\n" + "duplicate key(k1, k2, k3)\n"
                        + "partition by range(k2)\n" + "(partition p1 values less than(\"10\"))\n"
                        + "distributed by hash(k1) buckets 1\n" + "properties('replication_num' = '1',"
                        + " 'data_sort.sort_type' = 'lexical');"));

        // create z-order sort table, default col_num
        ExceptionChecker.expectThrowsNoException(() -> createTable(
                "create table test.zorder_tbl2\n" + "(k1 varchar(40), k2 int, k3 int)\n" + "duplicate key(k1, k2, k3)\n"
                        + "partition by range(k2)\n" + "(partition p1 values less than(\"10\"))\n"
                        + "distributed by hash(k1) buckets 1\n" + "properties('replication_num' = '1',"
                        + " 'data_sort.sort_type' = 'zorder');"));

        // create z-order sort table, define sort_col_num
        ExceptionChecker.expectThrowsNoException(() -> createTable(
                "create table test.zorder_tbl3\n" + "(k1 varchar(40), k2 int, k3 int)\n" + "duplicate key(k1, k2, k3)\n"
                        + "partition by range(k2)\n" + "(partition p1 values less than(\"10\"))\n"
                        + "distributed by hash(k1) buckets 1\n" + "properties('replication_num' = '1',"
                        + " 'data_sort.sort_type' = 'zorder',"
                        + " 'data_sort.col_num' = '2');"));
        // create z-order sort table, only 1 sort column
        ExceptionChecker
                .expectThrowsWithMsg(AnalysisException.class, "z-order needs 2 columns at least, 3 columns at most",
                        () -> createTable("create table test.zorder_tbl4\n" + "(k1 varchar(40), k2 int, k3 int)\n" + "duplicate key(k1, k2, k3)\n"
                                + "partition by range(k2)\n" + "(partition p1 values less than(\"10\"))\n"
                                + "distributed by hash(k1) buckets 1\n" + "properties('replication_num' = '1',"
                                + " 'data_sort.sort_type' = 'zorder',"
                                + " 'data_sort.col_num' = '1');"));
        // create z-order sort table, sort column is empty
        ExceptionChecker
                .expectThrowsWithMsg(AnalysisException.class, "param data_sort.col_num error",
                        () -> createTable("create table test.zorder_tbl4\n" + "(k1 varchar(40), k2 int, k3 int)\n" + "duplicate key(k1, k2, k3)\n"
                                + "partition by range(k2)\n" + "(partition p1 values less than(\"10\"))\n"
                                + "distributed by hash(k1) buckets 1\n" + "properties('replication_num' = '1',"
                                + " 'data_sort.sort_type' = 'zorder',"
                                + " 'data_sort.col_num' = '');"));
    }

    @Test
    public void testCreateTableWithArrayType() throws Exception {
        ExceptionChecker.expectThrowsNoException(() -> {
            createTable("create table test.table1(k1 INT, k2 Array<int>) duplicate key (k1) "
                    + "distributed by hash(k1) buckets 1 properties('replication_num' = '1');");
        });
        ExceptionChecker.expectThrowsNoException(() -> {
            createTable("create table test.table2(k1 INT, k2 Array<Array<int>>) duplicate key (k1) "
                    + "distributed by hash(k1) buckets 1 properties('replication_num' = '1');");
        });
        ExceptionChecker.expectThrowsNoException(() -> {
            createTable("CREATE TABLE test.table3 (\n"
                    + "  `k1` INT(11) NULL COMMENT \"\",\n"
                    + "  `k2` ARRAY<ARRAY<SMALLINT>> NULL COMMENT \"\",\n"
                    + "  `k3` ARRAY<ARRAY<ARRAY<INT(11)>>> NULL COMMENT \"\",\n"
                    + "  `k4` ARRAY<ARRAY<ARRAY<ARRAY<BIGINT>>>> NULL COMMENT \"\",\n"
                    + "  `k5` ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<CHAR>>>>> NULL COMMENT \"\",\n"
                    + "  `k6` ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<VARCHAR(20)>>>>>> NULL COMMENT \"\",\n"
                    + "  `k7` ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<DATE>>>>>>> NULL COMMENT \"\",\n"
                    + "  `k8` ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<DATETIME>>>>>>>> NULL COMMENT \"\",\n"
                    + "  `k11` ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<DECIMAL(20, 6)>>>>>>>>> NULL COMMENT \"\"\n"
                    + ") ENGINE=OLAP\n"
                    + "DUPLICATE KEY(`k1`)\n"
                    + "DISTRIBUTED BY HASH(`k1`) BUCKETS 3\n"
                    + "PROPERTIES (\n"
                    + "\"replication_allocation\" = \"tag.location.default: 1\"\n"
                    + ");");
        });
        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class, "Type exceeds the maximum nesting depth of 9",
                () -> {
                    createTable("CREATE TABLE test.table4 (\n"
                            + "  `k1` INT(11) NULL COMMENT \"\",\n"
                            + "  `k2` ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<DECIMAL(20, 6)>>>>>>>>>> NULL COMMENT \"\"\n"
                            + ") ENGINE=OLAP\n"
                            + "DUPLICATE KEY(`k1`)\n"
                            + "DISTRIBUTED BY HASH(`k1`) BUCKETS 3\n"
                            + "PROPERTIES (\n"
                            + "\"replication_allocation\" = \"tag.location.default: 1\"\n"
                            + ");");
                });

        ExceptionChecker.expectThrowsNoException(() -> {
            createTable("create table test.table5(\n"
                    + "\tk1 int,\n"
                    + "\tv1 array<int>\n"
                    + ") distributed by hash(k1) buckets 1\n"
                    + "properties(\"replication_num\" = \"1\");");
        });
    }
}
