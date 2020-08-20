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

import avro.shaded.com.google.common.collect.Lists;
import org.apache.doris.analysis.CreateDbStmt;
import org.apache.doris.analysis.CreateTableLikeStmt;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.UtFrameUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.List;
import java.util.UUID;

public class CreateTableLikeTest {
    private static String runningDir = "fe/mocked/CreateTableLikeTest/" + UUID.randomUUID().toString() + "/";

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
        String createDbStmtStr2 = "create database test2;";
        CreateDbStmt createDbStmt2 = (CreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt(createDbStmtStr2, connectContext);
        Catalog.getCurrentCatalog().createDb(createDbStmt2);
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

    private static void createTableLike(String sql) throws Exception {
        CreateTableLikeStmt createTableLikeStmt = (CreateTableLikeStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        Catalog.getCurrentCatalog().createTableLike(createTableLikeStmt);
    }

    private static void checkTableEqual(OlapTable newTable, OlapTable existedTable) {
        List<String> newCreateTableStmt = Lists.newArrayList();
        Catalog.getDdlStmt(newTable, newCreateTableStmt, null, null, false, true /* hide password */);
        List<String> existedTableStmt = Lists.newArrayList();
        Catalog.getDdlStmt(existedTable, existedTableStmt, null, null, false, true /* hide password */);
        Assert.assertEquals(newCreateTableStmt.get(0).replace(newTable.getName(), existedTable.getName()), existedTableStmt.get(0));
    }

    private static void checkCreateTableLike(String createTableSql, String createTableLikeSql,
                                             String newDbName, String existedDbName,
                                             String newTblName, String existedTblName) throws Exception {
        createTable(createTableSql);
        createTableLike(createTableLikeSql);
        Database newDb = Catalog.getCurrentCatalog().getDb("default_cluster:" + newDbName);
        Database existedDb = Catalog.getCurrentCatalog().getDb("default_cluster:" + existedDbName);
        OlapTable newTbl = (OlapTable) newDb.getTable(newTblName);
        OlapTable existedTbl = (OlapTable) existedDb.getTable(existedTblName);
        checkTableEqual(newTbl, existedTbl);
    }

    @Test
    public void testNormal() throws Exception {
        // 1. creat table with single partition
        String createTableSql = "create table test.testTbl1\n" + "(k1 int, k2 int)\n" + "duplicate key(k1)\n"
                + "distributed by hash(k2) buckets 1\n" + "properties('replication_num' = '1'); ";
        String createTableLikeSql = "create table test.testTbl1_like like test.testTbl1";
        String newDbName = "test";
        String newTblName = "testTbl1_like";
        String existedTblName = "testTbl1";
        checkCreateTableLike(createTableSql, createTableLikeSql, newDbName, newDbName, newTblName, existedTblName);
        // 2. create table with hash partition
        String createTableWithHashPartitionSql = "create table test.testTbl2\n" + "(k1 int, k2 int)\n"
                + "duplicate key(k1)\n" + "partition by range(k2)\n" + "(partition p1 values less than(\"10\"))\n"
                + "distributed by hash(k2) buckets 1\n" + "properties('replication_num' = '1'); ";
        String createTableLikeSql2 = "create table test.testTbl2_like like test.testTbl2";
        String newDbName2 = "test";
        String newTblName2 = "testTbl2_like";
        String existedTblName2 = "testTbl2";
        checkCreateTableLike(createTableWithHashPartitionSql, createTableLikeSql2, newDbName2, newDbName2, newTblName2, existedTblName2);
        // 3. create aggregate table
        String createAggTableSql3 = "create table test.testTbl3\n" + "(k1 varchar(40), k2 int, v1 int sum)\n"
                + "partition by range(k2)\n" + "(partition p1 values less than(\"10\"))\n"
                + "distributed by hash(k1) buckets 1\n" + "properties('replication_num' = '1');";
        String createTableLikeSql3 = "create table test.testTbl3_like like test.testTbl3";
        String newDbName3 = "test";
        String newTblName3 = "testTbl3_like";
        String existedTblName3 = "testTbl3";
        checkCreateTableLike(createAggTableSql3, createTableLikeSql3, newDbName3, newDbName3, newTblName3, existedTblName3);
        // 4. create aggregate table without partition
        String createAggTableWithoutPartitionSql4 = "create table test.testTbl4\n" + "(k1 varchar(40), k2 int, k3 int)\n" + "duplicate key(k1, k2, k3)\n"
                + "partition by range(k2)\n" + "()\n"
                + "distributed by hash(k1) buckets 1\n" + "properties('replication_num' = '1');";
        String createTableLikeSql4 = "create table test.testTbl4_like like test.testTbl4";
        String newDbName4 = "test";
        String newTblName4 = "testTbl4_like";
        String existedTblName4 = "testTbl4";
        checkCreateTableLike(createAggTableWithoutPartitionSql4, createTableLikeSql4, newDbName4, newDbName4, newTblName4, existedTblName4);
        // 5. create table from different db
        String createTableFromDiffDb5 = "create table test.testTbl5\n" + "(k1 varchar(40), k2 int, k3 int)\n" + "duplicate key(k1, k2, k3)\n"
                + "partition by range(k2)\n" + "()\n"
                + "distributed by hash(k1) buckets 1\n" + "properties('replication_num' = '1');";
        String createTableLikeSql5 = "create table test2.testTbl5_like like test.testTbl5";
        String newDbName5 = "test2";
        String existedDbName5 = "test";
        String newTblName5 = "testTbl5_like";
        String existedTblName5 = "testTbl5";
        checkCreateTableLike(createTableFromDiffDb5, createTableLikeSql5, newDbName5, existedDbName5, newTblName5, existedTblName5);
        // 6. create table from dynamic partition table
        String createDynamicTblSql = "CREATE TABLE test.`dynamic_partition_normal` (\n" +
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
        String createTableLikeSql6 = "create table test.dynamic_partition_normal_like like test.dynamic_partition_normal";
        String newDbName6 = "test";
        String newTblName6 = "dynamic_partition_normal_like";
        String existedTblName6 = "dynamic_partition_normal";
        checkCreateTableLike(createDynamicTblSql, createTableLikeSql6, newDbName6, newDbName6, newTblName6, existedTblName6);
    }

    @Test
    public void testAbnormal() throws Exception {
        // 1. create table with same name
        String createTableSql = "create table test.testAbTbl1\n" + "(k1 int, k2 int)\n" + "duplicate key(k1)\n"
                + "distributed by hash(k2) buckets 1\n" + "properties('replication_num' = '1'); ";
        String createTableLikeSql = "create table test.testAbTbl1 like test.testAbTbl1";
        String newDbName = "test";
        String newTblName = "testAbTbl1";
        ExceptionChecker.expectThrowsWithMsg(DdlException.class, "Table 'testAbTbl1' already exists",
                () -> checkCreateTableLike(createTableSql, createTableLikeSql, newDbName, newDbName, newTblName, newTblName));
        // 2. create table with not existed DB
        String createTableSql2 = "create table test.testAbTbl2\n" + "(k1 int, k2 int)\n" + "duplicate key(k1)\n"
                + "distributed by hash(k2) buckets 1\n" + "properties('replication_num' = '1'); ";
        String createTableLikeSql2 = "create table fake_test.testAbTbl2_like like test.testAbTbl1";
        String newDbName2 = "fake_test";
        String existedDbName2 = "test";
        String newTblName2 = "testAbTbl2_like";
        String existedTblName2 = "testAbTbl1";
        ExceptionChecker.expectThrowsWithMsg(DdlException.class, "Unknown database 'default_cluster:fake_test'",
                () -> checkCreateTableLike(createTableSql2, createTableLikeSql2, newDbName2, existedDbName2, newTblName2, existedTblName2));
    }
}
