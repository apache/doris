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
import org.apache.doris.analysis.CreateTableLikeStmt;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.UtFrameUtils;

import com.google.common.collect.Lists;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.List;
import java.util.UUID;

/**
 * @author wangcong
 * @version 1.0
 * @date 2020/10/7 12:31 下午
 */
public class CreateTableLikeTest {
    private static String runningDir = "fe/mocked/CreateTableLikeTest/" + UUID.randomUUID().toString() + "/";

    private static ConnectContext connectContext;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createDorisCluster(runningDir);

        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        Config.enable_odbc_mysql_broker_table = true;
        // create database
        String createDbStmtStr = "create database test;";
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt(createDbStmtStr, connectContext);
        Env.getCurrentEnv().createDb(createDbStmt);
        String createDbStmtStr2 = "create database test2;";
        CreateDbStmt createDbStmt2 = (CreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt(createDbStmtStr2, connectContext);
        Env.getCurrentEnv().createDb(createDbStmt2);
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

    private static void createTableLike(String sql) throws Exception {
        CreateTableLikeStmt createTableLikeStmt =
                (CreateTableLikeStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        Env.getCurrentEnv().createTableLike(createTableLikeStmt);
    }

    private static void checkTableEqual(Table newTable, Table existedTable, int rollupSize) {
        List<String> newCreateTableStmt = Lists.newArrayList();
        List<String> newAddRollupStmt = Lists.newArrayList();
        Env.getDdlStmt(newTable, newCreateTableStmt, null, newAddRollupStmt, false, true /* hide password */, -1L);
        List<String> existedTableStmt = Lists.newArrayList();
        List<String> existedAddRollupStmt = Lists.newArrayList();
        Env.getDdlStmt(existedTable, existedTableStmt, null, existedAddRollupStmt, false, true /* hide password */,
                -1L);
        Assert.assertEquals(newCreateTableStmt.get(0).replace(newTable.getName(), existedTable.getName()),
                existedTableStmt.get(0));
        checkTableRollup(existedAddRollupStmt, newAddRollupStmt, newTable.getName(), existedTable.getName(),
                rollupSize);
    }

    private static void checkTableRollup(List<String> existedAddRollupStmt, List<String> newAddRollupStmt,
            String newTableName, String existedTableName, int rollupSize) {
        if (rollupSize != 0) {
            List<String> addRollupStmt = Lists.newArrayList();
            for (String aaRollupStmt : newAddRollupStmt) {
                addRollupStmt.add(aaRollupStmt.replace(newTableName, existedTableName));
            }
            Assert.assertEquals(addRollupStmt.size(), rollupSize);
            Assert.assertTrue(existedAddRollupStmt.containsAll(addRollupStmt));
        }
    }

    private static void checkCreateOlapTableLike(String createTableSql, String createTableLikeSql, String newDbName,
            String existedDbName, String newTblName, String existedTblName) throws Exception {
        checkCreateOlapTableLike(createTableSql, createTableLikeSql, newDbName, existedDbName, newTblName,
                existedTblName, 0);
    }

    private static void checkCreateOlapTableLike(String createTableSql, String createTableLikeSql, String newDbName,
            String existedDbName, String newTblName, String existedTblName, int rollupSize) throws Exception {
        createTable(createTableSql);
        createTableLike(createTableLikeSql);
        Database newDb =
                Env.getCurrentInternalCatalog().getDbOrDdlException("" + newDbName);
        Database existedDb = Env.getCurrentInternalCatalog()
                .getDbOrDdlException("" + existedDbName);
        OlapTable newTbl = (OlapTable) newDb.getTableOrDdlException(newTblName);
        OlapTable existedTbl = (OlapTable) existedDb.getTableOrDdlException(existedTblName);
        checkTableEqual(newTbl, existedTbl, rollupSize);
    }

    private static void checkCreateMysqlTableLike(String createTableSql, String createTableLikeSql, String newDbName,
            String existedDbName, String newTblName, String existedTblName) throws Exception {

        createTable(createTableSql);
        createTableLike(createTableLikeSql);
        Database newDb =
                Env.getCurrentInternalCatalog().getDbOrDdlException("" + newDbName);
        Database existedDb = Env.getCurrentInternalCatalog()
                .getDbOrDdlException("" + existedDbName);
        MysqlTable newTbl = (MysqlTable) newDb.getTableOrDdlException(newTblName);
        MysqlTable existedTbl = (MysqlTable) existedDb.getTableOrDdlException(existedTblName);
        checkTableEqual(newTbl, existedTbl, 0);
    }

    @Test
    public void testDeepCopyOfDistributionInfo() throws Exception {
        String createTableSql = "CREATE TABLE IF NOT EXISTS test.bucket_distribution_test (\n"
                + "  `dt` bigint(20) NULL COMMENT \"\",\n" + "  `id1` bigint(20) NULL COMMENT \"\",\n"
                + "  `last_time` varchar(20) MAX NULL COMMENT \"\"\n" + ") ENGINE=OLAP\n"
                + "AGGREGATE KEY(`dt`, `id1`) \n" + "PARTITION BY RANGE(`dt`) \n" + "(\n"
                + "    PARTITION p1 VALUES  [(\"20220101\"),(\"20220201\")),\n"
                + "    partition p2 VALUES  [(\"20211201\"),(\"20220101\"))\n" + ")\n"
                + "DISTRIBUTED BY HASH(`id1`) BUCKETS 1\n" + "PROPERTIES (\n" + "\"replication_num\" = \"1\"\n" + ");";
        createTable(createTableSql);
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException("test");
        OlapTable table = (OlapTable) db.getTableOrDdlException("bucket_distribution_test");
        DistributionInfo defaultInfo = table.getDefaultDistributionInfo();
        DistributionInfo previous = null;
        for (Partition p : table.getPartitions()) {
            Assert.assertFalse(p.getDistributionInfo() == defaultInfo);
            Assert.assertFalse(p.getDistributionInfo() == previous);
            previous = p.getDistributionInfo();
        }
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
        checkCreateOlapTableLike(createTableSql, createTableLikeSql, newDbName, newDbName, newTblName, existedTblName);
        // 2. create table with hash partition
        String createTableWithHashPartitionSql =
                "create table test.testTbl2\n" + "(k1 int, k2 int)\n" + "duplicate key(k1)\n"
                        + "partition by range(k2)\n" + "(partition p1 values less than(\"10\"))\n"
                        + "distributed by hash(k2) buckets 1\n" + "properties('replication_num' = '1'); ";
        String createTableLikeSql2 = "create table test.testTbl2_like like test.testTbl2";
        String newDbName2 = "test";
        String newTblName2 = "testTbl2_like";
        String existedTblName2 = "testTbl2";
        checkCreateOlapTableLike(createTableWithHashPartitionSql, createTableLikeSql2, newDbName2, newDbName2,
                newTblName2, existedTblName2);
        // 3. create aggregate table
        String createAggTableSql3 =
                "create table test.testTbl3\n" + "(k1 varchar(40), k2 int, v1 int sum)\n" + "partition by range(k2)\n"
                        + "(partition p1 values less than(\"10\"))\n" + "distributed by hash(k1) buckets 1\n"
                        + "properties('replication_num' = '1');";
        String createTableLikeSql3 = "create table test.testTbl3_like like test.testTbl3";
        String newDbName3 = "test";
        String newTblName3 = "testTbl3_like";
        String existedTblName3 = "testTbl3";
        checkCreateOlapTableLike(createAggTableSql3, createTableLikeSql3, newDbName3, newDbName3, newTblName3,
                existedTblName3);
        // 4. create aggregate table without partition
        String createAggTableWithoutPartitionSql4 =
                "create table test.testTbl4\n" + "(k1 varchar(40), k2 int, k3 int)\n" + "duplicate key(k1, k2, k3)\n"
                        + "partition by range(k2)\n" + "()\n" + "distributed by hash(k1) buckets 1\n"
                        + "properties('replication_num' = '1');";
        String createTableLikeSql4 = "create table test.testTbl4_like like test.testTbl4";
        String newDbName4 = "test";
        String newTblName4 = "testTbl4_like";
        String existedTblName4 = "testTbl4";
        checkCreateOlapTableLike(createAggTableWithoutPartitionSql4, createTableLikeSql4, newDbName4, newDbName4,
                newTblName4, existedTblName4);
        // 5. create table from different db
        String createTableFromDiffDb5 =
                "create table test.testTbl5\n" + "(k1 varchar(40), k2 int, k3 int)\n" + "duplicate key(k1, k2, k3)\n"
                        + "partition by range(k2)\n" + "()\n" + "distributed by hash(k1) buckets 1\n"
                        + "properties('replication_num' = '1');";
        String createTableLikeSql5 = "create table test2.testTbl5_like like test.testTbl5";
        String newDbName5 = "test2";
        String existedDbName5 = "test";
        String newTblName5 = "testTbl5_like";
        String existedTblName5 = "testTbl5";
        checkCreateOlapTableLike(createTableFromDiffDb5, createTableLikeSql5, newDbName5, existedDbName5, newTblName5,
                existedTblName5);
        // 6. create table from dynamic partition table
        String createDynamicTblSql =
                "CREATE TABLE test.`dynamic_partition_normal` (\n" + "  `k1` date NULL COMMENT \"\",\n"
                        + "  `k2` int NULL COMMENT \"\",\n" + "  `k3` smallint NULL COMMENT \"\",\n"
                        + "  `v1` varchar(2048) NULL COMMENT \"\",\n" + "  `v2` datetime NULL COMMENT \"\"\n"
                        + ") ENGINE=OLAP\n" + "DUPLICATE KEY(`k1`, `k2`, `k3`)\n" + "COMMENT \"OLAP\"\n"
                        + "PARTITION BY RANGE (k1)\n" + "(\n" + "PARTITION p1 VALUES LESS THAN (\"2014-01-01\"),\n"
                        + "PARTITION p2 VALUES LESS THAN (\"2014-06-01\"),\n"
                        + "PARTITION p3 VALUES LESS THAN (\"2014-12-01\")\n" + ")\n"
                        + "DISTRIBUTED BY HASH(`k1`) BUCKETS 32\n" + "PROPERTIES (\n" + "\"replication_num\" = \"1\",\n"
                        + "\"dynamic_partition.enable\" = \"true\",\n" + "\"dynamic_partition.start\" = \"-3\",\n"
                        + "\"dynamic_partition.end\" = \"3\",\n" + "\"dynamic_partition.time_unit\" = \"day\",\n"
                        + "\"dynamic_partition.prefix\" = \"p\",\n" + "\"dynamic_partition.buckets\" = \"1\"\n" + ");";
        String createTableLikeSql6 =
                "create table test.dynamic_partition_normal_like" + " like test.dynamic_partition_normal";
        String newDbName6 = "test";
        String newTblName6 = "dynamic_partition_normal_like";
        String existedTblName6 = "dynamic_partition_normal";
        checkCreateOlapTableLike(createDynamicTblSql, createTableLikeSql6, newDbName6, newDbName6, newTblName6,
                existedTblName6);
        // 7. create table from colocate table
        String createColocateTblSql = "create table test.colocateTbl (\n" + " `k1` int NULL COMMENT \"\",\n"
                + " `k2` varchar(10) NULL COMMENT \"\"\n" + ") ENGINE=OLAP\n" + "DUPLICATE KEY(`k1`, `k2`)\n"
                + "COMMENT \"OLAP\"\n" + "DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 1\n" + "PROPERTIES (\n"
                + " \"replication_num\" = \"1\",\n" + " \"colocate_with\" = \"test_group\"\n" + ");";
        String createTableLikeSql7 = "create table test.colocateTbl_like like test.colocateTbl";
        String newDbName7 = "test";
        String newTblName7 = "colocateTbl_like";
        String existedTblName7 = "colocateTbl";
        checkCreateOlapTableLike(createColocateTblSql, createTableLikeSql7, newDbName7, newDbName7, newTblName7,
                existedTblName7);

        // 8. creat non-OLAP table
        String createNonOlapTableSql =
                "create table test.testMysqlTbl\n" + "(k1 DATE, k2 INT, k3 SMALLINT, k4 VARCHAR(2048), k5 DATETIME)\n"
                        + "ENGINE=mysql\nPROPERTIES(\n" + "\"host\" = \"127.0.0.1\",\n" + "\"port\" = \"8239\",\n"
                        + "\"user\" = \"mysql_passwd\",\n" + "\"password\" = \"mysql_passwd\",\n"
                        + "\"database\" = \"mysql_db_test\",\n" + "\"table\" = \"mysql_table_test\");";
        String createTableLikeSql8 = "create table test.testMysqlTbl_like like test.testMysqlTbl";
        String newDbName8 = "test";
        String existedDbName8 = "test";
        String newTblName8 = "testMysqlTbl_like";
        String existedTblName8 = "testMysqlTbl";
        checkCreateMysqlTableLike(createNonOlapTableSql, createTableLikeSql8, newDbName8, existedDbName8, newTblName8,
                existedTblName8);

        // 9. test if not exist
        String createTableLikeSql9 = "create table test.testMysqlTbl_like like test.testMysqlTbl";
        try {
            createTableLike(createTableLikeSql9);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("already exists"));
        }
        createTableLikeSql9 = "create table if not exists test.testMysqlTbl_like like test.testMysqlTbl";
        try {
            createTableLike(createTableLikeSql9);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }

        // 10. test create table like with rollup
        String createTableWithRollup =
                "CREATE TABLE IF NOT EXISTS test.table_with_rollup\n" + "(\n" + "    event_day DATE,\n"
                        + "    siteid INT DEFAULT '10',\n" + "    citycode SMALLINT,\n"
                        + "    username VARCHAR(32) DEFAULT '',\n" + "    pv BIGINT SUM DEFAULT '0'\n" + ")\n"
                        + "AGGREGATE KEY(event_day, siteid, citycode, username)\n" + "PARTITION BY RANGE(event_day)\n"
                        + "(\n" + "    PARTITION p201706 VALUES LESS THAN ('2021-07-01'),\n"
                        + "    PARTITION p201707 VALUES LESS THAN ('2021-08-01'),\n"
                        + "    PARTITION p201708 VALUES LESS THAN ('2021-09-01')\n" + ")\n"
                        + "DISTRIBUTED BY HASH(siteid) BUCKETS 10\n" + "ROLLUP\n" + "(\n" + "r(event_day,pv),\n"
                        + "r1(event_day,siteid,pv),\n" + "r2(siteid,pv),\n" + "r3(siteid,citycode,username,pv)\n"
                        + ")\n" + "PROPERTIES(\"replication_num\" = \"1\");";

        String createTableLikeWithRollupSql1Dot1 =
                "create table test.table_like_rollup" + " like test.table_with_rollup with rollup (r1,r2)";
        String createTableLikeWithRollupSql1Dot2 =
                "create table test.table_like_rollup1" + " like test.table_with_rollup with rollup";

        String newDbName10 = "test";
        String existedDbName10 = "test";
        String newTblName10Dot1 = "table_like_rollup";
        String newTblName10Dot2 = "table_like_rollup1";
        String existedTblName10 = "table_with_rollup";
        checkCreateOlapTableLike(createTableWithRollup, createTableLikeWithRollupSql1Dot1, newDbName10, existedDbName10,
                newTblName10Dot1, existedTblName10, 2);
        checkCreateOlapTableLike(createTableWithRollup, createTableLikeWithRollupSql1Dot2, newDbName10, existedDbName10,
                newTblName10Dot2, existedTblName10, 4);

        String createTableLikeWithRollupSql2Dot1 =
                "create table test2.table_like_rollup" + " like test.table_with_rollup with rollup (r1,r2)";
        String createTableLikeWithRollupSql2Dot2 =
                "create table test2.table_like_rollup1" + " like test.table_with_rollup with rollup";

        String newDbName11 = "test2";
        String existedDbName11 = "test";
        String newTblName11Dot1 = "table_like_rollup";
        String newTblName11Dot2 = "table_like_rollup1";
        String existedTblName11 = "table_with_rollup";
        checkCreateOlapTableLike(createTableWithRollup, createTableLikeWithRollupSql2Dot1, newDbName11, existedDbName11,
                newTblName11Dot1, existedTblName11, 2);
        checkCreateOlapTableLike(createTableWithRollup, createTableLikeWithRollupSql2Dot2, newDbName11, existedDbName11,
                newTblName11Dot2, existedTblName11, 4);

    }

    @Test
    public void testAbnormal() {
        // 1. create table with same name
        String createTableSql = "create table test.testAbTbl1\n" + "(k1 int, k2 int)\n" + "duplicate key(k1)\n"
                + "distributed by hash(k2) buckets 1\n" + "properties('replication_num' = '1'); ";
        String createTableLikeSql = "create table test.testAbTbl1 like test.testAbTbl1";
        String newDbName = "test";
        String newTblName = "testAbTbl1";
        ExceptionChecker.expectThrowsWithMsg(DdlException.class, "Table 'testAbTbl1' already exists",
                () -> checkCreateOlapTableLike(createTableSql, createTableLikeSql, newDbName, newDbName, newTblName,
                        newTblName));
        // 2. create table with not existed DB
        String createTableSql2 = "create table test.testAbTbl2\n" + "(k1 int, k2 int)\n" + "duplicate key(k1)\n"
                + "distributed by hash(k2) buckets 1\n" + "properties('replication_num' = '1'); ";
        String createTableLikeSql2 = "create table fake_test.testAbTbl2_like like test.testAbTbl1";
        String newDbName2 = "fake_test";
        String existedDbName2 = "test";
        String newTblName2 = "testAbTbl2_like";
        String existedTblName2 = "testAbTbl1";
        ExceptionChecker.expectThrowsWithMsg(DdlException.class, "Unknown database 'fake_test'",
                () -> checkCreateOlapTableLike(createTableSql2, createTableLikeSql2, newDbName2, existedDbName2,
                        newTblName2, existedTblName2));

        //3. add not existed rollup
        String createTableWithRollup =
                "CREATE TABLE IF NOT EXISTS test.table_with_rollup\n" + "(\n" + "    event_day DATE,\n"
                        + "    siteid INT DEFAULT '10',\n" + "    citycode SMALLINT,\n"
                        + "    username VARCHAR(32) DEFAULT '',\n" + "    pv BIGINT SUM DEFAULT '0'\n" + ")\n"
                        + "AGGREGATE KEY(event_day, siteid, citycode, username)\n" + "PARTITION BY RANGE(event_day)\n"
                        + "(\n" + "    PARTITION p201706 VALUES LESS THAN ('2021-07-01'),\n"
                        + "    PARTITION p201707 VALUES LESS THAN ('2021-08-01'),\n"
                        + "    PARTITION p201708 VALUES LESS THAN ('2021-09-01')\n" + ")\n"
                        + "DISTRIBUTED BY HASH(siteid) BUCKETS 10\n" + "ROLLUP\n" + "(\n" + "r(event_day,pv),\n"
                        + "r1(event_day,siteid,pv),\n" + "r2(siteid,pv),\n" + "r3(siteid,citycode,username,pv)\n"
                        + ")\n" + "PROPERTIES(\"replication_num\" = \"1\");";

        String createTableLikeWithRollupSq3 =
                "create table test.table_like_rollup" + " like test.table_with_rollup with rollup (r11)";

        String newDbName3 = "test";
        String existedDbName3 = "test";
        String newTblName3 = "table_like_rollup";
        String existedTblName3 = "table_with_rollup";
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "Rollup index[r11] not exists in Table[table_with_rollup]",
                () -> checkCreateOlapTableLike(createTableWithRollup, createTableLikeWithRollupSq3, newDbName3,
                        existedDbName3, newTblName3, existedTblName3, 1));
    }
}
