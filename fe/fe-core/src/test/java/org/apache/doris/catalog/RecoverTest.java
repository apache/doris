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
import org.apache.doris.analysis.DropDbStmt;
import org.apache.doris.analysis.DropTableStmt;
import org.apache.doris.analysis.RecoverDbStmt;
import org.apache.doris.analysis.RecoverPartitionStmt;
import org.apache.doris.analysis.RecoverTableStmt;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.DdlException;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.utframe.UtFrameUtils;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.UUID;

public class RecoverTest {

    private static String runningDir = "fe/mocked/RecoverTest/" + UUID.randomUUID().toString() + "/";

    private static ConnectContext connectContext;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createDorisCluster(runningDir);

        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
    }


    @AfterClass
    public static void tearDown() {
        File file = new File(runningDir);
        file.delete();
    }

    private static void createDb(String db) throws Exception {
        CreateDbStmt createDbStmt = (CreateDbStmt)UtFrameUtils.parseAndAnalyzeStmt("create database " + db, connectContext);
        Catalog.getCurrentCatalog().createDb(createDbStmt);
    }

    private static void createTable(String sql) throws Exception {
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        Catalog.getCurrentCatalog().createTable(createTableStmt);
    }

    private static void dropDb(String db) throws Exception {
        DropDbStmt dropDbStmt = (DropDbStmt)UtFrameUtils.parseAndAnalyzeStmt("drop database " + db, connectContext);
        Catalog.getCurrentCatalog().dropDb(dropDbStmt);
    }

    private static void dropTable(String db, String tbl) throws Exception {
        DropTableStmt dropTableStmt = (DropTableStmt) UtFrameUtils.parseAndAnalyzeStmt("drop table " + db + "." + tbl, connectContext);
        Catalog.getCurrentCatalog().dropTable(dropTableStmt);
    }

    private static void dropPartition(String db, String tbl, String part) throws Exception {
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseAndAnalyzeStmt(
                "alter table " + db + "." + tbl + " drop partition " + part, connectContext);
        Catalog.getCurrentCatalog().getAlterInstance().processAlterTable(alterTableStmt);
    }

    private static void recoverDb(String db) throws Exception {
        RecoverDbStmt recoverDbStmt = (RecoverDbStmt) UtFrameUtils.parseAndAnalyzeStmt("recover database " + db, connectContext);
        Catalog.getCurrentCatalog().recoverDatabase(recoverDbStmt);
    }

    private static void recoverTable(String db, String tbl) throws Exception {
        RecoverTableStmt recoverTableStmt = (RecoverTableStmt) UtFrameUtils.parseAndAnalyzeStmt("recover table " + db + "." + tbl, connectContext);
        Catalog.getCurrentCatalog().recoverTable(recoverTableStmt);
    }

    private static void recoverPartition(String db, String tbl, String part) throws Exception {
        RecoverPartitionStmt recoverPartitionStmt = (RecoverPartitionStmt) UtFrameUtils.parseAndAnalyzeStmt(
                "recover partition " + part + " from " + db + "." + tbl, connectContext);
        Catalog.getCurrentCatalog().recoverPartition(recoverPartitionStmt);
    }

    private static boolean checkDbExist(String dbName) {
        Database db = Catalog.getCurrentCatalog().getDb(ClusterNamespace.getFullName(SystemInfoService.DEFAULT_CLUSTER, dbName));
        return db != null;
    }

    private static boolean checkTableExist(String dbName, String tblName) {
        Database db = Catalog.getCurrentCatalog().getDb(ClusterNamespace.getFullName(SystemInfoService.DEFAULT_CLUSTER, dbName));
        if (db == null) {
            return false;
        }

        Table tbl = db.getTable(tblName);
        return tbl != null;
    }

    private static boolean checkPartitionExist(String dbName, String tblName, String partName) {
        Database db = Catalog.getCurrentCatalog().getDb(ClusterNamespace.getFullName(SystemInfoService.DEFAULT_CLUSTER, dbName));
        if (db == null) {
            return false;
        }

        Table tbl = db.getTable(tblName);
        if (tbl == null) {
            return false;
        }

        Partition partition = tbl.getPartition(partName);
        return partition != null;
    }



    @Test
    public void testRecover() throws Exception {
        createDb("test");
        createTable("CREATE TABLE test.`table1` (\n" +
                "  `event_date` date NOT NULL COMMENT \"\",\n" +
                "  `app_name` varchar(64) NOT NULL COMMENT \"\",\n" +
                "  `package_name` varchar(64) NOT NULL COMMENT \"\",\n" +
                "  `age` varchar(32) NOT NULL COMMENT \"\",\n" +
                "  `gender` varchar(32) NOT NULL COMMENT \"\",\n" +
                "  `level` varchar(64) NOT NULL COMMENT \"\",\n" +
                "  `city` varchar(64) NOT NULL COMMENT \"\",\n" +
                "  `model` varchar(64) NOT NULL COMMENT \"\",\n" +
                "  `brand` varchar(64) NOT NULL COMMENT \"\",\n" +
                "  `hours` varchar(16) NOT NULL COMMENT \"\",\n" +
                "  `use_num` int(11) SUM NOT NULL COMMENT \"\",\n" +
                "  `use_time` double SUM NOT NULL COMMENT \"\",\n" +
                "  `start_times` bigint(20) SUM NOT NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "AGGREGATE KEY(`event_date`, `app_name`, `package_name`, `age`, `gender`, `level`, `city`, `model`, `brand`, `hours`)\n"
                +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE(`event_date`)\n" +
                "(PARTITION p1 VALUES [('2020-02-27'), ('2020-03-02')),\n" +
                "PARTITION p2 VALUES [('2020-03-02'), ('2020-03-07')))\n" +
                "DISTRIBUTED BY HASH(`event_date`, `app_name`, `package_name`, `age`, `gender`, `level`, `city`, `model`, `brand`, `hours`) BUCKETS 1\n"
                +
                "PROPERTIES (\n" +
                " \"replication_num\" = \"1\"\n" +
                ");");

        Assert.assertTrue(checkDbExist("test"));
        Assert.assertTrue(checkTableExist("test", "table1"));

        dropDb("test");
        Assert.assertFalse(checkDbExist("test"));
        Assert.assertFalse(checkTableExist("test", "table1"));

        recoverDb("test");
        Assert.assertTrue(checkDbExist("test"));
        Assert.assertTrue(checkTableExist("test", "table1"));

        dropTable("test","table1");
        Assert.assertTrue(checkDbExist("test"));
        Assert.assertFalse(checkTableExist("test", "table1"));

        recoverTable("test","table1");
        Assert.assertTrue(checkDbExist("test"));
        Assert.assertTrue(checkTableExist("test", "table1"));

        dropTable("test","table1");
        Assert.assertTrue(checkDbExist("test"));
        Assert.assertFalse(checkTableExist("test", "table1"));

        createTable("CREATE TABLE test.`table1` (\n" +
                "  `event_date` date NOT NULL COMMENT \"\",\n" +
                "  `app_name` varchar(64) NOT NULL COMMENT \"\",\n" +
                "  `package_name` varchar(64) NOT NULL COMMENT \"\",\n" +
                "  `age` varchar(32) NOT NULL COMMENT \"\",\n" +
                "  `gender` varchar(32) NOT NULL COMMENT \"\",\n" +
                "  `level` varchar(64) NOT NULL COMMENT \"\",\n" +
                "  `city` varchar(64) NOT NULL COMMENT \"\",\n" +
                "  `model` varchar(64) NOT NULL COMMENT \"\",\n" +
                "  `brand` varchar(64) NOT NULL COMMENT \"\",\n" +
                "  `hours` varchar(16) NOT NULL COMMENT \"\",\n" +
                "  `use_num` int(11) SUM NOT NULL COMMENT \"\",\n" +
                "  `use_time` double SUM NOT NULL COMMENT \"\",\n" +
                "  `start_times` bigint(20) SUM NOT NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "AGGREGATE KEY(`event_date`, `app_name`, `package_name`, `age`, `gender`, `level`, `city`, `model`, `brand`, `hours`)\n"
                +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE(`event_date`)\n" +
                "(PARTITION p1 VALUES [('2020-02-27'), ('2020-03-02')),\n" +
                "PARTITION p2 VALUES [('2020-03-02'), ('2020-03-07')))\n" +
                "DISTRIBUTED BY HASH(`event_date`, `app_name`, `package_name`, `age`, `gender`, `level`, `city`, `model`, `brand`, `hours`) BUCKETS 1\n"
                +
                "PROPERTIES (\n" +
                " \"replication_num\" = \"1\"\n" +
                ");");
        Assert.assertTrue(checkDbExist("test"));
        Assert.assertTrue(checkTableExist("test", "table1"));

        try {
            recoverTable("test","table1");
            Assert.fail("should not recover succeed");
        } catch (DdlException e) {
            e.printStackTrace();
        }

        Assert.assertTrue(checkPartitionExist("test", "table1", "p1"));
        dropPartition("test","table1", "p1");
        Assert.assertFalse(checkPartitionExist("test", "table1", "p1"));

        recoverPartition("test","table1", "p1");
        Assert.assertTrue(checkPartitionExist("test", "table1", "p1"));
    }

}
