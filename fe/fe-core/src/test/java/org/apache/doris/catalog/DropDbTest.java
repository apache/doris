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
import org.apache.doris.analysis.DropDbStmt;
import org.apache.doris.analysis.RecoverDbStmt;
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

public class DropDbTest {
    private static String runningDir = "fe/mocked/DropDbTest/" + UUID.randomUUID().toString() + "/";

    private static ConnectContext connectContext;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createDorisCluster(runningDir);

        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        // create database
        String createDbStmtStr1 = "create database test1;";
        String createDbStmtStr2 = "create database test2;";
        String createTablleStr1 = "create table test1.tbl1(k1 int, k2 bigint) duplicate key(k1) "
                + "distributed by hash(k2) buckets 1" + " properties('replication_num' = '1');";
        String createTablleStr2 = "create table test2.tbl1" + "(k1 int, k2 bigint)" + " duplicate key(k1) "
                + "distributed by hash(k2) buckets 1 " + "properties('replication_num' = '1');";
        createDb(createDbStmtStr1);
        createDb(createDbStmtStr2);
        createTable(createTablleStr1);
        createTable(createTablleStr2);
    }

    @AfterClass
    public static void tearDown() {
        File file = new File(runningDir);
        file.delete();
    }

    private static void createDb(String sql) throws Exception {
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        Catalog.getCurrentCatalog().createDb(createDbStmt);
    }

    private static void dropDb(String sql) throws Exception {
        DropDbStmt dropDbStmt = (DropDbStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        Catalog.getCurrentCatalog().dropDb(dropDbStmt);
    }

    private static void createTable(String sql) throws Exception {
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        Catalog.getCurrentCatalog().createTable(createTableStmt);
    }

    @Test
    public void testNormalDropDb() throws Exception {
        Database db = Catalog.getCurrentCatalog().getDb("default_cluster:test1");
        OlapTable table = (OlapTable) db.getTable("tbl1");
        Partition partition = table.getAllPartitions().iterator().next();
        long tabletId = partition.getBaseIndex().getTablets().get(0).getId();
        String dropDbSql = "drop database test1";
        dropDb(dropDbSql);
        db = Catalog.getCurrentCatalog().getDb("default_cluster:test1");
        List<Replica> replicaList = Catalog.getCurrentCatalog().getTabletInvertedIndex().getReplicasByTabletId(tabletId);
        Assert.assertNull(db);
        Assert.assertEquals(1, replicaList.size());
        String recoverDbSql = "recover database test1";
        RecoverDbStmt recoverDbStmt = (RecoverDbStmt) UtFrameUtils.parseAndAnalyzeStmt(recoverDbSql, connectContext);
        Catalog.getCurrentCatalog().recoverDatabase(recoverDbStmt);
        db = Catalog.getCurrentCatalog().getDb("default_cluster:test1");
        Assert.assertNotNull(db);
        Assert.assertEquals("default_cluster:test1", db.getFullName());
        table = (OlapTable) db.getTable("tbl1");
        Assert.assertNotNull(table);
        Assert.assertEquals("tbl1", table.getName());
    }

    @Test
    public void testForceDropDb() throws Exception {
        String dropDbSql = "drop database test2 force";
        Database db = Catalog.getCurrentCatalog().getDb("default_cluster:test2");
        OlapTable table = (OlapTable) db.getTable("tbl1");
        Partition partition = table.getAllPartitions().iterator().next();
        long tabletId = partition.getBaseIndex().getTablets().get(0).getId();
        dropDb(dropDbSql);
        db = Catalog.getCurrentCatalog().getDb("default_cluster:test2");
        List<Replica> replicaList = Catalog.getCurrentCatalog().getTabletInvertedIndex().getReplicasByTabletId(tabletId);
        Assert.assertNull(db);
        Assert.assertTrue(replicaList.isEmpty());
        String recoverDbSql = "recover database test2";
        RecoverDbStmt recoverDbStmt = (RecoverDbStmt) UtFrameUtils.parseAndAnalyzeStmt(recoverDbSql, connectContext);
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "Unknown database 'default_cluster:test2'",
                () -> Catalog.getCurrentCatalog().recoverDatabase(recoverDbStmt));
    }
}
