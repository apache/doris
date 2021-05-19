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
import org.apache.doris.analysis.DropTableStmt;
import org.apache.doris.analysis.RecoverTableStmt;
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

public class DropTableTest {
    private static String runningDir = "fe/mocked/DropTableTest/" + UUID.randomUUID().toString() + "/";

    private static ConnectContext connectContext;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createDorisCluster(runningDir);

        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        // create database
        String createDbStmtStr = "create database test;";
        String createTablleStr1 = "create table test.tbl1(k1 int, k2 bigint) duplicate key(k1) "
                + "distributed by hash(k2) buckets 1 properties('replication_num' = '1');";
        String createTablleStr2 = "create table test.tbl2(k1 int, k2 bigint)" + "duplicate key(k1) "
                + "distributed by hash(k2) buckets 1 " + "properties('replication_num' = '1');";
        createDb(createDbStmtStr);
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

    private static void createTable(String sql) throws Exception {
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        Catalog.getCurrentCatalog().createTable(createTableStmt);
    }

    private static void dropTable(String sql) throws Exception {
        DropTableStmt dropTableStmt = (DropTableStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        Catalog.getCurrentCatalog().dropTable(dropTableStmt);
    }

    @Test
    public void testNormalDropTable() throws Exception {
        Database db = Catalog.getCurrentCatalog().getDb("default_cluster:test");
        OlapTable table = (OlapTable) db.getTable("tbl1");
        Partition partition = table.getAllPartitions().iterator().next();
        long tabletId = partition.getBaseIndex().getTablets().get(0).getId();
        String dropTableSql = "drop table test.tbl1";
        dropTable(dropTableSql);
        List<Replica> replicaList = Catalog.getCurrentCatalog().getTabletInvertedIndex().getReplicasByTabletId(tabletId);
        Assert.assertEquals(1, replicaList.size());
        String recoverDbSql = "recover table test.tbl1";
        RecoverTableStmt recoverTableStmt = (RecoverTableStmt) UtFrameUtils.parseAndAnalyzeStmt(recoverDbSql, connectContext);
        Catalog.getCurrentCatalog().recoverTable(recoverTableStmt);
        table = (OlapTable) db.getTable("tbl1");
        Assert.assertNotNull(table);
        Assert.assertEquals("tbl1", table.getName());
    }

    @Test
    public void testForceDropTable() throws Exception {
        Database db = Catalog.getCurrentCatalog().getDb("default_cluster:test");
        OlapTable table = (OlapTable) db.getTable("tbl2");
        Partition partition = table.getAllPartitions().iterator().next();
        long tabletId = partition.getBaseIndex().getTablets().get(0).getId();
        String dropTableSql = "drop table test.tbl2 force";
        dropTable(dropTableSql);
        List<Replica> replicaList = Catalog.getCurrentCatalog().getTabletInvertedIndex().getReplicasByTabletId(tabletId);
        Assert.assertTrue(replicaList.isEmpty());
        String recoverDbSql = "recover table test.tbl2";
        RecoverTableStmt recoverTableStmt = (RecoverTableStmt) UtFrameUtils.parseAndAnalyzeStmt(recoverDbSql, connectContext);
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "Unknown table 'tbl2'",
                () -> Catalog.getCurrentCatalog().recoverTable(recoverTableStmt));
    }
}
