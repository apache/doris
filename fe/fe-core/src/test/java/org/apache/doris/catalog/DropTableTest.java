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
        Env.getCurrentEnv().createDb(createDbStmt);
    }

    private static void createTable(String sql) throws Exception {
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        Env.getCurrentEnv().createTable(createTableStmt);
    }

    private static void dropTable(String sql) throws Exception {
        DropTableStmt dropTableStmt = (DropTableStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        Env.getCurrentEnv().dropTable(dropTableStmt);
    }

    @Test
    public void testNormalDropTable() throws Exception {
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException("default_cluster:test");
        OlapTable table = (OlapTable) db.getTableOrMetaException("tbl1");
        Partition partition = table.getAllPartitions().iterator().next();
        long tabletId = partition.getBaseIndex().getTablets().get(0).getId();
        String dropTableSql = "drop table test.tbl1";
        dropTable(dropTableSql);
        List<Replica> replicaList = Env.getCurrentEnv().getTabletInvertedIndex().getReplicasByTabletId(tabletId);
        Assert.assertEquals(1, replicaList.size());
        String recoverDbSql = "recover table test.tbl1";
        RecoverTableStmt recoverTableStmt = (RecoverTableStmt) UtFrameUtils.parseAndAnalyzeStmt(recoverDbSql, connectContext);
        Env.getCurrentEnv().recoverTable(recoverTableStmt);
        table = (OlapTable) db.getTableOrMetaException("tbl1");
        Assert.assertNotNull(table);
        Assert.assertEquals("tbl1", table.getName());
    }

    @Test
    public void testForceDropTable() throws Exception {
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException("default_cluster:test");
        OlapTable table = (OlapTable) db.getTableOrMetaException("tbl2");
        Partition partition = table.getAllPartitions().iterator().next();
        long tabletId = partition.getBaseIndex().getTablets().get(0).getId();
        String dropTableSql = "drop table test.tbl2 force";
        dropTable(dropTableSql);
        List<Replica> replicaList = Env.getCurrentEnv().getTabletInvertedIndex().getReplicasByTabletId(tabletId);
        Assert.assertTrue(replicaList.isEmpty());
        String recoverDbSql = "recover table test.tbl2";
        RecoverTableStmt recoverTableStmt = (RecoverTableStmt) UtFrameUtils.parseAndAnalyzeStmt(recoverDbSql, connectContext);
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "Unknown table 'tbl2' or table id '-1' in default_cluster:test",
                () -> Env.getCurrentEnv().recoverTable(recoverTableStmt));
    }

    @Test
    public void dropMultiTables() throws Exception {
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException("default_cluster:test");
        OlapTable table1 = (OlapTable) db.getTableOrMetaException("tbl1");
        OlapTable table2 = (OlapTable) db.getTableOrMetaException("tbl2");
        Partition partition1 = table1.getAllPartitions().iterator().next();
        Partition partition2 = table2.getAllPartitions().iterator().next();
        long tabletId1 = partition1.getBaseIndex().getTablets().get(0).getId();
        long tabletId2 = partition2.getBaseIndex().getTablets().get(0).getId();
        String dropTableSql = "drop table test.tbl1, test.tbl2";
        dropTable(dropTableSql);
        List<Replica> replicaList1 = Env.getCurrentEnv().getTabletInvertedIndex().getReplicasByTabletId(tabletId1);
        List<Replica> replicaList2 = Env.getCurrentEnv().getTabletInvertedIndex().getReplicasByTabletId(tabletId2);

        Assert.assertEquals(1, replicaList1.size());
        String recoverDbSql1 = "recover table test.tbl1";
        RecoverTableStmt recoverTableStmt1 = (RecoverTableStmt) UtFrameUtils.parseAndAnalyzeStmt(recoverDbSql1, connectContext);
        Env.getCurrentEnv().recoverTable(recoverTableStmt1);
        table1 = (OlapTable) db.getTableOrMetaException("tbl1");
        Assert.assertNotNull(table1);
        Assert.assertEquals("tbl1", table1.getName());

        Assert.assertEquals(1, replicaList2.size());
        String recoverDbSql2 = "recover table test.tbl2";
        RecoverTableStmt recoverTableStmt2 = (RecoverTableStmt) UtFrameUtils.parseAndAnalyzeStmt(recoverDbSql2, connectContext);
        Env.getCurrentEnv().recoverTable(recoverTableStmt2);
        table2 = (OlapTable) db.getTableOrMetaException("tbl2");
        Assert.assertNotNull(table2);
        Assert.assertEquals("tbl2", table2.getName());
    }
}
