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
import org.apache.doris.analysis.ShowStmt;
import org.apache.doris.analysis.ShowTabletStmt;
import org.apache.doris.analysis.TruncateTableStmt;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowExecutor;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.utframe.UtFrameUtils;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.List;
import java.util.UUID;

public class TruncateTableTest {
    private static String runningDir = "fe/mocked/TruncateTableTest/" + UUID.randomUUID().toString() + "/";

    private static ConnectContext connectContext;

    @BeforeClass
    public static void setup() throws Exception {
        UtFrameUtils.createDorisCluster(runningDir);
        connectContext = UtFrameUtils.createDefaultCtx();
        // create database
        String createDbStmtStr = "create database test;";
        String createTableStr = "create table test.tbl(d1 date, k1 int, k2 bigint)" +
                                        "duplicate key(d1, k1) " +
                                        "PARTITION BY RANGE(d1)" +
                                        "(PARTITION p20210901 VALUES [('2021-09-01'), ('2021-09-02')))" +
                                        "distributed by hash(k1) buckets 2 " +
                                        "properties('replication_num' = '1');";
        createDb(createDbStmtStr);
        createTable(createTableStr);
    }

    @AfterClass
    public static void tearDown() {
        File file = new File(runningDir);
        file.delete();
    }

    @Test
    public void testTruncateTable() throws Exception {
        String stmtStr = "ALTER TABLE test.tbl ADD PARTITION p20210902 VALUES [('2021-09-02'), ('2021-09-03')) DISTRIBUTED BY HASH(`k1`) BUCKETS 3;";
        alterTable(stmtStr);
        stmtStr = "ALTER TABLE test.tbl ADD PARTITION p20210903 VALUES [('2021-09-03'), ('2021-09-04')) DISTRIBUTED BY HASH(`k1`) BUCKETS 4;";
        alterTable(stmtStr);
        stmtStr = "ALTER TABLE test.tbl ADD PARTITION p20210904 VALUES [('2021-09-04'), ('2021-09-05')) DISTRIBUTED BY HASH(`k1`) BUCKETS 5;";
        alterTable(stmtStr);
        checkShowTabletResultNum("test.tbl", "p20210901", 2);
        checkShowTabletResultNum("test.tbl", "p20210902", 3);
        checkShowTabletResultNum("test.tbl", "p20210903", 4);
        checkShowTabletResultNum("test.tbl", "p20210904", 5);

        String truncateStr = "truncate table test.tbl;";
        TruncateTableStmt truncateTableStmt = (TruncateTableStmt)UtFrameUtils.parseAndAnalyzeStmt(truncateStr, connectContext);
        Catalog.getCurrentCatalog().truncateTable(truncateTableStmt);
        checkShowTabletResultNum("test.tbl", "p20210901", 2);
        checkShowTabletResultNum("test.tbl", "p20210902", 3);
        checkShowTabletResultNum("test.tbl", "p20210903", 4);
        checkShowTabletResultNum("test.tbl", "p20210904", 5);

        truncateStr = "truncate table test.tbl partition(p20210901, p20210902, p20210903, p20210904);";
        truncateTableStmt = (TruncateTableStmt)UtFrameUtils.parseAndAnalyzeStmt(truncateStr, connectContext);
        Catalog.getCurrentCatalog().truncateTable(truncateTableStmt);
        checkShowTabletResultNum("test.tbl", "p20210901", 2);
        checkShowTabletResultNum("test.tbl", "p20210902", 3);
        checkShowTabletResultNum("test.tbl", "p20210903", 4);
        checkShowTabletResultNum("test.tbl", "p20210904", 5);

        truncateStr = "truncate table test.tbl partition (p20210901);";
        truncateTableStmt = (TruncateTableStmt)UtFrameUtils.parseAndAnalyzeStmt(truncateStr, connectContext);
        Catalog.getCurrentCatalog().truncateTable(truncateTableStmt);
        checkShowTabletResultNum("test.tbl", "p20210901", 2);

        truncateStr = "truncate table test.tbl partition (p20210902);";
        truncateTableStmt = (TruncateTableStmt)UtFrameUtils.parseAndAnalyzeStmt(truncateStr, connectContext);
        Catalog.getCurrentCatalog().truncateTable(truncateTableStmt);
        checkShowTabletResultNum("test.tbl", "p20210902", 3);

        truncateStr = "truncate table test.tbl partition (p20210903);";
        truncateTableStmt = (TruncateTableStmt)UtFrameUtils.parseAndAnalyzeStmt(truncateStr, connectContext);
        Catalog.getCurrentCatalog().truncateTable(truncateTableStmt);
        checkShowTabletResultNum("test.tbl", "p20210903", 4);

        truncateStr = "truncate table test.tbl partition (p20210904);";
        truncateTableStmt = (TruncateTableStmt)UtFrameUtils.parseAndAnalyzeStmt(truncateStr, connectContext);
        Catalog.getCurrentCatalog().truncateTable(truncateTableStmt);
        checkShowTabletResultNum("test.tbl", "p20210904", 5);
    }

    private static void createDb(String sql) throws Exception {
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        Catalog.getCurrentCatalog().createDb(createDbStmt);
    }

    private static void createTable(String sql) throws Exception {
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        Catalog.getCurrentCatalog().createTable(createTableStmt);
    }

    private List<List<String>> checkShowTabletResultNum(String tbl, String partition, int expected) throws Exception {
        String showStr = "show tablets from " + tbl + " partition(" + partition + ")";
        ShowTabletStmt showStmt = (ShowTabletStmt) UtFrameUtils.parseAndAnalyzeStmt(showStr, connectContext);
        ShowExecutor executor = new ShowExecutor(connectContext, (ShowStmt) showStmt);
        ShowResultSet showResultSet = executor.execute();
        List<List<String>> rows = showResultSet.getResultRows();
        Assert.assertEquals(expected, rows.size());
        return rows;
    }

    private void alterTable(String sql) throws Exception {
        try {
            AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
            Catalog.getCurrentCatalog().getAlterInstance().processAlterTable(alterTableStmt);
        } catch (Exception e) {
            throw e;
        }
    }
}
