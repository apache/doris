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
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.common.util.DebugPointUtil;
import org.apache.doris.common.util.DebugPointUtil.DebugPoint;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowExecutor;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.utframe.UtFrameUtils;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class TruncateTableTest {
    private static String runningDir = "fe/mocked/TruncateTableTest/" + UUID.randomUUID().toString() + "/";

    private static ConnectContext connectContext;

    @BeforeClass
    public static void setup() throws Exception {
        Config.disable_balance = true;
        Config.enable_debug_points = true;
        UtFrameUtils.createDorisCluster(runningDir);
        connectContext = UtFrameUtils.createDefaultCtx();
        // create database
        String createDbStmtStr = "create database test;";
        String createTableStr = "create table test.tbl(d1 date, k1 int, k2 bigint)"
                + "duplicate key(d1, k1) "
                + "PARTITION BY RANGE(d1)"
                + "(PARTITION p20210901 VALUES [('2021-09-01'), ('2021-09-02')))"
                + "distributed by hash(k1) buckets 2 "
                + "properties('replication_num' = '1');";
        createDb(createDbStmtStr);
        createTable(createTableStr);

        String createTable2 = "CREATE TABLE test.case_sensitive_table (\n"
                + "  `date_id` date NULL COMMENT \"\",\n"
                + "  `column2` tinyint(4) NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`date_id`, `column2`)\n"
                + "COMMENT \"OLAP\"\n"
                + "PARTITION BY RANGE(`date_id`)\n"
                + "(\n"
                + "PARTITION p20211006 VALUES [('2021-10-06'), ('2021-10-07')),\n"
                + "PARTITION P20211007 VALUES [('2021-10-07'), ('2021-10-08')),\n"
                + "PARTITION P20211008 VALUES [('2021-10-08'), ('2021-10-09')))\n"
                + "DISTRIBUTED BY HASH(`column2`) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "\"replication_allocation\" = \"tag.location.default: 1\"\n"
                + ");";

        createTable(createTable2);
    }

    @AfterClass
    public static void tearDown() {
        File file = new File(runningDir);
        file.delete();
    }

    @Test
    public void testTruncateWithCaseInsensitivePartitionName() throws Exception {
        //now in order to support auto create partition, need set partition name is case sensitive
        Database db = Env.getCurrentInternalCatalog().getDbNullable("test");
        OlapTable tbl = db.getOlapTableOrDdlException("case_sensitive_table");
        long p20211006Id = tbl.getPartition("p20211006").getId();
        long p20211007Id = tbl.getPartition("P20211007").getId();
        long p20211008Id = tbl.getPartition("P20211008").getId();
        // truncate P20211008(real name is P20211008)
        Partition p20211008 = tbl.getPartition("P20211008");
        p20211008.updateVisibleVersion(2L);
        p20211008.setNextVersion(p20211008.getVisibleVersion() + 1);
        String truncateStr = "TRUNCATE TABLE test.case_sensitive_table PARTITION P20211008; \n";
        TruncateTableStmt truncateTableStmt
                = (TruncateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(truncateStr, connectContext);
        Env.getCurrentEnv().truncateTable(truncateTableStmt);
        Assert.assertNotEquals(p20211008Id, tbl.getPartition("P20211008").getId());
        // 2. truncate P20211007
        truncateStr = "TRUNCATE TABLE test.case_sensitive_table PARTITION P20211007; \n";
        truncateTableStmt = (TruncateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(truncateStr, connectContext);
        Env.getCurrentEnv().truncateTable(truncateTableStmt);
        Assert.assertEquals(3, tbl.getPartitionInfo().idToDataProperty.size());
        Assert.assertEquals(p20211006Id, tbl.getPartition("p20211006").getId());
        Assert.assertEquals(p20211007Id, tbl.getPartition("P20211007").getId());
        Assert.assertNotNull(tbl.getPartition("p20211006"));
        Assert.assertNotNull(tbl.getPartition("P20211007"));
        Assert.assertNotNull(tbl.getPartition("P20211008"));
    }

    @Test
    public void testTruncateTable() throws Exception {
        String stmtStr = "ALTER TABLE test.tbl ADD PARTITION p20210902 VALUES [('2021-09-02'), ('2021-09-03'))"
                + " DISTRIBUTED BY HASH(`k1`) BUCKETS 3;";
        alterTable(stmtStr);
        stmtStr = "ALTER TABLE test.tbl ADD PARTITION p20210903 VALUES [('2021-09-03'), ('2021-09-04'))"
                + " DISTRIBUTED BY HASH(`k1`) BUCKETS 4;";
        alterTable(stmtStr);
        stmtStr = "ALTER TABLE test.tbl ADD PARTITION p20210904 VALUES [('2021-09-04'), ('2021-09-05'))"
                + " DISTRIBUTED BY HASH(`k1`) BUCKETS 5;";
        alterTable(stmtStr);
        checkShowTabletResultNum("test.tbl", "p20210901", 2);
        checkShowTabletResultNum("test.tbl", "p20210902", 3);
        checkShowTabletResultNum("test.tbl", "p20210903", 4);
        checkShowTabletResultNum("test.tbl", "p20210904", 5);

        String truncateStr = "truncate table test.tbl;";
        TruncateTableStmt truncateTableStmt
                = (TruncateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(truncateStr, connectContext);
        Env.getCurrentEnv().truncateTable(truncateTableStmt);
        checkShowTabletResultNum("test.tbl", "p20210901", 2);
        checkShowTabletResultNum("test.tbl", "p20210902", 3);
        checkShowTabletResultNum("test.tbl", "p20210903", 4);
        checkShowTabletResultNum("test.tbl", "p20210904", 5);

        truncateStr = "truncate table test.tbl partition(p20210901, p20210902, p20210903, p20210904);";
        truncateTableStmt = (TruncateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(truncateStr, connectContext);
        Env.getCurrentEnv().truncateTable(truncateTableStmt);
        checkShowTabletResultNum("test.tbl", "p20210901", 2);
        checkShowTabletResultNum("test.tbl", "p20210902", 3);
        checkShowTabletResultNum("test.tbl", "p20210903", 4);
        checkShowTabletResultNum("test.tbl", "p20210904", 5);

        truncateStr = "truncate table test.tbl partition (p20210901);";
        truncateTableStmt = (TruncateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(truncateStr, connectContext);
        Env.getCurrentEnv().truncateTable(truncateTableStmt);
        checkShowTabletResultNum("test.tbl", "p20210901", 2);

        truncateStr = "truncate table test.tbl partition (p20210902);";
        truncateTableStmt = (TruncateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(truncateStr, connectContext);
        Env.getCurrentEnv().truncateTable(truncateTableStmt);
        checkShowTabletResultNum("test.tbl", "p20210902", 3);

        truncateStr = "truncate table test.tbl partition (p20210903);";
        truncateTableStmt = (TruncateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(truncateStr, connectContext);
        Env.getCurrentEnv().truncateTable(truncateTableStmt);
        checkShowTabletResultNum("test.tbl", "p20210903", 4);

        truncateStr = "truncate table test.tbl partition (p20210904);";
        truncateTableStmt = (TruncateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(truncateStr, connectContext);
        Env.getCurrentEnv().truncateTable(truncateTableStmt);
        checkShowTabletResultNum("test.tbl", "p20210904", 5);
    }

    @Test
    public void testTruncateTableFailed() throws Exception {
        String createTableStr = "create table test.tbl2(d1 date, k1 int, k2 bigint)"
                + "duplicate key(d1, k1) "
                + "PARTITION BY RANGE(d1)"
                + "(PARTITION p20210901 VALUES [('2021-09-01'), ('2021-09-02')))"
                + "distributed by hash(k1) buckets 2 "
                + "properties('replication_num' = '1');";
        createTable(createTableStr);
        String partitionName = "p20210901";
        Database db = Env.getCurrentInternalCatalog().getDbNullable("test");
        OlapTable tbl2 = db.getOlapTableOrDdlException("tbl2");
        Assert.assertNotNull(tbl2);
        Partition p20210901 = tbl2.getPartition(partitionName);
        Assert.assertNotNull(p20210901);
        long partitionId = p20210901.getId();
        p20210901.setVisibleVersionAndTime(2L, System.currentTimeMillis());

        try {
            List<Long> backendIds = Env.getCurrentSystemInfo().getAllBackendIds();
            Map<Long, Set<Long>> oldBackendTablets = Maps.newHashMap();
            for (long backendId : backendIds) {
                Set<Long> tablets = Sets.newHashSet(Env.getCurrentInvertedIndex().getTabletIdsByBackendId(backendId));
                oldBackendTablets.put(backendId, tablets);
            }

            DebugPointUtil.addDebugPoint("InternalCatalog.truncateTable.metaChanged", new DebugPoint());

            String truncateStr = "truncate table test.tbl2 partition (" + partitionName + ");";
            TruncateTableStmt truncateTableStmt = (TruncateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(
                    truncateStr, connectContext);
            ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                    "Table[tbl2]'s meta has been changed. try again",
                    () -> Env.getCurrentEnv().truncateTable(truncateTableStmt));

            Assert.assertEquals(partitionId, tbl2.getPartition(partitionName).getId());
            for (long backendId : backendIds) {
                Set<Long> tablets = Sets.newHashSet(Env.getCurrentInvertedIndex().getTabletIdsByBackendId(backendId));
                Assert.assertEquals(oldBackendTablets.get(backendId), tablets);
            }
        } finally {
            DebugPointUtil.removeDebugPoint("InternalCatalog.truncateTable.metaChanged");
        }
    }

    private static void createDb(String sql) throws Exception {
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        Env.getCurrentEnv().createDb(createDbStmt);
    }

    private static void createTable(String sql) throws Exception {
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        Env.getCurrentEnv().createTable(createTableStmt);
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
            Env.getCurrentEnv().getAlterInstance().processAlterTable(alterTableStmt);
        } catch (Exception e) {
            throw e;
        }
    }
}
