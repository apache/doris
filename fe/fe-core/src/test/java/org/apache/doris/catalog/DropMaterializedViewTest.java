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

import org.apache.doris.alter.AlterJobV2;
import org.apache.doris.analysis.CreateDbStmt;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.analysis.DropTableStmt;
import org.apache.doris.analysis.TableName;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.util.UnitTestUtil;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.CreateMTMVCommand;
import org.apache.doris.nereids.trees.plans.commands.DropMTMVCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.utframe.UtFrameUtils;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.io.File;
import java.util.Map;
import java.util.UUID;

public class DropMaterializedViewTest {
    private static String runningDir = "fe/mocked/DropMaterializedViewTest/" + UUID.randomUUID() + "/";

    private static ConnectContext connectContext;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createDorisCluster(runningDir);

        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();

        String createDbStmtStr = String.format("CREATE DATABASE %s;", UnitTestUtil.DB_NAME);
        String createTableStr1 = String.format("CREATE TABLE %s.%s(k1 int, k2 bigint) DUPLICATE KEY(k1) DISTRIBUTED BY "
                + "HASH(k2) BUCKETS 1 PROPERTIES('replication_num' = '1');", UnitTestUtil.DB_NAME, UnitTestUtil.TABLE_NAME);
        String createMVStr1 = String.format("CREATE MATERIALIZED VIEW %s.%s BUILD IMMEDIATE REFRESH AUTO ON MANUAL "
                + "DISTRIBUTED BY RANDOM BUCKETS 1 PROPERTIES ('replication_num' = '1') AS SELECT k1, sum(k2) as k3 from %s.%s"
                + " GROUP BY k1;", UnitTestUtil.DB_NAME, UnitTestUtil.MV_NAME, UnitTestUtil.DB_NAME, UnitTestUtil.TABLE_NAME);
        createDb(createDbStmtStr);
        createTable(createTableStr1);
        createMvByNereids(createMVStr1);
    }

    @AfterClass
    public static void tearDown() {
        File file = new File(runningDir);
        file.delete();
    }

    private static void createMvByNereids(String sql) throws Exception {
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan parsed = nereidsParser.parseSingle(sql);
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, sql);
        if (parsed instanceof CreateMTMVCommand) {
            ((CreateMTMVCommand) parsed).run(connectContext, stmtExecutor);
        }
        checkAlterJob();
        // waiting table state to normal
        Thread.sleep(1000);
    }

    private static void dropMvByNereids(String sql) throws Exception {
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan parsed = nereidsParser.parseSingle(sql);
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, sql);
        if (parsed instanceof DropMTMVCommand) {
            ((DropMTMVCommand) parsed).run(connectContext, stmtExecutor);
        }
        checkAlterJob();
        // waiting table state to normal
        Thread.sleep(1000);
    }

    private static void createDb(String sql) throws Exception {
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        Env.getCurrentEnv().createDb(createDbStmt);
    }

    private static void createTable(String sql) throws Exception {
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        Env.getCurrentEnv().createTable(createTableStmt);
    }

    private static void dropTable(String db, String tbl, boolean isMaterializedView) throws Exception {
        DropTableStmt dropTableStmt = new DropTableStmt(false,
                new TableName(InternalCatalog.INTERNAL_CATALOG_NAME, db, tbl), false, false);
        if (isMaterializedView) {
            dropTableStmt.setMaterializedView(true);
        }
        Env.getCurrentEnv().dropTable(dropTableStmt);
    }

    private static void checkAlterJob() throws InterruptedException {
        // check alter job
        Map<Long, AlterJobV2> alterJobs = Env.getCurrentEnv().getMaterializedViewHandler().getAlterJobsV2();
        for (AlterJobV2 alterJobV2 : alterJobs.values()) {
            while (!alterJobV2.getJobState().isFinalState()) {
                System.out.println("alter job " + alterJobV2.getDbId()
                        + " is running. state: " + alterJobV2.getJobState());
                Thread.sleep(100);
            }
            System.out.println("alter job " + alterJobV2.getDbId() + " is done. state: " + alterJobV2.getJobState());
            Assertions.assertEquals(AlterJobV2.JobState.FINISHED, alterJobV2.getJobState());

            try {
                // Add table state check in case of below Exception:
                // there is still a short gap between "job finish" and "table become normal",
                // so if user send next alter job right after the "job finish",
                // it may encounter "table's state not NORMAL" error.
                Database db = Env.getCurrentInternalCatalog().getDbOrMetaException(alterJobV2.getDbId());
                OlapTable tbl = (OlapTable) db.getTableOrMetaException(alterJobV2.getTableId(), Table.TableType.OLAP);
                while (tbl.getState() != OlapTable.OlapTableState.NORMAL) {
                    Thread.sleep(1000);
                }
            } catch (MetaNotFoundException e) {
                // Sometimes table could be dropped by tests, but the corresponding alter job is not deleted yet.
                // Ignore this error.
                System.out.println(e.getMessage());
            }
        }
    }

    @Test
    public void testDropMv() throws Exception {
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                String.format("'%s.%s' is not TABLE. Use 'DROP MATERIALIZED VIEW %s.%s'",
                    UnitTestUtil.DB_NAME, UnitTestUtil.MV_NAME, UnitTestUtil.DB_NAME, UnitTestUtil.MV_NAME),
                () -> dropTable(UnitTestUtil.DB_NAME, UnitTestUtil.MV_NAME, false));
        ExceptionChecker.expectThrowsNoException(() -> dropMvByNereids(String.format("DROP MATERIALIZED VIEW %s.%s",
                UnitTestUtil.DB_NAME, UnitTestUtil.MV_NAME)));
    }
}
