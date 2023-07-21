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

package org.apache.doris.alter;

import org.apache.doris.analysis.AlterTableStmt;
import org.apache.doris.analysis.CreateDbStmt;
import org.apache.doris.analysis.CreateMaterializedViewStmt;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.analysis.ShowAlterStmt;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.common.FeConstants;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowExecutor;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.utframe.UtFrameUtils;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;
import java.util.UUID;

public class AlterJobV2Test {
    // use a unique dir so that it won't be conflict with other unit test which
    // may also start a Mocked Frontend
    private static String runningDir = "fe/mocked/AlterJobV2Test/" + UUID.randomUUID().toString() + "/";

    private static ConnectContext connectContext;

    @BeforeClass
    public static void beforeClass() throws Exception {
        FeConstants.default_scheduler_interval_millisecond = 1000;
        FeConstants.runningUnitTest = true;

        UtFrameUtils.createDorisCluster(runningDir);

        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        // create database
        String createDbStmtStr = "create database test;";
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt(createDbStmtStr, connectContext);
        Env.getCurrentEnv().createDb(createDbStmt);

        createTable("CREATE TABLE test.schema_change_test(k1 int, k2 int, k3 int) distributed by hash(k1) buckets 3 properties('replication_num' = '1');");
    }

    @AfterClass
    public static void tearDown() {
        UtFrameUtils.cleanDorisFeDir(runningDir);
    }

    private static void createTable(String sql) throws Exception {
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        Env.getCurrentEnv().createTable(createTableStmt);
    }

    private static void alterTable(String sql) throws Exception {
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        Env.getCurrentEnv().getAlterInstance().processAlterTable(alterTableStmt);
    }

    private static void createMaterializedView(String sql) throws Exception {
        CreateMaterializedViewStmt stmt = (CreateMaterializedViewStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        Env.getCurrentEnv().getAlterInstance().processCreateMaterializedView(stmt);
    }

    @Test
    public void testSchemaChange() throws Exception {
        // 1. process a schema change job
        String alterStmtStr = "alter table test.schema_change_test add column k4 int default '1'";
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseAndAnalyzeStmt(alterStmtStr, connectContext);
        Env.getCurrentEnv().getAlterInstance().processAlterTable(alterTableStmt);
        // 2. check alter job
        Map<Long, AlterJobV2> alterJobs = Env.getCurrentEnv().getSchemaChangeHandler().getAlterJobsV2();
        Assert.assertEquals(1, alterJobs.size());
        waitAlterJobDone(alterJobs);
        // 3. check show alter table column
        String showAlterStmtStr = "show alter table column from test;";
        ShowAlterStmt showAlterStmt = (ShowAlterStmt) UtFrameUtils.parseAndAnalyzeStmt(showAlterStmtStr, connectContext);
        ShowExecutor showExecutor = new ShowExecutor(connectContext, showAlterStmt);
        ShowResultSet showResultSet = showExecutor.execute();
        System.out.println(showResultSet.getMetaData());
        System.out.println(showResultSet.getResultRows());
    }

    private void waitAlterJobDone(Map<Long, AlterJobV2> alterJobs) throws Exception {
        for (AlterJobV2 alterJobV2 : alterJobs.values()) {
            while (!alterJobV2.getJobState().isFinalState()) {
                System.out.println("alter job " + alterJobV2.getDbId() + " is running. state: " + alterJobV2.getJobState());
                Thread.sleep(1000);
            }
            System.out.println("alter job " + alterJobV2.getDbId() + " is done. state: " + alterJobV2.getJobState());
            Assert.assertEquals(AlterJobV2.JobState.FINISHED, alterJobV2.getJobState());

            Database db =
                    Env.getCurrentInternalCatalog().getDbOrMetaException(alterJobV2.getDbId());
            OlapTable tbl = (OlapTable) db.getTableOrMetaException(alterJobV2.getTableId(), Table.TableType.OLAP);
            while (tbl.getState() != OlapTable.OlapTableState.NORMAL) {
                Thread.sleep(1000);
            }
        }
    }

    @Test
    public void testRollup() throws Exception {
        // 1. process a rollup job
        String alterStmtStr = "alter table test.schema_change_test add rollup test_rollup(k2, k1);";
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseAndAnalyzeStmt(alterStmtStr, connectContext);
        Env.getCurrentEnv().getAlterInstance().processAlterTable(alterTableStmt);
        // 2. check alter job
        Map<Long, AlterJobV2> alterJobs = Env.getCurrentEnv().getMaterializedViewHandler().getAlterJobsV2();
        waitAlterJobDone(alterJobs);
        // 3. check show alter table column
        String showAlterStmtStr = "show alter table rollup from test;";
        ShowAlterStmt showAlterStmt = (ShowAlterStmt) UtFrameUtils.parseAndAnalyzeStmt(showAlterStmtStr, connectContext);
        ShowExecutor showExecutor = new ShowExecutor(connectContext, showAlterStmt);
        ShowResultSet showResultSet = showExecutor.execute();
        System.out.println(showResultSet.getMetaData());
        System.out.println(showResultSet.getResultRows());
    }

    @Test
    public void testDupTableSchemaChange() throws Exception {

        createTable("CREATE TABLE test.dup_table (\n"
                + "  k1 bigint(20) NULL ,\n"
                + "  k2 bigint(20) NULL ,\n"
                + "  k3 bigint(20) NULL,\n"
                + "  v1 bigint(20) NULL ,\n"
                + "  v2 varchar(1) NULL,\n"
                + "  v3 varchar(1) NULL \n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(k1, k2, k3)\n"
                + "PARTITION BY RANGE(k1, v1)\n"
                + "(PARTITION p1 VALUES LESS THAN (\"10\", \"10\"))\n"
                + "DISTRIBUTED BY HASH(v1,k2) BUCKETS 10\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\"\n"
                + ");");


        alterTable("alter table test.dup_table add rollup r1(v1,v2,k2,k1);");
        Map<Long, AlterJobV2> alterJobs = Env.getCurrentEnv().getMaterializedViewHandler().getAlterJobsV2();
        waitAlterJobDone(alterJobs);
        ExceptionChecker.expectThrowsNoException(() -> alterTable("alter table test.dup_table modify column v2 varchar(2);"));
    }

    @Test
    public void testCreateMVForListPartitionTable() throws Exception {
        createTable("CREATE TABLE test.list_tbl (\n"
                + "city VARCHAR(20) NOT NULL,\n"
                + "user_id BIGINT NOT NULL,\n"
                + "date DATE NOT NULL,\n"
                + "age SMALLINT NOT NULL,\n"
                + "sex TINYINT NOT NULL,\n"
                + "cost BIGINT NOT NULL DEFAULT \"0\"\n"
                + ") DUPLICATE KEY(city) PARTITION BY LIST(city) (\n"
                + "PARTITION p_bj\n"
                + "VALUES IN (\"beijing\"),\n"
                + "PARTITION p_gz\n"
                + "VALUES IN (\"guangzhou\"),\n"
                + "PARTITION p_sz\n"
                + "VALUES IN (\"shenzhen\")\n"
                + ") DISTRIBUTED BY HASH(date) BUCKETS 1 PROPERTIES(\"replication_num\" = \"1\");");

        createMaterializedView("create materialized view list_view as\n"
                + "select city,\n"
                + "user_id,\n"
                + "date,\n"
                + "sum(cost)\n"
                + "from\n"
                + "test.list_tbl\n"
                + "group by\n"
                + "city,\n"
                + "user_id,\n"
                + "date;");
        Map<Long, AlterJobV2> alterJobs = Env.getCurrentEnv().getMaterializedViewHandler().getAlterJobsV2();
        waitAlterJobDone(alterJobs);
    }

    @Test
    public void testDupTableWithoutKeysSchemaChange() throws Exception {

        createTable("CREATE TABLE test.dup_table_without_keys (\n"
                + "  k1 bigint(20) NULL ,\n"
                + "  k2 bigint(20) NULL ,\n"
                + "  k3 bigint(20) NULL,\n"
                + "  v1 bigint(20) NULL ,\n"
                + "  v2 varchar(1) NULL,\n"
                + "  v3 varchar(1) NULL \n"
                + ") ENGINE=OLAP\n"
                + "PARTITION BY RANGE(k1, v1)\n"
                + "(PARTITION p1 VALUES LESS THAN (\"10\", \"10\"))\n"
                + "DISTRIBUTED BY HASH(v1,k2) BUCKETS 10\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\",\n"
                + "\"enable_duplicate_without_keys_by_default\" = \"true\""
                + ");");

        ExceptionChecker.expectThrowsWithMsg(DdlException.class, "Duplicate table without keys do not support alter rollup!",
                                () -> alterTable("alter table test.dup_table_without_keys add rollup r1(v1,v2,k2,k1);"));
        ExceptionChecker.expectThrowsNoException(() -> alterTable("alter table test.dup_table_without_keys modify column v2 varchar(2);"));
        ExceptionChecker.expectThrowsNoException(() -> alterTable("alter table test.dup_table_without_keys add column v4 varchar(2);"));
        ExceptionChecker.expectThrowsWithMsg(DdlException.class, "Duplicate table without keys do not support add key column!",
                                () -> alterTable("alter table test.dup_table_without_keys add column new_col INT KEY DEFAULT \"0\" AFTER k3;"));

        createMaterializedView("create materialized view k1_k33 as select k2, k1 from test.dup_table_without_keys;");
        Map<Long, AlterJobV2> alterJobs = Env.getCurrentEnv().getMaterializedViewHandler().getAlterJobsV2();
        waitAlterJobDone(alterJobs);

        createMaterializedView("create materialized view k1_k24 as select k2, k1 from test.dup_table_without_keys order by k2,k1;");
        alterJobs = Env.getCurrentEnv().getMaterializedViewHandler().getAlterJobsV2();
        waitAlterJobDone(alterJobs);
    }
}
