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
import org.apache.doris.analysis.AlterTableStmt;
import org.apache.doris.analysis.CreateDbStmt;
import org.apache.doris.analysis.CreateResourceStmt;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.UtFrameUtils;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.Map;
import java.util.UUID;

public class EnvOperationTest {
    // use a unique dir so that it won't be conflict with other unit test which
    // may also start a Mocked Frontend
    private static String runningDir = "fe/mocked/CatalogOperationTest/" + UUID.randomUUID().toString() + "/";

    private static ConnectContext connectContext;

    @BeforeClass
    public static void beforeClass() throws Exception {
        FeConstants.default_scheduler_interval_millisecond = 1000;
        UtFrameUtils.createDorisCluster(runningDir);

        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        // create database
        String createDbStmtStr = "create database test;";
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt(createDbStmtStr, connectContext);
        Env.getCurrentEnv().createDb(createDbStmt);
        Config.enable_odbc_mysql_broker_table = true;

        createTable("create table test.renameTest\n"
                + "(k1 int,k2 int)\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties(\"replication_num\" = \"1\");");

        createResource("CREATE EXTERNAL RESOURCE \"mysql_resource\"\n"
                + "PROPERTIES\n"
                + "(\n"
                + "  \"type\" = \"odbc_catalog\",\n"
                + "  \"user\" = \"mysql_user\",\n"
                + "  \"password\" = \"mysql_passwd\",\n"
                + "  \"host\" = \"127.0.0.1\",\n"
                + "   \"port\" = \"8239\"\n"
                + ");");

        createTable("CREATE EXTERNAL TABLE test.mysqlRenameTest\n"
                + "(\n"
                + "k1 DATE,\n"
                + "k2 INT,\n"
                + "k3 SMALLINT,\n"
                + "k4 VARCHAR(2048),\n"
                + "k5 DATETIME\n"
                + ")\n"
                + "ENGINE=mysql\n"
                + "PROPERTIES\n"
                + "(\n"
                + "\"odbc_catalog_resource\" = \"mysql_resource\",\n"
                + "\"database\" = \"mysql_db_test\",\n"
                + "\"table\" = \"mysql_table_test\"\n"
                + ");");
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

    private static void createResource(String sql) throws Exception {
        CreateResourceStmt createResourceStmt = (CreateResourceStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        Env.getCurrentEnv().getResourceMgr().createResource(createResourceStmt);
    }

    @Test
    public void testRenameTable() throws Exception {
        // rename olap table
        String renameTblStmt = "alter table test.renameTest rename newNewTest";
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseAndAnalyzeStmt(renameTblStmt, connectContext);

        Database db = Env.getCurrentInternalCatalog().getDbNullable("test");
        Assert.assertNotNull(db);
        Assert.assertNotNull(db.getTableNullable("renameTest"));

        Env.getCurrentEnv().getAlterInstance().processAlterTable(alterTableStmt);
        Assert.assertNull(db.getTableNullable("renameTest"));
        Assert.assertNotNull(db.getTableNullable("newNewTest"));

        // add a rollup and test rename to a rollup name(expect throw exception)
        String alterStmtStr = "alter table test.newNewTest add rollup r1(k2,k1)";
        alterTableStmt = (AlterTableStmt) UtFrameUtils.parseAndAnalyzeStmt(alterStmtStr, connectContext);
        Env.getCurrentEnv().getAlterInstance().processAlterTable(alterTableStmt);
        Map<Long, AlterJobV2> alterJobs = Env.getCurrentEnv().getMaterializedViewHandler().getAlterJobsV2();
        Assert.assertEquals(1, alterJobs.size());
        for (AlterJobV2 alterJobV2 : alterJobs.values()) {
            while (!alterJobV2.getJobState().isFinalState()) {
                System.out.println("alter job " + alterJobV2.getJobId() + " is running. state: " + alterJobV2.getJobState());
                Thread.sleep(1000);
            }
            System.out.println("alter job " + alterJobV2.getJobId() + " is done. state: " + alterJobV2.getJobState());
            Assert.assertEquals(AlterJobV2.JobState.FINISHED, alterJobV2.getJobState());
        }

        Thread.sleep(1000);
        renameTblStmt = "alter table test.newNewTest rename r1";
        alterTableStmt = (AlterTableStmt) UtFrameUtils.parseAndAnalyzeStmt(renameTblStmt, connectContext);
        try {
            Env.getCurrentEnv().getAlterInstance().processAlterTable(alterTableStmt);
            Assert.fail();
        } catch (DdlException e) {
            Assert.assertTrue(e.getMessage().contains("New name conflicts with rollup index name: r1"));
        }

        renameTblStmt = "alter table test.newNewTest rename goodName";
        alterTableStmt = (AlterTableStmt) UtFrameUtils.parseAndAnalyzeStmt(renameTblStmt, connectContext);
        Env.getCurrentEnv().getAlterInstance().processAlterTable(alterTableStmt);
        Assert.assertNull(db.getTableNullable("newNewTest"));
        Assert.assertNotNull(db.getTableNullable("goodName"));

        // rename external table
        renameTblStmt = "alter table test.mysqlRenameTest rename newMysqlRenameTest";
        alterTableStmt = (AlterTableStmt) UtFrameUtils.parseAndAnalyzeStmt(renameTblStmt, connectContext);
        Assert.assertNotNull(db.getTableNullable("mysqlRenameTest"));

        Env.getCurrentEnv().getAlterInstance().processAlterTable(alterTableStmt);
        Assert.assertNull(db.getTableNullable("mysqlRenameTest"));
        Assert.assertNotNull(db.getTableNullable("newMysqlRenameTest"));
    }
}
