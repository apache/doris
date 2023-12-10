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

package org.apache.doris.utframe;

import org.apache.doris.analysis.CreateDbStmt;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.Planner;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.utframe.MockedFrontend.EnvVarNotSetException;
import org.apache.doris.utframe.MockedFrontend.FeStartException;
import org.apache.doris.utframe.MockedFrontend.NotInitException;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

/*
 * This demo is mainly used to confirm that
 * repeatedly starting FE and BE in 2 UnitTest will not cause conflict
 */
public class AnotherDemoTest {

    private static int fe_http_port;
    private static int fe_rpc_port;
    private static int fe_query_port;
    private static int fe_arrow_flight_sql_port;
    private static int fe_edit_log_port;

    private static int be_heartbeat_port;
    private static int be_thrift_port;
    private static int be_brpc_port;
    private static int be_http_port;
    private static int be_arrow_flight_sql_port;

    // use a unique dir so that it won't be conflict with other unit test which
    // may also start a Mocked Frontend
    private static String runningDirBase = "fe";
    private static String runningDir = runningDirBase + "/mocked/AnotherDemoTest/" + UUID.randomUUID().toString() + "/";

    @BeforeClass
    public static void beforeClass() throws EnvVarNotSetException, IOException,
            FeStartException, NotInitException, DdlException, InterruptedException {
        FeConstants.default_scheduler_interval_millisecond = 10;
        UtFrameUtils.createDorisCluster(runningDir, 1);
    }

    @AfterClass
    public static void tearDown() {
        UtFrameUtils.cleanDorisFeDir(runningDir);
    }

    // generate all port from valid ports
    private static void getPorts() {
        fe_http_port = UtFrameUtils.findValidPort();
        fe_rpc_port = UtFrameUtils.findValidPort();
        fe_query_port = UtFrameUtils.findValidPort();
        fe_arrow_flight_sql_port = UtFrameUtils.findValidPort();
        fe_edit_log_port = UtFrameUtils.findValidPort();

        be_heartbeat_port = UtFrameUtils.findValidPort();
        be_thrift_port = UtFrameUtils.findValidPort();
        be_brpc_port = UtFrameUtils.findValidPort();
        be_http_port = UtFrameUtils.findValidPort();
        be_arrow_flight_sql_port = UtFrameUtils.findValidPort();
    }

    @Test
    public void testCreateDbAndTable() throws Exception {
        // 1. create connect context
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        // 2. create database db1
        String createDbStmtStr = "create database db1;";
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt(createDbStmtStr, ctx);
        Env.getCurrentEnv().createDb(createDbStmt);
        System.out.println(Env.getCurrentInternalCatalog().getDbNames());
        // 3. create table tbl1
        String createTblStmtStr = "create table db1.tbl1(k1 int) distributed by hash(k1) buckets 3 properties('replication_num' = '1');";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(createTblStmtStr, ctx);
        Env.getCurrentEnv().createTable(createTableStmt);
        // 4. get and test the created db and table
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException("default_cluster:db1");
        OlapTable tbl = (OlapTable) db.getTableOrMetaException("tbl1", Table.TableType.OLAP);
        tbl.readLock();
        try {
            Assert.assertNotNull(tbl);
            System.out.println(tbl.getName());
            Assert.assertEquals("Doris", tbl.getEngine());
            Assert.assertEquals(1, tbl.getBaseSchema().size());
        } finally {
            tbl.readUnlock();
        }
        // 5. query
        // TODO: we can not process real query for now. So it has to be a explain query
        String queryStr = "explain select /*+ SET_VAR(enable_nereids_planner=false) */ * from db1.tbl1";
        StmtExecutor stmtExecutor = new StmtExecutor(ctx, queryStr);
        stmtExecutor.execute();
        Planner planner = stmtExecutor.planner();
        List<PlanFragment> fragments = planner.getFragments();
        Assert.assertEquals(1, fragments.size());
        PlanFragment fragment = fragments.get(0);
        Assert.assertTrue(fragment.getPlanRoot() instanceof OlapScanNode);
        Assert.assertEquals(0, fragment.getChildren().size());
    }
}
