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

import org.apache.doris.alter.AlterJobV2;
import org.apache.doris.alter.AlterJobV2.JobState;
import org.apache.doris.analysis.AlterTableStmt;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.FeConstants;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.Planner;
import org.apache.doris.qe.StmtExecutor;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

/*
 * This demo shows how to run unit test with mocked FE and BE.
 * It will
 *  1. start a mocked FE and a mocked BE.
 *  2. Create a database and a tbl.
 *  3. Make a schema change to tbl.
 *  4. send a query and get query plan
 */
public class DemoTest extends TestWithFeService {

    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.default_scheduler_interval_millisecond = 10;
    }

    @Test
    public void testCreateDbAndTable() throws Exception {
        // 1. create connect context
        connectContext = createDefaultCtx();

        // 2. create database db1
        createDatabase("db1");
        System.out.println(Env.getCurrentInternalCatalog().getDbNames());

        // 3. create table tbl1
        createTable("create table db1.tbl1(k1 int) distributed by hash(k1) buckets 3 properties('replication_num' = '1');");

        // 4. get and test the created db and table
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException("db1");
        OlapTable tbl = (OlapTable) db.getTableOrMetaException("tbl1", Table.TableType.OLAP);
        tbl.readLock();
        try {
            Assertions.assertNotNull(tbl);
            System.out.println(tbl.getName());
            Assertions.assertEquals("Doris", tbl.getEngine());
            Assertions.assertEquals(1, tbl.getBaseSchema().size());
        } finally {
            tbl.readUnlock();
        }

        // 5. process a schema change job
        String alterStmtStr = "alter table db1.tbl1 add column k2 int default '1'";
        AlterTableStmt alterTableStmt = (AlterTableStmt) parseAndAnalyzeStmt(alterStmtStr);
        Env.getCurrentEnv().getAlterInstance().processAlterTable(alterTableStmt);

        // 6. check alter job
        Map<Long, AlterJobV2> alterJobs = Env.getCurrentEnv().getSchemaChangeHandler().getAlterJobsV2();
        Assertions.assertEquals(1, alterJobs.size());
        for (AlterJobV2 alterJobV2 : alterJobs.values()) {
            while (!alterJobV2.getJobState().isFinalState()) {
                System.out.println("alter job " + alterJobV2.getJobId() + " is running. state: " + alterJobV2.getJobState());
                Thread.sleep(1000);
            }
            System.out.println("alter job " + alterJobV2.getJobId() + " is done. state: " + alterJobV2.getJobState());
            Assertions.assertEquals(JobState.FINISHED, alterJobV2.getJobState());
        }

        OlapTable tbl1 = (OlapTable) db.getTableOrMetaException("tbl1", Table.TableType.OLAP);
        tbl1.readLock();
        try {
            Assertions.assertEquals(2, tbl1.getBaseSchema().size());
            String baseIndexName = tbl1.getIndexNameById(tbl.getBaseIndexId());
            Assertions.assertEquals(baseIndexName, tbl1.getName());
            MaterializedIndexMeta indexMeta = tbl1.getIndexMetaByIndexId(tbl1.getBaseIndexId());
            Assertions.assertNotNull(indexMeta);
        } finally {
            tbl1.readUnlock();
        }

        // 7. query
        // TODO: we can not process real query for now. So it has to be a explain query
        String queryStr = "explain select /*+ SET_VAR(enable_nereids_planner=false) */ * from db1.tbl1";
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, queryStr);
        stmtExecutor.execute();
        Planner planner = stmtExecutor.planner();
        List<PlanFragment> fragments = planner.getFragments();
        Assertions.assertEquals(1, fragments.size());
        PlanFragment fragment = fragments.get(0);
        Assertions.assertTrue(fragment.getPlanRoot() instanceof OlapScanNode);
        Assertions.assertEquals(0, fragment.getChildren().size());
    }
}
