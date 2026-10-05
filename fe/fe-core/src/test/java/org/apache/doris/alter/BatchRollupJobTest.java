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

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.OlapTable.OlapTableState;
import org.apache.doris.catalog.Partition;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.AlterTableCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class BatchRollupJobTest extends TestWithFeService {

    @Override
    protected void runBeforeEach() throws Exception {
        Map<Long, AlterJobV2> alterJobs = Env.getCurrentEnv().getMaterializedViewHandler().getAlterJobsV2();
        alterJobs.clear();

        // create database db1
        createDatabaseWithSql("create database if not exists db1;");
        System.out.println(Env.getCurrentInternalCatalog().getDbNames());
    }

    @Test
    public void testBatchRollup() throws Exception {
        // create table tbl1
        createTable("create table db1.tbl1(k1 int, k2 int, k3 int) distributed by hash(k1) buckets 3 "
                + "properties('replication_num' = '1');");

        // batch add 3 rollups
        String stmtStr = "alter table db1.tbl1 add rollup r1(k3) duplicate key(k3), r2(k2, k1) duplicate key(k2), r3(k2) duplicate key(k2);";
        alterTable(stmtStr, connectContext);

        Map<Long, AlterJobV2> alterJobs = Env.getCurrentEnv().getMaterializedViewHandler().getAlterJobsV2();
        Assertions.assertEquals(3, alterJobs.size());

        Database db = Env.getCurrentInternalCatalog().getDbNullable("db1");
        Assertions.assertNotNull(db);
        OlapTable tbl = (OlapTable) db.getTableNullable("tbl1");
        Assertions.assertNotNull(tbl);

        int finishedNum = 0;
        for (AlterJobV2 alterJobV2 : alterJobs.values()) {
            if (alterJobV2.getType() != AlterJobV2.JobType.ROLLUP) {
                continue;
            }
            while (!alterJobV2.getJobState().isFinalState()) {
                System.out.println(
                        "rollup job " + alterJobV2.getJobId() + " is running. state: " + alterJobV2.getJobState());
                Thread.sleep(1000);
            }
            System.out.println("rollup job " + alterJobV2.getJobId() + " is done. state: " + alterJobV2.getJobState());
            Assertions.assertEquals(AlterJobV2.JobState.FINISHED, alterJobV2.getJobState());
            ++finishedNum;
            if (finishedNum == 3) {
                int i = 3;
                while (tbl.getState() != OlapTableState.NORMAL && i > 0) {
                    Thread.sleep(1000);
                    i--;
                }
                Assertions.assertEquals(OlapTableState.NORMAL, tbl.getState());
            } else {
                Assertions.assertEquals(OlapTableState.ROLLUP, tbl.getState());
            }
        }

        for (Partition partition : tbl.getPartitions()) {
            Assertions.assertEquals(4, partition.getMaterializedIndices(IndexExtState.VISIBLE).size());
        }
    }

    private void alterTable(String sql, ConnectContext ctx) throws Exception {
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan parsed = nereidsParser.parseSingle(sql);
        StmtExecutor stmtExecutor = new StmtExecutor(ctx, sql);
        ctx.setStatementContext(new StatementContext());
        if (parsed instanceof AlterTableCommand) {
            ((AlterTableCommand) parsed).run(ctx, stmtExecutor);
        }
    }
}
