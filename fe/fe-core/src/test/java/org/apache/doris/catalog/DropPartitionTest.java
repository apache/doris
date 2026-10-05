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

import org.apache.doris.common.DdlException;
import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.AlterTableCommand;
import org.apache.doris.nereids.trees.plans.commands.RecoverPartitionCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class DropPartitionTest extends TestWithFeService {

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        createTable("create table test.tbl1(d1 date, k1 int, k2 bigint) duplicate key(d1, k1) "
                + "PARTITION BY RANGE(d1) (PARTITION p20210201 VALUES [('2021-02-01'), ('2021-02-02')),"
                + "PARTITION p20210202 VALUES [('2021-02-02'), ('2021-02-03')),"
                + "PARTITION p20210203 VALUES [('2021-02-03'), ('2021-02-04'))) distributed by hash(k1) "
                + "buckets 1 properties('replication_num' = '1');");
    }

    private void dropPartition(String sql) throws Exception {
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan parsed = nereidsParser.parseSingle(sql);
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, sql);
        if (parsed instanceof AlterTableCommand) {
            ((AlterTableCommand) parsed).run(connectContext, stmtExecutor);
        }
    }

    private void recoverPartition(String sql) throws Exception {
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan parsed = nereidsParser.parseSingle(sql);
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, sql);
        if (parsed instanceof RecoverPartitionCommand) {
            ((RecoverPartitionCommand) parsed).run(connectContext, stmtExecutor);
        }
    }

    @Test
    public void testNormalDropPartition() throws Exception {
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException("test");
        OlapTable table = (OlapTable) db.getTableOrMetaException("tbl1", Table.TableType.OLAP);
        Partition partition = table.getPartition("p20210201");
        long tabletId = partition.getBaseIndex().getTablets().get(0).getId();
        String dropPartitionSql = " alter table test.tbl1 drop partition p20210201;";
        dropPartition(dropPartitionSql);
        List<Replica> replicaList = Env.getCurrentEnv().getTabletInvertedIndex().getReplicasByTabletId(tabletId);
        partition = table.getPartition("p20210201");
        Assertions.assertEquals(1, replicaList.size());
        Assertions.assertNull(partition);
        String recoverPartitionSql = "recover partition p20210201 from test.tbl1";
        recoverPartition(recoverPartitionSql);
        partition = table.getPartition("p20210201");
        Assertions.assertNotNull(partition);
        Assertions.assertEquals("p20210201", partition.getName());
    }

    @Test
    public void testForceDropPartition() throws Exception {
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException("test");
        OlapTable table = (OlapTable) db.getTableOrMetaException("tbl1", Table.TableType.OLAP);
        Partition partition = table.getPartition("p20210202");
        long tabletId = partition.getBaseIndex().getTablets().get(0).getId();
        String dropPartitionSql = " alter table test.tbl1 drop partition p20210202 force;";
        dropPartition(dropPartitionSql);
        List<Replica> replicaList = Env.getCurrentEnv().getTabletInvertedIndex().getReplicasByTabletId(tabletId);
        partition = table.getPartition("p20210202");
        Assertions.assertTrue(replicaList.isEmpty());
        Assertions.assertNull(partition);
        String recoverPartitionSql = "recover partition p20210202 from test.tbl1";
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "No partition named 'p20210202' or partition id '-1' in table tbl1",
                () -> recoverPartition(recoverPartitionSql));
    }

    @Test
    public void testDropPartitionAndReserveTablets() throws Exception {
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException("test");
        OlapTable table = (OlapTable) db.getTableOrMetaException("tbl1", Table.TableType.OLAP);
        Partition partition = table.getPartition("p20210203");
        long tabletId = partition.getBaseIndex().getTablets().get(0).getId();
        table.dropPartitionAndReserveTablet("p20210203");
        List<Replica> replicaList = Env.getCurrentEnv().getTabletInvertedIndex().getReplicasByTabletId(tabletId);
        partition = table.getPartition("p20210203");
        Assertions.assertEquals(1, replicaList.size());
        Assertions.assertNull(partition);
    }
}
