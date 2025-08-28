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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.common.util.DebugPointUtil;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.info.PartitionNamesInfo;
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class TruncateTableCommandTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        Config.enable_debug_points = true;
        createDatabase("testcommand");
        connectContext.setDatabase("testcommand");

        String createTableStr = "create table testcommand.tblcommand(d1 date, k1 int, k2 bigint)"
                + "duplicate key(d1, k1) "
                + "PARTITION BY RANGE(d1)"
                + "(PARTITION p20210901 VALUES [('2021-09-01'), ('2021-09-02')))"
                + "distributed by hash(k1) buckets 2 "
                + "properties('replication_num' = '1');";
        createTable(createTableStr);

        String createTable2 = "CREATE TABLE testcommand.case_sensitive_table_command (\n"
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

    @Test
    public void testValidate(@Mocked AccessControllerManager accessManager) {
        new Expectations() {
            {
                Env.getCurrentEnv().getAccessManager();
                minTimes = 0;
                result = accessManager;

                accessManager.checkTblPriv((ConnectContext) any, anyString, anyString, anyString, PrivPredicate.LOAD);
                minTimes = 0;
                result = true;
            }
        };

        String truncateStr = "TRUNCATE TABLE internal.testcommand.case_sensitive_table_command PARTITION P20211008; \n";

        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan plan = nereidsParser.parseSingle(truncateStr);
        Assertions.assertTrue(plan instanceof TruncateTableCommand);
        Assertions.assertDoesNotThrow(() -> ((TruncateTableCommand) plan).validate(connectContext));

        // test no database
        TableNameInfo tableNameInfo = new TableNameInfo("internal", "", "test");
        TruncateTableCommand truncateTableCommand = new TruncateTableCommand(tableNameInfo, Optional.empty(), false);
        connectContext.setDatabase("");
        Assertions.assertThrows(AnalysisException.class, () -> truncateTableCommand.validate(connectContext));
        connectContext.setDatabase("test"); //reset database

        // test no table
        tableNameInfo = new TableNameInfo("internal", "testcommand", "");
        TruncateTableCommand truncateTableCommand1 = new TruncateTableCommand(tableNameInfo, Optional.empty(), false);
        Assertions.assertThrows(AnalysisException.class, () -> truncateTableCommand1.validate(connectContext));

        // test no partition
        tableNameInfo = new TableNameInfo("internal", "testcommand", "test");
        PartitionNamesInfo partitionNamesInfo = new PartitionNamesInfo(false);
        TruncateTableCommand truncateTableCommand2 = new TruncateTableCommand(tableNameInfo, Optional.of(partitionNamesInfo), false);
        Assertions.assertThrows(AnalysisException.class, () -> truncateTableCommand2.validate(connectContext));
    }

    @Test
    public void testTruncateWithCaseInsensitivePartitionName() throws Exception {
        //now in order to support auto create partition, need set partition name is case sensitive
        Database db = Env.getCurrentInternalCatalog().getDbNullable("testcommand");
        OlapTable tbl = db.getOlapTableOrDdlException("case_sensitive_table_command");

        long p20211006Id = tbl.getPartition("p20211006").getId();
        long p20211007Id = tbl.getPartition("P20211007").getId();
        long p20211008Id = tbl.getPartition("P20211008").getId();

        // truncate P20211008(real name is P20211008)
        Partition p20211008 = tbl.getPartition("P20211008");
        p20211008.updateVisibleVersion(2L);
        p20211008.setNextVersion(p20211008.getVisibleVersion() + 1);
        String truncateStr = "TRUNCATE TABLE internal.testcommand.case_sensitive_table_command PARTITION P20211008; \n";

        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan plan = nereidsParser.parseSingle(truncateStr);
        Assertions.assertTrue(plan instanceof TruncateTableCommand);
        Env.getCurrentEnv().truncateTable((TruncateTableCommand) plan);
        Assertions.assertNotEquals(p20211008Id, tbl.getPartition("P20211008").getId());

        // 2. truncate P20211007
        truncateStr = "TRUNCATE TABLE internal.testcommand.case_sensitive_table_command PARTITION P20211007 force; \n";
        plan = nereidsParser.parseSingle(truncateStr);
        Assertions.assertTrue(plan instanceof TruncateTableCommand);
        Env.getCurrentEnv().truncateTable((TruncateTableCommand) plan);

        Assertions.assertEquals(p20211006Id, tbl.getPartition("p20211006").getId());
        Assertions.assertEquals(p20211007Id, tbl.getPartition("P20211007").getId());
        Assertions.assertNotNull(tbl.getPartition("p20211006"));
        Assertions.assertNotNull(tbl.getPartition("P20211007"));
        Assertions.assertNotNull(tbl.getPartition("P20211008"));
    }

    @Test
    public void testTruncateTable() throws Exception {
        String stmtStr = "ALTER TABLE internal.testcommand.tblcommand ADD PARTITION p20210902 VALUES [('2021-09-02'), ('2021-09-03'))"
                + " DISTRIBUTED BY HASH(`k1`) BUCKETS 3;";
        alterTable(stmtStr);
        stmtStr = "ALTER TABLE internal.testcommand.tblcommand ADD PARTITION p20210903 VALUES [('2021-09-03'), ('2021-09-04'))"
                + " DISTRIBUTED BY HASH(`k1`) BUCKETS 4;";
        alterTable(stmtStr);
        stmtStr = "ALTER TABLE internal.testcommand.tblcommand ADD PARTITION p20210904 VALUES [('2021-09-04'), ('2021-09-05'))"
                + " DISTRIBUTED BY HASH(`k1`) BUCKETS 5;";
        alterTable(stmtStr);
        checkShowTabletResultNum("internal.testcommand.tblcommand", "p20210901", 2);
        checkShowTabletResultNum("internal.testcommand.tblcommand", "p20210902", 3);
        checkShowTabletResultNum("internal.testcommand.tblcommand", "p20210903", 4);
        checkShowTabletResultNum("internal.testcommand.tblcommand", "p20210904", 5);

        String truncateStr = "truncate table internal.testcommand.tblcommand;";
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan plan = nereidsParser.parseSingle(truncateStr);
        Assertions.assertTrue(plan instanceof TruncateTableCommand);
        Env.getCurrentEnv().truncateTable((TruncateTableCommand) plan);

        checkShowTabletResultNum("internal.testcommand.tblcommand", "p20210901", 2);
        checkShowTabletResultNum("internal.testcommand.tblcommand", "p20210902", 3);
        checkShowTabletResultNum("internal.testcommand.tblcommand", "p20210903", 4);
        checkShowTabletResultNum("internal.testcommand.tblcommand", "p20210904", 5);

        truncateStr = "truncate table internal.testcommand.tblcommand partition(p20210901, p20210902, p20210903, p20210904);";
        plan = nereidsParser.parseSingle(truncateStr);
        Assertions.assertTrue(plan instanceof TruncateTableCommand);
        Env.getCurrentEnv().truncateTable((TruncateTableCommand) plan);

        checkShowTabletResultNum("internal.testcommand.tblcommand", "p20210901", 2);
        checkShowTabletResultNum("internal.testcommand.tblcommand", "p20210902", 3);
        checkShowTabletResultNum("internal.testcommand.tblcommand", "p20210903", 4);
        checkShowTabletResultNum("internal.testcommand.tblcommand", "p20210904", 5);

        truncateStr = "truncate table internal.testcommand.tblcommand partition (p20210901);";
        plan = nereidsParser.parseSingle(truncateStr);
        Assertions.assertTrue(plan instanceof TruncateTableCommand);
        Env.getCurrentEnv().truncateTable((TruncateTableCommand) plan);
        checkShowTabletResultNum("internal.testcommand.tblcommand", "p20210901", 2);

        truncateStr = "truncate table internal.testcommand.tblcommand partition (p20210902);";
        plan = nereidsParser.parseSingle(truncateStr);
        Assertions.assertTrue(plan instanceof TruncateTableCommand);
        Env.getCurrentEnv().truncateTable((TruncateTableCommand) plan);
        checkShowTabletResultNum("internal.testcommand.tblcommand", "p20210902", 3);

        truncateStr = "truncate table internal.testcommand.tblcommand partition (p20210903);";
        plan = nereidsParser.parseSingle(truncateStr);
        Assertions.assertTrue(plan instanceof TruncateTableCommand);
        Env.getCurrentEnv().truncateTable((TruncateTableCommand) plan);
        checkShowTabletResultNum("internal.testcommand.tblcommand", "p20210903", 4);

        truncateStr = "truncate table internal.testcommand.tblcommand partition (p20210904);";
        plan = nereidsParser.parseSingle(truncateStr);
        Assertions.assertTrue(plan instanceof TruncateTableCommand);
        Env.getCurrentEnv().truncateTable((TruncateTableCommand) plan);
        checkShowTabletResultNum("internal.testcommand.tblcommand", "p20210904", 5);
    }

    @Test
    public void testTruncateTableFailed() throws Exception {
        String createTableStr = "create table internal.testcommand.tbl2(d1 date, k1 int, k2 bigint)"
                + "duplicate key(d1, k1) "
                + "PARTITION BY RANGE(d1)"
                + "(PARTITION p20210901 VALUES [('2021-09-01'), ('2021-09-02')))"
                + "distributed by hash(k1) buckets 2 "
                + "properties('replication_num' = '1');";
        createTable(createTableStr);
        String partitionName = "p20210901";
        Database db = Env.getCurrentInternalCatalog().getDbNullable("testcommand");
        OlapTable tbl2 = db.getOlapTableOrDdlException("tbl2");
        Assertions.assertNotNull(tbl2);

        Partition p20210901 = tbl2.getPartition(partitionName);
        Assertions.assertNotNull(p20210901);

        long partitionId = p20210901.getId();
        p20210901.setVisibleVersionAndTime(2L, System.currentTimeMillis());

        try {
            List<Long> backendIds = Env.getCurrentSystemInfo().getAllBackendIds();
            Map<Long, Set<Long>> oldBackendTablets = Maps.newHashMap();
            for (long backendId : backendIds) {
                Set<Long> tablets = Sets.newHashSet(Env.getCurrentInvertedIndex().getTabletIdsByBackendId(backendId));
                oldBackendTablets.put(backendId, tablets);
            }

            DebugPointUtil.addDebugPoint("InternalCatalog.truncateTable.metaChanged", new DebugPointUtil.DebugPoint());
            String truncateStr = "truncate table internal.testcommand.tbl2 partition (" + partitionName + ");";

            NereidsParser nereidsParser = new NereidsParser();
            LogicalPlan plan = nereidsParser.parseSingle(truncateStr);
            Assertions.assertTrue(plan instanceof TruncateTableCommand);

            ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                    "Table[tbl2]'s meta has been changed. try again",
                    () -> Env.getCurrentEnv().truncateTable((TruncateTableCommand) plan));

            Assertions.assertEquals(partitionId, tbl2.getPartition(partitionName).getId());
            for (long backendId : backendIds) {
                Set<Long> tablets = Sets.newHashSet(Env.getCurrentInvertedIndex().getTabletIdsByBackendId(backendId));
                Assertions.assertEquals(oldBackendTablets.get(backendId), tablets);
            }
        } finally {
            DebugPointUtil.removeDebugPoint("InternalCatalog.truncateTable.metaChanged");
        }
    }

    private List<List<String>> checkShowTabletResultNum(String tbl, String partition, int expected) throws Exception {
        String showStr = "show tablets from " + tbl + " partition(" + partition + ")";

        LogicalPlan plan = new NereidsParser().parseSingle(showStr);
        Assertions.assertTrue(plan instanceof ShowTabletsFromTableCommand);
        ShowResultSet showResultSet = ((ShowTabletsFromTableCommand) plan).doRun(connectContext, null);

        List<List<String>> rows = showResultSet.getResultRows();
        Assertions.assertEquals(expected, rows.size());
        return rows;
    }

    private void alterTable(String sql) throws Exception {
        try {
            LogicalPlan plan = new NereidsParser().parseSingle(sql);
            Assertions.assertTrue(plan instanceof AlterTableCommand);
            ((AlterTableCommand) plan).run(connectContext, null);
        } catch (Exception e) {
            throw e;
        }
    }
}
