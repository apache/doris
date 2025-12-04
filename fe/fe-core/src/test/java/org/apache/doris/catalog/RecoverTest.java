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
import org.apache.doris.common.FeConstants;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.AlterTableCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateDatabaseCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateTableCommand;
import org.apache.doris.nereids.trees.plans.commands.DropDatabaseCommand;
import org.apache.doris.nereids.trees.plans.commands.DropTableCommand;
import org.apache.doris.nereids.trees.plans.commands.RecoverDatabaseCommand;
import org.apache.doris.nereids.trees.plans.commands.RecoverPartitionCommand;
import org.apache.doris.nereids.trees.plans.commands.RecoverTableCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.utframe.UtFrameUtils;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.UUID;

public class RecoverTest {

    private static String runningDir = "fe/mocked/RecoverTest/" + UUID.randomUUID() + "/";

    private static ConnectContext connectContext;

    @BeforeClass
    public static void beforeClass() throws Exception {
        FeConstants.runningUnitTest = true;
        UtFrameUtils.createDorisCluster(runningDir);

        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
    }

    @AfterClass
    public static void tearDown() {
        File file = new File(runningDir);
        file.delete();
    }

    private static void createDb(String db) throws Exception {
        String createDbStmtStr = "create database " + db;
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan logicalPlan = nereidsParser.parseSingle(createDbStmtStr);
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, createDbStmtStr);
        if (logicalPlan instanceof CreateDatabaseCommand) {
            ((CreateDatabaseCommand) logicalPlan).run(connectContext, stmtExecutor);
        }
    }

    private static void createTable(String sql) throws Exception {
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan parsed = nereidsParser.parseSingle(sql);
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, sql);
        if (parsed instanceof CreateTableCommand) {
            ((CreateTableCommand) parsed).run(connectContext, stmtExecutor);
        }
    }

    private static void dropDb(String db) throws Exception {
        String sql = "drop database " + db;
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan parsed = nereidsParser.parseSingle(sql);
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, sql);
        if (parsed instanceof DropDatabaseCommand) {
            ((DropDatabaseCommand) parsed).run(connectContext, stmtExecutor);
        }
    }

    private static void dropTable(String db, String tbl) throws Exception {
        String sql = "drop table " + db + "." + tbl;
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan parsed = nereidsParser.parseSingle(sql);
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, sql);
        if (parsed instanceof DropTableCommand) {
            ((DropTableCommand) parsed).run(connectContext, stmtExecutor);
        }
    }

    private static void dropPartition(String db, String tbl, String part) throws Exception {
        String sql = "alter table " + db + "." + tbl + " drop partition " + part;
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan parsed = nereidsParser.parseSingle(sql);
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, sql);
        if (parsed instanceof AlterTableCommand) {
            ((AlterTableCommand) parsed).run(connectContext, stmtExecutor);
        }
    }

    private static void recoverDb(String db, long dbId) throws Exception {
        String sql;
        if (dbId != -1) {
            sql = "recover database " + db + " " + String.valueOf(dbId);
        } else {
            sql = "recover database " + db;
        }
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan parsed = nereidsParser.parseSingle(sql);
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, sql);
        if (parsed instanceof RecoverDatabaseCommand) {
            ((RecoverDatabaseCommand) parsed).run(connectContext, stmtExecutor);
        }
    }

    private static void recoverTable(String db, String tbl, long tableId) throws Exception {
        String sql;
        if (tableId != -1) {
            sql = "recover table " + db + "." + tbl + " " + String.valueOf(tableId);
        } else {
            sql = "recover table " + db + "." + tbl;
        }
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan parsed = nereidsParser.parseSingle(sql);
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, sql);
        if (parsed instanceof RecoverTableCommand) {
            ((RecoverTableCommand) parsed).run(connectContext, stmtExecutor);
        }
    }

    private static void recoverPartition(String db, String tbl, String part, long partId) throws Exception {
        String sql;
        if (partId != -1) {
            sql = "recover partition " + part + " " + String.valueOf(partId) + " from " + db + "." + tbl;
        } else {
            sql = "recover partition " + part + " from " + db + "." + tbl;
        }
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan parsed = nereidsParser.parseSingle(sql);
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, sql);
        if (parsed instanceof RecoverPartitionCommand) {
            ((RecoverPartitionCommand) parsed).run(connectContext, stmtExecutor);
        }
    }

    private static boolean checkDbExist(String dbName) {
        return Env.getCurrentInternalCatalog().getDb(dbName).isPresent();
    }

    private static boolean checkTableExist(String dbName, String tblName) {
        return Env.getCurrentInternalCatalog()
                .getDb(dbName)
                .flatMap(db -> db.getTable(tblName)).isPresent();
    }

    private static boolean checkTableInDynamicScheduler(Long dbId, Long tableId) {
        return Env.getCurrentEnv().getDynamicPartitionScheduler().containsDynamicPartitionTable(dbId, tableId);
    }

    private static boolean checkPartitionExist(String dbName, String tblName, String partName) {
        return Env.getCurrentInternalCatalog()
                .getDb(dbName)
                .flatMap(db -> db.getTable(tblName)).map(table -> table.getPartition(partName)).isPresent();
    }

    private static long getDbId(String dbName) throws DdlException {
        Database db = (Database) Env.getCurrentInternalCatalog()
                .getDbOrDdlException(dbName);
        if (db != null) {
            return db.getId();
        } else {
            return -1;
        }
    }

    private static long getTableId(String dbName, String tblName) throws DdlException {
        Database db = (Database) Env.getCurrentInternalCatalog()
                .getDbOrDdlException(dbName);
        if (db != null) {
            OlapTable olapTable = db.getOlapTableOrDdlException(tblName);
            if (olapTable != null) {
                return olapTable.getId();
            } else {
                return -1;
            }
        } else {
            return -1;
        }
    }

    private static long getPartId(String dbName, String tblName, String partName) throws DdlException {
        Database db = (Database) Env.getCurrentInternalCatalog()
                .getDbOrDdlException(dbName);
        if (db != null) {
            OlapTable olapTable = db.getOlapTableOrDdlException(tblName);
            if (olapTable != null) {
                Partition partition = olapTable.getPartition(partName);
                if (partition != null) {
                    return partition.getId();
                } else {
                    return -1;
                }
            } else {
                return -1;
            }
        } else {
            return -1;
        }
    }

    @Test
    public void testRecover() throws Exception {
        createDb("test");
        createTable("CREATE TABLE test.`table1` (\n"
                + "  `event_date` date NOT NULL COMMENT \"\",\n"
                + "  `app_name` varchar(64) NOT NULL COMMENT \"\",\n"
                + "  `package_name` varchar(64) NOT NULL COMMENT \"\",\n"
                + "  `age` varchar(32) NOT NULL COMMENT \"\",\n"
                + "  `gender` varchar(32) NOT NULL COMMENT \"\",\n"
                + "  `level` varchar(64) NOT NULL COMMENT \"\",\n"
                + "  `city` varchar(64) NOT NULL COMMENT \"\",\n"
                + "  `model` varchar(64) NOT NULL COMMENT \"\",\n"
                + "  `brand` varchar(64) NOT NULL COMMENT \"\",\n"
                + "  `hours` varchar(16) NOT NULL COMMENT \"\",\n"
                + "  `use_num` int(11) SUM NOT NULL COMMENT \"\",\n"
                + "  `use_time` double SUM NOT NULL COMMENT \"\",\n"
                + "  `start_times` bigint(20) SUM NOT NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "AGGREGATE KEY(`event_date`, `app_name`, `package_name`, `age`, `gender`, `level`, `city`, \n"
                + " `model`, `brand`, `hours`) COMMENT \"OLAP\"\n"
                + "PARTITION BY RANGE(`event_date`)\n"
                + "(PARTITION p1 VALUES [('2020-02-27'), ('2020-03-02')),\n"
                + "PARTITION p2 VALUES [('2020-03-02'), ('2020-03-07')))\n"
                + "DISTRIBUTED BY HASH(`event_date`, `app_name`, `package_name`, `age`, `gender`, `level`, `city`, \n"
                + " `model`, `brand`, `hours`) BUCKETS 1 PROPERTIES (\n"
                + " \"replication_num\" = \"1\"\n"
                + ");");

        Assert.assertTrue(checkDbExist("test"));
        Assert.assertTrue(checkTableExist("test", "table1"));

        dropDb("test");
        Assert.assertFalse(checkDbExist("test"));
        Assert.assertFalse(checkTableExist("test", "table1"));

        recoverDb("test", -1);
        Assert.assertTrue(checkDbExist("test"));
        Assert.assertTrue(checkTableExist("test", "table1"));

        long dbId = getDbId("test");
        dropDb("test");
        Assert.assertFalse(checkDbExist("test"));
        Assert.assertFalse(checkTableExist("test", "table1"));

        recoverDb("test", dbId);
        Assert.assertTrue(checkDbExist("test"));
        Assert.assertTrue(checkTableExist("test", "table1"));

        dropTable("test", "table1");
        Assert.assertTrue(checkDbExist("test"));
        Assert.assertFalse(checkTableExist("test", "table1"));

        recoverTable("test", "table1", -1);
        Assert.assertTrue(checkDbExist("test"));
        Assert.assertTrue(checkTableExist("test", "table1"));

        dropTable("test", "table1");
        Assert.assertTrue(checkDbExist("test"));
        Assert.assertFalse(checkTableExist("test", "table1"));

        createTable("CREATE TABLE test.`table1` (\n"
                + "  `event_date` date NOT NULL COMMENT \"\",\n"
                + "  `app_name` varchar(64) NOT NULL COMMENT \"\",\n"
                + "  `package_name` varchar(64) NOT NULL COMMENT \"\",\n"
                + "  `age` varchar(32) NOT NULL COMMENT \"\",\n"
                + "  `gender` varchar(32) NOT NULL COMMENT \"\",\n"
                + "  `level` varchar(64) NOT NULL COMMENT \"\",\n"
                + "  `city` varchar(64) NOT NULL COMMENT \"\",\n"
                + "  `model` varchar(64) NOT NULL COMMENT \"\",\n"
                + "  `brand` varchar(64) NOT NULL COMMENT \"\",\n"
                + "  `hours` varchar(16) NOT NULL COMMENT \"\",\n"
                + "  `use_num` int(11) SUM NOT NULL COMMENT \"\",\n"
                + "  `use_time` double SUM NOT NULL COMMENT \"\",\n"
                + "  `start_times` bigint(20) SUM NOT NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "AGGREGATE KEY(`event_date`, `app_name`, `package_name`, `age`, `gender`, `level`, \n"
                + " `city`, `model`, `brand`, `hours`) COMMENT \"OLAP\"\n"
                + "PARTITION BY RANGE(`event_date`)\n"
                + "(PARTITION p1 VALUES [('2020-02-27'), ('2020-03-02')),\n"
                + "PARTITION p2 VALUES [('2020-03-02'), ('2020-03-07')))\n"
                + "DISTRIBUTED BY HASH(`event_date`, `app_name`, `package_name`, `age`, `gender`, `level`, `city`, \n"
                + " `model`, `brand`, `hours`) BUCKETS 1 PROPERTIES (\n"
                + " \"replication_num\" = \"1\"\n"
                + ");");
        Assert.assertTrue(checkDbExist("test"));
        Assert.assertTrue(checkTableExist("test", "table1"));

        try {
            recoverTable("test", "table1", -1);
            Assert.fail("should not recover succeed");
        } catch (DdlException e) {
            e.printStackTrace();
        }

        Assert.assertTrue(checkPartitionExist("test", "table1", "p1"));
        dropPartition("test", "table1", "p1");
        Assert.assertFalse(checkPartitionExist("test", "table1", "p1"));

        recoverPartition("test", "table1", "p1", -1);
        Assert.assertTrue(checkPartitionExist("test", "table1", "p1"));
    }

    @Test
    public void testRecover2() throws Exception {
        createDb("test2");
        createTable("CREATE TABLE test2.`table2` (\n"
                + "  `event_date` datetime(3) NOT NULL COMMENT \"\",\n"
                + "  `app_name` varchar(64) NOT NULL COMMENT \"\",\n"
                + "  `package_name` varchar(64) NOT NULL COMMENT \"\",\n"
                + "  `age` varchar(32) NOT NULL COMMENT \"\",\n"
                + "  `gender` varchar(32) NOT NULL COMMENT \"\",\n"
                + "  `level` varchar(64) NOT NULL COMMENT \"\",\n"
                + "  `city` varchar(64) NOT NULL COMMENT \"\",\n"
                + "  `model` varchar(64) NOT NULL COMMENT \"\",\n"
                + "  `brand` varchar(64) NOT NULL COMMENT \"\",\n"
                + "  `hours` varchar(16) NOT NULL COMMENT \"\",\n"
                + "  `use_num` int(11) SUM NOT NULL COMMENT \"\",\n"
                + "  `use_time` double SUM NOT NULL COMMENT \"\",\n"
                + "  `start_times` bigint(20) SUM NOT NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "AGGREGATE KEY(`event_date`, `app_name`, `package_name`, `age`, `gender`, `level`, \n"
                + "`city`, `model`, `brand`, `hours`) COMMENT \"OLAP\"\n"
                + "PARTITION BY RANGE(`event_date`)\n"
                + "(PARTITION p1 VALUES [('2020-02-27 00:00:00'), ('2020-03-02 00:00:00')),\n"
                + "PARTITION p2 VALUES [('2020-03-02 00:00:00'), ('2020-03-07 00:00:00')))\n"
                + "DISTRIBUTED BY HASH(`event_date`, `app_name`, `package_name`, `age`, `gender`, `level`, \n"
                + " `city`, `model`, `brand`, `hours`) BUCKETS 1 PROPERTIES (\n"
                + " \"replication_num\" = \"1\"\n"
                + ");");

        Assert.assertTrue(checkDbExist("test2"));
        Assert.assertTrue(checkTableExist("test2", "table2"));

        dropDb("test2");
        Assert.assertFalse(checkDbExist("test2"));
        Assert.assertFalse(checkTableExist("test2", "table2"));

        recoverDb("test2", -1);
        Assert.assertTrue(checkDbExist("test2"));
        Assert.assertTrue(checkTableExist("test2", "table2"));

        long dbId = getDbId("test2");
        dropDb("test2");
        Assert.assertFalse(checkDbExist("test2"));
        Assert.assertFalse(checkTableExist("test2", "table2"));

        recoverDb("test2", dbId);
        Assert.assertTrue(checkDbExist("test2"));
        Assert.assertTrue(checkTableExist("test2", "table2"));

        dropTable("test2", "table2");
        Assert.assertTrue(checkDbExist("test2"));
        Assert.assertFalse(checkTableExist("test2", "table2"));

        recoverTable("test2", "table2", -1);
        Assert.assertTrue(checkDbExist("test2"));
        Assert.assertTrue(checkTableExist("test2", "table2"));

        dropTable("test2", "table2");
        Assert.assertTrue(checkDbExist("test2"));
        Assert.assertFalse(checkTableExist("test2", "table2"));

        createTable("CREATE TABLE test2.`table2` (\n"
                + "  `event_date` datetime(3) NOT NULL COMMENT \"\",\n"
                + "  `app_name` varchar(64) NOT NULL COMMENT \"\",\n"
                + "  `package_name` varchar(64) NOT NULL COMMENT \"\",\n"
                + "  `age` varchar(32) NOT NULL COMMENT \"\",\n"
                + "  `gender` varchar(32) NOT NULL COMMENT \"\",\n"
                + "  `level` varchar(64) NOT NULL COMMENT \"\",\n"
                + "  `city` varchar(64) NOT NULL COMMENT \"\",\n"
                + "  `model` varchar(64) NOT NULL COMMENT \"\",\n"
                + "  `brand` varchar(64) NOT NULL COMMENT \"\",\n"
                + "  `hours` varchar(16) NOT NULL COMMENT \"\",\n"
                + "  `use_num` int(11) SUM NOT NULL COMMENT \"\",\n"
                + "  `use_time` double SUM NOT NULL COMMENT \"\",\n"
                + "  `start_times` bigint(20) SUM NOT NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "AGGREGATE KEY(`event_date`, `app_name`, `package_name`, `age`, `gender`, `level`, `city`, \n"
                + "  `model`, `brand`, `hours`) COMMENT \"OLAP\"\n"
                + "PARTITION BY RANGE(`event_date`)\n"
                + "(PARTITION p1 VALUES [('2020-02-27 00:00:00'), ('2020-03-02 00:00:00')),\n"
                + "PARTITION p2 VALUES [('2020-03-02 00:00:00'), ('2020-03-07 00:00:00')))\n"
                + "DISTRIBUTED BY HASH(`event_date`, `app_name`, `package_name`, `age`, `gender`, `level`, `city`, \n"
                + " `model`, `brand`, `hours`) BUCKETS 1 PROPERTIES (\n"
                + " \"replication_num\" = \"1\"\n"
                + ");");
        Assert.assertTrue(checkDbExist("test2"));
        Assert.assertTrue(checkTableExist("test2", "table2"));

        try {
            recoverTable("test2", "table2", -1);
            Assert.fail("should not recover succeed");
        } catch (DdlException e) {
            e.printStackTrace();
        }

        Assert.assertTrue(checkPartitionExist("test2", "table2", "p1"));
        dropPartition("test2", "table2", "p1");
        Assert.assertFalse(checkPartitionExist("test2", "table2", "p1"));

        recoverPartition("test2", "table2", "p1", -1);
        Assert.assertTrue(checkPartitionExist("test2", "table2", "p1"));
    }


    @Test
    public void testDynamicTableRecover() throws Exception {
        createDb("test3");
        createTable("CREATE TABLE test3.`table3` (\n"
                + "  `event_date` datetime(3) NOT NULL COMMENT \"\",\n"
                + "  `app_name` varchar(64) NOT NULL COMMENT \"\",\n"
                + "  `package_name` varchar(64) NOT NULL COMMENT \"\",\n"
                + "  `age` varchar(32) NOT NULL COMMENT \"\",\n"
                + "  `gender` varchar(32) NOT NULL COMMENT \"\",\n"
                + "  `level` varchar(64) NOT NULL COMMENT \"\",\n"
                + "  `city` varchar(64) NOT NULL COMMENT \"\",\n"
                + "  `model` varchar(64) NOT NULL COMMENT \"\",\n"
                + "  `brand` varchar(64) NOT NULL COMMENT \"\",\n"
                + "  `hours` varchar(16) NOT NULL COMMENT \"\",\n"
                + "  `use_num` int(11) SUM NOT NULL COMMENT \"\",\n"
                + "  `use_time` double SUM NOT NULL COMMENT \"\",\n"
                + "  `start_times` bigint(20) SUM NOT NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "AGGREGATE KEY(`event_date`, `app_name`, `package_name`, `age`, `gender`, `level`, `city`, \n"
                + "  `model`, `brand`, `hours`) COMMENT \"OLAP\"\n"
                + "PARTITION BY RANGE(`event_date`)\n"
                + "(PARTITION p1 VALUES [('2020-02-27 00:00:00'), ('2020-03-02 00:00:00')),\n"
                + "PARTITION p2 VALUES [('2020-03-02 00:00:00'), ('2020-03-07 00:00:00')))\n"
                + "DISTRIBUTED BY HASH(`event_date`, `app_name`, `package_name`, `age`, `gender`, `level`, `city`, \n"
                + " `model`, `brand`, `hours`) BUCKETS 1 PROPERTIES (\n"
                + "\"replication_num\" = \"1\",\n"
                + "\"dynamic_partition.enable\" = \"true\",\n"
                + "\"dynamic_partition.time_unit\" = \"DAY\",\n"
                + "\"dynamic_partition.end\" = \"3\",\n"
                + "\"dynamic_partition.prefix\" = \"p\",\n"
                + "\"dynamic_partition.buckets\" = \"1\",\n"
                + "\"dynamic_partition.replication_num\" = \"1\",\n"
                + "\"dynamic_partition.create_history_partition\"=\"true\",\n"
                + "\"dynamic_partition.start\" = \"-3\"\n"
                + ");\n");
        Long dbId = getDbId("test3");
        Long tableId = getTableId("test3", "table3");
        dropTable("test3", "table3");
        recoverTable("test3", "table3", -1);
        Assert.assertTrue(checkTableInDynamicScheduler(dbId, tableId));
    }
}
