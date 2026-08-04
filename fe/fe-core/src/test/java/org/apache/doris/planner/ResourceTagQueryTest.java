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

package org.apache.doris.planner;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DiskInfo;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.AlterDatabasePropertiesCommand;
import org.apache.doris.nereids.trees.plans.commands.AlterSystemCommand;
import org.apache.doris.nereids.trees.plans.commands.AlterTableCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateDatabaseCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateTableCommand;
import org.apache.doris.nereids.trees.plans.commands.SetUserPropertiesCommand;
import org.apache.doris.nereids.trees.plans.commands.info.ModifyBackendOp;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.resource.Tag;
import org.apache.doris.resource.computegroup.AllBackendComputeGroup;
import org.apache.doris.resource.computegroup.ComputeGroup;
import org.apache.doris.resource.computegroup.MergedComputeGroup;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TDisk;
import org.apache.doris.thrift.TStorageMedium;
import org.apache.doris.utframe.TestWithFeService;
import org.apache.doris.utframe.UtFrameUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class ResourceTagQueryTest extends TestWithFeService {
    private static final Logger LOG = LogManager.getLogger(ResourceTagQueryTest.class);


    private static Random random = new Random(System.currentTimeMillis());

    private List<Backend> backends = Lists.newArrayList();

    private static Tag tag1;
    private static Tag tag2;

    static {
        try {
            tag1 = Tag.create(Tag.TYPE_LOCATION, "zone1");
            tag2 = Tag.create(Tag.TYPE_LOCATION, "zone2");
        } catch (AnalysisException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected int backendNum() {
        return 5;
    }

    @Override
    protected void beforeCreatingConnectContext() throws Exception {
        System.out.println(runningDir);
        FeConstants.runningUnitTest = true;
        Config.tablet_checker_interval_ms = 1000;
        Config.tablet_repair_delay_factor_second = 1;
        // 5 backends:
        // 127.0.0.1
        // 127.0.0.2
        // 127.0.0.3
        // 127.0.0.4
        // 127.0.0.5
    }

    @Override
    protected void runBeforeAll() throws Exception {
        // create database
        String createDbStmtStr = "create database test;";
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan logicalPlan = nereidsParser.parseSingle(createDbStmtStr);
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, createDbStmtStr);
        if (logicalPlan instanceof CreateDatabaseCommand) {
            ((CreateDatabaseCommand) logicalPlan).run(connectContext, stmtExecutor);
        }

        // must set disk info, or the tablet scheduler won't work
        backends = Env.getCurrentSystemInfo().getAllBackendsByAllCluster().values().asList();
        for (Backend be : backends) {
            Map<String, TDisk> backendDisks = Maps.newHashMap();
            TDisk tDisk1 = new TDisk();
            tDisk1.setRootPath("/home/doris.HDD");
            tDisk1.setDiskTotalCapacity(2000000000);
            tDisk1.setDataUsedCapacity(1);
            tDisk1.setUsed(true);
            tDisk1.setDiskAvailableCapacity(tDisk1.disk_total_capacity - tDisk1.data_used_capacity);
            tDisk1.setPathHash(random.nextLong());
            tDisk1.setStorageMedium(TStorageMedium.HDD);
            backendDisks.put(tDisk1.getRootPath(), tDisk1);

            TDisk tDisk2 = new TDisk();
            tDisk2.setRootPath("/home/doris.SSD");
            tDisk2.setDiskTotalCapacity(2000000000);
            tDisk2.setDataUsedCapacity(1);
            tDisk2.setUsed(true);
            tDisk2.setDiskAvailableCapacity(tDisk2.disk_total_capacity - tDisk2.data_used_capacity);
            tDisk2.setPathHash(random.nextLong());
            tDisk2.setStorageMedium(TStorageMedium.SSD);
            backendDisks.put(tDisk2.getRootPath(), tDisk2);

            be.updateDisks(backendDisks);
        }
    }

    private void createTableStmt(String sql) throws Exception {
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan parsed = nereidsParser.parseSingle(sql);
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, sql);
        if (parsed instanceof CreateTableCommand) {
            ((CreateTableCommand) parsed).run(connectContext, stmtExecutor);
        }
        // must set replicas' path hash, or the tablet scheduler won't work
        updateReplicaPathHash();
    }

    private static void updateReplicaPathHash() {
        Table<Long, Long, Replica> replicaMetaTable = Env.getCurrentInvertedIndex().getReplicaMetaTable();
        for (Table.Cell<Long, Long, Replica> cell : replicaMetaTable.cellSet()) {
            long beId = cell.getColumnKey();
            Backend be = Env.getCurrentSystemInfo().getBackend(beId);
            if (be == null) {
                continue;
            }
            Replica replica = cell.getValue();
            TabletMeta tabletMeta = Env.getCurrentInvertedIndex().getTabletMeta(cell.getRowKey());
            ImmutableMap<String, DiskInfo> diskMap = be.getDisks();
            for (DiskInfo diskInfo : diskMap.values()) {
                if (diskInfo.getStorageMedium() == tabletMeta.getStorageMedium()) {
                    replica.setPathHash(diskInfo.getPathHash());
                    break;
                }
            }
        }
    }

    private void alterTable(String sql) throws Exception {
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan parsed = nereidsParser.parseSingle(sql);
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, sql);
        if (parsed instanceof AlterTableCommand) {
            ((AlterTableCommand) parsed).run(connectContext, stmtExecutor);
        }
    }

    private void setProperty(String sql) throws Exception {
        SetUserPropertiesCommand setUserPropertyStmt
                = (SetUserPropertiesCommand) UtFrameUtils.parseStmt(sql, connectContext);
        setUserPropertyStmt.run(connectContext, null);
    }

    @Test
    public void test() throws Exception {

        // create table with default tag
        String createStr = "create table test.tbl1\n"
                + "(k1 date, k2 int)\n"
                + "partition by range(k1)\n"
                + "(\n"
                + " partition p1 values less than(\"2021-06-01\"),\n"
                + " partition p2 values less than(\"2021-07-01\"),\n"
                + " partition p3 values less than(\"2021-08-01\")\n"
                + ")\n"
                + "distributed by hash(k2) buckets 10;";
        ExceptionChecker.expectThrowsNoException(() -> createTableStmt(createStr));
        Database db = Env.getCurrentInternalCatalog().getDbNullable("test");
        OlapTable tbl = (OlapTable) db.getTableNullable("tbl1");

        ComputeGroup cg1 = Env.getCurrentEnv().getAuth().getComputeGroup(Auth.ROOT_USER);
        Assertions.assertTrue(cg1 instanceof AllBackendComputeGroup);

        // set default tag for root
        String setPropStr = "set property for 'root' 'resource_tags.location' = 'default';";
        ExceptionChecker.expectThrowsNoException(() -> setProperty(setPropStr));
        ComputeGroup cg2 = Env.getCurrentEnv().getAuth().getComputeGroup(Auth.ROOT_USER);
        Assertions.assertTrue(cg2 instanceof MergedComputeGroup);
        MergedComputeGroup cg3 = (MergedComputeGroup) cg2;
        String cg3Name = cg3.getName();
        Assertions.assertTrue(cg3Name.contains("default"));

        // update connection context and query
        connectContext.setComputeGroup(cg2);
        String queryStr = "explain select * from test.tbl1";
        String explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
        System.out.println(explainString);
        Assertions.assertTrue(explainString.contains("tablets=30/30"));

        // set zone1 tag for root
        String setPropStr2 = "set property for 'root' 'resource_tags.location' = 'zone1';";
        ExceptionChecker.expectThrowsNoException(() -> setProperty(setPropStr2));
        ComputeGroup cg4 = Env.getCurrentEnv().getAuth().getComputeGroup(Auth.ROOT_USER);
        Assertions.assertTrue(cg4 instanceof MergedComputeGroup);
        MergedComputeGroup cg5 = (MergedComputeGroup) cg4;
        Assertions.assertTrue(cg5.getName().contains("zone1"));

        // update connection context and query, it will failed because no zone1 backend
        connectContext.setComputeGroup(cg4);
        Assertions.assertTrue(connectContext.isSetComputeGroup());
        queryStr = "explain select * from test.tbl1";
        String error = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
        Assertions.assertTrue(error.contains("no queryable replicas"));

        // set [0, 1, 2] backends' tag to zone1, so that at least 1 replica can be queried.
        // set tag for all backends. 0-2 to zone1, 4 and 5 to zone2
        for (int i = 0; i < backends.size(); ++i) {
            Backend be = backends.get(i);
            String tag = "zone1";
            if (i > 2) {
                break;
            }
            // "alter system modify backend \"" + be.getHost() + ":" + be.getHeartbeatPort()
            //   + "\" set ('tag.location' = '" + tag + "')";
            String hostPort = be.getHost() + ":" + be.getHeartbeatPort();
            Map<String, String> properties = new HashMap<>();
            properties.put("tag.location", tag);
            ModifyBackendOp op = new ModifyBackendOp(ImmutableList.of(hostPort), properties);
            AlterSystemCommand command = new AlterSystemCommand(op, PlanType.ALTER_SYSTEM_MODIFY_BACKEND);
            command.doRun(connectContext, new StmtExecutor(connectContext, ""));
        }
        Assertions.assertEquals(tag1, backends.get(0).getLocationTag());
        Assertions.assertEquals(tag1, backends.get(1).getLocationTag());
        Assertions.assertEquals(tag1, backends.get(2).getLocationTag());

        queryStr = "explain select * from test.tbl1";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
        System.out.println(explainString);
        Assertions.assertTrue(explainString.contains("tablets=30/30"));

        // for now, 3 backends with tag zone1, 2 with tag default, so table is not stable.
        ExceptionChecker.expectThrows(UserException.class, () -> tbl.checkReplicaAllocation());
        // alter table's replication allocation to zone1:2 and default:1
        String alterStr
                = "alter table test.tbl1 modify partition (p1, p2, p3) set ('replication_allocation' = 'tag.location.zone1:2, tag.location.default:1')";
        ExceptionChecker.expectThrowsNoException(() -> alterTable(alterStr));
        Map<Tag, Short> expectedAllocMap = Maps.newHashMap();
        expectedAllocMap.put(Tag.DEFAULT_BACKEND_TAG, (short) 1);
        expectedAllocMap.put(tag1, (short) 2);
        for (Partition partition : tbl.getPartitions()) {
            ReplicaAllocation replicaAllocation = tbl.getPartitionInfo().getReplicaAllocation(partition.getId());
            Map<Tag, Short> allocMap = replicaAllocation.getAllocMap();
            Assertions.assertEquals(expectedAllocMap, allocMap);
        }
        ReplicaAllocation tblReplicaAllocation = tbl.getDefaultReplicaAllocation();
        Assertions.assertNotEquals(expectedAllocMap, tblReplicaAllocation.getAllocMap());
        ExceptionChecker.expectThrowsNoException(() -> checkTableReplicaAllocation(tbl));

        // can still query
        queryStr = "explain select * from test.tbl1";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
        System.out.println(explainString);
        Assertions.assertTrue(explainString.contains("tablets=30/30"));

        // set user exec mem limit
        String setExecMemLimitStr = "set property for 'root' 'exec_mem_limit' = '1000000';";
        ExceptionChecker.expectThrowsNoException(() -> setProperty(setExecMemLimitStr));
        long execMemLimit = Env.getCurrentEnv().getAuth().getExecMemLimit(Auth.ROOT_USER);
        Assertions.assertEquals(1000000, execMemLimit);

        List<List<String>> userProps = Env.getCurrentEnv().getAuth().getUserProperties(Auth.ROOT_USER);
        Assertions.assertEquals(15, userProps.size());

        // now :
        // be1 be2 be3 ==>tag1;
        // be4,be5  ==> default;
        // root ==> zone1;

        // create database
        String createDbStmtStr
                = "create database test_prop PROPERTIES('replication_allocation' = 'tag.location.default:3');";
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan logicalPlan = nereidsParser.parseSingle(createDbStmtStr);
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, createDbStmtStr);
        if (logicalPlan instanceof CreateDatabaseCommand) {
            ((CreateDatabaseCommand) logicalPlan).run(connectContext, stmtExecutor);
        }

        // create table with default tag
        String createTableStr2 = "create table test_prop.tbl2\n"
                + "(k1 date, k2 int)\n"
                + "distributed by hash(k2) buckets 2;";
        // table will inherit db prop, only have 2 default be, so `AnalysisException`
        ExceptionChecker.expectThrows(AnalysisException.class, () -> createTableStmt(createTableStr2));
        //alter db change `replication_allocation`
        String alterDbStmtStr
                = "alter database test_prop set PROPERTIES('replication_allocation' = 'tag.location.default:2');";
        AlterDatabasePropertiesCommand alterDatabasePropertiesCommand =
                (AlterDatabasePropertiesCommand) nereidsParser.parseSingle(alterDbStmtStr);
        Env.getCurrentEnv().alterDatabaseProperty(alterDatabasePropertiesCommand.getDbName(), alterDatabasePropertiesCommand.getProperties());

        ExceptionChecker.expectThrowsNoException(() -> createTableStmt(createTableStr2));
        Database propDb = Env.getCurrentInternalCatalog().getDbNullable("test_prop");
        OlapTable tbl2 = (OlapTable) propDb.getTableNullable("tbl2");
        // should same with db
        Map<Tag, Short> tbl2ExpectedAllocMap = Maps.newHashMap();
        tbl2ExpectedAllocMap.put(Tag.DEFAULT_BACKEND_TAG, (short) 2);
        for (Partition partition : tbl2.getPartitions()) {
            ReplicaAllocation replicaAllocation = tbl2.getPartitionInfo().getReplicaAllocation(partition.getId());
            Map<Tag, Short> allocMap = replicaAllocation.getAllocMap();
            Assertions.assertEquals(tbl2ExpectedAllocMap, allocMap);
        }
        // can not query due root ==> zone1
        queryStr = "explain select * from test_prop.tbl2";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
        System.out.println(explainString);
        Assertions.assertTrue(explainString.contains("no queryable replicas"));

        // The priority of table is higher than db,should same with table
        String createTableStr3 = "create table test_prop.tbl3\n"
                + "(k1 date, k2 int)\n"
                + "distributed by hash(k2) buckets 2 PROPERTIES('replication_allocation' = 'tag.location.zone1:3');";
        ExceptionChecker.expectThrowsNoException(() -> createTableStmt(createTableStr3));
        OlapTable tbl3 = (OlapTable) propDb.getTableNullable("tbl3");
        Map<Tag, Short> tbl3ExpectedAllocMap = Maps.newHashMap();
        tbl3ExpectedAllocMap.put(tag1, (short) 3);
        for (Partition partition : tbl3.getPartitions()) {
            ReplicaAllocation replicaAllocation = tbl3.getPartitionInfo().getReplicaAllocation(partition.getId());
            Map<Tag, Short> allocMap = replicaAllocation.getAllocMap();
            Assertions.assertEquals(tbl3ExpectedAllocMap, allocMap);
        }
        // can still query
        queryStr = "explain select * from test_prop.tbl3";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
        System.out.println(explainString);
        Assertions.assertTrue(explainString.contains("tablets=2/2"));
        //alter db change `replication_allocation` to null
        alterDbStmtStr = "alter database test_prop set PROPERTIES('replication_allocation' = '');";
        alterDatabasePropertiesCommand =
                (AlterDatabasePropertiesCommand) nereidsParser.parseSingle(alterDbStmtStr);
        Env.getCurrentEnv().alterDatabaseProperty(alterDatabasePropertiesCommand.getDbName(), alterDatabasePropertiesCommand.getProperties());
        // create table with default tag
        String createTableStr4 = "create table test_prop.tbl4\n"
                + "(k1 date, k2 int)\n"
                + "distributed by hash(k2) buckets 2;";
        // only have 2 default tag be, default need 3, so error
        ExceptionChecker.expectThrows(DdlException.class, () -> createTableStmt(createTableStr4));

        // now db not set `PROPERTIES`
        // The priority of partition is higher than table
        String createStr5 = "create table test_prop.tbl5\n"
                + "(k1 date, k2 int)\n"
                + "partition by range(k1)\n"
                + "(\n"
                + " partition p1 values less than(\"2021-06-01\") ('replication_allocation' = 'tag.location.zone1:1'),\n"
                + " partition p2 values less than(\"2021-08-01\")\n"
                + ")\n"
                + "distributed by hash(k2) buckets 2 "
                + "PROPERTIES('replication_allocation' = 'tag.location.default:1');";
        ExceptionChecker.expectThrowsNoException(() -> createTableStmt(createStr5));
        OlapTable tbl5 = (OlapTable) propDb.getTableNullable("tbl5");
        Map<Tag, Short> p1ExpectedAllocMap = Maps.newHashMap();
        p1ExpectedAllocMap.put(tag1, (short) 1);
        Map<Tag, Short> p2ExpectedAllocMap = Maps.newHashMap();
        p2ExpectedAllocMap.put(Tag.DEFAULT_BACKEND_TAG, (short) 1);
        for (Partition partition : tbl5.getPartitions()) {
            ReplicaAllocation replicaAllocation = tbl5.getPartitionInfo().getReplicaAllocation(partition.getId());
            Map<Tag, Short> allocMap = replicaAllocation.getAllocMap();
            if (partition.getName().equals("p1")) {
                Assertions.assertEquals(p1ExpectedAllocMap, allocMap);
            } else {
                Assertions.assertEquals(p2ExpectedAllocMap, allocMap);
            }
        }
    }

    private void checkTableReplicaAllocation(OlapTable tbl) throws InterruptedException {
        int maxLoop = 300;
        while (maxLoop-- > 0) {
            try {
                tbl.checkReplicaAllocation();
                break;
            } catch (UserException e) {
                System.out.println(e.getMessage());
            }
            Thread.sleep(1000);
            System.out.println("wait table " + tbl.getId() + " to be stable");
        }
        ExceptionChecker.expectThrowsNoException(() -> tbl.checkReplicaAllocation());
        System.out.println("table " + tbl.getId() + " is stable");
    }
}
