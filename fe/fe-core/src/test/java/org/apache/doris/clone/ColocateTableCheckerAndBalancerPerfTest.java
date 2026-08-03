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

package org.apache.doris.clone;

import org.apache.doris.catalog.ColocateTableIndex;
import org.apache.doris.catalog.ColocateTableIndex.GroupId;
import org.apache.doris.catalog.DiskInfo;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.CreateDatabaseCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateTableCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.resource.Tag;
import org.apache.doris.system.Backend;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

public class ColocateTableCheckerAndBalancerPerfTest extends TestWithFeService {

    private static final int TEMP_DISALBE_BE_NUM = 2;
    private List<Backend> backends;

    @Override
    protected int backendNum() {
        return 6;
    }

    @Override
    protected void beforeCreatingConnectContext() throws Exception {
        FeConstants.runningUnitTest = true;
        FeConstants.enableInternalSchemaDb = false;
        Config.tablet_checker_interval_ms = 100;
        Config.tablet_schedule_interval_ms = 100;
        Config.enable_round_robin_create_tablet = false;
        Config.disable_balance = true;
        Config.schedule_batch_size = 500;
        Config.schedule_slot_num_per_hdd_path = 1000;
        Config.disable_colocate_balance = true;
        Config.disable_tablet_scheduler = true;
    }

    @Override
    protected void runBeforeAll() throws Exception {
        backends = Env.getCurrentSystemInfo().getAllBackendsByAllCluster().values().asList();
        for (Backend be : backends) {
            for (DiskInfo diskInfo : be.getDisks().values()) {
                diskInfo.setTotalCapacityB(10L << 40);
                diskInfo.setDataUsedCapacityB(1L);
                diskInfo.setAvailableCapacityB(
                        diskInfo.getTotalCapacityB() - diskInfo.getDataUsedCapacityB());
            }
        }
        Map<String, String> tagMap = Maps.newHashMap();
        tagMap.put(Tag.TYPE_LOCATION, "zone_a");
        for (int i = 0; i < TEMP_DISALBE_BE_NUM; i++) {
            backends.get(i).setTagMap(tagMap);
        }
    }

    @Test
    public void testRelocateAndBalance() throws Exception {

        Env env = Env.getCurrentEnv();
        String createDbStmtStr = "create database test;";
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan logicalPlan = nereidsParser.parseSingle(createDbStmtStr);
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, createDbStmtStr);
        if (logicalPlan instanceof CreateDatabaseCommand) {
            ((CreateDatabaseCommand) logicalPlan).run(connectContext, stmtExecutor);
        }

        Random random = new Random();
        final int groupNum = 100;
        for (int groupIndex = 0; groupIndex <= groupNum; groupIndex++) {
            int tableNum = 1 + random.nextInt(10);
            for (int tableIndex = 0; tableIndex < tableNum; tableIndex++) {
                String sql = String.format("CREATE TABLE test.table_%s_%s\n"
                        + "( k1 int, k2 int, v1 int )\n"
                        + "ENGINE=OLAP\n"
                        + "UNIQUE KEY (k1,k2)\n"
                        + "DISTRIBUTED BY HASH(k2) BUCKETS 11\n"
                        + "PROPERTIES('colocate_with' = 'group_%s');",
                        groupIndex, tableIndex, groupIndex);

                nereidsParser = new NereidsParser();
                LogicalPlan parsed = nereidsParser.parseSingle(sql);
                stmtExecutor = new StmtExecutor(connectContext, sql);
                if (parsed instanceof CreateTableCommand) {
                    ((CreateTableCommand) parsed).run(connectContext, stmtExecutor);
                }

                BalanceStatistic beforeBalanceStatistic = BalanceStatistic.getCurrentBalanceStatistic();
                Assertions.assertEquals(0, beforeBalanceStatistic.getBeMinTotalReplicaNum(),
                        "group: " + groupIndex + ", table: " + tableIndex + ", "
                        + beforeBalanceStatistic.getBackendTotalReplicaNum());
            }
        }

        ColocateTableIndex colocateIndex = env.getColocateTableIndex();
        Set<GroupId> groupIds = colocateIndex.getAllGroupIds();

        RebalancerTestUtil.updateReplicaDataSize(1L << 10, 10, 10);
        RebalancerTestUtil.updateReplicaPathHash();

        BalanceStatistic beforeBalanceStatistic = BalanceStatistic.getCurrentBalanceStatistic();
        Assertions.assertEquals(0, beforeBalanceStatistic.getBeMinTotalReplicaNum(),
                "" + beforeBalanceStatistic.getBackendTotalReplicaNum());

        // all groups stable
        Thread.sleep(1000);
        Assertions.assertTrue(groupIds.stream().noneMatch(groupId -> colocateIndex.isGroupUnstable(groupId)),
                "some groups are unstable");

        // after enable colocate balance and some backends return,  it should relocate all groups.
        // and they will be unstable
        Map<String, String> tagMap = backends.get(TEMP_DISALBE_BE_NUM).getTagMap();
        for (int i = 0; i < TEMP_DISALBE_BE_NUM; i++) {
            backends.get(i).setTagMap(tagMap);
        }
        Config.disable_colocate_balance = false;
        for (int i = 0; i < 10; i++) {
            Thread.sleep(1000);
            if (groupIds.stream().allMatch(groupId -> colocateIndex.isGroupUnstable(groupId))) {
                break;
            }
        }
        Assertions.assertTrue(groupIds.stream().allMatch(groupId -> colocateIndex.isGroupUnstable(groupId)),
                "some groups are stable");


        // after enable scheduler, the unstable groups should shed their tablets and change to stable
        Config.disable_tablet_scheduler = false;
        for (int i = 0; true; i++) {
            Thread.sleep(1000);

            boolean allStable = groupIds.stream().noneMatch(
                    groupId -> colocateIndex.isGroupUnstable(groupId));

            if (allStable) {
                break;
            }

            Assertions.assertTrue(i < 60, "some groups are unstable");
        }

        System.out.println("=== before colocate relocate and balance:");
        beforeBalanceStatistic.printToStdout();
        Assertions.assertEquals(0, beforeBalanceStatistic.getBeMinTotalReplicaNum(),
                "" + beforeBalanceStatistic.getBackendTotalReplicaNum());
        Assertions.assertEquals(0, beforeBalanceStatistic.getBeMinTotalDataSize(),
                "" + beforeBalanceStatistic.getBackendTotalDataSize());
        long beforeDataSizeDiff = beforeBalanceStatistic.getBeMaxTotalDataSize()
                - beforeBalanceStatistic.getBeMinTotalDataSize();
        int beforeReplicaNumDiff = beforeBalanceStatistic.getBeMaxTotalReplicaNum()
                - beforeBalanceStatistic.getBeMinTotalReplicaNum();

        BalanceStatistic afterBalanceStatistic = BalanceStatistic.getCurrentBalanceStatistic();
        System.out.println("");
        System.out.println("=== after colocate relocate and balance:");
        afterBalanceStatistic.printToStdout();

        Assertions.assertTrue(afterBalanceStatistic.getBeMinTotalReplicaNum() > 0,
                "" + afterBalanceStatistic.getBackendTotalReplicaNum());
        Assertions.assertTrue(afterBalanceStatistic.getBeMinTotalDataSize() > 0,
                "" + afterBalanceStatistic.getBackendTotalDataSize());
        long afterDataSizeDiff = afterBalanceStatistic.getBeMaxTotalDataSize()
                - afterBalanceStatistic.getBeMinTotalDataSize();
        int afterReplicaNumDiff = afterBalanceStatistic.getBeMaxTotalReplicaNum()
                - afterBalanceStatistic.getBeMinTotalReplicaNum();
        Assertions.assertTrue(afterDataSizeDiff <= beforeDataSizeDiff);
        Assertions.assertTrue(afterReplicaNumDiff <= beforeReplicaNumDiff);
    }
}
