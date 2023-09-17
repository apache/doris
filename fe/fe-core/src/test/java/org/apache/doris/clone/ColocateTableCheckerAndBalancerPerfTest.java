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

import org.apache.doris.analysis.CreateDbStmt;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.catalog.ColocateTableIndex;
import org.apache.doris.catalog.ColocateTableIndex.GroupId;
import org.apache.doris.catalog.DiskInfo;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.DdlExecutor;
import org.apache.doris.resource.Tag;
import org.apache.doris.system.Backend;
import org.apache.doris.utframe.UtFrameUtils;

import com.google.common.collect.Maps;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

public class ColocateTableCheckerAndBalancerPerfTest {
    private static String runningDir = "fe/mocked/ColocateTableCheckerAndBalancerPerfTest/"
            + UUID.randomUUID().toString() + "/";

    private static ConnectContext connectContext;
    private static final int TEMP_DISALBE_BE_NUM = 2;
    private static List<Backend> backends;

    @BeforeClass
    public static void beforeClass() throws Exception {
        FeConstants.runningUnitTest = true;
        FeConstants.enableInternalSchemaDb = false;
        FeConstants.tablet_checker_interval_ms = 100;
        FeConstants.tablet_schedule_interval_ms = 100;
        Config.enable_round_robin_create_tablet = false;
        Config.disable_balance = true;
        Config.schedule_batch_size = 400;
        Config.schedule_slot_num_per_hdd_path = 1000;
        Config.disable_colocate_balance = true;
        Config.disable_tablet_scheduler = true;
        UtFrameUtils.createDorisClusterWithMultiTag(runningDir, 6);

        backends = Env.getCurrentSystemInfo().getIdToBackend().values().asList();
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

        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
    }

    @AfterClass
    public static void tearDown() {
        UtFrameUtils.cleanDorisFeDir(runningDir);
    }

    @Test
    public void testRelocateAndBalance() throws Exception {

        Env env = Env.getCurrentEnv();
        String createDbStmtStr = "create database test;";
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt(createDbStmtStr, connectContext);
        DdlExecutor.execute(env, createDbStmt);

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
                CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
                try {
                    DdlExecutor.execute(env, createTableStmt);
                } catch (Exception e) {
                    e.printStackTrace();
                    throw e;
                }

                BalanceStatistic beforeBalanceStatistic = BalanceStatistic.getCurrentBalanceStatistic();
                Assert.assertEquals("group: " + groupIndex + ", table: " + tableIndex + ", "
                        + beforeBalanceStatistic.getBackendTotalReplicaNum(),
                        0, beforeBalanceStatistic.getBeMinTotalReplicaNum());
            }
        }

        ColocateTableIndex colocateIndex = env.getColocateTableIndex();
        Set<GroupId> groupIds = colocateIndex.getAllGroupIds();

        RebalancerTestUtil.updateReplicaDataSize(100L << 10, 10, 10);
        RebalancerTestUtil.updateReplicaPathHash();

        BalanceStatistic beforeBalanceStatistic = BalanceStatistic.getCurrentBalanceStatistic();
        Assert.assertEquals("" + beforeBalanceStatistic.getBackendTotalReplicaNum(),
                0, beforeBalanceStatistic.getBeMinTotalReplicaNum());

        // all groups stable
        Thread.sleep(1000);
        Assert.assertTrue("some groups are unstable",
                groupIds.stream().noneMatch(groupId -> colocateIndex.isGroupUnstable(groupId)));

        // after enable colocate balance and some backends return,  it should relocate all groups.
        // and they will be unstable
        Map<String, String> tagMap = backends.get(TEMP_DISALBE_BE_NUM).getTagMap();
        for (int i = 0; i < TEMP_DISALBE_BE_NUM; i++) {
            backends.get(i).setTagMap(tagMap);
        }
        Config.disable_colocate_balance = false;
        Thread.sleep(1000);
        Assert.assertTrue("some groups are stable",
                groupIds.stream().allMatch(groupId -> colocateIndex.isGroupUnstable(groupId)));


        // after enable scheduler, the unstable groups should shed their tablets and change to stable
        Config.disable_tablet_scheduler = false;
        for (int i = 0; true; i++) {
            Thread.sleep(1000);

            boolean allStable = groupIds.stream().noneMatch(
                    groupId -> colocateIndex.isGroupUnstable(groupId));

            if (allStable) {
                break;
            }

            Assert.assertTrue("some groups are unstable", i < 60);
        }

        System.out.println("=== before colocate relocate and balance:");
        beforeBalanceStatistic.printToStdout();
        Assert.assertEquals("" + beforeBalanceStatistic.getBackendTotalReplicaNum(),
                0, beforeBalanceStatistic.getBeMinTotalReplicaNum());
        Assert.assertEquals("" + beforeBalanceStatistic.getBackendTotalDataSize(),
                0, beforeBalanceStatistic.getBeMinTotalDataSize());
        long beforeDataSizeDiff = beforeBalanceStatistic.getBeMaxTotalDataSize()
                - beforeBalanceStatistic.getBeMinTotalDataSize();
        int beforeReplicaNumDiff = beforeBalanceStatistic.getBeMaxTotalReplicaNum()
                - beforeBalanceStatistic.getBeMinTotalReplicaNum();

        BalanceStatistic afterBalanceStatistic = BalanceStatistic.getCurrentBalanceStatistic();
        System.out.println("");
        System.out.println("=== after colocate relocate and balance:");
        afterBalanceStatistic.printToStdout();

        Assert.assertTrue("" + afterBalanceStatistic.getBackendTotalReplicaNum(),
                afterBalanceStatistic.getBeMinTotalReplicaNum() > 0);
        Assert.assertTrue("" + afterBalanceStatistic.getBackendTotalDataSize(),
                afterBalanceStatistic.getBeMinTotalDataSize() > 0);
        long afterDataSizeDiff = afterBalanceStatistic.getBeMaxTotalDataSize()
                - afterBalanceStatistic.getBeMinTotalDataSize();
        int afterReplicaNumDiff = afterBalanceStatistic.getBeMaxTotalReplicaNum()
                - afterBalanceStatistic.getBeMinTotalReplicaNum();
        Assert.assertTrue(afterDataSizeDiff <= beforeDataSizeDiff);
        Assert.assertTrue(afterReplicaNumDiff <= beforeReplicaNumDiff);
    }
}
