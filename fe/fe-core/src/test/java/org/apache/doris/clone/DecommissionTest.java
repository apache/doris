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

import org.apache.doris.analysis.AlterSystemStmt;
import org.apache.doris.analysis.CreateDbStmt;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.common.Config;
import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.common.FeConstants;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TDisk;
import org.apache.doris.thrift.TStorageMedium;
import org.apache.doris.utframe.UtFrameUtils;

import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;

public class DecommissionTest {
    private static final Logger LOG = LogManager.getLogger(TabletReplicaTooSlowTest.class);
    // use a unique dir so that it won't be conflict with other unit test which
    // may also start a Mocked Frontend
    private static String runningDirBase = "fe";
    private static String runningDir = runningDirBase + "/mocked/DecommissionTest/" + UUID.randomUUID() + "/";
    private static ConnectContext connectContext;

    private static Random random = new Random(System.currentTimeMillis());

    private long id = 10086;

    private final SystemInfoService systemInfoService = new SystemInfoService();
    private final TabletInvertedIndex invertedIndex = new TabletInvertedIndex();

    @BeforeClass
    public static void beforeClass() throws Exception {
        FeConstants.runningUnitTest = true;
        System.out.println(runningDir);
        FeConstants.runningUnitTest = true;
        Config.tablet_schedule_interval_ms = 2000;
        Config.tablet_checker_interval_ms = 200;
        Config.tablet_repair_delay_factor_second = 1;
        Config.enable_round_robin_create_tablet = true;
        Config.schedule_slot_num_per_hdd_path = 10000;
        Config.max_scheduling_tablets = 10000;
        Config.schedule_batch_size = 10000;
        Config.disable_balance = true;
        // 4 backends:
        // 127.0.0.1
        // 127.0.0.2
        // 127.0.0.3
        // 127.0.0.4
        UtFrameUtils.createDorisClusterWithMultiTag(runningDir, 4);
        List<Backend> backends = Env.getCurrentSystemInfo().getAllBackendsByAllCluster().values().asList();
        for (Backend be : backends) {
            Map<String, TDisk> backendDisks = Maps.newHashMap();
            TDisk tDisk1 = new TDisk();
            tDisk1.setRootPath("/home/doris1.HDD");
            tDisk1.setDiskTotalCapacity(10L << 30);
            tDisk1.setDataUsedCapacity(1);
            tDisk1.setUsed(true);
            tDisk1.setDiskAvailableCapacity(tDisk1.disk_total_capacity - tDisk1.data_used_capacity);
            tDisk1.setPathHash(random.nextLong());
            tDisk1.setStorageMedium(TStorageMedium.HDD);
            backendDisks.put(tDisk1.getRootPath(), tDisk1);

            TDisk tDisk2 = new TDisk();
            tDisk2.setRootPath("/home/doris2.HHD");
            tDisk2.setDiskTotalCapacity(10L << 30);
            tDisk2.setDataUsedCapacity(1);
            tDisk2.setUsed(true);
            tDisk2.setDiskAvailableCapacity(tDisk2.disk_total_capacity - tDisk2.data_used_capacity);
            tDisk2.setPathHash(random.nextLong());
            tDisk2.setStorageMedium(TStorageMedium.HDD);
            backendDisks.put(tDisk2.getRootPath(), tDisk2);

            be.updateDisks(backendDisks);
        }

        connectContext = UtFrameUtils.createDefaultCtx();

        // create database
        String createDbStmtStr = "create database test;";
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt(createDbStmtStr, connectContext);
        Env.getCurrentEnv().createDb(createDbStmt);
    }

    @AfterClass
    public static void tearDown() {
        //UtFrameUtils.cleanDorisFeDir(runningDirBase);
    }

    private static void createTable(String sql) throws Exception {
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        Env.getCurrentEnv().createTable(createTableStmt);
        RebalancerTestUtil.updateReplicaPathHash();
    }

    @Test
    public void testDecommissionBackend() throws Exception {
        // test colocate tablet repair
        String createStr = "create table test.tbl1\n"
                + "(k1 date, k2 int)\n"
                + "distributed by hash(k2) buckets 2400\n"
                + "properties\n"
                + "(\n"
                + "    \"replication_num\" = \"1\"\n"
                + ")";
        ExceptionChecker.expectThrowsNoException(() -> createTable(createStr));
        int totalReplicaNum = 1 * 2400;
        checkBalance(1, totalReplicaNum, 4);

        Backend backend = Env.getCurrentSystemInfo().getAllBackendsByAllCluster().values().asList().get(0);
        String decommissionStmtStr = "alter system decommission backend \"" + backend.getHost()
                + ":" + backend.getHeartbeatPort() + "\"";
        AlterSystemStmt decommissionStmt =
                (AlterSystemStmt) UtFrameUtils.parseAndAnalyzeStmt(decommissionStmtStr, connectContext);
        Env.getCurrentEnv().getAlterInstance().processAlterCluster(decommissionStmt);

        Assert.assertEquals(true, backend.isDecommissioned());

        checkBalance(200, totalReplicaNum, 3);
    }

    void checkBalance(int tryTimes, int totalReplicaNum, int backendNum) throws Exception {
        for (int i = 0; i < tryTimes; i++) {
            List<Long> backendIds = Env.getCurrentSystemInfo().getAllBackendIds(true);
            if (backendNum == backendIds.size()) {
                break;
            }

            Thread.sleep(1000);
        }

        List<Long> backendIds = Env.getCurrentSystemInfo().getAllBackendIds(true);
        Assert.assertEquals(backendNum, backendIds.size());
        List<Integer> tabletNums = backendIds.stream()
                .map(beId -> Env.getCurrentInvertedIndex().getTabletNumByBackendId(beId))
                .collect(Collectors.toList());

        int avgReplicaNum = totalReplicaNum / backendNum;
        boolean balanced = tabletNums.stream().allMatch(num -> Math.abs(num - avgReplicaNum) <= 30);
        Assert.assertTrue("not balance, tablet nums = " + tabletNums, balanced);
    }
}
