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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class DecommissionTest {
    private static final Logger LOG = LogManager.getLogger(TabletReplicaTooSlowTest.class);
    // use a unique dir so that it won't be conflict with other unit test which
    // may also start a Mocked Frontend
    private static String runningDirBase = "fe";
    private static String runningDir = runningDirBase + "/mocked/DecommissionTest/" + UUID.randomUUID() + "/";
    private static ConnectContext connectContext;

    private static Random random = new Random(System.currentTimeMillis());

    private static List<Backend> backends = Lists.newArrayList();

    private long id = 10086;

    private final SystemInfoService systemInfoService = new SystemInfoService();
    private final TabletInvertedIndex invertedIndex = new TabletInvertedIndex();

    @BeforeClass
    public static void beforeClass() throws Exception {
        FeConstants.runningUnitTest = true;
        System.out.println(runningDir);
        FeConstants.runningUnitTest = true;
        FeConstants.tablet_checker_interval_ms = 1000;
        Config.tablet_repair_delay_factor_second = 1;
        Config.enable_round_robin_create_tablet = true;
        Config.schedule_slot_num_per_path = 10000;
        Config.schedule_decommission_slot_num_per_path = 10000;
        Config.disable_balance = true;
        // 4 backends:
        // 127.0.0.1
        // 127.0.0.2
        // 127.0.0.3
        // 127.0.0.4
        UtFrameUtils.createDorisClusterWithMultiTag(runningDir, 4);
        connectContext = UtFrameUtils.createDefaultCtx();

        // create database
        String createDbStmtStr = "create database test;";
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt(createDbStmtStr, connectContext);
        Env.getCurrentEnv().createDb(createDbStmt);

        // must set disk info, or the tablet scheduler won't work
        backends = Env.getCurrentSystemInfo().getAllBackends();
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

    @AfterClass
    public static void tearDown() {
        UtFrameUtils.cleanDorisFeDir(runningDirBase);
    }

    private static void createTable(String sql) throws Exception {
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        Env.getCurrentEnv().createTable(createTableStmt);
    }

    @Test
    public void testDecommissionBackend() throws Exception {
        // test colocate tablet repair
        String createStr = "create table test.tbl1\n"
                + "(k1 date, k2 int)\n"
                + "distributed by hash(k2) buckets 4000\n"
                + "properties\n"
                + "(\n"
                + "    \"replication_num\" = \"2\"\n"
                + ")";
        ExceptionChecker.expectThrowsNoException(() -> createTable(createStr));
        int totalReplicaNum = 2 * 4000;
        checkBalance(1, totalReplicaNum, 4);

        String decommissionStmtStr = "alter system decommission backend \"127.0.0.1:" + srcBackend.getHeartbeatPort() + "\"";
        AlterSystemStmt decommissionStmt = (AlterSystemStmt) parseAndAnalyzeStmt(decommissionStmtStr);
        Env.getCurrentEnv().getAlterInstance().processAlterCluster(decommissionStmt);

        Assertions.assertEquals(true, srcBackend.isDecommissioned());

        checkBalance(60, totalReplicaNum, 3);
    }

    void checkBalance(int tryTimes, int totalReplicaNum, int backendNum) {
        int beReplicaNumLower = totalReplicaNum/backendNum;
        int beReplicaNumUpper = beReplicaNumLower + 200; // inner database replica.
        for (int i = 0; i < tryTimes; i++) {
            List<Long> backendIds = Env.getCurrentSystemInfo().getAllBackendIds(true);
            if (backendNum != backends.size() && i != tryTimes - 1) {
                Thread.sleep(1000);
                continue;
            }

            Assert.assertEquals(backends, backends.size());
            for (long beId : backendIds) {
                int tabletNum = Env.getCurrentInvertedIndex().getTabletNumByBackendId(beId);
                Assert.assertTrue(tabletNum >= beReplicaNumLower);
                Assert.assertTrue(tabletNum <= beReplicaNumUpper);
            }
        }
    }
}
