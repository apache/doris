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
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.common.Config;
import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.UserException;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.resource.Tag;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TDisk;
import org.apache.doris.thrift.TStorageMedium;
import org.apache.doris.utframe.UtFrameUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;

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

import mockit.Mocked;

public class TabletReplicaTooSlowTest {
    private static final Logger LOG = LogManager.getLogger(TabletReplicaTooSlowTest.class);
    // use a unique dir so that it won't be conflict with other unit test which
    // may also start a Mocked Frontend
    private static String runningDirBase = "fe";
    private static String runningDir = runningDirBase + "/mocked/TabletReplicaTooSlowTest/" + UUID.randomUUID() + "/";
    private static ConnectContext connectContext;

    private static Random random = new Random(System.currentTimeMillis());

    private static List<Backend> backends = Lists.newArrayList();

    private long id = 10086;

    private final SystemInfoService systemInfoService = new SystemInfoService();
    private final TabletInvertedIndex invertedIndex = new TabletInvertedIndex();
    private Table<String, Tag, ClusterLoadStatistic> statisticMap;

    @BeforeClass
    public static void beforeClass() throws Exception {
        System.out.println(runningDir);
        FeConstants.runningUnitTest = true;
        FeConstants.tablet_checker_interval_ms = 1000;
        Config.tablet_repair_delay_factor_second = 1;
        // 5 backends:
        // 127.0.0.1
        // 127.0.0.2
        // 127.0.0.3
        // 127.0.0.4
        // 127.0.0.5
        UtFrameUtils.createDorisClusterWithMultiTag(runningDir, 5);
        connectContext = UtFrameUtils.createDefaultCtx();

        // create database
        String createDbStmtStr = "create database test;";
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt(createDbStmtStr, connectContext);
        Catalog.getCurrentCatalog().createDb(createDbStmt);

        // must set disk info, or the tablet scheduler won't work
        backends = Catalog.getCurrentSystemInfo().getClusterBackends(SystemInfoService.DEFAULT_CLUSTER);
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
    public static void TearDown() {
        UtFrameUtils.cleanDorisFeDir(runningDirBase);
    }

    private static void createTable(String sql) throws Exception {
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        Catalog.getCurrentCatalog().createTable(createTableStmt);
        updateReplicaVersionCount();
    }

    private static void updateReplicaVersionCount() {
        Table<Long, Long, Replica> replicaMetaTable = Catalog.getCurrentInvertedIndex().getReplicaMetaTable();
        int versionCount = 1;
        for (Table.Cell<Long, Long, Replica> cell : replicaMetaTable.cellSet()) {
            long beId = cell.getColumnKey();
            Backend be = Catalog.getCurrentSystemInfo().getBackend(beId);
            if (be == null) {
                continue;
            }
            Replica replica = cell.getValue();
            replica.setVersionCount(versionCount);
            versionCount = versionCount + 200;
        }
    }

    @Test
    public void test() throws Exception {
        // test colocate tablet repair
        String createStr = "create table test.tbl1\n" +
                "(k1 date, k2 int)\n" +
                "distributed by hash(k2) buckets 1\n" +
                "properties\n" +
                "(\n" +
                "    \"replication_num\" = \"3\"\n" +
                ")";
        ExceptionChecker.expectThrowsNoException(() -> createTable(createStr));
        int maxLoop = 300;
        boolean deleted = true;
        while (maxLoop-- > 0) {
            Table<Long, Long, Replica> replicaMetaTable = Catalog.getCurrentInvertedIndex().getReplicaMetaTable();

            for (Table.Cell<Long, Long, Replica> cell : replicaMetaTable.cellSet()) {
                Replica replica = cell.getValue();
                if (replica.getVersionCount() == 401) {
                    LOG.info("slow replica is not deleted.");
                    deleted = replica.isBad();
                    break;
                }
            }
            if (deleted) {
                break;
            }
            Thread.sleep(1000);
        }
        Assert.assertTrue(deleted);
    }
}
