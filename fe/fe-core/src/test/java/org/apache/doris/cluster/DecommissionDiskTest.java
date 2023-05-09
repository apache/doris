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

package org.apache.doris.cluster;

import org.apache.doris.analysis.AlterSystemStmt;
import org.apache.doris.catalog.DiskInfo;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TDisk;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DecommissionDiskTest extends TestWithFeService {

    @Override
    protected int backendNum() {
        return 3;
    }

    @Override
    protected void beforeCluster() {
        FeConstants.runningUnitTest = true;
    }

    @BeforeAll
    public void beforeClass() {
        FeConstants.runningUnitTest = true;
    }

    @Override
    protected void beforeCreatingConnectContext() throws Exception {
        FeConstants.default_scheduler_interval_millisecond = 1000;
        FeConstants.tablet_checker_interval_ms = 1000;
        Config.tablet_repair_delay_factor_second = 1;
        Config.allow_replica_on_same_host = true;
    }

    @Test
    public void testDecommissionDisk() throws Exception {
        // 1. create connect context
        connectContext = createDefaultCtx();

        ImmutableMap<Long, Backend> idToBackendRef = Env.getCurrentSystemInfo().getIdToBackend();
        Assertions.assertEquals(backendNum(), idToBackendRef.size());

        // 2. create database db1
        createDatabase("db1");
        System.out.println(Env.getCurrentInternalCatalog().getDbNames());

        // 3. create table tbl1
        createTable("create table db1.tbl1(k1 int) distributed by hash(k1) buckets 3 properties('replication_num' = '1');");

        // 4. query tablet num
        int tabletNum = Env.getCurrentInvertedIndex().getTabletMetaMap().size();

        // 5. execute decommission
        Backend srcBackend = null;
        for (Backend backend : idToBackendRef.values()) {
            if (Env.getCurrentInvertedIndex().getTabletIdsByBackendId(backend.getId()).size() > 0) {
                srcBackend = backend;
                break;
            }
        }

        List<DiskInfo> disks = srcBackend.getDisks().values().asList();
        Assertions.assertEquals(1, disks.size());
        DiskInfo disk = disks.get(0);

        Assertions.assertTrue(srcBackend != null);
        String decommissionStmtStr = "alter system decommission disk \"" + disk.getRootPath()
                + "\" on \"127.0.0.1:" + srcBackend.getBePort() + "\"";
        AlterSystemStmt decommissionStmt = (AlterSystemStmt) parseAndAnalyzeStmt(decommissionStmtStr);
        Env.getCurrentEnv().getAlterInstance().processAlterCluster(decommissionStmt);

        // 6. be heartbeat to fe
        Map<String, TDisk> diskInfos = new HashMap<>();
        TDisk disk1 = new TDisk(disk.getRootPath(), disk.getTotalCapacityB(), disk.getDiskUsedCapacityB(), true, true);
        diskInfos.put(disk1.getRootPath(), disk1);
        srcBackend.updateDisks(diskInfos);

        // 7. clone replica from decommission disk to others, and delete decommissioned replica;
        long startTimestamp = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTimestamp < 90000
                && 0 < Env.getCurrentInvertedIndex().getTabletIdNumByBackendIdAndPathHash(
                        srcBackend.getId(), disk.getPathHash())) {
            Thread.sleep(1000);
        }
        Assertions.assertEquals(0, Env.getCurrentInvertedIndex().getTabletIdNumByBackendIdAndPathHash(
                srcBackend.getId(), disk.getPathHash()));
        Assertions.assertEquals(tabletNum,
                Env.getCurrentInvertedIndex().getTabletMetaMap().size());
    }
}
