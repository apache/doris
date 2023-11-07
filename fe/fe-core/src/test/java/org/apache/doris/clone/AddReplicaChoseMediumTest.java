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

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.common.Config;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TStorageMedium;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

public class AddReplicaChoseMediumTest extends TestWithFeService {

    @Override
    protected void beforeCreatingConnectContext() throws Exception {
        Config.enable_round_robin_create_tablet = true;
        Config.allow_replica_on_same_host = true;
        Config.tablet_checker_interval_ms = 100;
        Config.tablet_schedule_interval_ms = 100;
        Config.schedule_slot_num_per_hdd_path = 1;
    }

    @Override
    protected int backendNum() {
        return 4;
    }

    @Test
    public void testAddReplicaChoseMedium() throws Exception {
        TabletInvertedIndex invertedIndex = Env.getCurrentInvertedIndex();
        List<Backend> backends = Env.getCurrentSystemInfo().getAllBackends();
        Assertions.assertEquals(backendNum(), backends.size());
        for (Backend be : backends) {
            Assertions.assertEquals(0, invertedIndex.getTabletNumByBackendId(be.getId()));
        }

        Backend beWithSsd = backends.get(3);
        beWithSsd.getDisks().values().forEach(it -> {
            it.setStorageMedium(TStorageMedium.SSD);
            it.setTotalCapacityB(it.getTotalCapacityB() * 2);
        });

        createDatabase("test");
        createTable("CREATE TABLE test.tbl1 (k INT) DISTRIBUTED BY HASH(k) "
                + " BUCKETS 12 PROPERTIES ( \"replication_num\" = \"2\","
                + " \"storage_medium\" = \"HDD\")");
        RebalancerTestUtil.updateReplicaPathHash();

        Assertions.assertEquals(0, invertedIndex.getTabletNumByBackendId(beWithSsd.getId()));
        for (Backend be : backends) {
            if (be.getId() != beWithSsd.getId()) {
                Assertions.assertEquals(8, invertedIndex.getTabletNumByBackendId(be.getId()));
            }
        }

        Backend decommissionBe = backends.get(0);
        String decommissionStmtStr = "alter system decommission backend \"" + decommissionBe.getHost()
                + ":" + decommissionBe.getHeartbeatPort() + "\"";
        Assertions.assertNotNull(getSqlStmtExecutor(decommissionStmtStr));
        Assertions.assertTrue(decommissionBe.isDecommissioned());

        List<Integer> gotTabletNums = null;
        List<Integer> expectTabletNums = Lists.newArrayList(0, 12, 12, 0);
        for (int i = 0; i < 10; i++) {
            gotTabletNums = backends.stream().map(it -> invertedIndex.getTabletNumByBackendId(it.getId()))
                    .collect(Collectors.toList());
            if (expectTabletNums.equals(gotTabletNums)) {
                break;
            }
            Thread.sleep(1000);
        }

        Assertions.assertEquals(expectTabletNums, gotTabletNums);
    }
}
