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
import org.apache.doris.common.Config;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.stream.Collectors;

public class PartitionRebalancerTest extends TestWithFeService {

    @Override
    protected void beforeCreatingConnectContext() throws Exception {
        Config.tablet_schedule_interval_ms = 100;
        Config.tablet_checker_interval_ms = 100;
        Config.tablet_rebalancer_type = "partition";
        Config.tablet_repair_delay_factor_second = 1;
        Config.schedule_slot_num_per_hdd_path = 10000;
        Config.schedule_slot_num_per_ssd_path = 10000;
        Config.schedule_batch_size = 10000;
        Config.max_scheduling_tablets = 10000;
        Config.max_balancing_tablets = 10000;
        Config.partition_rebalance_max_moves_num_per_selection = 5;
    }

    @Override
    protected int backendNum() {
        return 3;
    }

    @Test
    public void testBalance() throws Exception {
        createDatabase("test");
        createTable("CREATE TABLE test.tbl1 (k INT) DISTRIBUTED BY HASH(k) BUCKETS 32"
                + " PROPERTIES ('replication_num' = '1')");

        Thread.sleep(2000);
        Assertions.assertEquals(Sets.newHashSet(11, 11, 10), getBackendTabletNums());

        checkBEHeartbeat(Lists.newArrayList(createBackend("127.0.0.4", lastFeRpcPort)));
        Thread.sleep(2000);
        Assertions.assertEquals(Sets.newHashSet(8, 8, 8, 8), getBackendTabletNums());

        checkBEHeartbeat(Lists.newArrayList(createBackend("127.0.0.5", lastFeRpcPort)));
        Thread.sleep(2000);
        Assertions.assertEquals(Sets.newHashSet(7, 7, 6, 6, 6), getBackendTabletNums());
    }

    private Set<Integer> getBackendTabletNums() {
        return Env.getCurrentSystemInfo().getAllBackendIds().stream()
                .map(beId -> Env.getCurrentInvertedIndex().getTabletIdsByBackendId(beId).size())
                .collect(Collectors.toSet());
    }

}

