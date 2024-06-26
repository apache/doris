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

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.util.DebugPointUtil;
import org.apache.doris.system.Backend;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class BeDownCancelCloneTest extends TestWithFeService {

    @Override
    protected int backendNum() {
        return 4;
    }

    @Override
    protected void beforeCreatingConnectContext() throws Exception {
        FeConstants.runningUnitTest = true;
        FeConstants.default_scheduler_interval_millisecond = 1000;
        Config.enable_debug_points = true;
        Config.tablet_checker_interval_ms = 100;
        Config.tablet_schedule_interval_ms = 100;
        Config.tablet_repair_delay_factor_second = 1;
        Config.allow_replica_on_same_host = true;
        Config.disable_balance = true;
        Config.schedule_batch_size = 1000;
        Config.schedule_slot_num_per_hdd_path = 1000;
        Config.heartbeat_interval_second = 5;
        Config.max_backend_heartbeat_failure_tolerance_count = 1;
        Config.min_clone_task_timeout_sec = 20 * 60 * 1000;
    }

    @Test
    public void test() throws Exception {
        connectContext = createDefaultCtx();

        createDatabase("db1");
        System.out.println(Env.getCurrentInternalCatalog().getDbNames());

        // 3. create table tbl1
        createTable("create table db1.tbl1(k1 int) distributed by hash(k1) buckets 1;");
        RebalancerTestUtil.updateReplicaPathHash();

        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException("db1");
        OlapTable tbl = (OlapTable) db.getTableOrMetaException("tbl1");
        Assertions.assertNotNull(tbl);
        Tablet tablet = tbl.getPartitions().iterator().next()
                .getMaterializedIndices(MaterializedIndex.IndexExtState.ALL).iterator().next()
                .getTablets().iterator().next();

        Assertions.assertEquals(3, tablet.getReplicas().size());
        long destBeId = Env.getCurrentSystemInfo().getAllBackendIds(true).stream()
                .filter(beId -> tablet.getReplicaByBackendId(beId) == null)
                .findFirst()
                .orElse(-1L);
        Assertions.assertTrue(destBeId != -1L);
        Backend destBe = Env.getCurrentSystemInfo().getBackend(destBeId);
        Assertions.assertNotNull(destBe);
        Assertions.assertTrue(destBe.isAlive());

        // add debug point, make clone wait
        DebugPointUtil.addDebugPoint("MockedBackendFactory.handleCloneTablet.block");

        // move replica[0] to destBeId
        Replica srcReplica = tablet.getReplicas().get(0);
        String moveTabletSql = "ADMIN SET REPLICA STATUS PROPERTIES(\"tablet_id\" = \"" + tablet.getId() + "\", "
                + "\"backend_id\" = \"" + srcReplica.getBackendId() + "\", \"status\" = \"drop\")";
        Assertions.assertNotNull(getSqlStmtExecutor(moveTabletSql));
        Assertions.assertFalse(srcReplica.isScheduleAvailable());

        Thread.sleep(3000);

        Assertions.assertEquals(0, Env.getCurrentEnv().getTabletScheduler().getHistoryTablets(100).size());
        Assertions.assertEquals(4, tablet.getReplicas().size());
        Replica destReplica = tablet.getReplicaByBackendId(destBeId);
        Assertions.assertNotNull(destReplica);
        Assertions.assertEquals(Replica.ReplicaState.CLONE, destReplica.getState());

        // clone a replica on destBe
        List<TabletSchedCtx> runningTablets = Env.getCurrentEnv().getTabletScheduler().getRunningTablets(100);
        Assertions.assertEquals(1, runningTablets.size());
        Assertions.assertEquals(destBeId, runningTablets.get(0).getDestBackendId());

        Map<String, String> params2 = Maps.newHashMap();
        params2.put("deadBeIds", String.valueOf(destBeId));
        DebugPointUtil.addDebugPointWithParams("HeartbeatMgr.BackendHeartbeatHandler", params2);

        Thread.sleep((Config.heartbeat_interval_second
                * Config.max_backend_heartbeat_failure_tolerance_count + 4) * 1000L);

        destBe = Env.getCurrentSystemInfo().getBackend(destBeId);
        Assertions.assertNotNull(destBe);
        Assertions.assertFalse(destBe.isAlive());

        // delete clone dest task
        Assertions.assertFalse(Env.getCurrentEnv().getTabletScheduler().getHistoryTablets(100).isEmpty());

        // first drop dest replica (its backend is dead) and src replica (it's mark as drop)
        // then re clone a replica to src be, and waiting for cloning.
        runningTablets = Env.getCurrentEnv().getTabletScheduler().getRunningTablets(100);
        Assertions.assertEquals(1, runningTablets.size());
        Assertions.assertEquals(srcReplica.getBackendId(), runningTablets.get(0).getDestBackendId());

        DebugPointUtil.removeDebugPoint("MockedBackendFactory.handleCloneTablet.block");
        Thread.sleep(2000);

        // destBe is dead, cancel clone task
        runningTablets = Env.getCurrentEnv().getTabletScheduler().getRunningTablets(100);
        Assertions.assertEquals(0, runningTablets.size());

        Assertions.assertEquals(3, tablet.getReplicas().size());
        for (Replica replica : tablet.getReplicas()) {
            Assertions.assertTrue(replica.getBackendId() != destBeId);
            Assertions.assertTrue(replica.isScheduleAvailable());
            Assertions.assertEquals(Replica.ReplicaState.NORMAL, replica.getState());
        }
    }
}
