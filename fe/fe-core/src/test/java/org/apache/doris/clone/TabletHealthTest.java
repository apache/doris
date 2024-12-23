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
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.Tablet.TabletHealth;
import org.apache.doris.catalog.Tablet.TabletStatus;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.DebugPointUtil;
import org.apache.doris.resource.Tag;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.MinMaxPriorityQueue;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class TabletHealthTest extends TestWithFeService {

    private Database db;

    @Override
    protected void beforeCreatingConnectContext() throws Exception {
        Config.enable_debug_points = true;
        Config.disable_balance = true;
        Config.disable_colocate_balance_between_groups = true;
        Config.drop_backend_after_decommission = false;
        Config.colocate_group_relocate_delay_second = -1000; // be dead will imm relocate
        Config.tablet_schedule_interval_ms = 7200_000L;  //disable schedule
        Config.tablet_checker_interval_ms = 7200_000L;  //disable checker
    }

    @Override
    protected int backendNum() {
        return 3;
    }

    @Override
    protected void runBeforeAll() throws Exception {
        Thread.sleep(1000);
        createDatabase("test");
        useDatabase("test");
        db = Env.getCurrentInternalCatalog().getDbOrMetaException("test");
    }

    @Override
    protected void runBeforeEach() throws Exception {
        // set back to default value
        Config.max_scheduling_tablets = 2000;
        for (Table table : db.getTables()) {
            dropTable(table.getName(), true);
        }
        for (Backend be : Env.getCurrentSystemInfo().getBackendsByTag(Tag.DEFAULT_BACKEND_TAG)) {
            be.setDecommissioned(false);
        }
        Env.getCurrentEnv().getTabletScheduler().clear();
        DebugPointUtil.clearDebugPoints();
        Assertions.assertTrue(checkBEHeartbeat(Env.getCurrentSystemInfo().getBackendsByTag(Tag.DEFAULT_BACKEND_TAG)));
    }

    private void shutdownBackends(List<Long> backendIds) throws Exception {
        if (backendIds.isEmpty()) {
            return;
        }
        Map<String, String> params = Maps.newHashMap();
        params.put("deadBeIds", Joiner.on(",").join(backendIds));
        DebugPointUtil.addDebugPointWithParams("HeartbeatMgr.BackendHeartbeatHandler", params);
        List<Backend> backends = backendIds.stream().map(beId -> Env.getCurrentSystemInfo().getBackend(beId))
                .collect(Collectors.toList());
        Assertions.assertTrue(checkBELostHeartbeat(backends));
    }

    private void doRepair() throws Exception {
        RebalancerTestUtil.updateReplicaPathHash();
        for (int i = 0; i < 10; i++) {
            Env.getCurrentEnv().getTabletChecker().runAfterCatalogReady();
            ColocateTableCheckerAndBalancer.getInstance().runAfterCatalogReady();
            if (Env.getCurrentEnv().getTabletScheduler().getPendingNum() == 0) {
                break;
            }

            Env.getCurrentEnv().getTabletScheduler().runAfterCatalogReady();
            Thread.sleep(500);
        }
    }

    private void checkTabletStatus(Tablet tablet, TabletStatus status,
            OlapTable table, Partition partition) throws Exception {
        SystemInfoService infoService = Env.getCurrentSystemInfo();
        ColocateTableIndex colocateTableIndex = Env.getCurrentColocateIndex();
        TabletHealth health;
        if (colocateTableIndex.isColocateTable(table.getId())) {
            GroupId groupId = colocateTableIndex.getGroup(table.getId());
            ReplicaAllocation replicaAlloc = colocateTableIndex.getGroupSchema(groupId).getReplicaAlloc();
            Set<Long> colocateBackends = colocateTableIndex.getTabletBackendsByGroup(groupId, 0);
            health = tablet.getColocateHealth(partition.getVisibleVersion(), replicaAlloc, colocateBackends);
        } else {
            ReplicaAllocation replicaAlloc = table.getPartitionInfo().getReplicaAllocation(partition.getId());
            health = tablet.getHealth(infoService, partition.getVisibleVersion(),
                    replicaAlloc, infoService.getAllBackendIds(true));
        }
        Assertions.assertEquals(status, health.status);
    }

    private void checkTabletIsHealth(Tablet tablet, OlapTable table, Partition partition) throws Exception {
        checkTabletStatus(tablet, TabletStatus.HEALTHY, table, partition);
        ReplicaAllocation replicaAlloc = table.getPartitionInfo().getReplicaAllocation(partition.getId());
        Assertions.assertEquals((int) replicaAlloc.getTotalReplicaNum(), tablet.getReplicas().size());
        for (Replica replica : tablet.getReplicas()) {
            Assertions.assertEquals(partition.getVisibleVersion(), replica.getVersion());
            Assertions.assertEquals(-1L, replica.getLastFailedVersion());
            Assertions.assertTrue(replica.isScheduleAvailable());
            Assertions.assertTrue(replica.isAlive());
        }
        ColocateTableIndex colocateTableIndex = Env.getCurrentColocateIndex();
        if (colocateTableIndex.isColocateTable(table.getId())) {
            GroupId groupId = colocateTableIndex.getGroup(table.getId());
            Assertions.assertFalse(colocateTableIndex.isGroupUnstable(groupId));
        }
    }

    @Test
    public void testTabletHealth() throws Exception {
        createTable("CREATE TABLE tbl1 (k INT) DISTRIBUTED BY HASH(k) BUCKETS 1"
                + " PROPERTIES ('replication_num' = '3')");

        OlapTable table = (OlapTable) db.getTableOrMetaException("tbl1");
        Partition partition = table.getPartitions().iterator().next();
        Tablet tablet = partition.getMaterializedIndices(IndexExtState.ALL).iterator().next()
                .getTablets().iterator().next();

        partition.updateVisibleVersion(10L);
        tablet.getReplicas().forEach(replica -> replica.updateVersion(10L));

        checkTabletIsHealth(tablet, table, partition);

        tablet.getReplicas().get(0).adminUpdateVersionInfo(8L, null, null, 0L);
        // 1 replica miss version
        checkTabletStatus(tablet, TabletStatus.VERSION_INCOMPLETE, table, partition);
        tablet.deleteReplicaByBackendId(tablet.getReplicas().get(2).getBackendId());
        Assertions.assertEquals(2, tablet.getReplicas().size());
        // 1 replica miss version, 1 replica lost
        checkTabletStatus(tablet, TabletStatus.VERSION_INCOMPLETE, table, partition);
        doRepair();
        checkTabletIsHealth(tablet, table, partition);

        tablet.getReplicas().get(0).adminUpdateVersionInfo(8L, null, null, 0L);
        tablet.getReplicas().get(1).setBad(true);
        // 1 replica miss version, 1 replica bad
        checkTabletStatus(tablet, TabletStatus.VERSION_INCOMPLETE, table, partition);
        doRepair();
        checkTabletIsHealth(tablet, table, partition);

        tablet.deleteReplicaByBackendId(tablet.getReplicas().get(2).getBackendId());
        Assertions.assertEquals(2, tablet.getReplicas().size());
        // 1 replica lost
        checkTabletStatus(tablet, TabletStatus.REPLICA_MISSING, table, partition);
        doRepair();
        checkTabletIsHealth(tablet, table, partition);

        tablet.getReplicas().get(2).setBad(true);
        // 1 replica bad
        checkTabletStatus(tablet, TabletStatus.FORCE_REDUNDANT, table, partition);
        doRepair();
        checkTabletIsHealth(tablet, table, partition);

        tablet.getReplicas().get(0).adminUpdateVersionInfo(8L, null, null, 0L);
        Assertions.assertEquals(8L, tablet.getReplicas().get(0).getVersion());
        // 1 replica miss version
        checkTabletStatus(tablet, TabletStatus.VERSION_INCOMPLETE, table, partition);

        shutdownBackends(Lists.newArrayList(tablet.getReplicas().get(2).getBackendId()));
        // 1 replica miss version, 1 replica dead
        checkTabletStatus(tablet, TabletStatus.VERSION_INCOMPLETE, table, partition);
        doRepair();
        Assertions.assertEquals(10L, tablet.getReplicas().get(0).getVersion());
        // 1 replica dead
        checkTabletStatus(tablet, TabletStatus.REPLICA_MISSING, table, partition);

        // be alive again
        DebugPointUtil.clearDebugPoints();
        Assertions.assertTrue(checkBEHeartbeat(Env.getCurrentSystemInfo().getBackendsByTag(Tag.DEFAULT_BACKEND_TAG)));

        alterTableSync("ALTER TABLE tbl1 MODIFY PARTITION(*) SET ('replication_num' = '2')");
        ReplicaAllocation replicaAlloc = table.getPartitionInfo().getReplicaAllocation(partition.getId());
        Assertions.assertEquals(2, (int) replicaAlloc.getTotalReplicaNum());

        checkTabletStatus(tablet, TabletStatus.REDUNDANT, table, partition);
        doRepair();
        checkTabletIsHealth(tablet, table, partition);

        tablet.getReplicas().get(1).setBad(true);
        // 1 replica bad
        checkTabletStatus(tablet, TabletStatus.REPLICA_MISSING, table, partition);
        doRepair();
        checkTabletIsHealth(tablet, table, partition);

        Backend decommissionBe = Env.getCurrentSystemInfo().getBackend(tablet.getReplicas().get(0).getBackendId());
        decommissionBe.setDecommissioned(true);
        // 1 replica's backend is decommission
        checkTabletStatus(tablet, TabletStatus.REPLICA_RELOCATING, table, partition);
        doRepair();
        checkTabletIsHealth(tablet, table, partition);
        decommissionBe.setDecommissioned(false);

        shutdownBackends(Lists.newArrayList(tablet.getReplicas().get(1).getBackendId()));
        // 1 replica dead
        checkTabletStatus(tablet, TabletStatus.FORCE_REDUNDANT, table, partition);
        doRepair();
        checkTabletIsHealth(tablet, table, partition);

        shutdownBackends(Lists.newArrayList(tablet.getBackendIds()));
        doRepair();
        // all replica dead
        checkTabletStatus(tablet, TabletStatus.UNRECOVERABLE, table, partition);
        Assertions.assertEquals(0, Env.getCurrentEnv().getTabletScheduler().getPendingNum());

        dropTable(table.getName(), true);
    }

    @Test
    public void testColocateTabletHealth() throws Exception {
        createTable("CREATE TABLE tbl2 (k INT) DISTRIBUTED BY HASH(k) BUCKETS 1"
                + " PROPERTIES ('replication_num' = '3', 'colocate_with' = 'foo')");

        OlapTable table = (OlapTable) db.getTableOrMetaException("tbl2");
        Partition partition = table.getPartitions().iterator().next();
        Tablet tablet = partition.getMaterializedIndices(IndexExtState.ALL).iterator().next()
                .getTablets().iterator().next();

        Assertions.assertTrue(Env.getCurrentColocateIndex().isColocateTable(table.getId()));

        partition.updateVisibleVersion(10L);
        tablet.getReplicas().forEach(replica -> replica.updateVersion(10L));

        checkTabletIsHealth(tablet, table, partition);

        tablet.getReplicas().get(0).adminUpdateVersionInfo(8L, null, null, 0L);
        // 1 replica miss version
        checkTabletStatus(tablet, TabletStatus.VERSION_INCOMPLETE, table, partition);

        tablet.deleteReplicaByBackendId(tablet.getReplicas().get(2).getBackendId());
        Assertions.assertEquals(2, tablet.getReplicas().size());
        // 1 replica miss version, 1 replica lost
        checkTabletStatus(tablet, TabletStatus.VERSION_INCOMPLETE, table, partition);
        doRepair();
        checkTabletIsHealth(tablet, table, partition);

        tablet.deleteReplicaByBackendId(tablet.getReplicas().get(2).getBackendId());
        Assertions.assertEquals(2, tablet.getReplicas().size());
        // 1 replica lost
        checkTabletStatus(tablet, TabletStatus.COLOCATE_MISMATCH, table, partition);
        doRepair();
        checkTabletIsHealth(tablet, table, partition);

        tablet.getReplicas().get(0).adminUpdateVersionInfo(8L, null, null, 0L);
        Assertions.assertEquals(8L, tablet.getReplicas().get(0).getVersion());
        // 1 replica miss version
        checkTabletStatus(tablet, TabletStatus.VERSION_INCOMPLETE, table, partition);
        tablet.getReplicas().get(2).setBad(true);
        // 1 replica miss version, 1 replica bad
        checkTabletStatus(tablet, TabletStatus.VERSION_INCOMPLETE, table, partition);
        doRepair();
        checkTabletIsHealth(tablet, table, partition);

        tablet.getReplicas().get(2).setBad(true);
        // 1 replica bad
        checkTabletStatus(tablet, TabletStatus.COLOCATE_REDUNDANT, table, partition);
        doRepair();
        checkTabletIsHealth(tablet, table, partition);

        Assertions.assertNotNull(getSqlStmtExecutor("ALTER COLOCATE GROUP foo SET ('replication_num' = '2')"));
        ReplicaAllocation replicaAlloc = table.getPartitionInfo().getReplicaAllocation(partition.getId());
        Assertions.assertEquals(2, (int) replicaAlloc.getTotalReplicaNum());

        checkTabletStatus(tablet, TabletStatus.COLOCATE_REDUNDANT, table, partition);
        doRepair();
        checkTabletIsHealth(tablet, table, partition);

        tablet.getReplicas().get(1).setBad(true);
        // 1 replica bad, first delete it, then re-add it
        checkTabletStatus(tablet, TabletStatus.COLOCATE_REDUNDANT, table, partition);
        doRepair();
        checkTabletIsHealth(tablet, table, partition);

        long deleteBeId = tablet.getReplicas().get(1).getBackendId();
        shutdownBackends(Lists.newArrayList(deleteBeId));
        ColocateTableCheckerAndBalancer.getInstance().runAfterCatalogReady(); // colocate relocate
        // 1 replica dead
        checkTabletStatus(tablet, TabletStatus.COLOCATE_MISMATCH, table, partition);
        doRepair();
        checkTabletIsHealth(tablet, table, partition);

        // be alive again
        DebugPointUtil.clearDebugPoints();
        Assertions.assertTrue(checkBEHeartbeat(Env.getCurrentSystemInfo().getBackendsByTag(Tag.DEFAULT_BACKEND_TAG)));

        // temporary delete replica 1
        tablet.deleteReplica(tablet.getReplicas().get(1));
        Assertions.assertFalse(tablet.getBackendIds().contains(deleteBeId));
        Replica replica = new Replica(1234567890L, deleteBeId, Replica.ReplicaState.NORMAL, 8L, 0);
        // add a `error` replica on other backend
        tablet.addReplica(replica);
        // colocate don't relocate because no be dead
        ColocateTableCheckerAndBalancer.getInstance().runAfterCatalogReady();
        // first repair the replica on deleteBeId, then add a new replica on the located backend,
        // then drop the replica on deleteBeId
        checkTabletStatus(tablet, TabletStatus.VERSION_INCOMPLETE, table, partition);
        doRepair();
        checkTabletIsHealth(tablet, table, partition);
        Assertions.assertFalse(tablet.getBackendIds().contains(deleteBeId));

        Backend decommissionBe = Env.getCurrentSystemInfo().getBackend(tablet.getReplicas().get(0).getBackendId());
        decommissionBe.setDecommissioned(true);
        // 1 replica's backend is decommission
        ColocateTableCheckerAndBalancer.getInstance().runAfterCatalogReady();
        checkTabletStatus(tablet, TabletStatus.COLOCATE_MISMATCH, table, partition);
        doRepair();
        checkTabletIsHealth(tablet, table, partition);
        decommissionBe.setDecommissioned(false);

        shutdownBackends(Lists.newArrayList(tablet.getBackendIds()));
        doRepair();
        // all replica dead
        checkTabletStatus(tablet, TabletStatus.UNRECOVERABLE, table, partition);
        Assertions.assertEquals(0, Env.getCurrentEnv().getTabletScheduler().getPendingNum());

        dropTable(table.getName(), true);
    }

    @Test
    public void testAddTabletNoDeadLock() throws Exception {
        Config.max_scheduling_tablets = 1;
        createTable("CREATE TABLE tbl3 (k INT) DISTRIBUTED BY HASH(k) BUCKETS 2"
                + " PROPERTIES ('replication_num' = '3')");
        DebugPointUtil.addDebugPoint("MockedBackendFactory.handleCloneTablet.failed");
        OlapTable table = (OlapTable) db.getTableOrMetaException("tbl3");
        Partition partition = table.getPartitions().iterator().next();
        List<Tablet> tablets = partition.getMaterializedIndices(IndexExtState.ALL).iterator().next().getTablets();
        Assertions.assertEquals(2, tablets.size());

        partition.updateVisibleVersion(10L);
        tablets.forEach(tablet -> tablet.getReplicas().forEach(replica -> replica.updateVersion(10)));

        Tablet tabletA = tablets.get(0);
        Tablet tabletB = tablets.get(1);
        TabletScheduler scheduler = Env.getCurrentEnv().getTabletScheduler();
        tabletA.getReplicas().get(0).adminUpdateVersionInfo(8L, null, null, 0L);
        checkTabletStatus(tabletA, TabletStatus.VERSION_INCOMPLETE, table, partition);
        Env.getCurrentEnv().getTabletChecker().runAfterCatalogReady();
        Env.getCurrentEnv().getTabletScheduler().runAfterCatalogReady();
        Thread.sleep(1000);
        MinMaxPriorityQueue<TabletSchedCtx> queue = scheduler.getPendingTabletQueue();
        TabletSchedCtx tabletACtx = queue.peekFirst();
        Assertions.assertNotNull(tabletACtx);
        tabletACtx.setLastVisitedTime(System.currentTimeMillis() + 3600 * 1000L);
        tabletB.getReplicas().get(0).adminUpdateVersionInfo(8L, null, null, 0L);
        checkTabletStatus(tabletB, TabletStatus.VERSION_INCOMPLETE, table, partition);
        Thread thread = new Thread(() -> {
            try {
                Env.getCurrentEnv().getTabletChecker().runAfterCatalogReady();
                Env.getCurrentEnv().getTabletScheduler().runAfterCatalogReady();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        thread.start();
        Thread.sleep(1000);
        Assertions.assertTrue(table.tryWriteLock(2, TimeUnit.SECONDS));
        table.writeUnlock();
        DebugPointUtil.clearDebugPoints();
        doRepair();
        Thread.sleep(1000);
        doRepair();
        checkTabletIsHealth(tabletA, table, partition);
        checkTabletIsHealth(tabletB, table, partition);
    }
}
