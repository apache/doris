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
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.clone.TabletSchedCtx.Priority;
import org.apache.doris.clone.TabletSchedCtx.Type;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.DebugPointUtil;
import org.apache.doris.resource.Tag;
import org.apache.doris.system.Backend;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import com.google.common.collect.MinMaxPriorityQueue;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class TabletSchedCtxTest extends TestWithFeService {
    private Database db;

    @Override
    protected void beforeCreatingConnectContext() throws Exception {
        Config.enable_debug_points = true;
        Config.allow_replica_on_same_host = false;
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

    @Test
    public void testAddTablet() {
        List<TabletSchedCtx> tablets = Lists.newArrayList();
        ReplicaAllocation replicaAlloc = ReplicaAllocation.DEFAULT_ALLOCATION;
        for (long i = 0; i < 20; i++) {
            tablets.add(new TabletSchedCtx(Type.REPAIR, 1, 2, 3, 4,
                    i, replicaAlloc, i));
            tablets.add(new TabletSchedCtx(Type.BALANCE, 1, 2, 3, 4,
                    1000 + i, replicaAlloc, i));
        }
        Collections.shuffle(tablets);
        Config.max_scheduling_tablets = 5;
        TabletScheduler scheduler = Env.getCurrentEnv().getTabletScheduler();
        for (TabletSchedCtx tablet : tablets) {
            scheduler.addTablet(tablet, false);
        }

        MinMaxPriorityQueue<TabletSchedCtx> queue = scheduler.getPendingTabletQueue();
        List<TabletSchedCtx> gotTablets = Lists.newArrayList();
        while (!queue.isEmpty()) {
            gotTablets.add(queue.pollFirst());
        }
        Assert.assertEquals(Config.max_scheduling_tablets, gotTablets.size());
        for (int i = 0; i < gotTablets.size(); i++) {
            TabletSchedCtx tablet = gotTablets.get(i);
            Assert.assertEquals(Type.REPAIR, tablet.getType());
            Assert.assertEquals((long) i, tablet.getCreateTime());
        }
    }

    @Test
    public void testPriorityCompare() {
        // equal priority, but info3's last visit time is earlier than info2 and info1, so info1 should ranks ahead
        MinMaxPriorityQueue<TabletSchedCtx> pendingTablets = MinMaxPriorityQueue.create();
        ReplicaAllocation replicaAlloc = ReplicaAllocation.DEFAULT_ALLOCATION;
        TabletSchedCtx ctx1 = new TabletSchedCtx(Type.REPAIR,
                1, 2, 3, 4, 1000, replicaAlloc, System.currentTimeMillis());
        ctx1.setPriority(Priority.NORMAL);
        ctx1.setLastVisitedTime(2);

        TabletSchedCtx ctx2 = new TabletSchedCtx(Type.REPAIR,
                1, 2, 3, 4, 1001, replicaAlloc, System.currentTimeMillis());
        ctx2.setPriority(Priority.NORMAL);
        ctx2.setLastVisitedTime(3);

        TabletSchedCtx ctx3 = new TabletSchedCtx(Type.REPAIR,
                1, 2, 3, 4, 1002, replicaAlloc, System.currentTimeMillis());
        ctx3.setPriority(Priority.NORMAL);
        ctx3.setLastVisitedTime(1);

        pendingTablets.add(ctx1);
        pendingTablets.add(ctx2);
        pendingTablets.add(ctx3);

        TabletSchedCtx expectedCtx = pendingTablets.poll();
        Assert.assertNotNull(expectedCtx);
        Assert.assertEquals(ctx3.getTabletId(), expectedCtx.getTabletId());

        // priority is not equal, info2 is HIGH, should ranks ahead
        pendingTablets.clear();
        ctx1.setPriority(Priority.NORMAL);
        ctx2.setPriority(Priority.HIGH);
        ctx1.setLastVisitedTime(2);
        ctx2.setLastVisitedTime(2);
        pendingTablets.add(ctx2);
        pendingTablets.add(ctx1);
        expectedCtx = pendingTablets.poll();
        Assert.assertNotNull(expectedCtx);
        Assert.assertEquals(ctx2.getTabletId(), expectedCtx.getTabletId());

        // add info2 back to priority queue, and it should ranks ahead still.
        pendingTablets.add(ctx2);
        expectedCtx = pendingTablets.poll();
        Assert.assertNotNull(expectedCtx);
        Assert.assertEquals(ctx2.getTabletId(), expectedCtx.getTabletId());
    }

    @Test
    public void testVersionCountComparator() {
        TabletSchedCtx.CloneSrcComparator countComparator
                = new TabletSchedCtx.CloneSrcComparator();
        List<Replica> replicaList = Lists.newArrayList();
        Replica replica1 = new Replica();
        replica1.setVisibleVersionCount(100);
        replica1.setState(Replica.ReplicaState.NORMAL);
        // user drop true
        replica1.setUserDropTime(System.currentTimeMillis());

        Replica replica2 = new Replica();
        replica2.setVisibleVersionCount(50);
        replica2.setState(Replica.ReplicaState.NORMAL);
        // user drop false
        replica2.setUserDropTime(-1);

        Replica replica3 = new Replica();
        replica3.setVisibleVersionCount(-1);
        replica3.setState(Replica.ReplicaState.NORMAL);
        // user drop false
        replica3.setUserDropTime(-1);

        Replica replica4 = new Replica();
        replica4.setVisibleVersionCount(200);
        replica4.setState(Replica.ReplicaState.NORMAL);
        // user drop false
        replica4.setUserDropTime(-1);

        Replica replica5 = new Replica();
        replica5.setVisibleVersionCount(-1);
        replica5.setState(Replica.ReplicaState.NORMAL);
        // user drop true
        replica5.setUserDropTime(System.currentTimeMillis());

        replicaList.add(replica1);
        replicaList.add(replica2);
        replicaList.add(replica3);
        replicaList.add(replica4);
        replicaList.add(replica5);

        Collections.sort(replicaList, countComparator);
        // user drop false
        Assert.assertEquals(50, replicaList.get(0).getVisibleVersionCount());
        Assert.assertEquals(200, replicaList.get(1).getVisibleVersionCount());
        Assert.assertEquals(-1, replicaList.get(2).getVisibleVersionCount());
        // user drop true
        Assert.assertEquals(100, replicaList.get(3).getVisibleVersionCount());
        Assert.assertEquals(-1, replicaList.get(4).getVisibleVersionCount());
    }

    @Test
    public void testFilterDestBE() throws Exception {
        createTable("CREATE TABLE tbl1 (k INT) DISTRIBUTED BY HASH(k) BUCKETS 1"
                + " PROPERTIES ('replication_num' = '2')");

        OlapTable table = (OlapTable) db.getTableOrMetaException("tbl1");
        Partition partition = table.getPartitions().iterator().next();
        Tablet tablet = partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL).iterator().next()
                .getTablets().iterator().next();

        TabletSchedCtx tabletSchedCtx = new TabletSchedCtx(Type.REPAIR, db.getId(), table.getId(),
                partition.getId(), table.getBaseIndexId(),
                tablet.getId(), ReplicaAllocation.DEFAULT_ALLOCATION, System.currentTimeMillis());
        tabletSchedCtx.setTablet(table.getPartition(tabletSchedCtx.getPartitionId())
                .getIndex(tabletSchedCtx.getIndexId()).getTablet(tabletSchedCtx.getTabletId()));
        // 1L not exist return true
        Assertions.assertTrue(tabletSchedCtx.filterDestBE(1L));
        List<Long> backendIds = Env.getCurrentSystemInfo().getAllBackendsByAllCluster().values()
                .stream()
                .map(Backend::getId)
                .collect(Collectors.toList());
        List<Long> replicasBeIds = tablet.getReplicas().stream()
                .map(Replica::getBackendIdWithoutException).collect(Collectors.toList());

        Assertions.assertEquals(2, replicasBeIds.size());
        Assertions.assertTrue(tabletSchedCtx.filterDestBE(replicasBeIds.get(0)));
        Assertions.assertTrue(tabletSchedCtx.filterDestBE(replicasBeIds.get(1)));

        List<Long> notInReplicaBeIds = backendIds.stream()
                .filter(beId -> !replicasBeIds.contains(beId)).collect(Collectors.toList());
        Assertions.assertEquals(1, notInReplicaBeIds.size());
        Assertions.assertFalse(tabletSchedCtx.filterDestBE(notInReplicaBeIds.get(0)));
    }
}
