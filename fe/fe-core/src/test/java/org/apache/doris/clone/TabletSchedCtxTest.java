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

import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.clone.TabletSchedCtx.Priority;
import org.apache.doris.clone.TabletSchedCtx.Type;

import org.junit.Assert;
import org.junit.Test;

import java.util.PriorityQueue;

public class TabletSchedCtxTest {

    @Test
    public void testPriorityCompare() {
        // equal priority, but info3's last visit time is earlier than info2 and info1, so info1 should ranks ahead
        PriorityQueue<TabletSchedCtx> pendingTablets = new PriorityQueue<>();
        ReplicaAllocation replicaAlloc = ReplicaAllocation.DEFAULT_ALLOCATION;
        TabletSchedCtx ctx1 = new TabletSchedCtx(Type.REPAIR, "default_cluster",
                1, 2, 3, 4, 1000, replicaAlloc, System.currentTimeMillis());
        ctx1.setOrigPriority(Priority.NORMAL);
        ctx1.setLastVisitedTime(2);

        TabletSchedCtx ctx2 = new TabletSchedCtx(Type.REPAIR, "default_cluster",
                1, 2, 3, 4, 1001, replicaAlloc, System.currentTimeMillis());
        ctx2.setOrigPriority(Priority.NORMAL);
        ctx2.setLastVisitedTime(3);

        TabletSchedCtx ctx3 = new TabletSchedCtx(Type.REPAIR, "default_cluster",
                1, 2, 3, 4, 1001, replicaAlloc, System.currentTimeMillis());
        ctx3.setOrigPriority(Priority.NORMAL);
        ctx3.setLastVisitedTime(1);

        pendingTablets.add(ctx1);
        pendingTablets.add(ctx2);
        pendingTablets.add(ctx3);

        TabletSchedCtx expectedCtx = pendingTablets.poll();
        Assert.assertNotNull(expectedCtx);
        Assert.assertEquals(ctx3.getTabletId(), expectedCtx.getTabletId());

        // priority is not equal, info2 is HIGH, should ranks ahead
        pendingTablets.clear();
        ctx1.setOrigPriority(Priority.NORMAL);
        ctx2.setOrigPriority(Priority.HIGH);
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

}
