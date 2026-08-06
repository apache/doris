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

package org.apache.doris.nereids.trees.plans.distribute;

import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.trees.plans.distribute.worker.DistributedPlanWorker;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.AssignedJob;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.LocalShuffleAssignedJob;
import org.apache.doris.planner.ExchangeNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;

/**
 * Verifies that DistributePlanner picks the receiver instances a remote sender addresses
 * based on whether the exchange is serial on BE -- not on whether the receiver fragment
 * happens to use local shuffle.
 *
 * A non-serial exchange runs a live receiver on every instance, so the sender must address
 * them all. Funneling such an exchange to the first instance per worker leaves the other
 * instances waiting forever for an EOS that no sender sends (query hangs until timeout).
 */
public class DistributePlannerReceiverDestinationTest {

    private DistributePlanner newPlanner() {
        List<PlanFragment> noFragments = Lists.newArrayList();
        return new DistributePlanner(Mockito.mock(StatementContext.class), noFragments, false, false);
    }

    private AssignedJob instanceOn(DistributedPlanWorker worker) {
        LocalShuffleAssignedJob instance = Mockito.mock(LocalShuffleAssignedJob.class);
        Mockito.when(instance.getAssignedWorker()).thenReturn(worker);
        return instance;
    }

    private ExchangeNode exchange(boolean serialOnBe, boolean rightChildOfBroadcastJoin) {
        ExchangeNode node = Mockito.mock(ExchangeNode.class);
        Mockito.when(node.isSerialOperatorOnBe(Mockito.nullable(ConnectContext.class))).thenReturn(serialOnBe);
        Mockito.when(node.isRightChildOfBroadcastHashJoin()).thenReturn(rightChildOfBroadcastJoin);
        return node;
    }

    private PipelineDistributedPlan receiverWith(List<AssignedJob> instances) {
        PipelineDistributedPlan plan = Mockito.mock(PipelineDistributedPlan.class);
        Mockito.when(plan.getInstanceJobs()).thenReturn(instances);
        return plan;
    }

    @Test
    public void nonSerialExchangeAddressesEveryReceiverInstance() {
        // 4 receiver instances across 2 workers (e.g. a bucket-to-hash upgraded fragment)
        DistributedPlanWorker w0 = Mockito.mock(DistributedPlanWorker.class);
        DistributedPlanWorker w1 = Mockito.mock(DistributedPlanWorker.class);
        AssignedJob i0 = instanceOn(w0);
        AssignedJob i1 = instanceOn(w0);
        AssignedJob i2 = instanceOn(w1);
        AssignedJob i3 = instanceOn(w1);
        List<AssignedJob> all = Lists.newArrayList(i0, i1, i2, i3);

        List<AssignedJob> destinations = newPlanner().filterInstancesWhichCanReceiveDataFromRemote(
                receiverWith(all), false, exchange(false, false));

        // Non-serial exchange: every instance owns a live receiver, so all must be addressed.
        Assertions.assertEquals(all, destinations);
    }

    @Test
    public void serialExchangeFunnelsToFirstInstancePerWorker() {
        DistributedPlanWorker w0 = Mockito.mock(DistributedPlanWorker.class);
        DistributedPlanWorker w1 = Mockito.mock(DistributedPlanWorker.class);
        AssignedJob i0 = instanceOn(w0);
        AssignedJob i1 = instanceOn(w0);
        AssignedJob i2 = instanceOn(w1);
        AssignedJob i3 = instanceOn(w1);
        List<AssignedJob> all = Lists.newArrayList(i0, i1, i2, i3);

        List<AssignedJob> destinations = newPlanner().filterInstancesWhichCanReceiveDataFromRemote(
                receiverWith(all), false, exchange(true, false));

        // Serial exchange: BE runs a single receiver per worker, funnel to the first instance.
        Assertions.assertEquals(Lists.newArrayList(i0, i2), destinations);
    }

    @Test
    public void broadcastShareHashTableFunnelsEvenWhenNonSerial() {
        DistributedPlanWorker w0 = Mockito.mock(DistributedPlanWorker.class);
        DistributedPlanWorker w1 = Mockito.mock(DistributedPlanWorker.class);
        AssignedJob i0 = instanceOn(w0);
        AssignedJob i1 = instanceOn(w0);
        AssignedJob i2 = instanceOn(w1);
        List<AssignedJob> all = Lists.newArrayList(i0, i1, i2);

        List<AssignedJob> destinations = newPlanner().filterInstancesWhichCanReceiveDataFromRemote(
                receiverWith(all), true, exchange(false, true));

        // Shared-hash-table broadcast build still funnels one build per worker.
        Assertions.assertEquals(Lists.newArrayList(i0, i2), destinations);
    }
}
