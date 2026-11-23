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

package org.apache.doris.ha;

import org.apache.doris.system.Frontend;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

public class MasterTransferTargetTest {

    private static Frontend fe(FrontendNodeType role, String nodeName, boolean alive) {
        Frontend frontend = new Frontend(role, nodeName, "127.0.0.1", 9010);
        frontend.setIsAlive(alive);
        return frontend;
    }

    @Test
    public void testChoosesAliveFollowersExceptSelf() {
        List<Frontend> frontends = Lists.newArrayList(
                fe(FrontendNodeType.FOLLOWER, "fe_master", true),
                fe(FrontendNodeType.FOLLOWER, "fe_a", true),
                fe(FrontendNodeType.FOLLOWER, "fe_b", true));
        Assertions.assertEquals(ImmutableSet.of("fe_a", "fe_b"),
                BDBHA.chooseMasterTransferTargets(frontends, "fe_master"));
    }

    @Test
    public void testSkipsDeadFollowersAndObservers() {
        List<Frontend> frontends = Lists.newArrayList(
                fe(FrontendNodeType.FOLLOWER, "fe_master", true),
                fe(FrontendNodeType.FOLLOWER, "fe_dead", false),
                fe(FrontendNodeType.OBSERVER, "fe_observer", true),
                fe(FrontendNodeType.FOLLOWER, "fe_alive", true));
        Assertions.assertEquals(ImmutableSet.of("fe_alive"),
                BDBHA.chooseMasterTransferTargets(frontends, "fe_master"));
    }

    @Test
    public void testReadyTargetsLeaveOutFrozenAndLaggingFollowers() {
        // fe_frozen did not answer the heartbeat probe, so it is absent; fe_lagging trails by more than the lag.
        Assertions.assertEquals(ImmutableSet.of("fe_ready"), BDBHA.chooseReadyTransferTargets(
                ImmutableMap.of("fe_ready", 990L, "fe_lagging", 500L), 1000L, 100L));
    }

    @Test
    public void testReadyTargetsAcceptFollowersWithinTheLag() {
        Assertions.assertEquals(ImmutableSet.of("fe_a", "fe_b"), BDBHA.chooseReadyTransferTargets(
                ImmutableMap.of("fe_a", 1000L, "fe_b", 900L), 1000L, 100L));
        Assertions.assertTrue(BDBHA.chooseReadyTransferTargets(ImmutableMap.of(), 1000L, 100L).isEmpty());
        Assertions.assertTrue(BDBHA.chooseReadyTransferTargets(ImmutableMap.of("fe_a", 899L), 1000L, 100L).isEmpty());
    }

    @Test
    public void testNoTargetWithoutOtherAliveFollower() {
        Assertions.assertTrue(BDBHA.chooseMasterTransferTargets(
                Lists.newArrayList(fe(FrontendNodeType.FOLLOWER, "fe_master", true),
                        fe(FrontendNodeType.FOLLOWER, "fe_dead", false)), "fe_master").isEmpty());
        Assertions.assertTrue(BDBHA.chooseMasterTransferTargets(Collections.emptyList(), "fe_master").isEmpty());
    }

    @Test
    public void testQuorumWithoutTheDepartingMaster() {
        // Three electable nodes: both other followers must answer, one of them alone cannot form a majority.
        Assertions.assertTrue(BDBHA.keepsQuorumWithout(3, 2));
        Assertions.assertFalse(BDBHA.keepsQuorumWithout(3, 1));
        // Five electable nodes: three of the other four are enough.
        Assertions.assertTrue(BDBHA.keepsQuorumWithout(5, 3));
        Assertions.assertFalse(BDBHA.keepsQuorumWithout(5, 2));
        // Two electable nodes never keep a quorum without one of them.
        Assertions.assertFalse(BDBHA.keepsQuorumWithout(2, 1));
    }

    @Test
    public void testHandoverTargetsNeedAReadyMajority() {
        // Three electable nodes: both other followers ready, both are targets.
        Assertions.assertEquals(ImmutableSet.of("fe_a", "fe_b"), BDBHA.chooseHandoverTargets(3,
                ImmutableMap.of("fe_a", 1000L, "fe_b", 990L), 1000L, 100L));
        // One of them lags: the ready one alone is no majority of three, so there is no handover.
        Assertions.assertTrue(BDBHA.chooseHandoverTargets(3,
                ImmutableMap.of("fe_a", 1000L, "fe_b", 500L), 1000L, 100L).isEmpty());
        // One of them is frozen and did not answer: no handover either.
        Assertions.assertTrue(BDBHA.chooseHandoverTargets(3, ImmutableMap.of("fe_a", 1000L), 1000L, 100L).isEmpty());
        // Five electable nodes: three ready followers of the other four are enough.
        Assertions.assertEquals(ImmutableSet.of("fe_a", "fe_b", "fe_c"), BDBHA.chooseHandoverTargets(5,
                ImmutableMap.of("fe_a", 1000L, "fe_b", 1000L, "fe_c", 950L, "fe_d", 10L), 1000L, 100L));
    }
}
