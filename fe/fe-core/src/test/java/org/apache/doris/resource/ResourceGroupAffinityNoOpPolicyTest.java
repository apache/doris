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

package org.apache.doris.resource;

import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.Backend;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

class ResourceGroupAffinityNoOpPolicyTest {
    @Test
    void testAffinityDecisionPublicConstructorDefaultsNulls() {
        ResourceGroupAffinity.AffinityDecision decision =
                new ResourceGroupAffinity.AffinityDecision(null, null, null);

        Assertions.assertEquals("", decision.getEffectivePreferredGroup());
        Assertions.assertEquals(ResourceGroupAffinity.Policy.RANDOM, decision.getEffectivePolicy());
        Assertions.assertEquals("", decision.getResolveNote());
    }

    @Test
    void testQueryNoOpDecisionAndApplyKeepCandidatesUnchanged() throws Exception {
        ResourceGroupAffinityPolicy policy = ResourceGroupAffinityPolicyFactory.load(
                ResourceGroupAffinityNoOpPolicyTest.class.getClassLoader());
        List<Candidate> candidates = ImmutableList.of(candidate("a", "g_b"), candidate("b", "g_a"));

        ResourceGroupAffinity.AffinityDecision decision = policy.decideForQuery(new ConnectContext());
        List<Candidate> ordered = policy.applyQueryAffinity(decision, candidates, candidate -> candidate.tag);
        List<Candidate> orderedWithinTies = policy.applyQueryAffinityWithinTies(decision, candidates,
                (left, right) -> 0, candidate -> candidate.tag);

        Assertions.assertEquals(ResourceGroupAffinity.Policy.RANDOM, decision.getEffectivePolicy());
        Assertions.assertEquals("", decision.getEffectivePreferredGroup());
        Assertions.assertFalse(policy.hasEffectiveQueryAffinity(decision));
        Assertions.assertSame(candidates, ordered);
        Assertions.assertSame(candidates, orderedWithinTies);
    }

    @Test
    void testConnectContextCachesQueryDecisionPerStatement() {
        ConnectContext context = new ConnectContext();

        ResourceGroupAffinity.AffinityDecision first = context.getQueryResourceGroupAffinityDecision();
        Assertions.assertSame(first, context.getQueryResourceGroupAffinityDecision());

        context.setStartTime();
        Assertions.assertNotSame(first, context.getQueryResourceGroupAffinityDecision());
    }

    @Test
    void testLoadNoOpSemantics() throws Exception {
        ResourceGroupAffinityPolicy policy = ResourceGroupAffinityPolicyFactory.load(
                ResourceGroupAffinityNoOpPolicyTest.class.getClassLoader());
        ConnectContext context = new ConnectContext();
        context.getSessionVariable().enableLoadLocalAffinity = true;
        Backend unavailable = backend(1, "g_a");
        unavailable.setAlive(false);
        Backend available = backend(2, "g_b");
        List<Backend> candidates = ImmutableList.of(unavailable, available);

        Assertions.assertFalse(policy.isLoadAffinityEnabled(context));
        Assertions.assertNull(policy.decideForLoad(context));
        Assertions.assertFalse(policy.hasEffectiveLoadAffinity(null));
        Assertions.assertSame(candidates, policy.orderLoadBackends(context, candidates));
        Assertions.assertSame(candidates, policy.orderLoadBackends(
                (ResourceGroupAffinity.AffinityDecision) null, candidates));
        Assertions.assertEquals(available, policy.chooseLoadBackendWithAffinity(context, candidates));
        Assertions.assertNull(policy.forwardedLoadDecision("g_a", "random"));
    }

    @Test
    void testLoadNoOpReturnsNullWhenNoneLoadAvailable() throws Exception {
        ResourceGroupAffinityPolicy policy = ResourceGroupAffinityPolicyFactory.load(
                ResourceGroupAffinityNoOpPolicyTest.class.getClassLoader());
        Backend first = backend(1, "g_a");
        Backend second = backend(2, "g_b");
        first.setAlive(false);
        second.setAlive(false);

        Backend selected = policy.chooseLoadBackendWithAffinity(new ConnectContext(),
                ImmutableList.of(first, second));

        Assertions.assertNull(selected);
    }

    private Backend backend(long id, String group) {
        Backend backend = new Backend(id, "127.0.0." + id, 9050);
        backend.setAlive(true);
        backend.setBePort(9060 + (int) id);
        backend.setHttpPort(8040 + (int) id);
        backend.setTagMap(ImmutableMap.of(Tag.TYPE_LOCATION, group));
        return backend;
    }

    private Candidate candidate(String name, String group) {
        return new Candidate(name, Tag.createNotCheck(Tag.TYPE_LOCATION, group));
    }

    private static final class Candidate {
        private final String name;
        private final Tag tag;

        private Candidate(String name, Tag tag) {
            this.name = name;
            this.tag = tag;
        }
    }
}
