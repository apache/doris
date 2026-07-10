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

class BackendSelectionNoOpPolicyTest {
    @Test
    void testSelectionHintPublicConstructorDefaultsNulls() {
        BackendSelection.SelectionHint decision =
                new BackendSelection.SelectionHint(null, null, null);

        Assertions.assertEquals("", decision.getPreferredKey());
        Assertions.assertEquals(BackendSelection.Mode.DEFAULT, decision.getMode());
        Assertions.assertEquals("", decision.getReason());
    }

    @Test
    void testDefaultPolicyKeepsQueryAndLoadCandidatesUnchanged() throws Exception {
        BackendSelectionPolicy policy = BackendSelectionPolicyFactory.load(
                BackendSelectionNoOpPolicyTest.class.getClassLoader());
        List<Candidate> queryCandidates = ImmutableList.of(candidate("a", "key_b"), candidate("b", "key_a"));

        BackendSelection.SelectionHint decision = policy.getQuerySelectionHint(new ConnectContext());
        List<Candidate> ordered = policy.orderQueryCandidates(
                decision, queryCandidates, candidate -> candidate.tag);

        Assertions.assertEquals(BackendSelection.Mode.DEFAULT, decision.getMode());
        Assertions.assertEquals("", decision.getPreferredKey());
        Assertions.assertFalse(policy.hasQuerySelectionPreference(decision));
        Assertions.assertSame(queryCandidates, ordered);

        ConnectContext context = new ConnectContext();
        context.getSessionVariable().enableLoadBackendSelection = true;
        List<Backend> loadCandidates = ImmutableList.of(backend(1, "key_a"), backend(2, "key_b"));
        Assertions.assertFalse(policy.isLoadSelectionEnabled(context));
        Assertions.assertNull(policy.getLoadSelectionHint(context));
        Assertions.assertFalse(policy.hasLoadSelectionPreference(null));
        Assertions.assertSame(loadCandidates, policy.orderLoadCandidates(
                (BackendSelection.SelectionHint) null, loadCandidates));
        Assertions.assertNull(policy.getForwardedLoadSelectionHint("key_a", "default"));
    }

    @Test
    void testConnectContextCachesQueryDecisionPerStatement() {
        ConnectContext context = new ConnectContext();

        BackendSelection.SelectionHint first = context.getQueryBackendSelectionDecision();
        Assertions.assertSame(first, context.getQueryBackendSelectionDecision());

        context.setStartTime();
        Assertions.assertNotSame(first, context.getQueryBackendSelectionDecision());
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
