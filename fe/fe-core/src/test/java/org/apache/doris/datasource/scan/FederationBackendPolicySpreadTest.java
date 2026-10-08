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

package org.apache.doris.datasource.scan;

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.util.LocationPath;
import org.apache.doris.datasource.split.FileSplit;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.resource.computegroup.ComputeGroup;
import org.apache.doris.spi.Split;
import org.apache.doris.system.Backend;

import com.google.common.collect.Multimap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntUnaryOperator;

public class FederationBackendPolicySpreadTest {
    private static final AtomicLong NEXT_BACKEND_ID = new AtomicLong(91000);

    private MockedStatic<Env> mockedEnv;
    private ConnectContext previousContext;
    private ConnectContext context;
    private ComputeGroup computeGroup;
    private List<Backend> backends;
    private int originalCandidateNum;
    private boolean originalLocalScheduling;

    @BeforeEach
    public void setUp() {
        previousContext = ConnectContext.get();
        originalCandidateNum = Config.split_assigner_min_consistent_hash_candidate_num;
        originalLocalScheduling = Config.split_assigner_optimized_local_scheduling;
        Config.split_assigner_min_consistent_hash_candidate_num = 2;
        Config.split_assigner_optimized_local_scheduling = true;
        mockedEnv = Mockito.mockStatic(Env.class);
        mockedEnv.when(Env::getCurrentEnv).thenReturn(Mockito.mock(Env.class));
        backends = new ArrayList<>();
        long firstId = NEXT_BACKEND_ID.getAndAdd(3);
        for (int i = 0; i < 3; i++) {
            Backend backend = new Backend(firstId + i, "192.0.2." + (i + 1), 9050);
            backend.setAlive(true);
            backends.add(backend);
        }
        computeGroup = Mockito.mock(ComputeGroup.class);
        Mockito.when(computeGroup.getBackendList()).thenAnswer(invocation -> backends);
        context = new ConnectContext();
        context.setComputeGroup(computeGroup);
        context.setThreadLocalInfo();
    }

    @AfterEach
    public void tearDown() {
        Config.split_assigner_min_consistent_hash_candidate_num = originalCandidateNum;
        Config.split_assigner_optimized_local_scheduling = originalLocalScheduling;
        mockedEnv.close();
        ConnectContext.remove();
        if (previousContext != null) {
            previousContext.setThreadLocalInfo();
        }
    }

    private FileSplit split() {
        return new FileSplit(LocationPath.of("s3://bucket/hot.csv"), 0, 1000, 1000,
                0, new String[0], Collections.emptyList());
    }

    private FederationBackendPolicy policy(int candidates, IntUnaryOperator random) throws UserException {
        FederationBackendPolicy policy = new FederationBackendPolicy(
                NodeSelectionStrategy.CONSISTENT_HASHING, candidates, random);
        policy.init();
        return policy;
    }

    private Backend assign(FederationBackendPolicy policy, Split split) throws UserException {
        Multimap<Backend, Split> assignment = policy.computeScanRangeAssignment(
                new ArrayList<>(Collections.singletonList(split)));
        Assertions.assertEquals(1, assignment.size());
        Assertions.assertEquals(1, assignment.keySet().size());
        Assertions.assertSame(split, assignment.values().iterator().next());
        return assignment.keySet().iterator().next();
    }

    private static IntUnaryOperator selectCandidate(int index) {
        // Replace the reservoir through the desired index, then keep that node.
        return bound -> bound <= index + 1 ? 0 : bound - 1;
    }

    @Test
    public void testIndependentHotQueriesCanChooseEveryCandidateExactlyOnce() throws Exception {
        Set<Backend> selected = new HashSet<>();
        for (int index = 0; index < 3; index++) {
            FederationBackendPolicy policy = policy(3, selectCandidate(index));
            FileSplit split = split();
            List<Backend> candidates = policy.consistentHash.getNode(split, 3);
            Backend target = assign(policy, split);
            Assertions.assertEquals(candidates.get(index), target);
            selected.add(target);
            Assertions.assertEquals(2, split.getAlternativeHosts().size());
        }
        Assertions.assertEquals(new HashSet<>(backends), selected);
    }

    @Test
    public void testTopTwoCandidateBoundarySurvivesMultipleBatches() throws Exception {
        FederationBackendPolicy policy = policy(2, bound -> bound - 1);
        Set<Backend> candidates = new HashSet<>(policy.consistentHash.getNode(split(), 2));
        for (int batch = 0; batch < 2; batch++) {
            List<Split> splits = new ArrayList<>();
            for (int i = 0; i < 20; i++) {
                splits.add(split());
            }
            Multimap<Backend, Split> assignment = policy.computeScanRangeAssignment(splits);
            Assertions.assertEquals(20, assignment.size());
            Assertions.assertTrue(candidates.containsAll(assignment.keySet()));
            for (Backend backend : candidates) {
                Assertions.assertEquals(10, assignment.get(backend).size());
                Assertions.assertEquals((batch + 1) * 1000L, policy.getAssignedWeightPerBackend().get(backend));
            }
        }
        for (Backend backend : backends) {
            if (!candidates.contains(backend)) {
                Assertions.assertEquals(0L, policy.getAssignedWeightPerBackend().get(backend));
            }
        }
    }

    @Test
    public void testMinimumWeightWinsBeforeRandomTieBreaking() throws Exception {
        FederationBackendPolicy policy = policy(3, selectCandidate(2));
        List<Backend> candidates = policy.consistentHash.getNode(split(), 3);
        policy.getAssignedWeightPerBackend().put(candidates.get(0), 1000L);
        policy.getAssignedWeightPerBackend().put(candidates.get(1), 0L);
        policy.getAssignedWeightPerBackend().put(candidates.get(2), 100L);
        Assertions.assertEquals(candidates.get(1), assign(policy, split()));
    }

    @Test
    public void testDefaultPreservesOriginalCandidateNumberAndTieBreaking() throws Exception {
        FederationBackendPolicy original = new FederationBackendPolicy(NodeSelectionStrategy.CONSISTENT_HASHING);
        original.init();
        FederationBackendPolicy disabled = policy(1, bound -> {
            throw new AssertionError("Disabled spread must not use randomness");
        });
        List<Backend> candidates = disabled.consistentHash.getNode(split(), 2);
        Assertions.assertEquals(candidates.get(1), assign(disabled, split()));
        Assertions.assertEquals(candidates.get(1), assign(original, split()));
    }

    @Test
    public void testDefaultPreservesGlobalRedistribution() throws Exception {
        FederationBackendPolicy disabled = policy(1, bound -> {
            throw new AssertionError("Disabled spread must not use randomness");
        });
        List<Split> splits = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
            splits.add(split());
        }
        Assertions.assertEquals(3, disabled.computeScanRangeAssignment(splits).keySet().size());
    }

    @Test
    public void testCandidateCountIsCappedByEligibleBackends() throws Exception {
        Set<Backend> targets = new HashSet<>();
        for (int index = 0; index < 3; index++) {
            targets.add(assign(policy(Integer.MAX_VALUE, selectCandidate(index)), split()));
        }
        Assertions.assertEquals(new HashSet<>(backends), targets);
    }

    @Test
    public void testSingleBackend() throws Exception {
        backends = new ArrayList<>(Collections.singletonList(backends.get(0)));
        Assertions.assertEquals(backends.get(0), assign(policy(3, bound -> bound - 1), split()));
    }

    @Test
    public void testSameHostBackendsRemainDistinctCandidates() throws Exception {
        backends = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            Backend backend = new Backend(92000 + i, "192.0.2.20", 9050 + i);
            backend.setAlive(true);
            backends.add(backend);
        }
        Set<Backend> targets = new HashSet<>();
        for (int index = 0; index < 3; index++) {
            targets.add(assign(policy(3, selectCandidate(index)), split()));
        }
        Assertions.assertEquals(new HashSet<>(backends), targets);
    }

    @Test
    public void testUnavailableAndOtherComputeGroupBackendsAreExcluded() throws Exception {
        Backend dead = backends.get(2);
        dead.setAlive(false);
        Backend otherGroup = backends.get(1);
        backends = new ArrayList<>(Arrays.asList(backends.get(0), dead));
        FederationBackendPolicy policy = policy(3, bound -> bound - 1);
        Assertions.assertEquals(1, policy.numBackends());
        Assertions.assertEquals(backends.get(0), assign(policy, split()));
        Assertions.assertFalse(policy.getBackends().contains(otherGroup));
        Assertions.assertFalse(policy.getBackends().contains(dead));
    }

    @Test
    public void testMembershipChangesRefreshCandidates() throws Exception {
        Backend removed = backends.remove(2);
        FederationBackendPolicy afterRemoval = policy(3, selectCandidate(1));
        Assertions.assertEquals(2, afterRemoval.numBackends());
        Assertions.assertFalse(afterRemoval.consistentHash.getNode(split(), 3).contains(removed));
        Assertions.assertTrue(backends.contains(assign(afterRemoval, split())));
        backends.add(removed);
        FederationBackendPolicy afterAddition = policy(3, selectCandidate(2));
        Assertions.assertEquals(3, afterAddition.numBackends());
        Assertions.assertEquals(new HashSet<>(backends),
                new HashSet<>(afterAddition.consistentHash.getNode(split(), 3)));
        Assertions.assertTrue(backends.contains(assign(afterAddition, split())));
    }

    @Test
    public void testLocalPreferenceCanChooseOutsideHashCandidates() throws Exception {
        FederationBackendPolicy policy = policy(2, bound -> {
            throw new AssertionError("Local selection must not use spread randomness");
        });
        FileSplit split = split();
        List<Backend> candidates = policy.consistentHash.getNode(split, 2);
        Backend local = backends.stream().filter(backend -> !candidates.contains(backend)).findFirst().get();
        split.setHosts(new String[] {local.getHost()});
        Assertions.assertEquals(local, assign(policy, split));
    }

    @Test
    public void testNonRemoteSplitsRetainHostConstraintWithoutRedistribution() throws Exception {
        for (boolean preferLocal : Arrays.asList(true, false)) {
            Config.split_assigner_optimized_local_scheduling = preferLocal;
            FederationBackendPolicy policy = policy(3, bound -> {
                throw new AssertionError("Non-remote selection must not use spread randomness");
            });
            FileSplit split = new FileSplit(LocationPath.of("file:///hot.csv"), 0, 1000, 1000,
                    0, new String[] {backends.get(0).getHost()}, Collections.emptyList()) {
                @Override
                public boolean isRemotelyAccessible() {
                    return false;
                }
            };
            List<Split> splits = new ArrayList<>(Collections.nCopies(12, split));
            Multimap<Backend, Split> assignment = policy.computeScanRangeAssignment(splits);
            Assertions.assertEquals(12, assignment.size());
            Assertions.assertEquals(Collections.singleton(backends.get(0)), assignment.keySet());
        }
    }

    @Test
    public void testRemoteLocalityIsIgnoredWhenLocalPreferenceIsDisabled() throws Exception {
        Config.split_assigner_optimized_local_scheduling = false;
        FederationBackendPolicy policy = policy(2, selectCandidate(1));
        FileSplit split = split();
        List<Backend> candidates = policy.consistentHash.getNode(split, 2);
        Backend local = backends.stream().filter(backend -> !candidates.contains(backend)).findFirst().get();
        split.setHosts(new String[] {local.getHost()});
        Assertions.assertEquals(candidates.get(1), assign(policy, split));
    }

    @Test
    public void testQueryAndLoadDisabledBackendsAreExcluded() throws Exception {
        backends.get(1).setQueryDisabled(true);
        backends.get(2).setLoadDisabled(true);
        FederationBackendPolicy policy = policy(3, bound -> bound - 1);
        Assertions.assertEquals(1, policy.numBackends());
        Assertions.assertEquals(backends.get(0), assign(policy, split()));
    }

    @Test
    public void testMissingRequiredHostFails() throws Exception {
        FederationBackendPolicy policy = policy(3, bound -> bound - 1);
        FileSplit split = new FileSplit(LocationPath.of("file:///hot.csv"), 0, 1000, 1000,
                0, new String[] {"192.0.2.200"}, Collections.emptyList()) {
            @Override
            public boolean isRemotelyAccessible() {
                return false;
            }
        };
        Assertions.assertThrows(UserException.class, () -> assign(policy, split));
    }

    @Test
    public void testNoEligibleBackendFails() {
        backends.clear();
        Assertions.assertThrows(UserException.class, () -> policy(3, bound -> bound - 1));
    }

    @Test
    public void testInvalidSpreadCountFails() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new FederationBackendPolicy(NodeSelectionStrategy.CONSISTENT_HASHING, -1));
    }

    @Test
    public void testNonHashStrategiesDoNotUseSpreadRandomness() throws Exception {
        for (NodeSelectionStrategy strategy : Arrays.asList(NodeSelectionStrategy.ROUND_ROBIN,
                NodeSelectionStrategy.RANDOM)) {
            FederationBackendPolicy policy = new FederationBackendPolicy(strategy, 3, bound -> {
                throw new AssertionError("Non-hash policy must not use spread randomness");
            });
            policy.init();
            Assertions.assertTrue(backends.contains(assign(policy, split())));
        }
    }

    @Test
    public void testExternalScanGateAndSessionIsolation() throws Exception {
        SessionVariable session = context.getSessionVariable();
        session.externalScanConsistentHashSpreadNum = 3;
        for (boolean cache : Arrays.asList(false, true)) {
            for (boolean hash : Arrays.asList(false, true)) {
                session.enableFileCache = cache;
                session.useConsistentHashForExternalScan = hash;
                FederationBackendPolicy policy = new TestExternalScanNode().backendPolicy;
                Assertions.assertEquals(cache || hash ? NodeSelectionStrategy.CONSISTENT_HASHING
                                : NodeSelectionStrategy.ROUND_ROBIN,
                        Deencapsulation.getField(policy, "nodeSelectionStrategy"));
                Assertions.assertEquals(cache || hash ? 3 : 1,
                        (int) Deencapsulation.getField(policy, "consistentHashSpreadNum"));
            }
        }
        FederationBackendPolicy plain = new FederationBackendPolicy(NodeSelectionStrategy.CONSISTENT_HASHING);
        Assertions.assertEquals(1, (int) Deencapsulation.getField(plain, "consistentHashSpreadNum"));
        ConnectContext.remove();
        FederationBackendPolicy noContext = new TestExternalScanNode().backendPolicy;
        Assertions.assertEquals(1, (int) Deencapsulation.getField(noContext, "consistentHashSpreadNum"));
    }

    @Test
    public void testAutomaticModeSupportsAnyBackendCountAndBatches() throws Exception {
        for (int count : Arrays.asList(1, 2, 3, 5)) {
            backends = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                Backend backend = new Backend(NEXT_BACKEND_ID.getAndIncrement(), "192.0.2." + (i + 1), 9050);
                backend.setAlive(true);
                backends.add(backend);
            }
            for (NodeSelectionStrategy strategy : Arrays.asList(NodeSelectionStrategy.RANDOM,
                    NodeSelectionStrategy.CONSISTENT_HASHING)) {
                Set<Backend> targets = new HashSet<>();
                for (int index = 0; index < count; index++) {
                    FederationBackendPolicy policy = new FederationBackendPolicy(strategy, 0, selectCandidate(index));
                    policy.init();
                    targets.add(assign(policy, split()));
                }
                Assertions.assertEquals(new HashSet<>(backends), targets);
                FederationBackendPolicy batched = new FederationBackendPolicy(strategy, 0, bound -> bound - 1);
                batched.init();
                for (int batch = 0; batch < 2; batch++) {
                    for (int i = 0; i < count; i++) {
                        assign(batched, split());
                    }
                    for (Backend backend : backends) {
                        Assertions.assertEquals((batch + 1) * 100L,
                                batched.getAssignedWeightPerBackend().get(backend));
                    }
                }
            }
        }
    }

    @Test
    public void testAutomaticModeRetainsEligibilityAndMandatoryLocality() throws Exception {
        Backend unavailable = backends.get(2);
        unavailable.setAlive(false);
        Backend otherGroup = backends.remove(1);
        for (NodeSelectionStrategy strategy : Arrays.asList(NodeSelectionStrategy.RANDOM,
                NodeSelectionStrategy.CONSISTENT_HASHING)) {
            FederationBackendPolicy policy = new FederationBackendPolicy(strategy, 0, bound -> bound - 1);
            policy.init();
            Assertions.assertEquals(1, policy.numBackends());
            Assertions.assertEquals(backends.get(0), assign(policy, split()));
            Assertions.assertFalse(policy.getBackends().contains(otherGroup));
            FileSplit local = new FileSplit(LocationPath.of("file:///hot.csv"), 0, 1000, 1000,
                    0, new String[] {backends.get(0).getHost()}, Collections.emptyList()) {
                @Override
                public boolean isRemotelyAccessible() {
                    return false;
                }
            };
            Assertions.assertEquals(backends.get(0), assign(policy, local));
            local.setHosts(new String[] {otherGroup.getHost()});
            Assertions.assertThrows(UserException.class, () -> assign(policy, local));
        }
    }

    @Test
    public void testAutomaticExternalScanStrategyAndLegacyOptOut() {
        SessionVariable session = context.getSessionVariable();
        Assertions.assertEquals(0, session.getExternalScanConsistentHashSpreadNum());
        for (boolean cache : Arrays.asList(false, true)) {
            for (boolean hash : Arrays.asList(false, true)) {
                session.enableFileCache = cache;
                session.useConsistentHashForExternalScan = hash;
                FederationBackendPolicy policy = new TestExternalScanNode().backendPolicy;
                Assertions.assertEquals(cache || hash ? NodeSelectionStrategy.CONSISTENT_HASHING
                                : NodeSelectionStrategy.RANDOM,
                        Deencapsulation.getField(policy, "nodeSelectionStrategy"));
                Assertions.assertEquals(0, (int) Deencapsulation.getField(policy, "consistentHashSpreadNum"));
            }
        }
        session.enableFileCache = false;
        session.useConsistentHashForExternalScan = false;
        session.externalScanConsistentHashSpreadNum = 1;
        Assertions.assertEquals(NodeSelectionStrategy.ROUND_ROBIN,
                Deencapsulation.getField(new TestExternalScanNode().backendPolicy, "nodeSelectionStrategy"));
    }

    private static class TestExternalScanNode extends ExternalScanNode {
        TestExternalScanNode() {
            super(new PlanNodeId(0), new TupleDescriptor(new TupleId(0)), "test", ScanContext.EMPTY, false);
        }

        @Override
        protected void createScanRangeLocations() {
        }
    }
}
