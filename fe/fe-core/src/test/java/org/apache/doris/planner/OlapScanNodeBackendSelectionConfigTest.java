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

package org.apache.doris.planner;

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.LocalReplica;
import org.apache.doris.catalog.LocalTablet;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.resource.BackendSelection;
import org.apache.doris.resource.BackendSelectionManager;
import org.apache.doris.resource.Tag;
import org.apache.doris.resource.spi.BackendSelectionProvider;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TScanRangeLocations;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

class OlapScanNodeBackendSelectionConfigTest {

    @AfterEach
    void resetBackendSelectionProvider() {
        BackendSelectionManager.resetProviderForTest();
    }

    @Test
    void testQuerySelectionDisabledInCloudMode() {
        String oldCloudUniqueId = Config.cloud_unique_id;
        String oldDeployMode = Config.deploy_mode;
        try {
            Config.cloud_unique_id = "cloud_id";
            Config.deploy_mode = "cloud";
            Assertions.assertFalse(OlapScanNode.shouldApplyQuerySelection(false));
            Assertions.assertFalse(OlapScanNode.shouldApplyQuerySelection(true));

            Config.cloud_unique_id = "";
            Config.deploy_mode = "";
            Assertions.assertTrue(OlapScanNode.shouldApplyQuerySelection(false));
            Assertions.assertFalse(OlapScanNode.shouldApplyQuerySelection(true));
        } finally {
            Config.cloud_unique_id = oldCloudUniqueId;
            Config.deploy_mode = oldDeployMode;
        }
    }

    @Test
    void testRequiredQuerySelectionRejectsBypassModes() {
        BackendSelection.SelectionHint hint = new BackendSelection.SelectionHint(
                "key_a", BackendSelection.Mode.REQUIRE, "test");

        Assertions.assertThrows(UserException.class,
                () -> OlapScanNode.validateRequiredQuerySelection(true, -1, hint));
        Assertions.assertThrows(UserException.class,
                () -> OlapScanNode.validateRequiredQuerySelection(false, 0, hint));
        Assertions.assertDoesNotThrow(
                () -> OlapScanNode.validateRequiredQuerySelection(false, -1, hint));
    }

    @Test
    void testSkipMissingVersionUsesTieAwareQuerySelection() throws Exception {
        Replica first = replica(1, 10);
        Replica second = replica(2, 10);
        Replica third = replica(3, 9);
        Replica fourth = replica(4, 9);
        List<Replica> replicas = new ArrayList<>(ImmutableList.of(first, second, third, fourth));
        BackendSelection.SelectionHint hint = new BackendSelection.SelectionHint(
                "key_a", BackendSelection.Mode.PREFER, "test");
        BackendSelectionProvider policy = Mockito.mock(BackendSelectionProvider.class);
        Mockito.when(policy.hasQuerySelectionPreference(hint)).thenReturn(true);
        Mockito.when(policy.orderQueryCandidates(
                        Mockito.eq(hint), Mockito.anyList(), Mockito.any()))
                .thenAnswer(invocation -> {
                    List<Replica> ordered = new ArrayList<>(invocation.getArgument(1));
                    Collections.reverse(ordered);
                    return ordered;
                });

        BackendSelectionManager.setProviderForTest(policy);

        List<Replica> ordered = OlapScanNode.orderReplicasForQuerySelection(
                    true, replicas, hint, replica -> Tag.DEFAULT_BACKEND_TAG);

        Assertions.assertEquals(ImmutableList.of(second, first, fourth, third), ordered);
        Mockito.verify(policy, Mockito.times(2)).orderQueryCandidates(
                    Mockito.eq(hint), Mockito.anyList(), Mockito.any());
    }

    @Test
    void testNonSkipMissingVersionUsesExistingQuerySelectionPath() throws Exception {
        Replica first = replica(1, 10);
        Replica second = replica(2, 10);
        List<Replica> replicas = new ArrayList<>(ImmutableList.of(first, second));
        BackendSelection.SelectionHint hint = new BackendSelection.SelectionHint(
                "key_a", BackendSelection.Mode.PREFER, "test");
        BackendSelectionProvider policy = Mockito.mock(BackendSelectionProvider.class);
        Mockito.when(policy.hasQuerySelectionPreference(hint)).thenReturn(true);
        Mockito.when(policy.orderQueryCandidates(
                        Mockito.eq(hint), Mockito.anyList(), Mockito.any()))
                .thenAnswer(invocation -> invocation.getArgument(1));

        BackendSelectionManager.setProviderForTest(policy);

        List<Replica> ordered = OlapScanNode.orderReplicasForQuerySelection(
                    false, replicas, hint, replica -> Tag.DEFAULT_BACKEND_TAG);

        Assertions.assertEquals(replicas.size(), ordered.size());
        Mockito.verify(policy).hasQuerySelectionPreference(hint);
        Mockito.verify(policy).orderQueryCandidates(
                    Mockito.eq(hint), Mockito.anyList(), Mockito.any());
    }

    @Test
    void testRequiredQuerySelectionKeepsCompactionSlowPreferredReplica() throws Exception {
        Replica preferredReplica = replica(1, 10);
        preferredReplica.setVisibleVersionCount(200);
        Replica fallbackReplica = replica(2, 10);
        fallbackReplica.setVisibleVersionCount(10);
        Backend preferredBackend = backend(1, "group_a");
        Backend fallbackBackend = backend(2, "group_b");
        BackendSelection.SelectionHint hint = new BackendSelection.SelectionHint(
                "group_a", BackendSelection.Mode.REQUIRE, "test");

        BackendSelectionProvider policy = new BackendSelectionProvider() {
            @Override
            public <T> BackendSelection.CandidateSelection<T> partitionRequiredQueryCandidates(
                    BackendSelection.SelectionHint selectionHint, List<T> candidates, Function<T, Tag> tagOf) {
                List<T> preferred = new ArrayList<>();
                List<T> fallback = new ArrayList<>();
                for (T candidate : candidates) {
                    Tag tag = tagOf.apply(candidate);
                    if (tag != null && selectionHint.getPreferredKey().equals(tag.value)) {
                        preferred.add(candidate);
                    } else {
                        fallback.add(candidate);
                    }
                }
                return new BackendSelection.CandidateSelection<>(preferred, fallback);
            }
        };
        BackendSelectionManager.setProviderForTest(policy);

        SessionVariable sessionVariable = new SessionVariable();
        ConnectContext context = Mockito.mock(ConnectContext.class);
        QueryState queryState = Mockito.mock(QueryState.class);
        Mockito.when(context.getSessionVariable()).thenReturn(sessionVariable);
        Mockito.when(context.getState()).thenReturn(queryState);
        Mockito.when(context.getStatementContext()).thenReturn(new StatementContext());
        Mockito.when(context.getQueryBackendSelectionDecision()).thenReturn(hint);
        Mockito.when(context.getComputeGroupSafely()).thenReturn(null);

        SystemInfoService systemInfo = Mockito.mock(SystemInfoService.class);
        Mockito.when(systemInfo.getBackend(1L)).thenReturn(preferredBackend);
        Mockito.when(systemInfo.getBackend(2L)).thenReturn(fallbackBackend);

        LocalTablet tablet = new LocalTablet(20L);
        tablet.addReplica(preferredReplica, true);
        tablet.addReplica(fallbackReplica, true);

        boolean oldSkipCompactionSlowerReplica = Config.skip_compaction_slower_replica;
        int oldMinVersionCount = Config.min_version_count_indicate_replica_compaction_too_slow;
        try (MockedStatic<ConnectContext> mockedContext = Mockito.mockStatic(ConnectContext.class);
                MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            Config.skip_compaction_slower_replica = true;
            Config.min_version_count_indicate_replica_compaction_too_slow = 200;
            mockedContext.when(ConnectContext::get).thenReturn(context);
            mockedEnv.when(Env::getCurrentSystemInfo).thenReturn(systemInfo);

            List<TScanRangeLocations> scanRanges = createScanRanges(
                    tablet, preferredBackend, fallbackBackend);

            Assertions.assertEquals(ImmutableList.of(1L),
                    scanRanges.get(0).getLocations().stream()
                            .map(location -> location.getBackendId()).collect(Collectors.toList()));
        } finally {
            Config.skip_compaction_slower_replica = oldSkipCompactionSlowerReplica;
            Config.min_version_count_indicate_replica_compaction_too_slow = oldMinVersionCount;
        }
    }

    @Test
    void testTieAwareNoOpKeepsOriginalReplicaList() throws Exception {
        Replica first = replica(1, 10);
        Replica second = replica(2, 9);
        List<Replica> replicas = new ArrayList<>(ImmutableList.of(first, second));
        BackendSelection.SelectionHint hint = new BackendSelection.SelectionHint(
                "key_a", BackendSelection.Mode.PREFER, "test");
        BackendSelectionProvider policy = Mockito.mock(BackendSelectionProvider.class);
        Mockito.when(policy.hasQuerySelectionPreference(hint)).thenReturn(true);
        Mockito.when(policy.orderQueryCandidates(
                        Mockito.eq(hint), Mockito.anyList(), Mockito.any()))
                .thenAnswer(invocation -> invocation.getArgument(1));

        BackendSelectionManager.setProviderForTest(policy);

        Assertions.assertSame(replicas, OlapScanNode.orderReplicasForQuerySelection(
                    true, replicas, hint, replica -> Tag.DEFAULT_BACKEND_TAG));
    }

    @Test
    void testCooldownReplicaAffinityOverridesQueryAffinityOnlyWhenEnabled() throws Exception {
        Replica preferredReplica = replica(1, 10);
        Replica cooldownReplica = replica(2, 10);
        Backend preferredBackend = backend(1, "group_a");
        Backend cooldownBackend = backend(2, "group_b");
        BackendSelection.SelectionHint hint = new BackendSelection.SelectionHint(
                "group_a", BackendSelection.Mode.PREFER, "test");

        BackendSelectionProvider policy = Mockito.mock(BackendSelectionProvider.class);
        Mockito.when(policy.hasQuerySelectionPreference(hint)).thenReturn(true);
        Mockito.when(policy.orderQueryCandidates(
                        Mockito.eq(hint), Mockito.anyList(), Mockito.any()))
                .thenReturn(ImmutableList.of(preferredReplica, cooldownReplica));
        Mockito.when(policy.classifyQuerySelection(
                        Mockito.eq(hint), Mockito.anyList(), Mockito.any()))
                .thenReturn(BackendSelection.QuerySelectionResult.PREFERRED_HIT);
        BackendSelectionManager.setProviderForTest(policy);

        SessionVariable sessionVariable = new SessionVariable();
        ConnectContext context = Mockito.mock(ConnectContext.class);
        QueryState queryState = Mockito.mock(QueryState.class);
        Mockito.when(context.getSessionVariable()).thenReturn(sessionVariable);
        Mockito.when(context.getState()).thenReturn(queryState);
        Mockito.when(context.getStatementContext()).thenReturn(new StatementContext());
        Mockito.when(context.getQueryBackendSelectionDecision()).thenReturn(hint);
        Mockito.when(context.getComputeGroupSafely()).thenReturn(null);

        SystemInfoService systemInfo = Mockito.mock(SystemInfoService.class);
        Mockito.when(systemInfo.getBackend(1L)).thenReturn(preferredBackend);
        Mockito.when(systemInfo.getBackend(2L)).thenReturn(cooldownBackend);

        try (MockedStatic<ConnectContext> mockedContext = Mockito.mockStatic(ConnectContext.class);
                MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedContext.when(ConnectContext::get).thenReturn(context);
            mockedEnv.when(Env::getCurrentSystemInfo).thenReturn(systemInfo);

            sessionVariable.enableCooldownReplicaAffinity = false;
            List<TScanRangeLocations> withoutCooldown = createScanRanges(
                    preferredReplica, cooldownReplica, preferredBackend, cooldownBackend);
            Assertions.assertEquals(ImmutableList.of(1L, 2L),
                    withoutCooldown.get(0).getLocations().stream()
                            .map(location -> location.getBackendId()).collect(Collectors.toList()));

            sessionVariable.enableCooldownReplicaAffinity = true;
            List<TScanRangeLocations> withCooldown = createScanRanges(
                    preferredReplica, cooldownReplica, preferredBackend, cooldownBackend);
            Assertions.assertEquals(ImmutableList.of(2L),
                    withCooldown.get(0).getLocations().stream()
                            .map(location -> location.getBackendId()).collect(Collectors.toList()));
        }
    }

    @Test
    void testQueryDisabledBackendExcludedFromScanCandidates() throws Exception {
        Replica replica = replica(1, 10);
        Backend disabledBackend = backend(1, "group_a");
        disabledBackend.setQueryDisabled(true);
        Backend otherBackend = backend(2, "group_b");

        Tablet tablet = Mockito.mock(Tablet.class);
        Mockito.when(tablet.getId()).thenReturn(20L);
        Mockito.when(tablet.getQueryableReplicas(
                        Mockito.eq(10L), Mockito.anyMap(), Mockito.eq(false)))
                .thenReturn(new ArrayList<>(ImmutableList.of(replica)));
        Mockito.when(tablet.getCooldownReplicaId()).thenReturn(-1L);

        UserException ex = Assertions.assertThrows(UserException.class,
                () -> createScanRanges(tablet, disabledBackend, otherBackend));
        Assertions.assertTrue(ex.getMessage().contains("has no queryable replicas"));
    }

    @Test
    void testCooldownReplicaOnQueryDisabledBackendIsNotNarrowed() throws Exception {
        Replica preferredReplica = replica(1, 10);
        Replica cooldownReplica = replica(2, 10);
        Backend preferredBackend = backend(1, "group_a");
        Backend cooldownBackend = backend(2, "group_b");
        cooldownBackend.setQueryDisabled(true);

        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.enableCooldownReplicaAffinity = true;
        ConnectContext context = Mockito.mock(ConnectContext.class);
        QueryState queryState = Mockito.mock(QueryState.class);
        Mockito.when(context.getSessionVariable()).thenReturn(sessionVariable);
        Mockito.when(context.getState()).thenReturn(queryState);
        Mockito.when(context.getStatementContext()).thenReturn(new StatementContext());
        Mockito.when(context.getComputeGroupSafely()).thenReturn(null);

        try (MockedStatic<ConnectContext> mockedContext = Mockito.mockStatic(ConnectContext.class)) {
            mockedContext.when(ConnectContext::get).thenReturn(context);

            List<TScanRangeLocations> scanRanges = createScanRanges(
                    preferredReplica, cooldownReplica, preferredBackend, cooldownBackend);

            Assertions.assertEquals(ImmutableList.of(1L),
                    scanRanges.get(0).getLocations().stream()
                            .map(location -> location.getBackendId()).collect(Collectors.toList()));
        }
    }

    private List<TScanRangeLocations> createScanRanges(Replica preferredReplica, Replica cooldownReplica,
            Backend preferredBackend, Backend cooldownBackend) throws Exception {
        Tablet tablet = Mockito.mock(Tablet.class);
        Mockito.when(tablet.getId()).thenReturn(20L);
        Mockito.when(tablet.getQueryableReplicas(
                        Mockito.eq(10L), Mockito.anyMap(), Mockito.eq(false)))
                .thenReturn(new ArrayList<>(ImmutableList.of(cooldownReplica, preferredReplica)));
        Mockito.when(tablet.getCooldownReplicaId()).thenReturn(cooldownReplica.getId());
        return createScanRanges(tablet, preferredBackend, cooldownBackend);
    }

    private List<TScanRangeLocations> createScanRanges(Tablet tablet,
            Backend preferredBackend, Backend fallbackBackend) throws Exception {
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(table.getDistributionColumnNames()).thenReturn(Collections.emptySet());
        Mockito.when(table.getAllBackendsByAllCluster()).thenReturn(
                ImmutableMap.of(preferredBackend.getId(), preferredBackend,
                        fallbackBackend.getId(), fallbackBackend));

        TupleDescriptor descriptor = Mockito.mock(TupleDescriptor.class);
        Mockito.when(descriptor.getId()).thenReturn(new TupleId(1));
        Mockito.when(descriptor.getTable()).thenReturn(table);
        Mockito.when(descriptor.getSlots()).thenReturn(new ArrayList<>());
        OlapScanNode scanNode = new OlapScanNode(
                new PlanNodeId(1), descriptor, "cooldown-affinity-test", ScanContext.EMPTY);

        Partition partition = Mockito.mock(Partition.class);
        Mockito.when(partition.getId()).thenReturn(10L);
        Mockito.when(partition.getVisibleVersion()).thenReturn(10L);

        Deencapsulation.setField(scanNode, "tabletId2BucketSeq", ImmutableMap.of(20L, 0));
        Deencapsulation.invoke(scanNode, "addScanRangeLocations",
                partition, ImmutableList.of(tablet), ImmutableMap.of());
        return scanNode.getScanRangeLocations(0);
    }

    private Backend backend(long backendId, String group) throws Exception {
        Backend backend = new Backend(backendId, "127.0.0." + backendId, 9050);
        backend.setAlive(true);
        backend.setBePort(9060);
        backend.setTagMap(ImmutableMap.of(Tag.TYPE_LOCATION, group));
        return backend;
    }

    private Replica replica(long replicaId, long version) {
        return new LocalReplica(replicaId, replicaId, Replica.ReplicaState.NORMAL, version, 0);
    }

    @Test
    void testQuerySelectionExplainAggregatesTabletOutcomes() {
        OlapScanNode scanNode = Mockito.mock(OlapScanNode.class, Mockito.CALLS_REAL_METHODS);
        BackendSelection.SelectionHint hint = new BackendSelection.SelectionHint(
                "key_a", BackendSelection.Mode.PREFER, "test");

        Assertions.assertEquals("", scanNode.getQuerySelectionExplain("  "));
        scanNode.recordQuerySelectionResult(hint, BackendSelection.QuerySelectionResult.PREFERRED_HIT);
        scanNode.recordQuerySelectionResult(hint, BackendSelection.QuerySelectionResult.PREFERRED_HIT);
        scanNode.recordQuerySelectionResult(
                hint, BackendSelection.QuerySelectionResult.FALLBACK_PREFERRED_UNAVAILABLE);

        Assertions.assertEquals("  QUERY BACKEND SELECTION: preferred=key_a, mode=PREFER, "
                        + "preferred_available_tablets=2, fallback_preferred_unavailable_tablets=1\n",
                        scanNode.getQuerySelectionExplain("  "));
    }
}
