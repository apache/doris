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

import org.apache.doris.catalog.LocalReplica;
import org.apache.doris.catalog.Replica;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.resource.BackendSelection;
import org.apache.doris.resource.BackendSelectionPolicy;
import org.apache.doris.resource.BackendSelectionPolicyFactory;
import org.apache.doris.resource.Tag;
import org.apache.doris.resource.computegroup.ComputeGroup;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

class OlapScanNodeBackendSelectionConfigTest {
    @Test
    void testResourceTagLocationCheckConfigGate() {
        boolean oldConfig = Config.enable_resource_tag_location_check;
        try {
            Config.enable_resource_tag_location_check = false;
            Assertions.assertTrue(OlapScanNode.shouldFilterReplicaByResourceTag(
                    true, true, ComputeGroup.INVALID_COMPUTE_GROUP, "rg_a"));
            ComputeGroup computeGroup = new ComputeGroup("rg_a", "rg_a", null);
            Assertions.assertFalse(OlapScanNode.shouldFilterReplicaByResourceTag(false, true, computeGroup, "rg_b"));

            Config.enable_resource_tag_location_check = true;
            Assertions.assertTrue(OlapScanNode.shouldFilterReplicaByResourceTag(
                    true, false, ComputeGroup.INVALID_COMPUTE_GROUP, "rg_a"));
            Assertions.assertTrue(OlapScanNode.shouldFilterReplicaByResourceTag(false, true, computeGroup, "rg_b"));
            Assertions.assertFalse(OlapScanNode.shouldFilterReplicaByResourceTag(false, true, computeGroup, "rg_a"));
        } finally {
            Config.enable_resource_tag_location_check = oldConfig;
        }
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
        BackendSelectionPolicy policy = Mockito.mock(BackendSelectionPolicy.class);
        Mockito.when(policy.hasQuerySelectionPreference(hint)).thenReturn(true);
        Mockito.when(policy.orderQueryCandidates(
                        Mockito.eq(hint), Mockito.anyList(), Mockito.any()))
                .thenAnswer(invocation -> {
                    List<Replica> ordered = new ArrayList<>(invocation.getArgument(1));
                    Collections.reverse(ordered);
                    return ordered;
                });

        try (MockedStatic<BackendSelectionPolicyFactory> mockedFactory =
                Mockito.mockStatic(BackendSelectionPolicyFactory.class)) {
            mockedFactory.when(BackendSelectionPolicyFactory::get).thenReturn(policy);

            List<Replica> ordered = OlapScanNode.orderReplicasForQuerySelection(
                    true, replicas, hint, replica -> Tag.DEFAULT_BACKEND_TAG);

            Assertions.assertEquals(ImmutableList.of(second, first, fourth, third), ordered);
            Mockito.verify(policy, Mockito.times(2)).orderQueryCandidates(
                    Mockito.eq(hint), Mockito.anyList(), Mockito.any());
        }
    }

    @Test
    void testNonSkipMissingVersionUsesExistingQuerySelectionPath() throws Exception {
        Replica first = replica(1, 10);
        Replica second = replica(2, 10);
        List<Replica> replicas = new ArrayList<>(ImmutableList.of(first, second));
        BackendSelection.SelectionHint hint = new BackendSelection.SelectionHint(
                "key_a", BackendSelection.Mode.PREFER, "test");
        BackendSelectionPolicy policy = Mockito.mock(BackendSelectionPolicy.class);
        Mockito.when(policy.hasQuerySelectionPreference(hint)).thenReturn(true);
        Mockito.when(policy.orderQueryCandidates(
                        Mockito.eq(hint), Mockito.anyList(), Mockito.any()))
                .thenAnswer(invocation -> invocation.getArgument(1));

        try (MockedStatic<BackendSelectionPolicyFactory> mockedFactory =
                Mockito.mockStatic(BackendSelectionPolicyFactory.class)) {
            mockedFactory.when(BackendSelectionPolicyFactory::get).thenReturn(policy);

            List<Replica> ordered = OlapScanNode.orderReplicasForQuerySelection(
                    false, replicas, hint, replica -> Tag.DEFAULT_BACKEND_TAG);

            Assertions.assertEquals(replicas.size(), ordered.size());
            Mockito.verify(policy).hasQuerySelectionPreference(hint);
            Mockito.verify(policy).orderQueryCandidates(
                    Mockito.eq(hint), Mockito.anyList(), Mockito.any());
        }
    }

    @Test
    void testTieAwareNoOpKeepsOriginalReplicaList() throws Exception {
        Replica first = replica(1, 10);
        Replica second = replica(2, 9);
        List<Replica> replicas = new ArrayList<>(ImmutableList.of(first, second));
        BackendSelection.SelectionHint hint = new BackendSelection.SelectionHint(
                "key_a", BackendSelection.Mode.PREFER, "test");
        BackendSelectionPolicy policy = Mockito.mock(BackendSelectionPolicy.class);
        Mockito.when(policy.hasQuerySelectionPreference(hint)).thenReturn(true);
        Mockito.when(policy.orderQueryCandidates(
                        Mockito.eq(hint), Mockito.anyList(), Mockito.any()))
                .thenAnswer(invocation -> invocation.getArgument(1));

        try (MockedStatic<BackendSelectionPolicyFactory> mockedFactory =
                Mockito.mockStatic(BackendSelectionPolicyFactory.class)) {
            mockedFactory.when(BackendSelectionPolicyFactory::get).thenReturn(policy);

            Assertions.assertSame(replicas, OlapScanNode.orderReplicasForQuerySelection(
                    true, replicas, hint, replica -> Tag.DEFAULT_BACKEND_TAG));
        }
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
                        + "preferred_hit_tablets=2, fallback_preferred_unavailable_tablets=1\n",
                scanNode.getQuerySelectionExplain("  "));
    }
}
