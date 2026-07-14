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

package org.apache.doris.nereids.trees.plans.distribute.worker;

import org.apache.doris.common.Config;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.resource.BackendSelection;
import org.apache.doris.resource.BackendSelectionPolicy;
import org.apache.doris.resource.BackendSelectionPolicyFactory;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TScanRangeLocation;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

class LoadBalanceScanWorkerSelectorBackendSelectionTest {
    @Test
    void testLoadSelectionOrdersNereidsReplicaCandidates() {
        String oldCloudUniqueId = Config.cloud_unique_id;
        String oldDeployMode = Config.deploy_mode;
        ConnectContext context = new ConnectContext();
        try {
            Config.cloud_unique_id = "";
            Config.deploy_mode = "";
            Backend first = backend(1L);
            Backend preferred = backend(2L);
            DistributedPlanWorkerManager workerManager = Mockito.mock(DistributedPlanWorkerManager.class);
            Mockito.when(workerManager.getWorker(0L, 1L)).thenReturn(new BackendWorker(0L, first));
            Mockito.when(workerManager.getWorker(0L, 2L)).thenReturn(new BackendWorker(0L, preferred));
            BackendSelectionPolicy policy = new BackendSelectionPolicy() {
                @Override
                public boolean isLoadSelectionEnabled(ConnectContext ignored) {
                    return true;
                }

                @Override
                public BackendSelection.SelectionHint getLoadSelectionHint(ConnectContext ignored) {
                    return new BackendSelection.SelectionHint("preferred", BackendSelection.Mode.PREFER, "test");
                }

                @Override
                public boolean hasLoadSelectionPreference(BackendSelection.SelectionHint hint) {
                    return true;
                }

                @Override
                public List<Backend> orderLoadCandidates(BackendSelection.SelectionHint hint,
                        List<Backend> candidates) {
                    return Arrays.asList(preferred, first);
                }
            };

            try (MockedStatic<BackendSelectionPolicyFactory> mockedFactory =
                    Mockito.mockStatic(BackendSelectionPolicyFactory.class)) {
                mockedFactory.when(BackendSelectionPolicyFactory::get).thenReturn(policy);
                LoadBalanceScanWorkerSelector selector =
                        new LoadBalanceScanWorkerSelector(workerManager, context, true);
                List<TScanRangeLocation> ordered = selector.orderLoadReplicas(
                        Arrays.asList(location(1L), location(2L)), 0L);

                Assertions.assertEquals(2L, ordered.get(0).getBackendId());
                Assertions.assertEquals(1L, ordered.get(1).getBackendId());
            }
        } finally {
            Config.cloud_unique_id = oldCloudUniqueId;
            Config.deploy_mode = oldDeployMode;
        }
    }

    @Test
    void testQueryJobDoesNotApplyLoadSelection() {
        DistributedPlanWorkerManager workerManager = Mockito.mock(DistributedPlanWorkerManager.class);
        LoadBalanceScanWorkerSelector selector = new LoadBalanceScanWorkerSelector(
                workerManager, new ConnectContext(), false);
        List<TScanRangeLocation> locations = Collections.singletonList(location(1L));

        Assertions.assertSame(locations, selector.orderLoadReplicas(locations, 0L));
        Mockito.verifyNoInteractions(workerManager);
    }

    private static Backend backend(long id) {
        Backend backend = new Backend(id, "127.0.0." + id, 9050);
        backend.setAlive(true);
        return backend;
    }

    private static TScanRangeLocation location(long backendId) {
        TScanRangeLocation location = new TScanRangeLocation(new TNetworkAddress("127.0.0.1", 9060));
        location.setBackendId(backendId);
        return location;
    }
}
