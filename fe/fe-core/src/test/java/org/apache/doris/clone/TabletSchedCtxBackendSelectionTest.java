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

import org.apache.doris.catalog.LocalReplica;
import org.apache.doris.catalog.Replica;
import org.apache.doris.common.Config;
import org.apache.doris.resource.BackendSelectionManager;
import org.apache.doris.resource.spi.BackendSelectionProvider;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

class TabletSchedCtxBackendSelectionTest {

    @AfterEach
    void resetBackendSelectionProvider() {
        BackendSelectionManager.resetProviderForTest();
    }

    @Test
    void testRepairSourceCandidateOrderIsCopied() throws Exception {
        Replica replica = new LocalReplica();
        List<Replica> candidates = Collections.singletonList(replica);
        BackendSelectionProvider policy = new BackendSelectionProvider() {
            @Override
            public List<Replica> orderRepairSourceCandidates(List<Replica> healthyCandidates, long destBackendId) {
                return Collections.unmodifiableList(healthyCandidates);
            }
        };

        BackendSelectionManager.setProviderForTest(policy);
        List<Replica> orderedCandidates = TabletSchedCtx.orderRepairSourceCandidates(candidates, 1L);
        orderedCandidates.add(new LocalReplica());
        Assertions.assertEquals(2, orderedCandidates.size());
    }

    @Test
    void testRepairSourceSelectionConfigGate() {
        boolean oldConfig = Config.enable_repair_source_backend_selection;
        try {
            BackendSelectionProvider enabledPolicy = new BackendSelectionProvider() {
                @Override
                public boolean isRepairSourceSelectionEnabled() {
                    return true;
                }
            };

            Config.enable_repair_source_backend_selection = false;
            BackendSelectionManager.setProviderForTest(enabledPolicy);
            Assertions.assertFalse(TabletSchedCtx.isRepairSourceSelectionEnabled(1L));

            Config.enable_repair_source_backend_selection = true;
            Assertions.assertTrue(TabletSchedCtx.isRepairSourceSelectionEnabled(1L));
            Assertions.assertFalse(TabletSchedCtx.isRepairSourceSelectionEnabled(-1L));
        } finally {
            Config.enable_repair_source_backend_selection = oldConfig;
        }
    }
}
