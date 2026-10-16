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

import org.apache.doris.datasource.scan.FederationBackendPolicy;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.resource.BackendSelection;
import org.apache.doris.resource.BackendSelectionManager;
import org.apache.doris.resource.spi.BackendSelectionProvider;
import org.apache.doris.system.Backend;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;

class FileLoadScanNodeBackendSelectionTest {

    @AfterEach
    void resetBackendSelectionProvider() {
        BackendSelectionManager.resetProviderForTest();
    }


    @Test
    void testApplyLoadSelectionReordersCompleteBackendPolicyCandidates() throws Exception {
        ConnectContext context = new ConnectContext();
        Backend first = new Backend(1L, "127.0.0.1", 9050);
        Backend preferred = new Backend(2L, "127.0.0.2", 9050);
        List<Backend> candidates = ImmutableList.of(first, preferred);
        BackendSelection.SelectionHint hint = new BackendSelection.SelectionHint(
                "key_a", BackendSelection.Mode.PREFER, "persisted");
        context.recordLoadBackendSelectionDecision(hint);

        FederationBackendPolicy backendPolicy = Mockito.mock(FederationBackendPolicy.class);
        Mockito.doReturn(candidates).when(backendPolicy).getBackends();
        BackendSelectionProvider selectionPolicy = Mockito.mock(BackendSelectionProvider.class);
        Mockito.when(selectionPolicy.hasLoadSelectionPreference(hint)).thenReturn(true);
        Mockito.when(selectionPolicy.orderLoadCandidates(hint, candidates))
                .thenReturn(ImmutableList.of(preferred, first));

        BackendSelectionManager.setProviderForTest(selectionPolicy);

        FileLoadScanNode.applyLoadBackendSelection(context, backendPolicy);

        Mockito.verify(backendPolicy).replaceBackendOrder(ImmutableList.of(preferred, first));
        Assertions.assertTrue(
                context.getBackendSelectionProfile().getLoadSummary().contains("coordinator_backend=2"));
    }
}
