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

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class BackendSelectionProfileTest {
    @Test
    void testQueryAndLoadSummariesRemainIndependent() throws Exception {
        BackendSelection.SelectionHint queryHint = new BackendSelection.SelectionHint(
                "query_group", BackendSelection.Mode.PREFER, "test");
        BackendSelection.SelectionHint loadHint = new BackendSelection.SelectionHint(
                "load_group", BackendSelection.Mode.REQUIRE, "test");
        BackendSelectionProfile profile = new BackendSelectionProfile();

        profile.recordQuerySelection(queryHint, BackendSelection.QuerySelectionResult.PREFERRED_HIT);
        profile.recordQuerySelection(queryHint,
                BackendSelection.QuerySelectionResult.FALLBACK_PREFERRED_UNAVAILABLE);
        profile.recordQuerySelection(queryHint, BackendSelection.QuerySelectionResult.PREFERRED_HIT);
        profile.recordLoadCoordinator(loadHint, backend(10002L, "load_group"));

        Assertions.assertEquals(
                "preferred=query_group, mode=PREFER, selected_preferred_scan_ranges=2, "
                        + "selected_non_preferred_scan_ranges=1",
                profile.getQuerySummary());
        Assertions.assertEquals(
                "preferred=load_group, mode=REQUIRE, coordinator_backend=10002, coordinator_group=load_group",
                profile.getLoadSummary());
    }

    @Test
    void testDisabledSelectionIsNotDisplayedAndStatementStartResetsStats() throws Exception {
        ConnectContext context = new ConnectContext();
        BackendSelectionProfile profile = context.getBackendSelectionProfile();

        profile.recordQuerySelection(BackendSelection.SelectionHint.noSelection(),
                BackendSelection.QuerySelectionResult.DISABLED);
        Assertions.assertNull(profile.getQuerySummary());
        Assertions.assertNull(profile.getLoadSummary());

        profile.recordQuerySelection(new BackendSelection.SelectionHint(
                        "query_group", BackendSelection.Mode.PREFER, "test"),
                BackendSelection.QuerySelectionResult.PREFERRED_HIT);
        profile.recordLoadCoordinator(new BackendSelection.SelectionHint(
                        "load_group", BackendSelection.Mode.PREFER, "test"),
                backend(10001L, "load_group"));
        context.setStartTime();

        Assertions.assertNull(context.getBackendSelectionProfile().getQuerySummary());
        Assertions.assertNull(context.getBackendSelectionProfile().getLoadSummary());
    }

    private Backend backend(long id, String group) throws Exception {
        Backend backend = new Backend(id, "127.0.0.1", 9050);
        backend.setTagMap(ImmutableMap.of(Tag.TYPE_LOCATION, group));
        return backend;
    }
}
