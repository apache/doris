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

import org.apache.doris.system.Backend;

/** Statement-level backend selection results displayed in the query profile. */
public final class BackendSelectionProfile {
    private BackendSelection.SelectionHint queryHint;
    private long selectedPreferredScanRanges;
    private long selectedNonPreferredScanRanges;
    private BackendSelection.SelectionHint loadHint;
    private Long coordinatorBackendId;
    private String coordinatorGroup;

    public synchronized void recordQuerySelection(BackendSelection.SelectionHint hint,
            BackendSelection.QuerySelectionResult result) {
        switch (result) {
            case PREFERRED_HIT:
                queryHint = hint;
                selectedPreferredScanRanges++;
                break;
            case FALLBACK_PREFERRED_UNAVAILABLE:
                queryHint = hint;
                selectedNonPreferredScanRanges++;
                break;
            case DISABLED:
                break;
            default:
                throw new IllegalStateException("Unknown query selection result: " + result);
        }
    }

    public synchronized void recordLoadCoordinator(BackendSelection.SelectionHint hint, Backend backend) {
        if (hint == null || backend == null || coordinatorBackendId != null) {
            return;
        }
        loadHint = hint;
        coordinatorBackendId = backend.getId();
        Tag locationTag = backend.getLocationTag();
        coordinatorGroup = locationTag == null ? "N/A" : locationTag.value;
    }

    public synchronized String getQuerySummary() {
        if (queryHint == null) {
            return null;
        }
        return "preferred=" + queryHint.getPreferredKey()
                + ", mode=" + queryHint.getMode()
                + ", selected_preferred_scan_ranges=" + selectedPreferredScanRanges
                + ", selected_non_preferred_scan_ranges=" + selectedNonPreferredScanRanges;
    }

    public synchronized String getLoadSummary() {
        if (loadHint == null || coordinatorBackendId == null) {
            return null;
        }
        return "preferred=" + loadHint.getPreferredKey()
                + ", mode=" + loadHint.getMode()
                + ", coordinator_backend=" + coordinatorBackendId
                + ", coordinator_group=" + coordinatorGroup;
    }

    public synchronized void reset() {
        queryHint = null;
        selectedPreferredScanRanges = 0;
        selectedNonPreferredScanRanges = 0;
        loadHint = null;
        coordinatorBackendId = null;
        coordinatorGroup = null;
    }
}
