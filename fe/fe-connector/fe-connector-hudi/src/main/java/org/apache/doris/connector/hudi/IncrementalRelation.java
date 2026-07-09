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

package org.apache.doris.connector.hudi;

import org.apache.doris.connector.api.DorisConnectorException;

import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.table.timeline.TimelineUtils.HollowCommitHandling;

import java.util.List;

/**
 * Selects the files an {@code @incr(...)} incremental read must scan over a resolved {@code (begin, end]}
 * commit-time window. Connector-internal port of legacy {@code datasource.hudi.source.IncrementalRelation},
 * with the split type re-homed from fe-core {@code spi.Split}/{@code HudiSplit} to the connector's
 * {@link HudiScanRange} (fe-core is off-limits across the plugin boundary).
 *
 * <p><b>Window is already resolved.</b> Unlike legacy — where each relation constructor re-parsed / re-resolved
 * the window from {@code optParams} — the ported relations take the ALREADY-RESOLVED {@code startTs}/{@code endTs}
 * that {@link HudiConnectorMetadata#resolveTimeTravel} stamped on the {@link HudiTableHandle}, and do FILE
 * SELECTION only. This keeps a SINGLE window authority (the handle) feeding both the file set here and the
 * synthetic {@code _hoodie_commit_time} row filter a later step injects, so the two can never disagree on the
 * window; the dead {@code MORIncrementalRelation:92} sentinel bug (which tested {@code latestTime} instead of the
 * end) vanishes by construction because no sentinel re-resolution survives in the relation.
 *
 * <p>Two shapes, mirroring legacy's own asymmetry: a COW relation yields native {@link HudiScanRange}s directly
 * ({@link #collectSplits()}); a MOR relation yields merged {@link FileSlice}s ({@link #collectFileSlices()}) that
 * the scan planner later turns into JNI ranges at {@code endTs}. Each relation supports only its own shape and
 * throws {@link UnsupportedOperationException} for the other.
 *
 * <p><b>DORMANT.</b> Nothing wires these relations into {@code HudiScanPlanProvider.planScan} yet (that is the
 * next step); they are ported + unit-testable in isolation. The empty-completed-timeline case routes to
 * {@link EmptyIncrementalRelation} in the scan planner (legacy {@code LogicalHudiScan.withScanParams:261-262}),
 * so COW/MOR are never constructed on an empty timeline and their meta-fields guard fires only for a non-empty
 * one (legacy parity).
 */
interface IncrementalRelation {

    /** Merged file slices at {@code endTs} for the MOR path (COW throws {@link UnsupportedOperationException}). */
    List<FileSlice> collectFileSlices();

    /** Native base-file ranges for the COW path (MOR throws {@link UnsupportedOperationException}). */
    List<HudiScanRange> collectSplits();

    /**
     * Whether the window fell back to a full-table scan (archived instant / missing file). The scan planner
     * checks this BEFORE calling {@link #collectSplits()}/{@link #collectFileSlices()} and, when true, degrades
     * to the normal latest-snapshot partition scan instead of the incremental path (legacy
     * {@code HudiScanNode.getSplits:470}).
     */
    boolean fallbackFullTableScan();

    /** The resolved inclusive window end (the MOR-JNI merge instant); {@code "000"} for the empty relation. */
    String getEndTs();

    /**
     * Fail loud when a table has meta fields disabled, byte-for-byte with legacy
     * ({@code COWIncrementalRelation:81-83} / {@code MORIncrementalRelation:73-75}) but re-typed to the
     * connector's {@link DorisConnectorException} (the user-visible message is preserved, exactly as INC-1
     * re-typed the begin-required throw). Ported here from INC-1's deferral list. Because the empty timeline
     * routes to {@link EmptyIncrementalRelation} upstream, this is reached only for a non-empty timeline
     * (legacy's "non-empty only" semantics), so it stays the FIRST guard in each COW/MOR constructor.
     */
    static void checkIncrementalMetaFields(boolean populateMetaFields) {
        if (!populateMetaFields) {
            throw new DorisConnectorException(
                    "Incremental queries are not supported when meta fields are disabled");
        }
    }

    /**
     * Fail loud when the {@code USE_TRANSITION_TIME} hollow-commit policy meets a full-table-scan fallback,
     * byte-for-byte with legacy ({@code COWIncrementalRelation:178-180} inside {@code shouldFullTableScan},
     * {@code MORIncrementalRelation:104-106} in the constructor). Shared so both relations emit the identical
     * message.
     */
    static void checkStateTransitionTimeFullTableScan(HollowCommitHandling policy, boolean fullTableScan) {
        if (policy == HollowCommitHandling.USE_TRANSITION_TIME && fullTableScan) {
            throw new DorisConnectorException(
                    "Cannot use stateTransitionTime while enables full table scan");
        }
    }

    /**
     * Fail loud when a resolved window fell back to a full-table scan, byte-for-byte with legacy
     * ({@code COWIncrementalRelation:206} / {@code MORIncrementalRelation:177}), re-typed to the connector's
     * {@link DorisConnectorException}. A DEFENSIVE invariant: the scan planner is contracted to check
     * {@link #fallbackFullTableScan()} and degrade to the snapshot path BEFORE calling {@link #collectSplits()}
     * / {@link #collectFileSlices()}, so this normally never fires. Extracted (like the other guards) so the
     * message is byte-verifiable offline.
     */
    static void checkNotFullTableScan(boolean fullTableScan) {
        if (fullTableScan) {
            throw new DorisConnectorException("Fallback to full table scan");
        }
    }

    /**
     * The archival half of legacy COW {@code shouldFullTableScan} ({@code COWIncrementalRelation:177-182}): a
     * full-table scan is required when the fallback is enabled AND either bound is archived (before the
     * timeline start); under {@code USE_TRANSITION_TIME} that combination is rejected instead. Pure booleans in,
     * so it is unit-testable as a matrix; the file-existence half of {@code shouldFullTableScan} stays in the
     * COW relation because it probes the filesystem.
     */
    static boolean decideArchivalFullTableScan(boolean fallbackEnabled, boolean startArchived,
            boolean endArchived, HollowCommitHandling policy) {
        if (fallbackEnabled && (startArchived || endArchived)) {
            checkStateTransitionTimeFullTableScan(policy, true);
            return true;
        }
        return false;
    }
}
