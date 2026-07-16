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

import org.apache.hudi.common.table.timeline.TimelineUtils.HollowCommitHandling;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.UnaryOperator;

/**
 * Tests the OFFLINE-verifiable surface of the ported {@code @incr} IncrementalRelation family (INC-2): the pure
 * fail-loud guards extracted onto {@link IncrementalRelation} and the {@link EmptyIncrementalRelation}. Each
 * assertion pins WHY the behavior matters.
 *
 * <p><b>Coverage scope (Rule 12 — no over-claim).</b> The COW/MOR relation CONSTRUCTORS do all timeline +
 * metadata + filesystem work EAGERLY on a live {@code HoodieTableMetaClient}, and the connector deliberately has
 * no Mockito / no hudi write deps, so the file-SELECTION pipeline (commit-range selection, write-stat &rarr;
 * {@link HudiScanRange} mapping, file-slice selection, {@code fs.exists} full-table-scan probes, the meta-fields
 * guard on a REAL disabled table, and the {@code USE_TRANSITION_TIME} completion-time END axis) is inherently
 * e2e-only and is DEFERRED to the flip-time e2e (design §5 now mandates a meta-fields-disabled fixture and a
 * USE_TRANSITION_TIME completion-axis fixture). Until that e2e lands, the completion-time END axis is UNVERIFIED
 * at unit level (the stubbed executor swallows it). These unit tests cover ONLY the pure decisions they name;
 * they do NOT prove file selection or the axis resolution.
 */
public class HudiIncrementalRelationTest {

    // ── meta-fields fail-loud (ported from INC-1 deferral; byte-for-byte message) ────────────────────────────

    @Test
    public void metaFieldsDisabledThrowsByteForByteLegacyMessage() {
        // Legacy COW:81-83 / MOR:73-75 reject incremental on a meta-fields-disabled table. The message must be
        // byte-for-byte (re-typed to DorisConnectorException, the connector's fail-loud type) so it reaches the
        // user verbatim. This guard is the FIRST check in each ported COW/MOR constructor.
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> IncrementalRelation.checkIncrementalMetaFields(false));
        Assertions.assertEquals(
                "Incremental queries are not supported when meta fields are disabled", ex.getMessage());
    }

    @Test
    public void metaFieldsEnabledIsNoOp() {
        // A normal (meta-fields-enabled) table must pass the guard. Guards a mutation that inverts the condition.
        Assertions.assertDoesNotThrow(() -> IncrementalRelation.checkIncrementalMetaFields(true));
    }

    // ── state-transition-time + full-table-scan rejection (byte-for-byte message) ────────────────────────────

    @Test
    public void stateTransitionTimeWithFullTableScanThrows() {
        // Legacy COW:178-180 / MOR:104-106 reject USE_TRANSITION_TIME combined with a full-table-scan fallback.
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> IncrementalRelation.checkStateTransitionTimeFullTableScan(
                        HollowCommitHandling.USE_TRANSITION_TIME, true));
        Assertions.assertEquals(
                "Cannot use stateTransitionTime while enables full table scan", ex.getMessage());
    }

    @Test
    public void stateTransitionTimeWithoutFullTableScanIsNoOp() {
        // USE_TRANSITION_TIME alone (no full-table scan) is fine — only the COMBINATION is rejected.
        Assertions.assertDoesNotThrow(() -> IncrementalRelation.checkStateTransitionTimeFullTableScan(
                HollowCommitHandling.USE_TRANSITION_TIME, false));
    }

    @Test
    public void fullTableScanUnderDefaultPolicyIsNoOp() {
        // A full-table scan under the default FAIL policy must NOT throw — the throw is specific to
        // USE_TRANSITION_TIME. Guards a mutation that drops the policy condition.
        Assertions.assertDoesNotThrow(() -> IncrementalRelation.checkStateTransitionTimeFullTableScan(
                HollowCommitHandling.FAIL, true));
    }

    // ── archival full-table-scan decision matrix (COW pure gate) ─────────────────────────────────────────────

    @Test
    public void archivalFullTableScanRequiresFallbackEnabled() {
        // Fallback disabled -> never a full-table scan on archival, even if a bound is archived. Guards the
        // fallback-gates-everything semantics (legacy COW:177).
        Assertions.assertFalse(IncrementalRelation.decideArchivalFullTableScan(
                false, true, true, HollowCommitHandling.FAIL));
    }

    @Test
    public void archivalFullTableScanRequiresAnArchivedBound() {
        // Fallback enabled but NEITHER bound archived -> the archival gate does not fire (the file-existence
        // probe, not tested here, may still trigger). Guards a mutation that drops the archived condition.
        Assertions.assertFalse(IncrementalRelation.decideArchivalFullTableScan(
                true, false, false, HollowCommitHandling.FAIL));
    }

    @Test
    public void archivalFullTableScanFiresWhenStartOrEndArchived() {
        // Either archived bound (with fallback) triggers a full-table scan under a non-transition policy.
        Assertions.assertTrue(IncrementalRelation.decideArchivalFullTableScan(
                true, true, false, HollowCommitHandling.FAIL));
        Assertions.assertTrue(IncrementalRelation.decideArchivalFullTableScan(
                true, false, true, HollowCommitHandling.BLOCK));
    }

    @Test
    public void archivalFullTableScanRejectsStateTransitionTime() {
        // The archival trigger under USE_TRANSITION_TIME is rejected (legacy COW:178-180), not returned as true.
        Assertions.assertThrows(DorisConnectorException.class,
                () -> IncrementalRelation.decideArchivalFullTableScan(
                        true, true, false, HollowCommitHandling.USE_TRANSITION_TIME));
    }

    @Test
    public void stateTransitionTimeRejectionIsGatedOnAnArchivedBound() {
        // USE_TRANSITION_TIME alone must NOT reject: legacy nests the transition-time throw INSIDE the archival
        // trigger `fallback && (startArchived||endArchived)` (COW:177-181), so a non-archived window returns false
        // even under USE_TRANSITION_TIME. Kills a mutation that hoists the state-transition check above the
        // archival guard (rejecting any USE_TRANSITION_TIME+fallback window).
        Assertions.assertFalse(IncrementalRelation.decideArchivalFullTableScan(
                true, false, false, HollowCommitHandling.USE_TRANSITION_TIME));
    }

    // ── fallback-to-full-table-scan defensive throw (byte-for-byte message) ──────────────────────────────────

    @Test
    public void fullTableScanFallbackThrowsByteForByteLegacyMessage() {
        // The defensive guard COW.collectSplits / MOR.collectFileSlices fire when the window fell back to a full
        // scan (legacy COW:206 / MOR:177). Byte-for-byte message, re-typed to DorisConnectorException. The scan
        // planner is contracted to degrade before calling collect*, so this rarely fires — but the message must
        // still reach the user verbatim if it does.
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> IncrementalRelation.checkNotFullTableScan(true));
        Assertions.assertEquals("Fallback to full table scan", ex.getMessage());
    }

    @Test
    public void notFullTableScanIsNoOp() {
        // A window that did NOT fall back must pass the guard (the normal incremental path). Guards a condition
        // inversion.
        Assertions.assertDoesNotThrow(() -> IncrementalRelation.checkNotFullTableScan(false));
    }

    // ── hollow-commit policy → HollowCommitHandling enum (byte-faithful to legacy COW:74-75 / MOR:76-77) ─────

    @Test
    public void hollowCommitHandlingDefaultsToFailWhenPolicyAbsent() {
        // No policy param -> FAIL (legacy getOrDefault(policyKey, "FAIL")). This is the default axis
        // (requested-time); guards a mutation that drops the "FAIL" default (which would NPE valueOf(null)).
        Assertions.assertEquals(HollowCommitHandling.FAIL,
                IncrementalRelation.hollowCommitHandling(Collections.emptyMap()));
    }

    @Test
    public void hollowCommitHandlingReadsUseTransitionTime() {
        // The one non-default value that matters: USE_TRANSITION_TIME switches the relation's own file selection
        // to the completion-time axis (COW:93-97 / MOR:100-104), which must match the END axis resolveIncremental
        // resolved on the SAME policy. Guards dropping/misreading the policy key.
        Map<String, String> params = new HashMap<>();
        params.put("hoodie.read.timeline.holes.resolution.policy", "USE_TRANSITION_TIME");
        Assertions.assertEquals(HollowCommitHandling.USE_TRANSITION_TIME,
                IncrementalRelation.hollowCommitHandling(params));
    }

    @Test
    public void hollowCommitHandlingThrowsOnBogusPolicyLikeLegacy() {
        // A bogus policy value reaches HollowCommitHandling.valueOf and THROWS IllegalArgumentException — legacy
        // parity (legacy also valueOf's it, COW:74-75 / MOR:76-77). Same terminal error, one phase later (at
        // planScan rather than the relation ctor). Guards a mutation that would swallow the bad value.
        Map<String, String> params = new HashMap<>();
        params.put("hoodie.read.timeline.holes.resolution.policy", "NOT_A_POLICY");
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> IncrementalRelation.hollowCommitHandling(params));
    }

    // ── empty relation (empty completed timeline) ────────────────────────────────────────────────────────────

    @Test
    public void emptyRelationSelectsNothing() {
        // The empty-timeline relation selects no files on either shape and never falls back. Its end bound is the
        // legacy "000" sentinel. Guards against a mutation returning non-empty / a non-"000" bound.
        EmptyIncrementalRelation empty = new EmptyIncrementalRelation();
        Assertions.assertTrue(empty.collectSplits(UnaryOperator.identity()).isEmpty(),
                "empty relation must select no splits");
        Assertions.assertTrue(empty.collectFileSlices().isEmpty(), "empty relation must select no file slices");
        Assertions.assertFalse(empty.fallbackFullTableScan(), "empty relation never falls back to a full scan");
        Assertions.assertEquals("000", empty.getEndTs(), "empty relation end bound is the legacy \"000\"");
    }
}
