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

package org.apache.doris.connector.paimon;

import org.apache.doris.connector.api.DorisConnectorException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Mutation-killing tests for {@link PaimonIncrementalScanParams#validate}, the byte-faithful port of
 * legacy {@code PaimonScanNode.validateIncrementalReadParams} (lines 701-878). Each test encodes WHY
 * a rule matters (a wrong window silently reads the WRONG incremental diff -> wrong rows), and pins
 * the EXACT legacy error message so the connector's {@link DorisConnectorException} stays parity with
 * the legacy {@code UserException}. The two parameter groups (snapshot-based vs timestamp-based) are
 * mutually exclusive; the produced map carries ONLY the non-null {@code incremental-between*} keys
 * (the legacy null {@code scan.snapshot-id}/{@code scan.mode} resets are STRIPPED).
 */
public class PaimonIncrementalScanParamsTest {

    private static Map<String, String> params(String... kv) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return m;
    }

    // ==================== mutual exclusion / required-start / empty ====================

    @Test
    public void snapshotAndTimestampGroupsAreMutuallyExclusive() {
        // WHY: mixing snapshot ids and timestamps is ambiguous (two contradictory window definitions);
        // legacy rejects it outright. MUTATION: dropping the mutual-exclusion check -> one group wins
        // silently -> no throw -> red.
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> PaimonIncrementalScanParams.validate(
                        params("startSnapshotId", "1", "endSnapshotId", "2", "startTimestamp", "100")),
                "snapshot-based and timestamp-based params must be mutually exclusive");
        Assertions.assertTrue(ex.getMessage().contains("Cannot specify both snapshot-based parameters"),
                "the mutual-exclusion error must match the legacy message verbatim");
    }

    @Test
    public void emptyParamsAreInvalid() {
        // WHY: @incr with no window is meaningless; legacy fails loud rather than reading everything.
        // MUTATION: returning an empty map instead of throwing -> no throw -> red.
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> PaimonIncrementalScanParams.validate(params()),
                "no incremental params at all must be rejected");
        Assertions.assertTrue(ex.getMessage().contains("at least one valid parameter group"),
                "the empty-params error must match the legacy message");
    }

    @Test
    public void snapshotGroupRequiresStart() {
        // WHY: an incremental window needs a START; only endSnapshotId (no start) is invalid. This is
        // the snapshot-group "start required" rule (legacy line 732-734). MUTATION: not requiring start
        // -> no throw -> red.
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> PaimonIncrementalScanParams.validate(params("endSnapshotId", "5")),
                "snapshot-based incremental read must require startSnapshotId");
        Assertions.assertTrue(ex.getMessage().contains(
                        "startSnapshotId is required when using snapshot-based incremental read"),
                "the missing-start error must match the legacy message");
    }

    @Test
    public void scanModeOnlyWithBothStartAndEnd() {
        // WHY: incrementalBetweenScanMode describes HOW to diff a [start,end] range, so it is illegal
        // without BOTH ids (legacy line 738-742; here only start + scanMode, no end). MUTATION:
        // allowing scanMode with a half-open range -> no throw -> red.
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> PaimonIncrementalScanParams.validate(
                        params("startSnapshotId", "1", "incrementalBetweenScanMode", "diff")),
                "incrementalBetweenScanMode requires both start and end snapshot ids");
        Assertions.assertTrue(ex.getMessage().contains(
                        "incrementalBetweenScanMode can only be specified when"),
                "the scanMode-needs-both error must match the legacy message");
    }

    @Test
    public void timestampGroupRequiresStart() {
        // WHY: same start-required rule for the timestamp group (legacy line 793-794); only endTimestamp
        // is invalid. MUTATION: not requiring startTimestamp -> no throw -> red.
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> PaimonIncrementalScanParams.validate(params("endTimestamp", "200")),
                "timestamp-based incremental read must require startTimestamp");
        Assertions.assertTrue(ex.getMessage().contains(
                        "startTimestamp is required when using timestamp-based incremental read"),
                "the missing-start-timestamp error must match the legacy message");
    }

    @Test
    public void onlyStartSnapshotIdRequiresEnd() {
        // WHY: a snapshot-based window with start but no end is rejected (legacy line 847-849) — the
        // snapshot path has no Long.MAX_VALUE open-ended fallback (unlike timestamps). MUTATION:
        // silently allowing only-start -> no throw -> red.
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> PaimonIncrementalScanParams.validate(params("startSnapshotId", "1")),
                "snapshot-based incremental read with only start must require end");
        Assertions.assertTrue(ex.getMessage().contains(
                        "endSnapshotId is required when using snapshot-based incremental read"),
                "the missing-end error must match the legacy message");
    }

    // ==================== numeric range rules ====================

    @Test
    public void snapshotIdsMustBeNonNegative() {
        // WHY: snapshot ids are >= 0 (legacy line 748). MUTATION: dropping the >=0 check -> no throw.
        Assertions.assertThrows(DorisConnectorException.class,
                () -> PaimonIncrementalScanParams.validate(params("startSnapshotId", "-1", "endSnapshotId", "2")),
                "a negative startSnapshotId must be rejected");
    }

    @Test
    public void startSnapshotIdMustNotExceedEnd() {
        // WHY: a window must run forward: startSId <= endSId (legacy line 772). MUTATION: dropping the
        // ordering check -> an inverted window is accepted -> no throw -> red.
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> PaimonIncrementalScanParams.validate(params("startSnapshotId", "5", "endSnapshotId", "2")),
                "startSnapshotId must be <= endSnapshotId");
        Assertions.assertTrue(ex.getMessage().contains(
                        "startSnapshotId must be less than or equal to endSnapshotId"),
                "the snapshot-ordering error must match the legacy message");
    }

    @Test
    public void endTimestampMustBePositive() {
        // WHY: endTimestamp must be > 0 (strictly positive, legacy line 812 uses <= 0), distinct from
        // startTimestamp's >= 0. MUTATION: weakening to >= 0 -> endTimestamp=0 accepted -> no throw red.
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> PaimonIncrementalScanParams.validate(params("startTimestamp", "0", "endTimestamp", "0")),
                "endTimestamp must be strictly greater than 0");
        Assertions.assertTrue(ex.getMessage().contains("endTimestamp must be greater than 0"),
                "the endTimestamp-positive error must match the legacy message");
    }

    @Test
    public void startTimestampMustBeLessThanEnd() {
        // WHY: timestamp window must run forward: startTS < endTS (STRICT, legacy line 825 uses >=).
        // MUTATION: weakening to <= -> equal timestamps accepted -> no throw -> red.
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> PaimonIncrementalScanParams.validate(params("startTimestamp", "200", "endTimestamp", "200")),
                "startTimestamp must be strictly less than endTimestamp");
        Assertions.assertTrue(ex.getMessage().contains("startTimestamp must be less than endTimestamp"),
                "the timestamp-ordering error must match the legacy message");
    }

    // ==================== scanMode enum + original-case gotcha ====================

    @Test
    public void scanModeRejectsUnknownValue() {
        // WHY: scanMode is a closed enum {auto,diff,delta,changelog} (legacy line 783-785). MUTATION:
        // dropping the enum check -> a bogus mode reaches the SDK -> no throw here -> red.
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> PaimonIncrementalScanParams.validate(
                        params("startSnapshotId", "1", "endSnapshotId", "2", "incrementalBetweenScanMode", "bogus")),
                "an unknown incrementalBetweenScanMode must be rejected");
        Assertions.assertTrue(ex.getMessage().contains(
                        "incrementalBetweenScanMode must be one of: auto, diff, delta, changelog"),
                "the scanMode-enum error must match the legacy message");
    }

    @Test
    public void scanModeValidatedCaseInsensitivelyButEmittedOriginalCase() {
        // WHY (parity gotcha): legacy validates the scan mode LOWERCASED (line 782) but emits the
        // ORIGINAL-CASE value (line 859-860 puts params.get(...) verbatim, not the lowercased copy). So
        // "DELTA" passes validation AND is emitted as "DELTA" (not "delta"). MUTATION: emitting the
        // lowercased copy -> value == "delta" -> red; failing to accept upper-case at all -> throw red.
        Map<String, String> out = PaimonIncrementalScanParams.validate(
                params("startSnapshotId", "1", "endSnapshotId", "2", "incrementalBetweenScanMode", "DELTA"));
        Assertions.assertEquals("DELTA", out.get("incremental-between-scan-mode"),
                "scanMode must be validated case-insensitively but emitted in its ORIGINAL case");
    }

    // ==================== produced-map shape ====================

    @Test
    public void bothSnapshotIdsProduceIncrementalBetween() {
        // WHY: a [start,end] snapshot window emits incremental-between=start,end (legacy line 854).
        // MUTATION: wrong separator/order -> value != "1,5" -> red.
        Map<String, String> out = PaimonIncrementalScanParams.validate(
                params("startSnapshotId", "1", "endSnapshotId", "5"));
        Assertions.assertEquals("1,5", out.get("incremental-between"),
                "both snapshot ids must emit incremental-between=start,end");
    }

    @Test
    public void onlyStartTimestampUsesLongMaxAsOpenEnd() {
        // WHY: a timestamp window with only a start is OPEN-ENDED -> start,Long.MAX_VALUE (legacy line
        // 870). MUTATION: using a different open-end sentinel (e.g. -1 or 0) -> value mismatch -> red.
        Map<String, String> out = PaimonIncrementalScanParams.validate(params("startTimestamp", "100"));
        Assertions.assertEquals("100," + Long.MAX_VALUE, out.get("incremental-between-timestamp"),
                "only-start timestamp must emit start,Long.MAX_VALUE (open-ended)");
    }

    @Test
    public void bothTimestampsProduceIncrementalBetweenTimestamp() {
        // WHY: a [start,end] timestamp window emits incremental-between-timestamp=start,end (legacy
        // line 873). MUTATION: wrong key/value -> red.
        Map<String, String> out = PaimonIncrementalScanParams.validate(
                params("startTimestamp", "100", "endTimestamp", "200"));
        Assertions.assertEquals("100,200", out.get("incremental-between-timestamp"),
                "both timestamps must emit incremental-between-timestamp=start,end");
    }

    @Test
    public void nullResetKeysAreStrippedNotPresentWithNull() {
        // WHY (the documented benign divergence): legacy SEEDS scan.snapshot-id=null and scan.mode=null
        // (lines 842-843/846) as defensive resets against an inherited base Table. The connector loads a
        // FRESH Table per query (nothing to reset) and ConnectorMvccSnapshot rejects null values, so the
        // port STRIPS these — they must be ABSENT, not present-with-null. Stripping is byte-parity in
        // EFFECT on a freshly-loaded base. MUTATION: re-seeding the null keys -> containsKey true -> red.
        Map<String, String> out = PaimonIncrementalScanParams.validate(
                params("startSnapshotId", "1", "endSnapshotId", "5"));
        Assertions.assertFalse(out.containsKey("scan.snapshot-id"),
                "the legacy null scan.snapshot-id reset must be STRIPPED (absent), not present-with-null");
        Assertions.assertFalse(out.containsKey("scan.mode"),
                "the legacy null scan.mode reset must be STRIPPED (absent), not present-with-null");
        Assertions.assertFalse(out.containsValue(null),
                "the produced option map must contain NO null values (ConnectorMvccSnapshot rejects them)");
    }
}
