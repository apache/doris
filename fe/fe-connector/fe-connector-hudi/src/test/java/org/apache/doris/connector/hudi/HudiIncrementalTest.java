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

import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.mvcc.ConnectorMvccSnapshot;
import org.apache.doris.connector.api.mvcc.ConnectorTimeTravelSpec;
import org.apache.doris.connector.api.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.api.pushdown.ConnectorComparison;
import org.apache.doris.connector.api.pushdown.ConnectorExpression;
import org.apache.doris.connector.api.pushdown.ConnectorLiteral;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;

/**
 * Tests the Hudi {@code @incr(...)} incremental-read window-resolution surface (INC-1): the
 * {@code resolveTimeTravel(INCREMENTAL)} case + {@code applySnapshot} + the {@code begin/endInstant} pin on the
 * handle, added so a hudi-on-HMS table served post-flip through the GENERIC {@code PluginDrivenScanNode} path
 * resolves an incremental window byte-faithfully to legacy {@code COW/MORIncrementalRelation} — but consolidated
 * into ONE connector locus. Each assertion pins WHY the behavior matters:
 * <ul>
 *   <li>{@code beginTime} is required with the byte-for-byte legacy fail-loud message, so a missing bound reaches
 *       the user verbatim (an empty return would surface fe-core's wrong-domain "can't resolve time travel"
 *       text, since {@code loadSnapshot} has no INCREMENTAL not-found arm);</li>
 *   <li>an omitted / {@code "latest"} end bound resolves to the latest completed instant — the sentinel test is
 *       on the RESOLVED end value (legacy COW form), which is why {@code end="latest"} yields the instant, not
 *       the literal (guarding against the dead-code MOR bug that tested {@code latestTime});</li>
 *   <li>an empty completed timeline yields the {@code (000, 000]} window WITHOUT the begin-required check (legacy
 *       {@code withScanParams} short-circuits to {@code EmptyIncrementalRelation} first);</li>
 *   <li>applySnapshot stamps the window via {@code toBuilder()} so it PRESERVES applyFilter's prunedPartitionPaths
 *       and does not cross-contaminate the {@code FOR TIME AS OF} queryInstant carrier.</li>
 * </ul>
 *
 * <p>Unlike {@code FOR TIME AS OF}, INCREMENTAL resolution DOES touch the metaClient (to resolve the latest
 * completed instant), so the metadata is built with a STUB {@link HudiMetaClientExecutor} that returns a canned
 * latest-instant {@code Optional} without building a live metaClient — the same offline pattern as the
 * partition-listing tests.
 */
public class HudiIncrementalTest {

    private static final List<String> YEAR_MONTH = Arrays.asList("year", "month");
    private static final String LATEST = "20240102030405006";
    private static final String BEGIN_REQUIRED_MESSAGE =
            "Specify the begin instant time to pull from using option hoodie.datasource.read.begin.instanttime";

    // ── window resolution: begin/end pinned onto the handle ─────────────────────────────────────────────

    @Test
    public void explicitBeginAndEndAreCarriedVerbatimOntoHandle() {
        // Both bounds explicit and non-sentinel: they pass through unchanged (latestTime is resolved but unused).
        // Drive the full path resolveTimeTravel -> applySnapshot so the FE-internal carrier properties are
        // exercised end-to-end.
        HudiConnectorMetadata md = metadata(stub(Optional.of(LATEST)));
        HudiTableHandle pinned = resolveAndApply(md, window("20240101000000", "20240101120000"));
        Assertions.assertEquals("20240101000000", pinned.getBeginInstant());
        Assertions.assertEquals("20240101120000", pinned.getEndInstant());
    }

    @Test
    public void omittedEndDefaultsToLatestCompletedInstant() {
        // No endTime param -> end defaults to the latest completed instant (legacy getOrDefault(end-key,
        // latestTime)). Guards a mutation that would leave end null / empty for an open-ended @incr window.
        HudiConnectorMetadata md = metadata(stub(Optional.of(LATEST)));
        HudiTableHandle pinned = resolveAndApply(md, window("20240101000000", null));
        Assertions.assertEquals("20240101000000", pinned.getBeginInstant());
        Assertions.assertEquals(LATEST, pinned.getEndInstant(),
                "an omitted endTime must resolve to the latest completed instant");
    }

    @Test
    public void latestSentinelResolvesEndToTheLatestCompletedInstant() {
        // end="latest" must resolve to the instant, NOT stay the literal "latest". The sentinel test is on the
        // RESOLVED end value (COWIncrementalRelation:98); the dead-code MOR bug (MORIncrementalRelation:92 tested
        // latestTime, so end="latest" was left unresolved) is inherently avoided by the single locus. This
        // assertion KILLS a mutation back to the buggy MOR form.
        HudiConnectorMetadata md = metadata(stub(Optional.of(LATEST)));
        HudiTableHandle pinned = resolveAndApply(md, window("20240101000000", "latest"));
        Assertions.assertEquals(LATEST, pinned.getEndInstant(),
                "end=\"latest\" must resolve to the latest completed instant, not the literal sentinel");
    }

    @Test
    public void earliestSentinelResolvesBeginToZero() {
        // begin="earliest" -> "000" (legacy EARLIEST_TIME). Guards dropping the sentinel mapping.
        HudiConnectorMetadata md = metadata(stub(Optional.of(LATEST)));
        HudiTableHandle pinned = resolveAndApply(md, window("earliest", "20240101120000"));
        Assertions.assertEquals("000", pinned.getBeginInstant(),
                "begin=\"earliest\" must collapse to \"000\" (legacy EARLIEST_TIME)");
    }

    @Test
    public void useTransitionTimePolicyStillResolvesWindow() {
        // A USE_TRANSITION_TIME hollow-commit policy param must not break window resolution: resolveIncremental
        // computes the completion-time axis (useCompletionTime=true) and still pins a valid (begin, end]. The stub
        // executor returns a canned latest regardless of the axis, so the requested-vs-completion AXIS itself is
        // NOT verified here — it is deferred to the §5 USE_TRANSITION_TIME completion-axis e2e fixture. This test
        // only guards that adding the policy branch did not crash resolution or drop the pin.
        HudiConnectorMetadata md = metadata(stub(Optional.of(LATEST)));
        Map<String, String> params = window("20240101000000", null);
        params.put("hoodie.read.timeline.holes.resolution.policy", "USE_TRANSITION_TIME");
        HudiTableHandle pinned = resolveAndApply(md, params);
        Assertions.assertEquals("20240101000000", pinned.getBeginInstant());
        Assertions.assertEquals(LATEST, pinned.getEndInstant(),
                "USE_TRANSITION_TIME must still resolve an omitted end to the (canned) latest completed instant");
    }

    // ── fail-loud + empty-timeline ───────────────────────────────────────────────────────────────────────

    @Test
    public void missingBeginThrowsByteForByteLegacyMessageWhenTimelineNonEmpty() {
        // Non-empty timeline (stub returns a latest instant) + no beginTime -> THROW the byte-for-byte legacy
        // message (it propagates as-is through loadSnapshot). An empty return would surface fe-core's wrong-domain
        // "can't resolve time travel" text.
        HudiConnectorMetadata md = metadata(stub(Optional.of(LATEST)));
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> md.resolveTimeTravel(null, partitioned(),
                        ConnectorTimeTravelSpec.incremental(window(null, "20240101120000"))));
        Assertions.assertEquals(BEGIN_REQUIRED_MESSAGE, ex.getMessage());
    }

    @Test
    public void emptyTimelineYieldsZeroWindowWithoutBeginRequiredCheck() {
        // Empty completed timeline (stub returns Optional.empty()) short-circuits to the (000, 000] window BEFORE
        // the begin-required check — so a MISSING beginTime is NOT an error here (legacy withScanParams builds
        // EmptyIncrementalRelation first). The window selects nothing.
        HudiConnectorMetadata md = metadata(stub(Optional.empty()));
        HudiTableHandle pinned = resolveAndApply(md, window(null, null));
        Assertions.assertEquals("000", pinned.getBeginInstant());
        Assertions.assertEquals("000", pinned.getEndInstant());
    }

    @Test
    public void resolveIncrementalNeverReturnsEmptyForAValidWindow() {
        // The generic loadSnapshot fail-loud has no INCREMENTAL not-found arm, so INCREMENTAL must ALWAYS pin.
        HudiConnectorMetadata md = metadata(stub(Optional.of(LATEST)));
        Optional<ConnectorMvccSnapshot> pin = md.resolveTimeTravel(null, partitioned(),
                ConnectorTimeTravelSpec.incremental(window("20240101000000", null)));
        Assertions.assertTrue(pin.isPresent(), "a valid @incr window must always pin, never return empty");
    }

    // ── applySnapshot: stamp preserving pruning / carrier isolation ─────────────────────────────────────

    @Test
    public void applySnapshotStampsWindowPreservingPrunedPartitions() {
        // applyFilter runs BEFORE applySnapshot at scan time, so a pruned handle must keep its pruning after the
        // window pin. Guards a rebuild-from-scratch mutation (which would silently turn a pruned incremental scan
        // into a full scan).
        HudiConnectorMetadata md = metadata(stub(Optional.of(LATEST)));
        List<String> pruned = Arrays.asList("year=2024/month=01", "year=2024/month=02");
        HudiTableHandle prunedHandle = partitioned().toBuilder().prunedPartitionPaths(pruned).build();
        ConnectorMvccSnapshot pin = md.resolveTimeTravel(null, prunedHandle,
                ConnectorTimeTravelSpec.incremental(window("20240101000000", "20240101120000")))
                .orElseThrow(AssertionError::new);
        HudiTableHandle stamped = (HudiTableHandle) md.applySnapshot(null, prunedHandle, pin);
        Assertions.assertEquals("20240101000000", stamped.getBeginInstant());
        Assertions.assertEquals("20240101120000", stamped.getEndInstant());
        Assertions.assertEquals(pruned, stamped.getPrunedPartitionPaths(),
                "the incremental pin must preserve applyFilter's partition pruning");
        Assertions.assertNull(stamped.getQueryInstant(),
                "an incremental pin must not set the FOR TIME AS OF queryInstant carrier");
    }

    @Test
    public void timeTravelPinDoesNotSetIncrementalWindow() {
        // Cross-isolation the other way: a FOR TIME AS OF pin must leave begin/endInstant null (the two carriers
        // are mutually exclusive). Guards accidental cross-wiring in applySnapshot.
        HudiConnectorMetadata md = metadata(stub(Optional.of(LATEST)));
        ConnectorMvccSnapshot pin = md.resolveTimeTravel(null, partitioned(),
                ConnectorTimeTravelSpec.timestamp("2024-01-01 12:00:00", false)).orElseThrow(AssertionError::new);
        HudiTableHandle stamped = (HudiTableHandle) md.applySnapshot(null, partitioned(), pin);
        Assertions.assertEquals("20240101120000", stamped.getQueryInstant());
        Assertions.assertNull(stamped.getBeginInstant());
        Assertions.assertNull(stamped.getEndInstant());
    }

    @Test
    public void applySnapshotLeavesLatestPinUnchanged() {
        // The query-begin latest pin (beginQuerySnapshot output) carries ONLY a snapshotId, NO window property.
        // applySnapshot must return the handle UNCHANGED so a plain read stays byte-identical.
        HudiConnectorMetadata md = metadata(stub(Optional.of(LATEST)));
        ConnectorMvccSnapshot latestPin =
                HudiConnectorMetadata.buildBeginQuerySnapshot(20240101120000000L).orElseThrow(AssertionError::new);
        HudiTableHandle base = partitioned();
        HudiTableHandle result = (HudiTableHandle) md.applySnapshot(null, base, latestPin);
        Assertions.assertSame(base, result, "a latest pin must not rebuild the handle");
        Assertions.assertNull(result.getBeginInstant());
        Assertions.assertNull(result.getEndInstant());
    }

    // ── raw @incr option-param threading (glob / fallback / policy → handle) ────────────────────────────

    @Test
    public void resolveThreadsRawIncrParamsOntoHandleExcludingWindowCarriers() {
        // The raw @incr option params (glob / fallback / hollow policy) must ride the FE-internal transport to
        // the handle so planScan can feed them to the ported relations; the begin/end WINDOW carriers (their own
        // "hudi.incremental-*" keys) must NOT leak into that opt-param map. Guards both the copy AND the namespace
        // isolation (a mutation that dropped the prefix filter would pollute the relations' optParams with the
        // internal carriers).
        HudiConnectorMetadata md = metadata(stub(Optional.of(LATEST)));
        Map<String, String> params = window("20240101000000", "20240101120000");
        params.put("hoodie.datasource.read.incr.path.glob", "*/2024/*");
        params.put("hoodie.datasource.read.incr.fallback.fulltablescan.enable", "true");
        params.put("hoodie.read.timeline.holes.resolution.policy", "USE_TRANSITION_TIME");
        HudiTableHandle pinned = resolveAndApply(md, params);

        Map<String, String> incr = pinned.getIncrementalParams();
        Assertions.assertEquals("*/2024/*", incr.get("hoodie.datasource.read.incr.path.glob"));
        Assertions.assertEquals("true", incr.get("hoodie.datasource.read.incr.fallback.fulltablescan.enable"));
        Assertions.assertEquals("USE_TRANSITION_TIME", incr.get("hoodie.read.timeline.holes.resolution.policy"));
        // The resolved window rides begin/endInstant separately; the FE-internal carriers must not leak into the
        // opt-param map the relations read.
        Assertions.assertFalse(incr.containsKey("hudi.incremental-begin"),
                "the FE-internal begin carrier must not appear in the relations' opt-param map");
        Assertions.assertFalse(incr.containsKey("hudi.incremental-end"),
                "the FE-internal end carrier must not appear in the relations' opt-param map");
        // beginTime/endTime aliases are carried verbatim (legacy passed the full optParams); they are inert for
        // the relations (which read only glob/fallback/policy) but round-trip fidelity is asserted.
        Assertions.assertEquals("20240101000000", incr.get("beginTime"));
        Assertions.assertEquals("20240101120000", incr.get("endTime"));
        // And the window itself still lands on the dedicated fields.
        Assertions.assertEquals("20240101000000", pinned.getBeginInstant());
        Assertions.assertEquals("20240101120000", pinned.getEndInstant());
    }

    @Test
    public void nonIncrementalPinsLeaveIncrementalParamsEmpty() {
        // A FOR TIME AS OF pin and a plain (latest) handle must both carry an EMPTY incrementalParams — only an
        // @incr read populates it. Guards accidental cross-wiring in applySnapshot.
        HudiConnectorMetadata md = metadata(stub(Optional.of(LATEST)));
        ConnectorMvccSnapshot ttPin = md.resolveTimeTravel(null, partitioned(),
                ConnectorTimeTravelSpec.timestamp("2024-01-01 12:00:00", false)).orElseThrow(AssertionError::new);
        HudiTableHandle ttStamped = (HudiTableHandle) md.applySnapshot(null, partitioned(), ttPin);
        Assertions.assertTrue(ttStamped.getIncrementalParams().isEmpty(),
                "a FOR TIME AS OF pin must not populate incrementalParams");
        Assertions.assertTrue(partitioned().getIncrementalParams().isEmpty(),
                "a plain handle has empty incrementalParams");
    }

    @Test
    public void toBuilderRoundTripsIncrementalParams() {
        // Guards the toBuilder() copy line for incrementalParams: a dropped copy would silently lose glob/
        // fallback/policy when applyFilter/applySnapshot rebuild the handle, so the relations would read defaults.
        Map<String, String> params = new HashMap<>();
        params.put("hoodie.datasource.read.incr.path.glob", "*/x/*");
        HudiTableHandle original = new HudiTableHandle.Builder("db", "t", "s3://b/t", "MERGE_ON_READ")
                .beginInstant("20240101000000").endInstant("20240101120000").incrementalParams(params).build();
        HudiTableHandle copy = original.toBuilder().build();
        Assertions.assertEquals("*/x/*", copy.getIncrementalParams().get("hoodie.datasource.read.incr.path.glob"));
    }

    // ── synthetic scan predicate (the neutral row-level @incr filter SPI) ───────────────────────────────

    @Test
    public void syntheticScanPredicatesEmitStringTypedCommitTimeWindow() {
        // The @incr row filter is required because a COW base file rewritten inside the window ALSO carries
        // forward out-of-window rows. The SPI reads the window off the SAME resolved pin applySnapshot consumes
        // (single window authority, so file selection and the row filter can never diverge) and emits
        // `_hoodie_commit_time > begin AND <= end` as two flat conjuncts, STRING-typed both sides for
        // lexicographic instant compare. Byte-faithful to legacy LogicalHudiScan.generateIncrementalExpression.
        HudiConnectorMetadata md = metadata(stub(Optional.of(LATEST)));
        ConnectorMvccSnapshot pin = md.resolveTimeTravel(null, partitioned(),
                ConnectorTimeTravelSpec.incremental(window("20240101000000", "20240101120000")))
                .orElseThrow(AssertionError::new);

        List<ConnectorExpression> predicates = md.getSyntheticScanPredicates(null, partitioned(), pin);

        ConnectorColumnRef commitTime = new ConnectorColumnRef("_hoodie_commit_time", ConnectorType.of("STRING"));
        Assertions.assertEquals(Arrays.asList(
                new ConnectorComparison(ConnectorComparison.Operator.GT, commitTime,
                        ConnectorLiteral.ofString("20240101000000")),
                new ConnectorComparison(ConnectorComparison.Operator.LE, commitTime,
                        ConnectorLiteral.ofString("20240101120000"))),
                predicates,
                "an @incr read must emit `_hoodie_commit_time > begin AND <= end`, STRING-typed on both sides");
    }

    @Test
    public void syntheticScanPredicatesAreEmptyForTimeTravelPin() {
        // A FOR TIME AS OF pin carries a queryInstant, NOT a (begin, end] window -> no row filter (its rows are
        // already correct by file selection at the instant). Guards a mutation that would emit a bogus window.
        HudiConnectorMetadata md = metadata(stub(Optional.of(LATEST)));
        ConnectorMvccSnapshot ttPin = md.resolveTimeTravel(null, partitioned(),
                ConnectorTimeTravelSpec.timestamp("2024-01-01 12:00:00", false)).orElseThrow(AssertionError::new);
        Assertions.assertTrue(md.getSyntheticScanPredicates(null, partitioned(), ttPin).isEmpty(),
                "a FOR TIME AS OF pin must not produce a synthetic row filter");
    }

    @Test
    public void syntheticScanPredicatesAreEmptyForPlainAndNullPin() {
        // The query-begin latest pin (beginQuerySnapshot) carries only a snapshotId, no window -> a plain read
        // gets NO synthetic filter, so its plan is byte-identical to today. A null snapshot is likewise a no-op.
        HudiConnectorMetadata md = metadata(stub(Optional.of(LATEST)));
        ConnectorMvccSnapshot latestPin =
                HudiConnectorMetadata.buildBeginQuerySnapshot(20240101120000000L).orElseThrow(AssertionError::new);
        Assertions.assertTrue(md.getSyntheticScanPredicates(null, partitioned(), latestPin).isEmpty(),
                "a plain latest pin must not produce a synthetic row filter");
        Assertions.assertTrue(md.getSyntheticScanPredicates(null, partitioned(), null).isEmpty(),
                "a null snapshot must not produce a synthetic row filter");
    }

    // ── handle field round-trip ─────────────────────────────────────────────────────────────────────────

    @Test
    public void toBuilderRoundTripsWindowFields() {
        // Guards the toBuilder() copy lines for begin/endInstant (a dropped copy would silently lose the window
        // when applyFilter/applySnapshot rebuild the handle).
        HudiTableHandle original = new HudiTableHandle.Builder("db", "t", "s3://b/t", "MERGE_ON_READ")
                .partitionKeyNames(YEAR_MONTH)
                .beginInstant("20240101000000")
                .endInstant("20240101120000")
                .build();
        HudiTableHandle copy = original.toBuilder().build();
        Assertions.assertEquals("20240101000000", copy.getBeginInstant());
        Assertions.assertEquals("20240101120000", copy.getEndInstant());
        // A fresh handle carries no window (null), so a plain read is not treated as incremental.
        Assertions.assertNull(partitioned().getBeginInstant());
        Assertions.assertNull(partitioned().getEndInstant());
    }

    // ── helpers ────────────────────────────────────────────────────────────────────────────────────────

    private static HudiConnectorMetadata metadata(HudiMetaClientExecutor executor) {
        return new HudiConnectorMetadata(null, Collections.emptyMap(), executor);
    }

    private static HudiTableHandle partitioned() {
        return new HudiTableHandle.Builder("db", "t", "s3://b/t", "COPY_ON_WRITE")
                .partitionKeyNames(YEAR_MONTH).build();
    }

    /** Builds the raw @incr param map fe-core threads via getIncrementalParams() (null entries omitted). */
    private static Map<String, String> window(String beginTime, String endTime) {
        Map<String, String> params = new HashMap<>();
        if (beginTime != null) {
            params.put("beginTime", beginTime);
        }
        if (endTime != null) {
            params.put("endTime", endTime);
        }
        return params;
    }

    private static HudiTableHandle resolveAndApply(HudiConnectorMetadata md, Map<String, String> params) {
        ConnectorMvccSnapshot pin = md.resolveTimeTravel(null, partitioned(),
                ConnectorTimeTravelSpec.incremental(params)).orElseThrow(AssertionError::new);
        return (HudiTableHandle) md.applySnapshot(null, partitioned(), pin);
    }

    /** Executor that ignores the action and returns a canned value (stubs out the live metaClient). */
    private static HudiMetaClientExecutor stub(Object cannedReturn) {
        return new HudiMetaClientExecutor() {
            @Override
            @SuppressWarnings("unchecked")
            public <T> T execute(Callable<T> action) {
                return (T) cannedReturn;
            }
        };
    }
}
