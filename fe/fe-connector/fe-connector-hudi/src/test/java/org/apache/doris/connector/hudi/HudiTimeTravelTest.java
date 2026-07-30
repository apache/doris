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
import org.apache.doris.connector.api.mvcc.ConnectorMvccSnapshot;
import org.apache.doris.connector.api.mvcc.ConnectorTimeTravelSpec;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Tests the Hudi {@code FOR TIME AS OF} time-travel surface (resolveTimeTravel + applySnapshot + the
 * queryInstant pin on the handle) added so a hudi-on-HMS table served post-flip through the GENERIC
 * {@code PluginDrivenScanNode} path honors an explicit instant, byte-faithfully to legacy {@code HudiScanNode}.
 * Each assertion pins WHY the behavior matters:
 * <ul>
 *   <li>{@code FOR TIME AS OF} is a pure lexical {@code replaceAll("[-: ]","")} of the value (legacy
 *       {@code HudiScanNode.java:211}) — NO session time zone, NO epoch-millis conversion, NO timeline
 *       validation, and it NEVER errors on a well-formed value (a too-early/future instant reads empty/latest
 *       via before-or-on);</li>
 *   <li>{@code FOR VERSION AS OF} (numeric snapshot-id or named ref) is rejected by THROWING the byte-for-byte
 *       legacy message, so the exact wording reaches the user (an empty return would surface fe-core's
 *       wrong-domain "can't find snapshot" text);</li>
 *   <li>applySnapshot stamps the instant via {@code toBuilder()} so it PRESERVES applyFilter's
 *       prunedPartitionPaths (applyFilter runs first at scan time);</li>
 *   <li>the query-begin latest pin (empty properties) leaves the handle UNCHANGED, so a plain read stays
 *       byte-identical to before this step.</li>
 * </ul>
 *
 * <p>resolveTimeTravel and applySnapshot never touch the hmsClient or the metaClient executor, so the metadata
 * is built with {@code null} collaborators: any accidental metadata access would NPE, which itself proves the
 * fully-offline / no-validation contract.
 */
public class HudiTimeTravelTest {

    private static final String VERSION_REJECT_MESSAGE =
            "Hudi does not support `FOR VERSION AS OF`, please use `FOR TIME AS OF`";
    private static final List<String> YEAR_MONTH = Arrays.asList("year", "month");

    // ── FOR TIME AS OF: normalize + pin ────────────────────────────────────────────────────────────────

    @Test
    public void resolveTimestampStripsSeparatorsAndPinsInstantOntoHandle() {
        HudiConnectorMetadata md = metadata();
        // "2024-01-01 12:00:00" -> "20240101120000": dashes/colons/space stripped, nothing else. Drive the
        // full path resolveTimeTravel -> applySnapshot so the private carrier property is exercised end-to-end.
        ConnectorMvccSnapshot pin = resolveTimestamp(md, "2024-01-01 12:00:00");
        HudiTableHandle pinned = (HudiTableHandle) md.applySnapshot(null, partitioned(), pin);
        Assertions.assertEquals("20240101120000", pinned.getQueryInstant(),
                "FOR TIME AS OF must strip [-: ] and pin the value verbatim (legacy HudiScanNode:211) — "
                        + "no session-TZ shift, no epoch-millis conversion");
    }

    @Test
    public void resolveTimestampIgnoresDigitalFlagAndDoesNoConversion() {
        HudiConnectorMetadata md = metadata();
        // A digital (epoch-millis-looking) value has no [-: ] to strip, so it passes through VERBATIM — legacy
        // hudi never treated FOR TIME AS OF as epoch-millis (unlike paimon); it is compared lexically before-or-on.
        ConnectorMvccSnapshot pin = md.resolveTimeTravel(null, partitioned(),
                ConnectorTimeTravelSpec.timestamp("1704067200000", true)).orElseThrow(AssertionError::new);
        HudiTableHandle pinned = (HudiTableHandle) md.applySnapshot(null, partitioned(), pin);
        Assertions.assertEquals("1704067200000", pinned.getQueryInstant());
    }

    @Test
    public void resolveTimestampIsPermissiveAndFullyOffline() {
        // A too-early instant (before any commit) resolves to a NON-EMPTY pin, NOT Optional.empty(): legacy
        // never validates FOR TIME AS OF against the timeline and never errors — before-or-on simply reads
        // empty. The metadata's hmsClient + executor are null, so a present result also proves resolveTimeTravel
        // performs ZERO metadata access (any timeline lookup would NPE here).
        HudiConnectorMetadata md = metadata();
        Optional<ConnectorMvccSnapshot> pin = md.resolveTimeTravel(null, partitioned(),
                ConnectorTimeTravelSpec.timestamp("1999-01-01 00:00:00", false));
        Assertions.assertTrue(pin.isPresent(),
                "well-formed FOR TIME AS OF must always pin (permissive, legacy parity), never return empty");
    }

    // ── FOR VERSION AS OF: reject with the byte-for-byte legacy message ─────────────────────────────────

    @Test
    public void resolveSnapshotIdRejectsWithByteForByteLegacyMessage() {
        HudiConnectorMetadata md = metadata();
        // FOR VERSION AS OF <numeric> arrives as SNAPSHOT_ID. Must THROW (not empty) so the exact legacy string
        // reaches the user verbatim (it propagates as-is through loadSnapshot, which has no try/catch).
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> md.resolveTimeTravel(null, partitioned(), ConnectorTimeTravelSpec.snapshotId("123")));
        Assertions.assertEquals(VERSION_REJECT_MESSAGE, ex.getMessage());
    }

    @Test
    public void resolveVersionRefRejectsWithByteForByteLegacyMessage() {
        HudiConnectorMetadata md = metadata();
        // FOR VERSION AS OF '<name>' arrives as VERSION_REF — hudi rejects it identically (it has no
        // tags/branches), with the SAME message.
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> md.resolveTimeTravel(null, partitioned(), ConnectorTimeTravelSpec.versionRef("v1.0")));
        Assertions.assertEquals(VERSION_REJECT_MESSAGE, ex.getMessage());
    }

    // ── applySnapshot: stamp preserving pruning / no-op for the latest pin ──────────────────────────────

    @Test
    public void applySnapshotStampsInstantPreservingPrunedPartitions() {
        HudiConnectorMetadata md = metadata();
        // applyFilter runs BEFORE applySnapshot at scan time, so a pruned handle must keep its pruning after the
        // pin. Guards a rebuild-from-scratch mutation (which would silently turn a pruned scan into a full scan).
        List<String> pruned = Arrays.asList("year=2024/month=01", "year=2024/month=02");
        HudiTableHandle prunedHandle = partitioned().toBuilder().prunedPartitionPaths(pruned).build();
        ConnectorMvccSnapshot pin = resolveTimestamp(md, "2024-06-01 00:00:00");
        HudiTableHandle stamped = (HudiTableHandle) md.applySnapshot(null, prunedHandle, pin);
        Assertions.assertEquals("20240601000000", stamped.getQueryInstant());
        Assertions.assertEquals(pruned, stamped.getPrunedPartitionPaths());
    }

    @Test
    public void applySnapshotLeavesLatestPinUnchanged() {
        HudiConnectorMetadata md = metadata();
        // The query-begin latest pin (beginQuerySnapshot output) carries ONLY a snapshotId, NO query-instant
        // property. applySnapshot must return the handle UNCHANGED so planScan falls back to timeline.lastInstant()
        // — the no-regression guard for every ordinary (non-time-travel) hudi read.
        ConnectorMvccSnapshot latestPin =
                HudiConnectorMetadata.buildBeginQuerySnapshot(20240101120000000L).orElseThrow(AssertionError::new);
        HudiTableHandle base = partitioned();
        HudiTableHandle result = (HudiTableHandle) md.applySnapshot(null, base, latestPin);
        Assertions.assertSame(base, result, "latest pin must not rebuild the handle");
        Assertions.assertNull(result.getQueryInstant());
    }

    @Test
    public void applySnapshotWithNullSnapshotIsUnchanged() {
        HudiConnectorMetadata md = metadata();
        HudiTableHandle base = partitioned();
        Assertions.assertSame(base, md.applySnapshot(null, base, null));
    }

    // ── handle field round-trip ─────────────────────────────────────────────────────────────────────────

    @Test
    public void toBuilderRoundTripsQueryInstantAndPreservesEveryField() {
        Map<String, String> params = Collections.singletonMap("k", "v");
        HudiTableHandle original = new HudiTableHandle.Builder("db", "t", "s3://b/t", "MERGE_ON_READ")
                .inputFormat("org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat")
                .serdeLib("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe")
                .partitionKeyNames(YEAR_MONTH)
                .tableParameters(params)
                .prunedPartitionPaths(Arrays.asList("year=2024/month=01"))
                .queryInstant("20240101120000")
                .build();
        HudiTableHandle copy = original.toBuilder().build();
        Assertions.assertEquals("20240101120000", copy.getQueryInstant());
        Assertions.assertEquals(original.getInputFormat(), copy.getInputFormat());
        Assertions.assertEquals(original.getSerdeLib(), copy.getSerdeLib());
        Assertions.assertEquals(original.getPartitionKeyNames(), copy.getPartitionKeyNames());
        Assertions.assertEquals(original.getTableParameters(), copy.getTableParameters());
        Assertions.assertEquals(original.getPrunedPartitionPaths(), copy.getPrunedPartitionPaths());
        // A fresh handle carries no pin (null), so a plain read stays on timeline.lastInstant().
        Assertions.assertNull(partitioned().getQueryInstant());
    }

    // ── helpers ────────────────────────────────────────────────────────────────────────────────────────

    /** resolveTimeTravel/applySnapshot never touch hmsClient or the executor → null collaborators are safe. */
    private static HudiConnectorMetadata metadata() {
        return new HudiConnectorMetadata(null, Collections.emptyMap(), null);
    }

    private static HudiTableHandle partitioned() {
        return new HudiTableHandle.Builder("db", "t", "s3://b/t", "COPY_ON_WRITE")
                .partitionKeyNames(YEAR_MONTH).build();
    }

    private static ConnectorMvccSnapshot resolveTimestamp(HudiConnectorMetadata md, String value) {
        return md.resolveTimeTravel(null, partitioned(),
                ConnectorTimeTravelSpec.timestamp(value, false)).orElseThrow(AssertionError::new);
    }
}
