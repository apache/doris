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

import org.apache.doris.connector.api.scan.ConnectorScanRange;

import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Tests the OFFLINE-verifiable core of the INC-3 incremental {@code planScan} wiring: the pure static helpers
 * {@link HudiScanPlanProvider#incrementalRanges} (COW/MOR routing + fallback degrade) and
 * {@link HudiScanPlanProvider#buildMorRange} (file-slice &rarr; JNI range mapping). Both are exercised without a
 * live {@code HoodieTableMetaClient} — {@code incrementalRanges} via a FAKE {@link IncrementalRelation}, and
 * {@code buildMorRange} via a hand-built {@link FileSlice}.
 *
 * <p><b>Coverage scope (Rule 12 — no over-claim).</b> These cover the routing + mapping DECISIONS only. Building
 * the real relation (which does eager timeline/metadata/filesystem I/O) and the actual file SELECTION
 * (commit-range, write-stat mapping, {@code fs.exists} full-table-scan probes, the completion-time END axis) are
 * e2e-only (design §5) — same boundary as {@link HudiIncrementalRelationTest}. Row-level filtering of the read to
 * the {@code (begin, end]} window is a LATER step (an FE-side synthetic {@code _hoodie_commit_time} predicate),
 * NOT this file-selection step.
 */
public class HudiIncrementalPlanScanTest {

    private static final List<String> YEAR_MONTH = Arrays.asList("year", "month");
    private static final String END_TS = "20240101120000";
    private static final String BASE_PATH = "s3://b/t";
    private static final String INPUT_FORMAT = "org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat";
    private static final String SERDE = "org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat";

    // ── fallback → degrade (Optional.empty), collect* NOT called ─────────────────────────────────────────────

    @Test
    public void fallbackFullTableScanDegradesToSnapshotScanWithoutCollecting() {
        // relation.fallbackFullTableScan()==true must yield Optional.empty() = the caller degrades to the normal
        // latest-snapshot scan (legacy HudiScanNode.getSplits:470), and NEITHER collect method is invoked (both
        // throw to prove they are not called). Kills a mutation inverting the fallback check.
        FakeRelation fallback = new FakeRelation();
        fallback.fallback = true;
        fallback.collectSplitsThrows = true;
        fallback.collectFileSlicesThrows = true;
        Optional<List<ConnectorScanRange>> result = HudiScanPlanProvider.incrementalRanges(
                fallback, /*isCow*/ true, /*forceJni*/ false, BASE_PATH, INPUT_FORMAT, SERDE,
                Collections.emptyList(), Collections.emptyList(), YEAR_MONTH);
        Assertions.assertFalse(result.isPresent(),
                "fallbackFullTableScan must signal degrade to the snapshot scan (Optional.empty)");
    }

    // ── COW → collectSplits (native ranges returned verbatim); force_jni IGNORED for COW ─────────────────────

    @Test
    public void cowRoutesToCollectSplitsAndNeverCallsCollectFileSlices() {
        // A COW incremental read returns the relation's native collectSplits() ranges directly and must NEVER
        // call collectFileSlices() (which throws on a COW relation = the shape contract). Kills a mutation that
        // routes COW through the MOR branch (which would reproduce the legacy UnsupportedOperationException crash).
        HudiScanRange native1 = new HudiScanRange.Builder().path("s3://b/t/f1.parquet").fileFormat("parquet").build();
        FakeRelation cow = new FakeRelation();
        cow.splits = Collections.singletonList(native1);
        cow.collectFileSlicesThrows = true;
        Optional<List<ConnectorScanRange>> result = HudiScanPlanProvider.incrementalRanges(
                cow, /*isCow*/ true, /*forceJni*/ false, BASE_PATH, INPUT_FORMAT, SERDE,
                Collections.emptyList(), Collections.emptyList(), YEAR_MONTH);
        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals(1, result.get().size());
        Assertions.assertSame(native1, result.get().get(0), "COW ranges must be the relation's collectSplits output");
    }

    @Test
    public void cowIncrementalIgnoresForceJniAndStaysNative() {
        // The signed graceful deviation: under force_jni a COW incremental read STILL reads native (collectSplits),
        // instead of legacy's route to collectFileSlices() on a COW relation → UnsupportedOperationException. So
        // force_jni=true must NOT change COW routing. collectFileSlices throws to prove it is not taken.
        HudiScanRange native1 = new HudiScanRange.Builder().path("s3://b/t/f1.parquet").fileFormat("parquet").build();
        FakeRelation cow = new FakeRelation();
        cow.splits = Collections.singletonList(native1);
        cow.collectFileSlicesThrows = true;
        Optional<List<ConnectorScanRange>> result = HudiScanPlanProvider.incrementalRanges(
                cow, /*isCow*/ true, /*forceJni*/ true, BASE_PATH, INPUT_FORMAT, SERDE,
                Collections.emptyList(), Collections.emptyList(), YEAR_MONTH);
        Assertions.assertTrue(result.isPresent());
        Assertions.assertSame(native1, result.get().get(0),
                "COW incremental must stay native even under force_jni (never call collectFileSlices)");
    }

    // ── MOR → collectFileSlices mapped to JNI ranges at endTs; collectSplits NEVER called ────────────────────

    @Test
    public void morRoutesToCollectFileSlicesMappedAtEndTs() {
        // A MOR incremental read maps each collectFileSlices() slice to a JNI range at the resolved window END
        // (relation.getEndTs()), with per-slice partition values from the slice's OWN partition path. It must
        // NEVER call collectSplits() (which throws on a MOR relation). Uses force_jni so a base-only (no-log)
        // slice stays on the JNI reader, exercising the instantTime=endTs stamping.
        FileSlice slice = baseOnlySlice("year=2024/month=01",
                "s3://b/t/year=2024/month=01/fileid-1_0_20240101000000.parquet");
        FakeRelation mor = new FakeRelation();
        mor.fileSlices = Collections.singletonList(slice);
        mor.collectSplitsThrows = true;
        Optional<List<ConnectorScanRange>> result = HudiScanPlanProvider.incrementalRanges(
                mor, /*isCow*/ false, /*forceJni*/ true, BASE_PATH, INPUT_FORMAT, SERDE,
                Arrays.asList("c1"), Arrays.asList("int"), YEAR_MONTH);
        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals(1, result.get().size());
        HudiScanRange range = (HudiScanRange) result.get().get(0);
        Assertions.assertEquals("jni", range.getFileFormat(), "force_jni keeps the no-log MOR slice on JNI");
        Assertions.assertEquals(END_TS, range.getProperties().get("hudi.instant_time"),
                "the JNI merge instant must be the resolved window END (getEndTs), not the latest instant");
        Map<String, String> partValues = range.getPartitionValues();
        Assertions.assertEquals("2024", partValues.get("year"));
        Assertions.assertEquals("01", partValues.get("month"),
                "partition values must be parsed from the slice's own partition path against the table-config fields");
    }

    @Test
    public void morEmptySliceListYieldsEmptyRangesButTakesMorBranch() {
        // A MOR relation with no slices returns Optional.of([]) — present (not degrade) but empty. collectSplits
        // throws to prove the MOR (collectFileSlices) branch was taken, not the COW branch.
        FakeRelation mor = new FakeRelation();
        mor.fileSlices = Collections.emptyList();
        mor.collectSplitsThrows = true;
        Optional<List<ConnectorScanRange>> result = HudiScanPlanProvider.incrementalRanges(
                mor, /*isCow*/ false, /*forceJni*/ false, BASE_PATH, INPUT_FORMAT, SERDE,
                Collections.emptyList(), Collections.emptyList(), YEAR_MONTH);
        Assertions.assertTrue(result.isPresent());
        Assertions.assertTrue(result.get().isEmpty());
    }

    // ── buildMorRange mapping (hand-built FileSlice) ─────────────────────────────────────────────────────────

    @Test
    public void buildMorRangeStampsJniInstantAndMetadataForForceJniSlice() {
        // buildMorRange on a base-only slice with force_jni: a JNI range carrying instantTime=jniInstant + serde +
        // input format + base path + column lists. Pins the exact JNI metadata BE needs.
        FileSlice slice = baseOnlySlice("year=2024/month=01",
                "s3://b/t/year=2024/month=01/fileid-1_0_20240101000000.parquet");
        Map<String, String> partValues = HudiScanPlanProvider.parsePartitionValues(
                slice.getPartitionPath(), YEAR_MONTH);
        HudiScanRange range = HudiScanPlanProvider.buildMorRange(slice, partValues, END_TS, /*forceJni*/ true,
                BASE_PATH, INPUT_FORMAT, SERDE, Arrays.asList("c1"), Arrays.asList("int"));
        Assertions.assertEquals("jni", range.getFileFormat());
        Assertions.assertEquals(END_TS, range.getProperties().get("hudi.instant_time"));
        Assertions.assertEquals(SERDE, range.getProperties().get("hudi.serde"));
        Assertions.assertEquals(BASE_PATH, range.getProperties().get("hudi.base_path"));
    }

    @Test
    public void buildMorRangeDowngradesNoLogSliceToNativeWithoutForceJni() {
        // A base-only (no delta log) slice WITHOUT force_jni reads natively: format from the base file suffix, and
        // NO JNI instant metadata (the native reader needs only the path). Guards the native downgrade parity.
        FileSlice slice = baseOnlySlice("year=2024/month=01",
                "s3://b/t/year=2024/month=01/fileid-1_0_20240101000000.parquet");
        HudiScanRange range = HudiScanPlanProvider.buildMorRange(slice, Collections.emptyMap(), END_TS,
                /*forceJni*/ false, BASE_PATH, INPUT_FORMAT, SERDE,
                Arrays.asList("c1"), Arrays.asList("int"));
        Assertions.assertEquals("parquet", range.getFileFormat(),
                "a no-log slice without force_jni must downgrade to the native parquet reader");
    }

    // ── helpers ────────────────────────────────────────────────────────────────────────────────────────────

    /** A MOR file slice with a single base file and no delta logs. */
    private static FileSlice baseOnlySlice(String partitionPath, String baseFilePath) {
        FileSlice slice = new FileSlice(partitionPath, "20240101000000", "fileid-1");
        slice.setBaseFile(new HoodieBaseFile(baseFilePath));
        return slice;
    }

    /** A recording fake {@link IncrementalRelation}: canned outputs, throws on the shape it should not serve. */
    private static final class FakeRelation implements IncrementalRelation {
        private List<HudiScanRange> splits = new ArrayList<>();
        private List<FileSlice> fileSlices = new ArrayList<>();
        private boolean fallback;
        private String endTs = END_TS;
        private boolean collectSplitsThrows;
        private boolean collectFileSlicesThrows;

        @Override
        public List<FileSlice> collectFileSlices() {
            if (collectFileSlicesThrows) {
                throw new UnsupportedOperationException("collectFileSlices must not be called on this route");
            }
            return fileSlices;
        }

        @Override
        public List<HudiScanRange> collectSplits() {
            if (collectSplitsThrows) {
                throw new UnsupportedOperationException("collectSplits must not be called on this route");
            }
            return splits;
        }

        @Override
        public boolean fallbackFullTableScan() {
            return fallback;
        }

        @Override
        public String getEndTs() {
            return endTs;
        }
    }
}
