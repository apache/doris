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

import org.apache.doris.connector.spi.scan.ConnectorScanRange;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineUtils.HollowCommitHandling;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.UnaryOperator;

/**
 * Tests incremental scan-range mapping through both a real local Hudi commit and recording fake relations.
 * Covers {@link HudiScanPlanProvider#incrementalRanges} (COW/MOR routing and fallback) and
 * {@link HudiScanPlanProvider#buildMorRange} (file-slice to native/JNI range mapping).
 *
 * <p>The COW timestamp regression persists a local SDK commit and exercises the real relation's write-stat
 * mapping. Other selection scenarios use a fake relation; these tests do not decode Parquet data or exercise
 * a remote Hudi deployment. Incremental row-level filtering is outside the scan-range mapping tested here.
 */
public class HudiIncrementalPlanScanTest {

    private static final List<String> YEAR_MONTH = Arrays.asList("year", "month");
    private static final String END_TS = "20240101120000";
    private static final String BASE_PATH = "s3://b/t";
    private static final String INPUT_FORMAT = "org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat";
    private static final String SERDE = "org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat";

    // ── fallback → degrade (Optional.empty), collect* NOT called ─────────────────────────────────────────────

    @Test
    public void cowIncrementalTimestampDecodingPreservesCommittedFilePath(@TempDir Path warehouse) throws Exception {
        Configuration configuration = new Configuration();
        HoodieTableMetaClient metaClient = HoodieTableMetaClient.newTableBuilder()
                .setTableType(HoodieTableType.COPY_ON_WRITE).setTableName("t")
                .setPartitionFields("event_time,label").setUrlEncodePartitioning(false)
                .initTable(new HadoopStorageConfiguration(configuration), warehouse.toUri().toString());
        String partition = "event_time=2024-01-01 10%3A00%3A00/label=%3A";
        String instantTime = "20240101120000000";
        String relativeFile = partition + "/fileid_0_" + instantTime + ".parquet";
        HoodieWriteStat stat = new HoodieWriteStat();
        stat.setFileId("fileid");
        stat.setPartitionPath(partition);
        stat.setPath(relativeFile);
        stat.setFileSizeInBytes(128);
        HoodieCommitMetadata metadata = new HoodieCommitMetadata();
        metadata.addWriteStat(partition, stat);
        HoodieActiveTimeline timeline = metaClient.getActiveTimeline();
        HoodieInstant requested = metaClient.createNewInstant(HoodieInstant.State.REQUESTED,
                HoodieTimeline.COMMIT_ACTION, instantTime);
        timeline.createNewInstant(requested);
        timeline.transitionRequestedToInflight(requested, Option.empty());
        timeline.saveAsComplete(metaClient.createNewInstant(HoodieInstant.State.INFLIGHT,
                HoodieTimeline.COMMIT_ACTION, instantTime), Option.of(metadata));
        metaClient.reloadActiveTimeline();
        COWIncrementalRelation relation = new COWIncrementalRelation(metaClient, configuration,
                "20240101000000000", instantTime, HollowCommitHandling.FAIL,
                Collections.emptyMap(), Collections.singleton("event_time"));
        List<HudiScanRange> ranges = relation.collectSplits(UnaryOperator.identity());
        Assertions.assertEquals(1, ranges.size());
        HudiScanRange range = ranges.get(0);
        Assertions.assertTrue(range.getPath().get().endsWith(relativeFile));
        TFileRangeDesc descriptor = new TFileRangeDesc();
        range.populateRangeParams(new TTableFormatFileDesc(), descriptor);
        Assertions.assertEquals(Arrays.asList("event_time", "label"), descriptor.getColumnsFromPathKeys());
        Assertions.assertEquals(Arrays.asList("2024-01-01 10:00:00", "%3A"), descriptor.getColumnsFromPath());
    }

    @Test
    public void morIncrementalTimestampDecodingReachesNativeAndJniScanRanges() {
        String path = "event_time=2024-01-01 10%3A00%3A00/label=%3A";
        String file = BASE_PATH + "/" + path + "/fileid_0_20240101000000.parquet";
        for (boolean forceJni : Arrays.asList(false, true)) {
            FakeRelation mor = new FakeRelation();
            mor.fileSlices = Collections.singletonList(baseOnlySlice(path, file));
            HudiScanRange range = (HudiScanRange) HudiScanPlanProvider.incrementalRanges(
                    mor, false, forceJni, BASE_PATH, INPUT_FORMAT, SERDE,
                    Arrays.asList("event_time", "label"), Arrays.asList("timestamp", "string"),
                    Arrays.asList("event_time", "label"), UnaryOperator.identity(), false,
                    Collections.singleton("event_time")).get().get(0);
            TFileRangeDesc descriptor = new TFileRangeDesc();
            range.populateRangeParams(new TTableFormatFileDesc(), descriptor);
            Assertions.assertEquals(Arrays.asList("event_time", "label"), descriptor.getColumnsFromPathKeys());
            Assertions.assertEquals(Arrays.asList("2024-01-01 10:00:00", "%3A"), descriptor.getColumnsFromPath());
            Assertions.assertEquals(Arrays.asList(false, false), descriptor.getColumnsFromPathIsNull());
            Assertions.assertEquals(file, range.getPath().get());
        }
    }

    @Test
    public void morIncrementalRespectsPhysicalPartitionEncoding() {
        for (boolean encoded : Arrays.asList(false, true)) {
            for (boolean forceJni : Arrays.asList(false, true)) {
                String path = encoded ? "dt=%253A" : "dt=%3A";
                FakeRelation mor = new FakeRelation();
                mor.fileSlices = Collections.singletonList(baseOnlySlice(path,
                        BASE_PATH + "/" + path + "/fileid_0_20240101000000.parquet"));
                HudiScanRange range = (HudiScanRange) HudiScanPlanProvider.incrementalRanges(
                        mor, false, forceJni, BASE_PATH, INPUT_FORMAT, SERDE,
                        Collections.singletonList("dt"), Collections.singletonList("string"),
                        Collections.singletonList("dt"), UnaryOperator.identity(), encoded).get().get(0);
                Assertions.assertEquals("%3A", range.getPartitionValues().get("dt"));
                Assertions.assertEquals(forceJni ? "jni" : "parquet", range.getFileFormat());
            }
        }
    }

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
                Collections.emptyList(), Collections.emptyList(), YEAR_MONTH, UnaryOperator.identity(), true);
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
                Collections.emptyList(), Collections.emptyList(), YEAR_MONTH, UnaryOperator.identity(), true);
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
                Collections.emptyList(), Collections.emptyList(), YEAR_MONTH, UnaryOperator.identity(), true);
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
                Arrays.asList("c1"), Arrays.asList("int"), YEAR_MONTH, UnaryOperator.identity(), true);
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
                Collections.emptyList(), Collections.emptyList(), YEAR_MONTH, UnaryOperator.identity(), true);
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
                slice.getPartitionPath(), YEAR_MONTH, true);
        HudiScanRange range = HudiScanPlanProvider.buildMorRange(slice, partValues, END_TS, /*forceJni*/ true,
                BASE_PATH, INPUT_FORMAT, SERDE, Arrays.asList("c1"), Arrays.asList("int"), p -> 7L,
                UnaryOperator.identity());
        Assertions.assertEquals("jni", range.getFileFormat());
        Assertions.assertEquals(END_TS, range.getProperties().get("hudi.instant_time"));
        Assertions.assertEquals(SERDE, range.getProperties().get("hudi.serde"));
        Assertions.assertEquals(BASE_PATH, range.getProperties().get("hudi.base_path"));
        // C4c: a JNI slice NEVER carries schema_id even when the resolver returns one (native field-id path only).
        Assertions.assertNull(range.getProperties().get("hudi.schema_id"));
    }

    @Test
    public void buildMorRangeDowngradesNoLogSliceToNativeWithoutForceJni() {
        // A base-only (no delta log) slice WITHOUT force_jni reads natively: format from the base file suffix, and
        // NO JNI instant metadata (the native reader needs only the path). Guards the native downgrade parity.
        FileSlice slice = baseOnlySlice("year=2024/month=01",
                "s3://b/t/year=2024/month=01/fileid-1_0_20240101000000.parquet");
        HudiScanRange range = HudiScanPlanProvider.buildMorRange(slice, Collections.emptyMap(), END_TS,
                /*forceJni*/ false, BASE_PATH, INPUT_FORMAT, SERDE,
                Arrays.asList("c1"), Arrays.asList("int"), p -> 7L, UnaryOperator.identity());
        Assertions.assertEquals("parquet", range.getFileFormat(),
                "a no-log slice without force_jni must downgrade to the native parquet reader");
        // C4c: a native downgraded slice carries the per-file schema_id from the resolver (native field-id path).
        Assertions.assertEquals("7", range.getProperties().get("hudi.schema_id"));
    }

    // ── native path scheme normalization (FIX-hudi-s3a-native-scheme) ────────────────────────────────────────

    @Test
    public void cowIncrementalNormalizesNativePathScheme() {
        // The COW @incr branch must thread incrementalRanges' normalizer into relation.collectSplits so the native
        // range path is rewritten s3a->s3 for BE's native S3 reader (which rejects s3a). Kills a mutation that drops
        // the normalizer or passes identity: the range path would stay s3a:// and BE would throw "Invalid S3 URI".
        FakeRelation cow = new FakeRelation();
        cow.rawCowPath = "s3a://datalake/warehouse/t/f1.parquet";
        cow.collectFileSlicesThrows = true;
        Optional<List<ConnectorScanRange>> result = HudiScanPlanProvider.incrementalRanges(
                cow, /*isCow*/ true, /*forceJni*/ false, BASE_PATH, INPUT_FORMAT, SERDE,
                Collections.emptyList(), Collections.emptyList(), YEAR_MONTH,
                s -> s.replace("s3a://", "s3://"), true);
        Assertions.assertTrue(result.isPresent());
        HudiScanRange range = (HudiScanRange) result.get().get(0);
        Assertions.assertEquals("s3://datalake/warehouse/t/f1.parquet", range.getPath().orElse(null),
                "COW @incr native range path must be scheme-normalized (s3a->s3) via the threaded normalizer");
    }

    @Test
    public void buildMorRangeNormalizesNativeNoLogSlicePathScheme() {
        // A no-log MOR slice reads native; buildMorRange must apply the normalizer to the base-file path (agencyPath)
        // so BE's native reader gets s3://, not the raw s3a:// from the Hudi SDK. The JNI metadata paths are a
        // separate concern (this slice reads native, so none are set).
        FileSlice slice = baseOnlySlice("year=2024/month=01",
                "s3a://datalake/warehouse/t/year=2024/month=01/fileid-1_0_20240101000000.parquet");
        HudiScanRange range = HudiScanPlanProvider.buildMorRange(slice, Collections.emptyMap(), END_TS,
                /*forceJni*/ false, BASE_PATH, INPUT_FORMAT, SERDE,
                Arrays.asList("c1"), Arrays.asList("int"), null, s -> s.replace("s3a://", "s3://"));
        Assertions.assertEquals("s3://datalake/warehouse/t/year=2024/month=01/fileid-1_0_20240101000000.parquet",
                range.getPath().orElse(null),
                "a native no-log MOR slice's range path must be scheme-normalized (s3a->s3) by buildMorRange");
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
        // When set, collectSplits builds ONE native range whose path is the normalizer applied to this raw URI —
        // stands in for the real COWIncrementalRelation.collectSplits (which needs a live metaClient), letting the
        // test verify incrementalRanges THREADS its normalizer into the COW collectSplits call.
        private String rawCowPath;

        @Override
        public List<FileSlice> collectFileSlices() {
            if (collectFileSlicesThrows) {
                throw new UnsupportedOperationException("collectFileSlices must not be called on this route");
            }
            return fileSlices;
        }

        @Override
        public List<HudiScanRange> collectSplits(UnaryOperator<String> nativePathNormalizer) {
            if (collectSplitsThrows) {
                throw new UnsupportedOperationException("collectSplits must not be called on this route");
            }
            if (rawCowPath != null) {
                return Collections.singletonList(new HudiScanRange.Builder()
                        .path(nativePathNormalizer.apply(rawCowPath)).fileFormat("parquet").build());
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
