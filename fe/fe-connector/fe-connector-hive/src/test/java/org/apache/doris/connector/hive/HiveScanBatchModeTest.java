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

package org.apache.doris.connector.hive;

import org.apache.doris.connector.hms.HmsClient;
import org.apache.doris.connector.hms.HmsClientException;
import org.apache.doris.connector.hms.HmsDatabaseInfo;
import org.apache.doris.connector.hms.HmsPartitionBatchResult;
import org.apache.doris.connector.hms.HmsPartitionBatchStats;
import org.apache.doris.connector.hms.HmsPartitionInfo;
import org.apache.doris.connector.hms.HmsTableInfo;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorStatementScope;
import org.apache.doris.connector.spi.ConnectorTaskScheduler;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.handle.ConnectorColumnHandle;
import org.apache.doris.connector.spi.scan.ConnectorScanContext;
import org.apache.doris.connector.spi.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.spi.scan.ConnectorScanProfile;
import org.apache.doris.connector.spi.scan.ConnectorScanRange;
import org.apache.doris.connector.spi.scan.ConnectorScanRequest;
import org.apache.doris.connector.spi.scan.ConnectorSplitSource;
import org.apache.doris.connector.spi.scan.ScanNodePropertyKeys;
import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.foundation.format.FileCompression;
import org.apache.doris.thrift.TFileCompressType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Tests the two connector-local batch-mode overrides {@link HiveScanPlanProvider} adds so a large partitioned
 * hive scan streams its splits per partition batch instead of materializing every file synchronously into the FE
 * heap (post-HMS-cutover, hive scans through the generic {@code PluginDrivenScanNode}, which enters batch mode
 * only when the connector opts in).
 *
 * <p>WHY (Rule 9): the two overrides encode two correctness-critical decisions.</p>
 * <ul>
 *   <li>{@code supportsBatchScan}: partitioned tables can use batch planning; transactional tables
 *       are rejected because their read visibility is no longer supported.</li>
 *   <li>{@code planScanForPartitionBatch}: hive's {@code planScan} resolves partitions from the handle's pruned
 *       set and IGNORES the passed partition set, so hive MUST override the batch hook to scope resolution to the
 *       batch. If it inherited the SPI default (which re-runs the whole-pruned-set {@code planScan} per batch),
 *       every partition's files would be emitted once per batch — DUPLICATE ROWS. The {@code ranges.size()==1}
 *       (and single-location listing) assertion below fails precisely under that bug.</li>
 * </ul>
 *
 * <p>No Mockito: reuses the proven {@code FakeHmsClient} echo (getPartitions(names) -&gt; one partition per name,
 * location = name) and a counting {@link HiveFileListingCache.DirectoryLister} so the listed locations can be
 * asserted, exactly as {@code HiveConnectorMetadataPartitionPruningTest} / {@code HiveFileListingCacheTest}.</p>
 */
public class HiveScanBatchModeTest {

    private static final List<String> PART_KEYS = Arrays.asList("year", "month");
    private static final String PARQUET_INPUT_FORMAT =
            "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat";
    private static final String PARQUET_SERDE =
            "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe";
    private static final String TEXT_INPUT_FORMAT = "org.apache.hadoop.mapred.TextInputFormat";
    private static final String LZO_TEXT_INPUT_FORMAT = "com.hadoop.mapred.DeprecatedLzoTextInputFormat";
    private static final String LAZY_SIMPLE_SERDE = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe";
    private static final String OPEN_CSV_SERDE = "org.apache.hadoop.hive.serde2.OpenCSVSerde";

    @Test
    public void millionFilesOnlyCreateTheConsumedSplitPrefix() throws Exception {
        AtomicInteger normalizedFiles = new AtomicInteger();
        AtomicInteger listings = new AtomicInteger();
        HiveFileListingCache cache = new HiveFileListingCache(HiveTestProperties.minimal(), (location, fs) -> {
            listings.incrementAndGet();
            return Collections.nCopies(1000000, new HiveFileStatus(location + "/data.parquet", 10, 1));
        });
        FakeConnectorContext context = new FakeConnectorContext() {
            @Override
            public String normalizeStorageUri(String uri) {
                normalizedFiles.incrementAndGet();
                return uri;
            }
        };
        HiveScanPlanProvider provider = new HiveScanPlanProvider(null, HiveTestProperties.minimal(), context,
                cache);
        HiveTableHandle handle = parquetHandle(Collections.emptyList());
        try (ConnectorSplitSource source = provider.streamSplits(new FakeSession(),
                ConnectorScanRequest.builder(handle, Collections.emptyList())
                        .filter(Optional.empty()).limit(-1).build())) {
            Assertions.assertEquals(0, listings.get());
            for (int i = 0; i < 128; i++) {
                Assertions.assertTrue(source.hasNext());
                Assertions.assertEquals(10, source.next().getLength());
            }
            Assertions.assertEquals(128, normalizedFiles.get());
            Assertions.assertEquals(1, listings.get());
            source.close();
            Assertions.assertFalse(source.hasNext());
            Assertions.assertEquals(128, normalizedFiles.get());
        }
    }

    @Test
    public void hugeSingleFileDoesNotMaterializeItsSplitList() throws Exception {
        HiveScanPlanProvider provider = provider(null, (location, fs) -> Collections.singletonList(
                new HiveFileStatus(location + "/large.parquet", Long.MAX_VALUE, 1)));
        FakeSession session = new FakeSession(Collections.singletonMap("file_split_size", "1"));
        try (ConnectorSplitSource source = provider.streamSplits(session,
                ConnectorScanRequest.builder(parquetHandle(Collections.emptyList()), Collections.emptyList())
                        .filter(Optional.empty()).limit(-1).build())) {
            for (int i = 0; i < 3; i++) {
                Assertions.assertTrue(source.hasNext());
                HiveScanRange range = (HiveScanRange) source.next();
                Assertions.assertEquals(i, range.getStart());
                Assertions.assertEquals(1, range.getLength());
            }
        }
    }

    @Test
    public void streamingAndSynchronousSplitsMatchAcrossPartitionsAndFileBoundaries() throws Exception {
        HiveScanPlanProvider provider = provider(new FakeHmsClient(), (location, fs) -> Arrays.asList(
                new HiveFileStatus(location + "/empty.parquet", 0, 1),
                new HiveFileStatus(location + "/small.parquet", 3, 2),
                new HiveFileStatus(location + "/large.parquet", 35, 3)));
        HiveTableHandle handle = parquetHandle(PART_KEYS).toBuilder().prunedPartitions(Arrays.asList(
                new HmsPartitionInfo(Arrays.asList("2024", "01"), "year=2024/month=01",
                        null, null, null, Collections.emptyMap()),
                new HmsPartitionInfo(Arrays.asList("2024", "02"), "year=2024/month=02",
                        null, null, null, Collections.emptyMap()))).build();
        FakeSession session = new FakeSession(Collections.singletonMap("file_split_size", "8"));
        List<ConnectorScanRange> synchronous = provider.planScan(session,
                ConnectorScanRequest.builder(handle, Collections.emptyList()).build());
        List<ConnectorScanRange> streaming = new ArrayList<>();
        try (ConnectorSplitSource source = provider.streamSplits(session,
                ConnectorScanRequest.builder(handle, Collections.emptyList())
                        .filter(Optional.empty()).limit(-1).build())) {
            while (source.hasNext()) {
                streaming.add(source.next());
            }
        }
        long[] starts = {0, 0, 8, 16, 24, 32};
        long[] lengths = {3, 8, 8, 8, 8, 3};
        for (List<ConnectorScanRange> ranges : Arrays.asList(synchronous, streaming)) {
            Assertions.assertEquals(12, ranges.size());
            for (int partition = 0; partition < 2; partition++) {
                String month = partition == 0 ? "01" : "02";
                Map<String, String> partitionValues = new HashMap<>();
                partitionValues.put("year", "2024");
                partitionValues.put("month", month);
                for (int i = 0; i < starts.length; i++) {
                    HiveScanRange range = (HiveScanRange) ranges.get(partition * starts.length + i);
                    String fileName = i == 0 ? "small.parquet" : "large.parquet";
                    Assertions.assertEquals("year=2024/month=" + month + "/" + fileName, range.getPath().get());
                    Assertions.assertEquals(starts[i], range.getStart());
                    Assertions.assertEquals(lengths[i], range.getLength());
                    Assertions.assertEquals(partitionValues, range.getPartitionValues());
                }
            }
        }
    }

    @Test
    public void cancellationBetweenEmptyPartitionsDoesNotListRemainingPartitions() throws Exception {
        AtomicBoolean cancelled = new AtomicBoolean();
        AtomicInteger listings = new AtomicInteger();
        ConnectorTaskScheduler scheduler = new ConnectorTaskScheduler() {
            @Override
            public boolean isClosed() {
                return cancelled.get();
            }

            @Override
            public boolean tryExecute(Runnable task, Consumer<Throwable> failure, Runnable onSpace) {
                throw new UnsupportedOperationException("This source only observes the query lifetime");
            }

            @Override
            public void removeSpaceListener(Runnable onSpace) {
            }

            @Override
            public void execute(Runnable task) {
                throw new UnsupportedOperationException("This source only observes the query lifetime");
            }
        };
        HiveScanPlanProvider provider = provider(new FakeHmsClient(), (location, fs) -> {
            listings.incrementAndGet();
            cancelled.set(true);
            return Collections.emptyList();
        });
        HiveTableHandle handle = parquetHandle(PART_KEYS).toBuilder().prunedPartitions(Arrays.asList(
                part("year=2024/month=01"), part("year=2024/month=02"))).build();
        FakeSession session = new FakeSession(Collections.singletonMap("split_partition_batch_size", "1"),
                scheduler);
        try (ConnectorSplitSource source = provider.streamSplits(session,
                ConnectorScanRequest.builder(handle, Collections.emptyList()).build())) {
            Assertions.assertFalse(source.hasNext());
            Assertions.assertEquals(1, listings.get(), "cancellation must stop listing the next empty partition");
        }
    }

    @Test
    public void closingFirstPartitionDoesNotListRemainingPartitions() throws Exception {
        CountingLister lister = new CountingLister();
        HiveScanPlanProvider provider = provider(new FakeHmsClient(), lister);
        HiveTableHandle handle = parquetHandle(PART_KEYS).toBuilder().prunedPartitions(Arrays.asList(
                part("year=2024/month=01"), part("year=2024/month=02"))).build();
        try (ConnectorSplitSource source = provider.streamSplits(new FakeSession(),
                ConnectorScanRequest.builder(handle, Collections.emptyList())
                        .filter(Optional.empty()).limit(-1).build())) {
            Assertions.assertEquals(0, lister.totalCalls);
            Assertions.assertTrue(source.hasNext());
            source.next();
            Assertions.assertEquals(1, lister.totalCalls);
            source.close();
            Assertions.assertFalse(source.hasNext());
            Assertions.assertNull(lister.callsPerLocation.get("year=2024/month=02"));
        }
    }

    @Test
    public void streamingUsesEnginePartitionSelectionWithoutListingOtherPartitions() throws Exception {
        CountingLister lister = new CountingLister();
        // listPartitionNames fails if called: the engine already supplies the selected names.
        HiveScanPlanProvider provider = provider(new FakeHmsClient(), lister);
        HiveTableHandle handle = parquetHandle(PART_KEYS);
        ConnectorScanRequest request = ConnectorScanRequest.builder(handle, Collections.emptyList())
                .requiredPartitions(Collections.singletonList("year=2024/month=02")).build();
        try (ConnectorSplitSource source = provider.streamSplits(new FakeSession(), request)) {
            Assertions.assertTrue(source.hasNext());
            HiveScanRange range = (HiveScanRange) source.next();
            Assertions.assertEquals("year=2024/month=02/000000_0", range.getPath().get());
            Assertions.assertFalse(source.hasNext());
        }
        Assertions.assertEquals(1, lister.totalCalls);
        Assertions.assertEquals(Collections.singleton("year=2024/month=02"), lister.callsPerLocation.keySet());
    }

    @Test
    public void streamingPassesCancellationToPartitionMetadataLookup() throws Exception {
        ConnectorScanContext context = new ConnectorScanContext();
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        HmsClient client = new FakeHmsClient() {
            @Override
            public HmsPartitionBatchResult getExistingPartitionsWithStats(String db, String table,
                    List<String> names) {
                throw new AssertionError("Streaming metadata must carry its scan context");
            }

            @Override
            public HmsPartitionBatchResult getExistingPartitionsWithStats(String db, String table,
                    List<String> names, ConnectorScanContext actual) {
                Assertions.assertSame(context, actual);
                entered.countDown();
                try {
                    Assertions.assertTrue(release.await(5, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                }
                actual.checkCancelled();
                throw new AssertionError("The metadata lookup was cancelled");
            }
        };
        CountingLister lister = new CountingLister();
        HiveScanPlanProvider provider = provider(client, lister);
        ConnectorScanRequest request = ConnectorScanRequest.builder(parquetHandle(PART_KEYS),
                Collections.emptyList()).requiredPartitions(Collections.singletonList("year=2024/month=02"))
                .scanContext(context).build();
        ExecutorService worker = Executors.newSingleThreadExecutor();
        try (ConnectorSplitSource source = provider.streamSplits(new FakeSession(), request)) {
            Future<Boolean> pending = worker.submit(source::hasNext);
            Assertions.assertTrue(entered.await(5, TimeUnit.SECONDS));
            context.cancel();
            release.countDown();
            ExecutionException error = Assertions.assertThrows(ExecutionException.class,
                    () -> pending.get(5, TimeUnit.SECONDS));
            Assertions.assertInstanceOf(CancellationException.class, error.getCause());
            Assertions.assertEquals(0, lister.totalCalls);
        } finally {
            release.countDown();
            worker.shutdownNow();
            Assertions.assertTrue(worker.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    public void partitionMetadataBatchSizeDoesNotFollowBatchModeThreshold() throws Exception {
        List<Integer> requestedBatchSizes = new ArrayList<>();
        FakeHmsClient hms = new FakeHmsClient() {
            @Override
            public HmsPartitionBatchResult getExistingPartitionsWithStats(
                    String dbName, String tableName, List<String> partitionNames) {
                requestedBatchSizes.add(partitionNames.size());
                return super.getExistingPartitionsWithStats(dbName, tableName, partitionNames);
            }
        };
        CountingLister lister = new CountingLister();
        HiveScanPlanProvider provider = provider(hms, lister);
        List<String> names = Arrays.asList("year=2024/month=01", "year=2024/month=02",
                "year=2024/month=03", "year=2024/month=04", "year=2024/month=05");
        Map<String, String> settings = new HashMap<>();
        settings.put("num_partitions_in_batch_mode", "1");
        settings.put("split_partition_batch_size", "2");
        ConnectorScanRequest request = ConnectorScanRequest.builder(
                parquetHandle(PART_KEYS), Collections.emptyList()).requiredPartitions(names).build();

        int ranges = 0;
        try (ConnectorSplitSource source = provider.streamSplits(new FakeSession(settings), request)) {
            while (source.hasNext()) {
                source.next();
                ranges++;
            }
        }

        Assertions.assertEquals(5, ranges);
        Assertions.assertEquals(Arrays.asList(2, 2, 1), requestedBatchSizes);
        Assertions.assertEquals(5, lister.totalCalls);
    }

    @Test
    public void sharedCompressionNamesMapToNativeCodecs() {
        for (FileCompression codec : FileCompression.values()) {
            Assertions.assertDoesNotThrow(() -> TFileCompressType.valueOf(codec.name()));
        }
    }

    @Test
    public void compressedTextFilesStayWholeInEveryPlanningPath() throws Exception {
        long splitSize = 8L * 1024 * 1024;
        long fileSize = 2 * splitSize + 3;
        List<String> extensions = Arrays.asList(".gz", ".bz2", ".lz4", ".lzo", ".lzo_deflate",
                ".deflate", ".snappy", ".zst", ".zstd", ".GZ");
        String partition = "year=2024/month=01";
        String[] inputs = {TEXT_INPUT_FORMAT, TEXT_INPUT_FORMAT, PARQUET_INPUT_FORMAT,
                "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat"};
        String[] serdes = {LAZY_SIMPLE_SERDE, OPEN_CSV_SERDE, PARQUET_SERDE,
                "org.apache.hadoop.hive.ql.io.orc.OrcSerde"};
        for (int format = 0; format < inputs.length; format++) {
            HiveScanPlanProvider provider = provider(new FakeHmsClient(), (location, fs) -> {
                List<HiveFileStatus> files = new ArrayList<>();
                for (String extension : extensions) {
                    files.add(new HiveFileStatus(location + "/data" + extension, fileSize, 1));
                }
                files.add(new HiveFileStatus(location + "/plain", fileSize, 1));
                files.add(new HiveFileStatus(location + "/empty.gz", 0, 1));
                return files;
            });
            HiveTableHandle handle = parquetHandle(PART_KEYS).toBuilder()
                    .inputFormat(inputs[format]).serializationLib(serdes[format])
                    .prunedPartitions(Collections.singletonList(part(partition))).build();
            ConnectorScanRequest request = scanOf(handle);
            FakeSession session = new FakeSession(Collections.singletonMap("file_split_size", "" + splitSize));
            List<ConnectorScanRange> synchronous = provider.planScan(session, request);
            List<ConnectorScanRange> batch = provider.planScanForPartitionBatch(session, request,
                    Collections.singletonList(partition));
            List<ConnectorScanRange> streaming = new ArrayList<>();
            try (ConnectorSplitSource source = provider.streamSplits(session, request)) {
                while (source.hasNext()) {
                    streaming.add(source.next());
                }
            }
            for (List<ConnectorScanRange> ranges : Arrays.asList(synchronous, batch, streaming)) {
                boolean text = format < 2;
                Assertions.assertEquals(text ? extensions.size() + 3 : (extensions.size() + 1) * 3,
                        ranges.size(), "only plain text and columnar files support byte-range splitting");
                for (String extension : extensions) {
                    List<ConnectorScanRange> fileRanges = new ArrayList<>();
                    for (ConnectorScanRange range : ranges) {
                        if (range.getPath().get().equals(partition + "/data" + extension)) {
                            fileRanges.add(range);
                        }
                    }
                    Assertions.assertEquals(text ? 1 : 3, fileRanges.size());
                    Assertions.assertEquals(0, fileRanges.get(0).getStart());
                    Assertions.assertEquals(text ? fileSize : splitSize, fileRanges.get(0).getLength());
                }
                int plainStart = ranges.size() - 3;
                for (int i = 0; i < 3; i++) {
                    ConnectorScanRange range = ranges.get(plainStart + i);
                    Assertions.assertEquals(partition + "/plain", range.getPath().get());
                    Assertions.assertEquals(i * splitSize, range.getStart());
                    Assertions.assertEquals(i == 2 ? 3 : splitSize, range.getLength());
                }
            }
        }
    }

    @Test
    public void partitionBatchAndStreamingMatchAcrossFormatsAndListingFailures() throws Exception {
        List<String> batch = Arrays.asList("year=2024/month=01", "year=2024/month=02", "year=2024/month=03");
        for (boolean lzo : Arrays.asList(false, true)) {
            Map<String, Integer> listings = new HashMap<>();
            HiveScanPlanProvider provider = provider(new FakeHmsClient(), (location, fs) -> {
                listings.merge(location, 1, Integer::sum);
                if (location.equals(batch.get(0))) {
                    return Collections.emptyList();
                }
                if (location.equals(batch.get(1))) {
                    throw new HiveDirectoryListingException("unreadable partition", new IOException("retry"));
                }
                String extension = lzo ? ".lzo" : ".parquet";
                List<HiveFileStatus> files = new ArrayList<>(Arrays.asList(
                        new HiveFileStatus(location + "/empty" + extension, 0, 1),
                        new HiveFileStatus(location + "/small" + extension, 3, 2),
                        new HiveFileStatus(location + "/large" + extension, 35, 3)));
                if (lzo) {
                    files.add(new HiveFileStatus(location + "/large.lzo.index", 20, 4));
                    files.add(new HiveFileStatus(location + "/other.txt", 20, 4));
                }
                return files;
            });
            HiveTableHandle handle = parquetHandle(PART_KEYS).toBuilder()
                    .inputFormat(lzo ? LZO_TEXT_INPUT_FORMAT : PARQUET_INPUT_FORMAT)
                    .serializationLib(lzo ? LAZY_SIMPLE_SERDE : PARQUET_SERDE)
                    .prunedPartitions(Collections.singletonList(part("year=2023/month=12"))).build();
            Map<String, String> settings = new HashMap<>();
            settings.put("file_split_size", "8");
            settings.put("split_partition_batch_size", "1");
            FakeSession session = new FakeSession(settings);
            ConnectorScanRequest request = ConnectorScanRequest.builder(handle, Collections.emptyList())
                    .requiredPartitions(Collections.singletonList("year=2023/month=12")).build();

            Assertions.assertTrue(provider.planScanForPartitionBatch(session, request,
                    Collections.emptyList()).isEmpty());
            Assertions.assertTrue(listings.isEmpty(), "An empty batch must not enumerate the handle's partitions");
            List<ConnectorScanRange> synchronous = provider.planScanForPartitionBatch(session, request, batch);
            List<ConnectorScanRange> streaming = new ArrayList<>();
            try (ConnectorSplitSource source = provider.streamSplits(session, request.withRequiredPartitions(batch))) {
                while (source.hasNext()) {
                    streaming.add(source.next());
                }
            }
            long[] starts = lzo ? new long[] {0, 0} : new long[] {0, 0, 8, 16, 24, 32};
            long[] lengths = lzo ? new long[] {3, 35} : new long[] {3, 8, 8, 8, 8, 3};
            for (List<ConnectorScanRange> ranges : Arrays.asList(synchronous, streaming)) {
                Assertions.assertEquals(starts.length, ranges.size());
                for (int i = 0; i < starts.length; i++) {
                    ConnectorScanRange range = ranges.get(i);
                    String file = i == 0 ? "/small" : "/large";
                    Assertions.assertEquals(batch.get(2) + file + (lzo ? ".lzo" : ".parquet"),
                            range.getPath().get());
                    Assertions.assertEquals(starts[i], range.getStart());
                    Assertions.assertEquals(lengths[i], range.getLength());
                }
            }
            Assertions.assertEquals(new HashSet<>(batch), listings.keySet());
            Assertions.assertEquals(2, (int) listings.get(batch.get(1)),
                    "A tolerated directory failure must not be cached by either enumeration path");
        }
    }

    @Test
    public void streamingDecisionUsesCachedCountWithoutListing() {
        AtomicInteger listings = new AtomicInteger();
        HiveFileListingCache cache = new HiveFileListingCache(HiveTestProperties.minimal(), (location, fs) -> {
            listings.incrementAndGet();
            return Collections.nCopies(3, new HiveFileStatus(location + "/data.parquet", 10, 1));
        });
        HiveScanPlanProvider provider = new HiveScanPlanProvider(null, HiveTestProperties.minimal(),
                new FakeConnectorContext(), cache);
        HiveTableHandle handle = parquetHandle(Collections.emptyList());
        FakeSession session = new FakeSession(Collections.singletonMap("num_files_in_batch_mode", "2"));
        Assertions.assertTrue(provider.streamingSplitEstimate(session, handle, Optional.empty(), false) >= 0);
        Assertions.assertEquals(0, listings.get());
        cache.listDataFiles("db", "t", handle.getLocation(), null);
        Assertions.assertEquals(3, provider.streamingSplitEstimate(session, handle, Optional.empty(), false));
        Assertions.assertEquals(-1, provider.streamingSplitEstimate(new FakeSession(), handle, Optional.empty(), false));
        Assertions.assertEquals(1, listings.get());
    }

    private static HiveTableHandle parquetHandle(List<String> partitionKeys) {
        return new HiveTableHandle.Builder("db", "t", HiveTableType.HIVE)
                .inputFormat(PARQUET_INPUT_FORMAT).serializationLib(PARQUET_SERDE)
                .location("s3://bucket/table").partitionKeyNames(partitionKeys).build();
    }

    @Test
    public void scanReuseNamespaceUsesConnectorType() {
        String prefix = new HiveConnectorProvider().getType() + ".";
        Assertions.assertTrue(HiveScanPlanProvider.SCAN_REUSE_NAMESPACE.startsWith(prefix));
    }

    // ==================== supportsBatchScan: partitioned AND non-transactional ====================

    @Test
    public void supportsBatchScanIsTrueForPartitionedNonTransactionalTable() {
        HiveScanPlanProvider provider = provider(null, new CountingLister());
        HiveTableHandle handle = new HiveTableHandle.Builder("db", "t", HiveTableType.HIVE)
                .partitionKeyNames(PART_KEYS)
                .build();
        // WHY: a partitioned, non-transactional table is exactly the case that must stream its splits per batch.
        Assertions.assertTrue(provider.supportsBatchScan(new FakeSession(), handle));
    }

    @Test
    public void partitionBatchSizePreservesParallelListingAndBoundsHmsRequests() {
        HiveScanPlanProvider provider = provider(null, new CountingLister());
        Assertions.assertEquals(16, provider.partitionBatchSize(new FakeSession(), 1000, 64));
        Assertions.assertEquals(5, provider.partitionBatchSize(new FakeSession(), 9, 2));
        Assertions.assertEquals(2, provider.partitionBatchSize(
                new FakeSession(Collections.singletonMap("split_partition_batch_size", "2")), 1000, 64));
    }

    @Test
    public void supportsBatchScanIsFalseForUnpartitionedTable() {
        HiveScanPlanProvider provider = provider(null, new CountingLister());
        HiveTableHandle handle = new HiveTableHandle.Builder("db", "t", HiveTableType.HIVE)
                .partitionKeyNames(Collections.emptyList())
                .build();
        // WHY: an unpartitioned table has a single partition (the table location); there is nothing to batch, so
        // it stays on the synchronous planScan path.
        Assertions.assertFalse(provider.supportsBatchScan(new FakeSession(), handle));
    }

    @Test
    public void supportsBatchScanRejectsTransactionalPartitionedTable() {
        HiveScanPlanProvider provider = provider(null, new CountingLister());
        Map<String, String> txnParams = new HashMap<>();
        txnParams.put("transactional", "true");
        HiveTableHandle handle = new HiveTableHandle.Builder("db", "t", HiveTableType.HIVE)
                .partitionKeyNames(PART_KEYS)
                .tableParameters(txnParams)
                .build();
        Assertions.assertThrows(DorisConnectorException.class,
                () -> provider.supportsBatchScan(new FakeSession(), handle));
    }

    // ==================== planScanForPartitionBatch: scoped to the batch, no duplication ====================

    @Test
    public void planScanForPartitionBatchResolvesOnlyTheBatch() {
        CountingLister lister = new CountingLister();
        // getPartitions echoes each requested name back as a partition whose location IS the name, so the counting
        // lister's keys are the partition names actually resolved for this batch.
        HiveScanPlanProvider provider = provider(new FakeHmsClient(), lister);

        // The handle carries the FULL pruned set (all three partitions). If the batch hook were NOT scoped to the
        // batch (SPI default -> whole-pruned-set planScan), all three would be listed -> 3 ranges.
        HiveTableHandle handle = new HiveTableHandle.Builder("db", "t", HiveTableType.HIVE)
                .inputFormat(PARQUET_INPUT_FORMAT)
                .serializationLib(PARQUET_SERDE)
                .partitionKeyNames(PART_KEYS)
                .prunedPartitions(Arrays.asList(
                        part("year=2023/month=12"),
                        part("year=2024/month=01"),
                        part("year=2024/month=02")))
                .build();

        List<ConnectorScanRange> ranges = provider.planScanForPartitionBatch(new FakeSession(),
                ConnectorScanRequest.builder(handle, Collections.<ConnectorColumnHandle>emptyList())
                .build(), Collections.singletonList("year=2024/month=01"));

        // WHY (Rule 9): exactly one range for the single-partition batch. This fails (== 3) precisely if the batch
        // is not partition-scoped — the duplicate-splits bug the override exists to prevent.
        Assertions.assertEquals(1, ranges.size());
        // ...and the file lister was asked ONLY for the batch's partition, never the other two.
        Assertions.assertEquals(1, lister.totalCalls);
        Assertions.assertEquals(1, (int) lister.callsPerLocation.get("year=2024/month=01"));
        Assertions.assertNull(lister.callsPerLocation.get("year=2023/month=12"));
        Assertions.assertNull(lister.callsPerLocation.get("year=2024/month=02"));
    }

    @Test
    public void fullScanOmitsPartitionDroppedAfterNameListing() {
        CountingLister lister = new CountingLister();
        List<String> listed = Arrays.asList("year=2024/month=01", "year=2024/month=02");
        HiveScanPlanProvider provider = provider(
                new FakeHmsClient(listed, "year=2024/month=01"), lister);
        HiveTableHandle handle = new HiveTableHandle.Builder("db", "t", HiveTableType.HIVE)
                .inputFormat(PARQUET_INPUT_FORMAT)
                .serializationLib(PARQUET_SERDE)
                .partitionKeyNames(PART_KEYS)
                .build();

        List<ConnectorScanRange> ranges = provider.planScan(new FakeSession(),
                ConnectorScanRequest.builder(handle, Collections.<ConnectorColumnHandle>emptyList()).build());

        Assertions.assertEquals(1, ranges.size());
        Assertions.assertNull(lister.callsPerLocation.get("year=2024/month=01"));
        Assertions.assertEquals(1, (int) lister.callsPerLocation.get("year=2024/month=02"));
    }

    @Test
    public void partitionBatchStatsAreExposedAsOneScanProfile() {
        HiveScanPlanProvider provider = provider(new FakeHmsClient(), new CountingLister());
        HiveTableHandle handle = new HiveTableHandle.Builder("db", "t", HiveTableType.HIVE)
                .inputFormat(PARQUET_INPUT_FORMAT)
                .serializationLib(PARQUET_SERDE)
                .partitionKeyNames(PART_KEYS)
                .build();
        FakeSession session = new FakeSession();
        ConnectorScanRequest request = ConnectorScanRequest.builder(
                handle, Collections.<ConnectorColumnHandle>emptyList()).build();

        provider.planScanForPartitionBatch(session, request,
                Arrays.asList("year=2024/month=01", "year=2024/month=02"));
        provider.planScanForPartitionBatch(session, request,
                Collections.singletonList("year=2024/month=03"));

        List<ConnectorScanProfile> profiles = provider.collectScanProfiles(session);
        Assertions.assertEquals(1, profiles.size());
        ConnectorScanProfile profile = profiles.get(0);
        Assertions.assertEquals("Connector Metadata Access", profile.getGroupName());
        Assertions.assertTrue(profile.getScanLabel().contains("db.t"));
        Assertions.assertEquals("2", profile.getMetrics().get("LogicalRequests"));
        Assertions.assertEquals("0", profile.getMetrics().get("FailedRequests"));
        Assertions.assertEquals("3", profile.getMetrics().get("RequestedItems"));
        Assertions.assertEquals("2", profile.getMetrics().get("TransportInvocations"));
        Assertions.assertEquals("3", profile.getMetrics().get("TransportItems"));
        Assertions.assertEquals("2", profile.getMetrics().get("LargestBatchSize"));
        Assertions.assertEquals("1", profile.getMetrics().get("SmallestBatchSize"));
        Assertions.assertTrue(provider.collectScanProfiles(session).isEmpty());
    }

    @Test
    public void firstFailedPartitionRequestIsExposedForSynchronousAndBatchPlanning() {
        assertFailedPartitionProfile(false, HmsPartitionBatchStats.builder()
                .requestedItems(2)
                .transportInvocations(1)
                .transportItems(2)
                .largestBatchSize(2)
                .smallestBatchSize(2)
                .build());
        assertFailedPartitionProfile(true, HmsPartitionBatchStats.builder()
                .requestedItems(2)
                .transportInvocations(1)
                .transportItems(2)
                .largestBatchSize(2)
                .smallestBatchSize(2)
                .build());
    }

    @Test
    public void exhaustedFallbackIsExposedForSynchronousAndBatchPlanning() {
        HmsPartitionBatchStats stats = HmsPartitionBatchStats.builder()
                .requestedItems(2)
                .transportInvocations(2)
                .transportItems(3)
                .largestBatchSize(2)
                .smallestBatchSize(1)
                .fallbackCount(1)
                .build();
        assertFailedPartitionProfile(false, stats);
        assertFailedPartitionProfile(true, stats);
    }

    private static void assertFailedPartitionProfile(boolean batchPlanning, HmsPartitionBatchStats stats) {
        List<String> names = Arrays.asList("year=2024/month=01", "year=2024/month=02");
        HmsClientException partitionFailure = new HmsClientException("failed", stats);
        HiveScanPlanProvider provider = provider(
                new FakeHmsClient(names, null, partitionFailure), new CountingLister());
        HiveTableHandle handle = new HiveTableHandle.Builder("db", "t", HiveTableType.HIVE)
                .inputFormat(PARQUET_INPUT_FORMAT)
                .serializationLib(PARQUET_SERDE)
                .partitionKeyNames(PART_KEYS)
                .build();
        FakeSession session = new FakeSession();
        ConnectorScanRequest request = ConnectorScanRequest.builder(
                handle, Collections.<ConnectorColumnHandle>emptyList()).build();

        if (batchPlanning) {
            Assertions.assertThrows(HmsClientException.class,
                    () -> provider.planScanForPartitionBatch(session, request, names));
        } else {
            Assertions.assertThrows(HmsClientException.class,
                    () -> provider.planScan(session, request));
        }

        ConnectorScanProfile profile = provider.collectScanProfiles(session).get(0);
        Assertions.assertEquals("1", profile.getMetrics().get("LogicalRequests"));
        Assertions.assertEquals("1", profile.getMetrics().get("FailedRequests"));
        Assertions.assertEquals(String.valueOf(stats.getRequestedItems()),
                profile.getMetrics().get("RequestedItems"));
        Assertions.assertEquals(String.valueOf(stats.getTransportInvocations()),
                profile.getMetrics().get("TransportInvocations"));
        Assertions.assertEquals(String.valueOf(stats.getTransportItems()),
                profile.getMetrics().get("TransportItems"));
        Assertions.assertEquals(String.valueOf(stats.getFallbackCount()), profile.getMetrics().get("Fallbacks"));
    }

    @Test
    public void predicatePruningStatsSurvivePartitionBatchPlanning() {
        HiveScanPlanProvider provider = provider(new FakeHmsClient(), new CountingLister());
        HmsPartitionBatchStats pruningStats = HmsPartitionBatchStats.builder()
                .requestedItems(3)
                .transportInvocations(1)
                .transportItems(3)
                .largestBatchSize(3)
                .smallestBatchSize(3)
                .build();
        HiveTableHandle handle = new HiveTableHandle.Builder("db", "t", HiveTableType.HIVE)
                .inputFormat(PARQUET_INPUT_FORMAT)
                .serializationLib(PARQUET_SERDE)
                .partitionKeyNames(PART_KEYS)
                .pruningBatchStats(pruningStats)
                .build();
        FakeSession session = new FakeSession();
        ConnectorScanRequest request = ConnectorScanRequest.builder(
                handle, Collections.<ConnectorColumnHandle>emptyList()).build();

        provider.planScanForPartitionBatch(session, request,
                Collections.singletonList("year=2024/month=01"));

        ConnectorScanProfile profile = provider.collectScanProfiles(session).get(0);
        Assertions.assertEquals("2", profile.getMetrics().get("LogicalRequests"));
        Assertions.assertEquals("4", profile.getMetrics().get("RequestedItems"));
        Assertions.assertEquals("2", profile.getMetrics().get("TransportInvocations"));
        Assertions.assertEquals("4", profile.getMetrics().get("TransportItems"));
        Assertions.assertEquals("3", profile.getMetrics().get("LargestBatchSize"));
        Assertions.assertEquals("1", profile.getMetrics().get("SmallestBatchSize"));
    }

    @Test
    public void predicatePruningStatsAreAvailableBeforeScanPlanning() {
        HiveScanPlanProvider provider = provider(new FakeHmsClient(), new CountingLister());
        HmsPartitionBatchStats pruningStats = HmsPartitionBatchStats.builder()
                .requestedItems(3)
                .transportInvocations(1)
                .transportItems(3)
                .largestBatchSize(3)
                .smallestBatchSize(3)
                .build();
        HiveTableHandle handle = new HiveTableHandle.Builder("db", "t", HiveTableType.HIVE)
                .partitionKeyNames(PART_KEYS)
                .pruningBatchStats(pruningStats)
                .build();
        FakeSession session = new FakeSession();

        HiveConnector connector = new HiveConnector(HiveTestProperties.minimalMap(), new FakeConnectorContext()) {
            @Override
            public ConnectorScanPlanProvider getScanPlanProvider() {
                return provider;
            }
        };

        ConnectorScanPlanProvider selectedProvider = connector.getScanPlanProvider(handle);

        Assertions.assertSame(provider, selectedProvider);
        ConnectorScanProfile profile = selectedProvider.collectScanProfiles(session).get(0);
        Assertions.assertEquals("1", profile.getMetrics().get("LogicalRequests"));
        Assertions.assertEquals("3", profile.getMetrics().get("RequestedItems"));
        Assertions.assertEquals("1", profile.getMetrics().get("TransportInvocations"));
    }

    @Test
    public void predicatePruningFailureProfileTransfersThroughTheStatementScope() {
        HiveScanPlanProvider provider = provider(new FakeHmsClient(), new CountingLister());
        FakeSession session = new FakeSession();
        HmsPartitionBatchStats failedStats = HmsPartitionBatchStats.builder()
                .requestedItems(3)
                .transportInvocations(2)
                .transportItems(4)
                .fallbackCount(1)
                .build();

        HiveScanPlanProvider.recordPruningFailure(session, "db", "t", failedStats);

        ConnectorScanProfile profile = provider.collectScanProfiles(session).get(0);
        Assertions.assertEquals("1", profile.getMetrics().get("LogicalRequests"));
        Assertions.assertEquals("1", profile.getMetrics().get("FailedRequests"));
        Assertions.assertEquals("2", profile.getMetrics().get("TransportInvocations"));
        Assertions.assertTrue(provider.collectScanProfiles(session).isEmpty());
    }

    @Test
    public void statementReuseKeepsZeroPrunedAndUnprunedScansDistinct() {
        CountingLister lister = new CountingLister();
        int[] partitionListCalls = new int[1];
        HmsClient hmsClient = new FakeHmsClient() {
            @Override
            public List<String> listPartitionNames(String dbName, String tableName, int maxParts) {
                partitionListCalls[0]++;
                return Collections.singletonList("year=2024/month=01");
            }
        };
        HiveScanPlanProvider provider = provider(hmsClient, lister);
        HiveTableHandle unpruned = new HiveTableHandle.Builder("db", "t", HiveTableType.HIVE)
                .inputFormat(PARQUET_INPUT_FORMAT)
                .serializationLib(PARQUET_SERDE)
                .partitionKeyNames(PART_KEYS)
                .build();
        HiveTableHandle zeroPruned = unpruned.toBuilder()
                .prunedPartitions(Collections.emptyList())
                .build();
        ConnectorSession session = new ScopeSession(7L, "same-statement", new TestStatementScope());

        List<ConnectorScanRange> zeroRanges = provider.planScan(session,
                ConnectorScanRequest.builder(zeroPruned, Collections.<ConnectorColumnHandle>emptyList()).build());
        List<ConnectorScanRange> allRanges = provider.planScan(session,
                ConnectorScanRequest.builder(unpruned, Collections.<ConnectorColumnHandle>emptyList()).build());

        Assertions.assertTrue(zeroRanges.isEmpty());
        Assertions.assertEquals(1, allRanges.size(),
                "an unpruned alias must not reuse a zero-pruned alias's empty ranges");
        Assertions.assertEquals(1, partitionListCalls[0]);
        Assertions.assertEquals(1, lister.totalCalls);
    }

    @Test
    public void statementReusePlansAnIdenticalScanOnce() {
        CountingLister lister = new CountingLister();
        HiveScanPlanProvider provider = provider(new FakeHmsClient(), lister);
        HiveTableHandle handle = new HiveTableHandle.Builder("db", "t", HiveTableType.HIVE)
                .inputFormat(PARQUET_INPUT_FORMAT)
                .serializationLib(PARQUET_SERDE)
                .partitionKeyNames(PART_KEYS)
                .prunedPartitions(Collections.singletonList(part("year=2024/month=01")))
                .build();
        ConnectorSession session = new ScopeSession(7L, "same-statement", new TestStatementScope());
        ConnectorScanRequest request = ConnectorScanRequest.builder(
                handle, Collections.<ConnectorColumnHandle>emptyList()).build();

        List<ConnectorScanRange> first = provider.planScan(session, request);
        List<ConnectorScanRange> second = provider.planScan(session, request);

        Assertions.assertSame(first, second, "an identical scan must reuse the statement's planned range list");
        Assertions.assertEquals(1, lister.totalCalls, "the underlying Hive file listing must run once");
    }

    @Test
    public void statementReuseLastsOnlyWhilePlanning() {
        // Identical scans share their ranges only within one plan, so the memo is kept for planning only: once the
        // engine ends planning no range is held, and a later plan of the statement plans the scan again.
        // MUTATION: keeping the memo for the whole statement (computeIfAbsent) -> red.
        HiveScanPlanProvider provider = provider(new FakeHmsClient(), new CountingLister());
        HiveTableHandle handle = new HiveTableHandle.Builder("db", "t", HiveTableType.HIVE)
                .inputFormat(PARQUET_INPUT_FORMAT)
                .serializationLib(PARQUET_SERDE)
                .partitionKeyNames(PART_KEYS)
                .prunedPartitions(Collections.singletonList(part("year=2024/month=01")))
                .build();
        TestStatementScope scope = new TestStatementScope();
        ConnectorSession session = new ScopeSession(7L, "same-statement", scope);
        ConnectorScanRequest request = ConnectorScanRequest.builder(
                handle, Collections.<ConnectorColumnHandle>emptyList()).build();
        List<ConnectorScanRange> planned = provider.planScan(session, request);

        scope.endPlanning();

        Assertions.assertFalse(scope.contains(HiveScanPlanProvider.SCAN_REUSE_NAMESPACE + ":7:same-statement"),
                "the planned ranges must not outlive planning");
        List<ConnectorScanRange> replanned = provider.planScan(session, request);
        Assertions.assertFalse(planned.isEmpty(), "the partition's file must plan a range");
        Assertions.assertNotSame(planned, replanned, "a scan planned after planning ended plans again");
        Assertions.assertEquals(planned.size(), replanned.size());
    }

    @Test
    public void statementReuseRetriesAListingThatFailedOnce() {
        FailOnceLister lister = new FailOnceLister();
        HiveScanPlanProvider provider = provider(new FakeHmsClient(), lister);
        HiveTableHandle handle = new HiveTableHandle.Builder("db", "t", HiveTableType.HIVE)
                .inputFormat(PARQUET_INPUT_FORMAT)
                .serializationLib(PARQUET_SERDE)
                .partitionKeyNames(PART_KEYS)
                .prunedPartitions(Collections.singletonList(part("year=2024/month=01")))
                .build();
        ConnectorSession session = new ScopeSession(7L, "same-statement", new TestStatementScope());
        ConnectorScanRequest request = ConnectorScanRequest.builder(
                handle, Collections.<ConnectorColumnHandle>emptyList()).build();

        List<ConnectorScanRange> degraded = provider.planScan(session, request);
        List<ConnectorScanRange> recovered = provider.planScan(session, request);
        List<ConnectorScanRange> reused = provider.planScan(session, request);

        Assertions.assertTrue(degraded.isEmpty(), "the tolerated first listing failure skips its partition");
        Assertions.assertEquals(1, recovered.size(), "the next alias must retry and recover the skipped partition");
        Assertions.assertSame(recovered, reused, "only the complete retry result should enter statement reuse");
        Assertions.assertEquals(2, lister.totalCalls,
                "the transient failure must not be sticky, while the successful retry should be reused");
    }

    @Test
    public void missingReusePropertyDoesNotReuse() {
        CountingLister lister = new CountingLister();
        HiveScanPlanProvider provider = provider(new FakeHmsClient(), lister);
        HiveTableHandle handle = new HiveTableHandle.Builder("db", "t", HiveTableType.HIVE)
                .inputFormat(PARQUET_INPUT_FORMAT)
                .serializationLib(PARQUET_SERDE)
                .partitionKeyNames(PART_KEYS)
                .prunedPartitions(Collections.singletonList(part("year=2024/month=01")))
                .build();
        ConnectorSession session = new ScopeSession(
                7L, "mixed-version", new TestStatementScope(), Collections.emptyMap());
        ConnectorScanRequest request = ConnectorScanRequest.builder(
                handle, Collections.<ConnectorColumnHandle>emptyList()).build();

        List<ConnectorScanRange> first = provider.planScan(session, request);
        List<ConnectorScanRange> second = provider.planScan(session, request);

        Assertions.assertNotSame(first, second,
                "an older planning FE's missing property must not enable reuse in a newer connector");
    }

    @Test
    public void statementReuseKeepsScansWithDifferentInputsApart() {
        // Separately built but equal handles share one plan; a scan that differs in any input the planned ranges
        // depend on plans on its own (the ranges carry the partition files and the BE file format).
        // MUTATION: dropping a fact from HiveScanReuseKey (or the catalog id from the memo key) makes that scan
        // return an earlier plan -> red.
        HiveScanPlanProvider provider = provider(new FakeHmsClient(), new CountingLister());
        TestStatementScope scope = new TestStatementScope();
        ConnectorSession session = new ScopeSession(7L, "same-statement", scope);
        HiveTableHandle text = textHandle("t", LAZY_SIMPLE_SERDE, part("year=2024/month=01"));

        List<ConnectorScanRange> plain = provider.planScan(session, scanOf(text));
        Assertions.assertEquals(1, plain.size());
        Assertions.assertSame(plain, provider.planScan(session,
                scanOf(textHandle("t", LAZY_SIMPLE_SERDE, part("year=2024/month=01")))),
                "separately built but equal handles must share one plan");
        List<ConnectorScanRange> otherPartition = provider.planScan(session,
                scanOf(textHandle("t", LAZY_SIMPLE_SERDE, part("year=2024/month=02"))));
        List<ConnectorScanRange> otherTable = provider.planScan(session,
                scanOf(textHandle("t2", LAZY_SIMPLE_SERDE, part("year=2024/month=01"))));
        List<ConnectorScanRange> csv = provider.planScan(session,
                scanOf(textHandle("t", OPEN_CSV_SERDE, part("year=2024/month=01"))));
        List<ConnectorScanRange> stringFirstColumn = provider.planScan(session,
                scanOf(text.toBuilder().firstColumnIsString(true).build()));
        List<ConnectorScanRange> parquet = provider.planScan(session, scanOf(text.toBuilder()
                .inputFormat(PARQUET_INPUT_FORMAT)
                .serializationLib(PARQUET_SERDE)
                .build()));
        // Same serde as the plain text scan; only the input format differs, and an LZO text table reads *.lzo files.
        List<ConnectorScanRange> lzo = provider.planScan(session,
                scanOf(text.toBuilder().inputFormat(LZO_TEXT_INPUT_FORMAT).build()));
        Assertions.assertTrue(lzo.isEmpty(), "an LZO text scan skips the non-.lzo file the plain text scan reads");
        List<ConnectorScanRange> otherCatalog = provider.planScan(
                new ScopeSession(8L, "same-statement", scope), scanOf(text));
        HiveTableHandle unpartitioned = new HiveTableHandle.Builder("db", "u", HiveTableType.HIVE)
                .inputFormat(PARQUET_INPUT_FORMAT)
                .serializationLib(PARQUET_SERDE)
                .partitionKeyNames(Collections.emptyList())
                .location("hdfs://nn/warehouse/u")
                .build();
        List<ConnectorScanRange> atLocation = provider.planScan(session, scanOf(unpartitioned));
        List<ConnectorScanRange> moved = provider.planScan(session,
                scanOf(unpartitioned.toBuilder().location("hdfs://nn/warehouse/u_moved").build()));
        Assertions.assertEquals("hdfs://nn/warehouse/u_moved/000000_0",
                ((HiveScanRange) moved.get(0)).getPath().orElse(null),
                "an unpartitioned table is identified by its location");

        List<List<ConnectorScanRange>> plans = Arrays.asList(plain, otherPartition, otherTable, csv,
                stringFirstColumn, parquet, lzo, otherCatalog, atLocation, moved);
        Set<List<ConnectorScanRange>> distinct = Collections.newSetFromMap(new IdentityHashMap<>());
        distinct.addAll(plans);
        Assertions.assertEquals(plans.size(), distinct.size(), "scans with different inputs must not share a plan");
    }

    @Test
    public void partitionBatchPlanningNeverUsesStatementReuse() {
        // Large partitioned scans stream their splits per batch on background threads; the batch path must not
        // retain plans in the statement memo. MUTATION: routing planScanForPartitionBatch through the memo ->
        // the second batch reuses the first plan -> red.
        HiveScanPlanProvider provider = provider(new FakeHmsClient(), new CountingLister());
        TestStatementScope scope = new TestStatementScope();
        ConnectorSession session = new ScopeSession(7L, "same-statement", scope);
        HiveTableHandle handle = textHandle("t", LAZY_SIMPLE_SERDE, part("year=2024/month=01"));
        List<String> batch = Collections.singletonList("year=2024/month=01");

        List<ConnectorScanRange> first = provider.planScanForPartitionBatch(session, scanOf(handle), batch);
        List<ConnectorScanRange> second = provider.planScanForPartitionBatch(session, scanOf(handle), batch);

        Assertions.assertEquals(1, first.size());
        Assertions.assertNotSame(first, second);
        Assertions.assertFalse(scope.contains(HiveScanPlanProvider.SCAN_REUSE_NAMESPACE + ":7:same-statement"),
                "partition batches must not be planned through the statement reuse memo");
    }

    private static HiveTableHandle textHandle(String tableName, String serde, HmsPartitionInfo partition) {
        return new HiveTableHandle.Builder("db", tableName, HiveTableType.HIVE)
                .inputFormat(TEXT_INPUT_FORMAT)
                .serializationLib(serde)
                .partitionKeyNames(PART_KEYS)
                .prunedPartitions(Collections.singletonList(partition))
                .build();
    }

    private static ConnectorScanRequest scanOf(HiveTableHandle handle) {
        return ConnectorScanRequest.builder(handle, Collections.<ConnectorColumnHandle>emptyList()).build();
    }

    // ===== object-store native read (FIX-hive-s3a: scheme normalization + canonical creds) =====

    @Test
    public void nativeScanRangePathNormalizedS3aToS3() {
        // BE's native S3 reader rejects the s3a scheme (S3URI accepts only s3/http/https). The connector lists
        // files with the raw scheme (Hadoop wants s3a) but must normalize the BE-facing range path to s3://.
        // MUTATION: dropping the splitFile normalization leaves the range path s3a:// -> BE "Invalid S3 URI".
        HiveFileListingCache.DirectoryLister s3aLister = (location, fs) ->
                new ArrayList<>(Collections.singletonList(
                        new HiveFileStatus("s3a://bucket/db/t/p/000000_0", 10L, 1L)));
        FakeConnectorContext normCtx = new FakeConnectorContext() {
            @Override
            public String normalizeStorageUri(String rawUri) {
                return rawUri == null ? null : rawUri.replace("s3a://", "s3://");
            }
        };
        HiveScanPlanProvider provider = new HiveScanPlanProvider(new FakeHmsClient(),
                HiveTestProperties.minimal(), normCtx,
                new HiveFileListingCache(HiveTestProperties.minimal(), s3aLister));
        HiveTableHandle handle = new HiveTableHandle.Builder("db", "t", HiveTableType.HIVE)
                .inputFormat(PARQUET_INPUT_FORMAT)
                .serializationLib(PARQUET_SERDE)
                .partitionKeyNames(PART_KEYS)
                .prunedPartitions(Collections.singletonList(part("year=2024/month=01")))
                .build();

        List<ConnectorScanRange> ranges = provider.planScanForPartitionBatch(new FakeSession(),
                ConnectorScanRequest.builder(handle, Collections.<ConnectorColumnHandle>emptyList())
                .build(), Collections.singletonList("year=2024/month=01"));

        Assertions.assertEquals(1, ranges.size());
        Assertions.assertEquals("s3://bucket/db/t/p/000000_0",
                ((HiveScanRange) ranges.get(0)).getPath().orElse(null),
                "native hive range path must be scheme-normalized s3a->s3 for BE's native reader");
    }

    @Test
    public void scanNodePropertiesEmitsCanonicalCredsForNativeReader() {
        // BE's native FILE_S3 reader reads ONLY AWS_ACCESS_KEY/AWS_SECRET_KEY/AWS_ENDPOINT (s3_util.cpp), never the
        // raw s3.access_key alias. getScanNodeProperties must emit the BE-canonical creds
        // (getBackendStorageProperties) like legacy HiveScanNode.getLocationProperties did; without them a private
        // bucket 403s. MUTATION: dropping the canonical emission (pre-fix: only raw s3. aliases were emitted).
        FakeConnectorContext credCtx = new FakeConnectorContext() {
            @Override
            public Map<String, String> getBackendStorageProperties() {
                return Collections.singletonMap("AWS_ACCESS_KEY", "canonAK");
            }
        };
        HiveScanPlanProvider provider = new HiveScanPlanProvider(new FakeHmsClient(),
                HiveTestProperties.with("s3.access_key", "aliasAK"), credCtx,
                                new HiveFileListingCache(HiveTestProperties.minimal(), new CountingLister()));
        HiveTableHandle handle = new HiveTableHandle.Builder("db", "t", HiveTableType.HIVE)
                .inputFormat(PARQUET_INPUT_FORMAT)
                .serializationLib(PARQUET_SERDE)
                .build();

        Map<String, String> props = provider.getScanNodeProperties(
                new FakeSession(), handle, Collections.<ConnectorColumnHandle>emptyList(), Optional.empty());

        Assertions.assertEquals("canonAK", props.get("location.AWS_ACCESS_KEY"),
                "BE-canonical AWS_* creds must be emitted for the native reader (legacy parity)");
        // the raw s3. alias is still forwarded (harmless, ignored by BE), so no configured key is dropped
        Assertions.assertEquals("aliasAK", props.get("location.s3.access_key"));
    }

    @Test
    public void getScanNodePropertiesUsesCsvTableParametersForPartitionedAndUnpartitionedTables() {
        HiveScanPlanProvider provider = provider(new FakeHmsClient(), new CountingLister());
        for (List<String> partitionKeys : Arrays.asList(Collections.<String>emptyList(),
                Collections.singletonList("bucket"))) {
            HiveTableHandle handle = new HiveTableHandle.Builder("db", "csv_table", HiveTableType.HIVE)
                    .inputFormat("org.apache.hadoop.mapred.TextInputFormat")
                    .serializationLib(HiveTextProperties.HIVE_OPEN_CSV_SERDE)
                    .partitionKeyNames(partitionKeys)
                    .tableParameters(Map.of("separatorChar", "s", "quoteChar", "q", "escapeChar", "e"))
                    .build();
            Map<String, String> props = provider.getScanNodeProperties(
                    new FakeSession(), handle, Collections.emptyList(), Optional.empty());
            Assertions.assertAll(
                    () -> Assertions.assertEquals("csv", props.get(ScanNodePropertyKeys.FILE_FORMAT_TYPE)),
                    () -> Assertions.assertEquals("s", props.get(ScanNodePropertyKeys.TEXT_COLUMN_SEPARATOR)),
                    () -> Assertions.assertEquals("q", props.get(ScanNodePropertyKeys.TEXT_ENCLOSE)),
                    () -> Assertions.assertEquals("e", props.get(ScanNodePropertyKeys.TEXT_ESCAPE)),
                    () -> Assertions.assertEquals("false", props.get(ScanNodePropertyKeys.TEXT_TRIM_DOUBLE_QUOTES)));
            Assertions.assertEquals(partitionKeys.isEmpty() ? null : "bucket",
                    props.get(ScanNodePropertyKeys.PATH_PARTITION_KEYS));
        }
    }

    // ===== helpers =====

    @Test
    public void testAdjustFileCompressTypeRemapsOnlyLz4Frame() {
        // hadoop/hive write .lz4 as the LZ4 BLOCK codec, but the generic node infers LZ4FRAME from the .lz4
        // extension; BE's text reader then fails on block bytes decoded as frame. Restore legacy
        // HiveScanNode.getFileCompressType parity: remap ONLY LZ4FRAME -> LZ4BLOCK, pass every other codec
        // through unchanged. MUTATION: broadening or dropping the remap would either mis-decode other codecs
        // or reintroduce the LZ4F_getFrameInfo failure on .lz4 text tables.
        HiveScanPlanProvider provider = provider(null, new CountingLister());
        Assertions.assertEquals(TFileCompressType.LZ4BLOCK,
                provider.adjustFileCompressType(TFileCompressType.LZ4FRAME));
        Assertions.assertEquals(TFileCompressType.LZ4BLOCK,
                provider.adjustFileCompressType(TFileCompressType.LZ4BLOCK));
        Assertions.assertEquals(TFileCompressType.GZ,
                provider.adjustFileCompressType(TFileCompressType.GZ));
        Assertions.assertEquals(TFileCompressType.ZSTD,
                provider.adjustFileCompressType(TFileCompressType.ZSTD));
        Assertions.assertEquals(TFileCompressType.SNAPPYBLOCK,
                provider.adjustFileCompressType(TFileCompressType.SNAPPYBLOCK));
        Assertions.assertEquals(TFileCompressType.PLAIN,
                provider.adjustFileCompressType(TFileCompressType.PLAIN));
    }

    @Test
    public void usesHiveParquetInt96TimeZone() {
        Assertions.assertTrue(provider(null, new CountingLister()).usesHiveParquetInt96TimeZone());
    }

    private static HiveScanPlanProvider provider(
            HmsClient hmsClient, HiveFileListingCache.DirectoryLister lister) {
        return new HiveScanPlanProvider(hmsClient, HiveTestProperties.minimal(), new FakeConnectorContext(),
                new HiveFileListingCache(HiveTestProperties.minimal(), lister));
    }

    private static HmsPartitionInfo part(String name) {
        return new HmsPartitionInfo(Collections.emptyList(), name, null, null, null, Collections.emptyMap());
    }

    /**
     * A {@link HiveFileListingCache.DirectoryLister} double: counts calls (total + per location) and returns a
     * fresh single-file listing per call, so each partition location contributes exactly one range.
     */
    private static final class CountingLister implements HiveFileListingCache.DirectoryLister {
        final Map<String, Integer> callsPerLocation = new HashMap<>();
        int totalCalls;

        @Override
        public List<HiveFileStatus> list(String location, FileSystem fs) {
            totalCalls++;
            callsPerLocation.merge(location, 1, Integer::sum);
            return new ArrayList<>(Collections.singletonList(new HiveFileStatus(location + "/000000_0", 10L, 1L)));
        }
    }

    private static final class FailOnceLister implements HiveFileListingCache.DirectoryLister {
        private int totalCalls;

        @Override
        public List<HiveFileStatus> list(String location, FileSystem fs) {
            totalCalls++;
            if (totalCalls == 1) {
                throw new HiveDirectoryListingException("transient listing failure", new IOException("retry"));
            }
            return new ArrayList<>(Collections.singletonList(
                    new HiveFileStatus(location + "/000000_0", 10L, 1L)));
        }
    }

    /**
     * Minimal {@link HmsClient} double whose {@code getPartitions} echoes each requested name back as an
     * {@link HmsPartitionInfo} whose location IS the name, so the batch-scoped resolution can be asserted through
     * the listed locations. The rest fail loud.
     */
    private static class FakeHmsClient implements HmsClient {
        private final List<String> listedPartitionNames;
        private final String absentPartitionName;
        private final HmsClientException partitionFailure;

        FakeHmsClient() {
            this(null, null, null);
        }

        FakeHmsClient(List<String> listedPartitionNames, String absentPartitionName) {
            this(listedPartitionNames, absentPartitionName, null);
        }

        FakeHmsClient(List<String> listedPartitionNames, String absentPartitionName,
                HmsClientException partitionFailure) {
            this.listedPartitionNames = listedPartitionNames;
            this.absentPartitionName = absentPartitionName;
            this.partitionFailure = partitionFailure;
        }

        @Override
        public List<HmsPartitionInfo> getPartitions(String dbName, String tableName, List<String> partNames) {
            List<HmsPartitionInfo> result = new ArrayList<>();
            for (String name : partNames) {
                result.add(new HmsPartitionInfo(Collections.emptyList(), name,
                        null, null, null, Collections.emptyMap()));
            }
            return result;
        }

        @Override
        public HmsPartitionBatchResult getPartitionsWithStats(
                String dbName, String tableName, List<String> partNames) {
            if (partitionFailure != null) {
                throw partitionFailure;
            }
            List<HmsPartitionInfo> partitions = getPartitions(dbName, tableName, partNames);
            HmsPartitionBatchStats stats = HmsPartitionBatchStats.builder()
                    .requestedItems(partNames.size())
                    .transportInvocations(1)
                    .transportItems(partNames.size())
                    .largestBatchSize(partNames.size())
                    .smallestBatchSize(partNames.size())
                    .build();
            return new HmsPartitionBatchResult(partitions, stats);
        }

        @Override
        public HmsPartitionBatchResult getExistingPartitionsWithStats(
                String dbName, String tableName, List<String> partNames) {
            if (partitionFailure != null) {
                throw partitionFailure;
            }
            List<String> existingNames = new ArrayList<>();
            for (String partitionName : partNames) {
                if (!partitionName.equals(absentPartitionName)) {
                    existingNames.add(partitionName);
                }
            }
            HmsPartitionBatchStats stats = HmsPartitionBatchStats.builder()
                    .requestedItems(partNames.size())
                    .transportInvocations(1)
                    .transportItems(partNames.size())
                    .largestBatchSize(partNames.size())
                    .smallestBatchSize(partNames.size())
                    .build();
            return new HmsPartitionBatchResult(getPartitions(dbName, tableName, existingNames), stats);
        }

        @Override
        public List<String> listPartitionNames(String dbName, String tableName, int maxParts) {
            if (listedPartitionNames == null) {
                throw new UnsupportedOperationException();
            }
            return listedPartitionNames;
        }

        @Override
        public List<String> listDatabases() {
            throw new UnsupportedOperationException();
        }

        @Override
        public HmsDatabaseInfo getDatabase(String dbName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<String> listTables(String dbName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean tableExists(String dbName, String tableName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public HmsTableInfo getTable(String dbName, String tableName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<String, String> getDefaultColumnValues(String dbName, String tableName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public HmsPartitionInfo getPartition(String dbName, String tableName, List<String> values) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
        }
    }

    /** Minimal {@link ConnectorSession} (no split-size override, empty session properties). */
    private static final class FakeSession implements ConnectorSession {
        private final Map<String, String> properties;
        private final ConnectorTaskScheduler scheduler;

        FakeSession() {
            this(Collections.emptyMap());
        }

        FakeSession(Map<String, String> properties) {
            this(properties, ConnectorTaskScheduler.DIRECT);
        }

        FakeSession(Map<String, String> properties, ConnectorTaskScheduler scheduler) {
            this.properties = properties;
            this.scheduler = scheduler;
        }

        @Override
        public ConnectorTaskScheduler getTaskScheduler(ConnectorTaskScheduler.Stage stage) {
            return scheduler;
        }

        private final Map<String, Object> statementMemos = new HashMap<>();
        private final ConnectorStatementScope statementScope = new ConnectorStatementScope() {
            @Override
            @SuppressWarnings("unchecked")
            public <T> T computeIfAbsent(String key, Supplier<T> loader) {
                return (T) statementMemos.computeIfAbsent(key, ignored -> loader.get());
            }
        };

        @Override
        public String getQueryId() {
            return "q";
        }

        @Override
        public String getUser() {
            return "u";
        }

        @Override
        public String getTimeZone() {
            return "UTC";
        }

        @Override
        public String getLocale() {
            return "en_US";
        }

        @Override
        public long getCatalogId() {
            return 0L;
        }

        @Override
        public String getCatalogName() {
            return "hive_catalog";
        }

        @Override
        public <T> T getProperty(String name, Class<T> type) {
            String value = properties.get(name);
            if (value == null) {
                return null;
            }
            if (type == Integer.class) {
                return type.cast(Integer.valueOf(value));
            }
            if (type == Boolean.class) {
                return type.cast(Boolean.valueOf(value));
            }
            return type.cast(value);
        }

        @Override
        public Map<String, String> getCatalogProperties() {
            return Collections.emptyMap();
        }

        @Override
        public ConnectorStatementScope getStatementScope() {
            return statementScope;
        }
    }
}
