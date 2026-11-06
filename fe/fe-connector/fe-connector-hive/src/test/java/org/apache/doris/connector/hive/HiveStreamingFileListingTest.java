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

import org.apache.doris.connector.cache.CatalogMetaCache;
import org.apache.doris.connector.cache.MetaCache;
import org.apache.doris.connector.hms.HmsPartitionInfo;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorStatementScope;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.scan.ConnectorScanContext;
import org.apache.doris.connector.spi.scan.ConnectorScanRequest;
import org.apache.doris.connector.spi.scan.ConnectorSplitSource;
import org.apache.doris.filesystem.FileEntry;
import org.apache.doris.filesystem.FileIterator;
import org.apache.doris.filesystem.FileSystem;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicInteger;

/** Exercises real lazy filesystem cursors, including failures after ranges have escaped to the framework. */
class HiveStreamingFileListingTest {
    private static final String LOCATION = "s3://bucket/table";
    private static final ConnectorSession SESSION = new ScopeSession(0, "q", ConnectorStatementScope.NONE, Map.of());

    @Test
    void firstSplitDoesNotReadTheRestOfTheDirectory() throws Exception {
        RecordingFiles files = new RecordingFiles(1000000);
        HiveFileListingCache cache = new HiveFileListingCache(HiveTestProperties.minimal());
        FakeFileSystem fs = new FakeFileSystem().withIteratorFactory(location -> files);
        HiveScanPlanProvider provider = provider(cache, fs);
        try (ConnectorSplitSource source = provider.streamSplits(SESSION, request())) {
            Assertions.assertEquals(0, files.read.get());
            Assertions.assertTrue(source.hasNext());
            Assertions.assertTrue(source.hasNext(), "lookahead must be idempotent");
            Assertions.assertEquals(10, source.next().getLength());
            Assertions.assertEquals(1, files.read.get(), "first split must precede full directory listing");
        }
        Assertions.assertEquals(1, files.closed.get());
        Assertions.assertEquals(0, cache.size());
        Assertions.assertEquals(0, cache.pendingBytes());
        Assertions.assertEquals(-1, cache.cachedDataSize("db", "t", LOCATION, List.of()));
    }

    @Test
    void completeListingIsSharedAndClosingAReaderCannotModifyIt() {
        AtomicInteger listings = new AtomicInteger();
        FakeFileSystem fs = new FakeFileSystem().withIteratorFactory(location -> {
            listings.incrementAndGet();
            return new RecordingFiles(3);
        });
        HiveFileListingCache cache = new HiveFileListingCache(HiveTestProperties.minimal());
        Assertions.assertEquals(3, consume(cache, fs).size());
        Assertions.assertEquals(1, cache.size());
        Assertions.assertEquals(30, cache.cachedDataSize("db", "t", LOCATION, List.of()));
        try (HiveFileIterator first = open(cache, fs); HiveFileIterator second = open(cache, fs)) {
            first.next();
            first.close();
            List<String> paths = new ArrayList<>();
            second.forEachRemaining(file -> paths.add(file.getPath()));
            Assertions.assertEquals(3, new HashSet<>(paths).size());
        }
        Assertions.assertEquals(1, listings.get());
        Assertions.assertEquals(0, cache.pendingBytes());
    }

    @Test
    void consumedLastFileWithoutObservedEofDoesNotPublish() {
        HiveFileListingCache cache = new HiveFileListingCache(HiveTestProperties.minimal());
        FakeFileSystem fs = new FakeFileSystem().withIteratorFactory(location -> new RecordingFiles(1));
        try (HiveFileIterator files = open(cache, fs)) {
            files.next();
        }
        Assertions.assertEquals(0, cache.size());
        Assertions.assertEquals(0, cache.pendingBytes());
        Assertions.assertEquals(-1, cache.cachedDataSize("db", "t", LOCATION, List.of()));
    }

    @Test
    void retiredCacheOwnerDoesNotFailSuccessfulStreamingEof() throws Exception {
        try (CatalogMetaCache owner = CatalogMetaCache.unmanaged()) {
            HiveFileListingCache cache = new HiveFileListingCache(owner, HiveTestProperties.minimal());
            List<MetaCache<?, ?>> entries = new ArrayList<>(owner.entries().values());
            RecordingFiles raw = new RecordingFiles(1);
            FakeFileSystem fs = new FakeFileSystem().withIteratorFactory(location -> raw);
            try (ConnectorSplitSource source = provider(cache, fs).streamSplits(SESSION, request())) {
                Assertions.assertEquals(10, source.next().getLength());
                Assertions.assertEquals(1, raw.read.get());
                Assertions.assertTrue(cache.pendingBytes() > 0, "EOF has not been observed yet");
                owner.close();
                Assertions.assertFalse(source.hasNext(), "optional retention must not fail completed listing");
            }
            Assertions.assertEquals(1, raw.closed.get());
            Assertions.assertEquals(0, cache.pendingBytes());
            Assertions.assertEquals(0, owner.metrics().getActiveLoadCount());
            for (MetaCache<?, ?> entry : entries) {
                Assertions.assertEquals(0, entry.size());
                Assertions.assertEquals(0, entry.metrics().getActiveBulkHandleCount());
            }
        }
    }

    @Test
    void emptyDirectoryCanPublishACompleteEmptyListing() {
        HiveFileListingCache cache = new HiveFileListingCache(HiveTestProperties.minimal());
        Assertions.assertTrue(consume(cache, new FakeFileSystem()).isEmpty());
        Assertions.assertEquals(1, cache.size());
        Assertions.assertEquals(0, cache.cachedFileCount("db", "t", LOCATION, List.of()));
        Assertions.assertEquals(0, cache.cachedDataSize("db", "t", LOCATION, List.of()));
    }

    @Test
    void oversizedCandidateIsReleasedAndItsTailIsNeverCached() {
        HiveCatalogProperties properties = properties(4096, 8192);
        HiveFileListingCache cache = new HiveFileListingCache(properties);
        RecordingFiles raw = new RecordingFiles(100);
        raw.pathPadding = "x".repeat(300);
        FakeFileSystem fs = new FakeFileSystem().withIteratorFactory(location -> raw);
        List<HiveFileStatus> all = new ArrayList<>();
        boolean abandoned = false;
        try (HiveFileIterator files = open(cache, fs)) {
            while (files.hasNext()) {
                all.add(files.next());
                Assertions.assertTrue(cache.pendingBytes() <= 4096);
                if (cache.pendingBytes() == 0) {
                    abandoned = true;
                }
                if (abandoned) {
                    Assertions.assertEquals(0, cache.pendingBytes(), "a discarded candidate must never restart");
                }
            }
        }
        Assertions.assertTrue(abandoned);
        Assertions.assertEquals(100, all.size());
        Assertions.assertEquals(100, all.stream().map(HiveFileStatus::getPath).distinct().count());
        Assertions.assertEquals(1000, all.stream().mapToLong(HiveFileStatus::getLength).sum());
        Assertions.assertEquals(0, cache.size());
        Assertions.assertEquals(0, cache.pendingBytes());
        Assertions.assertEquals(1000, cache.cachedDataSize("db", "t", LOCATION, List.of()));
    }

    @Test
    void catalogBudgetRejectsAnotherCandidateWithoutStoppingItsScan() {
        HiveFileListingCache cache = new HiveFileListingCache(properties(8192, 1024));
        FakeFileSystem fs = new FakeFileSystem().withIteratorFactory(location -> {
            RecordingFiles raw = new RecordingFiles(2);
            raw.pathPadding = "x".repeat(100);
            return raw;
        });
        try (HiveFileIterator first = open(cache, fs)) {
            first.next();
            long held = cache.pendingBytes();
            Assertions.assertTrue(held > 0);
            try (HiveFileIterator second = cache.openDataFiles("db", "t2", LOCATION, List.of(), fs,
                    new ConnectorScanContext())) {
                Assertions.assertEquals(2, count(second));
                Assertions.assertEquals(held, cache.pendingBytes());
            }
            Assertions.assertEquals(0, cache.size(), "the second complete listing exceeded the staging budget");
            Assertions.assertEquals(20, cache.cachedDataSize("db", "t2", LOCATION, List.of()));
        }
        Assertions.assertEquals(0, cache.pendingBytes());
    }

    @Test
    void sharedBudgetIsReleasedAfterAnOverlappingListingIsAbandoned() {
        HiveFileListingCache cache = new HiveFileListingCache(properties(64 * 1024, 64 * 1024));
        FakeFileSystem fs = new FakeFileSystem().withIteratorFactory(location -> {
            RecordingFiles raw = new RecordingFiles(200);
            raw.pathPadding = "x".repeat(100);
            return raw;
        });
        try (HiveFileIterator first = open(cache, fs)) {
            for (int i = 0; i < 80; i++) {
                first.next();
            }
            long held = cache.pendingBytes();
            try (HiveFileIterator second = cache.openDataFiles("db", "t2", LOCATION, List.of(), fs,
                    new ConnectorScanContext())) {
                while (second.hasNext()) {
                    second.next();
                    Assertions.assertTrue(cache.pendingBytes() <= 64 * 1024);
                }
            }
            Assertions.assertEquals(0, cache.size());
            Assertions.assertEquals(held, cache.pendingBytes());
        }
        Assertions.assertEquals(0, cache.pendingBytes());
        Assertions.assertEquals(200, consume(cache, fs).size());
        Assertions.assertEquals(1, cache.size(), "the listing fits once the overlapping cursor releases its budget");
        Assertions.assertEquals(0, cache.pendingBytes());
    }

    @Test
    void refreshAtEveryScopeFencesListsAndIndependentSummaries() {
        for (long entryBudget : new long[] {8388608, 0}) {
            for (int scope = 0; scope < 4; scope++) {
                HiveFileListingCache cache = new HiveFileListingCache(properties(entryBudget, 67108864));
                FakeFileSystem fs = new FakeFileSystem().withIteratorFactory(location -> new RecordingFiles(3));
                try (HiveFileIterator files = cache.openDataFiles("db", "t", LOCATION, List.of("1"), fs,
                        new ConnectorScanContext())) {
                    files.next();
                    switch (scope) {
                        case 0:
                            cache.invalidateAll();
                            break;
                        case 1:
                            cache.invalidateDb("db");
                            break;
                        case 2:
                            cache.invalidateTable("db", "t");
                            break;
                        default:
                            cache.invalidatePartitions("db", "t", Collections.singleton(List.of("1")));
                            break;
                    }
                    Assertions.assertEquals(2, count(files));
                }
                Assertions.assertEquals(0, cache.size(), "scope " + scope + " must fence publication");
                Assertions.assertEquals(-1, cache.cachedDataSize("db", "t", LOCATION, List.of("1")));
                Assertions.assertEquals(0, cache.pendingBytes());
                try (HiveFileIterator files = cache.openDataFiles("db", "t", LOCATION, List.of("1"), fs,
                        new ConnectorScanContext())) {
                    Assertions.assertEquals(3, count(files));
                }
                Assertions.assertEquals(entryBudget > 0, cache.size() > 0);
                Assertions.assertEquals(30, cache.cachedDataSize("db", "t", LOCATION, List.of("1")));
            }
        }
    }

    @Test
    void sizeSummariesNeverServeAPartialOrRetainedFileList() {
        for (String ending : List.of("close", "cancel", "listing-failure", "close-failure")) {
            HiveFileListingCache cache = new HiveFileListingCache(properties(0, 0));
            RecordingFiles raw = new RecordingFiles(2);
            ConnectorScanContext context = new ConnectorScanContext();
            FakeFileSystem fs = new FakeFileSystem().withIteratorFactory(location -> raw);
            try (HiveFileIterator files = cache.openDataFiles("db", "t", LOCATION, List.of(), fs, context)) {
                files.next();
                if (ending.equals("cancel")) {
                    context.cancel();
                    Assertions.assertThrows(CancellationException.class, files::hasNext);
                }
                if (ending.equals("listing-failure")) {
                    raw.failAfter = 1;
                    Assertions.assertThrows(HiveDirectoryListingException.class, files::hasNext);
                }
                if (ending.equals("close-failure")) {
                    files.next();
                    raw.failClose = true;
                    Assertions.assertThrows(HiveDirectoryListingException.class, files::hasNext);
                }
            }
            Assertions.assertEquals(-1, cache.cachedDataSize("db", "t", LOCATION, List.of()), ending);
            Assertions.assertEquals(0, cache.pendingBytes());
        }

        AtomicInteger listings = new AtomicInteger();
        HiveFileListingCache cache = new HiveFileListingCache(properties(0, 0));
        FakeFileSystem fs = new FakeFileSystem().withIteratorFactory(location -> {
            listings.incrementAndGet();
            return new RecordingFiles(2);
        });
        Assertions.assertEquals(2, consume(cache, fs).size());
        Assertions.assertEquals(20, cache.cachedDataSize("db", "t", LOCATION, List.of()));
        Assertions.assertEquals(2, consume(cache, fs).size());
        Assertions.assertEquals(2, listings.get(), "a scalar hint cannot satisfy a later file enumeration");
    }

    @Test
    void cancellationClosesTheIteratorAndReleasesTheCandidate() {
        RecordingFiles raw = new RecordingFiles(100);
        HiveFileListingCache cache = new HiveFileListingCache(HiveTestProperties.minimal());
        ConnectorScanContext context = new ConnectorScanContext();
        FakeFileSystem fs = new FakeFileSystem().withIteratorFactory(location -> raw);
        try (HiveFileIterator files = cache.openDataFiles("db", "t", LOCATION, List.of(), fs, context)) {
            files.next();
            context.cancel();
            Assertions.assertThrows(CancellationException.class, files::hasNext);
        }
        Assertions.assertEquals(1, raw.read.get());
        Assertions.assertEquals(1, raw.closed.get());
        Assertions.assertEquals(0, cache.size());
        Assertions.assertEquals(0, cache.pendingBytes());
        Assertions.assertEquals(-1, cache.cachedDataSize("db", "t", LOCATION, List.of()));
    }

    @Test
    void cancellingOneStreamingCursorDoesNotCancelAnotherCursorOrItsPublication() {
        HiveFileListingCache cache = new HiveFileListingCache(HiveTestProperties.minimal());
        FakeFileSystem fs = new FakeFileSystem().withIteratorFactory(location -> new RecordingFiles(3));
        ConnectorScanContext firstContext = new ConnectorScanContext();
        try (HiveFileIterator first = cache.openDataFiles("db", "t", LOCATION, List.of(), fs, firstContext);
                HiveFileIterator second = open(cache, fs)) {
            first.next();
            second.next();
            firstContext.cancel();
            Assertions.assertThrows(CancellationException.class, first::hasNext);
            Assertions.assertEquals(2, count(second));
        }
        Assertions.assertEquals(1, cache.size());
        Assertions.assertEquals(0, cache.pendingBytes());
        Assertions.assertEquals(3, consume(cache, fs).size());
    }

    @Test
    void midListingFailureAfterFirstSplitFailsTheStreamingScan() throws Exception {
        RecordingFiles raw = new RecordingFiles(3);
        raw.failAfter = 1;
        HiveFileListingCache cache = new HiveFileListingCache(HiveTestProperties.minimal());
        FakeFileSystem fs = new FakeFileSystem().withIteratorFactory(location -> raw);
        try (ConnectorSplitSource source = provider(cache, fs).streamSplits(SESSION, request())) {
            Assertions.assertEquals(10, source.next().getLength());
            DorisConnectorException failure = Assertions.assertThrows(DorisConnectorException.class, source::hasNext);
            Assertions.assertTrue(failure.getMessage().contains("after producing splits"));
        }
        Assertions.assertEquals(0, cache.size());
        Assertions.assertEquals(0, cache.pendingBytes());
        Assertions.assertEquals(1, raw.closed.get());
    }

    @Test
    void cooperatingReadinessDoesNotAdvanceTheDirectoryOrBypassListingFailureHandling() throws Exception {
        RecordingFiles raw = new RecordingFiles(3);
        raw.failAfter = 1;
        HiveFileListingCache cache = new HiveFileListingCache(HiveTestProperties.minimal());
        FakeFileSystem fs = new FakeFileSystem().withIteratorFactory(location -> raw);
        HmsPartitionInfo partition = new HmsPartitionInfo(List.of("1"), LOCATION, null, null, null, Map.of());
        HiveTableHandle table = ((HiveTableHandle) request().getTableHandle()).toBuilder()
                .partitionKeyNames(List.of("p")).prunedPartitionsByName(Map.of("p=1", partition)).build();
        ConnectorScanRequest request = ConnectorScanRequest.builder(table, List.of())
                .requiredPartitions(List.of("p=1")).build();
        try (ConnectorSplitSource source = provider(cache, fs).streamPartitionSources(SESSION, request, 1, 1)
                .orElseThrow().get(0)) {
            Assertions.assertNull(source.isBlocked());
            Assertions.assertTrue(source.hasNext());
            Assertions.assertEquals(10, source.next().getLength());
            Assertions.assertNull(source.isBlocked(), "readiness must not probe the next directory entry");
            DorisConnectorException failure = Assertions.assertThrows(DorisConnectorException.class, source::hasNext);
            Assertions.assertTrue(failure.getMessage().contains("after producing splits"));
        }
        Assertions.assertEquals(1, raw.read.get());
        Assertions.assertEquals(1, raw.closed.get());
        Assertions.assertEquals(0, cache.size());
        Assertions.assertEquals(0, cache.pendingBytes());
    }

    @Test
    void inlineFailureRollsBackTheWholePartitionBeforeReturningItsPlan() {
        RecordingFiles raw = new RecordingFiles(3);
        raw.failAfter = 1;
        HiveFileListingCache cache = new HiveFileListingCache(HiveTestProperties.minimal());
        FakeFileSystem fs = new FakeFileSystem().withIteratorFactory(location -> raw);
        Assertions.assertTrue(provider(cache, fs).planScan(SESSION, request()).isEmpty());
        Assertions.assertEquals(0, cache.size());
        Assertions.assertEquals(0, cache.pendingBytes());
    }

    @Test
    void closeFailureAtEofDoesNotPublish() {
        RecordingFiles raw = new RecordingFiles(1);
        raw.failClose = true;
        HiveFileListingCache cache = new HiveFileListingCache(HiveTestProperties.minimal());
        FakeFileSystem fs = new FakeFileSystem().withIteratorFactory(location -> raw);
        try (HiveFileIterator files = open(cache, fs)) {
            files.next();
            Assertions.assertThrows(HiveDirectoryListingException.class, files::hasNext);
        }
        Assertions.assertEquals(0, cache.size());
        Assertions.assertEquals(0, cache.pendingBytes());
    }

    @Test
    void closingRecursiveTraversalClosesEveryOpenDirectory() {
        AtomicInteger closed = new AtomicInteger();
        FakeFileSystem fs = new FakeFileSystem().withIteratorFactory(location -> {
            if (location.uri().equals(LOCATION)) {
                return new FileIterator() {
                    private boolean returned;

                    @Override
                    public boolean hasNext() {
                        return !returned;
                    }

                    @Override
                    public FileEntry next() {
                        returned = true;
                        return FakeFileSystem.dir(LOCATION + "/child");
                    }

                    @Override
                    public void close() {
                        closed.incrementAndGet();
                    }
                };
            }
            return new RecordingFiles(100, closed);
        });
        HiveFileListingCache cache = new HiveFileListingCache(HiveTestProperties.minimal());
        try (HiveFileIterator files = open(cache, fs)) {
            files.next();
        }
        Assertions.assertEquals(2, closed.get());
        Assertions.assertEquals(0, cache.size());
    }

    @Test
    void disabledCollectionNeverAccumulatesButStillEnumeratesAllFiles() {
        HiveFileListingCache cache = new HiveFileListingCache(properties(0, 0));
        FakeFileSystem fs = new FakeFileSystem().withIteratorFactory(location -> new RecordingFiles(20));
        try (HiveFileIterator files = open(cache, fs)) {
            while (files.hasNext()) {
                files.next();
                Assertions.assertEquals(0, cache.pendingBytes());
            }
        }
        Assertions.assertEquals(0, cache.size());
        Assertions.assertEquals(200, cache.cachedDataSize("db", "t", LOCATION, List.of()));
    }

    @Test
    void summaryCacheHasIndependentAdmissionAndDisableControls() {
        for (Map<String, String> controls : List.of(
                Map.of("meta.cache.hive.file_summary.enable", "false"),
                Map.of("meta.cache.hive.file_summary.ttl-second", "0"),
                Map.of("meta.cache.hive.file_summary.capacity", "0"),
                Map.of("meta.cache.hive.file_summary.max-weight", "1B"))) {
            Map<String, String> raw = HiveTestProperties.mapWith(
                    HiveCatalogProperties.FILE_LISTING_CACHE_ENTRY_MAX_BYTES, "0");
            raw.putAll(controls);
            try (CatalogMetaCache owner = CatalogMetaCache.unmanaged()) {
                HiveFileListingCache cache = new HiveFileListingCache(owner, HiveCatalogProperties.of(raw));
                FakeFileSystem fs = new FakeFileSystem().withIteratorFactory(location -> new RecordingFiles(2));
                Assertions.assertEquals(2, consume(cache, fs).size());
                Assertions.assertEquals(-1, cache.cachedDataSize("db", "t", LOCATION, List.of()), controls.toString());
                Assertions.assertEquals(0, owner.entries().get("hive-file-summary")
                        .metrics().getActiveBulkHandleCount());
            }
        }
        HiveFileListingCache cache = new HiveFileListingCache(HiveCatalogProperties.of(
                HiveTestProperties.mapWith("meta.cache.hive.file.enable", "false")));
        FakeFileSystem fs = new FakeFileSystem().withIteratorFactory(location -> new RecordingFiles(2));
        Assertions.assertEquals(2, consume(cache, fs).size());
        Assertions.assertEquals(0, cache.size());
        Assertions.assertEquals(20, cache.cachedDataSize("db", "t", LOCATION, List.of()));
    }

    private static HiveCatalogProperties properties(long entry, long pending) {
        return HiveCatalogProperties.of(HiveTestProperties.mapWith(
                HiveCatalogProperties.FILE_LISTING_CACHE_ENTRY_MAX_BYTES, Long.toString(entry),
                HiveCatalogProperties.FILE_LISTING_CACHE_PENDING_MAX_BYTES, Long.toString(pending)));
    }

    private static HiveFileIterator open(HiveFileListingCache cache, FileSystem fs) {
        return cache.openDataFiles("db", "t", LOCATION, List.of(), fs, new ConnectorScanContext());
    }

    private static List<HiveFileStatus> consume(HiveFileListingCache cache, FileSystem fs) {
        List<HiveFileStatus> files = new ArrayList<>();
        try (HiveFileIterator iterator = open(cache, fs)) {
            iterator.forEachRemaining(files::add);
        }
        return files;
    }

    private static int count(HiveFileIterator files) {
        int result = 0;
        while (files.hasNext()) {
            files.next();
            result++;
        }
        return result;
    }

    private static HiveScanPlanProvider provider(HiveFileListingCache cache, FileSystem fs) {
        return new HiveScanPlanProvider(null, HiveTestProperties.minimal(), new FakeConnectorContext() {
            @Override
            public FileSystem getFileSystem(ConnectorSession session) {
                return fs;
            }
        }, cache);
    }

    private static ConnectorScanRequest request() {
        HiveTableHandle table = new HiveTableHandle.Builder("db", "t", HiveTableType.HIVE)
                .location(LOCATION)
                .inputFormat("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat")
                .serializationLib("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe")
                .build();
        return ConnectorScanRequest.builder(table, List.of()).build();
    }

    private static final class RecordingFiles implements FileIterator {
        private final int size;
        private final AtomicInteger read = new AtomicInteger();
        private final AtomicInteger closed;
        private int failAfter = -1;
        private boolean failClose;
        private String pathPadding = "";

        RecordingFiles(int size) {
            this(size, new AtomicInteger());
        }

        RecordingFiles(int size, AtomicInteger closed) {
            this.size = size;
            this.closed = closed;
        }

        @Override
        public boolean hasNext() throws IOException {
            if (read.get() == failAfter) {
                throw new IOException("remote listing failed");
            }
            return read.get() < size;
        }

        @Override
        public FileEntry next() {
            return FakeFileSystem.file(LOCATION + "/" + pathPadding + read.getAndIncrement() + ".parquet", 10, 1);
        }

        @Override
        public void close() throws IOException {
            closed.incrementAndGet();
            if (failClose) {
                throw new IOException("cursor close failed");
            }
        }
    }
}
