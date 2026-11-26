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

package org.apache.doris.connector.iceberg.action;

import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.GenericStatisticsFile;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

class IcebergExpireSnapshotsDeletionTest {
    private enum Kind {
        DATA, POSITION_DELETES, EQUALITY_DELETES, MANIFEST, MANIFEST_LIST, STATISTICS
    }

    private enum Failure {
        NONE, ONCE, ALWAYS, RESPONSE_LOST, NOT_FOUND
    }

    @ParameterizedTest
    @EnumSource(Kind.class)
    void countsNormalReturns(Kind kind) throws IOException {
        assertDeletion(kind, Failure.NONE);
    }

    @ParameterizedTest
    @EnumSource(Kind.class)
    void countsOnlyTheSuccessfulRetry(Kind kind) throws IOException {
        assertDeletion(kind, Failure.ONCE);
    }

    @ParameterizedTest
    @EnumSource(Kind.class)
    void excludesEveryFailedAttempt(Kind kind) throws IOException {
        assertDeletion(kind, Failure.ALWAYS);
    }

    private static void assertDeletion(Kind kind, Failure failure) throws IOException {
        Fixture fixture = fixture(kind);
        RecordingFileIO io = new RecordingFileIO(fixture.table().io(), Map.of(fixture.path(), failure));
        List<String> row = execute(fixture.table(), io, 0);

        Assertions.assertEquals(failure == Failure.ALWAYS ? 0 : 1, io.normalReturns(fixture.path()));
        Assertions.assertEquals(failure == Failure.ALWAYS, io.delegate.fileExists(fixture.path()));
        int attempts = io.attempts.get(fixture.path()).get();
        if (failure == Failure.ALWAYS) {
            Assertions.assertTrue(attempts > 1, "exercise SDK retries before failure suppression");
        } else {
            Assertions.assertEquals(failure == Failure.NONE ? 1 : 2, attempts);
        }
        long normalReturns = fixture.kindPaths().stream().mapToLong(io::normalReturns).sum();
        Assertions.assertEquals(normalReturns, Long.parseLong(row.get(kind.ordinal())),
                "success counter for " + kind + ": attempts=" + attempts + ", normal returns=" + normalReturns);
    }

    @Test
    void countsAnIdempotentRetryAfterADeletionResponseIsLost() throws IOException {
        Fixture fixture = fixture(Kind.MANIFEST_LIST);
        RecordingFileIO io = new RecordingFileIO(fixture.table().io(),
                Map.of(fixture.path(), Failure.RESPONSE_LOST));
        List<String> row = execute(fixture.table(), io, 0);

        Assertions.assertEquals(2, io.attempts.get(fixture.path()).get());
        Assertions.assertEquals(1, io.normalReturns(fixture.path()));
        Assertions.assertFalse(io.delegate.fileExists(fixture.path()));
        Assertions.assertEquals("1", row.get(4));
    }

    @Test
    void excludesNotFoundWithoutRetrying() throws IOException {
        Fixture fixture = fixture(Kind.MANIFEST_LIST);
        RecordingFileIO io = new RecordingFileIO(fixture.table().io(), Map.of(fixture.path(), Failure.NOT_FOUND));
        List<String> row = execute(fixture.table(), io, 0);

        Assertions.assertEquals(1, io.attempts.get(fixture.path()).get());
        Assertions.assertEquals(0, io.normalReturns(fixture.path()));
        Assertions.assertFalse(io.delegate.fileExists(fixture.path()));
        Assertions.assertEquals("0", row.get(4));
    }

    @Test
    void countsConcurrentSuccessesWithoutFailedAttempts() throws IOException {
        Fixture fixture = fixture(Kind.MANIFEST_LIST);
        Table table = fixture.table();
        for (int i = 0; i < 7; i++) {
            table.newAppend().appendFile(ActionTestTables.dataFile("current-" + i + ".parquet", 1)).commit();
        }
        Map<String, Failure> failures = new ConcurrentHashMap<>();
        for (Snapshot snapshot : table.snapshots()) {
            if (snapshot.snapshotId() != table.currentSnapshot().snapshotId()) {
                failures.put(snapshot.manifestListLocation(), Failure.ONCE);
            }
        }
        failures.put(fixture.path(), Failure.ALWAYS);
        RecordingFileIO io = new RecordingFileIO(table.io(), failures, 2);
        List<String> row = execute(table, io, 4);

        long normalReturns = failures.keySet().stream().mapToLong(io::normalReturns).sum();
        Assertions.assertEquals(7, normalReturns);
        Assertions.assertTrue(io.deleteThreads.size() > 1, "exercise concurrent callback execution");
        Assertions.assertEquals(0, io.normalReturns(fixture.path()));
        Assertions.assertTrue(io.delegate.fileExists(fixture.path()));
        Assertions.assertEquals(List.of("0", "0", "0", "0", "7", "0"), row);
    }

    private static List<String> execute(Table table, RecordingFileIO io, int concurrency) {
        Table recordingTable = new BaseTable(((HasTableOperations) table).operations(), table.name()) {
            @Override
            public FileIO io() {
                return io;
            }
        };
        IcebergExpireSnapshotsAction action = new IcebergExpireSnapshotsAction(
                Map.of("older_than", "4102444800000", "retain_last", "1",
                        "max_concurrent_deletes", String.valueOf(concurrency)), Collections.emptyList(), null);
        action.validate();
        return action.execute(recordingTable, ActionTestTables.session("UTC")).getRows().get(0);
    }

    private static Fixture fixture(Kind kind) throws IOException {
        InMemoryCatalog catalog = ActionTestTables.freshCatalog();
        Table table = ActionTestTables.createTable(catalog, "deletion_counts_" + UUID.randomUUID());
        DataFile data = ActionTestTables.dataFile("expired-" + UUID.randomUUID() + ".parquet", 1);
        table.io().newOutputFile(data.location()).create().close();
        table.newAppend().appendFile(data).commit();
        Snapshot first = table.currentSnapshot();
        String path;
        switch (kind) {
            case DATA:
            case MANIFEST:
                path = kind == Kind.DATA ? data.location() : first.dataManifests(table.io()).get(0).path();
                table.newDelete().deleteFile(data.location()).commit();
                break;
            case POSITION_DELETES:
            case EQUALITY_DELETES:
                path = table.location() + "/expired-delete.parquet";
                FileMetadata.Builder builder = FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned());
                if (kind == Kind.POSITION_DELETES) {
                    builder.ofPositionDeletes().withReferencedDataFile(data.location());
                } else {
                    builder.ofEqualityDeletes(1);
                }
                DeleteFile delete = builder.withPath(path).withFormat(FileFormat.PARQUET)
                        .withFileSizeInBytes(1).withRecordCount(1).build();
                table.io().newOutputFile(path).create().close();
                table.newRowDelta().addDeletes(delete).commit();
                table.newRowDelta().removeDeletes(delete).commit();
                break;
            case STATISTICS:
                path = table.location() + "/expired.stats";
                table.io().newOutputFile(path).create().close();
                table.updateStatistics().setStatistics(new GenericStatisticsFile(
                        first.snapshotId(), path, 0, 0, Collections.emptyList())).commit();
                break;
            case MANIFEST_LIST:
                path = first.manifestListLocation();
                break;
            default:
                throw new AssertionError(kind);
        }
        table.newAppend().appendFile(ActionTestTables.dataFile("current-" + UUID.randomUUID() + ".parquet", 1)).commit();
        Set<String> kindPaths = new HashSet<>();
        kindPaths.add(path);
        for (Snapshot snapshot : table.snapshots()) {
            if (kind == Kind.MANIFEST) {
                for (ManifestFile manifest : snapshot.allManifests(table.io())) {
                    kindPaths.add(manifest.path());
                }
            } else if (kind == Kind.MANIFEST_LIST) {
                kindPaths.add(snapshot.manifestListLocation());
            }
        }
        return new Fixture(table, path, kindPaths);
    }

    private record Fixture(Table table, String path, Set<String> kindPaths) {
    }

    private static class RecordingFileIO implements FileIO {
        private final InMemoryFileIO delegate;
        private final Map<String, Failure> failures;
        private final Map<String, AtomicInteger> attempts = new ConcurrentHashMap<>();
        private final Map<String, AtomicInteger> successes = new ConcurrentHashMap<>();
        private final Set<String> deleteThreads = ConcurrentHashMap.newKeySet();
        private final CountDownLatch concurrentStarts;

        RecordingFileIO(FileIO delegate, Map<String, Failure> failures) {
            this(delegate, failures, 0);
        }

        RecordingFileIO(FileIO delegate, Map<String, Failure> failures, int concurrentStarts) {
            this.delegate = (InMemoryFileIO) delegate;
            this.failures = failures;
            this.concurrentStarts = new CountDownLatch(concurrentStarts);
        }

        int normalReturns(String path) {
            return successes.getOrDefault(path, new AtomicInteger()).get();
        }

        @Override
        public InputFile newInputFile(String path) {
            return delegate.newInputFile(path);
        }

        @Override
        public OutputFile newOutputFile(String path) {
            return delegate.newOutputFile(path);
        }

        @Override
        public void deleteFile(String path) {
            deleteThreads.add(Thread.currentThread().getName());
            concurrentStarts.countDown();
            try {
                Assertions.assertTrue(concurrentStarts.await(30, TimeUnit.SECONDS), "concurrent deletes must overlap");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
            int attempt = attempts.computeIfAbsent(path, ignored -> new AtomicInteger()).incrementAndGet();
            Failure failure = failures.getOrDefault(path, Failure.NONE);
            if (failure == Failure.ALWAYS || (failure == Failure.ONCE && attempt == 1)) {
                throw new RuntimeIOException("Injected delete failure: %s", path);
            }
            if (failure == Failure.NOT_FOUND) {
                // Model a competing deletion after classification, so the following delete reports NotFound.
                delegate.deleteFile(path);
            }
            // Model an idempotent backend: a retry can return normally after the first response was lost.
            if (failure != Failure.RESPONSE_LOST || attempt == 1) {
                delegate.deleteFile(path);
            }
            if (failure == Failure.RESPONSE_LOST && attempt == 1) {
                throw new RuntimeIOException("Injected lost delete response: %s", path);
            }
            successes.computeIfAbsent(path, ignored -> new AtomicInteger()).incrementAndGet();
        }
    }
}
