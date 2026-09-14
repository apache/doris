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

package org.apache.doris.datasource.iceberg.action;

import org.apache.doris.common.security.authentication.ExecutionAuthenticator;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.GenericStatisticsFile;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ImmutableGenericPartitionStatisticsFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.actions.DeleteOrphanFiles.PrefixMismatchMode;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.BulkDeletionFailureException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.SupportsBulkOperations;
import org.apache.iceberg.io.SupportsPrefixOperations;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class IcebergOrphanFilesTest {
    private static final String ROOT = "s3://bucket/table";
    private static final long CUTOFF = 1_700_000_000_000L;
    private static final ExecutionAuthenticator AUTH = new ExecutionAuthenticator() {};
    @TempDir
    Path directory;

    private IcebergOrphanFiles operation(Table table, PrefixMismatchMode mode, Runnable cancellation,
            Map<String, String> authorities, ExecutionAuthenticator auth) {
        Map<String, String> schemes = new HashMap<>();
        schemes.put("s3a", "s3");
        schemes.put("s3n", "s3");
        return new IcebergOrphanFiles(table, new Configuration(), table.location(), CUTOFF,
                schemes, authorities, mode, cancellation, auth, directory.resolve("spool"));
    }

    private IcebergOrphanFiles operation(Table table, PrefixMismatchMode mode) {
        return operation(table, mode, () -> { }, Collections.emptyMap(), AUTH);
    }

    private Table table(FileIO io, String... referencedPaths) {
        Table table = Mockito.mock(Table.class, Mockito.withSettings().extraInterfaces(HasTableOperations.class));
        TableOperations ops = Mockito.mock(TableOperations.class);
        TableMetadata metadata = Mockito.mock(TableMetadata.class);
        Mockito.when(((HasTableOperations) table).operations()).thenReturn(ops);
        Mockito.when(ops.current()).thenReturn(metadata);
        Mockito.when(metadata.metadataFileLocation()).thenReturn(ROOT + "/metadata/v1.metadata.json");
        Mockito.when(metadata.previousFiles()).thenReturn(Collections.emptyList());
        Mockito.when(table.location()).thenReturn(ROOT);
        Mockito.when(table.io()).thenReturn(io);
        Mockito.when(table.properties()).thenReturn(Collections.emptyMap());
        Mockito.when(table.specs()).thenReturn(Collections.singletonMap(0, PartitionSpec.unpartitioned()));
        Mockito.when(table.snapshots()).thenReturn(Collections.emptyList());
        Mockito.when(table.partitionStatisticsFiles()).thenReturn(Collections.emptyList());
        List<StatisticsFile> stats = Arrays.stream(referencedPaths)
                .map(path -> new GenericStatisticsFile(1, path, 1, 1, Collections.emptyList()))
                .collect(Collectors.toList());
        Mockito.when(table.statisticsFiles()).thenReturn(stats);
        return table;
    }

    private Consumer<Consumer<String>> paths(String... paths) {
        return consumer -> Arrays.asList(paths).forEach(consumer);
    }

    private void assertSpoolRemoved() throws IOException {
        try (Stream<Path> files = Files.list(directory.resolve("spool"))) {
            Assertions.assertEquals(0, files.count());
        }
    }

    @Test
    void hadoopListingProtectsRetainedSnapshotsMetadataDeletesAndStatistics() throws Exception {
        Path root = directory.resolve("table");
        Table table = new HadoopTables(new Configuration()).create(
                new Schema(Types.NestedField.required(1, "id", Types.LongType.get())),
                PartitionSpec.unpartitioned(), Collections.singletonMap("format-version", "2"), root.toString());
        Path data = Files.createDirectories(root.resolve("data"));
        Path historical = Files.write(data.resolve("historical.parquet"), new byte[] {1});
        Path current = Files.write(data.resolve("current.parquet"), new byte[] {1});
        DataFile oldFile = dataFile(table, historical);
        table.newAppend().appendFile(oldFile).commit();
        long oldSnapshot = table.currentSnapshot().snapshotId();
        table.newAppend().appendFile(dataFile(table, current)).commit();
        table.newDelete().deleteFile(oldFile).commit();
        table.manageSnapshots().createBranch("retained", oldSnapshot).createTag("tag", oldSnapshot).commit();
        Path equality = Files.write(data.resolve("equality.parquet"), new byte[] {1});
        Path position = Files.write(data.resolve("position.parquet"), new byte[] {1});
        table.newRowDelta()
                .addDeletes(FileMetadata.deleteFileBuilder(table.spec()).ofEqualityDeletes(1)
                        .withPath(equality.toString()).withFileSizeInBytes(1).withRecordCount(1).build())
                .addDeletes(FileMetadata.deleteFileBuilder(table.spec()).ofPositionDeletes()
                        .withPath(position.toString()).withFileSizeInBytes(1).withRecordCount(1).build()).commit();
        long snapshot = table.currentSnapshot().snapshotId();
        Path stats = Files.write(root.resolve("metadata/stats.puffin"), new byte[] {1});
        Path partitionStats = Files.write(root.resolve("metadata/partition-stats.parquet"), new byte[] {1});
        table.updateStatistics().setStatistics(new GenericStatisticsFile(snapshot, stats.toString(), 1, 1,
                Collections.emptyList())).commit();
        table.updatePartitionStatistics().setPartitionStatistics(
                ImmutableGenericPartitionStatisticsFile.builder().snapshotId(snapshot)
                        .path(partitionStats.toString()).fileSizeInBytes(1).build()).commit();
        Path orphan = Files.write(data.resolve("aborted.parquet"), new byte[] {1});
        Path hidden = Files.write(data.resolve("_SUCCESS"), new byte[] {1});
        try (Stream<Path> files = Files.walk(root)) {
            for (Path file : files.filter(Files::isRegularFile).collect(Collectors.toList())) {
                Files.setLastModifiedTime(file, FileTime.fromMillis(CUTOFF - 1));
            }
        }
        Path boundary = Files.write(data.resolve("boundary.parquet"), new byte[] {1});
        Files.setLastModifiedTime(boundary, FileTime.fromMillis(CUTOFF));
        Path recent = Files.write(data.resolve("recent.parquet"), new byte[] {1});
        List<List<String>> result = operation(table, PrefixMismatchMode.ERROR).execute(null, false, true, false, null);
        Assertions.assertEquals(Collections.singletonList(Collections.singletonList(new org.apache.hadoop.fs.Path(
                orphan.toUri()).toString())), result);
        Assertions.assertTrue(Files.exists(orphan));
        Assertions.assertEquals(result, operation(table, PrefixMismatchMode.ERROR).execute(null, false, false, false, 2));
        Assertions.assertFalse(Files.exists(orphan));
        for (Path protectedPath : Arrays.asList(historical, current, equality, position, stats, partitionStats,
                hidden, boundary, recent, root.resolve("metadata/version-hint.text"))) {
            Assertions.assertTrue(Files.exists(protectedPath), protectedPath.toString());
        }
        assertSpoolRemoved();
    }

    private DataFile dataFile(Table table, Path path) {
        return DataFiles.builder(table.spec()).withPath(path.toString()).withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(1).withRecordCount(1).build();
    }

    @Test
    void protectsThePhysicalFileContainingAV3DeletionVector() throws Exception {
        Path root = directory.resolve("v3");
        Table table = new HadoopTables(new Configuration()).create(
                new Schema(Types.NestedField.required(1, "id", Types.LongType.get())),
                PartitionSpec.unpartitioned(), Collections.singletonMap("format-version", "3"), root.toString());
        Path data = Files.write(root.resolve("data.parquet"), new byte[] {1});
        table.newAppend().appendFile(dataFile(table, data)).commit();
        Path vector = Files.write(root.resolve("vectors.puffin"), new byte[] {1});
        DeleteFile dv = FileMetadata.deleteFileBuilder(table.spec()).ofPositionDeletes().withPath(vector.toString())
                .withFormat(FileFormat.PUFFIN).withFileSizeInBytes(1).withRecordCount(1)
                .withReferencedDataFile(data.toString()).withContentOffset(0).withContentSizeInBytes(1).build();
        table.newRowDelta().addDeletes(dv).commit();
        Assertions.assertTrue(operation(table, PrefixMismatchMode.ERROR)
                .execute(paths(vector.toString()), false, false, false, null).isEmpty());
        Assertions.assertTrue(Files.exists(vector));
    }

    @Test
    void preservesAliasesOriginalPathsAndDuplicateInput() throws Exception {
        FileIO io = Mockito.mock(FileIO.class);
        Table table = table(io, ROOT + "/live", "/table/no-prefix");
        String orphan = ROOT + "/space and #hash%20.parquet";
        List<List<String>> result = operation(table, PrefixMismatchMode.ERROR).execute(
                paths("s3a://bucket/table/live", "hdfs://nn/table/no-prefix", orphan, orphan),
                false, true, false, null);
        Assertions.assertEquals(Arrays.asList(Collections.singletonList(orphan), Collections.singletonList(orphan)), result);
        Mockito.verify(io, Mockito.never()).deleteFile(ArgumentMatchers.any(String.class));
    }

    @Test
    void checksAllConflictsBeforeDeletionAndReleasesTemporaryResults() throws Exception {
        FileIO io = Mockito.mock(FileIO.class);
        Table table = table(io, "s3://old-bucket/table/live");
        Assertions.assertThrows(ValidationException.class, () -> operation(table, PrefixMismatchMode.ERROR)
                .execute(paths(ROOT + "/orphan", ROOT + "/live"), false, false, false, null));
        Mockito.verify(io, Mockito.never()).deleteFile(ArgumentMatchers.any(String.class));
        assertSpoolRemoved();
        Assertions.assertTrue(operation(table, PrefixMismatchMode.IGNORE)
                .execute(paths(ROOT + "/live"), false, true, false, null).isEmpty());
        Map<String, String> authorities = Collections.singletonMap("old-bucket", "bucket");
        Assertions.assertTrue(operation(table, PrefixMismatchMode.ERROR, () -> { }, authorities, AUTH)
                .execute(paths(ROOT + "/live"), false, false, false, null).isEmpty());
    }

    @Test
    void deleteModeKeepsTheUpstreamJoinMultiplicityEvenWhenAnotherPrefixMatches() throws Exception {
        String path = ROOT + "/data";
        Table table = table(Mockito.mock(FileIO.class), path, "s3://other/table/data", "s3://other/table/data");
        List<List<String>> result = operation(table, PrefixMismatchMode.DELETE)
                .execute(paths(path, path), false, true, false, null);
        Assertions.assertEquals(4, result.size());
        result.forEach(row -> Assertions.assertEquals(path, row.get(0)));
    }

    @Test
    void sourceFailureAndGcDisabledNeverDelete() throws Exception {
        FileIO io = Mockito.mock(FileIO.class);
        Table table = table(io);
        Assertions.assertThrows(IllegalStateException.class, () -> operation(table, PrefixMismatchMode.ERROR)
                .execute(consumer -> {
                    consumer.accept(ROOT + "/orphan");
                    throw new IllegalStateException("listing failed");
                }, false, false, true, null));
        Mockito.verify(io, Mockito.never()).deleteFile(ArgumentMatchers.any(String.class));
        assertSpoolRemoved();
        Mockito.when(table.properties()).thenReturn(Collections.singletonMap("gc.enabled", "false"));
        Assertions.assertThrows(ValidationException.class, () -> operation(table, PrefixMismatchMode.ERROR)
                .execute(paths(ROOT + "/orphan"), false, true, true, null));
    }

    @Test
    void prefixListingUsesStrictCutoffAndFileListTakesPrecedence() throws Exception {
        FileIO io = Mockito.mock(FileIO.class, Mockito.withSettings().extraInterfaces(SupportsPrefixOperations.class));
        Table table = table(io);
        Mockito.when(((SupportsPrefixOperations) io).listPrefix(ROOT + "/")).thenReturn(Arrays.asList(
                new FileInfo(ROOT + "/old", 1, CUTOFF - 1), new FileInfo(ROOT + "/equal", 1, CUTOFF),
                new FileInfo(ROOT + "/_hidden", 1, CUTOFF - 1)));
        Assertions.assertEquals(Collections.singletonList(Collections.singletonList(ROOT + "/old")),
                operation(table, PrefixMismatchMode.ERROR).execute(null, true, true, false, null));
        Table unsupported = table(Mockito.mock(FileIO.class));
        Assertions.assertThrows(ValidationException.class, () -> operation(unsupported, PrefixMismatchMode.ERROR)
                .execute(null, true, true, false, null));
        Assertions.assertEquals(1, operation(unsupported, PrefixMismatchMode.ERROR)
                .execute(paths(ROOT + "/old"), true, true, false, null).size());
    }

    @Test
    void streamingSamples20000ButDeletesAll30000AndBulkIgnoresConcurrency() throws Exception {
        FileIO io = Mockito.mock(FileIO.class, Mockito.withSettings().extraInterfaces(SupportsBulkOperations.class));
        AtomicInteger deleted = new AtomicInteger();
        Mockito.doAnswer(invocation -> {
            ((Iterable<String>) invocation.getArgument(0)).forEach(path -> deleted.incrementAndGet());
            return null;
        }).when((SupportsBulkOperations) io).deleteFiles(ArgumentMatchers.any());
        Consumer<Consumer<String>> input = consumer -> {
            for (int i = 0; i < 30_000; i++) {
                consumer.accept(ROOT + "/orphan-" + i);
            }
        };
        List<List<String>> result = operation(table(io), PrefixMismatchMode.ERROR)
                .execute(input, false, false, true, Integer.MAX_VALUE);
        Assertions.assertEquals(20_000, result.size());
        Assertions.assertEquals(30_000, deleted.get());
        Assertions.assertEquals(30_000, operation(table(io), PrefixMismatchMode.ERROR)
                .execute(input, false, true, false, null).size());
        assertSpoolRemoved();
    }

    @Test
    void deletionFailuresReturnCandidatesButUnexpectedBulkFailurePropagates() throws Exception {
        FileIO single = Mockito.mock(FileIO.class);
        Mockito.doThrow(new RuntimeException("single failure")).when(single).deleteFile(ROOT + "/a");
        AtomicInteger authenticated = new AtomicInteger();
        ExecutionAuthenticator auth = new ExecutionAuthenticator() {
            @Override
            public <T> T execute(Callable<T> task) throws Exception {
                authenticated.incrementAndGet();
                return task.call();
            }
        };
        Assertions.assertEquals(2, operation(table(single), PrefixMismatchMode.ERROR, () -> { }, Collections.emptyMap(), auth)
                .execute(paths(ROOT + "/a", ROOT + "/b"), false, false, false, 2).size());
        Assertions.assertEquals(2, authenticated.get());
        Mockito.verify(single).deleteFile(ROOT + "/b");
        FileIO bulk = Mockito.mock(FileIO.class, Mockito.withSettings().extraInterfaces(SupportsBulkOperations.class));
        Mockito.doThrow(new BulkDeletionFailureException(1)).when((SupportsBulkOperations) bulk).deleteFiles(ArgumentMatchers.any());
        Assertions.assertEquals(2, operation(table(bulk), PrefixMismatchMode.ERROR)
                .execute(paths(ROOT + "/a", ROOT + "/b"), false, false, false, null).size());
        Mockito.doThrow(new IllegalStateException("unexpected bulk failure")).when((SupportsBulkOperations) bulk)
                .deleteFiles(ArgumentMatchers.any());
        Assertions.assertThrows(IllegalStateException.class, () -> operation(table(bulk), PrefixMismatchMode.ERROR)
                .execute(paths(ROOT + "/a"), false, false, false, null));
        assertSpoolRemoved();
    }

    @Test
    void cancellationStopsFurtherDeletesAndCleansSpool() throws Exception {
        FileIO io = Mockito.mock(FileIO.class);
        AtomicBoolean cancelled = new AtomicBoolean();
        Mockito.doAnswer(invocation -> {
            cancelled.set(true);
            return null;
        }).when(io).deleteFile(ROOT + "/a");
        Runnable cancellation = () -> {
            if (cancelled.get()) {
                throw new IllegalStateException("cancelled");
            }
        };
        Assertions.assertThrows(IllegalStateException.class, () -> operation(table(io), PrefixMismatchMode.ERROR,
                cancellation, Collections.emptyMap(), AUTH)
                .execute(paths(ROOT + "/a", ROOT + "/b"), false, false, true, null));
        Mockito.verify(io).deleteFile(ROOT + "/a");
        Mockito.verify(io, Mockito.never()).deleteFile(ROOT + "/b");
        assertSpoolRemoved();
    }
}
