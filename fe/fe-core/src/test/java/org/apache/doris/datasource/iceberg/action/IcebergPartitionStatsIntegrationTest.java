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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.UserException;
import org.apache.doris.common.security.authentication.ExecutionAuthenticator;
import org.apache.doris.datasource.ExternalMetaCacheMgr;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.datasource.iceberg.IcebergMetadataOps;
import org.apache.doris.datasource.iceberg.IcebergUtils;
import org.apache.doris.qe.ResultSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionStatistics;
import org.apache.iceberg.PartitionStatisticsFile;
import org.apache.iceberg.PartitionStatsHandler;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ThreadPools;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

class IcebergPartitionStatsIntegrationTest {
    private static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "p", Types.StringType.get()));
    private static final PartitionSpec SPEC = PartitionSpec.builderFor(SCHEMA).identity("p").build();

    @TempDir
    Path temporary;
    private ExternalMetaCacheMgr cache;
    private MockedStatic<Env> mockedEnv;

    @BeforeEach
    void setUp() {
        Env env = Mockito.mock(Env.class);
        cache = Mockito.mock(ExternalMetaCacheMgr.class);
        Mockito.when(env.getExtMetaCacheMgr()).thenReturn(cache);
        mockedEnv = Mockito.mockStatic(Env.class);
        mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
    }

    @AfterEach
    void tearDown() {
        mockedEnv.close();
    }

    @Test
    void testAvroAndParquetContentRegistrationAndReuse() throws Exception {
        for (String format : Arrays.asList("avro", "parquet")) {
            Table table = create(format, 2, format, SPEC);
            table.newAppend().appendFile(data(table, "a1", "a", 10))
                    .appendFile(data(table, "a2", "a", 20))
                    .appendFile(data(table, "b1", "b", 7)).commit();
            long snapshot = table.currentSnapshot().snapshotId();
            String path = compute(table, null);
            Assertions.assertTrue(path.endsWith("." + format));
            table.refresh();
            Assertions.assertEquals(snapshot, table.currentSnapshot().snapshotId());
            Assertions.assertEquals(1, table.partitionStatisticsFiles().size());
            PartitionStatisticsFile file = table.partitionStatisticsFiles().get(0);
            Assertions.assertEquals(snapshot, file.snapshotId());
            Assertions.assertEquals(path, file.path());
            Assertions.assertEquals(table.io().newInputFile(path).getLength(), file.fileSizeInBytes());
            assertData(stats(table, snapshot), "0/a", 30, 2, 300);
            assertData(stats(table, snapshot), "0/b", 7, 1, 70);
            Assertions.assertEquals(path, compute(table, null));
            Assertions.assertEquals(1, statisticsFileCount(table));
        }
    }

    @Test
    void testIncrementalOverwriteAndHistoricalSnapshot() throws Exception {
        Table table = create("incremental", 2, "avro", SPEC);
        DataFile original = data(table, "a1", "a", 10);
        table.newAppend().appendFile(original).appendFile(data(table, "b1", "b", 7)).commit();
        long first = table.currentSnapshot().snapshotId();
        String firstStats = compute(table, null);
        table.newOverwrite().deleteFile(original).addFile(data(table, "a2", "a", 5)).commit();
        table.newAppend().appendFile(data(table, "b2", "b", 3)).commit();
        long current = table.currentSnapshot().snapshotId();
        compute(table, null);
        assertData(stats(table, current), "0/a", 5, 1, 50);
        assertData(stats(table, current), "0/b", 10, 2, 100);
        Assertions.assertEquals(firstStats, compute(table, first));
        Assertions.assertEquals(current, table.currentSnapshot().snapshotId());
        assertData(stats(table, first), "0/a", 10, 1, 100);
    }

    @Test
    void testStatisticsFromAnotherBranchAreNotAnAncestor() throws Exception {
        Table table = create("branches", 2, "avro", SPEC);
        table.newAppend().appendFile(data(table, "base", "a", 10)).commit();
        long base = table.currentSnapshot().snapshotId();
        compute(table, base);
        table.manageSnapshots().createBranch("other", base).commit();
        table.newAppend().appendFile(data(table, "other", "a", 9)).toBranch("other").commit();
        long branch = table.refs().get("other").snapshotId();
        compute(table, branch);
        table.newAppend().appendFile(data(table, "main", "a", 4)).commit();
        compute(table, null);
        assertData(stats(table, table.currentSnapshot().snapshotId()), "0/a", 14, 2, 140);
        assertData(stats(table, branch), "0/a", 19, 2, 190);
    }

    @Test
    void testCorruptAncestorStatisticsFallBackToFullCompute() throws Exception {
        Table table = create("corrupt", 2, "avro", SPEC);
        table.newAppend().appendFile(data(table, "a1", "a", 10)).commit();
        String oldStats = compute(table, null);
        Files.write(localPath(oldStats), new byte[] {0, 1, 2});
        // The selected snapshot's own file is reused without reading it.
        Assertions.assertEquals(oldStats, compute(table, null));
        table.newAppend().appendFile(data(table, "a2", "a", 5)).commit();
        compute(table, null);
        assertData(stats(table, table.currentSnapshot().snapshotId()), "0/a", 15, 2, 150);
    }

    @Test
    void testEvolutionToUnpartitionedSpec() throws Exception {
        Table table = create("evolution", 2, "avro", SPEC);
        table.newAppend().appendFile(data(table, "partitioned", "a", 10)).commit();
        compute(table, null);
        table.updateSpec().removeField("p").commit();
        Assertions.assertTrue(table.spec().isUnpartitioned());
        int currentSpec = table.spec().specId();
        table.newAppend().appendFile(data(table, "unpartitioned", null, 8)).commit();
        compute(table, null);
        Map<String, List<Long>> result = stats(table, table.currentSnapshot().snapshotId());
        assertData(result, "0/a", 10, 1, 100);
        assertData(result, currentSpec + "/null", 8, 1, 80);
    }

    @Test
    void testDeleteAndDeletionVectorCounters() throws Exception {
        for (int version : new int[] {2, 3}) {
            Table table = create("deletes" + version, version, "parquet", SPEC);
            DataFile data = data(table, "a1", "a", 100);
            table.newAppend().appendFile(data).commit();
            FileMetadata.Builder builder = FileMetadata.deleteFileBuilder(table.spec()).ofPositionDeletes()
                    .withPath(table.location() + "/position." + (version == 3 ? "puffin" : "parquet"))
                    .withPartitionPath("p=a").withRecordCount(3).withFileSizeInBytes(30)
                    .withFormat(version == 3 ? FileFormat.PUFFIN : FileFormat.PARQUET);
            if (version == 3) {
                builder.withReferencedDataFile(data.path()).withContentOffset(0).withContentSizeInBytes(20);
            }
            DeleteFile position = builder.build();
            DeleteFile equality = FileMetadata.deleteFileBuilder(table.spec()).ofEqualityDeletes(1)
                    .withPath(table.location() + "/equality.parquet").withFormat(FileFormat.PARQUET)
                    .withPartitionPath("p=a").withRecordCount(5).withFileSizeInBytes(50).build();
            table.newRowDelta().addDeletes(position).addDeletes(equality).commit();
            compute(table, null);
            List<Long> counts = stats(table, table.currentSnapshot().snapshotId()).get("0/a");
            Assertions.assertEquals(Arrays.asList(100L, 1L, 1000L, 3L, version == 3 ? 0L : 1L, 5L, 1L),
                    counts.subList(0, 7));
            Assertions.assertEquals(version == 3 ? 1L : -1L, counts.get(7).longValue());
            Assertions.assertEquals(-1L, counts.get(8).longValue());
        }
    }

    @Test
    void testEmptyAndUnpartitionedBoundaries() throws Exception {
        Table table = create("empty", 2, "avro", PartitionSpec.unpartitioned());
        Assertions.assertTrue(execute(table, null).getResultRows().isEmpty());
        UserException invalid = Assertions.assertThrows(UserException.class, () -> execute(table, 123L));
        Assertions.assertTrue(invalid.getMessage().contains("Snapshot not found: 123"));
        Assertions.assertEquals(0, statisticsFileCount(table));
        table.newAppend().appendFile(data(table, "data", null, 1)).commit();
        UserException unpartitioned = Assertions.assertThrows(UserException.class, () -> execute(table, null));
        Assertions.assertTrue(unpartitioned.getMessage().contains("Table must be partitioned"));
        Assertions.assertTrue(table.partitionStatisticsFiles().isEmpty());
    }

    @Test
    void testDeletedDataIsNotTreatedAsNoSnapshot() throws Exception {
        Table table = create("deleted", 2, "avro", SPEC);
        DataFile file = data(table, "data", "a", 10);
        table.newAppend().appendFile(file).commit();
        compute(table, null);
        table.newDelete().deleteFile(file).commit();
        compute(table, null);
        assertData(stats(table, table.currentSnapshot().snapshotId()), "0/a", 0, 0, 0);
    }

    @Test
    void testUnregisteredFormatOnlyFailsWhenWritingNewStatistics() throws Exception {
        Table table = create("orc", 2, "avro", SPEC);
        table.newAppend().appendFile(data(table, "a1", "a", 10)).commit();
        String existing = compute(table, null);
        table.updateProperties().set(TableProperties.DEFAULT_FILE_FORMAT, "orc").commit();
        Assertions.assertEquals(existing, compute(table, null));
        table.newAppend().appendFile(data(table, "a2", "a", 5)).commit();
        UserException error = Assertions.assertThrows(UserException.class, () -> compute(table, null));
        Assertions.assertTrue(error.getMessage().contains("unregistered internal data format: ORC"));
        Assertions.assertEquals(1, table.partitionStatisticsFiles().size());
    }

    @Test
    void testRealCommitFailureAndLostCommitResponsePreserveFiles() throws Exception {
        for (boolean committed : new boolean[] {false, true}) {
            Table table = create("commit" + committed, 2, "avro", SPEC);
            table.newAppend().appendFile(data(table, "a1", "a", 10)).commit();
            TableOperations delegate = ((HasTableOperations) table).operations();
            TableOperations failing = Mockito.spy(delegate);
            FileIO io = Mockito.spy(delegate.io());
            Mockito.doReturn(io).when(failing).io();
            RuntimeException failure = committed
                    ? new CommitStateUnknownException(new IOException("lost response"))
                    : new CommitFailedException("injected conflict");
            Mockito.doAnswer(invocation -> {
                if (committed) {
                    delegate.commit(invocation.getArgument(0), invocation.getArgument(1));
                }
                throw failure;
            }).when(failing).commit(Mockito.any(TableMetadata.class), Mockito.any(TableMetadata.class));
            Table target = new BaseTable(failing, table.name());
            UserException error = Assertions.assertThrows(UserException.class, () -> execute(target, null));
            Assertions.assertSame(failure, error.getCause());
            Mockito.verify(failing, Mockito.times(1))
                    .commit(Mockito.any(TableMetadata.class), Mockito.any(TableMetadata.class));
            Mockito.verify(io, Mockito.never()).deleteFile(Mockito.anyString());
            table.refresh();
            Assertions.assertEquals(committed ? 1 : 0, table.partitionStatisticsFiles().size());
            Assertions.assertEquals(1, statisticsFileCount(table));
            Mockito.verifyNoInteractions(cache);
        }
    }

    private Table create(String name, int version, String format, PartitionSpec spec) {
        Map<String, String> properties = new HashMap<>();
        properties.put(TableProperties.FORMAT_VERSION, Integer.toString(version));
        properties.put(TableProperties.DEFAULT_FILE_FORMAT, format);
        return new HadoopTables(new Configuration()).create(
                SCHEMA, spec, SortOrder.unsorted(), properties, temporary.resolve(name).toString());
    }

    @Test
    void testStatisticsWorkersUseTheSelectedCatalogIdentity() throws Exception {
        UserGroupInformation user = UserGroupInformation.createRemoteUser("stats_" + UUID.randomUUID());
        String userName = user.getUserName();
        CountDownLatch started = new CountDownLatch(ThreadPools.WORKER_THREAD_POOL_SIZE);
        CountDownLatch release = new CountDownLatch(1);
        List<Future<String>> workers = new ArrayList<>();
        try {
            for (int i = 0; i < ThreadPools.WORKER_THREAD_POOL_SIZE; i++) {
                workers.add(ThreadPools.getWorkerPool().submit(() -> {
                    started.countDown();
                    release.await();
                    return UserGroupInformation.getCurrentUser().getUserName();
                }));
            }
            Assertions.assertTrue(started.await(30, TimeUnit.SECONDS));
        } finally {
            release.countDown();
        }
        for (Future<String> worker : workers) {
            Assertions.assertNotEquals(userName, worker.get(30, TimeUnit.SECONDS));
        }

        Table table = create("worker-auth", 2, "parquet", SPEC);
        table.newFastAppend().appendFile(data(table, "data", "a", 3)).commit();
        long snapshot = table.currentSnapshot().snapshotId();
        FileIO delegate = table.io();
        Thread caller = Thread.currentThread();
        AtomicInteger wrongUsers = new AtomicInteger();
        AtomicInteger authenticatedWorkerReads = new AtomicInteger();
        FileIO checkingIo = new FileIO() {
            private void checkUser() {
                try {
                    if (!userName.equals(UserGroupInformation.getCurrentUser().getUserName())) {
                        wrongUsers.incrementAndGet();
                        throw new IllegalStateException("Statistics I/O used the worker's previous identity");
                    }
                    if (Thread.currentThread() != caller) {
                        authenticatedWorkerReads.incrementAndGet();
                    }
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }
            }

            @Override
            public InputFile newInputFile(String path) {
                checkUser();
                return delegate.newInputFile(path);
            }

            @Override
            public InputFile newInputFile(String path, long length) {
                checkUser();
                return delegate.newInputFile(path, length);
            }

            @Override
            public OutputFile newOutputFile(String path) {
                checkUser();
                return delegate.newOutputFile(path);
            }

            @Override
            public void deleteFile(String path) {
                delegate.deleteFile(path);
            }

            @Override
            public Map<String, String> properties() {
                return delegate.properties();
            }
        };
        Table checkedTable = new BaseTable(((HasTableOperations) table).operations(), table.name()) {
            @Override
            public FileIO io() {
                return checkingIo;
            }
        };
        ExecutionAuthenticator authenticator = new ExecutionAuthenticator() {
            @Override
            public <T> T execute(Callable<T> task) throws Exception {
                return user.doAs((PrivilegedExceptionAction<T>) task::call);
            }
        };

        Assertions.assertThrows(RuntimeException.class, () -> authenticator.execute(
                (Callable<PartitionStatisticsFile>) () ->
                        PartitionStatsHandler.computeAndWriteStatsFile(checkedTable, snapshot)));
        Assertions.assertTrue(wrongUsers.get() > 0);
        Assertions.assertEquals(0, statisticsFileCount(table));
        wrongUsers.set(0);
        ResultSet result = authenticator.execute(
                (Callable<ResultSet>) () -> execute(checkedTable, snapshot, authenticator));
        Assertions.assertEquals(1, result.getResultRows().size());
        Assertions.assertEquals(0, wrongUsers.get());
        Assertions.assertTrue(authenticatedWorkerReads.get() > 0);
        table.refresh();
        assertData(stats(table, snapshot), "0/a", 3, 1, 30);
    }

    private DataFile data(Table table, String name, String partition, long records) {
        DataFiles.Builder builder = DataFiles.builder(table.spec())
                .withPath(table.location() + "/" + name + ".parquet").withFormat(FileFormat.PARQUET)
                .withRecordCount(records).withFileSizeInBytes(records * 10);
        if (partition != null) {
            builder.withPartitionPath("p=" + partition);
        }
        return builder.build();
    }

    private ResultSet execute(Table table, Long snapshot) throws UserException {
        return execute(table, snapshot, Mockito.mock(ExecutionAuthenticator.class, Mockito.CALLS_REAL_METHODS));
    }

    private ResultSet execute(Table table, Long snapshot, ExecutionAuthenticator authenticator) throws UserException {
        IcebergExternalTable dorisTable = Mockito.mock(IcebergExternalTable.class);
        IcebergMetadataOps metadataOps = Mockito.mock(IcebergMetadataOps.class);
        Mockito.when(metadataOps.getExecutionAuthenticator()).thenReturn(authenticator);
        try (MockedStatic<IcebergUtils> utils = Mockito.mockStatic(IcebergUtils.class)) {
            utils.when(() -> IcebergUtils.getWritableIcebergTable(dorisTable, metadataOps)).thenReturn(table);
            TestAction action = new TestAction(snapshot, metadataOps);
            return action.execute(dorisTable);
        }
    }

    private String compute(Table table, Long snapshot) throws UserException {
        ResultSet result = execute(table, snapshot);
        Assertions.assertEquals(1, result.getResultRows().size());
        return result.getResultRows().get(0).get(0);
    }

    private Map<String, List<Long>> stats(Table table, long snapshot) throws IOException {
        Map<String, List<Long>> result = new HashMap<>();
        try (CloseableIterable<PartitionStatistics> records =
                table.newPartitionStatisticsScan().useSnapshot(snapshot).scan()) {
            for (PartitionStatistics record : records) {
                String key = record.specId() + "/" + record.partition().get(0, Object.class);
                result.put(key, Arrays.asList(record.dataRecordCount(), record.dataFileCount().longValue(),
                        record.totalDataFileSizeInBytes(), record.positionDeleteRecordCount(),
                        record.positionDeleteFileCount().longValue(), record.equalityDeleteRecordCount(),
                        record.equalityDeleteFileCount().longValue(), record.dvCount() == null ? -1L : record.dvCount(),
                        record.totalRecords() == null ? -1L : record.totalRecords()));
            }
        }
        return result;
    }

    private void assertData(Map<String, List<Long>> stats, String key, long records, long files, long bytes) {
        Assertions.assertTrue(stats.containsKey(key), stats.toString());
        Assertions.assertEquals(Arrays.asList(records, files, bytes), stats.get(key).subList(0, 3));
    }

    private long statisticsFileCount(Table table) throws IOException {
        try (Stream<Path> files = Files.list(localPath(table.location()).resolve("metadata"))) {
            return files.filter(path -> path.getFileName().toString().startsWith("partition-stats-")).count();
        }
    }

    private Path localPath(String location) {
        URI uri = URI.create(location);
        return uri.getScheme() == null ? Paths.get(location) : Paths.get(uri);
    }

    private static class TestAction extends IcebergComputePartitionStatsAction {
        TestAction(Long snapshot, IcebergMetadataOps metadataOps) throws UserException {
            super(snapshot == null ? Collections.emptyMap()
                            : Collections.singletonMap("snapshot_id", snapshot.toString()),
                    Optional.empty(), Optional.empty(), metadataOps);
            namedArguments.validate(properties);
            validateIcebergAction();
        }
    }
}
