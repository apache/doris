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
import org.apache.doris.catalog.Type;
import org.apache.doris.common.UserException;
import org.apache.doris.common.security.authentication.ExecutionAuthenticator;
import org.apache.doris.datasource.ExternalMetaCacheMgr;
import org.apache.doris.datasource.iceberg.IcebergExternalCatalog;
import org.apache.doris.datasource.iceberg.IcebergExternalMetaCache.CatalogGenerationChangedException;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.datasource.iceberg.IcebergMetadataOps;
import org.apache.doris.datasource.iceberg.IcebergUtils;
import org.apache.doris.info.PartitionNamesInfo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.plans.commands.execute.ExecuteAction;
import org.apache.doris.qe.ResultSet;

import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionStatisticsFile;
import org.apache.iceberg.PartitionStatsHandler;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.UpdatePartitionStatistics;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SeekableInputStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

class IcebergComputePartitionStatsActionTest {
    private IcebergExternalTable dorisTable;
    private Table table;
    private FileIO io;
    private ExternalMetaCacheMgr cache;
    private IcebergMetadataOps metadataOps;
    private MockedStatic<Env> mockedEnv;
    private MockedStatic<IcebergUtils> mockedUtils;

    @BeforeEach
    void setUp() {
        dorisTable = Mockito.mock(IcebergExternalTable.class);
        table = Mockito.mock(Table.class, Mockito.withSettings().extraInterfaces(HasTableOperations.class));
        io = Mockito.mock(FileIO.class);
        Mockito.when(table.io()).thenReturn(io);
        Mockito.when(table.name()).thenReturn("test_table");
        Mockito.when(table.snapshots()).thenReturn(Collections.emptyList());
        Mockito.when(((HasTableOperations) table).operations()).thenReturn(Mockito.mock(TableOperations.class));
        cache = Mockito.mock(ExternalMetaCacheMgr.class);
        metadataOps = Mockito.mock(IcebergMetadataOps.class);
        Mockito.when(metadataOps.getExecutionAuthenticator())
                .thenReturn(Mockito.mock(ExecutionAuthenticator.class, Mockito.CALLS_REAL_METHODS));
        IcebergExternalCatalog catalog = Mockito.mock(IcebergExternalCatalog.class);
        Env env = Mockito.mock(Env.class);
        Mockito.when(dorisTable.getCatalog()).thenReturn(catalog);
        Mockito.when(catalog.getMetadataOps()).thenReturn(metadataOps);
        Mockito.when(env.getExtMetaCacheMgr()).thenReturn(cache);
        mockedEnv = Mockito.mockStatic(Env.class);
        mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
        mockedUtils = Mockito.mockStatic(IcebergUtils.class);
        mockedUtils.when(() -> IcebergUtils.getWritableIcebergTable(dorisTable, metadataOps)).thenReturn(table);
    }

    @AfterEach
    void tearDown() {
        mockedUtils.close();
        mockedEnv.close();
    }

    @Test
    void testFactoryRegistrationAndSchema() throws Exception {
        ExecuteAction action = IcebergExecuteActionFactory.createAction("COMPUTE_PARTITION_STATS",
                Collections.emptyMap(), Optional.empty(), Optional.empty(), dorisTable);
        Assertions.assertTrue(action instanceof IcebergComputePartitionStatsAction);
        Assertions.assertTrue(Arrays.asList(IcebergExecuteActionFactory.getSupportedActions())
                .contains("compute_partition_stats"));
        Assertions.assertTrue(action.isSupported(dorisTable));
        Assertions.assertFalse(action.isSupported(null));
        ResultSet result = action.execute(dorisTable);
        Assertions.assertTrue(result.getResultRows().isEmpty());
        Assertions.assertEquals("partition_statistics_file", result.getMetaData().getColumn(0).getName());
        Assertions.assertEquals(Type.STRING, result.getMetaData().getColumn(0).getType());
        Assertions.assertTrue(result.getMetaData().getColumn(0).isAllowNull());
    }

    @Test
    void testArgumentsAndForbiddenClauses() throws Exception {
        for (String value : Arrays.asList("", "abc", "1.5", "9223372036854775808", "-9223372036854775809")) {
            Assertions.assertThrows(UserException.class,
                    () -> action(Collections.singletonMap("snapshot_id", value)));
        }
        Assertions.assertThrows(UserException.class, () -> action(Collections.singletonMap("unknown", "1")));
        TestAction partitions = new TestAction(Collections.emptyMap(),
                Optional.of(new PartitionNamesInfo(false, Collections.singletonList("p"))), Optional.empty(), metadataOps);
        Assertions.assertThrows(UserException.class, partitions::validateParameters);
        TestAction where = new TestAction(Collections.emptyMap(), Optional.empty(),
                Optional.of(BooleanLiteral.TRUE), metadataOps);
        Assertions.assertThrows(UserException.class, where::validateParameters);
    }

    @Test
    void testExplicitIdsUseFullLongRangeAndFailBeforeSdk() throws Exception {
        try (MockedStatic<PartitionStatsHandler> handler = Mockito.mockStatic(PartitionStatsHandler.class)) {
            for (long id : new long[] {Long.MIN_VALUE, Long.MAX_VALUE, 0, -1, 7}) {
                TestAction action = action(Collections.singletonMap("snapshot_id", Long.toString(id)));
                UserException exception = Assertions.assertThrows(UserException.class, () -> action.execute(dorisTable));
                Assertions.assertTrue(exception.getMessage().contains("Snapshot not found: " + id));
                Mockito.verify(table).snapshot(id);
            }
            handler.verifyNoInteractions();
            Mockito.verify(table, Mockito.never()).currentSnapshot();
            Mockito.verify(table, Mockito.never()).updatePartitionStatistics();
        }
    }

    @Test
    void testNoCurrentSnapshotDoesNotCallSdk() throws Exception {
        try (MockedStatic<PartitionStatsHandler> handler = Mockito.mockStatic(PartitionStatsHandler.class)) {
            ResultSet result = action(Collections.emptyMap()).execute(dorisTable);
            Assertions.assertNotNull(result);
            Assertions.assertTrue(result.getResultRows().isEmpty());
            handler.verifyNoInteractions();
            Mockito.verifyNoInteractions(cache);
        }
    }

    @Test
    void testCatalogGenerationFencePropagatesBeforeStatisticsIo() throws Exception {
        CatalogGenerationChangedException failure = new CatalogGenerationChangedException("catalog reset");
        mockedUtils.when(() -> IcebergUtils.getWritableIcebergTable(dorisTable, metadataOps)).thenThrow(failure);
        try (MockedStatic<PartitionStatsHandler> handler = Mockito.mockStatic(PartitionStatsHandler.class)) {
            Assertions.assertSame(failure, Assertions.assertThrows(CatalogGenerationChangedException.class,
                    () -> action(Collections.emptyMap()).execute(dorisTable)));
            handler.verifyNoInteractions();
            Mockito.verifyNoInteractions(table, cache);
        }
    }

    @Test
    void testSdkEmptyResultDoesNotCommit() throws Exception {
        currentSnapshot(42);
        try (MockedStatic<PartitionStatsHandler> handler = Mockito.mockStatic(PartitionStatsHandler.class)) {
            ResultSet result = action(Collections.emptyMap()).execute(dorisTable);
            Assertions.assertTrue(result.getResultRows().isEmpty());
            handler.verify(() -> PartitionStatsHandler.computeAndWriteStatsFile(
                    Mockito.any(Table.class), Mockito.eq(42L)));
            Mockito.verify(table, Mockito.never()).updatePartitionStatistics();
            Mockito.verifyNoInteractions(cache);
        }
    }

    @Test
    void testDefaultSnapshotIsPinnedAndCommitPrecedesInvalidation() throws Exception {
        currentSnapshot(42);
        PartitionStatisticsFile file = statisticsFile("stats-42.parquet");
        UpdatePartitionStatistics update = update(file);
        try (MockedStatic<PartitionStatsHandler> handler = Mockito.mockStatic(PartitionStatsHandler.class)) {
            handler.when(() -> PartitionStatsHandler.computeAndWriteStatsFile(
                    Mockito.any(Table.class), Mockito.eq(42L))).thenAnswer(invocation -> {
                        currentSnapshot(99);
                        return file;
                    });
            ResultSet result = action(Collections.emptyMap()).execute(dorisTable);
            Assertions.assertEquals(Collections.singletonList(Collections.singletonList(file.path())),
                    result.getResultRows());
            Mockito.verify(table, Mockito.times(1)).currentSnapshot();
            InOrder order = Mockito.inOrder(update, cache);
            order.verify(update).setPartitionStatistics(file);
            order.verify(update).commit();
            order.verify(cache).invalidateTableCache(dorisTable);
        }
    }

    @Test
    void testExplicitSnapshotAndExistingFileReuse() throws Exception {
        Snapshot historical = Mockito.mock(Snapshot.class);
        Mockito.when(historical.snapshotId()).thenReturn(7L);
        Mockito.when(table.snapshot(7)).thenReturn(historical);
        PartitionStatisticsFile file = statisticsFile("existing-7.avro");
        UpdatePartitionStatistics update = update(file);
        try (MockedStatic<PartitionStatsHandler> handler = Mockito.mockStatic(PartitionStatsHandler.class)) {
            handler.when(() -> PartitionStatsHandler.computeAndWriteStatsFile(
                    Mockito.any(Table.class), Mockito.eq(7L))).thenReturn(file);
            TestAction action = action(Collections.singletonMap("snapshot_id", "7"));
            Assertions.assertEquals(action.execute(dorisTable).getResultRows(), action.execute(dorisTable).getResultRows());
            Mockito.verify(table, Mockito.never()).currentSnapshot();
            Mockito.verify(update, Mockito.times(2)).commit();
            Mockito.verify(cache, Mockito.times(2)).invalidateTableCache(dorisTable);
        }
    }

    @Test
    void testSdkFailurePreservesCause() throws Exception {
        currentSnapshot(42);
        IOException failure = new IOException("statistics write failed");
        try (MockedStatic<PartitionStatsHandler> handler = Mockito.mockStatic(PartitionStatsHandler.class)) {
            handler.when(() -> PartitionStatsHandler.computeAndWriteStatsFile(
                    Mockito.any(Table.class), Mockito.eq(42L))).thenThrow(failure);
            UserException error = Assertions.assertThrows(UserException.class,
                    () -> action(Collections.emptyMap()).execute(dorisTable));
            Assertions.assertSame(failure, error.getCause());
            Mockito.verify(table, Mockito.never()).updatePartitionStatistics();
            Mockito.verifyNoInteractions(cache);
        }
    }

    @Test
    void testCommitFailuresDoNotRetryOrCleanFiles() throws Exception {
        currentSnapshot(42);
        PartitionStatisticsFile file = statisticsFile("possibly-committed.parquet");
        UpdatePartitionStatistics update = update(file);
        for (RuntimeException failure : Arrays.asList(new CommitFailedException("conflict"),
                new CommitStateUnknownException(new IOException("lost commit response")))) {
            Mockito.clearInvocations(update);
            Mockito.doThrow(failure).when(update).commit();
            try (MockedStatic<PartitionStatsHandler> handler = Mockito.mockStatic(PartitionStatsHandler.class)) {
                handler.when(() -> PartitionStatsHandler.computeAndWriteStatsFile(
                        Mockito.any(Table.class), Mockito.eq(42L))).thenReturn(file);
                UserException error = Assertions.assertThrows(UserException.class,
                        () -> action(Collections.emptyMap()).execute(dorisTable));
                Assertions.assertSame(failure, error.getCause());
                Mockito.verify(update, Mockito.times(1)).commit();
                Mockito.verifyNoInteractions(io);
                Mockito.verifyNoInteractions(cache);
            }
        }
    }

    @Test
    void testPostCommitFailureDoesNotUndoCommit() throws Exception {
        currentSnapshot(42);
        PartitionStatisticsFile file = statisticsFile("committed.parquet");
        UpdatePartitionStatistics update = update(file);
        Mockito.doThrow(new IllegalStateException("cache failure")).when(cache).invalidateTableCache(dorisTable);
        try (MockedStatic<PartitionStatsHandler> handler = Mockito.mockStatic(PartitionStatsHandler.class)) {
            handler.when(() -> PartitionStatsHandler.computeAndWriteStatsFile(
                    Mockito.any(Table.class), Mockito.eq(42L))).thenReturn(file);
            Assertions.assertThrows(UserException.class, () -> action(Collections.emptyMap()).execute(dorisTable));
            Mockito.verify(update).commit();
            Mockito.verifyNoInteractions(io);
        }
    }

    @Test
    void testExistingEmptyRewriteActionsKeepOneRow() throws Exception {
        mockedUtils.when(() -> IcebergUtils.getIcebergTable(dorisTable)).thenReturn(table);
        Assertions.assertEquals(Collections.singletonList(Arrays.asList("0", "0")),
                new IcebergRewriteManifestsAction(Collections.emptyMap(), Optional.empty(), Optional.empty(), metadataOps)
                        .execute(dorisTable).getResultRows());
        Assertions.assertEquals(Collections.singletonList(Arrays.asList("0", "0", "0", "0")),
                new IcebergRewriteDataFilesAction(Collections.emptyMap(), Optional.empty(), Optional.empty(), metadataOps)
                        .execute(dorisTable).getResultRows());
    }

    @Test
    void testSnapshotLookupsDoNotRepeatAuthentication() {
        Snapshot current = Mockito.mock(Snapshot.class);
        Snapshot historical = Mockito.mock(Snapshot.class);
        Mockito.when(current.snapshotId()).thenReturn(42L);
        Mockito.when(historical.snapshotId()).thenReturn(7L);
        Mockito.when(table.snapshot(42L)).thenReturn(current);
        Mockito.when(table.snapshot(7L)).thenReturn(historical);
        AtomicBoolean authenticated = new AtomicBoolean();
        AtomicInteger authenticationCalls = new AtomicInteger();
        Mockito.when(table.snapshots()).thenAnswer(invocation -> {
            Assertions.assertTrue(authenticated.get());
            return (Iterable<Snapshot>) () -> {
                Assertions.assertTrue(authenticated.get(), "Lazy metadata must load under catalog authentication");
                return Arrays.asList(historical, current).iterator();
            };
        });
        ExecutionAuthenticator authenticator = new ExecutionAuthenticator() {
            @Override
            public <T> T execute(Callable<T> task) throws Exception {
                authenticationCalls.incrementAndGet();
                boolean previous = authenticated.getAndSet(true);
                try {
                    return task.call();
                } finally {
                    authenticated.set(previous);
                }
            }
        };
        Table view = new IcebergPartitionStatsTable(table, authenticator);
        for (int entry = 0; entry < 1000; entry++) {
            Assertions.assertSame(current, view.snapshot(42L));
            Assertions.assertSame(historical, view.snapshot(7L));
            Assertions.assertNull(view.snapshot(Long.MIN_VALUE));
        }
        Assertions.assertEquals(1, authenticationCalls.get());
        Assertions.assertFalse(authenticated.get());
        Mockito.verify(table, Mockito.times(1)).snapshots();
    }

    @Test
    void testOwnedStreamsCloseWhenAuthenticationFailsBeforeCleanup() throws Exception {
        AtomicBoolean failAuthentication = new AtomicBoolean();
        IOException authenticationFailure = new IOException("ticket refresh failed");
        ExecutionAuthenticator authenticator = new ExecutionAuthenticator() {
            @Override
            public <T> T execute(Callable<T> task) throws Exception {
                if (failAuthentication.get()) {
                    throw authenticationFailure;
                }
                return task.call();
            }
        };
        InputFile input = Mockito.mock(InputFile.class);
        OutputFile output = Mockito.mock(OutputFile.class);
        SeekableInputStream inputStream = Mockito.mock(SeekableInputStream.class);
        PositionOutputStream outputStream = Mockito.mock(PositionOutputStream.class);
        Mockito.when(io.newInputFile("input")).thenReturn(input);
        Mockito.when(input.newStream()).thenReturn(inputStream);
        Mockito.when(io.newOutputFile("output")).thenReturn(output);
        Mockito.when(output.create()).thenReturn(outputStream);
        Table view = new IcebergPartitionStatsTable(table, authenticator);
        SeekableInputStream scopedInput = view.io().newInputFile("input").newStream();
        PositionOutputStream scopedOutput = view.io().newOutputFile("output").create();
        failAuthentication.set(true);

        Assertions.assertSame(authenticationFailure, Assertions.assertThrows(IOException.class, scopedInput::close));
        IOException cleanupFailure = new IOException("output close failed");
        Mockito.doThrow(cleanupFailure).when(outputStream).close();
        Assertions.assertSame(authenticationFailure, Assertions.assertThrows(IOException.class, scopedOutput::close));
        Assertions.assertArrayEquals(new Throwable[] {cleanupFailure}, authenticationFailure.getSuppressed());
        Mockito.verify(inputStream, Mockito.times(1)).close();
        Mockito.verify(outputStream, Mockito.times(1)).close();
        Mockito.verify(io, Mockito.never()).close();
    }

    private void currentSnapshot(long id) {
        Snapshot snapshot = Mockito.mock(Snapshot.class);
        Mockito.when(snapshot.snapshotId()).thenReturn(id);
        Mockito.when(table.currentSnapshot()).thenReturn(snapshot);
    }

    private PartitionStatisticsFile statisticsFile(String path) {
        PartitionStatisticsFile file = Mockito.mock(PartitionStatisticsFile.class);
        Mockito.when(file.path()).thenReturn(path);
        return file;
    }

    private UpdatePartitionStatistics update(PartitionStatisticsFile file) {
        UpdatePartitionStatistics update = Mockito.mock(UpdatePartitionStatistics.class);
        Mockito.when(table.updatePartitionStatistics()).thenReturn(update);
        Mockito.when(update.setPartitionStatistics(file)).thenReturn(update);
        return update;
    }

    private TestAction action(Map<String, String> properties) throws UserException {
        TestAction action = new TestAction(properties, Optional.empty(), Optional.empty(), metadataOps);
        action.validateParameters();
        return action;
    }

    private static class TestAction extends IcebergComputePartitionStatsAction {
        TestAction(Map<String, String> properties, Optional<PartitionNamesInfo> partitions, Optional<Expression> where,
                IcebergMetadataOps metadataOps) {
            super(properties, partitions, where, metadataOps);
        }

        void validateParameters() throws UserException {
            namedArguments.validate(properties);
            validateIcebergAction();
        }
    }
}
