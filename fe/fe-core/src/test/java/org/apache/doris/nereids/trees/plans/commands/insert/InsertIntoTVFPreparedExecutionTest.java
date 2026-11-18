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

package org.apache.doris.nereids.trees.plans.commands.insert;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.EnvFactory;
import org.apache.doris.common.Status;
import org.apache.doris.datasource.storage.StorageAdapter;
import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.Location;
import org.apache.doris.fs.FileSystemFactory;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.ExecuteCommand;
import org.apache.doris.nereids.trees.plans.commands.PrepareCommand;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.Coordinator;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.PreparedStatementContext;
import org.apache.doris.qe.QeProcessorImpl;
import org.apache.doris.qe.QueryState.MysqlStateType;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.task.LoadEtlTask;
import org.apache.doris.thrift.TQueryOptions;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

public class InsertIntoTVFPreparedExecutionTest extends TestWithFeService {
    private static final String SQL = "INSERT INTO s3("
            + "\"file_path\"=\"s3://unit-test/result_\",\"format\"=\"csv\","
            + "\"delete_existing_files\"=\"true\","
            + "\"s3.endpoint\"=\"http://127.0.0.1:1\",\"s3.region\"=\"us-east-1\","
            + "\"s3.access_key\"=\"unit-test-access\",\"s3.secret_key\"=\"unit-test-secret\") "
            + "SELECT k FROM tvf_prepared_test.src";
    private Coordinator firstCoordinator;
    private Coordinator nextCoordinator;
    private FileSystem filesystem;
    private EnvFactory factory;
    private MockedStatic<EnvFactory> factoryMock;
    private MockedStatic<FileSystemFactory> filesystemMock;

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("tvf_prepared_test");
        connectContext.setDatabase("tvf_prepared_test");
        createTable("CREATE TABLE src(k INT) DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 "
                + "PROPERTIES(\"replication_num\"=\"1\")");
    }

    @Override
    protected void runBeforeEach() throws Exception {
        connectContext.setThreadLocalInfo();
        connectContext.setCurrentUserIdentity(UserIdentity.ROOT);
        connectContext.getSessionVariable().setQueryTimeoutS(3);
        firstCoordinator = coordinator();
        nextCoordinator = coordinator();
        factory = Mockito.spy(EnvFactory.getInstance());
        Mockito.doReturn(firstCoordinator, nextCoordinator).when(factory)
                .createCoordinator(Mockito.any(), Mockito.any(), Mockito.any());
        factoryMock = Mockito.mockStatic(EnvFactory.class);
        factoryMock.when(EnvFactory::getInstance).thenReturn(factory);
        filesystem = Mockito.mock(FileSystem.class);
        filesystemMock = Mockito.mockStatic(FileSystemFactory.class, Mockito.CALLS_REAL_METHODS);
        filesystemMock.when(() -> FileSystemFactory.getFileSystem(Mockito.any(StorageAdapter.class)))
                .thenReturn(filesystem);
    }

    @AfterEach
    public void closeMocksAndContext() throws Exception {
        try {
            connectContext.getStatementContext().close();
        } finally {
            filesystemMock.close();
            factoryMock.close();
            ConnectContext.remove();
        }
    }

    private Coordinator coordinator() {
        Coordinator coordinator = Mockito.mock(Coordinator.class);
        Mockito.when(coordinator.getQueryOptions()).thenReturn(new TQueryOptions());
        Mockito.when(coordinator.getDispatchedBackendIds()).thenReturn(Set.of(10001L));
        Mockito.when(coordinator.getExecStatus()).thenReturn(Status.OK);
        Mockito.when(coordinator.join(Mockito.anyInt())).thenReturn(true);
        Mockito.when(coordinator.isDone()).thenReturn(true);
        Mockito.when(coordinator.getLoadCounters()).thenReturn(Collections.singletonMap(LoadEtlTask.DPP_NORMAL_ALL, "2"));
        return coordinator;
    }

    private PreparedStatementContext prepare() throws Exception {
        StatementContext statement = new StatementContext(connectContext, new OriginStatement(SQL, 0));
        connectContext.setStatementContext(statement);
        InsertIntoTVFCommand command = (InsertIntoTVFCommand) new NereidsParser().parseSingle(SQL);
        PrepareCommand prepare = new PrepareCommand("tvf", command, Collections.emptyList(),
                new OriginStatement(SQL, 0));
        prepare.run(connectContext, new StmtExecutor(connectContext, new LogicalPlanAdapter(prepare, statement)));
        PreparedStatementContext prepared = connectContext.getPreparedStementContext("tvf");
        Assertions.assertSame(command, prepared.command.getLogicalPlan());
        return prepared;
    }

    private void execute(PreparedStatementContext prepared, StmtExecutor executor) throws Exception {
        StatementContext previous = prepared.getStatementContext();
        connectContext.getState().reset();
        connectContext.setExecutor(executor);
        new ExecuteCommand("tvf", prepared.command, previous).run(connectContext, executor);
        Assertions.assertSame(prepared, connectContext.getPreparedStementContext("tvf"));
        Assertions.assertSame(prepared.command.getLogicalPlan(),
                ((LogicalPlanAdapter) executor.getParsedStmt()).getLogicalPlan());
        Assertions.assertNotSame(previous, prepared.getStatementContext());
        Assertions.assertSame(prepared.getStatementContext(), connectContext.getStatementContext());
    }

    private void assertSuccessful(StmtExecutor executor, Coordinator coordinator) throws Exception {
        Assertions.assertEquals(MysqlStateType.OK, connectContext.getState().getStateType(),
                connectContext.getState().getErrorMessage());
        Mockito.verify(coordinator).exec();
        Mockito.verify(coordinator).join(3);
        Mockito.verify(coordinator, Mockito.never()).cancel(Mockito.any(Status.class));
        Mockito.verify(coordinator).close();
        Assertions.assertNull(QeProcessorImpl.INSTANCE.getCoordinator(connectContext.queryId()));
        Assertions.assertSame(coordinator, executor.getCoord());
        Assertions.assertEquals(Set.of(10001L), executor.getAuditStatisticsBackendIds());
    }

    @Test
    public void canceledExecutionDoesNotPoisonSamePreparedHandle() throws Exception {
        PreparedStatementContext prepared = prepare();
        StmtExecutor first = new StmtExecutor(connectContext, "EXECUTE tvf");
        Mockito.when(firstCoordinator.join(Mockito.anyInt())).thenAnswer(invocation -> {
            first.cancel(new Status(TStatusCode.CANCELLED, "cancel first execution"));
            return true;
        });
        execute(prepared, first);
        Assertions.assertEquals(MysqlStateType.ERR, connectContext.getState().getStateType());
        Assertions.assertEquals("cancel first execution", connectContext.getState().getErrorMessage());
        Mockito.verify(firstCoordinator).exec();
        Mockito.verify(firstCoordinator).close();
        Assertions.assertNull(QeProcessorImpl.INSTANCE.getCoordinator(connectContext.queryId()));
        StatementContext firstStatement = prepared.getStatementContext();
        StmtExecutor next = new StmtExecutor(connectContext, "EXECUTE tvf");
        Assertions.assertNull(next.getPendingCancelReason());

        execute(prepared, next);

        Assertions.assertNotSame(firstStatement, prepared.getStatementContext());
        Assertions.assertNotNull(first.getPendingCancelReason());
        assertSuccessful(next, nextCoordinator);
        Mockito.verify(filesystem, Mockito.times(2)).delete(Mockito.any(Location.class), Mockito.eq(true));
    }

    @Test
    public void lateOldExecutorCallbackDoesNotCancelNextPreparedExecution() throws Exception {
        PreparedStatementContext prepared = prepare();
        CountDownLatch callbackEntered = new CountDownLatch(1);
        CountDownLatch releaseCallback = new CountDownLatch(1);
        StmtExecutor first = new StmtExecutor(connectContext, "EXECUTE tvf") {
            @Override
            public void registerQueryCancellationCallback(Runnable callback) {
                // Hold a real callback already selected by the old executor's cancel thread.
                // It can finish after execute() clears the queue and a new EXECUTE starts.
                super.registerQueryCancellationCallback(() -> {
                    callbackEntered.countDown();
                    try {
                        Assertions.assertTrue(releaseCallback.await(10, TimeUnit.SECONDS));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new AssertionError(e);
                    }
                    callback.run();
                });
            }
        };
        ExecutorService thread = Executors.newSingleThreadExecutor();
        FutureTask<Void> lateCancel = new FutureTask<>(() -> {
            try {
                first.cancel(new Status(TStatusCode.CANCELLED, "late old cancellation"));
            } finally {
                ConnectContext.remove();
            }
            return null;
        });
        try {
            Mockito.when(firstCoordinator.join(Mockito.anyInt())).thenAnswer(invocation -> {
                thread.submit(lateCancel);
                Assertions.assertTrue(callbackEntered.await(10, TimeUnit.SECONDS));
                return true;
            });
            execute(prepared, first);
            Assertions.assertEquals(MysqlStateType.OK, connectContext.getState().getStateType());
            Mockito.verify(firstCoordinator).close();
            Assertions.assertFalse(lateCancel.isDone());
            StmtExecutor next = Mockito.spy(new StmtExecutor(connectContext, "EXECUTE tvf"));
            Mockito.doAnswer(invocation -> {
                invocation.callRealMethod();
                releaseCallback.countDown();
                lateCancel.get(10, TimeUnit.SECONDS);
                Assertions.assertNull(next.getPendingCancelReason());
                return null;
            }).when(next).checkBlockRules();

            execute(prepared, next);

            assertSuccessful(next, nextCoordinator);
            Mockito.verify(firstCoordinator, Mockito.atLeastOnce()).cancel(Mockito.any(Status.class));
            Mockito.verify(filesystem, Mockito.times(2)).delete(Mockito.any(Location.class), Mockito.eq(true));
        } finally {
            releaseCallback.countDown();
            thread.shutdownNow();
            Assertions.assertTrue(thread.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    @Test
    public void alreadyCanceledCurrentExecutorDoesNotPlanDeleteOrDispatch() throws Exception {
        PreparedStatementContext prepared = prepare();
        StmtExecutor current = new StmtExecutor(connectContext, "EXECUTE tvf");
        Status reason = new Status(TStatusCode.CANCELLED, "canceled before EXECUTE");
        current.cancel(reason);
        try (MockedConstruction<NereidsPlanner> planners = Mockito.mockConstruction(NereidsPlanner.class)) {
            execute(prepared, current);
            Assertions.assertEquals(MysqlStateType.ERR, connectContext.getState().getStateType());
            Assertions.assertTrue(connectContext.getState().getErrorMessage().contains("canceled before EXECUTE"),
                    connectContext.getState().getErrorMessage());
            Assertions.assertTrue(planners.constructed().isEmpty(), "canceled EXECUTE must not plan");
        }
        Assertions.assertSame(reason, current.getPendingCancelReason());
        Assertions.assertSame(prepared.command.getLogicalPlan(),
                ((LogicalPlanAdapter) current.getParsedStmt()).getLogicalPlan());
        Mockito.verify(factory, Mockito.never()).createCoordinator(Mockito.any(), Mockito.any(), Mockito.any());
        Mockito.verifyNoInteractions(filesystem, firstCoordinator, nextCoordinator);
    }
}
