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

import org.apache.doris.catalog.EnvFactory;
import org.apache.doris.common.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.datasource.storage.StorageAdapter;
import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.Location;
import org.apache.doris.fs.FileSystemFactory;
import org.apache.doris.mysql.MysqlChannel;
import org.apache.doris.mysql.MysqlCommand;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.Coordinator;
import org.apache.doris.qe.QeProcessorImpl;
import org.apache.doris.qe.QueryState.MysqlStateType;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class InsertIntoTVFDeletionCancellationTest extends TestWithFeService {
    private static final String SQL = "INSERT INTO s3("
            + "\"file_path\"=\"s3://unit-test/result_\",\"format\"=\"csv\","
            + "\"delete_existing_files\"=\"true\","
            + "\"s3.endpoint\"=\"http://127.0.0.1:1\",\"s3.region\"=\"us-east-1\","
            + "\"s3.access_key\"=\"unit-test-access\",\"s3.secret_key\"=\"unit-test-secret\") "
            + "SELECT k FROM deletion_cancellation_test.src";
    private ConnectContext context;
    private StmtExecutor executor;
    private InsertIntoTVFCommand command;
    private MysqlChannel channel;
    private FileSystem filesystem;
    private RecordingCoordinator coordinator;
    private Thread runThread;
    private volatile Thread cancelThread;
    private CountDownLatch entered;
    private CountDownLatch release;
    private CountDownLatch firstCancel;
    private final List<Thread> filesystemCloses = new CopyOnWriteArrayList<>();
    private final List<Thread> unregisters = new CopyOnWriteArrayList<>();
    private final List<String> events = new CopyOnWriteArrayList<>();
    private ExecutorService runner;
    private ExecutorService checker;
    private Boundary boundary;
    private boolean deleteFails;
    private boolean cancelBeforeDeletion;

    private enum Boundary { FACTORY, DELETE, CLOSE }

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("deletion_cancellation_test");
        createTable("CREATE TABLE deletion_cancellation_test.src(k INT) DUPLICATE KEY(k) "
                + "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES(\"replication_num\"=\"1\")");
    }

    @Override
    protected void runBeforeEach() throws Exception {
        context = Mockito.spy(createDefaultCtx());
        channel = Mockito.mock(MysqlChannel.class);
        Mockito.doReturn(channel).when(context).getMysqlChannel();
        context.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION,OLAP_SCAN_TABLET_PRUNE");
        context.getSessionVariable().setQueryTimeoutS(60);
        createStatementCtx(SQL, context);
        command = (InsertIntoTVFCommand) new NereidsParser().parseSingle(SQL);
        executor = new StmtExecutor(context, new LogicalPlanAdapter(command, context.getStatementContext()));
        context.setExecutor(executor);
        context.setCommand(MysqlCommand.COM_QUERY);
        context.setStartTime();
        filesystem = Mockito.mock(FileSystem.class);
        entered = new CountDownLatch(1);
        release = new CountDownLatch(1);
        firstCancel = new CountDownLatch(1);
        filesystemCloses.clear();
        unregisters.clear();
        events.clear();
        coordinator = null;
        boundary = Boundary.DELETE;
        deleteFails = false;
        cancelBeforeDeletion = false;
        runner = Executors.newSingleThreadExecutor();
        checker = Executors.newSingleThreadExecutor();
        Mockito.doAnswer(call -> {
            Assertions.assertEquals("s3://unit-test/", call.<Location>getArgument(0).toString());
            events.add("delete");
            blockAt(Boundary.DELETE);
            if (deleteFails) {
                throw new IOException("remote deletion failed");
            }
            return null;
        }).when(filesystem).delete(Mockito.any(Location.class), Mockito.eq(true));
        Mockito.doAnswer(call -> {
            filesystemCloses.add(Thread.currentThread());
            blockAt(Boundary.CLOSE);
            events.add("closed");
            return null;
        }).when(filesystem).close();
    }

    @AfterEach
    public void releaseResources() throws Exception {
        release.countDown();
        runner.shutdown();
        checker.shutdown();
        try {
            Assertions.assertTrue(runner.awaitTermination(10, TimeUnit.SECONDS), "run worker terminated");
            Assertions.assertTrue(checker.awaitTermination(10, TimeUnit.SECONDS), "cancel worker terminated");
        } finally {
            runner.shutdownNow();
            checker.shutdownNow();
            context.getStatementContext().close();
            connectContext.setThreadLocalInfo();
        }
    }

    @Test
    public void timeoutDuringDeleteKeepsCheckerResponsive() throws Exception {
        runCancellation(true);
    }

    @Test
    public void cancelWithoutWaitDuringDelete() throws Exception {
        runCancellation(false);
    }

    @Test
    public void deletionFailureAfterTimeoutPreservesReason() throws Exception {
        deleteFails = true;
        runCancellation(true);
    }

    @Test
    public void deletionFailureAfterCancelPreservesReason() throws Exception {
        deleteFails = true;
        runCancellation(false);
    }

    @Test
    public void cancellationDuringFilesystemCreation() throws Exception {
        boundary = Boundary.FACTORY;
        runCancellation(true);
    }

    @Test
    public void cancellationDuringFilesystemClose() throws Exception {
        boundary = Boundary.CLOSE;
        runCancellation(false);
    }

    @Test
    public void preCanceledExecutorDoesNotDelete() {
        Status reason = reason(false);
        executor.cancel(reason, false);
        try (MockedStatic<FileSystemFactory> filesystems = Mockito.mockStatic(FileSystemFactory.class)) {
            UserException failure = Assertions.assertThrows(UserException.class, () -> command.run(context, executor));
            Assertions.assertEquals(reason.getErrorMsg(), failure.getDetailMessage());
            Assertions.assertNull(executor.getCoord());
            filesystems.verifyNoInteractions();
        }
    }

    @Test
    public void cancellationBeforeDeletionDoesNotOpenFilesystem() throws Exception {
        cancelBeforeDeletion = true;
        startCommand().get(30, TimeUnit.SECONDS);
        Assertions.assertTrue(events.isEmpty());
        Mockito.verifyNoInteractions(filesystem);
        assertCanceled(reason(false));
        assertCoordinatorCleanup();
    }

    @Test
    public void deletesAndClosesBeforeDispatch() throws Exception {
        release.countDown();
        startCommand().get(30, TimeUnit.SECONDS);
        Assertions.assertEquals(List.of("factory", "delete", "closed", "exec"), events);
        Assertions.assertEquals(MysqlStateType.OK, context.getState().getStateType());
        Mockito.verify(channel).reset();
        Mockito.verify(coordinator).exec();
        assertFilesystemCleanup();
        assertCoordinatorCleanup();
    }

    @Test
    public void deletionFailureWithoutCancellationPreventsDispatch() throws Exception {
        deleteFails = true;
        release.countDown();
        startCommand().get(30, TimeUnit.SECONDS);
        Assertions.assertNull(executor.getPendingCancelReason());
        Assertions.assertEquals(MysqlStateType.ERR, context.getState().getStateType());
        Assertions.assertEquals("errCode = 2, detailMessage = Failed to delete existing files in "
                        + "s3://unit-test/: remote deletion failed",
                context.getState().getErrorMessage());
        Assertions.assertEquals(TStatusCode.INTERNAL_ERROR, coordinator.getExecStatus().getErrorCode());
        Mockito.verify(channel, Mockito.never()).reset();
        Mockito.verify(coordinator, Mockito.never()).exec();
        assertFilesystemCleanup();
        assertCoordinatorCleanup();
    }

    private void runCancellation(boolean timeout) throws Exception {
        Future<?> run = startCommand();
        Future<?> cancel = null;
        Future<?> probe = null;
        Status expected = reason(timeout);
        try {
            Assertions.assertTrue(entered.await(30, TimeUnit.SECONDS), "entered remote " + boundary);
            Assertions.assertSame(coordinator, QeProcessorImpl.INSTANCE.getCoordinator(context.queryId()));
            cancel = checker.submit(() -> {
                cancelThread = Thread.currentThread();
                if (timeout) {
                    context.checkTimeout(context.getStartTime() + context.getExecTimeoutS() * 1000L + 1);
                } else {
                    executor.cancel(expected, false);
                }
            });
            probe = checker.submit(() -> Assertions.assertSame(cancelThread, Thread.currentThread()));
            Assertions.assertTrue(firstCancel.await(10, TimeUnit.SECONDS), "real coordinator cancellation reached");
            try {
                cancel.get(2, TimeUnit.SECONDS);
            } catch (TimeoutException e) {
                Assertions.assertEquals(Thread.State.BLOCKED, cancelThread.getState());
                StackTraceElement[] stack = cancelThread.getStackTrace();
                Assertions.assertTrue(Arrays.stream(stack).anyMatch(frame -> frame.getClassName()
                        .endsWith("InsertIntoTVFCommand$ExecutionState") && frame.getMethodName().equals("cancel")));
                Assertions.assertFalse(probe.isDone(), "single checker probe waits behind cancellation");
                Assertions.fail("cancellation blocked by remote " + boundary + ": " + expected.getErrorCode()
                        + "\nBlocked cancellation stack: " + Arrays.toString(stack), e);
            }
            probe.get(2, TimeUnit.SECONDS);
            Assertions.assertEquals(1, release.getCount(), "remote boundary remains held");
            Assertions.assertFalse(run.isDone(), "execution thread still owns remote IO");
            Assertions.assertTrue(unregisters.isEmpty());
            Assertions.assertTrue(coordinator.closes.isEmpty());
            Assertions.assertEquals(boundary == Boundary.CLOSE ? 1 : 0, filesystemCloses.size());
            Mockito.verify(channel, Mockito.never()).reset();
            Mockito.verify(coordinator, Mockito.never()).exec();
            Assertions.assertEquals(expected.getErrorCode(), executor.getPendingCancelReason().getErrorCode());
            Assertions.assertEquals(expected.getErrorMsg(), executor.getPendingCancelReason().getErrorMsg());
        } finally {
            release.countDown();
            // Always drain both workers, including on the expected baseline assertion failure.
            try {
                run.get(10, TimeUnit.SECONDS);
            } finally {
                if (cancel != null) {
                    cancel.get(10, TimeUnit.SECONDS);
                }
                if (probe != null) {
                    probe.get(10, TimeUnit.SECONDS);
                }
            }
        }
        assertCanceled(expected);
        assertFilesystemCleanup();
        assertCoordinatorCleanup();
    }

    private void blockAt(Boundary current) throws InterruptedException {
        if (boundary == current) {
            entered.countDown();
            Assertions.assertTrue(release.await(40, TimeUnit.SECONDS), "remote boundary released by test owner");
        }
    }

    private Future<?> startCommand() {
        return runner.submit(() -> {
            runThread = Thread.currentThread();
            context.setThreadLocalInfo();
            EnvFactory factory = Mockito.spy(EnvFactory.getInstance());
            try (MockedStatic<EnvFactory> factories = Mockito.mockStatic(EnvFactory.class);
                    MockedStatic<FileSystemFactory> filesystems = Mockito.mockStatic(FileSystemFactory.class)) {
                factories.when(EnvFactory::getInstance).thenReturn(factory);
                filesystems.when(() -> FileSystemFactory.getFileSystem(Mockito.any(StorageAdapter.class)))
                        .thenAnswer(call -> {
                            events.add("factory");
                            blockAt(Boundary.FACTORY);
                            return filesystem;
                        });
                Mockito.doAnswer(call -> {
                    coordinator = Mockito.spy(new RecordingCoordinator(call.getArgument(1)));
                    // Only fragment transport/wait are replaced; cancellation and status updates remain real.
                    Mockito.doAnswer(dispatch -> {
                        events.add("exec");
                        return null;
                    }).when(coordinator).exec();
                    Mockito.doReturn(true).when(coordinator).join(Mockito.anyInt());
                    Mockito.doReturn(true).when(coordinator).isDone();
                    QeProcessorImpl.INSTANCE.registerQueryFinishCallback(DebugUtil.printId(context.queryId()),
                            () -> unregisters.add(Thread.currentThread()));
                    if (cancelBeforeDeletion) {
                        executor.cancel(reason(false), false);
                    }
                    return coordinator;
                }).when(factory).createCoordinator(Mockito.eq(context), Mockito.any(), Mockito.any());
                command.run(context, executor);
            } finally {
                ConnectContext.remove();
            }
            return null;
        });
    }

    private Status reason(boolean timeout) {
        return timeout ? new Status(TStatusCode.TIMEOUT, "query is timeout, killed by timeout checker")
                : new Status(TStatusCode.CANCELLED, "cancel without waiting for remote deletion");
    }

    private void assertCanceled(Status expected) throws Exception {
        Assertions.assertEquals(MysqlStateType.ERR, context.getState().getStateType());
        Assertions.assertEquals(expected.getErrorMsg(), context.getState().getErrorMessage());
        Assertions.assertEquals(expected.getErrorCode(), coordinator.getExecStatus().getErrorCode());
        Assertions.assertEquals(expected.getErrorMsg(), coordinator.getExecStatus().getErrorMsg());
        Assertions.assertFalse(coordinator.reasons.isEmpty());
        for (Status delivered : coordinator.reasons) {
            Assertions.assertEquals(expected.getErrorCode(), delivered.getErrorCode());
            Assertions.assertEquals(expected.getErrorMsg(), delivered.getErrorMsg());
        }
        Mockito.verify(channel, Mockito.never()).reset();
        Mockito.verify(coordinator, Mockito.never()).exec();
    }

    private void assertFilesystemCleanup() throws Exception {
        Mockito.verify(filesystem).delete(Mockito.any(Location.class), Mockito.eq(true));
        Mockito.verify(filesystem).close();
        Assertions.assertEquals(List.of(runThread), filesystemCloses);
    }

    private void assertCoordinatorCleanup() {
        Assertions.assertEquals(List.of(runThread), unregisters);
        Assertions.assertEquals(List.of(runThread), coordinator.closes);
        Mockito.verify(coordinator).close();
        Assertions.assertNull(QeProcessorImpl.INSTANCE.getCoordinator(context.queryId()));
    }

    private class RecordingCoordinator extends Coordinator {
        private final List<Status> reasons = new CopyOnWriteArrayList<>();
        private final List<Thread> closes = new CopyOnWriteArrayList<>();

        private RecordingCoordinator(NereidsPlanner planner) {
            super(context, planner, null);
        }

        @Override
        public void cancel(Status reason) {
            reasons.add(new Status(reason));
            super.cancel(reason);
            firstCancel.countDown();
        }

        @Override
        public void close() {
            Assertions.assertNull(QeProcessorImpl.INSTANCE.getCoordinator(context.queryId()));
            closes.add(Thread.currentThread());
            super.close();
        }
    }
}
