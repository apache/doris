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

package org.apache.doris.job.extensions.insert;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.connector.ConnectorSessionBuilder;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorStatementScope;
import org.apache.doris.connector.spi.ConnectorTaskScheduler;
import org.apache.doris.datasource.split.SplitAssignment;
import org.apache.doris.job.base.AbstractJob;
import org.apache.doris.job.base.JobExecuteType;
import org.apache.doris.job.base.JobExecutionConfiguration;
import org.apache.doris.job.common.JobStatus;
import org.apache.doris.job.common.TaskStatus;
import org.apache.doris.job.common.TaskType;
import org.apache.doris.job.executor.TaskProcessor;
import org.apache.doris.job.manager.JobManager;
import org.apache.doris.load.loadv2.LoadStatistic;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTVFCommand;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.persist.EditLog;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QeProcessorImpl;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class InsertTaskLifecycleTest extends TestWithFeService {
    private static final String SQL = "INSERT INTO s3(\"file_path\"=\"s3://unit-test/result_\","
            + "\"format\"=\"csv\") SELECT 1";
    private final List<String> persisted = new ArrayList<>();
    private InsertJob job;
    private InsertTask task;
    private Execution execution;
    private Object originalJobManager;
    private EditLog originalEditLog;
    private InsertIntoTVFCommand tvfCommand;

    @FunctionalInterface
    private interface Execution {
        void run(ConnectContext ctx) throws Exception;
    }

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("task_lifecycle_test");
    }

    @Override
    protected void runBeforeEach() throws Exception {
        connectContext.setThreadLocalInfo();
        connectContext.setDatabase("task_lifecycle_test");
        connectContext.setCurrentUserIdentity(UserIdentity.ROOT);
        JobExecutionConfiguration config = new JobExecutionConfiguration();
        config.setExecuteType(JobExecuteType.ONE_TIME);
        config.setImmediate(true);
        job = new InsertJob("task_lifecycle_job", JobStatus.RUNNING, "task_lifecycle_test", "",
                UserIdentity.ROOT, config, System.currentTimeMillis(), SQL);
        JobManager manager = Mockito.mock(JobManager.class);
        Mockito.when(manager.getJob(Mockito.anyLong())).thenReturn(job);
        Mockito.when(manager.getJob(Mockito.any(Long.class))).thenReturn(job);
        originalJobManager = Env.getCurrentEnv().getJobManager();
        originalEditLog = Env.getCurrentEnv().getEditLog();
        Deencapsulation.setField(Env.getCurrentEnv(), "jobManager", manager);
        EditLog editLog = Mockito.mock(EditLog.class);
        Mockito.doAnswer(invocation -> {
            AbstractJob updated = invocation.getArgument(0);
            persisted.add(GsonUtils.GSON.toJson(updated));
            return null;
        }).when(editLog).logUpdateJob(Mockito.any(AbstractJob.class));
        Env.getCurrentEnv().setEditLog(editLog);
        execution = ctx -> ctx.getState().setOk();
        tvfCommand = Mockito.spy((InsertIntoTVFCommand) new NereidsParser().parseSingle(SQL));
        Mockito.doAnswer(invocation -> {
            execution.run(invocation.getArgument(0));
            return null;
        }).when(tvfCommand).run(Mockito.any(ConnectContext.class), Mockito.any(StmtExecutor.class));
        persisted.clear();
        task = job.createTasks(TaskType.SCHEDULED, null).get(0);
        persisted.clear();
    }

    @AfterEach
    public void restoreEnvironment() {
        Deencapsulation.setField(Env.getCurrentEnv(), "jobManager", originalJobManager);
        Env.getCurrentEnv().setEditLog(originalEditLog);
        ConnectContext.remove();
    }

    private void runTaskWithCommand() throws Exception {
        runTaskWithCommand(SQL, tvfCommand);
    }

    private void runTaskWithCommand(String sql, Command command) throws Exception {
        AtomicReference<StatementContext> statement = new AtomicReference<>();
        AtomicReference<Thread> worker = new AtomicReference<>();
        AtomicReference<Thread> reusedThread = new AtomicReference<>();
        CompletableFuture<ConnectContext> reusedContext = new CompletableFuture<>();
        // Parser construction mocks must live on the actual TaskProcessor worker.
        TaskProcessor processor = new TaskProcessor(1, 2, runnable -> new Thread(() -> {
            worker.compareAndSet(null, Thread.currentThread());
            try (MockedConstruction<NereidsParser> parsers = Mockito.mockConstruction(NereidsParser.class,
                    Mockito.withSettings().defaultAnswer(Mockito.CALLS_REAL_METHODS),
                    (parser, creation) -> Mockito.doAnswer(invocation -> {
                        ConnectContext ctx = ConnectContext.get();
                        StatementContext spy = Mockito.spy(ctx.getStatementContext());
                        ctx.setStatementContext(spy);
                        statement.set(spy);
                        return sql.equals(invocation.getArgument(0)) ? command : invocation.callRealMethod();
                    }).when(parser).parseSingle(Mockito.anyString()))) {
                runnable.run();
            }
        }));
        InsertTask probe = new InsertTask("worker-reuse-probe", null, null, null) {
            @Override
            public void runTask() {
                reusedThread.set(Thread.currentThread());
                reusedContext.complete(ConnectContext.get());
            }
        };
        probe.setStatus(TaskStatus.PENDING);
        try {
            Assertions.assertTrue(processor.addTask(task));
            Assertions.assertTrue(processor.addTask(probe));
            Assertions.assertNull(reusedContext.get(10, TimeUnit.SECONDS));
            Assertions.assertSame(worker.get(), reusedThread.get());
            if (statement.get() != null) {
                Mockito.verify(statement.get()).close();
            }
        } finally {
            processor.shutdown();
        }
    }

    private InsertTask restoredTask() {
        Assertions.assertFalse(persisted.isEmpty());
        InsertJob restored = (InsertJob) GsonUtils.GSON.fromJson(persisted.get(persisted.size() - 1), AbstractJob.class);
        return restored.queryTasks().get(0);
    }

    @Test
    public void pendingCancellationIsPersistedAndNeverExecutes() throws Exception {
        AtomicInteger runs = new AtomicInteger();
        execution = ctx -> runs.incrementAndGet();
        job.cancelTaskById(task.getTaskId());
        runTaskWithCommand();
        Assertions.assertEquals(0, runs.get());
        Assertions.assertEquals(TaskStatus.CANCELED, task.getStatus());
        Assertions.assertEquals(JobStatus.FINISHED, job.getJobStatus());
        Assertions.assertNotNull(task.getFinishTimeMs());
        Assertions.assertEquals(1, job.getCanceledTaskCount().get());
        Assertions.assertEquals(0, job.getSucceedTaskCount().get());
        InsertTask restored = restoredTask();
        Assertions.assertEquals(TaskStatus.CANCELED, restored.getStatus());
        Assertions.assertEquals(task.getFinishTimeMs(), restored.getFinishTimeMs());
        Assertions.assertEquals("insert task canceled", restored.getErrMsg());
        Assertions.assertEquals("CANCELED", restored.getTvfInfo("task_lifecycle_job").getColumnValue().get(4).getStringVal());
    }

    @Test
    public void successIsPersistedAndCallbackIsNotRepeated() throws Exception {
        runTaskWithCommand();
        Assertions.assertEquals(TaskStatus.SUCCESS, task.getStatus());
        Assertions.assertEquals(JobStatus.FINISHED, job.getJobStatus());
        Assertions.assertEquals(1, job.getSucceedTaskCount().get());
        Assertions.assertNotNull(task.getFinishTimeMs());
        Assertions.assertEquals(TaskStatus.SUCCESS, restoredTask().getStatus());
        Assertions.assertFalse(task.onSuccess());
        Assertions.assertFalse(task.cancel(true));
        Assertions.assertEquals(1, job.getSucceedTaskCount().get());
        Assertions.assertSame(connectContext, ConnectContext.get());
    }

    @Test
    public void failureHasTimeErrorAndFinishedJobAndSurvivesSerialization() throws Exception {
        execution = ctx -> ctx.getState().setError(ErrorCode.ERR_UNKNOWN_ERROR, "S3 upload failed");
        runTaskWithCommand();
        Assertions.assertEquals(TaskStatus.FAILED, task.getStatus());
        Assertions.assertEquals(JobStatus.FINISHED, job.getJobStatus());
        Assertions.assertEquals(1, job.getFailedTaskCount().get());
        Assertions.assertNotNull(task.getFinishTimeMs());
        Assertions.assertTrue(restoredTask().getErrMsg().contains("S3 upload failed"));
        Assertions.assertEquals(TaskStatus.FAILED, restoredTask().getStatus());
        Assertions.assertFalse(task.onSuccess());
        Assertions.assertFalse(task.cancel(true));
    }

    private void cancelDuringExecution(boolean lateFailure) throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        execution = ctx -> {
            entered.countDown();
            Assertions.assertTrue(release.await(10, TimeUnit.SECONDS));
            if (lateFailure) {
                throw new UserException("late failure must not replace cancellation");
            }
            ctx.getState().setOk();
        };
        ExecutorService thread = Executors.newSingleThreadExecutor();
        try {
            Future<?> future = thread.submit(() -> {
                runTaskWithCommand();
                Assertions.assertNull(ConnectContext.get());
                return null;
            });
            Assertions.assertTrue(entered.await(10, TimeUnit.SECONDS));
            job.cancelTaskById(task.getTaskId());
            Assertions.assertSame(connectContext, ConnectContext.get(), "cancel must retain its caller's context");
            release.countDown();
            future.get(10, TimeUnit.SECONDS);
            Assertions.assertEquals(TaskStatus.CANCELED, task.getStatus());
            Assertions.assertEquals("insert task canceled", task.getErrMsg());
            Assertions.assertEquals(0, job.getSucceedTaskCount().get());
            Assertions.assertEquals(0, job.getFailedTaskCount().get());
            Assertions.assertEquals(1, job.getCanceledTaskCount().get());
            Assertions.assertEquals(TaskStatus.CANCELED, restoredTask().getStatus());
        } finally {
            release.countDown();
            thread.shutdownNow();
        }
    }

    @Test
    public void runningCancellationKeepsHandlesAliveAndIgnoresLateSuccess() throws Exception {
        cancelDuringExecution(false);
    }

    @Test
    public void lateFailureCannotOverwriteCancellationReason() throws Exception {
        cancelDuringExecution(true);
    }

    @Test
    public void arbitraryCommandsAreRejected() throws Exception {
        Deencapsulation.setField(task, "sql", "SELECT 1");
        runTaskWithCommand();
        Assertions.assertEquals(TaskStatus.FAILED, task.getStatus());
        Assertions.assertTrue(task.getErrMsg().contains("Unsupported INSERT JOB command"));
    }

    @Test
    public void initializationFailureRunsFinishCallbacksWithoutAnExecutor() throws Exception {
        AtomicInteger finishes = new AtomicInteger();
        AtomicInteger closes = new AtomicInteger();
        AtomicReference<ConnectorTaskScheduler> group = new AtomicReference<>();
        ScanNode scan = Mockito.mock(ScanNode.class);
        Mockito.doAnswer(invocation -> {
            ConnectContext ctx = ConnectContext.get();
            Assertions.assertNull(ctx.getExecutor());
            Assertions.assertSame(ctx, ctx.getStatementContext().getConnectContext());
            ConnectorSession session = ConnectorSessionBuilder.from(ctx).withCatalogId(1).build();
            session.getStatementScope().computeIfAbsent("initialization-resource",
                    () -> (AutoCloseable) closes::incrementAndGet);
            group.set(session.getTaskScheduler(ConnectorTaskScheduler.Stage.METADATA));
            ctx.getStatementContext().stopScanNodeAtClose(scan);
            QeProcessorImpl.INSTANCE.registerQueryFinishCallback(DebugUtil.printId(ctx.queryId()),
                    finishes::incrementAndGet);
            throw new AnalysisException("injected initialization failure");
        }).when(tvfCommand).validateJobTarget();
        runTaskWithCommand();
        Assertions.assertEquals(TaskStatus.FAILED, task.getStatus());
        Assertions.assertTrue(task.getErrMsg().contains("injected initialization failure"));
        Assertions.assertEquals(1, finishes.get());
        Assertions.assertEquals(1, closes.get());
        Assertions.assertTrue(group.get().isClosed());
        Mockito.verify(scan).stop();
    }

    @Test
    public void tableInsertKeepsRunWithUpdateInfoAndLabel() throws Exception {
        String sql = "INSERT INTO example_table SELECT 1";
        Deencapsulation.setField(task, "sql", sql);
        AtomicInteger tableRuns = new AtomicInteger();
        InsertIntoTableCommand command = Mockito.spy((InsertIntoTableCommand) new NereidsParser().parseSingle(sql));
        Mockito.doAnswer(invocation -> {
            tableRuns.incrementAndGet();
            ConnectContext ctx = invocation.getArgument(0);
            Assertions.assertSame(invocation.getArgument(1), ctx.getExecutor());
            ctx.getState().setOk();
            return null;
        }).when(command).runWithUpdateInfo(Mockito.any(ConnectContext.class), Mockito.any(StmtExecutor.class),
                Mockito.nullable(LoadStatistic.class));
        runTaskWithCommand(sql, command);
        Assertions.assertEquals(job.getJobId() + "_" + task.getTaskId(), command.getLabelName().get());
        Assertions.assertEquals(1, tableRuns.get());
        Assertions.assertEquals(TaskStatus.SUCCESS, task.getStatus());
    }

    @Test
    public void backgroundContextUsesFreshSessionTimeout() throws Exception {
        int defaultTimeout = new ConnectContext().getSessionVariable().getQueryTimeoutS();
        connectContext.getSessionVariable().setQueryTimeoutS(7);
        execution = ctx -> {
            Assertions.assertNotSame(connectContext, ctx);
            Assertions.assertEquals(defaultTimeout, ctx.getSessionVariable().getQueryTimeoutS());
            ctx.getState().setOk();
        };
        runTaskWithCommand();
        Assertions.assertEquals(TaskStatus.SUCCESS, task.getStatus());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void taskProcessorClosesQueryResourcesAfterSuccessAndFailure(boolean failure) throws Exception {
        AtomicReference<QueryResources> resources = new AtomicReference<>();
        execution = ctx -> {
            resources.set(new QueryResources(ctx));
            if (failure) {
                throw new UserException("planning failed before coordinator creation");
            }
            ctx.getState().setOk();
        };
        runTaskWithCommand();
        Assertions.assertEquals(failure ? TaskStatus.FAILED : TaskStatus.SUCCESS, task.getStatus());
        resources.get().assertClosed();
    }

    private static class QueryResources {
        private final ConnectContext ctx;
        private final StmtExecutor executor;
        private final SplitAssignment assignment = Mockito.mock(SplitAssignment.class);
        private final ScanNode scan = Mockito.mock(ScanNode.class);
        private final AtomicInteger unregisters = new AtomicInteger();
        private final AtomicInteger finishes = new AtomicInteger();
        private final AtomicInteger statementCloses = new AtomicInteger();
        private final ConnectorTaskScheduler group;
        private final ConnectorStatementScope scope;

        QueryResources(ConnectContext ctx) {
            this.ctx = ctx;
            executor = ctx.getExecutor();
            Assertions.assertNotNull(executor, "parsed executor must be bound before the command plans");
            executor.registerSplitAssignment(assignment, unregisters::incrementAndGet);
            executor.registerQueryCancellationCallback(() -> { });
            ctx.getStatementContext().stopScanNodeAtClose(scan);
            ConnectorSession session = ConnectorSessionBuilder.from(ctx).withCatalogId(1).build();
            scope = session.getStatementScope();
            scope.computeIfAbsent("statement-resource", () -> (AutoCloseable) statementCloses::incrementAndGet);
            scope.computeIfAbsentForPlanning("planning-value", Object::new);
            group = session.getTaskScheduler(ConnectorTaskScheduler.Stage.METADATA);
            QeProcessorImpl.INSTANCE.registerQueryFinishCallback(DebugUtil.printId(ctx.queryId()), finishes::incrementAndGet);
        }

        void assertClosed() {
            Mockito.verify(assignment).stop();
            Mockito.verify(scan).stop();
            Assertions.assertEquals(1, unregisters.get());
            Assertions.assertEquals(1, finishes.get());
            Assertions.assertEquals(1, statementCloses.get());
            Assertions.assertNull(scope.getIfPresent("planning-value"));
            Assertions.assertThrows(CancellationException.class,
                    () -> scope.computeIfAbsent("after-close", Object::new));
            Assertions.assertTrue(group.isClosed());
            List<?> callbacks = Deencapsulation.getField(executor, "queryCancelCallbacks");
            Assertions.assertTrue(callbacks.isEmpty());
            QeProcessorImpl.INSTANCE.unregisterQuery(ctx.queryId());
            Assertions.assertEquals(1, finishes.get(), "query finish callbacks run once");
        }
    }
}
