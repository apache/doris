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
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.job.base.AbstractJob;
import org.apache.doris.job.base.JobExecuteType;
import org.apache.doris.job.base.JobExecutionConfiguration;
import org.apache.doris.job.common.JobStatus;
import org.apache.doris.job.common.TaskStatus;
import org.apache.doris.job.common.TaskType;
import org.apache.doris.job.manager.JobManager;
import org.apache.doris.load.loadv2.LoadStatistic;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTVFCommand;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.persist.EditLog;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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
        // Construction mocks are thread-local: install on the same worker that runs before().
        try (MockedConstruction<NereidsParser> parsers = Mockito.mockConstruction(NereidsParser.class,
                Mockito.withSettings().defaultAnswer(Mockito.CALLS_REAL_METHODS),
                (parser, creation) -> Mockito.doReturn(tvfCommand).when(parser).parseSingle(SQL))) {
            task.runTask();
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
        Assertions.assertNull(ConnectContext.get());
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
    public void tableInsertKeepsRunWithUpdateInfoAndLabel() throws Exception {
        String sql = "INSERT INTO example_table SELECT 1";
        Deencapsulation.setField(task, "sql", sql);
        AtomicInteger tableRuns = new AtomicInteger();
        InsertIntoTableCommand command = Mockito.spy((InsertIntoTableCommand) new NereidsParser().parseSingle(sql));
        Mockito.doAnswer(invocation -> {
            tableRuns.incrementAndGet();
            ((ConnectContext) invocation.getArgument(0)).getState().setOk();
            return null;
        }).when(command).runWithUpdateInfo(Mockito.any(ConnectContext.class), Mockito.any(StmtExecutor.class),
                Mockito.nullable(LoadStatistic.class));
        try (MockedConstruction<NereidsParser> parsers = Mockito.mockConstruction(NereidsParser.class,
                (parser, creation) -> Mockito.when(parser.parseSingle(sql)).thenReturn(command))) {
            task.before();
            Assertions.assertSame(command, Deencapsulation.getField(task, "command"));
            Assertions.assertEquals(job.getJobId() + "_" + task.getTaskId(), command.getLabelName().get());
            task.run();
            task.onSuccess();
        }
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
}
