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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.storage.StorageAdapter;
import org.apache.doris.fs.FileSystemFactory;
import org.apache.doris.job.base.AbstractJob;
import org.apache.doris.job.base.JobExecuteType;
import org.apache.doris.job.extensions.insert.InsertJob;
import org.apache.doris.job.extensions.insert.streaming.StreamingJobProperties;
import org.apache.doris.job.offset.SourceOffsetProvider;
import org.apache.doris.job.offset.SourceOffsetProviderFactory;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.CreateJobCommand;
import org.apache.doris.nereids.trees.plans.commands.info.CreateJobInfo;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.Coordinator;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.QeProcessorImpl;
import org.apache.doris.qe.QueryState;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.task.LoadEtlTask;
import org.apache.doris.thrift.TQueryOptions;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class InsertIntoTVFJobTest extends TestWithFeService {
    private static final String INSERT = "INSERT INTO s3("
            + "\"file_path\"=\"s3://unit-test/result_\",\"format\"=\"csv\","
            + "\"s3.endpoint\"=\"http://127.0.0.1:1\",\"s3.region\"=\"us-east-1\","
            + "\"s3.access_key\"=\"unit-test-access\",\"s3.secret_key\"=\"unit-test-secret\") ";
    private static final String SELECT = "SELECT k FROM tvf_job_test.src";
    private final AtomicInteger coordinatorCreations = new AtomicInteger();
    private Coordinator coordinator;
    private StmtExecutor executor;
    private Runnable beforeCoordinatorReturn;
    private EnvFactory factory;
    private MockedStatic<EnvFactory> factoryMock;

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("tvf_job_test");
        connectContext.setDatabase("tvf_job_test");
        createTable("CREATE TABLE src(k INT) DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 "
                + "PROPERTIES(\"replication_num\"=\"1\")");
    }

    @Override
    protected void runBeforeEach() throws Exception {
        connectContext.setThreadLocalInfo();
        connectContext.setCurrentUserIdentity(UserIdentity.ROOT);
        connectContext.getState().reset();
        connectContext.getSessionVariable().setQueryTimeoutS(3);
        connectContext.setQueryId(new TUniqueId(1, System.nanoTime()));
        coordinatorCreations.set(0);
        beforeCoordinatorReturn = () -> { };
        coordinator = Mockito.mock(Coordinator.class);
        Mockito.when(coordinator.getQueryOptions()).thenReturn(new TQueryOptions());
        Mockito.when(coordinator.getExecStatus()).thenReturn(Status.OK);
        Mockito.when(coordinator.join(Mockito.anyInt())).thenReturn(true);
        Mockito.when(coordinator.isDone()).thenReturn(true);
        Mockito.when(coordinator.getLoadCounters()).thenReturn(Collections.singletonMap(LoadEtlTask.DPP_NORMAL_ALL, "2"));
        factory = Mockito.spy(EnvFactory.getInstance());
        Mockito.doAnswer(invocation -> {
            coordinatorCreations.incrementAndGet();
            beforeCoordinatorReturn.run();
            return coordinator;
        }).when(factory).createCoordinator(Mockito.any(), Mockito.any(), Mockito.any());
        factoryMock = Mockito.mockStatic(EnvFactory.class);
        factoryMock.when(EnvFactory::getInstance).thenReturn(factory);
    }

    @AfterEach
    public void closeFactoryMock() {
        factoryMock.close();
        ConnectContext.remove();
    }

    private LogicalPlan parse(String sql) {
        StatementContext statement = new StatementContext(connectContext, new OriginStatement(sql, 0));
        connectContext.setStatementContext(statement);
        LogicalPlan plan = new NereidsParser().parseSingle(sql);
        executor = new StmtExecutor(connectContext, new LogicalPlanAdapter(plan, statement));
        connectContext.setExecutor(executor);
        return plan;
    }

    private AbstractJob buildJob(String schedule, String insert) throws Exception {
        String clause = "STREAMING".equals(schedule) ? "STREAMING" : "SCHEDULE " + schedule;
        CreateJobCommand command = (CreateJobCommand) parse("CREATE JOB tvf_job ON " + clause + " DO " + insert);
        CreateJobInfo info = Deencapsulation.getField(command, "createJobInfo");
        return info.analyzeAndBuildJobInfo(connectContext);
    }

    private InsertIntoTVFCommand parseInsert() {
        return (InsertIntoTVFCommand) parse(INSERT + SELECT);
    }

    private void assertCleanedUp() {
        Mockito.verify(coordinator).close();
        Assertions.assertNull(QeProcessorImpl.INSTANCE.getCoordinator(connectContext.queryId()));
        Assertions.assertNull(Deencapsulation.getField(executor, "coord"));
    }

    @Test
    public void immediateS3ValidationHasNoSideEffects() throws Exception {
        try (MockedStatic<FileSystemFactory> filesystems = Mockito.mockStatic(
                FileSystemFactory.class, Mockito.CALLS_REAL_METHODS)) {
            filesystems.when(() -> FileSystemFactory.getFileSystem(Mockito.any(StorageAdapter.class)))
                    .thenThrow(new AssertionError("CREATE JOB must not access storage"));
            String insert = INSERT.replace("\"format\"=\"csv\"",
                    "\"format\"=\"csv\",\"delete_existing_files\"=\"true\"");
            AbstractJob job = buildJob("AT CURRENT_TIMESTAMP", insert + SELECT);
            Assertions.assertInstanceOf(InsertJob.class, job);
            Assertions.assertEquals(JobExecuteType.ONE_TIME, job.getJobConfig().getExecuteType());
            Assertions.assertTrue(job.getJobConfig().isImmediate());
            Assertions.assertEquals(0, coordinatorCreations.get());
            Mockito.verifyNoInteractions(coordinator);
            Assertions.assertFalse(job.getTvfInfo().toString().contains("unit-test-secret"));
            AbstractJob restored = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(job), AbstractJob.class);
            Assertions.assertFalse(restored.getTvfInfo().toString().contains("unit-test-secret"));
            Assertions.assertFalse(restored.getCommonShowInfo().toString().contains("unit-test-secret"));
        }
    }

    @Test
    public void rejectsUnsupportedSchedulesAtCreation() {
        for (String schedule : new String[] {"EVERY 1 MINUTE", "AT '2099-01-01 00:00:00'", "STREAMING"}) {
            AnalysisException error = Assertions.assertThrows(AnalysisException.class,
                    () -> buildJob(schedule, INSERT + SELECT));
            Assertions.assertTrue(error.getMessage().contains("CURRENT_TIMESTAMP"), error.getMessage());
        }
        Assertions.assertEquals(0, coordinatorCreations.get());
    }

    @Test
    public void rejectsLocalAndHdfsAtCreation() {
        for (String target : new String[] {"local", "hdfs"}) {
            AnalysisException error = Assertions.assertThrows(AnalysisException.class,
                    () -> buildJob("AT CURRENT_TIMESTAMP", INSERT.replace("s3(", target + "(") + SELECT));
            Assertions.assertTrue(error.getMessage().contains("only supports S3 TVF"), error.getMessage());
        }
    }

    @Test
    public void rejectsInvalidPropertiesAndQueryAtCreation() {
        Assertions.assertThrows(AnalysisException.class, () -> buildJob("AT CURRENT_TIMESTAMP",
                INSERT.replace("\"format\"=\"csv\",", "") + SELECT));
        Assertions.assertThrows(AnalysisException.class, () -> buildJob("AT CURRENT_TIMESTAMP",
                INSERT + "SELECT missing_column FROM tvf_job_test.src"));
        Assertions.assertEquals(0, coordinatorCreations.get());
    }

    @Test
    public void requiresAdminAndMasksAuditCredentials() {
        CreateJobCommand command = (CreateJobCommand) parse(
                "CREATE JOB tvf_job ON SCHEDULE AT CURRENT_TIMESTAMP DO " + INSERT + SELECT);
        String encrypted = command.geneEncryptionSQL(
                "CREATE JOB tvf_job ON SCHEDULE AT CURRENT_TIMESTAMP DO " + INSERT + SELECT);
        Assertions.assertFalse(encrypted.contains("unit-test-secret"), encrypted);
        connectContext.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp("no_job_priv", "%"));
        CreateJobInfo info = Deencapsulation.getField(command, "createJobInfo");
        Assertions.assertThrows(AnalysisException.class, () -> info.analyzeAndBuildJobInfo(connectContext));
    }

    @Test
    public void validatesJoinAggregationAndSubquery() throws Exception {
        buildJob("AT CURRENT_TIMESTAMP", INSERT + "SELECT a.k, count(*) FROM tvf_job_test.src a "
                + "JOIN (SELECT k FROM tvf_job_test.src) b ON a.k=b.k GROUP BY a.k");
        Assertions.assertEquals(0, coordinatorCreations.get());
    }

    @Test
    public void preservesTableJobSchedules() throws Exception {
        String tableInsert = "INSERT INTO tvf_job_test.src SELECT 1";
        Assertions.assertEquals(JobExecuteType.RECURRING,
                buildJob("EVERY 1 MINUTE", tableInsert).getJobConfig().getExecuteType());
        Assertions.assertEquals(JobExecuteType.ONE_TIME,
                buildJob("AT '2099-01-01 00:00:00'", tableInsert).getJobConfig().getExecuteType());
        // This schedule-only compatibility check must not enumerate remote S3 files.
        String source = INSERT.substring("INSERT INTO ".length())
                .replace("\"file_path\"", "\"uri\"")
                .replace("\"format\"=\"csv\"", "\"format\"=\"csv\",\"csv_schema\"=\"k:int\"");
        boolean runningUnitTest = FeConstants.runningUnitTest;
        try (MockedStatic<SourceOffsetProviderFactory> providers = Mockito.mockStatic(SourceOffsetProviderFactory.class)) {
            FeConstants.runningUnitTest = true;
            providers.when(() -> SourceOffsetProviderFactory.createSourceOffsetProvider(
                    Mockito.eq("s3"), Mockito.any(StreamingJobProperties.class)))
                    .thenReturn(Mockito.mock(SourceOffsetProvider.class));
            Assertions.assertEquals(JobExecuteType.STREAMING,
                    buildJob("STREAMING", "INSERT INTO tvf_job_test.src SELECT * FROM " + source)
                            .getJobConfig().getExecuteType());
        } finally {
            FeConstants.runningUnitTest = runningUnitTest;
        }
    }

    @Test
    public void successfulExecutionWaitsAndCleansUp() throws Exception {
        parseInsert().run(connectContext, executor);
        Assertions.assertEquals(QueryState.MysqlStateType.OK, connectContext.getState().getStateType());
        Mockito.verify(coordinator).exec();
        Mockito.verify(coordinator).join(3);
        Assertions.assertSame(coordinator, Deencapsulation.getField(executor, "externalDmlAuditCoordinator"));
        assertCleanedUp();
    }

    @Test
    public void joinFalseIsTimeoutEvenWithOkStatus() throws Exception {
        Mockito.when(coordinator.join(Mockito.anyInt())).thenReturn(false);
        Mockito.when(coordinator.isDone()).thenReturn(false);
        parseInsert().run(connectContext, executor);
        Assertions.assertEquals(QueryState.MysqlStateType.ERR, connectContext.getState().getStateType());
        Assertions.assertTrue(connectContext.getState().getErrorMessage().contains("timeout"));
        Mockito.verify(coordinator).cancel(Mockito.any(Status.class));
        assertCleanedUp();
    }

    @Test
    public void backendFailureIsNotReportedAsSuccessOrTimeout() throws Exception {
        Mockito.when(coordinator.isDone()).thenReturn(false);
        Mockito.when(coordinator.getExecStatus()).thenReturn(new Status(TStatusCode.INTERNAL_ERROR, "backend is down"));
        parseInsert().run(connectContext, executor);
        Assertions.assertEquals("backend is down", connectContext.getState().getErrorMessage());
        assertCleanedUp();
    }

    @Test
    public void uploadFailureIsNotSuccess() throws Exception {
        Mockito.when(coordinator.getExecStatus()).thenReturn(new Status(TStatusCode.INTERNAL_ERROR, "S3 AccessDenied"));
        parseInsert().run(connectContext, executor);
        Assertions.assertEquals(QueryState.MysqlStateType.ERR, connectContext.getState().getStateType());
        Assertions.assertEquals("S3 AccessDenied", connectContext.getState().getErrorMessage());
        assertCleanedUp();
    }

    @Test
    public void dispatchExceptionCancelsAndCleansUp() throws Exception {
        Mockito.doThrow(new UserException("dispatch failed")).when(coordinator).exec();
        parseInsert().run(connectContext, executor);
        Assertions.assertTrue(connectContext.getState().getErrorMessage().contains("dispatch failed"));
        Mockito.verify(coordinator).cancel(Mockito.any(Status.class));
        assertCleanedUp();
    }

    @Test
    public void cancellationBeforePlanningNeverCreatesCoordinator() {
        InsertIntoTVFCommand command = parseInsert();
        executor.cancel(new Status(TStatusCode.CANCELLED, "user canceled"));
        Assertions.assertThrows(UserException.class, () -> command.run(connectContext, executor));
        Assertions.assertEquals(0, coordinatorCreations.get());
    }

    @Test
    public void cancellationDuringPlanningPreventsCoordinatorCreation() throws Exception {
        InsertIntoTVFCommand command = parseInsert();
        executor = Mockito.spy(executor);
        connectContext.setExecutor(executor);
        Mockito.doAnswer(invocation -> {
            executor.cancel(new Status(TStatusCode.CANCELLED, "user canceled while planning"));
            return null;
        }).when(executor).checkBlockRules();
        Assertions.assertThrows(UserException.class, () -> command.run(connectContext, executor));
        Assertions.assertEquals(0, coordinatorCreations.get());
        Mockito.verifyNoInteractions(coordinator);
    }

    @Test
    public void cancellationWhileWaitingCannotReportSuccess() throws Exception {
        Mockito.when(coordinator.join(Mockito.anyInt())).thenAnswer(call -> {
            executor.cancel(new Status(TStatusCode.CANCELLED, "user canceled while waiting"));
            return true;
        });
        parseInsert().run(connectContext, executor);
        Assertions.assertEquals(QueryState.MysqlStateType.ERR, connectContext.getState().getStateType());
        Assertions.assertTrue(connectContext.getState().getErrorMessage().contains("canceled"));
        assertCleanedUp();
    }

    @Test
    public void cancellationBeforeCoordinatorInstallationPreventsDispatch() throws Exception {
        InsertIntoTVFCommand command = parseInsert();
        beforeCoordinatorReturn = () -> executor.cancel(new Status(TStatusCode.CANCELLED, "user canceled"));
        command.run(connectContext, executor);
        Mockito.verify(coordinator, Mockito.never()).exec();
        Assertions.assertEquals(QueryState.MysqlStateType.ERR, connectContext.getState().getStateType());
        assertCleanedUp();
    }

    @Test
    public void cancellationDuringDispatchCancelsNewlyStartedFragments() throws Exception {
        CountDownLatch enteredDispatch = new CountDownLatch(1);
        CountDownLatch allowDispatch = new CountDownLatch(1);
        CountDownLatch firstCancel = new CountDownLatch(1);
        Mockito.doAnswer(call -> {
            enteredDispatch.countDown();
            Assertions.assertTrue(allowDispatch.await(10, TimeUnit.SECONDS));
            return null;
        }).when(coordinator).exec();
        Mockito.doAnswer(call -> {
            firstCancel.countDown();
            return null;
        }).when(coordinator).cancel(Mockito.any(Status.class));
        InsertIntoTVFCommand command = parseInsert();
        ExecutorService threads = Executors.newFixedThreadPool(2);
        try {
            Future<?> run = threads.submit(() -> {
                connectContext.setThreadLocalInfo();
                try (MockedStatic<EnvFactory> workerFactory = Mockito.mockStatic(EnvFactory.class)) {
                    workerFactory.when(EnvFactory::getInstance).thenReturn(factory);
                    command.run(connectContext, executor);
                } finally {
                    ConnectContext.remove();
                }
                return null;
            });
            Assertions.assertTrue(enteredDispatch.await(10, TimeUnit.SECONDS));
            Future<?> cancel = threads.submit(() -> executor.cancel(new Status(TStatusCode.CANCELLED, "user canceled")));
            Assertions.assertTrue(firstCancel.await(10, TimeUnit.SECONDS));
            Assertions.assertFalse(cancel.isDone(), "cancellation must synchronize with fragment startup");
            allowDispatch.countDown();
            cancel.get(10, TimeUnit.SECONDS);
            run.get(10, TimeUnit.SECONDS);
            Assertions.assertEquals(QueryState.MysqlStateType.ERR, connectContext.getState().getStateType());
            Mockito.verify(coordinator, Mockito.atLeast(2)).cancel(Mockito.any(Status.class));
            assertCleanedUp();
        } finally {
            allowDispatch.countDown();
            threads.shutdownNow();
        }
    }
}
