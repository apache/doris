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

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.EnvFactory;
import org.apache.doris.common.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.Coordinator;
import org.apache.doris.qe.QeProcessorImpl;
import org.apache.doris.qe.QueryState.MysqlStateType;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.resource.workloadgroup.QueryQueue;
import org.apache.doris.resource.workloadgroup.QueueToken;
import org.apache.doris.resource.workloadgroup.WorkloadGroup;
import org.apache.doris.resource.workloadgroup.WorkloadGroupMgr;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.utframe.MockedBackendFactory.DefaultPBackendServiceImpl;
import org.apache.doris.utframe.TestWithFeService;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.AdditionalAnswers;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class InsertIntoTVFCancellationReasonTest extends TestWithFeService {
    private static final String SQL = "INSERT INTO s3("
            + "\"file_path\"=\"s3://unit-test/result_\",\"format\"=\"csv\","
            + "\"s3.endpoint\"=\"http://127.0.0.1:1\",\"s3.region\"=\"us-east-1\","
            + "\"s3.access_key\"=\"unit-test-access\",\"s3.secret_key\"=\"unit-test-secret\") "
            + "SELECT k FROM cancellation_reason_test.src";
    private ConnectContext context;
    private StmtExecutor executor;
    private InsertIntoTVFCommand command;
    private QueryQueue queue;
    private QueueToken holder;
    private ExecutorService workers;
    private RecordingCoordinator coordinator;
    private Thread runThread;
    private CountDownLatch firstCancel;
    private CountDownLatch cancelFinished;
    private boolean holdCatch;
    private int dispatchesBefore;

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("cancellation_reason_test");
        createTable("CREATE TABLE cancellation_reason_test.src(k INT) DUPLICATE KEY(k) "
                + "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES(\"replication_num\"=\"1\")");
    }

    @Override
    protected void runBeforeEach() throws Exception {
        context = createDefaultCtx();
        context.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION,OLAP_SCAN_TABLET_PRUNE");
        context.getSessionVariable().setQueryTimeoutS(60);
        createStatementCtx(SQL, context);
        executor = new StmtExecutor(context, SQL);
        command = (InsertIntoTVFCommand) new NereidsParser().parseSingle(SQL);
        queue = Mockito.spy(new QueryQueue(1, 1, 1, 60000));
        WorkloadGroup group = Mockito.spy(context.getEnv().getWorkloadGroupMgr().getWorkloadGroup(context).get(0));
        Mockito.doReturn(queue).when(group).getQueryQueue();
        WorkloadGroupMgr groups = Mockito.mock(WorkloadGroupMgr.class);
        Mockito.when(groups.getWorkloadGroup(context)).thenReturn(List.of(group));
        Env env = Mockito.mock(Env.class, AdditionalAnswers.delegatesTo(context.getEnv()));
        Mockito.doReturn(groups).when(env).getWorkloadGroupMgr();
        context.setEnv(env);
        holder = queue.getToken(1);
        Assertions.assertTrue(holder.isReadyToRun());
        workers = Executors.newFixedThreadPool(2);
        coordinator = null;
        firstCancel = new CountDownLatch(1);
        cancelFinished = new CountDownLatch(1);
        holdCatch = false;
        dispatchesBefore = DefaultPBackendServiceImpl.getExecPlanFragmentCalls();
    }

    @AfterEach
    public void releaseResources() throws Exception {
        cancelFinished.countDown();
        queue.releaseAndNotify(holder);
        workers.shutdownNow();
        Assertions.assertTrue(workers.awaitTermination(10, TimeUnit.SECONDS));
        context.getStatementContext().close();
        connectContext.setThreadLocalInfo();
    }

    @ParameterizedTest
    @EnumSource(value = TStatusCode.class, names = {"TIMEOUT", "CANCELLED", "INTERNAL_ERROR"})
    public void preCanceledExecutor(TStatusCode code) {
        Status reason = reason(code);
        cancelTwice(reason);
        try (MockedConstruction<NereidsPlanner> planners = Mockito.mockConstruction(NereidsPlanner.class)) {
            UserException failure = Assertions.assertThrows(UserException.class, () -> command.run(context, executor));
            Assertions.assertTrue(planners.constructed().isEmpty());
            Assertions.assertNull(executor.getCoord());
            Assertions.assertEquals(reason.getErrorMsg(), failure.getDetailMessage(), "pre-cancel diagnostic");
        }
    }

    @ParameterizedTest
    @EnumSource(value = TStatusCode.class, names = {"TIMEOUT", "CANCELLED", "INTERNAL_ERROR"})
    public void cancellationDuringPlanning(TStatusCode code) throws Exception {
        Status reason = reason(code);
        executor = Mockito.spy(executor);
        Mockito.doAnswer(call -> {
            call.callRealMethod();
            cancelTwice(reason);
            return null;
        }).when(executor).checkBlockRules();
        UserException failure = Assertions.assertThrows(UserException.class, () -> command.run(context, executor));
        Assertions.assertNull(executor.getCoord());
        Assertions.assertEquals(reason.getErrorMsg(), failure.getDetailMessage(), "planning diagnostic");
    }

    @ParameterizedTest
    @EnumSource(value = TStatusCode.class, names = {"TIMEOUT", "CANCELLED", "INTERNAL_ERROR"})
    public void cancelBeforeTokenPublication(TStatusCode code) throws Exception {
        CountDownLatch allocated = new CountDownLatch(1);
        CountDownLatch publish = new CountDownLatch(1);
        Mockito.doAnswer(call -> {
            QueueToken token = (QueueToken) call.callRealMethod();
            Assertions.assertFalse(token.isReadyToRun());
            allocated.countDown();
            Assertions.assertTrue(publish.await(30, TimeUnit.SECONDS));
            return token;
        }).when(queue).getToken(1);
        holdCatch = true;
        Future<?> run = startCommand(null);
        try {
            Assertions.assertTrue(allocated.await(30, TimeUnit.SECONDS));
            Status reason = reason(code);
            Future<?> cancel = cancelAsync(reason);
            Assertions.assertTrue(firstCancel.await(10, TimeUnit.SECONDS));
            Assertions.assertTrue(coordinator.getCoordinatorContext().getQueueToken().isEmpty());
            Status beforePublication = coordinator.getExecStatus();
            Assertions.assertFalse(cancel.isDone(), "cancel must wait for exec to leave the dispatch lock");
            publish.countDown();
            cancel.get(10, TimeUnit.SECONDS);
            run.get(10, TimeUnit.SECONDS);
            assertAdmissionReleased();
            assertStatus(reason, beforePublication);
            assertCanceled(reason);
        } finally {
            publish.countDown();
        }
    }

    @ParameterizedTest
    @EnumSource(value = TStatusCode.class, names = {"TIMEOUT", "CANCELLED", "INTERNAL_ERROR"})
    public void cancelDuringAdmission(TStatusCode code) throws Exception {
        holdCatch = true;
        Future<?> run = startCommand(null);
        Awaitility.await().atMost(30, TimeUnit.SECONDS).until(() -> executor.getCoord() != null
                && executor.getCoord().getCoordinatorContext().getQueueToken().isPresent());
        Assertions.assertFalse(coordinator.getCoordinatorContext().getQueueToken().get().isReadyToRun());
        Status reason = reason(code);
        cancelAsync(reason).get(10, TimeUnit.SECONDS);
        run.get(10, TimeUnit.SECONDS);
        assertAdmissionReleased();
        assertCanceled(reason);
    }

    @ParameterizedTest
    @EnumSource(value = TStatusCode.class, names = {"TIMEOUT", "CANCELLED", "INTERNAL_ERROR"})
    public void cancelWhileWaiting(TStatusCode code) throws Exception {
        runWaitingCancellation(reason(code), false, false);
    }

    @ParameterizedTest
    @EnumSource(value = TStatusCode.class, names = {"TIMEOUT", "CANCELLED", "INTERNAL_ERROR"})
    public void cancelWithWaitFailure(TStatusCode code) throws Exception {
        runWaitingCancellation(reason(code), true, false);
    }

    @ParameterizedTest
    @EnumSource(value = TStatusCode.class, names = {"TIMEOUT", "CANCELLED", "INTERNAL_ERROR"})
    public void preserveEarlierCoordinatorFailure(TStatusCode code) throws Exception {
        runWaitingCancellation(reason(code), true, true);
    }

    private void runWaitingCancellation(Status reason, boolean throwOnJoin, boolean priorFailure) throws Exception {
        startCommand(current -> {
            // Dispatch/wait are controlled; executor callbacks and Coordinator.cancel/status updates stay real.
            Mockito.doNothing().when(current).exec();
            Mockito.doAnswer(call -> {
                if (priorFailure) {
                    current.getCoordinatorContext().updateStatusIfOk(
                            new Status(TStatusCode.INTERNAL_ERROR, "earlier backend failure"));
                }
                cancelTwice(reason);
                if (throwOnJoin) {
                    throw new IllegalStateException("secondary wait failure");
                }
                return true;
            }).when(current).join(Mockito.anyInt());
        }).get(30, TimeUnit.SECONDS);
        assertCleanedUp();
        if (priorFailure) {
            assertStatus(new Status(TStatusCode.INTERNAL_ERROR, "earlier backend failure"), coordinator.getExecStatus());
        } else {
            assertStatus(reason, coordinator.getExecStatus());
        }
        assertDeliveredReasons(reason);
    }

    private Future<?> startCommand(CoordinatorSetup setup) {
        return workers.submit(() -> {
            runThread = Thread.currentThread();
            context.setThreadLocalInfo();
            EnvFactory factory = Mockito.spy(EnvFactory.getInstance());
            try (MockedStatic<EnvFactory> factories = Mockito.mockStatic(EnvFactory.class)) {
                factories.when(EnvFactory::getInstance).thenReturn(factory);
                Mockito.doAnswer(call -> {
                    coordinator = Mockito.spy(new RecordingCoordinator(call.getArgument(1)));
                    if (setup != null) {
                        setup.configure(coordinator);
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

    private Future<?> cancelAsync(Status reason) {
        return workers.submit(() -> {
            try {
                cancelTwice(reason);
            } finally {
                cancelFinished.countDown();
            }
        });
    }

    private void cancelTwice(Status reason) {
        executor.cancel(reason, false);
        executor.cancel(new Status(TStatusCode.CANCELLED, "later cancel must not replace first reason"), false);
        Assertions.assertSame(reason, executor.getPendingCancelReason());
    }

    private Status reason(TStatusCode code) {
        return new Status(code, "original executor " + code + " diagnostic");
    }

    private void assertStatus(Status expected, Status actual) {
        Assertions.assertEquals(expected.getErrorCode(), actual.getErrorCode(), "executor cancellation classification");
        Assertions.assertEquals(expected.getErrorMsg(), actual.getErrorMsg(), "executor cancellation diagnostic");
    }

    private void assertCanceled(Status reason) {
        assertCleanedUp();
        assertStatus(reason, coordinator.getExecStatus());
        // Two callback phases, the executor forwarding, the repeated cancel and the command catch.
        Assertions.assertEquals(5, coordinator.reasons.size());
        assertDeliveredReasons(reason);
    }

    private void assertDeliveredReasons(Status reason) {
        Assertions.assertFalse(coordinator.reasons.isEmpty());
        for (Status delivered : coordinator.reasons) {
            assertStatus(reason, delivered);
        }
        Assertions.assertEquals(reason.getErrorMsg(), context.getState().getErrorMessage(), "client diagnostic");
    }

    private void assertCleanedUp() {
        Assertions.assertEquals(MysqlStateType.ERR, context.getState().getStateType());
        Assertions.assertNull(QeProcessorImpl.INSTANCE.getCoordinator(context.queryId()));
        Mockito.verify(coordinator).close();
    }

    private void assertAdmissionReleased() {
        QueueToken token = coordinator.getCoordinatorContext().getQueueToken().orElseThrow();
        Mockito.verify(queue).releaseAndNotify(token);
        Mockito.verify(queue, Mockito.never()).releaseAndNotify(holder);
        Assertions.assertEquals(1, queue.usedSlotCount());
        Assertions.assertTrue(coordinator.getDispatchedBackendIds().isEmpty());
        Assertions.assertEquals(dispatchesBefore, DefaultPBackendServiceImpl.getExecPlanFragmentCalls());
    }

    private interface CoordinatorSetup {
        void configure(RecordingCoordinator current) throws Exception;
    }

    private class RecordingCoordinator extends Coordinator {
        private final List<Status> reasons = new CopyOnWriteArrayList<>();

        private RecordingCoordinator(NereidsPlanner planner) {
            super(context, planner, null);
        }

        @Override
        public void cancel(Status reason) {
            reasons.add(new Status(reason));
            super.cancel(reason);
            firstCancel.countDown();
            // Keep the command in catch until both callback phases and executor forwarding finish.
            if (holdCatch && Thread.currentThread() == runThread) {
                try {
                    Assertions.assertTrue(cancelFinished.await(10, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                }
            }
        }
    }
}
