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
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.Coordinator;
import org.apache.doris.qe.QeProcessorImpl;
import org.apache.doris.qe.QueryState.MysqlStateType;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.resource.workloadgroup.PausedAdmission;
import org.apache.doris.resource.workloadgroup.QueryQueue;
import org.apache.doris.resource.workloadgroup.QueueToken;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.utframe.MockedBackendFactory.DefaultPBackendServiceImpl;
import org.apache.doris.utframe.TestWithFeService;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class InsertIntoTVFAdmissionCancellationTest extends TestWithFeService {
    private static final String SELECT = "SELECT k FROM admission_test.src";
    private static final String INSERT = "INSERT INTO s3("
            + "\"file_path\"=\"s3://unit-test/result_\",\"format\"=\"csv\","
            + "\"s3.endpoint\"=\"http://127.0.0.1:1\",\"s3.region\"=\"us-east-1\","
            + "\"s3.access_key\"=\"unit-test-access\",\"s3.secret_key\"=\"unit-test-secret\") " + SELECT;
    private ConnectContext context;
    private QueryQueue queue;
    private QueueToken holder;
    private Coordinator selectCoordinator;
    private ExecutorService workers;
    private int dispatchesBefore;

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("admission_test");
        createTable("CREATE TABLE admission_test.src(k INT) DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 "
                + "PROPERTIES(\"replication_num\"=\"1\")");
    }

    @Override
    protected void runBeforeEach() throws Exception {
        context = createDefaultCtx();
        context.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION,OLAP_SCAN_TABLET_PRUNE");
        context.getSessionVariable().setQueryTimeoutS(60);
        queue = Mockito.spy(new QueryQueue(1, 1, 1, 60000));
        PausedAdmission.useQueue(context, queue);

        workers = Executors.newFixedThreadPool(2);
        selectCoordinator = null;
        holder = queue.getToken(1);
        Assertions.assertTrue(holder.isReadyToRun());
        dispatchesBefore = DefaultPBackendServiceImpl.getExecPlanFragmentCalls();
    }

    @AfterEach
    public void releaseQueueAndThreads() throws Exception {
        try {
            if (holder != null) {
                queue.releaseAndNotify(holder);
            }
            workers.shutdownNow();
            Assertions.assertTrue(workers.awaitTermination(10, TimeUnit.SECONDS));
        } finally {
            if (selectCoordinator != null) {
                QeProcessorImpl.INSTANCE.unregisterQuery(context.queryId());
                selectCoordinator.close();
                context.getStatementContext().close();
            }
            connectContext.setThreadLocalInfo();
        }
    }

    @Test
    public void cancelBeforeTokenPublication() throws Exception {
        createStatementCtx(INSERT, context);
        StmtExecutor executor = new StmtExecutor(context, INSERT);
        try (PausedAdmission admission = new PausedAdmission(queue)) {
            Future<?> run = workers.submit(() -> {
                context.setThreadLocalInfo();
                try (MockedStatic<EnvFactory> ignored = admission.observeCancellation(context)) {
                    InsertIntoTVFCommand command = (InsertIntoTVFCommand) new NereidsParser().parseSingle(INSERT);
                    command.run(context, executor);
                } finally {
                    context.getStatementContext().close();
                    ConnectContext.remove();
                }
                return null;
            });
            Assertions.assertFalse(admission.awaitWaiter().isReadyToRun());
            Coordinator coordinator = executor.getCoord();
            Future<?> cancel = workers.submit(() -> executor.cancel(
                    new Status(TStatusCode.CANCELLED, "cancel from executor"), false));
            Assertions.assertSame(coordinator, admission.awaitFirstCancel());
            Assertions.assertTrue(coordinator.getCoordinatorContext().getQueueToken().isEmpty());
            Assertions.assertEquals("cancel from executor", coordinator.getExecStatus().getErrorMsg());

            admission.publish();
            QueueToken token = awaitPublishedToken(coordinator);
            Assertions.assertDoesNotThrow(() -> cancel.get(5, TimeUnit.SECONDS),
                    "cancel(false) remained blocked after token publication with the admission slot held");
            run.get(10, TimeUnit.SECONDS);

            Assertions.assertEquals(MysqlStateType.ERR, context.getState().getStateType());
            Assertions.assertEquals("cancel from executor", context.getState().getErrorMessage());
            Assertions.assertEquals(TStatusCode.CANCELLED, coordinator.getExecStatus().getErrorCode());
            Assertions.assertEquals("cancel from executor", coordinator.getExecStatus().getErrorMsg());
            Assertions.assertNull(QeProcessorImpl.INSTANCE.getCoordinator(context.queryId()));
            Assertions.assertTrue(coordinator.getDispatchedBackendIds().isEmpty());
            Assertions.assertEquals(dispatchesBefore, DefaultPBackendServiceImpl.getExecPlanFragmentCalls());
            Mockito.verify(queue).releaseAndNotify(token);
            Mockito.verify(queue, Mockito.never()).releaseAndNotify(holder);
            Assertions.assertEquals(1, queue.usedSlotCount());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void timeoutBeforePublication(boolean ready) throws Exception {
        if (ready) {
            releaseHolder();
        }
        Mockito.doAnswer(call -> {
            QueueToken token = (QueueToken) call.callRealMethod();
            Assertions.assertEquals(ready, token.isReadyToRun());
            selectCoordinator.cancel(new Status(TStatusCode.TIMEOUT, "original admission timeout"));
            return token;
        }).when(queue).getToken(1);

        assertSelectRejected(startSelect(), TStatusCode.TIMEOUT, "original admission timeout");
    }

    @Test
    public void cancelAfterPublication() throws Exception {
        Future<?> select = startSelect();
        Assertions.assertFalse(awaitPublishedToken(selectCoordinator).isReadyToRun());
        selectCoordinator.cancel(new Status(TStatusCode.CANCELLED, "cancel published token"));
        assertSelectRejected(select, TStatusCode.CANCELLED, "cancel published token");
    }

    @Test
    public void admitHealthyWaiter() throws Exception {
        Future<?> select = startSelect();
        QueueToken token = awaitPublishedToken(selectCoordinator);
        Assertions.assertFalse(token.isReadyToRun());
        releaseHolder();
        select.get(10, TimeUnit.SECONDS);
        Assertions.assertTrue(token.isReadyToRun());
        Assertions.assertTrue(selectCoordinator.getExecStatus().ok());
        Assertions.assertTrue(DefaultPBackendServiceImpl.getExecPlanFragmentCalls() > dispatchesBefore);
    }

    private void releaseHolder() {
        queue.releaseAndNotify(holder);
        holder = null;
    }

    private QueueToken awaitPublishedToken(Coordinator coordinator) {
        Awaitility.await().atMost(10, TimeUnit.SECONDS)
                .until(() -> coordinator.getCoordinatorContext().getQueueToken().isPresent());
        return coordinator.getCoordinatorContext().getQueueToken().get();
    }

    private Future<?> startSelect() throws Exception {
        NereidsPlanner planner = PlanChecker.from(context).plan(SELECT);
        selectCoordinator = new Coordinator(context, planner, null);
        QeProcessorImpl.INSTANCE.registerQuery(context.queryId(), new QeProcessorImpl.QueryInfo(selectCoordinator));
        return workers.submit(() -> {
            context.setThreadLocalInfo();
            try {
                selectCoordinator.exec();
            } finally {
                ConnectContext.remove();
            }
            return null;
        });
    }

    private void assertSelectRejected(Future<?> select, TStatusCode code, String reason) {
        awaitPublishedToken(selectCoordinator);
        ExecutionException failure = Assertions.assertThrows(ExecutionException.class,
                () -> select.get(5, TimeUnit.SECONDS));
        Assertions.assertInstanceOf(UserException.class, failure.getCause());
        Assertions.assertTrue(failure.getCause().getMessage().contains(reason));
        Assertions.assertEquals(code, selectCoordinator.getExecStatus().getErrorCode());
        Assertions.assertEquals(reason, selectCoordinator.getExecStatus().getErrorMsg());
        Assertions.assertEquals(dispatchesBefore, DefaultPBackendServiceImpl.getExecPlanFragmentCalls());
    }
}
