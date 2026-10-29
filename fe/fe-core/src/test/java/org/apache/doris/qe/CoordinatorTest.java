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

package org.apache.doris.qe;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.EnvFactory;
import org.apache.doris.cloud.catalog.CloudEnvFactory;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.trees.plans.distribute.PipelineDistributedPlan;
import org.apache.doris.nereids.trees.plans.distribute.worker.BackendWorker;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.runtime.MultiFragmentsPipelineTask;
import org.apache.doris.qe.runtime.PipelineExecutionTask;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TDataSinkType;
import org.apache.doris.thrift.TPipelineFragmentParams;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

public class CoordinatorTest extends TestWithFeService {
    @BeforeAll
    public void init() throws Exception {
        FeConstants.runningUnitTest = true;

        createDatabase("test");
        useDatabase("test");

        createTable("create table tbl(id int) distributed by hash(id) buckets 10 properties('replication_num' = '1');");
    }

    @Test
    public void testCoordinatorScanRangeNum() throws IOException {
        NereidsPlanner planner = plan("select * from test.tbl");
        Coordinator coordinator = (Coordinator) EnvFactory.getInstance()
                .createCoordinator(connectContext, planner, null);
        int scanRangeNum = coordinator.getScanRangeNum();
        Assertions.assertEquals(10, scanRangeNum);
    }

    @Test
    public void testCoordinatorScanRangeNum2() throws IOException {
        NereidsPlanner planner = plan("select * from information_schema.columns");
        Coordinator coordinator = (Coordinator) EnvFactory.getInstance()
                .createCoordinator(connectContext, planner, null);
        int scanRangeNum = coordinator.getScanRangeNum();
        Assertions.assertEquals(0, scanRangeNum);
    }

    @Test
    public void testSimpleQueryUseOneInstance() throws IOException {
        ConnectContext connectContext = createDefaultCtx();
        connectContext.getSessionVariable().parallelPipelineTaskNum = 10;
        NereidsPlanner planner = plan("select * from test.tbl", connectContext);
        for (PlanFragment fragment : planner.getFragments()) {
            Assertions.assertEquals(1, fragment.getParallelExecNum());
        }

        planner = plan("select * from test.tbl where id=1", connectContext);
        for (PlanFragment fragment : planner.getFragments()) {
            Assertions.assertEquals(1, fragment.getParallelExecNum());
        }

        planner = plan("select id, id + 1 from test.tbl where id = 2 limit 1", connectContext);
        for (PlanFragment fragment : planner.getFragments()) {
            Assertions.assertEquals(1, fragment.getParallelExecNum());
        }
    }

    @ParameterizedTest
    @EnumSource(value = TStatusCode.class, names = {"CANCELLED", "TIMEOUT"})
    public void testTerminateBeforeFragmentDispatch(TStatusCode statusCode) throws Exception {
        ConnectContext context = createDefaultCtx();
        NereidsPlanner planner = plan("select * from test.tbl", context);
        Status cancelReason = new Status(statusCode, "terminate before fragment dispatch");
        Coordinator coordinator = new Coordinator(context, planner, null) {
            @Override
            protected void processTopSink(CoordinatorContext coordinatorContext,
                    PipelineDistributedPlan topPlan) throws AnalysisException {
                cancel(cancelReason);
            }
        };

        UserException exception = Assertions.assertThrows(UserException.class, coordinator::exec);
        Assertions.assertTrue(exception.getMessage().contains("terminate before fragment dispatch"));
    }

    @Test
    public void testCancelPublishesStatusBeforeScanCleanupFailure() throws Exception {
        ConnectContext context = createDefaultCtx();
        NereidsPlanner planner = plan("select * from test.tbl", context);
        Status cancelReason = new Status(TStatusCode.TIMEOUT, "timeout before scan cleanup");
        ScanNode failingScan = Mockito.mock(ScanNode.class);
        Mockito.doThrow(new RuntimeException("scan cleanup failed")).when(failingScan).stop();
        ScanNode remainingScan = Mockito.mock(ScanNode.class);
        AtomicInteger cancelInternalCalls = new AtomicInteger();
        Coordinator coordinator = new Coordinator(context, planner, null) {
            @Override
            protected void cancelInternal(Status status) {
                cancelInternalCalls.incrementAndGet();
            }
        };
        coordinator.coordinatorContext.scanNodes.clear();
        coordinator.coordinatorContext.scanNodes.add(failingScan);
        coordinator.coordinatorContext.scanNodes.add(remainingScan);

        // A fallible scan cleanup must neither escape to the caller nor skip the remaining scans; the
        // terminal status is published before cleanup and cancelInternal() still runs in finally.
        Assertions.assertDoesNotThrow(() -> coordinator.cancel(cancelReason));

        Assertions.assertEquals(TStatusCode.TIMEOUT, coordinator.getExecStatus().getErrorCode());
        Assertions.assertEquals("timeout before scan cleanup", coordinator.getExecStatus().getErrorMsg());
        Assertions.assertTrue(cancelInternalCalls.get() >= 1);
        Mockito.verify(remainingScan).stop();
    }

    @Test
    public void testQueueCancellationPrefersRetainedTerminalReason() throws Exception {
        ConnectContext context = createDefaultCtx();
        NereidsPlanner planner = plan("select * from test.tbl", context);
        Coordinator coordinator = new Coordinator(context, planner, null) {
            @Override
            protected void cancelInternal(Status status) {
            }
        };

        Assertions.assertTrue(coordinator.preferTerminalReason(new UserException("query is cancelled"))
                .getMessage().contains("query is cancelled"));

        coordinator.cancel(new Status(TStatusCode.TIMEOUT, "retained queue timeout"));
        Assertions.assertTrue(coordinator.preferTerminalReason(new UserException("query is cancelled"))
                .getMessage().contains("retained queue timeout"));
    }

    @Test
    public void testCancelStillCleansUpWhenStatusPublicationFails() throws Exception {
        ConnectContext context = createDefaultCtx();
        NereidsPlanner planner = plan("select * from test.tbl", context);
        Status cancelReason = new Status(TStatusCode.TIMEOUT, "timeout before cleanup");
        ScanNode scanNode = Mockito.mock(ScanNode.class);
        AtomicInteger cancelInternalCalls = new AtomicInteger();
        Coordinator coordinator = new Coordinator(context, planner, null) {
            @Override
            protected void cancelInternal(Status status) {
                // updateStatusIfOk calls this while publishing; simulate a partially initialized processor
                // whose cancel throws before the cleanup scope used to start.
                if (cancelInternalCalls.incrementAndGet() == 1) {
                    throw new RuntimeException("partially initialized processor cancel");
                }
            }
        };
        coordinator.coordinatorContext.scanNodes.clear();
        coordinator.coordinatorContext.scanNodes.add(scanNode);

        Assertions.assertThrows(RuntimeException.class, () -> coordinator.cancel(cancelReason));
        // The publication threw, but the scan cleanup and the final internal cancel still ran.
        Mockito.verify(scanNode).stop();
        Assertions.assertTrue(cancelInternalCalls.get() >= 2, "the final internal cancel must still be resent");
        Assertions.assertEquals(TStatusCode.TIMEOUT, coordinator.getExecStatus().getErrorCode());
    }

    @Test
    public void testCoordinatorRetainsScanLifecycleAndBackendIds() throws Exception {
        Coordinator coordinator = EnvFactory.getInstance().createCoordinator(
                connectContext, plan("select * from test.tbl"), null);
        ScanNode scan = Mockito.mock(ScanNode.class);
        Mockito.when(scan.getScanBackendIds()).thenReturn(Set.of(10001L));
        coordinator.getCoordinatorContext().scanNodes.clear();
        coordinator.getCoordinatorContext().scanNodes.add(scan);
        Assertions.assertFalse(coordinator.mustOutliveDispatch());
        Mockito.when(scan.coordinatorMustOutliveDispatch()).thenReturn(true);
        Assertions.assertTrue(coordinator.mustOutliveDispatch());
        Assertions.assertEquals(List.of(10001L), coordinator.getScanBackendIds());
        coordinator.close();
        Mockito.verify(scan).stop();
    }

    @Test
    public void testCloudFactoryUsesCoordinatorForSqlAndLoad() throws Exception {
        CloudEnvFactory factory = new CloudEnvFactory();
        NereidsPlanner planner = plan("select * from test.tbl");
        Assertions.assertEquals(Coordinator.class,
                factory.createCoordinator(connectContext, planner, null).getClass());
        Coordinator insert = factory.createCoordinator(connectContext, planner, null, 123L);
        Assertions.assertEquals(123L, insert.getJobId());
        Coordinator load = factory.createCoordinator(456L, connectContext.queryId(), planner.getDescTable(),
                planner.getFragments(), planner.getScanNodes(), "UTC", false, false);
        Assertions.assertEquals(Coordinator.class, load.getClass());
        Assertions.assertEquals(456L, load.getJobId());
        Assertions.assertTrue(load.getQueryOptions().isNewVersionUnixTimestamp());
        Assertions.assertTrue(load.getQueryOptions().isNewVersionPercentile());
        Assertions.assertTrue(load.getQueryOptions().isNewVersionBitmapOpCount());
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "sync_mode"})
    public void testHttpStreamLoadPlanUsesNereidsTransport(String groupCommitMode) throws Exception {
        ConnectContext context = createDefaultCtx();
        UUID uuid = UUID.randomUUID();
        context.setQueryId(new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits()));
        context.setBackendId(Env.getCurrentSystemInfo().getAllBackendsByAllCluster().keySet().iterator().next());
        context.getSessionVariable().groupCommit = groupCommitMode;
        context.setThreadLocalInfo();
        try {
            StmtExecutor executor = new StmtExecutor(context,
                    "insert into test.tbl select * from http_stream('format'='csv','csv_schema'='c1:int')");
            executor.generateHttpStreamPlan(context.queryId());
            Coordinator coordinator = EnvFactory.getInstance().createCoordinator(context, executor.planner(), null);
            coordinator.setLoadMemLimit(123456L);
            TPipelineFragmentParams params = coordinator.getStreamLoadPlan();
            Assertions.assertEquals(123456L, params.getQueryOptions().getLoadMemLimit());
            Assertions.assertEquals(context.queryId(), params.getQueryId());
            Assertions.assertEquals(groupCommitMode.isEmpty()
                            ? TDataSinkType.OLAP_TABLE_SINK : TDataSinkType.GROUP_COMMIT_BLOCK_SINK,
                    params.getFragment().getOutputSink().getType());
            Assertions.assertFalse(params.getLocalParams().isEmpty());
        } finally {
            connectContext.setThreadLocalInfo();
        }
    }

    @Test
    public void testBackendCancellationUsesNereidsTasksAndSkipsRemoteCatalogs() throws Exception {
        Coordinator coordinator = EnvFactory.getInstance().createCoordinator(
                connectContext, plan("select * from test.tbl"), null);
        Backend backend = new Backend(10001L, "127.0.0.1", 9050);
        backend.setAlive(true);
        PipelineExecutionTask task = Mockito.mock(
                PipelineExecutionTask.class);
        MultiFragmentsPipelineTask backendTask = Mockito.mock(
                MultiFragmentsPipelineTask.class);
        Mockito.when(backendTask.getBackend()).thenReturn(backend);
        Mockito.when(backendTask.getChildrenTasks()).thenReturn(Map.of());
        BackendWorker worker =
                new BackendWorker(0L, backend);
        Mockito.when(task.getChildrenTasks()).thenReturn(Map.of(worker, backendTask));
        coordinator.executionTask = task;
        Assertions.assertTrue(coordinator.shouldCancel(List.of(backend)).ok());
        Assertions.assertFalse(coordinator.shouldCancel(List.of()).ok());
        backend.setAlive(false);
        Assertions.assertFalse(coordinator.shouldCancel(List.of(backend)).ok());

        worker = new BackendWorker(999L, backend);
        Mockito.when(task.getChildrenTasks()).thenReturn(Map.of(worker, backendTask));
        Assertions.assertTrue(coordinator.shouldCancel(List.of()).ok());
    }

    @Test
    public void testRemovedSwitchCannotSelectLegacyScheduling() throws Exception {
        connectContext.getState().reset();
        getSqlStmtExecutor("set enable_nereids_distribute_planner=false");
        Assertions.assertNotEquals(QueryState.MysqlStateType.ERR, connectContext.getState().getStateType());
        NereidsPlanner planner = plan("select * from test.tbl");
        Assertions.assertFalse(planner.getDistributedPlans().isEmpty());
        Assertions.assertEquals(Coordinator.class,
                EnvFactory.getInstance().createCoordinator(connectContext, planner, null).getClass());
    }

    private NereidsPlanner plan(String sql) throws IOException {
        return plan(sql, connectContext);
    }

    private NereidsPlanner plan(String sql, ConnectContext connectContext) throws IOException {
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION,OLAP_SCAN_TABLET_PRUNE");
        connectContext.setThreadLocalInfo();

        UUID uuid = UUID.randomUUID();
        connectContext.setQueryId(new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits()));
        NereidsPlanner planner = PlanChecker.from(connectContext).plan(sql);
        return planner;
    }
}
