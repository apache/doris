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

package org.apache.doris.qe.runtime;

import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.Status;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.load.loadv2.LoadJob;
import org.apache.doris.load.loadv2.LoadManager;
import org.apache.doris.load.loadv2.ProgressManager;
import org.apache.doris.nereids.trees.plans.distribute.PipelineDistributedPlan;
import org.apache.doris.nereids.trees.plans.distribute.worker.BackendWorker;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.UnassignedJob;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanFragmentId;
import org.apache.doris.planner.PlanNode;
import org.apache.doris.qe.CoordinatorContext;
import org.apache.doris.qe.NereidsCoordinator;
import org.apache.doris.qe.ResultReceiverConsumer;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.system.Backend;
import org.apache.doris.task.LoadEtlTask;
import org.apache.doris.thrift.TErrorTabletInfo;
import org.apache.doris.thrift.TReportExecStatusParams;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TTabletCommitInfo;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

class LoadProcessorTest {
    private static final long BACKEND_ID = 9;
    private static final int FRAGMENT_ID = 7;
    private static final String TRACKING_URL = "http://127.0.0.1/error-log";
    private static final String FIRST_ERROR = "Cannot convert value to integer";

    @Test
    void publishesCompleteResultBeforeCancellationReleasesLoadWaiters() throws Exception {
        Fixture fixture = createFixture();
        TReportExecStatusParams report = fullReport(TStatusCode.DATA_QUALITY_ERROR);
        Mockito.doAnswer(invocation -> {
            // LoadProcessor.cancel invokes remote cancellation before releasing its real latch.
            Assertions.assertFalse(fixture.processor.isDone());
            assertReportResult(fixture, report);
            Assertions.assertEquals(TStatusCode.DATA_QUALITY_ERROR,
                    fixture.coordinator.getExecStatus().getErrorCode());
            return null;
        }).when(fixture.fragmentsTask).cancelExecute(Mockito.any(Status.class));

        Assertions.assertFalse(fixture.processor.await(0, TimeUnit.MILLISECONDS));
        fixture.coordinator.updateFragmentExecStatus(report);

        Mockito.verify(fixture.fragmentsTask).cancelExecute(Mockito.any(Status.class));
        Assertions.assertTrue(fixture.processor.await(0, TimeUnit.MILLISECONDS));
        Assertions.assertTrue(fixture.coordinator.join(1));
    }

    @Test
    void successfulFinalReportPreservesCompleteResult() throws Exception {
        Fixture fixture = createFixture();

        TReportExecStatusParams report = fullReport(TStatusCode.OK);
        fixture.coordinator.updateFragmentExecStatus(report);

        Assertions.assertTrue(fixture.coordinator.join(1));
        Assertions.assertTrue(fixture.coordinator.getExecStatus().ok());
        assertReportResult(fixture, report);
        Mockito.verify(fixture.fragmentsTask, Mockito.never()).cancelExecute(Mockito.any(Status.class));
    }

    @Test
    void failedReportWithoutDiagnosticsPreservesEarlierDiagnostics() throws Exception {
        Fixture fixture = createFixture();
        fixture.processor.loadContext.updateTrackingUrl(TRACKING_URL);
        fixture.processor.loadContext.updateFirstErrorMsg(FIRST_ERROR);
        Mockito.doAnswer(invocation -> {
            Assertions.assertFalse(fixture.processor.isDone());
            Assertions.assertEquals(TRACKING_URL, fixture.coordinator.getTrackingUrl());
            Assertions.assertEquals(FIRST_ERROR, fixture.coordinator.getFirstErrorMsg());
            return null;
        }).when(fixture.fragmentsTask).cancelExecute(Mockito.any(Status.class));

        fixture.coordinator.updateFragmentExecStatus(report(TStatusCode.DATA_QUALITY_ERROR));

        Mockito.verify(fixture.fragmentsTask).cancelExecute(Mockito.any(Status.class));
        Assertions.assertTrue(fixture.coordinator.join(1));
        Assertions.assertEquals(TRACKING_URL, fixture.coordinator.getTrackingUrl());
        Assertions.assertEquals(FIRST_ERROR, fixture.coordinator.getFirstErrorMsg());
    }

    @Test
    void duplicateFailedFinalReportDoesNotAggregateAgain() throws Exception {
        Fixture fixture = createFixture();
        TReportExecStatusParams report = fullReport(TStatusCode.DATA_QUALITY_ERROR);

        fixture.coordinator.updateFragmentExecStatus(report);
        fixture.coordinator.updateFragmentExecStatus(report.deepCopy());

        Assertions.assertTrue(fixture.coordinator.join(1));
        assertReportResult(fixture, report);
        Mockito.verify(fixture.fragmentsTask).cancelExecute(Mockito.any(Status.class));
    }

    @Test
    void incompleteFailedReportStillPublishesDiagnosticsAndCancels() throws Exception {
        Fixture fixture = createFixture();
        TReportExecStatusParams report = fullReport(TStatusCode.DATA_QUALITY_ERROR).setDone(false);

        fixture.coordinator.updateFragmentExecStatus(report);

        Assertions.assertTrue(fixture.coordinator.join(1));
        Assertions.assertEquals(TStatusCode.DATA_QUALITY_ERROR, fixture.coordinator.getExecStatus().getErrorCode());
        Assertions.assertEquals(TRACKING_URL, fixture.coordinator.getTrackingUrl());
        Assertions.assertEquals(FIRST_ERROR, fixture.coordinator.getFirstErrorMsg());
        Assertions.assertTrue(fixture.coordinator.getLoadCounters().isEmpty());
        Assertions.assertTrue(fixture.coordinator.getCommitInfos().isEmpty());
        Mockito.verify(fixture.fragmentsTask).cancelExecute(Mockito.any(Status.class));
    }

    @Test
    void externalCommitDataFailureDoesNotDelayCancellation() throws Exception {
        Fixture fixture = createFixture();
        TReportExecStatusParams report = fullReport(TStatusCode.DATA_QUALITY_ERROR)
                .setHivePartitionUpdates(Collections.emptyList());

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenThrow(new IllegalStateException("external transaction unavailable"));
            Assertions.assertThrows(IllegalStateException.class,
                    () -> fixture.coordinator.updateFragmentExecStatus(report));
        }

        Assertions.assertTrue(fixture.coordinator.join(1));
        Assertions.assertEquals(TStatusCode.DATA_QUALITY_ERROR, fixture.coordinator.getExecStatus().getErrorCode());
        assertReportResult(fixture, report);
        Mockito.verify(fixture.fragmentsTask).cancelExecute(Mockito.any(Status.class));
    }

    @Test
    void queryFailureStillCancelsImmediately() {
        Fixture fixture = createFixture();
        CoordinatorContext context = fixture.coordinator.getCoordinatorContext();
        ResultReceiverConsumer receiver = Mockito.mock(ResultReceiverConsumer.class);
        QueryProcessor processor = new QueryProcessor(context, receiver);
        context.setJobProcessor(processor);
        processor.setPipelineExecutionTask(fixture.executionTask);

        fixture.coordinator.updateFragmentExecStatus(report(TStatusCode.INTERNAL_ERROR).setDone(false));

        Assertions.assertEquals(TStatusCode.INTERNAL_ERROR, fixture.coordinator.getExecStatus().getErrorCode());
        Mockito.verify(receiver).cancel(Mockito.any(Status.class));
        Mockito.verify(fixture.fragmentsTask).cancelExecute(Mockito.any(Status.class));
    }

    private static TReportExecStatusParams fullReport(TStatusCode status) {
        return report(status)
                .setTrackingUrl(TRACKING_URL)
                .setFirstErrorMsg(FIRST_ERROR)
                .setLoadCounters(ImmutableMap.of(LoadEtlTask.DPP_NORMAL_ALL, "11",
                        LoadEtlTask.DPP_ABNORMAL_ALL, "3", LoadJob.UNSELECTED_ROWS, "2"))
                .setDeltaUrls(Collections.singletonList("delta-file"))
                .setCommitInfos(Collections.singletonList(new TTabletCommitInfo(101, BACKEND_ID)))
                .setErrorTabletInfos(Collections.singletonList(new TErrorTabletInfo()
                        .setTabletId(102).setMsg(FIRST_ERROR)))
                .setTxnId(103)
                .setLabel("load-report")
                .setExportFiles(Collections.singletonList("export-file"));
    }

    private static void assertReportResult(Fixture fixture, TReportExecStatusParams report) {
        Assertions.assertEquals(report.getTrackingUrl(), fixture.coordinator.getTrackingUrl());
        Assertions.assertEquals(report.getFirstErrorMsg(), fixture.coordinator.getFirstErrorMsg());
        Assertions.assertEquals(report.getLoadCounters(), fixture.coordinator.getLoadCounters());
        Assertions.assertEquals(report.getDeltaUrls(), fixture.coordinator.getDeltaUrls());
        Assertions.assertEquals(report.getCommitInfos(), fixture.coordinator.getCommitInfos());
        Assertions.assertEquals(report.getErrorTabletInfos(), fixture.coordinator.getErrorTabletInfos());
        Assertions.assertEquals(report.getTxnId(), fixture.coordinator.getTxnId());
        Assertions.assertEquals(report.getLabel(), fixture.coordinator.getLabel());
        Assertions.assertEquals(report.getExportFiles(), fixture.coordinator.getExportFiles());
    }

    private static TReportExecStatusParams report(TStatusCode status) {
        return new TReportExecStatusParams()
                .setBackendId(BACKEND_ID)
                .setFragmentId(FRAGMENT_ID)
                .setDone(true)
                .setStatus(new TStatus(status));
    }

    private static Fixture createFixture() {
        PlanFragment fragment = Mockito.mock(PlanFragment.class);
        Mockito.when(fragment.getFragmentId()).thenReturn(new PlanFragmentId(FRAGMENT_ID));
        Mockito.when(fragment.getPlanRoot()).thenReturn(Mockito.mock(PlanNode.class));
        UnassignedJob fragmentJob = Mockito.mock(UnassignedJob.class);
        Mockito.when(fragmentJob.getFragment()).thenReturn(fragment);
        PipelineDistributedPlan distributedPlan = Mockito.mock(PipelineDistributedPlan.class);
        Mockito.when(distributedPlan.getFragmentJob()).thenReturn(fragmentJob);
        Mockito.when(distributedPlan.getInstanceJobs()).thenReturn(Collections.emptyList());

        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getLoadManager()).thenReturn(Mockito.mock(LoadManager.class));
        Mockito.when(env.getProgressManager()).thenReturn(Mockito.mock(ProgressManager.class));
        NereidsCoordinator coordinator = Mockito.mock(NereidsCoordinator.class, Mockito.CALLS_REAL_METHODS);
        CoordinatorContext context;
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            context = CoordinatorContext.buildForLoad(coordinator, -1, new TUniqueId(1, 2),
                    Collections.singletonList(fragment), Collections.singletonList(distributedPlan),
                    Collections.emptyList(), new DescriptorTable(), "UTC", true, false);
        }
        Deencapsulation.setField(coordinator, "coordinatorContext", context);
        LoadProcessor processor = context.asLoadProcessor();

        Backend backend = Mockito.mock(Backend.class);
        Mockito.when(backend.getId()).thenReturn(BACKEND_ID);
        BackendWorker worker = Mockito.mock(BackendWorker.class);
        Mockito.when(worker.id()).thenReturn(BACKEND_ID);
        BackendServiceProxy backendProxy = Mockito.mock(BackendServiceProxy.class);
        SingleFragmentPipelineTask fragmentTask = new SingleFragmentPipelineTask(
                backend, FRAGMENT_ID, Collections.singleton(new TUniqueId(3, 4)));
        MultiFragmentsPipelineTask fragmentsTask = Mockito.spy(new MultiFragmentsPipelineTask(
                context, backend, backendProxy, ByteString.EMPTY,
                Collections.singletonMap(FRAGMENT_ID, fragmentTask)));
        Mockito.doNothing().when(fragmentsTask).cancelExecute(Mockito.any(Status.class));
        PipelineExecutionTask executionTask = new PipelineExecutionTask(context, backendProxy,
                Collections.singletonMap(worker, fragmentsTask));
        processor.setPipelineExecutionTask(executionTask);
        return new Fixture(coordinator, processor, fragmentsTask, executionTask);
    }

    private static class Fixture {
        final NereidsCoordinator coordinator;
        final LoadProcessor processor;
        final MultiFragmentsPipelineTask fragmentsTask;
        final PipelineExecutionTask executionTask;

        Fixture(NereidsCoordinator coordinator, LoadProcessor processor, MultiFragmentsPipelineTask fragmentsTask,
                PipelineExecutionTask executionTask) {
            this.coordinator = coordinator;
            this.processor = processor;
            this.fragmentsTask = fragmentsTask;
            this.executionTask = executionTask;
        }
    }
}
