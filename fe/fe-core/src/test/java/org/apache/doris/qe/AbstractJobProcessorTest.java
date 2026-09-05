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

import org.apache.doris.common.Status;
import org.apache.doris.nereids.trees.plans.distribute.worker.BackendWorker;
import org.apache.doris.qe.runtime.BackendFragmentId;
import org.apache.doris.qe.runtime.MultiFragmentsPipelineTask;
import org.apache.doris.qe.runtime.PipelineExecutionTask;
import org.apache.doris.qe.runtime.SingleFragmentPipelineTask;
import org.apache.doris.thrift.TReportExecStatusParams;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

class AbstractJobProcessorTest {
    @Test
    void finishBeforeFragmentDispatchDoesNotCancelPreparedFragments() {
        MultiFragmentsPipelineTask fragmentsTask = Mockito.mock(MultiFragmentsPipelineTask.class);
        TestJobProcessor processor = createProcessor(fragmentsTask);

        processor.tryFinishSchedule();
        Mockito.verifyNoInteractions(fragmentsTask);

        processor.markFragmentDispatchCompleted();
        Mockito.verify(fragmentsTask).cancelExecute(Status.FINISHED);

        processor.tryFinishSchedule();
        processor.markFragmentDispatchCompleted();
        Mockito.verifyNoMoreInteractions(fragmentsTask);
    }

    @Test
    void fragmentDispatchBeforeFinishBroadcastsWhenExecutionFinishes() {
        MultiFragmentsPipelineTask fragmentsTask = Mockito.mock(MultiFragmentsPipelineTask.class);
        TestJobProcessor processor = createProcessor(fragmentsTask);

        processor.markFragmentDispatchCompleted();
        Mockito.verifyNoInteractions(fragmentsTask);

        processor.tryFinishSchedule();
        Mockito.verify(fragmentsTask).cancelExecute(Status.FINISHED);
    }

    @Test
    void publishesReportDiagnosticsBeforeFailedStatusAndFinalReport() {
        long backendId = 9;
        int fragmentId = 7;
        String trackingUrl = "http://127.0.0.1/error-log";
        List<String> events = new ArrayList<>();
        CoordinatorContext coordinatorContext = Mockito.mock(CoordinatorContext.class);
        TestJobProcessor processor = new TestJobProcessor(coordinatorContext);
        processor.setReportEventRecorder(events::add);
        processor.setBackendFragmentTask(backendId, fragmentId, Mockito.mock(SingleFragmentPipelineTask.class));
        Mockito.when(coordinatorContext.updateStatusIfOk(Mockito.any(Status.class))).thenAnswer(invocation -> {
            events.add("cancel");
            return Status.OK;
        });

        TReportExecStatusParams params = new TReportExecStatusParams()
                .setBackendId(backendId)
                .setFragmentId(fragmentId)
                .setDone(true)
                .setStatus(new TStatus(TStatusCode.DATA_QUALITY_ERROR))
                .setTrackingUrl(trackingUrl);

        processor.updateFragmentExecStatus(params);

        Assertions.assertEquals(Arrays.asList("publish:" + trackingUrl, "cancel", "final"), events);
    }

    private static TestJobProcessor createProcessor(MultiFragmentsPipelineTask fragmentsTask) {
        BackendWorker worker = Mockito.mock(BackendWorker.class);
        PipelineExecutionTask executionTask = Mockito.mock(PipelineExecutionTask.class);
        Mockito.when(executionTask.getChildrenTasks()).thenReturn(Collections.singletonMap(worker, fragmentsTask));

        TestJobProcessor processor = new TestJobProcessor(Mockito.mock(CoordinatorContext.class));
        processor.setExecutionTask(executionTask);
        return processor;
    }

    private static class TestJobProcessor extends AbstractJobProcessor {
        private Consumer<String> reportEventRecorder = ignored -> {};

        TestJobProcessor(CoordinatorContext coordinatorContext) {
            super(coordinatorContext);
        }

        void setExecutionTask(PipelineExecutionTask executionTask) {
            this.executionTask = Optional.of(executionTask);
        }

        void setBackendFragmentTask(long backendId, int fragmentId, SingleFragmentPipelineTask fragmentTask) {
            this.backendFragmentTasks = Optional.of(Collections.singletonMap(
                    new BackendFragmentId(backendId, fragmentId), fragmentTask));
        }

        void setReportEventRecorder(Consumer<String> reportEventRecorder) {
            this.reportEventRecorder = reportEventRecorder;
        }

        @Override
        protected void publishReportDiagnosticsBeforeStatus(TReportExecStatusParams params) {
            reportEventRecorder.accept("publish:" + params.getTrackingUrl());
        }

        @Override
        protected void doProcessReportExecStatus(
                TReportExecStatusParams params, SingleFragmentPipelineTask fragmentTask) {
            reportEventRecorder.accept("final");
        }

        @Override
        public void cancel(Status cancelReason) {}
    }
}
