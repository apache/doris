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

import org.apache.doris.common.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.nereids.trees.plans.distribute.worker.BackendWorker;
import org.apache.doris.qe.CoordinatorContext;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TStatusCode;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

class PipelineExecutionTaskTest {
    @Test
    void timeoutStatusPreventsFragmentDispatch() throws Exception {
        CoordinatorContext coordinatorContext = Mockito.mock(CoordinatorContext.class);
        Deencapsulation.setField(coordinatorContext, "timeoutDeadline", (Supplier<Long>) () -> Long.MAX_VALUE);
        Mockito.when(coordinatorContext.withLock(ArgumentMatchers.<Callable<Object>>any()))
                .thenAnswer(invocation -> invocation.<Callable<Object>>getArgument(0).call());
        Mockito.when(coordinatorContext.readCloneStatus())
                .thenReturn(new Status(TStatusCode.TIMEOUT, "timeout before fragment dispatch"));

        MultiFragmentsPipelineTask fragmentsTask = mockFragmentTask(10001L);
        PipelineExecutionTask executionTask = new PipelineExecutionTask(
                coordinatorContext,
                Mockito.mock(BackendServiceProxy.class),
                Collections.singletonMap(Mockito.mock(BackendWorker.class), fragmentsTask));

        UserException exception = Assertions.assertThrows(UserException.class, executionTask::execute);

        Assertions.assertTrue(exception.getMessage().contains("timeout before fragment dispatch"));
        Mockito.verify(fragmentsTask, Mockito.never()).sendPhaseOneRpc(ArgumentMatchers.anyBoolean());
    }

    @Test
    void loadProcessorCancelToleratesTaskPublishedBeforeLatch() {
        LoadProcessor processor = Mockito.mock(LoadProcessor.class, Mockito.CALLS_REAL_METHODS);
        PipelineExecutionTask task = Mockito.mock(PipelineExecutionTask.class);
        Mockito.when(task.getChildrenTasks()).thenReturn(Collections.emptyMap());
        Deencapsulation.setField(processor, "executionTask", java.util.Optional.of(task));
        // AfterSetPipelineExecutionTask has not created the latch yet (Objenesis skipped the constructor).
        Deencapsulation.setField(processor, "latch", java.util.Optional.empty());
        // latch deliberately stays Optional.empty(): the window between executionTask publication and
        // afterSetPipelineExecutionTask creating the latch must not throw out of the coordinator cleanup.
        Assertions.assertDoesNotThrow(() ->
                processor.cancel(new Status(TStatusCode.TIMEOUT, "cancel during publication")));
    }

    private static MultiFragmentsPipelineTask mockFragmentTask(long backendId) {
        MultiFragmentsPipelineTask task = Mockito.mock(MultiFragmentsPipelineTask.class);
        Backend backend = Mockito.mock(Backend.class);
        Mockito.when(backend.getId()).thenReturn(backendId);
        Mockito.when(task.getBackend()).thenReturn(backend);
        Mockito.when(task.getChildrenTasks()).thenReturn(Collections.emptyMap());
        return task;
    }
}
