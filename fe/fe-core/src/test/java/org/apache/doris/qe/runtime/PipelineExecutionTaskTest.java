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
import org.apache.doris.proto.InternalService.PExecPlanFragmentResult;
import org.apache.doris.qe.CoordinatorContext;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.thrift.TQueryOptions;
import org.apache.doris.thrift.TUniqueId;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

class PipelineExecutionTaskTest {
    @Test
    void expiredDeadlineCancelsAlreadySubmittedFragments() throws Exception {
        CoordinatorContext coordinatorContext = Mockito.mock(CoordinatorContext.class);
        TQueryOptions queryOptions = new TQueryOptions();
        queryOptions.setExecutionTimeout(1);
        queryOptions.setQueryTimeout(1);
        Deencapsulation.setField(coordinatorContext, "queryOptions", queryOptions);
        Deencapsulation.setField(coordinatorContext, "queryId", new TUniqueId(1, 2));
        Deencapsulation.setField(coordinatorContext, "timeoutDeadline", (Supplier<Long>) () -> 0L);
        Mockito.when(coordinatorContext.withLock(ArgumentMatchers.<Callable<Object>>any()))
                .thenAnswer(invocation -> invocation.<Callable<Object>>getArgument(0).call());
        Mockito.when(coordinatorContext.twoPhaseExecution()).thenReturn(false);

        MultiFragmentsPipelineTask fragmentsTask = Mockito.mock(MultiFragmentsPipelineTask.class);
        Mockito.when(fragmentsTask.getChildrenTasks()).thenReturn(Collections.emptyMap());
        Mockito.when(fragmentsTask.sendPhaseOneRpc(false))
                .thenReturn(CompletableFuture.completedFuture(PExecPlanFragmentResult.getDefaultInstance()));
        PipelineExecutionTask executionTask = new PipelineExecutionTask(
                coordinatorContext,
                Mockito.mock(BackendServiceProxy.class),
                Collections.singletonMap(Mockito.mock(BackendWorker.class), fragmentsTask));

        UserException exception = Assertions.assertThrows(UserException.class, executionTask::execute);

        Assertions.assertTrue(exception.getMessage().contains("timeout before waiting send fragments rpc"));
        Mockito.verify(fragmentsTask).sendPhaseOneRpc(false);
        Mockito.verify(coordinatorContext).updateStatusIfOk(ArgumentMatchers.argThat(
                status -> hasDeadlineTimeoutMessage(status)));
        Mockito.verify(coordinatorContext).cancelSchedule(ArgumentMatchers.argThat(
                status -> hasDeadlineTimeoutMessage(status)));
    }

    private static boolean hasDeadlineTimeoutMessage(Status status) {
        return status.getErrorMsg().contains("timeout before waiting send fragments rpc");
    }
}
