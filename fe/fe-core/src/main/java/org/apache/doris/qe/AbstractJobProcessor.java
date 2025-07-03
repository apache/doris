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
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.qe.runtime.BackendFragmentId;
import org.apache.doris.qe.runtime.MultiFragmentsPipelineTask;
import org.apache.doris.qe.runtime.PipelineExecutionTask;
import org.apache.doris.qe.runtime.SingleFragmentPipelineTask;
import org.apache.doris.thrift.TReportExecStatusParams;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;

/** AbstractJobProcessor */
public abstract class AbstractJobProcessor implements JobProcessor {
    private final Logger logger = LogManager.getLogger(getClass());

    protected final CoordinatorContext coordinatorContext;
    protected volatile Optional<PipelineExecutionTask> executionTask;
    protected volatile Optional<Map<BackendFragmentId, SingleFragmentPipelineTask>> backendFragmentTasks;

    public AbstractJobProcessor(CoordinatorContext coordinatorContext) {
        this.coordinatorContext = Objects.requireNonNull(coordinatorContext, "coordinatorContext can not be null");
        this.executionTask = Optional.empty();
        this.backendFragmentTasks = Optional.empty();
    }

    protected abstract void doProcessReportExecStatus(
            TReportExecStatusParams params, SingleFragmentPipelineTask fragmentTask);

    @Override
    public final void setPipelineExecutionTask(PipelineExecutionTask pipelineExecutionTask) {
        Preconditions.checkArgument(pipelineExecutionTask != null, "sqlPipelineTask can not be null");

        this.executionTask = Optional.of(pipelineExecutionTask);
        Map<BackendFragmentId, SingleFragmentPipelineTask> backendFragmentTasks
                = buildBackendFragmentTasks(pipelineExecutionTask);
        this.backendFragmentTasks = Optional.of(backendFragmentTasks);

        afterSetPipelineExecutionTask(pipelineExecutionTask);
    }

    protected void afterSetPipelineExecutionTask(PipelineExecutionTask pipelineExecutionTask) {}

    @Override
    public final void updateFragmentExecStatus(TReportExecStatusParams params) {
        SingleFragmentPipelineTask fragmentTask = backendFragmentTasks.get().get(
                new BackendFragmentId(params.getBackendId(), params.getFragmentId()));
        if (fragmentTask == null || !fragmentTask.processReportExecStatus(params)) {
            return;
        }

        TUniqueId queryId = coordinatorContext.queryId;
        Status status = new Status(params.status);
        // for now, abort the query if we see any error except if the error is cancelled
        // and returned_all_results_ is true.
        // (UpdateStatus() initiates cancellation, if it hasn't already been initiated)
        if (!status.ok()) {
            if (coordinatorContext.isEos() && status.isCancelled()) {
                logger.warn("Query {} has returned all results, fragment_id={} instance_id={}, be={}"
                                + " is reporting failed status {}",
                        DebugUtil.printId(queryId), params.getFragmentId(),
                        DebugUtil.printId(params.getFragmentInstanceId()),
                        params.getBackendId(),
                        status.toString());
            } else {
                logger.warn("one instance report fail, query_id={} fragment_id={} instance_id={}, be={},"
                                + " error message: {}",
                        DebugUtil.printId(queryId), params.getFragmentId(),
                        DebugUtil.printId(params.getFragmentInstanceId()),
                        params.getBackendId(), status.toString());
                coordinatorContext.updateStatusIfOk(status);
            }
        }
        doProcessReportExecStatus(params, fragmentTask);
    }

    private Map<BackendFragmentId, SingleFragmentPipelineTask> buildBackendFragmentTasks(
            PipelineExecutionTask executionTask) {
        ImmutableMap.Builder<BackendFragmentId, SingleFragmentPipelineTask> backendFragmentTasks
                = ImmutableMap.builder();
        for (Entry<Long, MultiFragmentsPipelineTask> backendTask : executionTask.getChildrenTasks().entrySet()) {
            Long backendId = backendTask.getKey();
            for (Entry<Integer, SingleFragmentPipelineTask> fragmentIdToTask : backendTask.getValue()
                    .getChildrenTasks().entrySet()) {
                Integer fragmentId = fragmentIdToTask.getKey();
                SingleFragmentPipelineTask fragmentTask = fragmentIdToTask.getValue();
                backendFragmentTasks.put(new BackendFragmentId(backendId, fragmentId), fragmentTask);
            }
        }
        return backendFragmentTasks.build();
    }
}
