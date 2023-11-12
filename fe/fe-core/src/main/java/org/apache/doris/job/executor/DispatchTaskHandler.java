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

package org.apache.doris.job.executor;

import org.apache.doris.job.base.AbstractJob;
import org.apache.doris.job.common.JobType;
import org.apache.doris.job.disruptor.TaskDisruptor;
import org.apache.doris.job.disruptor.TimerJobEvent;
import org.apache.doris.job.task.AbstractTask;

import com.lmax.disruptor.WorkHandler;
import jline.internal.Log;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;

@Slf4j
public class DispatchTaskHandler<T extends AbstractJob<?>> implements WorkHandler<TimerJobEvent<T>> {

    private final Map<JobType, TaskDisruptor<T>> disruptorMap;

    public DispatchTaskHandler(Map<JobType, TaskDisruptor<T>> disruptorMap) {
        this.disruptorMap = disruptorMap;
    }


    @Override
    public void onEvent(TimerJobEvent<T> event) throws Exception {
        try {
            if (null == event.getJob()) {
                log.info("job is null,may be job is deleted, ignore");
                return;
            }
            if (event.getJob().isReadyForScheduling()) {
                List<? extends AbstractTask> tasks = event.getJob().createTasks();
                JobType jobType = event.getJob().getJobType();
                for (AbstractTask task : tasks) {
                    disruptorMap.get(jobType).publishEvent(task);
                }
            }
        } catch (Exception e) {
            Log.warn("dispatch timer job error, task id is {}", event.getJob().getJobId(), e);
        }
    }
}
