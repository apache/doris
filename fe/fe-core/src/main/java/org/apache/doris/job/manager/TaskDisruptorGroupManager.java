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

package org.apache.doris.job.manager;

import org.apache.doris.common.CustomThreadFactory;
import org.apache.doris.job.base.AbstractJob;
import org.apache.doris.job.base.JobExecutionConfiguration;
import org.apache.doris.job.common.JobType;
import org.apache.doris.job.disruptor.ExecuteTaskEvent;
import org.apache.doris.job.disruptor.TaskDisruptor;
import org.apache.doris.job.disruptor.TimerJobEvent;
import org.apache.doris.job.executor.DefaultTaskExecutorHandler;
import org.apache.doris.job.executor.DispatchTaskHandler;
import org.apache.doris.job.extensions.insert.InsertTask;
import org.apache.doris.job.task.AbstractTask;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventTranslatorVararg;
import com.lmax.disruptor.WorkHandler;
import lombok.Getter;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.ThreadFactory;

public class TaskDisruptorGroupManager<T extends AbstractTask> {

    private final Map<JobType, TaskDisruptor<T>> disruptorMap = new EnumMap<>(JobType.class);

    @Getter
    private TaskDisruptor<TimerJobEvent<AbstractJob<?>>> dispatchDisruptor;


    public void init() {
        registerInsertDisruptor();
        //when all task queue is ready, dispatch task to registered task executor
        registerDispatchDisruptor();
    }

    private void registerDispatchDisruptor() {
        EventFactory<TimerJobEvent<AbstractJob>> dispatchEventFactory = TimerJobEvent.factory();
        ThreadFactory dispatchThreadFactory = new CustomThreadFactory("dispatch-task");
        WorkHandler[] dispatchTaskExecutorHandlers = new WorkHandler[5];
        for (int i = 0; i < 5; i++) {
            dispatchTaskExecutorHandlers[i] = new DispatchTaskHandler(this.disruptorMap);
        }
        EventTranslatorVararg<TimerJobEvent<AbstractJob>> eventTranslator =
                (event, sequence, args) -> event.setJob((AbstractJob) args[0]);
        this.dispatchDisruptor = new TaskDisruptor<>(dispatchEventFactory, 1024, dispatchThreadFactory,
                new BlockingWaitStrategy(), dispatchTaskExecutorHandlers, eventTranslator);
    }

    private void registerInsertDisruptor() {
        EventFactory<ExecuteTaskEvent<InsertTask>> insertEventFactory = ExecuteTaskEvent.factory();
        ThreadFactory insertTaskThreadFactory = new CustomThreadFactory("insert-task-execute");
        WorkHandler[] insertTaskExecutorHandlers = new WorkHandler[5];
        for (int i = 0; i < 5; i++) {
            insertTaskExecutorHandlers[i] = new DefaultTaskExecutorHandler<InsertTask>();
        }
        EventTranslatorVararg<ExecuteTaskEvent<InsertTask>> eventTranslator =
                (event, sequence, args) -> {
                    event.setTask((InsertTask) args[0]);
                    event.setJobConfig((JobExecutionConfiguration) args[1]);
                };
        TaskDisruptor insertDisruptor = new TaskDisruptor<>(insertEventFactory, 1024,
                insertTaskThreadFactory, new BlockingWaitStrategy(), insertTaskExecutorHandlers, eventTranslator);
        disruptorMap.put(JobType.INSERT, insertDisruptor);
    }

    public void dispatchTimerJob(AbstractJob job) {
        dispatchDisruptor.publishEvent(job);
    }

    public void dispatchInstantTask(AbstractTask task, JobType jobType,
                                    JobExecutionConfiguration jobExecutionConfiguration) {
        disruptorMap.get(jobType).publishEvent(task, jobExecutionConfiguration);
    }


}
