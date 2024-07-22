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

import org.apache.doris.common.Config;
import org.apache.doris.common.CustomThreadFactory;
import org.apache.doris.job.base.AbstractJob;
import org.apache.doris.job.base.JobExecutionConfiguration;
import org.apache.doris.job.common.JobType;
import org.apache.doris.job.disruptor.ExecuteTaskEvent;
import org.apache.doris.job.disruptor.TaskDisruptor;
import org.apache.doris.job.disruptor.TimerJobEvent;
import org.apache.doris.job.executor.DefaultTaskExecutorHandler;
import org.apache.doris.job.executor.DispatchTaskHandler;
import org.apache.doris.job.extensions.insert.BatchInsertTask;
import org.apache.doris.job.extensions.insert.InsertTask;
import org.apache.doris.job.extensions.mtmv.MTMVTask;
import org.apache.doris.job.task.AbstractTask;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventTranslatorVararg;
import com.lmax.disruptor.WorkHandler;
import lombok.Getter;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadFactory;

public class TaskDisruptorGroupManager<T extends AbstractTask> {

    private final Map<JobType, TaskDisruptor<T>> disruptorMap = new EnumMap<>(JobType.class);

    @Getter
    private TaskDisruptor<TimerJobEvent<AbstractJob>> dispatchDisruptor;

    private static final int DEFAULT_RING_BUFFER_SIZE = 1024;

    private static final int DEFAULT_CONSUMER_THREAD_NUM = 5;

    private static final int DISPATCH_TIMER_JOB_QUEUE_SIZE = Config.job_dispatch_timer_job_queue_size > 0
            ? Config.job_dispatch_timer_job_queue_size : DEFAULT_RING_BUFFER_SIZE;

    private static final int DISPATCH_TIMER_JOB_CONSUMER_THREAD_NUM = Config.job_dispatch_timer_job_thread_num > 0
            ? Config.job_dispatch_timer_job_thread_num : DEFAULT_CONSUMER_THREAD_NUM;

    private static final int DISPATCH_INSERT_THREAD_NUM = Config.job_insert_task_consumer_thread_num > 0
            ? Config.job_insert_task_consumer_thread_num : DEFAULT_CONSUMER_THREAD_NUM;

    private static final int DISPATCH_MTMV_THREAD_NUM = Config.job_mtmv_task_consumer_thread_num > 0
            ? Config.job_mtmv_task_consumer_thread_num : DEFAULT_CONSUMER_THREAD_NUM;

    private static final int DISPATCH_INSERT_TASK_QUEUE_SIZE = DEFAULT_RING_BUFFER_SIZE;
    private static final int DISPATCH_MTMV_TASK_QUEUE_SIZE = DEFAULT_RING_BUFFER_SIZE;

    public void init() {
        registerInsertDisruptor();
        registerMTMVDisruptor();
        registerBatchInsertDisruptor();
        //when all task queue is ready, dispatch task to registered task executor
        registerDispatchDisruptor();
    }

    private void registerDispatchDisruptor() {
        EventFactory<TimerJobEvent<AbstractJob>> dispatchEventFactory = TimerJobEvent.factory();
        ThreadFactory dispatchThreadFactory = new CustomThreadFactory("dispatch-task");
        WorkHandler[] dispatchTaskExecutorHandlers = new WorkHandler[DISPATCH_TIMER_JOB_CONSUMER_THREAD_NUM];
        for (int i = 0; i < DISPATCH_TIMER_JOB_CONSUMER_THREAD_NUM; i++) {
            dispatchTaskExecutorHandlers[i] = new DispatchTaskHandler(this.disruptorMap);
        }
        EventTranslatorVararg<TimerJobEvent<AbstractJob>> eventTranslator =
                (event, sequence, args) -> event.setJob((AbstractJob) args[0]);
        this.dispatchDisruptor = new TaskDisruptor<>(dispatchEventFactory, DISPATCH_TIMER_JOB_QUEUE_SIZE,
                dispatchThreadFactory,
                new BlockingWaitStrategy(), dispatchTaskExecutorHandlers, eventTranslator);
    }

    private void registerInsertDisruptor() {
        EventFactory<ExecuteTaskEvent<InsertTask>> insertEventFactory = ExecuteTaskEvent.factory();
        ThreadFactory insertTaskThreadFactory = new CustomThreadFactory("insert-task-execute");
        WorkHandler[] insertTaskExecutorHandlers = new WorkHandler[DISPATCH_INSERT_THREAD_NUM];
        for (int i = 0; i < DISPATCH_INSERT_THREAD_NUM; i++) {
            insertTaskExecutorHandlers[i] = new DefaultTaskExecutorHandler<InsertTask>();
        }
        EventTranslatorVararg<ExecuteTaskEvent<InsertTask>> eventTranslator =
                (event, sequence, args) -> {
                    event.setTask((InsertTask) args[0]);
                    event.setJobConfig((JobExecutionConfiguration) args[1]);
                };
        TaskDisruptor insertDisruptor = new TaskDisruptor<>(insertEventFactory, DISPATCH_INSERT_TASK_QUEUE_SIZE,
                insertTaskThreadFactory, new BlockingWaitStrategy(), insertTaskExecutorHandlers, eventTranslator);
        disruptorMap.put(JobType.INSERT, insertDisruptor);
    }

    private void registerBatchInsertDisruptor() {
        EventFactory<ExecuteTaskEvent<BatchInsertTask>> insertEventFactory = ExecuteTaskEvent.factory();
        ThreadFactory insertTaskThreadFactory = new CustomThreadFactory("insert-task-execute");
        WorkHandler[] insertTaskExecutorHandlers = new WorkHandler[DISPATCH_INSERT_THREAD_NUM];
        for (int i = 0; i < DISPATCH_INSERT_THREAD_NUM; i++) {
            insertTaskExecutorHandlers[i] = new DefaultTaskExecutorHandler<InsertTask>();
        }
        EventTranslatorVararg<ExecuteTaskEvent<BatchInsertTask>> eventTranslator =
                (event, sequence, args) -> {
                    event.setTask((BatchInsertTask) args[0]);
                    event.setJobConfig((JobExecutionConfiguration) args[1]);
                };
        TaskDisruptor insertDisruptor = new TaskDisruptor<>(insertEventFactory, DISPATCH_INSERT_TASK_QUEUE_SIZE,
                insertTaskThreadFactory, new BlockingWaitStrategy(), insertTaskExecutorHandlers, eventTranslator);
        disruptorMap.put(JobType.BATCH_INSERT, insertDisruptor);
    }

    private void registerMTMVDisruptor() {
        EventFactory<ExecuteTaskEvent<MTMVTask>> mtmvEventFactory = ExecuteTaskEvent.factory();
        ThreadFactory mtmvTaskThreadFactory = new CustomThreadFactory("mtmv-task-execute");
        WorkHandler[] insertTaskExecutorHandlers = new WorkHandler[DISPATCH_MTMV_THREAD_NUM];
        for (int i = 0; i < DISPATCH_MTMV_THREAD_NUM; i++) {
            insertTaskExecutorHandlers[i] = new DefaultTaskExecutorHandler<MTMVTask>();
        }
        EventTranslatorVararg<ExecuteTaskEvent<MTMVTask>> eventTranslator =
                (event, sequence, args) -> {
                    event.setTask((MTMVTask) args[0]);
                    event.setJobConfig((JobExecutionConfiguration) args[1]);
                };
        TaskDisruptor mtmvDisruptor = new TaskDisruptor<>(mtmvEventFactory, DISPATCH_MTMV_TASK_QUEUE_SIZE,
                mtmvTaskThreadFactory, new BlockingWaitStrategy(), insertTaskExecutorHandlers, eventTranslator);
        disruptorMap.put(JobType.MV, mtmvDisruptor);
    }

    public void dispatchTimerJob(AbstractJob job) {
        dispatchDisruptor.publishEvent(job);
    }

    public void dispatchInstantTask(AbstractTask task, JobType jobType,
                                    JobExecutionConfiguration jobExecutionConfiguration) {
        disruptorMap.get(jobType).publishEvent(task, jobExecutionConfiguration);
    }

    public void dispatchInstantTasks(List<AbstractTask> task, JobType jobType, long groupId,
                                     JobExecutionConfiguration jobExecutionConfiguration) {
       /* int maxConcurrentTaskNum = jobExecutionConfiguration.getMaxConcurrentTaskNum();
        if(task.size() <= maxConcurrentTaskNum) {
            task.forEach(t -> dispatchInstantTask(t, jobType, jobExecutionConfiguration));
            return;
        }
        // when task size is larger than maxConcurrentTaskNum, we need to dispatch task one by one
        currentTaskMap.putIfAbsent(groupId, new ConcurrentLinkedQueue<>());
        ConcurrentLinkedQueue<AbstractTask> taskQueue = currentTaskMap.get(groupId);
        taskQueue.addAll(task);
        if (taskQueue.size() <= maxConcurrentTaskNum) {
            disruptorMap.get(jobType).publishEvent(task);
        }*/
    }

    /*@Subscribe
    public void onTaskDispatchEvent(TaskDispatchEvent event) {
        AbstractTask task = null;

        switch (event.getTaskDispatchOperate()) {
            case EXECUTE_GROUP_NEXT_TASK:
                if (null == currentTaskMap.get(event.getGroupId())) {
                    return;
                }
                currentTaskMap.get(event.getGroupId()).remove(event.getLastCompletedTaskId());
                task = currentTaskMap.get(event.getGroupId()).poll();
                if (currentTaskMap.get(event.getGroupId()).isEmpty()) {
                    currentTaskMap.remove(event.getGroupId());
                }
                break;
            case DROP_GROUP_TASK:
                currentTaskMap.remove(event.getGroupId());
                break;
            case DROP_ALL_TASK:
                if (CollectionUtils.isNotEmpty(event.getGroupIds())) {
                    event.getGroupIds().forEach(currentTaskMap::remove);
                }
                break;
            default:
                break;
        }
        if (null != task) {
            disruptorMap.get(event.getJobType()).publishEvent(task);
        }
    }*/

}
