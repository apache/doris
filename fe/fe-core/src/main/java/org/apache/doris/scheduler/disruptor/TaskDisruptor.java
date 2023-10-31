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

package org.apache.doris.scheduler.disruptor;

import org.apache.doris.common.Config;
import org.apache.doris.scheduler.constants.TaskType;
import org.apache.doris.scheduler.manager.TimerJobManager;
import org.apache.doris.scheduler.manager.TransientTaskManager;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventTranslatorThreeArg;
import com.lmax.disruptor.TimeoutException;
import com.lmax.disruptor.WorkHandler;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.DaemonThreadFactory;
import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * This class represents a disruptor for processing event tasks consumed by a Disruptor.
 *
 * <p>The work handler retrieves the associated event job and executes it if it is running.
 * If the event job is not running, the work handler logs an error message. If the event job execution fails,
 * the work handler logs an error message and pauses the event job.
 *
 * <p>The work handler also handles system events by scheduling batch scheduler tasks.
 */
@Slf4j
public class TaskDisruptor implements Closeable {

    private  Disruptor<TaskEvent> disruptor;
    
    private TimerJobManager timerJobManager;
    private TransientTaskManager transientTaskManager;
    private static final int DEFAULT_RING_BUFFER_SIZE = Config.async_task_queen_size;

    private static final int consumerThreadCount = Config.async_task_consumer_thread_num;

    /**
     * The default timeout for {@link #close()} in seconds.
     */
    private static final int DEFAULT_CLOSE_WAIT_TIME_SECONDS = 5;

    /**
     * Whether this disruptor has been closed.
     * if true, then we can't publish any more events.
     */
    private boolean isClosed = false;

    /**
     * The default {@link EventTranslatorThreeArg} to use for {@link #tryPublish(Long, Long)}.
     * This is used to avoid creating a new object for each publish.
     */
    private static final EventTranslatorThreeArg<TaskEvent, Long, Long, TaskType> TRANSLATOR
            = (event, sequence, jobId, taskId, taskType) -> {
                event.setId(jobId);
                event.setTaskId(taskId);
                event.setTaskType(taskType);
            };

    public TaskDisruptor(TimerJobManager timerJobManager, TransientTaskManager transientTaskManager) {
       this.timerJobManager = timerJobManager;
       this.transientTaskManager = transientTaskManager;
    }
    
    public void start() {
        ThreadFactory producerThreadFactory = DaemonThreadFactory.INSTANCE;
        disruptor = new Disruptor<>(TaskEvent.FACTORY, DEFAULT_RING_BUFFER_SIZE, producerThreadFactory,
                ProducerType.SINGLE, new BlockingWaitStrategy());
        WorkHandler<TaskEvent>[] workers = new TaskHandler[consumerThreadCount];
        for (int i = 0; i < consumerThreadCount; i++) {
            workers[i] = new TaskHandler(timerJobManager, transientTaskManager);
        }
        disruptor.handleEventsWithWorkerPool(workers);
        disruptor.start();
    }

    /**
     * Publishes a job to the disruptor.
     * Default task type is {@link TaskType#SCHEDULER_JOB_TASK}
     *
     * @param jobId job id
     */
    public void tryPublish(Long jobId, Long taskId) {
        this.tryPublish(jobId, taskId, TaskType.SCHEDULER_JOB_TASK);
    }


    /**
     * Publishes a job task to the disruptor.
     *
     * @param jobId    job id, describe which job this task belongs to
     * @param taskId   task id, it's linked to job id, we can get job detail by task id
     * @param taskType {@link TaskType}
     */
    public void tryPublish(Long jobId, Long taskId, TaskType taskType) {
        if (isClosed) {
            log.info("tryPublish failed, disruptor is closed, jobId: {}", jobId);
            return;
        }
        try {
            disruptor.publishEvent(TRANSLATOR, jobId, taskId, taskType);
        } catch (Exception e) {
            log.error("tryPublish failed, jobId: {}", jobId, e);
        }
    }

    /**
     * Publishes a task to the disruptor.
     * Default task type is {@link TaskType#TRANSIENT_TASK}
     *
     * @param taskId task id
     */
    public void tryPublishTask(Long taskId) {
        if (isClosed) {
            log.info("tryPublish failed, disruptor is closed, taskId: {}", taskId);
            return;
        }
        try {
            disruptor.publishEvent(TRANSLATOR, taskId, 0L, TaskType.TRANSIENT_TASK);
        } catch (Exception e) {
            log.error("tryPublish failed, taskId: {}", taskId, e);
        }
    }


    @Override
    public void close() {
        try {
            isClosed = true;
            // we can wait for 5 seconds, so that backlog can be committed
            disruptor.shutdown(DEFAULT_CLOSE_WAIT_TIME_SECONDS, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            log.warn("close disruptor failed", e);
        }
    }
}
