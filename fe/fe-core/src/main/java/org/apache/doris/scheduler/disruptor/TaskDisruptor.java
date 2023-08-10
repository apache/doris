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

import org.apache.doris.scheduler.constants.TaskType;
import org.apache.doris.scheduler.manager.TimerJobManager;
import org.apache.doris.scheduler.manager.TransientTaskManager;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.EventTranslatorTwoArg;
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

    private final Disruptor<TaskEvent> disruptor;
    private static final int DEFAULT_RING_BUFFER_SIZE = 1024;

    /**
     * The default timeout for {@link #close()} in seconds.
     */
    private static final int DEFAULT_CLOSE_WAIT_TIME_SECONDS = 5;

    /**
     * The default number of consumers to create for each {@link Disruptor} instance.
     */
    private static final int DEFAULT_CONSUMER_COUNT = System.getProperty("event.task.disruptor.consumer.count")
            == null ? Runtime.getRuntime().availableProcessors()
            : Integer.parseInt(System.getProperty("event.task.disruptor.consumer.count"));

    /**
     * Whether this disruptor has been closed.
     * if true, then we can't publish any more events.
     */
    private boolean isClosed = false;

    /**
     * The default {@link EventTranslatorOneArg} to use for {@link #tryPublish(Long)}.
     * This is used to avoid creating a new object for each publish.
     */
    private static final EventTranslatorTwoArg<TaskEvent, Long, TaskType> TRANSLATOR
            = (event, sequence, jobId, taskType) -> {
                event.setId(jobId);
                event.setTaskType(taskType);
            };

    public TaskDisruptor(TimerJobManager timerJobManager, TransientTaskManager transientTaskManager) {
        ThreadFactory producerThreadFactory = DaemonThreadFactory.INSTANCE;
        disruptor = new Disruptor<>(TaskEvent.FACTORY, DEFAULT_RING_BUFFER_SIZE, producerThreadFactory,
                ProducerType.SINGLE, new BlockingWaitStrategy());
        WorkHandler<TaskEvent>[] workers = new TaskHandler[DEFAULT_CONSUMER_COUNT];
        for (int i = 0; i < DEFAULT_CONSUMER_COUNT; i++) {
            workers[i] = new TaskHandler(timerJobManager, transientTaskManager);
        }
        disruptor.handleEventsWithWorkerPool(workers);
        disruptor.start();
    }

    /**
     * Publishes a job to the disruptor.
     *
     * @param jobId  job id
     */
    public void tryPublish(Long jobId) {
        if (isClosed) {
            log.info("tryPublish failed, disruptor is closed, jobId: {}", jobId);
            return;
        }
        try {
            disruptor.publishEvent(TRANSLATOR, jobId, TaskType.TimerJobTask);
        } catch (Exception e) {
            log.error("tryPublish failed, jobId: {}", jobId, e);
        }
    }

    /**
     * Publishes a task to the disruptor.
     *
     * @param taskId  task id
     */
    public void tryPublishTask(Long taskId) {
        if (isClosed) {
            log.info("tryPublish failed, disruptor is closed, taskId: {}", taskId);
            return;
        }
        try {
            disruptor.publishEvent(TRANSLATOR, taskId, TaskType.TransientTask);
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
