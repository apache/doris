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

import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.job.base.JobExecuteType;
import org.apache.doris.job.base.JobExecutionConfiguration;
import org.apache.doris.job.common.JobStatus;
import org.apache.doris.job.common.JobType;
import org.apache.doris.job.common.TaskStatus;
import org.apache.doris.job.common.TaskType;
import org.apache.doris.job.disruptor.TimerJobEvent;
import org.apache.doris.job.executor.DispatchTaskHandler;
import org.apache.doris.job.executor.TaskProcessor;
import org.apache.doris.job.extensions.insert.InsertTask;
import org.apache.doris.job.extensions.insert.streaming.StreamingInsertJob;
import org.apache.doris.job.extensions.insert.streaming.StreamingJobSchedulerTask;
import org.apache.doris.job.extensions.mtmv.MTMVTask;
import org.apache.doris.job.scheduler.JobScheduler;
import org.apache.doris.job.task.AbstractTask;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class TaskDisruptorGroupManagerTest {

    private final CountDownLatch release = new CountDownLatch(1);
    private final TaskDisruptorGroupManager<AbstractTask> manager = new TaskDisruptorGroupManager<>();
    private Map<JobType, TaskProcessor> processors;
    private TaskProcessor streamingTaskProcessor;

    @BeforeEach
    public void setUp() {
        manager.init();
        processors = Deencapsulation.getField(manager, "disruptorMap");
        streamingTaskProcessor = Deencapsulation.getField(manager, "streamingTaskProcessor");
    }

    @AfterEach
    public void tearDown() {
        release.countDown();
        manager.getDispatchDisruptor().shutdown();
        processors.values().forEach(TaskProcessor::shutdown);
        streamingTaskProcessor.shutdown();
    }

    @Test
    public void testInsertSaturationDoesNotBlockStreamingControl() throws Exception {
        assertPoolIsolation(false);
    }

    @Test
    public void testStreamingControlSaturationDoesNotBlockInsert() throws Exception {
        assertPoolIsolation(true);
    }

    private void assertPoolIsolation(boolean blockStreaming) throws Exception {
        saturate(blockStreaming);
        CountDownLatch completed = new CountDownLatch(1);
        Assertions.assertTrue(submit(!blockStreaming, completed::countDown),
                "The other processor must accept work while this processor is full");
        Assertions.assertTrue(completed.await(5, TimeUnit.SECONDS));
    }

    private void saturate(boolean streaming) throws Exception {
        TaskProcessor processor = streaming ? streamingTaskProcessor : processors.get(JobType.INSERT);
        ThreadPoolExecutor executor = Deencapsulation.getField(processor, "executor");
        int workers = executor.getCorePoolSize();
        CountDownLatch started = new CountDownLatch(workers);
        for (int i = 0; i < workers; i++) {
            Assertions.assertTrue(submit(streaming, () -> {
                started.countDown();
                await(release);
            }));
        }
        Assertions.assertTrue(started.await(5, TimeUnit.SECONDS));
        int queueSize = executor.getQueue().remainingCapacity();
        for (int i = 0; i < queueSize; i++) {
            Assertions.assertTrue(submit(streaming, () -> await(release)));
        }
    }

    @Test
    public void testTimerRejectionAllowsNextTick() throws Exception {
        saturate(true);
        TestStreamingJob job = new TestStreamingJob();
        TimerJobEvent<StreamingInsertJob> event = new TimerJobEvent<>();
        event.setJob(job);
        DispatchTaskHandler<StreamingInsertJob> handler = new DispatchTaskHandler<>(processors, streamingTaskProcessor);
        handler.onEvent(event);
        Assertions.assertTrue(job.getRunningTasks().isEmpty());
        Assertions.assertTrue(job.isReadyForScheduling(null));
        Assertions.assertEquals(1L, job.completed.getCount());

        release.countDown();
        ThreadPoolExecutor executor = Deencapsulation.getField(streamingTaskProcessor, "executor");
        CountDownLatch drained = new CountDownLatch(1);
        // Wait for one worker to drain the bounded queue before submitting the next tick.
        Assertions.assertTrue(executor.getQueue().offer(drained::countDown, 5, TimeUnit.SECONDS));
        Assertions.assertTrue(drained.await(5, TimeUnit.SECONDS));
        handler.onEvent(event);
        Assertions.assertTrue(job.completed.await(5, TimeUnit.SECONDS));
    }

    @Test
    public void testInstantStreamingDispatchWhileInsertIsFull() throws Exception {
        saturate(false);
        TestStreamingJob job = new TestStreamingJob();
        JobScheduler<StreamingInsertJob, Map<Object, Object>> scheduler = new JobScheduler<>(Collections.emptyMap());
        Deencapsulation.setField(scheduler, "taskDisruptorGroupManager", manager);
        scheduler.schedulerInstantJob(job, TaskType.MANUAL, null);
        Assertions.assertTrue(job.completed.await(5, TimeUnit.SECONDS));
    }

    @Test
    public void testMtmvDispatchWhileInsertIsFull() throws Exception {
        saturate(false);
        CountDownLatch completed = new CountDownLatch(1);
        MTMVTask task = new MTMVTask() {
            @Override
            public void runTask() {
                completed.countDown();
            }
        };
        task.setStatus(TaskStatus.PENDING);
        JobExecutionConfiguration config = new JobExecutionConfiguration();
        config.setExecuteType(JobExecuteType.RECURRING);
        Assertions.assertTrue(manager.dispatchInstantTask(task, JobType.MV, config));
        Assertions.assertTrue(completed.await(5, TimeUnit.SECONDS));
    }

    private static class TestStreamingJob extends StreamingInsertJob {
        private final CountDownLatch completed = new CountDownLatch(1);

        TestStreamingJob() {
            setJobId(1L);
            setJobName("isolation");
            setJobStatus(JobStatus.PENDING);
            JobExecutionConfiguration config = new JobExecutionConfiguration();
            config.setExecuteType(JobExecuteType.STREAMING);
            setJobConfig(config);
        }

        @Override
        public List<StreamingJobSchedulerTask> createTasks(TaskType taskType, Map<Object, Object> context) {
            StreamingJobSchedulerTask task = new StreamingJobSchedulerTask(this) {
                @Override
                public void runTask() {
                    getRunningTasks().remove(this);
                    completed.countDown();
                }
            };
            List<StreamingJobSchedulerTask> tasks = Collections.singletonList(task);
            initTasks(tasks, taskType);
            return tasks;
        }
    }

    private boolean submit(boolean streaming, Runnable action) {
        JobExecutionConfiguration config = new JobExecutionConfiguration();
        config.setExecuteType(streaming ? JobExecuteType.STREAMING : JobExecuteType.RECURRING);
        return manager.dispatchInstantTask(task(streaming, action), JobType.INSERT, config);
    }

    private AbstractTask task(boolean streaming, Runnable action) {
        AbstractTask task;
        if (streaming) {
            task = new StreamingJobSchedulerTask(null) {
                @Override
                public void runTask() {
                    action.run();
                }
            };
        } else {
            task = new InsertTask("isolation", "db", "SELECT 1", null) {
                @Override
                public void runTask() {
                    action.run();
                }
            };
        }
        task.setStatus(TaskStatus.PENDING);
        return task;
    }

    private static void await(CountDownLatch latch) {
        try {
            Assertions.assertTrue(latch.await(30, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    @Test
    public void testInit() {
        Assertions.assertEquals(1024, TaskDisruptorGroupManager.normalizeRingbufferSize(1024));
        Assertions.assertEquals(1024, TaskDisruptorGroupManager.normalizeRingbufferSize(-1));
        Assertions.assertEquals(16, TaskDisruptorGroupManager.normalizeRingbufferSize(15));
        Assertions.assertEquals(1024, TaskDisruptorGroupManager.normalizeRingbufferSize(1023));
        Assertions.assertEquals(1024, TaskDisruptorGroupManager.normalizeRingbufferSize(-8));
        Assertions.assertEquals(2048, TaskDisruptorGroupManager.normalizeRingbufferSize(1025));
        Assertions.assertEquals(4096, TaskDisruptorGroupManager.normalizeRingbufferSize(2049));
    }
}
