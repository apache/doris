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

package org.apache.doris.cloud;

import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

public class CacheHotspotManagerSchedulerTest {
    private boolean originalRunningUnitTest;
    private int originalMaxActiveCloudWarmUpJob;
    private ThreadPoolExecutor executor;
    private CacheHotspotManager manager;

    @BeforeEach
    public void setUp() {
        originalRunningUnitTest = FeConstants.runningUnitTest;
        originalMaxActiveCloudWarmUpJob = Config.max_active_cloud_warm_up_job;
        FeConstants.runningUnitTest = false;
        Config.max_active_cloud_warm_up_job = 2;

        executor = Mockito.mock(ThreadPoolExecutor.class);
        Mockito.when(executor.getMaximumPoolSize()).thenReturn(2);
        manager = new CacheHotspotManager(Mockito.mock(CloudSystemInfoService.class), executor);
    }

    @AfterEach
    public void tearDown() {
        FeConstants.runningUnitTest = originalRunningUnitTest;
        Config.max_active_cloud_warm_up_job = originalMaxActiveCloudWarmUpJob;
    }

    @Test
    public void testNewOnceJobGetsFirstTurnAndJobsRotate() throws Exception {
        List<Long> runOrder = new ArrayList<>();
        Mockito.doAnswer(invocation -> {
            ((Runnable) invocation.getArgument(0)).run();
            return null;
        }).when(executor).execute(Mockito.any(Runnable.class));

        manager.addCloudWarmUpJob(mockJob(1L, false, 1L, runOrder));
        manager.addCloudWarmUpJob(mockJob(2L, false, 2L, runOrder));
        manager.addCloudWarmUpJob(mockJob(3L, false, 3L, runOrder));
        manager.addCloudWarmUpJob(mockJob(4L, true, 4L, runOrder));

        manager.runCloudWarmUpJob();
        manager.runCloudWarmUpJob();
        manager.runCloudWarmUpJob();
        manager.runCloudWarmUpJob();

        Assertions.assertEquals(Arrays.asList(4L, 1L, 2L, 3L, 4L, 1L, 2L, 3L), runOrder);
    }

    @Test
    public void testActiveJobIsNotSubmittedAgain() throws Exception {
        List<Runnable> submittedTasks = new ArrayList<>();
        Mockito.doAnswer(invocation -> {
            submittedTasks.add(invocation.getArgument(0));
            return null;
        }).when(executor).execute(Mockito.any(Runnable.class));

        CloudWarmUpJob job = mockJob(1L, true, 1L, new ArrayList<>());
        manager.addCloudWarmUpJob(job);

        manager.runCloudWarmUpJob();
        manager.runCloudWarmUpJob();
        Assertions.assertEquals(1, submittedTasks.size());

        submittedTasks.get(0).run();
        manager.runCloudWarmUpJob();
        Assertions.assertEquals(2, submittedTasks.size());
        submittedTasks.get(1).run();
        Mockito.verify(job, Mockito.times(2)).run();
    }

    @Test
    public void testRejectedJobIsRetried() throws Exception {
        AtomicInteger submitCount = new AtomicInteger();
        Mockito.doAnswer(invocation -> {
            if (submitCount.incrementAndGet() == 1) {
                throw new RejectedExecutionException("injected rejection");
            }
            ((Runnable) invocation.getArgument(0)).run();
            return null;
        }).when(executor).execute(Mockito.any(Runnable.class));

        CloudWarmUpJob job = mockJob(1L, true, 1L, new ArrayList<>());
        manager.addCloudWarmUpJob(job);

        manager.runCloudWarmUpJob();
        Mockito.verify(job, Mockito.never()).run();
        manager.runCloudWarmUpJob();

        Assertions.assertEquals(2, submitCount.get());
        Mockito.verify(job, Mockito.times(1)).run();
    }

    @Test
    public void testThreadPoolSizeFollowsMutableConfig() {
        Config.max_active_cloud_warm_up_job = 3;
        Mockito.when(executor.getMaximumPoolSize()).thenReturn(1);

        manager.runCloudWarmUpJob();

        Mockito.verify(executor).setMaximumPoolSize(3);
    }

    private CloudWarmUpJob mockJob(long jobId, boolean once, long createTimeMs, List<Long> runOrder) {
        CloudWarmUpJob job = Mockito.mock(CloudWarmUpJob.class);
        Mockito.when(job.getJobId()).thenReturn(jobId);
        Mockito.when(job.isOnce()).thenReturn(once);
        Mockito.when(job.getCreateTimeMs()).thenReturn(createTimeMs);
        Mockito.when(job.shouldWait()).thenReturn(false);
        Mockito.when(job.isDone()).thenReturn(false);
        Mockito.doAnswer(invocation -> {
            runOrder.add(jobId);
            return null;
        }).when(job).run();
        return job;
    }
}
