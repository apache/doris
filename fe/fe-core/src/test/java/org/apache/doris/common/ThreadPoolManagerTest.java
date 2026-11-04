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

package org.apache.doris.common;

import org.apache.doris.common.util.FairTaskExecutor;
import org.apache.doris.datasource.ExternalMetaCacheMgr;
import org.apache.doris.metric.Metric;
import org.apache.doris.metric.MetricRepo;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ThreadPoolManagerTest {

    @Test(timeout = 30000)
    public void testExternalScanSchedulersExportMetrics() throws Exception {
        int splitThreads = Config.external_scan_split_threads;
        int metadataThreads = Config.external_scan_metadata_threads;
        try {
            Config.external_scan_split_threads = 1;
            Config.external_scan_metadata_threads = 1;
            for (boolean checkpoint : new boolean[] {false, true}) {
                ExternalMetaCacheMgr manager = new ExternalMetaCacheMgr(checkpoint);
                try (FairTaskExecutor split = manager.getScanTaskExecutor(false);
                        FairTaskExecutor metadata = manager.getScanTaskExecutor(true)) {
                    ThreadPoolManager.registerAllThreadPoolMetric();
                    String prefix = checkpoint ? "Checkpoint" : "NotCheckpoint";
                    verifyFairTaskMetrics(split, prefix + "ExternalSplit");
                    verifyFairTaskMetrics(metadata, prefix + "ExternalMetadata");
                }
            }
        } finally {
            Config.external_scan_split_threads = splitThreads;
            Config.external_scan_metadata_threads = metadataThreads;
        }
    }

    private void verifyFairTaskMetrics(FairTaskExecutor executor, String name) throws Exception {
        Assert.assertEquals(0, metricValue(name, "pool_size"));
        Assert.assertEquals(0, metricValue(name, "active_thread_num"));
        Assert.assertEquals(0, metricValue(name, "task_in_queue"));
        Assert.assertEquals(0, metricValue(name, "completed_task_count"));
        Assert.assertEquals(0, metricValue(name, "task_group_num"));
        Assert.assertEquals(Metric.MetricType.GAUGE, metric(name, "task_in_queue").getType());
        Assert.assertEquals(Metric.MetricType.COUNTER, metric(name, "completed_task_count").getType());

        FairTaskExecutor.Group group = executor.newGroup(2);
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch completed = new CountDownLatch(1);
        try {
            group.execute(() -> {
                started.countDown();
                try {
                    Assert.assertTrue(release.await(10, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                }
            });
            Assert.assertTrue(started.await(10, TimeUnit.SECONDS));
            group.execute(completed::countDown);
            Assert.assertEquals(1, metricValue(name, "pool_size"));
            Assert.assertEquals(1, metricValue(name, "active_thread_num"));
            Assert.assertEquals(1, metricValue(name, "task_in_queue"));
            Assert.assertEquals(1, metricValue(name, "task_group_num"));
            Assert.assertEquals(0, metricValue(name, "completed_task_count"));
            release.countDown();
            Assert.assertTrue(completed.await(10, TimeUnit.SECONDS));
        } finally {
            release.countDown();
            executor.close();
            Assert.assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        }
        Assert.assertEquals(0, metricValue(name, "active_thread_num"));
        Assert.assertEquals(0, metricValue(name, "task_in_queue"));
        Assert.assertEquals(0, metricValue(name, "task_group_num"));
        Assert.assertEquals(2, metricValue(name, "completed_task_count"));
    }

    private long metricValue(String name, String type) {
        return ((Number) metric(name, type).getValue()).longValue();
    }

    private Metric<?> metric(String name, String type) {
        List<Metric<?>> metrics = MetricRepo.getMetricsByName("thread_pool").stream()
                .<Metric<?>>map(metric -> metric)
                .filter(metric -> metric.getLabels().stream()
                        .anyMatch(label -> label.getKey().equals("name") && label.getValue().equals(name)))
                .filter(metric -> metric.getLabels().stream()
                        .anyMatch(label -> label.getKey().equals("type") && label.getValue().equals(type)))
                .collect(Collectors.toList());
        Assert.assertEquals(name + "/" + type, 1, metrics.size());
        return metrics.get(0);
    }

    @Test
    public void testBlockedPolicySkipsCancelledTask() {
        LinkedBlockingQueue<Runnable> queue = new LinkedBlockingQueue<>(1);
        Runnable queued = () -> { };
        queue.add(queued);
        ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS, queue);
        FutureTask<Void> cancelled = new FutureTask<>(() -> null);
        cancelled.cancel(false);

        new ThreadPoolManager.BlockedPolicy("test", 10).rejectedExecution(cancelled, executor);

        Assert.assertEquals(1, queue.size());
        Assert.assertSame(queued, queue.peek());
        executor.shutdownNow();
    }

    @Test
    public void testBlockedPolicyRestoresInterrupt() {
        LinkedBlockingQueue<Runnable> queue = new LinkedBlockingQueue<>(1);
        queue.add(() -> { });
        ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS, queue);
        FutureTask<Void> task = new FutureTask<>(() -> null);
        ThreadPoolManager.BlockedPolicy blockedPolicy = new ThreadPoolManager.BlockedPolicy("test", 10);

        try {
            Thread.currentThread().interrupt();
            Assert.assertThrows(RejectedExecutionException.class,
                    () -> blockedPolicy.rejectedExecution(task, executor));
            Assert.assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
            executor.shutdownNow();
        }
    }

    @Test
    public void testNormal() throws InterruptedException {
        ThreadPoolExecutor testCachedPool = ThreadPoolManager.newDaemonCacheThreadPool(2, "test_cache_pool", true);
        ThreadPoolExecutor testFixedThreaddPool = ThreadPoolManager.newDaemonFixedThreadPool(2, 2,
                "test_fixed_thread_pool", true);

        ThreadPoolManager.registerThreadPoolMetric("test_cache_pool", testCachedPool);
        ThreadPoolManager.registerThreadPoolMetric("test_fixed_thread_pool", testFixedThreaddPool);

        Assert.assertEquals(ThreadPoolManager.LogDiscardPolicy.class,
                testCachedPool.getRejectedExecutionHandler().getClass());
        Assert.assertEquals(ThreadPoolManager.BlockedPolicy.class,
                testFixedThreaddPool.getRejectedExecutionHandler().getClass());

        Runnable task = () -> {
            try {
                Thread.sleep(500);
            } catch (Exception e) {
                e.printStackTrace();
            }
        };
        for (int i = 0; i < 4; i++) {
            testCachedPool.submit(task);
        }

        Assert.assertEquals(2, testCachedPool.getPoolSize());
        Assert.assertEquals(2, testCachedPool.getActiveCount());
        Assert.assertEquals(0, testCachedPool.getQueue().size());
        Assert.assertEquals(0, testCachedPool.getCompletedTaskCount());

        Thread.sleep(700);

        Assert.assertEquals(2, testCachedPool.getPoolSize());
        Assert.assertEquals(0, testCachedPool.getActiveCount());
        Assert.assertEquals(0, testCachedPool.getQueue().size());
        Assert.assertEquals(2, testCachedPool.getCompletedTaskCount());

        for (int i = 0; i < 4; i++) {
            testFixedThreaddPool.submit(task);
        }

        Assert.assertTrue(testFixedThreaddPool.getActiveCount() <= 2);
        Assert.assertTrue(testFixedThreaddPool.getQueue().size() > 0);
        Assert.assertEquals(2, testFixedThreaddPool.getPoolSize());
        Assert.assertEquals(0, testFixedThreaddPool.getCompletedTaskCount());

        Thread.sleep(2000);

        Assert.assertEquals(2, testFixedThreaddPool.getPoolSize());
        Assert.assertEquals(0, testFixedThreaddPool.getActiveCount());
        Assert.assertEquals(0, testFixedThreaddPool.getQueue().size());
        Assert.assertEquals(4, testFixedThreaddPool.getCompletedTaskCount());
    }
}
