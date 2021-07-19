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

import org.apache.doris.metric.Metric;
import org.apache.doris.metric.MetricRepo;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;

public class ThreadPoolManagerTest {

    @Test
    public void testNormal() throws InterruptedException {
        ThreadPoolExecutor testCachedPool = ThreadPoolManager.newDaemonCacheThreadPool(2, "test_cache_pool", true);
        ThreadPoolExecutor testFixedThreaddPool = ThreadPoolManager.newDaemonFixedThreadPool(2, 2,
                "test_fixed_thread_pool", true);

        ThreadPoolManager.registerThreadPoolMetric("test_cache_pool", testCachedPool);
        ThreadPoolManager.registerThreadPoolMetric("test_fixed_thread_pool", testFixedThreaddPool);

        List<Metric> metricList = MetricRepo.getMetricsByName("thread_pool");

        Assert.assertEquals(6, metricList.size());
        Assert.assertEquals(ThreadPoolManager.LogDiscardPolicy.class, testCachedPool.getRejectedExecutionHandler().getClass());
        Assert.assertEquals(ThreadPoolManager.BlockedPolicy.class, testFixedThreaddPool.getRejectedExecutionHandler().getClass());

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
