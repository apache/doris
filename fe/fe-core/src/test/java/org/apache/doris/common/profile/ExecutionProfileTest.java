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

package org.apache.doris.common.profile;

import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ExecutionProfileTest {

    @Test
    public void testConcurrentUpdate() throws InterruptedException {
        UUID taskId = UUID.randomUUID();
        TUniqueId queryId = new TUniqueId(taskId.getMostSignificantBits(), taskId.getLeastSignificantBits());
        List<Integer> fragmentIds = Lists.newArrayList();
        ExecutionProfile executionProfile = new ExecutionProfile(queryId, fragmentIds);

        // Set initial value
        final long initialValue = 100L;
        executionProfile.setQueryFinishTime(initialValue);

        // Reset counter after initial setup
        long startCount = executionProfile.getQueryFinishTimeUpdateCount();

        // Test with multiple threads updating concurrently
        final int threadCount = 20;
        final int iterationsPerThread = 50;
        final int expectedUpdates = threadCount * iterationsPerThread;

        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch endLatch = new CountDownLatch(threadCount);

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        // Have all threads just call setQueryFinishTime multiple times
        for (int i = 0; i < threadCount; i++) {
            final int threadNum = i;
            executor.submit(() -> {
                try {
                    startLatch.await(); // Wait for all threads to be ready

                    for (int j = 0; j < iterationsPerThread; j++) {
                        long newValue = initialValue + threadNum + j;
                        executionProfile.setQueryFinishTime(newValue);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    endLatch.countDown();
                }
            });
        }

        // Start all threads at once
        startLatch.countDown();

        // Wait for all threads to complete
        boolean allFinished = endLatch.await(10, TimeUnit.SECONDS);
        executor.shutdown();

        Assert.assertTrue("Not all threads completed in time", allFinished);

        // Verify the counter has been incremented exactly the expected number of times
        long updateCount = executionProfile.getQueryFinishTimeUpdateCount() - startCount;
        Assert.assertEquals("Update count should match threadCount * iterationsPerThread",
                            expectedUpdates, updateCount);

        // Also verify the value is higher than initial
        long finalValue = executionProfile.getQueryFinishTime();
        Assert.assertTrue("Final value should be greater than initial", finalValue > initialValue);
    }
}
