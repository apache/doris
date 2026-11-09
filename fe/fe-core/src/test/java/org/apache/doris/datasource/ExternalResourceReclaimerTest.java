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

package org.apache.doris.datasource;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

class ExternalResourceReclaimerTest {
    @Test
    void rejectedDispatchRetainsOwnershipAndNeverRunsOnCaller() {
        Queue<Runnable> accepted = new ArrayDeque<>();
        AtomicInteger attempts = new AtomicInteger();
        AtomicInteger closed = new AtomicInteger();
        ExternalResourceReclaimer reclaimer = new ExternalResourceReclaimer(action -> {
            if (attempts.getAndIncrement() == 0) {
                throw new RejectedExecutionException("saturated");
            }
            accepted.add(action);
        });
        reclaimer.submit("old connector", closed::incrementAndGet);
        Assertions.assertEquals(1L, reclaimer.pendingCount());
        Assertions.assertEquals(0, closed.get());
        reclaimer.dispatchPending();
        reclaimer.dispatchPending();
        Assertions.assertEquals(1, accepted.size(), "a dispatched task cannot be scheduled twice");
        accepted.remove().run();
        Assertions.assertEquals(1, closed.get());
        Assertions.assertEquals(0L, reclaimer.pendingCount());
    }

    @Test
    void blockedCloseDoesNotPreventAnotherWorkerOrSubmission() throws Exception {
        ExecutorService workers = Executors.newFixedThreadPool(2);
        ExternalResourceReclaimer reclaimer = new ExternalResourceReclaimer(workers);
        CountDownLatch closeStarted = new CountDownLatch(1);
        CountDownLatch releaseClose = new CountDownLatch(1);
        CountDownLatch firstFinished = new CountDownLatch(1);
        CountDownLatch secondFinished = new CountDownLatch(1);
        try {
            reclaimer.submit("blocked old connector", () -> {
                closeStarted.countDown();
                await(releaseClose);
                firstFinished.countDown();
            });
            Assertions.assertTrue(closeStarted.await(10L, TimeUnit.SECONDS));
            reclaimer.submit("independent resource", secondFinished::countDown);
            Assertions.assertTrue(secondFinished.await(10L, TimeUnit.SECONDS));
            Assertions.assertEquals(1L, releaseClose.getCount());
            Assertions.assertEquals(1L, firstFinished.getCount());
        } finally {
            releaseClose.countDown();
            workers.shutdown();
            Assertions.assertTrue(workers.awaitTermination(10L, TimeUnit.SECONDS));
        }
        Assertions.assertEquals(0L, reclaimer.pendingCount());
    }

    @Test
    void failedCloseIsCountedWithoutRetryAndContextLoaderIsRestored() {
        Queue<Runnable> accepted = new ArrayDeque<>();
        ExternalResourceReclaimer reclaimer = new ExternalResourceReclaimer(accepted::add);
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        ClassLoader submitted = new ClassLoader(previous) { };
        AtomicReference<ClassLoader> seen = new AtomicReference<>();
        try {
            Thread.currentThread().setContextClassLoader(submitted);
            reclaimer.submit("failed resource", () -> {
                seen.set(Thread.currentThread().getContextClassLoader());
                throw new IllegalStateException("close failed");
            });
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
        }
        accepted.remove().run();
        Assertions.assertSame(submitted, seen.get());
        Assertions.assertSame(previous, Thread.currentThread().getContextClassLoader());
        Assertions.assertEquals(1L, reclaimer.failureCount());
        Assertions.assertEquals(0L, reclaimer.pendingCount());
        reclaimer.dispatchPending();
        Assertions.assertTrue(accepted.isEmpty());
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(10L, TimeUnit.SECONDS)) {
                throw new IllegalStateException("timed out waiting for test latch");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
