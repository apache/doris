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

package org.apache.doris.datasource.lance.index;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class LanceIndexInspectionExecutorTest {
    @Test
    public void testIndexMetadataReadTimeoutKeepsWorkerOwnershipUntilReturn() throws Exception {
        CountDownLatch taskStarted = new CountDownLatch(1);
        CountDownLatch releaseTask = new CountDownLatch(1);
        CountDownLatch taskFinished = new CountDownLatch(1);
        AtomicBoolean ownerOpen = new AtomicBoolean(false);
        AtomicReference<Throwable> callerFailure = new AtomicReference<>();
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                1, 1, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>()) {
            @Override
            protected <T> RunnableFuture<T> newTaskFor(Callable<T> callable) {
                return new FutureTask<T>(callable) {
                    @Override
                    public T get(long timeout, TimeUnit unit)
                            throws InterruptedException, ExecutionException, TimeoutException {
                        if (!taskStarted.await(5, TimeUnit.SECONDS)) {
                            throw new AssertionError("Metadata read task did not start");
                        }
                        throw new TimeoutException("deterministic test deadline");
                    }
                };
            }
        };
        Thread caller = new Thread(() -> {
            try {
                LanceIndexInspectionExecutor.execute(() -> {
                    ownerOpen.set(true);
                    taskStarted.countDown();
                    try {
                        releaseTask.await();
                        return Collections.emptyList();
                    } finally {
                        ownerOpen.set(false);
                        taskFinished.countDown();
                    }
                }, executor, 5, TimeUnit.SECONDS);
            } catch (Throwable throwable) {
                callerFailure.set(throwable);
            }
        }, "lance-metadata-read-timeout-caller-test");
        try {
            caller.start();
            Assert.assertTrue(taskStarted.await(5, TimeUnit.SECONDS));
            caller.join(TimeUnit.SECONDS.toMillis(5));

            Assert.assertFalse(caller.isAlive());
            Assert.assertTrue(callerFailure.get()
                    instanceof LanceIndexInspectionExecutor.MetadataReadTimeoutException);
            Assert.assertEquals("Lance metadata read timed out after 5 seconds",
                    callerFailure.get().getMessage());
            Assert.assertTrue(ownerOpen.get());
            Assert.assertEquals(1, taskFinished.getCount());

            releaseTask.countDown();
            Assert.assertTrue(taskFinished.await(5, TimeUnit.SECONDS));
            Assert.assertFalse(ownerOpen.get());
        } finally {
            releaseTask.countDown();
            caller.interrupt();
            caller.join(TimeUnit.SECONDS.toMillis(5));
            executor.shutdownNow();
            Assert.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testInterruptedIndexMetadataWaitKeepsWorkerOwnershipUntilReturn() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch taskStarted = new CountDownLatch(1);
        CountDownLatch releaseTask = new CountDownLatch(1);
        CountDownLatch taskFinished = new CountDownLatch(1);
        AtomicBoolean ownerOpen = new AtomicBoolean(false);
        AtomicReference<Throwable> callerFailure = new AtomicReference<>();
        Thread caller = new Thread(() -> {
            try {
                LanceIndexInspectionExecutor.execute(() -> {
                    ownerOpen.set(true);
                    taskStarted.countDown();
                    try {
                        releaseTask.await();
                        return Collections.emptyList();
                    } finally {
                        ownerOpen.set(false);
                        taskFinished.countDown();
                    }
                }, executor, 5, TimeUnit.SECONDS);
            } catch (Throwable throwable) {
                callerFailure.set(throwable);
            }
        }, "lance-metadata-read-interrupted-caller-test");
        try {
            caller.start();
            Assert.assertTrue(taskStarted.await(5, TimeUnit.SECONDS));
            caller.interrupt();
            caller.join(TimeUnit.SECONDS.toMillis(5));

            Assert.assertFalse(caller.isAlive());
            Assert.assertTrue(callerFailure.get()
                    instanceof LanceIndexInspectionExecutor.MetadataReadInterruptedException);
            Assert.assertTrue(ownerOpen.get());
            Assert.assertEquals(1, taskFinished.getCount());

            releaseTask.countDown();
            Assert.assertTrue(taskFinished.await(5, TimeUnit.SECONDS));
            Assert.assertFalse(ownerOpen.get());
        } finally {
            releaseTask.countDown();
            caller.interrupt();
            caller.join(TimeUnit.SECONDS.toMillis(5));
            executor.shutdownNow();
            Assert.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testExpiredQueuedIndexMetadataReadDoesNotEnterProvider() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch blockerStarted = new CountDownLatch(1);
        CountDownLatch releaseBlocker = new CountDownLatch(1);
        AtomicBoolean providerEntered = new AtomicBoolean(false);
        try {
            executor.submit(() -> {
                blockerStarted.countDown();
                releaseBlocker.await();
                return null;
            });
            Assert.assertTrue(blockerStarted.await(5, TimeUnit.SECONDS));

            try {
                LanceIndexInspectionExecutor.execute(() -> {
                    providerEntered.set(true);
                    return Collections.emptyList();
                }, executor, 20, TimeUnit.MILLISECONDS);
                Assert.fail("Expected Lance metadata read timeout");
            } catch (LanceIndexInspectionExecutor.MetadataReadTimeoutException expected) {
                Assert.assertTrue(expected.getMessage().contains("timed out"));
            }

            releaseBlocker.countDown();
            executor.shutdown();
            Assert.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
            Assert.assertFalse(providerEntered.get());
        } finally {
            releaseBlocker.countDown();
            executor.shutdownNow();
            Assert.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testIndexMetadataReadRejectsWhenCapacityIsExhausted() throws Exception {
        CountDownLatch blockerStarted = new CountDownLatch(1);
        CountDownLatch releaseBlocker = new CountDownLatch(1);
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                1, 1, 0, TimeUnit.MILLISECONDS, new SynchronousQueue<>(),
                new ThreadPoolExecutor.AbortPolicy());
        try {
            executor.submit(() -> {
                blockerStarted.countDown();
                releaseBlocker.await();
                return null;
            });
            Assert.assertTrue(blockerStarted.await(5, TimeUnit.SECONDS));

            try {
                LanceIndexInspectionExecutor.execute(
                        Collections::emptyList, executor, 1, TimeUnit.SECONDS);
                Assert.fail("Expected Lance metadata read capacity rejection");
            } catch (LanceIndexInspectionExecutor.MetadataReadCapacityException expected) {
                Assert.assertEquals(
                        "Lance metadata read capacity is exhausted", expected.getMessage());
            }
        } finally {
            releaseBlocker.countDown();
            executor.shutdownNow();
            Assert.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }
}
