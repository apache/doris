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

package org.apache.doris.datasource.iceberg.source;

import org.apache.doris.common.util.FairTaskExecutor;
import org.apache.doris.datasource.SplitAssignment;
import org.apache.doris.datasource.iceberg.cache.ManifestCacheValue;

import org.apache.iceberg.ManifestFile;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Timeout(30)
class NativeManifestPrefetcherTest {
    private FairTaskExecutor pool(int workers) {
        return new FairTaskExecutor(workers, task -> {
            Thread thread = new Thread(task);
            thread.setDaemon(true);
            return thread;
        });
    }

    private void await(CountDownLatch latch) {
        try {
            Assertions.assertTrue(latch.await(10, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void boundedWindowReturnsCompletedManifestBeforeBlockedFirstManifest() throws Exception {
        ManifestFile first = Mockito.mock(ManifestFile.class);
        ManifestFile second = Mockito.mock(ManifestFile.class);
        ManifestFile third = Mockito.mock(ManifestFile.class);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch initialStarted = new CountDownLatch(2);
        CountDownLatch thirdStarted = new CountDownLatch(1);
        AtomicInteger leases = new AtomicInteger();
        try (FairTaskExecutor pool = pool(2);
                NativeManifestPrefetcher prefetch = new NativeManifestPrefetcher(Arrays.asList(first, second, third),
                        pool.newGroup(8), Mockito.mock(SplitAssignment.class), false, 2,
                        () -> {
                            leases.incrementAndGet();
                            return leases::decrementAndGet;
                        }, manifest -> {
                            if (manifest == third) {
                                thirdStarted.countDown();
                            } else {
                                initialStarted.countDown();
                            }
                            if (manifest == first) {
                                await(release);
                            }
                            return ManifestCacheValue.forDataFiles(Collections.emptyList());
                        })) {
            await(initialStarted);
            Assertions.assertEquals(1L, thirdStarted.getCount());
            Iterator<NativeManifestPrefetcher.Result> iterator = prefetch.iterator();
            Assertions.assertSame(second, iterator.next().manifest);
            await(thirdStarted);
            Assertions.assertSame(third, iterator.next().manifest);
            release.countDown();
            Assertions.assertSame(first, iterator.next().manifest);
            Assertions.assertFalse(iterator.hasNext());
            Assertions.assertEquals(0, leases.get());
        } finally {
            release.countDown();
        }
    }

    @Test
    void cancellationRetiresQueuedLeaseButKeepsRunningLeaseUntilWorkerActuallyExits() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch retired = new CountDownLatch(1);
        AtomicInteger leases = new AtomicInteger();
        try (FairTaskExecutor pool = pool(1)) {
            NativeManifestPrefetcher prefetch = new NativeManifestPrefetcher(
                    Arrays.asList(Mockito.mock(ManifestFile.class), Mockito.mock(ManifestFile.class)),
                    pool.newGroup(8), Mockito.mock(SplitAssignment.class), false, 2,
                    () -> {
                        leases.incrementAndGet();
                        return () -> {
                            if (leases.decrementAndGet() == 0) {
                                retired.countDown();
                            }
                        };
                    }, manifest -> {
                        started.countDown();
                        boolean done = false;
                        while (!done) {
                            try {
                                release.await();
                                done = true;
                            } catch (InterruptedException ignored) {
                                // Simulate an IO operation that does not exit on interruption.
                            }
                        }
                        return ManifestCacheValue.forDataFiles(Collections.emptyList());
                    });
            await(started);
            prefetch.close();
            Assertions.assertEquals(1, leases.get());
            release.countDown();
            await(retired);
            Assertions.assertEquals(0, leases.get());
        } finally {
            release.countDown();
        }
    }

    @Test
    void rejectedAdmissionReleasesGenerationAndLoaderFailureRemainsAnError() throws Exception {
        AtomicInteger leases = new AtomicInteger();
        try (FairTaskExecutor pool = pool(1)) {
            FairTaskExecutor.Group closed = pool.newGroup(1);
            closed.close();
            Assertions.assertThrows(RuntimeException.class, () -> new NativeManifestPrefetcher(
                    Collections.singletonList(Mockito.mock(ManifestFile.class)), closed,
                    Mockito.mock(SplitAssignment.class), false, 1,
                    () -> {
                        leases.incrementAndGet();
                        return leases::decrementAndGet;
                    }, manifest -> null));
            Assertions.assertEquals(0, leases.get());
            try (NativeManifestPrefetcher prefetch = new NativeManifestPrefetcher(
                    Collections.singletonList(Mockito.mock(ManifestFile.class)), pool.newGroup(1),
                    Mockito.mock(SplitAssignment.class), false, 1, () -> () -> { }, manifest -> {
                        throw new IllegalStateException("read failed");
                    })) {
                Assertions.assertThrows(RuntimeException.class, () -> prefetch.iterator().next());
            }
        }
    }
}
