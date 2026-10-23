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

import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.scheduler.exception.JobException;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.TimeoutException;
import com.lmax.disruptor.WorkHandler;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReference;

@Timeout(30)
public class TaskDisruptorTest {
    @Test
    public void fullRingRejectsWithoutWaitingAndUsesLastSlot() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicIntegerArray counts = new AtomicIntegerArray(9);
        ExecutorService caller = Executors.newSingleThreadExecutor();
        try (Fixture fixture = new Fixture(event -> {
            entered.countDown();
            Assertions.assertTrue(release.await(10, TimeUnit.SECONDS));
            counts.incrementAndGet(event.getId().intValue());
        })) {
            try {
                fixture.scheduler.tryPublishTask(0L);
                Assertions.assertTrue(entered.await(10, TimeUnit.SECONDS));
                for (long id = 1; id < 8; id++) {
                    fixture.scheduler.tryPublishTask(id);
                }
                Future<JobException> rejection = caller.submit(() ->
                        Assertions.assertThrows(JobException.class, () -> fixture.scheduler.tryPublishTask(8L)));
                Assertions.assertTrue(rejection.get(3, TimeUnit.SECONDS).getMessage().contains("capacity"));
            } finally {
                release.countDown();
            }
            fixture.scheduler.close();
            for (int id = 0; id < 8; id++) {
                Assertions.assertEquals(1, counts.get(id), "accepted id " + id);
            }
            Assertions.assertEquals(0, counts.get(8));
        } finally {
            release.countDown();
            caller.shutdownNow();
            Assertions.assertTrue(caller.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    @Test
    public void concurrentProducersPublishEachAcceptedIdOnceAcrossWrapAround() throws Exception {
        int producers = 16;
        int perProducer = 200;
        int total = producers * perProducer;
        AtomicIntegerArray counts = new AtomicIntegerArray(total);
        CountDownLatch[] consumed = new CountDownLatch[total];
        for (int id = 0; id < total; id++) {
            consumed[id] = new CountDownLatch(1);
        }
        Set<Long> accepted = ConcurrentHashMap.newKeySet();
        Set<Long> rejected = ConcurrentHashMap.newKeySet();
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService callers = Executors.newFixedThreadPool(producers);
        try (Fixture fixture = new Fixture(event -> {
            int id = event.getId().intValue();
            counts.incrementAndGet(id);
            consumed[id].countDown();
        })) {
            List<Future<?>> futures = new ArrayList<>();
            for (int producer = 0; producer < producers; producer++) {
                final int offset = producer * perProducer;
                futures.add(callers.submit(() -> {
                    Assertions.assertTrue(start.await(10, TimeUnit.SECONDS));
                    for (long id = offset; id < offset + perProducer; id++) {
                        try {
                            fixture.scheduler.tryPublishTask(id);
                            accepted.add(id);
                        } catch (JobException e) {
                            Assertions.assertTrue(e.getMessage().contains("capacity"), e.getMessage());
                            rejected.add(id);
                            continue;
                        }
                        // Forces repeated wrap-around without sleeps or retrying rejected IDs.
                        Assertions.assertTrue(consumed[(int) id].await(10, TimeUnit.SECONDS));
                    }
                    return null;
                }));
            }
            start.countDown();
            for (Future<?> future : futures) {
                future.get(20, TimeUnit.SECONDS);
            }
            fixture.scheduler.close();
            Assertions.assertTrue(accepted.size() > 8);
            Assertions.assertEquals(total, accepted.size() + rejected.size());
            for (long id = 0; id < total; id++) {
                Assertions.assertNotEquals(accepted.contains(id), rejected.contains(id));
                Assertions.assertEquals(accepted.contains(id) ? 1 : 0, counts.get((int) id), "id " + id);
            }
        } finally {
            start.countDown();
            callers.shutdownNow();
            Assertions.assertTrue(callers.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    @Test
    public void publicationFinishesBeforeShutdownStarts() throws Exception {
        Disruptor<TaskEvent> raw = mockDisruptor();
        RingBuffer<TaskEvent> ring = RingBuffer.createMultiProducer(TaskEvent.FACTORY, 8);
        CountDownLatch publishing = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Mockito.when(raw.getRingBuffer()).thenAnswer(invocation -> {
            publishing.countDown();
            Assertions.assertTrue(release.await(10, TimeUnit.SECONDS));
            return ring;
        });
        Mockito.doAnswer(invocation -> {
            Assertions.assertEquals(0L, ring.getCursor(), "publication must precede shutdown");
            return null;
        }).when(raw).shutdown(5, TimeUnit.SECONDS);
        TaskDisruptor scheduler = bind(raw);
        ExecutorService callers = Executors.newFixedThreadPool(2);
        try {
            Future<?> publish = callers.submit(() -> {
                scheduler.tryPublishTask(1L);
                return null;
            });
            Assertions.assertTrue(publishing.await(10, TimeUnit.SECONDS));
            CountDownLatch closing = new CountDownLatch(1);
            AtomicReference<Thread> closingThread = new AtomicReference<>();
            Future<?> close = callers.submit(() -> {
                closingThread.set(Thread.currentThread());
                closing.countDown();
                scheduler.close();
            });
            Assertions.assertTrue(closing.await(10, TimeUnit.SECONDS));
            Object publishLock = Deencapsulation.getField(scheduler, "publishLock");
            awaitBlockedOn(closingThread.get(), publishLock);
            Assertions.assertFalse(close.isDone());
            release.countDown();
            publish.get(10, TimeUnit.SECONDS);
            close.get(10, TimeUnit.SECONDS);
            Mockito.verify(raw).shutdown(5, TimeUnit.SECONDS);
            Assertions.assertThrows(JobException.class, () -> scheduler.tryPublishTask(2L));
        } finally {
            release.countDown();
            callers.shutdownNow();
            Assertions.assertTrue(callers.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    @Test
    public void shutdownWaitDoesNotBlockClosedRejection() throws Exception {
        Disruptor<TaskEvent> raw = mockDisruptor();
        CountDownLatch draining = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Mockito.doAnswer(invocation -> {
            draining.countDown();
            Assertions.assertTrue(release.await(10, TimeUnit.SECONDS));
            return null;
        }).when(raw).shutdown(5, TimeUnit.SECONDS);
        TaskDisruptor scheduler = bind(raw);
        ExecutorService callers = Executors.newFixedThreadPool(2);
        try {
            Future<?> close = callers.submit(scheduler::close);
            Assertions.assertTrue(draining.await(10, TimeUnit.SECONDS));
            Future<JobException> publish = callers.submit(() ->
                    Assertions.assertThrows(JobException.class, () -> scheduler.tryPublishTask(1L)));
            Assertions.assertTrue(publish.get(3, TimeUnit.SECONDS).getMessage().contains("closed"));
            Mockito.verify(raw, Mockito.never()).getRingBuffer();
            release.countDown();
            close.get(10, TimeUnit.SECONDS);
        } finally {
            release.countDown();
            callers.shutdownNow();
            Assertions.assertTrue(callers.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    @Test
    public void drainTimeoutDoesNotReopenOrHaltAcceptedWork() throws Exception {
        Disruptor<TaskEvent> raw = mockDisruptor();
        RingBuffer<TaskEvent> ring = RingBuffer.createMultiProducer(TaskEvent.FACTORY, 8);
        Mockito.when(raw.getRingBuffer()).thenReturn(ring);
        Mockito.doThrow(TimeoutException.INSTANCE).when(raw).shutdown(5, TimeUnit.SECONDS);
        TaskDisruptor scheduler = bind(raw);
        scheduler.tryPublishTask(1L);
        scheduler.close();
        Assertions.assertThrows(JobException.class, () -> scheduler.tryPublishTask(2L));
        Assertions.assertEquals(0L, ring.getCursor());
        Assertions.assertEquals(1L, ring.get(0).getId());
        Mockito.verify(raw, Mockito.never()).halt();
    }

    @Test
    public void notStartedAndClosedAreExplicitRejections() {
        TaskDisruptor scheduler = new TaskDisruptor();
        Assertions.assertTrue(Assertions.assertThrows(JobException.class,
                () -> scheduler.tryPublishTask(1L)).getMessage().contains("not started"));
        scheduler.close();
        Assertions.assertTrue(Assertions.assertThrows(JobException.class,
                () -> scheduler.tryPublishTask(2L)).getMessage().contains("closed"));
    }

    @SuppressWarnings("unchecked")
    private static Disruptor<TaskEvent> mockDisruptor() {
        return Mockito.mock(Disruptor.class);
    }

    private static void awaitBlockedOn(Thread thread, Object lock) {
        ThreadMXBean threads = ManagementFactory.getThreadMXBean();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        do {
            ThreadInfo info = threads.getThreadInfo(thread.getId());
            if (info != null && info.getThreadState() == Thread.State.BLOCKED
                    && info.getLockInfo() != null
                    && info.getLockInfo().getIdentityHashCode() == System.identityHashCode(lock)) {
                return;
            }
            Thread.yield();
        } while (System.nanoTime() < deadline);
        Assertions.fail("close did not wait for the in-flight publication lock");
    }

    private static TaskDisruptor bind(Disruptor<TaskEvent> raw) {
        TaskDisruptor scheduler = new TaskDisruptor();
        Deencapsulation.setField(scheduler, "disruptor", raw);
        return scheduler;
    }

    private static class Fixture implements AutoCloseable {
        private final List<Thread> threads = new CopyOnWriteArrayList<>();
        private final Disruptor<TaskEvent> raw;
        private final TaskDisruptor scheduler;

        @SuppressWarnings("unchecked")
        Fixture(WorkHandler<TaskEvent> handler) {
            raw = new Disruptor<>(TaskEvent.FACTORY, 8, runnable -> {
                Thread thread = new Thread(runnable, "g3-test-consumer");
                thread.setDaemon(true);
                threads.add(thread);
                return thread;
            }, ProducerType.MULTI, new BlockingWaitStrategy());
            raw.handleEventsWithWorkerPool(handler);
            raw.start();
            scheduler = bind(raw);
        }

        @Override
        public void close() throws InterruptedException {
            raw.halt();
            for (Thread thread : threads) {
                thread.join(3000);
                Assertions.assertFalse(thread.isAlive(), "consumer thread leaked");
            }
        }
    }
}
