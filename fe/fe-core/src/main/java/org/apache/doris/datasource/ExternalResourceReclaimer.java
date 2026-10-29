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

import org.apache.doris.common.ThreadPoolManager;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/** Owns detached external resources until their close attempt finishes. Never runs close on the submitter. */
public final class ExternalResourceReclaimer {
    private static final Logger LOG = LogManager.getLogger(ExternalResourceReclaimer.class);
    private static final int WORKERS = 4;
    private static final long WARN_AGE_MILLIS = TimeUnit.MINUTES.toMillis(1);
    private static final int WARN_BACKLOG = 1024;

    private final Executor executor;
    private final ConcurrentMap<Long, Reclamation> pending = new ConcurrentHashMap<>();
    private final ConcurrentLinkedQueue<Reclamation> waiting = new ConcurrentLinkedQueue<>();
    private final AtomicLong nextId = new AtomicLong();
    private final AtomicLong failures = new AtomicLong();
    private final AtomicLong lastDurationMillis = new AtomicLong();
    private final AtomicLong running = new AtomicLong();
    private final AtomicLong lastWarningMillis = new AtomicLong();

    private static final class Holder {
        private static final ExternalResourceReclaimer INSTANCE = create();

        private static ExternalResourceReclaimer create() {
            ThreadPoolExecutor workers = ThreadPoolManager.newDaemonThreadPool(
                    WORKERS, WORKERS, 0L, TimeUnit.SECONDS, new SynchronousQueue<>(),
                    new ThreadPoolExecutor.AbortPolicy(), "ExternalResourceReclaimer", true);
            workers.setThreadFactory(engineThreadFactory(workers.getThreadFactory()));
            ExternalResourceReclaimer reclaimer = new ExternalResourceReclaimer(workers);
            // Only dispatch/monitor here: a hung close must never stop the dispatcher.
            ScheduledThreadPoolExecutor dispatcher = ThreadPoolManager.newDaemonScheduledThreadPool(
                    1, "ExternalResourceReclaimDispatcher", true);
            dispatcher.setThreadFactory(engineThreadFactory(dispatcher.getThreadFactory()));
            dispatcher.scheduleWithFixedDelay(reclaimer::dispatchPending, 1L, 1L, TimeUnit.SECONDS);
            return reclaimer;
        }
    }

    private static ThreadFactory engineThreadFactory(ThreadFactory delegate) {
        return action -> {
            Thread thread = delegate.newThread(action);
            // The first submission can come from a plugin; a permanent FE worker must not inherit its loader.
            thread.setContextClassLoader(ExternalResourceReclaimer.class.getClassLoader());
            return thread;
        };
    }

    // The injected executor must reject rather than run on the caller when saturated.
    ExternalResourceReclaimer(Executor executor) {
        this.executor = Objects.requireNonNull(executor, "executor");
    }

    public static ExternalResourceReclaimer getInstance() {
        return Holder.INSTANCE;
    }

    public void submit(String description, Runnable cleanup) {
        long id = nextId.incrementAndGet();
        Reclamation task = new Reclamation(id, description, Objects.requireNonNull(cleanup, "cleanup"));
        // Register ownership before dispatch. Rejection leaves the exact same task for the next dispatch.
        pending.put(id, task);
        waiting.add(task);
        dispatchWaiting();
    }

    private void dispatchWaiting() {
        for (int i = 0; i < WORKERS; i++) {
            Reclamation task = waiting.poll();
            if (task == null) {
                return;
            }
            try {
                executor.execute(() -> reclaimAndDrain(task));
            } catch (RejectedExecutionException e) {
                // No caller-runs fallback. The same registered task remains owned and can be retried.
                waiting.add(task);
                return;
            }
        }
    }

    void dispatchPending() {
        try {
            dispatchWaiting();
            long oldest = oldestAgeMillis();
            long now = System.currentTimeMillis();
            long lastWarning = lastWarningMillis.get();
            if ((pending.size() >= WARN_BACKLOG || oldest >= WARN_AGE_MILLIS)
                    && now - lastWarning >= WARN_AGE_MILLIS
                    && lastWarningMillis.compareAndSet(lastWarning, now)) {
                Reclamation oldestTask = null;
                for (Reclamation task : pending.values()) {
                    if (oldestTask == null || task.createdNanos < oldestTask.createdNanos) {
                        oldestTask = task;
                    }
                }
                if (oldestTask != null) {
                    LOG.warn("External resource cleanup backlog: pending={}, running={}, oldestAgeMs={}, task={}",
                            pending.size(), running.get(), oldest, oldestTask.description);
                }
            }
        } catch (RuntimeException e) {
            // ScheduledThreadPoolExecutor suppresses future executions when an iteration throws.
            LOG.warn("Failed to dispatch external resource cleanup; registered tasks remain pending", e);
        }
    }

    private void reclaimAndDrain(Reclamation first) {
        // Poll each waiting record once, rather than rescanning all pending records after every short close.
        for (Reclamation task = first; task != null; task = waiting.poll()) {
            reclaim(task.id, task);
        }
    }

    private void reclaim(long id, Reclamation task) {
        long start = System.nanoTime();
        running.incrementAndGet();
        Thread thread = Thread.currentThread();
        ClassLoader previous = thread.getContextClassLoader();
        try {
            thread.setContextClassLoader(task.classLoader);
            task.cleanup.run();
        } catch (Throwable e) {
            failures.incrementAndGet();
            // A close may have partially succeeded; retrying it implicitly is not safe.
            LOG.warn("Failed to reclaim external resources: {}", task.description, e);
        } finally {
            thread.setContextClassLoader(previous);
            long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            lastDurationMillis.set(elapsed);
            if (elapsed >= WARN_AGE_MILLIS) {
                LOG.warn("Slow external resource cleanup: {}, durationMs={}", task.description, elapsed);
            }
            running.decrementAndGet();
            pending.remove(id, task);
        }
    }

    public long pendingCount() {
        return pending.size();
    }

    public long runningCount() {
        return running.get();
    }

    public long failureCount() {
        return failures.get();
    }

    public long lastDurationMillis() {
        return lastDurationMillis.get();
    }

    public long oldestAgeMillis() {
        long now = System.nanoTime();
        long oldest = 0L;
        for (Reclamation task : pending.values()) {
            oldest = Math.max(oldest, now - task.createdNanos);
        }
        return TimeUnit.NANOSECONDS.toMillis(oldest);
    }

    private static final class Reclamation {
        private final long id;
        private final String description;
        private final Runnable cleanup;
        private final long createdNanos = System.nanoTime();
        private final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

        private Reclamation(long id, String description, Runnable cleanup) {
            this.id = id;
            this.description = description;
            this.cleanup = cleanup;
        }
    }
}
