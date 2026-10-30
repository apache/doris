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

package org.apache.doris.common.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

/**
 * Fixed workers fairly dequeue task groups, then execute outside scheduler locks. A group can use
 * all workers; fairness applies to task admission, not preemption of blocking calls. Queue capacity
 * belongs to each group, so one caller cannot fill another caller's queue. Tasks must yield by
 * returning and resubmitting, and must not wait for children submitted to this same executor.
 */
public final class FairTaskExecutor implements Closeable {
    private static final Logger LOG = LogManager.getLogger(FairTaskExecutor.class);
    private final ConcurrentLinkedQueue<Group> ready = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<Waiter> waiters = new ConcurrentLinkedQueue<>();
    private final List<Thread> workers = new ArrayList<>();
    private final ThreadLocal<WorkerState> currentWorker = new ThreadLocal<>();
    private final Set<Group> groups = java.util.concurrent.ConcurrentHashMap.newKeySet();
    private final AtomicBoolean started = new AtomicBoolean();
    private volatile boolean closed;
    private final LongAdder active = new LongAdder();
    private final LongAdder completed = new LongAdder();

    public FairTaskExecutor(int parallelism, ThreadFactory threadFactory) {
        if (parallelism < 1) {
            throw new IllegalArgumentException("Parallelism must be positive");
        }
        for (int i = 0; i < parallelism; i++) {
            workers.add(threadFactory.newThread(this::work));
        }
    }

    public synchronized Group newGroup(int capacity) {
        if (closed) {
            throw new RejectedExecutionException("Executor is closed");
        }
        if (capacity < 1) {
            throw new IllegalArgumentException("Group queue capacity must be positive");
        }
        Group group = new Group(capacity);
        groups.add(group);
        return group;
    }

    private synchronized void start() {
        if (closed) {
            throw new RejectedExecutionException("Executor is closed");
        }
        if (started.compareAndSet(false, true)) {
            workers.forEach(Thread::start);
        }
    }

    private static final class WorkerState {
        private Group group;
        private Runnable task;
        private boolean continuationQueued;
    }

    private static final class Entry {
        private final Runnable task;
        private final Consumer<Throwable> rejected;

        private Entry(Runnable task, Consumer<Throwable> rejected) {
            this.task = task;
            this.rejected = rejected;
        }
    }

    public final class Group implements Executor, Closeable {
        private final ConcurrentLinkedQueue<Entry> pending = new ConcurrentLinkedQueue<>();
        private final Set<Entry> runningFutures = java.util.concurrent.ConcurrentHashMap.newKeySet();
        private final AtomicInteger queued = new AtomicInteger();
        private final AtomicBoolean published = new AtomicBoolean();
        // Enqueue/dequeue use only CLQ and CAS. This lock protects capacity listeners and close;
        // neither the normal admission path nor user callbacks run under it.
        private final Object admissionLock = new Object();
        private final Map<Runnable, Consumer<Throwable>> spaceListeners = new HashMap<>();
        private volatile boolean hasSpaceListeners;
        private volatile boolean stopped;
        private final int capacity;

        private Group(int capacity) {
            this.capacity = capacity;
        }

        @Override
        public void execute(Runnable task) {
            if (!tryExecute(task, failure -> {
                if (task instanceof Future<?>) {
                    ((Future<?>) task).cancel(false);
                } else {
                    LOG.warn("Task was rejected or failed", failure);
                }
            }, null)) {
                throw new RejectedExecutionException("Task group queue is full (capacity=" + capacity + ")");
            }
        }

        /** False registers one capacity callback. The caller retains its task and retries on notification. */
        public boolean tryExecute(Runnable task, Consumer<Throwable> rejected, Runnable onSpace) {
            return tryExecute(task, rejected, onSpace, null);
        }

        /** The caller must return after admission; the current worker will poll the global queue again. */
        public boolean tryExecuteContinuation(Runnable task, Consumer<Throwable> rejected, Runnable onSpace) {
            WorkerState worker = currentWorker.get();
            return tryExecute(task, rejected, onSpace,
                    worker != null && worker.group == this && worker.task == task ? worker : null);
        }

        private boolean tryExecute(Runnable task, Consumer<Throwable> rejected, Runnable onSpace, WorkerState worker) {
            if (stopped || closed) {
                throw new RejectedExecutionException("Task group is closed");
            }
            if (!started.get()) {
                start();
            }
            while (true) {
                int count = queued.get();
                if (count >= capacity) {
                    synchronized (admissionLock) {
                        if (stopped || closed) {
                            throw new RejectedExecutionException("Task group is closed");
                        }
                        if (onSpace != null) {
                            spaceListeners.put(onSpace, rejected);
                            hasSpaceListeners = true;
                        }
                    }
                    // Dequeue can precede registration. Either this recheck or the dequeuer
                    // notifies; removing the callback under admissionLock prevents double wakes.
                    if (queued.get() < capacity || stopped || closed) {
                        notifySpace();
                    }
                    return false;
                }
                if (queued.compareAndSet(count, count + 1)) {
                    Entry entry = new Entry(task, rejected);
                    pending.add(entry);
                    // close may have drained before this reserved slot was published. Removal
                    // transfers ownership to exactly one of close, the worker, or this submitter.
                    if (stopped || closed) {
                        if (pending.remove(entry)) {
                            queued.decrementAndGet();
                            reject(entry);
                        }
                    } else {
                        publish(this, worker == null);
                        if (worker != null) {
                            worker.continuationQueued = true;
                        }
                    }
                    return true;
                }
            }
        }

        public void remove(Runnable task) {
            for (Entry entry : pending) {
                if (entry.task == task && pending.remove(entry)) {
                    queued.decrementAndGet();
                    notifySpace();
                }
            }
        }

        public void removeSpaceListener(Runnable listener) {
            synchronized (admissionLock) {
                spaceListeners.remove(listener);
                hasSpaceListeners = !spaceListeners.isEmpty();
            }
        }

        private void notifySpace() {
            if (!hasSpaceListeners) {
                return;
            }
            Map<Runnable, Consumer<Throwable>> callbacks;
            synchronized (admissionLock) {
                callbacks = new HashMap<>(spaceListeners);
                spaceListeners.clear();
                hasSpaceListeners = false;
            }
            callbacks.forEach((callback, failureHandler) -> {
                try {
                    callback.run();
                } catch (Throwable e) {
                    try {
                        failureHandler.accept(e);
                    } catch (Throwable failure) {
                        LOG.error("Task group capacity failure callback failed", failure);
                    }
                }
            });
        }

        public int queuedTasks() {
            return queued.get();
        }

        public boolean isClosed() {
            return stopped || closed;
        }

        @Override
        public void close() {
            synchronized (admissionLock) {
                stopped = true;
            }
            Entry entry;
            while ((entry = pending.poll()) != null) {
                queued.decrementAndGet();
                reject(entry);
            }
            // Wake parents waiting on futures without interrupting an elected shared cache loader.
            // Plain resumable tasks cancel cooperatively; their source must not close during iteration.
            for (Entry running : runningFutures) {
                try {
                    ((Future<?>) running.task).cancel(false);
                } catch (Throwable failure) {
                    LOG.error("Task cancellation callback failed", failure);
                }
            }
            notifySpace();
            ready.remove(this);
            groups.remove(this);
        }
    }

    private void publish(Group group) {
        publish(group, true);
    }

    private void publish(Group group, boolean wakeWorker) {
        if (!group.stopped && !group.pending.isEmpty() && group.published.compareAndSet(false, true)) {
            ready.add(group);
            if (group.stopped || closed) {
                ready.remove(group);
            } else if (wakeWorker) {
                signalOne();
            }
        }
    }

    private void reject(Entry entry) {
        try {
            entry.rejected.accept(new RejectedExecutionException("Task group is closed"));
        } catch (Throwable e) {
            LOG.warn("Task rejection callback failed", e);
        }
    }

    private void work() {
        WorkerState worker = new WorkerState();
        currentWorker.set(worker);
        try {
            work(worker);
        } finally {
            currentWorker.remove();
        }
    }

    private void work(WorkerState worker) {
        while (!closed) {
            Group group = awaitGroup();
            if (group == null) {
                return;
            }
            if (worker.continuationQueued) {
                worker.continuationQueued = false;
                // We retained a worker for its continuation, but may have dequeued another query.
                // Wake a peer for remaining ready work before this task can block on external IO.
                if (!ready.isEmpty()) {
                    signalOne();
                }
            }
            Entry entry = group.pending.poll();
            Future<?> future = entry != null && entry.task instanceof Future<?> ? (Future<?>) entry.task : null;
            if (entry != null) {
                if (future != null) {
                    group.runningFutures.add(entry);
                    if (future instanceof CompletionStage<?>) {
                        // A cooperative task may return while observing a shared asynchronous load.
                        // Keep its cancellation ownership until completion, without retaining a worker.
                        ((CompletionStage<?>) future).whenComplete((value, failure) ->
                                group.runningFutures.remove(entry));
                    }
                }
                group.queued.decrementAndGet();
            }
            group.published.set(false);
            publish(group);
            group.notifySpace();
            if (entry == null) {
                continue;
            }
            if (group.stopped || closed) {
                reject(entry);
                if (future != null) {
                    group.runningFutures.remove(entry);
                }
                continue;
            }
            active.increment();
            ClassLoader previousClassLoader = Thread.currentThread().getContextClassLoader();
            worker.group = group;
            worker.task = entry.task;
            try {
                entry.task.run();
            } catch (Throwable e) {
                // A failed caller must not permanently remove an executor worker.
                try {
                    entry.rejected.accept(e);
                } catch (Throwable callbackFailure) {
                    LOG.error("Task group failure callback failed", callbackFailure);
                }
            } finally {
                worker.group = null;
                worker.task = null;
                if (future != null && !(future instanceof CompletionStage<?>)) {
                    group.runningFutures.remove(entry);
                }
                active.decrement();
                completed.increment();
                Thread.currentThread().setContextClassLoader(previousClassLoader);
                Thread.interrupted();
            }
        }
    }

    public int getPoolSize() {
        return started.get() ? workers.size() : 0;
    }

    public long getActiveCount() {
        return active.sum();
    }

    public long getCompletedTaskCount() {
        return completed.sum();
    }

    public int getGroupCount() {
        return groups.size();
    }

    public long getQueuedTaskCount() {
        return groups.stream().mapToLong(Group::queuedTasks).sum();
    }

    /** Closing cancels queued work but deliberately does not interrupt shared metadata IO. */
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        for (Thread worker : workers) {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                return workers.stream().noneMatch(Thread::isAlive);
            }
            TimeUnit.NANOSECONDS.timedJoin(worker, remaining);
        }
        return workers.stream().noneMatch(Thread::isAlive);
    }

    private static final class Waiter {
        private final Thread thread = Thread.currentThread();
        private final AtomicBoolean waiting = new AtomicBoolean(true);
    }

    private Group awaitGroup() {
        while (!closed) {
            Group group = ready.poll();
            if (group != null) {
                return group;
            }
            Waiter waiter = new Waiter();
            waiters.add(waiter);
            try {
                group = ready.poll();
                if (group != null) {
                    return group;
                }
                while (!closed && waiter.waiting.get()) {
                    LockSupport.park(this);
                    Thread.interrupted();
                }
            } finally {
                boolean notified = !waiter.waiting.getAndSet(false);
                waiters.remove(waiter);
                if (notified && !ready.isEmpty()) {
                    signalOne();
                }
            }
        }
        return null;
    }

    private void signalOne() {
        Waiter waiter;
        while ((waiter = waiters.poll()) != null) {
            if (waiter.waiting.compareAndSet(true, false)) {
                LockSupport.unpark(waiter.thread);
                return;
            }
        }
    }

    @Override
    public void close() {
        synchronized (this) {
            closed = true;
        }
        for (Group group : groups) {
            group.close();
        }
        for (Thread worker : workers) {
            LockSupport.unpark(worker);
        }
        ready.clear();
    }
}
