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

package org.apache.doris.task;

import org.apache.doris.common.Config;
import org.apache.doris.common.ThreadPoolManager;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This thread pool ensures that all tasks belonging to
 * the same stripe are executed in the order of submission.
 */
public class StripedTaskExecutor extends AbstractExecutorService {

    private final ExecutorService executor;

    private final ReentrantLock lock = new ReentrantLock();

    private final Condition terminating = lock.newCondition();

    private final Map<Object, SerialExecutor> executors = new IdentityHashMap<>();

    private final static ThreadLocal<Object> stripes = new ThreadLocal<>();

    private State state = State.RUNNING;

    private static enum State {
        RUNNING, SHUTDOWN
    }

    private StripedTaskExecutor(ExecutorService executor) {
        this.executor = executor;
    }

    public StripedTaskExecutor() {
        this(ThreadPoolManager.newDaemonCacheThreadPool(Config.max_sync_task_threads_num,
                "sync-task-pool", true));
    }

    protected <T> RunnableFuture<T> newTaskFor(Runnable runnable, T value) {
        saveStripedObject(runnable);
        return super.newTaskFor(runnable, value);
    }

    protected <T> RunnableFuture<T> newTaskFor(Callable<T> callable) {
        saveStripedObject(callable);
        return super.newTaskFor(callable);
    }

    private void saveStripedObject(Object task) {
        if (isStripedObject(task)) {
            stripes.set(((StripedObject) task).getStripe());
        }
    }

    private static boolean isStripedObject(Object o) {
        return o instanceof StripedObject;
    }

    public Future<?> submit(Runnable task) {
        return submit(task, null);
    }

    public <T> Future<T> submit(Runnable task, T result) {
        lock.lock();
        try {
            checkPoolIsRunning();
            if (isStripedObject(task)) {
                return super.submit(task, result);
            } else {
                return executor.submit(task, result);
            }
        } finally {
            lock.unlock();
        }
    }

    public <T> Future<T> submit(Callable<T> task) {
        lock.lock();
        try {
            checkPoolIsRunning();
            if (isStripedObject(task)) {
                return super.submit(task);
            } else {
                return executor.submit(task);
            }
        } finally {
            lock.unlock();
        }
    }

    private void checkPoolIsRunning() {
        Preconditions.checkState(lock.isHeldByCurrentThread());
        if (state != State.RUNNING) {
            throw new RejectedExecutionException(
                    "executor not running");
        }
    }

    public void execute(Runnable command) {
        lock.lock();
        try {
            checkPoolIsRunning();
            Object stripe = getStripe(command);
            if (stripe != null) {
                SerialExecutor serialEx = executors.get(stripe);
                if (serialEx == null) {
                    serialEx = new SerialExecutor(stripe);
                    executors.put(stripe, serialEx);
                }
                serialEx.execute(command);
            } else {
                executor.execute(command);
            }
        } finally {
            lock.unlock();
        }
    }

    private Object getStripe(Runnable command) {
        Object stripe;
        if (command instanceof StripedObject) {
            stripe = (((StripedObject) command).getStripe());
        } else {
            stripe = stripes.get();
        }
        stripes.remove();
        return stripe;
    }

    public void shutdown() {
        lock.lock();
        try {
            state = State.SHUTDOWN;
            if (executors.isEmpty()) {
                executor.shutdown();
            }
        } finally {
            lock.unlock();
        }
    }

    public List<Runnable> shutdownNow() {
        lock.lock();
        try {
            shutdown();
            List<Runnable> result = new ArrayList<>();
            for (SerialExecutor serialEx : executors.values()) {
                serialEx.tasks.drainTo(result);
            }
            result.addAll(executor.shutdownNow());
            return result;
        } finally {
            lock.unlock();
        }
    }

    public boolean isShutdown() {
        lock.lock();
        try {
            return state == State.SHUTDOWN;
        } finally {
            lock.unlock();
        }
    }

    public boolean isTerminated() {
        lock.lock();
        try {
            if (state == State.RUNNING) return false;
            for (SerialExecutor executor : executors.values()) {
                if (!executor.isEmpty()) return false;
            }
            return executor.isTerminated();
        } finally {
            lock.unlock();
        }
    }

    public boolean awaitTermination(long timeout, TimeUnit unit)
            throws InterruptedException {
        lock.lock();
        try {
            long waitUntil = System.nanoTime() + unit.toNanos(timeout);
            long remainingTime;
            while ((remainingTime = waitUntil - System.nanoTime()) > 0
                    && !executors.isEmpty()) {
                terminating.awaitNanos(remainingTime);
            }
            if (remainingTime <= 0) return false;
            if (executors.isEmpty()) {
                return executor.awaitTermination(
                        remainingTime, TimeUnit.NANOSECONDS);
            }
            return false;
        } finally {
            lock.unlock();
        }
    }

    private void removeEmptySerialExecutor(Object stripe, SerialExecutor serialEx) {
        Preconditions.checkState(serialEx == executors.get(stripe));
        Preconditions.checkState(lock.isHeldByCurrentThread());
        Preconditions.checkState(serialEx.isEmpty());

        executors.remove(stripe);
        terminating.signalAll();
        if (state == State.SHUTDOWN && executors.isEmpty()) {
            executor.shutdown();
        }
    }

    private class SerialExecutor implements Executor {

        private final BlockingQueue<Runnable> tasks = new LinkedBlockingQueue<>();

        private Runnable active;

        private final Object stripe;

        private SerialExecutor(Object stripe) {
            this.stripe = stripe;
        }

        public void execute(final Runnable r) {
            lock.lock();
            try {
                tasks.add(new Runnable() {
                    public void run() {
                        try {
                            r.run();
                        } finally {
                            scheduleNext();
                        }
                    }
                });
                if (active == null) {
                    scheduleNext();
                }
            } finally {
                lock.unlock();
            }
        }

        private void scheduleNext() {
            lock.lock();
            try {
                if ((active = tasks.poll()) != null) {
                    executor.execute(active);
                    terminating.signalAll();
                } else {
                    removeEmptySerialExecutor(stripe, this);
                }
            } finally {
                lock.unlock();
            }
        }

        public boolean isEmpty() {
            lock.lock();
            try {
                return active == null && tasks.isEmpty();
            } finally {
                lock.unlock();
            }
        }
    }
}
