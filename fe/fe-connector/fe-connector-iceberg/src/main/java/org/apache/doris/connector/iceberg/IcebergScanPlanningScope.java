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

package org.apache.doris.connector.iceberg;

import org.apache.doris.connector.spi.scan.ConnectorScanContext;

import org.apache.iceberg.util.ThreadPools;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

/** Owns SDK planning jobs for one source, borrowing the catalog-independent Iceberg worker pool. */
final class IcebergScanPlanningScope extends AbstractExecutorService implements AutoCloseable {
    private static final Logger LOG = LogManager.getLogger(IcebergScanPlanningScope.class);
    private static final ExecutorService CLEANUP = ThreadPools.newFixedThreadPool("doris-iceberg-split-cleanup", 2);

    private final ConnectorScanContext scanContext;
    private final ExecutorService workers;
    private final Object lock = new Object();
    private final Set<TrackedTask> tasks = new HashSet<>();
    private final CompletableFuture<Void> termination = new CompletableFuture<>();
    private final AtomicReference<IcebergScanPlanningScope> cancellationHook;
    private volatile boolean stopping;
    // Protected by lock; an unexpected bare Runnable failure must also fail source cleanup.
    private Throwable taskFailure;

    IcebergScanPlanningScope(ConnectorScanContext scanContext) {
        this(scanContext, ThreadPools.getWorkerPool());
    }

    IcebergScanPlanningScope(ConnectorScanContext scanContext, ExecutorService workers) {
        this.scanContext = scanContext;
        this.workers = workers;
        AtomicReference<IcebergScanPlanningScope> target = new AtomicReference<>(this);
        cancellationHook = target;
        scanContext.whenCancelled(() -> {
            IcebergScanPlanningScope scope = target.getAndSet(null);
            if (scope != null) {
                // The engine can hold its assignment lock here. Never run SDK code or join on this thread.
                scope.shutdown();
            }
        });
    }

    void checkCancelled() {
        scanContext.checkCancelled();
        if (stopping) {
            throw new CancellationException("Iceberg split planning stopped");
        }
    }

    @Override
    public void execute(Runnable command) {
        checkCancelled();
        TrackedTask task;
        synchronized (lock) {
            checkCancelled();
            task = new TrackedTask(this, command);
            tasks.add(task);
        }
        // Submission stays outside the lock: cancellation can claim an accepted task even before enqueueing.
        try {
            workers.execute(task);
        } catch (RuntimeException | Error e) {
            task.rejected();
            throw e;
        }
    }

    @Override
    protected <T> RunnableFuture<T> newTaskFor(Callable<T> callable) {
        return new FutureTask<>(() -> {
            checkCancelled();
            return callable.call();
        });
    }

    @Override
    protected <T> RunnableFuture<T> newTaskFor(Runnable runnable, T result) {
        return newTaskFor(() -> {
            runnable.run();
            return result;
        });
    }

    @Override
    public void shutdown() {
        cancellationHook.set(null);
        List<TrackedTask> pending;
        synchronized (lock) {
            stopping = true;
            pending = new ArrayList<>(tasks);
        }
        if (pending.isEmpty()) {
            completeTermination();
        } else {
            CLEANUP.execute(() -> {
                for (TrackedTask task : pending) {
                    try {
                        // Do not skip or cancel AsyncSupply: running it lets the SDK complete its own future.
                        // Queued SDK readers fail at the request-local FileIO before starting another request.
                        task.run();
                    } catch (RuntimeException | Error e) {
                        LOG.error("Iceberg split planning task failed during cleanup", e);
                    }
                }
            });
        }
    }

    @Override
    public List<Runnable> shutdownNow() {
        // Interrupting a shared SDK worker is unsafe; cooperative FileIO checks settle the owned tasks.
        shutdown();
        return Collections.emptyList();
    }

    @Override
    public boolean isShutdown() {
        return stopping;
    }

    @Override
    public boolean isTerminated() {
        return termination.isDone();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        try {
            termination.get(timeout, unit);
            return true;
        } catch (ExecutionException e) {
            return true;
        } catch (TimeoutException e) {
            return false;
        }
    }

    @Override
    public void close() {
        shutdown();
        // A cancelled SDK future is not proof that its supplier returned. Join our actual run-finally instead.
        termination.join();
    }

    private void finished(TrackedTask task, Throwable failure) {
        boolean terminated;
        synchronized (lock) {
            tasks.remove(task);
            if (failure != null && taskFailure == null) {
                taskFailure = failure;
            }
            terminated = stopping && tasks.isEmpty();
        }
        if (terminated) {
            completeTermination();
        }
    }

    private void completeTermination() {
        Throwable failure;
        synchronized (lock) {
            failure = taskFailure;
        }
        if (failure == null) {
            termination.complete(null);
        } else {
            termination.completeExceptionally(failure);
        }
    }

    private static final class TaskBody {
        private final IcebergScanPlanningScope owner;
        private final Runnable command;

        TaskBody(IcebergScanPlanningScope owner, Runnable command) {
            this.owner = owner;
            this.command = command;
        }
    }

    private static final class TrackedTask implements Runnable {
        // A shared pool may retain the no-op wrapper after cleanup wins. Clear ALL query references together.
        private final AtomicReference<TaskBody> body;

        TrackedTask(IcebergScanPlanningScope owner, Runnable command) {
            body = new AtomicReference<>(new TaskBody(owner, command));
        }

        @Override
        public void run() {
            TaskBody claimed = body.getAndSet(null);
            if (claimed == null) {
                return;
            }
            Thread thread = Thread.currentThread();
            ClassLoader original = thread.getContextClassLoader();
            Throwable failure = null;
            try {
                thread.setContextClassLoader(IcebergScanPlanningScope.class.getClassLoader());
                claimed.command.run();
            } catch (RuntimeException | Error e) {
                failure = e;
                throw e;
            } finally {
                thread.setContextClassLoader(original);
                claimed.owner.finished(this, failure);
            }
        }

        void rejected() {
            TaskBody claimed = body.getAndSet(null);
            if (claimed != null) {
                claimed.owner.finished(this, null);
            }
        }
    }
}
