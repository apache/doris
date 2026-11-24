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

import org.apache.doris.common.UserException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.util.concurrent.Executor;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ThreadPoolExecutor;

/** Owns one legacy enumeration task until its worker and cleanup have actually returned. */
public final class SplitProducer implements Runnable, Closeable {
    private static final Logger LOG = LogManager.getLogger(SplitProducer.class);

    private enum State {
        QUEUED, RUNNING, CLOSING, CLOSED
    }

    private final SplitAssignment assignment;
    private final Executor executor;
    private final Executor cleanupExecutor;
    private final Object lock = new Object();
    private final FutureTask<Void> submission = new FutureTask<>(this, null);
    private Runnable planning;
    private Closeable cleanup;
    private State state = State.QUEUED;
    private boolean registered;
    private volatile boolean cancelled;

    public SplitProducer(Executor executor, SplitAssignment assignment, Runnable planning, Closeable cleanup) {
        this(executor, assignment, planning, cleanup, SplitTaskCleanup.executor());
    }

    SplitProducer(Executor executor, SplitAssignment assignment, Runnable planning, Closeable cleanup,
            Executor cleanupExecutor) {
        this.executor = executor;
        this.assignment = assignment;
        this.planning = planning;
        this.cleanup = cleanup;
        this.cleanupExecutor = cleanupExecutor;
    }

    public void submit() {
        synchronized (lock) {
            if (state != State.QUEUED) {
                return;
            }
            if (registered) {
                return;
            }
            registered = true;
            assignment.addProducer(this);
        }
        if (cancelled) {
            return;
        }
        try {
            executor.execute(submission);
            // close() may have removed the task before execute() actually enqueued it.
            if (cancelled) {
                removeQueuedTask();
            }
        } catch (RuntimeException e) {
            publishFailure(e);
            close();
        }
    }

    @Override
    public void run() {
        synchronized (lock) {
            if (state != State.QUEUED) {
                return;
            }
            state = State.RUNNING;
        }
        try {
            assignment.getScanContext().checkCancelled();
            planning.run();
        } catch (Throwable failure) {
            // Publish before cleanup, which can wait for an already-entered SDK call.
            publishFailure(failure);
        } finally {
            synchronized (lock) {
                state = State.CLOSING;
            }
            finish();
        }
    }

    private void publishFailure(Throwable failure) {
        if (cancelled || assignment.isStop()) {
            return;
        }
        assignment.setException(new UserException(failure.getMessage(), failure));
    }

    private void removeQueuedTask() {
        if (executor instanceof ThreadPoolExecutor) {
            ((ThreadPoolExecutor) executor).remove(submission);
        }
    }

    @Override
    public void close() {
        synchronized (lock) {
            cancelled = true;
            if (state != State.QUEUED) {
                return;
            }
            state = State.CLOSING;
            submission.cancel(false);
        }
        removeQueuedTask();
        cleanupExecutor.execute(this::finish);
    }

    private void finish() {
        try {
            cleanup.close();
        } catch (Throwable failure) {
            if (assignment.isStop()) {
                LOG.warn("Failed to close stopped split producer", failure);
            } else {
                publishFailure(failure);
            }
        } finally {
            planning = null;
            cleanup = null;
            synchronized (lock) {
                state = State.CLOSED;
            }
            if (registered) {
                assignment.producerFinished(this);
            }
        }
    }
}
