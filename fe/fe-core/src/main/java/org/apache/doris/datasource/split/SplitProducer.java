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

package org.apache.doris.datasource.split;

import org.apache.doris.common.UserException;
import org.apache.doris.connector.spi.ConnectorTaskScheduler;
import org.apache.doris.spi.Split;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TScanRangeLocations;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

/**
 * Serially advances bounded batches per task, yielding for fairness or queue backpressure.
 * A running worker owns source closure. For an idle/queued producer, close claims ownership under
 * scheduleLock and dispatches cleanup separately; a stale queued run then does nothing. Neither
 * plugin code nor assignment callbacks run under scheduleLock.
 */
public class SplitProducer implements Runnable, Closeable {
    private static final Logger LOG = LogManager.getLogger(SplitProducer.class);

    // Bound cached batches and remote listing work so other queries can run.
    static final int MAX_BATCHES_PER_TASK = 16;
    static final long MAX_TASK_NANOS = TimeUnit.MILLISECONDS.toNanos(1);

    public interface BatchSource extends Closeable {
        List<Split> getNextBatch() throws Exception;

        @Override
        default void close() throws IOException {
        }
    }

    private enum State {
        IDLE, QUEUED, RUNNING, CLOSING, CLOSED
    }

    private final SplitAssignment assignment;
    private final Executor executor;
    private final Executor cleanupExecutor;
    private final LongSupplier nanoTime;
    private BatchSource source;
    private Consumer<Exception> completion;
    private final Runnable resume = this::schedule;
    private final Runnable retryAdmission = this::submit;
    private final Object scheduleLock = new Object();
    private State state = State.IDLE;
    private boolean wakeupRequested;
    private boolean registered;
    private volatile boolean cancelled;
    private Iterator<Map.Entry<Backend, Collection<Split>>> pending = Collections.emptyIterator();
    private Backend pendingBackend;
    private List<TScanRangeLocations> pendingRanges;
    private int pendingRangeOffset;

    public SplitProducer(SplitAssignment assignment, Executor executor, BatchSource source,
            Consumer<Exception> completion) {
        this(assignment, executor, source, completion, SplitTaskCleanup.executor());
    }

    SplitProducer(SplitAssignment assignment, Executor executor, BatchSource source,
            Consumer<Exception> completion, Executor cleanupExecutor) {
        this(assignment, executor, source, completion, cleanupExecutor, System::nanoTime);
    }

    SplitProducer(SplitAssignment assignment, Executor executor, BatchSource source,
            Consumer<Exception> completion, LongSupplier nanoTime) {
        this(assignment, executor, source, completion, SplitTaskCleanup.executor(), nanoTime);
    }

    SplitProducer(SplitAssignment assignment, Executor executor, BatchSource source,
            Consumer<Exception> completion, Executor cleanupExecutor, LongSupplier nanoTime) {
        this.nanoTime = nanoTime;
        this.assignment = assignment;
        this.executor = executor;
        this.source = source;
        this.completion = completion;
        this.cleanupExecutor = cleanupExecutor;
    }

    public void start() {
        synchronized (scheduleLock) {
            registered = assignment.addProducer(this);
        }
        if (registered) {
            schedule();
        } else {
            close();
        }
    }

    private void schedule() {
        synchronized (scheduleLock) {
            if (state == State.CLOSING || state == State.CLOSED) {
                return;
            }
            wakeupRequested = true;
            if (state != State.IDLE) {
                return;
            }
            state = State.QUEUED;
        }
        submit();
    }

    private void submit() {
        submit(false);
    }

    private void submit(boolean continuation) {
        try {
            if (executor instanceof ConnectorTaskScheduler) {
                ConnectorTaskScheduler scheduler = (ConnectorTaskScheduler) executor;
                if (continuation) {
                    scheduler.tryExecuteContinuation(this, this::admissionFailed, retryAdmission);
                } else {
                    scheduler.tryExecute(this, this::admissionFailed, retryAdmission);
                }
            } else {
                executor.execute(this);
            }
            // Cancellation may precede admission. Remove either a queued entry or its capacity callback.
            if (cancelled) {
                removeAdmission();
            }
        } catch (RuntimeException e) {
            admissionFailed(e);
        }
    }

    private void admissionFailed(Throwable failure) {
        synchronized (scheduleLock) {
            if (state != State.QUEUED) {
                return;
            }
            state = State.CLOSING;
        }
        UserException error = publishFailure(failure);
        cleanupExecutor.execute(() -> finish(error));
    }

    private void removeAdmission() {
        if (executor instanceof ConnectorTaskScheduler) {
            ConnectorTaskScheduler scheduler = (ConnectorTaskScheduler) executor;
            scheduler.removeSpaceListener(retryAdmission);
            scheduler.remove(this);
        } else if (executor instanceof ThreadPoolExecutor) {
            ((ThreadPoolExecutor) executor).remove(this);
        }
    }

    @Override
    public void run() {
        synchronized (scheduleLock) {
            if (state != State.QUEUED) {
                return;
            }
            state = State.RUNNING;
            wakeupRequested = false;
        }
        boolean more = false;
        boolean exhausted = false;
        Throwable failure = null;
        try {
            long start = nanoTime.getAsLong();
            for (int batches = 0; batches < MAX_BATCHES_PER_TASK; batches++) {
                if (cancelled || !assignment.needMoreSplit()) {
                    break;
                }
                if (pendingRanges == null && !pending.hasNext()) {
                    List<Split> batch = source.getNextBatch();
                    if (batch.isEmpty()) {
                        exhausted = true;
                        break;
                    }
                    if (cancelled || !assignment.needMoreSplit()) {
                        break;
                    }
                    pending = assignment.assignBatch(batch).asMap().entrySet().iterator();
                }
                more = offerPending();
                if (!more || nanoTime.getAsLong() - start >= MAX_TASK_NANOS) {
                    break;
                }
            }
        } catch (Throwable e) {
            failure = e;
        }
        boolean closing;
        boolean resubmit;
        synchronized (scheduleLock) {
            closing = cancelled || !assignment.needMoreSplit() || exhausted || failure != null;
            resubmit = !closing && (more || wakeupRequested);
            state = closing ? State.CLOSING : resubmit ? State.QUEUED : State.IDLE;
        }
        if (closing) {
            finish(publishFailure(failure));
        } else if (resubmit) {
            submit(true);
        }
    }

    private boolean offerPending() throws UserException {
        while (!cancelled && assignment.needMoreSplit()) {
            if (pendingRanges == null) {
                if (!pending.hasNext()) {
                    return true;
                }
                Map.Entry<Backend, Collection<Split>> entry = pending.next();
                pendingBackend = entry.getKey();
                pendingRanges = assignment.createScanRanges(pendingBackend, entry.getValue());
                pendingRangeOffset = 0;
            }
            int accepted = assignment.offerBatch(pendingBackend, pendingRanges, pendingRangeOffset, resume);
            if (accepted == 0) {
                return false;
            }
            pendingRangeOffset += accepted;
            if (pendingRangeOffset == pendingRanges.size()) {
                pendingRanges = null;
            }
        }
        return false;
    }

    private UserException publishFailure(Throwable failure) {
        if (failure == null || cancelled || assignment.isStop()) {
            return null;
        }
        UserException error = new UserException(failure.getMessage(), failure);
        // Wake the query and stop peers before potentially blocking source closure. Termination
        // still completes only after close and the completion callback have both finished.
        assignment.setException(error);
        return error;
    }

    private void finish(Exception failure) {
        removeAdmission();
        assignment.removeQueueSpaceListener(pendingBackend, resume);
        pending = Collections.emptyIterator();
        pendingRanges = null;
        pendingBackend = null;
        try {
            source.close();
        } catch (Throwable e) {
            if (failure != null) {
                failure.addSuppressed(e);
            } else if (!cancelled && !assignment.isStop()) {
                failure = publishFailure(e);
            } else {
                LOG.warn("Failed to close stopped split producer source", e);
            }
        } finally {
            source = null;
        }
        try {
            completion.accept(failure);
        } catch (Throwable e) {
            assignment.setException(new UserException(e.getMessage(), e));
        } finally {
            completion = null;
            synchronized (scheduleLock) {
                state = State.CLOSED;
            }
            if (registered) {
                assignment.producerFinished(this);
            }
        }
    }

    @Override
    public void close() {
        synchronized (scheduleLock) {
            cancelled = true;
            if (state == State.RUNNING || state == State.CLOSING || state == State.CLOSED) {
                return;
            }
            state = State.CLOSING;
        }
        removeAdmission();
        cleanupExecutor.execute(() -> finish(null));
    }
}
