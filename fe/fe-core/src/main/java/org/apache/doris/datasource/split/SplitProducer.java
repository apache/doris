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
import java.util.function.Consumer;

/**
 * Serially advances one batch per task, yielding the enumeration executor under backpressure.
 * A running worker owns source closure. For an idle/queued producer, close claims ownership under
 * scheduleLock and dispatches cleanup separately; a stale queued run then does nothing. Neither
 * plugin code nor assignment callbacks run under scheduleLock.
 */
public class SplitProducer implements Runnable, Closeable {
    private static final Logger LOG = LogManager.getLogger(SplitProducer.class);

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
    private BatchSource source;
    private Consumer<Exception> completion;
    private final Runnable resume = this::schedule;
    private final Object scheduleLock = new Object();
    private State state = State.IDLE;
    private boolean wakeupRequested;
    private boolean registered;
    private volatile boolean cancelled;
    private Iterator<Map.Entry<Backend, Collection<Split>>> pending = Collections.emptyIterator();
    private Backend pendingBackend;
    private Collection<TScanRangeLocations> pendingRanges;

    public SplitProducer(SplitAssignment assignment, Executor executor, BatchSource source,
            Consumer<Exception> completion) {
        this(assignment, executor, source, completion, SplitTaskCleanup.executor());
    }

    SplitProducer(SplitAssignment assignment, Executor executor, BatchSource source,
            Consumer<Exception> completion, Executor cleanupExecutor) {
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
        try {
            executor.execute(this);
            // close() can run between claiming QUEUED and execute(). Remove an admission that
            // raced that earlier removal, so a saturated pool does not retain the stopped scan.
            if (cancelled && executor instanceof ThreadPoolExecutor) {
                ((ThreadPoolExecutor) executor).remove(this);
            }
        } catch (RuntimeException e) {
            synchronized (scheduleLock) {
                if (state != State.QUEUED) {
                    return;
                }
                state = State.CLOSING;
            }
            UserException failure = publishFailure(e);
            cleanupExecutor.execute(() -> finish(failure));
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
        Exception failure = null;
        try {
            if (!cancelled && assignment.needMoreSplit()) {
                if (pendingRanges == null && !pending.hasNext()) {
                    List<Split> batch = source.getNextBatch();
                    if (batch.isEmpty()) {
                        exhausted = true;
                    } else if (!cancelled && assignment.needMoreSplit()) {
                        pending = assignment.assignBatch(batch).asMap().entrySet().iterator();
                    }
                }
                if (!exhausted) {
                    more = offerPending();
                }
            }
        } catch (Exception e) {
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
            submit();
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
            }
            if (!assignment.offerBatch(pendingBackend, pendingRanges, resume)) {
                return false;
            }
            pendingRanges = null;
        }
        return false;
    }

    private UserException publishFailure(Exception failure) {
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
        assignment.removeQueueSpaceListener(pendingBackend, resume);
        pending = Collections.emptyIterator();
        pendingRanges = null;
        pendingBackend = null;
        try {
            source.close();
        } catch (Exception e) {
            if (failure != null) {
                failure.addSuppressed(e);
            } else if (!cancelled && !assignment.isStop()) {
                failure = e;
            } else {
                LOG.warn("Failed to close stopped split producer source", e);
            }
        } finally {
            source = null;
        }
        try {
            completion.accept(failure);
        } catch (Exception e) {
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
        if (executor instanceof ThreadPoolExecutor) {
            ((ThreadPoolExecutor) executor).remove(this);
        }
        cleanupExecutor.execute(() -> finish(null));
    }
}
