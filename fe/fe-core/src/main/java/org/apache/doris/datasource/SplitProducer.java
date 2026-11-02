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
import java.util.function.Consumer;

/**
 * Serially advances one producer batch per task, yielding the shared executor when a BE queue is
 * full. Each producer retains at most one assigned batch and resumes when a consumer frees space.
 */
public class SplitProducer implements Runnable, Closeable {
    private static final Logger LOG = LogManager.getLogger(SplitProducer.class);

    public interface BatchSource extends Closeable {
        List<Split> getNextBatch() throws Exception;

        @Override
        default void close() throws IOException {
        }
    }

    private final SplitAssignment assignment;
    private final Executor executor;
    private final BatchSource source;
    private final Consumer<Exception> completion;
    private final Runnable resume = this::schedule;
    private final Object scheduleLock = new Object();
    private boolean scheduled;
    private boolean wakeupRequested;
    private boolean completed;
    private volatile boolean cancelled;
    private Iterator<Map.Entry<Backend, Collection<Split>>> pending = Collections.emptyIterator();
    private Backend pendingBackend;
    private Collection<TScanRangeLocations> pendingRanges;

    public SplitProducer(SplitAssignment assignment, Executor executor, BatchSource source,
            Consumer<Exception> completion) {
        this.assignment = assignment;
        this.executor = executor;
        this.source = source;
        this.completion = completion;
    }

    public void start() {
        assignment.addCloseable(this);
        schedule();
    }

    private void schedule() {
        synchronized (scheduleLock) {
            if (completed) {
                return;
            }
            wakeupRequested = true;
            if (scheduled) {
                return;
            }
            scheduled = true;
        }
        submit();
    }

    private void submit() {
        try {
            executor.execute(this);
        } catch (RuntimeException e) {
            complete(cancelled || !assignment.needMoreSplit() ? null : e);
        }
    }

    @Override
    public void run() {
        synchronized (scheduleLock) {
            wakeupRequested = false;
        }
        boolean more = false;
        try {
            more = produceBatch();
        } catch (Exception e) {
            complete(e);
        }
        synchronized (scheduleLock) {
            if (completed || (!more && !wakeupRequested)) {
                scheduled = false;
                return;
            }
        }
        submit();
    }

    private boolean produceBatch() throws Exception {
        if (cancelled || !assignment.needMoreSplit()) {
            complete(null);
            return false;
        }
        if (pendingRanges == null && !pending.hasNext()) {
            List<Split> batch = source.getNextBatch();
            if (batch.isEmpty()) {
                complete(null);
                return false;
            }
            pending = assignment.assignBatch(batch).asMap().entrySet().iterator();
        }
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
                if (cancelled || !assignment.needMoreSplit()) {
                    complete(null);
                }
                return false;
            }
            pendingRanges = null;
        }
        complete(null);
        return false;
    }

    private void complete(Exception failure) {
        synchronized (scheduleLock) {
            if (completed) {
                return;
            }
            completed = true;
        }
        assignment.removeQueueSpaceListener(pendingBackend, resume);
        assignment.removeCloseable(this);
        try {
            completion.accept(failure);
        } catch (Exception e) {
            assignment.setException(new UserException(e.getMessage(), e));
        } finally {
            try {
                source.close();
            } catch (Exception e) {
                LOG.warn("Failed to close split producer source", e);
            }
        }
    }

    @Override
    public void close() {
        cancelled = true;
        schedule();
    }
}
