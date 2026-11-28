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

package org.apache.doris.cdcclient.common;

import org.apache.doris.cdcclient.common.CdcResourceManager.ReaderGroup;

import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Owns the reservation until the connector exits and its polled batches are released. */
public final class ManagedReader<R> implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(ManagedReader.class);

    @FunctionalInterface
    public interface ReaderPoll<R, T> {
        T poll(R reader) throws InterruptedException;
    }

    private final CdcResourceManager resources;
    private final String jobId;
    private final ReaderGroup group;
    private final Function<R, ExecutorService> closeReader;
    private final Runnable releaseQuota;
    private R reader;
    private String snapshotSplitId;
    private boolean closing;
    private boolean closeInProgress;
    // Counts each poll from its start until the returned batch is closed.
    private int pendingBatches;
    private ExecutorService worker;

    ManagedReader(
            CdcResourceManager resources,
            String jobId,
            ReaderGroup group,
            R reader,
            Function<R, ExecutorService> closeReader,
            Runnable releaseQuota) {
        this.resources = resources;
        this.jobId = jobId;
        this.group = group;
        this.reader = reader;
        this.closeReader = closeReader;
        this.releaseQuota = releaseQuota;
    }

    public synchronized R getReader() {
        return closing ? null : reader;
    }

    public synchronized void setSnapshotSplitId(String splitId) {
        snapshotSplitId = splitId;
    }

    public <T> ReaderBatch<T> pollBatch(ReaderPoll<R, Iterator<T>> operation)
            throws InterruptedException {
        R pollingReader;
        synchronized (this) {
            if (closing) {
                return ReaderBatch.wrap(Collections.emptyIterator());
            }
            pollingReader = reader;
            pendingBatches++;
        }
        boolean handedOff = false;
        try {
            ReaderBatch<T> batch =
                    new ReaderBatch<>(operation.poll(pollingReader), this::releaseBatch);
            handedOff = true;
            return batch;
        } finally {
            if (!handedOff) {
                releaseBatch();
            }
        }
    }

    private synchronized void releaseBatch() {
        pendingBatches--;
        if (closing) {
            releaseIfTerminated();
        }
    }

    @Override
    public void close() {
        R closingReader;
        synchronized (this) {
            if ((reader == null && worker == null) || closeInProgress) {
                return;
            }
            closing = true;
            closeInProgress = true;
            closingReader = reader;
        }
        try {
            if (closingReader != null) {
                ExecutorService closingWorker = closeReader.apply(closingReader);
                synchronized (this) {
                    worker = closingWorker;
                    if (worker.isShutdown()) {
                        reader = null;
                    }
                }
            }
        } finally {
            synchronized (this) {
                closeInProgress = false;
                releaseIfTerminated();
                if ((reader != null || worker != null) && resources.registerClosingReader(this)) {
                    LOG.warn(
                            "CDC reader close pending, jobId={}, snapshotSplitId={}, group={}, "
                                    + "reservedBytes={}, pendingBatches={}",
                            jobId,
                            snapshotSplitId,
                            group,
                            resources.getReaderReservationBytes(),
                            pendingBatches);
                }
            }
        }
    }

    synchronized void releaseIfTerminated() {
        if (!closeInProgress && pendingBatches == 0 && worker != null && worker.isTerminated()) {
            resources.unregisterClosingReader(this);
            releaseQuota.run();
            reader = null;
            worker = null;
        }
    }
}
