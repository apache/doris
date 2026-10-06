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
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.datasource.scan.FederationBackendPolicy;
import org.apache.doris.spi.Split;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TScanRangeLocations;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Multimap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * When file splits are supplied in batch mode, splits are generated lazily and assigned in each call of `getNextBatch`.
 * `SplitGenerator` provides the file splits, and `FederationBackendPolicy` assigns these splits to backends.
 *
 * <p>An assignment serves one dispatch of its plan and holds what the backends need from this frontend while they
 * scan: the split sources they fetch the splits from, and what the generator opened to produce them (the Flight SQL
 * session of a remote Doris scan). {@link #start()} makes the sources reachable to the backends and starts the
 * generator, {@link #stop()} releases all of it. The coordinator dispatching the plan starts the assignment and
 * stops it when it closes or cancels (ScanNode#start, ScanNode#stopAll), so a plan nobody dispatches holds nothing.
 * A scan that has to start generating splits while it is planned (FileQueryScanNode#needsSampleSplit) starts its
 * assignment then ({@link #startWhilePlanning}): the assignment is the statement's until the coordinator dispatching
 * the plan takes it over, and the statement stops it when it ends if none did ({@link #stopIfNotDispatched}).
 */
public class SplitAssignment {
    private static final Logger LOG = LogManager.getLogger(SplitAssignment.class);
    private final FederationBackendPolicy backendPolicy;
    private final SplitGenerator splitGenerator;
    private final ConcurrentHashMap<Backend, BlockingQueue<Collection<TScanRangeLocations>>> assignment
            = new ConcurrentHashMap<>();
    private final SplitToScanRange splitToScanRange;
    private final Map<String, String> locationProperties;
    private final List<String> pathPartitionKeys;
    private final boolean fileCacheAdmission;
    private final SplitSourceManager splitSourceManager;
    private final Object assignLock = new Object();
    private Split sampleSplit = null;
    private final AtomicBoolean isStopped = new AtomicBoolean(false);
    private final AtomicBoolean scheduleFinished = new AtomicBoolean(false);

    private UserException exception = null;

    // Whether the generator was started, whether a coordinator took the assignment over, the split sources of the
    // backends, and what stop() closes. Guarded by lifecycleLock, under which isStopped is set as well: start() may
    // race stop() (a coordinator cancelled while it dispatches the plan), and so may addCloseable() (a generator still
    // opening what it hands over when the scan is stopped).
    private final Object lifecycleLock = new Object();
    private boolean started = false;
    // Set when the coordinator dispatching the plan takes the assignment over (start()), which owns it from then on.
    // Until then an assignment started while its scan was planned is the statement's, and nothing fetches its splits.
    private boolean dispatched = false;
    // Until when the generator of an assignment started while planned waits for a coordinator to take it over, once a
    // backend's queue is full (stopIfNeverDispatched).
    private long undispatchedDeadlineMs = Long.MAX_VALUE;
    // The query whose planning started the assignment (startWhilePlanning), for the logs of its stop: the generator
    // stopping it runs on a thread of the shared executor, which knows no query.
    private TUniqueId plannedByQueryId = null;
    private final List<SplitSource> sources = new ArrayList<>();
    private final List<Closeable> closeableResources = new ArrayList<>();

    public SplitAssignment(
            FederationBackendPolicy backendPolicy,
            SplitGenerator splitGenerator,
            SplitToScanRange splitToScanRange,
            Map<String, String> locationProperties,
            List<String> pathPartitionKeys,
            boolean fileCacheAdmission,
            SplitSourceManager splitSourceManager) {
        this.backendPolicy = backendPolicy;
        this.splitGenerator = splitGenerator;
        this.splitToScanRange = splitToScanRange;
        this.locationProperties = locationProperties;
        this.pathPartitionKeys = pathPartitionKeys;
        this.fileCacheAdmission = fileCacheAdmission;
        this.splitSourceManager = splitSourceManager;
    }

    /**
     * Starts the assignment for the coordinator dispatching the plan, once the query is admitted (ScanNode#start):
     * makes the split sources reachable to the backends and starts the generator, then waits for its first split.
     * Takes over an assignment its scan started while planned instead: either way the coordinator owns it from now on,
     * and stops it when it closes or cancels. Starts once, and a stopped assignment - its coordinator was cancelled
     * before it got here - not at all.
     */
    public void start() throws UserException {
        synchronized (lifecycleLock) {
            dispatched = true;
        }
        startOnce();
    }

    /**
     * Starts the assignment while its scan is planned, for a scan that plans with its first split
     * (FileQueryScanNode#needsSampleSplit). It is the statement's until the coordinator dispatching the plan takes it
     * over ({@link #start()}), and the statement stops it when it ends if none did ({@link #stopIfNotDispatched()}).
     * Nothing fetches its splits meanwhile, so once a backend's queue is full the generator waits for the dispatch -
     * for undispatchedTimeoutMs at most, the timeout of the statement, past which no coordinator takes the plan any
     * more: the generator then stops the assignment, should the statement have ended without stopping it.
     * plannedByQueryId, the query of that statement, names it in the logs of the stop.
     */
    public void startWhilePlanning(long undispatchedTimeoutMs, TUniqueId plannedByQueryId) throws UserException {
        synchronized (lifecycleLock) {
            undispatchedDeadlineMs = System.currentTimeMillis() + undispatchedTimeoutMs;
            this.plannedByQueryId = plannedByQueryId;
        }
        startOnce();
    }

    // Makes the split sources reachable to the backends and starts the generator, then waits for its first split
    // (init()). Once, and a stopped assignment not at all.
    private void startOnce() throws UserException {
        synchronized (lifecycleLock) {
            if (started || isStopped.get()) {
                return;
            }
            started = true;
            for (SplitSource source : sources) {
                splitSourceManager.registerSplitSource(source);
            }
        }
        init();
    }

    // Starts the generator and waits for its first split, for startOnce() alone: it makes the split sources reachable
    // first, and starts an assignment once - and a stopped one not at all.
    @VisibleForTesting
    void init() throws UserException {
        splitGenerator.startSplit(backendPolicy.numBackends());
        synchronized (assignLock) {
            final int waitIntervalTimeMillis = 100;
            final int initTimeoutMillis = 30000; // 30s
            int waitTotalTime = 0;
            while (sampleSplit == null && needMoreSplit()) {
                try {
                    assignLock.wait(waitIntervalTimeMillis);
                } catch (InterruptedException e) {
                    throw new UserException(e.getMessage(), e);
                }
                waitTotalTime += waitIntervalTimeMillis;
                if (waitTotalTime > initTimeoutMillis) {
                    throw new UserException("Failed to get first split after waiting for "
                            + (waitTotalTime / 1000) + " seconds.");
                }
            }
        }
        if (exception != null) {
            throw exception;
        }
    }

    public boolean needMoreSplit() {
        return !scheduleFinished.get() && !isStopped.get() && exception == null;
    }

    private void appendBatch(Multimap<Backend, Split> batch) throws UserException {
        for (Backend backend : batch.keySet()) {
            Collection<Split> splits = batch.get(backend);
            List<TScanRangeLocations> locations = new ArrayList<>(splits.size());
            for (Split split : splits) {
                locations.add(splitToScanRange.getScanRange(backend, locationProperties, split, pathPartitionKeys,
                        fileCacheAdmission));
            }
            while (needMoreSplit()) {
                BlockingQueue<Collection<TScanRangeLocations>> queue =
                        assignment.computeIfAbsent(backend, be -> new LinkedBlockingQueue<>(10000));
                try {
                    if (queue.offer(locations, 100, TimeUnit.MILLISECONDS)) {
                        break;
                    }
                } catch (InterruptedException e) {
                    addUserException(new UserException("Failed to offer batch split by interrupted", e));
                }
                stopIfNeverDispatched();
            }
        }
        // stop() drops what is queued, but a batch offered while it ran may have landed after that.
        if (isStopped.get()) {
            dropQueuedSplits();
        }
    }

    // The queue of the backend is full, and only that backend empties it, once a coordinator dispatched the plan. The
    // plan of an assignment started while planned that no coordinator took over by the timeout of its statement never
    // will be - the statement ended without stopping it, or was killed for its timeout - so the generator stops it
    // rather than wait forever on a thread of the shared executor, holding the splits it queued.
    private void stopIfNeverDispatched() {
        long deadlineMs;
        TUniqueId queryId;
        synchronized (lifecycleLock) {
            if (dispatched || System.currentTimeMillis() < undispatchedDeadlineMs) {
                return;
            }
            deadlineMs = undispatchedDeadlineMs;
            queryId = plannedByQueryId;
        }
        LOG.warn("Stop generating the splits of {}, planned by query {}: no coordinator dispatched the plan by {},"
                + " the timeout of the statement that planned it", splitGenerator, DebugUtil.printId(queryId),
                TimeUtils.longToTimeString(deadlineMs));
        stopIfNotDispatched();
    }

    /**
     * Records the split source a backend fetches its splits from: reachable to the backend once the assignment has
     * started ({@link #start()}, {@link #startWhilePlanning}) - at once if it has already - until {@link #stop()}.
     */
    public void registerSource(SplitSource source) {
        synchronized (lifecycleLock) {
            sources.add(source);
            if (started && !isStopped.get()) {
                splitSourceManager.registerSplitSource(source);
            }
        }
    }

    public Split getSampleSplit() {
        return sampleSplit;
    }

    public void addToQueue(List<Split> splits) throws UserException {
        if (splits.isEmpty()) {
            return;
        }
        Multimap<Backend, Split> batch = null;
        synchronized (assignLock) {
            if (sampleSplit == null) {
                sampleSplit = splits.get(0);
                assignLock.notify();
            }
            batch = backendPolicy.computeScanRangeAssignment(splits);
        }
        appendBatch(batch);
    }

    private void notifyAssignment() {
        synchronized (assignLock) {
            assignLock.notify();
        }
    }

    public BlockingQueue<Collection<TScanRangeLocations>> getAssignedSplits(Backend backend) throws UserException {
        if (exception != null) {
            throw exception;
        }
        BlockingQueue<Collection<TScanRangeLocations>> splits = assignment.computeIfAbsent(backend,
                be -> new LinkedBlockingQueue<>());
        if (scheduleFinished.get() && splits.isEmpty() || isStopped.get()) {
            return null;
        }
        return splits;
    }

    public void setException(UserException e) {
        addUserException(e);
        notifyAssignment();
    }

    private void addUserException(UserException e) {
        if (exception != null) {
            exception.addSuppressed(e);
        } else {
            exception = e;
        }
    }

    public void finishSchedule() {
        scheduleFinished.set(true);
        notifyAssignment();
    }

    /**
     * Stops the generator, makes the split sources unreachable to the backends, drops the splits they did not fetch
     * and closes what was handed over ({@link #addCloseable}). Idempotent. A failure of the asynchronous split
     * generation is rethrown, once all of that is released.
     */
    public void stop() {
        if (stopOnce(false) && exception != null) {
            throw new RuntimeException(exception);
        }
    }

    /**
     * Stops the assignment ({@link #stop()}) unless a coordinator took it over ({@link #start()}): for the statement
     * whose planning started it ({@link #startWhilePlanning}), when the statement ends. No coordinator took it by then,
     * so its plan was never dispatched and nothing will fetch its splits. Never throws: a failure of the split
     * generation is logged instead, as no backend read those splits and the statement that planned them is over.
     */
    public void stopIfNotDispatched() {
        if (stopOnce(true) && exception != null) {
            LOG.warn("The split generation of {}, planned by query {} and never dispatched, had failed",
                    splitGenerator, DebugUtil.printId(plannedByQueryId), exception);
        }
    }

    // Stops the assignment unless it is stopped already, or onlyIfNotDispatched and a coordinator took it over. Returns
    // whether it did.
    private boolean stopOnce(boolean onlyIfNotDispatched) {
        List<SplitSource> toUnregister;
        List<Closeable> toClose;
        synchronized (lifecycleLock) {
            if (isStopped.get() || (onlyIfNotDispatched && dispatched)) {
                return false;
            }
            isStopped.set(true);
            toUnregister = new ArrayList<>(sources);
            toClose = new ArrayList<>(closeableResources);
            closeableResources.clear();
        }
        for (SplitSource source : toUnregister) {
            splitSourceManager.removeSplitSource(source.getUniqueId());
        }
        dropQueuedSplits();
        toClose.forEach(this::closeQuietly);
        notifyAssignment();
        return true;
    }

    // Nothing fetches the splits of a stopped assignment, its sources being unreachable: what still references it (its
    // scan node, and the plan of that) must not keep them on the heap.
    private void dropQueuedSplits() {
        assignment.values().forEach(Collection::clear);
    }

    public boolean isStop() {
        return isStopped.get();
    }

    /**
     * Hands over what the backends need until the scan is over, for {@link #stop()} to close: the Flight SQL session
     * a remote Doris query runs in. One handed over once the assignment is stopped is closed at once - the generator
     * was still opening it when the scan was stopped, and nothing would close it later.
     */
    public void addCloseable(Closeable resource) {
        synchronized (lifecycleLock) {
            if (!isStopped.get()) {
                closeableResources.add(resource);
                return;
            }
        }
        closeQuietly(resource);
    }

    private void closeQuietly(Closeable resource) {
        try {
            resource.close();
        } catch (Exception e) {
            LOG.warn("close resource error:{}", e.getMessage(), e);
        }
    }
}
