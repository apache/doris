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
import org.apache.doris.datasource.scan.FederationBackendPolicy;
import org.apache.doris.spi.Split;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TScanRangeLocations;

import com.google.common.base.Preconditions;
import com.google.common.collect.Multimap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * When file splits are supplied in batch mode, splits are generated lazily and assigned in each call of `getNextBatch`.
 * `SplitGenerator` provides the file splits, and `FederationBackendPolicy` assigns these splits to backends.
 */
public class SplitAssignment {
    private static final Logger LOG = LogManager.getLogger(SplitAssignment.class);
    private final Set<Long> sources = new HashSet<>();
    private final FederationBackendPolicy backendPolicy;
    private final SplitGenerator splitGenerator;
    // Queue identity is the stable backend ID; Backend.hashCode also hashes mutable endpoint/health fields.
    private final ConcurrentHashMap<Long, AssignmentQueue> assignment
            = new ConcurrentHashMap<>();
    private final SplitToScanRange splitToScanRange;
    private final Map<String, String> locationProperties;
    private final List<String> pathPartitionKeys;
    private final boolean fileCacheAdmission;
    private final int splitBatchSize;
    private final int queueCapacity;
    // Backend policy state is mutable; it must not delay publishing an already converted first split.
    private final Object backendAssignmentLock = new Object();
    private final Object readinessLock = new Object();
    private Split sampleSplit = null;
    private final AtomicBoolean isStopped = new AtomicBoolean(false);
    private final AtomicBoolean scheduleFinished = new AtomicBoolean(false);

    private volatile UserException exception = null;
    private final List<Closeable> closeableResources = new ArrayList<>();
    private final ConcurrentHashMap<Long, Set<Runnable>> queueSpaceListeners = new ConcurrentHashMap<>();
    private volatile Runnable moreSplitsRequestedCallback = () -> {};

    /** Data readiness and terminal state use the same per-backend wait channel. */
    private static final class AssignmentQueue {
        private final BlockingQueue<Collection<TScanRangeLocations>> batches;
        // Written under this channel's monitor; producers avoid that monitor when nobody waits.
        private volatile int waitingReaders;

        private AssignmentQueue(int capacity) {
            batches = new LinkedBlockingQueue<>(capacity);
        }

        private void signalReaders() {
            if (waitingReaders > 0) {
                synchronized (this) {
                    notifyAll();
                }
            }
        }
    }

    public SplitAssignment(
            FederationBackendPolicy backendPolicy,
            SplitGenerator splitGenerator,
            SplitToScanRange splitToScanRange,
            Map<String, String> locationProperties,
            List<String> pathPartitionKeys,
            boolean fileCacheAdmission,
            int splitBatchSize,
            int queueCapacity) {
        Preconditions.checkArgument(splitBatchSize > 0, "Split batch size must be positive");
        Preconditions.checkArgument(queueCapacity > 0, "Split queue capacity must be positive");
        this.backendPolicy = backendPolicy;
        this.splitGenerator = splitGenerator;
        this.splitToScanRange = splitToScanRange;
        this.locationProperties = locationProperties;
        this.pathPartitionKeys = pathPartitionKeys;
        this.fileCacheAdmission = fileCacheAdmission;
        this.splitBatchSize = splitBatchSize;
        this.queueCapacity = queueCapacity;
    }

    public void init() throws UserException {
        splitGenerator.startSplit(backendPolicy.numBackends());
        synchronized (readinessLock) {
            final int waitIntervalTimeMillis = 100;
            final int initTimeoutMillis = 30000; // 30s
            int waitTotalTime = 0;
            while (sampleSplit == null && needMoreSplit()) {
                try {
                    readinessLock.wait(waitIntervalTimeMillis);
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
            Collection<TScanRangeLocations> locations = createScanRanges(backend, batch.get(backend));
            while (needMoreSplit()) {
                AssignmentQueue queue = getOrCreateAssignmentQueue(backend);
                try {
                    if (queue.batches.offer(locations, 100, TimeUnit.MILLISECONDS)) {
                        queue.signalReaders();
                        break;
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    setException(new UserException("Failed to offer batch split by interrupted", e));
                }
            }
        }
    }

    Multimap<Backend, Split> assignBatch(List<Split> splits) throws UserException {
        Preconditions.checkArgument(splits.size() <= splitBatchSize, "Producer batch exceeds split batch size");
        synchronized (backendAssignmentLock) {
            return backendPolicy.computeScanRangeAssignment(splits);
        }
    }

    Collection<TScanRangeLocations> createScanRanges(Backend backend, Collection<Split> splits)
            throws UserException {
        List<TScanRangeLocations> locations = new ArrayList<>(splits.size());
        for (Split split : splits) {
            locations.add(splitToScanRange.getScanRange(backend, locationProperties, split, pathPartitionKeys,
                    fileCacheAdmission));
        }
        // Publish readiness after range conversion initializes the scan-level parameters.
        synchronized (readinessLock) {
            if (sampleSplit == null) {
                sampleSplit = splits.iterator().next();
                readinessLock.notifyAll();
            }
        }
        return locations;
    }

    boolean offerBatch(Backend backend, Collection<TScanRangeLocations> locations, Runnable resume) {
        if (!needMoreSplit()) {
            return false;
        }
        AssignmentQueue queue = getOrCreateAssignmentQueue(backend);
        if (queue.batches.offer(locations)) {
            queue.signalReaders();
            removeQueueSpaceListener(backend, resume);
            return true;
        }
        synchronized (queueSpaceListeners) {
            // An error/stop may race range conversion. Do not register a waiter after its wakeup.
            if (!needMoreSplit()) {
                return false;
            }
            queueSpaceListeners.computeIfAbsent(backend.getId(), key -> new HashSet<>()).add(resume);
            // A consumer with no registered waiter skips this monitor. Retry after registration
            // so capacity released between the first offer and registration cannot be missed.
            if (queue.batches.offer(locations)) {
                queue.signalReaders();
                removeQueueSpaceListener(backend, resume);
                return true;
            }
            return false;
        }
    }

    void removeQueueSpaceListener(Backend backend, Runnable listener) {
        if (!queueSpaceListeners.containsKey(backend.getId())) {
            return;
        }
        synchronized (queueSpaceListeners) {
            Set<Runnable> listeners = queueSpaceListeners.get(backend.getId());
            if (listeners != null) {
                listeners.remove(listener);
                if (listeners.isEmpty()) {
                    queueSpaceListeners.remove(backend.getId());
                }
            }
        }
    }

    void notifyQueueSpace(Backend backend) {
        if (!queueSpaceListeners.containsKey(backend.getId())) {
            return;
        }
        Set<Runnable> listeners;
        synchronized (queueSpaceListeners) {
            listeners = queueSpaceListeners.remove(backend.getId());
        }
        if (listeners != null) {
            listeners.forEach(Runnable::run);
        }
    }

    public void registerSource(long uniqueId) {
        sources.add(uniqueId);
    }

    public Set<Long> getSources() {
        return sources;
    }

    public Split getSampleSplit() {
        return sampleSplit;
    }

    public void addToQueue(List<Split> splits) throws UserException {
        for (int begin = 0; begin < splits.size() && needMoreSplit();) {
            int end = begin + Math.min(splitBatchSize, splits.size() - begin);
            List<Split> batchSplits = new ArrayList<>(splits.subList(begin, end));
            appendBatch(assignBatch(batchSplits));
            begin = end;
        }
    }

    public int getSplitBatchSize() {
        return splitBatchSize;
    }

    private void notifyAssignment() {
        synchronized (readinessLock) {
            readinessLock.notifyAll();
        }
        assignment.values().forEach(AssignmentQueue::signalReaders);
    }

    private AssignmentQueue getOrCreateAssignmentQueue(Backend backend) {
        return assignment.computeIfAbsent(backend.getId(), be -> new AssignmentQueue(queueCapacity));
    }

    Collection<TScanRangeLocations> awaitBatch(Backend backend, long waitNanos) throws InterruptedException {
        AssignmentQueue queue = getOrCreateAssignmentQueue(backend);
        synchronized (queue) {
            queue.waitingReaders++;
            try {
                // Register before checking both predicates. An offer or terminal transition either
                // precedes this check or signals after wait releases this channel's monitor.
                if (queue.batches.isEmpty() && needMoreSplit()) {
                    TimeUnit.NANOSECONDS.timedWait(queue, waitNanos);
                }
                return queue.batches.poll();
            } finally {
                queue.waitingReaders--;
            }
        }
    }

    public BlockingQueue<Collection<TScanRangeLocations>> getAssignedSplits(Backend backend) throws UserException {
        if (exception != null) {
            throw exception;
        }
        BlockingQueue<Collection<TScanRangeLocations>> splits = getOrCreateAssignmentQueue(backend).batches;
        if (scheduleFinished.get() && splits.isEmpty() || isStopped.get()) {
            return null;
        }
        return splits;
    }

    public void setException(UserException e) {
        addUserException(e);
        notifyAssignment();
        Set<Runnable> listeners = new HashSet<>();
        synchronized (queueSpaceListeners) {
            queueSpaceListeners.values().forEach(listeners::addAll);
            queueSpaceListeners.clear();
        }
        listeners.forEach(Runnable::run);
    }

    private void addUserException(UserException e) {
        synchronized (readinessLock) {
            // Cancellation can interrupt an active source; preserve failures published before stop.
            if (isStopped.get()) {
                return;
            }
            if (exception != null) {
                exception.addSuppressed(e);
            } else {
                exception = e;
            }
        }
    }

    public void finishSchedule() {
        scheduleFinished.set(true);
        notifyAssignment();
    }

    public void stop() {
        synchronized (readinessLock) {
            if (!isStopped.compareAndSet(false, true)) {
                return;
            }
        }
        notifyAssignment();
        List<Closeable> resources;
        synchronized (closeableResources) {
            resources = new ArrayList<>(closeableResources);
            closeableResources.clear();
            moreSplitsRequestedCallback = () -> {};
        }
        resources.forEach(this::closeResource);
        if (exception != null) {
            throw new RuntimeException(exception);
        }
    }

    public boolean isStop() {
        return isStopped.get();
    }

    /** Registered by the streaming producer before publishing its first split. */
    public void setMoreSplitsRequestedCallback(Runnable callback) {
        synchronized (closeableResources) {
            if (!isStopped.get()) {
                moreSplitsRequestedCallback = callback;
            }
        }
    }

    public void onMoreSplitsRequested() {
        if (needMoreSplit()) {
            moreSplitsRequestedCallback.run();
        }
    }

    public void addCloseable(Closeable resource) {
        synchronized (closeableResources) {
            if (!isStop()) {
                closeableResources.add(resource);
                return;
            }
        }
        closeResource(resource);
    }

    void removeCloseable(Closeable resource) {
        synchronized (closeableResources) {
            closeableResources.remove(resource);
        }
    }

    private void closeResource(Closeable resource) {
        try {
            resource.close();
        } catch (Exception e) {
            LOG.warn("close resource error:{}", e.getMessage(), e);
        }
    }
}
