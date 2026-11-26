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
import org.apache.doris.connector.spi.scan.ConnectorScanContext;
import org.apache.doris.datasource.scan.FederationBackendPolicy;
import org.apache.doris.spi.Split;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TScanRangeLocations;

import com.google.common.base.Preconditions;
import com.google.common.collect.Multimap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
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
    private volatile Split sampleSplit = null;
    private final ConnectorScanContext scanContext = new ConnectorScanContext();
    private final CompletableFuture<Void> termination = new CompletableFuture<>();
    private int liveProducers;
    private boolean starting;
    private long enumerationStartNanos;
    private long sampleReadyNanos;
    private final AtomicBoolean isStopped = new AtomicBoolean(false);
    private final AtomicBoolean scheduleFinished = new AtomicBoolean(false);

    private volatile UserException exception = null;
    private final List<Closeable> closeableResources = new ArrayList<>();
    private final ConcurrentHashMap<Long, Set<Runnable>> queueSpaceListeners = new ConcurrentHashMap<>();
    private volatile Runnable moreSplitsRequestedCallback = () -> {};

    /** Per-backend scan ranges; bulk operations amortize synchronization without reserving an entire batch. */
    static final class AssignmentQueue {
        private final Deque<TScanRangeLocations> splits = new ArrayDeque<>();
        private final int capacity;
        private int waitingReaders;
        private int waitingProducers;
        private boolean stopped;

        private AssignmentQueue(int capacity) {
            this.capacity = capacity;
        }

        synchronized int offer(List<TScanRangeLocations> batch, int offset) {
            Preconditions.checkArgument(!batch.isEmpty() && offset >= 0 && offset < batch.size(),
                    "Invalid split batch offset: %s", offset);
            int accepted = stopped ? 0 : Math.min(batch.size() - offset, capacity - splits.size());
            for (int i = offset; i < offset + accepted; i++) {
                splits.addLast(batch.get(i));
            }
            if (accepted > 0 && waitingReaders > 0) {
                notifyAll();
            }
            return accepted;
        }

        synchronized int offer(List<TScanRangeLocations> batch, int offset, long timeout, TimeUnit unit)
                throws InterruptedException {
            Preconditions.checkArgument(!batch.isEmpty() && offset >= 0 && offset < batch.size(),
                    "Invalid split batch offset: %s", offset);
            long remaining = unit.toNanos(timeout);
            long deadline = System.nanoTime() + remaining;
            while (!stopped && splits.size() == capacity) {
                if (remaining <= 0) {
                    return 0;
                }
                waitingProducers++;
                try {
                    TimeUnit.NANOSECONDS.timedWait(this, remaining);
                } finally {
                    waitingProducers--;
                }
                remaining = deadline - System.nanoTime();
            }
            return offer(batch, offset);
        }

        synchronized TScanRangeLocations poll() {
            TScanRangeLocations split = splits.pollFirst();
            if (split != null && waitingProducers > 0) {
                notifyAll();
            }
            return split;
        }

        synchronized int drainTo(Collection<TScanRangeLocations> target, int maxSplits) {
            int count = 0;
            while (count < maxSplits && !splits.isEmpty()) {
                target.add(splits.removeFirst());
                count++;
            }
            if (count > 0 && waitingProducers > 0) {
                notifyAll();
            }
            return count;
        }

        synchronized TScanRangeLocations awaitSplit(long waitNanos, SplitAssignment assignment)
                throws InterruptedException {
            waitingReaders++;
            try {
                if (splits.isEmpty() && assignment.needMoreSplit()) {
                    TimeUnit.NANOSECONDS.timedWait(this, waitNanos);
                }
                return poll();
            } finally {
                waitingReaders--;
            }
        }

        synchronized boolean isEmpty() {
            return splits.isEmpty();
        }

        synchronized int size() {
            return splits.size();
        }

        synchronized int remainingCapacity() {
            return capacity - splits.size();
        }

        synchronized TScanRangeLocations peek() {
            return splits.peekFirst();
        }

        synchronized void stop() {
            stopped = true;
            splits.clear();
            notifyAll();
        }

        synchronized void signalWaiters() {
            if (waitingReaders > 0 || waitingProducers > 0) {
                notifyAll();
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
        this.splitBatchSize = Math.min(splitBatchSize, queueCapacity);
        this.queueCapacity = queueCapacity;
    }

    public void init() throws UserException {
        init(30000);
    }

    public void init(long timeoutMillis) throws UserException {
        Preconditions.checkArgument(timeoutMillis > 0, "First split wait timeout must be positive");
        try {
            synchronized (readinessLock) {
                starting = true;
            }
            try {
                if (needMoreSplit()) {
                    splitGenerator.startSplit(backendPolicy.numBackends());
                }
            } finally {
                synchronized (readinessLock) {
                    starting = false;
                }
                completeTerminationIfReady();
            }
            synchronized (readinessLock) {
                if (enumerationStartNanos == 0) {
                    enumerationStartNanos = System.nanoTime();
                }
                long timeoutNanos = TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
                while (sampleSplit == null && needMoreSplit()) {
                    long remaining = timeoutNanos - (System.nanoTime() - enumerationStartNanos);
                    if (remaining <= 0) {
                        throw firstSplitTimeout(timeoutMillis);
                    }
                    TimeUnit.NANOSECONDS.timedWait(readinessLock, remaining);
                }
                if (exception != null) {
                    throw exception;
                }
                if (sampleSplit != null && sampleReadyNanos - enumerationStartNanos > timeoutNanos) {
                    throw firstSplitTimeout(timeoutMillis);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            setException(new UserException("Interrupted waiting for the first split", e));
        } catch (UserException e) {
            setException(e);
        } catch (RuntimeException e) {
            setException(new UserException(e.getMessage(), e));
        }
        if (exception != null) {
            throw exception;
        }
    }

    private UserException firstSplitTimeout(long timeoutMillis) {
        return new UserException("Timed out waiting for the first split after " + timeoutMillis
                + " ms: " + scanContext.describeProgress() + ", active_producers=" + liveProducers);
    }

    /** Called after scan preparation, immediately before submitting enumeration tasks. */
    public void startEnumeration() {
        synchronized (readinessLock) {
            enumerationStartNanos = System.nanoTime();
        }
    }

    public ConnectorScanContext getScanContext() {
        return scanContext;
    }

    public CompletableFuture<Void> getTerminationFuture() {
        return termination;
    }

    boolean addProducer(SplitProducer producer) {
        synchronized (readinessLock) {
            if (isStopped.get()) {
                return false;
            }
            liveProducers++;
            closeableResources.add(producer);
            return true;
        }
    }

    void producerFinished(SplitProducer producer) {
        synchronized (readinessLock) {
            closeableResources.remove(producer);
            liveProducers--;
            Preconditions.checkState(liveProducers >= 0, "Unbalanced split producer completion");
        }
        completeTerminationIfReady();
    }

    private void completeTerminationIfReady() {
        boolean ready;
        synchronized (readinessLock) {
            ready = !starting && liveProducers == 0 && (scheduleFinished.get() || isStopped.get());
        }
        if (ready) {
            termination.complete(null);
        }
    }

    public boolean needMoreSplit() {
        return !scheduleFinished.get() && !isStopped.get() && exception == null;
    }

    private void appendBatch(Multimap<Backend, Split> batch) throws UserException {
        for (Backend backend : batch.keySet()) {
            List<TScanRangeLocations> locations = createScanRanges(backend, batch.get(backend));
            int offset = 0;
            while (offset < locations.size() && needMoreSplit()) {
                AssignmentQueue queue = getOrCreateAssignmentQueue(backend);
                if (queue == null) {
                    break;
                }
                try {
                    offset += queue.offer(locations, offset, 100, TimeUnit.MILLISECONDS);
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
            scanContext.splitsGenerated(splits.size());
            return backendPolicy.computeScanRangeAssignment(splits);
        }
    }

    List<TScanRangeLocations> createScanRanges(Backend backend, Collection<Split> splits)
            throws UserException {
        List<TScanRangeLocations> locations = new ArrayList<>(splits.size());
        for (Split split : splits) {
            locations.add(splitToScanRange.getScanRange(backend, locationProperties, split, pathPartitionKeys,
                    fileCacheAdmission));
        }
        // Publish readiness after range conversion initializes the scan-level parameters.
        synchronized (readinessLock) {
            if (sampleSplit == null && needMoreSplit()) {
                sampleSplit = splits.iterator().next();
                sampleReadyNanos = System.nanoTime();
                readinessLock.notifyAll();
            }
        }
        return locations;
    }

    int offerBatch(Backend backend, List<TScanRangeLocations> locations, int offset, Runnable resume) {
        if (!needMoreSplit()) {
            return 0;
        }
        AssignmentQueue queue = getOrCreateAssignmentQueue(backend);
        if (queue == null) {
            return 0;
        }
        int accepted = queue.offer(locations, offset);
        if (accepted > 0) {
            removeQueueSpaceListener(backend, resume);
            return accepted;
        }
        synchronized (queueSpaceListeners) {
            // An error/stop may race range conversion. Do not register a waiter after its wakeup.
            if (!needMoreSplit()) {
                return 0;
            }
            queueSpaceListeners.computeIfAbsent(backend.getId(), key -> new HashSet<>()).add(resume);
            // A consumer with no registered waiter skips this monitor. Retry after registration
            // so capacity released between the first offer and registration cannot be missed.
            accepted = queue.offer(locations, offset);
            if (accepted > 0) {
                removeQueueSpaceListener(backend, resume);
                return accepted;
            }
            return 0;
        }
    }

    void removeQueueSpaceListener(Backend backend, Runnable listener) {
        if (backend == null || !queueSpaceListeners.containsKey(backend.getId())) {
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
        if (backend == null || !queueSpaceListeners.containsKey(backend.getId())) {
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
        synchronized (readinessLock) {
            sources.add(uniqueId);
        }
    }

    public Set<Long> getSources() {
        synchronized (readinessLock) {
            return new HashSet<>(sources);
        }
    }

    public Split getSampleSplit() throws UserException {
        synchronized (readinessLock) {
            if (exception != null) {
                throw exception;
            }
            return sampleSplit;
        }
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

    /** Serialize manager publication with stop: a cancelled planning thread cannot republish a source. */
    public void publishSource(SplitSource source, SplitSourceManager manager) {
        synchronized (readinessLock) {
            if (!isStopped.get()) {
                manager.registerSplitSource(source);
            }
        }
    }

    private void notifyAssignment() {
        synchronized (readinessLock) {
            readinessLock.notifyAll();
        }
        assignment.values().forEach(AssignmentQueue::signalWaiters);
    }

    private AssignmentQueue getOrCreateAssignmentQueue(Backend backend) {
        synchronized (readinessLock) {
            return isStopped.get() ? null
                    : assignment.computeIfAbsent(backend.getId(), be -> new AssignmentQueue(queueCapacity));
        }
    }

    TScanRangeLocations awaitSplit(Backend backend, long waitNanos) throws InterruptedException {
        AssignmentQueue queue = getOrCreateAssignmentQueue(backend);
        return queue == null ? null : queue.awaitSplit(waitNanos, this);
    }

    AssignmentQueue getAssignedSplits(Backend backend) throws UserException {
        synchronized (readinessLock) {
            if (exception != null) {
                throw exception;
            }
            AssignmentQueue splits = getOrCreateAssignmentQueue(backend);
            return splits == null || scheduleFinished.get() && splits.isEmpty() ? null : splits;
        }
    }

    public void setException(UserException e) {
        synchronized (readinessLock) {
            // A successful LIMIT/cleanup already owns the terminal state. A producer or profile
            // callback finishing later must not turn its EOF endpoints into errors.
            if (isStopped.get() && exception == null) {
                return;
            }
            if (exception == null) {
                exception = e;
            } else if (exception != e) {
                exception.addSuppressed(e);
            }
        }
        stop();
    }

    public void finishSchedule() {
        synchronized (readinessLock) {
            scheduleFinished.set(true);
            readinessLock.notifyAll();
        }
        notifyAssignment();
        completeTerminationIfReady();
    }

    public void stop() {
        List<Closeable> resources;
        synchronized (readinessLock) {
            if (!isStopped.compareAndSet(false, true)) {
                return;
            }
            scanContext.cancel();
            assignment.values().forEach(AssignmentQueue::stop);
            assignment.clear();
            synchronized (queueSpaceListeners) {
                queueSpaceListeners.clear();
            }
            moreSplitsRequestedCallback = () -> {};
            sampleSplit = null;
            resources = new ArrayList<>(closeableResources);
            closeableResources.clear();
            readinessLock.notifyAll();
        }
        resources.forEach(this::closeResource);
        completeTerminationIfReady();
    }

    public boolean isStop() {
        return isStopped.get();
    }

    /** Registered by the streaming producer before publishing its first split. */
    public void setMoreSplitsRequestedCallback(Runnable callback) {
        synchronized (readinessLock) {
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
        synchronized (readinessLock) {
            if (!isStop()) {
                closeableResources.add(resource);
                return;
            }
        }
        closeResource(resource);
    }

    void removeCloseable(Closeable resource) {
        synchronized (readinessLock) {
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
