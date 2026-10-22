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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
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
    private final Map<Backend, BlockingQueue<Collection<TScanRangeLocations>>> assignment = new HashMap<>();
    private final SplitToScanRange splitToScanRange;
    private final Map<String, String> locationProperties;
    private final List<String> pathPartitionKeys;
    private final boolean fileCacheAdmission;
    private final int splitBatchSize;
    private final int queueCapacity;
    private final Object assignLock = new Object();
    private volatile Split sampleSplit = null;
    private final ConnectorScanContext scanContext = new ConnectorScanContext();
    private final CompletableFuture<Void> termination = new CompletableFuture<>();
    private final Set<SplitSource> consumers = new HashSet<>();
    private int liveProducers;
    private boolean starting;
    private long enumerationStartNanos;
    private long sampleReadyNanos;
    private final AtomicBoolean isStopped = new AtomicBoolean(false);
    private final AtomicBoolean scheduleFinished = new AtomicBoolean(false);

    private volatile UserException exception = null;
    private final List<Closeable> closeableResources = new ArrayList<>();
    private final Map<Backend, Set<Runnable>> queueSpaceListeners = new HashMap<>();

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
        init(30000);
    }

    public void init(long timeoutMillis) throws UserException {
        Preconditions.checkArgument(timeoutMillis > 0, "First split wait timeout must be positive");
        try {
            synchronized (assignLock) {
                starting = true;
            }
            try {
                if (needMoreSplit()) {
                    splitGenerator.startSplit(backendPolicy.numBackends());
                }
            } finally {
                synchronized (assignLock) {
                    starting = false;
                }
                completeTerminationIfReady();
            }
            synchronized (assignLock) {
                if (enumerationStartNanos == 0) {
                    enumerationStartNanos = System.nanoTime();
                }
                long timeoutNanos = TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
                while (sampleSplit == null && needMoreSplit()) {
                    long remaining = timeoutNanos - (System.nanoTime() - enumerationStartNanos);
                    if (remaining <= 0) {
                        throw firstSplitTimeout(timeoutMillis);
                    }
                    TimeUnit.NANOSECONDS.timedWait(assignLock, remaining);
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
        synchronized (assignLock) {
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
        synchronized (assignLock) {
            if (isStopped.get()) {
                return false;
            }
            liveProducers++;
            closeableResources.add(producer);
            return true;
        }
    }

    void producerFinished(SplitProducer producer) {
        synchronized (assignLock) {
            closeableResources.remove(producer);
            liveProducers--;
            Preconditions.checkState(liveProducers >= 0, "Unbalanced split producer completion");
        }
        completeTerminationIfReady();
    }

    private void completeTerminationIfReady() {
        boolean ready;
        synchronized (assignLock) {
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
            Collection<TScanRangeLocations> locations = createScanRanges(backend, batch.get(backend));
            try {
                synchronized (assignLock) {
                    while (needMoreSplit()) {
                        if (getOrCreateAssignmentQueue(backend).offer(locations)) {
                            break;
                        }
                        assignLock.wait(100);
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                UserException failure = new UserException("Interrupted offering split batch", e);
                setException(failure);
                throw failure;
            }
        }
    }

    Multimap<Backend, Split> assignBatch(List<Split> splits) throws UserException {
        Preconditions.checkArgument(splits.size() <= splitBatchSize, "Producer batch exceeds split batch size");
        synchronized (assignLock) {
            scanContext.splitsGenerated(splits.size());
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
        synchronized (assignLock) {
            if (sampleSplit == null && needMoreSplit()) {
                sampleSplit = splits.iterator().next();
                sampleReadyNanos = System.nanoTime();
                assignLock.notifyAll();
            }
        }
        return locations;
    }

    boolean offerBatch(Backend backend, Collection<TScanRangeLocations> locations, Runnable resume) {
        // Coordinate the failed offer and listener registration with consumer notifications so
        // capacity released just before suspension cannot be missed.
        synchronized (assignLock) {
            // An error/stop may race range conversion. Do not register a waiter after its wakeup.
            if (!needMoreSplit()) {
                return false;
            }
            if (getOrCreateAssignmentQueue(backend).offer(locations)) {
                removeQueueSpaceListener(backend, resume);
                return true;
            }
            queueSpaceListeners.computeIfAbsent(backend, key -> new HashSet<>()).add(resume);
            return false;
        }
    }

    void removeQueueSpaceListener(Backend backend, Runnable listener) {
        synchronized (assignLock) {
            Set<Runnable> listeners = queueSpaceListeners.get(backend);
            if (listeners != null) {
                listeners.remove(listener);
                if (listeners.isEmpty()) {
                    queueSpaceListeners.remove(backend);
                }
            }
        }
    }

    void notifyQueueSpace(Backend backend) {
        Set<Runnable> listeners;
        synchronized (assignLock) {
            listeners = queueSpaceListeners.remove(backend);
            assignLock.notifyAll();
        }
        if (listeners != null) {
            listeners.forEach(Runnable::run);
        }
    }

    public void registerSource(long uniqueId) {
        synchronized (assignLock) {
            sources.add(uniqueId);
        }
    }

    public Set<Long> getSources() {
        synchronized (assignLock) {
            return new HashSet<>(sources);
        }
    }

    public Split getSampleSplit() throws UserException {
        synchronized (assignLock) {
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

    void registerConsumer(SplitSource consumer) {
        synchronized (assignLock) {
            if (isStopped.get()) {
                consumer.close();
            } else {
                consumers.add(consumer);
            }
        }
    }

    /** Serialize manager publication with stop: a cancelled planning thread cannot republish a source. */
    public void publishSource(SplitSource source, SplitSourceManager manager) {
        synchronized (assignLock) {
            if (!isStopped.get()) {
                manager.registerSplitSource(source);
            }
        }
    }

    private BlockingQueue<Collection<TScanRangeLocations>> getOrCreateAssignmentQueue(Backend backend) {
        return assignment.computeIfAbsent(backend, be -> new LinkedBlockingQueue<>(queueCapacity));
    }

    public BlockingQueue<Collection<TScanRangeLocations>> getAssignedSplits(Backend backend) throws UserException {
        synchronized (assignLock) {
            if (exception != null) {
                throw exception;
            }
            if (isStopped.get()) {
                return null;
            }
            BlockingQueue<Collection<TScanRangeLocations>> splits = getOrCreateAssignmentQueue(backend);
            return scheduleFinished.get() && splits.isEmpty() ? null : splits;
        }
    }

    public void setException(UserException e) {
        synchronized (assignLock) {
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
        synchronized (assignLock) {
            scheduleFinished.set(true);
            assignLock.notifyAll();
        }
        completeTerminationIfReady();
    }

    /** Stops production and drops buffers. Normal enumeration completion uses finishSchedule instead. */
    public void stop() {
        List<Closeable> resources;
        synchronized (assignLock) {
            if (!isStopped.compareAndSet(false, true)) {
                return;
            }
            scanContext.cancel();
            assignment.values().forEach(Collection::clear);
            assignment.clear();
            queueSpaceListeners.clear();
            sampleSplit = null;
            consumers.forEach(SplitSource::close);
            consumers.clear();
            resources = new ArrayList<>(closeableResources);
            closeableResources.clear();
            assignLock.notifyAll();
        }
        resources.forEach(this::closeResource);
        completeTerminationIfReady();
    }

    public boolean isStop() {
        return isStopped.get();
    }

    public void addCloseable(Closeable resource) {
        synchronized (assignLock) {
            if (!isStop()) {
                closeableResources.add(resource);
                return;
            }
        }
        closeResource(resource);
    }

    void removeCloseable(Closeable resource) {
        synchronized (assignLock) {
            closeableResources.remove(resource);
        }
    }

    private void closeResource(Closeable resource) {
        try {
            resource.close();
        } catch (Exception e) {
            LOG.warn("Failed to close split resource", e);
        }
    }
}
