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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** Scan-local cancellation, bounded legacy batch queues, and actual producer termination. */
public class SplitAssignment {
    private static final Logger LOG = LogManager.getLogger(SplitAssignment.class);
    private static final int QUEUE_CAPACITY = 10000;
    private final Set<Long> sources = new HashSet<>();
    private final FederationBackendPolicy backendPolicy;
    private final SplitGenerator splitGenerator;
    private final ConcurrentHashMap<Backend, BlockingQueue<Collection<TScanRangeLocations>>> assignment
            = new ConcurrentHashMap<>();
    private final SplitToScanRange splitToScanRange;
    private final Map<String, String> locationProperties;
    private final List<String> pathPartitionKeys;
    private final boolean fileCacheAdmission;
    private final Object assignLock = new Object();
    private final AtomicBoolean isStopped = new AtomicBoolean();
    private final AtomicBoolean scheduleFinished = new AtomicBoolean();
    private final SplitScanContext scanContext = new SplitScanContext();
    private final CompletableFuture<Void> termination = new CompletableFuture<>();
    private final List<Closeable> closeableResources = new ArrayList<>();
    private Split sampleSplit;
    private volatile UserException exception;
    private long enumerationStartNanos;
    private long sampleReadyNanos;
    private boolean starting;
    private int liveProducers;

    public SplitAssignment(FederationBackendPolicy backendPolicy, SplitGenerator splitGenerator,
            SplitToScanRange splitToScanRange, Map<String, String> locationProperties,
            List<String> pathPartitionKeys, boolean fileCacheAdmission) {
        this.backendPolicy = backendPolicy;
        this.splitGenerator = splitGenerator;
        this.splitToScanRange = splitToScanRange;
        this.locationProperties = locationProperties;
        this.pathPartitionKeys = pathPartitionKeys;
        this.fileCacheAdmission = fileCacheAdmission;
    }

    public void init() throws UserException {
        init(30000);
    }

    public void init(long timeoutMillis) throws UserException {
        Preconditions.checkArgument(timeoutMillis > 0, "First split wait timeout must be positive");
        try {
            synchronized (assignLock) {
                starting = true;
                enumerationStartNanos = System.nanoTime();
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

    public SplitScanContext getScanContext() {
        return scanContext;
    }

    public CompletableFuture<Void> getTerminationFuture() {
        return termination;
    }

    public void addProducer(Closeable producer) {
        boolean stopped;
        synchronized (assignLock) {
            liveProducers++;
            stopped = isStopped.get();
            if (!stopped) {
                closeableResources.add(producer);
            }
        }
        if (stopped) {
            closeResource(producer);
        }
    }

    public void producerFinished(Closeable producer) {
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

    public Split getSampleSplit() {
        synchronized (assignLock) {
            return sampleSplit;
        }
    }

    public void addToQueue(List<Split> splits) throws UserException {
        if (splits.isEmpty() || !needMoreSplit()) {
            return;
        }
        Multimap<Backend, Split> batch;
        synchronized (assignLock) {
            if (!needMoreSplit()) {
                return;
            }
            batch = backendPolicy.computeScanRangeAssignment(splits);
        }
        for (Backend backend : batch.keySet()) {
            Collection<Split> assigned = batch.get(backend);
            List<TScanRangeLocations> locations = new ArrayList<>(assigned.size());
            for (Split split : assigned) {
                if (!needMoreSplit()) {
                    return;
                }
                locations.add(splitToScanRange.getScanRange(backend, locationProperties, split, pathPartitionKeys,
                        fileCacheAdmission));
            }
            BlockingQueue<Collection<TScanRangeLocations>> queue;
            synchronized (assignLock) {
                if (!needMoreSplit()) {
                    return;
                }
                queue = assignment.computeIfAbsent(backend, be -> new LinkedBlockingQueue<>(QUEUE_CAPACITY));
                if (sampleSplit == null) {
                    sampleSplit = splits.get(0);
                    sampleReadyNanos = System.nanoTime();
                    assignLock.notifyAll();
                }
            }
            while (needMoreSplit()) {
                try {
                    if (queue.offer(locations, 100, TimeUnit.MILLISECONDS)) {
                        // A stopped producer must not leave a late publication in a detached queue.
                        if (isStopped.get()) {
                            queue.clear();
                        }
                        break;
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    setException(new UserException("Interrupted offering split batch", e));
                    return;
                }
            }
        }
        scanContext.splitsGenerated(splits.size());
    }

    public BlockingQueue<Collection<TScanRangeLocations>> getAssignedSplits(Backend backend) throws UserException {
        synchronized (assignLock) {
            if (exception != null) {
                throw exception;
            }
            if (isStopped.get()) {
                return null;
            }
            BlockingQueue<Collection<TScanRangeLocations>> queue = assignment.computeIfAbsent(backend,
                    be -> new LinkedBlockingQueue<>(QUEUE_CAPACITY));
            return scheduleFinished.get() && queue.isEmpty() ? null : queue;
        }
    }

    public void checkException() throws UserException {
        if (exception != null) {
            throw exception;
        }
    }

    public void setException(UserException e) {
        synchronized (assignLock) {
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

    public void stop() {
        List<Closeable> resources;
        synchronized (assignLock) {
            if (!isStopped.compareAndSet(false, true)) {
                return;
            }
            scanContext.cancel();
            assignment.values().forEach(Collection::clear);
            assignment.clear();
            sampleSplit = null;
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
            if (!isStopped.get()) {
                closeableResources.add(resource);
                return;
            }
        }
        closeResource(resource);
    }

    public void removeCloseable(Closeable resource) {
        synchronized (assignLock) {
            closeableResources.remove(resource);
        }
    }

    private void closeResource(Closeable resource) {
        try {
            resource.close();
        } catch (Exception e) {
            LOG.warn("Close split resource failed", e);
        }
    }
}
