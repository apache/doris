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
    private final ConcurrentHashMap<Backend, BlockingQueue<Collection<TScanRangeLocations>>> assignment
            = new ConcurrentHashMap<>();
    private final SplitToScanRange splitToScanRange;
    private final Map<String, String> locationProperties;
    private final List<String> pathPartitionKeys;
    private final Object assignLock = new Object();
    private Split sampleSplit = null;
    private final AtomicBoolean isStopped = new AtomicBoolean(false);
    private final AtomicBoolean scheduleFinished = new AtomicBoolean(false);

    private UserException exception = null;
    private final List<Closeable> closeableResources = new ArrayList<>();

    public SplitAssignment(
            FederationBackendPolicy backendPolicy,
            SplitGenerator splitGenerator,
            SplitToScanRange splitToScanRange,
            Map<String, String> locationProperties,
            List<String> pathPartitionKeys) {
        this.backendPolicy = backendPolicy;
        this.splitGenerator = splitGenerator;
        this.splitToScanRange = splitToScanRange;
        this.locationProperties = locationProperties;
        this.pathPartitionKeys = pathPartitionKeys;
    }

    public void init() throws UserException {
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
                locations.add(splitToScanRange.getScanRange(backend, locationProperties, split, pathPartitionKeys));
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
            }
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

    public void stop() {
        if (isStop()) {
            return;
        }
        isStopped.set(true);
        closeableResources.forEach((closeable) -> {
            try {
                closeable.close();
            } catch (Exception e) {
                LOG.warn("close resource error:{}", e.getMessage(), e);
                // ignore
            }
        });
        notifyAssignment();
        if (exception != null) {
            throw new RuntimeException(exception);
        }
    }

    public boolean isStop() {
        return isStopped.get();
    }

    public void addCloseable(Closeable resource) {
        closeableResources.add(resource);
    }
}
