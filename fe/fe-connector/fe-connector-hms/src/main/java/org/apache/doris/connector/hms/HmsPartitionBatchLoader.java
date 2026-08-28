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

package org.apache.doris.connector.hms;

import org.apache.doris.connector.spi.ConnectorMetadataAccessEvent;
import org.apache.doris.connector.spi.ConnectorMetadataAccessObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/** The single chunking, adaptive fallback, integrity and observability implementation for HMS partitions. */
final class HmsPartitionBatchLoader {

    private static final Logger LOG = LogManager.getLogger(HmsPartitionBatchLoader.class);

    @FunctionalInterface
    interface Fetcher {
        List<HmsPartitionInfo> fetch(String dbName, String tableName, List<String> partitionNames) throws Exception;
    }

    @FunctionalInterface
    interface TrackedFetcher {
        List<HmsPartitionInfo> fetch(String dbName, String tableName, List<String> partitionNames,
                RemoteCallTracker remoteCallTracker) throws Exception;
    }

    @FunctionalInterface
    interface FailureClassifier {
        boolean isDegradable(Throwable failure);
    }

    private final int maxBatchSize;
    private final int minBatchSize;
    private final long fallbackTimeoutMillis;
    private final TrackedFetcher fetcher;
    private final FailureClassifier failureClassifier;
    private final ConnectorMetadataAccessObserver observer;
    private final LongSupplier nanoTime;

    private HmsPartitionBatchLoader(Builder builder) {
        this.maxBatchSize = builder.maxBatchSize;
        this.minBatchSize = builder.minBatchSize;
        this.fallbackTimeoutMillis = builder.fallbackTimeoutMillis;
        this.fetcher = builder.fetcher;
        this.failureClassifier = builder.failureClassifier;
        this.observer = builder.observer;
        this.nanoTime = builder.nanoTime;
    }

    static Builder builder() {
        return new Builder();
    }

    List<HmsPartitionInfo> load(HmsPartitionRequest request) {
        List<String> names = request.getPartitionNames();
        if (names.isEmpty()) {
            return java.util.Collections.emptyList();
        }
        long startNanos = nanoTime.getAsLong();
        long fallbackTimeoutNanos = TimeUnit.MILLISECONDS.toNanos(fallbackTimeoutMillis);
        long fallbackStartNanos = request.fallbackStartNanos();
        RemoteCallTracker remoteCalls = new RemoteCallTracker(nanoTime);
        int fallbackCount = 0;
        boolean success = false;
        try {
            List<HmsPartitionInfo> result = new ArrayList<>(names.size());
            List<HmsPartitionIdentity.ParsedPartitionName> partitions = request.getPartitions();
            int offset = 0;
            int effectiveBatchSize = request.effectiveBatchSize(maxBatchSize);
            while (offset < names.size()) {
                checkFallbackTimeout(fallbackStartNanos, fallbackTimeoutNanos);
                int batchSize = Math.min(effectiveBatchSize, names.size() - offset);
                List<String> batch = new ArrayList<>(names.subList(offset, offset + batchSize));
                List<HmsPartitionIdentity.ParsedPartitionName> batchPartitions
                        = partitions.subList(offset, offset + batchSize);
                List<HmsPartitionInfo> returned;
                try {
                    returned = fetcher.fetch(
                            request.getDbName(), request.getTableName(), batch,
                            remoteCalls);
                } catch (HmsRemoteCallException e) {
                    checkFallbackTimeout(fallbackStartNanos, fallbackTimeoutNanos);
                    if (batchSize <= minBatchSize || !failureClassifier.isDegradable(e)) {
                        throw finalBatchFailure(
                                request, offset, batchSize, effectiveBatchSize,
                                remoteCalls.count, fallbackCount, e);
                    }
                    if (fallbackStartNanos == HmsPartitionRequest.NO_FALLBACK_START_NANOS) {
                        fallbackStartNanos = request.startFallback(nanoTime.getAsLong());
                    }
                    checkFallbackTimeout(fallbackStartNanos, fallbackTimeoutNanos);
                    effectiveBatchSize = Math.max(minBatchSize, batchSize / 2);
                    request.reduceEffectiveBatchSize(effectiveBatchSize);
                    fallbackCount++;
                    continue;
                } catch (RuntimeException e) {
                    // Authorization and local programming failures are not transport fallback candidates.
                    // Preserve their original type and stack instead of disguising them as a failed HMS batch.
                    throw e;
                } catch (Exception e) {
                    throw new HmsClientException("Unexpected checked failure fetching HMS partitions", e);
                }
                // Integrity validation and cache publication are deliberately outside the remote-failure catch:
                // a malformed response or a local write-back bug must never trigger transport fallback or be
                // wrapped as an HMS RPC failure.
                List<HmsPartitionInfo> ordered = validateParsedAndOrder(batchPartitions, returned);
                request.getPartitionChunkConsumer().accept(batch, ordered);
                result.addAll(ordered);
                offset += batchSize;
            }
            checkFallbackTimeout(fallbackStartNanos, fallbackTimeoutNanos);
            success = true;
            return result;
        } finally {
            ConnectorMetadataAccessEvent event = ConnectorMetadataAccessEvent.builder()
                    .operation("hms.get_partitions_by_names")
                    .source(request.getSource().name())
                    .requestedItems(names.size())
                    .rpcCount(remoteCalls.count)
                    .rpcItems(remoteCalls.items)
                    .largestBatchSize(remoteCalls.largestBatchSize)
                    .smallestBatchSize(remoteCalls.smallestBatchSize == Integer.MAX_VALUE
                            ? 0 : remoteCalls.smallestBatchSize)
                    .fallbackCount(fallbackCount)
                    .logicalElapsedMillis(TimeUnit.NANOSECONDS.toMillis(nanoTime.getAsLong() - startNanos))
                    .rpcElapsedMillis(TimeUnit.NANOSECONDS.toMillis(remoteCalls.elapsedNanos))
                    .maxRpcElapsedMillis(TimeUnit.NANOSECONDS.toMillis(remoteCalls.maxElapsedNanos))
                    .success(success)
                    .build();
            if (request.isLogicalAccessOwner()) {
                recordSafely("catalog metrics", observer, event);
                recordSafely("query profile", request.getMetadataAccessObserver(), event);
            } else {
                request.addPhysicalAccess(event);
            }
        }
    }

    private void recordSafely(String sinkName, ConnectorMetadataAccessObserver sink,
            ConnectorMetadataAccessEvent event) {
        try {
            sink.record(event);
        } catch (RuntimeException e) {
            LOG.warn("Failed to record HMS partition metadata access in {}", sinkName, e);
        }
    }

    private void checkFallbackTimeout(long fallbackStartNanos, long fallbackTimeoutNanos) {
        if (fallbackStartNanos != HmsPartitionRequest.NO_FALLBACK_START_NANOS
                && nanoTime.getAsLong() - fallbackStartNanos >= fallbackTimeoutNanos) {
            throw new HmsClientException("HMS partition batch fallback timeout exceeded");
        }
    }

    static List<HmsPartitionInfo> validateParsedAndOrder(
            List<HmsPartitionIdentity.ParsedPartitionName> requested,
            List<HmsPartitionInfo> returned) {
        int expectedValueCount = requested.get(0).getValues().size();
        Map<List<String>, Integer> expected = new HashMap<>();
        for (int i = 0; i < requested.size(); i++) {
            HmsPartitionIdentity.ParsedPartitionName partition = requested.get(i);
            List<String> identity = partition.getValues();
            Integer previous = expected.put(identity, i);
            if (previous != null) {
                throw new IllegalArgumentException(
                        "duplicate partition identity in request: " + partition.getName());
            }
        }
        HmsPartitionResultException.Builder failure = HmsPartitionResultException.builder(
                requested.size(), returned == null ? 0 : returned.size());
        List<HmsPartitionInfo> ordered = new ArrayList<>(java.util.Collections.nCopies(requested.size(), null));
        Map<List<String>, Integer> returnedCounts = new LinkedHashMap<>();
        if (returned == null) {
            failure.invalid("<null response>");
        } else {
            for (int i = 0; i < returned.size(); i++) {
                HmsPartitionInfo partition = returned.get(i);
                if (partition == null) {
                    failure.invalid("<null partition>");
                    continue;
                }
                List<String> identity = partition.getValues();
                if (identity.size() != expectedValueCount) {
                    failure.invalid(identity.toString());
                    continue;
                }
                returnedCounts.merge(identity, 1, Integer::sum);
                Integer index = expected.get(identity);
                if (index != null && ordered.get(index) == null) {
                    ordered.set(index, partition);
                }
            }
        }
        for (int i = 0; i < requested.size(); i++) {
            if (!returnedCounts.containsKey(requested.get(i).getValues())) {
                failure.missing(requested.get(i).getName());
            }
        }
        for (Map.Entry<List<String>, Integer> entry : returnedCounts.entrySet()) {
            if (!expected.containsKey(entry.getKey())) {
                failure.unexpected(entry.getKey().toString());
            }
            if (entry.getValue() > 1) {
                failure.duplicate(entry.getKey().toString());
            }
        }
        if (failure.hasMismatches()) {
            throw failure.build();
        }
        return ordered;
    }

    private RuntimeException finalBatchFailure(HmsPartitionRequest request, int offset,
            int failedBatchSize, int effectiveBatchSize, int rpcCount, int fallbackCount, Exception failure) {
        if (failure instanceof HmsPartitionResultException) {
            return (RuntimeException) failure;
        }
        String message = String.format(
                "HMS partition batch request failed: db=%s, table=%s, requested=%d, offset=%d, "
                        + "failedBatchSize=%d, effectiveBatchSize=%d, minBatchSize=%d, attempts=%d, "
                        + "fallbacks=%d: %s",
                request.getDbName(), request.getTableName(), request.getPartitionNames().size(), offset,
                failedBatchSize, effectiveBatchSize, minBatchSize, rpcCount, fallbackCount,
                failure.getMessage());
        return new HmsClientException(message, failure);
    }

    static final class RemoteCallTracker {
        private final LongSupplier nanoTime;
        private int count;
        private long items;
        private long elapsedNanos;
        private long maxElapsedNanos;
        private int largestBatchSize;
        private int smallestBatchSize = Integer.MAX_VALUE;

        RemoteCallTracker(LongSupplier nanoTime) {
            this.nanoTime = nanoTime;
        }

        <T> T call(int itemCount, Callable<T> remoteCall) throws Exception {
            count++;
            items += itemCount;
            largestBatchSize = Math.max(largestBatchSize, itemCount);
            smallestBatchSize = Math.min(smallestBatchSize, itemCount);
            long startNanos = nanoTime.getAsLong();
            try {
                return remoteCall.call();
            } finally {
                long currentElapsedNanos = nanoTime.getAsLong() - startNanos;
                elapsedNanos += currentElapsedNanos;
                maxElapsedNanos = Math.max(maxElapsedNanos, currentElapsedNanos);
            }
        }
    }

    static boolean isDegradableRemoteFailure(Throwable failure) {
        if (!(failure instanceof HmsRemoteCallException)) {
            return false;
        }
        for (Throwable current = failure.getCause(); current != null; current = current.getCause()) {
            String message = current.getMessage();
            if (message != null) {
                String normalized = message.toLowerCase(Locale.ROOT);
                if (normalized.contains("message size") || normalized.contains("max message")
                        || normalized.contains("maxmessagesize")
                        || normalized.contains("frame size") || normalized.contains("frame too large")
                        || normalized.contains("request too large") || normalized.contains("payload too large")
                        || normalized.contains("too many partitions")
                        || normalized.contains("partition limit")) {
                    return true;
                }
            }
        }
        return false;
    }

    static final class Builder {
        private int maxBatchSize;
        private int minBatchSize = 1;
        private long fallbackTimeoutMillis;
        private TrackedFetcher fetcher;
        private FailureClassifier failureClassifier = HmsPartitionBatchLoader::isDegradableRemoteFailure;
        private ConnectorMetadataAccessObserver observer = ConnectorMetadataAccessObserver.NOOP;
        private LongSupplier nanoTime = System::nanoTime;

        private Builder() {
        }

        Builder maxBatchSize(int maxBatchSize) {
            this.maxBatchSize = maxBatchSize;
            return this;
        }

        Builder minBatchSize(int minBatchSize) {
            this.minBatchSize = minBatchSize;
            return this;
        }

        Builder fallbackTimeoutMillis(long fallbackTimeoutMillis) {
            this.fallbackTimeoutMillis = fallbackTimeoutMillis;
            return this;
        }

        Builder fetcher(Fetcher fetcher) {
            java.util.Objects.requireNonNull(fetcher, "fetcher");
            this.fetcher = (dbName, tableName, partitionNames, remoteCallTracker) ->
                    remoteCallTracker.call(partitionNames.size(),
                            () -> fetcher.fetch(dbName, tableName, partitionNames));
            return this;
        }

        Builder trackedFetcher(TrackedFetcher fetcher) {
            this.fetcher = fetcher;
            return this;
        }

        Builder failureClassifier(FailureClassifier failureClassifier) {
            this.failureClassifier = failureClassifier;
            return this;
        }

        Builder observer(ConnectorMetadataAccessObserver observer) {
            this.observer = observer;
            return this;
        }

        Builder nanoTime(LongSupplier nanoTime) {
            this.nanoTime = nanoTime;
            return this;
        }

        HmsPartitionBatchLoader build() {
            if (maxBatchSize <= 0 || minBatchSize <= 0 || minBatchSize > maxBatchSize) {
                throw new IllegalArgumentException("invalid HMS partition batch size range");
            }
            if (fallbackTimeoutMillis <= 0) {
                throw new IllegalArgumentException("fallback timeout must be positive");
            }
            java.util.Objects.requireNonNull(fetcher, "fetcher");
            java.util.Objects.requireNonNull(failureClassifier, "failureClassifier");
            java.util.Objects.requireNonNull(observer, "observer");
            java.util.Objects.requireNonNull(nanoTime, "nanoTime");
            return new HmsPartitionBatchLoader(this);
        }
    }
}
