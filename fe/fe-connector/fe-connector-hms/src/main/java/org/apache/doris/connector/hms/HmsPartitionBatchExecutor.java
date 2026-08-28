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
import shade.doris.hive.org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

final class HmsPartitionBatchExecutor {

    private static final Logger LOG = LogManager.getLogger(HmsPartitionBatchExecutor.class);
    static final long NO_FALLBACK_START_NANOS = Long.MIN_VALUE;

    @FunctionalInterface
    interface Fetcher {
        List<HmsPartitionInfo> fetch(String dbName, String tableName, List<String> partitionNames) throws Exception;
    }

    @FunctionalInterface
    interface Transport {
        List<HmsPartitionInfo> getPartitionsByNames(String dbName, String tableName,
                List<String> partitionNames, RemoteCallTracker tracker) throws Exception;
    }

    static final class RemoteCallException extends HmsClientException {
        RemoteCallException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    static final class Access {
        private final HmsPartitionBatchExecutor executor;
        private final ConnectorMetadataAccessObserver catalogObserver;
        private final LongSupplier nanoTime;

        Access(HmsPartitionBatchExecutor executor, ConnectorMetadataAccessObserver catalogObserver) {
            this(executor, catalogObserver, System::nanoTime);
        }

        Access(HmsPartitionBatchExecutor executor, ConnectorMetadataAccessObserver catalogObserver,
                LongSupplier nanoTime) {
            this.executor = java.util.Objects.requireNonNull(executor, "executor");
            this.catalogObserver = java.util.Objects.requireNonNull(catalogObserver, "catalogObserver");
            this.nanoTime = java.util.Objects.requireNonNull(nanoTime, "nanoTime");
        }

        List<HmsPartitionInfo> load(HmsPartitionRequest request) {
            LogicalAccess access = begin();
            boolean success = false;
            try {
                List<HmsPartitionInfo> result = executor.execute(request, access.execution);
                success = true;
                return result;
            } finally {
                complete(request, access, success);
            }
        }

        LogicalAccess begin() {
            return new LogicalAccess(executor.newExecution(), nanoTime.getAsLong());
        }

        void executeChunks(HmsPartitionRequest request, LogicalAccess access,
                PartitionChunkConsumer chunkConsumer) {
            executor.executeChunks(request, access.execution, chunkConsumer);
        }

        void complete(HmsPartitionRequest request, LogicalAccess access, boolean success) {
            ConnectorMetadataAccessEvent event = access.execution.logicalAccessEvent(request,
                    TimeUnit.NANOSECONDS.toMillis(nanoTime.getAsLong() - access.startNanos), success);
            recordSafely("catalog metrics", catalogObserver, event);
            recordSafely("query profile", request.getMetadataAccessObserver(), event);
        }

        private static void recordSafely(String sinkName, ConnectorMetadataAccessObserver sink,
                ConnectorMetadataAccessEvent event) {
            try {
                sink.record(event);
            } catch (RuntimeException e) {
                LOG.warn("Failed to record HMS partition metadata access in {}", sinkName, e);
            }
        }

        static final class LogicalAccess {
            private final Execution execution;
            private final long startNanos;

            private LogicalAccess(Execution execution, long startNanos) {
                this.execution = execution;
                this.startNanos = startNanos;
            }
        }
    }

    @FunctionalInterface
    interface FailureClassifier {
        boolean isDegradable(Throwable failure);
    }

    @FunctionalInterface
    interface PartitionChunkConsumer {
        void accept(List<HmsPartitionInfo> partitions);
    }

    private final int maxBatchSize;
    private final int minBatchSize;
    private final long fallbackTimeoutMillis;
    private final Transport transport;
    private final FailureClassifier failureClassifier;
    private final LongSupplier nanoTime;

    private HmsPartitionBatchExecutor(Builder builder) {
        this.maxBatchSize = builder.maxBatchSize;
        this.minBatchSize = builder.minBatchSize;
        this.fallbackTimeoutMillis = builder.fallbackTimeoutMillis;
        this.transport = builder.transport;
        this.failureClassifier = builder.failureClassifier;
        this.nanoTime = builder.nanoTime;
    }

    static Builder builder() {
        return new Builder();
    }

    Execution newExecution() {
        return new Execution();
    }

    List<HmsPartitionInfo> execute(HmsPartitionRequest request, Execution execution) {
        List<HmsPartitionInfo> result = new ArrayList<>(request.getPartitionNames().size());
        executeChunks(request, execution, result::addAll);
        return result;
    }

    void executeChunks(HmsPartitionRequest request, Execution execution,
            PartitionChunkConsumer chunkConsumer) {
        List<String> names = request.getPartitionNames();
        if (names.isEmpty()) {
            return;
        }
        long fallbackTimeoutNanos = TimeUnit.MILLISECONDS.toNanos(fallbackTimeoutMillis);
        long fallbackStartNanos = execution.fallbackStartNanos;
        RemoteCallTracker remoteCalls = new RemoteCallTracker(nanoTime);
        int fallbackCount = 0;
        try {
            List<HmsPartitionIdentity.ParsedPartitionName> partitions = request.getPartitions();
            int offset = 0;
            int effectiveBatchSize = execution.effectiveBatchSize(maxBatchSize);
            while (offset < names.size()) {
                checkFallbackTimeout(fallbackStartNanos, fallbackTimeoutNanos);
                int batchSize = Math.min(effectiveBatchSize, names.size() - offset);
                List<String> batch = new ArrayList<>(names.subList(offset, offset + batchSize));
                List<HmsPartitionIdentity.ParsedPartitionName> batchPartitions
                        = partitions.subList(offset, offset + batchSize);
                List<HmsPartitionInfo> returned;
                try {
                    returned = transport.getPartitionsByNames(
                            request.getDbName(), request.getTableName(), batch,
                            remoteCalls);
                } catch (RemoteCallException e) {
                    checkFallbackTimeout(fallbackStartNanos, fallbackTimeoutNanos);
                    if (batchSize <= minBatchSize || !failureClassifier.isDegradable(e)) {
                        throw finalBatchFailure(
                                request, offset, batchSize, effectiveBatchSize,
                                remoteCalls.count, fallbackCount, e);
                    }
                    if (fallbackStartNanos == NO_FALLBACK_START_NANOS) {
                        fallbackStartNanos = execution.startFallback(nanoTime.getAsLong());
                    }
                    checkFallbackTimeout(fallbackStartNanos, fallbackTimeoutNanos);
                    effectiveBatchSize = Math.max(minBatchSize, batchSize / 2);
                    execution.reduceEffectiveBatchSize(effectiveBatchSize);
                    fallbackCount++;
                    continue;
                } catch (RuntimeException e) {
                    throw e;
                } catch (Exception e) {
                    throw new HmsClientException("Unexpected checked failure fetching HMS partitions", e);
                }
                List<HmsPartitionInfo> ordered = validateParsedAndOrder(batchPartitions, returned);
                chunkConsumer.accept(ordered);
                offset += batchSize;
            }
            checkFallbackTimeout(fallbackStartNanos, fallbackTimeoutNanos);
        } finally {
            execution.addPhysicalAccess(remoteCalls, fallbackCount);
        }
    }

    private void checkFallbackTimeout(long fallbackStartNanos, long fallbackTimeoutNanos) {
        if (fallbackStartNanos != NO_FALLBACK_START_NANOS
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
        String message = String.format(
                "HMS partition batch request failed: db=%s, table=%s, requested=%d, offset=%d, "
                        + "failedBatchSize=%d, effectiveBatchSize=%d, minBatchSize=%d, attempts=%d, "
                        + "fallbacks=%d: %s",
                request.getDbName(), request.getTableName(), request.getPartitionNames().size(), offset,
                failedBatchSize, effectiveBatchSize, minBatchSize, rpcCount, fallbackCount,
                failure.getMessage());
        return new HmsClientException(message, failure);
    }

    static final class Execution {
        private int effectiveBatchSize = Integer.MAX_VALUE;
        private long fallbackStartNanos = NO_FALLBACK_START_NANOS;
        private int rpcCount;
        private long rpcItems;
        private int largestBatchSize;
        private int smallestBatchSize = Integer.MAX_VALUE;
        private int fallbackCount;
        private long rpcElapsedMillis;
        private long maxRpcElapsedMillis;

        private int effectiveBatchSize(int configuredMaxBatchSize) {
            return Math.min(configuredMaxBatchSize, effectiveBatchSize);
        }

        private void reduceEffectiveBatchSize(int batchSize) {
            effectiveBatchSize = Math.min(effectiveBatchSize, batchSize);
        }

        private long startFallback(long startNanos) {
            if (fallbackStartNanos == NO_FALLBACK_START_NANOS) {
                fallbackStartNanos = startNanos;
            }
            return fallbackStartNanos;
        }

        private void addPhysicalAccess(RemoteCallTracker calls, int fallbacks) {
            rpcCount += calls.count;
            rpcItems += calls.items;
            largestBatchSize = Math.max(largestBatchSize, calls.largestBatchSize);
            if (calls.smallestBatchSize != Integer.MAX_VALUE) {
                smallestBatchSize = Math.min(smallestBatchSize, calls.smallestBatchSize);
            }
            fallbackCount += fallbacks;
            rpcElapsedMillis += TimeUnit.NANOSECONDS.toMillis(calls.elapsedNanos);
            maxRpcElapsedMillis = Math.max(
                    maxRpcElapsedMillis, TimeUnit.NANOSECONDS.toMillis(calls.maxElapsedNanos));
        }

        ConnectorMetadataAccessEvent logicalAccessEvent(
                HmsPartitionRequest request, long elapsedMillis, boolean success) {
            return ConnectorMetadataAccessEvent.builder()
                    .operation("hms.get_partitions_by_names")
                    .source(request.getSource().name())
                    .requestedItems(request.getPartitionNames().size())
                    .rpcCount(rpcCount)
                    .rpcItems(rpcItems)
                    .largestBatchSize(largestBatchSize)
                    .smallestBatchSize(smallestBatchSize == Integer.MAX_VALUE ? 0 : smallestBatchSize)
                    .fallbackCount(fallbackCount)
                    .logicalElapsedMillis(elapsedMillis)
                    .rpcElapsedMillis(rpcElapsedMillis)
                    .maxRpcElapsedMillis(maxRpcElapsedMillis)
                    .success(success)
                    .build();
        }
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
        if (!(failure instanceof RemoteCallException)) {
            return false;
        }
        boolean thriftFailure = false;
        boolean sizeFailure = false;
        for (Throwable current = failure.getCause(); current != null; current = current.getCause()) {
            thriftFailure |= current instanceof TException;
            String message = current.getMessage();
            if (message != null) {
                String normalized = message.toLowerCase(Locale.ROOT);
                if (normalized.contains("message size") || normalized.contains("max message")
                        || normalized.contains("maxmessagesize")
                        || (normalized.contains("frame size")
                                && normalized.contains("larger than max length"))
                        || normalized.contains("frame too large")
                        || normalized.contains("request too large") || normalized.contains("payload too large")
                        || normalized.contains("too many partitions")
                        || normalized.contains("partition limit")) {
                    sizeFailure = true;
                }
            }
        }
        return thriftFailure && sizeFailure;
    }

    static final class Builder {
        private int maxBatchSize;
        private int minBatchSize = 1;
        private long fallbackTimeoutMillis;
        private Transport transport;
        private FailureClassifier failureClassifier = HmsPartitionBatchExecutor::isDegradableRemoteFailure;
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
            this.transport = (dbName, tableName, partitionNames, remoteCallTracker) ->
                    remoteCallTracker.call(partitionNames.size(),
                            () -> fetcher.fetch(dbName, tableName, partitionNames));
            return this;
        }

        Builder transport(Transport transport) {
            this.transport = transport;
            return this;
        }

        Builder failureClassifier(FailureClassifier failureClassifier) {
            this.failureClassifier = failureClassifier;
            return this;
        }

        Builder nanoTime(LongSupplier nanoTime) {
            this.nanoTime = nanoTime;
            return this;
        }

        HmsPartitionBatchExecutor build() {
            if (maxBatchSize <= 0 || minBatchSize <= 0 || minBatchSize > maxBatchSize) {
                throw new IllegalArgumentException("invalid HMS partition batch size range");
            }
            if (fallbackTimeoutMillis <= 0) {
                throw new IllegalArgumentException("fallback timeout must be positive");
            }
            java.util.Objects.requireNonNull(transport, "transport");
            java.util.Objects.requireNonNull(failureClassifier, "failureClassifier");
            java.util.Objects.requireNonNull(nanoTime, "nanoTime");
            return new HmsPartitionBatchExecutor(this);
        }
    }
}
