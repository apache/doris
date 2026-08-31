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

import shade.doris.hive.org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/** Splits one logical partition request into bounded, validated HMS RPCs. */
final class HmsPartitionBatchExecutor {

    @FunctionalInterface
    interface FailureClassifier {
        boolean isDegradable(Throwable failure);
    }

    static final class RemoteCallException extends HmsClientException {
        RemoteCallException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    private final int maxBatchSize;
    private final int minBatchSize;
    private final long fallbackTimeoutNanos;
    private final HmsPartitionTransport transport;
    private final FailureClassifier failureClassifier;
    private final LongSupplier nanoTime;

    private HmsPartitionBatchExecutor(Builder builder) {
        this.maxBatchSize = builder.maxBatchSize;
        this.minBatchSize = builder.minBatchSize;
        this.fallbackTimeoutNanos = TimeUnit.MILLISECONDS.toNanos(builder.fallbackTimeoutMillis);
        this.transport = builder.transport;
        this.failureClassifier = builder.failureClassifier;
        this.nanoTime = builder.nanoTime;
    }

    static Builder builder() {
        return new Builder();
    }

    List<HmsPartitionInfo> execute(HmsPartitionRequest request) {
        return executeWithStats(request).getPartitions();
    }

    HmsPartitionBatchResult executeWithStats(HmsPartitionRequest request) {
        long logicalStartNanos = System.nanoTime();
        List<HmsPartitionIdentity.ParsedPartitionName> partitions = request.getPartitions();
        if (partitions.isEmpty()) {
            HmsPartitionBatchStats stats = HmsPartitionBatchStats.builder()
                    .logicalElapsedNanos(System.nanoTime() - logicalStartNanos)
                    .build();
            return new HmsPartitionBatchResult(new ArrayList<>(), stats);
        }

        List<HmsPartitionInfo> result = new ArrayList<>(partitions.size());
        int offset = 0;
        int effectiveBatchSize = maxBatchSize;
        int attempts = 0;
        int fallbackCount = 0;
        long rpcItems = 0;
        long rpcElapsedNanos = 0;
        long maxRpcElapsedNanos = 0;
        int largestBatchSize = 0;
        int smallestBatchSize = Integer.MAX_VALUE;
        boolean fallbackStarted = false;
        long fallbackStartNanos = 0;
        while (offset < partitions.size()) {
            checkFallbackTimeout(request, offset, fallbackStarted, fallbackStartNanos);
            int batchSize = Math.min(effectiveBatchSize, partitions.size() - offset);
            List<HmsPartitionIdentity.ParsedPartitionName> batch =
                    partitions.subList(offset, offset + batchSize);
            List<String> batchNames = new ArrayList<>(batch.size());
            for (HmsPartitionIdentity.ParsedPartitionName partition : batch) {
                batchNames.add(partition.getName());
            }
            attempts++;
            rpcItems += batchSize;
            largestBatchSize = Math.max(largestBatchSize, batchSize);
            smallestBatchSize = Math.min(smallestBatchSize, batchSize);
            long rpcStartNanos = System.nanoTime();
            try {
                List<HmsPartitionInfo> returned = transport.getPartitionsByNames(
                        request.getDbName(), request.getTableName(), batchNames);
                result.addAll(validateAndOrder(batch, returned));
                offset += batchSize;
            } catch (RemoteCallException e) {
                if (batchSize <= minBatchSize || !failureClassifier.isDegradable(e)) {
                    throw finalBatchFailure(request, offset, batchSize, effectiveBatchSize,
                            attempts, fallbackCount, e);
                }
                if (!fallbackStarted) {
                    fallbackStartNanos = nanoTime.getAsLong();
                    fallbackStarted = true;
                }
                checkFallbackTimeout(request, offset, true, fallbackStartNanos);
                effectiveBatchSize = Math.max(minBatchSize, batchSize / 2);
                fallbackCount++;
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new HmsClientException("Unexpected checked failure fetching HMS partitions", e);
            } finally {
                long elapsedNanos = System.nanoTime() - rpcStartNanos;
                rpcElapsedNanos += elapsedNanos;
                maxRpcElapsedNanos = Math.max(maxRpcElapsedNanos, elapsedNanos);
            }
        }
        checkFallbackTimeout(request, offset, fallbackStarted, fallbackStartNanos);
        HmsPartitionBatchStats stats = HmsPartitionBatchStats.builder()
                .requestedItems(partitions.size())
                .rpcAttempts(attempts)
                .rpcItems(rpcItems)
                .largestBatchSize(largestBatchSize)
                .smallestBatchSize(smallestBatchSize)
                .fallbackCount(fallbackCount)
                .logicalElapsedNanos(System.nanoTime() - logicalStartNanos)
                .rpcElapsedNanos(rpcElapsedNanos)
                .maxRpcElapsedNanos(maxRpcElapsedNanos)
                .build();
        return new HmsPartitionBatchResult(result, stats);
    }

    private void checkFallbackTimeout(HmsPartitionRequest request, int offset,
            boolean fallbackStarted, long fallbackStartNanos) {
        if (fallbackStarted && nanoTime.getAsLong() - fallbackStartNanos >= fallbackTimeoutNanos) {
            throw new HmsClientException(
                    "HMS partition batch fallback timeout exceeded: db=%s, table=%s, requested=%d, "
                            + "offset=%d, timeoutMs=%d",
                    request.getDbName(), request.getTableName(), request.getPartitions().size(), offset,
                    TimeUnit.NANOSECONDS.toMillis(fallbackTimeoutNanos));
        }
    }

    static List<HmsPartitionInfo> validateAndOrder(
            List<HmsPartitionIdentity.ParsedPartitionName> requested,
            List<HmsPartitionInfo> returned) {
        int expectedValueCount = requested.get(0).getValues().size();
        Map<List<String>, Integer> expected = new HashMap<>();
        for (int i = 0; i < requested.size(); i++) {
            expected.put(requested.get(i).getValues(), i);
        }

        HmsPartitionResultException.Builder failure = HmsPartitionResultException.builder(
                requested.size(), returned == null ? 0 : returned.size());
        List<HmsPartitionInfo> ordered = new ArrayList<>(java.util.Collections.nCopies(requested.size(), null));
        Map<List<String>, Integer> returnedCounts = new LinkedHashMap<>();
        if (returned == null) {
            failure.invalid("<null response>");
        } else {
            for (HmsPartitionInfo partition : returned) {
                if (partition == null) {
                    failure.invalid("<null partition>");
                    continue;
                }
                List<String> identity = partition.getValues();
                if (identity == null || identity.size() != expectedValueCount) {
                    failure.invalid(String.valueOf(identity));
                    continue;
                }
                returnedCounts.merge(identity, 1, Integer::sum);
                Integer index = expected.get(identity);
                if (index != null && ordered.get(index) == null) {
                    ordered.set(index, partition);
                }
            }
        }
        for (HmsPartitionIdentity.ParsedPartitionName partition : requested) {
            if (!returnedCounts.containsKey(partition.getValues())) {
                failure.missing(partition.getName());
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

    private HmsClientException finalBatchFailure(HmsPartitionRequest request, int offset,
            int failedBatchSize, int effectiveBatchSize, int attempts, int fallbackCount,
            RemoteCallException failure) {
        return new HmsClientException(String.format(
                "HMS partition batch request failed: db=%s, table=%s, requested=%d, offset=%d, "
                        + "failedBatchSize=%d, effectiveBatchSize=%d, minBatchSize=%d, attempts=%d, "
                        + "fallbacks=%d: %s",
                request.getDbName(), request.getTableName(), request.getPartitions().size(), offset,
                failedBatchSize, effectiveBatchSize, minBatchSize, attempts, fallbackCount,
                failure.getMessage()), failure);
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
                sizeFailure |= normalized.contains("message size")
                        || normalized.contains("max message")
                        || normalized.contains("maxmessagesize")
                        || normalized.contains("frame too large")
                        || normalized.contains("request too large")
                        || normalized.contains("payload too large")
                        || normalized.contains("too many partitions")
                        || normalized.contains("partition limit")
                        || normalized.contains("hive.metastore.limit.partition.request")
                        || (normalized.contains("partitions scanned")
                                && normalized.contains("exceeds limit"))
                        || (normalized.contains("frame size")
                                && normalized.contains("larger than max length"));
            }
        }
        return thriftFailure && sizeFailure;
    }

    static final class Builder {
        private int maxBatchSize;
        private int minBatchSize = 1;
        private long fallbackTimeoutMillis;
        private HmsPartitionTransport transport;
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

        Builder transport(HmsPartitionTransport transport) {
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
