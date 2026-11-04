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

package org.apache.doris.datasource.hive;

import org.apache.hadoop.hive.metastore.api.Partition;
import shade.doris.hive.org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/** Splits one logical partition request into bounded, validated HMS RPCs. */
final class HmsPartitionBatchExecutor {

    static final class RemoteCallException extends HMSClientException {
        RemoteCallException(String messageDetail, Throwable cause) {
            // The two-argument parent constructor treats the message as a format string; route the detail
            // through an explicit %s argument so partition names containing '%' cannot break formatting.
            super("Remote HMS partition operation failed: %s", cause, messageDetail);
        }
    }

    private final int maxBatchSize;
    private final HmsPartitionTransport transport;

    HmsPartitionBatchExecutor(int maxBatchSize, HmsPartitionTransport transport) {
        if (maxBatchSize <= 0) {
            throw new IllegalArgumentException("invalid HMS partition batch size");
        }
        this.maxBatchSize = maxBatchSize;
        this.transport = java.util.Objects.requireNonNull(transport, "transport");
    }

    HmsPartitionBatchResult executeExistingWithStats(HmsPartitionRequest request) {
        return executeWithStats(request, true);
    }

    HmsPartitionBatchResult executeWithStats(HmsPartitionRequest request) {
        return executeWithStats(request, false);
    }

    private HmsPartitionBatchResult executeWithStats(HmsPartitionRequest request, boolean allowMissing) {
        long logicalStartNanos = System.nanoTime();
        List<HmsPartitionIdentity.ParsedPartitionName> partitions = request.getPartitions();
        if (partitions.isEmpty()) {
            HmsPartitionBatchStats stats = HmsPartitionBatchStats.builder()
                    .logicalElapsedNanos(System.nanoTime() - logicalStartNanos)
                    .build();
            return new HmsPartitionBatchResult(new ArrayList<>(), stats);
        }

        List<Partition> result = new ArrayList<>(partitions.size());
        int offset = 0;
        int effectiveBatchSize = maxBatchSize;
        int transportInvocations = 0;
        int fallbackCount = 0;
        long transportItems = 0;
        long transportElapsedNanos = 0;
        long maxTransportElapsedNanos = 0;
        int largestBatchSize = 0;
        int smallestBatchSize = Integer.MAX_VALUE;
        while (offset < partitions.size()) {
            int batchSize = Math.min(effectiveBatchSize, partitions.size() - offset);
            List<HmsPartitionIdentity.ParsedPartitionName> batch =
                    partitions.subList(offset, offset + batchSize);
            List<String> batchNames = new ArrayList<>(batch.size());
            for (HmsPartitionIdentity.ParsedPartitionName partition : batch) {
                batchNames.add(partition.getName());
            }
            transportInvocations++;
            transportItems += batchSize;
            largestBatchSize = Math.max(largestBatchSize, batchSize);
            smallestBatchSize = Math.min(smallestBatchSize, batchSize);
            long transportStartNanos = System.nanoTime();
            HMSClientException terminalFailure = null;
            try {
                List<Partition> returned = transport.getPartitionsByNames(
                        request.getDbName(), request.getTableName(), batchNames);
                result.addAll(validateAndOrder(batch, returned, allowMissing));
                offset += batchSize;
            } catch (RemoteCallException e) {
                if (batchSize == 1 || !isDegradableRemoteFailure(e)) {
                    terminalFailure = finalBatchFailure(request, offset, batchSize, effectiveBatchSize,
                            transportInvocations, fallbackCount, e);
                } else {
                    effectiveBatchSize = Math.max(1, batchSize / 2);
                    fallbackCount++;
                }
            } catch (HMSClientException e) {
                terminalFailure = e;
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                terminalFailure = new HMSClientException(
                        "Unexpected checked failure fetching HMS partitions", e);
            } finally {
                long elapsedNanos = System.nanoTime() - transportStartNanos;
                transportElapsedNanos += elapsedNanos;
                maxTransportElapsedNanos = Math.max(maxTransportElapsedNanos, elapsedNanos);
            }
            if (terminalFailure != null) {
                throw terminalFailure.withPartitionBatchStats(buildStats(
                        partitions.size(), transportInvocations, transportItems,
                        largestBatchSize, smallestBatchSize,
                        fallbackCount, System.nanoTime() - logicalStartNanos,
                        transportElapsedNanos, maxTransportElapsedNanos));
            }
        }
        HmsPartitionBatchStats stats = buildStats(
                partitions.size(), transportInvocations, transportItems, largestBatchSize, smallestBatchSize,
                fallbackCount, System.nanoTime() - logicalStartNanos,
                transportElapsedNanos, maxTransportElapsedNanos);
        return new HmsPartitionBatchResult(result, stats);
    }

    private static HmsPartitionBatchStats buildStats(
            int requestedItems, int invocations, long transportItems, int largestBatchSize,
            int smallestBatchSize, int fallbackCount, long logicalElapsedNanos,
            long transportElapsedNanos, long maxTransportElapsedNanos) {
        return HmsPartitionBatchStats.builder()
                .requestedItems(requestedItems)
                .transportInvocations(invocations)
                .transportItems(transportItems)
                .largestBatchSize(largestBatchSize)
                .smallestBatchSize(smallestBatchSize)
                .fallbackCount(fallbackCount)
                .logicalElapsedNanos(logicalElapsedNanos)
                .transportElapsedNanos(transportElapsedNanos)
                .maxTransportElapsedNanos(maxTransportElapsedNanos)
                .build();
    }

    private static List<Partition> validateAndOrder(
            List<HmsPartitionIdentity.ParsedPartitionName> requested,
            List<Partition> returned, boolean allowMissing) {
        int expectedValueCount = requested.get(0).getValues().size();
        Map<List<String>, Integer> expected = new HashMap<>();
        for (int i = 0; i < requested.size(); i++) {
            expected.put(requested.get(i).getValues(), i);
        }

        HmsPartitionResultException.Builder failure = HmsPartitionResultException.builder(
                requested.size(), returned == null ? 0 : returned.size());
        List<Partition> ordered = new ArrayList<>(java.util.Collections.nCopies(requested.size(), null));
        Map<List<String>, Integer> returnedCounts = new LinkedHashMap<>();
        if (returned == null) {
            failure.invalid("<null response>");
        } else {
            for (Partition partition : returned) {
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
            if (!allowMissing && !returnedCounts.containsKey(partition.getValues())) {
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
        if (!allowMissing) {
            return ordered;
        }
        List<Partition> existing = new ArrayList<>(returnedCounts.size());
        for (Partition partition : ordered) {
            if (partition != null) {
                existing.add(partition);
            }
        }
        return existing;
    }

    private HMSClientException finalBatchFailure(HmsPartitionRequest request, int offset,
            int failedBatchSize, int effectiveBatchSize, int transportInvocations, int fallbackCount,
            RemoteCallException failure) {
        return new HMSClientException(
                "HMS partition batch request failed: db=%s, table=%s, requested=%d, offset=%d, "
                        + "failedBatchSize=%d, effectiveBatchSize=%d, "
                        + "transportInvocations=%d, "
                        + "fallbacks=%d: %s",
                failure,
                request.getDbName(), request.getTableName(), request.getPartitions().size(), offset,
                failedBatchSize, effectiveBatchSize, transportInvocations, fallbackCount,
                failure.getMessage());
    }

    private static boolean isDegradableRemoteFailure(RemoteCallException failure) {
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

}
