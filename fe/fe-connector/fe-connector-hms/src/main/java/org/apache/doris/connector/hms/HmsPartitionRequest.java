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
import org.apache.doris.connector.spi.ConnectorMetadataAccessSource;
import org.apache.doris.connector.spi.ConnectorSession;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/** Immutable logical HMS request data with request-scoped adaptive execution state. */
final class HmsPartitionRequest {

    static final long NO_FALLBACK_START_NANOS = Long.MIN_VALUE;

    private final String dbName;
    private final String tableName;
    private final List<String> partitionNames;
    private final List<HmsPartitionIdentity.ParsedPartitionName> partitions;
    private final ConnectorMetadataAccessSource source;
    private final ConnectorMetadataAccessObserver metadataAccessObserver;
    private final PartitionChunkConsumer partitionChunkConsumer;
    private final BatchExecutionState batchExecutionState;
    private final boolean logicalAccessOwner;

    private HmsPartitionRequest(Builder builder) {
        this.dbName = builder.dbName;
        this.tableName = builder.tableName;
        this.partitions = builder.partitions == null
                ? parsePartitions(builder.partitionNames, builder.partitionParser)
                : Collections.unmodifiableList(new ArrayList<>(builder.partitions));
        List<String> names = new ArrayList<>(partitions.size());
        for (HmsPartitionIdentity.ParsedPartitionName partition : partitions) {
            names.add(partition.getName());
        }
        this.partitionNames = Collections.unmodifiableList(names);
        this.source = builder.source;
        this.metadataAccessObserver = builder.metadataAccessObserver;
        this.partitionChunkConsumer = builder.partitionChunkConsumer;
        this.batchExecutionState = builder.batchExecutionState;
        this.logicalAccessOwner = builder.logicalAccessOwner;
    }

    static HmsPartitionRequest from(ConnectorSession session, ConnectorMetadataAccessSource source,
            String dbName, String tableName, List<String> partitionNames) {
        return builder()
                .database(dbName)
                .table(tableName)
                .partitionNames(partitionNames)
                .source(source)
                .metadataAccessObserver(session == null
                        ? ConnectorMetadataAccessObserver.NOOP : session.getMetadataAccessObserver())
                .build();
    }

    static Builder builder() {
        return new Builder();
    }

    String getDbName() {
        return dbName;
    }

    String getTableName() {
        return tableName;
    }

    List<String> getPartitionNames() {
        return partitionNames;
    }

    List<HmsPartitionIdentity.ParsedPartitionName> getPartitions() {
        return partitions;
    }

    ConnectorMetadataAccessSource getSource() {
        return source;
    }

    ConnectorMetadataAccessObserver getMetadataAccessObserver() {
        return metadataAccessObserver;
    }

    PartitionChunkConsumer getPartitionChunkConsumer() {
        return partitionChunkConsumer;
    }

    boolean isLogicalAccessOwner() {
        return logicalAccessOwner;
    }

    void addPhysicalAccess(ConnectorMetadataAccessEvent event) {
        batchExecutionState.addPhysicalAccess(event);
    }

    ConnectorMetadataAccessEvent logicalAccessEvent(long elapsedMillis, boolean success) {
        return batchExecutionState.logicalAccessEvent(this, elapsedMillis, success);
    }

    int effectiveBatchSize(int configuredMaxBatchSize) {
        return Math.min(configuredMaxBatchSize, batchExecutionState.effectiveBatchSize.get());
    }

    void reduceEffectiveBatchSize(int batchSize) {
        batchExecutionState.effectiveBatchSize.accumulateAndGet(batchSize, Math::min);
    }

    long fallbackStartNanos() {
        return batchExecutionState.fallbackStartNanos.get();
    }

    long startFallback(long startNanos) {
        batchExecutionState.fallbackStartNanos.compareAndSet(NO_FALLBACK_START_NANOS, startNanos);
        return batchExecutionState.fallbackStartNanos.get();
    }

    /** Receives one fully validated and request-ordered physical chunk before the next HMS chunk starts. */
    @FunctionalInterface
    interface PartitionChunkConsumer {
        PartitionChunkConsumer NOOP = (partitionNames, partitions) -> { };

        void accept(List<String> partitionNames, List<HmsPartitionInfo> partitions);
    }

    static final class Builder {
        private String dbName;
        private String tableName;
        private List<String> partitionNames;
        private List<HmsPartitionIdentity.ParsedPartitionName> partitions;
        private ConnectorMetadataAccessSource source = ConnectorMetadataAccessSource.UNKNOWN;
        private ConnectorMetadataAccessObserver metadataAccessObserver = ConnectorMetadataAccessObserver.NOOP;
        private PartitionChunkConsumer partitionChunkConsumer = PartitionChunkConsumer.NOOP;
        private BatchExecutionState batchExecutionState = new BatchExecutionState();
        private boolean logicalAccessOwner = true;
        private Function<String, HmsPartitionIdentity.ParsedPartitionName> partitionParser
                = HmsPartitionIdentity::parse;

        private Builder() {
        }

        Builder database(String dbName) {
            this.dbName = dbName;
            return this;
        }

        Builder table(String tableName) {
            this.tableName = tableName;
            return this;
        }

        Builder partitionNames(List<String> partitionNames) {
            this.partitionNames = partitionNames;
            return this;
        }

        Builder partitions(List<HmsPartitionIdentity.ParsedPartitionName> partitions) {
            this.partitions = partitions;
            return this;
        }

        Builder partitionParserForTest(
                Function<String, HmsPartitionIdentity.ParsedPartitionName> partitionParser) {
            this.partitionParser = partitionParser;
            return this;
        }

        Builder source(ConnectorMetadataAccessSource source) {
            this.source = source;
            return this;
        }

        Builder metadataAccessObserver(ConnectorMetadataAccessObserver metadataAccessObserver) {
            this.metadataAccessObserver = metadataAccessObserver;
            return this;
        }

        Builder partitionChunkConsumer(PartitionChunkConsumer partitionChunkConsumer) {
            this.partitionChunkConsumer = partitionChunkConsumer;
            return this;
        }

        Builder shareBatchExecutionWith(HmsPartitionRequest request) {
            this.batchExecutionState = request.batchExecutionState;
            this.logicalAccessOwner = false;
            return this;
        }

        HmsPartitionRequest build() {
            requireName(dbName, "database");
            requireName(tableName, "table");
            if (partitions == null) {
                Objects.requireNonNull(partitionNames, "partitionNames");
            }
            Objects.requireNonNull(source, "source");
            Objects.requireNonNull(metadataAccessObserver, "metadataAccessObserver");
            Objects.requireNonNull(partitionChunkConsumer, "partitionChunkConsumer");
            Objects.requireNonNull(batchExecutionState, "batchExecutionState");
            return new HmsPartitionRequest(this);
        }

        private static void requireName(String value, String field) {
            if (value == null || value.isEmpty()) {
                throw new IllegalArgumentException(field + " must not be empty");
            }
        }
    }

    private static List<HmsPartitionIdentity.ParsedPartitionName> parsePartitions(List<String> names,
            Function<String, HmsPartitionIdentity.ParsedPartitionName> parser) {
        List<HmsPartitionIdentity.ParsedPartitionName> parsedPartitions = new ArrayList<>(names.size());
        Set<List<String>> identities = new HashSet<>();
        List<String> partitionKeys = null;
        for (int i = 0; i < names.size(); i++) {
            HmsPartitionIdentity.ParsedPartitionName parsed = parser.apply(names.get(i));
            if (partitionKeys == null) {
                partitionKeys = parsed.getKeys();
            } else if (!partitionKeys.equals(parsed.getKeys())) {
                throw new IllegalArgumentException("inconsistent partition keys in request: " + parsed.getName());
            }
            if (!identities.add(parsed.getValues())) {
                throw new IllegalArgumentException("duplicate partition identity in request: " + parsed.getName());
            }
            parsedPartitions.add(parsed);
        }
        return Collections.unmodifiableList(parsedPartitions);
    }

    /** Carries adaptive batch state across bounded cache-miss windows of one business request. */
    private static final class BatchExecutionState {
        private final AtomicInteger effectiveBatchSize = new AtomicInteger(Integer.MAX_VALUE);
        private final AtomicLong fallbackStartNanos = new AtomicLong(NO_FALLBACK_START_NANOS);
        private int rpcCount;
        private long rpcItems;
        private int largestBatchSize;
        private int smallestBatchSize = Integer.MAX_VALUE;
        private int fallbackCount;
        private long rpcElapsedMillis;
        private long maxRpcElapsedMillis;

        private synchronized void addPhysicalAccess(ConnectorMetadataAccessEvent event) {
            rpcCount += event.getRpcCount();
            rpcItems += event.getRpcItems();
            largestBatchSize = Math.max(largestBatchSize, event.getLargestBatchSize());
            if (event.getSmallestBatchSize() > 0) {
                smallestBatchSize = Math.min(smallestBatchSize, event.getSmallestBatchSize());
            }
            fallbackCount += event.getFallbackCount();
            rpcElapsedMillis += event.getRpcElapsedMillis();
            maxRpcElapsedMillis = Math.max(maxRpcElapsedMillis, event.getMaxRpcElapsedMillis());
        }

        private synchronized ConnectorMetadataAccessEvent logicalAccessEvent(
                HmsPartitionRequest request, long elapsedMillis, boolean success) {
            return ConnectorMetadataAccessEvent.builder()
                    .operation("hms.get_partitions_by_names")
                    .source(request.source.name())
                    .requestedItems(request.partitionNames.size())
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
}
