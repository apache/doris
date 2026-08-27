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

import org.apache.doris.connector.spi.ConnectorMetadataAccessObserver;
import org.apache.doris.connector.spi.ConnectorOperationControl;
import org.apache.doris.connector.spi.ConnectorSession;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/** Immutable logical HMS request data with request-scoped adaptive execution state. */
final class HmsPartitionRequest {

    static final long NO_FALLBACK_START_NANOS = Long.MIN_VALUE;

    private final String dbName;
    private final String tableName;
    private final List<String> partitionNames;
    private final HmsPartitionAccessSource source;
    private final ConnectorOperationControl operationControl;
    private final ConnectorMetadataAccessObserver metadataAccessObserver;
    private final PartitionChunkConsumer partitionChunkConsumer;
    private final BatchExecutionState batchExecutionState;

    private HmsPartitionRequest(Builder builder) {
        this.dbName = builder.dbName;
        this.tableName = builder.tableName;
        this.partitionNames = Collections.unmodifiableList(new ArrayList<>(builder.partitionNames));
        this.source = builder.source;
        this.operationControl = builder.operationControl;
        this.metadataAccessObserver = builder.metadataAccessObserver;
        this.partitionChunkConsumer = builder.partitionChunkConsumer;
        this.batchExecutionState = builder.batchExecutionState;
    }

    static HmsPartitionRequest from(ConnectorSession session, HmsPartitionAccessSource source,
            String dbName, String tableName, List<String> partitionNames) {
        return builder()
                .database(dbName)
                .table(tableName)
                .partitionNames(partitionNames)
                .source(source)
                .operationControl(session == null
                        ? ConnectorOperationControl.NONE : session.getOperationControl())
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

    HmsPartitionAccessSource getSource() {
        return source;
    }

    ConnectorOperationControl getOperationControl() {
        return operationControl;
    }

    ConnectorOperationControl getEffectiveOperationControl() {
        ConnectorOperationControl effective = batchExecutionState.effectiveOperationControl.get();
        return effective == null ? operationControl : effective;
    }

    void updateEffectiveOperationControl(ConnectorOperationControl effectiveOperationControl) {
        batchExecutionState.effectiveOperationControl.set(effectiveOperationControl);
    }

    ConnectorMetadataAccessObserver getMetadataAccessObserver() {
        return metadataAccessObserver;
    }

    PartitionChunkConsumer getPartitionChunkConsumer() {
        return partitionChunkConsumer;
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
        PartitionChunkConsumer NOOP = (partitionNames, partitions, operationControl) -> { };

        void accept(List<String> partitionNames, List<HmsPartitionInfo> partitions,
                ConnectorOperationControl operationControl);
    }

    static final class Builder {
        private String dbName;
        private String tableName;
        private List<String> partitionNames;
        private HmsPartitionAccessSource source = HmsPartitionAccessSource.UNKNOWN;
        private ConnectorOperationControl operationControl = ConnectorOperationControl.NONE;
        private ConnectorMetadataAccessObserver metadataAccessObserver = ConnectorMetadataAccessObserver.NOOP;
        private PartitionChunkConsumer partitionChunkConsumer = PartitionChunkConsumer.NOOP;
        private BatchExecutionState batchExecutionState = new BatchExecutionState();

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

        Builder source(HmsPartitionAccessSource source) {
            this.source = source;
            return this;
        }

        Builder operationControl(ConnectorOperationControl operationControl) {
            this.operationControl = operationControl;
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
            return this;
        }

        HmsPartitionRequest build() {
            requireName(dbName, "database");
            requireName(tableName, "table");
            Objects.requireNonNull(partitionNames, "partitionNames");
            Objects.requireNonNull(source, "source");
            Objects.requireNonNull(operationControl, "operationControl");
            Objects.requireNonNull(metadataAccessObserver, "metadataAccessObserver");
            Objects.requireNonNull(partitionChunkConsumer, "partitionChunkConsumer");
            Objects.requireNonNull(batchExecutionState, "batchExecutionState");
            Set<List<String>> identities = new HashSet<>();
            List<String> partitionKeys = null;
            for (int i = 0; i < partitionNames.size(); i++) {
                if ((i & 1023) == 0) {
                    operationControl.checkActive();
                }
                String partitionName = partitionNames.get(i);
                HmsPartitionIdentity.ParsedPartitionName parsed = HmsPartitionIdentity.parse(partitionName);
                List<String> currentKeys = parsed.getKeys();
                if (partitionKeys == null) {
                    partitionKeys = currentKeys;
                } else if (!partitionKeys.equals(currentKeys)) {
                    throw new IllegalArgumentException(
                            "inconsistent partition keys in request: " + partitionName);
                }
                List<String> identity = parsed.getValues();
                if (!identities.add(identity)) {
                    throw new IllegalArgumentException("duplicate partition identity in request: " + partitionName);
                }
            }
            return new HmsPartitionRequest(this);
        }

        private static void requireName(String value, String field) {
            if (value == null || value.isEmpty()) {
                throw new IllegalArgumentException(field + " must not be empty");
            }
        }
    }

    /** Carries adaptive batch state across bounded cache-miss windows of one business request. */
    private static final class BatchExecutionState {
        private final AtomicInteger effectiveBatchSize = new AtomicInteger(Integer.MAX_VALUE);
        private final AtomicLong fallbackStartNanos = new AtomicLong(NO_FALLBACK_START_NANOS);
        private final AtomicReference<ConnectorOperationControl> effectiveOperationControl = new AtomicReference<>();
    }
}
