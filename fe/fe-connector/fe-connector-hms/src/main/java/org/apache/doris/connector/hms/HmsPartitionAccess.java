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

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/** One-way logical access facade over the physical batch executor. */
final class HmsPartitionAccess {
    private static final Logger LOG = LogManager.getLogger(HmsPartitionAccess.class);

    private final HmsPartitionBatchExecutor executor;
    private final ConnectorMetadataAccessObserver catalogObserver;
    private final LongSupplier nanoTime;

    HmsPartitionAccess(HmsPartitionBatchExecutor executor, ConnectorMetadataAccessObserver catalogObserver) {
        this(executor, catalogObserver, System::nanoTime);
    }

    HmsPartitionAccess(HmsPartitionBatchExecutor executor, ConnectorMetadataAccessObserver catalogObserver,
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
            HmsPartitionBatchExecutor.PartitionChunkConsumer chunkConsumer) {
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
        private final HmsPartitionBatchExecutor.Execution execution;
        private final long startNanos;

        private LogicalAccess(HmsPartitionBatchExecutor.Execution execution, long startNanos) {
            this.execution = execution;
            this.startNanos = startNanos;
        }
    }
}
