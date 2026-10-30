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

package org.apache.doris.connector.spi.scan;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Cancellation and progress of one scan, shared by its partition batches. A connector checks cancellation
 * before advancing a potentially blocking iterator or issuing another request. Cancellation does not close
 * a source concurrently with its reader, and must never be converted into a successful, partial listing.
 * The engine retains the query's original termination reason; the exception here only unwinds enumeration.
 */
public final class ConnectorScanContext {
    private final CompletableFuture<Void> cancellation = new CompletableFuture<>();
    private final AtomicBoolean cancelled = new AtomicBoolean();
    private final AtomicLong partitionsProcessed = new AtomicLong();
    private final AtomicLong filesEnumerated = new AtomicLong();
    private final AtomicLong splitsGenerated = new AtomicLong();
    private volatile long totalPartitions = -1;

    public void cancel() {
        cancelled.set(true);
        cancellation.complete(null);
    }

    /**
     * Registers a once-only notification, including when cancellation has already occurred. The callback
     * runs on the cancelling or registering thread and must only signal/schedule cleanup: never block, close
     * a source, or join a worker here. Its failure is isolated in the returned stage.
     */
    public CompletionStage<Void> whenCancelled(Runnable action) {
        return cancellation.thenRun(action);
    }

    public boolean isCancelled() {
        return cancelled.get();
    }

    public void checkCancelled() {
        if (isCancelled()) {
            throw new CancellationException("Split generation stopped");
        }
    }

    public void setTotalPartitions(long totalPartitions) {
        this.totalPartitions = totalPartitions;
    }

    public void partitionProcessed() {
        partitionsProcessed.incrementAndGet();
    }

    public void filesEnumerated(long count) {
        filesEnumerated.addAndGet(count);
    }

    public void splitsGenerated(long count) {
        splitsGenerated.addAndGet(count);
    }

    public String describeProgress() {
        return "partitions_processed=" + partitionsProcessed.get() + "/"
                + (totalPartitions < 0 ? "unknown" : totalPartitions)
                + ", files_enumerated=" + filesEnumerated.get()
                + ", splits_generated=" + splitsGenerated.get();
    }
}
