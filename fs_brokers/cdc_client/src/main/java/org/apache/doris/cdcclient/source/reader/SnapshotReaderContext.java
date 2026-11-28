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

package org.apache.doris.cdcclient.source.reader;

import org.apache.doris.cdcclient.common.ManagedReader;
import org.apache.doris.cdcclient.common.ReaderBatch;

import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import lombok.Getter;
import lombok.Setter;

/** Keeps one split's reader, state and outstanding result together. */
@Getter
public class SnapshotReaderContext<S extends SourceSplit, R, ST> implements AutoCloseable {
    private final S split;
    private final ST splitState;
    private ManagedReader<R> managedReader;
    private CompletableFuture<Void> submitFuture;
    @Setter private CompletableFuture<ReaderBatch<SourceRecord>> pollFuture;
    @Setter private ReaderBatch<SourceRecord> batch;
    private volatile boolean closed;

    public SnapshotReaderContext(S split, ST splitState) {
        this.split = split;
        this.splitState = splitState;
    }

    public void submit(ManagedReader<R> reader, Runnable submitSplit, Executor executor) {
        reader.setSnapshotSplitId(split.splitId());
        managedReader = reader;
        submitFuture =
                CompletableFuture.runAsync(
                        () -> {
                            if (!closed) {
                                submitSplit.run();
                            }
                        },
                        executor);
    }

    @Override
    public void close() {
        closed = true;
        if (pollFuture != null) {
            // Claimed batches belong to the consumer; discard only unclaimed results.
            pollFuture.thenAccept(ReaderBatch::close);
        }
        if (submitFuture != null && !submitFuture.isDone()) {
            // An in-flight submit may still open a connection; close it after submit returns.
            submitFuture.whenComplete((ignored, failure) -> managedReader.close());
        } else if (managedReader != null) {
            managedReader.close();
        }
    }
}
