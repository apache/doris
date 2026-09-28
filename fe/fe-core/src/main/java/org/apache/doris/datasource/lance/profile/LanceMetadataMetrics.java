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

package org.apache.doris.datasource.lance.profile;

import org.apache.doris.common.profile.Counter;
import org.apache.doris.common.profile.RuntimeProfile;
import org.apache.doris.common.profile.SummaryProfile;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TUnit;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Per-operation FE timings, accumulated into the owning SQL profile on close.
 * The profile is captured at entry; publishing does not depend on the closing thread's context.
 * No query identifiers, table URLs, credentials or native Session-wide cache statistics are stored.
 */
public final class LanceMetadataMetrics implements AutoCloseable {
    public static final String GROUP_NAME = "Lance Metadata Metrics";

    /** Disjoint parts of a metadata read; none of these is added to the generic total a second time. */
    public enum Stage {
        /** Namespace describe, credential vending and storage option normalization. */
        TABLE_ACCESS("TableAccessResolve"),
        /** All Dataset.open calls, including the latest open needed for time travel. */
        DATASET_OPEN("DatasetOpen"),
        /** Read version history and select the version at or before a timestamp. */
        VERSION_RESOLVE("VersionResolve"),
        /** Read the Arrow schema. */
        SCHEMA("SchemaRead"),
        /** Read fragment metadata and construct row-count descriptors. */
        FRAGMENTS("FragmentMetadataRead"),
        /** Read and map Lance field IDs; includes the known schema-conversion fallback. */
        FIELD_IDS("FieldIdsRead"),
        /** Discover logical indexes, resolve legacy details and construct segment metadata. */
        INDEXES("IndexMetadataRead");

        private final String prefix;

        Stage(String prefix) {
            this.prefix = prefix;
        }
    }

    enum Operation {
        METADATA_READ("MetadataRead"),
        SPLIT_PLANNING("SplitPlanning");

        private final String prefix;

        Operation(String prefix) {
            this.prefix = prefix;
        }
    }

    private static final LanceMetadataMetrics DISABLED = new LanceMetadataMetrics(null, Operation.METADATA_READ);
    private final SummaryProfile summary;
    private final Operation operation;
    private final long startedNanos;
    private final long[] stageNanos;
    private final long[] stageCalls;
    private boolean successful;
    private boolean closed;

    // Each scope belongs to one operation. Concurrent operations share only the final SummaryProfile update.
    LanceMetadataMetrics(SummaryProfile summary, Operation operation) {
        this.summary = summary;
        this.operation = operation;
        this.startedNanos = summary == null ? 0 : System.nanoTime();
        this.stageNanos = summary == null ? null : new long[Stage.values().length];
        this.stageCalls = summary == null ? null : new long[Stage.values().length];
    }

    public static LanceMetadataMetrics startMetadataRead() {
        return start(Operation.METADATA_READ);
    }

    public static LanceMetadataMetrics startSplitPlanning() {
        return start(Operation.SPLIT_PLANNING);
    }

    private static LanceMetadataMetrics start(Operation operation) {
        SummaryProfile summary = SummaryProfile.getSummaryProfile(ConnectContext.get());
        return summary == null ? DISABLED : new LanceMetadataMetrics(summary, operation);
    }

    /** Used by standalone TVFs and tests that read a Dataset outside a catalog query. */
    public static LanceMetadataMetrics disabled() {
        return DISABLED;
    }

    /** Includes failed attempts in both elapsed time and call count. */
    public <T> T measure(Stage stage, Supplier<T> action) {
        if (summary == null) {
            return action.get();
        }
        long start = System.nanoTime();
        try {
            return action.get();
        } finally {
            stageNanos[stage.ordinal()] += System.nanoTime() - start;
            stageCalls[stage.ordinal()]++;
        }
    }

    /** Call only after all Dataset/allocator resources have closed successfully. */
    public void succeeded() {
        if (summary != null) {
            successful = true;
        }
    }

    @Override
    public void close() {
        if (summary == null || closed) {
            return;
        }
        closed = true;
        long elapsedNanos = System.nanoTime() - startedNanos;
        // Counter increments and child creation need one atomic update. SDK calls are outside this lock.
        synchronized (summary) {
            RuntimeProfile executionSummary = summary.getExecutionSummary();
            RuntimeProfile group = executionSummary.getChildMap().get(GROUP_NAME);
            if (group == null) {
                group = new RuntimeProfile(GROUP_NAME);
                executionSummary.addChild(group, true);
            }
            Counter total = group.addCounter(operation.prefix + "Time", TUnit.TIME_NS, RuntimeProfile.ROOT_COUNTER);
            long oldNanos = total.getValue();
            total.setValue(oldNanos + elapsedNanos);
            increment(group, operation.prefix + "Calls", TUnit.UNIT, 1);
            increment(group, operation.prefix + "Failures", TUnit.UNIT, successful ? 0 : 1);
            for (Stage stage : Stage.values()) {
                if (stageCalls[stage.ordinal()] > 0) {
                    increment(group, stage.prefix + "Time", TUnit.TIME_NS, stageNanos[stage.ordinal()]);
                    increment(group, stage.prefix + "Calls", TUnit.UNIT, stageCalls[stage.ordinal()]);
                }
            }
            // Convert after accumulation, so many sub-millisecond reads are not rounded away individually.
            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(total.getValue()) - TimeUnit.NANOSECONDS.toMillis(oldNanos);
            if (operation == Operation.METADATA_READ) {
                summary.addExternalTableGetTableMetaTime(elapsedMs);
            } else {
                summary.addExternalTableGetFileScanTasksTime(elapsedMs);
            }
        }
    }

    private static void increment(RuntimeProfile group, String name, TUnit unit, long value) {
        Counter counter = group.addCounter(name, unit, RuntimeProfile.ROOT_COUNTER);
        counter.setValue(counter.getValue() + value);
    }
}
