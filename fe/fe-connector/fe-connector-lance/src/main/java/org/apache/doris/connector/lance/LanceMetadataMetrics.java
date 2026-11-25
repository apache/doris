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

package org.apache.doris.connector.lance;

import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorStatementScopes;
import org.apache.doris.connector.spi.scan.ConnectorScanProfile;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * The FE time one statement spends on one Lance table's metadata, published with each scan of the table as the
 * {@value #GROUP_NAME} group of the query profile.
 *
 * <p>Two operations are timed: a metadata read (describe the table, open the dataset at a version, read what the
 * statement asks of it) and the split planning of a scan. A read's time is also broken into disjoint stages, so
 * that a slow namespace, a slow store and a large manifest can be told apart. Failed attempts count in the time
 * and in the calls. No query identifier, dataset URI, credential or Session-wide cache statistic is recorded.
 *
 * <p>The totals are kept per table in the statement scope, because the reads happen in the statement's metadata
 * calls and the planning in its scans: {@link #publishScan} hands the table's totals so far to the scan being
 * planned and starts them over, and the engine collects that scan's profile right after planning it
 * ({@link #collectScanProfiles}). Outside a live statement every lookup returns fresh totals that nothing
 * publishes, so a read there is timed for nothing and otherwise behaves the same.
 */
final class LanceMetadataMetrics {
    /** The query-profile group; user-visible. */
    static final String GROUP_NAME = "Lance Metadata Metrics";
    /** Statement-scope namespace of the per-table totals; guarded by {@code LanceStatementScopeTest}. */
    static final String TABLE_METRICS_NAMESPACE = "lance.metadata_metrics";
    /**
     * Statement-scope key prefix of the profiles planned scans leave for the engine to collect, followed by the
     * catalog and query ids, as other connectors key their per-statement profiles; guarded by
     * {@code LanceStatementScopeTest}.
     */
    static final String SCAN_PROFILES_KEY_PREFIX = "lance.scan_profiles";

    /** Disjoint parts of a metadata read. */
    enum Stage {
        /** Describing the table: namespace request, credential vending and storage options. */
        TABLE_ACCESS("TableAccessResolve"),
        /** Every Dataset.open, the newest-version open that FOR TIME AS OF needs included. */
        DATASET_OPEN("DatasetOpen"),
        /** Listing the versions and choosing the one at or before a time. */
        VERSION_RESOLVE("VersionResolve"),
        /** Reading the Arrow schema. */
        SCHEMA("SchemaRead"),
        /** Reading the fragments and their row counts. */
        FRAGMENTS("FragmentMetadataRead"),
        /** Reading the Lance field ids, the known schema-conversion fallback included. */
        FIELD_IDS("FieldIdsRead"),
        /** Discovering the user indexes and their segments. */
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

    private final long[] operationNanos = new long[Operation.values().length];
    private final long[] operationCalls = new long[Operation.values().length];
    private final long[] operationFailures = new long[Operation.values().length];
    private final long[] stageNanos = new long[Stage.values().length];
    private final long[] stageCalls = new long[Stage.values().length];

    /**
     * The totals of {@code dbName.tableName} in the session's statement. A statement-scope lookup itself, so never
     * call it from a loader of that scope: the engine's scope throws "Recursive update" on such a re-entry.
     */
    static LanceMetadataMetrics forTable(ConnectorSession session, String dbName, String tableName) {
        return ConnectorStatementScopes.resolveInStatement(session, TABLE_METRICS_NAMESPACE, dbName, tableName,
                LanceMetadataMetrics::new);
    }

    /**
     * Queues the table's totals so far as the profile of the scan of {@code dbName.tableName} just planned, for
     * {@link #collectScanProfiles}, and starts the totals over: a later scan of the same table in the statement
     * reports only what came after.
     */
    static void publishScan(ConnectorSession session, String dbName, String tableName,
            LanceMetadataMetrics metrics) {
        pendingProfiles(session).add(new ConnectorScanProfile(GROUP_NAME,
                "Table Scan (" + dbName + "." + tableName + ")", metrics.drain()));
    }

    /** The profiles queued in the session's statement since the last collection. */
    static List<ConnectorScanProfile> collectScanProfiles(ConnectorSession session) {
        Queue<ConnectorScanProfile> pending = pendingProfiles(session);
        List<ConnectorScanProfile> profiles = new ArrayList<>();
        for (ConnectorScanProfile profile = pending.poll(); profile != null; profile = pending.poll()) {
            profiles.add(profile);
        }
        return profiles;
    }

    private static Queue<ConnectorScanProfile> pendingProfiles(ConnectorSession session) {
        String key = SCAN_PROFILES_KEY_PREFIX + ":" + session.getCatalogId() + ":" + session.getQueryId();
        return session.getStatementScope().computeIfAbsent(key, ConcurrentLinkedQueue::new);
    }

    /** Starts timing one operation; the returned scope adds itself to these totals when closed. */
    Timer start(Operation operation) {
        return new Timer(operation);
    }

    /**
     * The totals as profile values, then cleared: each operation that ran with its time, calls and failures, then
     * each stage that ran with its time and calls. Times are summed in nanoseconds before rendering, so many short
     * reads are not rounded away one by one.
     */
    synchronized Map<String, String> drain() {
        Map<String, String> values = new LinkedHashMap<>();
        for (Operation operation : Operation.values()) {
            int index = operation.ordinal();
            if (operationCalls[index] > 0) {
                values.put(operation.prefix + "Time", prettyNanos(operationNanos[index]));
                values.put(operation.prefix + "Calls", String.valueOf(operationCalls[index]));
                values.put(operation.prefix + "Failures", String.valueOf(operationFailures[index]));
            }
        }
        for (Stage stage : Stage.values()) {
            int index = stage.ordinal();
            if (stageCalls[index] > 0) {
                values.put(stage.prefix + "Time", prettyNanos(stageNanos[index]));
                values.put(stage.prefix + "Calls", String.valueOf(stageCalls[index]));
            }
        }
        clear(operationNanos, operationCalls, operationFailures, stageNanos, stageCalls);
        return values;
    }

    private synchronized void add(Timer timer, long elapsedNanos) {
        int index = timer.operation.ordinal();
        operationNanos[index] += elapsedNanos;
        operationCalls[index]++;
        if (!timer.successful) {
            operationFailures[index]++;
        }
        for (int stage = 0; stage < stageCalls.length; stage++) {
            stageNanos[stage] += timer.stageNanos[stage];
            stageCalls[stage] += timer.stageCalls[stage];
        }
    }

    private static void clear(long[]... counters) {
        for (long[] counter : counters) {
            Arrays.fill(counter, 0);
        }
    }

    /**
     * A duration as the query profile renders its nanosecond timers: from one second up as fe-core's
     * {@code DebugUtil.printTimeMs} does (hours and minutes drop the smaller units), else milliseconds or
     * microseconds with three decimals, else nanoseconds.
     */
    static String prettyNanos(long nanos) {
        if (nanos >= TimeUnit.SECONDS.toNanos(1)) {
            long remaining = TimeUnit.NANOSECONDS.toMillis(nanos);
            StringBuilder builder = new StringBuilder();
            boolean hour = false;
            boolean minute = false;
            if (remaining >= TimeUnit.HOURS.toMillis(1)) {
                builder.append(remaining / TimeUnit.HOURS.toMillis(1)).append("hour");
                remaining %= TimeUnit.HOURS.toMillis(1);
                hour = true;
            }
            if (remaining >= TimeUnit.MINUTES.toMillis(1)) {
                builder.append(remaining / TimeUnit.MINUTES.toMillis(1)).append("min");
                remaining %= TimeUnit.MINUTES.toMillis(1);
                minute = true;
            }
            if (!hour && remaining >= TimeUnit.SECONDS.toMillis(1)) {
                builder.append(remaining / TimeUnit.SECONDS.toMillis(1)).append("sec");
                remaining %= TimeUnit.SECONDS.toMillis(1);
            }
            if (!hour && !minute) {
                builder.append(remaining).append("ms");
            }
            return builder.toString();
        }
        if (nanos >= TimeUnit.MILLISECONDS.toNanos(1)) {
            long micros = TimeUnit.NANOSECONDS.toMicros(nanos);
            return String.format(Locale.ROOT, "%d.%03dms", micros / 1000, micros % 1000);
        }
        if (nanos >= TimeUnit.MICROSECONDS.toNanos(1)) {
            return String.format(Locale.ROOT, "%d.%03dus", nanos / 1000, nanos % 1000);
        }
        return nanos + "ns";
    }

    /** The timing of one operation; the stages measured in it count toward it, and it counts on close. */
    final class Timer implements AutoCloseable {
        private final Operation operation;
        private final long startedNanos = System.nanoTime();
        private final long[] stageNanos = new long[Stage.values().length];
        private final long[] stageCalls = new long[Stage.values().length];
        private boolean successful;
        private boolean closed;

        private Timer(Operation operation) {
            this.operation = operation;
        }

        /** Runs {@code action} as one call of {@code stage}; a failed call counts too, and its error propagates. */
        <T> T measure(Stage stage, Supplier<T> action) {
            long start = System.nanoTime();
            try {
                return action.get();
            } finally {
                stageNanos[stage.ordinal()] += System.nanoTime() - start;
                stageCalls[stage.ordinal()]++;
            }
        }

        /** Marks the operation successful: call it once everything it opened has been released. */
        void succeeded() {
            successful = true;
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            add(this, System.nanoTime() - startedNanos);
        }
    }
}
