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

package org.apache.doris.connector.mongodb;

import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorStatementScope;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.scan.ConnectorScanContext;
import org.apache.doris.thrift.TMongoSnapshot;

import org.bson.BsonValue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/** Complete planning precedes publication. Statement memos retain either that plan or its failure. */
final class MongoRangePlanner {
    static final int SAMPLE_ROWS = 4096;
    static final int PROBE_TIMEOUT_MS = 2000;
    private static final long MAX_PLAN_DESCRIPTOR_BYTES = 8L * 1024 * 1024;
    private final String memoKey = "mongodb.split.plan:" + UUID.randomUUID();
    private final Supplier<MongoCatalogClient> client;
    private final long probeTimeoutMs;

    MongoRangePlanner(Supplier<MongoCatalogClient> client, long probeTimeoutMs) {
        this.client = client;
        this.probeTimeoutMs = probeTimeoutMs;
    }

    MongoSplitPlan plan(ConnectorSession session, MongoTableHandle table, TMongoSnapshot snapshot,
            String preference, ConnectorScanContext context, long descriptorBytes) {
        checkCancelled(session, context);
        var options = session.getSessionProperties();
        String strategy = options.getOrDefault("mongodb_split_strategy", "single");
        if (!Set.of("single", "auto", "range").contains(strategy)) {
            throw new IllegalArgumentException("Invalid mongodb_split_strategy");
        }
        if ("single".equals(strategy)) {
            return MongoSplitPlan.single(1, 1, "single_strategy");
        }
        String field = options.getOrDefault("mongodb_split_field", "");
        boolean custom = !field.isEmpty();
        if (!custom) {
            field = "_id";
        }
        if (!MongoPredicateTranslator.safePath(field) || field.isEmpty() || field.length() > 256) {
            throw new IllegalArgumentException("MongoDB split field must be an unambiguous top-level field");
        }
        String selectedField = field;
        Budget budget = session.getStatementScope().computeIfAbsent("mongodb.split.budget", Budget::new);
        String tableKey = tableKey(session, table);
        // Count fallback scans too: a view must not make a global custom field look unambiguous.
        if (custom) {
            budget.customField(tableKey);
        }
        Memo memo = session.getStatementScope().computeIfAbsent(tableKey, Memo::new);
        return memo.get(session, context, () -> load(session, table, snapshot, preference, context,
                strategy, selectedField, descriptorBytes, budget));
    }

    Optional<MongoSplitPlan> planned(ConnectorSession session, MongoTableHandle table) {
        Memo memo = session.getStatementScope().getIfPresent(tableKey(session, table));
        return memo == null ? Optional.empty() : memo.completed();
    }

    void freezeDiagnostics(ConnectorSession session, MongoTableHandle table, Map<String, String> properties) {
        session.getStatementScope().computeIfAbsent(tableKey(session, table) + ":diagnostics",
                () -> Map.copyOf(properties));
    }

    Map<String, String> diagnostics(ConnectorSession session, MongoTableHandle table) {
        Map<String, String> properties = session.getStatementScope()
                .getIfPresent(tableKey(session, table) + ":diagnostics");
        return properties == null ? Map.of() : properties;
    }

    private String tableKey(ConnectorSession session, MongoTableHandle table) {
        // One provider can see several namespaces; never reuse another collection's bounds/admission.
        return memoKey + ":" + session.getCatalogId() + ":" + table.getSchema().getVersion();
    }

    private MongoSplitPlan load(ConnectorSession session, MongoTableHandle table, TMongoSnapshot snapshot,
            String preference, ConnectorScanContext context, String strategy, String field,
            long descriptorBytes, Budget budget) {
        int maximum = splitLimit(session);
        validateSplitSizes(session);
        int requested = Math.min(8, maximum);
        // The default single strategy exits before this path; opted-in plans always reserve bytes.
        assert descriptorBytes > 0 : "MongoDB range descriptor bytes must be positive";
        int descriptorMaximum = (int) Math.min(maximum, MAX_PLAN_DESCRIPTOR_BYTES / descriptorBytes);
        if (descriptorMaximum == 0) {
            throw new DorisConnectorException("MongoDB split descriptor exceeds planning byte budget");
        }
        String source = snapshot == null ? "catalog:" + session.getCatalogId()
                : snapshot.getSourceKind() + ":" + snapshot.getSourceId();
        if (!"collection".equals(table.getObjectKind())) {
            return fallback(strategy, requested, "unsupported_object", source, descriptorBytes, budget);
        }
        if (snapshot == null || session.getStatementScope() == ConnectorStatementScope.NONE) {
            return fallback(strategy, requested, "snapshot_required", source, descriptorBytes, budget);
        }
        if (descriptorMaximum == 1) {
            return single(requested, maximum == 1 ? "split_limit" : "descriptor_budget",
                    source, descriptorBytes, budget);
        }
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(
                Math.min(probeTimeoutMs, MongoSnapshot.remainingMillis(session)));
        MongoSplitProbe probe;
        try {
            budget.acquireProbe(session, context, deadline);
            try {
                checkCancelled(session, context);
                probe = client.get().probeSplits(table, field, preference, snapshot, SAMPLE_ROWS,
                        remaining(session, context, deadline), () -> checkCancelled(session, context));
                budget.sampled(probe.sampleBytes());
            } finally {
                budget.probes.release();
            }
        } catch (PlanningLimit limit) {
            checkCancelled(session, context);
            MongoSnapshot.remainingMillis(session);
            // Only optional pre-publication tuning can fall back. Cancellation, authorization,
            // history and source errors must never be converted into a successful full scan.
            return fallback(strategy, requested, "probe_budget", source, descriptorBytes, budget);
        }
        checkCancelled(session, context);
        MongoSnapshot.remainingMillis(session);
        long targetBytes = targetBytes(session, probe.logicalBytes());
        long estimate = probe.logicalBytes() < 0 ? 8
                : Math.max(1, probe.logicalBytes() / targetBytes + (probe.logicalBytes() % targetBytes == 0 ? 0 : 1));
        requested = (int) Math.min(maximum, estimate);
        if (!probe.indexed()) {
            return fallback(strategy, requested, "missing_index", source, descriptorBytes, budget);
        }
        String family = family(probe.samples());
        if (family.isEmpty()) {
            return probe.samples().isEmpty()
                    ? single(requested, "empty_sample", source, descriptorBytes, budget)
                    : fallback(strategy, requested, "unsupported_type", source, descriptorBytes, budget);
        }
        if ("auto".equals(strategy) && probe.logicalBytes() < 0) {
            return single(requested, "unknown_complement_cost", source, descriptorBytes, budget);
        }
        // Division avoids overflow when a valid shared split-size setting is near Long.MAX_VALUE.
        if ("auto".equals(strategy) && probe.logicalBytes() / targetBytes < 2) {
            return single(requested, "small_collection", source, descriptorBytes, budget);
        }
        int admitted = budget.admit(source, Math.min(requested, descriptorMaximum), descriptorBytes);
        try {
            var values = probe.samples().stream().filter(v -> family.equals(family(v))).toList();
            MongoSplitPlan plan = MongoSplitPlan.ranges(field, family, values, requested, admitted);
            budget.refund(source, admitted - plan.size(), descriptorBytes);
            return plan;
        } catch (RuntimeException failure) {
            budget.refund(source, admitted, descriptorBytes);
            throw failure;
        }
    }

    static int splitLimit(ConnectorSession session) {
        long limit = number(session.getSessionProperties(), "max_file_split_num", 100000);
        // Mongo freezes all boundaries even when transport is streaming; that retained plan must
        // honor the shared count cap too. Nonpositive means no user cap, as on other file scans.
        return limit <= 0 ? 256 : (int) Math.min(256, limit);
    }

    private static void validateSplitSizes(ConnectorSession session) {
        targetBytes(session, -1);
    }

    static long targetBytes(ConnectorSession session, long logicalBytes) {
        var options = session.getSessionProperties();
        long specified = number(options, "file_split_size", 0);
        if (specified > 0) {
            return specified;
        }
        long regular = number(options, "max_file_split_size", 64L * 1024 * 1024);
        long initial = number(options, "max_initial_file_split_size", 32L * 1024 * 1024);
        long initialCount = number(options, "max_initial_file_split_num", 200);
        if (regular <= 0 || initial <= 0) {
            throw new IllegalArgumentException("MongoDB split sizes must be positive");
        }
        // Match the shared scan-size heuristic without overflowing regular * initialCount.
        return logicalBytes < 0 || initialCount <= 0 || logicalBytes / regular >= initialCount
                ? regular : initial;
    }

    static void checkCancelled(ConnectorSession session, ConnectorScanContext context) {
        context.checkCancelled();
        // Synchronous final planning may have no registered scan context. Observe the same query
        // task-group lifetime as asynchronous split producers before every remote cursor advance.
        MongoSnapshot.checkQueryCancelled(session);
    }

    static String family(BsonValue value) {
        return value.isInt32() || value.isInt64() ? "integer"
                : value.isObjectId() ? "objectId" : value.isDateTime() ? "date" : "";
    }

    static String family(List<BsonValue> samples) {
        Map<String, Integer> counts = new HashMap<>();
        for (BsonValue value : samples) {
            String family = family(value);
            if (!family.isEmpty()) {
                counts.merge(family, 1, Integer::sum);
            }
        }
        // Deterministic ties affect balance only; the complete partitions still cover every source type.
        return counts.keySet().stream().sorted().max(java.util.Comparator.comparingInt(counts::get)).orElse("");
    }

    private static MongoSplitPlan fallback(String strategy, int requested, String reason,
            String source, long descriptorBytes, Budget budget) {
        if ("range".equals(strategy)) {
            throw new IllegalArgumentException("MongoDB range planning unavailable: " + reason);
        }
        return single(requested, reason, source, descriptorBytes, budget);
    }

    private static MongoSplitPlan single(int requested, String reason, String source,
            long descriptorBytes, Budget budget) {
        // A successful fallback still publishes a descriptor and a source scan. For example,
        // 257 empty collection scans must not bypass the same 256-partition pinned-source cap.
        budget.admit(source, 1, descriptorBytes);
        return MongoSplitPlan.single(requested, 1, reason);
    }

    static long remaining(ConnectorSession session, ConnectorScanContext context, long deadline) {
        checkCancelled(session, context);
        long query = MongoSnapshot.remainingMillis(session);
        long left = deadline - System.nanoTime();
        if (left <= 0) {
            throw new PlanningLimit();
        }
        return Math.min(query, Math.max(1, TimeUnit.NANOSECONDS.toMillis(left)));
    }

    private static long number(Map<String, String> options, String name, long fallback) {
        try {
            return Long.parseLong(options.getOrDefault(name, Long.toString(fallback)));
        } catch (NumberFormatException malformed) {
            throw new IllegalArgumentException("Invalid split setting: " + name);
        }
    }

    static final class PlanningLimit extends RuntimeException {
        PlanningLimit() {
            super("MongoDB split probe budget exceeded");
        }
    }

    static final class Budget {
        private final Semaphore probes = new Semaphore(2);
        private final Map<String, Integer> sourceSplits = new HashMap<>();
        private int splits;
        private long bytes;
        private long descriptorBytes;
        private final Map<String, Long> sourceDescriptorBytes = new HashMap<>();
        private String fieldOwner;

        synchronized void customField(String owner) {
            if (fieldOwner != null && !fieldOwner.equals(owner)) {
                throw new IllegalArgumentException("mongodb_split_field requires exactly one MongoDB scan");
            }
            fieldOwner = owner;
        }

        synchronized int admit(String source, int requested, long perRangeBytes) {
            long remainingBytes = Math.min(64L * 1024 * 1024 - descriptorBytes,
                    32L * 1024 * 1024 - sourceDescriptorBytes.getOrDefault(source, 0L));
            int remaining = (int) Math.min(Math.min(4096 - splits, 256 - sourceSplits.getOrDefault(source, 0)),
                    remainingBytes / perRangeBytes);
            if (remaining < 1) {
                throw new DorisConnectorException("MongoDB statement/source split budget exhausted");
            }
            int count = Math.min(requested, remaining);
            splits += count;
            descriptorBytes += count * perRangeBytes;
            sourceSplits.merge(source, count, Integer::sum);
            sourceDescriptorBytes.merge(source, count * perRangeBytes, Long::sum);
            return count;
        }

        synchronized void refund(String source, int count, long perRangeBytes) {
            splits -= count;
            descriptorBytes -= count * perRangeBytes;
            sourceSplits.merge(source, -count, Integer::sum);
            sourceDescriptorBytes.merge(source, -count * perRangeBytes, Long::sum);
        }

        synchronized void sampled(long count) {
            if (count < 0 || count > 1024 * 1024 || bytes + count > 16L * 1024 * 1024) {
                throw new PlanningLimit();
            }
            bytes += count;
        }

        void acquireProbe(ConnectorSession session, ConnectorScanContext context, long deadline) {
            try {
                while (!probes.tryAcquire(Math.min(50, remaining(session, context, deadline)), TimeUnit.MILLISECONDS)) {
                    checkCancelled(session, context);
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new CancellationException("MongoDB split planning interrupted");
            }
        }
    }

    private static final class Memo {
        private CompletableFuture<MongoSplitPlan> value;

        synchronized Optional<MongoSplitPlan> completed() {
            return value != null && value.isDone() && !value.isCompletedExceptionally()
                    ? Optional.of(value.getNow(null)) : Optional.empty();
        }

        MongoSplitPlan get(ConnectorSession session, ConnectorScanContext context, Supplier<MongoSplitPlan> loader) {
            CompletableFuture<MongoSplitPlan> selected;
            boolean owner;
            synchronized (this) {
                owner = value == null;
                if (owner) {
                    value = new CompletableFuture<>();
                }
                selected = value;
            }
            if (owner) {
                try {
                    selected.complete(loader.get());
                } catch (Throwable failure) {
                    // A fatal loader failure must wake every waiter instead of leaving the memo pending.
                    selected.completeExceptionally(failure);
                    if (failure instanceof Error fatal) {
                        throw fatal;
                    }
                }
            }
            while (true) {
                checkCancelled(session, context);
                try {
                    // An offline auto fallback has no statement pin/deadline. Its private memo
                    // was completed by this caller; only live statement waiters need timed polling.
                    return session.getStatementScope() == ConnectorStatementScope.NONE ? selected.get()
                            : selected.get(Math.min(50, MongoSnapshot.remainingMillis(session)), TimeUnit.MILLISECONDS);
                } catch (TimeoutException waiting) {
                    // Poll cancellation while another planner owns the bounded probe.
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new CancellationException("MongoDB split planning interrupted");
                } catch (ExecutionException failure) {
                    if (failure.getCause() instanceof Error fatal) {
                        throw fatal;
                    }
                    if (failure.getCause() instanceof RuntimeException runtime) {
                        throw runtime;
                    }
                    throw new DorisConnectorException("MongoDB split planning failed");
                }
            }
        }
    }
}
