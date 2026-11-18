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
    private final String memoKey = "mongodb.split.plan:" + UUID.randomUUID();
    private final Supplier<MongoCatalogClient> client;

    MongoRangePlanner(Supplier<MongoCatalogClient> client) {
        this.client = client;
    }

    MongoSplitPlan plan(ConnectorSession session, MongoTableHandle table, TMongoSnapshot snapshot,
            String preference, ConnectorScanContext context, long descriptorBytes) {
        context.checkCancelled();
        var options = session.getSessionProperties();
        String strategy = options.getOrDefault("mongodb_split_strategy", "single");
        if (!Set.of("single", "auto", "range").contains(strategy)) {
            throw new IllegalArgumentException("Invalid mongodb_split_strategy");
        }
        int requested = number(options, "mongodb_split_count", 0, 0, 256);
        int maximum = number(options, "mongodb_split_max_count", 256, 1, 256);
        int rows = number(options, "mongodb_split_sample_rows", 4096, 1, 4096);
        int timeout = number(options, "mongodb_split_plan_timeout_ms", 2000, 1, 300000);
        int targetBytes = number(options, "mongodb_split_target_bytes", 268435456, 1, Integer.MAX_VALUE);
        if (requested > maximum) {
            throw new IllegalArgumentException("mongodb_split_count exceeds mongodb_split_max_count");
        }
        if ("single".equals(strategy) || requested == 1) {
            return MongoSplitPlan.single(Math.max(1, requested), 1,
                    requested == 1 ? "requested_one" : "single_strategy");
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
        // A provider may be reused: records and another_records must not share boundaries or admission.
        String tableKey = memoKey + ":" + session.getCatalogId() + ":" + table.getSchema().getVersion();
        // Count fallback scans too: an auto view scan must not make a global custom field appear
        // unambiguous when another MongoDB relation also participates in the statement.
        if (custom) {
            budget.customField(tableKey);
        }
        if (!"collection".equals(table.getObjectKind())) {
            return fallback(strategy, requested, "unsupported_object");
        }
        if (snapshot == null || session.getStatementScope() == ConnectorStatementScope.NONE) {
            return fallback(strategy, requested, "snapshot_required");
        }
        Memo memo = session.getStatementScope().computeIfAbsent(tableKey, Memo::new);
        return memo.get(session, context, () -> load(session, table, snapshot, preference, context, strategy,
                selectedField, requested, maximum, rows, timeout, targetBytes, descriptorBytes, budget));
    }

    private MongoSplitPlan load(ConnectorSession session, MongoTableHandle table, TMongoSnapshot snapshot,
            String preference, ConnectorScanContext context, String strategy, String field, int requested,
            int maximum, int rows, int timeout, int targetBytes, long descriptorBytes, Budget budget) {
        int descriptorMaximum = (int) Math.min(maximum, (8L * 1024 * 1024) / descriptorBytes);
        if (descriptorMaximum == 0) {
            throw new DorisConnectorException("MongoDB split descriptor exceeds planning byte budget");
        }
        String source = snapshot.getSourceKind() + ":" + snapshot.getSourceId();
        if (descriptorMaximum == 1) {
            budget.admit(source, 1, descriptorBytes);
            return MongoSplitPlan.single(Math.max(1, requested), 1, "descriptor_budget")
                    .withDescriptorBudget(descriptorBytes);
        }
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(
                Math.min(timeout, MongoSnapshot.remainingMillis(session)));
        MongoSplitProbe probe;
        try {
            // Waiting behind two active probes consumes the same optional budget as source I/O.
            // Keep acquisition inside the fallback boundary, but never release a slot we did not acquire.
            budget.acquireProbe(session, context, deadline);
            try {
                context.checkCancelled();
                probe = client.get().probeSplits(table, field, preference, snapshot, rows,
                        remaining(session, context, deadline), context);
                budget.sampled(probe.sampleBytes());
            } finally {
                budget.probes.release();
            }
        } catch (PlanningLimit limit) {
            context.checkCancelled();
            MongoSnapshot.remainingMillis(session);
            // Only this optional, pre-publication tuning budget may trigger auto fallback.
            // Source authorization/identity/history errors and query cancellation remain failures.
            return fallback(strategy, requested, "probe_budget");
        }
        context.checkCancelled();
        MongoSnapshot.remainingMillis(session);
        if (!probe.indexed()) {
            return fallback(strategy, requested, "missing_index");
        }
        String family = family(probe.samples());
        if (family.isEmpty()) {
            return probe.samples().isEmpty() ? MongoSplitPlan.single(Math.max(1, requested), 1, "empty_sample")
                    : fallback(strategy, requested, "unsupported_type");
        }
        if ("auto".equals(strategy) && probe.logicalBytes() < 0) {
            // Every ordinary collection needs a complement; without size metadata its extra scan
            // cost is unknown. A homogeneous sample must never remove that partition.
            return MongoSplitPlan.single(Math.max(1, requested), 1, "unknown_complement_cost");
        }
        long estimate = probe.logicalBytes() < 0 ? 8
                : Math.max(1, probe.logicalBytes() / targetBytes + (probe.logicalBytes() % targetBytes == 0 ? 0 : 1));
        int desired = requested == 0 ? (int) Math.min(maximum, estimate) : requested;
        int target = Math.min(desired, descriptorMaximum);
        if ("auto".equals(strategy) && probe.logicalBytes() < 2L * targetBytes) {
            return MongoSplitPlan.single(Math.max(1, requested), 1, "small_collection");
        }
        int admitted = budget.admit(source, target, descriptorBytes);
        try {
            var values = probe.samples().stream().filter(v -> family.equals(family(List.of(v)))).toList();
            MongoSplitPlan plan = MongoSplitPlan.ranges(field, family, values, Math.max(1, desired), admitted)
                    .withDescriptorBudget(descriptorBytes);
            budget.refund(source, admitted - plan.size(), descriptorBytes);
            return plan;
        } catch (RuntimeException failure) {
            budget.refund(source, admitted, descriptorBytes);
            throw failure;
        }
    }

    static String family(List<BsonValue> samples) {
        Map<String, Integer> counts = new HashMap<>();
        for (BsonValue value : samples) {
            String family = value.isInt32() || value.isInt64() ? "integer"
                    : value.isObjectId() ? "objectId" : value.isDateTime() ? "date" : "";
            if (!family.isEmpty()) {
                counts.merge(family, 1, Integer::sum);
            }
        }
        // Deterministic tie breaking affects balance only; every other family remains in NOT E.
        return counts.keySet().stream().sorted().max(java.util.Comparator.comparingInt(counts::get)).orElse("");
    }

    private static MongoSplitPlan fallback(String strategy, int requested, String reason) {
        if ("range".equals(strategy)) {
            throw new IllegalArgumentException("MongoDB range planning unavailable: " + reason);
        }
        return MongoSplitPlan.single(Math.max(1, requested), 1, reason);
    }

    static long remaining(ConnectorSession session, ConnectorScanContext context, long deadline) {
        context.checkCancelled();
        long query = MongoSnapshot.remainingMillis(session);
        long left = deadline - System.nanoTime();
        if (left <= 0) {
            throw new PlanningLimit();
        }
        return Math.min(query, Math.max(1, TimeUnit.NANOSECONDS.toMillis(left)));
    }

    static int number(Map<String, String> options, String name, int fallback, int minimum, int maximum) {
        try {
            int value = Integer.parseInt(options.getOrDefault(name, Integer.toString(fallback)));
            if (value >= minimum && value <= maximum) {
                return value;
            }
        } catch (NumberFormatException ignored) {
            // The connector validates direct SPI calls as well as SQL session-variable assignments.
        }
        throw new IllegalArgumentException("Invalid MongoDB split policy: " + name);
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
                    context.checkCancelled();
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new CancellationException("MongoDB split planning interrupted");
            }
        }
    }

    private static final class Memo {
        private CompletableFuture<MongoSplitPlan> value;

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
                } catch (RuntimeException failure) {
                    selected.completeExceptionally(failure);
                }
            }
            while (true) {
                context.checkCancelled();
                try {
                    return selected.get(Math.min(50, MongoSnapshot.remainingMillis(session)), TimeUnit.MILLISECONDS);
                } catch (TimeoutException waiting) {
                    // Poll cancellation while another planner owns the bounded probe.
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new CancellationException("MongoDB split planning interrupted");
                } catch (ExecutionException failure) {
                    if (failure.getCause() instanceof RuntimeException runtime) {
                        throw runtime;
                    }
                    throw new DorisConnectorException("MongoDB split planning failed");
                }
            }
        }
    }
}
