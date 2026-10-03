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

package org.apache.doris.common.profile;

import org.apache.doris.common.Config;
import org.apache.doris.common.QueryLogContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * One planning pass. The planning thread owns timers and the bounded slow-operation buffer.
 * The connection checker reads only immutable, volatile-published steps, never planner/table locks.
 * Timings are inclusive: nested phases and operations must not be added to their enclosing phase.
 */
public final class PlanningDiagnostics {
    private static final Logger LOG = LogManager.getLogger(PlanningDiagnostics.class);
    private static final int MAX_SLOW_OPERATIONS = 16;

    public enum Phase {
        PREPROCESS, COLLECT_TABLES, PRELOAD_METADATA, WAIT_CHANGE_VISIBLE, LOCK_TABLES,
        ANALYZE, REWRITE, PRE_REWRITE_MV, OPTIMIZE, CHOOSE_PLAN, POST_PROCESS,
        TRANSLATE, DISTRIBUTE, RELEASE_RESOURCES;

        public String key() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    private final ConnectContext context;
    private final SummaryProfile summary;
    private final long passId;
    private PlanningDiagnostics parent;
    private final TUniqueId queryId;
    private final long statementId;
    private final String threadName;
    private volatile long timeoutMs;
    private final LongSupplier clock;
    private final long started;
    private final EnumMap<Phase, Timing> timings = new EnumMap<>(Phase.class);
    private final List<Event> slowOperations = new ArrayList<>();
    private final AtomicLong lastReport = new AtomicLong(Long.MIN_VALUE);
    private volatile Step current;
    private volatile Class<?> currentJob;
    private volatile HeldLocks heldLocks = new HeldLocks(0, 0, "");
    private volatile boolean finished;
    private String failedStep = "";
    private Throwable stepFailure;
    private long omittedOperations;

    private PlanningDiagnostics(ConnectContext context) {
        this(context, System::nanoTime);
    }

    @VisibleForTesting
    PlanningDiagnostics(ConnectContext context, LongSupplier clock) {
        this.context = context;
        this.summary = SummaryProfile.getSummaryProfile(context);
        this.passId = summary == null ? 0 : summary.nextPlanningPassId();
        this.queryId = context.queryId() == null ? null : context.queryId().deepCopy();
        this.statementId = context.getStmtId();
        this.threadName = Thread.currentThread().getName();
        this.timeoutMs = context.getExecTimeoutS() * 1000L;
        this.clock = clock;
        this.started = clock.getAsLong();
    }

    /** Includes cleanup and restores an outer pass when MV planning nests on the same connection. */
    public static <T> T plan(ConnectContext context, Supplier<T> action) {
        return execute(new PlanningDiagnostics(context), action);
    }

    @VisibleForTesting
    static <T> T execute(PlanningDiagnostics diagnostics, Supplier<T> action) {
        ConnectContext context = diagnostics.context;
        PlanningDiagnostics previous = context.getPlanningDiagnostics();
        diagnostics.parent = previous;
        context.setPlanningDiagnostics(diagnostics);
        boolean success = false;
        try {
            T result = action.get();
            success = true;
            return result;
        } catch (RuntimeException | Error failure) {
            if (diagnostics.stepFailure != failure) {
                diagnostics.failedStep = "between_phases";
            }
            throw failure;
        } finally {
            diagnostics.finished = true;
            context.setPlanningDiagnostics(previous);
            diagnostics.finish(success);
        }
    }

    public static void runPhase(ConnectContext context, Phase phase, Runnable action) {
        phase(context, phase, () -> {
            action.run();
            return null;
        });
    }

    public static <T> T phase(ConnectContext context, Phase phase, Supplier<T> action) {
        PlanningDiagnostics diagnostics = current(context);
        return diagnostics == null ? action.get() : diagnostics.measure(phase, "", "", -1, action);
    }

    /** A negative operation budget means that timeout handling belongs to the connector. */
    public static <T> T operation(ConnectContext context, String operation, String target,
            long budgetMs, Supplier<T> action) {
        PlanningDiagnostics diagnostics = current(context);
        if (diagnostics == null) {
            return action.get();
        }
        Step step = diagnostics.current;
        return diagnostics.measure(step == null ? Phase.PREPROCESS : step.phase,
                operation, target, budgetMs, action);
    }

    public static PlanningDiagnostics current(ConnectContext context) {
        return context == null ? null : context.getPlanningDiagnostics();
    }

    private <T> T measure(Phase phase, String operation, String target, long budgetMs, Supplier<T> action) {
        Step previous = current;
        Step step = new Step(phase, operation, target, budgetMs, clock.getAsLong(),
                operation.isEmpty() || previous == null ? -1 : previous.phaseStarted);
        current = step;
        boolean success = false;
        try {
            T result = action.get();
            success = true;
            return result;
        } catch (RuntimeException | Error failure) {
            // Preserve the innermost failing step of this exception, not an earlier recovered MV failure.
            if (stepFailure != failure) {
                stepFailure = failure;
                failedStep = phase.key() + (operation.isEmpty() ? "" : "/" + operation);
            }
            throw failure;
        } finally {
            long elapsed = clock.getAsLong() - step.started;
            if (operation.isEmpty()) {
                Timing timing = timings.computeIfAbsent(phase, key -> new Timing());
                timing.nanos += elapsed;
                timing.calls++;
                timing.failures += success ? 0 : 1;
                if (phase == Phase.PREPROCESS && success) {
                    // SET_VAR hints are applied by preprocessing, after this pass was created.
                    timeoutMs = context.getExecTimeoutS() * 1000L;
                }
            } else {
                recordOperation(step, elapsed, success);
            }
            current = previous;
        }
    }

    /** Avoid allocating task descriptions for every optimizer job; format the class only on a slow report. */
    public Class<?> setCurrentJob(Class<?> job) {
        Class<?> previous = currentJob;
        currentJob = job;
        return previous;
    }

    public LockHold acquiredLock(String table) {
        long now = clock.getAsLong();
        HeldLocks previous = heldLocks;
        heldLocks = new HeldLocks(previous.count + 1, previous.count == 0 ? now : previous.started,
                previous.count == 0 ? table : previous.oldestTable);
        return new LockHold(table, now);
    }

    /** Closed after unlocking; planner resources release in reverse acquisition order. */
    public final class LockHold implements AutoCloseable {
        private final String table;
        private final long acquired;

        private LockHold(String table, long acquired) {
            this.table = table;
            this.acquired = acquired;
        }

        @Override
        public void close() {
            HeldLocks previous = heldLocks;
            heldLocks = new HeldLocks(previous.count - 1, previous.started, previous.oldestTable);
            recordOperation(new Step(Phase.RELEASE_RESOURCES, "read_lock_hold", table, -1, acquired, -1),
                    clock.getAsLong() - acquired, true);
        }
    }

    private void recordOperation(Step step, long nanos, boolean success) {
        long threshold = Config.nereids_planning_operation_log_threshold_ms;
        if (success && (threshold <= 0 || millis(nanos) < threshold)) {
            return;
        }
        if (slowOperations.size() == MAX_SLOW_OPERATIONS) {
            omittedOperations++;
            return;
        }
        JsonObject event = step.toJson();
        event.addProperty("elapsed_ms", millis(nanos));
        event.addProperty("status", success ? "completed" : "failed");
        slowOperations.add(new Event(this, "Planning operation", event));
    }

    /** Invoked by the existing connection timeout checker, including while a planner job is blocked. */
    public void reportIfSlow() {
        long threshold = Config.nereids_planning_log_threshold_ms;
        long now = clock.getAsLong();
        Step step = current;
        if (finished || threshold <= 0 || millis(now - started) < threshold) {
            return;
        }
        long previous = lastReport.get();
        // A minimum interval protects the checker from an accidentally zero/negative dynamic setting.
        long interval = Math.max(1000, Config.nereids_planning_log_interval_ms);
        if ((previous != Long.MIN_VALUE && millis(now - previous) < interval)
                || !lastReport.compareAndSet(previous, now)) {
            return;
        }
        JsonObject event = step == null ? new JsonObject() : step.toJson();
        event.addProperty("status", "running");
        event.addProperty("elapsed_ms", millis(now - started));
        event.addProperty("phase_elapsed_ms", step == null ? 0 : millis(now - step.phaseStarted));
        event.addProperty("operation_elapsed_ms", step == null || step.operation.isEmpty()
                ? 0 : millis(now - step.started));
        Class<?> job = currentJob;
        event.addProperty("job", job == null ? "" : job.getSimpleName());
        HeldLocks oldest = heldLocks;
        int lockCount = oldest.count;
        for (PlanningDiagnostics outer = parent; outer != null; outer = outer.parent) {
            HeldLocks locks = outer.heldLocks;
            lockCount += locks.count;
            if (locks.count > 0 && (oldest.count == 0 || locks.started < oldest.started)) {
                oldest = locks;
            }
        }
        event.addProperty("held_locks", lockCount);
        event.addProperty("oldest_lock", lockCount == 0 ? "" : bounded(oldest.oldestTable));
        event.addProperty("oldest_lock_hold_ms", lockCount == 0 ? 0 : millis(now - oldest.started));
        log("Slow planning", event);
    }

    private void finish(boolean success) {
        JsonObject result = new JsonObject();
        result.addProperty("pass_id", passId);
        result.addProperty("status", success ? "completed" : "failed");
        result.addProperty("elapsed_ms", millis(clock.getAsLong() - started));
        result.addProperty("failed_step", success ? "" : failedStep);
        JsonObject phases = new JsonObject();
        for (Phase phase : Phase.values()) {
            Timing timing = timings.get(phase);
            JsonObject value = new JsonObject();
            value.addProperty("status", timing == null ? "not_run" : timing.failures > 0 ? "failed" : "completed");
            value.addProperty("elapsed_ms", timing == null ? 0 : millis(timing.nanos));
            value.addProperty("calls", timing == null ? 0 : timing.calls);
            phases.add(phase.key(), value);
        }
        result.add("phases", phases);
        result.addProperty("omitted_operations", omittedOperations);
        if (summary != null) {
            summary.addPlanningPass(result);
        }
        long threshold = Config.nereids_planning_log_threshold_ms;
        boolean report = !success || !slowOperations.isEmpty()
                || (threshold > 0 && result.get("elapsed_ms").getAsLong() >= threshold);
        // An enclosing MV planning pass may still hold locks. Defer its children's logs as well.
        if (parent != null) {
            for (Event event : slowOperations) {
                parent.defer(event);
            }
            parent.omittedOperations += omittedOperations;
            if (report) {
                parent.defer(new Event(this, "Planning finished", result));
            }
        } else {
            for (Event event : slowOperations) {
                event.owner.log(event.message, event.json);
            }
            if (report) {
                log("Planning finished", result);
            }
        }
    }

    private void defer(Event event) {
        if (slowOperations.size() == MAX_SLOW_OPERATIONS) {
            omittedOperations++;
        } else {
            slowOperations.add(event);
        }
    }

    private void log(String message, JsonObject event) {
        event.addProperty("pass_id", passId);
        event.addProperty("statement_id", statementId);
        event.addProperty("planner_thread", threadName);
        event.addProperty("query_timeout_ms", timeoutMs);
        try (QueryLogContext ignored = QueryLogContext.open(queryId)) {
            LOG.warn("{}{}: {}", message, QueryLogContext.queryIdSuffix(queryId), event);
        }
    }

    private static long millis(long nanos) {
        return TimeUnit.NANOSECONDS.toMillis(nanos);
    }

    private static String bounded(String text) {
        return text.substring(0, Math.min(text.length(), 256)).replace('\n', ' ').replace('\r', ' ');
    }

    private static final class Event {
        private final PlanningDiagnostics owner;
        private final String message;
        private final JsonObject json;

        private Event(PlanningDiagnostics owner, String message, JsonObject json) {
            this.owner = owner;
            this.message = message;
            this.json = json;
        }
    }

    private static final class Timing {
        private long nanos;
        private long calls;
        private long failures;
    }

    private static final class HeldLocks {
        private final int count;
        private final long started;
        private final String oldestTable;

        private HeldLocks(int count, long started, String oldestTable) {
            this.count = count;
            this.started = started;
            this.oldestTable = oldestTable;
        }
    }

    private static final class Step {
        private final Phase phase;
        private final String operation;
        private final String target;
        private final long budgetMs;
        private final long started;
        private final long phaseStarted;

        private Step(Phase phase, String operation, String target, long budgetMs, long started, long phaseStarted) {
            this.phase = phase;
            this.operation = operation;
            this.target = target;
            this.budgetMs = budgetMs;
            this.started = started;
            this.phaseStarted = phaseStarted == -1 ? started : phaseStarted;
        }

        private JsonObject toJson() {
            JsonObject json = new JsonObject();
            json.addProperty("phase", phase.key());
            json.addProperty("operation", operation);
            json.addProperty("target", bounded(target));
            json.addProperty("operation_timeout_ms", budgetMs);
            return json;
        }
    }
}
